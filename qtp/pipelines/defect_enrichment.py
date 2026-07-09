"""Pipeline: manufacturing__defect_enrichment.

Enrich defect records with full supplier traceability, classify them, quarantine them,
and write to the defective-parts warehouse. Feeds from the quality_ingest defect lane.

    Kafka (quality.defects.major)
        -> kafka_header_enricher    (standard metadata headers)
        -> data_quality_tagger      (dq_score / dq_status)
        -> schema_version_tagger    (schema_version - ISO audit)
        -> JDBC Lookup              (MySQL ERP: batch → supplier + grade + certificate)
        -> HTTP Client classify     (POST /defects/classify → defect_category, recommended_action)
        -> Expression Evaluator     (lift classify response fields onto record root)
        -> [fan-out]
             main spine : HTTP Client quarantine (PUT /quarantine/register)
                               -> Expression Evaluator (quarantine_id + traceability_completed_at)
                                   -> Field Remover (drop non-column fields)
                                       -> JDBC Producer (Postgres quality.defective_parts)
             alert branch : Stream Selector (defect_category == 'CRITICAL')
                               lane 0 CRITICAL -> alert_notification_dispatcher
                               lane 1 default  -> Trash
    Error records -> Amazon S3 (MinIO kermit-error-dlq)
    Pipeline stop -> audit_log_writer fragment (quality.pipeline_audit)  [pending lifecycle label]

Fan-out semantics: classify_ee has a single output lane, so `>>`-ing it to both the
quarantine chain and the alert Stream Selector broadcasts every record to both. The main
spine quarantines and writes EVERY defect; the alert branch only raises a notification for
CRITICAL ones (a precondition can't be set on a fragment-instance wrapper via the SDK, so a
Stream Selector + Trash does the severity filtering instead).

JDBC Lookup assumes the MySQL driver (mariadb or mysql) is installed in the SDC.
WireMock endpoints are internal network hostnames (wiremock:8080).
"""

from ..common.connections import apply_connection
from ..common.fragments import resolve_fragment
from ..common.util import get_logger, set_config, set_first

PIPELINE_NAME = "manufacturing__defect_enrichment"
LABELS = ["industry:manufacturing", "env:dev"]

INLINE_FRAGMENTS = ["kafka_header_enricher", "data_quality_tagger", "schema_version_tagger"]

# Rollup window for the Teams/Jira alert lanes (see the rollup-gate JavaScript stages in
# build()): the first CRITICAL/MAJOR defect for a given (line_id, severity) fires one real
# alert immediately; further defects for that same key inside the window are counted, not
# paged, and their running total is reported on the NEXT alert ("+N more since the last
# alert"). This caps the outbound Teams/Jira call rate - the fix for the Power Automate
# connector action-limit and the Jira flood - while preserving the suppressed count, unlike
# a plain dedup-and-drop. The audit sink (WireMock dispatcher) and the Postgres
# quarantine/defective_parts spine still record 100% of defects regardless.
ALERT_ROLLUP_WINDOW_SECS = 300

log = get_logger()

# Stateful rollup gate (shared by the CRITICAL and MAJOR lanes; each stage instance keeps
# its own `state`, which the SDC JavaScript Evaluator persists across batches for the run).
# For each (line_id, defect_category) key it opens a time window: the first record of a
# window is flagged /alert_emit='true' and carries /rolled_up_count = however many were
# suppressed in the PREVIOUS window (0 on the very first). Every later record inside the
# open window is flagged /alert_emit='false' and increments that window's counter. A
# downstream Stream Selector routes 'true' to the real Teams/Jira delivery and everything
# else to Trash. Processing-time (wall-clock) windows are the right basis for rate-limiting
# outbound notifications. %WINDOW_MS% is substituted from ALERT_ROLLUP_WINDOW_SECS.
_ROLLUP_GATE_SCRIPT = """
if (!state.win) { state.win = {}; }
var WINDOW_MS = %WINDOW_MS%;
for (var i = 0; i < records.length; i++) {
  var r = records[i];
  try {
    var key = r.value['line_id'] + '|' + r.value['defect_category'];
    var now = new Date().getTime();
    var w = state.win[key];
    if (!w || (now - w.start) >= WINDOW_MS) {
      r.value['alert_emit'] = 'true';
      r.value['rolled_up_count'] = w ? w.suppressed : 0;
      state.win[key] = { start: now, suppressed: 0 };
    } else {
      w.suppressed = w.suppressed + 1;
      r.value['alert_emit'] = 'false';
      r.value['rolled_up_count'] = 0;
    }
    output.write(r);
  } catch (e) {
    error.write(r, 'rollup gate error: ' + e);
  }
}
"""

# Builds the Jira Cloud REST v3 "create issue" payload for a CRITICAL defect. Jira's
# `description` field requires Atlassian Document Format (a nested doc/paragraph/text
# structure) - not a plain string - so this is built in JavaScript rather than via
# Expression Evaluator field paths, which is far more reliable for nested JSON. ${...}
# parameter substitution in the script text mirrors the already-working pattern in the
# reused alert_notification_dispatcher fragment's own JavaScript stage.
_JIRA_PAYLOAD_SCRIPT = """
for (var i = 0; i < records.length; i++) {
  try {
    var r = records[i];
    var partId    = r.value['part_id'];
    var lineId    = r.value['line_id'];
    var category  = r.value['defect_category'];
    var codes     = r.value['defect_codes_str'];
    var supplier  = r.value['supplier_name'];
    var ts        = r.value['inspection_timestamp'];

    var rolled    = r.value['rolled_up_count'];

    var summary = 'CRITICAL quality defect: part ' + partId + ' on ' + lineId;
    var detail  = 'Part ' + partId + ' on line ' + lineId + ' failed inspection at ' + ts +
                  ' with defect code(s) ' + codes +
                  '. Supplier: ' + supplier + '. Category: ' + category + '.';
    if (rolled && rolled > 0) {
      detail += ' Rolled up: ' + rolled + ' further CRITICAL defect(s) on ' + lineId +
                ' occurred since the last alert and are represented by this ticket.';
    }

    r.value['jira_payload'] = JSON.stringify({
      fields: {
        project: { key: '${JIRA_PROJECT_KEY}' },
        issuetype: { name: '${JIRA_ISSUE_TYPE}' },
        summary: summary,
        description: {
          type: 'doc',
          version: 1,
          content: [ { type: 'paragraph', content: [ { type: 'text', text: detail } ] } ]
        }
      }
    });
    output.write(r);
  } catch (e) {
    error.write(records[i], 'jira payload build error: ' + e);
  }
}
"""

# Builds a Microsoft Teams message payload. A Teams webhook (whether classic "Incoming
# Webhook" or the newer "Workflows" one) will NOT render the reused dispatcher fragment's
# {id, anomaly_type, severity, timestamp} JSON - it needs an Adaptive Card wrapped in the
# `{type:"message", attachments:[...]}` envelope. Built in JavaScript because the payload is
# deeply nested. `${SEVERITY_LABEL}` is substituted per lane (CRITICAL vs MAJOR/WARN).
_TEAMS_CARD_SCRIPT = """
for (var i = 0; i < records.length; i++) {
  try {
    var r = records[i];
    var sev = '${SEVERITY_LABEL}';
    var title = sev + ' quality defect - part ' + r.value['part_id'];
    var rolled = r.value['rolled_up_count'];
    var body = [
      { type: 'TextBlock', size: 'Large', weight: 'Bolder', color: (sev === 'CRITICAL' ? 'Attention' : 'Warning'), text: title },
      { type: 'FactSet', facts: [
        { title: 'Part', value: '' + r.value['part_id'] },
        { title: 'Line', value: '' + r.value['line_id'] },
        { title: 'Category', value: '' + r.value['defect_category'] },
        { title: 'Defect codes', value: '' + r.value['defect_codes_str'] },
        { title: 'Supplier', value: '' + r.value['supplier_name'] },
        { title: 'Detected', value: '' + r.value['inspection_timestamp'] }
      ]}
    ];
    if (rolled && rolled > 0) {
      body.push({ type: 'TextBlock', wrap: true, isSubtle: true, spacing: 'Medium',
        text: '+ ' + rolled + ' more ' + sev + ' defect(s) on ' + r.value['line_id'] +
              ' since the last alert (rolled up, not individually paged).' });
    }
    r.value['teams_card'] = JSON.stringify({
      type: 'message',
      attachments: [{
        contentType: 'application/vnd.microsoft.card.adaptive',
        content: {
          '$schema': 'http://adaptivecards.io/schemas/adaptive-card.json',
          type: 'AdaptiveCard',
          version: '1.4',
          body: body
        }
      }]
    });
    output.write(r);
  } catch (e) {
    error.write(records[i], 'teams card build error: ' + e);
  }
}
"""


def build(sch, engine_id, name=None, line_id=None, env="dev"):
    """Build the defect-enrichment pipeline. line_id=None builds the all-lines pipeline;
    a line_id builds a dedicated per-line variant (name/group/label + line precondition),
    generated by quality_pipeline_manager. env sets the env:<env> label (dev/prod)."""
    builder = sch.get_pipeline_builder(engine_type="data_collector", engine_id=engine_id)
    group = "qtp-defect-enrichment" + (f"-{line_id}" if line_id else "")

    # --- Origin: consume defect events published by quality_ingest -------------------
    origin = builder.add_stage("Kafka Multitopic Consumer")
    set_config(
        origin,
        topic_list=["quality.defects.major"],
        consumer_group=group,
        data_format="JSON",
        broker_uri="${KAFKA_BROKER}",  # ignored once a named connection is applied below
    )
    apply_connection(origin, sch, "manufacturing__kafka__dev")
    # Per-line filter: Stream Selector after the origin keeps this line's records and
    # discards the rest (the Kafka origin has no record-precondition config).
    line_filter = line_trash = None
    if line_id:
        line_filter = builder.add_stage("Stream Selector")
        line_filter.predicates = [f"${{record:value('/line_id') == '{line_id}'}}", "default"]
        line_trash = builder.add_stage("Trash")

    # --- Inline enrichment fragments -------------------------------------------------
    enrichers = []
    for frag_name in INLINE_FRAGMENTS:
        fragment = resolve_fragment(sch, frag_name)
        enrichers.append(builder.add_fragment(fragment))

    # --- ERP traceability join: batch → supplier + material grade + certificate ------
    # Lookup key: /material_batch_id → matches erp.material_batches.batch_id.
    # Returns: supplier_id, supplier_name, material_grade, certificate_url.
    erp_lookup = builder.add_stage("JDBC Lookup")
    set_first(erp_lookup, "${MYSQL_JDBC_URL}", "connection_string", "jdbc_connection_string")
    set_config(
        erp_lookup,
        sql_query=(
            "SELECT mb.supplier_id, s.name AS supplier_name, "
            "mb.material_grade, mb.certificate_url "
            "FROM erp.material_batches mb "
            "JOIN erp.suppliers s ON mb.supplier_id = s.supplier_id "
            "WHERE mb.batch_id = '${record:value(\"/material_batch_id\")}'"
        ),
        use_credentials=True,
        username="${MYSQL_USER}",
        password="${MYSQL_PASSWORD}",
    )
    apply_connection(erp_lookup, sch, "manufacturing__mysql__dev")

    # --- Classify API: POST defect codes → category + recommended action -------------
    # The whole record is sent as JSON; WireMock stubs match on defect code substrings.
    # type="processor": there is also an "HTTP Client" *destination* (fire-and-forget);
    # we need the processor that issues the request and writes the response back onto
    # the record (output_field), so classify_response is available downstream.
    classify = builder.add_stage("HTTP Client", type="processor")
    set_config(
        classify,
        resource_url="${CLASSIFY_URL}",
        http_method="POST",
        data_format="JSON",
        output_field="/classify_response",
        # The processor sends nothing by default; request_data is the request body. Send the
        # whole record as JSON so the classify API can match on the defect codes it contains.
        request_data="${record:value('/')}",
    )

    # Lift classify response fields onto the record root (for JDBC column mapping).
    classify_ee = builder.add_stage("Expression Evaluator")
    set_first(
        classify_ee,
        [
            {"fieldToSet": "/defect_category",    "expression": "${record:value('/classify_response/defect_category')}"},
            {"fieldToSet": "/recommended_action", "expression": "${record:value('/classify_response/recommended_action')}"},
        ],
        "field_expressions",
        "expression_processor_configs",
    )

    # Flatten /defect_codes (a LIST) into a plain readable string, /defect_codes_str.
    # Neither EL nor a straight JS JSON.stringify() render this cleanly: EL's
    # record:value() on a LIST gives each element's raw Field[TYPE:value] toString, this
    # engine has no str:join EL function ("No function is mapped to the name"), and
    # JSON.stringify() on the Java-backed list Rhino hands back from r.value[...] returns
    # undefined rather than an array literal. A plain indexed loop (bracket access +
    # .length, which Rhino's LiveConnect does support for a Java List) is the one thing
    # that reliably works. Every downstream alert/email reads /defect_codes_str, never
    # /defect_codes directly.
    codes_join = builder.add_stage("JavaScript Evaluator")
    codes_join.script = """
for (var i = 0; i < records.length; i++) {
  try {
    var r = records[i];
    var list = r.value['defect_codes'];
    var codes = '';
    if (list) {
      for (var j = 0; j < list.length; j++) {
        if (j > 0) { codes += ', '; }
        codes += list[j];
      }
    }
    r.value['defect_codes_str'] = codes;
    output.write(r);
  } catch (e) {
    error.write(records[i], 'defect_codes join error: ' + e);
  }
}
"""

    # --- Severity-routed alerting (brief §2.1 step 8 / Master §4.2) ------------------
    # MINOR -> INFO (email) · MAJOR -> WARN (Teams) · CRITICAL -> Teams + Jira. Every
    # CRITICAL/MAJOR also fans out to the reused alert_notification_dispatcher fragment,
    # which POSTs to the WireMock alerts stub - an auditable central alert trail (Master §4.2
    # "all alerts route through this fragment") that also backs the qtp_alerts_fired_total
    # Prometheus metric. The dispatcher's fixed payload won't render in Teams, so the REAL
    # human-facing Teams notification is a proper Adaptive Card posted by a dedicated HTTP
    # Client (below) - the fragment stays the audit sink, our stage does the real delivery.
    #
    # Real secrets (Teams webhook URL, Jira email/token) are NEVER baked into this pipeline's
    # own parameters below (safe placeholders only) - they are supplied as CONTROL HUB JOB
    # runtime parameters at job-creation time by `qtp jobs create` (see qtp/jobs/job_manager.py
    # + qtp/common/env.py:job_runtime_secrets()), so they never appear in the exported JSON.
    alert_selector = builder.add_stage("Stream Selector")
    #   lane 0 CRITICAL -> audit + Teams card + Jira · lane 1 MAJOR -> audit + Teams card · lane 2 MINOR -> email.
    alert_selector.predicates = [
        "${record:value('/defect_category') == 'CRITICAL'}",
        "${record:value('/defect_category') == 'MAJOR'}",
        "default",
    ]

    # Reused shared fragment = the WireMock audit sink for every CRITICAL/MAJOR alert.
    alert_frag_obj = resolve_fragment(sch, "alert_notification_dispatcher")
    alert_frag = builder.add_fragment(alert_frag_obj)

    # Real Teams delivery: one HTTP Client fed by both severity lanes (each builds its own
    # /teams_card via the shapers below), POSTing the Adaptive Card to the webhook (no auth).
    teams_http = builder.add_stage("HTTP Client", type="processor")
    set_config(
        teams_http,
        resource_url="${TEAMS_ALERT_WEBHOOK_URL}",
        http_method="POST",
        data_format="JSON",
        output_field="/teams_response",
        request_data="${record:value('/teams_card')}",
        max_retries=3,
        base_backoff_interval_in_ms=1000,
        action_for_timeout="RETRY_EXPONENTIAL_BACKOFF",
    )
    teams_trash = builder.add_stage("Trash")

    # --- CRITICAL lane: severity + Teams card + Jira payload, then fan out -------------
    critical_shaper = builder.add_stage("JavaScript Evaluator")
    critical_shaper.script = (
        _TEAMS_CARD_SCRIPT.replace("${SEVERITY_LABEL}", "CRITICAL")
    )
    # A second JS to add the Jira payload + severity (kept separate so each script stays
    # single-purpose and readable). Runs after the Teams card is built.
    critical_meta = builder.add_stage("Expression Evaluator")
    set_first(
        critical_meta,
        [{"fieldToSet": "/severity", "expression": "CRITICAL"},
         {"fieldToSet": "/timestamp", "expression": "${record:value('/inspection_timestamp')}"}],
        "field_expressions", "expression_processor_configs",
    )

    jira_shaper = builder.add_stage("JavaScript Evaluator")
    jira_shaper.script = _JIRA_PAYLOAD_SCRIPT

    jira_http = builder.add_stage("HTTP Client", type="processor")
    set_config(
        jira_http,
        resource_url="${JIRA_BASE_URL}/rest/api/3/issue",
        http_method="POST",
        data_format="JSON",
        output_field="/jira_response",
        request_data="${record:value('/jira_payload')}",
        authentication_type="BASIC",
        username="${JIRA_USER_EMAIL}",
        password="${JIRA_API_TOKEN}",
        max_retries=3,
        base_backoff_interval_in_ms=1000,
        action_for_timeout="RETRY_EXPONENTIAL_BACKOFF",
    )
    jira_trash = builder.add_stage("Trash")  # absorb the Jira API response; nothing downstream needs it

    # --- Alert rollup gate: protect Teams/Jira from a hot line paging on every record --
    # A line producing CRITICAL/MAJOR defects back-to-back was posting a Teams card + Jira
    # ticket per record, which tripped Power Automate's own connector action-limit ("Your
    # operation has been throttled") and flooded Jira. Each severity lane runs the stateful
    # _ROLLUP_GATE_SCRIPT: the first defect of a (line_id, severity) window fires one alert
    # carrying the count suppressed since the previous alert; later defects in the window are
    # counted, not paged. A 2-lane Stream Selector then routes /alert_emit=='true' to the
    # real Teams/Jira delivery and the rest to Trash (suppressed, NOT an error). The audit
    # sink (alert_frag) and the Postgres spine still see 100% of defects - only the outbound
    # notification rate is capped; the suppressed count is preserved on the next alert.
    _gate_script = _ROLLUP_GATE_SCRIPT.replace("%WINDOW_MS%", str(ALERT_ROLLUP_WINDOW_SECS * 1000))

    critical_gate = builder.add_stage("JavaScript Evaluator")
    critical_gate.script = _gate_script
    critical_gate_selector = builder.add_stage("Stream Selector")
    critical_gate_selector.predicates = ["${record:value('/alert_emit') == 'true'}", "default"]
    critical_gate_trash = builder.add_stage("Trash")

    major_gate = builder.add_stage("JavaScript Evaluator")
    major_gate.script = _gate_script
    major_gate_selector = builder.add_stage("Stream Selector")
    major_gate_selector.predicates = ["${record:value('/alert_emit') == 'true'}", "default"]
    major_gate_trash = builder.add_stage("Trash")

    # --- MAJOR lane: severity=WARN + Teams card ----------------------------------------
    major_shaper = builder.add_stage("JavaScript Evaluator")
    major_shaper.script = _TEAMS_CARD_SCRIPT.replace("${SEVERITY_LABEL}", "MAJOR")
    major_meta = builder.add_stage("Expression Evaluator")
    set_first(
        major_meta,
        [{"fieldToSet": "/severity", "expression": "WARN"},
         {"fieldToSet": "/timestamp", "expression": "${record:value('/inspection_timestamp')}"}],
        "field_expressions", "expression_processor_configs",
    )

    # --- MINOR lane (default): INFO via email --------------------------------------
    # NOTE (residual verification): requires the engine's SMTP relay to be configured
    # (mail.smtp.host/port in the engine's sdc.properties, pointed at Mailhog) + a restart
    # to apply - this is the one alerting tier not yet proven live end-to-end. If the relay
    # isn't configured, MINOR defects fail this stage and land in the DLQ (visible, not
    # silently lost) instead of emailing. Confirm with a live preview once the relay is set.
    minor_email = builder.add_stage("Email")
    minor_email.email_configuration = [{
        "email": ["${QUALITY_MANAGER_EMAIL}"],
        "subject": "INFO: MINOR defect - ${record:value('/part_id')} (${record:value('/line_id')})",
        "body": (
            "MINOR defect recorded (informational - no action required).\n\n"
            "Part: ${record:value('/part_id')}\nLine: ${record:value('/line_id')}\n"
            "Defect codes: ${record:value('/defect_codes_str')}\n"
            "Detected: ${record:value('/inspection_timestamp')}"
        ),
    }]

    # --- Quarantine API: PUT /quarantine/register (all defects) -----------------------
    # Compliance (brief §6): "Quarantine API call is mandatory. Retry 3 times with
    # exponential backoff before escalating." max_retries=3 + base_backoff_interval_in_ms=1000
    # with RETRY_EXPONENTIAL_BACKOFF gives ~1s/2s/4s backoff between attempts; once retries are
    # exhausted the record still fails the stage and is routed to the pipeline's error stage
    # (MinIO DLQ, below) - the escalation path is "visible in the DLQ + defect-rate/DLQ
    # monitoring" rather than silently dropped.
    quarantine = builder.add_stage("HTTP Client", type="processor")
    set_config(
        quarantine,
        resource_url="${QUARANTINE_URL}",
        http_method="PUT",
        data_format="JSON",
        output_field="/quarantine_response",
        request_data="${record:value('/')}",  # send the record as the request body
        max_retries=3,
        base_backoff_interval_in_ms=1000,
        action_for_timeout="RETRY_EXPONENTIAL_BACKOFF",
    )

    # Lift quarantine_id + stamp traceability_completed_at (ISO 9001 + 2-hour SLA anchor).
    quarantine_ee = builder.add_stage("Expression Evaluator")
    set_first(
        quarantine_ee,
        [
            {"fieldToSet": "/quarantine_id",             "expression": "${record:value('/quarantine_response/quarantine_id')}"},
            {"fieldToSet": "/traceability_completed_at", "expression": "${time:now()}"},
        ],
        "field_expressions",
        "expression_processor_configs",
    )

    # --- Strip fields defective_parts can't take (same lesson as quality_ingest) ------
    # Removed: the enrichment scratch fields (dq_*), the raw API response objects, the
    # nested measurements object, and the raw defect_codes LIST (the JDBC Producer can't
    # coerce a list into the JSONB column). What remains maps 1:1 to defective_parts:
    # part_id, line_id, inspection_timestamp, material_batch_id, operator_id, shift,
    # schema_version, supplier_id, supplier_name, material_grade, certificate_url,
    # defect_category, recommended_action, quarantine_id, traceability_completed_at -
    # plus defect_codes_str (a plain TEXT column, added specifically so this flattened,
    # already-comma-joined string survives to Postgres for the Grafana "top defect codes"
    # panel; the raw JSONB defect_codes column was otherwise always NULL).
    field_remover = builder.add_stage("Field Remover")
    set_first(
        field_remover,
        [
            "/measurements", "/dq_score", "/dq_status", "/dq_checked_at",
            "/classify_response", "/quarantine_response", "/defect_codes",
        ],
        "fields",
    )
    set_config(field_remover, action="REMOVE")

    # --- Destination: fully-enriched defect record → Postgres defective_parts --------
    defect_out = builder.add_stage("JDBC Producer")
    set_first(defect_out, "${PG_JDBC_URL}", "jdbc_connection_string", "connection_string")
    set_config(
        defect_out,
        schema_name="quality",
        table_name="defective_parts",
        use_credentials=True,
        username="${PG_USER}",
        password="${PG_PASSWORD}",
    )
    apply_connection(defect_out, sch, "manufacturing__postgres__dev")

    # --- DLQ error stage → MinIO kermit-error-dlq ------------------------------------
    # Label confirmed: "Write to Amazon S3" (canvas Error Records dropdown).
    dlq = builder.add_error_stage(label="Write to Amazon S3")
    set_config(
        dlq,
        authentication_method="WITH_CREDENTIALS",
        access_key_id="${MINIO_ACCESS_KEY}",
        secret_access_key="${MINIO_SECRET_KEY}",
        bucket="${DLQ_BUCKET}",
        common_prefix="defect-enrichment/",
        # No data_format: the error-records S3 target writes SDC's fixed error-record format.
        region_definition_for_s3="SPECIFY_NON_REGIONAL_ENDPOINT",
        custom_endpoint_for_s3="${MINIO_ENDPOINT}",
        signing_region_for_s3="US_EAST_1",
        use_path_style_address_model=True,
    )
    # See the identical note in quality_ingest.py: this error-stage variant reports no
    # supported_connection_types, but `.connection` exists - attempted, verify via validate.
    apply_connection(dlq, sch, "manufacturing__minio__dev", fallback_note="error-stage variant may not support connections")

    # --- Audit stop event (PENDING - same blocker as quality_ingest) ------------------
    # Uncomment once the correct lifecycle stage label is confirmed in the canvas:
    #
    # audit_frag_obj = sch.pipelines.get(name="audit_log_writer", fragment=True)
    # audit_stop = builder.add_stop_event_stage(label="<confirmed-label>")
    # set_first(audit_stop, "${AUDIT_JDBC_URL}", "jdbc_connection_string", "connection_string")
    # set_config(audit_stop, schema_name="${AUDIT_SCHEMA}", table_name="${AUDIT_TABLE}",
    #            use_credentials=True, username="${PG_USER}", password="${PG_PASSWORD}")

    # --- Wire ------------------------------------------------------------------
    # Chain: (line filter ->) origin → enrichers → ERP lookup → classify → classify_ee
    #        → quarantine → quarantine_ee → defective_parts
    if line_filter is not None:
        origin >> line_filter
        head = line_filter
    else:
        head = origin
    stage = head
    for enricher in enrichers:
        stage = stage >> enricher
    stage = stage >> erp_lookup >> classify >> classify_ee >> codes_join
    # Main spine: every defect is quarantined, enriched, and written to defective_parts.
    codes_join >> quarantine >> quarantine_ee >> field_remover >> defect_out
    # Fan-out: severity-routed alerting in parallel (broadcast - same records).
    codes_join >> alert_selector
    # lane 0 CRITICAL: set severity/timestamp -> audit EVERY defect -> rollup gate; the
    # gate's emit lane builds the Teams card + Jira payload (with the rolled-up count) and
    # fans out to both HTTP clients, its suppress lane goes to Trash.
    alert_selector >> critical_meta
    critical_meta >> alert_frag                               # audit sink (WireMock, every CRITICAL)
    critical_meta >> critical_gate >> critical_gate_selector
    critical_gate_selector >> critical_shaper >> jira_shaper  # emit lane: build both payloads in sequence
    jira_shaper >> teams_http >> teams_trash                  # real Teams Adaptive Card
    jira_shaper >> jira_http >> jira_trash                    # real Jira ticket
    critical_gate_selector >> critical_gate_trash             # suppress lane: counted, not paged
    # lane 1 MAJOR: set severity=WARN -> audit EVERY defect -> rollup gate -> Teams only.
    alert_selector >> major_meta
    major_meta >> alert_frag
    major_meta >> major_gate >> major_gate_selector
    major_gate_selector >> major_shaper >> teams_http         # emit lane: merges into the shared Teams stage
    major_gate_selector >> major_gate_trash                   # suppress lane: counted, not paged
    # lane 2 MINOR (default): informational email
    alert_selector >> minor_email
    if line_filter is not None:
        line_filter >> line_trash                  # other lines -> Trash

    full_name = name or (PIPELINE_NAME + (f"__{line_id}" if line_id else ""))
    labels = ["industry:manufacturing", f"env:{env}"] + ([f"line:{line_id}"] if line_id else [])
    pipeline = builder.build(full_name, description=__doc__.split("\n")[0], labels=labels)
    try:
        pipeline.configuration["errorRecordPolicy"] = "STAGE_RECORD"
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not set errorRecordPolicy (%s); using engine default.", exc)
    # NOTE: .update() (merge), not assignment. add_fragment() promotes the alert fragment's
    # declared parameters into the pipeline under a prefix and rewrites the fragment's
    # script/URL references to match; replacing pipeline.parameters wholesale would wipe
    # those promoted params and leave ${alert_01_ID_FIELD} unresolved. Merging preserves
    # them, and we override the oil&gas defaults with our manufacturing fields below.
    #
    # The prefix is the SDK default: first 5 chars of the fragment name + instance number,
    # i.e. "alert_" + "01_" = "alert_01_" for alert_notification_dispatcher's first instance.
    pipeline.parameters.update({
        "KAFKA_BROKER":    "kafka:29092",
        "MYSQL_JDBC_URL":  "jdbc:mysql://mysql:3306/erp?useUnicode=true&characterEncoding=UTF-8",
        "MYSQL_USER":      "qtp",
        "MYSQL_PASSWORD":  "qtp_pw",
        "CLASSIFY_URL":    "http://wiremock:8080/defects/classify",
        "QUARANTINE_URL":  "http://wiremock:8080/quarantine/register",
        # Alert fragment params (prefixed). id/metric/id-out-key drive the JS payload;
        # severity/timestamp come from the alert_shaper EE (fixed field names).
        "alert_01_ID_FIELD":            "part_id",
        "alert_01_METRIC_FIELD":        "defect_category",
        "alert_01_ID_OUT_KEY":          "part_id",
        # The dispatcher stays on WireMock: it is the auditable alert trail (Master §4.2) +
        # the qtp_alerts_fired_total Prometheus source - NOT the real Teams delivery. So these
        # keep the WireMock default and are NOT overridden by the job.
        "alert_01_MAINTENANCE_API_URL": "http://wiremock:8080/alerts",
        "MAINTENANCE_API_URL":          "http://wiremock:8080/alerts",
        # Real Teams webhook: SAFE PLACEHOLDER default (WireMock, not a secret). `qtp jobs
        # create` overrides this with the real TEAMS_ALERT_WEBHOOK_URL from .env as a JOB
        # runtime parameter - never baked into the published pipeline or the exported JSON.
        "TEAMS_ALERT_WEBHOOK_URL":      "http://wiremock:8080/alerts",
        "PG_JDBC_URL":     "jdbc:postgresql://postgres:5432/quality",
        "PG_USER":         "qtp",
        "PG_PASSWORD":     "qtp_pw",
        "MINIO_ENDPOINT":  "https://minio:9000",  # TLS required by SDC's AWS SDK (see certs/)
        "MINIO_ACCESS_KEY": "minioadmin",
        "MINIO_SECRET_KEY": "minioadmin",
        "DLQ_BUCKET":      "kermit-error-dlq",
        "AUDIT_JDBC_URL":  "jdbc:postgresql://postgres:5432/quality",
        "AUDIT_SCHEMA":    "quality",
        "AUDIT_TABLE":     "pipeline_audit",
        "SOURCE_SYSTEM":   "manufacturing",
        "ENVIRONMENT":     "dev",
        "SCHEMA_VERSION":  "1",
        # Real Jira credentials: SAFE PLACEHOLDERS only (never real secrets in the
        # published pipeline). `qtp jobs create` overrides these with the real values
        # from .env as job runtime parameters, exactly like the Teams URL above.
        "JIRA_BASE_URL":    "http://wiremock:8080",
        "JIRA_USER_EMAIL":  "",
        "JIRA_API_TOKEN":   "",
        "JIRA_PROJECT_KEY": "QUAL",
        "JIRA_ISSUE_TYPE":  "Task",
        # Not a secret - safe to bake in directly.
        "QUALITY_MANAGER_EMAIL": "quality.manager@kermit.local",
    })
    return pipeline
