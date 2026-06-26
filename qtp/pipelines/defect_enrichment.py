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
             alert lane  : alert_notification_dispatcher (CRITICAL only, precondition)
             quarantine  : HTTP Client (PUT /quarantine/register; retry×3)
                               -> Expression Evaluator (quarantine_id + traceability_completed_at)
                                   -> JDBC Producer (Postgres quality.defective_parts)
    Error records -> Amazon S3 (MinIO kermit-error-dlq)
    Pipeline stop -> audit_log_writer fragment (quality.pipeline_audit)

Fan-out semantics: after classify_ee, both alert_frag and quarantine receive all records.
alert_frag has a precondition `${record:value('/defect_category') == 'CRITICAL'}` and
stage_on_record_error=DISCARD, so non-CRITICAL records are silently dropped in that branch.
All records continue through the quarantine → JDBC chain on the main branch.

JDBC Lookup assumes the MySQL driver (mariadb or mysql) is installed in the SDC.
WireMock endpoints are internal network hostnames (wiremock:8080).
"""

from ..common.util import get_logger, set_config, set_first

PIPELINE_NAME = "manufacturing__defect_enrichment"
LABELS = ["industry:manufacturing", "env:dev"]

INLINE_FRAGMENTS = ["kafka_header_enricher", "data_quality_tagger", "schema_version_tagger"]

log = get_logger()


def build(sch, engine_id, name=None):
    builder = sch.get_pipeline_builder(engine_type="data_collector", engine_id=engine_id)

    # --- Origin: consume defect events published by quality_ingest -------------------
    origin = builder.add_stage("Kafka Multitopic Consumer")
    set_config(
        origin,
        topic_list=["quality.defects.major"],
        consumer_group="qtp-defect-enrichment",
        data_format="JSON",
        broker_uri="${KAFKA_BROKER}",
    )

    # --- Inline enrichment fragments -------------------------------------------------
    enrichers = []
    for frag_name in INLINE_FRAGMENTS:
        fragment = sch.pipelines.get(name=frag_name, fragment=True)
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

    # --- Classify API: POST defect codes → category + recommended action -------------
    # The whole record is sent as JSON; WireMock stubs match on defect code substrings.
    classify = builder.add_stage("HTTP Client")
    set_config(
        classify,
        resource_url="${CLASSIFY_URL}",
        http_method="POST",
        data_format="JSON",
        output_field="/classify_response",
        default_request_content_type="application/json",
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

    # --- CRITICAL alert fan-out (teammate fragment; non-CRITICAL silently discarded) -
    alert_frag_obj = sch.pipelines.get(name="alert_notification_dispatcher", fragment=True)
    alert_frag = builder.add_fragment(alert_frag_obj)
    alert_frag.stage_record_preconditions = ["${record:value('/defect_category') == 'CRITICAL'}"]
    alert_frag.stage_on_record_error = "DISCARD"

    # --- Quarantine API: PUT /quarantine/register (all defects, retry on transient err)
    quarantine = builder.add_stage("HTTP Client")
    set_config(
        quarantine,
        resource_url="${QUARANTINE_URL}",
        http_method="PUT",
        data_format="JSON",
        output_field="/quarantine_response",
        default_request_content_type="application/json",
        # Retry configuration attribute names vary by SDC version; set_config warns + skips unknowns.
        num_retries=3,
        wait_time_before_retry=1000,
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

    # --- Audit stop event (PENDING — same blocker as quality_ingest) ------------------
    # Uncomment once the correct lifecycle stage label is confirmed in the canvas:
    #
    # audit_frag_obj = sch.pipelines.get(name="audit_log_writer", fragment=True)
    # audit_stop = builder.add_stop_event_stage(label="<confirmed-label>")
    # set_first(audit_stop, "${AUDIT_JDBC_URL}", "jdbc_connection_string", "connection_string")
    # set_config(audit_stop, schema_name="${AUDIT_SCHEMA}", table_name="${AUDIT_TABLE}",
    #            use_credentials=True, username="${PG_USER}", password="${PG_PASSWORD}")

    # --- Wire ------------------------------------------------------------------
    # Chain: origin → enrichers → ERP lookup → classify → classify_ee
    stage = origin
    for enricher in enrichers:
        stage = stage >> enricher
    stage = stage >> erp_lookup >> classify >> classify_ee

    # Fan-out from classify_ee:
    #   branch 1 (alert)     → alert_notification_dispatcher [terminates; CRITICAL only via precondition]
    #   branch 2 (quarantine chain) → quarantine → quarantine_ee → defect_out
    classify_ee >> alert_frag
    classify_ee >> quarantine
    quarantine >> quarantine_ee >> defect_out

    pipeline = builder.build(name or PIPELINE_NAME, description=__doc__.split("\n")[0], labels=LABELS)
    try:
        pipeline.configuration["errorRecordPolicy"] = "STAGE_RECORD"
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not set errorRecordPolicy (%s); using engine default.", exc)
    pipeline.parameters = {
        "KAFKA_BROKER":    "kafka:29092",
        "MYSQL_JDBC_URL":  "jdbc:mysql://mysql:3306/erp",
        "MYSQL_USER":      "qtp",
        "MYSQL_PASSWORD":  "qtp_pw",
        "CLASSIFY_URL":    "http://wiremock:8080/defects/classify",
        "QUARANTINE_URL":  "http://wiremock:8080/quarantine/register",
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
    }
    return pipeline
