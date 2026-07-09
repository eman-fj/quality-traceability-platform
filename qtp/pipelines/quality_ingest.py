"""Pipeline: manufacturing__quality_ingest.

Stage A - Ingest + enrich + route:

    Kafka (quality.inspections)
        -> kafka_header_enricher   (standard metadata headers)
        -> data_quality_tagger     (dq_score / dq_status)
        -> schema_version_tagger   (schema_version on every record - ISO audit)
        -> Stream Selector
             -> defect lane : Kafka Producer (quality.defects.major)   [enrich in defect_enrichment]
             -> clean lane  : JDBC Producer  (quality.conforming_parts)

Stage B - Resilience + compliance:

    Error records -> Amazon S3 (MinIO kermit-error-dlq)
    Pipeline stop -> audit_log_writer fragment (JDBC Producer -> quality.pipeline_audit)

Routing rule: a record is a DEFECT if it has any defect code, detected with
`${record:exists('/defect_codes[0]')}`. Everything else is the default (clean) lane.

The three inline enrichment fragments are shared, published Control Hub fragments.
Connections and credentials are runtime parameters - nothing is hardcoded.
The dead_letter_queue_handler fragment stamps dlq_* fields on error records in the DLQ
recovery pipeline that reads from kermit-error-dlq; it is not needed here.
"""

from ..common.connections import apply_connection
from ..common.fragments import resolve_fragment
from ..common.util import get_logger, set_config, set_first

PIPELINE_NAME = "manufacturing__quality_ingest"
LABELS = ["industry:manufacturing", "env:dev"]

INLINE_FRAGMENTS = ["kafka_header_enricher", "data_quality_tagger", "schema_version_tagger"]

log = get_logger()


def build(sch, engine_id, name=None, line_id=None, env="dev"):
    """Build the quality-ingestion pipeline.

    line_id=None builds the all-lines pipeline (default). When a line_id is given (by
    quality_pipeline_manager during per-line generation) the pipeline is named/grouped
    per line and an origin precondition keeps only that line's records, so each
    production line gets its own dedicated, independently-scalable pipeline.
    """
    builder = sch.get_pipeline_builder(engine_type="data_collector", engine_id=engine_id)
    group = "qtp-quality-ingest" + (f"-{line_id}" if line_id else "")

    # --- Origin: read inspection events from Kafka -----------------------------------
    origin = builder.add_stage("Kafka Multitopic Consumer")
    set_config(
        origin,
        topic_list=["quality.inspections"],
        consumer_group=group,
        data_format="JSON",
        broker_uri="${KAFKA_BROKER}",  # ignored once a named connection is applied below
    )
    # Master §9.2: prefer a Control Hub named Connection over the manual broker_uri param
    # (created via `qtp connections create`); falls back to broker_uri if it doesn't exist.
    apply_connection(origin, sch, "manufacturing__kafka__dev")
    # Dedicated per-line pipeline: a Stream Selector right after the origin keeps only this
    # line's records (lane 0) and discards the rest (lane 1 -> Trash). The Kafka origin has
    # no record-precondition config, so a selector is the reliable per-line filter.
    line_filter = line_trash = None
    if line_id:
        line_filter = builder.add_stage("Stream Selector")
        line_filter.predicates = [f"${{record:value('/line_id') == '{line_id}'}}", "default"]
        line_trash = builder.add_stage("Trash")

    # --- Stage A: inline enrichment - pull published shared fragments and chain them -
    enrichers = []
    for frag_name in INLINE_FRAGMENTS:
        fragment = resolve_fragment(sch, frag_name)
        enrichers.append(builder.add_fragment(fragment))

    # --- Stage A: route clean vs defect ----------------------------------------------
    selector = builder.add_stage("Stream Selector")
    # Predicates DEFINE the output lanes; must be set before wiring lanes.
    #   lane 0 → records with a defect code (DEFECT); lane 1 → default (clean/conforming).
    selector.predicates = ["${record:exists('/defect_codes[0]')}", "default"]

    # --- Stage A: defect lane - publish to defects topic (enriched by defect_enrichment)
    defect_out = builder.add_stage("Kafka Producer")
    set_config(defect_out, topic="quality.defects.major", data_format="JSON", broker_uri="${KAFKA_BROKER}")
    apply_connection(defect_out, sch, "manufacturing__kafka__dev")

    # --- Stage A: clean lane - strip non-column fields, then write to Postgres --------
    # The JDBC Producer auto-maps every record field to a same-named column. Drop the
    # fields conforming_parts can't take:
    #   - defect_codes / dq_score / dq_status / dq_checked_at : no such columns
    #   - measurements : a nested JSON object; the JDBC Producer can't coerce a MAP into
    #     a scalar/JSONB column (it would fail like the timestamp did)
    # The remaining fields map 1:1 to the table (part_id, line_id, inspection_timestamp,
    # material_batch_id, operator_id, shift, schema_version) - the team's auto-map pattern.
    field_remover = builder.add_stage("Field Remover")
    set_first(
        field_remover,
        ["/defect_codes", "/dq_score", "/dq_status", "/dq_checked_at", "/measurements"],
        "fields",
    )
    set_config(field_remover, action="REMOVE")

    clean_out = builder.add_stage("JDBC Producer")
    set_first(clean_out, "${PG_JDBC_URL}", "jdbc_connection_string", "connection_string")
    set_config(
        clean_out,
        schema_name="quality",
        table_name="conforming_parts",
        use_credentials=True,
        username="${PG_USER}",
        password="${PG_PASSWORD}",
    )
    apply_connection(clean_out, sch, "manufacturing__postgres__dev")

    # --- Stage B: DLQ error stage → MinIO kermit-error-dlq --------------------------
    # Label confirmed from canvas Error Records dropdown: "Write to Amazon S3".
    # Error record policy set to "AT_CURRENTLY_FAILED_STAGE" so the record carries all
    # enrichment (DQ score, schema version, header attrs) applied before the failure -
    # matching the dlq_* fields the dead_letter_queue_handler fragment would add.
    dlq = builder.add_error_stage(label="Write to Amazon S3")
    set_config(
        dlq,
        authentication_method="WITH_CREDENTIALS",
        access_key_id="${MINIO_ACCESS_KEY}",
        secret_access_key="${MINIO_SECRET_KEY}",
        bucket="${DLQ_BUCKET}",
        common_prefix="quality-ingest/",
        # No data_format: the error-records S3 target writes SDC's fixed error-record
        # format (the full error wrapper), so it has no data_format selector.
        region_definition_for_s3="SPECIFY_NON_REGIONAL_ENDPOINT",
        custom_endpoint_for_s3="${MINIO_ENDPOINT}",
        signing_region_for_s3="US_EAST_1",
        use_path_style_address_model=True,
    )
    # This error-stage variant ("ToErrorAmazonS3DTarget") reports supported_connection_types
    # as None - unlike the regular "Amazon S3" destination/origin, which both support
    # STREAMSETS_AWS_S3 - but the `.connection` attribute exists and accepts a value. Attempt
    # it anyway; `qtp pipelines validate` after rebuild is the real test of whether the
    # engine accepts it. If validation rejects it, this line is the one to revert (falls back
    # to the MANUAL params above, which keep working either way).
    apply_connection(dlq, sch, "manufacturing__minio__dev", fallback_note="error-stage variant may not support connections")

    # --- Stage B: audit stop event (PENDING - lifecycle label needs canvas verification) --
    # add_stop_event_stage() requires pipelineLifecycleStage=True in the engine's stage
    # catalog. "JDBC Producer" passes the label check but has no lifecycle-flagged definition
    # on this engine (StopIteration → blank [ERROR]). To find the accepted label: open any
    # pipeline in the Control Hub canvas → Edit → Lifecycle tab and note which executor
    # stages are offered (e.g. "Email Executor", "HTTP Client", "Pipeline Finisher").
    # Once confirmed, uncomment:
    #
    # audit_frag_obj = sch.pipelines.get(name="audit_log_writer", fragment=True)
    # audit_stop = builder.add_stop_event_stage(label="<confirmed-label>")
    # set_first(audit_stop, "${AUDIT_JDBC_URL}", "jdbc_connection_string", "connection_string")
    # set_config(audit_stop, schema_name="${AUDIT_SCHEMA}", table_name="${AUDIT_TABLE}",
    #            use_credentials=True, username="${PG_USER}", password="${PG_PASSWORD}")
    #
    # AUDIT_JDBC_URL / AUDIT_SCHEMA / AUDIT_TABLE are in pipeline.parameters below, ready.

    # --- Stage A: Wire (optional line filter ->) origin -> enrichers -> selector ------
    if line_filter is not None:
        origin >> line_filter           # per-line: keep this line, discard the rest
        head = line_filter              # lane 0 (matching line) feeds the chain
    else:
        head = origin
    stage = head
    for enricher in enrichers:
        stage = stage >> enricher
    stage >> selector
    selector >> defect_out              # lane 0: defect  -> Kafka Producer
    selector >> field_remover           # lane 1: clean   -> strip non-column fields
    field_remover >> clean_out          #                 -> JDBC Producer (conforming_parts)
    if line_filter is not None:
        line_filter >> line_trash       # lane 1: other lines -> Trash

    full_name = name or (PIPELINE_NAME + (f"__{line_id}" if line_id else ""))
    labels = ["industry:manufacturing", f"env:{env}"] + ([f"line:{line_id}"] if line_id else [])
    pipeline = builder.build(full_name, description=__doc__.split("\n")[0], labels=labels)

    # Capture the record as it looked at the failing stage (post-enrichment), not the
    # raw origin record - so DQ scores, schema version, and header attrs are preserved
    # in the DLQ bucket for lossless recovery. Pipeline-level config is set via the
    # Configuration object, not set_config (which only handles stage attributes).
    try:
        pipeline.configuration["errorRecordPolicy"] = "STAGE_RECORD"
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not set errorRecordPolicy (%s); using engine default.", exc)

    # Runtime parameters: docker-stack defaults (qtp_qtp network hostnames).
    # Fragment params (SOURCE_SYSTEM / ENVIRONMENT / SCHEMA_VERSION) feed the inline
    # enrichment fragments. Audit params feed the stop event writer (pending label fix).
    # NOTE: must be .update(), not `=` - add_fragment() has already promoted each
    # fragment's own prefixed parameters (e.g. kafka_01_KAFKA_SOURCE_SYSTEM,
    # schem_01_SCHEMA_VERSION) onto pipeline.parameters; a plain assignment here wipes
    # them out, leaving every enrichment EL expression unresolved (EXPR_03 on every record).
    pipeline.parameters.update({
        "KAFKA_BROKER":    "kafka:29092",
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
    })
    return pipeline
