"""Pipeline: manufacturing__dlq_recovery.

Recovery flow for the dead-letter queue. The quality_ingest / defect_enrichment pipelines
park failed records in MinIO (kermit-error-dlq) via their Write-to-S3 error stage, in SDC
error-record format (which preserves the error.* header attributes). This pipeline reads
them back, enriches each with the shared **dead_letter_queue_handler** fragment (stamping
dlq_source_pipeline / dlq_error_stage / dlq_error_message / dlq_error_code /
dlq_error_timestamp from those attributes), and writes the recovery metadata to
quality.dlq_recovery so failures are auditable and recoverable - never silently dropped.

    Amazon S3 (MinIO kermit-error-dlq, SDC-record format)
        -> dead_letter_queue_handler   (stamp dlq_* recovery metadata)
        -> Field Remover               (keep only the dlq_* fields)
        -> JDBC Producer               (Postgres quality.dlq_recovery)

The DLQ objects are written in SDC record format (framed, not plain JSON), so the origin
reads them as SDC_JSON - which reconstructs only the original record value and DISCARDS the
per-stage error metadata (errorStage/errorCode/errorMessage are gone on re-read; verified
via preview). So instead of the (unrecoverable) error detail, recovery captures the failed
record's identity: dlq_source_pipeline + dlq_error_timestamp from the fragment, plus the
re-read record's part_id/line_id - enough to locate and reprocess the failed part.

NOTE: S3 origin prefix/credentials attribute names vary by engine; confirm in the canvas.
"""

from ..common.connections import apply_connection
from ..common.fragments import resolve_fragment
from ..common.util import get_logger, set_config, set_first

PIPELINE_NAME = "manufacturing__dlq_recovery"
LABELS = ["industry:manufacturing", "env:dev"]

# SDC discards per-stage error metadata (errorStage/errorCode/errorMessage) when error
# records are re-read from the archive, so we recover the failed record's identity instead:
# which part / line failed, from which pipeline (dlq fragment), and when.
_DLQ_FIELDS = ["/dlq_source_pipeline", "/dlq_error_timestamp", "/part_id", "/line_id"]

log = get_logger()


def build(sch, engine_id, name=None):
    builder = sch.get_pipeline_builder(engine_type="data_collector", engine_id=engine_id)

    # Read the parked error records back from the MinIO DLQ bucket. SDC-record format
    # restores the error.* header attributes the dlq fragment reads.
    origin = builder.add_stage("Amazon S3", type="origin")
    set_config(
        origin,
        authentication_method="WITH_CREDENTIALS",
        access_key_id="${MINIO_ACCESS_KEY}",
        secret_access_key="${MINIO_SECRET_KEY}",
        bucket="${DLQ_BUCKET}",
        common_prefix="",
        prefix_pattern="**/*",
        data_format="SDC_JSON",
        region_definition_for_s3="SPECIFY_NON_REGIONAL_ENDPOINT",
        custom_endpoint_for_s3="${MINIO_ENDPOINT}",
        signing_region_for_s3="US_EAST_1",
        use_path_style_address_model=True,
    )
    apply_connection(origin, sch, "manufacturing__minio__dev")

    dlq = builder.add_fragment(resolve_fragment(sch, "dead_letter_queue_handler"))

    # Keep only the dlq_* recovery fields so the JDBC insert maps 1:1 to dlq_recovery.
    field_remover = builder.add_stage("Field Remover")
    set_first(field_remover, _DLQ_FIELDS, "fields")
    set_config(field_remover, action="KEEP")

    recovery_out = builder.add_stage("JDBC Producer")
    set_first(recovery_out, "${PG_JDBC_URL}", "jdbc_connection_string", "connection_string")
    set_config(
        recovery_out,
        schema_name="quality",
        table_name="dlq_recovery",
        use_credentials=True,
        username="${PG_USER}",
        password="${PG_PASSWORD}",
    )
    apply_connection(recovery_out, sch, "manufacturing__postgres__dev")

    origin >> dlq >> field_remover >> recovery_out

    pipeline = builder.build(name or PIPELINE_NAME, description=__doc__.split("\n")[0], labels=LABELS)
    pipeline.parameters = {
        "MINIO_ENDPOINT":  "https://minio:9000",
        "MINIO_ACCESS_KEY": "minioadmin",
        "MINIO_SECRET_KEY": "minioadmin",
        "DLQ_BUCKET":      "kermit-error-dlq",
        "PG_JDBC_URL":     "jdbc:postgresql://postgres:5432/quality",
        "PG_USER":         "qtp",
        "PG_PASSWORD":     "qtp_pw",
    }
    return pipeline
