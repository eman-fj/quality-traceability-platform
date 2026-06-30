"""Pipeline: manufacturing__conforming_archive.

Long-term archive of conforming (passing) parts to MinIO object storage, in the
standardised partitioned layout, using the shared **s3_partition_formatter** fragment:

    Kafka (quality.inspections)
        -> Stream Selector        (defect -> Trash; clean -> archive)
        -> s3_partition_formatter (sets /_partition_path = base/yyyy/MM/dd/shift/line_id)
        -> Amazon S3 (MinIO)      (Parquet, partitioned by /_partition_path)

This is the clean-path archive the brief calls for ("conforming parts ... a Parquet copy
to MinIO for long-term retention"), separate from the Postgres warehouse write in
quality_ingest. It gives the s3_partition_formatter fragment its consuming pipeline.

Connections/credentials are runtime parameters; the MinIO endpoint is the TLS endpoint
(SDC's AWS SDK requires HTTPS — see certs/). Parquet uses an inferred Avro schema; if the
engine needs an explicit schema, set it in the canvas.
"""

from ..common.fragments import resolve_fragment
from ..common.util import get_logger, set_config, set_first

PIPELINE_NAME = "manufacturing__conforming_archive"
LABELS = ["industry:manufacturing", "env:dev"]

log = get_logger()


def build(sch, engine_id, name=None):
    builder = sch.get_pipeline_builder(engine_type="data_collector", engine_id=engine_id)

    origin = builder.add_stage("Kafka Multitopic Consumer")
    set_config(
        origin,
        topic_list=["quality.inspections"],
        consumer_group="qtp-conforming-archive",
        data_format="JSON",
        broker_uri="${KAFKA_BROKER}",
    )

    # Keep only conforming parts (no defect code); route defects to Trash.
    selector = builder.add_stage("Stream Selector")
    selector.predicates = ["${record:exists('/defect_codes[0]')}", "default"]
    defect_trash = builder.add_stage("Trash")

    # Standardised partition path (base/yyyy/MM/dd/shift/line_id) on /_partition_path.
    formatter = builder.add_fragment(resolve_fragment(sch, "s3_partition_formatter"))

    # Parquet archive to MinIO, partitioned by the formatter's /_partition_path.
    archive = builder.add_stage("Amazon S3")
    set_config(
        archive,
        authentication_method="WITH_CREDENTIALS",
        access_key_id="${MINIO_ACCESS_KEY}",
        secret_access_key="${MINIO_SECRET_KEY}",
        bucket="${CONFORMING_BUCKET}",
        partition_prefix="${record:value('/_partition_path')}",
        data_format="PARQUET",
        region_definition_for_s3="SPECIFY_NON_REGIONAL_ENDPOINT",
        custom_endpoint_for_s3="${MINIO_ENDPOINT}",
        signing_region_for_s3="US_EAST_1",
        use_path_style_address_model=True,
    )

    # Drop fields Avro/Parquet schema inference can't handle: defect_codes is an empty
    # list on clean parts (no element type to infer), and measurements is a nested map.
    # Neither belongs in the conforming archive.
    field_remover = builder.add_stage("Field Remover")
    set_first(field_remover, ["/defect_codes", "/measurements"], "fields")
    set_config(field_remover, action="REMOVE")

    origin >> selector
    selector >> defect_trash            # lane 0: defect -> Trash
    selector >> formatter               # lane 1: clean  -> partition -> remove -> archive
    formatter >> field_remover >> archive

    pipeline = builder.build(name or PIPELINE_NAME, description=__doc__.split("\n")[0], labels=LABELS)
    pipeline.parameters = {
        "KAFKA_BROKER":    "kafka:29092",
        "MINIO_ENDPOINT":  "https://minio:9000",
        "MINIO_ACCESS_KEY": "minioadmin",
        "MINIO_SECRET_KEY": "minioadmin",
        "CONFORMING_BUCKET": "kermit-quality-conforming",
        "OUTPUT_BASE_PATH": "conforming",   # s3_partition_formatter base prefix (unprefixed param)
    }
    return pipeline
