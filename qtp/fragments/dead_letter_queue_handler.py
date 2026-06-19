"""Fragment: dead_letter_queue_handler (enrichment-only, cross-industry).

Failed/error records must never be silently dropped. This fragment stamps
source-agnostic error metadata onto an error record so it can be recovered with
full context. It is pure logic - it does NOT contain a destination.

The consuming pipeline owns the actual sink (e.g. an Amazon S3 destination pointed
at MinIO via a named Control Hub connection). Keeping the destination out of the
fragment helps making it reusable without modification, and
keeps all environment-specifics (endpoint, credentials, bucket) in the pipeline
where they belong - mirroring the team's proven uc7_pii_masking_standard pattern.

Inputs  : error record (carries StreamSets error.* header attributes).
Output  : same record with dlq_* metadata fields added, ready for any DLQ sink.
Params  : none - the fragment is environment-agnostic.
"""

from ..common.util import set_first

FRAGMENT_NAME = "dead_letter_queue_handler"
DESCRIPTION = (
    "Enrichment-only DLQ: stamps source-agnostic error metadata (source pipeline, "
    "failing stage, error message/code, timestamp) onto a failed record so it can be "
    "routed to any error sink. No destination/credentials in the fragment - the "
    "consuming pipeline owns the DLQ sink and its named connection."
)


def build(sch, engine_id, name=None):
    builder = sch.get_pipeline_builder(
        engine_type="data_collector", engine_id=engine_id, fragment=True
    )

    enrich = builder.add_stage("Expression Evaluator")
    set_first(
        enrich,
        [
            {"fieldToSet": "/dlq_source_pipeline", "expression": "${pipeline:name()}"},
            {"fieldToSet": "/dlq_error_stage", "expression": "${record:attribute('errorStage')}"},
            {"fieldToSet": "/dlq_error_message", "expression": "${record:attribute('errorMessage')}"},
            {"fieldToSet": "/dlq_error_code", "expression": "${record:attribute('errorCode')}"},
            {"fieldToSet": "/dlq_error_timestamp", "expression": "${time:now()}"},
        ],
        "field_expressions",
        "expression_processor_configs",
    )

    return builder.build(
        name or FRAGMENT_NAME,
        description=DESCRIPTION,
        labels=["industry:manufacturing", "env:dev"],
    )
