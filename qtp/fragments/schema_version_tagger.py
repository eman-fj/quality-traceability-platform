"""Fragment: schema_version_tagger (enrichment-only, cross-industry).

Stamps every record with the schema version it was processed under, so downstream
consumers - and ISO 9001 auditors - know exactly which schema applied to each part
at inspection time. Pure logic: a single Expression Evaluator, parameter-driven,
no destination (mirrors s3_partition_formatter / the dead_letter_queue_handler).

Inputs  : any record.
Output  : same record with /schema_version set to ${SCHEMA_VERSION}.
Params  : SCHEMA_VERSION - the pipeline's processing schema version (no hardcoded value).
"""

from ..common.util import set_first

FRAGMENT_NAME = "schema_version_tagger"
DESCRIPTION = (
    "Stamps every record with /schema_version = ${SCHEMA_VERSION}, the schema version "
    "it was processed under - critical for ISO 9001 audit traceability. "
    "Param: SCHEMA_VERSION (no hardcoded version)."
)


def build(sch, engine_id, name=None):
    builder = sch.get_pipeline_builder(
        engine_type="data_collector", engine_id=engine_id, fragment=True
    )

    tagger = builder.add_stage("Expression Evaluator")
    set_first(
        tagger,
        [{"fieldToSet": "/schema_version", "expression": "${SCHEMA_VERSION}"}],
        "field_expressions",
        "expression_processor_configs",
    )

    return builder.build(
        name or FRAGMENT_NAME,
        description=DESCRIPTION,
        labels=["industry:manufacturing", "env:dev"],
    )
