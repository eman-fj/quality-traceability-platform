"""Fragment: kafka_header_enricher (enrichment-only, cross-industry).

Adds standard metadata as record header attributes on every message: source system,
environment, pipeline name, schema version, and ingest timestamp. Lets downstream
consumers and ops filter/route by metadata without parsing the body. Pure logic - a
single Expression Evaluator, parameter-driven, no destination.

Inputs  : any record.
Output  : same record with standard header attributes set.
Params  : SOURCE_SYSTEM, ENVIRONMENT, SCHEMA_VERSION (no hardcoded values).

NOTE: sets header attributes (not fields). The SDK attribute name for the Expression
Evaluator's header-attribute list varies by version, so it's set via set_first with
known candidates - confirm on first publish if it flags an unknown attribute.
"""

from ..common.util import set_first

FRAGMENT_NAME = "kafka_header_enricher"
DESCRIPTION = (
    "Adds standard metadata headers to every message (source_system, environment, "
    "pipeline_name, schema_version, ingest_timestamp). "
    "Params: SOURCE_SYSTEM, ENVIRONMENT, SCHEMA_VERSION - no hardcoded values."
)


def build(sch, engine_id, name=None):
    builder = sch.get_pipeline_builder(
        engine_type="data_collector", engine_id=engine_id, fragment=True
    )

    enricher = builder.add_stage("Expression Evaluator")
    set_first(
        enricher,
        [
            {"attributeToSet": "source_system", "headerAttributeExpression": "${SOURCE_SYSTEM}"},
            {"attributeToSet": "environment", "headerAttributeExpression": "${ENVIRONMENT}"},
            {"attributeToSet": "pipeline_name", "headerAttributeExpression": "${pipeline:name()}"},
            {"attributeToSet": "schema_version", "headerAttributeExpression": "${SCHEMA_VERSION}"},
            {"attributeToSet": "ingest_timestamp", "headerAttributeExpression": "${time:now()}"},
        ],
        "header_attribute_expressions",
        "header_attribute_configs",
    )

    return builder.build(
        name or FRAGMENT_NAME,
        description=DESCRIPTION,
        labels=["industry:manufacturing", "env:dev"],
    )
