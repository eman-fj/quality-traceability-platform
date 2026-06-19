"""Fragment: s3_partition_formatter.

Standardises object-store (MinIO/S3) output paths so every pipeline lands data in a
consistent, queryable layout. Manufacturing extends the catalogue's
/year/month/day/hour/ standard with shift + line_id (agreed at the Fragment Review
Board): ${OUTPUT_BASE_PATH}/year/month/day/shift/line_id/.

Implementation: a single Expression Evaluator that computes the partition path into
a /_partition_path field, which downstream S3/MinIO destinations reference as their
partition prefix.

Inputs  : record carrying /line_id and /shift; runtime parameter OUTPUT_BASE_PATH.
Output  : same record with /_partition_path added.
Params  : OUTPUT_BASE_PATH (no hardcoded paths).
"""

from ..common.util import set_first

FRAGMENT_NAME = "s3_partition_formatter"
DESCRIPTION = (
    "Standardises MinIO/S3 output paths to "
    "${OUTPUT_BASE_PATH}/year/month/day/shift/line_id/. "
    "Input: record with /line_id and /shift + runtime param OUTPUT_BASE_PATH. "
    "Output: adds /_partition_path for downstream destinations."
)

_PARTITION_EXPRESSION = (
    "${OUTPUT_BASE_PATH}"
    "/${time:extractStringFromDate(time:now(),'yyyy')}"
    "/${time:extractStringFromDate(time:now(),'MM')}"
    "/${time:extractStringFromDate(time:now(),'dd')}"
    "/${record:value('/shift')}"
    "/${record:value('/line_id')}"
)


def build(sch, engine_id, name=None):
    builder = sch.get_pipeline_builder(
        engine_type="data_collector", engine_id=engine_id, fragment=True
    )

    expression_evaluator = builder.add_stage("Expression Evaluator")
    set_first(
        expression_evaluator,
        [{"fieldToSet": "/_partition_path", "expression": _PARTITION_EXPRESSION}],
        "field_expressions",
        "expression_processor_configs",
    )

    return builder.build(
        name or FRAGMENT_NAME,
        description=DESCRIPTION,
        labels=["industry:manufacturing", "env:dev"],
    )
