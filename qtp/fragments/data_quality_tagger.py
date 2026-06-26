"""Fragment: data_quality_tagger (enrichment-only, cross-industry).

Tags each record with a data-quality score and status so downstream consumers and
dashboards can trust/triage records. Pure logic - a single Expression Evaluator,
no destination.

Baseline tagging (set here): /dq_score, /dq_status, /dq_checked_at. The baseline is
GREEN/100; pipeline-specific rules (required-field nulls, type mismatches,
out-of-range values, duplicate keys) are layered on per pipeline by passing extra
field expressions, keeping the shared fragment generic.

Inputs  : any record.
Output  : same record with /dq_score, /dq_status, /dq_checked_at added.
Params  : none (baseline score is 100; per-pipeline rules adjust it downstream).
"""

from ..common.util import set_first

FRAGMENT_NAME = "data_quality_tagger"
DESCRIPTION = (
    "Tags each record with a data-quality score/status (/dq_score, /dq_status, "
    "/dq_checked_at). Baseline GREEN/100; per-pipeline rules extend it. Generic, "
    "no hardcoded values."
)


def build(sch, engine_id, name=None):
    builder = sch.get_pipeline_builder(
        engine_type="data_collector", engine_id=engine_id, fragment=True
    )

    tagger = builder.add_stage("Expression Evaluator")
    set_first(
        tagger,
        [
            {"fieldToSet": "/dq_score", "expression": "100"},
            {"fieldToSet": "/dq_status", "expression": "GREEN"},
            {"fieldToSet": "/dq_checked_at", "expression": "${time:now()}"},
        ],
        "field_expressions",
        "expression_processor_configs",
    )

    return builder.build(
        name or FRAGMENT_NAME,
        description=DESCRIPTION,
        labels=["industry:manufacturing", "env:dev"],
    )
