"""Pipeline: manufacturing__pipeline_audit.

Per-run compliance audit, built around the team's shared **audit_log_writer** fragment
(Expression Evaluator -> JDBC Producer). The fragment can only run as a pipeline
stop-event stage, which this engine doesn't support for JDBC, so it is driven instead by a
dedicated one-shot pipeline:

    Dev Raw Data Source (emit one record, stop)  ->  audit_log_writer

The fragment stamps pipeline_name / run_ts / operator / environment / audit_metrics and
writes one row to quality.pipeline_audit. Run it once per pipeline run (as a job), passing
the run's metrics as the AUDIT_METRICS_JSON parameter — this satisfies the brief's
"audit_log_writer mandatory on all manufacturing pipelines" without duplicating the
fragment's logic.

NOTE: the fragment's parameters are promoted under a prefix when embedded; the defaults
below set both the prefixed and unprefixed names so they resolve regardless. Confirm the
audit JDBC/credentials values in the canvas Parameters tab on first publish.
"""

from ..common.fragments import resolve_fragment
from ..common.util import get_logger, set_config

PIPELINE_NAME = "manufacturing__pipeline_audit"
LABELS = ["industry:manufacturing", "env:dev"]

# Local-dev audit warehouse. Credentials are embedded in the JDBC URL so the fragment's
# JDBC Producer needs no separate cred params.
# NOTE (checked): the audit_log_writer fragment's inner JDBC Producer cannot be pointed at a
# Control Hub named connection from here — fragments are opaque to the outer pipeline builder
# (the wrapper object the SDK returns from add_fragment() has no `.connection` attribute;
# verified: hasattr(audit, "connection") is False). Only a stage added directly with
# add_stage()/add_error_stage() exposes `.connection`. Wiring this would need the fragment
# itself republished with its JDBC Producer's connection field declared as promotable — out
# of scope here since it's a teammate's shared fragment, not ours to edit.
_AUDIT_JDBC_URL = "jdbc:postgresql://postgres:5432/quality?user=qtp&password=qtp_pw"
_AUDIT_DEFAULTS = {
    "AUDIT_JDBC_URL": _AUDIT_JDBC_URL,
    "AUDIT_SCHEMA": "quality",
    "AUDIT_TABLE": "pipeline_audit",
    "AUDIT_OPERATOR": "qtp-sdk",
    "AUDIT_ENVIRONMENT": "dev",
    "AUDIT_METRICS_JSON": "{}",
}

log = get_logger()


def build(sch, engine_id, name=None):
    builder = sch.get_pipeline_builder(engine_type="data_collector", engine_id=engine_id)

    # One-shot trigger: emit a single record, then stop the pipeline.
    origin = builder.add_stage("Dev Raw Data Source")
    set_config(origin, raw_data="{}", data_format="JSON", stop_after_first_batch=True)

    audit_obj = resolve_fragment(sch, "audit_log_writer")
    audit = builder.add_fragment(audit_obj)

    origin >> audit

    pipeline = builder.build(name or PIPELINE_NAME, description=__doc__.split("\n")[0], labels=LABELS)

    # Set the fragment's params under both the prefixed (promoted) and unprefixed names so
    # they resolve whether or not the fragment declared them as parameters. .update() keeps
    # the fragment's other promoted params intact.
    params = {f"audit_01_{k}": v for k, v in _AUDIT_DEFAULTS.items()}
    params.update(_AUDIT_DEFAULTS)
    pipeline.parameters.update(params)
    return pipeline
