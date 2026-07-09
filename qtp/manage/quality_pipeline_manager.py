"""quality_pipeline_manager.py - SDK-driven pipeline fleet management.

Generates, onboards, and promotes the per-line quality pipelines from configuration
instead of building each one by hand in the UI - the core "use the SDK to automate and
govern, not just build" capability:

  * generate : reads config/lines.yaml and, for every production line, builds and
    publishes a dedicated ingestion + defect-enrichment pipeline (shared fragments
    applied during construction), named ``manufacturing__<flow>__<line_id>`` and
    labelled ``line:<line_id>``.
  * add-line : appends a new line to config/lines.yaml and generates its pair of
    pipelines - onboarding a production line with a single command.
  * promote  : republishes a pipeline for a target environment using the env_map
    pattern (``env:prod`` label + the prod named-connection bindings), governing the
    dev -> prod path.

All Control Hub access goes through the shared session factory; nothing is hardcoded.
"""

from pathlib import Path

import yaml

from ..common.config import load_env_map, load_lines
from ..common.controlhub import get_authoring_engine, get_control_hub
from ..common.util import get_logger
from ..pipelines import defect_enrichment, quality_ingest

log = get_logger()

# The flows generated for every line, keyed by the flow name used in pipeline names.
FLOW_BUILDERS = {
    "quality_ingest": quality_ingest.build,
    "defect_enrichment": defect_enrichment.build,
}
_LINES_PATH = Path("config/lines.yaml")


def _publish(sch, pipeline, draft: bool, replace: bool) -> None:
    """Publish a freshly-built pipeline, optionally replacing an existing same-named one."""
    name = pipeline.name
    existing = list(sch.pipelines.get_all(name=name))
    if existing and replace:
        for old in existing:
            try:
                sch.delete_pipeline(old)
            except Exception as exc:  # noqa: BLE001
                log.warning("Could not delete existing '%s' (%s).", name, exc)
        existing = []
    if existing:
        log.info("Pipeline '%s' already exists - skipping (use --replace to recreate).", name)
        return
    sch.publish_pipeline(pipeline, commit_message=f"qtp: generate {name}", draft=draft)
    log.info("Published '%s'%s", name, " (draft)" if draft else "")


def _build_line(sch, engine_id, flow: str, line_id: str, env: str = "dev"):
    return FLOW_BUILDERS[flow](sch, engine_id, line_id=line_id, env=env)


# --------------------------------------------------------------------------- generate
def generate(engine_id=None, draft=True, replace=False) -> int:
    """Generate the ingestion + defect pipelines for every line in lines.yaml."""
    sch = get_control_hub()
    engine = get_authoring_engine(sch, engine_id)
    cfg = load_lines()
    lines = cfg.get("lines", [])
    log.info("Generating per-line pipelines for %d line(s)...", len(lines))
    for line in lines:
        line_id = line["id"]
        for flow in FLOW_BUILDERS:
            pipeline = _build_line(sch, engine.id, flow, line_id)
            _publish(sch, pipeline, draft=draft, replace=replace)
    log.info("Done: %d line(s) x %d flow(s).", len(lines), len(FLOW_BUILDERS))
    return 0


# ---------------------------------------------------------------------------- add-line
def add_line(line_id: str, partitions: str, engine_id=None, draft=True) -> int:
    """Append a line to lines.yaml and generate its pipelines (onboarding)."""
    cfg = load_lines()
    if any(l["id"] == line_id for l in cfg.get("lines", [])):
        log.error("Line '%s' already exists in %s.", line_id, _LINES_PATH)
        return 1
    cfg.setdefault("lines", []).append({"id": line_id, "partitions": str(partitions)})
    _LINES_PATH.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")
    log.info("Onboarded line '%s' (partitions %s) into %s.", line_id, partitions, _LINES_PATH)

    sch = get_control_hub()
    engine = get_authoring_engine(sch, engine_id)
    for flow in FLOW_BUILDERS:
        pipeline = _build_line(sch, engine.id, flow, line_id)
        _publish(sch, pipeline, draft=draft, replace=False)
    return 0


# ----------------------------------------------------------------------------- promote
def promote(name: str, to_env="prod", engine_id=None, draft=True) -> int:
    """Promote a pipeline to a target environment using the env_map pattern.

    Rebuilds the pipeline from its flow builder, tags it ``env:<to_env>``, names it
    ``<name>__<to_env>``, and binds the target environment's named connections (logged).
    Connection credentials live in Control Hub as runtime params, never in code.
    """
    env_map = load_env_map().get("environments", {})
    if to_env not in env_map:
        log.error("Unknown environment '%s'. Known: %s", to_env, ", ".join(env_map))
        return 1

    parts = name.split("__")  # manufacturing__<flow>[__<line_id>]
    flow = parts[1] if len(parts) > 1 else None
    line_id = parts[2] if len(parts) > 2 else None
    if flow not in FLOW_BUILDERS:
        log.error("Cannot promote '%s': unknown flow '%s'.", name, flow)
        return 1

    sch = get_control_hub()
    engine = get_authoring_engine(sch, engine_id)
    prod_name = f"{name}__{to_env}"
    pipeline = FLOW_BUILDERS[flow](sch, engine.id, name=prod_name, line_id=line_id, env=to_env)

    target = env_map[to_env]
    log.info("Promoting '%s' -> '%s' (%s). Binding connections: %s",
             name, prod_name, target.get("label"),
             ", ".join(f"{k}={v}" for k, v in target.get("connections", {}).items()))
    _publish(sch, pipeline, draft=draft, replace=True)
    return 0
