"""Deterministic fragment resolution for the shared Control Hub workspace.

The workspace is shared, and Control Hub does not enforce unique names — two members
can publish an object with the same fragment name, and (as happened here) a teammate
can publish further versions directly onto *our* fragment's pipeline id. Pinning by bare
pipeline id is NOT safe against that second case: ``sch.pipelines.get(id=...)`` always
resolves to the LATEST commit under that id, so a teammate's later republish (including
a rewrite to a different stage type) silently becomes what every consuming pipeline embeds
next time it's built — with no error, no warning.

config/fragment_pins.yaml pins each fragment by ``pipeline_id`` plus an optional ``tag``:

    pins:
      kafka_header_enricher:
        pipeline_id: <pipelineId>
        tag: <tag-name-on-the-exact-commit-to-embed>   # preferred: immune to later republishes
      data_quality_tagger:
        pipeline_id: <pipelineId>                       # no tag: resolves to latest (only safe
                                                          # while we are the only publisher)

A ``tag`` is a Control Hub tag applied to one specific commit (Pipeline canvas -> version
history -> tag icon). Resolving via tag looks up that tag's commit_id and fetches exactly
that version, so it keeps working even if someone else publishes v7, v8, ... on top.
Without a tag, resolution falls back to the pipeline id (latest version) — fine only until
another author touches that same fragment. A bare pin id string (old format) is still
accepted for backward compatibility.
"""

from pathlib import Path

from .config import load_yaml
from .util import get_logger

log = get_logger()
_PINS_PATH = Path("config/fragment_pins.yaml")


def _load_pins() -> dict:
    if not _PINS_PATH.exists():
        return {}
    return (load_yaml(_PINS_PATH) or {}).get("pins", {}) or {}


def resolve_fragment(sch, name: str):
    """Return the fragment to embed: by pinned tag/commit, pinned id, or plain name lookup."""
    pin = _load_pins().get(name)
    if not pin:
        return sch.pipelines.get(name=name, fragment=True)

    if isinstance(pin, str):  # legacy format: a bare pipeline id string
        log.info("Resolving fragment '%s' by pinned id %s (latest version).", name, pin)
        return sch.pipelines.get(id=pin, fragment=True)

    pipeline_id = pin.get("pipeline_id")
    tag_name = pin.get("tag")
    if not pipeline_id:
        raise RuntimeError(f"Pin for fragment '{name}' has no 'pipeline_id'.")

    if tag_name:
        frag = sch.pipelines.get(id=pipeline_id, fragment=True)
        tag = frag.tags.get(name=tag_name)
        log.info("Resolving fragment '%s' by tag '%s' -> commit %s (exact version, "
                 "immune to later republishes).", name, tag_name, tag.commit_id)
        return sch.pipelines.get(commit_id=tag.commit_id, fragment=True)

    log.info("Resolving fragment '%s' by pinned id %s (latest version — no tag pin; "
             "a republish by another author WILL change what gets embedded).",
             name, pipeline_id)
    return sch.pipelines.get(id=pipeline_id, fragment=True)
