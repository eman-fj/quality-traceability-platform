"""Deterministic fragment resolution for the shared Control Hub workspace.

The workspace is shared, and Control Hub does not enforce unique names — two members
can publish an object with the same fragment name. ``sch.pipelines.get(name=...)`` is
then ambiguous. To guarantee a pipeline embeds *our* fragment (not a teammate's
same-named one), pin the fragment's stable Control Hub id in config/fragment_pins.yaml:

    pins:
      kafka_header_enricher: <pipelineId-of-your-fragment>
      data_quality_tagger:   <pipelineId-of-your-fragment>

Find the ids with ``qtp fragments list`` (the ID column). Names without a pin fall back
to a plain name lookup, so this is only needed for the fragments that actually collide.
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
    """Return the fragment to embed: the pinned id if configured, else by name.

    Pinning by id (stable across versions) makes per-line/canonical pipelines embed our
    own fragment regardless of same-named copies a teammate may have published.
    """
    pin = _load_pins().get(name)
    if pin:
        log.info("Resolving fragment '%s' by pinned id %s.", name, pin)
        return sch.pipelines.get(id=pin, fragment=True)
    return sch.pipelines.get(name=name, fragment=True)
