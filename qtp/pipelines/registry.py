"""Registry of pipeline builders, keyed by short name."""

from . import defect_enrichment as _defect_enrichment
from . import quality_ingest as _quality_ingest

PIPELINE_BUILDERS = {
    "defect_enrichment": _defect_enrichment.build,
    "quality_ingest": _quality_ingest.build,
}
