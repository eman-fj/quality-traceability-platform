"""Registry of pipeline builders, keyed by short name."""

from . import conforming_archive as _conforming_archive
from . import defect_enrichment as _defect_enrichment
from . import dlq_recovery as _dlq_recovery
from . import pipeline_audit as _pipeline_audit
from . import quality_ingest as _quality_ingest

PIPELINE_BUILDERS = {
    "conforming_archive": _conforming_archive.build,
    "defect_enrichment": _defect_enrichment.build,
    "dlq_recovery": _dlq_recovery.build,
    "pipeline_audit": _pipeline_audit.build,
    "quality_ingest": _quality_ingest.build,
}
