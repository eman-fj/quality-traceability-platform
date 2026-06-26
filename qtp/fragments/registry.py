"""Registry of fragments QTP owns, keyed by their catalogue name."""

from . import data_quality_tagger as _dqt
from . import dead_letter_queue_handler as _dlq
from . import kafka_header_enricher as _khe
from . import s3_partition_formatter as _spf
from . import schema_version_tagger as _svt

FRAGMENT_BUILDERS = {
    _dlq.FRAGMENT_NAME: _dlq.build,
    _spf.FRAGMENT_NAME: _spf.build,
    _svt.FRAGMENT_NAME: _svt.build,
    _khe.FRAGMENT_NAME: _khe.build,
    _dqt.FRAGMENT_NAME: _dqt.build,
}

FRAGMENT_DESCRIPTIONS = {
    _dlq.FRAGMENT_NAME: _dlq.DESCRIPTION,
    _spf.FRAGMENT_NAME: _spf.DESCRIPTION,
    _svt.FRAGMENT_NAME: _svt.DESCRIPTION,
    _khe.FRAGMENT_NAME: _khe.DESCRIPTION,
    _dqt.FRAGMENT_NAME: _dqt.DESCRIPTION,
}
