"""Registry of fragments QTP owns, keyed by their catalogue name.

`data_quality_tagger` is intentionally NOT here: we reuse teammate rukhmah.aamir's published
shared fragment instead of our own (pinned by id in config/fragment_pins.yaml). Its builder
module is kept in this package for reference/history but is no longer built or published by us.
"""

from . import dead_letter_queue_handler as _dlq
from . import kafka_header_enricher as _khe
from . import s3_partition_formatter as _spf
from . import schema_version_tagger as _svt

FRAGMENT_BUILDERS = {
    _dlq.FRAGMENT_NAME: _dlq.build,
    _spf.FRAGMENT_NAME: _spf.build,
    _svt.FRAGMENT_NAME: _svt.build,
    _khe.FRAGMENT_NAME: _khe.build,
}

FRAGMENT_DESCRIPTIONS = {
    _dlq.FRAGMENT_NAME: _dlq.DESCRIPTION,
    _spf.FRAGMENT_NAME: _spf.DESCRIPTION,
    _svt.FRAGMENT_NAME: _svt.DESCRIPTION,
    _khe.FRAGMENT_NAME: _khe.DESCRIPTION,
}
