"""Registry of fragments QTP owns, keyed by their catalogue name."""

from . import dead_letter_queue_handler as _dlq
from . import s3_partition_formatter as _spf

FRAGMENT_BUILDERS = {
    _dlq.FRAGMENT_NAME: _dlq.build,
    _spf.FRAGMENT_NAME: _spf.build,
}

FRAGMENT_DESCRIPTIONS = {
    _dlq.FRAGMENT_NAME: _dlq.DESCRIPTION,
    _spf.FRAGMENT_NAME: _spf.DESCRIPTION,
}
