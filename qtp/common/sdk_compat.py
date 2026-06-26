"""Compatibility shims for known bugs in the installed streamsets SDK (7.x).

Imported for its side effects (it patches the SDK at import time). Keep each patch
small, guarded, and clearly tied to the upstream defect it works around.
"""

from streamsets.sdk import models as _sdk_models

from .util import get_logger

log = get_logger()


def _patch_configuration_empty_list():
    """Guard `Configuration.__init__` against an empty configuration list.

    Upstream bug (streamsets/sdk/models.py, line ~104):

        self._data = [configuration] if isinstance(configuration[0], dict) else configuration

    indexes `configuration[0]` with no check for an empty list. Any stage/service
    whose configuration list comes back empty therefore raises
    `IndexError: list index out of range` during `add_error_stage`/`add_stage`.

    This bites the "Write to Amazon S3" error stage: one of its services has an
    empty `configDefinitions`, so the SDK builds `Configuration([])` (a throwaway
    wrapper used only to merge runtime service config) and crashes.

    Fix: when the configuration list is empty, substitute `[[]]`. The SDK treats it
    as one empty inner config list → an empty index map → identical downstream
    behaviour, but the `configuration[0]` access no longer throws. The wrapper is
    discarded immediately after, so the substitution has no observable effect on the
    produced pipeline definition.
    """
    orig_init = _sdk_models.Configuration.__init__

    def patched_init(self, configuration=None, *args, **kwargs):
        if configuration == []:
            configuration = [[]]
        orig_init(self, configuration, *args, **kwargs)

    # Idempotent: don't double-wrap if this module is imported more than once.
    if getattr(_sdk_models.Configuration.__init__, "_qtp_patched", False):
        return
    patched_init._qtp_patched = True
    _sdk_models.Configuration.__init__ = patched_init
    log.debug("Applied SDK compat patch: Configuration empty-list guard.")


def apply_all():
    _patch_configuration_empty_list()


apply_all()
