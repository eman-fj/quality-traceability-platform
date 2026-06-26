"""Quality & Traceability Platform (QTP) — StreamSets SDK automation package.

A structured CLI (`qtp <group> <action>`) that builds, governs, and monitors the
manufacturing quality platform via the StreamSets Platform SDK, rather than the UI.
"""

__version__ = "0.1.0"

# Apply SDK compatibility shims (e.g. the Configuration empty-list IndexError guard)
# as early as possible, before any pipeline/fragment build touches the SDK.
from .common import sdk_compat as _sdk_compat  # noqa: E402,F401
