"""Control Hub session factory.

Reads credentials from the environment (.env) - never hardcoded - matching the
convention used across the team's SDK scripts:
    SCH_URL, SCH_CRED_ID, SCH_TOKEN, and optionally ENGINE_ID (authoring SDC).
"""

import os

from dotenv import load_dotenv
from streamsets.sdk import ControlHub

from .util import get_logger

_REQUIRED = ("SCH_URL", "SCH_CRED_ID", "SCH_TOKEN")


def get_control_hub() -> ControlHub:
    """Build an authenticated Control Hub session from environment variables."""
    load_dotenv()
    missing = [name for name in _REQUIRED if not os.getenv(name)]
    if missing:
        raise RuntimeError(
            "Missing Control Hub credentials: " + ", ".join(missing) + ". "
            "Set them in your .env (see .env.example / your SDK/.env)."
        )
    return ControlHub(
        sch_url=os.getenv("SCH_URL"),
        credential_id=os.getenv("SCH_CRED_ID"),
        token=os.getenv("SCH_TOKEN"),
    )


def get_authoring_engine(sch: ControlHub, engine_id: str | None = None):
    """Resolve the authoring Data Collector to build pipelines/fragments against.

    Uses the given id, else ENGINE_ID from the environment, else the first
    available Data Collector registered with Control Hub.
    """
    log = get_logger()
    engine_id = engine_id or os.getenv("ENGINE_ID") or os.getenv("SDC_ID")
    if engine_id:
        return sch.data_collectors.get(id=engine_id)
    engines = list(sch.data_collectors)
    if not engines:
        raise RuntimeError("No Data Collector registered with Control Hub; set ENGINE_ID in .env.")
    log.info("ENGINE_ID not set; using first available Data Collector: %s", engines[0].id)
    return engines[0]
