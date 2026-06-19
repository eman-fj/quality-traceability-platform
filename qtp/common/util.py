"""Small cross-cutting utilities: logging and resilient stage configuration."""

import logging
import sys


def get_logger(name: str = "qtp") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


def _stage_label(stage):
    return getattr(stage, "stage_name", None) or getattr(stage, "label", None) or stage


def set_first(stage, value, *candidates: str) -> str:
    """Set the first config attribute the stage actually has; raise if none match.

    SDK/stage-library attribute names vary by version (e.g. the Expression
    Evaluator's field list is `field_expressions`, not `expressionProcessorConfigs`),
    so each required config lists its known names. Mirrors ftrlib's _set_config.
    """
    for attr in candidates:
        if hasattr(stage, attr):
            setattr(stage, attr, value)
            return attr
    raise RuntimeError(
        f"None of {candidates} found on stage '{_stage_label(stage)}'. "
        f"Available: {sorted(a for a in dir(stage) if not a.startswith('_'))}"
    )


def set_config(stage, **kwargs) -> None:
    """Best-effort set of optional stage config attributes.

    Each attribute is set only if the stage has it; anything this engine's stage
    doesn't expose is logged as a warning to confirm in the canvas, rather than
    crashing the build. Use this for non-critical config; use set_first for config
    that must succeed.
    """
    log = get_logger()
    label = _stage_label(stage)
    for key, value in kwargs.items():
        if hasattr(stage, key):
            setattr(stage, key, value)
        else:
            log.warning("Stage '%s' has no attribute '%s'; skipped - confirm in canvas.", label, key)
