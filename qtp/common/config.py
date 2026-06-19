"""YAML config loading for lines and environment promotion maps."""

from pathlib import Path

import yaml


def load_yaml(path: str | Path) -> dict:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def load_lines(path: str | Path = "config/lines.yaml") -> dict:
    return load_yaml(path)


def load_env_map(path: str | Path = "config/env_map.yaml") -> dict:
    return load_yaml(path)
