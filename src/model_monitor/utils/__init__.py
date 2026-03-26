"""Shared utilities — config loading, logging setup."""

import yaml
from pathlib import Path


def load_thresholds(path: str = "configs/thresholds.yaml") -> dict:
    """Load threshold values from the config file."""
    with open(Path(path)) as f:
        return yaml.safe_load(f) or {}
