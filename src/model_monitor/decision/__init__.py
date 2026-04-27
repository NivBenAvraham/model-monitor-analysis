"""Decision logic — evaluate model health from computed metrics."""

from enum import Enum

from model_monitor.decision.temperature_health_rule import score_group_date


class ModelHealth(str, Enum):
    VALID = "VALID"
    NEEDS_CALIBRATION = "NEEDS_CALIBRATION"
    INVALID = "INVALID"


__all__ = ["ModelHealth", "score_group_date"]
