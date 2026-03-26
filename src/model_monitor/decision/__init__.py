"""Decision logic — evaluate model health from computed metrics."""

from enum import Enum


class ModelHealth(str, Enum):
    VALID = "VALID"
    NEEDS_CALIBRATION = "NEEDS_CALIBRATION"
    INVALID = "INVALID"
