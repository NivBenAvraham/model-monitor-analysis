"""Tests for model_monitor.decision."""

from model_monitor.decision import ModelHealth


def test_model_health_values() -> None:
    assert ModelHealth.VALID == "VALID"
    assert ModelHealth.NEEDS_CALIBRATION == "NEEDS_CALIBRATION"
    assert ModelHealth.INVALID == "INVALID"
