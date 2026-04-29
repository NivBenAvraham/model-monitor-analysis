"""Tests for model_monitor.decision.temperature_health_rule."""

from model_monitor.decision import ModelHealth, score_group_date

_ATV = "ambient_temperature_volatility"
_R3  = "bucket_reference_adherence"
_R5  = "bucket_temporal_stability"
_R7  = "bucket_diurnal_amplitude"
_GATES = (_ATV, _R3, _R5, _R7)
_SCORE = [
    "ambient_stability",
    "ambient_range",
    "sensor_spread_within_bucket",
    "small_hive_ambient_tracking",
    "large_hive_thermoregulation",
    "bucket_temperature_ordering",
]   # 6 non-gate metrics


def _metrics(*, atv: bool = True, r3: bool = True, r5: bool = True, r7: bool = True,
             score_pass: int = 6) -> list[dict]:
    """Build a full 10-metric result list for testing."""
    results = [
        {"metric_name": _ATV, "pass_metric": atv},
        {"metric_name": _R3,  "pass_metric": r3},
        {"metric_name": _R5,  "pass_metric": r5},
        {"metric_name": _R7,  "pass_metric": r7},
    ]
    results += [
        {"metric_name": name, "pass_metric": i < score_pass}
        for i, name in enumerate(_SCORE)
    ]
    return results


def test_model_health_values() -> None:
    assert ModelHealth.VALID             == "VALID"
    assert ModelHealth.NEEDS_CALIBRATION == "NEEDS_CALIBRATION"
    assert ModelHealth.INVALID           == "INVALID"


def test_atv_gate_fail_returns_invalid() -> None:
    """ATV failing must produce INVALID regardless of other metrics."""
    result = score_group_date(_metrics(atv=False))
    assert result["prediction"]    == "INVALID"
    assert result["confidence"]    == 1
    assert _ATV in result["failed_gates"]
    assert result["gate_results"][_ATV] is False


def test_r3_gate_fail_returns_invalid() -> None:
    """R3 failing must produce INVALID regardless of other metrics."""
    result = score_group_date(_metrics(r3=False))
    assert result["prediction"]    == "INVALID"
    assert result["confidence"]    == 1
    assert _R3 in result["failed_gates"]


def test_r5_gate_fail_returns_invalid() -> None:
    """R5 failing must produce INVALID regardless of other metrics."""
    result = score_group_date(_metrics(r5=False))
    assert result["prediction"]    == "INVALID"
    assert result["confidence"]    == 1
    assert _R5 in result["failed_gates"]


def test_r7_gate_fail_returns_invalid() -> None:
    """R7 failing must produce INVALID regardless of other metrics."""
    result = score_group_date(_metrics(r7=False))
    assert result["prediction"]    == "INVALID"
    assert result["confidence"]    == 1
    assert _R7 in result["failed_gates"]


def test_all_gates_and_score_pass_confidence_5() -> None:
    """All 4 gates pass + 6/6 scored metrics → confidence 5."""
    result = score_group_date(_metrics(score_pass=6))
    assert result["prediction"]   == "VALID"
    assert result["pass_count"]   == 6
    assert result["valid_score"]  == 1.0
    assert result["confidence"]   == 5
    assert result["failed_gates"] == []


def test_one_score_miss_drops_to_confidence_4() -> None:
    """All gates pass + 5/6 scored → confidence 4 (one allowed miss, still VALID)."""
    result = score_group_date(_metrics(score_pass=5))
    assert result["prediction"]  == "VALID"
    assert result["confidence"]  == 4
    assert result["pass_count"]  == 5


def test_two_score_misses_returns_invalid() -> None:
    """All gates pass + only 4/6 scored → below conf4 threshold → INVALID."""
    result = score_group_date(_metrics(score_pass=4))
    assert result["prediction"]  == "INVALID"
    assert result["confidence"]  <  4


def test_score_metrics_exclude_none() -> None:
    """pass_metric=None is excluded from denominator."""
    results = [
        {"metric_name": _ATV, "pass_metric": True},
        {"metric_name": _R3,  "pass_metric": True},
        {"metric_name": _R5,  "pass_metric": True},
        {"metric_name": _R7,  "pass_metric": True},
        {"metric_name": "ambient_stability",  "pass_metric": True},
        {"metric_name": "ambient_range",      "pass_metric": None},
    ]
    result = score_group_date(results)
    assert result["n_assessed"] == 1
    assert result["pass_count"] == 1
    assert "ambient_range" in result["metrics_error"]


def test_l1_gate_blocks_valid() -> None:
    """Low Layer 1 pct_pass overrides a valid temperature score → INVALID."""
    result = score_group_date(_metrics(score_pass=6), l1_pct_pass=0.40)
    assert result["l1_gate_pass"] is False
    assert result["prediction"]   == "INVALID"
    assert result["confidence"]   <= 2


def test_l1_none_skips_gate() -> None:
    """Missing Layer 1 input does not block a VALID prediction."""
    result = score_group_date(_metrics(score_pass=6), l1_pct_pass=None)
    assert result["l1_gate_pass"] is True
    assert result["prediction"]   == "VALID"
