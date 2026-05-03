"""
Stub tests for src/model_monitor/metrics/triage_rules/.

Each test verifies the function exists, returns the expected dict shape,
and handles empty / edge-case input without raising.
"""

import pandas as pd
import pytest

from model_monitor.metrics.triage_rules import (
    auto_review_score,
    clipping_diff,
    inspection_discrepancy,
    thermoreg_dipping,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _assert_result_shape(result: dict, expected_metric_name: str) -> None:
    """Every metric result must carry these keys with the correct types."""
    assert result["metric_name"] == expected_metric_name
    assert "pass_metric" in result
    assert "value" in result
    assert "threshold" in result


# ---------------------------------------------------------------------------
# Signal A — clipping_diff
# ---------------------------------------------------------------------------

class TestClippingDiff:
    def test_empty_df_passes(self):
        result = clipping_diff(pd.DataFrame())
        _assert_result_shape(result, "clipping_diff")
        assert result["pass_metric"] is True
        assert result["value"] is None

    def test_no_clipping_passes(self):
        df = pd.DataFrame({
            "pred_raw":     [5.0, 6.0, 7.0],
            "pred_clipped": [5.0, 6.0, 7.0],
        })
        result = clipping_diff(df)
        _assert_result_shape(result, "clipping_diff")
        assert result["pass_metric"] is True
        assert result["value"] == pytest.approx(0.0)

    def test_large_diff_fails(self):
        df = pd.DataFrame({
            "pred_raw":     [10.0, 12.0],
            "pred_clipped": [5.0,  5.0],
        })
        result = clipping_diff(df)
        _assert_result_shape(result, "clipping_diff")
        assert result["pass_metric"] is False
        assert result["value"] > 1.0

    def test_exactly_at_threshold_passes(self):
        df = pd.DataFrame({
            "pred_raw":     [6.0],
            "pred_clipped": [5.0],
        })
        result = clipping_diff(df)
        _assert_result_shape(result, "clipping_diff")
        assert result["pass_metric"] is True
        assert result["value"] == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# Signal B — inspection_discrepancy
# ---------------------------------------------------------------------------

class TestInspectionDiscrepancy:
    def test_empty_inspections_passes(self):
        result = inspection_discrepancy(pd.DataFrame(), pd.DataFrame())
        _assert_result_shape(result, "inspection_discrepancy")
        assert result["pass_metric"] is True
        assert result["value"] is None

    def test_small_discrepancy_passes(self):
        insp_df = pd.DataFrame({
            "group_id":               [1],
            "inspection_id":          [101],
            "bee_frames_distribution": ['{"5": 4, "6": 4}'],  # avg = 5.5
        })
        model_df = pd.DataFrame({
            "group_id":               [1],
            "sensor_mac_address":     ["AA:BB:CC"],
            "numerical_model_result": [5.8],  # discrepancy = 0.3
        })
        result = inspection_discrepancy(insp_df, model_df)
        _assert_result_shape(result, "inspection_discrepancy")
        assert result["pass_metric"] is True
        assert result["value"] == pytest.approx(0.3, abs=0.01)

    def test_large_discrepancy_fails(self):
        insp_df = pd.DataFrame({
            "group_id":               [1],
            "inspection_id":          [101],
            "bee_frames_distribution": ['{"3": 5}'],  # avg = 3.0
        })
        model_df = pd.DataFrame({
            "group_id":               [1],
            "sensor_mac_address":     ["AA:BB:CC"],
            "numerical_model_result": [6.0],  # discrepancy = 3.0
        })
        result = inspection_discrepancy(insp_df, model_df)
        _assert_result_shape(result, "inspection_discrepancy")
        assert result["pass_metric"] is False
        assert result["value"] > 1.5


# ---------------------------------------------------------------------------
# Signal C — thermoreg_dipping
# ---------------------------------------------------------------------------

class TestThermoregDipping:
    def test_empty_df_passes(self):
        result = thermoreg_dipping(pd.DataFrame())
        _assert_result_shape(result, "thermoreg_dipping")
        assert result["pass_metric"] is True
        assert result["value"] is None

    def test_stable_yards_pass(self):
        df = pd.DataFrame({
            "yard_id":    [1] * 7,
            "yard_name":  ["yard_1"] * 7,
            "date":       pd.date_range("2026-04-01", periods=7),
            "temp_std":   [0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5],
        })
        result = thermoreg_dipping(df)
        _assert_result_shape(result, "thermoreg_dipping")
        assert result["pass_metric"] is True
        assert result["value"] == pytest.approx(0.0)

    def test_dipping_yard_fails(self):
        # Strongly rising std from the start — should be classified as dipping
        df = pd.DataFrame({
            "yard_id":   [1] * 7,
            "yard_name": ["yard_1"] * 7,
            "date":      pd.date_range("2026-04-01", periods=7),
            "temp_std":  [0.1, 0.3, 0.6, 1.0, 1.5, 2.0, 2.8],
        })
        result = thermoreg_dipping(df)
        _assert_result_shape(result, "thermoreg_dipping")
        # 1/1 yard dipping = 100 % → fails threshold of 15 %
        assert result["pass_metric"] is False


# ---------------------------------------------------------------------------
# Signal D — auto_review_score
# ---------------------------------------------------------------------------

class TestAutoReviewScore:
    def test_empty_df_returns_none(self):
        result = auto_review_score(pd.DataFrame(), "2026-04-15")
        _assert_result_shape(result, "auto_review_score")
        assert result["pass_metric"] is None
        assert result["value"] is None

    def test_insufficient_data_returns_none(self):
        # Only 3 rows — well below MIN_RECENT_ROWS = 50
        df = pd.DataFrame({
            "sensor_mac_address": ["AA"] * 3,
            "input_date":         pd.date_range("2026-04-13", periods=3).date,
            "pred_raw":           [5.0, 5.1, 5.2],
        })
        result = auto_review_score(df, "2026-04-15")
        _assert_result_shape(result, "auto_review_score")
        assert result["pass_metric"] is None

    def test_stable_predictions_pass(self):
        # 3 sensors × 7 days × 10 readings = 210 rows, perfectly stable
        rows = []
        for sensor in ["S1", "S2", "S3"]:
            for day in pd.date_range("2026-04-09", periods=7):
                for _ in range(10):
                    rows.append({"sensor_mac_address": sensor,
                                 "input_date": day.date(),
                                 "pred_raw": 6.0})
        df = pd.DataFrame(rows)
        result = auto_review_score(df, "2026-04-15")
        _assert_result_shape(result, "auto_review_score")
        assert result["pass_metric"] is True
        assert result["value"] < 2.4
