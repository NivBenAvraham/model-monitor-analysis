"""Tests for model_monitor.metrics.pred_rules and decision.pred_rule."""

import pandas as pd
import numpy as np
import pytest

from model_monitor.metrics.pred_rules import clipping_pressure
from model_monitor.metrics.pred_rules import (
    clip_diff_mean, clip_diff_p90, clip_diff_max,
    clip_diff_mean_roll3, clip_diff_mean_roll5, clip_diff_mean_roll7,
    pct_clipped, pct_clipped_roll3, pct_clipped_roll7,
    pred_raw_std, pred_raw_range,
)
from model_monitor.decision.pred_rule import score_group_date


# ── helpers ──────────────────────────────────────────────────────────────────

def _make_preprocess(n_days: int, n_sensors: int,
                     clip_diff_per_day: float | list[float] = 0.0) -> pd.DataFrame:
    """Build a minimal preprocess DataFrame.

    clip_diff_per_day controls how much pred_raw exceeds pred_clipped each day.
    If a list, length must equal n_days.
    """
    if isinstance(clip_diff_per_day, (int, float)):
        clip_diffs = [clip_diff_per_day] * n_days
    else:
        clip_diffs = list(clip_diff_per_day)

    rows = []
    base = pd.Timestamp("2026-03-01")
    for d in range(n_days):
        date = base + pd.Timedelta(days=d)
        diff = clip_diffs[d]
        for s in range(n_sensors):
            pred_clipped = 12.0
            pred_raw     = pred_clipped + diff
            rows.append({"date": date, "mac": f"sensor_{s}",
                         "pred_raw": pred_raw, "pred_clipped": pred_clipped})
    return pd.DataFrame(rows)


# ── clipping_pressure tests ───────────────────────────────────────────────────

def test_clipping_pressure_passes_no_clipping() -> None:
    """All sensors unclipped → clip_diff=0 → pass_metric=True."""
    df = _make_preprocess(n_days=5, n_sensors=10, clip_diff_per_day=0.0)
    result = clipping_pressure(df)
    assert result["pass_metric"] is True
    assert result["value"]["clip_diff_mean"] == 0.0


def test_clipping_pressure_fails_high_clipping() -> None:
    """Clip diff=2.0 well above all thresholds → pass_metric=False."""
    df = _make_preprocess(n_days=5, n_sensors=10, clip_diff_per_day=2.0)
    result = clipping_pressure(df)
    assert result["pass_metric"] is False
    assert result["value"]["clip_diff_mean"] > 0.60


def test_clipping_pressure_roll3_uses_last_day() -> None:
    """Rolling mean is computed over the last 3 days; today's clip diff drives it."""
    # Last day spikes; prior days are clean → roll3 should be moderate
    diffs = [0.1, 0.1, 0.1, 0.1, 2.5]   # spike on day 5
    df = _make_preprocess(n_days=5, n_sensors=5, clip_diff_per_day=diffs)
    result = clipping_pressure(df)
    # roll3 = mean(0.1, 0.1, 2.5) ≈ 0.90 → fails roll3 gate (>0.65)
    assert result["value"]["clip_diff_roll3"] is not None
    assert result["value"]["clip_diff_roll3"] > 0.65
    assert result["pass_metric"] is False


def test_clipping_pressure_single_day_no_roll3() -> None:
    """Only 1 day of data → clip_diff_roll3=None, gate_verdict is None."""
    df = _make_preprocess(n_days=1, n_sensors=5, clip_diff_per_day=0.2)
    result = clipping_pressure(df)
    assert result["value"]["clip_diff_roll3"] is None
    assert result["metric_decision_data"]["gate_verdicts"]["clip_diff_roll3"] is None


def test_clipping_pressure_missing_columns() -> None:
    """Missing required columns → pass_metric=False with error."""
    bad_df = pd.DataFrame({"date": ["2026-03-01"], "mac": ["A"]})
    result = clipping_pressure(bad_df)
    assert result["pass_metric"] is False
    assert "error" in result["metric_decision_data"]


def test_clipping_pressure_result_schema() -> None:
    """Result contains all expected keys."""
    df = _make_preprocess(n_days=3, n_sensors=4, clip_diff_per_day=0.3)
    result = clipping_pressure(df)
    for key in ("metric_name", "pass_metric", "threshold", "value",
                "days_in_window", "metric_decision_data"):
        assert key in result
    for sig in ("clip_diff_mean", "clip_diff_p90", "pct_clipped",
                "clip_diff_roll3", "clip_diff_roll5"):
        assert sig in result["value"]


# ── pred_rule.score_group_date tests ─────────────────────────────────────────

def _clipping_result(clip_diff_mean: float,
                     clip_diff_roll3: float | None = None) -> dict:
    """Build a minimal clipping_pressure result dict."""
    return {
        "value": {
            "clip_diff_mean":  clip_diff_mean,
            "clip_diff_roll3": clip_diff_roll3,
            "clip_diff_p90":   clip_diff_mean * 2,
            "pct_clipped":     0.3,
            "clip_diff_roll5": clip_diff_roll3,
        },
        "metric_decision_data": {"gate_verdicts": {}, "daily": []},
    }


def test_pred_rule_valid_roll3_passes() -> None:
    """roll3 below threshold → valid, confidence=5."""
    r = score_group_date(_clipping_result(0.20, 0.30))
    assert r["prediction"] == "valid"
    assert r["confidence"] == 5
    assert r["signal"] == "clip_diff_roll3"


def test_pred_rule_invalid_roll3_fails() -> None:
    """roll3 above threshold → invalid."""
    r = score_group_date(_clipping_result(0.20, 0.80))
    assert r["prediction"] == "invalid"
    assert "clip_diff_roll3" in r["reason"]
    assert r["signal"] == "clip_diff_roll3"


def test_pred_rule_valid_fallback_mean() -> None:
    """No roll3 history, mean passes → valid at confidence=4."""
    r = score_group_date(_clipping_result(0.30, None))
    assert r["prediction"] == "valid"
    assert r["confidence"] == 4
    assert r["signal"] == "clip_diff_mean"
    assert "insufficient history" in r["reason"]


def test_pred_rule_invalid_fallback_mean_fails() -> None:
    """No roll3 history, mean also fails → invalid."""
    r = score_group_date(_clipping_result(1.50, None))
    assert r["prediction"] == "invalid"
    assert r["signal"] == "clip_diff_mean"


def test_pred_rule_invalid_no_data() -> None:
    """No signals at all → invalid, confidence=1."""
    r = score_group_date({"value": {}, "metric_decision_data": {"gate_verdicts": {}, "daily": []}})
    assert r["prediction"] == "invalid"
    assert r["confidence"] == 1
    assert r["signal"] is None


def test_pred_rule_very_high_clipping_low_confidence() -> None:
    """Clipping 3× threshold → confidence=1."""
    r = score_group_date(_clipping_result(0.20, 2.5))   # roll3=2.5, thr=0.591 → ratio≈4
    assert r["prediction"] == "invalid"
    assert r["confidence"] == 1


def test_pred_rule_moderate_clipping_confidence_3() -> None:
    """Clipping just above threshold → confidence=3."""
    r = score_group_date(_clipping_result(0.20, 0.65))  # roll3=0.65, thr=0.591 → ratio≈1.1
    assert r["prediction"] == "invalid"
    assert r["confidence"] == 3


# ── stub tests for individual pred_rules metrics ─────────────────────────────

def _make_pp(n_days: int = 7, n_sensors: int = 4,
             raw_above_clip: float = 0.0) -> pd.DataFrame:
    """Minimal preprocess DataFrame for individual metric tests."""
    dates = pd.date_range("2026-03-01", periods=n_days, freq="D")
    rows = []
    for d in dates:
        for i in range(n_sensors):
            raw = 13.0 + raw_above_clip
            rows.append({"date": d, "mac": f"aa:{i:02x}",
                         "pred_raw": raw, "pred_clipped": 13.0})
    return pd.DataFrame(rows)


def test_clip_diff_mean_low_clipping_passes() -> None:
    """Low daily clip_diff_mean → pass."""
    r = clip_diff_mean(_make_pp(raw_above_clip=0.1))
    assert r["pass_metric"] is True
    assert r["value"] == pytest.approx(0.1, abs=1e-4)


def test_clip_diff_mean_high_clipping_fails() -> None:
    """High daily clip_diff_mean → fail."""
    r = clip_diff_mean(_make_pp(raw_above_clip=2.0))
    assert r["pass_metric"] is False


def test_clip_diff_p90_passes() -> None:
    r = clip_diff_p90(_make_pp(raw_above_clip=0.5))
    assert r["pass_metric"] is True
    assert r["value"] == pytest.approx(0.5, abs=1e-4)


def test_clip_diff_max_fails_when_above_threshold() -> None:
    r = clip_diff_max(_make_pp(raw_above_clip=5.0))
    assert r["pass_metric"] is False


def test_clip_diff_mean_roll3_needs_history() -> None:
    """Single day → insufficient history → pass_metric=None."""
    r = clip_diff_mean_roll3(_make_pp(n_days=1))
    assert r["pass_metric"] is None


def test_clip_diff_mean_roll3_passes_with_history() -> None:
    r = clip_diff_mean_roll3(_make_pp(n_days=5, raw_above_clip=0.1))
    assert r["pass_metric"] is True


def test_clip_diff_mean_roll5_passes() -> None:
    r = clip_diff_mean_roll5(_make_pp(n_days=7, raw_above_clip=0.1))
    assert r["pass_metric"] is True


def test_clip_diff_mean_roll7_insufficient_history() -> None:
    r = clip_diff_mean_roll7(_make_pp(n_days=2))
    assert r["pass_metric"] is None


def test_pct_clipped_no_clipping_passes() -> None:
    r = pct_clipped(_make_pp(raw_above_clip=0.0))
    assert r["pass_metric"] is True
    assert r["value"] == pytest.approx(0.0, abs=1e-4)


def test_pct_clipped_all_clipped_fails() -> None:
    r = pct_clipped(_make_pp(raw_above_clip=1.0))
    assert r["pass_metric"] is False


def test_pct_clipped_roll3_passes_unclipped() -> None:
    r = pct_clipped_roll3(_make_pp(n_days=5, raw_above_clip=0.0))
    assert r["pass_metric"] is True


def test_pct_clipped_roll7_needs_history() -> None:
    r = pct_clipped_roll7(_make_pp(n_days=2))
    assert r["pass_metric"] is None


def test_pred_raw_std_tight_cluster_passes() -> None:
    """All sensors have same pred_raw → std=0 → pass."""
    r = pred_raw_std(_make_pp(n_days=1, raw_above_clip=0.0))
    assert r["pass_metric"] is True
    assert r["value"] == pytest.approx(0.0, abs=1e-6)


def test_pred_raw_range_small_range_passes() -> None:
    r = pred_raw_range(_make_pp(n_days=1, raw_above_clip=0.5))
    assert r["pass_metric"] is True
    assert r["value"] == pytest.approx(0.0, abs=1e-6)
