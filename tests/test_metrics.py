"""Tests for model_monitor.metrics.

All 9 Temperature-family metrics follow the same return schema:
  {
    "metric_name":          str,
    "pass_metric":          bool,
    "threshold":            scalar | dict,
    "value":                scalar | dict | None,
    "days_period":          int,
    "metric_decision_data": dict,
  }

Tests check `result["pass_metric"]` for the pass/fail verdict.
For ambient_temperature_volatility the "volatile" flag lives inside
`result["metric_decision_data"]["volatile"]`.
"""

import pandas as pd
import numpy as np
import pytest

from model_monitor.metrics.temperature import (
    ambient_temperature_volatility,
    ambient_stability,
    ambient_range,
    bucket_reference_adherence,
    sensor_spread_within_bucket,
    bucket_temporal_stability,
    bucket_diurnal_amplitude,
    small_hive_ambient_tracking,
    large_hive_thermoregulation,
    bucket_temperature_ordering,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_gateway(temps_by_day: dict[str, list[float]]) -> pd.DataFrame:
    """Build a gateway DataFrame from per-day hourly temperature lists.

    Includes gateway_mac_address so ambient_temperature_volatility can group by it.
    """
    rows = []
    for date_str, temps in temps_by_day.items():
        for hour, t in enumerate(temps):
            rows.append({
                "timestamp":           pd.Timestamp(f"{date_str} {hour:02d}:00:00"),
                "gateway_mac_address": "GW_TEST",
                "pcb_temperature_two": t,
            })
    return pd.DataFrame(rows)


def _make_gateway_hourly(mean_temp: float = 20.0, n_hours: int = 24) -> pd.DataFrame:
    """Simple flat-ambient gateway DataFrame (no gateway_mac_address column needed)."""
    return pd.DataFrame({
        "timestamp":           pd.date_range("2026-03-01", periods=n_hours, freq="1h"),
        "pcb_temperature_two": [mean_temp] * n_hours,
    })


def _make_sensor_hourly(
    bucket: str,
    mean_temp: float,
    n_sensors: int = 3,
    n_hours: int = 24,
    sensor_prefix: str = "S",
) -> pd.DataFrame:
    """Simple constant-temperature sensor DataFrame for one bucket."""
    rows = []
    for s in range(n_sensors):
        for h in range(n_hours):
            rows.append({
                "hive_size_bucket":    bucket,
                "sensor_mac_address":  f"{sensor_prefix}{s}",
                "timestamp":           pd.Timestamp("2026-03-01") + pd.Timedelta(hours=h),
                "pcb_temperature_one": mean_temp,
            })
    return pd.DataFrame(rows)


def _concat_buckets(*dfs: pd.DataFrame) -> pd.DataFrame:
    return pd.concat(dfs, ignore_index=True)


# ─────────────────────────────────────────────────────────────────────────────
# ambient_temperature_volatility
# ─────────────────────────────────────────────────────────────────────────────

def test_hourly_mean_is_used_not_raw_min() -> None:
    """Multiple gateways in the same hour are averaged, not min-picked.

    At 02:00 two gateways report 20 and 30 → mean = 25 (not 20).
    Both days have coldest-hour mean = 25 → delta = 0 < 5 → pass_metric=True (not volatile).
    """
    rows = []
    for hour in range(24):
        for gw, temp in [("gw_a", 27.0), ("gw_b", 27.0)]:
            rows.append({"timestamp": pd.Timestamp(f"2026-03-01 {hour:02d}:00:00"),
                         "gateway_mac_address": gw, "pcb_temperature_two": temp})
    for gw, temp in [("gw_a", 20.0), ("gw_b", 30.0)]:
        rows = [r for r in rows
                if not (r["timestamp"].hour == 2 and r["gateway_mac_address"] == gw
                        and str(r["timestamp"].date()) == "2026-03-01")]
        rows.append({"timestamp": pd.Timestamp("2026-03-01 02:00:00"),
                     "gateway_mac_address": gw, "pcb_temperature_two": temp})
    for hour in range(24):
        for gw, temp in [("gw_a", 27.0), ("gw_b", 27.0)]:
            rows.append({"timestamp": pd.Timestamp(f"2026-03-02 {hour:02d}:00:00"),
                         "gateway_mac_address": gw, "pcb_temperature_two": temp})
    for gw, temp in [("gw_a", 20.0), ("gw_b", 30.0)]:
        rows = [r for r in rows
                if not (r["timestamp"].hour == 2 and r["gateway_mac_address"] == gw
                        and str(r["timestamp"].date()) == "2026-03-02")]
        rows.append({"timestamp": pd.Timestamp("2026-03-02 02:00:00"),
                     "gateway_mac_address": gw, "pcb_temperature_two": temp})

    df = pd.DataFrame(rows)[["timestamp", "gateway_mac_address", "pcb_temperature_two"]]
    result = ambient_temperature_volatility(df)
    assert result["pass_metric"] is True
    assert result["metric_decision_data"]["volatile"] is False


def test_user_example_true() -> None:
    """day1 coldest=10, day2 coldest=5 → delta=5 ≥ 5 → volatile → pass_metric=False."""
    df = _make_gateway({
        "2026-03-01": [20, 18, 15, 12, 10, 11, 14, 18],
        "2026-03-02": [18, 16, 12,  8,  5,  6, 10, 15],
    })
    result = ambient_temperature_volatility(df)
    assert result["pass_metric"] is False
    assert result["metric_decision_data"]["volatile"] is True


def test_user_example_false() -> None:
    """day1 coldest=10, day2 coldest=8 → delta=2 < 5 → stable → pass_metric=True."""
    df = _make_gateway({
        "2026-03-01": [20, 18, 15, 12, 10, 11, 14, 18],
        "2026-03-02": [20, 18, 14, 10,  8,  9, 12, 16],
    })
    result = ambient_temperature_volatility(df)
    assert result["pass_metric"] is True
    assert result["metric_decision_data"]["volatile"] is False


def test_delta_exactly_at_threshold_is_true() -> None:
    """delta == 5.0 (boundary) → volatile → pass_metric=False (comparison is ≥)."""
    df = _make_gateway({
        "2026-03-01": [20, 15, 10, 10, 12, 18],
        "2026-03-02": [20, 17, 15, 15, 17, 20],
    })
    result = ambient_temperature_volatility(df)
    assert result["pass_metric"] is False
    assert result["metric_decision_data"]["volatile"] is True


def test_returns_false_when_only_one_day_of_data() -> None:
    """Single day → no consecutive pair → pass_metric=True (unassessable)."""
    df = _make_gateway({"2026-03-01": [20, 15, 12, 12, 15, 20, 25, 28]})
    result = ambient_temperature_volatility(df)
    assert result["pass_metric"] is True
    assert result["metric_decision_data"]["volatile"] is False


def test_returns_false_for_empty_dataframe() -> None:
    """Empty gateway → pass_metric=True (no evidence of volatility)."""
    df = pd.DataFrame(columns=["timestamp", "gateway_mac_address", "pcb_temperature_two"])
    result = ambient_temperature_volatility(df)
    assert result["pass_metric"] is True
    assert result["metric_decision_data"]["volatile"] is False


def test_custom_threshold() -> None:
    """delta=4 °C: volatile at threshold=3 (pass_metric=False), stable at threshold=5 (pass_metric=True)."""
    df = _make_gateway({
        "2026-03-01": [25, 20, 14, 14, 16, 20],
        "2026-03-02": [25, 20, 18, 18, 20, 25],
    })
    assert ambient_temperature_volatility(df, min_delta_celsius=3.0)["pass_metric"] is False
    assert ambient_temperature_volatility(df, min_delta_celsius=5.0)["pass_metric"] is True


def test_gradual_drift_does_not_trigger() -> None:
    """Troughs: 10→13→16 °C — each delta=3, never ≥ 5 → pass_metric=True."""
    df = _make_gateway({
        "2026-03-01": [22, 18, 10, 10, 11, 18],
        "2026-03-02": [24, 20, 13, 13, 14, 20],
        "2026-03-03": [26, 22, 16, 16, 17, 22],
    })
    assert ambient_temperature_volatility(df)["pass_metric"] is True


def test_second_consecutive_pair_triggers() -> None:
    """day1→day2 delta=2, day2→day3 delta=7 ≥ 5 → pass_metric=False."""
    df = _make_gateway({
        "2026-03-01": [25, 20, 15, 15, 16, 20],
        "2026-03-02": [26, 21, 17, 17, 18, 21],
        "2026-03-03": [28, 24, 24, 24, 25, 26],
    })
    assert ambient_temperature_volatility(df)["pass_metric"] is False


# ─────────────────────────────────────────────────────────────────────────────
# R1 — ambient_stability
# ─────────────────────────────────────────────────────────────────────────────

def test_ambient_stability_stable() -> None:
    """Flat ambient → CV ≈ 0 → pass_metric=True."""
    gw = _make_gateway_hourly(mean_temp=20.0)
    assert ambient_stability(gw)["pass_metric"] is True


def test_ambient_stability_highly_unstable() -> None:
    """CV far above 0.70 → pass_metric=False."""
    temps = [1.0, 50.0] * 12
    gw = pd.DataFrame({
        "timestamp":           pd.date_range("2026-03-01", periods=24, freq="1h"),
        "pcb_temperature_two": temps,
    })
    assert ambient_stability(gw)["pass_metric"] is False


# ─────────────────────────────────────────────────────────────────────────────
# R2 — ambient_range
# ─────────────────────────────────────────────────────────────────────────────

def test_ambient_range_in_bounds() -> None:
    """Ambient within [5, 50] → pass_metric=True."""
    gw = _make_gateway_hourly(mean_temp=22.0)
    assert ambient_range(gw)["pass_metric"] is True


def test_ambient_range_too_cold() -> None:
    """Ambient dips below 5 °C → pass_metric=False."""
    gw = _make_gateway_hourly(mean_temp=1.0)
    assert ambient_range(gw)["pass_metric"] is False


def test_ambient_range_too_hot() -> None:
    """Ambient exceeds 50 °C → pass_metric=False."""
    gw = _make_gateway_hourly(mean_temp=55.0)
    assert ambient_range(gw)["pass_metric"] is False


# ─────────────────────────────────────────────────────────────────────────────
# R3 — bucket_reference_adherence
# ─────────────────────────────────────────────────────────────────────────────

def test_bucket_reference_adherence_passes() -> None:
    """Sensors within all canonical bands → pass_metric=True."""
    sh = _concat_buckets(
        _make_sensor_hourly("small",  mean_temp=22.0),  # [17.4, 29.0] ✓
        _make_sensor_hourly("medium", mean_temp=29.0),  # [27.3, 32.0] ✓
        _make_sensor_hourly("large",  mean_temp=34.5),  # [33.9, 35.0] ✓
    )
    assert bucket_reference_adherence(sh)["pass_metric"] is True


def test_bucket_reference_adherence_fails() -> None:
    """Large bucket far outside its band → pass_metric=False."""
    sh = _concat_buckets(
        _make_sensor_hourly("small",  mean_temp=22.0),
        _make_sensor_hourly("medium", mean_temp=29.0),
        _make_sensor_hourly("large",  mean_temp=5.0),   # WAY outside [33.9, 35.0]
    )
    assert bucket_reference_adherence(sh)["pass_metric"] is False


# ─────────────────────────────────────────────────────────────────────────────
# R4 — sensor_spread_within_bucket
# ─────────────────────────────────────────────────────────────────────────────

def test_sensor_spread_within_bucket_passes() -> None:
    """All sensors at same temperature → spread = 0 → pass_metric=True."""
    sh = _make_sensor_hourly("large", mean_temp=32.0)
    assert sensor_spread_within_bucket(sh)["pass_metric"] is True


def test_sensor_spread_within_bucket_fails() -> None:
    """Two large-bucket sensors 4 °C apart → std ≈ 2.83 °C > BUCKET_SPREAD_MAX[large] (1.05 °C) → False."""
    rows = []
    for h in range(24):
        ts = pd.Timestamp("2026-03-01") + pd.Timedelta(hours=h)
        rows.append({"hive_size_bucket": "large", "sensor_mac_address": "A",
                     "timestamp": ts, "pcb_temperature_one": 32.0})
        rows.append({"hive_size_bucket": "large", "sensor_mac_address": "B",
                     "timestamp": ts, "pcb_temperature_one": 36.0})
    sh = pd.DataFrame(rows)
    assert sensor_spread_within_bucket(sh)["pass_metric"] is False


# ─────────────────────────────────────────────────────────────────────────────
# R5 — bucket_temporal_stability
# ─────────────────────────────────────────────────────────────────────────────

def test_bucket_temporal_stability_passes() -> None:
    """Flat temperature time series → std = 0 → pass_metric=True."""
    sh = _make_sensor_hourly("large", mean_temp=32.0)
    assert bucket_temporal_stability(sh)["pass_metric"] is True


def test_bucket_temporal_stability_large_fails() -> None:
    """Large bucket alternates 25/35 °C across days → temporal_std >> 4.5 → pass_metric=False."""
    rows = []
    for d in range(10):
        temp = 25.0 if d % 2 == 0 else 35.0
        for h in range(24):
            ts = pd.Timestamp("2026-03-01") + pd.Timedelta(days=d, hours=h)
            rows.append({"hive_size_bucket": "large", "sensor_mac_address": "A",
                         "timestamp": ts, "pcb_temperature_one": temp})
    sh = pd.DataFrame(rows)
    assert bucket_temporal_stability(sh)["pass_metric"] is False


# ─────────────────────────────────────────────────────────────────────────────
# R7 — bucket_diurnal_amplitude
# ─────────────────────────────────────────────────────────────────────────────

def test_bucket_diurnal_amplitude_passes() -> None:
    """Flat large-bucket temperature → daily amp ≈ 0 → pass_metric=True."""
    sh = _make_sensor_hourly("large", mean_temp=34.0)
    assert bucket_diurnal_amplitude(sh)["pass_metric"] is True


def test_bucket_diurnal_amplitude_large_fails() -> None:
    """Large bucket swings 20°C every day → amp ≈ 20 > 14 → pass_metric=False."""
    rows = []
    for d in range(2):
        for h in range(24):
            ts = pd.Timestamp("2026-03-01") + pd.Timedelta(days=d, hours=h)
            # Sinusoid with amplitude 10 → peak-to-trough ≈ 20°C
            temp = 25.0 + 10.0 * np.sin(2 * np.pi * h / 24)
            rows.append({"hive_size_bucket": "large", "sensor_mac_address": "A",
                         "timestamp": ts, "pcb_temperature_one": temp})
    sh = pd.DataFrame(rows)
    result = bucket_diurnal_amplitude(sh)
    assert result["pass_metric"] is False
    assert result["value"]["large"] > 14.0


def test_bucket_diurnal_amplitude_small_loose_passes() -> None:
    """Small bucket can swing widely (cap=40) → 20°C swing still passes."""
    rows = []
    for d in range(2):
        for h in range(24):
            ts = pd.Timestamp("2026-03-01") + pd.Timedelta(days=d, hours=h)
            temp = 25.0 + 10.0 * np.sin(2 * np.pi * h / 24)
            rows.append({"hive_size_bucket": "small", "sensor_mac_address": "A",
                         "timestamp": ts, "pcb_temperature_one": temp})
    sh = pd.DataFrame(rows)
    assert bucket_diurnal_amplitude(sh)["pass_metric"] is True


# ─────────────────────────────────────────────────────────────────────────────
# R6a — small_hive_ambient_tracking
# ─────────────────────────────────────────────────────────────────────────────

def test_small_hive_ambient_tracking_passes() -> None:
    """Small bucket follows ambient exactly → r = 1.0 ≥ 0.3 → pass_metric=True."""
    hours = 24
    ts = pd.date_range("2026-03-01", periods=hours, freq="1h")
    temps = [15.0 + i for i in range(hours)]
    gw = pd.DataFrame({"timestamp": ts, "pcb_temperature_two": temps})
    rows = [{"hive_size_bucket": "small", "sensor_mac_address": "S0",
             "timestamp": ts[h], "pcb_temperature_one": temps[h]}
            for h in range(hours)]
    sh = pd.DataFrame(rows)
    assert small_hive_ambient_tracking(sh, gw)["pass_metric"] is True


def test_small_hive_ambient_tracking_no_small_bucket() -> None:
    """No small bucket → not applicable → pass_metric=True."""
    gw = _make_gateway_hourly(mean_temp=20.0)
    sh = _make_sensor_hourly("large", mean_temp=32.0)
    assert small_hive_ambient_tracking(sh, gw)["pass_metric"] is True


def test_small_hive_ambient_tracking_fails() -> None:
    """Small bucket moves opposite to ambient → r = -1.0 < 0.3 → pass_metric=False."""
    hours = 24
    ts = pd.date_range("2026-03-01", periods=hours, freq="1h")
    ambient_temps = [15.0 + i for i in range(hours)]
    small_temps   = [38.0 - i for i in range(hours)]
    gw = pd.DataFrame({"timestamp": ts, "pcb_temperature_two": ambient_temps})
    rows = [{"hive_size_bucket": "small", "sensor_mac_address": "S0",
             "timestamp": ts[h], "pcb_temperature_one": small_temps[h]}
            for h in range(hours)]
    sh = pd.DataFrame(rows)
    assert small_hive_ambient_tracking(sh, gw)["pass_metric"] is False


# ─────────────────────────────────────────────────────────────────────────────
# R6b — large_hive_thermoregulation
# ─────────────────────────────────────────────────────────────────────────────

def test_large_hive_thermoregulation_passes() -> None:
    """Large bucket anti-correlated with ambient → r = -1.0 ≤ 0.6 → pass_metric=True."""
    hours = 24
    ts = pd.date_range("2026-03-01", periods=hours, freq="1h")
    ambient_temps = [15.0 + i for i in range(hours)]
    large_temps   = [38.0 - i for i in range(hours)]
    gw = pd.DataFrame({"timestamp": ts, "pcb_temperature_two": ambient_temps})
    rows = [{"hive_size_bucket": "large", "sensor_mac_address": "S0",
             "timestamp": ts[h], "pcb_temperature_one": large_temps[h]}
            for h in range(hours)]
    sh = pd.DataFrame(rows)
    assert large_hive_thermoregulation(sh, gw)["pass_metric"] is True


def test_large_hive_thermoregulation_fails() -> None:
    """Large bucket tracks ambient perfectly → r = 1.0 > 0.6 → pass_metric=False."""
    hours = 24
    ts = pd.date_range("2026-03-01", periods=hours, freq="1h")
    temps = [15.0 + i for i in range(hours)]
    gw = pd.DataFrame({"timestamp": ts, "pcb_temperature_two": temps})
    rows = [{"hive_size_bucket": "large", "sensor_mac_address": "A",
             "timestamp": ts[h], "pcb_temperature_one": temps[h]}
            for h in range(hours)]
    sh = pd.DataFrame(rows)
    assert large_hive_thermoregulation(sh, gw)["pass_metric"] is False


# ─────────────────────────────────────────────────────────────────────────────
# R6c — bucket_temperature_ordering
# ─────────────────────────────────────────────────────────────────────────────

def test_bucket_temperature_ordering_passes() -> None:
    """small < medium < large with gaps ≥ 1.5 °C → pass_metric=True."""
    sh = _concat_buckets(
        _make_sensor_hourly("small",  mean_temp=22.0),
        _make_sensor_hourly("medium", mean_temp=28.0),
        _make_sensor_hourly("large",  mean_temp=33.0),
    )
    assert bucket_temperature_ordering(sh)["pass_metric"] is True


def test_bucket_temperature_ordering_violated() -> None:
    """large < medium → ordering violated → pass_metric=False."""
    sh = _concat_buckets(
        _make_sensor_hourly("small",  mean_temp=22.0),
        _make_sensor_hourly("medium", mean_temp=33.0),
        _make_sensor_hourly("large",  mean_temp=28.0),
    )
    assert bucket_temperature_ordering(sh)["pass_metric"] is False


def test_bucket_temperature_ordering_gap_too_small() -> None:
    """Ordered but gap = 0.5 °C < 1.5 °C → pass_metric=False."""
    sh = _concat_buckets(
        _make_sensor_hourly("small",  mean_temp=22.0),
        _make_sensor_hourly("medium", mean_temp=28.0),
        _make_sensor_hourly("large",  mean_temp=28.5),
    )
    assert bucket_temperature_ordering(sh)["pass_metric"] is False
