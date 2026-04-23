"""Tests for model_monitor.metrics."""

import pandas as pd

from model_monitor.metrics.ambient_temperature_volatility import (
    ambient_temperature_volatility,
)


def _make_gateway(temps_by_day: dict[str, list[float]]) -> pd.DataFrame:
    """Build a gateway_hourly DataFrame from per-day hourly temperature lists."""
    rows = []
    for date_str, temps in temps_by_day.items():
        for hour, t in enumerate(temps):
            rows.append(
                {
                    "timestamp": pd.Timestamp(f"{date_str} {hour:02d}:00:00"),
                    "pcb_temperature_two": t,
                }
            )
    return pd.DataFrame(rows)


# ── exact examples from the spec ──────────────────────────────────────────────

def test_hourly_mean_is_used_not_raw_min() -> None:
    """Multiple gateways in the same hour are averaged, not min-picked.

    At 02:00 two gateways report 20 and 30 → mean = 25 (not 20).
    Coldest hour is still 02:00 (mean=25) vs all other hours (mean=27).
    Consecutive delta = |25 - 25| = 0 < 5 → False.
    """
    rows = []
    for hour in range(24):
        for gw, temp in [("gw_a", 27.0), ("gw_b", 27.0)]:
            rows.append({"timestamp": pd.Timestamp(f"2026-03-01 {hour:02d}:00:00"),
                         "gateway": gw, "pcb_temperature_two": temp})
    # Override hour 02 on day 1: two gateways → mean 25 (not the raw min 20)
    for gw, temp in [("gw_a", 20.0), ("gw_b", 30.0)]:
        rows = [r for r in rows
                if not (r["timestamp"].hour == 2 and r["gateway"] == gw
                        and str(r["timestamp"].date()) == "2026-03-01")]
        rows.append({"timestamp": pd.Timestamp("2026-03-01 02:00:00"),
                     "gateway": gw, "pcb_temperature_two": temp})
    # Day 2: same pattern, coldest hour mean = 25
    for hour in range(24):
        for gw, temp in [("gw_a", 27.0), ("gw_b", 27.0)]:
            rows.append({"timestamp": pd.Timestamp(f"2026-03-02 {hour:02d}:00:00"),
                         "gateway": gw, "pcb_temperature_two": temp})
    for gw, temp in [("gw_a", 20.0), ("gw_b", 30.0)]:
        rows = [r for r in rows
                if not (r["timestamp"].hour == 2 and r["gateway"] == gw
                        and str(r["timestamp"].date()) == "2026-03-02")]
        rows.append({"timestamp": pd.Timestamp("2026-03-02 02:00:00"),
                     "gateway": gw, "pcb_temperature_two": temp})

    df = pd.DataFrame(rows)[["timestamp", "pcb_temperature_two"]]
    # Both days coldest-hour mean = 25 → delta = 0 → False
    assert ambient_temperature_volatility(df) is False


def test_user_example_true() -> None:
    """day1 coldest=10, day2 coldest=5  →  delta=5 ≥ 5  →  True."""
    df = _make_gateway(
        {
            "2026-03-01": [20, 18, 15, 12, 10, 11, 14, 18],
            "2026-03-02": [18, 16, 12, 8,   5,  6, 10, 15],
        }
    )
    assert ambient_temperature_volatility(df) is True


def test_user_example_false() -> None:
    """day1 coldest=10, day2 coldest=8  →  delta=2 < 5  →  False."""
    df = _make_gateway(
        {
            "2026-03-01": [20, 18, 15, 12, 10, 11, 14, 18],
            "2026-03-02": [20, 18, 14, 10,  8,  9, 12, 16],
        }
    )
    assert ambient_temperature_volatility(df) is False


def test_delta_exactly_at_threshold_is_true() -> None:
    """delta == 5.0 (boundary) must return True (comparison is ≥)."""
    df = _make_gateway(
        {
            "2026-03-01": [20, 15, 10, 10, 12, 18],
            "2026-03-02": [20, 17, 15, 15, 17, 20],
        }
    )
    # day1 coldest=10, day2 coldest=15  →  delta=5.0
    assert ambient_temperature_volatility(df) is True


# ── edge cases ────────────────────────────────────────────────────────────────

def test_returns_false_when_only_one_day_of_data() -> None:
    df = _make_gateway({"2026-03-01": [20, 15, 12, 12, 15, 20, 25, 28]})
    assert ambient_temperature_volatility(df) is False


def test_returns_false_for_empty_dataframe() -> None:
    df = pd.DataFrame(columns=["timestamp", "pcb_temperature_two"])
    assert ambient_temperature_volatility(df) is False


def test_custom_threshold() -> None:
    """delta=4 °C: volatile at threshold=3, stable at threshold=5."""
    df = _make_gateway(
        {
            "2026-03-01": [25, 20, 14, 14, 16, 20],
            "2026-03-02": [25, 20, 18, 18, 20, 25],
        }
    )
    # day1 coldest=14, day2 coldest=18  →  delta=4
    assert ambient_temperature_volatility(df, min_delta_celsius=3.0) is True
    assert ambient_temperature_volatility(df, min_delta_celsius=5.0) is False


# ── multi-day behaviour ───────────────────────────────────────────────────────

def test_gradual_drift_does_not_trigger() -> None:
    """Troughs: 10→13→16 °C — each consecutive delta=3, never ≥ 5 → False."""
    df = _make_gateway(
        {
            "2026-03-01": [22, 18, 10, 10, 11, 18],
            "2026-03-02": [24, 20, 13, 13, 14, 20],
            "2026-03-03": [26, 22, 16, 16, 17, 22],
        }
    )
    assert ambient_temperature_volatility(df) is False


def test_second_consecutive_pair_triggers() -> None:
    """day1→day2 stable (delta=2), day2→day3 jumps 7 °C → True."""
    df = _make_gateway(
        {
            "2026-03-01": [25, 20, 15, 15, 16, 20],
            "2026-03-02": [26, 21, 17, 17, 18, 21],
            "2026-03-03": [28, 24, 24, 24, 25, 26],
        }
    )
    # day1=15, day2=17 (Δ=2), day2=17, day3=24 (Δ=7 ≥ 5) → True
    assert ambient_temperature_volatility(df) is True
