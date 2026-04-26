"""
Ambient Temperature Volatility — Temperature family metric.

Detects whether the coldest nightly temperature changes significantly between
consecutive days in the observation window.

Physical motivation
-------------------
Ambient (gateway) temperature follows a predictable diurnal curve: warm during
the day, cold overnight.  Each day has one coldest hour (typically 4–6 AM).

When the coldest nightly temperature of one day differs from the next by more
than ``MIN_DAILY_DELTA_CELSIUS``, the weather changed substantially between
those two nights — the hive model is being evaluated under shifting conditions.

Algorithm
---------
1. For each (gateway, clock-hour) bin compute the **min** temperature across
   all raw readings in that bin (``get_getway_min_temp_in_freq``).
2. For each (calendar date, clock-hour) compute the **min** across all
   gateways — giving one coldest value per hour per day.
3. For each calendar date find the clock-hour whose min is the lowest →
   that value is the day's "coldest hour temperature".
4. For each consecutive (day N−1, day N) pair compute
   ``|coldest_hour(N) − coldest_hour(N−1)|``.
5. ``volatile=True`` when **any** consecutive pair's delta ≥ threshold.
   ``pass_metric = not volatile`` — passes when ambient is stable.

Example (step 2–3 — one day, 24 hours):
    hour 00:00 → min across gateways = 27 °C
    hour 01:00 → min across gateways = 25 °C
    hour 02:00 → min across gateways = 23 °C   ← coldest hour  (value = 23 °C)
    hour 03:00 → min across gateways = 24 °C
    ...

    day 1 coldest = 10 °C
    day 2 coldest =  5 °C   →  delta = 5 °C  ≥ 5  →  volatile=True  → pass_metric=False

    day 1 coldest = 10 °C
    day 2 coldest =  8 °C   →  delta = 2 °C  < 5  →  volatile=False → pass_metric=True

Output (pass_metric)
--------------------
    True  — ambient is stable; all consecutive night deltas < MIN_DAILY_DELTA_CELSIUS
    False — ambient is volatile; at least one night-to-night jump ≥ threshold
    True  — also returned when < MIN_DAYS of data (insufficient data → neutral pass)

Input
-----
``getway_df`` : raw gateway DataFrame with at least:
    timestamp           — datetime (or parseable string), as a column
    gateway_mac_address — str, gateway identifier
    pcb_temperature_two — float, ambient °C (raw readings; resampling done internally)

Threshold
---------
Configured via MIN_DAILY_DELTA_CELSIUS.  Keep this constant in sync with
configs/thresholds.yaml when that file is introduced for this metric family.
"""

from __future__ import annotations

import logging

import pandas as pd
import numpy as np

log = logging.getLogger(__name__)

# ── Thresholds ────────────────────────────────────────────────────────────────
# A night-to-night jump of this size signals a meaningful weather change.
MIN_DAILY_DELTA_CELSIUS: float = 5.0

# Need at least this many distinct calendar days to compare consecutive nights.
MIN_DAYS: int = 2

def get_getway_min_temp_in_freq(gateway_df: pd.DataFrame, freq: str = "1h"):

    if "timestamp" in gateway_df.columns:
        gateway_df["timestamp"] = pd.to_datetime(gateway_df["timestamp"])

    elif not isinstance(gateway_df.index, pd.DatetimeIndex):
        raise ValueError(
            "gateway_hourly must have a 'timestamp' column or a DatetimeIndex"
        )

    if "pcb_temperature_two" not in gateway_df.columns:
        raise ValueError(
            "gateway_hourly must contain a 'pcb_temperature_two' column"
        )

    ambient = gateway_df["pcb_temperature_two"].dropna()
    if ambient.empty:
        log.debug("ambient_temperature_volatility: no valid readings → False")
        return False
   
    gateway_freq = (
        gateway_df
        .groupby(["gateway_mac_address", pd.Grouper(key="timestamp", freq=freq)])["pcb_temperature_two"]
        .min()
        .reset_index()
    )

    return gateway_freq


def ambient_temperature_volatility(
    getway_df: pd.DataFrame,
    min_delta_celsius: float = MIN_DAILY_DELTA_CELSIUS,
    min_days: int = MIN_DAYS,
) -> dict:
    """Return a standardised metric dict for ambient temperature volatility.

    For each day the "coldest hour" is the clock-hour (00–23) whose minimum
    temperature across all gateways is the lowest.

    Parameters
    ----------
    getway_df:
        Raw gateway DataFrame with ``timestamp``, ``gateway_mac_address``, and
        ``pcb_temperature_two`` columns.  Resampling is handled internally.
    min_delta_celsius:
        Minimum absolute difference between consecutive daily coldest-hour
        temperatures to flag the ambient as volatile.  Comparison is inclusive (≥).
    min_days:
        Minimum number of distinct calendar days required to evaluate volatility.
        Fewer days → neutral pass (``pass_metric=True``).

    Returns
    -------
    dict with keys:
        ``metric_name``          — "ambient_temperature_volatility".
        ``pass_metric``          — True when ambient is stable (not volatile).
        ``threshold``            — min_delta_celsius.
        ``value``                — max delta observed across all consecutive pairs (°C),
                                   or None when insufficient data.
        ``days_period``          — 2.
        ``metric_decision_data`` — {"volatile", "min_point1", "min_point2"}
                                   where min_point1/2 are {"date", "temp", "hour"}
                                   for the most volatile consecutive pair.
    """

    _METRIC_NAME = "ambient_temperature_volatility"
    _DAYS_PERIOD = 2

    def _empty(error: str) -> dict:
        return {
            "metric_name":          _METRIC_NAME,
            "pass_metric":          True,   # no data to trigger a violation → neutral pass
            "threshold":            min_delta_celsius,
            "value":                None,
            "days_period":          _DAYS_PERIOD,
            "metric_decision_data": {
                "volatile":   False,
                "min_point1": None,
                "min_point2": None,
                "error":      error,
            },
        }

    ambient_resampled = get_getway_min_temp_in_freq(getway_df)
    if ambient_resampled is False:
        return _empty("no valid gateway readings")

    # keep only the temperature series — gateway_mac_address must not leak into min/groupby
    ambient = ambient_resampled.set_index("timestamp")["pcb_temperature_two"]

    # Step 1: min temperature per (date, clock-hour) across all gateways
    freq_min = ambient.groupby(
        [ambient.index.date, ambient.index.hour]
    ).min()

    # Step 2: for each date, coldest temp and which hour it occurred
    daily_coldest      = freq_min.groupby(level=0).min()
    daily_coldest_hour = freq_min.groupby(level=0).idxmin()   # → (date, hour) tuple per date

    if len(daily_coldest) < min_days:
        log.debug(
            "ambient_temperature_volatility: only %d day(s) of data (need %d) → False",
            len(daily_coldest), min_days,
        )
        return _empty(f"only {len(daily_coldest)} day(s) of data (need {min_days})")

    # Find the consecutive pair with the largest delta
    troughs   = daily_coldest.sort_index()
    best_pair = None
    max_delta = -1.0
    volatile  = False

    for (d_prev, t_prev), (d_curr, t_curr) in zip(
        troughs.items(), list(troughs.items())[1:]
    ):
        delta  = abs(t_curr - t_prev)
        h_prev = daily_coldest_hour[d_prev][1]
        h_curr = daily_coldest_hour[d_curr][1]
        log.debug(
            "ambient_temperature_volatility: %s coldest=%.2f°C (hour %02d)  "
            "%s coldest=%.2f°C (hour %02d)  Δ=%.2f°C",
            d_prev, t_prev, h_prev, d_curr, t_curr, h_curr, delta,
        )
        if delta > max_delta:
            max_delta = delta
            best_pair = (d_prev, t_prev, h_prev, d_curr, t_curr, h_curr)
        if delta >= min_delta_celsius:
            if not volatile:
                log.debug(
                    "ambient_temperature_volatility: Δ=%.2f°C ≥ %.1f°C threshold → volatile=True",
                    delta, min_delta_celsius,
                )
            volatile = True

    if best_pair is None:
        return _empty("could not form consecutive day pairs")

    d_prev, t_prev, h_prev, d_curr, t_curr, h_curr = best_pair
    if not volatile:
        log.debug(
            "ambient_temperature_volatility: daily_coldest=%s  threshold=%.1f°C → stable",
            troughs.round(2).to_dict(), min_delta_celsius,
        )

    return {
        "metric_name":          _METRIC_NAME,
        "pass_metric":          not volatile,   # passes when ambient is stable (not volatile)
        "threshold":            min_delta_celsius,
        "value":                round(max_delta, 2),
        "days_period":          _DAYS_PERIOD,
        "metric_decision_data": {
            "volatile":   volatile,
            "min_point1": {"date": d_prev, "temp": round(t_prev, 2), "hour": h_prev},
            "min_point2": {"date": d_curr, "temp": round(t_curr, 2), "hour": h_curr},
        },
    }