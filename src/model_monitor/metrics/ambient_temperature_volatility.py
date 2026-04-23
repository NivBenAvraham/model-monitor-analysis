"""
Ambient Temperature Volatility — Layer 1 helper metric.

Detects whether the coldest hour of each day changes significantly between
consecutive days in the observation window.

Physical motivation
-------------------
Ambient (gateway) temperature follows a predictable diurnal curve: warm during
the day, cold overnight.  Each day has one coldest hour (typically 4–6 AM).

When the coldest hour of one day differs from the coldest hour of the next by
more than ``MIN_DAILY_DELTA_CELSIUS``, the weather changed substantially between
those two nights — the hive model is being evaluated under shifting conditions.

Algorithm
---------
1. For each (calendar date, clock hour) pair compute the **min temperature**
   across all gateway readings in that slot.  This handles multiple gateways
   reporting in the same hour — they are averaged, not min-picked.
2. For each calendar date find the clock hour whose MIN is the lowest →
   that min is the day's "coldest hour temperature".
3. For each consecutive (day N−1, day N) pair compute
   ``|coldest_hour(N) − coldest_hour(N−1)|``.
4. Return ``True`` when **any** consecutive pair's delta ≥ threshold.

Example (step 1 — one day, 24 hours):
    hour 00:00 → mean = 27 °C
    hour 01:00 → mean = 25 °C
    hour 02:00 → mean = 23 °C   ← coldest hour  (value = 23 °C)
    hour 03:00 → mean = 24 °C
    ...

    day 1 coldest hour = 10 °C
    day 2 coldest hour =  5 °C   →  delta = 5 °C  ≥ 5  →  True

    day 1 coldest hour = 10 °C
    day 2 coldest hour =  8 °C   →  delta = 2 °C  < 5  →  False

Output
------
    True  — a consecutive night-to-night jump ≥ MIN_DAILY_DELTA_CELSIUS detected
    False — all consecutive pairs are stable, or fewer than MIN_DAYS present

Input
-----
``gateway_hourly`` must be a DataFrame with at least:
    timestamp           — datetime (or parseable string), index or column
    pcb_temperature_two — float, ambient °C (already resampled to hourly means)

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
    """Return a dict with the volatility result and the two coldest-hour points
    from the most volatile consecutive day pair.

    For each day the "coldest freq_min" is the clock-hour (00–23) whose minimum
    temperature across all gateway readings in that slot is lowest.

    Parameters
    ----------
    gateway_hourly:
        DataFrame with a ``pcb_temperature_two`` column (ambient °C) and a
        ``timestamp`` column (or DatetimeIndex).
    min_delta_celsius:
        Minimum difference between consecutive daily coldest freq to flag as
        volatile.  Comparison is inclusive (≥).
    min_days:
        Minimum number of distinct calendar days required.

    Returns
    -------
    dict with keys:
        ``volatile``   — True if any consecutive pair exceeded the threshold.
        ``min_point1`` — {"date", "temp", "hour"} for the first day of the peak pair.
        ``min_point2`` — {"date", "temp", "hour"} for the second day of the peak pair.
        ``delta``      — absolute temperature difference between the two points (°C).
    """

    ambient = get_getway_min_temp_in_freq(getway_df)
    # keep only the temperature series — gateway_mac_address must not leak into min/groupby
    ambient = ambient.set_index("timestamp")["pcb_temperature_two"]

    # Step 1: min temperature per (date, clock-hour) across all gateways
    freq_min = ambient.groupby(
        [ambient.index.date, ambient.index.hour]
    ).min()

    # Step 2: for each date, coldest temp and which hour it occurred
    daily_coldest = freq_min.groupby(level=0).min()
    daily_coldest_hour = freq_min.groupby(level=0).idxmin()  # → (date, hour) tuple per date

    if len(daily_coldest) < min_days:
        log.debug(
            "ambient_temperature_volatility: only %d day(s) of data (need %d) → False",
            len(daily_coldest),
            min_days,
        )
        return {"volatile": False, "min_point1": None, "min_point2": None, "delta": None}

    # Find the consecutive pair with the largest delta
    troughs = daily_coldest.sort_index()
    best_pair = None
    max_delta = -1.0
    volatile = False

    for (d_prev, t_prev), (d_curr, t_curr) in zip(
        troughs.items(), list(troughs.items())[1:]
    ):
        delta = abs(t_curr - t_prev)
        h_prev = daily_coldest_hour[d_prev][1]
        h_curr = daily_coldest_hour[d_curr][1]
        log.debug(
            "ambient_temperature_volatility: %s coldest=%.2f°C (hour %02d)  %s coldest=%.2f°C (hour %02d)  Δ=%.2f°C",
            d_prev, t_prev, h_prev, d_curr, t_curr, h_curr, delta,
        )
        if delta > max_delta:
            max_delta = delta
            best_pair = (d_prev, t_prev, h_prev, d_curr, t_curr, h_curr)
        if delta >= min_delta_celsius:
            if not volatile:
                log.debug(
                    "ambient_temperature_volatility: Δ=%.2f°C ≥ %.1f°C threshold → True",
                    delta, min_delta_celsius,
                )
            volatile = True

    if best_pair is None:
        return {"volatile": False, "min_point1": None, "min_point2": None, "delta": None}

    d_prev, t_prev, h_prev, d_curr, t_curr, h_curr = best_pair
    if not volatile:
        log.debug(
            "ambient_temperature_volatility: daily_coldest=%s  threshold=%.1f°C → False",
            troughs.round(2).to_dict(),
            min_delta_celsius,
        )

    return {
        "volatile": volatile,
        "min_point1": {"date": d_prev, "temp": round(t_prev, 2), "hour": h_prev},
        "min_point2": {"date": d_curr, "temp": round(t_curr, 2), "hour": h_curr},
        "delta": round(max_delta, 2),
    }