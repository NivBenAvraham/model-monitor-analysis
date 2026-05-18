"""
Large Bucket Sensor Divergence — Temperature family metric (R8).

Detects days where sensors within the **large** hive-size bucket diverge
from each other — a signature of "messy temperature drops" where some hives
in the group lose their cluster warmth while others hold.

Physical motivation
-------------------
In a healthy group the large-bucket sensors all read ~34 °C because the bees
are actively thermoregulating.  When a calibration problem, swarm event, or
removal causes some hives to lose heat, those sensors plunge intra-day to
20–25 °C while healthy neighbours hold.  The result is a sudden spike in the
**standard deviation of per-sensor daily means** within the large bucket.

This pattern was identified from analyst-labelled "temperature drop" events:

  group 935 (2026-02-07) — INVALID "messy temperature drops"
    67/783 large sensors dropped >1.5 °C from prior baseline; per-sensor std
    within the large bucket spiked from ~0.5 °C to >5 °C on the drop day.

  group 625 (2026-02-12) — INVALID "temp drops"
    45/735 large sensors dropped; same spike pattern.

  group 1730 (2026-03-11) — VALID "temp drop"
    0/230 large sensors dropped — ambient cold event, large cluster held.

Difference from existing metrics
---------------------------------
R4 (sensor_spread_within_bucket): std of per-sensor means over the FULL
  2-day window.  A drop event that lasts only a few hours on one day dilutes
  to <1 °C across 48 h and passes R4.

R7 (bucket_diurnal_amplitude): max−min across all sensor-hour readings in
  the bucket per day.  Catches single-hive swings, but is dominated by the
  extreme values — it does not measure *how many* sensors diverge.

R8 (this metric): per-day std of per-sensor daily means, then max across
  days.  Captures "several sensors simultaneously colder than the rest"
  — orthogonal to R4 and R7.

Algorithm
---------
Primary check — large bucket:
  1. Resample raw sensor readings to 1-hour means (handled by util).
  2. Filter to large bucket.
  3. For each calendar day: compute the mean temperature per sensor (average
     of all hourly readings for that sensor on that day).
  4. For each day: compute std across sensors — the within-bucket daily spread.
  5. max_daily_spread = max of those daily stds across all days in the window.
  6. pass_metric = False when max_daily_spread > LARGE_DAILY_SPREAD_MAX.

Secondary check — medium bucket (informational):
  Same steps applied to the medium bucket.  Medium's threshold is loose and
  does NOT affect pass_metric — it is included in the value dict for
  visibility and future calibration.

Thresholds (configs/thresholds.yaml → metrics.temperature.large_bucket_sensor_divergence)
------------------------------------------------------------------------------------------
Calibrated against:
  - perfect-valid anchor max_daily_spread ≈ 0.5–1.5 °C (large)
  - clear-INVALID drop events: 4–8 °C (large)
  Clean gap → threshold at 3.0 °C for large.
  Medium: loose at 6.0 °C (informational only).

Family
------
METRIC_FAMILY = "temperature"

Input
-----
sensor_df : Raw sensor DataFrame with columns:
            ``sensor_mac_address``, ``hive_size_bucket``,
            ``timestamp`` (datetime-parseable), ``pcb_temperature_one`` (°C).
            Resampling to hourly means is handled internally.

Output
------
dict
    metric_name          : str   — "large_bucket_sensor_divergence".
    pass_metric          : bool  — False when large max_daily_spread > threshold.
    threshold            : dict  — {"large": LARGE_DAILY_SPREAD_MAX, "medium": MEDIUM_DAILY_SPREAD_MAX}.
    value                : dict  — {"large": max_daily_spread, "medium": max_daily_spread}.
    days_period          : int   — 2.
    metric_decision_data : dict  — {"bucket_verdicts": {"large": bool}, "daily_spreads": {...}}.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import yaml

from model_monitor.utils.data_utils import resample_sensor_to_hourly

log = logging.getLogger(__name__)

METRIC_FAMILY: str = "temperature"
_METRIC_NAME:  str = "large_bucket_sensor_divergence"
_DAYS_PERIOD:  int = 2


def _load_thresholds() -> dict:
    path = Path(__file__).resolve().parents[4] / "configs/thresholds.yaml"
    with open(path) as f:
        return yaml.safe_load(f)["metrics"]["temperature"]["large_bucket_sensor_divergence"]


_cfg = _load_thresholds()
LARGE_DAILY_SPREAD_MAX:  float = float(_cfg["large"])
MEDIUM_DAILY_SPREAD_MAX: float = float(_cfg["medium"])

THRESHOLDS: dict[str, float] = {
    "large":  LARGE_DAILY_SPREAD_MAX,
    "medium": MEDIUM_DAILY_SPREAD_MAX,
}


def large_bucket_sensor_divergence(sensor_df: pd.DataFrame) -> dict:
    """Return a standardised metric dict for large-bucket sensor divergence.

    Parameters
    ----------
    sensor_df:
        Raw sensor DataFrame with ``sensor_mac_address``, ``hive_size_bucket``,
        ``timestamp``, and ``pcb_temperature_one`` columns.
        Resampling to hourly means is handled internally.

    Returns
    -------
    dict with keys:
        ``metric_name``          — "large_bucket_sensor_divergence".
        ``pass_metric``          — False when large max daily sensor spread > threshold.
        ``threshold``            — per-bucket caps (°C).
        ``value``                — max daily sensor spread per bucket (°C).
        ``days_period``          — 2.
        ``metric_decision_data`` — daily spread details per bucket.
    """
    def _result(pass_metric: bool, value: dict, bucket_verdicts: dict,
                daily_spreads: dict, error: str | None = None) -> dict:
        return {
            "metric_name":          _METRIC_NAME,
            "pass_metric":          pass_metric,
            "threshold":            THRESHOLDS,
            "value":                value,
            "days_period":          _DAYS_PERIOD,
            "metric_decision_data": {
                "bucket_verdicts": bucket_verdicts,
                "daily_spreads":   daily_spreads,
                **({"error": error} if error else {}),
            },
        }

    try:
        sensor_hourly = resample_sensor_to_hourly(sensor_df)
    except ValueError as exc:
        log.warning("large_bucket_sensor_divergence: invalid input — %s", exc)
        return _result(False, {}, {}, {}, error=str(exc))

    if sensor_hourly.empty:
        log.debug("large_bucket_sensor_divergence: no sensor readings → pass_metric=False")
        return _result(False, {}, {}, {}, error="no data")

    df = sensor_hourly.copy()
    df["_date"] = pd.to_datetime(df["timestamp"]).dt.date

    max_spreads:     dict[str, float] = {}
    bucket_verdicts: dict[str, bool]  = {}
    daily_spreads:   dict[str, dict]  = {}

    for bucket in ("large", "medium"):
        cap     = THRESHOLDS[bucket]
        group   = df[df["hive_size_bucket"] == bucket]
        if group.empty:
            continue

        # Per-sensor daily mean: average hourly readings for each sensor on each day
        sensor_daily_means = (
            group.groupby(["_date", "sensor_mac_address"])["pcb_temperature_one"]
            .mean()
            .reset_index()
        )

        # Per-day: std of per-sensor daily means (how spread out are sensors on this day)
        daily_std_per_day = (
            sensor_daily_means.groupby("_date")["pcb_temperature_one"]
            .std(ddof=1)
            .dropna()
        )

        if daily_std_per_day.empty:
            continue

        max_spread = float(daily_std_per_day.max())
        max_spreads[bucket]    = round(max_spread, 4)
        daily_spreads[bucket]  = {
            str(d): round(float(v), 4)
            for d, v in daily_std_per_day.items()
        }

        if bucket == "large":
            bucket_verdicts[bucket] = max_spread <= cap
            log.debug(
                "large_bucket_sensor_divergence: large max_daily_spread=%.2f°C %s %.2f°C",
                max_spread, "≤" if bucket_verdicts[bucket] else ">", cap,
            )
        else:
            # medium: computed for visibility but does NOT affect pass_metric
            bucket_verdicts[bucket] = max_spread <= cap
            log.debug(
                "large_bucket_sensor_divergence: medium max_daily_spread=%.2f°C (informational)",
                max_spread,
            )

    # Only large drives pass_metric
    if "large" not in bucket_verdicts:
        # No large-bucket sensors → cannot evaluate → pass by default
        log.debug("large_bucket_sensor_divergence: no large bucket data → pass_metric=True")
        pass_metric = True
    else:
        pass_metric = bucket_verdicts["large"]

    log.debug("large_bucket_sensor_divergence: pass_metric=%s", pass_metric)
    return _result(pass_metric, max_spreads, bucket_verdicts, daily_spreads)
