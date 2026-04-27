"""
Bucket Diurnal Amplitude — Temperature family metric (R7).

Checks the **within-day swing** of each bucket's mean temperature, catching
hives whose model-predicted size is wrong because they oscillate too much
across each 24-hour cycle.

Physical motivation
-------------------
A genuinely large hive thermoregulates — its internal temperature stays close
to ~34 °C all day, regardless of the outside ambient swing.  A "large" hive
whose temperature swings 20–35 °C every day is either:
  • Too small to actually thermoregulate (model size prediction wrong), or
  • Has a sensor mounted outside the cluster.

Either way, the prediction for that group is unreliable.

Why R5 (bucket_temporal_stability) does NOT catch this
------------------------------------------------------
R5 computes a daily mean per bucket then takes the std across daily means.
A hive that swings 25 °C every day in a perfectly repeating cycle has a
**near-zero** day-to-day std — R5 happily passes.  This metric measures the
within-day amplitude itself.

Algorithm
---------
1. Resample raw sensor readings to 1-hour means per (bucket, sensor, hour).
2. For each bucket present in the data:
   a. For each calendar day, compute (max − min) across **all sensor-hour readings**
      in that bucket on that day.  (Sensor-level swing, not bucket-mean swing —
      this is more sensitive to "rogue sensor" patterns and matches the
      anchor-set discriminator validated in the per-bucket investigation.)
   b. Take the mean of those daily amplitudes.
   c. If amp > BUCKET_DIURNAL_MAX[bucket] → that bucket fails.
3. The overall result pass_metric=True only when *every present* bucket passes.

Thresholds (configs/thresholds.yaml → metrics.temperature.bucket_diurnal_amplitude)
-----------------------------------------------------------------------------------
Calibrated 2026-04-27 against analyst-labelled perfect anchors.

  large:   14.0 °C    (perfect-valid max 10.55, perfect-invalid min 17.37)
  medium:  25.0 °C    (loose — broader-train valids regularly exceed perfect-valid range)
  small:   40.0 °C    (loose — small hives naturally track ambient widely)

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
    metric_name          : str  — "bucket_diurnal_amplitude".
    pass_metric          : bool — True = every present bucket's daily swing is acceptable.
    threshold            : dict — BUCKET_DIURNAL_MAX per-bucket caps (°C).
    value                : dict — {bucket: mean daily (max-min)} for each bucket assessed.
    days_period          : int  — 2.
    metric_decision_data : dict — {"bucket_verdicts": {bucket: True/False}}.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import yaml

from model_monitor.utils.data_utils import resample_sensor_to_hourly

log = logging.getLogger(__name__)

METRIC_FAMILY: str = "temperature"
_METRIC_NAME:  str = "bucket_diurnal_amplitude"
_DAYS_PERIOD:  int = 2


def _load_thresholds() -> dict:
    path = Path(__file__).resolve().parents[4] / "configs/thresholds.yaml"
    with open(path) as f:
        return yaml.safe_load(f)["metrics"]["temperature"]["bucket_diurnal_amplitude"]


_cfg = _load_thresholds()
BUCKET_DIURNAL_MAX: dict[str, float] = {
    "small":  float(_cfg["small"]),
    "medium": float(_cfg["medium"]),
    "large":  float(_cfg["large"]),
}


def bucket_diurnal_amplitude(sensor_df: pd.DataFrame) -> dict:
    """Return a standardised metric dict for bucket diurnal-amplitude check.

    Parameters
    ----------
    sensor_df:
        Raw sensor DataFrame with ``sensor_mac_address``, ``hive_size_bucket``,
        ``timestamp``, and ``pcb_temperature_one`` columns.
        Resampling to hourly means is handled internally.

    Returns
    -------
    dict with keys:
        ``metric_name``          — "bucket_diurnal_amplitude".
        ``pass_metric``          — True when every present bucket's mean daily swing is within its cap.
        ``threshold``            — BUCKET_DIURNAL_MAX per-bucket caps.
        ``value``                — mean of daily (max-min) per bucket.
        ``days_period``          — 2.
        ``metric_decision_data`` — {"bucket_verdicts": {bucket: True/False}}.
    """
    def _result(pass_metric: bool, value: dict, bucket_verdicts: dict,
                error: str | None = None) -> dict:
        return {
            "metric_name":          _METRIC_NAME,
            "pass_metric":          pass_metric,
            "threshold":            BUCKET_DIURNAL_MAX,
            "value":                value,
            "days_period":          _DAYS_PERIOD,
            "metric_decision_data": {
                "bucket_verdicts": bucket_verdicts,
                **({"error": error} if error else {}),
            },
        }

    try:
        sensor_hourly = resample_sensor_to_hourly(sensor_df)
    except ValueError as exc:
        log.warning("bucket_diurnal_amplitude: invalid input — %s", exc)
        return _result(False, {}, {}, error=str(exc))

    if sensor_hourly.empty:
        log.debug("bucket_diurnal_amplitude: no sensor readings → pass_metric=False")
        return _result(False, {}, {}, error="no data")

    df = sensor_hourly.copy()
    df["_date"] = pd.to_datetime(df["timestamp"]).dt.date

    bucket_amps:     dict[str, float] = {}
    bucket_verdicts: dict[str, bool]  = {}

    for bucket, group in df.groupby("hive_size_bucket"):
        cap = BUCKET_DIURNAL_MAX.get(str(bucket))
        if cap is None:
            continue   # unknown bucket — ignore

        # Daily (max - min) across ALL sensor-hour readings in this bucket-day
        per_day = group.groupby("_date")["pcb_temperature_one"].agg(["max", "min"])
        if per_day.empty:
            continue

        amp = float((per_day["max"] - per_day["min"]).mean())
        bucket_amps[bucket]     = round(amp, 4)
        bucket_verdicts[bucket] = amp <= cap

        log.debug(
            "bucket_diurnal_amplitude: bucket=%s daily-amp=%.2f°C %s %.2f°C",
            bucket, amp, "≤" if bucket_verdicts[bucket] else ">", cap,
        )

    pass_metric = all(bucket_verdicts.values()) if bucket_verdicts else True
    log.debug("bucket_diurnal_amplitude: pass_metric=%s", pass_metric)
    return _result(pass_metric, bucket_amps, bucket_verdicts)
