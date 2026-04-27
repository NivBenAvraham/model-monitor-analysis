"""
Bucket Temperature Ordering — Temperature family metric (R6c).

Checks that the mean temperatures across hive-size buckets follow the
expected physical ordering: small < medium < large.

Physical motivation
-------------------
Small hives are passively warm, medium hives generate moderate heat, and
large hives thermoregulate actively to the highest internal temperature.
If the ordering is violated the model's size predictions are almost certainly
wrong or the sensor data is corrupted.

We also require a minimum *gap* between adjacent buckets to ensure the
separation is meaningful and not just noise.

Algorithm
---------
1. Resample raw sensor readings to 1-hour means per (bucket, sensor, hour).
2. Compute the overall mean temperature per bucket.
3. For every adjacent pair of present buckets (small → medium → large), check
   that the gap ≥ MIN_BUCKET_GAP_CELSIUS.  A negative gap is also a violation.
4. Return pass_metric=False if any violation is found.

Thresholds (from 2026-04-15 decide.py)
---------------------------------------
MIN_BUCKET_GAP_CELSIUS: float = 1.5   # °C — minimum gap between adjacent bucket means

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
    metric_name          : str  — "bucket_temperature_ordering".
    pass_metric          : bool — True = ordering holds with sufficient gaps everywhere.
    threshold            : float — MIN_BUCKET_GAP_CELSIUS.
    value                : dict — {"small→medium": gap, "medium→large": gap} (°C).
    days_period          : int  — 2.
    metric_decision_data : dict — {"bucket_means": dict, "violations": list}.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import yaml

from model_monitor.utils.data_utils import resample_sensor_to_hourly

log = logging.getLogger(__name__)

# ── Family metadata ───────────────────────────────────────────────────────────
METRIC_FAMILY: str = "temperature"
_METRIC_NAME:  str = "bucket_temperature_ordering"
_DAYS_PERIOD:  int = 2

# ── Threshold (loaded from configs/thresholds.yaml) ───────────────────────────
def _load_thresholds() -> dict:
    path = Path(__file__).resolve().parents[4] / "configs/thresholds.yaml"
    with open(path) as f:
        return yaml.safe_load(f)["metrics"]["temperature"]["bucket_temperature_ordering"]

_cfg = _load_thresholds()
MIN_BUCKET_GAP_CELSIUS: float = float(_cfg["min_gap_celsius"])

_BUCKET_ORDER = ["small", "medium", "large"]


def bucket_temperature_ordering(sensor_df: pd.DataFrame) -> dict:
    """Return a standardised metric dict for bucket temperature ordering.

    Parameters
    ----------
    sensor_df:
        Raw sensor DataFrame with ``sensor_mac_address``, ``hive_size_bucket``,
        ``timestamp``, and ``pcb_temperature_one`` columns.
        Resampling to hourly means is handled internally.

    Returns
    -------
    dict with keys:
        ``metric_name``          — "bucket_temperature_ordering".
        ``pass_metric``          — True when ordering and gaps hold for all adjacent pairs.
        ``threshold``            — MIN_BUCKET_GAP_CELSIUS.
        ``value``                — temperature gap per adjacent pair present.
        ``days_period``          — 2.
        ``metric_decision_data`` — {"bucket_means": dict, "violations": list}.
    """
    def _result(pass_metric: bool, value: dict, bucket_means: dict,
                violations: list, error: str | None = None) -> dict:
        return {
            "metric_name":          _METRIC_NAME,
            "pass_metric":          pass_metric,
            "threshold":            MIN_BUCKET_GAP_CELSIUS,
            "value":                value,
            "days_period":          _DAYS_PERIOD,
            "metric_decision_data": {
                "bucket_means": bucket_means,
                "violations":   violations,
                **({"error": error} if error else {}),
            },
        }

    try:
        sensor_hourly = resample_sensor_to_hourly(sensor_df)
    except ValueError as exc:
        log.warning("bucket_temperature_ordering: invalid input — %s", exc)
        return _result(False, {}, {}, [str(exc)], error=str(exc))

    if sensor_hourly.empty:
        log.debug("bucket_temperature_ordering: no sensor readings → pass_metric=False")
        return _result(False, {}, {}, ["no sensor data"], error="no data")

    bucket_means: dict[str, float] = {
        str(bucket): round(float(group["pcb_temperature_one"].mean()), 2)
        for bucket, group in sensor_hourly.groupby("hive_size_bucket")
    }

    present    = [b for b in _BUCKET_ORDER if b in bucket_means]
    gaps:       dict[str, float] = {}
    violations: list[str]        = []

    for i in range(len(present) - 1):
        lower  = present[i]
        higher = present[i + 1]
        gap    = bucket_means[higher] - bucket_means[lower]
        key    = f"{lower}→{higher}"
        gaps[key] = round(gap, 2)

        if gap < MIN_BUCKET_GAP_CELSIUS:
            msg = (f"{key}: gap={gap:.2f}°C < {MIN_BUCKET_GAP_CELSIUS}°C "
                   f"({lower}={bucket_means[lower]:.1f}°C, {higher}={bucket_means[higher]:.1f}°C)")
            violations.append(msg)
            log.debug("bucket_temperature_ordering: violation — %s", msg)
        else:
            log.debug("bucket_temperature_ordering: %s gap=%.2f°C ≥ %.1f°C → OK",
                      key, gap, MIN_BUCKET_GAP_CELSIUS)

    pass_metric = len(violations) == 0
    log.debug("bucket_temperature_ordering: pass_metric=%s  violations=%d", pass_metric, len(violations))
    return _result(pass_metric, gaps, bucket_means, violations)
