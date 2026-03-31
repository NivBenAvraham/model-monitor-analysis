"""
Temperature Metric — Phase 1 (Feature Engineering) + Phase 2 (Grading).

════════════════════════════════════════════════════════════════════
PHASE 1 — compute()
════════════════════════════════════════════════════════════════════
Translates raw temperature readings into three biological signatures
per sensor per day. The idea: a strong hive thermally regulates
itself regardless of outside weather; a weak hive can't.

  Metric 1 — Stability (std_dev, iqr)
    How much does the internal temperature fluctuate?
    Large hive → tight, low variance.  Small hive → noisy, high variance.

  Metric 2 — Decoupling (ambient_correlation)
    Does the hive temperature follow the outside weather curve?
    Large hive → r ≈ 0 (independent).  Small hive → r → 1 (slave to ambient).

  Metric 3 — Comfort Zone (mean_temp, percent_comfort)
    How often is the hive in the brood-rearing band [32°C, 36°C]?
    Large hive → ≈ 100%.  Small hive → rarely hits this range.

Data flow:
  sensor_temperature parquet  →  pcb_temperature_one  (internal temp)
                                 hive_size_bucket      (model prediction)
                                 gateway_mac_address   (join key to gateway)
  gateway_temperature parquet →  pcb_temperature_two  (ambient temp)

  Both files are resampled to 1-hour means and inner-joined on
  (gateway_mac_address, hour) before computing correlations,
  because sensor and gateway timestamps do not align exactly.

Output of compute(): one row per (group_id, date, sensor_mac_address) with:
  std_dev, iqr, ambient_correlation, mean_temp, percent_comfort, n_readings

════════════════════════════════════════════════════════════════════
PHASE 2 — grade()
════════════════════════════════════════════════════════════════════
Compares what the model predicted (hive_size_bucket) against what
the temperature physics actually observed. Flags mismatches.

Rules applied in priority order (first match wins):

  Rule A — FAIL    : predicted=large  but physics look weak
                     (too volatile, too correlated with ambient, or out of comfort zone)
  Rule B — WARNING : predicted=medium but hive is running too cold
  Rule C — WARNING : predicted=small  but hive is suspiciously stable
                     (could be a sensor error)
  Default — PASS   : physics align with the prediction

Thresholds are never hardcoded here — they live in:
  configs/thresholds.yaml → metrics.temp_metric.grading

Threshold values were derived from Phase 1 percentile distributions
on groups 491, 518, 625, 790 (Feb–Mar 2026) and should be revisited
as more ground-truth data becomes available.

Output of grade(): same DataFrame as compute() plus two columns:
  status  — PASS | WARNING | FAIL
  reason  — human-readable explanation of which rule fired
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
from scipy import stats

log = logging.getLogger(__name__)

COMFORT_LOW: float = 32.0   # lower bound of brood-rearing temperature band
COMFORT_HIGH: float = 36.0  # upper bound of brood-rearing temperature band
RESAMPLE_FREQ: str = "1h"   # bin size for aligning sensor vs gateway timestamps
MIN_READINGS: int = 2        # drop a sensor if fewer than this many aligned hours exist


# ---------------------------------------------------------------------------
# Phase 1 helpers — data alignment
#
# Sensor and gateway readings land at different timestamps, so we can't join
# them row-by-row. Instead we resample both to hourly means and then join on
# (gateway_mac_address, hour). This gives us aligned (internal, ambient) pairs
# for every sensor for every hour in the window.
# ---------------------------------------------------------------------------

def _resample_sensor(sensor_df: pd.DataFrame, freq: str) -> pd.DataFrame:
    """Hourly mean of internal temp per (sensor_mac_address, gateway_mac_address)."""
    gw_map = (
        sensor_df[["sensor_mac_address", "gateway_mac_address", "hive_size_bucket", "group_id"]]
        .drop_duplicates("sensor_mac_address", keep="last")
    )
    hourly = (
        sensor_df
        .groupby(["sensor_mac_address", pd.Grouper(key="timestamp", freq=freq)])["pcb_temperature_one"]
        .mean()
        .reset_index()
        .rename(columns={"pcb_temperature_one": "internal_temp"})
    )
    return hourly.merge(gw_map, on="sensor_mac_address", how="left")


def _resample_gateway(gateway_df: pd.DataFrame, freq: str) -> pd.DataFrame:
    """Hourly mean of ambient temp per gateway_mac_address."""
    return (
        gateway_df
        .groupby(["gateway_mac_address", pd.Grouper(key="timestamp", freq=freq)])["pcb_temperature_two"]
        .mean()
        .reset_index()
        .rename(columns={"pcb_temperature_two": "ambient_temp"})
    )


def _align(sensor_df: pd.DataFrame, gateway_df: pd.DataFrame, freq: str) -> pd.DataFrame:
    """
    Resample both to `freq` and inner-join on (gateway_mac_address, timestamp).
    Returns one row per (sensor_mac_address, hour) with both internal and ambient temp.
    """
    sensor_h  = _resample_sensor(sensor_df, freq)
    gateway_h = _resample_gateway(gateway_df, freq)
    return sensor_h.merge(gateway_h, on=["gateway_mac_address", "timestamp"], how="inner")


# ---------------------------------------------------------------------------
# Phase 1 helpers — per-sensor metric computations
#
# Each function receives a pandas Series of hourly values for one sensor
# and returns a dict that gets merged into the result row.
# ---------------------------------------------------------------------------

def _stability(internal: pd.Series) -> dict[str, float]:
    """Metric 1 — how tightly does the hive hold its temperature?
    std_dev: sensitive to all fluctuations.
    iqr: robust version — ignores extreme outlier hours."""
    return {
        "std_dev": float(internal.std()),
        "iqr":     float(internal.quantile(0.75) - internal.quantile(0.25)),
    }


def _decoupling(internal: pd.Series, ambient: pd.Series) -> dict[str, float]:
    """Metric 2 — does internal temp move with outside weather?
    r ≈ 0 → hive is thermally independent (strong).
    r → 1 → hive follows ambient curve (weak).
    Returns NaN if the ambient series is constant (sensor/gateway error)."""
    if len(internal) < MIN_READINGS:
        return {"ambient_correlation": float("nan")}
    with np.errstate(invalid="ignore"):
        r, _ = stats.pearsonr(internal, ambient)
    return {"ambient_correlation": float(r)}


def _comfort_zone(internal: pd.Series) -> dict[str, float]:
    """Metric 3 — how often is the hive in the brood-rearing band?
    mean_temp: overall average internal temperature.
    percent_comfort: % of hours in [32°C, 36°C]. Large hives → ≈ 100%."""
    in_zone = (internal >= COMFORT_LOW) & (internal <= COMFORT_HIGH)
    return {
        "mean_temp":       float(internal.mean()),
        "percent_comfort": float(in_zone.mean() * 100),
    }


# ---------------------------------------------------------------------------
# Public API — call these from scripts/run_temp_metric.py
#
#   Step 1:  result = compute(sensor_df, gateway_df, date)   ← Phase 1
#   Step 2:  result = grade(result, thresholds)              ← Phase 2
# ---------------------------------------------------------------------------

def grade(df: pd.DataFrame, thresholds: dict) -> pd.DataFrame:
    """
    Phase 2 — compare predicted hive size against observed temperature physics.

    Applies three rules in priority order (first match wins):
      A  FAIL    — large hive with unstable / ambient-coupled / cold physics
      B  WARNING — medium hive too cold
      C  WARNING — small hive suspiciously stable
      D  PASS    — physics align with prediction

    Parameters
    ----------
    df         : output of compute() — one row per (date, sensor_mac_address)
    thresholds : dict loaded from configs/thresholds.yaml → metrics.temp_metric.grading

    Returns
    -------
    Same DataFrame with two new columns: status, reason
    """
    t = thresholds
    large  = t["large"]
    medium = t["medium"]
    small  = t["small"]

    statuses: list[str] = []
    reasons:  list[str] = []

    for _, row in df.iterrows():
        size    = str(row["hive_size_bucket"]).lower()
        std_dev = row["std_dev"]
        corr    = row["ambient_correlation"]
        comfort = row["percent_comfort"]
        mean_t  = row["mean_temp"]

        if size == "large":
            fired = []
            if std_dev > large["std_dev_max"]:
                fired.append(f"std_dev={std_dev:.2f} > {large['std_dev_max']}")
            if not np.isnan(corr) and corr > large["corr_max"]:
                fired.append(f"corr={corr:.2f} > {large['corr_max']}")
            if comfort < large["comfort_min"]:
                fired.append(f"comfort={comfort:.1f}% < {large['comfort_min']}%")
            if fired:
                statuses.append("FAIL")
                reasons.append("Large hive physics mismatch: " + "; ".join(fired))
                continue

        elif size == "medium":
            if mean_t < medium["mean_temp_min"]:
                statuses.append("WARNING")
                reasons.append(f"Too cold for medium hive: mean_temp={mean_t:.1f}°C < {medium['mean_temp_min']}°C")
                continue

        elif size == "small":
            if std_dev < small["std_dev_min"]:
                statuses.append("WARNING")
                reasons.append(f"Unexpected stability for small hive: std_dev={std_dev:.2f} < {small['std_dev_min']}")
                continue

        statuses.append("PASS")
        reasons.append("Physics align with prediction")

    result = df.copy()
    result["status"] = statuses
    result["reason"] = reasons
    return result


def compute(sensor_df: pd.DataFrame, gateway_df: pd.DataFrame, date: str) -> pd.DataFrame:
    """
    Phase 1 feature engineering — one row per (date, sensor_mac_address).

    Parameters
    ----------
    sensor_df  : sensor_temperature parquet for one (group, date)
    gateway_df : gateway_temperature parquet for one (group, date)
    date       : ISO date string (YYYY-MM-DD) used to tag the output rows

    Returns
    -------
    DataFrame with columns:
        group_id, date, sensor_mac_address, hive_size_bucket,
        std_dev, iqr, ambient_correlation,
        mean_temp, percent_comfort, n_readings
    """
    aligned = _align(sensor_df, gateway_df, RESAMPLE_FREQ)

    meta = (
        sensor_df[["sensor_mac_address", "hive_size_bucket", "group_id"]]
        .drop_duplicates("sensor_mac_address", keep="last")
    )

    records: list[dict] = []

    for sensor_mac, grp in aligned.groupby("sensor_mac_address"):
        both = grp[["internal_temp", "ambient_temp"]].dropna()
        if len(both) < MIN_READINGS:
            log.debug(f"  skip {sensor_mac}: only {len(both)} aligned hours")
            continue

        row: dict = {"sensor_mac_address": sensor_mac, "n_readings": len(both)}
        row.update(_stability(both["internal_temp"]))
        row.update(_decoupling(both["internal_temp"], both["ambient_temp"]))
        row.update(_comfort_zone(both["internal_temp"]))
        records.append(row)

    if not records:
        return pd.DataFrame()

    result = pd.DataFrame(records).merge(meta, on="sensor_mac_address", how="left")
    result["date"] = date

    ordered_cols = [
        "group_id", "date", "sensor_mac_address", "hive_size_bucket",
        "std_dev", "iqr", "ambient_correlation",
        "mean_temp", "percent_comfort", "n_readings",
    ]
    return result[ordered_cols].sort_values(["group_id", "date", "sensor_mac_address"]).reset_index(drop=True)
