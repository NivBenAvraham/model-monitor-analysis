"""
Sensor Group Segment — Phase 1 (Feature Engineering) + Phase 2 (Grading).

Layer 1 of the temperature-based model monitoring pipeline.
Operates at the individual sensor level — each sensor is a data point.

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
  skills/sensor_group_segment/config/thresholds.yaml → metrics.sensor_group_segment.grading

Threshold values are calibrated from sensor physics distributions per hive size.
See: skills/sensor_group_segment/scripts/calibrate_thresholds.py

Output of grade(): same DataFrame as compute() plus two columns:
  status  — PASS | WARNING | FAIL
  reason  — human-readable explanation of which rule fired

The per-sensor status output is consumed by Layer 2
(group_model_temperature_health) which aggregates across sensors
to decide VALID / INVALID per (group_id, date).
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
# Public API — call these from skills/sensor_group_segment/scripts/run.py
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
    thresholds : dict loaded from skills/sensor_group_segment/config/thresholds.yaml
                 → metrics.sensor_group_segment.grading

    Returns
    -------
    Same DataFrame with two new columns: status, reason
    """
    large  = thresholds["large"]
    medium = thresholds["medium"]
    small  = thresholds["small"]

    size = df["hive_size_bucket"].str.lower()
    corr = df["ambient_correlation"]

    # ── boolean masks (vectorized) ──────────────────────────────────────────
    is_large  = size == "large"
    is_medium = size == "medium"
    is_small  = size == "small"

    # Rule A sub-conditions
    cond_std     = is_large & (df["std_dev"] > large["std_dev_max"])
    cond_corr    = is_large & corr.notna() & (corr > large["corr_max"])
    cond_comfort = is_large & (df["percent_comfort"] < large["comfort_min"])
    rule_a = cond_std | cond_corr | cond_comfort

    rule_b = is_medium & (df["mean_temp"] < medium["mean_temp_min"])
    rule_c = is_small  & (df["std_dev"]   < small["std_dev_min"])

    # ── status (vectorized) ─────────────────────────────────────────────────
    status = np.select(
        [rule_a, rule_b, rule_c],
        ["FAIL", "WARNING", "WARNING"],
        default="PASS",
    )

    # ── reason strings ──────────────────────────────────────────────────────
    # Rule A: join whichever sub-conditions fired into one string
    std_part     = np.where(cond_std,
                            "std_dev=" + df["std_dev"].round(2).astype(str)
                            + " > " + str(large["std_dev_max"]), "")
    corr_part    = np.where(cond_corr,
                            "corr=" + corr.round(2).astype(str)
                            + " > " + str(large["corr_max"]), "")
    comfort_part = np.where(cond_comfort,
                            "comfort=" + df["percent_comfort"].round(1).astype(str)
                            + "% < " + str(large["comfort_min"]) + "%", "")

    fail_detail = (
        pd.DataFrame({"a": std_part, "b": corr_part, "c": comfort_part})
        .apply(lambda r: "; ".join(v for v in r if v), axis=1)
    )
    reason_a = "Large hive physics mismatch: " + fail_detail

    reason_b = (
        "Too cold for medium hive: mean_temp="
        + df["mean_temp"].round(1).astype(str)
        + "°C < " + str(medium["mean_temp_min"]) + "°C"
    )
    reason_c = (
        "Unexpected stability for small hive: std_dev="
        + df["std_dev"].round(2).astype(str)
        + " < " + str(small["std_dev_min"])
    )

    reason = np.select(
        [rule_a, rule_b, rule_c],
        [reason_a.values, reason_b.values, reason_c.values],
        default="Physics align with prediction",
    )

    result = df.copy()
    result["status"] = status
    result["reason"] = reason
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
