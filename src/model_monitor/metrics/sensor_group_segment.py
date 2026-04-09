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

  Metric 2 — Decoupling (ambient_corr_median, ambient_corr_mean)
    Does the hive temperature follow the outside weather curve?
    Large hive → r ≈ 0 (independent).  Small hive → r → 1 (slave to ambient).
    Ambient reference = per-sensor local composite (median / mean across only
    the gateways that sensor communicated through) — handles groups that span
    multiple yards without mixing distant gateways' weather.

  Metric 3 — Comfort Zone (mean_temp, percent_comfort)
    How often is the hive in the brood-rearing band [32°C, 36°C]?
    Large hive → ≈ 100%.  Small hive → rarely hits this range.

Data flow:
  sensor_temperature parquet  →  pcb_temperature_one  (internal temp)
                                 hive_size_bucket      (model prediction)
                                 gateway_mac_address   (join key to gateway)
  gateway_temperature parquet →  pcb_temperature_two  (ambient temp)

  Both files are resampled to 1-hour means. For each sensor, ambient temp
  is aggregated (median + mean) from only the gateways that sensor actually
  communicated through — its local neighborhood, not the whole group.

Output of compute(): one row per (group_id, date, sensor_mac_address) with:
  gateway_mac_address (primary, metadata only),
  std_dev, iqr, ambient_corr_median, ambient_corr_mean,
  min_temp, mean_temp, max_temp, median_temp, percent_comfort, n_readings

════════════════════════════════════════════════════════════════════
PHASE 2 — grade()
════════════════════════════════════════════════════════════════════
Compares what the model predicted (hive_size_bucket) against what
the temperature physics actually observed. Flags mismatches.

Rules applied in priority order (first match wins):

  Rule A — FAIL    : predicted=large  but physics look weak
                     (too volatile [std_dev or iqr], too correlated with ambient, or out of comfort zone)
  Rule B — WARNING : predicted=medium but hive is too cold or never reaches brood zone
  Rule C — WARNING : predicted=small  but hive is suspiciously stable [std_dev or iqr]
                     or running too warm — could be a sensor error or under-counted hive
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
# them row-by-row. Both are resampled to hourly means.
#
# Ambient reference is built per sensor — using only the gateways that sensor
# actually communicated through (its "local" gateways). This avoids mixing
# ambient readings from distant gateways in groups that span multiple yards.
#   - 1 gateway  → that gateway's ambient (no aggregation needed)
#   - N gateways → median + mean across those N per hour
# ---------------------------------------------------------------------------

def _resample_sensor(sensor_df: pd.DataFrame, freq: str) -> pd.DataFrame:
    """Hourly mean of internal temp per sensor_mac_address.

    Also resolves the primary gateway (highest raw count) as metadata, and
    extracts the full set of gateways each sensor communicated through.
    """
    primary_gw = (
        sensor_df.groupby(["sensor_mac_address", "gateway_mac_address"])
        .size()
        .reset_index(name="_cnt")
        .sort_values("_cnt", ascending=False)
        .drop_duplicates("sensor_mac_address", keep="first")
        .drop(columns="_cnt")
    )
    gw_map = (
        sensor_df[["sensor_mac_address", "gateway_mac_address", "hive_size_bucket", "group_id"]]
        .merge(primary_gw, on=["sensor_mac_address", "gateway_mac_address"], how="inner")
        .drop_duplicates("sensor_mac_address", keep="first")
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


def _sensor_gateway_map(sensor_df: pd.DataFrame) -> pd.DataFrame:
    """Which gateways did each sensor communicate through?"""
    return sensor_df[["sensor_mac_address", "gateway_mac_address"]].drop_duplicates()


def _align(sensor_df: pd.DataFrame, gateway_df: pd.DataFrame, freq: str) -> pd.DataFrame:
    """
    Per-sensor local ambient: for each sensor, aggregate ambient temp from
    only the gateways it actually communicated through, then join by timestamp.

    Returns one row per (sensor_mac_address, hour) with internal_temp +
    ambient_median + ambient_mean (computed from that sensor's local gateways).
    """
    sensor_h   = _resample_sensor(sensor_df, freq)
    gateway_h  = _resample_gateway(gateway_df, freq)
    sg_map     = _sensor_gateway_map(sensor_df)

    # For each sensor, get all its gateways' hourly ambient readings
    local_ambient = sg_map.merge(gateway_h, on="gateway_mac_address", how="inner")

    # Aggregate to per-sensor per-hour: median + mean across local gateways
    local_agg = (
        local_ambient
        .groupby(["sensor_mac_address", "timestamp"])["ambient_temp"]
        .agg(ambient_median="median", ambient_mean="mean")
        .reset_index()
    )

    return sensor_h.merge(local_agg, on=["sensor_mac_address", "timestamp"], how="inner")


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


def _decoupling(internal: pd.Series, ambient_median: pd.Series,
                 ambient_mean: pd.Series) -> dict[str, float]:
    """Metric 2 — does internal temp move with outside weather?
    r ≈ 0 → hive is thermally independent (strong).
    r → 1 → hive follows ambient curve (weak).

    Computes against both yard-level ambient references so we can compare:
      ambient_corr_median — correlation with median-across-gateways (robust)
      ambient_corr_mean   — correlation with mean-across-gateways  (sensitive)
    Returns NaN if fewer than MIN_READINGS or constant ambient."""
    if len(internal) < MIN_READINGS:
        return {"ambient_corr_median": float("nan"),
                "ambient_corr_mean":   float("nan")}
    with np.errstate(invalid="ignore"):
        r_med, _ = stats.pearsonr(internal, ambient_median)
        r_mean, _ = stats.pearsonr(internal, ambient_mean)
    return {"ambient_corr_median": float(r_med),
            "ambient_corr_mean":   float(r_mean)}


def _comfort_zone(internal: pd.Series) -> dict[str, float]:
    """Metric 3 — how often is the hive in the brood-rearing band?
    min/mean/max/median_temp: distribution of raw internal temperature.
    percent_comfort: % of hours in [32°C, 36°C]. Large hives → ≈ 100%."""
    in_zone = (internal >= COMFORT_LOW) & (internal <= COMFORT_HIGH)
    return {
        "min_temp":        float(internal.min()),
        "mean_temp":       float(internal.mean()),
        "max_temp":        float(internal.max()),
        "median_temp":     float(internal.median()),
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
      A  FAIL    — large hive with unstable / ambient-coupled / out-of-comfort physics
      B  WARNING — medium hive too cold or never reaches brood zone
      C  WARNING — small hive suspiciously stable or running too warm
      D  PASS    — physics align with prediction

    Rule A sub-conditions (any one is sufficient to FAIL):
      std_dev > large.std_dev_max     — too volatile for a large hive
      iqr     > large.iqr_max         — robust volatility check (outlier-insensitive)
      corr    > large.corr_max        — too coupled to ambient weather
      percent_comfort < large.comfort_min — rarely in brood-rearing band

    Rule B sub-conditions (any one is sufficient to WARNING):
      mean_temp < medium.mean_temp_min         — too cold for a medium hive
      percent_comfort <= medium.percent_comfort_max — zero time in brood zone → behaves like small

    Rule C sub-conditions (any one is sufficient to WARNING):
      std_dev  < small.std_dev_min  — suspiciously stable for a small hive
      iqr      < small.iqr_min      — robust companion to std_dev check
      mean_temp > small.mean_temp_max — running too warm for a small hive

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
    corr = df["ambient_corr_median"]  # grading uses the robust yard-level median

    # ── boolean masks (vectorized) ──────────────────────────────────────────
    is_large  = size == "large"
    is_medium = size == "medium"
    is_small  = size == "small"

    # Rule A sub-conditions — large hive physics mismatch
    cond_std     = is_large & (df["std_dev"] > large["std_dev_max"])
    cond_iqr     = is_large & (df["iqr"]     > large["iqr_max"])
    cond_corr    = is_large & corr.notna() & (corr > large["corr_max"])
    cond_comfort = is_large & (df["percent_comfort"] < large["comfort_min"])
    rule_a = cond_std | cond_iqr | cond_corr | cond_comfort

    # Rule B sub-conditions — medium hive too cold or never in brood zone
    cond_b_temp    = is_medium & (df["mean_temp"] < medium["mean_temp_min"])
    cond_b_comfort = is_medium & (df["percent_comfort"] <= medium["percent_comfort_max"])
    rule_b = cond_b_temp | cond_b_comfort

    # Rule C sub-conditions — small hive suspiciously stable or too warm
    cond_c_std  = is_small & (df["std_dev"]   < small["std_dev_min"])
    cond_c_iqr  = is_small & (df["iqr"]       < small["iqr_min"])
    cond_c_temp = is_small & (df["mean_temp"] > small["mean_temp_max"])
    rule_c = cond_c_std | cond_c_iqr | cond_c_temp

    # ── status (vectorized) ─────────────────────────────────────────────────
    status = np.select(
        [rule_a, rule_b, rule_c],
        ["FAIL", "WARNING", "WARNING"],
        default="PASS",
    )

    # ── reason strings ──────────────────────────────────────────────────────
    # Rule A: join whichever sub-conditions fired
    std_part     = np.where(cond_std,
                            "std_dev=" + df["std_dev"].round(2).astype(str)
                            + " > " + str(large["std_dev_max"]), "")
    iqr_part     = np.where(cond_iqr,
                            "iqr=" + df["iqr"].round(2).astype(str)
                            + " > " + str(large["iqr_max"]), "")
    corr_part    = np.where(cond_corr,
                            "corr=" + corr.round(2).astype(str)
                            + " > " + str(large["corr_max"]), "")
    comfort_part = np.where(cond_comfort,
                            "comfort=" + df["percent_comfort"].round(1).astype(str)
                            + "% < " + str(large["comfort_min"]) + "%", "")

    fail_detail = (
        pd.DataFrame({"a": std_part, "b": iqr_part, "c": corr_part, "d": comfort_part})
        .apply(lambda r: "; ".join(v for v in r if v), axis=1)
    )
    reason_a = "Large hive physics mismatch: " + fail_detail

    # Rule B: join whichever sub-conditions fired
    b_temp_part    = np.where(cond_b_temp,
                              "mean_temp=" + df["mean_temp"].round(1).astype(str)
                              + "\u00b0C < " + str(medium["mean_temp_min"]) + "\u00b0C", "")
    b_comfort_part = np.where(cond_b_comfort,
                              "comfort=" + df["percent_comfort"].round(1).astype(str)
                              + "% <= " + str(medium["percent_comfort_max"]) + "% (cold like small)", "")

    medium_detail = (
        pd.DataFrame({"a": b_temp_part, "b": b_comfort_part})
        .apply(lambda r: "; ".join(v for v in r if v), axis=1)
    )
    reason_b = "Medium hive physics mismatch: " + medium_detail

    # Rule C: join whichever sub-conditions fired
    c_std_part  = np.where(cond_c_std,
                           "std_dev=" + df["std_dev"].round(2).astype(str)
                           + " < " + str(small["std_dev_min"]), "")
    c_iqr_part  = np.where(cond_c_iqr,
                           "iqr=" + df["iqr"].round(2).astype(str)
                           + " < " + str(small["iqr_min"]), "")
    c_temp_part = np.where(cond_c_temp,
                           "mean_temp=" + df["mean_temp"].round(1).astype(str)
                           + "\u00b0C > " + str(small["mean_temp_max"]) + "\u00b0C (too warm)", "")

    small_detail = (
        pd.DataFrame({"a": c_std_part, "b": c_iqr_part, "c": c_temp_part})
        .apply(lambda r: "; ".join(v for v in r if v), axis=1)
    )
    reason_c = "Small hive physics mismatch: " + small_detail

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
        group_id, date, sensor_mac_address, gateway_mac_address, hive_size_bucket,
        std_dev, iqr, ambient_corr_median, ambient_corr_mean,
        min_temp, mean_temp, max_temp, median_temp, percent_comfort, n_readings
    """
    aligned = _align(sensor_df, gateway_df, RESAMPLE_FREQ)

    meta = (
        sensor_df[["sensor_mac_address", "hive_size_bucket", "group_id"]]
        .drop_duplicates("sensor_mac_address", keep="last")
    )

    records: list[dict] = []

    for sensor_mac, grp in aligned.groupby("sensor_mac_address"):
        both = grp[["internal_temp", "ambient_median", "ambient_mean"]].dropna()
        if len(both) < MIN_READINGS:
            log.debug(f"  skip {sensor_mac}: only {len(both)} aligned hours")
            continue

        row: dict = {
            "sensor_mac_address":  sensor_mac,
            "gateway_mac_address": grp["gateway_mac_address"].iloc[0],
            "n_readings":          len(both),
        }
        row.update(_stability(both["internal_temp"]))
        row.update(_decoupling(both["internal_temp"],
                               both["ambient_median"], both["ambient_mean"]))
        row.update(_comfort_zone(both["internal_temp"]))
        records.append(row)

    if not records:
        return pd.DataFrame()

    result = pd.DataFrame(records).merge(meta, on="sensor_mac_address", how="left")
    result["date"] = date

    ordered_cols = [
        "group_id", "date", "sensor_mac_address", "gateway_mac_address", "hive_size_bucket",
        "std_dev", "iqr", "ambient_corr_median", "ambient_corr_mean",
        "min_temp", "mean_temp", "max_temp", "median_temp", "percent_comfort", "n_readings",
    ]
    return result[ordered_cols].sort_values(["group_id", "date", "sensor_mac_address"]).reset_index(drop=True)
