"""
Sensor Group Segment — Phase 1 (Feature Engineering) + Phase 2 (Grading).

Layer 1 of the temperature-based model monitoring pipeline.
Operates at the individual sensor level — each sensor is a data point.

════════════════════════════════════════════════════════════════════
PHASE 1 — compute()
════════════════════════════════════════════════════════════════════
Produces only the features used by the grading thresholds (Lean 5):
  std_dev, iqr, ambient_correlation, mean_temp, percent_comfort

Data flow:
  sensor_temperature parquet  →  pcb_temperature_one  (internal temp)
                                 hive_size_bucket      (model prediction)
                                 gateway_mac_address   (join key to gateway)
  gateway_temperature parquet →  pcb_temperature_two  (ambient temp)

Both are resampled to 1-hour means. Ambient temp is the median across
only the gateways each sensor communicated through (local reference).

Output of compute(): one row per (group_id, date, sensor_mac_address) with:
  group_id, date, sensor_mac_address, gateway_mac_address, hive_size_bucket,
  std_dev, iqr, ambient_correlation, mean_temp, percent_comfort, n_readings

For the full 10-metric feature set (adds ambient_corr_mean, min_temp,
max_temp, median_temp).

════════════════════════════════════════════════════════════════════
PHASE 2 — grade()
════════════════════════════════════════════════════════════════════
Compares predicted hive_size_bucket against observed temperature physics.
Output: PASS (physics match prediction) or FAIL (mismatch detected).

Rules (first match wins):
  Rule A — FAIL : predicted=large  but physics look weak
  Rule B — FAIL : predicted=medium but hive is too cold
  Rule C — FAIL : predicted=small  but hive is suspiciously stable or warm
  Default — PASS

Thresholds live in:
  skills/sensor_group_segment/config/thresholds.yaml → metrics.sensor_group_segment.grading

Output of grade(): same DataFrame + status (PASS|FAIL) + reason.
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

    # Aggregate to per-sensor per-hour: median across local gateways (robust)
    local_agg = (
        local_ambient
        .groupby(["sensor_mac_address", "timestamp"])["ambient_temp"]
        .median()
        .reset_index()
        .rename(columns={"ambient_temp": "ambient_median"})
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


def _decoupling(internal: pd.Series, ambient_median: pd.Series) -> dict[str, float]:
    """Metric 2 — does internal temp move with outside weather?
    r ≈ 0 → thermally independent (strong).  r → 1 → follows ambient (weak).
    Uses median-across-local-gateways as ambient reference (robust to outlier gateways).
    Returns NaN if fewer than MIN_READINGS or constant ambient."""
    if len(internal) < MIN_READINGS:
        return {"ambient_correlation": float("nan")}
    with np.errstate(invalid="ignore"):
        r, _ = stats.pearsonr(internal, ambient_median)
    return {"ambient_correlation": float(r)}


def _comfort_zone(internal: pd.Series) -> dict[str, float]:
    """Metric 3 — brood-rearing band presence.
    mean_temp: average internal temperature.
    percent_comfort: % of hours in [32°C, 36°C]."""
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

    Rules (first match wins → FAIL, otherwise PASS):
      A — large  hive with weak physics (volatile, coupled, or cold)
      B — medium hive too cold
      C — small  hive too stable or too warm

    Parameters
    ----------
    df         : output of compute() — one row per (date, sensor_mac_address)
    thresholds : grading dict from thresholds.yaml

    Returns
    -------
    Same DataFrame + status (PASS|FAIL) + reason
    """
    large  = thresholds["large"]
    medium = thresholds["medium"]
    small  = thresholds["small"]

    size = df["hive_size_bucket"].str.lower()
    corr = df["ambient_correlation"]

    is_large  = size == "large"
    is_medium = size == "medium"
    is_small  = size == "small"

    # Rule A — large hive physics mismatch
    cond_std     = is_large & (df["std_dev"] > large["std_dev_max"])
    cond_iqr     = is_large & (df["iqr"]     > large["iqr_max"])
    cond_corr    = is_large & corr.notna() & (corr > large["corr_max"])
    cond_comfort = is_large & (df["percent_comfort"] < large["comfort_min"])
    rule_a = cond_std | cond_iqr | cond_corr | cond_comfort

    # Rule B — medium hive too cold
    rule_b = is_medium & (df["mean_temp"] < medium["mean_temp_min"])

    # Rule C — small hive too stable or too warm
    cond_c_std  = is_small & (df["std_dev"]   < small["std_dev_min"])
    cond_c_iqr  = is_small & (df["iqr"]       < small["iqr_min"])
    cond_c_temp = is_small & (df["mean_temp"] > small["mean_temp_max"])
    rule_c = cond_c_std | cond_c_iqr | cond_c_temp

    any_fail = rule_a | rule_b | rule_c
    status = np.where(any_fail, "FAIL", "PASS")

    # ── reason strings ──────────────────────────────────────────────────────
    std_part     = np.where(cond_std,
                            "std_dev=" + df["std_dev"].round(2).astype(str)
                            + ">" + str(large["std_dev_max"]), "")
    iqr_part     = np.where(cond_iqr,
                            "iqr=" + df["iqr"].round(2).astype(str)
                            + ">" + str(large["iqr_max"]), "")
    corr_part    = np.where(cond_corr,
                            "corr=" + corr.round(2).astype(str)
                            + ">" + str(large["corr_max"]), "")
    comfort_part = np.where(cond_comfort,
                            "comfort=" + df["percent_comfort"].round(1).astype(str)
                            + "%<" + str(large["comfort_min"]) + "%", "")

    fail_a = (
        pd.DataFrame({"a": std_part, "b": iqr_part, "c": corr_part, "d": comfort_part})
        .apply(lambda r: "; ".join(v for v in r if v), axis=1)
    )
    reason_a = "Large mismatch: " + fail_a

    reason_b = (
        "Medium too cold: mean_temp="
        + df["mean_temp"].round(1).astype(str)
        + "<" + str(medium["mean_temp_min"])
    )

    c_std_part  = np.where(cond_c_std,
                           "std_dev=" + df["std_dev"].round(2).astype(str)
                           + "<" + str(small["std_dev_min"]), "")
    c_iqr_part  = np.where(cond_c_iqr,
                           "iqr=" + df["iqr"].round(2).astype(str)
                           + "<" + str(small["iqr_min"]), "")
    c_temp_part = np.where(cond_c_temp,
                           "mean_temp=" + df["mean_temp"].round(1).astype(str)
                           + ">" + str(small["mean_temp_max"]), "")

    fail_c = (
        pd.DataFrame({"a": c_std_part, "b": c_iqr_part, "c": c_temp_part})
        .apply(lambda r: "; ".join(v for v in r if v), axis=1)
    )
    reason_c = "Small mismatch: " + fail_c

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

    Produces only the Lean 5 features used by grade() thresholds.
    For bulk extraction across all samples see skills/sensor_group_segment/scripts/extract_features.py.

    Returns
    -------
    DataFrame with columns:
        group_id, date, sensor_mac_address, gateway_mac_address, hive_size_bucket,
        std_dev, iqr, ambient_correlation, mean_temp, percent_comfort, n_readings
    """
    aligned = _align(sensor_df, gateway_df, RESAMPLE_FREQ)

    meta = (
        sensor_df[["sensor_mac_address", "hive_size_bucket", "group_id"]]
        .drop_duplicates("sensor_mac_address", keep="last")
    )

    records: list[dict] = []

    for sensor_mac, grp in aligned.groupby("sensor_mac_address"):
        both = grp[["internal_temp", "ambient_median"]].dropna()
        if len(both) < MIN_READINGS:
            log.debug(f"  skip {sensor_mac}: only {len(both)} aligned hours")
            continue

        row: dict = {
            "sensor_mac_address":  sensor_mac,
            "gateway_mac_address": grp["gateway_mac_address"].iloc[0],
            "n_readings":          len(both),
        }
        row.update(_stability(both["internal_temp"]))
        row.update(_decoupling(both["internal_temp"], both["ambient_median"]))
        row.update(_comfort_zone(both["internal_temp"]))
        records.append(row)

    if not records:
        return pd.DataFrame()

    result = pd.DataFrame(records).merge(meta, on="sensor_mac_address", how="left")
    result["date"] = date

    ordered_cols = [
        "group_id", "date", "sensor_mac_address", "gateway_mac_address", "hive_size_bucket",
        "std_dev", "iqr", "ambient_correlation", "mean_temp", "percent_comfort", "n_readings",
    ]
    return result[ordered_cols].sort_values(["group_id", "date", "sensor_mac_address"]).reset_index(drop=True)
