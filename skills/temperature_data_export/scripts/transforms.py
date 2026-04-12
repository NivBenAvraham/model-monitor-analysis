"""
Data cleaning and enrichment for temperature exports.

sensor_samples_preprocess()  — fix bad temps, range filter, z-score outlier removal
add_hive_size_bucket()       — map bee_frames → small / medium / large

Thresholds mirror the production visualization pipeline exactly.
"""

from __future__ import annotations

from functools import partial

import numpy as np
import pandas as pd
from scipy.stats import zscore

# ---------------------------------------------------------------------------
# Sensor temperature cleaning constants
# ---------------------------------------------------------------------------
TEMP_FIX_THRESHOLD = -40        # readings below this are likely encoding errors
TEMP_FIX_OFFSET    = 175.71     # correction offset for those readings
TEMP_RANGE_MIN     = -30.0      # valid range lower bound (°C)
TEMP_RANGE_MAX     = 100.0      # valid range upper bound (°C)
HUMIDITY_RANGE_MAX = 95.0       # sensors saturate above this — unreliable
OUTLIER_ZSCORE_THRESHOLD = 3    # z-score cutoff for spike removal

# ---------------------------------------------------------------------------
# Hive size bucket constants
# ---------------------------------------------------------------------------
BEE_FRAMES_SMALL_MAX  = 6   # bee_frames < 6  → small
BEE_FRAMES_MEDIUM_MAX = 10  # bee_frames < 10 → medium, else → large

BUCKET_SMALL  = "small"
BUCKET_MEDIUM = "medium"
BUCKET_LARGE  = "large"


# ---------------------------------------------------------------------------
# Sensor preprocessing
# ---------------------------------------------------------------------------

def _fix_negative_temps(df: pd.DataFrame) -> pd.DataFrame:
    """Correct encoding-error readings below TEMP_FIX_THRESHOLD."""
    mask = df["pcb_temperature_one"] < TEMP_FIX_THRESHOLD
    df.loc[mask, "pcb_temperature_one"] += TEMP_FIX_OFFSET
    return df


def _filter_range(df: pd.DataFrame) -> pd.DataFrame:
    """Drop NaN temps, apply range filter, optionally filter humidity."""
    df = df.dropna(subset=["pcb_temperature_one"])
    df = df[df["pcb_temperature_one"].between(TEMP_RANGE_MIN, TEMP_RANGE_MAX)]
    if "humidity" in df.columns:
        df = df.dropna(subset=["humidity"])
        df = df[df["humidity"] <= HUMIDITY_RANGE_MAX]
    return df


def _remove_outliers_per_sensor(grp: pd.DataFrame, z_threshold: float) -> pd.DataFrame:
    """
    Spike removal using geometric mean of forward/backward diffs.
    First and last readings are always marked outlier (boundary artefacts).
    Readings within the non-outlier min–max range are never removed.
    """
    grp = grp.sort_values("timestamp").reset_index(drop=True)
    fwd  = grp["pcb_temperature_one"].diff()
    bwd  = grp["pcb_temperature_one"] - grp["pcb_temperature_one"].shift(-1)
    fill = (fwd.abs().mean() + bwd.abs().mean()) / 2
    fwd  = fwd.fillna(fill)
    bwd  = bwd.fillna(fill)

    geo_mean = (fwd.abs() * bwd.abs()) ** 0.5
    is_spike = (np.abs(zscore(geo_mean)) > z_threshold) & (geo_mean != 0)

    # boundary rows are always outliers
    is_spike.iloc[[0, -1]] = True

    # protect readings inside the clean band
    clean = grp.loc[~is_spike, "pcb_temperature_one"]
    if not clean.empty:
        is_spike &= ~grp["pcb_temperature_one"].between(clean.min(), clean.max())
    else:
        is_spike[:] = False

    return grp[~is_spike]


def sensor_samples_preprocess(df: pd.DataFrame) -> pd.DataFrame:
    """
    Full sensor preprocessing pipeline:
      1. Fix encoding-error negatives
      2. Range filter [TEMP_RANGE_MIN, TEMP_RANGE_MAX]
      3. Humidity filter (if column present)
      4. Per-sensor z-score spike removal

    Returns cleaned DataFrame (same columns, some rows removed).
    """
    df = df.copy()
    df = _fix_negative_temps(df)
    df = _filter_range(df)

    cleaned = (
        df.groupby("sensor_mac_address", group_keys=False)
        .apply(partial(_remove_outliers_per_sensor, z_threshold=OUTLIER_ZSCORE_THRESHOLD))
    )
    return cleaned.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Hive size bucket
# ---------------------------------------------------------------------------

def bee_frames_to_bucket(bee_frames: float) -> str:
    """Map a bee_frames count to a size bucket label."""
    if bee_frames < BEE_FRAMES_SMALL_MAX:
        return BUCKET_SMALL
    if bee_frames < BEE_FRAMES_MEDIUM_MAX:
        return BUCKET_MEDIUM
    return BUCKET_LARGE


def add_hive_size_bucket(
    sensor_samples: pd.DataFrame,
    hive_updates: pd.DataFrame,
    bucket_column: str = "hive_size_bucket",
) -> pd.DataFrame:
    """
    Join hive_size_bucket onto sensor_samples from hive_updates.bee_frames.
    Sensors missing from hive_updates get NaN bucket.
    """
    if sensor_samples.empty or hive_updates.empty or "bee_frames" not in hive_updates.columns:
        out = sensor_samples.copy()
        out[bucket_column] = pd.NA
        return out

    lookup = (
        hive_updates[["sensor_mac_address", "bee_frames"]]
        .drop_duplicates("sensor_mac_address", keep="last")
        .assign(**{bucket_column: lambda d: d["bee_frames"].map(bee_frames_to_bucket)})
        [["sensor_mac_address", bucket_column]]
    )
    return sensor_samples.merge(lookup, on="sensor_mac_address", how="left")
