"""
Metric computation — calculate metric values from ingested data.

── Temperature family metrics ──────────────────────────────────────────────
All live under model_monitor.metrics.temperature.*
Each accepts pre-loaded DataFrames and returns a dict with {"passed": bool, ...}.

  Module path                                          ID    Weight
  ─────────────────────────────────────────────────────────────────
  temperature.ambient_temperature_volatility           –     0.6
    Night-to-night ambient jump ≥ 5 °C → True (volatile).
    Input: raw gateway_df

  temperature.ambient_stability                        R1    0.4
    Ambient coefficient of variation ≤ 0.55 → True.
    Input: gateway_df

  temperature.ambient_range                            R2    0.5
    Ambient min ≥ 2 °C and max ≤ 50 °C → True.
    Input: gateway_df

  temperature.bucket_reference_adherence               R3    1.0
    Weighted fraction of readings inside canonical bands ≥ 0.50 → True.
    Input: sensor_df

  temperature.sensor_spread_within_bucket              R4    0.8
    Average inter-sensor spread per bucket ≤ 5.0 °C → True.
    Input: sensor_df

  temperature.bucket_temporal_stability                R5    0.8
    All buckets within temporal-std limits → True.
    Limits: large ≤ 2.5 °C, medium ≤ 4.0 °C, small ≤ 7.0 °C.
    Input: sensor_df

  temperature.small_hive_ambient_tracking              R6a   0.4
    Small-bucket Pearson r with ambient ≥ 0.40 → True.
    Input: sensor_df, gateway_df

  temperature.large_hive_thermoregulation              R6b   0.5
    Large-bucket Pearson r with ambient ≤ 0.85 → True.
    Input: sensor_df, gateway_df

  temperature.bucket_temperature_ordering              R6c   1.0
    mean(small) < mean(medium) < mean(large), gap ≥ 1.5 °C → True.
    Input: sensor_df

── Layer 1: sensor_group_segment ───────────────────────────────────────────
  sensor_group_segment.compute(sensor_df, gateway_df, date, full=False)
    Per-sensor physics check. Outputs PASS / FAIL per (sensor, date).
    full=False → lean 5 grading features
    full=True  → full 14-feature table (for EDA / calibration)

  sensor_group_segment.grade(df, thresholds)
    Compares predicted hive_size_bucket against observed physics.

  Skill: skills/sensor_group_segment/
  Thresholds: skills/sensor_group_segment/config/thresholds.yaml

── Layer 2: group_model_temperature_health ─────────────────────────────────
  Not yet implemented.
  Skill: skills/group_model_temperature_health/
"""

from model_monitor.metrics import temperature  # noqa: F401 — makes subpackage importable
from model_monitor.metrics.sensor_group_segment import compute, grade  # noqa: F401
