"""
Metric computation — calculate metric values from ingested data.

--- Current metrics (bee_frames model) ---

    mode_collapse       Detects prediction histogram collapse onto a single bin.
    avg_overtime        Rolling average error over 3/4/5-day windows vs threshold 0.84.
    yard_inspections    CV-ratio comparison of predictions vs yard inspection histograms.
    ops_inspections     CV-ratio comparison of predictions vs ops inspection records.
    hive_health         Ratio of healthy/strong hives vs total beeframes sensors.
    post_validation     Tier1/tier2 human validation gating.

--- Temperature family metrics ---

All metrics below accept already-resampled hourly DataFrames.
Use model_monitor.utils.data_utils.resample_to_hourly() to prepare inputs.
Each metric returns bool (True = check passes / healthy).
Each module exposes METRIC_FAMILY = "temperature" and DEFAULT_WEIGHT for the
future temperature_health aggregator.

    ambient_temperature_volatility          (existing)
        Night-to-night ambient jump ≥ 5 °C → True (volatile).
        Input : raw gateway_df (uses its own internal resampler).
        Weight: 0.6
        Module: model_monitor.metrics.ambient_temperature_volatility

    ambient_stability                       (R1)
        Ambient coefficient of variation ≤ 0.55 → True (stable).
        Input : gateway_hourly
        Weight: 0.4
        Module: model_monitor.metrics.ambient_stability

    ambient_range                           (R2)
        Ambient min ≥ 2 °C and max ≤ 50 °C → True (in range).
        Input : gateway_hourly
        Weight: 0.5
        Module: model_monitor.metrics.ambient_range

    bucket_reference_adherence              (R3)
        Weighted fraction of readings inside canonical bands ≥ 0.50 → True.
        Weights: small=0.10, medium=0.35, large=0.55.
        Input : sensor_hourly
        Weight: 1.0
        Module: model_monitor.metrics.bucket_reference_adherence

    sensor_spread_within_bucket             (R4)
        Average inter-sensor spread per bucket ≤ 5.0 °C → True.
        Input : sensor_hourly
        Weight: 0.8
        Module: model_monitor.metrics.sensor_spread_within_bucket

    bucket_temporal_stability               (R5)
        All buckets within their temporal-std limits → True.
        Limits: large ≤ 2.5 °C, medium ≤ 4.0 °C, small ≤ 7.0 °C.
        Input : sensor_hourly
        Weight: 0.8
        Module: model_monitor.metrics.bucket_temporal_stability

    small_hive_ambient_tracking             (R6a)
        Small-bucket Pearson r with ambient ≥ 0.40 → True.
        Input : sensor_hourly, gateway_hourly
        Weight: 0.4
        Module: model_monitor.metrics.small_hive_ambient_tracking

    large_hive_thermoregulation             (R6b)
        Large-bucket Pearson r with ambient ≤ 0.85 → True.
        Input : sensor_hourly, gateway_hourly
        Weight: 0.5
        Module: model_monitor.metrics.large_hive_thermoregulation

    bucket_temperature_ordering             (R6c)
        mean(small) < mean(medium) < mean(large), gap ≥ 1.5 °C → True.
        Input : sensor_hourly
        Weight: 1.0
        Module: model_monitor.metrics.bucket_temperature_ordering

--- Layer 1: sensor_group_segment skill ---

    sensor_group_segment    Per-sensor temperature physics check.
                            Computes std_dev, iqr, ambient_correlation, mean_temp,
                            percent_comfort and grades each sensor PASS/WARNING/FAIL
                            based on whether its physics match the predicted hive size.
                            See: skills/sensor_group_segment/

--- Layer 2: group_model_temperature_health skill (not yet implemented) ---

    group_model_temperature_health   Per-(group_id, date) model validity decision.
                                     Aggregates Layer 1 sensor outputs → VALID / INVALID.
                                     See: skills/group_model_temperature_health/

--- All thresholds live in skills/<skill>/config/thresholds.yaml ---
"""
