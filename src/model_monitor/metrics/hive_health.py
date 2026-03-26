"""
Hive Health — CURRENT metric.

Assesses whether BeeFrame model predictions align with hive health signals
from the daily_hive_health_monitoring table. A group is flagged if the
proportion of healthy sensors is too low relative to total beeframes sensors.

Source: beehero-model-monitoring / test/bee_frames/metric_hive_health.py

Data inputs:
    - daily_hive_health_monitoring: sensor_mac_address, run_date, is_healthy,
                                    group_id, yard_id
    - preprocess table:             sensor_mac_address, run_date, group_id, yard_id

Logic (from source):
    1. Join hive health data with preprocess to get active sensors.
    2. Per (group_id, run_date), compute:
       - n_beeframes_sensors:         total sensors with predictions
       - hive_health_unique_sensor_count: sensors with any is_healthy record
       - filtered_sensors_count:      sensors with pred_raw ≥ STRONG_HIVE (10)
       - strong_hive_total_num_samples: count of strong predictions
    3. A group FAILS (value=True) if ANY of:
       - ratio = hive_health_unique_sensor_count / n_beeframes_sensors < 0.1
       - filtered_sensors_count < MIN_STRONG_HIVES (10)
       - strong_ratio = filtered_sensors_count / strong_hive_total_num_samples < METRIC_RATIO (0.2)

Thresholds: configs/thresholds.yaml → metrics.hive_health
"""

import pandas as pd


def compute(data: pd.DataFrame) -> pd.DataFrame:
    """
    Compute hive health pass/fail per (group_id, run_date).

    Args:
        data: DataFrame with merged preprocess + daily_hive_health_monitoring columns:
              [group_id, run_date, sensor_mac_address, pred_raw, is_healthy]

    Returns:
        DataFrame with columns [group_id, run_date, value]
        where value=True means the group is flagged (metric FAILED).
    """
    raise NotImplementedError
