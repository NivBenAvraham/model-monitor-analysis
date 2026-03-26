"""
Temperature Metric — EXPERIMENTAL metric.

Explores whether temperature sensor data can be used as an additional signal
for BeeFrame model health. Motivated by the temperature_data_export_package,
which already produces clean 30-minute resampled hive and ambient temperature
readings with outlier removal.

Source data (read-only, do not modify):
    /Users/.../beehero-streamlit-app/temperature_data_export_package

Available cleaned fields (after ETL in temperature_data_export_package):
    Sensor (hive) frame:
        sensor_mac_address, timestamp (30-min bins), pcb_temperature_one,
        gateway_mac_address, group_id, hive_size_bucket (optional)

    Gateway (ambient) frame:
        gateway_mac_address, timestamp, pcb_temperature_two

    Hive updates frame:
        sensor_mac_address, created, group_id, bee_frames, model

ETL already applied upstream (do not redo):
    - Negative-temp correction: values < -40 → add 175.71
    - Range filter: pcb_temperature_one in [-30, 100]; humidity ≤ 95
    - Outlier removal: z-score threshold 3 per sensor
    - Resampling: 30-minute mean of pcb_temperature_one

--- Candidate metric ideas to explore in notebooks ---

1. Hive-to-ambient temperature differential:
   diff = pcb_temperature_one - pcb_temperature_two per (sensor, timestamp)
   Hypothesis: unusual differential may indicate sensor fault or data anomaly
   that could explain model degradation.

2. Temperature stability per sensor:
   std(pcb_temperature_one) over a rolling window per sensor/group.
   Hypothesis: high within-day variance may correlate with poor prediction quality.

3. Temperature-stratified model error:
   Segment groups by hive_size_bucket and compare pred_raw vs temp patterns.
   Hypothesis: model may behave differently for hives in extreme temperature ranges.

4. Correlation between temperature anomalies and bee_frames model failures:
   Join temp data with mode_collapse / avg_overtime outcomes by (group_id, date).
   Hypothesis: temperature outlier days may explain metric failures.

--- Next step ---
    Explore these ideas in notebooks/exploration/ before implementing here.
    Once a candidate is validated, implement compute() and add thresholds
    to configs/thresholds.yaml → metrics.temp_metric.

Thresholds (placeholders): configs/thresholds.yaml → metrics.temp_metric
"""

import pandas as pd


def compute(
    sensor_data: pd.DataFrame,
    gateway_data: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compute temperature-based metric per (group_id, run_date).

    Args:
        sensor_data:  Cleaned sensor DataFrame from temperature_data_export_package.
                      Columns: [sensor_mac_address, timestamp, pcb_temperature_one,
                                gateway_mac_address, group_id]
        gateway_data: Cleaned gateway DataFrame.
                      Columns: [gateway_mac_address, timestamp, pcb_temperature_two]

    Returns:
        DataFrame with columns [group_id, run_date, value]
        Semantics of value TBD during exploration.
    """
    raise NotImplementedError
