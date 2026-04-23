"""
Metric computation — calculate metric values from ingested data.

--- Current metrics (bee_frames model) ---

    mode_collapse       Detects prediction histogram collapse onto a single bin.
    avg_overtime        Rolling average error over 3/4/5-day windows vs threshold 0.84.
    yard_inspections    CV-ratio comparison of predictions vs yard inspection histograms.
    ops_inspections     CV-ratio comparison of predictions vs ops inspection records.
    hive_health         Ratio of healthy/strong hives vs total beeframes sensors.
    post_validation     Tier1/tier2 human validation gating.

--- Experimental metrics ---

    ambient_temperature_volatility
        Returns True when the overnight temperature minima (the troughs of each
        day's diurnal parabola) differ by more than MIN_DAILY_DELTA_CELSIUS (5 °C)
        across the observation window.  Signals that weather shifted during the
        evaluation period, which can confound temperature-based validity checks.
        Input : gateway_hourly DataFrame with pcb_temperature_two + timestamp.
        Output: bool — True = volatile ambient, False = stable or insufficient data.
        Module: model_monitor.metrics.ambient_temperature_volatility

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
