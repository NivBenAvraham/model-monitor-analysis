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

    temp_metric         Temperature-based signal (hive vs ambient differential, stability).
                        Ideas to explore in notebooks/exploration/ before implementing.

--- All thresholds live in configs/thresholds.yaml ---
"""
