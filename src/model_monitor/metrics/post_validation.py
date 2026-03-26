"""
Post Manual Validation — CURRENT metric.

Gates BeeFrame model output to production based on human validation decisions
recorded in tier1_status and tier2_status. This is the final approval gate
before model results are released.

Source: beehero-model-monitoring / test/bee_frames/metric_post_validation.py

Data inputs:
    - beekeeper_beeframe_model_monitoring_validations:
        group_id, timestamp, tier1_status, tier2_status
    - preprocess table:
        group_id, run_date, group_in_season, groups_in_season_ready_for_review

Logic (from source):
    Groups are classified as BLOCKED or RELEASED based on season status and tier decisions.

    Definitions:
        monitored_season      = group_in_season == True
        model_in_production   = groups_in_season_ready_for_review == True
        ready_for_review      = monitored_season AND model_in_production

    BLOCK (value=False) when:
        - ready_for_review AND tier2_status != 'valid'
        - ready_for_review AND tier1_status != 'valid' AND tier2_status is null
        - ready_for_review AND both tier statuses are null (unreviewed)
        - NOT ready_for_review AND tier2_status != 'valid' (got a bad status anyway)

    PASS (value=True) when:
        - ready_for_review AND tier2_status == 'valid'
        - ready_for_review AND tier1_status == 'valid' AND tier2_status is null
        - NOT ready_for_review AND both statuses null (not yet in scope)
        - NOT ready_for_review AND tier2_status == 'valid'
        - NOT ready_for_review AND tier1_status == 'valid' AND tier2_status is null

Thresholds: none — logic is string-based (tier1_status / tier2_status values).
"""

import pandas as pd


def compute(data: pd.DataFrame) -> pd.DataFrame:
    """
    Compute post-validation pass/fail per (group_id, run_date).

    Args:
        data: DataFrame with columns:
              [group_id, run_date, group_in_season, groups_in_season_ready_for_review,
               tier1_status, tier2_status]

    Returns:
        DataFrame with columns [group_id, run_date, value]
        where value=True means the group is RELEASED (metric PASSED).
    """
    raise NotImplementedError
