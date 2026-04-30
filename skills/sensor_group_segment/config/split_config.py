"""
Train / test split configuration for sensor_group_segment / group_model_temperature_health.

Decision log
────────────
Goal    : Binary classification — valid vs invalid.
          needs_recalibration is excluded from train/test because:
            • It introduces noise (same endpoint outcome as invalid in practice)
            • The final decision we care about: is this model valid or not?
          needs_recalibration samples remain in data/samples/ on disk but
          are not symlinked into train/ or test/.

Method  : Group-level assignment (not row-level stratification).
          Every date from a group goes entirely to train or entirely to test.
          Groups are never split across the boundary.

          Reason: The temperature_health_rule.py was calibrated using
          analyst anchor groups as the constraint.  Splitting those groups
          across train/test would leak the rule's design assumptions into
          the evaluation.

Anchor  : 23 groups locked to train (appeared in analyst plot-decision files):
            skills/hives_temperature_plot_decision/history/2026-04-15/invalid.txt
            skills/hives_temperature_plot_decision/data_analyst_plot_decisions/valid.txt
          These are never in the test set.

Ratio   : 75 / 25 (row level, achieved via optimised group assignment)
Seed    : 42 (hill-climbing search, 200k iterations)

Final counts (from 547 usable labeled pairs — valid + invalid only)
────────────────────────────────────────────────────────────────────
  Split   Groups   Rows   Valid   Invalid   Valid%
  train      36     410     256      154     62.4%
  test       15     137      85       52     62.0%
  TOTAL      51     547     341      206     62.3%

  Excluded (kept in data/samples/ but not in split):
  needs_recalibration: 75 samples

Previous split (archived in data/samples/split_30_4/):
  Row-level stratified, 565 pairs, train=423 / test=142
  Groups were split across train/test (no anchor constraint).
"""

SPLIT_RATIO_TEST = 0.25
RANDOM_SEED      = 42
GROUND_TRUTH_CSV = "ground_truth/gt_cleaned_ca_2026.csv"
MANIFEST_PATH    = "data/samples/split_manifest.csv"

# Only these statuses are included in the train/test split
SPLIT_STATUSES = {"valid", "invalid"}

# ── Group-level assignment ────────────────────────────────────────────────────
# 23 anchor groups locked to TRAIN (appeared in analyst plot-decision review).
# 13 free groups additionally assigned to TRAIN to achieve 75/25 row ratio.
# 15 free groups assigned to TEST.
TRAIN_GROUPS: frozenset[int] = frozenset({
    # Anchor groups (locked — appeared in analyst review files)
    163, 395, 483, 518, 549, 558, 625, 661, 750, 766, 790, 935, 968,
    1155, 1691, 1723, 1764, 2777, 2799, 2805, 2858, 2889, 2901,
    # Free groups assigned to train (optimised for 75/25 + class balance)
    47, 496, 687, 776, 940, 984, 1144, 1713, 1768, 1793, 1838, 2703, 2929,
})

TEST_GROUPS: frozenset[int] = frozenset({
    36, 48, 484, 491, 943, 962, 969, 970,
    1618, 1693, 1730, 1794, 1884, 2834, 2854,
})

# Data access rules
# ─────────────────
# ALL training, threshold tuning, and metric development → data/samples/train/
# Test set (data/samples/test/) is ONLY touched for final evaluation.
TRAIN_DIR = "data/samples/train"
TEST_DIR  = "data/samples/test"
