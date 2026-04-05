"""
Train / test split configuration for sensor_group_segment / group_model_temperature_health.

Decision log
────────────
Goal    : Binary classification — valid vs invalid.
          needs_recalibration is excluded from train/test because:
            • It introduces noise (it means "not valid yet" which overlaps
              with invalid in practice — same endpoint outcome)
            • The final decision we care about is: is this model valid or not?
          needs_recalibration samples remain in data/samples/ on disk but
          are not symlinked into train/ or test/.

Ratio   : 75 / 25
Reason  : Labels are trusted. 75/25 keeps enough test samples for
          evaluation while maximising training data.

Method  : Stratified by status (valid / invalid only).
          Within each stratum, entries are sorted by (group_id, date) and
          every Nth row is assigned to test — guaranteeing all groups and
          all date ranges appear in both sets.

Seed    : 42 (deterministic, reproducible)

Final counts (from 565 usable labeled pairs — valid + invalid only)
────────────────────────────────────────────────────────────────────
  Status     Train   Test   Total
  valid        269     90     359
  invalid      154     52     206
  TOTAL        423    142     565

  Excluded (kept in data/samples/ but not in split):
  needs_recalibration: 77 samples
"""

SPLIT_RATIO_TEST = 0.25
RANDOM_SEED      = 42
GROUND_TRUTH_CSV = "ground_truth/ground_truth_statuess_ca_2026.csv"
MANIFEST_PATH    = "data/samples/split_manifest.csv"

# Only these statuses are included in the train/test split
SPLIT_STATUSES = {"valid", "invalid"}

# Data access rules
# ─────────────────
# ALL training, threshold tuning, and metric development → data/samples/train/
# Test set (data/samples/test/) is ONLY touched for final evaluation.
TRAIN_DIR = "data/samples/train"
TEST_DIR  = "data/samples/test"
