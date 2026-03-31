"""
Extraction plan for the temp_metric pipeline.

Each entry: (group_id, start_date, end_date, model_name)
  - Dates are inclusive on both ends.
  - Consecutive dates with the same model are merged into a single range.
  - Dates < 2026-03-10  → beeframes_supervised_snapshot_24_v6_1_1_OS  (supervised_beeframes table)
  - Dates >= 2026-03-10 → unified_bee_frames_v1-3-11                   (unified_bee_frames table)

Source: data/temp_ground_truth/ground_truth_statuess_ca_2026.csv
"""

EXTRACTION_PLAN = [
    # ── Group 36 ──────────────────────────────────────────────────────────────
    (   36, "2026-02-08", "2026-02-10", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid, needs_recalibration, valid
    (   36, "2026-02-16", "2026-02-19", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (   36, "2026-02-22", "2026-02-22", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    (   36, "2026-02-26", "2026-02-26", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    (   36, "2026-03-03", "2026-03-03", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (   36, "2026-03-08", "2026-03-09", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    # ── Group 47 ──────────────────────────────────────────────────────────────
    (   47, "2026-01-25", "2026-01-26", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (   47, "2026-02-09", "2026-02-09", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    (   47, "2026-02-12", "2026-02-12", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (   47, "2026-02-17", "2026-02-17", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    (   47, "2026-02-20", "2026-02-20", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (   47, "2026-02-24", "2026-02-24", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (   47, "2026-02-26", "2026-02-26", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    # ── Group 48 ──────────────────────────────────────────────────────────────
    (   48, "2026-02-22", "2026-02-22", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (   48, "2026-03-01", "2026-03-01", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (   48, "2026-03-16", "2026-03-16", "unified_bee_frames_v1-3-11"),                  # needs_recalibration
    # ── Group 163 ─────────────────────────────────────────────────────────────
    (  163, "2026-02-16", "2026-02-16", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  163, "2026-02-24", "2026-02-26", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid, valid
    (  163, "2026-03-01", "2026-03-03", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  163, "2026-03-08", "2026-03-08", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  163, "2026-03-11", "2026-03-11", "unified_bee_frames_v1-3-11"),                  # valid
    # ── Group 395 ─────────────────────────────────────────────────────────────
    (  395, "2026-01-24", "2026-01-24", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    (  395, "2026-02-08", "2026-02-08", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    (  395, "2026-02-11", "2026-02-11", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    (  395, "2026-02-26", "2026-02-26", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    (  395, "2026-02-28", "2026-02-28", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    (  395, "2026-03-03", "2026-03-03", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    (  395, "2026-03-09", "2026-03-09", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  395, "2026-03-10", "2026-03-12", "unified_bee_frames_v1-3-11"),                  # valid
    (  395, "2026-03-22", "2026-03-22", "unified_bee_frames_v1-3-11"),                  # valid
    # ── Group 483 ─────────────────────────────────────────────────────────────
    (  483, "2026-02-10", "2026-02-10", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    (  483, "2026-02-15", "2026-02-16", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid, needs_recalibration
    (  483, "2026-02-19", "2026-02-19", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  483, "2026-02-26", "2026-02-26", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    (  483, "2026-02-28", "2026-03-01", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid, valid
    (  483, "2026-03-08", "2026-03-09", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  483, "2026-03-10", "2026-03-12", "unified_bee_frames_v1-3-11"),                  # valid
    (  483, "2026-03-17", "2026-03-19", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    (  483, "2026-03-21", "2026-03-23", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    # ── Group 484 ─────────────────────────────────────────────────────────────
    (  484, "2026-01-28", "2026-01-28", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  484, "2026-02-08", "2026-02-09", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid, needs_recalibration
    (  484, "2026-02-12", "2026-02-12", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  484, "2026-02-16", "2026-02-19", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid, valid
    (  484, "2026-02-22", "2026-02-23", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  484, "2026-02-26", "2026-02-26", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    (  484, "2026-02-28", "2026-03-02", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  484, "2026-03-14", "2026-03-19", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    # ── Group 491 ─────────────────────────────────────────────────────────────
    (  491, "2026-02-10", "2026-02-10", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  491, "2026-02-12", "2026-02-12", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  491, "2026-02-15", "2026-02-15", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    (  491, "2026-02-17", "2026-02-17", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  491, "2026-02-22", "2026-02-22", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  491, "2026-02-24", "2026-02-26", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    # ── Group 496 ─────────────────────────────────────────────────────────────
    (  496, "2026-02-16", "2026-02-16", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    (  496, "2026-02-19", "2026-02-19", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    (  496, "2026-02-25", "2026-02-25", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  496, "2026-03-02", "2026-03-02", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  496, "2026-03-09", "2026-03-09", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  496, "2026-03-10", "2026-03-12", "unified_bee_frames_v1-3-11"),                  # valid
    # ── Group 518 ─────────────────────────────────────────────────────────────
    (  518, "2026-01-29", "2026-01-29", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    (  518, "2026-02-10", "2026-02-10", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  518, "2026-02-15", "2026-02-15", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    (  518, "2026-02-17", "2026-02-17", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  518, "2026-02-25", "2026-02-25", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  518, "2026-03-01", "2026-03-01", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  518, "2026-03-22", "2026-03-22", "unified_bee_frames_v1-3-11"),                  # invalid
    # ── Group 549 ─────────────────────────────────────────────────────────────
    (  549, "2026-02-18", "2026-02-20", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    (  549, "2026-03-01", "2026-03-01", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  549, "2026-03-15", "2026-03-15", "unified_bee_frames_v1-3-11"),                  # invalid
    (  549, "2026-03-18", "2026-03-19", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    (  549, "2026-03-22", "2026-03-22", "unified_bee_frames_v1-3-11"),                  # invalid
    # ── Group 558 ─────────────────────────────────────────────────────────────
    (  558, "2026-02-11", "2026-02-11", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  558, "2026-02-15", "2026-02-19", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid, valid
    (  558, "2026-02-22", "2026-02-24", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  558, "2026-02-26", "2026-02-26", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  558, "2026-03-01", "2026-03-02", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid, valid
    (  558, "2026-03-05", "2026-03-05", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  558, "2026-03-09", "2026-03-09", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  558, "2026-03-10", "2026-03-12", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    (  558, "2026-03-15", "2026-03-19", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    (  558, "2026-03-22", "2026-03-22", "unified_bee_frames_v1-3-11"),                  # invalid
    # ── Group 625 ─────────────────────────────────────────────────────────────
    (  625, "2026-02-11", "2026-02-12", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    (  625, "2026-02-14", "2026-02-14", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    (  625, "2026-03-16", "2026-03-19", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    (  625, "2026-03-22", "2026-03-23", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    # ── Group 661 ─────────────────────────────────────────────────────────────
    (  661, "2026-02-23", "2026-02-26", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid, valid
    (  661, "2026-02-28", "2026-03-01", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration, valid
    (  661, "2026-03-03", "2026-03-03", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    (  661, "2026-03-05", "2026-03-05", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  661, "2026-03-08", "2026-03-09", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration, valid
    (  661, "2026-03-16", "2026-03-19", "unified_bee_frames_v1-3-11"),                  # valid
    (  661, "2026-03-23", "2026-03-23", "unified_bee_frames_v1-3-11"),                  # invalid
    # ── Group 687 ─────────────────────────────────────────────────────────────
    (  687, "2026-02-07", "2026-02-07", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    (  687, "2026-02-10", "2026-02-10", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  687, "2026-02-15", "2026-02-15", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  687, "2026-02-19", "2026-02-19", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  687, "2026-02-23", "2026-02-26", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid, valid
    (  687, "2026-02-28", "2026-03-01", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  687, "2026-03-05", "2026-03-05", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  687, "2026-03-11", "2026-03-12", "unified_bee_frames_v1-3-11"),                  # invalid
    # ── Group 750 ─────────────────────────────────────────────────────────────
    (  750, "2026-02-01", "2026-02-01", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    (  750, "2026-02-04", "2026-02-04", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    (  750, "2026-02-07", "2026-02-07", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  750, "2026-02-09", "2026-02-09", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    (  750, "2026-02-15", "2026-02-17", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid, valid
    (  750, "2026-02-19", "2026-02-19", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  750, "2026-02-23", "2026-02-23", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    (  750, "2026-02-26", "2026-02-26", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    (  750, "2026-02-28", "2026-02-28", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    (  750, "2026-03-03", "2026-03-03", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    (  750, "2026-03-08", "2026-03-08", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  750, "2026-03-19", "2026-03-19", "unified_bee_frames_v1-3-11"),                  # valid
    (  750, "2026-03-22", "2026-03-23", "unified_bee_frames_v1-3-11"),                  # invalid
    # ── Group 766 ─────────────────────────────────────────────────────────────
    (  766, "2026-02-12", "2026-02-12", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  766, "2026-02-16", "2026-02-20", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid, valid
    (  766, "2026-02-22", "2026-02-22", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    (  766, "2026-02-26", "2026-02-26", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  766, "2026-03-01", "2026-03-01", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  766, "2026-03-17", "2026-03-19", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    (  766, "2026-03-21", "2026-03-23", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    # ── Group 776 ─────────────────────────────────────────────────────────────
    (  776, "2026-03-09", "2026-03-09", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  776, "2026-03-10", "2026-03-12", "unified_bee_frames_v1-3-11"),                  # valid
    (  776, "2026-03-15", "2026-03-19", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    (  776, "2026-03-22", "2026-03-22", "unified_bee_frames_v1-3-11"),                  # valid
    # ── Group 790 ─────────────────────────────────────────────────────────────
    (  790, "2026-01-29", "2026-01-29", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  790, "2026-01-31", "2026-02-01", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid, valid
    (  790, "2026-02-03", "2026-02-05", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    (  790, "2026-02-11", "2026-02-11", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  790, "2026-02-15", "2026-02-18", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid, needs_recalibration
    (  790, "2026-02-20", "2026-02-20", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  790, "2026-02-24", "2026-02-24", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  790, "2026-03-01", "2026-03-02", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  790, "2026-03-08", "2026-03-09", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration, valid
    (  790, "2026-03-14", "2026-03-19", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    (  790, "2026-03-21", "2026-03-23", "unified_bee_frames_v1-3-11"),                  # invalid
    # ── Group 935 ─────────────────────────────────────────────────────────────
    (  935, "2026-01-24", "2026-01-24", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  935, "2026-01-26", "2026-01-26", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    (  935, "2026-01-29", "2026-01-29", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    (  935, "2026-02-01", "2026-02-02", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    (  935, "2026-02-05", "2026-02-05", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    (  935, "2026-02-07", "2026-02-10", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid, needs_recalibration, valid
    (  935, "2026-02-15", "2026-02-15", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    (  935, "2026-02-26", "2026-02-26", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  935, "2026-03-01", "2026-03-01", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  935, "2026-03-11", "2026-03-11", "unified_bee_frames_v1-3-11"),                  # valid
    (  935, "2026-03-18", "2026-03-19", "unified_bee_frames_v1-3-11"),                  # invalid
    (  935, "2026-03-21", "2026-03-23", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    # ── Group 940 ─────────────────────────────────────────────────────────────
    (  940, "2026-02-18", "2026-02-20", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    (  940, "2026-03-01", "2026-03-01", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  940, "2026-03-19", "2026-03-19", "unified_bee_frames_v1-3-11"),                  # valid
    # ── Group 943 ─────────────────────────────────────────────────────────────
    (  943, "2026-02-16", "2026-02-16", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  943, "2026-02-18", "2026-02-18", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    (  943, "2026-02-22", "2026-02-22", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  943, "2026-02-24", "2026-02-26", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  943, "2026-03-02", "2026-03-02", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  943, "2026-03-05", "2026-03-05", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  943, "2026-03-18", "2026-03-19", "unified_bee_frames_v1-3-11"),                  # valid
    # ── Group 962 ─────────────────────────────────────────────────────────────
    (  962, "2026-01-25", "2026-01-25", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    # ── Group 968 ─────────────────────────────────────────────────────────────
    (  968, "2026-01-24", "2026-01-24", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  968, "2026-01-27", "2026-01-29", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid, valid
    (  968, "2026-02-01", "2026-02-01", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  968, "2026-02-07", "2026-02-07", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    (  968, "2026-02-15", "2026-02-15", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    (  968, "2026-02-22", "2026-02-22", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  968, "2026-03-01", "2026-03-01", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  968, "2026-03-09", "2026-03-09", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  968, "2026-03-10", "2026-03-12", "unified_bee_frames_v1-3-11"),                  # valid
    (  968, "2026-03-15", "2026-03-15", "unified_bee_frames_v1-3-11"),                  # invalid
    # ── Group 969 ─────────────────────────────────────────────────────────────
    (  969, "2026-02-22", "2026-02-22", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    (  969, "2026-02-26", "2026-02-26", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  969, "2026-03-01", "2026-03-01", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  969, "2026-03-08", "2026-03-09", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid, valid
    (  969, "2026-03-10", "2026-03-12", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    # ── Group 970 ─────────────────────────────────────────────────────────────
    (  970, "2026-02-11", "2026-02-11", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    (  970, "2026-02-14", "2026-02-14", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    (  970, "2026-02-23", "2026-02-23", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    (  970, "2026-02-25", "2026-02-25", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  970, "2026-03-08", "2026-03-08", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    (  970, "2026-03-10", "2026-03-10", "unified_bee_frames_v1-3-11"),                  # valid
    (  970, "2026-03-16", "2026-03-17", "unified_bee_frames_v1-3-11"),                  # valid
    (  970, "2026-03-23", "2026-03-23", "unified_bee_frames_v1-3-11"),                  # invalid
    # ── Group 984 ─────────────────────────────────────────────────────────────
    (  984, "2026-02-08", "2026-02-09", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration, valid
    (  984, "2026-02-14", "2026-02-14", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    (  984, "2026-02-18", "2026-02-19", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    (  984, "2026-02-23", "2026-02-23", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    (  984, "2026-02-25", "2026-02-25", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    (  984, "2026-03-22", "2026-03-22", "unified_bee_frames_v1-3-11"),                  # valid
    # ── Group 1144 ────────────────────────────────────────────────────────────
    ( 1144, "2026-02-15", "2026-02-15", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    ( 1144, "2026-02-22", "2026-02-22", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1144, "2026-03-01", "2026-03-01", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1144, "2026-03-15", "2026-03-15", "unified_bee_frames_v1-3-11"),                  # invalid
    # ── Group 1155 ────────────────────────────────────────────────────────────
    ( 1155, "2026-02-07", "2026-02-09", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid, valid
    ( 1155, "2026-02-15", "2026-02-15", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    ( 1155, "2026-02-25", "2026-02-26", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid, valid
    ( 1155, "2026-03-03", "2026-03-03", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1155, "2026-03-05", "2026-03-05", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1155, "2026-03-08", "2026-03-09", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid, valid
    ( 1155, "2026-03-10", "2026-03-12", "unified_bee_frames_v1-3-11"),                  # valid
    ( 1155, "2026-03-15", "2026-03-15", "unified_bee_frames_v1-3-11"),                  # invalid
    ( 1155, "2026-03-17", "2026-03-19", "unified_bee_frames_v1-3-11"),                  # valid
    ( 1155, "2026-03-21", "2026-03-21", "unified_bee_frames_v1-3-11"),                  # invalid
    ( 1155, "2026-03-23", "2026-03-23", "unified_bee_frames_v1-3-11"),                  # invalid
    # ── Group 1618 ────────────────────────────────────────────────────────────
    ( 1618, "2026-02-16", "2026-02-19", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid, valid
    ( 1618, "2026-02-22", "2026-02-22", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    ( 1618, "2026-03-01", "2026-03-05", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1618, "2026-03-09", "2026-03-09", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1618, "2026-03-11", "2026-03-12", "unified_bee_frames_v1-3-11"),                  # valid
    ( 1618, "2026-03-14", "2026-03-19", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    # ── Group 1691 ────────────────────────────────────────────────────────────
    ( 1691, "2026-02-11", "2026-02-12", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1691, "2026-02-15", "2026-02-15", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1691, "2026-02-18", "2026-02-20", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid, valid
    ( 1691, "2026-03-01", "2026-03-02", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1691, "2026-03-09", "2026-03-09", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1691, "2026-03-10", "2026-03-12", "unified_bee_frames_v1-3-11"),                  # valid
    ( 1691, "2026-03-16", "2026-03-18", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    ( 1691, "2026-03-21", "2026-03-23", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    # ── Group 1693 ────────────────────────────────────────────────────────────
    ( 1693, "2026-01-27", "2026-01-29", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    ( 1693, "2026-02-18", "2026-02-18", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1693, "2026-02-22", "2026-02-23", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1693, "2026-03-01", "2026-03-01", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1693, "2026-03-09", "2026-03-09", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    ( 1693, "2026-03-10", "2026-03-12", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    ( 1693, "2026-03-19", "2026-03-19", "unified_bee_frames_v1-3-11"),                  # invalid
    ( 1693, "2026-03-21", "2026-03-21", "unified_bee_frames_v1-3-11"),                  # invalid
    # ── Group 1713 ────────────────────────────────────────────────────────────
    ( 1713, "2026-01-26", "2026-01-26", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    ( 1713, "2026-01-29", "2026-01-29", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1713, "2026-02-08", "2026-02-08", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1713, "2026-02-15", "2026-02-15", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    ( 1713, "2026-02-23", "2026-02-23", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1713, "2026-02-25", "2026-02-26", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid, valid
    ( 1713, "2026-02-28", "2026-02-28", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1713, "2026-03-02", "2026-03-02", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1713, "2026-03-09", "2026-03-09", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1713, "2026-03-10", "2026-03-12", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    ( 1713, "2026-03-19", "2026-03-19", "unified_bee_frames_v1-3-11"),                  # valid
    # ── Group 1723 ────────────────────────────────────────────────────────────
    ( 1723, "2026-02-01", "2026-02-02", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1723, "2026-02-15", "2026-02-19", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid, valid
    ( 1723, "2026-02-22", "2026-02-22", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1723, "2026-02-26", "2026-02-26", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1723, "2026-03-05", "2026-03-05", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1723, "2026-03-08", "2026-03-08", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1723, "2026-03-19", "2026-03-19", "unified_bee_frames_v1-3-11"),                  # valid
    # ── Group 1730 ────────────────────────────────────────────────────────────
    ( 1730, "2026-01-28", "2026-01-28", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    ( 1730, "2026-02-08", "2026-02-08", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    ( 1730, "2026-02-14", "2026-02-14", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    ( 1730, "2026-02-18", "2026-02-18", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    ( 1730, "2026-02-26", "2026-02-26", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    ( 1730, "2026-02-28", "2026-02-28", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    ( 1730, "2026-03-03", "2026-03-03", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1730, "2026-03-09", "2026-03-09", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    ( 1730, "2026-03-10", "2026-03-12", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    ( 1730, "2026-03-23", "2026-03-23", "unified_bee_frames_v1-3-11"),                  # valid
    # ── Group 1764 ────────────────────────────────────────────────────────────
    ( 1764, "2026-02-14", "2026-02-14", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    ( 1764, "2026-02-17", "2026-02-18", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid, needs_recalibration
    ( 1764, "2026-02-28", "2026-02-28", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    ( 1764, "2026-03-03", "2026-03-03", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1764, "2026-03-08", "2026-03-09", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1764, "2026-03-10", "2026-03-12", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    ( 1764, "2026-03-14", "2026-03-19", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    ( 1764, "2026-03-21", "2026-03-22", "unified_bee_frames_v1-3-11"),                  # invalid
    # ── Group 1768 ────────────────────────────────────────────────────────────
    ( 1768, "2026-01-26", "2026-01-26", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    ( 1768, "2026-01-29", "2026-01-29", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    ( 1768, "2026-02-10", "2026-02-10", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    ( 1768, "2026-02-14", "2026-02-14", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    ( 1768, "2026-02-17", "2026-02-19", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1768, "2026-02-22", "2026-02-22", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1768, "2026-03-01", "2026-03-01", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1768, "2026-03-09", "2026-03-09", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1768, "2026-03-10", "2026-03-12", "unified_bee_frames_v1-3-11"),                  # valid
    ( 1768, "2026-03-15", "2026-03-15", "unified_bee_frames_v1-3-11"),                  # valid
    ( 1768, "2026-03-19", "2026-03-19", "unified_bee_frames_v1-3-11"),                  # invalid
    ( 1768, "2026-03-22", "2026-03-23", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    # ── Group 1793 ────────────────────────────────────────────────────────────
    ( 1793, "2026-02-04", "2026-02-04", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    ( 1793, "2026-02-07", "2026-02-08", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid, valid
    ( 1793, "2026-02-10", "2026-02-10", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    ( 1793, "2026-03-04", "2026-03-05", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1793, "2026-03-08", "2026-03-08", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    ( 1793, "2026-03-10", "2026-03-12", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    ( 1793, "2026-03-14", "2026-03-16", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    ( 1793, "2026-03-19", "2026-03-19", "unified_bee_frames_v1-3-11"),                  # invalid
    # ── Group 1794 ────────────────────────────────────────────────────────────
    ( 1794, "2026-02-26", "2026-02-26", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1794, "2026-03-01", "2026-03-01", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1794, "2026-03-10", "2026-03-12", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    ( 1794, "2026-03-15", "2026-03-15", "unified_bee_frames_v1-3-11"),                  # invalid
    ( 1794, "2026-03-19", "2026-03-19", "unified_bee_frames_v1-3-11"),                  # invalid
    ( 1794, "2026-03-21", "2026-03-23", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    # ── Group 1838 ────────────────────────────────────────────────────────────
    ( 1838, "2026-01-28", "2026-01-29", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    ( 1838, "2026-02-15", "2026-02-15", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    ( 1838, "2026-02-18", "2026-02-18", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1838, "2026-02-23", "2026-02-23", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1838, "2026-02-26", "2026-02-26", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1838, "2026-03-01", "2026-03-01", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1838, "2026-03-04", "2026-03-04", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1838, "2026-03-11", "2026-03-11", "unified_bee_frames_v1-3-11"),                  # valid
    ( 1838, "2026-03-14", "2026-03-15", "unified_bee_frames_v1-3-11"),                  # invalid, needs_recalibration
    ( 1838, "2026-03-18", "2026-03-18", "unified_bee_frames_v1-3-11"),                  # valid
    # ── Group 1884 ────────────────────────────────────────────────────────────
    ( 1884, "2026-02-20", "2026-02-20", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    ( 1884, "2026-02-22", "2026-02-22", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1884, "2026-02-25", "2026-02-26", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 1884, "2026-03-09", "2026-03-09", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    ( 1884, "2026-03-16", "2026-03-16", "unified_bee_frames_v1-3-11"),                  # needs_recalibration
    ( 1884, "2026-03-19", "2026-03-19", "unified_bee_frames_v1-3-11"),                  # valid
    ( 1884, "2026-03-22", "2026-03-23", "unified_bee_frames_v1-3-11"),                  # invalid
    # ── Group 2703 ────────────────────────────────────────────────────────────
    ( 2703, "2026-02-04", "2026-02-04", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    ( 2703, "2026-02-14", "2026-02-14", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    ( 2703, "2026-02-19", "2026-02-19", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    ( 2703, "2026-02-22", "2026-02-22", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    ( 2703, "2026-02-26", "2026-02-26", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    ( 2703, "2026-03-02", "2026-03-02", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 2703, "2026-03-05", "2026-03-05", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    ( 2703, "2026-03-10", "2026-03-12", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    ( 2703, "2026-03-14", "2026-03-15", "unified_bee_frames_v1-3-11"),                  # valid
    ( 2703, "2026-03-19", "2026-03-19", "unified_bee_frames_v1-3-11"),                  # valid
    ( 2703, "2026-03-22", "2026-03-22", "unified_bee_frames_v1-3-11"),                  # invalid
    # ── Group 2777 ────────────────────────────────────────────────────────────
    ( 2777, "2026-01-20", "2026-01-20", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    ( 2777, "2026-01-26", "2026-01-26", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    ( 2777, "2026-01-29", "2026-01-29", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    ( 2777, "2026-01-31", "2026-01-31", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    ( 2777, "2026-02-22", "2026-02-22", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 2777, "2026-03-12", "2026-03-12", "unified_bee_frames_v1-3-11"),                  # valid
    ( 2777, "2026-03-15", "2026-03-15", "unified_bee_frames_v1-3-11"),                  # valid
    ( 2777, "2026-03-18", "2026-03-19", "unified_bee_frames_v1-3-11"),                  # valid
    ( 2777, "2026-03-21", "2026-03-22", "unified_bee_frames_v1-3-11"),                  # invalid
    # ── Group 2799 ────────────────────────────────────────────────────────────
    ( 2799, "2026-02-01", "2026-02-02", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid, valid
    ( 2799, "2026-02-08", "2026-02-09", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid, needs_recalibration
    ( 2799, "2026-02-12", "2026-02-12", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 2799, "2026-02-22", "2026-02-23", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 2799, "2026-03-01", "2026-03-01", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 2799, "2026-03-09", "2026-03-09", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    ( 2799, "2026-03-10", "2026-03-12", "unified_bee_frames_v1-3-11"),                  # valid
    ( 2799, "2026-03-15", "2026-03-16", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    ( 2799, "2026-03-23", "2026-03-23", "unified_bee_frames_v1-3-11"),                  # valid
    # ── Group 2805 ────────────────────────────────────────────────────────────
    ( 2805, "2026-02-08", "2026-02-09", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration, valid
    ( 2805, "2026-02-12", "2026-02-12", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    ( 2805, "2026-02-14", "2026-02-14", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    ( 2805, "2026-02-17", "2026-02-17", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 2805, "2026-03-08", "2026-03-09", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid, valid
    ( 2805, "2026-03-10", "2026-03-12", "unified_bee_frames_v1-3-11"),                  # valid
    ( 2805, "2026-03-15", "2026-03-15", "unified_bee_frames_v1-3-11"),                  # valid
    ( 2805, "2026-03-19", "2026-03-19", "unified_bee_frames_v1-3-11"),                  # invalid
    ( 2805, "2026-03-21", "2026-03-23", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    # ── Group 2834 ────────────────────────────────────────────────────────────
    ( 2834, "2026-01-27", "2026-01-29", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration, valid
    ( 2834, "2026-02-08", "2026-02-08", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    ( 2834, "2026-02-11", "2026-02-11", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    ( 2834, "2026-02-14", "2026-02-14", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    ( 2834, "2026-02-17", "2026-02-17", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    ( 2834, "2026-02-22", "2026-02-22", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    ( 2834, "2026-02-25", "2026-02-26", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid, valid
    ( 2834, "2026-02-28", "2026-02-28", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    ( 2834, "2026-03-03", "2026-03-03", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    ( 2834, "2026-03-08", "2026-03-08", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 2834, "2026-03-12", "2026-03-12", "unified_bee_frames_v1-3-11"),                  # valid
    ( 2834, "2026-03-15", "2026-03-19", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    # ── Group 2854 ────────────────────────────────────────────────────────────
    ( 2854, "2026-02-18", "2026-02-19", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    ( 2854, "2026-02-22", "2026-02-24", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 2854, "2026-03-01", "2026-03-01", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 2854, "2026-03-08", "2026-03-08", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 2854, "2026-03-15", "2026-03-15", "unified_bee_frames_v1-3-11"),                  # invalid
    ( 2854, "2026-03-18", "2026-03-19", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    ( 2854, "2026-03-22", "2026-03-22", "unified_bee_frames_v1-3-11"),                  # invalid
    # ── Group 2858 ────────────────────────────────────────────────────────────
    ( 2858, "2026-01-31", "2026-02-01", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid, needs_recalibration
    ( 2858, "2026-02-04", "2026-02-05", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    ( 2858, "2026-02-07", "2026-02-07", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    ( 2858, "2026-02-10", "2026-02-11", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid, valid
    ( 2858, "2026-02-14", "2026-02-15", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid
    ( 2858, "2026-02-17", "2026-02-17", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    ( 2858, "2026-02-25", "2026-02-26", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 2858, "2026-03-01", "2026-03-02", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 2858, "2026-03-04", "2026-03-04", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 2858, "2026-03-10", "2026-03-12", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    ( 2858, "2026-03-14", "2026-03-14", "unified_bee_frames_v1-3-11"),                  # invalid
    ( 2858, "2026-03-17", "2026-03-19", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    ( 2858, "2026-03-21", "2026-03-23", "unified_bee_frames_v1-3-11"),                  # invalid
    # ── Group 2889 ────────────────────────────────────────────────────────────
    ( 2889, "2026-02-14", "2026-02-14", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    ( 2889, "2026-02-18", "2026-02-19", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # invalid, valid
    ( 2889, "2026-02-22", "2026-02-25", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 2889, "2026-03-05", "2026-03-05", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 2889, "2026-03-08", "2026-03-08", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # needs_recalibration
    ( 2889, "2026-03-15", "2026-03-18", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    ( 2889, "2026-03-23", "2026-03-23", "unified_bee_frames_v1-3-11"),                  # invalid
    # ── Group 2901 ────────────────────────────────────────────────────────────
    ( 2901, "2026-03-01", "2026-03-01", "beeframes_supervised_snapshot_24_v6_1_1_OS"),  # valid
    ( 2901, "2026-03-15", "2026-03-15", "unified_bee_frames_v1-3-11"),                  # valid
    ( 2901, "2026-03-17", "2026-03-19", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    ( 2901, "2026-03-22", "2026-03-23", "unified_bee_frames_v1-3-11"),                  # invalid, valid
    # ── Group 2929 ────────────────────────────────────────────────────────────
    ( 2929, "2026-03-10", "2026-03-12", "unified_bee_frames_v1-3-11"),                  # valid
    ( 2929, "2026-03-15", "2026-03-16", "unified_bee_frames_v1-3-11"),                  # valid
    ( 2929, "2026-03-18", "2026-03-19", "unified_bee_frames_v1-3-11"),                  # valid
    ( 2929, "2026-03-22", "2026-03-22", "unified_bee_frames_v1-3-11"),                  # valid
]
