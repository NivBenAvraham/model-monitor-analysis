"""
Create group-level train/test split of local sample data.

Method: Group-level assignment — every date from a group goes entirely to
train OR entirely to test.  Groups are never split across the boundary.

This replaces the older row-level stratified split (archived in
data/samples/split_30_4/).

Design decisions
────────────────
Anchor constraint
  23 groups are locked to train because they appear in the analyst
  plot-decision review files:
    • skills/hives_temperature_plot_decision/history/2026-04-15/invalid.txt
    • skills/hives_temperature_plot_decision/data_analyst_plot_decisions/valid.txt
  These are the gold-standard examples used to derive and verify the
  gate set in temperature_health_rule.py.  They must never appear in the
  test set.

Remaining groups
  28 free groups are assigned to train/test by optimised search (hill
  climbing, 200k iterations, seed 42) minimising |test_rows − 25%| and
  class-imbalance divergence from the overall valid/invalid ratio.

Result (547 usable rows — valid + invalid only)
  Train: 36 groups / 410 rows (75.0%) — valid 256, invalid 154
  Test : 15 groups / 137 rows (25.0%) — valid  85, invalid  52
  Class ratio: train 62.4% valid / test 62.0% valid (overall 62.3%)

Symlinks are written to two locations:
  data/samples/temperature-export/train|test/   ← used by run.py / evaluate_metrics.py
  data/samples/train|test/                      ← used by calibrate_thresholds.py

Usage:
    python skills/sensor_group_segment/scripts/create_split.py

Re-running is safe — existing symlinks are removed and recreated.
"""

import csv
import os
import sys
from pathlib import Path

SKILL_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT  = Path(__file__).resolve().parents[3]

sys.path.insert(0, str(SKILL_ROOT))

from config.split_config import (
    GROUND_TRUTH_CSV, MANIFEST_PATH, SPLIT_STATUSES,
    TRAIN_GROUPS, TEST_GROUPS,
)

GT_CSV   = REPO_ROOT / GROUND_TRUTH_CSV
MANIFEST = REPO_ROOT / MANIFEST_PATH

# Primary split dirs (used by run.py, evaluate_metrics.py)
TE_SAMPLES    = REPO_ROOT / "data/samples/temperature-export"
TE_TRAIN_ROOT = TE_SAMPLES / "train"
TE_TEST_ROOT  = TE_SAMPLES / "test"

# Secondary split dirs (used by calibrate_thresholds.py)
SAMPLES_ROOT   = REPO_ROOT / "data/samples"
ALT_TRAIN_ROOT = SAMPLES_ROOT / "train"
ALT_TEST_ROOT  = SAMPLES_ROOT / "test"


def load_usable(gt_csv: Path) -> list[dict]:
    """
    Return GT rows where status is in SPLIT_STATUSES (valid / invalid).
    needs_recalibration rows are skipped.
    """
    rows = []
    with open(gt_csv) as f:
        for row in csv.DictReader(f):
            if row["status"] not in SPLIT_STATUSES:
                continue
            gid = int(row["group_id"])
            if gid not in TRAIN_GROUPS and gid not in TEST_GROUPS:
                print(f"  WARNING: group {gid} not in TRAIN_GROUPS or TEST_GROUPS — skipped")
                continue
            rows.append(row)
    return rows


def assign_split(rows: list[dict]) -> list[dict]:
    """Assign 'split' field based on group-level membership."""
    result = []
    for row in rows:
        gid = int(row["group_id"])
        split = "train" if gid in TRAIN_GROUPS else "test"
        result.append({**row, "split": split})
    return result


def _clear_dir(root: Path) -> None:
    """Remove all symlinks and empty subdirs under root."""
    if not root.exists():
        return
    for link in root.rglob("*"):
        if link.is_symlink():
            link.unlink()
    for d in sorted(root.rglob("*"), reverse=True):
        if d.is_dir():
            try:
                d.rmdir()
            except OSError:
                pass


def _make_symlink(src: Path, dst_root: Path, group_id: str, date: str) -> bool:
    """
    Create dst_root/group_{group_id}/{date} → src.
    Returns True if src exists (even if it was an existing link we replaced).
    """
    if not src.exists():
        return False
    dst_group = dst_root / f"group_{group_id}"
    dst_group.mkdir(parents=True, exist_ok=True)
    dst = dst_group / date
    if dst.exists() or dst.is_symlink():
        dst.unlink()
    os.symlink(src, dst)
    return True


def create_symlinks(entries: list[dict]) -> int:
    """
    Write symlinks into both split-dir pairs.
    Returns count of entries skipped (sample folder not on disk).
    """
    skipped = 0
    for entry in entries:
        gid    = entry["group_id"]
        date   = entry["date"]
        split  = entry["split"]

        te_src  = (TE_SAMPLES  / f"group_{gid}" / date).resolve()
        te_root = TE_TRAIN_ROOT if split == "train" else TE_TEST_ROOT
        te_ok   = _make_symlink(te_src, te_root, gid, date)

        alt_root = ALT_TRAIN_ROOT if split == "train" else ALT_TEST_ROOT
        _make_symlink(te_src, alt_root, gid, date)

        if not te_ok:
            print(f"  MISSING sample: group_{gid}/{date} — symlink skipped")
            skipped += 1
    return skipped


def save_manifest(entries: list[dict]) -> None:
    fieldnames = ["split", "group_id", "date", "status"]
    sorted_entries = sorted(
        entries,
        key=lambda r: (r["split"], int(r["group_id"]), r["date"]),
    )
    with open(MANIFEST, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(sorted_entries)


def main() -> None:
    print(f"Ground truth : {GT_CSV}")
    print(f"Manifest     : {MANIFEST}")
    print(f"Train groups : {len(TRAIN_GROUPS)}")
    print(f"Test groups  : {len(TEST_GROUPS)}")
    print()

    print("Clearing existing symlinks…")
    for d in [TE_TRAIN_ROOT, TE_TEST_ROOT, ALT_TRAIN_ROOT, ALT_TEST_ROOT]:
        _clear_dir(d)

    rows   = load_usable(GT_CSV)
    entries = assign_split(rows)

    from collections import Counter
    for split_name in ("train", "test"):
        split_rows = [e for e in entries if e["split"] == split_name]
        counts = Counter(r["status"] for r in split_rows)
        groups = len({r["group_id"] for r in split_rows})
        print(
            f"  {split_name}: groups={groups}  rows={len(split_rows)}  "
            f"valid={counts.get('valid', 0)}  invalid={counts.get('invalid', 0)}"
        )

    print()
    print("Creating symlinks…")
    skipped = create_symlinks(entries)

    print(f"\nPrimary   → {TE_TRAIN_ROOT}")
    print(f"           {TE_TEST_ROOT}")
    print(f"Secondary → {ALT_TRAIN_ROOT}")
    print(f"           {ALT_TEST_ROOT}")
    if skipped:
        print(f"\n  {skipped} entries skipped (samples not pulled yet)")

    save_manifest(entries)
    print(f"Manifest   → {MANIFEST}")


if __name__ == "__main__":
    main()
