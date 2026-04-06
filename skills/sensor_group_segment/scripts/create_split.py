"""
Create stratified train/test split of local sample data.

Only valid and invalid samples are included in the split.
needs_recalibration samples remain in data/samples/ but are excluded from
train/ and test/ — they introduce noise and the final goal is binary
classification: valid vs invalid.

Reads ground truth labels, finds available (group_id, date) pairs in
data/samples/, and creates symlinks under data/samples/train/ and
data/samples/test/. A manifest CSV is saved for full traceability.

Split config: skills/sensor_group_segment/config/split_config.py

Usage:
    python skills/sensor_group_segment/scripts/create_split.py

Re-running is safe — existing symlinks are removed and recreated.
"""

import csv
import os
import sys
from collections import defaultdict
from pathlib import Path

SKILL_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT  = Path(__file__).resolve().parents[3]

sys.path.insert(0, str(SKILL_ROOT))

from config.split_config import (
    SPLIT_RATIO_TEST, RANDOM_SEED, GROUND_TRUTH_CSV, MANIFEST_PATH,
    SPLIT_STATUSES,
)

GT_CSV     = REPO_ROOT / GROUND_TRUTH_CSV
SAMPLES    = REPO_ROOT / "data/samples/temperature-export"
MANIFEST   = REPO_ROOT / MANIFEST_PATH
TRAIN_ROOT = SAMPLES / "train"
TEST_ROOT  = SAMPLES / "test"


def load_usable(gt_csv: Path, samples_dir: Path) -> list[dict]:
    """
    Return ground truth rows where:
      - status is in SPLIT_STATUSES (valid / invalid only)
      - a non-empty sample folder exists on disk
    needs_recalibration rows are skipped — kept in data/samples/ but not in the split.
    """
    rows = []
    with open(gt_csv) as f:
        for row in csv.DictReader(f):
            if row["status"] not in SPLIT_STATUSES:
                continue
            d = samples_dir / f"group_{row['group_id']}" / row["date"]
            if d.exists() and any(d.iterdir()):
                rows.append(row)
    return rows


def stratified_split(rows: list[dict], test_ratio: float) -> tuple[list[dict], list[dict]]:
    """
    Split rows into (train, test) stratified by status.
    Within each stratum, sort by (group_id, date) and pick every Nth row
    for test — guarantees group and date diversity in both sets.
    """
    by_status: dict[str, list] = defaultdict(list)
    for r in rows:
        by_status[r["status"]].append(r)

    train, test = [], []
    for status, items in by_status.items():
        items_sorted = sorted(items, key=lambda r: (int(r["group_id"]), r["date"]))
        n_test = max(1, round(len(items_sorted) * test_ratio))
        step = max(1, len(items_sorted) // n_test)
        test_indices = set(sorted(range(0, len(items_sorted), step))[:n_test])
        for i, item in enumerate(items_sorted):
            entry = {**item, "split": "test" if i in test_indices else "train"}
            (test if i in test_indices else train).append(entry)
    return train, test


def create_symlinks(entries: list[dict], samples_dir: Path) -> None:
    for entry in entries:
        split_root = TRAIN_ROOT if entry["split"] == "train" else TEST_ROOT
        src = (samples_dir / f"group_{entry['group_id']}" / entry["date"]).resolve()
        dst_group = split_root / f"group_{entry['group_id']}"
        dst_group.mkdir(parents=True, exist_ok=True)
        dst = dst_group / entry["date"]
        if dst.exists() or dst.is_symlink():
            dst.unlink()
        os.symlink(src, dst)


def save_manifest(entries: list[dict], path: Path) -> None:
    sorted_entries = sorted(entries, key=lambda r: (r["split"], int(r["group_id"]), r["date"]))
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["split", "group_id", "date", "status"])
        writer.writeheader()
        writer.writerows(sorted_entries)


def clear_split_dirs() -> None:
    """Remove all existing symlinks from train/ and test/ before recreating."""
    for root in [TRAIN_ROOT, TEST_ROOT]:
        if not root.exists():
            continue
        for link in root.rglob("*"):
            if link.is_symlink():
                link.unlink()
        # Remove empty subdirectories left behind
        for d in sorted(root.rglob("*"), reverse=True):
            if d.is_dir():
                try:
                    d.rmdir()
                except OSError:
                    pass


def main() -> None:
    print(f"Ground truth  : {GT_CSV}")
    print(f"Samples dir   : {SAMPLES}")
    print(f"Test ratio    : {SPLIT_RATIO_TEST}")
    print(f"Split statuses: {sorted(SPLIT_STATUSES)}  (needs_recalibration excluded)")
    print()

    print("Clearing existing train/test symlinks…")
    clear_split_dirs()

    usable = load_usable(GT_CSV, SAMPLES)
    print(f"Usable labeled pairs (valid + invalid only): {len(usable)}")

    train, test = stratified_split(usable, SPLIT_RATIO_TEST)

    from collections import Counter
    for split_name, split_rows in [("train", train), ("test", test)]:
        counts = Counter(r["status"] for r in split_rows)
        print(f"  {split_name}: total={len(split_rows)}  "
              f"valid={counts.get('valid', 0)}  "
              f"invalid={counts.get('invalid', 0)}")

    create_symlinks(train + test, SAMPLES)
    print(f"\nSymlinks → {TRAIN_ROOT}")
    print(f"           {TEST_ROOT}")

    save_manifest(train + test, MANIFEST)
    print(f"Manifest   → {MANIFEST}")


if __name__ == "__main__":
    main()
