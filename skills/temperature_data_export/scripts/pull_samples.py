"""
Temperature data extraction — Athena → local Parquet.

Pulls hive_updates, sensor_temperature, and gateway_temperature for each
(group_id, date) and saves three Parquet files per pair.

BEFORE RUNNING — refresh AWS credentials (expire every 4 hours):
    source scripts/refresh_aws_credentials.sh

Usage
─────
# Single pair
python skills/temperature_data_export/scripts/pull_samples.py \\
    --group-id 1144 --date 2026-02-22

# All dates for one group (from extraction plan)
python skills/temperature_data_export/scripts/pull_samples.py --group-id 1144

# Full extraction plan (default)
python skills/temperature_data_export/scripts/pull_samples.py

# Control parallelism (default: 8 workers)
python skills/temperature_data_export/scripts/pull_samples.py --workers 4

# Force re-pull even if output already exists
python skills/temperature_data_export/scripts/pull_samples.py --group-id 1144 --date 2026-02-22 --force

Output
──────
data/samples/temperature-export/group_{id}/{date}/{id}_{date}_hive_updates.parquet
data/samples/temperature-export/group_{id}/{date}/{id}_{date}_sensor_temperature.parquet
data/samples/temperature-export/group_{id}/{date}/{id}_{date}_gateway_temperature.parquet
"""

from __future__ import annotations

import argparse
import logging
import sys
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from pyathena import connect

SKILL_ROOT = Path(__file__).resolve().parents[1]   # skills/temperature_data_export/
REPO_ROOT  = Path(__file__).resolve().parents[3]   # repo root

sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(SKILL_ROOT))

from scripts.queries import hive_updates_query, sensor_samples_query, gateway_samples_query
from scripts.transforms import sensor_samples_preprocess, add_hive_size_bucket
from config.extraction_plan import EXTRACTION_PLAN

# ---------------------------------------------------------------------------
# AWS / Athena config
# ---------------------------------------------------------------------------
S3_STAGING_DIR   = "s3://us-east-1-data-analytics-storage/athena-query-results/"
REGION           = "us-east-1"
WORKGROUP        = "data_analytics"
CURATED_DATABASE = "data_lake_curated_data"

OUTPUT_DIR       = REPO_ROOT / "data" / "samples" / "temperature-export"
DEFAULT_WORKERS  = 8

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Per-thread Athena connection (pyathena is not thread-safe)
# ---------------------------------------------------------------------------
_thread_local = threading.local()


def _get_curated_conn():
    if not hasattr(_thread_local, "conn"):
        _thread_local.conn = connect(
            s3_staging_dir=S3_STAGING_DIR,
            region_name=REGION,
            schema_name=CURATED_DATABASE,
            work_group=WORKGROUP,
        )
    return _thread_local.conn


def _read(sql: str) -> pd.DataFrame:
    return pd.read_sql(sql, _get_curated_conn())


# ---------------------------------------------------------------------------
# Task definition
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Task:
    group_id: int
    date_str: str
    model: str


# ---------------------------------------------------------------------------
# Extraction plan helpers
# ---------------------------------------------------------------------------

def plan_to_tasks(plan=None) -> list[Task]:
    """Expand EXTRACTION_PLAN into a flat list of Tasks (one per day)."""
    if plan is None:
        plan = EXTRACTION_PLAN
    schedule: dict[tuple[int, date], str] = {}
    for group_id, start_str, end_str, model in plan:
        current = date.fromisoformat(start_str)
        end     = date.fromisoformat(end_str)
        while current <= end:
            key = (group_id, current)
            if key not in schedule:          # first occurrence wins on overlap
                schedule[key] = model
            current += timedelta(days=1)
    return [Task(g, d.isoformat(), m) for (g, d), m in sorted(schedule.items())]


def filter_tasks(tasks: list[Task], group_id: int | None, date_str: str | None) -> list[Task]:
    if group_id is not None:
        tasks = [t for t in tasks if t.group_id == group_id]
    if date_str is not None:
        tasks = [t for t in tasks if t.date_str == date_str]
    return tasks


# ---------------------------------------------------------------------------
# Single (group_id, date) extraction — runs inside a worker thread
# ---------------------------------------------------------------------------

def _pull_one(task: Task, output_dir: Path, force: bool) -> str:
    """
    Pull one (group_id, date). Returns a status string for logging.

    Pipeline:
      1. hive_updates   → which sensors + bee_frames
      2. sensor_samples → pcb_temperature_one (preprocess + resample 30 min)
      3. gateway_samples→ pcb_temperature_two (filter zero-accel)
    """
    out_dir = output_dir / f"group_{task.group_id}" / task.date_str

    if not force and out_dir.exists() and any(out_dir.iterdir()):
        return f"SKIP  group={task.group_id}  date={task.date_str} (already exists)"

    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        # ── 1. Hive updates ──────────────────────────────────────────────
        hive_updates = _read(
            hive_updates_query([task.group_id], [task.model], task.date_str)
        )
        if hive_updates.empty:
            out_dir.rmdir()
            return f"EMPTY group={task.group_id}  date={task.date_str} (no hive_updates)"

        # Keep latest update per sensor
        hive_updates = (
            hive_updates
            .sort_values("created")
            .drop_duplicates("sensor_mac_address", keep="last")
            .reset_index(drop=True)
        )

        # ── 2. Sensor samples ────────────────────────────────────────────
        sensors = hive_updates["sensor_mac_address"].tolist()
        sensor_df = _read(sensor_samples_query(sensors, task.date_str))
        sensor_df = sensor_df.drop_duplicates(subset=["sensor_mac_address", "timestamp"])

        if not sensor_df.empty:
            # Capture gateway mapping before preprocessing drops rows
            sensor_to_gw = (
                sensor_df[["sensor_mac_address", "gateway_mac_address"]]
                .drop_duplicates()
            )
            sensor_df = sensor_samples_preprocess(sensor_df)

            # Resample to 30-min means
            sensor_df = (
                sensor_df[["sensor_mac_address", "timestamp", "pcb_temperature_one"]]
                .groupby("sensor_mac_address", group_keys=True)
                .resample("30min", on="timestamp")
                .mean(numeric_only=True)
                .reset_index()
            )
            sensor_df = sensor_df.merge(sensor_to_gw, on="sensor_mac_address", how="left")
            sensor_df = add_hive_size_bucket(sensor_df, hive_updates)

            # Attach group_id
            sg = (
                hive_updates[["sensor_mac_address", "group_id"]]
                .drop_duplicates("sensor_mac_address", keep="last")
            )
            sensor_df = sensor_df.merge(sg, on="sensor_mac_address", how="left")

        # ── 3. Gateway samples ───────────────────────────────────────────
        gateway_df = pd.DataFrame()
        if not sensor_df.empty and "gateway_mac_address" in sensor_df.columns:
            gateways = sensor_df["gateway_mac_address"].dropna().unique().tolist()
            if gateways:
                gateway_df = _read(gateway_samples_query(gateways, task.date_str))
                gateway_df = gateway_df.drop_duplicates(
                    subset=["gateway_mac_address", "timestamp"]
                )
                if not gateway_df.empty:
                    # Filter out zero-acceleration readings (static/bad hardware)
                    accel_cols = ["acceleration_x", "acceleration_y", "acceleration_z"]
                    gateway_df = gateway_df.dropna(subset=["pcb_temperature_two"])
                    gateway_df = gateway_df[
                        ~(gateway_df[accel_cols] == 0).all(axis=1)
                    ].reset_index(drop=True)

                    # Attach group_id from sensor mapping
                    if "group_id" in sensor_df.columns:
                        gw_group = (
                            sensor_df[["gateway_mac_address", "group_id"]]
                            .dropna(subset=["group_id"])
                            .drop_duplicates("gateway_mac_address", keep="first")
                        )
                        gateway_df = gateway_df.merge(
                            gw_group, on="gateway_mac_address", how="left"
                        )

        # ── Save ─────────────────────────────────────────────────────────
        prefix = f"{task.group_id}_{task.date_str}"
        for name, df in [
            ("hive_updates",        hive_updates),
            ("sensor_temperature",  sensor_df),
            ("gateway_temperature", gateway_df),
        ]:
            df.to_parquet(out_dir / f"{prefix}_{name}.parquet", index=False)

        return (
            f"OK    group={task.group_id}  date={task.date_str}"
            f"  sensors={len(sensor_df):,}  gw={len(gateway_df):,}"
        )

    except Exception as exc:
        log.error(f"FAIL  group={task.group_id}  date={task.date_str}: {exc}")
        return f"FAIL  group={task.group_id}  date={task.date_str}: {exc}"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Pull temperature samples from Athena → local Parquet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--group-id", type=int, default=None,
        help="Pull only this group (default: all groups from extraction plan)",
    )
    p.add_argument(
        "--date", type=str, default=None, metavar="YYYY-MM-DD",
        help="Pull only this date (requires --group-id, or filters plan to this date)",
    )
    p.add_argument(
        "--workers", type=int, default=DEFAULT_WORKERS,
        help=f"Parallel Athena workers (default: {DEFAULT_WORKERS})",
    )
    p.add_argument(
        "--output", type=str, default=str(OUTPUT_DIR),
        help=f"Output directory (default: {OUTPUT_DIR})",
    )
    p.add_argument(
        "--force", action="store_true",
        help="Re-pull even if output already exists",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    # If --group-id + --date given directly (not in plan), build a synthetic task
    if args.group_id is not None and args.date is not None:
        all_tasks = plan_to_tasks()
        tasks = filter_tasks(all_tasks, args.group_id, args.date)
        if not tasks:
            # Not in extraction plan — try to find any model from the plan for this group
            group_tasks = filter_tasks(all_tasks, args.group_id, None)
            if group_tasks:
                model = group_tasks[0].model   # use first known model as fallback
                tasks = [Task(args.group_id, args.date, model)]
                log.warning(
                    f"Date {args.date} not in extraction plan for group {args.group_id}. "
                    f"Using model '{model}' as fallback."
                )
            else:
                log.error(
                    f"Group {args.group_id} not found in extraction plan. "
                    "Cannot determine model name. Aborting."
                )
                sys.exit(1)
    else:
        all_tasks = plan_to_tasks()
        tasks = filter_tasks(all_tasks, args.group_id, args.date)

    if not tasks:
        log.error("No tasks matched. Check --group-id / --date arguments.")
        sys.exit(1)

    output_dir = Path(args.output)
    log.info(
        f"Tasks: {len(tasks)} | Workers: {args.workers} | Output: {output_dir}"
    )

    results: list[str] = []
    with ThreadPoolExecutor(max_workers=args.workers, thread_name_prefix="athena") as pool:
        futures = {
            pool.submit(_pull_one, task, output_dir, args.force): task
            for task in tasks
        }
        for future in as_completed(futures):
            status = future.result()
            results.append(status)
            log.info(status)

    # ── Summary ──────────────────────────────────────────────────────────────
    ok   = sum(1 for r in results if r.startswith("OK"))
    skip = sum(1 for r in results if r.startswith("SKIP"))
    fail = sum(1 for r in results if r.startswith("FAIL"))
    empty = sum(1 for r in results if r.startswith("EMPTY"))

    log.info(
        f"\nDone — OK: {ok}  SKIP: {skip}  EMPTY: {empty}  FAIL: {fail}  "
        f"(total: {len(results)})"
    )
    if fail:
        log.warning("Some tasks failed — check logs above for details.")


if __name__ == "__main__":
    main()
