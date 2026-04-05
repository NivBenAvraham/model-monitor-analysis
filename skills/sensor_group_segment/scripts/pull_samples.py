"""
Batch temperature data extraction — one Athena call per (group_id, date).

BEFORE RUNNING — refresh AWS credentials (valid 4 hours):
    source scripts/refresh_aws_credentials.sh

Usage:
    python skills/sensor_group_segment/scripts/pull_samples.py

Output:
    data/samples/group_{id}/{date}/{id}_{date}_hive_updates.parquet
    data/samples/group_{id}/{date}/{id}_{date}_sensor_temperature.parquet
    data/samples/group_{id}/{date}/{id}_{date}_gateway_temperature.parquet

Extraction plan: skills/sensor_group_segment/config/extraction_plan.py (derived from ground truth CSV).
Ground truth CSV: ground_truth/ground_truth_statuess_ca_2026.csv
"""

import sys
import logging
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from pyathena import connect

SKILL_ROOT = Path(__file__).resolve().parents[1]   # skills/sensor_group_segment/
REPO_ROOT  = Path(__file__).resolve().parents[3]   # repo root

sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(SKILL_ROOT))

from model_monitor.ingestion import raw_bee_frames_table, hive_updates_query
from config.extraction_plan import EXTRACTION_PLAN

# ---------------------------------------------------------------------------
# AWS config
# ---------------------------------------------------------------------------
S3_STAGING_DIR   = "s3://us-east-1-data-analytics-storage/athena-query-results/"
REGION           = "us-east-1"
WORKGROUP        = "data_analytics"
CURATED_DATABASE = "data_lake_curated_data"

TEMP_PACKAGE_PATH = Path(
    "/Users/nivbenavraham/Desktop/Codebase/Clones/beehero-streamlit-app"
)
OUTPUT_DIR = REPO_ROOT / "data/samples"

# ---------------------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger(__name__)


def get_connection(database: str):
    return connect(
        s3_staging_dir=S3_STAGING_DIR,
        region_name=REGION,
        schema_name=database,
        work_group=WORKGROUP,
    )


def build_schedule() -> dict[int, list[tuple[date, str]]]:
    """
    Expand EXTRACTION_PLAN into {group_id: [(date, model), ...]} sorted by date.
    Overlapping dates are deduplicated — first occurrence wins.
    """
    schedule: dict[int, dict[date, str]] = defaultdict(dict)
    for group_id, start_str, end_str, model in EXTRACTION_PLAN:
        current = date.fromisoformat(start_str)
        end     = date.fromisoformat(end_str)
        while current <= end:
            if current not in schedule[group_id]:
                schedule[group_id][current] = model
            current += timedelta(days=1)
    return {g: sorted(d.items()) for g, d in schedule.items()}


def pull_group(
    group_id: int,
    dates: list[tuple[date, str]],
    read_curated,
    load_sensor_samples,
    load_gateway_samples,
    add_hive_size_bucket,
) -> None:
    total = len(dates)

    for i, (d, model) in enumerate(dates, 1):
        date_str = d.isoformat()
        table    = raw_bee_frames_table(d)
        log.info(f"  [{i}/{total}] group={group_id}  date={date_str}  model={model}  table=data_lake_raw_data.{table}")

        out_dir = OUTPUT_DIR / f"group_{group_id}" / date_str
        if out_dir.exists() and any(out_dir.iterdir()):
            log.info(f"    → already exists, skipping")
            continue
        out_dir.mkdir(parents=True, exist_ok=True)

        try:
            g_tuple = (group_id, group_id)
            m_tuple = (model, model)

            hive_updates = read_curated(hive_updates_query(g_tuple, m_tuple, date_str))
            if hive_updates.empty:
                log.warning(f"    → no hive_updates for this date, skipping")
                out_dir.rmdir()
                continue

            sensor_samples  = load_sensor_samples(read_curated, hive_updates, date_str)
            gateway_samples = load_gateway_samples(read_curated, sensor_samples, date_str)

            if not sensor_samples.empty and not hive_updates.empty:
                sensor_samples = add_hive_size_bucket(sensor_samples, hive_updates)
                sg = hive_updates[["sensor_mac_address", "group_id"]].drop_duplicates("sensor_mac_address", keep="last")
                sensor_samples = sensor_samples.merge(sg, on="sensor_mac_address", how="left")

            if not gateway_samples.empty and "group_id" in sensor_samples.columns:
                gsg = (sensor_samples[["gateway_mac_address", "group_id"]]
                       .dropna(subset=["group_id"])
                       .drop_duplicates("gateway_mac_address", keep="first"))
                gateway_samples = gateway_samples.merge(gsg, on="gateway_mac_address", how="left")

            for name, df in [
                ("hive_updates",        hive_updates),
                ("sensor_temperature",  sensor_samples),
                ("gateway_temperature", gateway_samples),
            ]:
                path = out_dir / f"{group_id}_{date_str}_{name}.parquet"
                df.to_parquet(path, index=False)

            log.info(f"    → hive_updates={len(hive_updates)}  sensor={len(sensor_samples)}  gateway={len(gateway_samples)}")

        except Exception as e:
            log.error(f"    ✗ FAILED {date_str}: {e}")
            continue


def main() -> None:
    if str(TEMP_PACKAGE_PATH) not in sys.path:
        sys.path.insert(0, str(TEMP_PACKAGE_PATH))

    try:
        from temperature_data_export_package.data_extraction import (
            load_sensor_samples,
            load_gateway_samples,
        )
        from temperature_data_export_package.utils.data_bins import add_hive_size_bucket
    except ImportError as e:
        log.error(f"Cannot import temperature_data_export_package: {e}")
        log.error(f"Check TEMP_PACKAGE_PATH = {TEMP_PACKAGE_PATH}")
        return

    curated_conn = get_connection(CURATED_DATABASE)

    def read_curated(sql: str) -> pd.DataFrame:
        return pd.read_sql(sql, curated_conn)

    schedule    = build_schedule()
    total_calls = sum(len(v) for v in schedule.values())
    log.info(f"Schedule: {len(schedule)} group(s), {total_calls} Athena call(s)")

    for group_id, dates in sorted(schedule.items()):
        log.info(f"\n── Group {group_id}  ({len(dates)} dates) ──")
        pull_group(group_id, dates, read_curated, load_sensor_samples, load_gateway_samples, add_hive_size_bucket)

    log.info("\nDone. Summary:")
    for group_dir in sorted(OUTPUT_DIR.iterdir()):
        dates_pulled = sorted(d.name for d in group_dir.iterdir() if d.is_dir())
        if dates_pulled:
            log.info(f"  {group_dir.name}: {len(dates_pulled)} dates — {dates_pulled[0]} → {dates_pulled[-1]}")
        else:
            log.info(f"  {group_dir.name}: 0 dates")


if __name__ == "__main__":
    main()
