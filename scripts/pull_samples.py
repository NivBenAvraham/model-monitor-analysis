"""
Pull local development samples from Athena.

Saves CSV files to data/samples/ (gitignored — never committed).
Run this once to get data for offline notebook work and local development.

Usage:
    python scripts/pull_samples.py
    python scripts/pull_samples.py --days 14 --limit 2000

Requirements:
    pip install -e ".[dev]"
    AWS credentials configured (aws configure or environment variables)
"""

import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from pyathena import connect

# ---------------------------------------------------------------------------
# Configuration — fill in your values
# ---------------------------------------------------------------------------

S3_STAGING_DIR = "s3://YOUR_BUCKET/athena-results/"  # TODO: set your Athena S3 staging bucket
REGION = "us-east-1"                                  # TODO: set your AWS region
CURATED_DATABASE = "data_lake_curated_data"           # TODO: confirm curated DB name
RAW_DATABASE = "data_lake_raw_data"                   # TODO: confirm raw DB name
WORKGROUP = "primary"                                 # TODO: set your Athena workgroup if not default

OUTPUT_DIR = Path("data/samples")

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


def pull(name: str, sql: str, database: str) -> pd.DataFrame:
    log.info(f"Pulling {name} ...")
    conn = get_connection(database)
    df = pd.read_sql(sql, conn)
    log.info(f"  → {len(df)} rows")
    return df


def save(df: pd.DataFrame, name: str) -> None:
    path = OUTPUT_DIR / f"{name}.csv"
    df.to_csv(path, index=False)
    log.info(f"  Saved to {path}")


def pull_all(days: int, limit: int) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    start_date = (datetime.today() - timedelta(days=days)).strftime("%Y-%m-%d")
    end_date = datetime.today().strftime("%Y-%m-%d")
    log.info(f"Date range: {start_date} → {end_date}  |  row limit per table: {limit}")

    # ------------------------------------------------------------------
    # 1. Preprocess (latest run per sensor per date)
    # ------------------------------------------------------------------
    df = pull(
        "preprocess",
        f"""
        SELECT *
        FROM (
            SELECT *,
                row_number() OVER (
                    PARTITION BY mac, date, model_name
                    ORDER BY run_timestamp DESC
                ) AS rn
            FROM beekeeper_beeframe_model_monitoring_preprocess
            WHERE date BETWEEN date('{start_date}') AND date('{end_date}')
        )
        WHERE rn = 1
        LIMIT {limit}
        """,
        CURATED_DATABASE,
    )
    save(df, "preprocess")

    # ------------------------------------------------------------------
    # 2. Validations
    # ------------------------------------------------------------------
    df = pull(
        "validations",
        f"""
        SELECT timestamp, group_id, tier1_status, tier2_status
        FROM beekeeper_beeframe_model_monitoring_validations
        WHERE timestamp BETWEEN timestamp '{start_date}' AND timestamp '{end_date}'
        LIMIT {limit}
        """,
        CURATED_DATABASE,
    )
    save(df, "validations")

    # ------------------------------------------------------------------
    # 3. Ops inspections
    # ------------------------------------------------------------------
    df = pull(
        "ops_inspections",
        f"""
        SELECT *
        FROM ops_inspections
        WHERE date(utc_timestamp) BETWEEN date('{start_date}') AND date('{end_date}')
        LIMIT {limit}
        """,
        CURATED_DATABASE,
    )
    save(df, "ops_inspections")

    # ------------------------------------------------------------------
    # 4. Yard inspections
    # ------------------------------------------------------------------
    df = pull(
        "yard_inspections",
        f"""
        SELECT *
        FROM yard_inspections
        WHERE date(utc_end_time) BETWEEN date('{start_date}') AND date('{end_date}')
        LIMIT {limit}
        """,
        CURATED_DATABASE,
    )
    save(df, "yard_inspections")

    # ------------------------------------------------------------------
    # 5. Daily hive health
    # ------------------------------------------------------------------
    df = pull(
        "daily_hive_health",
        f"""
        SELECT run_date, sensor_mac_address, group_id, yard_id, is_healthy
        FROM daily_hive_health_monitoring
        WHERE is_healthy IS NOT NULL
          AND run_date BETWEEN date('{start_date}') AND date('{end_date}')
        LIMIT {limit}
        """,
        CURATED_DATABASE,
    )
    save(df, "daily_hive_health")

    # ------------------------------------------------------------------
    # 6. Sensor temperature samples
    # ------------------------------------------------------------------
    df = pull(
        "sensor_temperature",
        f"""
        SELECT sensor_mac_address, timestamp, pcb_temperature_one,
               gateway_mac_address, humidity
        FROM sensor_samples_curated
        WHERE date(timestamp) BETWEEN date('{start_date}') AND date('{end_date}')
          AND pcb_temperature_one BETWEEN -30 AND 100
        LIMIT {limit}
        """,
        CURATED_DATABASE,
    )
    save(df, "sensor_temperature")

    # ------------------------------------------------------------------
    # 7. Gateway (ambient) temperature samples
    # ------------------------------------------------------------------
    df = pull(
        "gateway_temperature",
        f"""
        SELECT gateway_mac_address, timestamp, pcb_temperature_two
        FROM gateway_samples_curated
        WHERE date(timestamp) BETWEEN date('{start_date}') AND date('{end_date}')
          AND pcb_temperature_two IS NOT NULL
        LIMIT {limit}
        """,
        CURATED_DATABASE,
    )
    save(df, "gateway_temperature")

    # ------------------------------------------------------------------
    # 8. Hive updates (bee_frames ground truth)
    # ------------------------------------------------------------------
    df = pull(
        "hive_updates",
        f"""
        SELECT sensor_mac_address, created, group_id, bee_frames, model
        FROM supervised_beeframes
        WHERE date(log_timestamp) BETWEEN date('{start_date}') AND date('{end_date}')
        LIMIT {limit}
        """,
        RAW_DATABASE,
    )
    save(df, "hive_updates")

    log.info("Done. Files saved to data/samples/")
    log.info("Expected files:")
    for f in sorted(OUTPUT_DIR.glob("*.csv")):
        log.info(f"  {f.name}  ({f.stat().st_size // 1024} KB)")


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pull Athena samples to data/samples/")
    parser.add_argument("--days", type=int, default=7, help="How many days back to pull (default: 7)")
    parser.add_argument("--limit", type=int, default=1000, help="Max rows per table (default: 1000)")
    args = parser.parse_args()

    pull_all(days=args.days, limit=args.limit)
