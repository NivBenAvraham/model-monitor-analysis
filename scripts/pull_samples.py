"""
Pull local development samples from Athena and save to data/samples/ (gitignored).

For temperature data, uses load_temperature_data() from temperature_data_export_package
so all preprocessing (outlier removal, 30-min resample, negative-temp fix) is applied.

For model monitoring data, queries Athena directly.

Usage:
    python scripts/pull_samples.py

Requirements:
    pip install -e ".[dev]"
    AWS credentials configured (aws configure or environment variables)
"""

import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from pyathena import connect

# ---------------------------------------------------------------------------
# Fill in these values before running
# ---------------------------------------------------------------------------

S3_STAGING_DIR = "s3://YOUR_BUCKET/athena-results/"        # Athena S3 staging bucket
REGION         = "us-east-1"                               # AWS region
WORKGROUP      = "primary"                                 # Athena workgroup

# Temperature data inputs
GROUP_IDS = [36]                                           # list of group_id integers to sample
DATE      = "2026-02-15"                                   # YYYY-MM-DD — date to pull temp data for
MODELS    = ["beeframes_supervised_snapshot_24_v6_1_1_OS"] # model name(s) to filter by

# Monitoring tables: date range
DAYS_BACK = 7        # how many days back from today
LIMIT     = 1000     # max rows per monitoring table

# Database names (match your Athena Glue catalog)
CURATED_DATABASE = "data_lake_curated_data"
RAW_DATABASE     = "data_lake_raw_data"

# Path to the temperature package (adjust if your clone is elsewhere)
TEMP_PACKAGE_PATH = Path(
    "/Users/nivbenavraham/Desktop/Codebase/Clones/beehero-streamlit-app"
)

# Output directory (gitignored)
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


def pull_sql(name: str, sql: str, database: str) -> pd.DataFrame:
    log.info(f"Pulling {name} ...")
    df = pd.read_sql(sql, get_connection(database))
    log.info(f"  → {len(df)} rows")
    return df


def save(df: pd.DataFrame, name: str) -> None:
    path = OUTPUT_DIR / f"{name}.csv"
    df.to_csv(path, index=False)
    log.info(f"  Saved → {path}  ({path.stat().st_size // 1024} KB)")


# ---------------------------------------------------------------------------
# Temperature data — via temperature_data_export_package
# ---------------------------------------------------------------------------

def pull_temperature() -> None:
    """
    Use load_temperature_data() from the temperature package.
    This applies all preprocessing: outlier removal, negative-temp fix, 30-min resample.
    Produces: hive_updates.csv, sensor_temperature.csv, gateway_temperature.csv
    """
    if str(TEMP_PACKAGE_PATH) not in sys.path:
        sys.path.insert(0, str(TEMP_PACKAGE_PATH))

    try:
        from temperature_data_export_package.data_extraction import load_temperature_data
    except ImportError as e:
        log.error(f"Cannot import temperature_data_export_package: {e}")
        log.error(f"Check TEMP_PACKAGE_PATH = {TEMP_PACKAGE_PATH}")
        return

    curated_conn = get_connection(CURATED_DATABASE)

    def read_curated(sql: str) -> pd.DataFrame:
        return pd.read_sql(sql, curated_conn)

    log.info(f"Pulling temperature data: group_ids={GROUP_IDS}, date={DATE}, models={MODELS}")
    date_obj = datetime.strptime(DATE, "%Y-%m-%d").date()

    hive_updates, sensor_samples, gateway_samples = load_temperature_data(
        read_curated=read_curated,
        read_raw=None,
        group_ids=GROUP_IDS,
        date=date_obj,
        models=MODELS,
        add_hive_size_buckets=True,
    )

    save(hive_updates,     "hive_updates")
    save(sensor_samples,   "sensor_temperature")
    save(gateway_samples,  "gateway_temperature")


# ---------------------------------------------------------------------------
# Model monitoring tables — direct Athena queries
# ---------------------------------------------------------------------------

def pull_monitoring() -> None:
    start_date = (datetime.today() - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%d")
    end_date   = datetime.today().strftime("%Y-%m-%d")
    log.info(f"Pulling monitoring tables: {start_date} → {end_date}  limit={LIMIT}")

    # 1. Preprocess (latest run per sensor per date — always dedup with row_number)
    save(pull_sql("preprocess", f"""
        SELECT * FROM (
            SELECT *,
                row_number() OVER (
                    PARTITION BY mac, date, model_name
                    ORDER BY run_timestamp DESC
                ) AS rn
            FROM beekeeper_beeframe_model_monitoring_preprocess
            WHERE date BETWEEN date('{start_date}') AND date('{end_date}')
        ) WHERE rn = 1
        LIMIT {LIMIT}
    """, CURATED_DATABASE), "preprocess")

    # 2. Validations (tier1 / tier2 human decisions)
    save(pull_sql("validations", f"""
        SELECT timestamp, group_id, tier1_status, tier2_status
        FROM beekeeper_beeframe_model_monitoring_validations
        WHERE timestamp BETWEEN timestamp '{start_date}' AND timestamp '{end_date}'
        LIMIT {LIMIT}
    """, CURATED_DATABASE), "validations")

    # 3. Ops inspections
    save(pull_sql("ops_inspections", f"""
        SELECT *
        FROM ops_inspections
        WHERE date(utc_timestamp) BETWEEN date('{start_date}') AND date('{end_date}')
        LIMIT {LIMIT}
    """, CURATED_DATABASE), "ops_inspections")

    # 4. Yard inspections (bee_frames_distribution is a JSON string — parse with ast.literal_eval)
    save(pull_sql("yard_inspections", f"""
        SELECT *
        FROM yard_inspections
        WHERE date(utc_end_time) BETWEEN date('{start_date}') AND date('{end_date}')
        LIMIT {LIMIT}
    """, CURATED_DATABASE), "yard_inspections")

    # 5. Daily hive health
    save(pull_sql("daily_hive_health", f"""
        SELECT run_date, sensor_mac_address, group_id, yard_id, is_healthy
        FROM daily_hive_health_monitoring
        WHERE is_healthy IS NOT NULL
          AND run_date BETWEEN date('{start_date}') AND date('{end_date}')
        LIMIT {LIMIT}
    """, CURATED_DATABASE), "daily_hive_health")


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    pull_temperature()
    pull_monitoring()
    log.info("Done. Files in data/samples/:")
    for f in sorted(OUTPUT_DIR.glob("*.csv")):
        log.info(f"  {f.name}")
