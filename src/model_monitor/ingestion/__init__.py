"""
Data ingestion — load raw data from SQL / AWS sources.

--- Data Sources ---

1. BeeFrame model monitoring (Athena / curated DB)
   Source repo (read-only): beehero-model-monitoring
   Tables:
     beekeeper_beeframe_model_monitoring_preprocess
       Columns: mac, date, run_date, model_name, pred_raw, group_id, yard_id,
                group_in_season, groups_in_season_ready_for_review, run_timestamp
       Note: use latest run_timestamp per (mac, date, model_name) via row_number()

     beekeeper_beeframe_model_monitoring_preprocess_hourly
       Same schema as above; used when post_metric=True

     beekeeper_beeframe_model_monitoring_validations
       Columns: timestamp, group_id, tier1_status, tier2_status

     ops_inspections
       Columns: sensor_mac_address, utc_timestamp, total_bee_frames, group_id, yard_id

     yard_inspections
       Columns: yard_id, utc_end_time, bee_frames_distribution (JSON histogram string)

     daily_hive_health_monitoring
       Columns: sensor_mac_address, run_date, is_healthy, group_id, yard_id

     supervised_beeframes (model run logs, raw)
       Columns: log_timestamp (+ others via SELECT *)

2. Temperature data (Athena / curated + raw DB)
   Source repo (read-only): beehero-streamlit-app/temperature_data_export_package
   ETL already handled by that package — use load_temperature_data() as-is.
   Produces:
     sensor_samples:  sensor_mac_address, timestamp (30-min bins), pcb_temperature_one,
                      gateway_mac_address, group_id, hive_size_bucket
     gateway_samples: gateway_mac_address, timestamp, pcb_temperature_two
     hive_updates:    sensor_mac_address, created, group_id, bee_frames, model

--- Table Source Switch (IMPORTANT) ---

Both tables live in data_lake_raw_data. Use raw_bee_frames_table() — never hardcode either name.

    date <= 2026-03-09  →  supervised_beeframes
    date >= 2026-03-10  →  unified_bee_frames   (fully replaces supervised_beeframes)

--- Ingestion functions are added here as data pipelines are implemented. ---
"""

from datetime import date as _date

_SWITCH_DATE = _date(2026, 3, 10)
_TABLE_LEGACY = "supervised_beeframes"
_TABLE_CURRENT = "unified_bee_frames"


def raw_bee_frames_table(query_date) -> str:
    """
    Return the correct raw bee_frames table name for a given date.

    Args:
        query_date: date object, or a string 'YYYY-MM-DD'

    Returns:
        'supervised_beeframes'  for dates up to 2026-03-09
        'unified_bee_frames'    for dates from 2026-03-10 onward
    """
    if isinstance(query_date, str):
        query_date = _date.fromisoformat(query_date[:10])
    return _TABLE_CURRENT if query_date >= _SWITCH_DATE else _TABLE_LEGACY


def hive_updates_query(
    group_ids_tuple: tuple,
    models_tuple: tuple,
    date_str: str,
) -> str:
    """
    Build the hive_updates SQL query using the correct raw bee_frames table for the given date.

    This is a local override of temperature_data_export_package.utils.queries.hive_updates_curated().
    The only difference: the FROM clause uses raw_bee_frames_table(date_str) instead of
    hardcoding 'supervised_beeframes'. Both tables have identical structure and columns.

    Args:
        group_ids_tuple: tuple of group_id integers (use at least 2 elements — SQL IN quirk)
        models_tuple:    tuple of model name strings
        date_str:        'YYYY-MM-DD'

    Returns:
        SQL string ready to pass to read_curated()
    """
    table = raw_bee_frames_table(date_str)
    return f"""
        WITH latest_bee_frames AS (
            SELECT
                sensor_mac_address,
                pred_raw,
                substring(lambda_name, 17) AS lambda_name,
                date(log_timestamp) AS date
            FROM (
                SELECT
                    sensor_mac_address,
                    pred_raw,
                    lambda_name,
                    log_timestamp,
                    row_number() OVER (
                        PARTITION BY sensor_mac_address, date(log_timestamp), lambda_name
                        ORDER BY log_timestamp DESC
                    ) AS rn
                FROM data_lake_raw_data.{table}
                WHERE log_timestamp BETWEEN DATE '{date_str}' AND DATE '{date_str}' + INTERVAL '1' DAY
            ) ranked
            WHERE rn = 1
        )
        SELECT
            hum.created,
            hum.sensor_mac_address,
            s.group_id,
            sbf.pred_raw AS bee_frames,
            hum.router_s3_pkl_file AS model
        FROM data_lake_curated_data.hive_updates_metadata hum
        JOIN sensor_daily_snapshot s
            ON hum.sensor_mac_address = s.mac
            AND s.date = date(hum.created)
        JOIN latest_bee_frames sbf
            ON hum.sensor_mac_address = sbf.sensor_mac_address
            AND date(hum.created) = sbf.date
            AND hum.router_s3_pkl_file = sbf.lambda_name
        WHERE s.group_id IN {group_ids_tuple}
        AND hum.router_s3_pkl_file IN {models_tuple}
        AND hum.created >= TIMESTAMP '{date_str}'
        AND hum.created < TIMESTAMP '{date_str}' + INTERVAL '1' DAY
        AND hum.model = 'BEE_FRAMES'
        ORDER BY hum.created
    """
