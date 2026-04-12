"""
All Athena SQL queries for temperature data extraction.

Query order per (group_id, date):
  1. hive_updates_query()    → which sensors ran which model, bee_frames count
  2. sensor_samples_query()  → pcb_temperature_one per sensor (2-day window ending on date)
  3. gateway_samples_query() → pcb_temperature_two per gateway (same window)

Table switch for bee_frames (IMPORTANT):
  date < 2026-03-10  → data_lake_raw_data.supervised_beeframes
  date ≥ 2026-03-10  → data_lake_raw_data.unified_bee_frames
Call raw_bee_frames_table(date) — never hardcode either name.
"""

from __future__ import annotations

from datetime import date as _date

# ---------------------------------------------------------------------------
# Table switch
# ---------------------------------------------------------------------------
_SWITCH_DATE  = _date(2026, 3, 10)
_TABLE_LEGACY = "supervised_beeframes"
_TABLE_CURRENT = "unified_bee_frames"


def raw_bee_frames_table(query_date: _date | str) -> str:
    """Return the correct raw bee_frames table name for a given date."""
    if isinstance(query_date, str):
        query_date = _date.fromisoformat(query_date[:10])
    return _TABLE_CURRENT if query_date >= _SWITCH_DATE else _TABLE_LEGACY


def _at_least_2(t: tuple) -> tuple:
    """SQL IN (x) is invalid in some engines; duplicate single-element tuples."""
    return t * 2 if len(t) == 1 else t


# ---------------------------------------------------------------------------
# Query 1 — Hive Updates
# ---------------------------------------------------------------------------

def hive_updates_query(
    group_ids: list[int] | tuple,
    models: list[str] | tuple,
    date_str: str,
) -> str:
    """
    Returns: created, sensor_mac_address, group_id, bee_frames, model

    Deduplication: latest log_timestamp per (sensor, date, model) via row_number().
    Table switch applied automatically.
    """
    table = raw_bee_frames_table(date_str)
    g = _at_least_2(tuple(group_ids))
    m = _at_least_2(tuple(models))
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
        WHERE s.group_id IN {g}
        AND hum.router_s3_pkl_file IN {m}
        AND hum.created >= TIMESTAMP '{date_str}'
        AND hum.created < TIMESTAMP '{date_str}' + INTERVAL '1' DAY
        AND hum.model = 'BEE_FRAMES'
        ORDER BY hum.created
    """


# ---------------------------------------------------------------------------
# Query 2 — Sensor Samples
# ---------------------------------------------------------------------------

def sensor_samples_query(sensors: list[str] | tuple, date_str: str) -> str:
    """
    Returns: sensor_mac_address, timestamp, pcb_temperature_one,
             gateway_mac_address, humidity

    Window: [date - 2 days, date)  — two full days of readings before the label date.
    """
    s = _at_least_2(tuple(sensors))
    return f"""
        SELECT
            sensor_mac_address,
            timestamp,
            pcb_temperature_one,
            gateway_mac_address,
            humidity
        FROM sensor_samples
        WHERE sensor_mac_address IN {s}
        AND timestamp >= DATE '{date_str}' - INTERVAL '2' DAY
        AND timestamp < DATE '{date_str}'
    """


# ---------------------------------------------------------------------------
# Query 3 — Gateway Samples
# ---------------------------------------------------------------------------

def gateway_samples_query(gateways: list[str] | tuple, date_str: str) -> str:
    """
    Returns: gateway_mac_address, timestamp, pcb_temperature_two,
             acceleration_x/y/z, hardware_extension_present

    Window: [date - 2 days, date) — same as sensor window.
    Filters out gateways with zero acceleration (bad/static hardware).
    """
    g = _at_least_2(tuple(gateways))
    return f"""
        SELECT
            gateway_samples.gateway_mac_address,
            gateway_samples.timestamp,
            gateway_samples.pcb_temperature_two,
            gateway_samples.acceleration_x,
            gateway_samples.acceleration_y,
            gateway_samples.acceleration_z,
            CASE
                WHEN gateways.hardware_extensions != '' THEN TRUE
                ELSE FALSE
            END AS hardware_extension_present
        FROM gateway_samples
        JOIN data_lake_raw_data.gateways
            ON gateway_samples.gateway_mac_address = gateways.mac
        WHERE gateway_mac_address IN {g}
        AND timestamp >= DATE '{date_str}' - INTERVAL '2' DAY
        AND timestamp < DATE '{date_str}'
    """
