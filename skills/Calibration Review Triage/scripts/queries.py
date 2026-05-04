"""
All Athena SQL queries for the Calibration Review Triage skill.

``timestamp`` is the day we are examining — the date whose data is evaluated.
The daily run uses today's date. Pass it as a ``YYYY-MM-DD`` string.

All threshold defaults follow SPECS.md:

    STALE_DAYS_THRESHOLD        = 3   days  (candidates must be in PROD ≥ this long)
    CLIPPING_DIFF_THRESHOLD     = 1.0        (Signal A — metric layer)
    DIPPING_YARD_PCT_THRESHOLD  = 15.0 %     (Signal C — metric layer)
    AUTO_REVIEW_THRESHOLD       = 2.4        (Signal D — metric layer)
    HIST_VALID_WINDOW_DAYS      = 14  days   (historical clipping exemption)
    INSPECTION_LOOKBACK_DAYS    = 14  days   (Signal B)
    THERMOREG_LOOKBACK_DAYS     = 14  days   (Signal C)
    AUTO_REVIEW_LOOKBACK_DAYS   = 21  days   (Signal D)

Table names are imported from skills.data_lake.scripts.catalog.
Each function returns a SQL string that can be passed to read_curated() or read_raw().
"""

from __future__ import annotations

from datetime import date as _date, timedelta


def _at_least_2(t: tuple) -> tuple:
    """Athena rejects IN (x) with a single element — duplicate it."""
    return t * 2 if len(t) == 1 else t


# ---------------------------------------------------------------------------
# Candidate Discovery
# ---------------------------------------------------------------------------

def candidates_query(timestamp: str) -> str:
    """
    Return all stale PRODUCTION beekeeper groups active on ``timestamp``.

    ``timestamp`` — the day we are examining (YYYY-MM-DD).

    A group is a candidate when:
      • It belongs to an active POLLINATION season on ``timestamp``.
      • Its latest model deployment is in PRODUCTION status.
      • That deployment was made at least STALE_DAYS_THRESHOLD (3) days before ``timestamp``.

    Returns columns:
        group_id, deployment_timestamp, days_since_deployment
    """
    
    return f"""

            SELECT
                distinct
                 group_id
                , calibration_date as deployment_timestamp
                ,date_diff('day', calibration_date, date('{timestamp}')) AS days_since_deployment
            FROM
                (SELECT 
                    *,
                    row_number() over(partition by mac, date, model_name order by run_timestamp desc) as rn
                FROM data_lake_curated_data.beekeeper_beeframe_model_monitoring_preprocess
                WHERE date = date('{timestamp}') 
                    AND groups_in_season_ready_for_review = true
                    )
            WHERE rn = 1
            
            """


# ---------------------------------------------------------------------------
# Validation History
# ---------------------------------------------------------------------------

def validation_history_query(group_ids: list[int], timestamp: str) -> str:
    """
    Return tier2 validation history for the given groups up to and including ``timestamp``.

    ``timestamp`` — the day we are examining (YYYY-MM-DD).

    Returns columns:
        group_id, date, tier2_status
    """
    g = _at_least_2(tuple(group_ids))
    return f"""
        SELECT
            group_id,
            DATE(timestamp) as date
            tier2_status
        FROM data_lake_curated_data.beekeeper_beeframe_model_monitoring_validations
        WHERE group_id IN {g}
          AND DATE(timestamp) <= DATE('{timestamp}') 
        ORDER BY group_id, date DESC
    """


# ---------------------------------------------------------------------------
# Signal A — Clipping Diff
# ---------------------------------------------------------------------------

def clipping_diff_query(group_ids: list[int], timestamp: str) -> str:
    """
    Return the latest pred_raw and pred_clipped per sensor for each group on ``timestamp``.

    ``timestamp`` — the day we are examining (YYYY-MM-DD).

    Used by Signal A: avg(abs(pred_raw - pred_clipped)) per group.
    Threshold: CLIPPING_DIFF_THRESHOLD = 1.0 (applied in metric layer).

    Returns columns:
        group_id, sensor_mac_address, pred_raw, pred_clipped, log_timestamp
    """
    g = _at_least_2(tuple(group_ids))
    return f"""
        WITH latest_per_sensor AS (
            SELECT *
            FROM
                (SELECT 
                    *,
                    row_number() over(partition by mac, date, model_name order by run_timestamp desc) as rn
                FROM data_lake_curated_data.beekeeper_beeframe_model_monitoring_preprocess
                WHERE date = date('{timestamp}') 
                    AND group_id IN {g}
                    AND groups_in_season_ready_for_review = true
                    )
            WHERE rn = 1
        )

        SELECT
            group_id,
            mac,
            pred_raw,
            pred_clipped,
            date
        FROM latest_per_sensor
    """


# ---------------------------------------------------------------------------
# Signal B — Inspection Discrepancy
# ---------------------------------------------------------------------------

def inspection_signal_query(group_ids: list[int], timestamp: str) -> str:
    """
    Return yard-level inspection averages and same-day model averages for Signal B.

    ``timestamp`` — the day we are examining (YYYY-MM-DD).
    Lookback window: INSPECTION_LOOKBACK_DAYS = 14 days.

    Returns two logical result sets combined into one query via UNION ALL with
    a ``source`` column so the caller can split them:
        source='inspection' → group_id, inspection_avg, inspection_count
        source='model'      → group_id, model_avg, sensor_count

    The metric layer joins and computes abs(inspection_avg - model_avg).
    Threshold: 1.5 (applied in metric layer).
    """
    g = _at_least_2(tuple(group_ids))
    # INSPECTION_LOOKBACK_DAYS = 14
    return f"""
        

        yard_inspections AS (
        select 
            distinct
                beekeeper_id as group_id
                , orchards_inspected
                , inspector
                , created_at as date
                , avg_bee_frames
        from data_lake_curated_data.inspections_by_beekeeper_and_season
        where beekeeper_id IN {g} 
            AND created_at between DATE('{timestamp}') - INTERVAL '14' DAY AND DATE('{timestamp}')
        ),

        model_outputs AS (
            SELECT 
                date,
                group_id
            FROM
                (SELECT 
                    *,
                    row_number() over(partition by mac, date, model_name order by run_timestamp desc) as rn
                FROM data_lake_curated_data.beekeeper_beeframe_model_monitoring_preprocess
                WHERE date = date('{timestamp}')
                    AND group_id IN {g}
                    AND groups_in_season_ready_for_review = true
                    )
            WHERE rn = 1
            
        )

        SELECT 
            'inspection' AS source
            , group_id
            , orchards_inspected
            , inspector
            , date
            , avg_bee_frames
            , NULL AS numerical_model_result
        FROM yard_inspections

        UNION ALL

        SELECT 
            'model' AS source
            , group_id
            , NULL AS orchards_inspected
            , NULL AS inspector
            , date
            , NULL AS avg_bee_frames
            , numerical_model_result
        FROM model_outputs
  
    """


# ---------------------------------------------------------------------------
# Signal C — Thermoregulation Dipping
# ---------------------------------------------------------------------------

def thermoreg_dipping_query(group_ids: list[int], timestamp: str) -> str:
    """
    Return daily per-yard temperature statistics over the last 14 days for Signal C.

    ``timestamp`` — the day we are examining (YYYY-MM-DD).
    Lookback window: THERMOREG_LOOKBACK_DAYS = 14 days.

    Only includes sensors that had a hive update on ``timestamp`` (active sensors).

    Returns columns:
        group_id, yard_id, yard_name, date,
        temp_mean, temp_std, temp_range, sensor_count
    """
    g = _at_least_2(tuple(group_ids))
    # THERMOREG_LOOKBACK_DAYS = 14
    return f"""
        WITH active_sensor_macs AS (
            -- Sensors with a hive update on timestamp (confirms they ran on that day)
            SELECT DISTINCT sensor_mac_address
            FROM data_lake_curated_data.hive_updates_metadata
            WHERE DATE(created) = DATE('{timestamp}')
              AND model = 'BEE_FRAMES'
        ),

        yard_map AS (
            -- Current sensor → group → yard mapping
            SELECT
                sds.mac  AS sensor_mac_address,
                sds.group_id,
                sds.yard_id,
                -- TODO: join a yards table for yard_name if available
                CAST(sds.yard_id AS VARCHAR) AS yard_name
            FROM data_lake_curated_data.sensor_daily_snapshot sds
            WHERE sds.group_id IN {g}
              AND DATE(sds.date) = DATE('{timestamp}')
              AND sds.mac IN (SELECT sensor_mac_address FROM active_sensor_macs)
        ),

        sensor_daily AS (
            -- Daily aggregated temperature features per sensor
            -- TODO: confirm table and column names for avg_temperature, temperature_std,
            --       temperature_range in the monitoring preprocess table.
            SELECT
                mac            AS sensor_mac_address,
                DATE(run_date) AS sensor_date,
                group_id,
                -- Placeholder: pred_raw used as temp proxy until correct columns confirmed
                AVG(pred_raw)  AS avg_temperature,
                STDDEV(pred_raw) AS temperature_std,
                MAX(pred_raw) - MIN(pred_raw) AS temperature_range
            FROM data_lake_curated_data.beekeeper_beeframe_model_monitoring_preprocess
            WHERE group_id IN {g}
              AND DATE(run_date) >= DATE('{timestamp}') - INTERVAL '14' DAY
              AND DATE(run_date) <= DATE('{timestamp}')
              AND mac IN (SELECT sensor_mac_address FROM active_sensor_macs)
            GROUP BY mac, DATE(run_date), group_id
        )

        SELECT
            ym.group_id,
            ym.yard_id,
            ym.yard_name,
            sd.sensor_date                  AS date,
            AVG(sd.avg_temperature)         AS temp_mean,
            AVG(sd.temperature_std)         AS temp_std,
            AVG(sd.temperature_range)       AS temp_range,
            COUNT(DISTINCT sd.sensor_mac_address) AS sensor_count
        FROM sensor_daily sd
        JOIN yard_map ym
          ON ym.sensor_mac_address = sd.sensor_mac_address
         AND ym.group_id = sd.group_id
        GROUP BY ym.group_id, ym.yard_id, ym.yard_name, sd.sensor_date
        ORDER BY ym.group_id, ym.yard_id, sd.sensor_date
    """


# ---------------------------------------------------------------------------
# Signal D — Auto Review Score
# ---------------------------------------------------------------------------

def auto_review_score_query(group_ids: list[int], timestamp: str) -> str:
    """
    Return the earliest pred_raw per (group, sensor, date) over the last 21 days.

    ``timestamp`` — the day we are examining (YYYY-MM-DD).
    Lookback window: AUTO_REVIEW_LOOKBACK_DAYS = 21 days.

    The metric layer computes all features and the composite score from this data.
    Features use the recent 7-day sub-window (AUTO_REVIEW_RECENT_DAYS = 7).

    Returns columns:
        group_id, sensor_mac_address, input_date, pred_raw, log_timestamp
    """
    g = _at_least_2(tuple(group_ids))
    # AUTO_REVIEW_LOOKBACK_DAYS = 21
    return f"""
        WITH deduped AS (
            
            SELECT *
            FROM
                (SELECT 
                    group_id
                    , mac
                    , date as input_date
                    , pred_raw
                    , row_number() over(partition by mac, date, model_name order by run_timestamp desc) as rn
                FROM data_lake_curated_data.beekeeper_beeframe_model_monitoring_preprocess
                WHERE date between DATE('{timestamp}') - INTERVAL '21' DAY AND DATE('{timestamp}')
                    AND group_id IN {g}
                    AND groups_in_season_ready_for_review = true
                    )
            WHERE rn = 1
        )

        SELECT
              group_id
            , mac
            , input_date
            , pred_raw
        FROM deduped
        ORDER BY group_id, mac, input_date
        

        
    """


# ---------------------------------------------------------------------------
# HU Stats (must_review ordering + prior-invalid blocker)
# ---------------------------------------------------------------------------

def hu_stats_query(group_ids: list[int], timestamp: str) -> str:
    """
    Return the latest hive update per (group, date) for anchor computation.

    ``timestamp`` — the day we are examining (YYYY-MM-DD).

    Used by:
      • must_review row ordering (latest valid HU date ascending).
      • prior-invalid blocker (has a newer valid HU superseded the invalid?).

    Returns columns:
        group_id, activity_date, hive_id, number_of_bee_frames
    """
    g = _at_least_2(tuple(group_ids))
    return f"""
            SELECT 
                date, 
                group_id, 
                valid
            FROM (
                SELECT 
                    DATE(created) AS date,
                    group_id,
                    valid,
                    ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY created DESC) as rn
                FROM data_lake_curated_data.hive_updates
                WHERE created BETWEEN date('2026-01-10') AND DATE('{timestamp}')
                AND validation_time IS NOT NULL
                AND group_id IN {g}
            ) AS latest_records
            WHERE rn = 1
        
          """


# ---------------------------------------------------------------------------
# Auto-Valid Blocker: Same-Day UBF Data Check
# ---------------------------------------------------------------------------

def ubf_presence_query(group_ids: list[int], timestamp: str) -> str:
    """
    Return the distinct group_ids that have at least one row on ``timestamp``.

    ``timestamp`` — the day we are examining (YYYY-MM-DD).

    Used by the auto-valid blocker: groups NOT in this result → needs_review (no_data).

    Returns columns:
        group_id
    """
    g = _at_least_2(tuple(group_ids))
    return f"""
        SELECT DISTINCT group_id
        FROM data_lake_curated_data.beekeeper_beeframe_model_monitoring_preprocess
        WHERE group_id IN {g}
          AND DATE(date) = DATE('{timestamp}')
    """
