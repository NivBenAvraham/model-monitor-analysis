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

--- Ingestion functions are added here as data pipelines are implemented. ---
"""
