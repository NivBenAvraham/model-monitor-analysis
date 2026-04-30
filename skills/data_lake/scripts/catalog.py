"""
Table catalog for data_lake_curated_data and data_lake_raw_data.

Provides:
  - String constants for all known table names  (avoids typos in queries)
  - SCHEMA dicts describing columns + types     (reference for query authors)
  - list_tables(database)                       (live Athena table list)
  - describe_table(table, database)             (live Athena column info)
  - sample(table, database, n)                  (quick LIMIT-n peek)

Usage
─────
from skills.data_lake.scripts.catalog import CURATED, RAW, sample, describe_table

df = sample(CURATED.SENSOR_SAMPLES)
df = describe_table(RAW.UNIFIED_BEE_FRAMES)
"""

from __future__ import annotations

import pandas as pd

from skills.data_lake.scripts.connection import (
    CURATED_DATABASE,
    RAW_DATABASE,
    read_curated,
    read_raw,
)


# ---------------------------------------------------------------------------
# Table name constants
# ---------------------------------------------------------------------------

class CURATED:
    """Table names in data_lake_curated_data."""
    SENSOR_SAMPLES                       = "sensor_samples"
    GATEWAY_SAMPLES                      = "gateway_samples"
    HIVE_UPDATES_METADATA                = "hive_updates_metadata"
    SENSOR_DAILY_SNAPSHOT                = "sensor_daily_snapshot"
    YARD_UPDATES                         = "yard_updates"
    SEASONAL_BEEKEEPERS                  = "seasonal_beekeepers"
    BEEFRAME_MODEL_MONITORING_PREPROCESS = "beekeeper_beeframe_model_monitoring_preprocess"
    BEEFRAME_MODEL_MONITORING_HOURLY     = "beekeeper_beeframe_model_monitoring_preprocess_hourly"
    BEEFRAME_MODEL_MONITORING_VALIDATIONS= "beekeeper_beeframe_model_monitoring_validations"
    OPS_INSPECTIONS                      = "ops_inspections"
    YARD_INSPECTIONS                     = "yard_inspections"
    DAILY_HIVE_HEALTH_MONITORING         = "daily_hive_health_monitoring"


class RAW:
    """Table names in data_lake_raw_data."""
    SUPERVISED_BEEFRAMES = "supervised_beeframes"   # dates < 2026-03-10
    UNIFIED_BEE_FRAMES   = "unified_bee_frames"     # dates ≥ 2026-03-10
    GATEWAYS             = "gateways"


# ---------------------------------------------------------------------------
# Schema reference
# ---------------------------------------------------------------------------

SCHEMA: dict[str, dict[str, str]] = {

    # ── data_lake_curated_data ───────────────────────────────────────────
    CURATED.SENSOR_SAMPLES: {
        "sensor_mac_address": "string",
        "timestamp":          "timestamp",
        "pcb_temperature_one":"double   — hive internal temperature (°C)",
        "gateway_mac_address":"string",
        "humidity":           "double",
    },

    CURATED.GATEWAY_SAMPLES: {
        "gateway_mac_address":"string",
        "timestamp":          "timestamp",
        "pcb_temperature_two":"double   — ambient (outdoor) temperature (°C)",
        "acceleration_x":     "double",
        "acceleration_y":     "double",
        "acceleration_z":     "double",
    },

    CURATED.HIVE_UPDATES_METADATA: {
        "created":             "timestamp",
        "sensor_mac_address":  "string",
        "router_s3_pkl_file":  "string   — model / lambda name",
        "model":               "string   — always 'BEE_FRAMES' for bee-frames model",
        "hardware_extensions": "string",
    },

    CURATED.SENSOR_DAILY_SNAPSHOT: {
        "mac":      "string   — sensor_mac_address",
        "date":     "date",
        "group_id": "bigint",
        "yard_id":  "bigint",
    },

    CURATED.YARD_UPDATES: {
        "yard_id":           "bigint",
        "bee_frames":        "double",
        "valid_hives_count": "bigint",
        "created":           "timestamp",
        "last_hive_update":  "timestamp",
    },

    CURATED.BEEFRAME_MODEL_MONITORING_PREPROCESS: {
        "mac":                              "string",
        "date":                             "date",
        "run_date":                         "date",
        "model_name":                       "string",
        "pred_raw":                         "double",
        "group_id":                         "bigint",
        "yard_id":                          "bigint",
        "group_in_season":                  "boolean",
        "groups_in_season_ready_for_review":"boolean",
        "run_timestamp":                    "timestamp",
    },

    CURATED.BEEFRAME_MODEL_MONITORING_VALIDATIONS: {
        "timestamp":    "timestamp",
        "group_id":     "bigint",
        "tier1_status": "string",
        "tier2_status": "string",
    },

    CURATED.OPS_INSPECTIONS: {
        "sensor_mac_address": "string",
        "utc_timestamp":      "timestamp",
        "total_bee_frames":   "double",
        "group_id":           "bigint",
        "yard_id":            "bigint",
    },

    CURATED.DAILY_HIVE_HEALTH_MONITORING: {
        "sensor_mac_address": "string",
        "run_date":           "date",
        "is_healthy":         "boolean",
        "group_id":           "bigint",
        "yard_id":            "bigint",
    },

    # ── data_lake_raw_data ───────────────────────────────────────────────
    RAW.SUPERVISED_BEEFRAMES: {
        "sensor_mac_address": "string",
        "pred_raw":           "double   — raw bee_frames prediction",
        "lambda_name":        "string   — model identifier (full path)",
        "log_timestamp":      "timestamp",
    },

    RAW.UNIFIED_BEE_FRAMES: {
        "sensor_mac_address": "string",
        "pred_raw":           "double",
        "lambda_name":        "string",
        "log_timestamp":      "timestamp",
    },

    RAW.GATEWAYS: {
        "mac":                "string   — gateway_mac_address",
        "hardware_extensions":"string   — non-empty → hardware_extension_present=True",
    },
}


# ---------------------------------------------------------------------------
# Live catalog helpers
# ---------------------------------------------------------------------------

def list_tables(database: str = CURATED_DATABASE) -> pd.DataFrame:
    """Return a DataFrame of all tables in the given database."""
    reader = read_curated if database == CURATED_DATABASE else read_raw
    return reader("SHOW TABLES")


def describe_table(table: str, database: str = CURATED_DATABASE) -> pd.DataFrame:
    """Return column names and types for a table (live Athena DESCRIBE)."""
    reader = read_curated if database == CURATED_DATABASE else read_raw
    return reader(f"DESCRIBE {table}")


def sample(
    table: str,
    database: str = CURATED_DATABASE,
    n: int = 5,
) -> pd.DataFrame:
    """Return the first n rows from a table for a quick schema sanity-check."""
    reader = read_curated if database == CURATED_DATABASE else read_raw
    return reader(f"SELECT * FROM {table} LIMIT {n}")


def schema(table: str) -> dict[str, str] | None:
    """Return the local schema reference dict for a known table, or None."""
    return SCHEMA.get(table)
