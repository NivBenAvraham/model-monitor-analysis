"""
Athena connection factory for data_lake_curated_data and data_lake_raw_data.

Provides simple read helpers that any script or notebook can import:

    from skills.data_lake.scripts.connection import read_curated, read_raw

Or use the context-manager for explicit connection lifetime:

    with AthenaSession() as session:
        df = session.read_curated(sql)
        df = session.read_raw(sql)

Thread safety
─────────────
pyathena connections are NOT thread-safe.
Use get_connection() inside ThreadPoolExecutor workers — it returns a
per-thread connection via threading.local(), so each thread owns its own.
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Callable

import pandas as pd
from pyathena import connect

# ---------------------------------------------------------------------------
# Athena config
# ---------------------------------------------------------------------------
S3_STAGING_DIR   = "s3://us-east-1-data-analytics-storage/athena-query-results/"
REGION           = "us-east-1"
WORKGROUP        = "data_analytics"
CURATED_DATABASE = "data_lake_curated_data"
RAW_DATABASE     = "data_lake_raw_data"

# ---------------------------------------------------------------------------
# Thread-local connection pool (one connection per thread per database)
# ---------------------------------------------------------------------------
_local = threading.local()


def get_connection(database: str):
    """
    Return a thread-local pyathena connection for the given database.
    Creates it on first access; reuses on subsequent calls within the same thread.

    Parameters
    ----------
    database : "data_lake_curated_data" | "data_lake_raw_data"
    """
    key = f"_conn_{database}"
    if not hasattr(_local, key):
        conn = connect(
            s3_staging_dir=S3_STAGING_DIR,
            region_name=REGION,
            schema_name=database,
            work_group=WORKGROUP,
        )
        setattr(_local, key, conn)
    return getattr(_local, key)


def read_curated(sql: str) -> pd.DataFrame:
    """Execute SQL against data_lake_curated_data and return a DataFrame."""
    return pd.read_sql(sql, get_connection(CURATED_DATABASE))


def read_raw(sql: str) -> pd.DataFrame:
    """Execute SQL against data_lake_raw_data and return a DataFrame."""
    return pd.read_sql(sql, get_connection(RAW_DATABASE))


def read(sql: str, database: str) -> pd.DataFrame:
    """Execute SQL against any database by name."""
    return pd.read_sql(sql, get_connection(database))


# ---------------------------------------------------------------------------
# Context manager — explicit session with guaranteed cleanup
# ---------------------------------------------------------------------------

class AthenaSession:
    """
    Context manager that opens fresh connections (not thread-local) for the
    session lifetime and closes them on exit.

    Use when you need explicit resource control, e.g. in scripts that run
    once and exit.

    Usage
    -----
    with AthenaSession() as s:
        df = s.read_curated("SELECT 1")
        df = s.read_raw("SELECT 1")
    """

    def __init__(self) -> None:
        self._curated = None
        self._raw = None

    def __enter__(self) -> "AthenaSession":
        self._curated = connect(
            s3_staging_dir=S3_STAGING_DIR,
            region_name=REGION,
            schema_name=CURATED_DATABASE,
            work_group=WORKGROUP,
        )
        self._raw = connect(
            s3_staging_dir=S3_STAGING_DIR,
            region_name=REGION,
            schema_name=RAW_DATABASE,
            work_group=WORKGROUP,
        )
        return self

    def __exit__(self, *_) -> None:
        for conn in (self._curated, self._raw):
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass

    def read_curated(self, sql: str) -> pd.DataFrame:
        return pd.read_sql(sql, self._curated)

    def read_raw(self, sql: str) -> pd.DataFrame:
        return pd.read_sql(sql, self._raw)

    def read(self, sql: str, database: str) -> pd.DataFrame:
        conn = self._curated if database == CURATED_DATABASE else self._raw
        return pd.read_sql(sql, conn)


# ---------------------------------------------------------------------------
# Reader factory — returns a callable, useful for passing into functions
# ---------------------------------------------------------------------------

def make_reader(database: str) -> Callable[[str], pd.DataFrame]:
    """
    Return a read(sql) → DataFrame function bound to the given database.
    Uses thread-local connections — safe in ThreadPoolExecutor.

    Usage
    -----
    read = make_reader("data_lake_curated_data")
    df = read("SELECT * FROM sensor_samples LIMIT 10")
    """
    def _reader(sql: str) -> pd.DataFrame:
        return pd.read_sql(sql, get_connection(database))
    _reader.__doc__ = f"Read SQL from {database}."
    return _reader
