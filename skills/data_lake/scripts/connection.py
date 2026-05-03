"""
Athena connection factory for data_lake_curated_data and data_lake_raw_data.

Performance
───────────
Uses PandasCursor throughout — it downloads the Athena result file from S3
and converts it to a DataFrame in one shot, instead of iterating row by row.
This is typically 5–20× faster than pd.read_sql with DefaultCursor and also
eliminates the pandas "Other DBAPI2 objects are not tested" warning.

Provides simple read helpers that any script or notebook can import:

    from skills.data_lake.scripts.connection import read_curated, read_raw

Or use the context-manager for explicit connection lifetime:

    with AthenaSession() as session:
        df = session.read_curated(sql)
        df = session.read_raw(sql)

Thread safety
─────────────
PandasCursor is NOT thread-safe at the cursor level.
get_connection() returns a thread-local connection via threading.local()
so each thread owns its own connection and cursor — safe in ThreadPoolExecutor.
"""

from __future__ import annotations

import threading
from typing import Callable

import pandas as pd
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

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
    Return a thread-local pyathena PandasCursor connection for the given database.
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
            cursor_class=PandasCursor,
        )
        setattr(_local, key, conn)
    return getattr(_local, key)


def _exec(sql: str, database: str) -> pd.DataFrame:
    """Core execute — uses PandasCursor.as_pandas() for bulk S3 download."""
    return get_connection(database).cursor().execute(sql).as_pandas()


def read_curated(sql: str) -> pd.DataFrame:
    """Execute SQL against data_lake_curated_data and return a DataFrame."""
    return _exec(sql, CURATED_DATABASE)


def read_raw(sql: str) -> pd.DataFrame:
    """Execute SQL against data_lake_raw_data and return a DataFrame."""
    return _exec(sql, RAW_DATABASE)


def read(sql: str, database: str) -> pd.DataFrame:
    """Execute SQL against any database by name."""
    return _exec(sql, database)


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
            cursor_class=PandasCursor,
        )
        self._raw = connect(
            s3_staging_dir=S3_STAGING_DIR,
            region_name=REGION,
            schema_name=RAW_DATABASE,
            work_group=WORKGROUP,
            cursor_class=PandasCursor,
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
        return self._curated.cursor().execute(sql).as_pandas()

    def read_raw(self, sql: str) -> pd.DataFrame:
        return self._raw.cursor().execute(sql).as_pandas()

    def read(self, sql: str, database: str) -> pd.DataFrame:
        conn = self._curated if database == CURATED_DATABASE else self._raw
        return conn.cursor().execute(sql).as_pandas()


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
        return _exec(sql, database)
    _reader.__doc__ = f"Read SQL from {database}."
    return _reader
