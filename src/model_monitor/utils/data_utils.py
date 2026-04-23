from __future__ import annotations

import logging
from typing import Generator

import pandas as pd
from pathlib import Path

log = logging.getLogger(__name__)


REPO_ROOT      = Path.cwd().resolve()
# walk up until we find the repo root (contains pyproject.toml)
for _p in [REPO_ROOT, *REPO_ROOT.parents]:
    if (_p / "pyproject.toml").exists():
        REPO_ROOT = _p
        break

DATA_DIR       = REPO_ROOT / "data/samples/temperature-export"
DATA_DIR_TRAIN = DATA_DIR / "train"
DATA_DIR_TEST = DATA_DIR / "test"
TAGS_DIR       = REPO_ROOT / "data_analyst_plot_decisions"

# ── data loading ────────────────────────────────────────────────────
# load sensor and gateway data for a given (group_id, date) from the given data directory
def load_group_date_data(group_id: int, date: str, data_dir: Path = DATA_DIR_TRAIN):
    """Load sensor + gateway parquet for one (group_id, date) from the train split."""
    base = data_dir / f"group_{group_id}" / date
    sensor_files  = list(base.glob(f"*sensor_temperature*.parquet"))
    gateway_files = list(base.glob(f"*gateway_temperature*.parquet"))
    hive_update_files = list[Path](base.glob(f"*hive_updates*.parquet"))
    if not sensor_files or not gateway_files:
        raise FileNotFoundError(f"Missing parquet files in {base}")
    return pd.read_parquet(sensor_files[0]), pd.read_parquet(gateway_files[0]), pd.read_parquet(hive_update_files[0])


def iter_all(
    data_dir: Path = DATA_DIR_TRAIN,
) -> Generator[tuple[int, str, pd.DataFrame, pd.DataFrame, pd.DataFrame], None, None]:
    """
    Yield (group_id, date, sensor_df, gateway_df, hive_updates_df) for every
    (group_id, date) folder found under data_dir.

    Skips folders that are missing sensor or gateway files and logs a warning.

    Usage
    -----
    for group_id, date, sensor, gateway, hive in iter_all():
        ...

    for group_id, date, sensor, gateway, hive in iter_all(DATA_DIR_TEST):
        ...
    """
    for group_dir in sorted(data_dir.iterdir()):
        if not group_dir.is_dir():
            continue
        try:
            group_id = int(group_dir.name.replace("group_", ""))
        except ValueError:
            continue

        for date_dir in sorted(group_dir.iterdir()):
            if not date_dir.is_dir():
                continue
            date = date_dir.name
            try:
                sensor_df, gateway_df, hive_df = load_group_date_data(
                    group_id, date, data_dir
                )
            except FileNotFoundError:
                log.warning(f"Skipping {group_dir.name}/{date} — missing parquet files")
                continue
            yield group_id, date, sensor_df, gateway_df, hive_df


def load_all(
    data_dir: Path = DATA_DIR_TRAIN,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Load and concatenate all (group_id, date) folders under data_dir into three
    DataFrames: sensor, gateway, hive_updates.

    A 'date' column (folder name, e.g. '2026-02-22') is added to each DataFrame.

    Usage
    -----
    sensor_all, gateway_all, hive_all = load_all()
    sensor_all, gateway_all, hive_all = load_all(DATA_DIR_TEST)
    """
    sensors, gateways, hives = [], [], []

    for group_id, date, sensor_df, gateway_df, hive_df in iter_all(data_dir):
        sensor_df  = sensor_df.copy();  sensor_df["date"]  = date
        gateway_df = gateway_df.copy(); gateway_df["date"] = date
        hive_df    = hive_df.copy();    hive_df["date"]    = date
        sensors.append(sensor_df)
        gateways.append(gateway_df)
        hives.append(hive_df)

    if not sensors:
        raise FileNotFoundError(f"No data found under {data_dir}")

    return (
        pd.concat(sensors,  ignore_index=True),
        pd.concat(gateways, ignore_index=True),
        pd.concat(hives,    ignore_index=True),
    )

