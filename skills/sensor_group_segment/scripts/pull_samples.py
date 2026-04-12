"""
Data pull for sensor_group_segment — delegates to temperature_data_export skill.

This script is a convenience entry point. All extraction logic lives in:
  skills/temperature_data_export/scripts/pull_samples.py

Usage (run from repo root):
    python skills/temperature_data_export/scripts/pull_samples.py
    python skills/temperature_data_export/scripts/pull_samples.py --group-id 1144 --date 2026-02-22
    python skills/temperature_data_export/scripts/pull_samples.py --workers 8

See skills/temperature_data_export/README.md for full documentation.
"""

import runpy
import sys
from pathlib import Path

TARGET = (
    Path(__file__).resolve().parents[3]
    / "skills" / "temperature_data_export" / "scripts" / "pull_samples.py"
)

if not TARGET.exists():
    print(f"ERROR: could not find {TARGET}")
    sys.exit(1)

runpy.run_path(str(TARGET), run_name="__main__")
