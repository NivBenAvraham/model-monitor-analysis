# model-monitor-analysis

Model monitor for BeeFrame at BeeHero — determines whether the bee-frames model
can be trusted for a given `(group_id, date)`.

All decisions are deterministic: metrics + thresholds + rules. No LLMs at runtime.

---

## What It Does

The monitor evaluates each `(group_id, date)` using two rules:

| Rule | Question | Output |
|---|---|---|
| **Temperature Health** | Are the hive temperatures physically consistent with the predicted hive size? | `VALID` / `INVALID` |
| **Calibration Review Triage** | Does this group need a human calibration review? | `auto_valid` / `needs_review` |

---

## Repository Structure

```
src/model_monitor/
  ingestion/       — Athena SQL helpers (never call tables by name, use helpers)
  metrics/
    temperature/   — 10 group-level temperature checks (R1–R7 + ATV gate)
    triage_rules/  — 4 signals for calibration review triage (A–D)
    sensor_group_segment.py  — per-sensor physics check (PASS / FAIL)
  decision/
    temperature_health_rule.py  — 4 gates + score 6 metrics → VALID / INVALID
    triage_rule.py              — signal coverage + last_status → auto_valid / needs_review
  utils/
    data_utils.py  — load_group_date_data(), iter_all(), load_all()
  reporting/

configs/
  thresholds.yaml      — all threshold values (never hardcoded in Python)

skills/
  data_lake/                       — Athena connection factory + table catalog
  temperature_data_export/         — pull sensor/gateway parquets from Athena
  sensor_group_segment/            — per-sensor feature extraction + grading
  group_model_temperature_health/  — evaluate temperature health per (group, date)
  Calibration Review Triage/       — evaluate triage signals per (group, date)
  hives_temperature_plot_decision/ — analyst-facing temperature plot review

ground_truth/
  ground_truth_statuess_ca_2026.csv  — 643 human-labelled (group_id, date) pairs

explore_notebooks/   — exploratory analysis only, never imported by src/
tests/               — one test file per src/ submodule
```

---

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

## Run Tests

```bash
pytest
```

## Athena Access

Credentials expire every **4 hours**:

```bash
source scripts/refresh_aws_credentials.sh
```

All Athena reads go through `skills/data_lake/scripts/connection.py`:

```python
from skills.data_lake.scripts.connection import read_curated, read_raw
from skills.data_lake.scripts.catalog    import CURATED, RAW
```

## Pull Temperature Data Samples

```bash
source .venv/bin/activate

# Full extraction plan (all 51 groups, parallel)
python skills/temperature_data_export/scripts/pull_samples.py

# Single (group_id, date)
python skills/temperature_data_export/scripts/pull_samples.py \
    --group-id 1144 --date 2026-02-22
```

Samples saved to `data/samples/temperature-export/` (gitignored).

## Key Conventions

- Thresholds → `configs/thresholds.yaml`, never hardcoded
- Notebooks → `explore_notebooks/`, exploration only
- Athena table names → use `skills/data_lake/scripts/catalog.py` constants
- bee_frames table switch: use `raw_bee_frames_table(date)` — never hardcode table name
