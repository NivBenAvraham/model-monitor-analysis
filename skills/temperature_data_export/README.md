# temperature_data_export

Pulls raw hive and ambient temperature data from Athena and saves it as local Parquet files.

Replaces the dependency on the external `temperature_data_export_package`. All queries,
preprocessing logic, and transforms are owned here.

---

## Prerequisites

```bash
source scripts/refresh_aws_credentials.sh   # credentials expire every 4 hours
source .venv/bin/activate
```

---

## How to Run

### Pull a single (group_id, date)
```bash
python skills/temperature_data_export/scripts/pull_samples.py \
    --group-id 1144 --date 2026-02-22
```

### Pull all dates for one group
```bash
python skills/temperature_data_export/scripts/pull_samples.py --group-id 1144
```

### Pull the full extraction plan (all 51 groups)
```bash
python skills/temperature_data_export/scripts/pull_samples.py
```

### Control parallelism
```bash
python skills/temperature_data_export/scripts/pull_samples.py --workers 4
```

### Force re-pull (overwrite existing)
```bash
python skills/temperature_data_export/scripts/pull_samples.py \
    --group-id 1144 --date 2026-02-22 --force
```

---

## Output

```
data/samples/temperature-export/
  group_{id}/
    {date}/
      {id}_{date}_hive_updates.parquet
      {id}_{date}_sensor_temperature.parquet
      {id}_{date}_gateway_temperature.parquet
```

Each `(group_id, date)` produces 3 files. Existing folders are skipped by default.

---

## Sensor temperature preprocessing (automatic)

Applied in `scripts/transforms.py` after the Athena query:

1. **Fix encoding errors** — readings below −40°C get +175.71°C correction
2. **Range filter** — keep pcb_temperature_one ∈ [−30, 100]°C
3. **Humidity filter** — drop rows where humidity > 95
4. **Z-score spike removal** — per-sensor, geometric-mean-of-diffs, threshold = 3
5. **Resample to 30-min means** — per sensor
6. **Join** gateway_mac_address, hive_size_bucket (small/medium/large), group_id

---

## Skill layout

```
skills/temperature_data_export/
  README.md
  spec/spec.txt               ← detailed spec: queries, schema, preprocessing rules
  config/
    extraction_plan.py        ← 51 groups × date ranges × model names
  scripts/
    queries.py                ← all Athena SQL
    transforms.py             ← sensor preprocessing + hive_size_bucket
    pull_samples.py           ← CLI orchestration (parallel ThreadPoolExecutor)
```
