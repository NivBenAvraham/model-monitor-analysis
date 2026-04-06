# sensor_group_segment — Layer 1

Per-sensor temperature physics check. Determines whether each sensor's thermal behavior is consistent with the bee-frames model's predicted hive size (`small` / `medium` / `large`).

## Architecture

```
Raw sensor + gateway temperature data
        │
        ▼
  Phase 1: compute()   → std_dev, iqr, ambient_correlation, mean_temp, percent_comfort
        │
        ▼
  Phase 2: grade()     → PASS / WARNING / FAIL  (per sensor)
        │
        ▼  (consumed by Layer 2)
  group_model_temperature_health
```

## How to Run

### Prerequisites

1. **Virtual environment** — all scripts must use the project virtualenv:
   ```bash
   source .venv/bin/activate
   # or full path:
   /Users/nivbenavraham/.local/share/virtualenvs/beehero-model-monitoring-zhiPBxK4/bin/python <script>
   ```

2. **AWS credentials** — required before any Athena / S3 access:
   ```bash
   source scripts/refresh_aws_credentials.sh
   ```

---

### Step 1 — Pull raw samples from Athena

Pulls one parquet file per `(group_id, date, data_type)` from Athena into `data/samples/`.
Runs in the background — takes several hours for the full extraction plan.

```bash
nohup python skills/sensor_group_segment/scripts/pull_samples.py > pull_log.txt 2>&1 &
tail -f pull_log.txt   # monitor progress
```

---

### Step 2 — Create train / test split

Stratified 75/25 split of `valid` + `invalid` samples (needs_recalibration excluded).
Creates symlinks under `data/samples/train/` and `data/samples/test/`, and writes `data/samples/split_manifest.csv`.

```bash
python skills/sensor_group_segment/scripts/create_split.py
```

---

### Step 3 — Run Layer 1 (feature engineering + grading)

Runs Phase 1 (`compute`) + Phase 2 (`grade`) on **train data only**.
Saves per-sensor results and per-hive-size summaries, plus a flat combined file.

```bash
# Default output → data/results/sensor_group_segment/
python skills/sensor_group_segment/scripts/run.py

# Named/dated run → data/results/<run name>/
python skills/sensor_group_segment/scripts/run.py --output "data/results/2026-04-06 sensor grp segment"
```

Output files:
```
data/results/<run>/
├── group_{id}/
│   └── {date}/
│       ├── {id}_{date}_sensor_group_segment.parquet          # per-sensor rows
│       └── {id}_{date}_sensor_group_segment_summary.parquet  # hive-size aggregation
└── results.parquet                                            # all groups + dates combined
```

---

### Step 4 — (Optional) Re-calibrate thresholds

Derives Layer 1 thresholds from the physical distributions of each `hive_size_bucket` in the
train set (p90 for MAX rules, p10 for MIN rules). Review output before updating `thresholds.yaml`.

```bash
python skills/sensor_group_segment/scripts/calibrate_thresholds.py
```

Outputs:
- `data/calibration/train_features.parquet` — cached Phase 1 features for all train samples
- `data/calibration/calibration_report.csv` — percentile table per (hive_size, metric)

## Inputs / Outputs

| Item | Path |
|---|---|
| Raw sensor data | `data/samples/group_{id}/{date}/` |
| Train samples | `data/samples/train/` |
| Ground truth (shared) | `ground_truth/ground_truth_statuess_ca_2026.csv` |
| Extraction plan | `skills/sensor_group_segment/config/extraction_plan.py` |
| Split config | `skills/sensor_group_segment/config/split_config.py` |
| Thresholds | `skills/sensor_group_segment/config/thresholds.yaml` |
| Per-sensor results | `data/results/<run>/group_{id}/{date}/{id}_{date}_sensor_group_segment.parquet` |
| Summary per hive size | `data/results/<run>/group_{id}/{date}/{id}_{date}_sensor_group_segment_summary.parquet` |
| Combined results | `data/results/<run>/results.parquet` |
| Calibration features | `data/calibration/train_features.parquet` |

## Train / Test split

Valid + invalid only (needs_recalibration excluded). 75/25 stratified by status.

| | Train | Test | Total |
|---|---|---|---|
| valid | 269 | 90 | 359 |
| invalid | 154 | 52 | 206 |
| **TOTAL** | **423** | **142** | **565** |

## Phases

**Phase 1 — Feature Engineering** (`compute()` in `src/model_monitor/metrics/sensor_group_segment.py`)

Computes per-sensor biological signatures:
- `std_dev`, `iqr` — temperature stability
- `ambient_correlation` — thermal independence from outside weather
- `mean_temp`, `percent_comfort` — time spent in brood-rearing range [32°C, 36°C]

**Phase 2 — Grading** (`grade()` in `src/model_monitor/metrics/sensor_group_segment.py`)

Compares predicted hive size against observed physics:
- `PASS` — physics consistent with prediction
- `WARNING` — borderline signal
- `FAIL` — physics inconsistent with prediction

> See `spec/spec.txt` for the full logic, rules, and threshold rationale.
