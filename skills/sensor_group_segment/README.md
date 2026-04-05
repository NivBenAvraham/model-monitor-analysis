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

## Run order

```bash
source scripts/refresh_aws_credentials.sh

# Pull data from Athena (runs in background — takes hours)
nohup python skills/sensor_group_segment/scripts/pull_samples.py > pull_log.txt 2>&1 &

# Create train/test split
python skills/sensor_group_segment/scripts/create_split.py

# Run Layer 1: compute features + grade per sensor
python skills/sensor_group_segment/scripts/run.py

# (Optional) Re-calibrate thresholds from sensor physics distributions
python skills/sensor_group_segment/scripts/calibrate_thresholds.py
```

## Inputs / Outputs

| Item | Path |
|---|---|
| Raw sensor data | `data/samples/group_{id}/{date}/` |
| Train samples | `data/samples/train/` |
| Ground truth (shared) | `ground_truth/ground_truth_statuess_ca_2026.csv` |
| Extraction plan | `skills/sensor_group_segment/config/extraction_plan.py` |
| Split config | `skills/sensor_group_segment/config/split_config.py` |
| Thresholds | `skills/sensor_group_segment/config/thresholds.yaml` |
| Per-sensor results | `data/results/sensor_group_segment/group_{id}/{date}/{id}_{date}_sensor_group_segment.parquet` |
| Summary per hive size | `data/results/sensor_group_segment/group_{id}/{date}/{id}_{date}_sensor_group_segment_summary.parquet` |
| Combined results | `data/results/sensor_group_segment/results.parquet` |
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
