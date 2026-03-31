# temp_metric Skill

End-to-end pipeline that evaluates hive temperature behaviour and grades each sensor as `PASS / WARNING / FAIL`.

## Folder Layout

```
skills/temp_metric/
  README.md                         ← you are here
  __init__.py
  spec/
    for_temp_metric.txt             ← source of truth: logic, rules, thresholds rationale
  config/
    extraction_plan.py              ← which groups / dates / models to pull (from ground truth CSV)
  scripts/
    pull_samples.py                 ← Step 1: pull raw data from Athena → local Parquet
    run_temp_metric.py              ← Step 2: compute features + grade → result Parquet
```

## How to Run

```bash
# 1. Refresh AWS credentials (valid 4 hours)
source scripts/refresh_aws_credentials.sh

# 2. Pull data overnight (runs in background, safe to close terminal)
nohup python skills/temp_metric/scripts/pull_samples.py > pull_log.txt 2>&1 &

# 3. Run metric once data is ready
python skills/temp_metric/scripts/run_temp_metric.py
```

## Inputs

| Source | Location |
|--------|----------|
| Ground truth labels | `data/temp_ground_truth/ground_truth_statuess_ca_2026.csv` |
| Extraction plan | `skills/temp_metric/config/extraction_plan.py` |
| Raw samples (after pull) | `data/samples/group_{id}/{date}/` |
| Thresholds | `configs/thresholds.yaml` → `metrics.temp_metric` |

## Outputs

| File | Description |
|------|-------------|
| `data/samples/group_{id}/{date}/{id}_{date}_sensor_temperature.parquet` | Raw sensor readings |
| `data/samples/group_{id}/{date}/{id}_{date}_gateway_temperature.parquet` | Raw gateway (ambient) readings |
| `data/samples/group_{id}/{date}/{id}_{date}_hive_updates.parquet` | Hive size labels |
| `data/results/group_{id}/{date}/{id}_{date}_temp_metric.parquet` | Per-sensor results + status + reason |
| `data/results/group_{id}/{date}/{id}_{date}_temp_metric_summary.parquet` | Mean metrics + grade counts per hive size |
| `data/results/temp_metric_results.parquet` | All groups / dates combined |

## Phases

**Phase 1 — Feature Engineering** (`compute()` in `src/model_monitor/metrics/temp_metric.py`)
Computes `std_dev`, `iqr`, `ambient_correlation`, `mean_temp`, `percent_comfort` per sensor per day.

**Phase 2 — Grading** (`grade()` in `src/model_monitor/metrics/temp_metric.py`)
Rule-based logic comparing predicted hive size against observed physics → `PASS / WARNING / FAIL`.

> See `spec/for_temp_metric.txt` for the full logic, rules, and threshold rationale.
