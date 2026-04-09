# sensor_group_segment — Layer 1

Per-sensor temperature physics check. Determines whether each sensor's thermal
behavior is consistent with the bee-frames model's predicted hive size
(`small` / `medium` / `large`).

## Architecture

```
Raw sensor + gateway temperature data
        │
        ▼
  Phase 1: compute()         → lean 5 grading features per sensor
        │  (extract_features.py → full 14-column feature table for EDA/calibration)
        ▼
  Phase 2: grade()           → PASS / WARNING / FAIL  (per sensor)
        │
        ▼  (consumed by Layer 2)
  group_model_temperature_health
```

### Ambient Correlation — Per-Sensor Local Gateways

A sensor physically communicates through multiple gateways simultaneously
(median 3–7 per day in US groups). Ambient temperature is computed as the
**median and mean across only the gateways that sensor actually talked to**
— its local neighborhood — not all gateways in the group.

This matters because a `group_id` in the USA can span multiple physical yards
with different microclimates. Using all gateways would contaminate the ambient
reference with distant weather readings.

---

## How to Run

### Prerequisites

1. **Virtual environment** — activate the local repo venv:
   ```bash
   source .venv/bin/activate
   ```

2. **AWS credentials** — required before Athena / S3 access:
   ```bash
   source scripts/refresh_aws_credentials.sh
   ```

---

### Step 1 — Pull raw samples from Athena

Pulls one parquet file per `(group_id, date, data_type)` into
`data/samples/temperature-export/`. Takes several hours for the full extraction plan.

```bash
nohup python skills/sensor_group_segment/scripts/pull_samples.py > pull_log.txt 2>&1 &
tail -f pull_log.txt
```

---

### Step 2 — Create train / test split

Stratified 75/25 split of `valid` + `invalid` samples (`needs_recalibration` excluded).
Creates symlinks under `data/samples/temperature-export/train/` and `.../test/`,
and writes `data/samples/split_manifest.csv`.

```bash
python skills/sensor_group_segment/scripts/create_split.py
```

---

### Step 3 — Run Layer 1 (feature engineering + grading)

Runs Phase 1 (`compute`) + Phase 2 (`grade`) on **train data only**.
Saves per-sensor results and per-hive-size summaries, plus a flat combined file.
Prints a full mean ± std summary table at the end.

```bash
# Named/dated run (recommended)
python skills/sensor_group_segment/scripts/run.py --output "data/results/2026-04-06 sensor grp segment"

# Default output → data/results/sensor_group_segment/
python skills/sensor_group_segment/scripts/run.py
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

### Step 4 — (Optional) Extract full feature table for EDA / calibration

Runs Phase 1 on all samples (any split) and produces the **full** 14-column
feature table including `min_temp`, `max_temp`, `median_temp`,
`ambient_corr_median`, and `ambient_corr_mean`.

```bash
python skills/sensor_group_segment/scripts/extract_features.py --split train
python skills/sensor_group_segment/scripts/extract_features.py --split test
python skills/sensor_group_segment/scripts/extract_features.py          # all samples
```

Output: `data/features/sensor_group_segment/{split}_features.parquet`

---

### Step 5 — (Optional) Re-calibrate thresholds

Derives Layer 1 thresholds from the physical distributions of each
`hive_size_bucket` in the train set (p90 for MAX rules, p10 for MIN rules).
Review output before updating `thresholds.yaml`.

```bash
python skills/sensor_group_segment/scripts/calibrate_thresholds.py
```

Outputs:
- `data/calibration/calibration_report.csv` — percentile table per (hive_size, metric)

---

## Inputs / Outputs

| Item | Path |
|---|---|
| Raw samples | `data/samples/temperature-export/group_{id}/{date}/` |
| Train symlinks | `data/samples/temperature-export/train/` |
| Test symlinks | `data/samples/temperature-export/test/` |
| Split manifest | `data/samples/split_manifest.csv` |
| Ground truth (shared) | `ground_truth/ground_truth_statuess_ca_2026.csv` |
| Extraction plan | `skills/sensor_group_segment/config/extraction_plan.py` |
| Split config | `skills/sensor_group_segment/config/split_config.py` |
| Thresholds | `skills/sensor_group_segment/config/thresholds.yaml` |
| Grading results (per sensor) | `data/results/<run>/group_{id}/{date}/{id}_{date}_sensor_group_segment.parquet` |
| Grading results (hive-size summary) | `data/results/<run>/group_{id}/{date}/{id}_{date}_sensor_group_segment_summary.parquet` |
| Grading results (combined) | `data/results/<run>/results.parquet` |
| Full feature table | `data/features/sensor_group_segment/{split}_features.parquet` |

---

## Train / Test Split

Valid + invalid only (`needs_recalibration` excluded). 75/25 stratified by status.

| | Train | Test | Total |
|---|---|---|---|
| valid | 269 | 90 | 359 |
| invalid | 154 | 52 | 206 |
| **TOTAL** | **423** | **142** | **565** |

---

## Feature Columns

### `compute()` — lean grading features (used by `run.py`)

| Column | Description |
|--------|-------------|
| `group_id` | Beekeeper group |
| `date` | Evaluation date |
| `sensor_mac_address` | Hive sensor |
| `gateway_mac_address` | Primary gateway (metadata only — highest read count) |
| `hive_size_bucket` | Model prediction: `small` / `medium` / `large` |
| `std_dev` | Std dev of hourly internal temp |
| `iqr` | IQR of hourly internal temp (robust companion to std_dev) |
| `ambient_correlation` | Pearson r vs local-gateway median ambient (robust) |
| `mean_temp` | Mean internal temp (°C) |
| `percent_comfort` | % hours in brood zone [32°C, 36°C] |
| `n_readings` | Aligned sensor–gateway hours |

### `compute_full()` — full feature table (used by `extract_features.py`)

All of the above plus:

| Column | Description |
|--------|-------------|
| `ambient_corr_median` | Pearson r vs median ambient across local gateways |
| `ambient_corr_mean` | Pearson r vs mean ambient across local gateways |
| `min_temp` | Min hourly internal temp |
| `max_temp` | Max hourly internal temp |
| `median_temp` | Median hourly internal temp |

---

## Phases

**Phase 1 — Feature Engineering** (`compute()` in `src/model_monitor/metrics/sensor_group_segment.py`)

Computes per-sensor biological signatures from aligned hourly sensor + gateway data.
Alignment is per-sensor: only the gateways that sensor communicated through are used
for the ambient reference.

**Phase 2 — Grading** (`grade()` in `src/model_monitor/metrics/sensor_group_segment.py`)

Compares predicted hive size against observed physics:
- **Rule A → FAIL** — large hive but physics are weak (std_dev, iqr, corr, or comfort zone)
- **Rule B → WARNING** — medium hive but mean_temp too cold
- **Rule C → WARNING** — small hive but too stable (std_dev, iqr) or too warm (mean_temp)
- **Default → PASS** — physics align with prediction

Thresholds: `config/thresholds.yaml` (calibrated at p90/p10 from train distributions).

> See `spec/spec.txt` for the full logic, rules, and threshold rationale.
