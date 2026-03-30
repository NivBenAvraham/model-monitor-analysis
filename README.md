# model-monitor-analysis

Analysis repository for BeeFrame model performance monitoring at BeeHero.

## Purpose

- Reproduce current monitoring metrics
- Improve existing metrics and evaluate new ones
- Determine model health: `VALID` / `NEEDS_CALIBRATION` / `INVALID`
- Provide clear, reproducible explanations for every decision

All decisions are based on deterministic logic (metrics + thresholds + rules). No LLMs at runtime.

## Structure

```
src/model_monitor/
  ingestion/    — data loading from SQL / AWS
  metrics/      — metric computation functions
  decision/     — model health evaluation logic
  reporting/    — output formatting and reports
  utils/        — shared helpers

configs/
  thresholds.yaml   — all threshold values (never hardcoded)

notebooks/
  exploration/      — exploratory analysis only, not production logic

tests/            — one test file per submodule
```

## Setup

```bash
pip install -e ".[dev]"
```

## Run Tests

```bash
pytest
```

## Pull Local Data Samples

Samples are saved to `data/samples/` (gitignored — never committed).

```bash
# Default: last 7 days, 1000 rows per table
python scripts/pull_samples.py

# Custom range
python scripts/pull_samples.py --days 14 --limit 5000
```

Before running, set your Athena config in `scripts/pull_samples.py`:
- `S3_STAGING_DIR`
- `REGION`
- `CURATED_DATABASE` / `RAW_DATABASE`
- `WORKGROUP`
