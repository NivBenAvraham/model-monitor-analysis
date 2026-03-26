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
