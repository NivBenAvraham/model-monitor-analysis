# AGENTS.md — Guidelines for AI Working in This Repo

This file applies to any AI agent (Cursor, Claude, Codex, etc.) contributing to `model-monitor-analysis`.

## What This Repo Does

Monitors BeeFrame model performance. Computes metrics, evaluates model health, and produces reproducible reports. All logic is deterministic — no LLMs involved at runtime.

## Four Layers — Keep Them Separate

| Layer | Location | Responsibility |
|---|---|---|
| Ingestion | `src/model_monitor/ingestion/` | Load data from SQL / AWS only |
| Metrics | `src/model_monitor/metrics/` | Compute metric values from data |
| Decision | `src/model_monitor/decision/` | Evaluate health from metrics |
| Reporting | `src/model_monitor/reporting/` | Format and output results |

Never mix these concerns. A metrics function must not load data. A decision function must not format output.

## Hard Rules

- **No LLM calls** in `decision/`, `metrics/`, `ingestion/`, or `reporting/`
- **Model health** has exactly three outcomes: `VALID`, `NEEDS_CALIBRATION`, `INVALID`
- **Thresholds** go in `configs/thresholds.yaml` — never hardcode a number in Python
- **Notebooks** go in `notebooks/exploration/` — exploration only, never imported by `src/`
- **Tests** — write a stub test for every new module added under `src/`

## Style

- Small, focused functions — one responsibility per function
- No abstractions or base classes unless explicitly asked
- No new dependencies without being asked
- Prefer explicit over clever
