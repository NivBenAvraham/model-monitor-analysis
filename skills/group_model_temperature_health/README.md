# group_model_temperature_health

**Layer 2 — Model Health per Group per Date**

> Status: **not yet implemented** — skeleton only.

## Purpose

Given the per-sensor outputs of `sensor_group_segment` (Layer 1), determine whether the bee-frames model is working correctly for a given `(group_id, date)`.

- **Input:** aggregated sensor_group_segment results
- **Output:** `VALID` / `INVALID` per `(group_id, date)`
- **Ground truth:** `ground_truth/ground_truth_statuess_ca_2026.csv`

## Architecture

```
sensor_group_segment (Layer 1)
  per-sensor PASS/WARNING/FAIL
        │
        │  aggregate per (group_id, date)
        ▼
group_model_temperature_health (Layer 2)
  VALID / INVALID per group per date
```

## Data

| | Train | Test | Total |
|---|---|---|---|
| valid | 269 | 90 | 359 |
| invalid | 154 | 52 | 206 |
| **TOTAL** | **423** | **142** | **565** |

Split manifest: `data/samples/split_manifest.csv`
Ground truth: `ground_truth/ground_truth_statuess_ca_2026.csv`

## Planned structure

```
skills/group_model_temperature_health/
  spec/spec.txt        ← requirements and open questions
  config/              ← thresholds (to be defined)
  scripts/
    run.py             ← aggregate Layer 1 → VALID/INVALID
    calibrate.py       ← calibrate using valid/invalid ground truth
  notebooks/           ← exploration
```

## Next steps

See `spec/spec.txt` for open design questions and the strongest signals identified in the Layer 1 exploration.
