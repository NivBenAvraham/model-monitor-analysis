# hives_temperature_plot_decision

Evaluates whether a BeeFrame model output is **Valid**, **Invalid**, or **Needs Review**
for a given `(date, group_id)` pair — mimicking how a data analyst reads the
temperature scatter plot.

---

## How it works

Two signals are combined into a `valid_score` and an `invalid_score` (both 0–10):

| Signal | Source | Weight |
|---|---|---|
| **Analyst tags** | `data_analyst_plot_decisions/valid.txt` and `invalid.txt` | Primary — exact `(date, group_id)` match overrides features |
| **Physics rules** | Raw parquet temperature data (6 rules, see below) | Secondary — always computed, adds nuance for untagged pairs |

The final decision:
```
valid_pct = valid_score / (valid_score + invalid_score)

valid_pct ≥ 80 %              →  Valid
invalid_score ≥ 5.0           →  Invalid
otherwise                     →  Needs Review
```

### The 6 physics rules

| Rule | What is checked | Key insight |
|---|---|---|
| R1 | Ambient stability (CV of gateway temp) | Noisy gateway = unreliable reference |
| R2 | Ambient temperature range | Too cold or too hot = sensor/environment issue |
| R3 | Per-bucket reference adherence | Each bucket judged against its own reference lines |
| R4 | Per-bucket sensor spread | Sensors in the same bucket should agree with each other |
| R5 | Per-bucket temporal stability | **Large** must be flat; **small** may vary (it tracks ambient) |
| R6 | Per-bucket ambient correlation + ordering | **Small** should correlate with ambient; **large** should NOT (self-regulating). Buckets must be ordered small < medium < large. |

Full scoring logic and all threshold rationale: [`spec/spec.txt`](spec/spec.txt)

---

## Usage

Prerequisites: raw parquet files must exist locally.
Pull them first if needed:
```bash
python skills/temperature_data_export/scripts/pull_samples.py --group-id 36 --date 2026-03-01
```

### Single pair
```bash
python skills/hives_temperature_plot_decision/scripts/decide.py \
    --group 36 --date 2026-03-01
```

Example output:
```json
{
  "date": "2026-03-01",
  "group_id": 36,
  "valid_score": 7.0,
  "invalid_score": 2.0,
  "valid_pct": 77.8,
  "decision": "Needs Review",
  "tag_source": "untagged",
  "tag_reason": null
}
```

### Single pair with per-rule reasons
```bash
python skills/hives_temperature_plot_decision/scripts/decide.py \
    --group 790 --date 2026-02-16 --verbose
```

### Batch mode
Provide a CSV with `date,group_id` columns:
```bash
python skills/hives_temperature_plot_decision/scripts/decide.py \
    --batch pairs.csv --output results.csv --verbose
```

---

## Output fields

| Field | Description |
|---|---|
| `date` | Evaluation date |
| `group_id` | Beekeeper group |
| `valid_score` | 0–10, combined valid evidence |
| `invalid_score` | 0–10, combined invalid evidence |
| `valid_pct` | valid_score / total × 100 |
| `decision` | `Valid` / `Invalid` / `Needs Review` |
| `tag_source` | `valid (confidence=N)` / `invalid (perfect\|normal)` / `untagged` |
| `tag_reason` | Analyst reason string (invalid entries only) |
| `feature_reasons` | List of rule-level reasons (only with `--verbose`) |

---

## Adding more tags

Tags live in plain CSV files — just append rows:

**`data_analyst_plot_decisions/valid.txt`**
```
'YYYY-MM-DD',group_id,confidence
```
Confidence: `5`=perfect, `4`=good, `3`=low warning, `2`=medium warning, `1`=high warning

**`data_analyst_plot_decisions/invalid.txt`**
```
'YYYY-MM-DD',group_id,reason
```
Add `| perfect invalid` to the reason when you are certain.

After adding tags, re-run and check that tagged pairs now score as expected.
If they don't, adjust the configurable thresholds at the top of `scripts/decide.py`.

---

## Calibrating thresholds

All decision and feature thresholds are constants at the top of
[`scripts/decide.py`](scripts/decide.py). They are grouped and commented by rule.
Start with the decision gates:

```python
VALID_PCT_THRESHOLD = 0.80   # raise to be more conservative about calling Valid
INVALID_MIN_SCORE   = 5.0    # lower to catch more invalids earlier
```

Then adjust per-rule thresholds (R1–R6) based on the distribution of scores
you see across your tagged corpus.
