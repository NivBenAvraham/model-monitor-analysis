# Decision History

Each dated snapshot is a complete, self-contained record of one evaluation run.
All four files must be present for a snapshot to be valid.

## File convention

```
{YYYY-MM-DD}_train_decisions.csv        — per-(date, group_id) results (423 rows)
{YYYY-MM-DD}_train_decisions.meta.json  — decision counts + every threshold value active at run time
{YYYY-MM-DD}_spec.txt                   — copy of spec/spec.txt at run time
{YYYY-MM-DD}_decide.py                  — copy of scripts/decide.py at run time
```

## How to create a new snapshot

Run this from the repo root (after activating the venv):

```bash
source .venv/bin/activate

DATE=$(date +%Y-%m-%d)
HIST="skills/hives_temperature_plot_decision/history"

# 1 — run all train decisions and save CSV + meta JSON
python skills/hives_temperature_plot_decision/scripts/decide.py \
    --batch <(python - <<'PY'
import pandas as pd
df = pd.read_csv("data/samples/split_manifest.csv")
print(df[df.split=="train"][["date","group_id"]].to_csv(index=False))
PY
) --output "${HIST}/${DATE}_train_decisions.csv"

# 2 — snapshot the spec and script
cp skills/hives_temperature_plot_decision/spec/spec.txt     "${HIST}/${DATE}_spec.txt"
cp skills/hives_temperature_plot_decision/scripts/decide.py "${HIST}/${DATE}_decide.py"
```

Or run the helper script:
```bash
bash skills/hives_temperature_plot_decision/scripts/snapshot_history.sh
```

## Comparing two snapshots

```python
import pandas as pd, json

old = pd.read_csv("history/2026-04-15_train_decisions.csv")
new = pd.read_csv("history/YYYY-MM-DD_train_decisions.csv")

diff = old.merge(new, on=["date","group_id"], suffixes=("_old","_new"))
changed = diff[diff["decision_old"] != diff["decision_new"]]
print(changed[["date","group_id","decision_old","decision_new","valid_pct_old","valid_pct_new"]])
```
