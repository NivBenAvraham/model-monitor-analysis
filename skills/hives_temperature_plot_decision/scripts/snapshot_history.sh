#!/usr/bin/env bash
# snapshot_history.sh — save a complete, dated snapshot of the current decision run.
#
# Usage (from repo root, venv active):
#   bash skills/hives_temperature_plot_decision/scripts/snapshot_history.sh
#
# Creates four files in history/:
#   {DATE}_train_decisions.csv
#   {DATE}_train_decisions.meta.json
#   {DATE}_spec.txt
#   {DATE}_decide.py

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
SKILL_DIR="${REPO_ROOT}/skills/hives_temperature_plot_decision"
HIST_DIR="${SKILL_DIR}/history"
DATE="$(date +%Y-%m-%d)"

echo "=== Snapshot: ${DATE} ==="
echo "REPO_ROOT : ${REPO_ROOT}"

# ── 1. Run all train decisions ────────────────────────────────────────────────
PAIRS_TMP="$(mktemp /tmp/train_pairs_XXXX.csv)"
python3 - <<PY
import pandas as pd
df = pd.read_csv("${REPO_ROOT}/data/samples/split_manifest.csv")
df[df["split"]=="train"][["date","group_id"]].to_csv("${PAIRS_TMP}", index=False)
PY

echo "Running decide() on $(wc -l < "${PAIRS_TMP}") train pairs …"
python3 "${SKILL_DIR}/scripts/decide.py" \
    --batch  "${PAIRS_TMP}" \
    --output "${HIST_DIR}/${DATE}_train_decisions.csv" \
    --verbose

rm -f "${PAIRS_TMP}"

# ── 2. Write meta JSON (thresholds + counts) ──────────────────────────────────
python3 - <<PY
import importlib.util, pandas as pd, json
from datetime import date

spec = importlib.util.spec_from_file_location(
    "decide", "${SKILL_DIR}/scripts/decide.py")
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

df = pd.read_csv("${HIST_DIR}/${DATE}_train_decisions.csv")

meta = {
    "run_date":       "${DATE}",
    "split":          "train",
    "n_pairs":        len(df),
    "decision_counts": df["decision"].value_counts().to_dict(),
    "thresholds": {
        "VALID_PCT_THRESHOLD":      mod.VALID_PCT_THRESHOLD,
        "INVALID_MIN_SCORE":        mod.INVALID_MIN_SCORE,
        "AMBIENT_CV_HIGH":          mod.AMBIENT_CV_HIGH,
        "AMBIENT_CV_MED":           mod.AMBIENT_CV_MED,
        "AMBIENT_CV_LOW":           mod.AMBIENT_CV_LOW,
        "AMBIENT_MIN_THRESHOLD":    mod.AMBIENT_MIN_THRESHOLD,
        "AMBIENT_MAX_THRESHOLD":    mod.AMBIENT_MAX_THRESHOLD,
        "BUCKET_ADHERENCE_WEIGHTS": mod.BUCKET_ADHERENCE_WEIGHTS,
        "ADHERENCE_MIN":            mod.ADHERENCE_MIN,
        "ADHERENCE_MED":            mod.ADHERENCE_MED,
        "ADHERENCE_GOOD":           mod.ADHERENCE_GOOD,
        "SENSOR_SPREAD_HIGH":       mod.SENSOR_SPREAD_HIGH,
        "SENSOR_SPREAD_MED":        mod.SENSOR_SPREAD_MED,
        "SENSOR_SPREAD_LOW":        mod.SENSOR_SPREAD_LOW,
        "LARGE_CORR_MAX":           mod.LARGE_CORR_MAX,
        "SMALL_CORR_MIN":           mod.SMALL_CORR_MIN,
        "BUCKET_SEP_MIN":           mod.BUCKET_SEP_MIN,
        "BUCKET_SEP_GOOD":          mod.BUCKET_SEP_GOOD,
        "PERFECT_INVALID_BONUS":    mod.PERFECT_INVALID_BONUS,
        "NORMAL_INVALID_BONUS":     mod.NORMAL_INVALID_BONUS,
    },
}

out = "${HIST_DIR}/${DATE}_train_decisions.meta.json"
with open(out, "w") as f:
    json.dump(meta, f, indent=2)
print(f"Meta written → {out}")
print(json.dumps(meta["decision_counts"], indent=2))
PY

# ── 3. Snapshot spec and script ───────────────────────────────────────────────
cp "${SKILL_DIR}/spec/spec.txt"     "${HIST_DIR}/${DATE}_spec.txt"
cp "${SKILL_DIR}/scripts/decide.py" "${HIST_DIR}/${DATE}_decide.py"

echo ""
echo "Snapshot complete:"
ls -lh "${HIST_DIR}/${DATE}"*
