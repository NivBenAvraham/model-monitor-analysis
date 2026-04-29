"""
Temperature Health Rule — decision layer.

Predicts whether a (group_id, date) is VALID using group-level temperature metrics.
Priority: minimise FP (invalid groups called VALID) above all else.

Decision logic
--------------

Step 1 — 4 Mandatory gates (ATV, R3, R5, R7)
    ANY gate fails  →  INVALID, confidence 1  (no scoring done).
    ALL gates pass  →  proceed to Step 2.

    ATV  ambient_temperature_volatility — night-to-night ambient jump check.
    R3   bucket_reference_adherence     — bucket means inside per-bucket reference bands.
    R5   bucket_temporal_stability      — bucket daily-mean std within per-bucket cap.
    R7   bucket_diurnal_amplitude       — per-bucket within-day swing under cap.

    Gate set chosen 2026-04-27 via exhaustive anchor-constrained search (847 combos).
    Perfectly separates all 19 analyst anchor pairs (14 valid, 5 invalid).
    Train result: TP=80  FP=11  Precision=0.879  Specificity=0.929
    Test result:  TP=27  FP=6   Precision=0.818  Specificity=0.885

Step 2 — Score 6 non-gate metrics
    R1   ambient_stability
    R2   ambient_range
    R4   sensor_spread_within_bucket
    R6a  small_hive_ambient_tracking
    R6b  large_hive_thermoregulation
    R6c  bucket_temperature_ordering

    valid_score = pass_count / n_assessed  (None metrics excluded from denominator).

    The effective threshold is 5/6 — both confidence 4 and confidence 5 predict VALID.
    Confidence is informational only; the binary prediction is what matters.

    valid_score = 6/6  →  VALID, confidence 5
    valid_score = 5/6  →  VALID, confidence 4
    valid_score ≤ 4/6  →  INVALID, confidence 1–3

    Why 5/6 and not 6/6:
      Requiring 6/6 gives TP=72 with the same 11 FPs.
      Allowing one miss (5/6) recovers 8 TPs at zero precision cost.
      The 6 scored metrics have near-zero discriminating power on the
      remaining FPs (all 11 pass every scored metric at 100%) — so the
      scoring layer only affects recall, never precision.

All thresholds come from configs/thresholds.yaml — never hardcoded here.
"""

from __future__ import annotations

import logging
from pathlib import Path

import yaml

log = logging.getLogger(__name__)

_REPO_ROOT       = Path(__file__).resolve().parents[3]
_THRESHOLDS_PATH = _REPO_ROOT / "configs/thresholds.yaml"

# Mandatory gate metric names
_ATV_GATE = "ambient_temperature_volatility"
_R3_GATE  = "bucket_reference_adherence"
_R5_GATE  = "bucket_temporal_stability"
_R7_GATE  = "bucket_diurnal_amplitude"
_GATES    = (_ATV_GATE, _R3_GATE, _R5_GATE, _R7_GATE)


def _load_thresholds() -> dict:
    with open(_THRESHOLDS_PATH) as f:
        return yaml.safe_load(f)["decision"]["temperature_health_rule"]


def score_group_date(
    metric_results: list[dict],
    l1_pct_pass: float | None = None,
) -> dict:
    """Score one (group_id, date) from its temperature metric results.

    Parameters
    ----------
    metric_results:
        List of dicts, one per temperature metric.  Each dict must contain at
        minimum: ``metric_name`` (str) and ``pass_metric`` (bool | None).
        ``pass_metric=None`` means the metric could not be assessed — excluded
        from the scored denominator.
    l1_pct_pass:
        Not yet connected — always pass None.  Reserved for future Layer 1
        sensor_group_segment integration.

    Returns
    -------
    dict with keys:
        ``prediction``     — "VALID" or "INVALID" (the only output that matters).
        ``confidence``     — int 1–5, informational (4 and 5 both mean VALID).
        ``failed_gates``   — list[str], gate metrics that returned False.
        ``gate_results``   — dict {gate_name: bool | None}.
        ``valid_score``    — float in [0, 1], pass_count / n_assessed.
        ``pass_count``     — int, scored (non-gate) metrics that passed.
        ``n_assessed``     — int, scored metrics with a non-None result.
        ``metrics_failed`` — list[str], scored metrics that returned False.
        ``metrics_error``  — list[str], metrics (any role) that returned None.
        ``l1_pct_pass``    — float | None, echoed back.
        ``l1_gate_pass``   — bool, always True until L1 is connected.

    Raises
    ------
    ValueError
        If metric_results is empty.
    """
    if not metric_results:
        raise ValueError("metric_results must not be empty")

    cfg            = _load_thresholds()
    l1_min         = float(cfg["l1_min_pass_rate"])
    conf5_min      = float(cfg["score_confidence_5_min"])
    conf4_min      = float(cfg["score_confidence_4_min"])
    atv_mandatory  = bool(cfg.get("atv_mandatory", True))
    r3_mandatory   = bool(cfg.get("r3_mandatory",  True))
    r5_mandatory   = bool(cfg.get("r5_mandatory",  True))
    r7_mandatory   = bool(cfg.get("r7_mandatory",  True))

    by_name: dict[str, bool | None] = {
        m.get("metric_name", "unknown"): m.get("pass_metric")
        for m in metric_results
    }

    # ── Step 1: mandatory gates ────────────────────────────────────────────────
    gate_active = {
        _ATV_GATE: atv_mandatory,
        _R3_GATE:  r3_mandatory,
        _R5_GATE:  r5_mandatory,
        _R7_GATE:  r7_mandatory,
    }
    gate_results: dict[str, bool | None] = {}
    failed_gates: list[str] = []
    metrics_error: list[str] = []

    for gate_name in _GATES:
        if not gate_active[gate_name]:
            gate_results[gate_name] = None
            continue
        result = by_name.get(gate_name)
        if result is None:
            gate_results[gate_name] = None
            metrics_error.append(gate_name)
            continue
        gate_results[gate_name] = bool(result)
        if not result:
            failed_gates.append(gate_name)

    if failed_gates:
        log.debug("temperature_health_rule: gate FAIL %s → INVALID", failed_gates)
        return {
            "pass_count":      0,
            "n_assessed":      0,
            "valid_score":     0.0,
            "gate_results":    gate_results,
            "failed_gates":    failed_gates,
            "l1_pct_pass":     l1_pct_pass,
            "l1_gate_pass":    True,
            "confidence":      1,
            "prediction":      "INVALID",
            "metrics_failed":  failed_gates,
            "metrics_error":   metrics_error,
        }

    # ── Step 2: valid_score over the 6 non-gate metrics ────────────────────────
    pass_count: int = 0
    n_assessed: int = 0
    metrics_failed: list[str] = []

    for m in metric_results:
        name        = m.get("metric_name", "unknown")
        pass_metric = m.get("pass_metric")

        if name in _GATES:
            continue   # already handled in step 1

        if pass_metric is None:
            metrics_error.append(name)
            continue

        n_assessed += 1
        if pass_metric:
            pass_count += 1
        else:
            metrics_failed.append(name)

    valid_score = pass_count / n_assessed if n_assessed > 0 else 0.0

    # ── Step 3: confidence mapping ─────────────────────────────────────────────
    # Effective threshold is 5/6 — both conf 4 and conf 5 predict VALID.
    # Confidence is informational; the binary prediction is what matters.
    if valid_score >= conf5_min:      # 6/6
        confidence = 5
        prediction = "VALID"
    elif valid_score >= conf4_min:    # 5/6 — one miss allowed
        confidence = 4
        prediction = "VALID"
    else:                             # ≤ 4/6
        gap        = int((conf4_min - valid_score) * n_assessed) + 1
        confidence = max(1, 3 - gap)
        prediction = "INVALID"

    # ── Layer 1 gate (not yet connected — l1_pct_pass is always None) ─────────
    if l1_pct_pass is None:
        l1_gate_pass = True
    else:
        l1_gate_pass = float(l1_pct_pass) >= l1_min

    if not l1_gate_pass:
        confidence = min(confidence, 2)
        prediction = "INVALID"

    log.debug(
        "temperature_health_rule: gates=%s l1=%.3f(gate=%s) "
        "score=%.2f(%d/%d) → conf=%d %s",
        gate_results,
        l1_pct_pass if l1_pct_pass is not None else -1,
        l1_gate_pass,
        valid_score, pass_count, n_assessed,
        confidence, prediction,
    )

    return {
        "pass_count":     pass_count,
        "n_assessed":     n_assessed,
        "valid_score":    round(valid_score, 4),
        "gate_results":   gate_results,
        "failed_gates":   failed_gates,
        "l1_pct_pass":    l1_pct_pass,
        "l1_gate_pass":   l1_gate_pass,
        "confidence":     confidence,
        "prediction":     prediction,
        "metrics_failed": metrics_failed,
        "metrics_error":  metrics_error,
    }
