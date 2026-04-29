"""
Temperature Health Rule — decision layer.

Predicts whether a (group_id, date) is VALID using group-level temperature metrics.
Priority: minimise FP (invalid groups called VALID) above all else.

Decision logic
--------------
Step 1 — Mandatory gates (ATV, R3, R5, R7)
    All four must pass.  Failing ANY gate → INVALID, confidence 1.

    ATV  ambient_temperature_volatility — night-to-night ambient stability.
    R3   bucket_reference_adherence     — bucket means inside per-bucket reference bands.
    R5   bucket_temporal_stability      — bucket daily-mean std within per-bucket cap.
    R7   bucket_diurnal_amplitude       — per-bucket within-day swing under cap.

    Gate set chosen 2026-04-27 via exhaustive anchor-constrained search over all
    847 combinations of 1–7 gates from 10 metrics.  Only combinations that
    perfectly separate all 19 anchor pairs (14 valid, 5 invalid) were kept (438).
    The winner on the full train set with 1miss_ok scoring:
      ATV + R3 + R5 + R7  →  TP=80  FP=11  P=0.879  Sp=0.929
      (prior R6c+R3+R4+R7 →  TP=72  FP=11  P=0.867  Sp=0.857)

Step 2 — valid_score over 6 NON-GATE metrics
    scored_metrics = {
        ambient_stability      (R1),
        ambient_range          (R2),
        sensor_spread_within_bucket (R4),
        small_hive_ambient_tracking (R6a),
        large_hive_thermoregulation (R6b),
        bucket_temperature_ordering (R6c),
    }
    valid_score = pass_count / n_assessed.

Step 3 — confidence mapping
    valid_score >= score_confidence_5_min   →  confidence 5, prediction VALID
    valid_score >= score_confidence_4_min   →  confidence 4, prediction VALID
    otherwise                               →  confidence ≤ 3, prediction INVALID

Step 4 — Layer 1 gate (optional)
    If l1_pct_pass < l1_min_pass_rate → cap confidence at 2, prediction INVALID.
    Skip if l1_pct_pass is None (Layer 1 results unavailable).

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
        from the denominator of valid_score.
    l1_pct_pass:
        Fraction of sensors that passed the Layer 1 sensor_group_segment check
        (value in [0, 1]).  Pass ``None`` to skip the L1 gate.

    Returns
    -------
    dict with keys:
        ``pass_count``     — int, scored (non-gate) metrics that passed.
        ``n_assessed``     — int, scored metrics with a non-None result.
        ``valid_score``    — float in [0, 1], pass_count / n_assessed.
        ``gate_results``   — dict {gate_name: bool | None}.
        ``failed_gates``   — list[str], gate metrics that returned False.
        ``l1_pct_pass``    — float | None, echoed back.
        ``l1_gate_pass``   — bool, whether the L1 gate was met.
        ``confidence``     — int 1–5 (5 = most confident VALID).
        ``prediction``     — "VALID" or "INVALID".
        ``metrics_failed`` — list[str], scored metrics that returned False.
        ``metrics_error``  — list[str], metrics (any role) that returned None.

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
    if valid_score >= conf5_min:
        confidence = 5
        prediction = "VALID"
    elif valid_score >= conf4_min:
        confidence = 4
        prediction = "VALID"
    else:
        gap        = int((conf4_min - valid_score) * n_assessed) + 1
        confidence = max(1, 3 - gap)
        prediction = "INVALID"

    # ── Step 4: Layer 1 gate ───────────────────────────────────────────────────
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
