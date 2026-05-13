"""
Stub tests for src/model_monitor/decision/triage_rule.py
"""

import pytest

from model_monitor.decision.triage_rule import score_group_date


def _signals(
    clipping=True,
    inspection=True,
    thermoreg=True,
    auto_review=True,
):
    """Build a minimal 4-signal metric_results list."""
    return [
        {"metric_name": "clipping_diff",          "pass_metric": clipping},
        {"metric_name": "inspection_discrepancy",  "pass_metric": inspection},
        {"metric_name": "thermoreg_dipping",       "pass_metric": thermoreg},
        {"metric_name": "auto_review_score",       "pass_metric": auto_review},
    ]


def _assert_shape(result: dict) -> None:
    for key in ("prediction", "passed_metrics", "last_status", "reason", "signals"):
        assert key in result


# ---------------------------------------------------------------------------
# auto_valid cases
# ---------------------------------------------------------------------------

class TestAutoValid:
    def test_all_signals_pass_last_valid(self):
        result = score_group_date(_signals(), last_status="valid")
        _assert_shape(result)
        assert result["prediction"]     == "auto_valid"
        assert result["passed_metrics"] == 4
        assert result["reason"]         is None

    def test_signal_fires_false_but_all_ran_last_valid(self):
        # pass_metric=False still counts toward passed_metrics (not None)
        result = score_group_date(_signals(clipping=False), last_status="valid")
        _assert_shape(result)
        assert result["prediction"]     == "auto_valid"
        assert result["passed_metrics"] == 4

    def test_all_signals_false_last_valid(self):
        # All 4 signals fired (False) but still have data — auto_valid
        result = score_group_date(
            _signals(clipping=False, inspection=False, thermoreg=False, auto_review=False),
            last_status="valid",
        )
        _assert_shape(result)
        assert result["prediction"]     == "auto_valid"
        assert result["passed_metrics"] == 4


# ---------------------------------------------------------------------------
# needs_review — last_status not valid
# ---------------------------------------------------------------------------

class TestNeedsReviewLastStatus:
    def test_last_status_invalid(self):
        result = score_group_date(_signals(), last_status="invalid")
        _assert_shape(result)
        assert result["prediction"] == "needs_review"
        assert result["reason"]     == "last_status_not_valid"

    def test_last_status_needs_recalibration(self):
        result = score_group_date(_signals(), last_status="needs_recalibration")
        _assert_shape(result)
        assert result["prediction"] == "needs_review"
        assert result["reason"]     == "last_status_not_valid"

    def test_last_status_none(self):
        result = score_group_date(_signals(), last_status=None)
        _assert_shape(result)
        assert result["prediction"] == "needs_review"
        assert result["reason"]     == "last_status_not_valid"


# ---------------------------------------------------------------------------
# needs_review — insufficient signal data
# ---------------------------------------------------------------------------

class TestNeedsReviewInsufficientData:
    def test_one_signal_none_last_valid(self):
        result = score_group_date(_signals(auto_review=None), last_status="valid")
        _assert_shape(result)
        assert result["prediction"]     == "needs_review"
        assert result["passed_metrics"] == 3
        assert result["reason"]         == "insufficient_signal_data"

    def test_two_signals_none_last_valid(self):
        result = score_group_date(
            _signals(auto_review=None, thermoreg=None), last_status="valid"
        )
        _assert_shape(result)
        assert result["prediction"]     == "needs_review"
        assert result["passed_metrics"] == 2

    def test_all_signals_none_last_invalid(self):
        result = score_group_date(_signals(
            clipping=None, inspection=None, thermoreg=None, auto_review=None
        ), last_status="invalid")
        _assert_shape(result)
        assert result["prediction"]     == "needs_review"
        assert result["passed_metrics"] == 0
        assert "insufficient_signal_data" in result["reason"]
        assert "last_status_not_valid"    in result["reason"]


# ---------------------------------------------------------------------------
# Signal dict in output
# ---------------------------------------------------------------------------

class TestSignalsOutput:
    def test_signals_echoed_correctly(self):
        result = score_group_date(
            _signals(clipping=False, auto_review=None), last_status="valid"
        )
        assert result["signals"]["clipping_diff"]          is False
        assert result["signals"]["inspection_discrepancy"] is True
        assert result["signals"]["thermoreg_dipping"]      is True
        assert result["signals"]["auto_review_score"]      is None

    def test_unknown_signal_ignored(self):
        metrics = _signals() + [{"metric_name": "unknown_metric", "pass_metric": False}]
        result = score_group_date(metrics, last_status="valid")
        _assert_shape(result)
        assert "unknown_metric" not in result["signals"]
        assert result["passed_metrics"] == 4


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_empty_metric_results_raises(self):
        with pytest.raises(ValueError):
            score_group_date([], last_status="valid")

    def test_last_status_echoed(self):
        result = score_group_date(_signals(), last_status="valid")
        assert result["last_status"] == "valid"
