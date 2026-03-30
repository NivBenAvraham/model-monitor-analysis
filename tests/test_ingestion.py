"""Tests for model_monitor.ingestion."""

from datetime import date
from model_monitor.ingestion import raw_bee_frames_table


def test_raw_bee_frames_table_legacy() -> None:
    assert raw_bee_frames_table(date(2026, 3, 9)) == "supervised_beeframes"
    assert raw_bee_frames_table("2026-01-01") == "supervised_beeframes"


def test_raw_bee_frames_table_current() -> None:
    assert raw_bee_frames_table(date(2026, 3, 10)) == "unified_bee_frames"
    assert raw_bee_frames_table("2026-03-15") == "unified_bee_frames"


def test_raw_bee_frames_table_switch_boundary() -> None:
    assert raw_bee_frames_table(date(2026, 3, 9))  == "supervised_beeframes"
    assert raw_bee_frames_table(date(2026, 3, 10)) == "unified_bee_frames"
