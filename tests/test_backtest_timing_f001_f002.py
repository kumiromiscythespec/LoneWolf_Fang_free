# BUILD_ID: 2026-05-28_ethusdt_f001_f002_timing_fix_tests_v1
from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import backtest  # noqa: E402


def test_signal_bar_cannot_fill_at_signal_open_and_fills_next_open() -> None:
    opens = [100.0, 101.0, 102.0]

    signal_i, exec_i = backtest._entry_signal_exec_indices(entry_ptr_value=2, n_entry_bars=len(opens))

    assert signal_i == 1
    assert exec_i == 2
    assert opens[exec_i] == 102.0
    assert opens[exec_i] != opens[signal_i]


def test_missing_next_bar_fails_closed() -> None:
    assert backtest._entry_signal_exec_indices(entry_ptr_value=3, n_entry_bars=3) is None
    assert not backtest._is_strict_next_entry_bar(0, 10 * 60 * 1000, 5 * 60 * 1000)


def test_signal_rr_distance_is_translated_to_next_open_basis() -> None:
    stop, tp = backtest._translate_signal_rr_to_exec_basis(
        signal_entry=100.0,
        signal_stop=95.0,
        signal_tp=110.0,
        exec_entry=103.0,
        direction="long",
    )

    assert stop == pytest.approx(98.0)
    assert tp == pytest.approx(113.0)
    assert 103.0 - stop == pytest.approx(5.0)
    assert tp - 103.0 == pytest.approx(10.0)


def test_same_bar_tp_and_sl_both_touched_uses_conservative_stop_first() -> None:
    assert (
        backtest._resolve_tp_sl_same_bar(
            side="long",
            bar_open=100.0,
            bar_high=112.0,
            bar_low=94.0,
            bar_close=111.0,
            tp=110.0,
            sl=95.0,
        )
        == "STOP_HIT"
    )
    assert backtest.INTRABAR_ORDER_POLICY == "conservative_stop_first"


def test_filter_alignment_uses_completed_1h_bar_only() -> None:
    one_hour = 60 * 60 * 1000
    filter_ts = [0, one_hour, 2 * one_hour]
    decision_ts = (2 * one_hour) + (5 * 60 * 1000)

    j = backtest._advance_filter_ptr(filter_ts, -1, decision_ts - one_hour)

    assert j == 1
    assert filter_ts[j] == one_hour
