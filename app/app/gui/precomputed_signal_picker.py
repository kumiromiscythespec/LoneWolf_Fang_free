# BUILD_ID: 2026-05-08_free_precomputed_gui_picker_wiring_v1
from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import precomputed_signals_gui_adapter as gui_adapter
import signal_tape as tape

BUILD_ID = "2026-05-08_free_precomputed_gui_picker_wiring_v1"

DISPLAY_ONLY_NOTICE = "Replay/backtest only"
LIVE_PAPER_WARNING = "Not selectable for LIVE/PAPER"
JA_DISPLAY_ONLY_NOTICE = "バックテスト / リプレイ専用"
JA_LIVE_PAPER_WARNING = "LIVE/PAPER には使用不可"


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _safe_bool_text(value: Any) -> str:
    return "true" if bool(value) else "false"


def _safe_float_or_none(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_amount(value: Any) -> str:
    parsed = _safe_float_or_none(value)
    if parsed is None:
        return "--"
    return f"{parsed:,.4f}"


def _format_pct(value: Any) -> str:
    parsed = _safe_float_or_none(value)
    if parsed is None:
        return "--"
    return f"{parsed * 100.0:.4f}%"


def _is_ja(ui_language: str | None) -> bool:
    return str(ui_language or "").strip().lower().startswith("ja")


def default_precomputed_signal_picker_root(*, env: Mapping[str, str] | None = None) -> str:
    return str(tape.resolve_precomputed_signals_root(env=env))


def format_precomputed_signal_picker_empty_text(*, ui_language: str = "en") -> str:
    if _is_ja(ui_language):
        return "\n".join(
            (
                "事前計算シグナル",
                "Signal dir: --",
                JA_DISPLAY_ONLY_NOTICE,
                JA_LIVE_PAPER_WARNING,
                "未選択",
            )
        )
    return "\n".join(
        (
            "Precomputed Signal Tape",
            "Signal dir: --",
            DISPLAY_ONLY_NOTICE,
            LIVE_PAPER_WARNING,
            "No signal tape selected.",
        )
    )


def format_precomputed_signal_picker_display_text(
    picker_item: Mapping[str, Any],
    *,
    ui_language: str = "en",
) -> str:
    item = gui_adapter.validate_gui_picker_item(picker_item)
    if item.get("status") != "valid":
        title = "事前計算シグナル" if _is_ja(ui_language) else "Precomputed Signal Tape"
        warning = JA_LIVE_PAPER_WARNING if _is_ja(ui_language) else LIVE_PAPER_WARNING
        return "\n".join(
            (
                title,
                f"Signal dir: {_safe_text(item.get('signal_dir')) or '--'}",
                "Status: invalid",
                f"Warning: {_safe_text(item.get('picker_warning')) or 'Invalid precomputed signal selection.'}",
                warning,
            )
        )

    if _is_ja(ui_language):
        title = "事前計算シグナル"
        dd_abs_label = "最大DD（正値表示）"
        dd_pct_label = "最大DD pct（正値表示）"
        replay_only = JA_DISPLAY_ONLY_NOTICE
        live_paper_warning = JA_LIVE_PAPER_WARNING
    else:
        title = "Precomputed Signal Tape"
        dd_abs_label = _safe_text(item.get("max_dd_display_label")) or gui_adapter.GUI_DD_DISPLAY_LABEL
        dd_pct_label = gui_adapter.GUI_DD_DISPLAY_PCT_LABEL
        replay_only = DISPLAY_ONLY_NOTICE
        live_paper_warning = LIVE_PAPER_WARNING

    return "\n".join(
        (
            title,
            f"Signal dir: {_safe_text(item.get('signal_dir'))}",
            f"Symbol: {_safe_text(item.get('symbol'))}",
            f"Timeframe: {_safe_text(item.get('entry_tf'))} / {_safe_text(item.get('filter_tf'))}",
            f"Signal set: {_safe_text(item.get('signal_set_id'))}",
            f"Dataset: {_safe_text(item.get('dataset_id'))}",
            f"Created: {_safe_text(item.get('created_at_utc'))}",
            f"Trades: {int(item.get('trade_count') or 0)}",
            f"Net total: {_safe_text(item.get('net_total_text')) or _format_amount(item.get('net_total'))}",
            f"Final equity: {_safe_text(item.get('final_equity_text')) or _format_amount(item.get('final_equity'))}",
            f"{dd_abs_label}: {_format_amount(item.get('max_dd_display_abs'))}",
            f"{dd_pct_label}: {_format_pct(item.get('max_dd_display_pct'))}",
            f"DD display: {_safe_text(item.get('dd_display_text'))}",
            replay_only,
            live_paper_warning,
            f"Backtest fast path: {_safe_bool_text(item.get('selectable_for_backtest_fast_path'))}",
            f"Runner replay fast path: {_safe_bool_text(item.get('selectable_for_runner_replay_fast_path'))}",
            f"Not selectable for live: {_safe_bool_text(item.get('not_selectable_for_live'))}",
            f"Not selectable for paper: {_safe_bool_text(item.get('not_selectable_for_paper'))}",
            f"Warning: {_safe_text(item.get('picker_warning'))}",
        )
    )


def build_precomputed_signal_picker_state(
    signal_dir: str | Path | None,
    *,
    ui_language: str = "en",
) -> dict[str, Any]:
    item = gui_adapter.build_gui_picker_item_from_signal_dir(signal_dir)
    display_text = format_precomputed_signal_picker_display_text(item, ui_language=ui_language)
    return {
        "signal_dir": _safe_text(item.get("signal_dir")),
        "picker_item": item,
        "display_text": display_text,
        "status": _safe_text(item.get("status")),
        "warning": _safe_text(item.get("picker_warning")),
    }
