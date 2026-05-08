# BUILD_ID: 2026-05-08_free_precomputed_gui_command_copy_ux_v1
from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import precomputed_signals_gui_adapter as gui_adapter
import signal_tape as tape

BUILD_ID = "2026-05-08_free_precomputed_gui_command_copy_ux_v1"

DISPLAY_ONLY_NOTICE = "Replay/backtest only"
LIVE_PAPER_WARNING = "Not selectable for LIVE/PAPER"
JA_DISPLAY_ONLY_NOTICE = "バックテスト / リプレイ専用"
JA_LIVE_PAPER_WARNING = "LIVE/PAPER には使用不可"
COMMAND_PREVIEW_NOTICE = "Backtest / replay preview only"
EXECUTION_DISABLED_NOTICE = "Execution is disabled in this panel"
BACKTEST_COMMAND_PREVIEW_LABEL = "Backtest command preview only"
RUNNER_REPLAY_COMMAND_PREVIEW_LABEL = "Replay command preview only"
COMMAND_PREVIEW_DISABLED_WARNING = "Command preview disabled for this selection."
COMMAND_PREVIEW_ENABLED_WARNING = "Command preview only; execution is disabled in this panel."
COPY_HELPER_WARNING = "Copy helper only handles preview command text; this panel does not execute commands."
COPY_READY_STATUS = "Preview only / Execution disabled / Not selectable for LIVE/PAPER"
COPIED_STATUS = "Copied"
COPY_DISABLED_INVALID_PREVIEW = "invalid_command_preview"
COPY_DISABLED_EMPTY_COMMAND = "empty_command_text"
COPY_DISABLED_UNSAFE_COMMAND = "unsafe_command_text"
JA_COMMAND_PREVIEW_NOTICE = "バックテスト / リプレイ コマンドプレビューのみ"
JA_EXECUTION_DISABLED_NOTICE = "このパネルからは実行しません"
JA_BACKTEST_COMMAND_PREVIEW_LABEL = "バックテスト用コマンドプレビュー"
JA_RUNNER_REPLAY_COMMAND_PREVIEW_LABEL = "リプレイ用コマンドプレビュー"

COMMAND_PREVIEW_FIELDS = (
    "signal_dir",
    "backtest_command_text",
    "runner_replay_command_text",
    "backtest_argv",
    "runner_replay_argv",
    "preview_only",
    "execution_enabled",
    "live_command_available",
    "paper_command_available",
    "warning",
    "source_symbol",
    "source_tf_pair",
)

COPY_STATE_FIELDS = (
    "backtest_command_text",
    "runner_replay_command_text",
    "can_copy_backtest_command",
    "can_copy_runner_replay_command",
    "copied_backtest_command",
    "copied_runner_replay_command",
    "copy_status_text",
    "copy_warning",
    "copy_disabled_reason",
    "preview_only",
    "execution_enabled",
    "live_command_available",
    "paper_command_available",
)

COPYABLE_COMMAND_FIELDS = {
    "backtest": "backtest_command_text",
    "runner_replay": "runner_replay_command_text",
}

FORBIDDEN_COPY_COMMAND_TEXT = (
    "entry_exec",
    "exit_exec",
    "trade_id",
    "trade id",
    "order_id",
    "order id",
    "raw_order",
    "raw order",
    "balance",
    "api_key",
    "apikey",
    "secret",
    "token",
    "authorization",
    "raw_billing",
    "raw billing",
    "raw market data",
    "raw ohlcv",
    "ohlcv",
    "private_key",
    "private key",
    "--qty",
    " qty ",
    "qty=",
    '"qty"',
    "'qty'",
)


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


def _safe_command_field(value: Any) -> str:
    return " ".join(_safe_text(value).splitlines()).strip()


def _quote_windows_command_arg(value: Any, *, always: bool = False) -> str:
    text = _safe_command_field(value)
    if text == "":
        return '""'
    needs_quote = always or any(ch.isspace() for ch in text) or any(ch in '"&|<>^' for ch in text)
    if not needs_quote:
        return text

    parts: list[str] = ['"']
    backslash_count = 0
    for char in text:
        if char == "\\":
            backslash_count += 1
            continue
        if char == '"':
            parts.append("\\" * (backslash_count * 2 + 1))
            parts.append('"')
            backslash_count = 0
            continue
        if backslash_count:
            parts.append("\\" * backslash_count)
            backslash_count = 0
        parts.append(char)
    if backslash_count:
        parts.append("\\" * (backslash_count * 2))
    parts.append('"')
    return "".join(parts)


def _format_windows_command(argv: list[str]) -> str:
    parts: list[str] = []
    quote_next = False
    for arg in argv:
        parts.append(_quote_windows_command_arg(arg, always=quote_next))
        quote_next = arg == "--precomputed-signals-dir"
    return " ".join(parts)


def _source_tf_pair(item: Mapping[str, Any]) -> str:
    entry_tf = _safe_command_field(item.get("entry_tf"))
    filter_tf = _safe_command_field(item.get("filter_tf"))
    if entry_tf and filter_tf:
        return f"{entry_tf}/{filter_tf}"
    return entry_tf or filter_tf


def _empty_command_preview(
    *,
    signal_dir: str = "",
    warning: str = COMMAND_PREVIEW_DISABLED_WARNING,
    source_symbol: str = "",
    source_tf_pair: str = "",
) -> dict[str, Any]:
    preview = {
        "signal_dir": _safe_command_field(signal_dir),
        "backtest_command_text": "",
        "runner_replay_command_text": "",
        "backtest_argv": [],
        "runner_replay_argv": [],
        "preview_only": True,
        "execution_enabled": False,
        "live_command_available": False,
        "paper_command_available": False,
        "warning": _safe_command_field(warning) or COMMAND_PREVIEW_DISABLED_WARNING,
        "source_symbol": _safe_command_field(source_symbol),
        "source_tf_pair": _safe_command_field(source_tf_pair),
    }
    return validate_precomputed_signal_command_preview(preview)


def build_precomputed_signal_command_preview(picker_item: Mapping[str, Any]) -> dict[str, Any]:
    source = picker_item if isinstance(picker_item, Mapping) else {}
    signal_dir = _safe_command_field(source.get("signal_dir"))
    try:
        item = gui_adapter.validate_gui_picker_item(picker_item)
    except Exception:
        return _empty_command_preview(signal_dir=signal_dir)

    source_symbol = _safe_command_field(item.get("symbol"))
    source_tf_pair = _source_tf_pair(item)
    if item.get("status") != "valid":
        return _empty_command_preview(
            signal_dir=signal_dir,
            warning="Command preview disabled for invalid precomputed signal selection.",
            source_symbol=source_symbol,
            source_tf_pair=source_tf_pair,
        )
    if item.get("not_selectable_for_live") is not True or item.get("not_selectable_for_paper") is not True:
        return _empty_command_preview(
            signal_dir=signal_dir,
            source_symbol=source_symbol,
            source_tf_pair=source_tf_pair,
        )
    if not signal_dir:
        return _empty_command_preview(
            warning="Command preview disabled because signal_dir is empty.",
            source_symbol=source_symbol,
            source_tf_pair=source_tf_pair,
        )

    backtest_argv: list[str] = []
    runner_replay_argv: list[str] = []
    if item.get("selectable_for_backtest_fast_path") is True:
        backtest_argv = [
            "python",
            "backtest.py",
            "--use-precomputed-signals",
            "--precomputed-signals-dir",
            signal_dir,
            "--precomputed-signals-write-report",
        ]
    if item.get("selectable_for_runner_replay_fast_path") is True:
        runner_replay_argv = [
            "python",
            "runner.py",
            "--mode",
            "replay",
            "--use-precomputed-signals",
            "--precomputed-signals-dir",
            signal_dir,
            "--precomputed-signals-write-report",
        ]

    preview = {
        "signal_dir": signal_dir,
        "backtest_command_text": _format_windows_command(backtest_argv) if backtest_argv else "",
        "runner_replay_command_text": _format_windows_command(runner_replay_argv) if runner_replay_argv else "",
        "backtest_argv": backtest_argv,
        "runner_replay_argv": runner_replay_argv,
        "preview_only": True,
        "execution_enabled": False,
        "live_command_available": False,
        "paper_command_available": False,
        "warning": COMMAND_PREVIEW_ENABLED_WARNING if backtest_argv or runner_replay_argv else COMMAND_PREVIEW_DISABLED_WARNING,
        "source_symbol": source_symbol,
        "source_tf_pair": source_tf_pair,
    }
    return validate_precomputed_signal_command_preview(preview)


def validate_precomputed_signal_command_preview(preview: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(preview, Mapping):
        raise ValueError("command preview must be a mapping")
    payload = dict(preview)
    unknown = sorted(set(payload) - set(COMMAND_PREVIEW_FIELDS))
    if unknown:
        raise ValueError("command preview contains unsupported fields: " + ", ".join(unknown[:10]))
    missing = [field for field in COMMAND_PREVIEW_FIELDS if field not in payload]
    if missing:
        raise ValueError("command preview missing fields: " + ", ".join(missing))
    if payload.get("preview_only") is not True:
        raise ValueError("command preview must be preview_only")
    if payload.get("execution_enabled") is not False:
        raise ValueError("command preview execution must be disabled")
    if payload.get("live_command_available") is not False:
        raise ValueError("live command preview must not be available")
    if payload.get("paper_command_available") is not False:
        raise ValueError("paper command preview must not be available")
    backtest_argv_raw = payload.get("backtest_argv")
    runner_replay_argv_raw = payload.get("runner_replay_argv")
    if not isinstance(backtest_argv_raw, (list, tuple)):
        raise ValueError("backtest argv must be a list")
    if not isinstance(runner_replay_argv_raw, (list, tuple)):
        raise ValueError("runner replay argv must be a list")

    normalized = {
        "signal_dir": _safe_command_field(payload.get("signal_dir")),
        "backtest_command_text": _safe_command_field(payload.get("backtest_command_text")),
        "runner_replay_command_text": _safe_command_field(payload.get("runner_replay_command_text")),
        "backtest_argv": [_safe_command_field(arg) for arg in backtest_argv_raw],
        "runner_replay_argv": [_safe_command_field(arg) for arg in runner_replay_argv_raw],
        "preview_only": True,
        "execution_enabled": False,
        "live_command_available": False,
        "paper_command_available": False,
        "warning": _safe_command_field(payload.get("warning")),
        "source_symbol": _safe_command_field(payload.get("source_symbol")),
        "source_tf_pair": _safe_command_field(payload.get("source_tf_pair")),
    }
    for command_field in ("backtest_command_text", "runner_replay_command_text"):
        command_lower = normalized[command_field].lower()
        if "--mode live" in command_lower or "--mode paper" in command_lower:
            raise ValueError("command preview must not contain live or paper mode")
    if normalized["backtest_command_text"] and "backtest.py" not in normalized["backtest_command_text"]:
        raise ValueError("backtest command preview must mention backtest.py")
    if normalized["backtest_command_text"] and "--use-precomputed-signals" not in normalized["backtest_command_text"]:
        raise ValueError("backtest command preview must use precomputed signals")
    if normalized["backtest_command_text"] and "--precomputed-signals-dir" not in normalized["backtest_command_text"]:
        raise ValueError("backtest command preview must include precomputed signals dir")
    if normalized["runner_replay_command_text"] and "runner.py" not in normalized["runner_replay_command_text"]:
        raise ValueError("runner replay command preview must mention runner.py")
    if normalized["runner_replay_command_text"] and "--mode replay" not in normalized["runner_replay_command_text"]:
        raise ValueError("runner replay command preview must include replay mode")
    if normalized["runner_replay_command_text"] and "--use-precomputed-signals" not in normalized["runner_replay_command_text"]:
        raise ValueError("runner replay command preview must use precomputed signals")
    if normalized["runner_replay_command_text"] and "--precomputed-signals-dir" not in normalized["runner_replay_command_text"]:
        raise ValueError("runner replay command preview must include precomputed signals dir")
    return normalized


def _copy_disabled_state(
    reason: str = COPY_DISABLED_INVALID_PREVIEW,
    warning: str = "",
) -> dict[str, Any]:
    return {
        "backtest_command_text": "",
        "runner_replay_command_text": "",
        "can_copy_backtest_command": False,
        "can_copy_runner_replay_command": False,
        "copied_backtest_command": False,
        "copied_runner_replay_command": False,
        "copy_status_text": "",
        "copy_warning": _safe_command_field(warning) or "Copy disabled for this command preview.",
        "copy_disabled_reason": _safe_command_field(reason) or COPY_DISABLED_INVALID_PREVIEW,
        "preview_only": True,
        "execution_enabled": False,
        "live_command_available": False,
        "paper_command_available": False,
    }


def _forbidden_copy_command_match(command_text: str) -> str:
    command_lower = f" {_safe_command_field(command_text).lower()} "
    for forbidden in FORBIDDEN_COPY_COMMAND_TEXT:
        token = str(forbidden or "").lower()
        if not token:
            continue
        if token.startswith(" ") or token.endswith(" "):
            if token in command_lower:
                return token.strip()
        elif token in command_lower:
            return token
    return ""


def _copyable_command_from_payload(payload: Mapping[str, Any], kind: str) -> tuple[str, str]:
    field_name = COPYABLE_COMMAND_FIELDS.get(kind)
    if not field_name:
        return "", COPY_DISABLED_INVALID_PREVIEW
    command_text = _safe_command_field(payload.get(field_name))
    if not command_text:
        return "", COPY_DISABLED_EMPTY_COMMAND
    if _forbidden_copy_command_match(command_text):
        return "", COPY_DISABLED_UNSAFE_COMMAND
    command_lower = command_text.lower()
    if "--mode live" in command_lower or "--mode paper" in command_lower:
        return "", COPY_DISABLED_INVALID_PREVIEW
    if kind == "backtest":
        if "backtest.py" not in command_text:
            return "", COPY_DISABLED_INVALID_PREVIEW
        if "--use-precomputed-signals" not in command_text or "--precomputed-signals-dir" not in command_text:
            return "", COPY_DISABLED_INVALID_PREVIEW
    if kind == "runner_replay":
        if "runner.py" not in command_text:
            return "", COPY_DISABLED_INVALID_PREVIEW
        if "--mode replay" not in command_text:
            return "", COPY_DISABLED_INVALID_PREVIEW
        if "--use-precomputed-signals" not in command_text or "--precomputed-signals-dir" not in command_text:
            return "", COPY_DISABLED_INVALID_PREVIEW
    return command_text, ""


def build_precomputed_signal_copy_state(command_preview: Mapping[str, Any]) -> dict[str, Any]:
    try:
        payload = validate_precomputed_signal_command_preview(command_preview)
    except Exception:
        return _copy_disabled_state(
            COPY_DISABLED_INVALID_PREVIEW,
            "Copy disabled for invalid command preview.",
        )

    backtest_command, backtest_reason = _copyable_command_from_payload(payload, "backtest")
    replay_command, replay_reason = _copyable_command_from_payload(payload, "runner_replay")
    can_copy_backtest = bool(backtest_command)
    can_copy_replay = bool(replay_command)
    if not can_copy_backtest and not can_copy_replay:
        reason = replay_reason or backtest_reason or COPY_DISABLED_EMPTY_COMMAND
        return _copy_disabled_state(reason, "Copy disabled because no safe preview command text is available.")

    state = {
        "backtest_command_text": backtest_command,
        "runner_replay_command_text": replay_command,
        "can_copy_backtest_command": can_copy_backtest,
        "can_copy_runner_replay_command": can_copy_replay,
        "copied_backtest_command": False,
        "copied_runner_replay_command": False,
        "copy_status_text": COPY_READY_STATUS,
        "copy_warning": COPY_HELPER_WARNING,
        "copy_disabled_reason": "",
        "preview_only": True,
        "execution_enabled": False,
        "live_command_available": False,
        "paper_command_available": False,
    }
    return validate_precomputed_signal_copy_state(state)


def get_copyable_precomputed_command(command_preview: Mapping[str, Any], kind: str) -> str:
    normalized_kind = _safe_command_field(kind)
    if normalized_kind not in COPYABLE_COMMAND_FIELDS:
        return ""
    copy_state = build_precomputed_signal_copy_state(command_preview)
    if normalized_kind == "backtest" and copy_state.get("can_copy_backtest_command") is True:
        return _safe_command_field(copy_state.get("backtest_command_text"))
    if normalized_kind == "runner_replay" and copy_state.get("can_copy_runner_replay_command") is True:
        return _safe_command_field(copy_state.get("runner_replay_command_text"))
    return ""


def mark_precomputed_command_copied(copy_state: Mapping[str, Any], kind: str) -> dict[str, Any]:
    normalized_kind = _safe_command_field(kind)
    if normalized_kind not in COPYABLE_COMMAND_FIELDS:
        return _copy_disabled_state(
            COPY_DISABLED_INVALID_PREVIEW,
            "Copy disabled for unsupported command kind.",
        )

    state = validate_precomputed_signal_copy_state(copy_state)
    if normalized_kind == "backtest":
        if state.get("can_copy_backtest_command") is not True:
            return state
        state["copied_backtest_command"] = True
        state["copied_runner_replay_command"] = False
    else:
        if state.get("can_copy_runner_replay_command") is not True:
            return state
        state["copied_backtest_command"] = False
        state["copied_runner_replay_command"] = True
    state["copy_status_text"] = COPIED_STATUS
    state["copy_warning"] = COPY_HELPER_WARNING
    state["copy_disabled_reason"] = ""
    return validate_precomputed_signal_copy_state(state)


def validate_precomputed_signal_copy_state(copy_state: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(copy_state, Mapping):
        return _copy_disabled_state(COPY_DISABLED_INVALID_PREVIEW, "Copy disabled for invalid copy state.")
    payload = dict(copy_state)
    if payload.get("preview_only") is not True:
        return _copy_disabled_state(COPY_DISABLED_INVALID_PREVIEW, "Copy disabled because preview_only is not true.")
    if payload.get("execution_enabled") is not False:
        return _copy_disabled_state(COPY_DISABLED_INVALID_PREVIEW, "Copy disabled because execution is enabled.")
    if payload.get("live_command_available") is not False:
        return _copy_disabled_state(COPY_DISABLED_INVALID_PREVIEW, "Copy disabled because a LIVE command is available.")
    if payload.get("paper_command_available") is not False:
        return _copy_disabled_state(COPY_DISABLED_INVALID_PREVIEW, "Copy disabled because a PAPER command is available.")

    backtest_command, backtest_reason = _copyable_command_from_payload(payload, "backtest")
    replay_command, replay_reason = _copyable_command_from_payload(payload, "runner_replay")
    can_copy_backtest = bool(payload.get("can_copy_backtest_command") is True and backtest_command)
    can_copy_replay = bool(payload.get("can_copy_runner_replay_command") is True and replay_command)
    if not can_copy_backtest:
        backtest_command = ""
    if not can_copy_replay:
        replay_command = ""

    copied_backtest = bool(payload.get("copied_backtest_command") is True and can_copy_backtest)
    copied_replay = bool(payload.get("copied_runner_replay_command") is True and can_copy_replay)
    if copied_backtest and copied_replay:
        copied_replay = False

    if can_copy_backtest or can_copy_replay:
        status_text = COPIED_STATUS if copied_backtest or copied_replay else COPY_READY_STATUS
        return {
            "backtest_command_text": backtest_command,
            "runner_replay_command_text": replay_command,
            "can_copy_backtest_command": can_copy_backtest,
            "can_copy_runner_replay_command": can_copy_replay,
            "copied_backtest_command": copied_backtest,
            "copied_runner_replay_command": copied_replay,
            "copy_status_text": _safe_command_field(payload.get("copy_status_text")) or status_text,
            "copy_warning": _safe_command_field(payload.get("copy_warning")) or COPY_HELPER_WARNING,
            "copy_disabled_reason": "",
            "preview_only": True,
            "execution_enabled": False,
            "live_command_available": False,
            "paper_command_available": False,
        }

    reason = _safe_command_field(payload.get("copy_disabled_reason")) or replay_reason or backtest_reason
    return _copy_disabled_state(
        reason or COPY_DISABLED_EMPTY_COMMAND,
        _safe_command_field(payload.get("copy_warning")) or "Copy disabled because no safe preview command text is available.",
    )


def format_precomputed_signal_command_preview(
    preview: Mapping[str, Any],
    *,
    ui_language: str = "en",
) -> str:
    payload = validate_precomputed_signal_command_preview(preview)
    if _is_ja(ui_language):
        notice = JA_COMMAND_PREVIEW_NOTICE
        execution_disabled = JA_EXECUTION_DISABLED_NOTICE
        live_paper_warning = JA_LIVE_PAPER_WARNING
        backtest_label = JA_BACKTEST_COMMAND_PREVIEW_LABEL
        replay_label = JA_RUNNER_REPLAY_COMMAND_PREVIEW_LABEL
    else:
        notice = COMMAND_PREVIEW_NOTICE
        execution_disabled = EXECUTION_DISABLED_NOTICE
        live_paper_warning = LIVE_PAPER_WARNING
        backtest_label = BACKTEST_COMMAND_PREVIEW_LABEL
        replay_label = RUNNER_REPLAY_COMMAND_PREVIEW_LABEL

    lines = [notice, execution_disabled, live_paper_warning]
    if payload["backtest_command_text"]:
        lines.extend((f"{backtest_label}:", payload["backtest_command_text"]))
    if payload["runner_replay_command_text"]:
        lines.extend((f"{replay_label}:", payload["runner_replay_command_text"]))
    if not payload["backtest_command_text"] and not payload["runner_replay_command_text"]:
        lines.append(f"Warning: {payload['warning'] or COMMAND_PREVIEW_DISABLED_WARNING}")
    return "\n".join(lines)


def default_precomputed_signal_picker_root(*, env: Mapping[str, str] | None = None) -> str:
    return str(tape.resolve_precomputed_signals_root(env=env))


def format_precomputed_signal_picker_empty_text(*, ui_language: str = "en") -> str:
    command_preview_text = format_precomputed_signal_command_preview(
        _empty_command_preview(warning="No signal tape selected."),
        ui_language=ui_language,
    )
    if _is_ja(ui_language):
        return "\n".join(
            (
                "事前計算シグナル",
                "Signal dir: --",
                JA_DISPLAY_ONLY_NOTICE,
                JA_LIVE_PAPER_WARNING,
                command_preview_text,
                "未選択",
            )
        )
    return "\n".join(
        (
            "Precomputed Signal Tape",
            "Signal dir: --",
            DISPLAY_ONLY_NOTICE,
            LIVE_PAPER_WARNING,
            command_preview_text,
            "No signal tape selected.",
        )
    )


def format_precomputed_signal_picker_display_text(
    picker_item: Mapping[str, Any],
    *,
    ui_language: str = "en",
) -> str:
    item = gui_adapter.validate_gui_picker_item(picker_item)
    command_preview_text = format_precomputed_signal_command_preview(
        build_precomputed_signal_command_preview(item),
        ui_language=ui_language,
    )
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
                command_preview_text,
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
            "",
            command_preview_text,
        )
    )


def build_precomputed_signal_picker_state(
    signal_dir: str | Path | None,
    *,
    ui_language: str = "en",
) -> dict[str, Any]:
    item = gui_adapter.build_gui_picker_item_from_signal_dir(signal_dir)
    display_text = format_precomputed_signal_picker_display_text(item, ui_language=ui_language)
    command_preview = build_precomputed_signal_command_preview(item)
    command_preview_text = format_precomputed_signal_command_preview(command_preview, ui_language=ui_language)
    copy_state = build_precomputed_signal_copy_state(command_preview)
    return {
        "signal_dir": _safe_text(item.get("signal_dir")),
        "picker_item": item,
        "command_preview": command_preview,
        "command_preview_text": command_preview_text,
        "copy_state": copy_state,
        "display_text": display_text,
        "status": _safe_text(item.get("status")),
        "warning": _safe_text(item.get("picker_warning")),
    }
