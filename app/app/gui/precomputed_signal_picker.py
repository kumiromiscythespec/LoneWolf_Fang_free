# BUILD_ID: 2026-05-09_free_precomputed_local_dry_run_gui_preview_wiring_v1
# BUILD_ID: 2026-05-08_free_precomputed_gui_diagnostics_polish_v1
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Mapping, Sequence

import precomputed_signals_gui_adapter as gui_adapter
import precomputed_signals_local_dry_run_gui_adapter as local_dry_run_gui_adapter
import precomputed_signals_local_dry_run_request as local_dry_run_request
import signal_tape as tape

BUILD_ID = "2026-05-09_free_precomputed_local_dry_run_gui_preview_wiring_v1"

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
COPY_DISABLED_STATUS = "Copy disabled"
COPY_BACKTEST_TOOLTIP = (
    "Copy backtest command. Preview only. This panel does not execute commands. "
    "Run this command manually in a terminal if needed. Not selectable for LIVE/PAPER."
)
COPY_REPLAY_TOOLTIP = (
    "Copy replay command. Preview only. This panel does not execute commands. "
    "Run this command manually in a terminal if needed. Not selectable for LIVE/PAPER."
)
PREVIEW_ACCESSIBILITY_LABEL = (
    "Precomputed signal command preview. Preview only. Execution disabled. "
    "This panel does not execute commands. Not selectable for LIVE/PAPER."
)
EXECUTION_DISABLED_TEXT = "Execution disabled"
OPERATOR_HINT_TEXT = "Run this command manually in a terminal if needed."
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
    "copy_backtest_tooltip",
    "copy_replay_tooltip",
    "preview_accessibility_label",
    "execution_disabled_text",
    "live_paper_warning_text",
    "operator_hint_text",
    "preview_only",
    "execution_enabled",
    "live_command_available",
    "paper_command_available",
)

COPYABLE_COMMAND_FIELDS = {
    "backtest": "backtest_command_text",
    "runner_replay": "runner_replay_command_text",
}

DIAGNOSTICS_TITLE = "Selection diagnostics"
DIAGNOSTICS_NO_RAW_ROWS_TEXT = "No raw trade rows are displayed"
DIAGNOSTICS_NO_EXECUTION_TEXT = "This panel does not execute commands"
DIAGNOSTICS_LIVE_PAPER_TEXT = "LIVE/PAPER: not selectable"
DIAGNOSTICS_WARNING_TEXT = f"{DIAGNOSTICS_NO_RAW_ROWS_TEXT}; {DIAGNOSTICS_NO_EXECUTION_TEXT}."
DIAGNOSTICS_INVALID_WARNING_TEXT = "Invalid precomputed signal selection."
LOCAL_DRY_RUN_PREVIEW_TITLE = "Local dry-run request preview"
LOCAL_DRY_RUN_PREVIEW_DISABLED_REASON = "No valid signal tape selected."
LOCAL_DRY_RUN_PREVIEW_REQUEST_NOT_EXECUTABLE = "request is not executable in this panel"
LOCAL_DRY_RUN_OUTPUT_ROOT_PLACEHOLDER = r"%LOCALAPPDATA%\LoneWolfFang\data\precomputed_signals_dry_runs\free"
LOCAL_DRY_RUN_PREVIEW_MODES = (
    "backtest_fast_path_local_only",
    "runner_replay_fast_path_local_only",
)
JA_DIAGNOSTICS_TITLE = "選択診断"

DIAGNOSTICS_FIELDS = (
    "signal_dir",
    "status",
    "status_reason",
    "safe_error_code",
    "product",
    "symbol",
    "symbol_normalized",
    "entry_tf",
    "filter_tf",
    "signal_set_id",
    "dataset_id",
    "created_at_utc",
    "since_ms",
    "until_ms",
    "trade_count",
    "net_total",
    "final_equity",
    "max_dd_display_abs",
    "max_dd_display_pct",
    "max_dd_display_label",
    "max_drawdown_legacy_note",
    "safety_research_only",
    "safety_paper_live_order_execution",
    "selectable_for_backtest_fast_path",
    "selectable_for_runner_replay_fast_path",
    "not_selectable_for_live",
    "not_selectable_for_paper",
    "tape_files_present",
    "manifest_sha256",
    "summary_sha256",
    "trades_csv_sha256_from_manifest",
    "diagnostics_warning",
    "picker_warning",
)

DIAGNOSTICS_DISPLAY_FIELDS = (
    "diagnostics_status_label",
    "diagnostics_status_kind",
    "diagnostics_status_text",
    "safe_error_code_label",
    "compact_signal_dir",
    "full_signal_dir",
    "compact_manifest_sha256",
    "full_manifest_sha256",
    "compact_summary_sha256",
    "full_summary_sha256",
    "compact_trades_csv_sha256",
    "full_trades_csv_sha256",
    "tape_files_present_label",
    "safety_flags_label",
    "fast_path_availability_label",
    "live_paper_not_selectable_label",
    "hashes_label",
    "dd_display_label",
    "diagnostics_warning_text",
    "diagnostics_details_text",
    "diagnostics_tooltip_text",
    "not_selectable_for_live",
    "not_selectable_for_paper",
)

CREDENTIAL_LIKE_PATH_SEGMENT_MARKERS = (
    "api_key",
    "apikey",
    "api-secret",
    "api_secret",
    "secret",
    "token",
    "authorization",
    "auth",
    "credential",
    "password",
    "passwd",
    "private_key",
    "private-key",
)

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

FORBIDDEN_DIAGNOSTICS_TEXT = (
    "entry_exec",
    "exit_exec",
    "trade_id",
    "trade id",
    "order_id",
    "order id",
    "raw_order",
    "raw order",
    "api_key",
    "apikey",
    "secret",
    "token",
    "authorization",
    "raw_billing",
    "raw billing",
    "raw market data",
    "raw ohlcv",
    "balance",
    '"qty"',
    "'qty'",
    " qty ",
    "qty=",
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


def _safe_int(value: Any) -> int:
    try:
        if value in (None, ""):
            return 0
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _safe_tape_files_present(value: Any) -> dict[str, bool]:
    payload = value if isinstance(value, Mapping) else {}
    return {
        "manifest_json": bool(payload.get("manifest_json")) if isinstance(payload, Mapping) else False,
        "summary_json": bool(payload.get("summary_json")) if isinstance(payload, Mapping) else False,
        "trades_csv": bool(payload.get("trades_csv")) if isinstance(payload, Mapping) else False,
    }


def _safe_error_code(value: Any, *, default: str = "invalid_gui_picker_item") -> str:
    text = _safe_command_field(value)
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.")
    if text and all(ch in allowed for ch in text):
        return text
    return default


def _contains_forbidden_diagnostics_text(value: Any) -> bool:
    text = _safe_command_field(value).lower()
    if not text:
        return False
    padded = f" {text} "
    for forbidden in FORBIDDEN_DIAGNOSTICS_TEXT:
        token = str(forbidden or "").lower()
        if not token:
            continue
        if token.startswith(" ") or token.endswith(" "):
            if token in padded:
                return True
        elif token in text:
            return True
    return False


def _safe_diagnostics_text(value: Any, *, default: str = "") -> str:
    text = _safe_command_field(value)
    if _contains_forbidden_diagnostics_text(text):
        return default
    return text


def compact_precomputed_hash(value: str, length: int = 10) -> str:
    text = _safe_command_field(value)
    if not text:
        return "missing"
    try:
        limit = int(length)
    except (TypeError, ValueError):
        limit = 10
    limit = max(1, limit)
    if len(text) <= limit:
        return text
    return text[:limit] + "..."


def _path_has_credential_like_segment(path: str) -> bool:
    normalized = _safe_command_field(path).replace("/", "\\")
    for segment in (part for part in normalized.split("\\") if part):
        lowered = segment.lower()
        collapsed = lowered.replace(" ", "_")
        if any(marker in lowered or marker in collapsed for marker in CREDENTIAL_LIKE_PATH_SEGMENT_MARKERS):
            return True
    return False


def _redact_credential_like_path(path: str) -> str:
    text = _safe_command_field(path)
    if not text:
        return ""
    separator = "\\" if "\\" in text or "/" not in text else "/"
    normalized = text.replace("/", "\\")
    parts: list[str] = []
    for segment in normalized.split("\\"):
        if not segment:
            continue
        lowered = segment.lower()
        collapsed = lowered.replace(" ", "_")
        if any(marker in lowered or marker in collapsed for marker in CREDENTIAL_LIKE_PATH_SEGMENT_MARKERS):
            parts.append("[redacted]")
        else:
            parts.append(segment)
    if not parts:
        return ""
    redacted = "\\".join(parts)
    if separator == "/":
        return redacted.replace("\\", "/")
    return redacted


def compact_precomputed_signal_dir(path: str) -> str:
    text = _redact_credential_like_path(path)
    if not text:
        return "missing"
    normalized = text.replace("/", "\\")
    segments = [segment for segment in normalized.split("\\") if segment]
    if not segments:
        return "missing"
    tail = segments[-4:]
    compact = "\\".join(tail)
    if len(segments) > len(tail):
        if "[redacted]" in segments and "[redacted]" not in tail:
            return "...\\[redacted]\\...\\" + compact
        return "...\\" + compact
    return compact


def _presence_text(value: bool) -> str:
    return "OK" if bool(value) else "missing"


def _availability_text(value: bool) -> str:
    return "OK" if bool(value) else "not selectable"


def _safe_full_hash(value: Any) -> str:
    return _safe_diagnostics_text(value) or "missing"


def _empty_precomputed_signal_diagnostics(
    *,
    signal_dir: str = "",
    status_reason: str = "No signal tape selected.",
    safe_error_code: str = "missing_signal_dir",
) -> dict[str, Any]:
    diagnostics = {
        "signal_dir": _safe_command_field(signal_dir),
        "status": "invalid",
        "status_reason": _safe_diagnostics_text(status_reason, default=DIAGNOSTICS_INVALID_WARNING_TEXT),
        "safe_error_code": _safe_error_code(safe_error_code),
        "product": "",
        "symbol": "",
        "symbol_normalized": "",
        "entry_tf": "",
        "filter_tf": "",
        "signal_set_id": "",
        "dataset_id": "",
        "created_at_utc": "",
        "since_ms": 0,
        "until_ms": 0,
        "trade_count": 0,
        "net_total": 0.0,
        "final_equity": 0.0,
        "max_dd_display_abs": 0.0,
        "max_dd_display_pct": 0.0,
        "max_dd_display_label": gui_adapter.GUI_DD_DISPLAY_LABEL,
        "max_drawdown_legacy_note": gui_adapter.GUI_DD_LEGACY_NOTE,
        "safety_research_only": False,
        "safety_paper_live_order_execution": False,
        "selectable_for_backtest_fast_path": False,
        "selectable_for_runner_replay_fast_path": False,
        "not_selectable_for_live": True,
        "not_selectable_for_paper": True,
        "tape_files_present": _safe_tape_files_present({}),
        "manifest_sha256": "",
        "summary_sha256": "",
        "trades_csv_sha256_from_manifest": "",
        "diagnostics_warning": DIAGNOSTICS_WARNING_TEXT,
        "picker_warning": DIAGNOSTICS_INVALID_WARNING_TEXT,
    }
    return validate_precomputed_signal_diagnostics(diagnostics)


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


def build_precomputed_signal_diagnostics(picker_item: Mapping[str, Any]) -> dict[str, Any]:
    source = picker_item if isinstance(picker_item, Mapping) else {}
    signal_dir = _safe_command_field(source.get("signal_dir")) if isinstance(source, Mapping) else ""
    try:
        item = gui_adapter.validate_gui_picker_item(picker_item)
    except Exception:
        reason = _safe_diagnostics_text(source.get("status_reason"), default=DIAGNOSTICS_INVALID_WARNING_TEXT)
        code = _safe_error_code(source.get("safe_error_code")) if isinstance(source, Mapping) else "invalid_gui_picker_item"
        return _empty_precomputed_signal_diagnostics(
            signal_dir=signal_dir,
            status_reason=reason or DIAGNOSTICS_INVALID_WARNING_TEXT,
            safe_error_code=code,
        )

    if item.get("status") != "valid":
        return _empty_precomputed_signal_diagnostics(
            signal_dir=_safe_command_field(item.get("signal_dir")),
            status_reason=_safe_diagnostics_text(item.get("status_reason"), default=DIAGNOSTICS_INVALID_WARNING_TEXT)
            or DIAGNOSTICS_INVALID_WARNING_TEXT,
            safe_error_code=_safe_error_code(item.get("safe_error_code")),
        )

    diagnostics = {
        "signal_dir": _safe_command_field(item.get("signal_dir")),
        "status": "valid",
        "status_reason": _safe_diagnostics_text(item.get("status_reason"), default="ok") or "ok",
        "safe_error_code": "",
        "product": _safe_diagnostics_text(item.get("product")),
        "symbol": _safe_diagnostics_text(item.get("symbol")),
        "symbol_normalized": _safe_diagnostics_text(item.get("symbol_normalized")),
        "entry_tf": _safe_diagnostics_text(item.get("entry_tf")),
        "filter_tf": _safe_diagnostics_text(item.get("filter_tf")),
        "signal_set_id": _safe_diagnostics_text(item.get("signal_set_id")),
        "dataset_id": _safe_diagnostics_text(item.get("dataset_id")),
        "created_at_utc": _safe_diagnostics_text(item.get("created_at_utc")),
        "since_ms": _safe_int(item.get("since_ms")),
        "until_ms": _safe_int(item.get("until_ms")),
        "trade_count": _safe_int(item.get("trade_count")),
        "net_total": float(_safe_float_or_none(item.get("net_total")) or 0.0),
        "final_equity": float(_safe_float_or_none(item.get("final_equity")) or 0.0),
        "max_dd_display_abs": float(_safe_float_or_none(item.get("max_dd_display_abs")) or 0.0),
        "max_dd_display_pct": float(_safe_float_or_none(item.get("max_dd_display_pct")) or 0.0),
        "max_dd_display_label": _safe_diagnostics_text(item.get("max_dd_display_label"))
        or gui_adapter.GUI_DD_DISPLAY_LABEL,
        "max_drawdown_legacy_note": _safe_diagnostics_text(item.get("max_drawdown_legacy_note"))
        or gui_adapter.GUI_DD_LEGACY_NOTE,
        "safety_research_only": item.get("safety_research_only") is True,
        "safety_paper_live_order_execution": item.get("safety_paper_live_order_execution") is True,
        "selectable_for_backtest_fast_path": item.get("selectable_for_backtest_fast_path") is True,
        "selectable_for_runner_replay_fast_path": item.get("selectable_for_runner_replay_fast_path") is True,
        "not_selectable_for_live": True,
        "not_selectable_for_paper": True,
        "tape_files_present": _safe_tape_files_present(item.get("tape_files_present")),
        "manifest_sha256": _safe_diagnostics_text(item.get("manifest_sha256")),
        "summary_sha256": _safe_diagnostics_text(item.get("summary_sha256")),
        "trades_csv_sha256_from_manifest": _safe_diagnostics_text(item.get("trades_csv_sha256_from_manifest")),
        "diagnostics_warning": DIAGNOSTICS_WARNING_TEXT,
        "picker_warning": _safe_diagnostics_text(item.get("picker_warning")),
    }
    return validate_precomputed_signal_diagnostics(diagnostics)


def validate_precomputed_signal_diagnostics(diagnostics: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(diagnostics, Mapping):
        raise ValueError("precomputed signal diagnostics must be a mapping")
    payload = dict(diagnostics)
    unknown = sorted(set(payload) - set(DIAGNOSTICS_FIELDS))
    if unknown:
        raise ValueError("diagnostics contains unsupported fields: " + ", ".join(unknown[:10]))
    missing = [field for field in DIAGNOSTICS_FIELDS if field not in payload]
    if missing:
        raise ValueError("diagnostics missing fields: " + ", ".join(missing))

    status = _safe_command_field(payload.get("status"))
    if status not in {"valid", "invalid"}:
        status = "invalid"
    normalized = {
        "signal_dir": _safe_diagnostics_text(payload.get("signal_dir")),
        "status": status,
        "status_reason": _safe_diagnostics_text(payload.get("status_reason"), default=DIAGNOSTICS_INVALID_WARNING_TEXT)
        or DIAGNOSTICS_INVALID_WARNING_TEXT,
        "safe_error_code": _safe_error_code(payload.get("safe_error_code"), default="" if status == "valid" else "invalid_gui_picker_item"),
        "product": _safe_diagnostics_text(payload.get("product")),
        "symbol": _safe_diagnostics_text(payload.get("symbol")),
        "symbol_normalized": _safe_diagnostics_text(payload.get("symbol_normalized")),
        "entry_tf": _safe_diagnostics_text(payload.get("entry_tf")),
        "filter_tf": _safe_diagnostics_text(payload.get("filter_tf")),
        "signal_set_id": _safe_diagnostics_text(payload.get("signal_set_id")),
        "dataset_id": _safe_diagnostics_text(payload.get("dataset_id")),
        "created_at_utc": _safe_diagnostics_text(payload.get("created_at_utc")),
        "since_ms": _safe_int(payload.get("since_ms")),
        "until_ms": _safe_int(payload.get("until_ms")),
        "trade_count": _safe_int(payload.get("trade_count")),
        "net_total": float(_safe_float_or_none(payload.get("net_total")) or 0.0),
        "final_equity": float(_safe_float_or_none(payload.get("final_equity")) or 0.0),
        "max_dd_display_abs": float(_safe_float_or_none(payload.get("max_dd_display_abs")) or 0.0),
        "max_dd_display_pct": float(_safe_float_or_none(payload.get("max_dd_display_pct")) or 0.0),
        "max_dd_display_label": _safe_diagnostics_text(payload.get("max_dd_display_label"))
        or gui_adapter.GUI_DD_DISPLAY_LABEL,
        "max_drawdown_legacy_note": _safe_diagnostics_text(payload.get("max_drawdown_legacy_note"))
        or gui_adapter.GUI_DD_LEGACY_NOTE,
        "safety_research_only": payload.get("safety_research_only") is True,
        "safety_paper_live_order_execution": payload.get("safety_paper_live_order_execution") is True,
        "selectable_for_backtest_fast_path": payload.get("selectable_for_backtest_fast_path") is True,
        "selectable_for_runner_replay_fast_path": payload.get("selectable_for_runner_replay_fast_path") is True,
        "not_selectable_for_live": True,
        "not_selectable_for_paper": True,
        "tape_files_present": _safe_tape_files_present(payload.get("tape_files_present")),
        "manifest_sha256": _safe_diagnostics_text(payload.get("manifest_sha256")),
        "summary_sha256": _safe_diagnostics_text(payload.get("summary_sha256")),
        "trades_csv_sha256_from_manifest": _safe_diagnostics_text(payload.get("trades_csv_sha256_from_manifest")),
        "diagnostics_warning": _safe_diagnostics_text(payload.get("diagnostics_warning"), default=DIAGNOSTICS_WARNING_TEXT)
        or DIAGNOSTICS_WARNING_TEXT,
        "picker_warning": _safe_diagnostics_text(payload.get("picker_warning")),
    }
    if status != "valid":
        normalized.update(
            {
                "product": "",
                "symbol": "",
                "symbol_normalized": "",
                "entry_tf": "",
                "filter_tf": "",
                "signal_set_id": "",
                "dataset_id": "",
                "created_at_utc": "",
                "since_ms": 0,
                "until_ms": 0,
                "trade_count": 0,
                "net_total": 0.0,
                "final_equity": 0.0,
                "max_dd_display_abs": 0.0,
                "max_dd_display_pct": 0.0,
                "safety_research_only": False,
                "safety_paper_live_order_execution": False,
                "selectable_for_backtest_fast_path": False,
                "selectable_for_runner_replay_fast_path": False,
                "tape_files_present": _safe_tape_files_present({}),
                "manifest_sha256": "",
                "summary_sha256": "",
                "trades_csv_sha256_from_manifest": "",
                "picker_warning": normalized["picker_warning"] or DIAGNOSTICS_INVALID_WARNING_TEXT,
            }
        )
    else:
        if normalized["safe_error_code"]:
            raise ValueError("valid diagnostics must not contain safe_error_code")
        if normalized["safety_research_only"] is not True:
            raise ValueError("valid diagnostics must be research-only")
        if normalized["safety_paper_live_order_execution"] is not False:
            raise ValueError("valid diagnostics must disable paper/live execution")
        if normalized["selectable_for_backtest_fast_path"] is not True:
            raise ValueError("valid diagnostics must be selectable for backtest fast path")
        if normalized["selectable_for_runner_replay_fast_path"] is not True:
            raise ValueError("valid diagnostics must be selectable for runner replay fast path")

    tape.validate_no_secret_payload(normalized)
    for value in normalized.values():
        if isinstance(value, str) and _contains_forbidden_diagnostics_text(value):
            raise ValueError("diagnostics contains forbidden raw/private text")
    return normalized


def build_precomputed_signal_diagnostics_display(diagnostics: Mapping[str, Any]) -> dict[str, Any]:
    payload = validate_precomputed_signal_diagnostics(diagnostics)
    status = _safe_text(payload.get("status")) or "invalid"
    status_kind = "valid" if status == "valid" else "invalid"
    status_text = _safe_diagnostics_text(payload.get("status_reason"), default=DIAGNOSTICS_INVALID_WARNING_TEXT)
    safe_error_code = _safe_error_code(
        payload.get("safe_error_code"),
        default="" if status_kind == "valid" else "invalid_gui_picker_item",
    )
    files_present = _safe_tape_files_present(payload.get("tape_files_present"))
    full_signal_dir = _redact_credential_like_path(_safe_text(payload.get("signal_dir")))
    compact_manifest = compact_precomputed_hash(_safe_text(payload.get("manifest_sha256")))
    compact_summary = compact_precomputed_hash(_safe_text(payload.get("summary_sha256")))
    compact_trades = compact_precomputed_hash(_safe_text(payload.get("trades_csv_sha256_from_manifest")))
    full_manifest = _safe_full_hash(payload.get("manifest_sha256"))
    full_summary = _safe_full_hash(payload.get("summary_sha256"))
    full_trades = _safe_full_hash(payload.get("trades_csv_sha256_from_manifest"))
    backtest_selectable = payload.get("selectable_for_backtest_fast_path") is True
    replay_selectable = payload.get("selectable_for_runner_replay_fast_path") is True
    no_live_paper_execution = payload.get("safety_paper_live_order_execution") is not True

    tape_files_present_label = (
        "Files: "
        f"manifest {_presence_text(files_present.get('manifest_json'))} / "
        f"summary {_presence_text(files_present.get('summary_json'))} / "
        f"trades.csv {_presence_text(files_present.get('trades_csv'))}"
    )
    safety_flags_label = (
        "Safety: "
        f"research-only {_presence_text(payload.get('safety_research_only') is True)} / "
        f"no live-paper execution {_presence_text(no_live_paper_execution)}"
    )
    fast_path_availability_label = (
        "Fast path: "
        f"Backtest {_availability_text(backtest_selectable)} / "
        f"Replay {_availability_text(replay_selectable)}"
    )
    hashes_label = (
        "Hashes: "
        f"manifest {compact_manifest} / "
        f"summary {compact_summary} / "
        f"trades.csv from manifest {compact_trades}"
    )
    dd_display_label = (
        "DD: "
        f"max_dd_display_abs {_format_amount(payload.get('max_dd_display_abs'))} / "
        f"max_dd_display_pct {_format_pct(payload.get('max_dd_display_pct'))}"
    )
    warning_parts = [
        _safe_diagnostics_text(payload.get("diagnostics_warning"), default=DIAGNOSTICS_WARNING_TEXT)
        or DIAGNOSTICS_WARNING_TEXT
    ]
    picker_warning = _safe_diagnostics_text(payload.get("picker_warning"))
    if picker_warning:
        warning_parts.append(picker_warning)
    diagnostics_warning_text = " ".join(part for part in warning_parts if part).strip()
    diagnostics_details_text = (
        f"Full signal_dir: {full_signal_dir or 'missing'}\n"
        f"Full hashes: manifest {full_manifest} / summary {full_summary} / "
        f"trades.csv from manifest {full_trades}"
    )
    diagnostics_tooltip_text = "\n".join(
        (
            f"Selection diagnostics: {status_kind}",
            f"Status detail: {status_text or ('ok' if status_kind == 'valid' else DIAGNOSTICS_INVALID_WARNING_TEXT)}",
            tape_files_present_label,
            safety_flags_label,
            fast_path_availability_label,
            hashes_label,
            diagnostics_details_text,
            DIAGNOSTICS_NO_RAW_ROWS_TEXT,
            DIAGNOSTICS_NO_EXECUTION_TEXT,
        )
    )

    display = {
        "diagnostics_status_label": f"Selection diagnostics: {status_kind}",
        "diagnostics_status_kind": status_kind,
        "diagnostics_status_text": status_text or ("ok" if status_kind == "valid" else DIAGNOSTICS_INVALID_WARNING_TEXT),
        "safe_error_code_label": f"Safe error code: {safe_error_code or 'missing'}" if status_kind != "valid" else "",
        "compact_signal_dir": compact_precomputed_signal_dir(full_signal_dir),
        "full_signal_dir": full_signal_dir,
        "compact_manifest_sha256": compact_manifest,
        "full_manifest_sha256": full_manifest,
        "compact_summary_sha256": compact_summary,
        "full_summary_sha256": full_summary,
        "compact_trades_csv_sha256": compact_trades,
        "full_trades_csv_sha256": full_trades,
        "tape_files_present_label": tape_files_present_label,
        "safety_flags_label": safety_flags_label,
        "fast_path_availability_label": fast_path_availability_label,
        "live_paper_not_selectable_label": DIAGNOSTICS_LIVE_PAPER_TEXT,
        "hashes_label": hashes_label,
        "dd_display_label": dd_display_label,
        "diagnostics_warning_text": diagnostics_warning_text,
        "diagnostics_details_text": diagnostics_details_text,
        "diagnostics_tooltip_text": diagnostics_tooltip_text,
        "not_selectable_for_live": True,
        "not_selectable_for_paper": True,
    }
    return validate_precomputed_signal_diagnostics_display(display)


def validate_precomputed_signal_diagnostics_display(display: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(display, Mapping):
        raise ValueError("precomputed signal diagnostics display must be a mapping")
    payload = dict(display)
    unknown = sorted(set(payload) - set(DIAGNOSTICS_DISPLAY_FIELDS))
    if unknown:
        raise ValueError("diagnostics display contains unsupported fields: " + ", ".join(unknown[:10]))
    missing = [field for field in DIAGNOSTICS_DISPLAY_FIELDS if field not in payload]
    if missing:
        raise ValueError("diagnostics display missing fields: " + ", ".join(missing))
    status_kind = _safe_command_field(payload.get("diagnostics_status_kind"))
    if status_kind not in {"valid", "invalid", "warning"}:
        status_kind = "invalid"
    if payload.get("not_selectable_for_live") is not True:
        raise ValueError("diagnostics display must keep not_selectable_for_live=true")
    if payload.get("not_selectable_for_paper") is not True:
        raise ValueError("diagnostics display must keep not_selectable_for_paper=true")

    normalized: dict[str, Any] = {
        "diagnostics_status_label": _safe_diagnostics_text(payload.get("diagnostics_status_label")),
        "diagnostics_status_kind": status_kind,
        "diagnostics_status_text": _safe_diagnostics_text(
            payload.get("diagnostics_status_text"),
            default=DIAGNOSTICS_INVALID_WARNING_TEXT,
        )
        or DIAGNOSTICS_INVALID_WARNING_TEXT,
        "safe_error_code_label": _safe_diagnostics_text(payload.get("safe_error_code_label")),
        "compact_signal_dir": _safe_diagnostics_text(payload.get("compact_signal_dir"), default="missing") or "missing",
        "full_signal_dir": _safe_diagnostics_text(payload.get("full_signal_dir")),
        "compact_manifest_sha256": compact_precomputed_hash(_safe_text(payload.get("compact_manifest_sha256"))),
        "full_manifest_sha256": _safe_full_hash(payload.get("full_manifest_sha256")),
        "compact_summary_sha256": compact_precomputed_hash(_safe_text(payload.get("compact_summary_sha256"))),
        "full_summary_sha256": _safe_full_hash(payload.get("full_summary_sha256")),
        "compact_trades_csv_sha256": compact_precomputed_hash(_safe_text(payload.get("compact_trades_csv_sha256"))),
        "full_trades_csv_sha256": _safe_full_hash(payload.get("full_trades_csv_sha256")),
        "tape_files_present_label": _safe_diagnostics_text(payload.get("tape_files_present_label")),
        "safety_flags_label": _safe_diagnostics_text(payload.get("safety_flags_label")),
        "fast_path_availability_label": _safe_diagnostics_text(payload.get("fast_path_availability_label")),
        "live_paper_not_selectable_label": _safe_diagnostics_text(payload.get("live_paper_not_selectable_label"))
        or DIAGNOSTICS_LIVE_PAPER_TEXT,
        "hashes_label": _safe_diagnostics_text(payload.get("hashes_label")),
        "dd_display_label": _safe_diagnostics_text(payload.get("dd_display_label")),
        "diagnostics_warning_text": _safe_diagnostics_text(
            payload.get("diagnostics_warning_text"),
            default=DIAGNOSTICS_WARNING_TEXT,
        )
        or DIAGNOSTICS_WARNING_TEXT,
        "diagnostics_details_text": _safe_diagnostics_text(payload.get("diagnostics_details_text")),
        "diagnostics_tooltip_text": _safe_diagnostics_text(payload.get("diagnostics_tooltip_text")),
        "not_selectable_for_live": True,
        "not_selectable_for_paper": True,
    }
    if not normalized["diagnostics_status_label"]:
        normalized["diagnostics_status_label"] = f"Selection diagnostics: {status_kind}"
    if status_kind == "valid" and normalized["safe_error_code_label"]:
        raise ValueError("valid diagnostics display must not show a safe error code")
    tape.validate_no_secret_payload(normalized)
    for value in normalized.values():
        if isinstance(value, str) and _contains_forbidden_diagnostics_text(value):
            raise ValueError("diagnostics display contains forbidden raw/private text")
    return normalized


def format_precomputed_signal_diagnostics_compact_text(display: Mapping[str, Any]) -> str:
    payload = validate_precomputed_signal_diagnostics_display(display)
    status_kind = _safe_text(payload.get("diagnostics_status_kind")) or "invalid"
    lines = [
        _safe_text(payload.get("diagnostics_status_label")) or f"Selection diagnostics: {status_kind}",
        f"Status: {status_kind}",
        f"Signal dir: {_safe_text(payload.get('compact_signal_dir')) or 'missing'}",
        _safe_text(payload.get("live_paper_not_selectable_label")) or DIAGNOSTICS_LIVE_PAPER_TEXT,
    ]
    if status_kind != "valid":
        lines.extend(
            (
                _safe_text(payload.get("safe_error_code_label")) or "Safe error code: invalid_gui_picker_item",
                f"Reason: {_safe_text(payload.get('diagnostics_status_text')) or DIAGNOSTICS_INVALID_WARNING_TEXT}",
                _safe_text(payload.get("fast_path_availability_label")),
                _safe_text(payload.get("diagnostics_warning_text")),
                DIAGNOSTICS_NO_RAW_ROWS_TEXT,
                DIAGNOSTICS_NO_EXECUTION_TEXT,
            )
        )
        return "\n".join(line for line in lines if line)

    lines.extend(
        (
            _safe_text(payload.get("tape_files_present_label")),
            _safe_text(payload.get("safety_flags_label")),
            _safe_text(payload.get("fast_path_availability_label")),
            "Backtest fast path: selectable",
            "Replay fast path: selectable",
            _safe_text(payload.get("hashes_label")),
            f"Manifest hash: {_safe_text(payload.get('compact_manifest_sha256')) or 'missing'}",
            f"Summary hash: {_safe_text(payload.get('compact_summary_sha256')) or 'missing'}",
            f"Trades CSV hash from manifest: {_safe_text(payload.get('compact_trades_csv_sha256')) or 'missing'}",
            _safe_text(payload.get("dd_display_label")),
            _safe_text(payload.get("diagnostics_warning_text")),
            DIAGNOSTICS_NO_RAW_ROWS_TEXT,
            DIAGNOSTICS_NO_EXECUTION_TEXT,
        )
    )
    return "\n".join(line for line in lines if line)


def format_precomputed_signal_diagnostics_text(
    diagnostics: Mapping[str, Any],
    *,
    ui_language: str = "en",
) -> str:
    del ui_language
    display = build_precomputed_signal_diagnostics_display(diagnostics)
    return format_precomputed_signal_diagnostics_compact_text(display)


def _copy_disabled_state(
    reason: str = COPY_DISABLED_INVALID_PREVIEW,
    warning: str = "",
) -> dict[str, Any]:
    safe_warning = _safe_command_field(warning) or "Copy disabled for this command preview."
    return {
        "backtest_command_text": "",
        "runner_replay_command_text": "",
        "can_copy_backtest_command": False,
        "can_copy_runner_replay_command": False,
        "copied_backtest_command": False,
        "copied_runner_replay_command": False,
        "copy_status_text": f"{COPY_DISABLED_STATUS} / {COPY_READY_STATUS} / {safe_warning}",
        "copy_warning": safe_warning,
        "copy_disabled_reason": _safe_command_field(reason) or COPY_DISABLED_INVALID_PREVIEW,
        "copy_backtest_tooltip": COPY_BACKTEST_TOOLTIP,
        "copy_replay_tooltip": COPY_REPLAY_TOOLTIP,
        "preview_accessibility_label": PREVIEW_ACCESSIBILITY_LABEL,
        "execution_disabled_text": EXECUTION_DISABLED_TEXT,
        "live_paper_warning_text": LIVE_PAPER_WARNING,
        "operator_hint_text": OPERATOR_HINT_TEXT,
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
        "copy_backtest_tooltip": COPY_BACKTEST_TOOLTIP,
        "copy_replay_tooltip": COPY_REPLAY_TOOLTIP,
        "preview_accessibility_label": PREVIEW_ACCESSIBILITY_LABEL,
        "execution_disabled_text": EXECUTION_DISABLED_TEXT,
        "live_paper_warning_text": LIVE_PAPER_WARNING,
        "operator_hint_text": OPERATOR_HINT_TEXT,
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
    state["operator_hint_text"] = OPERATOR_HINT_TEXT
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
            "copy_backtest_tooltip": _safe_command_field(payload.get("copy_backtest_tooltip")) or COPY_BACKTEST_TOOLTIP,
            "copy_replay_tooltip": _safe_command_field(payload.get("copy_replay_tooltip")) or COPY_REPLAY_TOOLTIP,
            "preview_accessibility_label": _safe_command_field(payload.get("preview_accessibility_label"))
            or PREVIEW_ACCESSIBILITY_LABEL,
            "execution_disabled_text": _safe_command_field(payload.get("execution_disabled_text"))
            or EXECUTION_DISABLED_TEXT,
            "live_paper_warning_text": _safe_command_field(payload.get("live_paper_warning_text"))
            or LIVE_PAPER_WARNING,
            "operator_hint_text": _safe_command_field(payload.get("operator_hint_text")) or OPERATOR_HINT_TEXT,
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


def _default_local_dry_run_output_root() -> str:
    base = _safe_command_field(os.environ.get("LOCALAPPDATA"))
    if not base:
        return LOCAL_DRY_RUN_OUTPUT_ROOT_PLACEHOLDER
    return str(Path(base) / "LoneWolfFang" / "data" / "precomputed_signals_dry_runs" / "free")


def _safe_preview_path_segment(value: Any, *, default: str = "unknown") -> str:
    text = _safe_command_field(value)
    if not text:
        return default
    chars: list[str] = []
    for char in text:
        if char.isalnum() or char in ("_", "-", "."):
            chars.append(char)
        elif char in ("/", "\\", " ", ":"):
            chars.append("_")
    segment = "".join(chars).strip("._-")
    return segment or default


def _local_dry_run_preview_output_dir(
    item: Mapping[str, Any],
    *,
    output_root: str | None = None,
    dry_run_mode: str,
) -> str:
    root = _safe_command_field(output_root) or _default_local_dry_run_output_root()
    symbol = _safe_preview_path_segment(item.get("symbol_normalized") or item.get("symbol"))
    tf_pair = "_".join(
        (
            _safe_preview_path_segment(item.get("entry_tf"), default="entry_tf"),
            _safe_preview_path_segment(item.get("filter_tf"), default="filter_tf"),
        )
    )
    signal_set_id = _safe_preview_path_segment(item.get("signal_set_id"), default="signal_set")
    mode = _safe_preview_path_segment(dry_run_mode, default="mode")
    return str(Path(root) / symbol / tf_pair / signal_set_id / mode)


def build_precomputed_signal_local_dry_run_preview_items(
    picker_item: Mapping[str, Any],
    *,
    output_root: str | None = None,
) -> list[dict[str, Any]]:
    source = picker_item if isinstance(picker_item, Mapping) else {}
    try:
        item = gui_adapter.validate_gui_picker_item(source)
    except Exception:
        return []
    if item.get("status") != "valid":
        return []
    if not _safe_command_field(item.get("signal_dir")):
        return []
    if item.get("not_selectable_for_live") is not True or item.get("not_selectable_for_paper") is not True:
        return []

    preview_items: list[dict[str, Any]] = []
    for mode in LOCAL_DRY_RUN_PREVIEW_MODES:
        if mode == "backtest_fast_path_local_only" and item.get("selectable_for_backtest_fast_path") is not True:
            continue
        if mode == "runner_replay_fast_path_local_only" and item.get("selectable_for_runner_replay_fast_path") is not True:
            continue
        output_dir = _local_dry_run_preview_output_dir(item, output_root=output_root, dry_run_mode=mode)
        try:
            request = local_dry_run_request.build_local_dry_run_request(
                item,
                dry_run_mode=mode,
                output_dir=output_dir,
                operator_confirmed=False,
            )
            preview_items.append(local_dry_run_gui_adapter.build_local_dry_run_gui_preview(request))
        except Exception:
            return []
    return validate_precomputed_signal_local_dry_run_preview_items(preview_items)


def validate_precomputed_signal_local_dry_run_preview_items(
    items: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    if not isinstance(items, Sequence) or isinstance(items, (str, bytes, bytearray)):
        return []
    normalized: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, Mapping):
            return []
        try:
            preview = local_dry_run_gui_adapter.validate_local_dry_run_gui_preview(item)
        except Exception:
            return []
        if preview.get("operator_confirmed") is not False:
            return []
        if preview.get("execution_enabled_after_confirmation") is not False:
            return []
        if preview.get("preview_only_before_confirmation") is not True:
            return []
        if preview.get("not_selectable_for_live") is not True or preview.get("not_selectable_for_paper") is not True:
            return []
        normalized.append(dict(preview))
    return normalized


def format_precomputed_signal_local_dry_run_preview_text(
    items: Sequence[Mapping[str, Any]],
) -> str:
    previews = validate_precomputed_signal_local_dry_run_preview_items(items)
    lines = [
        LOCAL_DRY_RUN_PREVIEW_TITLE,
        "Preview only",
        "Execution disabled",
        "Operator confirmation required",
        "Operator confirmed: false",
        "operator_confirmed=false",
        "execution_enabled_after_confirmation=false",
        "preview_only_before_confirmation=true",
        "Future execution requires separate approved phase",
        "Not LIVE/PAPER/order",
        "No private API / no balance fetch / no order fetch",
        LOCAL_DRY_RUN_PREVIEW_REQUEST_NOT_EXECUTABLE,
    ]
    if not previews:
        lines.extend(
            (
                f"Status: disabled - {LOCAL_DRY_RUN_PREVIEW_DISABLED_REASON}",
                "Backtest fast path local-only: disabled",
                "Runner replay fast path local-only: disabled",
            )
        )
        return "\n".join(lines)

    for index, preview in enumerate(previews):
        if index:
            lines.append("")
        lines.extend(
            (
                f"Mode: {preview['dry_run_mode_label']}",
                f"signal_dir: {preview['signal_dir']}",
                f"symbol: {preview['symbol']}",
                f"entry_tf: {preview['entry_tf']}",
                f"filter_tf: {preview['filter_tf']}",
                f"signal_set_id: {preview['signal_set_id']}",
                f"output_dir: {preview['output_dir']}",
                f"command_text_preview: {preview['command_text_preview']}",
                str(preview["allowed_artifacts_label"]),
                str(preview["forbidden_artifacts_label"]),
                str(preview["fail_closed_reasons_label"]),
                f"status: {preview['status']}",
                f"status_reason: {preview['status_reason']}",
                LOCAL_DRY_RUN_PREVIEW_REQUEST_NOT_EXECUTABLE,
            )
        )
    return "\n".join(line for line in lines if line)


def default_precomputed_signal_picker_root(*, env: Mapping[str, str] | None = None) -> str:
    return str(tape.resolve_precomputed_signals_root(env=env))


def format_precomputed_signal_picker_empty_text(*, ui_language: str = "en") -> str:
    command_preview_text = format_precomputed_signal_command_preview(
        _empty_command_preview(warning="No signal tape selected."),
        ui_language=ui_language,
    )
    diagnostics_text = format_precomputed_signal_diagnostics_text(
        _empty_precomputed_signal_diagnostics(),
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
                "",
                diagnostics_text,
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
            "",
            diagnostics_text,
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
    diagnostics_text = format_precomputed_signal_diagnostics_text(
        build_precomputed_signal_diagnostics(item),
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
                "",
                diagnostics_text,
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
            "",
            diagnostics_text,
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
    diagnostics = build_precomputed_signal_diagnostics(item)
    diagnostics_display = build_precomputed_signal_diagnostics_display(diagnostics)
    diagnostics_text = format_precomputed_signal_diagnostics_compact_text(diagnostics_display)
    copy_state = build_precomputed_signal_copy_state(command_preview)
    local_dry_run_preview_items = build_precomputed_signal_local_dry_run_preview_items(item)
    local_dry_run_preview_text = format_precomputed_signal_local_dry_run_preview_text(
        local_dry_run_preview_items
    )
    return {
        "signal_dir": _safe_text(item.get("signal_dir")),
        "picker_item": item,
        "diagnostics": diagnostics,
        "diagnostics_display": diagnostics_display,
        "diagnostics_text": diagnostics_text,
        "command_preview": command_preview,
        "command_preview_text": command_preview_text,
        "copy_state": copy_state,
        "local_dry_run_preview_items": local_dry_run_preview_items,
        "local_dry_run_preview_text": local_dry_run_preview_text,
        "display_text": display_text,
        "status": _safe_text(item.get("status")),
        "warning": _safe_text(item.get("picker_warning")),
    }
