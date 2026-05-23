# BUILD_ID: 2026-05-09_free_precomputed_local_dry_run_gui_preview_adapter_v1
from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Mapping

import precomputed_signals_local_dry_run_request as request_builder

BUILD_ID = "2026-05-09_free_precomputed_local_dry_run_gui_preview_adapter_v1"
SCHEMA_VERSION = 1
GUI_PREVIEW_SCHEMA_VERSION = "free_precomputed_local_dry_run_gui_preview_v1"
PHASE = "free_precomputed_signals_phase20_gui_preview_adapter"

DRY_RUN_MODE_LABELS = {
    "backtest_fast_path_local_only": "Backtest fast path local-only",
    "runner_replay_fast_path_local_only": "Runner replay fast path local-only",
}

ALLOWED_ARTIFACTS_LABEL = "Allowed artifacts: safe summary, equity curve, fast summary"
FORBIDDEN_ARTIFACTS_LABEL = "Forbidden artifacts: raw market data, raw trades rows, orders, balances, secrets"
PREFLIGHT_SUMMARY_LABEL = "Preflight: selection metadata valid; execution remains disabled."

_EXPECTED_TEXT_FIELDS = {
    "operator_confirmation_label": "Operator confirmation required",
    "execution_status_label": "Execution disabled",
    "confirmation_warning": "This preview does not execute commands. Future execution requires separate approved phase.",
    "live_paper_warning_label": "Not LIVE/PAPER/order",
    "title": "Local dry-run request preview",
    "subtitle": "Preview only - execution disabled",
    "status_label": "Preview only",
    "warning_label": "Not LIVE/PAPER/order",
    "preview_only_label": "Preview only",
    "manual_confirmation_required_label": "Operator confirmation required",
    "no_execution_label": "This preview does not execute commands",
    "no_live_paper_order_label": "No private API / no balance fetch / no order fetch",
    "operator_hint_text": "Future execution requires separate approved phase",
}

_REQUIRED_PREVIEW_FIELDS = frozenset({
    "schema_version",
    "gui_preview_schema_version",
    "source_request_schema_version",
    "request_type",
    "product",
    "phase",
    "signal_dir",
    "symbol",
    "symbol_normalized",
    "entry_tf",
    "filter_tf",
    "signal_set_id",
    "dry_run_mode",
    "dry_run_mode_label",
    "output_dir",
    "command_text_preview",
    "command_preview_lines",
    "operator_confirmation_required",
    "operator_confirmed",
    "operator_confirmation_label",
    "preview_only_before_confirmation",
    "execution_enabled_after_confirmation",
    "execution_status_label",
    "confirmation_warning",
    "not_selectable_for_live",
    "not_selectable_for_paper",
    "live_paper_warning_label",
    "safety_research_only",
    "paper_live_order_execution",
    "allowed_artifacts",
    "forbidden_artifacts",
    "allowed_artifacts_label",
    "forbidden_artifacts_label",
    "preflight_summary_label",
    "fail_closed_reasons",
    "fail_closed_reasons_label",
    "status",
    "status_reason",
    "title",
    "subtitle",
    "status_label",
    "warning_label",
    "preview_only_label",
    "manual_confirmation_required_label",
    "no_execution_label",
    "no_live_paper_order_label",
    "operator_hint_text",
})

_COMMAND_FORBIDDEN_PATTERNS = (
    re.compile(r"\b--mode\s+live\b", re.IGNORECASE),
    re.compile(r"\b--mode\s+paper\b", re.IGNORECASE),
    re.compile(r"\blive\b", re.IGNORECASE),
    re.compile(r"\bpaper\b", re.IGNORECASE),
    re.compile(r"\border\b", re.IGNORECASE),
    re.compile(r"\bbalance\b", re.IGNORECASE),
    re.compile(r"\bprivate[_ -]?api\b", re.IGNORECASE),
    re.compile(r"\bapi[_ -]?key\b", re.IGNORECASE),
    re.compile(r"\bsecret\b", re.IGNORECASE),
    re.compile(r"\btoken\b", re.IGNORECASE),
    re.compile(r"\bauth(?:orization)?\b", re.IGNORECASE),
)
_REQUEST_TEXT_FORBIDDEN_PATTERNS = (
    re.compile(r"\bapi[_ -]?key\b", re.IGNORECASE),
    re.compile(r"\bapi[_ -]?secret\b", re.IGNORECASE),
    re.compile(r"\bsecret\s*[:=]", re.IGNORECASE),
    re.compile(r"\btoken\s*[:=]", re.IGNORECASE),
    re.compile(r"\bauthorization\s*[:=]", re.IGNORECASE),
    re.compile(r"\bbearer\s+[a-z0-9]", re.IGNORECASE),
    re.compile(r"\braw[_ -]?order\b", re.IGNORECASE),
    re.compile(r"\bbalance[_ -]?snapshot\b", re.IGNORECASE),
    re.compile(r"\braw[_ -]?billing\b", re.IGNORECASE),
    re.compile(r"\braw[_ -]?market[_ -]?data\b", re.IGNORECASE),
    re.compile(r"\braw[_ -]?ohlcv\b", re.IGNORECASE),
    re.compile(r"\bentry_exec\b", re.IGNORECASE),
    re.compile(r"\bexit_exec\b", re.IGNORECASE),
    re.compile(r"\bqty\b", re.IGNORECASE),
    re.compile(r"\btrade[_ -]?id\b", re.IGNORECASE),
    re.compile(r"\border[_ -]?id\b", re.IGNORECASE),
)
_FORBIDDEN_PAYLOAD_KEYS = {
    "raw_trade_row",
    "raw_trade_rows",
    "raw_trades_rows",
    "trades_rows",
    "trades_csv_rows",
    "entry_exec",
    "exit_exec",
    "qty",
    "quantity",
    "trade_id",
    "exact_trade_id",
    "order",
    "orders",
    "order_id",
    "raw_order",
    "raw_orders",
    "balance",
    "balances",
    "balance_snapshot",
    "account_balance",
    "api_key",
    "apikey",
    "secret",
    "api_secret",
    "token",
    "authorization",
    "raw_billing",
    "raw_market_data",
    "raw_ohlcv",
    "ohlcv",
    "screenshot",
    "screenshot_path",
    "screenshots",
    "account_details",
    "private_key",
}
_POLICY_LIST_FIELDS = {"allowed_artifacts", "forbidden_artifacts", "fail_closed_reasons"}
_PATH_VALUE_FIELDS = {"signal_dir", "output_dir"}
_UNSAFE_OUTPUT_TOKENS = (
    "package",
    "packaging",
    "release",
    "installer",
    "setup",
    "exe",
    "staging",
    "build_signed",
    "raw_market_data",
    "raw-data",
    "rawdata",
    "raw_ohlcv",
    "ohlcv_raw",
)
_UNSAFE_OUTPUT_SUFFIXES = (".zip", ".exe", ".msi", ".apk")


class LocalDryRunGuiPreviewError(ValueError):
    pass


def _normalized_key(key: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", key.lower()).strip("_")


def _assert_command_preview_safe(command_text: str) -> None:
    command_without_paths = re.sub(r'"[^"\r\n]*"', '""', str(command_text or ""))
    for pattern in _COMMAND_FORBIDDEN_PATTERNS:
        if pattern.search(command_without_paths):
            raise LocalDryRunGuiPreviewError("unsafe command_text_preview")


def _looks_like_unsafe_output_path(raw: str | Path) -> bool:
    text = str(raw or "").replace("\\", "/").lower()
    if not text.strip() or any(ch in text for ch in "\r\n\t"):
        return True
    if text.endswith(_UNSAFE_OUTPUT_SUFFIXES):
        return True
    parts = [part for part in text.split("/") if part]
    return any(token in text or token in parts for token in _UNSAFE_OUTPUT_TOKENS)


def _assert_no_forbidden_request_payload(payload: Any, path: str = "$") -> None:
    if isinstance(payload, Mapping):
        for key, value in payload.items():
            key_text = str(key)
            if _normalized_key(key_text) in _FORBIDDEN_PAYLOAD_KEYS:
                raise LocalDryRunGuiPreviewError(f"forbidden field: {key_text}")
            if key_text == "command_text_preview":
                _assert_command_preview_safe(str(value))
            if key_text in _POLICY_LIST_FIELDS or key_text in _PATH_VALUE_FIELDS:
                continue
            _assert_no_forbidden_request_payload(value, f"{path}.{key_text}")
        return
    if isinstance(payload, (list, tuple)):
        for index, value in enumerate(payload):
            _assert_no_forbidden_request_payload(value, f"{path}[{index}]")
        return
    if isinstance(payload, str):
        for pattern in _REQUEST_TEXT_FORBIDDEN_PATTERNS:
            if pattern.search(payload):
                raise LocalDryRunGuiPreviewError(f"forbidden text at {path}")


def _dry_run_mode_label(mode: str) -> str:
    try:
        return DRY_RUN_MODE_LABELS[mode]
    except KeyError as exc:
        raise LocalDryRunGuiPreviewError("invalid dry_run_mode") from exc


def _fail_closed_reasons_label(reasons: list[str]) -> str:
    if not reasons:
        return "Fail-closed reasons: none"
    return "Fail-closed reasons: " + ", ".join(reason.replace("_", " ") for reason in reasons)


def _expected_command_preview_lines(preview: Mapping[str, Any]) -> list[str]:
    return [
        str(preview.get("title") or ""),
        str(preview.get("subtitle") or ""),
        f"Mode: {preview.get('dry_run_mode_label')}",
        str(preview.get("preview_only_label") or ""),
        str(preview.get("execution_status_label") or ""),
        str(preview.get("operator_confirmation_label") or ""),
        str(preview.get("live_paper_warning_label") or ""),
        str(preview.get("no_live_paper_order_label") or ""),
        str(preview.get("no_execution_label") or ""),
        str(preview.get("operator_hint_text") or ""),
        "Command preview (display-only):",
        str(preview.get("command_text_preview") or ""),
    ]


def build_local_dry_run_gui_preview(request: Mapping[str, Any]) -> dict[str, Any]:
    _assert_no_forbidden_request_payload(request)
    source = request_builder.validate_local_dry_run_request(request)
    if source.get("status") != "valid_preview":
        raise LocalDryRunGuiPreviewError("request status must be valid_preview")

    mode = str(source.get("dry_run_mode") or "")
    output_dir = str(source.get("output_dir") or "")
    command_text = str(source.get("command_text_preview") or "")
    if _looks_like_unsafe_output_path(output_dir):
        raise LocalDryRunGuiPreviewError("unsafe output_dir")
    _assert_command_preview_safe(command_text)

    fail_closed_reasons = list(source.get("fail_closed_reasons") or [])
    preview: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "gui_preview_schema_version": GUI_PREVIEW_SCHEMA_VERSION,
        "source_request_schema_version": str(source.get("request_schema_version") or ""),
        "request_type": str(source.get("request_type") or ""),
        "product": str(source.get("product") or ""),
        "phase": PHASE,
        "signal_dir": str(source.get("signal_dir") or ""),
        "symbol": str(source.get("symbol") or ""),
        "symbol_normalized": str(source.get("symbol_normalized") or ""),
        "entry_tf": str(source.get("entry_tf") or ""),
        "filter_tf": str(source.get("filter_tf") or ""),
        "signal_set_id": str(source.get("signal_set_id") or ""),
        "dry_run_mode": mode,
        "dry_run_mode_label": _dry_run_mode_label(mode),
        "output_dir": output_dir,
        "command_text_preview": command_text,
        "operator_confirmation_required": True,
        "operator_confirmed": False,
        "operator_confirmation_label": _EXPECTED_TEXT_FIELDS["operator_confirmation_label"],
        "preview_only_before_confirmation": True,
        "execution_enabled_after_confirmation": False,
        "execution_status_label": _EXPECTED_TEXT_FIELDS["execution_status_label"],
        "confirmation_warning": _EXPECTED_TEXT_FIELDS["confirmation_warning"],
        "not_selectable_for_live": True,
        "not_selectable_for_paper": True,
        "live_paper_warning_label": _EXPECTED_TEXT_FIELDS["live_paper_warning_label"],
        "safety_research_only": True,
        "paper_live_order_execution": False,
        "allowed_artifacts": list(source.get("allowed_artifacts") or []),
        "forbidden_artifacts": list(source.get("forbidden_artifacts") or []),
        "allowed_artifacts_label": ALLOWED_ARTIFACTS_LABEL,
        "forbidden_artifacts_label": FORBIDDEN_ARTIFACTS_LABEL,
        "preflight_summary_label": PREFLIGHT_SUMMARY_LABEL,
        "fail_closed_reasons": fail_closed_reasons,
        "fail_closed_reasons_label": _fail_closed_reasons_label(fail_closed_reasons),
        "status": str(source.get("status") or ""),
        "status_reason": str(source.get("status_reason") or ""),
        "title": _EXPECTED_TEXT_FIELDS["title"],
        "subtitle": _EXPECTED_TEXT_FIELDS["subtitle"],
        "status_label": _EXPECTED_TEXT_FIELDS["status_label"],
        "warning_label": _EXPECTED_TEXT_FIELDS["warning_label"],
        "preview_only_label": _EXPECTED_TEXT_FIELDS["preview_only_label"],
        "manual_confirmation_required_label": _EXPECTED_TEXT_FIELDS["manual_confirmation_required_label"],
        "no_execution_label": _EXPECTED_TEXT_FIELDS["no_execution_label"],
        "no_live_paper_order_label": _EXPECTED_TEXT_FIELDS["no_live_paper_order_label"],
        "operator_hint_text": _EXPECTED_TEXT_FIELDS["operator_hint_text"],
    }
    preview["command_preview_lines"] = _expected_command_preview_lines(preview)
    return validate_local_dry_run_gui_preview(preview)


def build_local_dry_run_gui_preview_from_signal_dir(
    signal_dir: str | Path,
    *,
    dry_run_mode: str,
    output_dir: str,
    **expected: Any,
) -> dict[str, Any]:
    request = request_builder.build_local_dry_run_request_from_signal_dir(
        signal_dir,
        dry_run_mode=dry_run_mode,
        output_dir=output_dir,
        **expected,
    )
    return build_local_dry_run_gui_preview(request)


def validate_local_dry_run_gui_preview(preview: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(preview)
    keys = set(payload)
    missing = sorted(_REQUIRED_PREVIEW_FIELDS - keys)
    if missing:
        raise LocalDryRunGuiPreviewError("missing preview fields: " + ", ".join(missing))
    unexpected = sorted(keys - _REQUIRED_PREVIEW_FIELDS)
    if unexpected:
        raise LocalDryRunGuiPreviewError("unexpected preview fields: " + ", ".join(unexpected))

    if payload.get("schema_version") != SCHEMA_VERSION:
        raise LocalDryRunGuiPreviewError("invalid schema_version")
    if payload.get("gui_preview_schema_version") != GUI_PREVIEW_SCHEMA_VERSION:
        raise LocalDryRunGuiPreviewError("invalid gui_preview_schema_version")
    if payload.get("source_request_schema_version") != request_builder.REQUEST_SCHEMA_VERSION:
        raise LocalDryRunGuiPreviewError("invalid source_request_schema_version")
    if payload.get("request_type") != request_builder.REQUEST_TYPE:
        raise LocalDryRunGuiPreviewError("invalid request_type")
    if payload.get("product") != request_builder.PRODUCT:
        raise LocalDryRunGuiPreviewError("product must be free")
    if payload.get("phase") != PHASE:
        raise LocalDryRunGuiPreviewError("invalid phase")
    if payload.get("dry_run_mode") not in request_builder.ALLOWED_DRY_RUN_MODES:
        raise LocalDryRunGuiPreviewError("invalid dry_run_mode")
    if payload.get("dry_run_mode_label") != _dry_run_mode_label(str(payload.get("dry_run_mode") or "")):
        raise LocalDryRunGuiPreviewError("invalid dry_run_mode_label")
    if payload.get("status") != "valid_preview":
        raise LocalDryRunGuiPreviewError("status must be valid_preview")

    for field in (
        "operator_confirmation_required",
        "preview_only_before_confirmation",
        "not_selectable_for_live",
        "not_selectable_for_paper",
        "safety_research_only",
    ):
        if payload.get(field) is not True:
            raise LocalDryRunGuiPreviewError(f"{field} must be true")
    for field in (
        "operator_confirmed",
        "execution_enabled_after_confirmation",
        "paper_live_order_execution",
    ):
        if payload.get(field) is not False:
            raise LocalDryRunGuiPreviewError(f"{field} must be false")
    for field, expected in _EXPECTED_TEXT_FIELDS.items():
        if payload.get(field) != expected:
            raise LocalDryRunGuiPreviewError(f"invalid {field}")
    if payload.get("allowed_artifacts") != list(request_builder.ALLOWED_ARTIFACTS):
        raise LocalDryRunGuiPreviewError("invalid allowed_artifacts")
    if payload.get("forbidden_artifacts") != list(request_builder.FORBIDDEN_ARTIFACTS):
        raise LocalDryRunGuiPreviewError("invalid forbidden_artifacts")
    if payload.get("allowed_artifacts_label") != ALLOWED_ARTIFACTS_LABEL:
        raise LocalDryRunGuiPreviewError("invalid allowed_artifacts_label")
    if payload.get("forbidden_artifacts_label") != FORBIDDEN_ARTIFACTS_LABEL:
        raise LocalDryRunGuiPreviewError("invalid forbidden_artifacts_label")
    if payload.get("preflight_summary_label") != PREFLIGHT_SUMMARY_LABEL:
        raise LocalDryRunGuiPreviewError("invalid preflight_summary_label")

    reasons = payload.get("fail_closed_reasons")
    if not isinstance(reasons, list) or not all(isinstance(reason, str) for reason in reasons):
        raise LocalDryRunGuiPreviewError("fail_closed_reasons must be a string list")
    if not set(reasons).issubset(set(request_builder.FAIL_CLOSED_REASON_OPTIONS)):
        raise LocalDryRunGuiPreviewError("invalid fail_closed_reasons")
    if "missing_operator_confirmation" not in reasons:
        raise LocalDryRunGuiPreviewError("missing operator confirmation fail-closed reason")
    if payload.get("fail_closed_reasons_label") != _fail_closed_reasons_label(reasons):
        raise LocalDryRunGuiPreviewError("invalid fail_closed_reasons_label")

    if not str(payload.get("signal_dir") or "").strip():
        raise LocalDryRunGuiPreviewError("missing signal_dir")
    if _looks_like_unsafe_output_path(str(payload.get("output_dir") or "")):
        raise LocalDryRunGuiPreviewError("unsafe output_dir")
    _assert_command_preview_safe(str(payload.get("command_text_preview") or ""))
    if payload.get("command_preview_lines") != _expected_command_preview_lines(payload):
        raise LocalDryRunGuiPreviewError("invalid command_preview_lines")
    return payload


def command_preview_lines(preview: Mapping[str, Any]) -> list[str]:
    payload = validate_local_dry_run_gui_preview(preview)
    return list(payload["command_preview_lines"])


def format_local_dry_run_gui_preview_text(preview: Mapping[str, Any]) -> str:
    payload = validate_local_dry_run_gui_preview(preview)
    lines = command_preview_lines(payload)
    lines.extend([
        str(payload["allowed_artifacts_label"]),
        str(payload["forbidden_artifacts_label"]),
        str(payload["fail_closed_reasons_label"]),
        str(payload["preflight_summary_label"]),
        f"Status: {payload['status']} - {payload['status_reason']}",
        f"Selection: {payload['symbol']} {payload['entry_tf']}/{payload['filter_tf']} {payload['signal_set_id']}",
        f"Signal dir: {payload['signal_dir']}",
        f"Output dir: {payload['output_dir']}",
    ])
    return "\n".join(lines)
