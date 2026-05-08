# BUILD_ID: 2026-05-09_free_precomputed_local_dry_run_request_builder_v1
from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import precomputed_signals_selection as selection_contract

BUILD_ID = "2026-05-09_free_precomputed_local_dry_run_request_builder_v1"
SCHEMA_VERSION = 1
REQUEST_SCHEMA_VERSION = "free_precomputed_local_dry_run_request_v1"
REQUEST_TYPE = "precomputed_signal_local_dry_run_request"
PHASE = "free_precomputed_signals_phase19_request_builder"
PRODUCT = "free"

ALLOWED_DRY_RUN_MODES = frozenset({
    "backtest_fast_path_local_only",
    "runner_replay_fast_path_local_only",
})
ALLOWED_STATUS = frozenset({"draft", "valid_preview", "blocked", "invalid", "not_run"})
ALLOWED_ARTIFACTS = (
    "safe_summary_json",
    "fast_path_equity_curve_csv",
    "fast_path_trades_csv_safe_condition",
    "fast_summary_json",
    "safe_metadata_log",
    "sanitized_manual_smoke_record",
)
FORBIDDEN_ARTIFACTS = (
    "raw_market_data",
    "raw_ohlcv",
    "raw_trades_rows",
    "entry_exec",
    "exit_exec",
    "qty",
    "trade_id",
    "order_id",
    "raw_order",
    "balance_snapshot",
    "api_key",
    "secret",
    "token",
    "authorization",
    "raw_billing",
    "package_zip",
    "exe",
    "installer",
    "release_asset",
    "screenshots_with_secrets_or_balances_or_orders",
)
FAIL_CLOSED_REASON_OPTIONS = (
    "invalid_selection",
    "missing_signal_dir",
    "missing_manifest",
    "missing_summary",
    "missing_trades_csv",
    "unsafe_manifest",
    "unsafe_summary",
    "forbidden_field",
    "positive_legacy_max_drawdown",
    "live_or_paper_requested",
    "order_or_balance_requested",
    "private_api_requested",
    "missing_operator_confirmation",
    "background_execution_requested",
    "output_path_in_package_release_area",
    "generated_artifact_policy_violation",
    "unknown_safety_violation",
)

REQUIRED_TOP_LEVEL_FIELDS = (
    "schema_version",
    "request_schema_version",
    "request_type",
    "phase",
    "product",
    "signal_dir",
    "symbol",
    "symbol_normalized",
    "entry_tf",
    "filter_tf",
    "signal_set_id",
    "selection_status",
    "selection_status_reason",
    "selection_safe_error_code",
    "dry_run_mode",
    "output_dir",
    "command_text_preview",
    "operator_confirmation_required",
    "operator_confirmed",
    "preview_only_before_confirmation",
    "execution_enabled_after_confirmation",
    "not_selectable_for_live",
    "not_selectable_for_paper",
    "safety_research_only",
    "paper_live_order_execution",
    "preflight",
    "allowed_artifacts",
    "forbidden_artifacts",
    "status",
    "status_reason",
    "fail_closed_reasons",
    "notes_sanitized",
    "requested_at_utc",
)
REQUIRED_PREFLIGHT_FIELDS = (
    "selection_contract_valid",
    "manifest_present",
    "summary_present",
    "trades_csv_present",
    "manifest_hash_present",
    "summary_hash_present",
    "trades_csv_hash_from_manifest_present",
    "positive_legacy_max_drawdown_rejected",
    "forbidden_fields_rejected",
    "live_paper_order_rejected",
    "private_api_rejected",
    "background_execution_rejected",
    "package_release_artifacts_excluded",
)

_POLICY_LIST_FIELDS = {"allowed_artifacts", "forbidden_artifacts", "fail_closed_reasons"}
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
    "order_id",
    "raw_order",
    "raw_orders",
    "balance",
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
_PACKAGE_RELEASE_TOKENS = (
    "package",
    "packaging",
    "release",
    "installer",
    "setup",
    "exe",
    "staging",
    "build_signed",
)
_PACKAGE_RELEASE_SUFFIXES = (".zip", ".exe", ".msi", ".apk")
_RAW_DATA_TOKENS = ("raw_market_data", "raw-data", "rawdata", "raw_ohlcv", "ohlcv_raw")
_PATH_VALUE_FIELDS = {"signal_dir", "output_dir"}


class LocalDryRunRequestError(ValueError):
    pass


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _expand_path(raw: str | Path) -> Path:
    text = str(raw or "").strip()
    if not text:
        raise LocalDryRunRequestError("path is required")
    if any(ch in text for ch in "\r\n\t"):
        raise LocalDryRunRequestError("path contains control characters")
    return Path(os.path.expandvars(os.path.expanduser(text)))


def _normalized_key(key: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", key.lower()).strip("_")


def _looks_like_package_release_path(raw: str | Path) -> bool:
    text = str(raw or "").replace("\\", "/").lower()
    if not text.strip():
        return False
    path = Path(text)
    if text.endswith(_PACKAGE_RELEASE_SUFFIXES):
        return True
    parts = [str(part).lower() for part in path.parts]
    for token in _PACKAGE_RELEASE_TOKENS:
        if token in parts or f"/{token}/" in f"/{text}/":
            return True
        if any(part.startswith(f"{token}_") or part.startswith(f"{token}-") for part in parts):
            return True
    return False


def _looks_like_raw_data_path(raw: str | Path) -> bool:
    text = str(raw or "").replace("\\", "/").lower()
    return any(token in text for token in _RAW_DATA_TOKENS)


def _validate_output_dir(output_dir: str | Path) -> str:
    path = _expand_path(output_dir)
    raw = str(output_dir or "")
    if _looks_like_package_release_path(raw):
        raise LocalDryRunRequestError("output_path_in_package_release_area")
    if _looks_like_raw_data_path(raw):
        raise LocalDryRunRequestError("generated_artifact_policy_violation")
    return str(path)


def _validate_preview_file_path(path: str | Path) -> Path:
    out_path = _expand_path(path)
    raw = str(path or "")
    if out_path.suffix.lower() != ".json":
        raise LocalDryRunRequestError("request preview path must be a .json file")
    if _looks_like_package_release_path(raw) or _looks_like_raw_data_path(raw):
        raise LocalDryRunRequestError("request preview path is not safe")
    return out_path


def _quote_preview_arg(value: Any) -> str:
    text = str(value or "")
    if any(ch in text for ch in "\r\n\t"):
        raise LocalDryRunRequestError("command preview path contains control characters")
    return '"' + text.replace('"', '\\"') + '"'


def _build_command_text_preview(signal_dir: str, dry_run_mode: str) -> str:
    quoted_dir = _quote_preview_arg(signal_dir)
    if dry_run_mode == "backtest_fast_path_local_only":
        command = (
            "python backtest.py --use-precomputed-signals "
            f"--precomputed-signals-dir {quoted_dir} --precomputed-signals-write-report"
        )
    elif dry_run_mode == "runner_replay_fast_path_local_only":
        command = (
            "python runner.py --mode replay --use-precomputed-signals "
            f"--precomputed-signals-dir {quoted_dir} --precomputed-signals-write-report"
        )
    else:
        raise LocalDryRunRequestError("invalid dry_run_mode")
    _assert_command_preview_safe(command)
    return command


def _selection_error_to_fail_reason(code: str) -> str:
    mapping = {
        "missing_signal_dir": "missing_signal_dir",
        "missing_manifest": "missing_manifest",
        "missing_summary": "missing_summary",
        "missing_trades_csv": "missing_trades_csv",
        "unsafe_manifest": "unsafe_manifest",
        "unsafe_summary": "unsafe_summary",
        "forbidden_field": "forbidden_field",
        "positive_legacy_drawdown": "positive_legacy_max_drawdown",
    }
    return mapping.get(str(code or ""), "invalid_selection")


def _selection_file_presence(selection: Mapping[str, Any], key: str) -> bool:
    files = selection.get("tape_files_present")
    if isinstance(files, Mapping):
        return files.get(key) is True
    return False


def _request_status(selection: Mapping[str, Any], preflight: Mapping[str, bool]) -> str:
    if str(selection.get("status") or "") != "valid":
        return "blocked"
    if all(preflight.values()):
        return "valid_preview"
    return "blocked"


def _status_reason(status: str, fail_closed_reasons: list[str]) -> str:
    if status == "valid_preview":
        return "Safe local-only dry-run request preview was built; no execution was performed."
    if fail_closed_reasons:
        return "Request preview failed closed: " + ", ".join(fail_closed_reasons)
    return "Request preview failed closed."


def _assert_dry_run_mode_allowed(dry_run_mode: str) -> str:
    mode = str(dry_run_mode or "").strip()
    if mode not in ALLOWED_DRY_RUN_MODES:
        raise LocalDryRunRequestError("invalid dry_run_mode")
    return mode


def _assert_command_preview_safe(command_text: str) -> None:
    command_without_paths = re.sub(r'"[^"\r\n]*"', '""', command_text)
    for pattern in _COMMAND_FORBIDDEN_PATTERNS:
        if pattern.search(command_without_paths):
            raise LocalDryRunRequestError("unsafe command_text_preview")


def _assert_no_forbidden_payload_fields(payload: Any, path: str = "$") -> None:
    if isinstance(payload, Mapping):
        for key, value in payload.items():
            key_text = str(key)
            if _normalized_key(key_text) in _FORBIDDEN_PAYLOAD_KEYS:
                raise LocalDryRunRequestError(f"forbidden field: {key_text}")
            if key_text == "command_text_preview":
                _assert_command_preview_safe(str(value))
            if key_text in _POLICY_LIST_FIELDS:
                continue
            if key_text in _PATH_VALUE_FIELDS:
                continue
            _assert_no_forbidden_payload_fields(value, f"{path}.{key_text}")
        return
    if isinstance(payload, (list, tuple)):
        for index, value in enumerate(payload):
            _assert_no_forbidden_payload_fields(value, f"{path}[{index}]")
        return
    if isinstance(payload, str):
        for pattern in _REQUEST_TEXT_FORBIDDEN_PATTERNS:
            if pattern.search(payload):
                raise LocalDryRunRequestError(f"forbidden text at {path}")


def _request_preflight(selection: Mapping[str, Any], output_dir: str) -> dict[str, bool]:
    status = str(selection.get("status") or "")
    safe_code = str(selection.get("safe_error_code") or "")
    return {
        "selection_contract_valid": status == "valid",
        "manifest_present": _selection_file_presence(selection, "manifest_json"),
        "summary_present": _selection_file_presence(selection, "summary_json"),
        "trades_csv_present": _selection_file_presence(selection, "trades_csv"),
        "manifest_hash_present": bool(selection.get("manifest_sha256")),
        "summary_hash_present": bool(selection.get("summary_sha256")),
        "trades_csv_hash_from_manifest_present": bool(selection.get("trades_csv_sha256_from_manifest")),
        "positive_legacy_max_drawdown_rejected": safe_code != "positive_legacy_drawdown"
        and float(selection.get("max_drawdown") or 0.0) <= 0.0,
        "forbidden_fields_rejected": safe_code != "forbidden_field",
        "live_paper_order_rejected": True,
        "private_api_rejected": True,
        "background_execution_rejected": True,
        "package_release_artifacts_excluded": not _looks_like_package_release_path(output_dir)
        and not _looks_like_raw_data_path(output_dir),
    }


def _dedupe_reasons(reasons: list[str]) -> list[str]:
    deduped: list[str] = []
    allowed = set(FAIL_CLOSED_REASON_OPTIONS)
    for reason in reasons:
        safe_reason = reason if reason in allowed else "unknown_safety_violation"
        if safe_reason not in deduped:
            deduped.append(safe_reason)
    return deduped


def _fail_closed_reasons_for(selection: Mapping[str, Any], preflight: Mapping[str, bool]) -> list[str]:
    reasons: list[str] = ["missing_operator_confirmation"]
    if str(selection.get("status") or "") != "valid":
        reasons.extend(["invalid_selection", _selection_error_to_fail_reason(str(selection.get("safe_error_code") or ""))])
    for field, reason in (
        ("manifest_present", "missing_manifest"),
        ("summary_present", "missing_summary"),
        ("trades_csv_present", "missing_trades_csv"),
        ("positive_legacy_max_drawdown_rejected", "positive_legacy_max_drawdown"),
        ("forbidden_fields_rejected", "forbidden_field"),
        ("package_release_artifacts_excluded", "output_path_in_package_release_area"),
    ):
        if preflight.get(field) is not True:
            reasons.append(reason)
    return _dedupe_reasons(reasons)


def _expected_kwarg(expected: Mapping[str, Any], canonical: str) -> Any:
    alt_name = canonical.replace("expected_", "expect_")
    return expected.get(canonical, expected.get(alt_name))


def build_local_dry_run_request(
    selection: Mapping[str, Any],
    *,
    dry_run_mode: str,
    output_dir: str,
    operator_confirmed: bool = False,
) -> dict[str, Any]:
    mode = _assert_dry_run_mode_allowed(dry_run_mode)
    safe_output_dir = _validate_output_dir(output_dir)
    if operator_confirmed is not False:
        raise LocalDryRunRequestError("operator_confirmed must remain false in Phase 19")

    selected = selection_contract.validate_signal_tape_selection_contract(selection)
    signal_dir = str(selected.get("signal_dir") or "")
    if not signal_dir:
        raise LocalDryRunRequestError("missing_signal_dir")
    command_text = _build_command_text_preview(signal_dir, mode)
    preflight = _request_preflight(selected, safe_output_dir)
    status = _request_status(selected, preflight)
    fail_closed_reasons = _fail_closed_reasons_for(selected, preflight)

    request = {
        "schema_version": SCHEMA_VERSION,
        "request_schema_version": REQUEST_SCHEMA_VERSION,
        "request_type": REQUEST_TYPE,
        "phase": PHASE,
        "product": PRODUCT,
        "signal_dir": signal_dir,
        "symbol": str(selected.get("symbol") or ""),
        "symbol_normalized": str(selected.get("symbol_normalized") or ""),
        "entry_tf": str(selected.get("entry_tf") or ""),
        "filter_tf": str(selected.get("filter_tf") or ""),
        "signal_set_id": str(selected.get("signal_set_id") or ""),
        "selection_status": str(selected.get("status") or ""),
        "selection_status_reason": str(selected.get("status_reason") or ""),
        "selection_safe_error_code": str(selected.get("safe_error_code") or ""),
        "dry_run_mode": mode,
        "output_dir": safe_output_dir,
        "command_text_preview": command_text,
        "operator_confirmation_required": True,
        "operator_confirmed": False,
        "preview_only_before_confirmation": True,
        "execution_enabled_after_confirmation": False,
        "not_selectable_for_live": True,
        "not_selectable_for_paper": True,
        "safety_research_only": True,
        "paper_live_order_execution": False,
        "preflight": preflight,
        "allowed_artifacts": list(ALLOWED_ARTIFACTS),
        "forbidden_artifacts": list(FORBIDDEN_ARTIFACTS),
        "status": status,
        "status_reason": _status_reason(status, fail_closed_reasons),
        "fail_closed_reasons": fail_closed_reasons,
        "notes_sanitized": "Request builder output only; no dry-run execution was performed.",
        "requested_at_utc": _utc_now_iso(),
    }
    if status == "valid_preview":
        return validate_local_dry_run_request(request)
    _assert_no_forbidden_payload_fields(request)
    return request


def build_local_dry_run_request_from_signal_dir(
    signal_dir: str | Path,
    *,
    dry_run_mode: str,
    output_dir: str,
    **expected: Any,
) -> dict[str, Any]:
    selected = selection_contract.build_signal_tape_selection_contract(
        signal_dir,
        expected_symbol=_expected_kwarg(expected, "expected_symbol") or None,
        expected_entry_tf=_expected_kwarg(expected, "expected_entry_tf") or None,
        expected_filter_tf=_expected_kwarg(expected, "expected_filter_tf") or None,
        expected_signal_set_id=_expected_kwarg(expected, "expected_signal_set_id") or None,
    )
    return build_local_dry_run_request(selected, dry_run_mode=dry_run_mode, output_dir=output_dir)


def validate_local_dry_run_request(request: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(request)
    missing = [field for field in REQUIRED_TOP_LEVEL_FIELDS if field not in payload]
    if missing:
        raise LocalDryRunRequestError("missing required request fields: " + ", ".join(missing))
    preflight = payload.get("preflight")
    if not isinstance(preflight, Mapping):
        raise LocalDryRunRequestError("preflight must be a mapping")
    missing_preflight = [field for field in REQUIRED_PREFLIGHT_FIELDS if field not in preflight]
    if missing_preflight:
        raise LocalDryRunRequestError("missing preflight fields: " + ", ".join(missing_preflight))

    if payload.get("request_type") != REQUEST_TYPE:
        raise LocalDryRunRequestError("invalid request_type")
    if payload.get("request_schema_version") != REQUEST_SCHEMA_VERSION:
        raise LocalDryRunRequestError("invalid request_schema_version")
    if payload.get("phase") != PHASE:
        raise LocalDryRunRequestError("invalid phase")
    if payload.get("product") != PRODUCT:
        raise LocalDryRunRequestError("product must be free")
    if payload.get("dry_run_mode") not in ALLOWED_DRY_RUN_MODES:
        raise LocalDryRunRequestError("invalid dry_run_mode")
    if payload.get("status") not in ALLOWED_STATUS:
        raise LocalDryRunRequestError("invalid status")

    for field in (
        "operator_confirmation_required",
        "preview_only_before_confirmation",
        "not_selectable_for_live",
        "not_selectable_for_paper",
        "safety_research_only",
    ):
        if payload.get(field) is not True:
            raise LocalDryRunRequestError(f"{field} must be true")
    for field in (
        "operator_confirmed",
        "execution_enabled_after_confirmation",
        "paper_live_order_execution",
    ):
        if payload.get(field) is not False:
            raise LocalDryRunRequestError(f"{field} must be false")
    for field in REQUIRED_PREFLIGHT_FIELDS:
        if type(preflight[field]) is not bool:
            raise LocalDryRunRequestError(f"{field} must be bool")
    for field in (
        "selection_contract_valid",
        "manifest_present",
        "summary_present",
        "trades_csv_present",
        "manifest_hash_present",
        "summary_hash_present",
        "trades_csv_hash_from_manifest_present",
        "positive_legacy_max_drawdown_rejected",
        "forbidden_fields_rejected",
        "live_paper_order_rejected",
        "private_api_rejected",
        "background_execution_rejected",
        "package_release_artifacts_excluded",
    ):
        if preflight.get(field) is not True:
            raise LocalDryRunRequestError(f"{field} must be true")

    if _looks_like_package_release_path(str(payload.get("output_dir") or "")):
        raise LocalDryRunRequestError("output_path_in_package_release_area")
    if _looks_like_raw_data_path(str(payload.get("output_dir") or "")):
        raise LocalDryRunRequestError("generated_artifact_policy_violation")
    if not str(payload.get("signal_dir") or "").strip():
        raise LocalDryRunRequestError("missing_signal_dir")

    if set(payload.get("allowed_artifacts") or []) != set(ALLOWED_ARTIFACTS):
        raise LocalDryRunRequestError("invalid allowed_artifacts")
    if set(payload.get("forbidden_artifacts") or []) != set(FORBIDDEN_ARTIFACTS):
        raise LocalDryRunRequestError("invalid forbidden_artifacts")
    if not set(payload.get("fail_closed_reasons") or []).issubset(set(FAIL_CLOSED_REASON_OPTIONS)):
        raise LocalDryRunRequestError("invalid fail_closed_reasons")

    _assert_no_forbidden_payload_fields(payload)
    return payload


def write_local_dry_run_request_json(path: str | Path, request: Mapping[str, Any]) -> Path:
    payload = validate_local_dry_run_request(request)
    out_path = _validate_preview_file_path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return out_path


def format_local_dry_run_request_preview(request: Mapping[str, Any]) -> str:
    payload = dict(request)
    _assert_no_forbidden_payload_fields(payload)
    return (
        "precomputed local dry-run request "
        f"product={payload.get('product')} status={payload.get('status')} "
        f"dry_run_mode={payload.get('dry_run_mode')} signal_dir={payload.get('signal_dir')} "
        f"output_dir={payload.get('output_dir')} operator_confirmed=false "
        "execution_enabled_after_confirmation=false "
        f"command_text_preview={payload.get('command_text_preview')}"
    )


def _invalid_cli_request(reason: str, fail_reason: str, args: argparse.Namespace) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "request_schema_version": REQUEST_SCHEMA_VERSION,
        "request_type": REQUEST_TYPE,
        "phase": PHASE,
        "product": PRODUCT,
        "signal_dir": str(getattr(args, "signal_dir", "") or ""),
        "symbol": "",
        "symbol_normalized": "",
        "entry_tf": "",
        "filter_tf": "",
        "signal_set_id": "",
        "selection_status": "invalid",
        "selection_status_reason": reason,
        "selection_safe_error_code": fail_reason,
        "dry_run_mode": str(getattr(args, "dry_run_mode", "") or ""),
        "output_dir": str(getattr(args, "output_dir", "") or ""),
        "command_text_preview": "",
        "operator_confirmation_required": True,
        "operator_confirmed": False,
        "preview_only_before_confirmation": True,
        "execution_enabled_after_confirmation": False,
        "not_selectable_for_live": True,
        "not_selectable_for_paper": True,
        "safety_research_only": True,
        "paper_live_order_execution": False,
        "preflight": {field: False for field in REQUIRED_PREFLIGHT_FIELDS},
        "allowed_artifacts": list(ALLOWED_ARTIFACTS),
        "forbidden_artifacts": list(FORBIDDEN_ARTIFACTS),
        "status": "invalid",
        "status_reason": reason,
        "fail_closed_reasons": _dedupe_reasons([fail_reason]),
        "notes_sanitized": "CLI request builder failed closed before creating a valid preview.",
        "requested_at_utc": _utc_now_iso(),
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a safe Free local-only dry-run request preview.")
    parser.add_argument("--signal-dir", default="")
    parser.add_argument("--dry-run-mode", default="")
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--out", default="")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--expect-symbol", default="")
    parser.add_argument("--expect-entry-tf", default="")
    parser.add_argument("--expect-filter-tf", default="")
    parser.add_argument("--expect-signal-set-id", default="")
    parser.add_argument("--strict", action="store_true")
    return parser


def _print_request(request: Mapping[str, Any], output_format: str) -> None:
    if output_format == "json":
        print(json.dumps(dict(request), ensure_ascii=True, sort_keys=True))
    else:
        print(format_local_dry_run_request_preview(request))


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    if not str(args.signal_dir or "").strip():
        _print_request(_invalid_cli_request("missing --signal-dir", "missing_signal_dir", args), args.format)
        return 2
    if not str(args.dry_run_mode or "").strip():
        _print_request(_invalid_cli_request("missing --dry-run-mode", "unknown_safety_violation", args), args.format)
        return 2
    if not str(args.output_dir or "").strip():
        _print_request(_invalid_cli_request("missing --output-dir", "generated_artifact_policy_violation", args), args.format)
        return 2

    try:
        request = build_local_dry_run_request_from_signal_dir(
            args.signal_dir,
            dry_run_mode=args.dry_run_mode,
            output_dir=args.output_dir,
            expect_symbol=str(args.expect_symbol or "").strip() or None,
            expect_entry_tf=str(args.expect_entry_tf or "").strip() or None,
            expect_filter_tf=str(args.expect_filter_tf or "").strip() or None,
            expect_signal_set_id=str(args.expect_signal_set_id or "").strip() or None,
        )
        out_path = str(args.out or "").strip()
        if out_path:
            write_local_dry_run_request_json(out_path, request)
            print(format_local_dry_run_request_preview(request))
        else:
            _print_request(request, args.format)
    except LocalDryRunRequestError as exc:
        fail_reason = str(exc) if str(exc) in FAIL_CLOSED_REASON_OPTIONS else "unknown_safety_violation"
        _print_request(_invalid_cli_request(str(exc), fail_reason, args), args.format)
        return 2

    if request.get("status") != "valid_preview":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
