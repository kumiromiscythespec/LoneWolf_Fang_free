# BUILD_ID: 2026-05-08_free_precomputed_signal_selection_contract_v1
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Mapping

import signal_tape as tape

BUILD_ID = "2026-05-08_free_precomputed_signal_selection_contract_v1"
SELECTION_SCHEMA_VERSION = "lwf.precomputed.signal_tape.selection_contract.v1"
PRODUCT = tape.DEFAULT_PRODUCT

SAFE_ERROR_CODES = {
    "missing_signal_dir",
    "missing_manifest",
    "missing_summary",
    "missing_trades_csv",
    "unsafe_manifest",
    "unsafe_summary",
    "product_mismatch",
    "symbol_mismatch",
    "timeframe_mismatch",
    "signal_set_id_mismatch",
    "forbidden_field",
    "positive_legacy_drawdown",
    "invalid_unknown",
}

_SELECTION_FORBIDDEN_KEYS = {
    "entry_exec",
    "exit_exec",
    "entry_raw",
    "exit_raw",
    "qty",
    "quantity",
    "trade_id",
    "raw_trades",
    "raw_ohlcv",
    "ohlcv",
    "market_data",
}


class SelectionError(ValueError):
    pass


def _safe_int(value: Any) -> int:
    try:
        if value in (None, ""):
            return 0
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _safe_float(value: Any) -> float:
    try:
        if value in (None, ""):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _selection_path(raw: str | Path | None) -> Path | None:
    text = str(raw or "").strip()
    if not text:
        return None
    return Path(os.path.expandvars(os.path.expanduser(text)))


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SelectionError("invalid json") from exc
    except OSError as exc:
        raise SelectionError("read error") from exc


def _manifest_trades_sha(manifest: Mapping[str, Any]) -> str:
    files = dict(manifest.get("files") or {})
    trades = files.get("trades_csv") or {}
    if isinstance(trades, Mapping):
        return str(trades.get("sha256") or "")
    return ""


def _file_presence(signal_dir: Path | None) -> dict[str, bool]:
    if signal_dir is None:
        return {"manifest_json": False, "summary_json": False, "trades_csv": False}
    return {
        "manifest_json": (signal_dir / "manifest.json").is_file(),
        "summary_json": (signal_dir / "summary.json").is_file(),
        "trades_csv": (signal_dir / "trades.csv").is_file(),
    }


def _invalid_selection(
    signal_dir: Path | None,
    *,
    product: str,
    code: str,
    reason: str,
) -> dict[str, Any]:
    safe_code = code if code in SAFE_ERROR_CODES else "invalid_unknown"
    return {
        "schema_version": SELECTION_SCHEMA_VERSION,
        "selection_schema_version": SELECTION_SCHEMA_VERSION,
        "product": str(product or ""),
        "signal_dir": str(signal_dir or ""),
        "tape_files_present": _file_presence(signal_dir),
        "status": "invalid",
        "status_reason": reason,
        "safe_error_code": safe_code,
        "selectable_for_backtest_fast_path": False,
        "selectable_for_runner_replay_fast_path": False,
        "not_selectable_for_live": True,
        "not_selectable_for_paper": True,
        "picker_warning": "Invalid precomputed signal selection.",
    }


def _error_code_and_reason(stage: str, exc: BaseException) -> tuple[str, str]:
    message = str(exc).lower()
    if "product mismatch" in message:
        return "product_mismatch", f"{stage} product is not supported"
    if "symbol mismatch" in message:
        return "symbol_mismatch", f"{stage} symbol does not match expected selection"
    if "entry_tf mismatch" in message or "filter_tf mismatch" in message:
        return "timeframe_mismatch", f"{stage} timeframe does not match expected selection"
    if "signal_set_id mismatch" in message:
        return "signal_set_id_mismatch", f"{stage} signal_set_id does not match expected selection"
    if "unsafe signal tape payload" in message or "forbidden field" in message:
        return "forbidden_field", f"{stage} contains forbidden private or row-level metadata"
    if "positive max_drawdown" in message or "signed negative" in message:
        return "positive_legacy_drawdown", f"{stage} contains invalid positive legacy drawdown"
    if "safety_scope" in message or "research_only" in message or "paper_live_order_execution" in message:
        code = "unsafe_manifest" if stage == "manifest.json" else "unsafe_summary"
        return code, f"{stage} is outside the research-only no-paper/live safety scope"
    if stage == "manifest.json":
        return "unsafe_manifest", "manifest.json failed safe validation"
    if stage == "summary.json":
        return "unsafe_summary", "summary.json failed safe validation"
    return "invalid_unknown", "selection failed safe validation"


def _validate_selection_safe_metadata(payload: Any) -> None:
    tape.validate_no_secret_payload(payload)
    violations: list[str] = []

    def visit(value: Any, path: tuple[str, ...]) -> None:
        if isinstance(value, Mapping):
            for key, nested in value.items():
                key_text = str(key)
                if key_text.lower() in _SELECTION_FORBIDDEN_KEYS:
                    violations.append(".".join(path + (key_text,)))
                if key_text == "max_drawdown" and _safe_float(nested) > 0.0:
                    raise SelectionError("positive max_drawdown")
                visit(nested, path + (key_text,))
            return
        if isinstance(value, (list, tuple)):
            for index, nested in enumerate(value):
                visit(nested, path + (str(index),))

    visit(payload, ())
    if violations:
        raise SelectionError("forbidden field: " + ", ".join(violations[:10]))


def build_signal_tape_selection_contract(
    signal_dir: str | Path | None,
    *,
    product: str = PRODUCT,
    expected_symbol: str | None = None,
    expected_entry_tf: str | None = None,
    expected_filter_tf: str | None = None,
    expected_signal_set_id: str | None = None,
) -> dict[str, Any]:
    product_text = str(product or PRODUCT).strip()
    signal_path = _selection_path(signal_dir)

    if product_text != PRODUCT:
        return _invalid_selection(
            signal_path,
            product=product_text,
            code="product_mismatch",
            reason="--product must be free",
        )
    if signal_path is None or not signal_path.is_dir():
        return _invalid_selection(
            signal_path,
            product=product_text,
            code="missing_signal_dir",
            reason="signal_dir is missing or does not exist",
        )

    manifest_path = signal_path / "manifest.json"
    summary_path = signal_path / "summary.json"
    trades_path = signal_path / "trades.csv"

    if not manifest_path.is_file():
        return _invalid_selection(signal_path, product=product_text, code="missing_manifest", reason="manifest.json is missing")
    if not summary_path.is_file():
        return _invalid_selection(signal_path, product=product_text, code="missing_summary", reason="summary.json is missing")
    if not trades_path.is_file():
        return _invalid_selection(signal_path, product=product_text, code="missing_trades_csv", reason="trades.csv is missing")

    try:
        manifest_payload = _read_json(manifest_path)
        _validate_selection_safe_metadata(manifest_payload)
        manifest = tape.validate_manifest(
            manifest_payload,
            expected_product=product_text,
            expected_symbol=expected_symbol,
            expected_entry_tf=expected_entry_tf,
            expected_filter_tf=expected_filter_tf,
            expected_signal_set_id=expected_signal_set_id,
        )
    except (SelectionError, tape.SignalTapeError) as exc:
        code, reason = _error_code_and_reason("manifest.json", exc)
        return _invalid_selection(signal_path, product=product_text, code=code, reason=reason)

    try:
        summary_payload = _read_json(summary_path)
        _validate_selection_safe_metadata(summary_payload)
        summary = tape.validate_summary(summary_payload, require_safety_flags=True)
    except (SelectionError, tape.SignalTapeError) as exc:
        code, reason = _error_code_and_reason("summary.json", exc)
        return _invalid_selection(signal_path, product=product_text, code=code, reason=reason)

    safety = dict(manifest.get("safety_scope") or {})
    contract = {
        "schema_version": str(manifest.get("schema_version") or tape.SCHEMA_VERSION),
        "selection_schema_version": SELECTION_SCHEMA_VERSION,
        "product": str(manifest.get("product") or ""),
        "signal_dir": str(signal_path),
        "symbol": str(manifest.get("symbol") or ""),
        "symbol_normalized": str(manifest.get("symbol_normalized") or ""),
        "entry_tf": str(manifest.get("entry_tf") or ""),
        "filter_tf": str(manifest.get("filter_tf") or ""),
        "signal_set_id": str(manifest.get("signal_set_id") or ""),
        "created_at_utc": str(manifest.get("created_at_utc") or ""),
        "dataset_id": str(manifest.get("dataset_id") or ""),
        "since_ms": _safe_int(manifest.get("since_ms")),
        "until_ms": _safe_int(manifest.get("until_ms")),
        "trade_count": _safe_int(summary.get("trade_count", dict(manifest.get("counts") or {}).get("trades"))),
        "net_total": _safe_float(summary.get("net_total")),
        "final_equity": _safe_float(summary.get("final_equity")),
        "dd_schema_version": str(summary.get("dd_schema_version") or ""),
        "dd_sign_convention": str(summary.get("dd_sign_convention") or ""),
        "max_drawdown": _safe_float(summary.get("max_drawdown")),
        "max_dd_signed": _safe_float(summary.get("max_dd_signed")),
        "max_dd_abs": _safe_float(summary.get("max_dd_abs")),
        "max_dd_pct": _safe_float(summary.get("max_dd_pct")),
        "max_dd_display_abs": _safe_float(summary.get("max_dd_display_abs")),
        "max_dd_display_pct": _safe_float(summary.get("max_dd_display_pct")),
        "max_dd_display_label": str(summary.get("max_dd_display_label") or ""),
        "max_drawdown_legacy_note": str(summary.get("max_drawdown_legacy_note") or ""),
        "safety_research_only": safety.get("research_only") is True,
        "safety_paper_live_order_execution": safety.get("paper_live_order_execution") is True,
        "tape_files_present": _file_presence(signal_path),
        "manifest_sha256": tape.sha256_file(manifest_path),
        "summary_sha256": tape.sha256_file(summary_path),
        "trades_csv_sha256_from_manifest": _manifest_trades_sha(manifest),
        "status": "valid",
        "status_reason": "ok",
        "safe_error_code": "",
        "selectable_for_backtest_fast_path": True,
        "selectable_for_runner_replay_fast_path": True,
        "not_selectable_for_live": True,
        "not_selectable_for_paper": True,
        "picker_warning": "Read-only research selection; not available for LIVE/PAPER.",
    }
    return validate_signal_tape_selection_contract(contract)


def validate_signal_tape_selection_contract(selection: Mapping[str, Any]) -> dict[str, Any]:
    tape.validate_no_secret_payload(selection)
    status = str(selection.get("status") or "")
    if status not in {"valid", "invalid"}:
        raise SelectionError("selection status must be valid or invalid")
    if selection.get("not_selectable_for_live") is not True:
        raise SelectionError("selection must not be selectable for live")
    if selection.get("not_selectable_for_paper") is not True:
        raise SelectionError("selection must not be selectable for paper")
    if status == "valid":
        if selection.get("product") != PRODUCT:
            raise SelectionError("valid selection product must be free")
        if selection.get("safety_research_only") is not True:
            raise SelectionError("valid selection must be research-only")
        if selection.get("safety_paper_live_order_execution") is not False:
            raise SelectionError("valid selection must disable paper/live execution")
        if selection.get("selectable_for_backtest_fast_path") is not True:
            raise SelectionError("valid selection must be selectable for backtest fast path")
        if selection.get("selectable_for_runner_replay_fast_path") is not True:
            raise SelectionError("valid selection must be selectable for runner replay fast path")
    return dict(selection)


def write_selection_contract_json(path: str | Path, selection: Mapping[str, Any]) -> Path:
    payload = validate_signal_tape_selection_contract(selection)
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return out_path


def _format_text(selection: Mapping[str, Any], *, compact: bool = False) -> str:
    status = selection.get("status")
    if status == "valid":
        line = (
            f"precomputed signal selection product={selection.get('product')} status=valid "
            f"signal_dir={selection.get('signal_dir')} symbol={selection.get('symbol')} "
            f"entry_tf={selection.get('entry_tf')} filter_tf={selection.get('filter_tf')} "
            f"signal_set_id={selection.get('signal_set_id')} "
            f"backtest_fast_path={str(selection.get('selectable_for_backtest_fast_path')).lower()} "
            f"runner_replay_fast_path={str(selection.get('selectable_for_runner_replay_fast_path')).lower()} "
            f"live_selectable={str(not bool(selection.get('not_selectable_for_live'))).lower()} "
            f"paper_selectable={str(not bool(selection.get('not_selectable_for_paper'))).lower()}"
        )
        if compact:
            return line
        return (
            line
            + f" net_total={selection.get('net_total')} final_equity={selection.get('final_equity')}"
            + f" max_dd_abs={selection.get('max_dd_abs')} max_dd_pct={selection.get('max_dd_pct')}"
        )
    return (
        f"precomputed signal selection product={selection.get('product')} status=invalid "
        f"signal_dir={selection.get('signal_dir')} safe_error_code={selection.get('safe_error_code')} "
        f"status_reason={selection.get('status_reason')}"
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read-only selection preflight for Free precomputed signal tapes.")
    parser.add_argument("--signal-dir", default="")
    parser.add_argument("--product", default=PRODUCT)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--out", default="")
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--expect-symbol", default="")
    parser.add_argument("--expect-entry-tf", default="")
    parser.add_argument("--expect-filter-tf", default="")
    parser.add_argument("--expect-signal-set-id", default="")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    selection = build_signal_tape_selection_contract(
        str(args.signal_dir or "").strip() or None,
        product=str(args.product or PRODUCT).strip(),
        expected_symbol=str(args.expect_symbol or "").strip() or None,
        expected_entry_tf=str(args.expect_entry_tf or "").strip() or None,
        expected_filter_tf=str(args.expect_filter_tf or "").strip() or None,
        expected_signal_set_id=str(args.expect_signal_set_id or "").strip() or None,
    )

    out_path = str(args.out or "").strip()
    if out_path:
        write_selection_contract_json(out_path, selection)
        print(_format_text(selection, compact=True))
    elif args.format == "json":
        print(json.dumps(selection, ensure_ascii=True, sort_keys=True))
    else:
        print(_format_text(selection))

    if selection.get("status") != "valid":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
