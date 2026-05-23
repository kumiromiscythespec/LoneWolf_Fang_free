# BUILD_ID: 2026-05-08_free_precomputed_gui_selection_diagnostics_v1
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping

import precomputed_signals_selection as selection_contract
import signal_tape as tape

BUILD_ID = "2026-05-08_free_precomputed_gui_selection_diagnostics_v1"
GUI_ADAPTER_SCHEMA_VERSION = "lwf.precomputed.signal_tape.gui_picker_item.v1"
PRODUCT = tape.DEFAULT_PRODUCT
GUI_DD_DISPLAY_LABEL = tape.DD_DISPLAY_LABEL
GUI_DD_DISPLAY_PCT_LABEL = "Max DD pct (display)"
GUI_DD_LEGACY_NOTE = tape.MAX_DRAWDOWN_LEGACY_NOTE

GUI_PICKER_ITEM_FIELDS = (
    "schema_version",
    "gui_adapter_schema_version",
    "product",
    "signal_dir",
    "title",
    "subtitle",
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
    "safety_research_only",
    "safety_paper_live_order_execution",
    "dd_schema_version",
    "dd_sign_convention",
    "max_drawdown",
    "max_dd_signed",
    "max_dd_abs",
    "max_dd_pct",
    "max_dd_display_abs",
    "max_dd_display_pct",
    "max_dd_display_label",
    "max_drawdown_legacy_note",
    "dd_display_text",
    "net_total_text",
    "final_equity_text",
    "tape_files_present",
    "manifest_sha256",
    "summary_sha256",
    "trades_csv_sha256_from_manifest",
    "safe_error_code",
    "selectable_for_backtest_fast_path",
    "selectable_for_runner_replay_fast_path",
    "not_selectable_for_live",
    "not_selectable_for_paper",
    "picker_warning",
    "status",
    "status_reason",
)

_FORBIDDEN_GUI_KEYS = {
    "api_key",
    "apikey",
    "secret",
    "token",
    "authorization",
    "entry_exec",
    "exit_exec",
    "entry_raw",
    "exit_raw",
    "qty",
    "quantity",
    "trade_id",
    "order_id",
    "raw_order",
    "raw_orders",
    "balance",
    "balance_snapshot",
    "raw_billing",
    "raw_market_data",
    "market_data",
    "raw_ohlcv",
    "ohlcv",
    "raw_trades",
    "raw_trade_rows",
    "trade_rows",
    "trades",
}


class GuiPickerAdapterError(ValueError):
    pass


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _safe_int(value: Any) -> int:
    try:
        if value in (None, ""):
            return 0
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _safe_float_or_none(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_amount(value: float | None) -> str:
    if value is None:
        return "--"
    return f"{float(value):,.4f}"


def _format_pct(value: float | None) -> str:
    if value is None:
        return "--"
    return f"{float(value) * 100.0:.4f}%"


def _safe_tape_files_present(value: Any) -> dict[str, bool]:
    payload = value if isinstance(value, Mapping) else {}
    return {
        "manifest_json": bool(payload.get("manifest_json")) if isinstance(payload, Mapping) else False,
        "summary_json": bool(payload.get("summary_json")) if isinstance(payload, Mapping) else False,
        "trades_csv": bool(payload.get("trades_csv")) if isinstance(payload, Mapping) else False,
    }


def _forbidden_key_paths(payload: Any) -> list[str]:
    violations: list[str] = []

    def visit(value: Any, path: tuple[str, ...]) -> None:
        if isinstance(value, Mapping):
            for key, nested in value.items():
                key_text = str(key)
                key_lower = key_text.lower()
                if key_lower in _FORBIDDEN_GUI_KEYS:
                    violations.append(".".join(path + (key_text,)))
                visit(nested, path + (key_text,))
            return
        if isinstance(value, (list, tuple)):
            for index, nested in enumerate(value):
                visit(nested, path + (str(index),))

    visit(payload, ())
    return violations


def _invalid_item(source: Mapping[str, Any] | None, reason: str) -> dict[str, Any]:
    payload = dict(source or {}) if isinstance(source, Mapping) else {}
    signal_dir = _safe_text(payload.get("signal_dir"))
    product = _safe_text(payload.get("product")) or PRODUCT
    status_reason = _safe_text(reason) or "invalid selection"
    safe_error_code = _safe_text(payload.get("safe_error_code")) or "invalid_unknown"
    item = {
        "schema_version": _safe_text(payload.get("schema_version")) or tape.SCHEMA_VERSION,
        "gui_adapter_schema_version": GUI_ADAPTER_SCHEMA_VERSION,
        "product": product,
        "signal_dir": signal_dir,
        "title": "Invalid precomputed signal selection",
        "subtitle": status_reason,
        "symbol": _safe_text(payload.get("symbol")),
        "symbol_normalized": _safe_text(payload.get("symbol_normalized")),
        "entry_tf": _safe_text(payload.get("entry_tf")),
        "filter_tf": _safe_text(payload.get("filter_tf")),
        "signal_set_id": _safe_text(payload.get("signal_set_id")),
        "dataset_id": _safe_text(payload.get("dataset_id")),
        "created_at_utc": _safe_text(payload.get("created_at_utc")),
        "since_ms": _safe_int(payload.get("since_ms")),
        "until_ms": _safe_int(payload.get("until_ms")),
        "trade_count": _safe_int(payload.get("trade_count")),
        "net_total": 0.0,
        "final_equity": 0.0,
        "safety_research_only": payload.get("safety_research_only") is True,
        "safety_paper_live_order_execution": payload.get("safety_paper_live_order_execution") is True,
        "dd_schema_version": "",
        "dd_sign_convention": "",
        "max_drawdown": 0.0,
        "max_dd_signed": 0.0,
        "max_dd_abs": 0.0,
        "max_dd_pct": 0.0,
        "max_dd_display_abs": 0.0,
        "max_dd_display_pct": 0.0,
        "max_dd_display_label": GUI_DD_DISPLAY_LABEL,
        "max_drawdown_legacy_note": GUI_DD_LEGACY_NOTE,
        "dd_display_text": "",
        "net_total_text": "--",
        "final_equity_text": "--",
        "tape_files_present": _safe_tape_files_present(payload.get("tape_files_present")),
        "manifest_sha256": "",
        "summary_sha256": "",
        "trades_csv_sha256_from_manifest": "",
        "safe_error_code": safe_error_code,
        "selectable_for_backtest_fast_path": False,
        "selectable_for_runner_replay_fast_path": False,
        "not_selectable_for_live": True,
        "not_selectable_for_paper": True,
        "picker_warning": "Invalid precomputed signal selection.",
        "status": "invalid",
        "status_reason": status_reason,
    }
    return validate_gui_picker_item(item)


def _dd_display_numbers(payload: Mapping[str, Any]) -> tuple[float, float, float, str, str]:
    legacy = _safe_float_or_none(payload.get("max_drawdown"))
    signed = _safe_float_or_none(payload.get("max_dd_signed"))
    if signed is None and legacy is not None and legacy <= 0.0:
        signed = legacy
    if legacy is None and signed is not None:
        legacy = signed
    if legacy is None or signed is None:
        raise GuiPickerAdapterError("missing signed drawdown fields")
    if legacy > 0.0 or signed > 0.0:
        raise GuiPickerAdapterError("positive legacy max_drawdown is invalid")
    if abs(float(legacy) - float(signed)) > 1e-9:
        raise GuiPickerAdapterError("max_drawdown must equal max_dd_signed")

    display_abs = _safe_float_or_none(payload.get("max_dd_display_abs"))
    if display_abs is None:
        display_abs = _safe_float_or_none(payload.get("max_dd_abs"))
    display_pct = _safe_float_or_none(payload.get("max_dd_display_pct"))
    if display_pct is None:
        display_pct = _safe_float_or_none(payload.get("max_dd_pct"))
    if display_abs is None or display_pct is None:
        raise GuiPickerAdapterError("missing drawdown display fields")
    if display_abs < 0.0 or display_pct < 0.0:
        raise GuiPickerAdapterError("drawdown display fields must be non-negative")

    label = _safe_text(payload.get("max_dd_display_label")) or GUI_DD_DISPLAY_LABEL
    legacy_note = _safe_text(payload.get("max_drawdown_legacy_note")) or GUI_DD_LEGACY_NOTE
    return float(signed), float(display_abs), float(display_pct), label, legacy_note


def format_gui_drawdown_text(selection_or_item: Mapping[str, Any]) -> str:
    signed, display_abs, display_pct, label, _legacy_note = _dd_display_numbers(selection_or_item)
    return " | ".join(
        (
            f"{label}: {_format_amount(display_abs)}",
            f"{GUI_DD_DISPLAY_PCT_LABEL}: {_format_pct(display_pct)}",
            f"Legacy signed max_drawdown: {_format_amount(signed)}",
        )
    )


def format_gui_signal_tape_title(selection_or_item: Mapping[str, Any]) -> str:
    symbol = _safe_text(selection_or_item.get("symbol")) or _safe_text(selection_or_item.get("symbol_normalized")) or "unknown"
    entry_tf = _safe_text(selection_or_item.get("entry_tf")) or "?"
    filter_tf = _safe_text(selection_or_item.get("filter_tf")) or "?"
    signal_set_id = _safe_text(selection_or_item.get("signal_set_id"))
    if signal_set_id:
        return f"{symbol} {entry_tf}/{filter_tf} {signal_set_id}"
    return f"{symbol} {entry_tf}/{filter_tf}"


def build_gui_picker_item(selection: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(selection, Mapping):
        return _invalid_item(None, "selection payload must be a mapping")

    try:
        tape.validate_no_secret_payload(selection)
    except tape.SignalTapeError:
        return _invalid_item(selection, "selection contains forbidden private/runtime metadata")

    if _forbidden_key_paths(selection):
        return _invalid_item(selection, "selection contains forbidden row-level metadata")

    try:
        payload = selection_contract.validate_signal_tape_selection_contract(selection)
    except (selection_contract.SelectionError, tape.SignalTapeError):
        return _invalid_item(selection, "selection failed safe GUI validation")

    if payload.get("status") != "valid":
        reason = _safe_text(payload.get("safe_error_code")) or _safe_text(payload.get("status_reason")) or "invalid selection"
        return _invalid_item(payload, reason)

    try:
        signed, display_abs, display_pct, label, legacy_note = _dd_display_numbers(payload)
    except GuiPickerAdapterError as exc:
        return _invalid_item(payload, str(exc))

    max_dd_abs = _safe_float_or_none(payload.get("max_dd_abs"))
    max_dd_pct = _safe_float_or_none(payload.get("max_dd_pct"))
    item = {
        "schema_version": _safe_text(payload.get("schema_version")) or tape.SCHEMA_VERSION,
        "gui_adapter_schema_version": GUI_ADAPTER_SCHEMA_VERSION,
        "product": _safe_text(payload.get("product")) or PRODUCT,
        "signal_dir": _safe_text(payload.get("signal_dir")),
        "title": format_gui_signal_tape_title(payload),
        "subtitle": (
            f"free | trades={_safe_int(payload.get('trade_count'))} | "
            f"{label}: {_format_amount(display_abs)}"
        ),
        "symbol": _safe_text(payload.get("symbol")),
        "symbol_normalized": _safe_text(payload.get("symbol_normalized")),
        "entry_tf": _safe_text(payload.get("entry_tf")),
        "filter_tf": _safe_text(payload.get("filter_tf")),
        "signal_set_id": _safe_text(payload.get("signal_set_id")),
        "dataset_id": _safe_text(payload.get("dataset_id")),
        "created_at_utc": _safe_text(payload.get("created_at_utc")),
        "since_ms": _safe_int(payload.get("since_ms")),
        "until_ms": _safe_int(payload.get("until_ms")),
        "trade_count": _safe_int(payload.get("trade_count")),
        "net_total": float(_safe_float_or_none(payload.get("net_total")) or 0.0),
        "final_equity": float(_safe_float_or_none(payload.get("final_equity")) or 0.0),
        "safety_research_only": payload.get("safety_research_only") is True,
        "safety_paper_live_order_execution": payload.get("safety_paper_live_order_execution") is True,
        "dd_schema_version": _safe_text(payload.get("dd_schema_version")),
        "dd_sign_convention": _safe_text(payload.get("dd_sign_convention")),
        "max_drawdown": float(signed),
        "max_dd_signed": float(signed),
        "max_dd_abs": float(max_dd_abs if max_dd_abs is not None else display_abs),
        "max_dd_pct": float(max_dd_pct if max_dd_pct is not None else display_pct),
        "max_dd_display_abs": float(display_abs),
        "max_dd_display_pct": float(display_pct),
        "max_dd_display_label": label,
        "max_drawdown_legacy_note": legacy_note,
        "dd_display_text": format_gui_drawdown_text(payload),
        "net_total_text": _format_amount(_safe_float_or_none(payload.get("net_total"))),
        "final_equity_text": _format_amount(_safe_float_or_none(payload.get("final_equity"))),
        "tape_files_present": _safe_tape_files_present(payload.get("tape_files_present")),
        "manifest_sha256": _safe_text(payload.get("manifest_sha256")),
        "summary_sha256": _safe_text(payload.get("summary_sha256")),
        "trades_csv_sha256_from_manifest": _safe_text(payload.get("trades_csv_sha256_from_manifest")),
        "safe_error_code": _safe_text(payload.get("safe_error_code")),
        "selectable_for_backtest_fast_path": True,
        "selectable_for_runner_replay_fast_path": True,
        "not_selectable_for_live": True,
        "not_selectable_for_paper": True,
        "picker_warning": _safe_text(payload.get("picker_warning")) or "Read-only research selection; not available for LIVE/PAPER.",
        "status": "valid",
        "status_reason": "ok",
    }
    return validate_gui_picker_item(item)


def build_gui_picker_item_from_signal_dir(
    signal_dir: str | Path | None,
    *,
    product: str = PRODUCT,
    expected_symbol: str | None = None,
    expected_entry_tf: str | None = None,
    expected_filter_tf: str | None = None,
    expected_signal_set_id: str | None = None,
) -> dict[str, Any]:
    contract = selection_contract.build_signal_tape_selection_contract(
        signal_dir,
        product=product,
        expected_symbol=expected_symbol,
        expected_entry_tf=expected_entry_tf,
        expected_filter_tf=expected_filter_tf,
        expected_signal_set_id=expected_signal_set_id,
    )
    return build_gui_picker_item(contract)


def validate_gui_picker_item(item: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(item, Mapping):
        raise GuiPickerAdapterError("GUI picker item must be a mapping")
    payload = dict(item)
    unknown = sorted(set(payload) - set(GUI_PICKER_ITEM_FIELDS))
    if unknown:
        raise GuiPickerAdapterError("GUI picker item contains unsupported fields: " + ", ".join(unknown[:10]))
    missing = [field for field in GUI_PICKER_ITEM_FIELDS if field not in payload]
    if missing:
        raise GuiPickerAdapterError("GUI picker item missing fields: " + ", ".join(missing))
    tape.validate_no_secret_payload(payload)
    if _forbidden_key_paths(payload):
        raise GuiPickerAdapterError("GUI picker item contains forbidden row-level fields")
    if payload.get("not_selectable_for_live") is not True:
        raise GuiPickerAdapterError("GUI picker item must not be selectable for live")
    if payload.get("not_selectable_for_paper") is not True:
        raise GuiPickerAdapterError("GUI picker item must not be selectable for paper")

    status = _safe_text(payload.get("status"))
    if status not in {"valid", "invalid"}:
        raise GuiPickerAdapterError("GUI picker item status must be valid or invalid")
    if status == "invalid":
        if payload.get("selectable_for_backtest_fast_path") is not False:
            raise GuiPickerAdapterError("invalid GUI picker item must not be selectable for backtest")
        if payload.get("selectable_for_runner_replay_fast_path") is not False:
            raise GuiPickerAdapterError("invalid GUI picker item must not be selectable for runner replay")
        if not isinstance(payload.get("tape_files_present"), Mapping):
            raise GuiPickerAdapterError("GUI picker item tape_files_present must be a mapping")
        return payload

    if payload.get("product") != PRODUCT:
        raise GuiPickerAdapterError("valid GUI picker item product must be free")
    if payload.get("safety_research_only") is not True:
        raise GuiPickerAdapterError("valid GUI picker item must be research-only")
    if payload.get("safety_paper_live_order_execution") is not False:
        raise GuiPickerAdapterError("valid GUI picker item must disable paper/live execution")
    if not isinstance(payload.get("tape_files_present"), Mapping):
        raise GuiPickerAdapterError("valid GUI picker item tape_files_present must be a mapping")
    if payload.get("selectable_for_backtest_fast_path") is not True:
        raise GuiPickerAdapterError("valid GUI picker item must be selectable for backtest fast path")
    if payload.get("selectable_for_runner_replay_fast_path") is not True:
        raise GuiPickerAdapterError("valid GUI picker item must be selectable for runner replay fast path")
    signed, display_abs, display_pct, _label, _legacy_note = _dd_display_numbers(payload)
    if abs(float(payload.get("max_drawdown")) - signed) > 1e-9:
        raise GuiPickerAdapterError("max_drawdown must equal max_dd_signed")
    if abs(float(payload.get("max_dd_display_abs")) - display_abs) > 1e-9:
        raise GuiPickerAdapterError("max_dd_display_abs mismatch")
    if abs(float(payload.get("max_dd_display_pct")) - display_pct) > 1e-12:
        raise GuiPickerAdapterError("max_dd_display_pct mismatch")
    return payload


def _format_text(item: Mapping[str, Any], *, compact: bool = False) -> str:
    status = item.get("status")
    if status != "valid":
        return (
            f"precomputed signal picker item product={item.get('product')} status=invalid "
            f"signal_dir={item.get('signal_dir')} status_reason={item.get('status_reason')}"
        )
    line = (
        f"precomputed signal picker item product={item.get('product')} status=valid "
        f"signal_dir={item.get('signal_dir')} title={item.get('title')} "
        f"backtest_fast_path={str(item.get('selectable_for_backtest_fast_path')).lower()} "
        f"runner_replay_fast_path={str(item.get('selectable_for_runner_replay_fast_path')).lower()} "
        f"live_selectable={str(not bool(item.get('not_selectable_for_live'))).lower()} "
        f"paper_selectable={str(not bool(item.get('not_selectable_for_paper'))).lower()}"
    )
    if compact:
        return line
    return line + " " + str(item.get("dd_display_text") or "")


def write_gui_picker_item_json(path: str | Path, item: Mapping[str, Any]) -> Path:
    payload = validate_gui_picker_item(item)
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return out_path


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read-only GUI picker adapter for Free precomputed signal tapes.")
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
    signal_dir = _safe_text(args.signal_dir) or None
    item = build_gui_picker_item_from_signal_dir(
        signal_dir,
        product=_safe_text(args.product) or PRODUCT,
        expected_symbol=_safe_text(args.expect_symbol) or None,
        expected_entry_tf=_safe_text(args.expect_entry_tf) or None,
        expected_filter_tf=_safe_text(args.expect_filter_tf) or None,
        expected_signal_set_id=_safe_text(args.expect_signal_set_id) or None,
    )

    out_path = _safe_text(args.out)
    if out_path:
        write_gui_picker_item_json(out_path, item)
        print(_format_text(item, compact=True))
    elif args.format == "json":
        print(json.dumps(item, ensure_ascii=True, sort_keys=True))
    else:
        print(_format_text(item))

    if item.get("status") != "valid":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
