# BUILD_ID: 2026-05-08_free_precomputed_safe_producer_v1
from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import signal_tape as tape

BUILD_ID = "2026-05-08_free_precomputed_safe_producer_v1"
PRODUCT = tape.DEFAULT_PRODUCT
DEFAULT_INITIAL_EQUITY = 300000.0
PARITY_TOLERANCE = 1e-6

_SYMBOL_COLUMNS = ("symbol", "sym")
_ENTRY_TS_COLUMNS = ("entry_signal_ts_ms", "entry_exec_ts_ms", "entry_ts_ms", "opened_ts", "entry_bar_ts_ms")
_EXIT_TS_COLUMNS = ("exit_exec_ts_ms", "exit_ts_ms", "close_ts_ms", "exit_signal_ts_ms", "ts")
_ENTRY_FEE_RATE_COLUMNS = ("entry_fee_rate", "fee_rate", "maker_fee_rate")
_EXIT_FEE_RATE_COLUMNS = ("exit_fee_rate", "fee_rate", "taker_fee_rate")
_GROSS_COLUMNS = ("gross_pnl", "pnl")


def _fail(message: str) -> None:
    raise SystemExit(f"fail closed: {message}")


def parse_utc_to_ms(value: str | None, *, end_exclusive_date: bool = False) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    if text.isdigit():
        return int(text)
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    parsed = parsed.astimezone(timezone.utc)
    if end_exclusive_date and len(text) == 10:
        parsed = parsed + timedelta(days=1)
    return int(parsed.timestamp() * 1000)


def _lowered_columns(columns: Iterable[str]) -> set[str]:
    return {str(column).strip().lower() for column in columns}


def _has_any_column(columns: Iterable[str], aliases: Iterable[str]) -> bool:
    lowered = _lowered_columns(columns)
    return any(alias.lower() in lowered for alias in aliases)


def _first(row: Mapping[str, Any], aliases: Iterable[str], default: Any = None) -> Any:
    lowered = {str(key).lower(): value for key, value in row.items()}
    for alias in aliases:
        value = lowered.get(alias.lower())
        if value not in (None, ""):
            return value
    return default


def _to_float(value: Any, *, label: str, row_index: int) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        _fail(f"row {row_index} has invalid {label}")
    if not math.isfinite(parsed):
        _fail(f"row {row_index} has non-finite {label}")
    return parsed


def _to_int(value: Any, *, label: str, row_index: int) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        _fail(f"row {row_index} has invalid {label}")


def _close_enough(left: float, right: float) -> bool:
    return abs(float(left) - float(right)) <= PARITY_TOLERANCE + (PARITY_TOLERANCE * max(abs(float(left)), abs(float(right)), 1.0))


def read_csv_rows_required(path: Path, *, label: str) -> tuple[list[str], list[dict[str, str]]]:
    if not path.is_file():
        _fail(f"{label} not found: {path}")
    try:
        columns, rows = tape.read_csv_rows(path)
    except OSError as exc:
        _fail(f"cannot read {label}: {exc}")
    try:
        tape.validate_no_secret_payload({column: "" for column in columns})
        tape.validate_no_secret_payload(rows)
    except tape.SignalTapeError as exc:
        _fail(f"unsafe {label}: {exc}")
    return columns, rows


def _require_columns(columns: Iterable[str], requirements: Mapping[str, Iterable[str]]) -> None:
    missing = [label for label, aliases in requirements.items() if not _has_any_column(columns, aliases)]
    if missing:
        _fail("trades.csv missing required accounting columns: " + ", ".join(missing))


def validate_and_prepare_source_trades(
    columns: Iterable[str],
    rows: Iterable[Mapping[str, Any]],
    *,
    expected_symbol: str,
) -> list[dict[str, Any]]:
    _require_columns(
        columns,
        {
            "symbol/sym": _SYMBOL_COLUMNS,
            "entry timestamp": _ENTRY_TS_COLUMNS,
            "exit timestamp": _EXIT_TS_COLUMNS,
            "entry_exec": ("entry_exec",),
            "exit_exec": ("exit_exec",),
            "qty": ("qty", "quantity", "qty_init"),
            "entry_fee_rate/fee_rate": _ENTRY_FEE_RATE_COLUMNS,
            "exit_fee_rate/fee_rate": _EXIT_FEE_RATE_COLUMNS,
            "fee": ("fee", "total_fee"),
            "net": ("net",),
        },
    )
    prepared: list[dict[str, Any]] = []
    expected_symbol_normalized = tape.normalize_symbol(expected_symbol)

    for index, source_row in enumerate(rows, start=1):
        row = dict(source_row)
        source_symbol = str(_first(row, _SYMBOL_COLUMNS, expected_symbol) or expected_symbol)
        if tape.normalize_symbol(source_symbol) != expected_symbol_normalized:
            _fail(f"row {index} symbol does not match --symbol")
        row["symbol"] = source_symbol

        side = str(_first(row, ("side", "direction"), "long") or "long").lower()
        if side != "long":
            _fail(f"row {index} side is not supported by the free fast path")

        entry_exec = _to_float(_first(row, ("entry_exec",)), label="entry_exec", row_index=index)
        exit_exec = _to_float(_first(row, ("exit_exec",)), label="exit_exec", row_index=index)
        qty = _to_float(_first(row, ("qty", "quantity", "qty_init")), label="qty", row_index=index)
        entry_fee_rate = _to_float(_first(row, _ENTRY_FEE_RATE_COLUMNS), label="entry_fee_rate/fee_rate", row_index=index)
        exit_fee_rate = _to_float(_first(row, _EXIT_FEE_RATE_COLUMNS), label="exit_fee_rate/fee_rate", row_index=index)
        fee = _to_float(_first(row, ("fee", "total_fee")), label="fee", row_index=index)
        net = _to_float(_first(row, ("net",)), label="net", row_index=index)
        _to_int(_first(row, _ENTRY_TS_COLUMNS), label="entry timestamp", row_index=index)
        _to_int(_first(row, _EXIT_TS_COLUMNS), label="exit timestamp", row_index=index)

        gross_raw = _first(row, _GROSS_COLUMNS)
        gross_pnl = net + fee if gross_raw in (None, "") else _to_float(gross_raw, label="gross_pnl/pnl", row_index=index)
        if gross_raw in (None, ""):
            row["gross_pnl"] = gross_pnl

        calc_gross = (exit_exec - entry_exec) * qty
        calc_fee = entry_exec * qty * entry_fee_rate + exit_exec * qty * exit_fee_rate
        calc_net = calc_gross - calc_fee
        if not _close_enough(gross_pnl, calc_gross):
            _fail(f"row {index} gross_pnl does not match entry/exit/qty")
        if not _close_enough(fee, calc_fee):
            _fail(f"row {index} fee does not match fee rates")
        if not _close_enough(net, calc_net):
            _fail(f"row {index} net does not match recomputed accounting")
        prepared.append(row)

    return prepared


def read_equity_reference(equity_csv: Path, *, initial_equity: float) -> list[dict[str, Any]]:
    columns, rows = read_csv_rows_required(equity_csv, label="equity-csv")
    if not _has_any_column(columns, ("equity", "mtm_equity", "source_equity_after")):
        _fail("equity-csv missing equity column")
    equity_rows: list[dict[str, Any]] = []
    peak = float(initial_equity)
    for index, row in enumerate(rows, start=1):
        equity = _to_float(_first(row, ("equity", "mtm_equity", "source_equity_after")), label="equity", row_index=index)
        ts_ms = _to_int(_first(row, ("ts_ms", "ts", "exit_exec_ts_ms", "close_ts_ms"), 0), label="equity timestamp", row_index=index)
        net = float(_first(row, ("net",), 0.0) or 0.0)
        peak = max(peak, equity)
        equity_rows.append({
            "trade_id": str(_first(row, ("trade_id",), f"equity_{index}") or f"equity_{index}"),
            "ts_ms": ts_ms,
            "equity": equity,
            "peak": peak,
            "dd": equity - peak,
            "net": net,
        })
    if not equity_rows:
        _fail("equity-csv has no rows")
    return equity_rows


def build_summary(
    trades: Iterable[Mapping[str, Any]],
    *,
    equity_reference: Iterable[Mapping[str, Any]],
    initial_equity: float,
    equity_reference_basis: str,
) -> dict[str, Any]:
    trade_list = list(trades)
    equity_rows = list(equity_reference)
    net_total = sum(float(row.get("net") or 0.0) for row in trade_list)
    gross_total = sum(float(row.get("gross_pnl") or 0.0) for row in trade_list)
    fee_total = sum(float(row.get("fee") or 0.0) for row in trade_list)
    final_equity = float(equity_rows[-1]["equity"]) if equity_rows else float(initial_equity) + net_total
    return tape.attach_drawdown_display_fields({
        "schema_version": tape.SCHEMA_VERSION,
        "trade_count": len(trade_list),
        "initial_equity": float(initial_equity),
        "final_equity": final_equity,
        "net_total": net_total,
        "gross_pnl_total": gross_total,
        "fee_total": fee_total,
        **tape.compute_drawdown_metrics(equity_rows),
        "win_count": sum(1 for row in trade_list if float(row.get("net") or 0.0) > 0.0),
        "loss_count": sum(1 for row in trade_list if float(row.get("net") or 0.0) < 0.0),
        "research_only": True,
        "paper_live_order_execution": False,
        "redaction": "safe-summary-v1",
        "equity_reference_basis": equity_reference_basis,
    })


def validate_optional_summary(summary_json: str) -> Path | None:
    if not str(summary_json or "").strip():
        return None
    path = Path(summary_json)
    if not path.is_file():
        _fail(f"summary-json not found: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        _fail(f"cannot read summary-json: {exc}")
    try:
        tape.validate_summary(payload, require_safety_flags=False)
    except tape.SignalTapeError as exc:
        _fail(f"unsafe summary-json: {exc}")
    return path


def infer_time_window(args: argparse.Namespace, trades: Iterable[Mapping[str, Any]]) -> tuple[int, int]:
    trade_list = list(trades)
    since_ms = parse_utc_to_ms(args.since) if args.since else 0
    until_ms = parse_utc_to_ms(args.until, end_exclusive_date=True) if args.until else 0
    if since_ms <= 0 and trade_list:
        since_ms = min(int(row.get("entry_signal_ts_ms") or row.get("entry_exec_ts_ms") or 0) for row in trade_list)
    if until_ms <= 0 and trade_list:
        until_ms = max(int(row.get("exit_exec_ts_ms") or row.get("exit_signal_ts_ms") or 0) for row in trade_list)
    return since_ms, until_ms


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Convert existing research trade exports into a Free signal tape.")
    parser.add_argument("--trades-csv", default="")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--entry-tf", required=True)
    parser.add_argument("--filter-tf", required=True)
    parser.add_argument("--equity-csv", default="")
    parser.add_argument("--summary-json", default="")
    parser.add_argument("--product", default=PRODUCT)
    parser.add_argument("--since", default="")
    parser.add_argument("--until", default="")
    parser.add_argument("--dataset-id", default="")
    parser.add_argument("--out-root", default="")
    parser.add_argument("--signal-set-id", default="")
    parser.add_argument("--initial-equity", type=float, default=DEFAULT_INITIAL_EQUITY)
    parser.add_argument("--write-summary", action="store_true")
    parser.add_argument("--strict", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    product = str(args.product or PRODUCT).strip()
    if product != PRODUCT:
        _fail("--product must be free")
    if not str(args.trades_csv or "").strip():
        _fail("--trades-csv is required")

    trades_csv = Path(args.trades_csv)
    columns, source_rows = read_csv_rows_required(trades_csv, label="trades-csv")
    prepared_rows = validate_and_prepare_source_trades(columns, source_rows, expected_symbol=str(args.symbol))
    trades = tape.canonicalize_trade_rows(
        prepared_rows,
        symbol=str(args.symbol),
        initial_equity=float(args.initial_equity),
        source_name="existing.trades_csv",
    )

    summary_path = validate_optional_summary(str(args.summary_json or ""))
    equity_path = Path(args.equity_csv) if str(args.equity_csv or "").strip() else None
    if equity_path is not None:
        equity_reference = read_equity_reference(equity_path, initial_equity=float(args.initial_equity))
        equity_basis = "source_equity_csv"
    else:
        equity_reference = tape.equity_reference_from_trades(trades, float(args.initial_equity))
        equity_basis = "synthetic_from_trades_net"

    summary = build_summary(
        trades,
        equity_reference=equity_reference,
        initial_equity=float(args.initial_equity),
        equity_reference_basis=equity_basis,
    )
    since_ms, until_ms = infer_time_window(args, trades)
    dataset_id = str(args.dataset_id or "").strip() or (
        f"{tape.normalize_symbol(str(args.symbol))}_{args.entry_tf}_{args.filter_tf}_{since_ms}_{until_ms}"
    )
    dataset_paths = [trades_csv]
    if equity_path is not None:
        dataset_paths.append(equity_path)
    if summary_path is not None:
        dataset_paths.append(summary_path)
    dataset_files_hash = tape.hash_existing_files(dataset_paths)
    signal_config_hash = tape.stable_json_hash({
        "product": product,
        "symbol": str(args.symbol),
        "entry_tf": str(args.entry_tf),
        "filter_tf": str(args.filter_tf),
        "since_ms": since_ms,
        "until_ms": until_ms,
        "dataset_id": dataset_id,
    })
    accounting_config_hash = tape.stable_json_hash({
        "fee_model": tape.DEFAULT_FEE_MODEL,
        "sizing_mode": "source_backtest",
        "market_type": "spot",
    })
    strategy_file_hash = tape.stable_json_hash({"strategy": "not_used_by_safe_producer"})
    config_file_hash = tape.stable_json_hash({"config": "not_used_by_safe_producer"})
    signal_set_id = str(args.signal_set_id or "").strip() or tape.make_signal_set_id(
        product=product,
        symbol=str(args.symbol),
        entry_tf=str(args.entry_tf),
        filter_tf=str(args.filter_tf),
        since_ms=since_ms,
        until_ms=until_ms,
        dataset_files_hash=dataset_files_hash,
        strategy_file_hash=strategy_file_hash,
        config_file_hash=config_file_hash,
        signal_config_hash=signal_config_hash,
        accounting_config_hash=accounting_config_hash,
    )
    manifest = tape.build_safe_manifest(
        product=product,
        producer_script="precompute_signals.py",
        producer_build_id=BUILD_ID,
        symbol=str(args.symbol),
        entry_tf=str(args.entry_tf),
        filter_tf=str(args.filter_tf),
        since_ms=since_ms,
        until_ms=until_ms,
        dataset_id=dataset_id,
        dataset_files_hash=dataset_files_hash,
        strategy_file_hash=strategy_file_hash,
        config_file_hash=config_file_hash,
        signal_config_hash=signal_config_hash,
        accounting_config_hash=accounting_config_hash,
        signal_set_id=signal_set_id,
    )
    manifest["initial_equity"] = float(args.initial_equity)
    manifest["source_artifacts"] = {
        "trades_csv": {"name": trades_csv.name, "sha256": tape.sha256_file(trades_csv)},
    }
    if equity_path is not None:
        manifest["source_artifacts"]["equity_csv"] = {"name": equity_path.name, "sha256": tape.sha256_file(equity_path)}
    if summary_path is not None:
        manifest["source_artifacts"]["summary_json"] = {"name": summary_path.name, "sha256": tape.sha256_file(summary_path)}
    manifest["source_net_only"] = False

    output_dir = tape.signal_tape_dir(
        product=product,
        symbol=str(args.symbol),
        entry_tf=str(args.entry_tf),
        filter_tf=str(args.filter_tf),
        signal_set_id=signal_set_id,
        root=str(args.out_root or "").strip() or None,
    )
    tape.write_signal_tape_bundle(output_dir, manifest=manifest, trades=trades, equity_reference=equity_reference, summary=summary)
    if bool(args.strict):
        tape.load_signal_tape(
            output_dir,
            expected_product=PRODUCT,
            expected_symbol=str(args.symbol),
            expected_entry_tf=str(args.entry_tf),
            expected_filter_tf=str(args.filter_tf),
            expected_signal_set_id=signal_set_id,
        )
    print(str(output_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
