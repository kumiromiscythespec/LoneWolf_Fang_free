# BUILD_ID: 2026-05-08_free_precomputed_backtest_fast_path_v1
# BUILD_ID: 2026-05-08_free_precomputed_signals_foundation_v1
from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
from pathlib import Path
from typing import Any, Iterable, Mapping

from signal_tape import (
    DEFAULT_PRODUCT,
    SignalTapeError,
    attach_drawdown_display_fields,
    compute_drawdown_metrics,
    load_manifest,
    load_summary,
    read_trades_csv,
    sha256_file,
    validate_manifest,
    validate_no_secret_payload,
)

BUILD_ID = "2026-05-08_free_precomputed_backtest_fast_path_v1"
REQUIRED_TAPE_FILES = ("manifest.json", "trades.csv", "summary.json")


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _signals_dir_path(signals_dir: str | Path) -> Path:
    raw = str(signals_dir or "").strip()
    if not raw:
        raise SignalTapeError("--signals-dir is required")
    path = Path(os.path.expandvars(os.path.expanduser(raw)))
    if not path.is_dir():
        raise SignalTapeError(f"precomputed signals dir not found: {path}")
    return path


def _require_tape_file(path: Path, label: str) -> None:
    if not path.is_file():
        raise SignalTapeError(f"precomputed signal tape missing {label}: {path}")


def _validate_manifest_file_contract(signals_path: Path, manifest: Mapping[str, Any], *, strict: bool) -> None:
    for filename in REQUIRED_TAPE_FILES:
        _require_tape_file(signals_path / filename, filename)

    if not strict:
        return

    files = dict(manifest.get("files") or {})
    for file_key, file_meta_raw in files.items():
        file_meta = dict(file_meta_raw or {})
        name = str(file_meta.get("name") or "").strip()
        expected_sha = str(file_meta.get("sha256") or "").strip()
        if not name or "/" in name or "\\" in name:
            raise SignalTapeError(f"manifest files.{file_key}.name must be a file name")
        path = signals_path / name
        _require_tape_file(path, f"manifest files.{file_key}")
        if expected_sha and sha256_file(path) != expected_sha:
            raise SignalTapeError(f"manifest hash mismatch for {name}")


def recompute_trade(row: Mapping[str, Any]) -> dict[str, float]:
    validate_no_secret_payload(row)
    side = str(row.get("side", "long") or "long").lower()
    if side != "long":
        raise SignalTapeError(f"fast signal MVP supports long rows only: {side!r}")
    entry_exec = _to_float(row.get("entry_exec"))
    exit_exec = _to_float(row.get("exit_exec"))
    qty = _to_float(row.get("qty"))
    fee_rate = _to_float(row.get("fee_rate"))
    entry_fee_rate = _to_float(row.get("entry_fee_rate"), fee_rate)
    exit_fee_rate = _to_float(row.get("exit_fee_rate"), fee_rate)
    gross_pnl = (exit_exec - entry_exec) * qty
    fee = entry_exec * qty * entry_fee_rate + exit_exec * qty * exit_fee_rate
    net = gross_pnl - fee
    return {
        "gross_pnl": gross_pnl,
        "fee": fee,
        "net": net,
    }


def run_fast_backtest(
    signals_dir: str | Path,
    *,
    initial_equity: float,
    expected_product: str | None = DEFAULT_PRODUCT,
    expected_symbol: str | None = None,
    expected_entry_tf: str | None = None,
    expected_filter_tf: str | None = None,
    strict: bool = False,
) -> dict[str, Any]:
    signals_path = _signals_dir_path(signals_dir)
    manifest = validate_manifest(
        load_manifest(signals_path, expected_product=expected_product),
        expected_product=expected_product,
        expected_symbol=expected_symbol,
        expected_entry_tf=expected_entry_tf,
        expected_filter_tf=expected_filter_tf,
    )
    _validate_manifest_file_contract(signals_path, manifest, strict=bool(strict))
    source_summary = load_summary(signals_path, require_safety_flags=True)
    rows = read_trades_csv(signals_path)
    equity = float(initial_equity)
    peak = equity
    equity_curve: list[dict[str, Any]] = [{
        "trade_id": "initial",
        "ts_ms": 0,
        "equity": equity,
        "peak": peak,
        "dd": 0.0,
        "net": 0.0,
    }]
    gross_total = 0.0
    fee_total = 0.0
    net_total = 0.0
    source_net_total = 0.0
    win_count = 0
    loss_count = 0

    for row in rows:
        calc = recompute_trade(row)
        gross_total += calc["gross_pnl"]
        fee_total += calc["fee"]
        net_total += calc["net"]
        source_net_total += _to_float(row.get("net"))
        equity += calc["net"]
        peak = max(peak, equity)
        if calc["net"] > 0.0:
            win_count += 1
        elif calc["net"] < 0.0:
            loss_count += 1
        equity_curve.append({
            "trade_id": row.get("trade_id", ""),
            "ts_ms": _to_int(row.get("exit_exec_ts_ms")),
            "equity": equity,
            "peak": peak,
            "dd": equity - peak,
            "net": calc["net"],
        })

    dd_metrics = compute_drawdown_metrics(equity_curve)
    summary = attach_drawdown_display_fields({
        "schema_version": manifest["schema_version"],
        "signal_set_id": manifest["signal_set_id"],
        "trade_count": len(rows),
        "initial_equity": float(initial_equity),
        "final_equity": equity,
        "gross_pnl_total": gross_total,
        "fee_total": fee_total,
        "net_total": net_total,
        "source_net_total": source_net_total,
        **dd_metrics,
        "max_dd": dd_metrics["max_dd_signed"],
        "win_count": win_count,
        "loss_count": loss_count,
        "research_only": True,
        "paper_live_order_execution": False,
    })
    return {
        "manifest": manifest,
        "source_summary": source_summary,
        "trades": rows,
        "summary": summary,
        "equity_curve": equity_curve,
    }


def verify_source_parity(
    signals_dir: str | Path,
    *,
    initial_equity: float,
    tolerance: float = 1e-6,
) -> dict[str, Any]:
    result = run_fast_backtest(signals_dir, initial_equity=initial_equity)
    rows = read_trades_csv(signals_dir)
    source_net = sum(_to_float(row.get("net")) for row in rows)
    source_final = _to_float(rows[-1].get("source_equity_after"), initial_equity) if rows else float(initial_equity)
    fast_net = float(result["summary"]["net_total"])
    fast_final = float(result["summary"]["final_equity"])
    return {
        "net_matches": abs(fast_net - source_net) <= tolerance,
        "final_equity_matches": abs(fast_final - source_final) <= tolerance,
        "fast_net_total": fast_net,
        "source_net_total": source_net,
        "fast_final_equity": fast_final,
        "source_final_equity": source_final,
        "tolerance": float(tolerance),
    }


def write_equity_curve_csv(path: str | Path, rows: Iterable[Mapping[str, Any]]) -> None:
    with Path(path).open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["trade_id", "ts_ms", "equity", "peak", "dd", "net"])
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))


def write_fast_backtest_artifacts(
    export_dir: str | Path,
    result: Mapping[str, Any],
    *,
    signals_dir: str | Path,
    write_report: bool = False,
) -> dict[str, str]:
    out_dir = Path(export_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    equity_path = out_dir / "equity_curve.csv"
    trades_path = out_dir / "trades.csv"
    write_equity_curve_csv(equity_path, result.get("equity_curve", []))

    source_trades = _signals_dir_path(signals_dir) / "trades.csv"
    if source_trades.resolve() != trades_path.resolve():
        shutil.copyfile(source_trades, trades_path)

    paths = {
        "equity_curve_csv": str(equity_path),
        "trades_csv": str(trades_path),
    }
    if write_report:
        summary_path = out_dir / "fast_summary.json"
        summary_path.write_text(
            json.dumps(dict(result.get("summary") or {}), ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        paths["fast_summary_json"] = str(summary_path)
    return paths


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fast research accounting from a precomputed signal tape.")
    parser.add_argument("--signals-dir", required=True, help="Directory containing manifest.json, trades.csv, and summary.json.")
    parser.add_argument("--initial-equity", type=float, default=300000.0)
    parser.add_argument("--strict", action="store_true", help="Verify manifest file hashes before accounting.")
    parser.add_argument("--write-report", action="store_true", help="Write fast_summary.json and fast_equity_curve.csv.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    result = run_fast_backtest(args.signals_dir, initial_equity=float(args.initial_equity), strict=bool(args.strict))
    if args.write_report:
        out_dir = Path(args.signals_dir)
        (out_dir / "fast_summary.json").write_text(
            json.dumps(result["summary"], ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        write_equity_curve_csv(out_dir / "fast_equity_curve.csv", result["equity_curve"])
    print(json.dumps(result["summary"], ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
