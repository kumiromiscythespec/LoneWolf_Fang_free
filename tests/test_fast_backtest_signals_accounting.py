# BUILD_ID: 2026-05-08_free_precomputed_signals_foundation_v1
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import fast_backtest_signals as fast
import signal_tape as tape


def _source_rows() -> list[dict[str, Any]]:
    return [
        {
            "symbol": "BTC/USDT",
            "direction": "long",
            "entry_ts_ms": 1000,
            "close_ts_ms": 2000,
            "entry_exec": 100.0,
            "exit_exec": 110.0,
            "qty": 2.0,
            "fee": 0.084,
            "pnl": 20.0,
            "net": 19.916,
            "equity_after": 1019.916,
            "reason": "TP_HIT",
        },
        {
            "symbol": "BTC/USDT",
            "direction": "long",
            "entry_ts_ms": 3000,
            "close_ts_ms": 4000,
            "entry_exec": 110.0,
            "exit_exec": 105.0,
            "qty": 1.0,
            "fee": 0.043,
            "pnl": -5.0,
            "net": -5.043,
            "equity_after": 1014.873,
            "reason": "STOP_HIT_LOSS",
        },
    ]


def _write_tape(tmp_path: Path) -> Path:
    signal_set_id = "sig_accounting"
    out_dir = tape.signal_tape_dir(
        product=tape.DEFAULT_PRODUCT,
        symbol="BTC/USDT",
        entry_tf="5m",
        filter_tf="1h",
        signal_set_id=signal_set_id,
        root=tmp_path,
    )
    out_dir.mkdir(parents=True)
    trades = tape.canonicalize_trade_rows(_source_rows(), symbol="BTC/USDT", initial_equity=1000.0)

    trades_path = out_dir / "trades.csv"
    with trades_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=tape.TRADE_COLUMNS)
        writer.writeheader()
        for row in trades:
            writer.writerow({key: row.get(key, "") for key in tape.TRADE_COLUMNS})

    summary = tape.build_summary(trades, initial_equity=1000.0)
    summary_path = out_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    manifest = tape.build_manifest_template(
        product=tape.DEFAULT_PRODUCT,
        producer_script="deferred_phase2",
        producer_build_id=tape.BUILD_ID,
        symbol="BTC/USDT",
        entry_tf="5m",
        filter_tf="1h",
        since_ms=1000,
        until_ms=5000,
        dataset_id="synthetic_unit",
        dataset_files_hash=tape.stable_json_hash({"dataset": "synthetic_unit"}),
        strategy_file_hash=tape.stable_json_hash({"strategy": "read_only_reference"}),
        strategy_build_id="read_only_reference",
        config_file_hash=tape.stable_json_hash({"config": "read_only_reference"}),
        signal_config_hash=tape.stable_json_hash({"signal": "synthetic_unit"}),
        accounting_config_hash=tape.stable_json_hash({"accounting": "synthetic_unit"}),
        signal_set_id=signal_set_id,
    )
    manifest["files"] = {
        "trades_csv": {"name": "trades.csv", "sha256": tape.sha256_file(trades_path)},
        "summary_json": {"name": "summary.json", "sha256": tape.sha256_file(summary_path)},
    }
    manifest["counts"] = {"trades": len(trades), "closed_trades": len(trades)}
    manifest["parity"] = {"fast_recompute_checked": False}
    (out_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return out_dir


def test_fast_backtest_recomputes_net_equity_and_dd(tmp_path) -> None:
    out_dir = _write_tape(tmp_path)
    result = fast.run_fast_backtest(out_dir, initial_equity=1000.0)
    summary = result["summary"]

    assert summary["gross_pnl_total"] == pytest.approx(15.0)
    assert summary["fee_total"] == pytest.approx(0.127)
    assert summary["net_total"] == pytest.approx(14.873)
    assert summary["final_equity"] == pytest.approx(1014.873)
    assert summary["max_drawdown"] == pytest.approx(-5.043)
    assert summary["max_drawdown"] == pytest.approx(summary["max_dd_signed"])
    assert summary["max_dd_abs"] == pytest.approx(abs(summary["max_dd_signed"]))
    assert summary["max_dd_pct"] >= 0.0
    assert result["equity_curve"][-1]["equity"] == pytest.approx(1014.873)


def test_fast_backtest_source_parity_helper(tmp_path) -> None:
    out_dir = _write_tape(tmp_path)
    parity = fast.verify_source_parity(out_dir, initial_equity=1000.0)
    assert parity["net_matches"] is True
    assert parity["final_equity_matches"] is True
