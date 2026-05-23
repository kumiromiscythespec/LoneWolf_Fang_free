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
    signal_set_id = "sig_dd_schema"
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


def assert_dd_v2(summary: dict[str, Any]) -> None:
    assert summary["dd_schema_version"] == tape.DD_SCHEMA_VERSION
    assert summary["dd_sign_convention"] == tape.DD_SIGN_CONVENTION
    assert summary["max_drawdown"] == pytest.approx(summary["max_dd_signed"])
    assert summary["max_drawdown"] <= 0.0
    assert summary["max_dd_signed"] <= 0.0
    assert summary["max_dd_abs"] == pytest.approx(abs(summary["max_dd_signed"]))
    assert summary["max_dd_pct"] >= 0.0
    assert summary["max_dd_display_abs"] == pytest.approx(summary["max_dd_abs"])
    assert summary["max_dd_display_pct"] == pytest.approx(summary["max_dd_pct"])
    assert summary["max_dd_display_label"] == "Max DD (abs, display)"
    assert summary["max_drawdown_legacy_note"]


def test_build_summary_emits_dd_v2_display_fields() -> None:
    trades = tape.canonicalize_trade_rows(_source_rows(), symbol="BTC/USDT", initial_equity=1000.0)
    summary = tape.build_summary(trades, initial_equity=1000.0)
    assert_dd_v2(summary)
    assert summary["net_total"] == pytest.approx(14.873)
    assert summary["final_equity"] == pytest.approx(1014.873)
    assert summary["max_drawdown"] == pytest.approx(-5.043)


def test_compute_drawdown_metrics_known_equity_curve() -> None:
    metrics = tape.compute_drawdown_metrics([
        (1000, 100.0),
        (2000, 120.0),
        (3000, 90.0),
        (4000, 130.0),
    ])
    displayed = tape.attach_drawdown_display_fields(metrics)
    assert displayed["max_drawdown"] == pytest.approx(-30.0)
    assert displayed["max_dd_signed"] == pytest.approx(-30.0)
    assert displayed["max_dd_abs"] == pytest.approx(30.0)
    assert displayed["max_dd_pct"] == pytest.approx(0.25)
    assert displayed["max_dd_peak_equity"] == pytest.approx(120.0)
    assert displayed["max_dd_trough_equity"] == pytest.approx(90.0)
    assert displayed["max_dd_ts_ms"] == 3000
    assert displayed["max_dd_ts_iso"] == "1970-01-01T00:00:03Z"
    assert_dd_v2(displayed)


def test_legacy_negative_max_drawdown_only_summary_normalizes() -> None:
    normalized = tape.normalize_drawdown_summary({"max_drawdown": -100.0})
    assert normalized["dd_schema_version"] == tape.DD_SCHEMA_VERSION_LEGACY_NORMALIZED
    assert normalized["dd_sign_convention"] == tape.DD_SIGN_CONVENTION
    assert normalized["max_drawdown"] == pytest.approx(-100.0)
    assert normalized["max_dd_signed"] == pytest.approx(-100.0)
    assert normalized["max_dd_abs"] == pytest.approx(100.0)
    assert normalized["max_dd_pct"] == 0.0


def test_positive_legacy_max_drawdown_fails_closed() -> None:
    with pytest.raises(tape.SignalTapeError):
        tape.normalize_drawdown_summary({"max_drawdown": 100.0})


def test_positive_new_summary_validation_fails_closed() -> None:
    summary = tape.compute_drawdown_metrics([(1000, 100.0), (2000, 90.0)])
    summary["max_drawdown"] = 10.0
    with pytest.raises(tape.SignalTapeError):
        tape.validate_drawdown_summary(summary)


def test_fast_backtest_write_report_emits_dd_v2_fields(tmp_path, capsys) -> None:
    tape_dir = _write_tape(tmp_path)
    assert fast.main(["--signals-dir", str(tape_dir), "--initial-equity", "1000", "--write-report"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert_dd_v2(payload)
    fast_summary = json.loads((tape_dir / "fast_summary.json").read_text(encoding="utf-8"))
    assert_dd_v2(fast_summary)
    assert fast_summary["net_total"] == pytest.approx(14.873)
    assert fast_summary["final_equity"] == pytest.approx(1014.873)


def test_fast_backtest_source_summary_legacy_is_normalized(tmp_path) -> None:
    tape_dir = _write_tape(tmp_path)
    legacy_summary = {
        "research_only": True,
        "paper_live_order_execution": False,
        "max_drawdown": -100.0,
    }
    (tape_dir / "summary.json").write_text(json.dumps(legacy_summary, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    result = fast.run_fast_backtest(tape_dir, initial_equity=1000.0)
    source_summary = result["source_summary"]
    assert source_summary["dd_schema_version"] == tape.DD_SCHEMA_VERSION_LEGACY_NORMALIZED
    assert source_summary["max_dd_signed"] == pytest.approx(-100.0)
    assert source_summary["max_dd_abs"] == pytest.approx(100.0)
