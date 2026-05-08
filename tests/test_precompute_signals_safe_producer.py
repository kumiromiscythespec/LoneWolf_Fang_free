# BUILD_ID: 2026-05-08_free_precomputed_safe_producer_v1
from __future__ import annotations

import ast
import csv
import json
import sys
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import backtest
import fast_backtest_signals as fast
import precompute_signals
import runner
import signal_tape as tape


def _source_trade_rows() -> list[dict[str, Any]]:
    return [
        {
            "symbol": "BTC/USDT",
            "direction": "long",
            "entry_ts_ms": 1000,
            "close_ts_ms": 2000,
            "entry_exec": 100.0,
            "exit_exec": 110.0,
            "qty": 2.0,
            "fee_rate": 0.0002,
            "fee": 0.084,
            "gross_pnl": 20.0,
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
            "fee_rate": 0.0002,
            "fee": 0.043,
            "gross_pnl": -5.0,
            "net": -5.043,
            "equity_after": 1014.873,
            "reason": "STOP_HIT_LOSS",
        },
    ]


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys()) if rows else ["symbol"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _write_source_trades(tmp_path: Path, *, rows: list[dict[str, Any]] | None = None) -> Path:
    return _write_csv(tmp_path / "source" / "trades.csv", rows or _source_trade_rows())


def _write_equity_curve(tmp_path: Path) -> Path:
    return _write_csv(
        tmp_path / "source" / "equity_curve.csv",
        [
            {"trade_id": "1", "ts_ms": 2000, "equity": 1019.916, "net": 19.916},
            {"trade_id": "2", "ts_ms": 4000, "equity": 1014.873, "net": -5.043},
        ],
    )


def _produce(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    *,
    trades_csv: Path | None = None,
    equity_csv: Path | None = None,
    extra: list[str] | None = None,
) -> Path:
    trades_path = trades_csv or _write_source_trades(tmp_path)
    equity_path = equity_csv or _write_equity_curve(tmp_path)
    out_root = tmp_path / "precomputed_root"
    args = [
        "--symbol",
        "BTC/USDT",
        "--entry-tf",
        "5m",
        "--filter-tf",
        "1h",
        "--trades-csv",
        str(trades_path),
        "--equity-csv",
        str(equity_path),
        "--since",
        "2025-01-01",
        "--until",
        "2025-12-31",
        "--dataset-id",
        "synthetic_or_existing",
        "--signal-set-id",
        "sig_safe_fixture",
        "--initial-equity",
        "1000",
        "--out-root",
        str(out_root),
        "--write-summary",
        "--strict",
    ]
    if extra:
        args.extend(extra)
    assert precompute_signals.main(args) == 0
    stdout = capsys.readouterr().out.strip().splitlines()
    return Path(stdout[-1])


def test_safe_producer_default_free_builds_bundle_and_paths(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    tape_dir = _produce(tmp_path, capsys)
    expected = tmp_path / "precomputed_root" / "free" / "BTCUSDT" / "5m_1h" / "sig_safe_fixture"
    assert tape_dir == expected
    assert {path.name for path in tape_dir.iterdir()} == {
        "manifest.json",
        "trades.csv",
        "trades.jsonl",
        "summary.json",
        "equity_reference.csv",
    }

    manifest = tape.load_manifest(tape_dir)
    assert manifest["product"] == "free"
    assert manifest["producer_script"] == "precompute_signals.py"
    assert manifest["producer_build_id"] == precompute_signals.BUILD_ID
    assert manifest["safety_scope"]["research_only"] is True
    assert manifest["safety_scope"]["paper_live_order_execution"] is False
    assert manifest["safety_scope"]["contains_api_key"] is False
    assert manifest["safety_scope"]["contains_secret"] is False
    assert manifest["safety_scope"]["contains_order_id"] is False
    assert manifest["dd_schema_version"] == tape.DD_SCHEMA_VERSION
    assert manifest["dd_sign_convention"] == tape.DD_SIGN_CONVENTION
    assert set(manifest["files"]) == {"trades_csv", "trades_jsonl", "summary_json", "equity_reference_csv"}

    summary = tape.load_summary(tape_dir)
    assert summary["dd_schema_version"] == tape.DD_SCHEMA_VERSION
    assert summary["max_drawdown"] == pytest.approx(summary["max_dd_signed"])
    assert summary["max_dd_abs"] >= 0.0
    assert summary["max_dd_display_abs"] == pytest.approx(summary["max_dd_abs"])
    assert summary["equity_reference_basis"] == "source_equity_csv"
    tape.validate_no_secret_payload(manifest)
    tape.validate_no_secret_payload(summary)
    tape.validate_no_secret_payload(tape.read_trades_csv(tape_dir))


def test_safe_producer_fallback_root_env_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    env_root = tmp_path / "env_root"
    monkeypatch.setenv("LWF_PRECOMPUTED_SIGNALS_ROOT", str(env_root))
    trades_csv = _write_source_trades(tmp_path)
    args = [
        "--symbol",
        "BTC/USDT",
        "--entry-tf",
        "5m",
        "--filter-tf",
        "1h",
        "--trades-csv",
        str(trades_csv),
        "--signal-set-id",
        "sig_env_fixture",
        "--initial-equity",
        "1000",
    ]
    assert precompute_signals.main(args) == 0
    tape_dir = Path(capsys.readouterr().out.strip())
    assert tape_dir == env_root / "free" / "BTCUSDT" / "5m_1h" / "sig_env_fixture"
    assert tape.load_summary(tape_dir)["equity_reference_basis"] == "synthetic_from_trades_net"


def test_safe_producer_fail_closed_product_missing_source_and_accounting(tmp_path: Path) -> None:
    trades_csv = _write_source_trades(tmp_path)
    base = ["--symbol", "BTC/USDT", "--entry-tf", "5m", "--filter-tf", "1h"]

    with pytest.raises(SystemExit):
        precompute_signals.main([*base, "--trades-csv", str(trades_csv), "--product", "standard"])
    with pytest.raises(SystemExit):
        precompute_signals.main(base)
    with pytest.raises(SystemExit):
        precompute_signals.main([*base, "--trades-csv", str(tmp_path / "missing.csv")])

    rows = _source_trade_rows()
    for row in rows:
        row.pop("fee_rate")
    missing_fee_rate = _write_source_trades(tmp_path / "missing_fee", rows=rows)
    with pytest.raises(SystemExit):
        precompute_signals.main([*base, "--trades-csv", str(missing_fee_rate)])


def test_safe_producer_fail_closed_forbidden_payloads_and_positive_legacy_dd(tmp_path: Path) -> None:
    base = ["--symbol", "BTC/USDT", "--entry-tf", "5m", "--filter-tf", "1h"]

    rows = _source_trade_rows()
    for row in rows:
        row["raw_order"] = "not allowed"
    forbidden_trades = _write_source_trades(tmp_path / "forbidden", rows=rows)
    with pytest.raises(SystemExit):
        precompute_signals.main([*base, "--trades-csv", str(forbidden_trades)])

    trades_csv = _write_source_trades(tmp_path)
    unsafe_summary = tmp_path / "source" / "unsafe_summary.json"
    unsafe_summary.write_text(json.dumps({"max_drawdown": -1.0, "raw_order": "not allowed"}) + "\n", encoding="utf-8")
    with pytest.raises(SystemExit):
        precompute_signals.main([*base, "--trades-csv", str(trades_csv), "--summary-json", str(unsafe_summary)])

    positive_dd = tmp_path / "source" / "positive_dd_summary.json"
    positive_dd.write_text(json.dumps({"max_drawdown": 1.0}) + "\n", encoding="utf-8")
    with pytest.raises(SystemExit):
        precompute_signals.main([*base, "--trades-csv", str(trades_csv), "--summary-json", str(positive_dd)])


def test_generated_tape_reads_through_fast_backtest_backtest_cli_and_runner(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tape_dir = _produce(tmp_path, capsys)

    fast_result = fast.run_fast_backtest(tape_dir, initial_equity=1000.0, expected_symbol="BTC/USDT", expected_entry_tf="5m", expected_filter_tf="1h")
    assert fast_result["summary"]["net_total"] == pytest.approx(14.873)
    assert fast_result["summary"]["final_equity"] == pytest.approx(1014.873)

    monkeypatch.setattr(backtest, "_activate_export_context", lambda **_kwargs: str(tmp_path / "backtest_exports"))
    monkeypatch.setattr(backtest, "_write_last_run_reference", lambda **_kwargs: "")
    monkeypatch.setattr(backtest, "run_backtest", lambda **_kwargs: pytest.fail("regular backtest path must not run"))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backtest.py",
            "--use-precomputed-signals",
            "--precomputed-signals-dir",
            str(tape_dir),
            "--symbols",
            "BTC/USDT",
            "--entry-tf",
            "5m",
            "--filter-tf",
            "1h",
            "--initial",
            "1000",
        ],
    )
    assert backtest.main() == 0

    def forbidden_runtime(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("live/paper/order runtime must not be touched")

    monkeypatch.setattr(runner, "_activate_export_context", lambda **_kwargs: str(tmp_path / "runner_exports"))
    monkeypatch.setattr(runner, "_write_last_run_reference", lambda **_kwargs: "")
    monkeypatch.setattr(runner, "ExchangeClient", forbidden_runtime)
    monkeypatch.setattr(runner, "StateStore", forbidden_runtime)
    monkeypatch.setattr(runner, "check_and_update_emergency_stop", forbidden_runtime)
    args = runner._parse_runner_args([
        "--mode",
        "replay",
        "--use-precomputed-signals",
        "--precomputed-signals-dir",
        str(tape_dir),
        "--symbol",
        "BTC/USDT",
        "--initial",
        "1000",
    ])
    assert runner._run_precomputed_signals_replay(args) == 0


def test_safe_producer_import_and_runtime_boundaries() -> None:
    source = (REPO_ROOT / "precompute_signals.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module.split(".", 1)[0])
    assert {"strategy", "indicators", "exchange", "ccxt", "runner", "backtest"}.isdisjoint(imports)
    lowered = source.lower()
    for term in ("mexc", "fetch_balance", "fetch_order", "create_order", "submit_order", "subprocess"):
        assert term not in lowered
    assert 'APP_VERSION = "1.1.3"' in (REPO_ROOT / "config.py").read_text(encoding="utf-8")
