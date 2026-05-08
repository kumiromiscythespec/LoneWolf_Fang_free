# BUILD_ID: 2026-05-08_free_precomputed_runner_replay_fast_path_v1
from __future__ import annotations

import ast
import csv
import inspect
import json
import os
import sys
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import runner
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
    signal_set_id = "sig_runner_phase3"
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
        producer_script="deferred_phase3",
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


def _patch_replay_exports(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    export_dir = tmp_path / "replay_exports"
    monkeypatch.setattr(runner, "_activate_export_context", lambda **_kwargs: str(export_dir))
    monkeypatch.setattr(runner, "_write_last_run_reference", lambda **_kwargs: "")
    return export_dir


def _json_line(output: str) -> dict[str, Any]:
    for line in reversed(output.splitlines()):
        text = line.strip()
        if text.startswith("{") and text.endswith("}"):
            return json.loads(text)
    raise AssertionError(f"JSON line not found in output: {output!r}")


def _precomputed_args(tape_dir: Path, *extra: str) -> runner.argparse.Namespace:
    return runner._parse_runner_args(
        [
            "--mode",
            "replay",
            "--use-precomputed-signals",
            "--precomputed-signals-dir",
            str(tape_dir),
            "--symbol",
            "BTC/USDT",
            "--initial",
            "1000",
            *extra,
        ]
    )


def _raise_if_called(*_args: Any, **_kwargs: Any) -> None:
    raise AssertionError("live/paper/order path must not be touched by precomputed replay")


def test_runner_without_precomputed_flag_keeps_existing_replay_path(monkeypatch: pytest.MonkeyPatch) -> None:
    args = runner._parse_runner_args(["--replay", "--symbol", "BTC/USDT"])
    monkeypatch.setattr(runner, "_run_precomputed_signals_replay", _raise_if_called)

    runner._enforce_precomputed_signals_cli_scope(args)

    assert args.replay is True
    assert args.use_precomputed_signals is False
    assert "use_precomputed_signals" in inspect.getsource(runner._run_replay)


def test_runner_mode_replay_precomputed_calls_fast_path_and_exports(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    tape_dir = _write_tape(tmp_path)
    export_dir = _patch_replay_exports(monkeypatch, tmp_path)
    monkeypatch.setattr(runner, "ExchangeClient", _raise_if_called)
    monkeypatch.setattr(runner, "StateStore", _raise_if_called)
    monkeypatch.setattr(runner, "check_and_update_emergency_stop", _raise_if_called)
    args = _precomputed_args(tape_dir, "--precomputed-signals-write-report")

    assert args.replay is True
    assert runner._run_precomputed_signals_replay(args) == 0

    payload = _json_line(capsys.readouterr().out)
    summary = payload["summary"]
    assert payload["engine"] == "precomputed_signals"
    assert summary["net_total"] == pytest.approx(14.873)
    assert summary["final_equity"] == pytest.approx(1014.873)
    assert summary["dd_schema_version"] == tape.DD_SCHEMA_VERSION
    assert summary["dd_sign_convention"] == tape.DD_SIGN_CONVENTION
    assert summary["max_drawdown"] == pytest.approx(summary["max_dd_signed"])
    assert summary["max_dd_abs"] == pytest.approx(abs(summary["max_dd_signed"]))
    assert summary["max_dd_abs"] >= 0.0
    assert summary["max_dd_pct"] >= 0.0
    assert summary["max_dd_display_abs"] == pytest.approx(summary["max_dd_abs"])
    assert summary["max_dd_display_pct"] == pytest.approx(summary["max_dd_pct"])
    assert summary["max_drawdown_legacy_note"]
    assert args._replay_report_overall["engine"] == "precomputed_signals"
    assert (export_dir / "equity_curve.csv").is_file()
    assert (export_dir / "trades.csv").is_file()
    assert (export_dir / "fast_summary.json").is_file()


def test_runner_precomputed_without_replay_fails_closed(tmp_path: Path) -> None:
    tape_dir = _write_tape(tmp_path)
    args = runner._parse_runner_args(["--use-precomputed-signals", "--precomputed-signals-dir", str(tape_dir)])
    with pytest.raises(SystemExit):
        runner._enforce_precomputed_signals_cli_scope(args)


@pytest.mark.parametrize("mode", ["live", "paper"])
def test_runner_live_and_paper_precomputed_fail_closed(tmp_path: Path, mode: str) -> None:
    tape_dir = _write_tape(tmp_path)
    try:
        args = runner._parse_runner_args(["--mode", mode, "--use-precomputed-signals", "--precomputed-signals-dir", str(tape_dir)])
    finally:
        os.environ.pop("LWF_MODE_OVERRIDE", None)
    with pytest.raises(SystemExit):
        runner._enforce_precomputed_signals_cli_scope(args)


def test_runner_precomputed_without_dir_fails_closed() -> None:
    args = runner._parse_runner_args(["--mode", "replay", "--use-precomputed-signals"])
    with pytest.raises(SystemExit):
        runner._enforce_precomputed_signals_cli_scope(args)


@pytest.mark.parametrize("missing_file", ["manifest.json", "trades.csv"])
def test_runner_missing_required_tape_file_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    missing_file: str,
) -> None:
    tape_dir = _write_tape(tmp_path)
    (tape_dir / missing_file).unlink()
    _patch_replay_exports(monkeypatch, tmp_path)

    assert runner._run_precomputed_signals_replay(_precomputed_args(tape_dir)) != 0


def test_runner_unsafe_manifest_fails_closed(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    tape_dir = _write_tape(tmp_path)
    manifest_path = tape_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["safety_scope"]["paper_live_order_execution"] = True
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    _patch_replay_exports(monkeypatch, tmp_path)

    assert runner._run_precomputed_signals_replay(_precomputed_args(tape_dir)) != 0


def test_runner_non_free_product_fails_closed(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    tape_dir = _write_tape(tmp_path)
    manifest_path = tape_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["product"] = "standard"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    _patch_replay_exports(monkeypatch, tmp_path)

    assert runner._run_precomputed_signals_replay(_precomputed_args(tape_dir)) != 0


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("research_only", False),
        ("paper_live_order_execution", True),
    ],
)
def test_runner_unsafe_summary_safety_flags_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    field: str,
    value: bool,
) -> None:
    tape_dir = _write_tape(tmp_path)
    summary_path = tape_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary[field] = value
    summary_path.write_text(json.dumps(summary, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    _patch_replay_exports(monkeypatch, tmp_path)

    assert runner._run_precomputed_signals_replay(_precomputed_args(tape_dir)) != 0


@pytest.mark.parametrize("target", ["manifest", "summary", "trades"])
def test_runner_forbidden_fields_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    target: str,
) -> None:
    tape_dir = _write_tape(tmp_path)
    if target == "manifest":
        path = tape_dir / "manifest.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["apiSecret"] = "not allowed"
        path.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    elif target == "summary":
        path = tape_dir / "summary.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["raw_order"] = "not allowed"
        path.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    else:
        path = tape_dir / "trades.csv"
        lines = path.read_text(encoding="utf-8").splitlines()
        lines[0] = "raw_order," + lines[0]
        lines[1:] = ["not allowed," + line for line in lines[1:]]
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    _patch_replay_exports(monkeypatch, tmp_path)

    assert runner._run_precomputed_signals_replay(_precomputed_args(tape_dir)) != 0


def test_runner_positive_legacy_max_drawdown_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    tape_dir = _write_tape(tmp_path)
    summary_path = tape_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["max_drawdown"] = 1.0
    summary_path.write_text(json.dumps(summary, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    _patch_replay_exports(monkeypatch, tmp_path)

    assert runner._run_precomputed_signals_replay(_precomputed_args(tape_dir)) != 0


def test_fast_backtest_signals_imports_no_runtime_modules() -> None:
    source = (REPO_ROOT / "fast_backtest_signals.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module.split(".", 1)[0])
    assert {"strategy", "indicators", "exchange", "ccxt", "runner"}.isdisjoint(imports)


def test_runner_precomputed_helper_has_no_order_balance_exchange_terms() -> None:
    source = inspect.getsource(runner._run_precomputed_signals_replay)
    for term in ("ExchangeClient", "create_order", "fetch_balance", "fetch_order", "ccxt", "submit", "StateStore"):
        assert term not in source


def test_runner_precomputed_fixture_contains_only_small_synthetic_tape(tmp_path: Path) -> None:
    tape_dir = _write_tape(tmp_path)
    assert {path.name for path in tape_dir.iterdir()} == {"manifest.json", "summary.json", "trades.csv"}
    combined = "\n".join(path.read_text(encoding="utf-8") for path in tape_dir.iterdir()).lower()
    assert "raw market" not in combined
    assert "ohlcv" not in combined
    assert "mexc_api_key" not in combined
    assert "mexc_api_secret" not in combined
    assert "raw_order" not in combined
    assert "balance_snapshot" not in combined


def test_app_version_unchanged() -> None:
    assert 'APP_VERSION = "1.1.3"' in (REPO_ROOT / "config.py").read_text(encoding="utf-8")
