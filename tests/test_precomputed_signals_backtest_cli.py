# BUILD_ID: 2026-05-08_free_precomputed_backtest_fast_path_v1
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

import backtest
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
    signal_set_id = "sig_cli_phase2"
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


def _json_line(output: str) -> dict[str, Any]:
    for line in reversed(output.splitlines()):
        text = line.strip()
        if text.startswith("{") and text.endswith("}"):
            return json.loads(text)
    raise AssertionError(f"JSON line not found in output: {output!r}")


def _minimal_result(kwargs: dict[str, Any]) -> dict[str, Any]:
    initial = float(kwargs.get("initial_equity", 1000.0) or 1000.0)
    return {
        "symbols": list(kwargs.get("symbols") or ["BTC/USDT"]),
        "since_ms": int(kwargs.get("since_ms") or 0),
        "since_ms_effective": int(kwargs.get("since_ms") or 0),
        "since_iso_effective": "",
        "entry_tf": str(kwargs.get("entry_tf") or "5m"),
        "filter_tf": str(kwargs.get("filter_tf") or "1h"),
        "warmup_bars": int(kwargs.get("warmup_bars") or 0),
        "spread_bps_est": 0.0,
        "recent_bars_entry": kwargs.get("recent_bars_entry"),
        "recent_bars_filter": kwargs.get("recent_bars_filter"),
        "trades": 0,
        "buy_signals": 0,
        "entries_opened": 0,
        "forced_closes": 0,
        "open_positions_end": 0,
        "win": 0,
        "lose": 0,
        "final_equity": initial,
        "max_dd": 0.0,
        "max_dd_mtm": 0.0,
        "net_total": 0.0,
        "net_avg": 0.0,
        "export_dir": "",
    }


def _patch_fast_exports(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    export_dir = tmp_path / "exports"
    monkeypatch.setattr(backtest, "_activate_export_context", lambda **_kwargs: str(export_dir))
    monkeypatch.setattr(backtest, "_write_last_run_reference", lambda **_kwargs: "")
    return export_dir


def _run_fast_cli(monkeypatch: pytest.MonkeyPatch, tape_dir: Path) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backtest.py",
            "--use-precomputed-signals",
            "--precomputed-signals-dir",
            str(tape_dir),
            "--precomputed-signals-write-report",
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


def test_backtest_without_flag_uses_existing_path(monkeypatch: pytest.MonkeyPatch) -> None:
    called: dict[str, Any] = {}

    def fake_run_backtest(**kwargs: Any) -> dict[str, Any]:
        called.update(kwargs)
        return _minimal_result(kwargs)

    monkeypatch.setattr(backtest, "run_backtest", fake_run_backtest)
    monkeypatch.setattr(
        sys,
        "argv",
        ["backtest.py", "--symbols", "BTC/USDT", "--entry-tf", "5m", "--filter-tf", "1h", "--initial", "1000", "--no-csv"],
    )

    assert backtest.main() == 0
    assert called["symbols"] == ["BTC/USDT"]


def test_backtest_use_precomputed_signals_calls_fast_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    tape_dir = _write_tape(tmp_path)
    _patch_fast_exports(monkeypatch, tmp_path)
    monkeypatch.setattr(backtest, "run_backtest", lambda **_kwargs: pytest.fail("regular backtest path must not run"))
    _run_fast_cli(monkeypatch, tape_dir)

    assert backtest.main() == 0
    payload = _json_line(capsys.readouterr().out)
    summary = payload["summary"]
    assert payload["engine"] == "precomputed_signals"
    assert summary["net_total"] == pytest.approx(14.873)
    assert summary["final_equity"] == pytest.approx(1014.873)
    assert summary["dd_schema_version"] == tape.DD_SCHEMA_VERSION
    assert summary["dd_sign_convention"] == tape.DD_SIGN_CONVENTION
    assert summary["max_drawdown"] == pytest.approx(summary["max_dd_signed"])
    assert summary["max_dd_abs"] == pytest.approx(abs(summary["max_dd_signed"]))
    assert summary["max_dd_pct"] >= 0.0
    assert summary["max_dd_display_abs"] == pytest.approx(summary["max_dd_abs"])
    assert summary["max_dd_display_pct"] == pytest.approx(summary["max_dd_pct"])
    assert summary["max_drawdown_legacy_note"]
    assert Path(payload["exports"]["equity_curve_csv"]).is_file()
    assert Path(payload["exports"]["trades_csv"]).is_file()
    assert Path(payload["exports"]["fast_summary_json"]).is_file()


def test_backtest_use_precomputed_without_dir_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(backtest, "run_backtest", lambda **_kwargs: pytest.fail("regular backtest path must not run"))
    monkeypatch.setattr(sys, "argv", ["backtest.py", "--use-precomputed-signals", "--symbols", "BTC/USDT"])
    assert backtest.main() != 0


@pytest.mark.parametrize("missing_file", ["manifest.json", "trades.csv"])
def test_backtest_missing_required_tape_file_fails_closed(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, missing_file: str) -> None:
    tape_dir = _write_tape(tmp_path)
    (tape_dir / missing_file).unlink()
    _patch_fast_exports(monkeypatch, tmp_path)
    monkeypatch.setattr(backtest, "run_backtest", lambda **_kwargs: pytest.fail("regular backtest path must not run"))
    _run_fast_cli(monkeypatch, tape_dir)
    assert backtest.main() != 0


def test_backtest_unsafe_manifest_fails_closed(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    tape_dir = _write_tape(tmp_path)
    manifest_path = tape_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["safety_scope"]["paper_live_order_execution"] = True
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    _patch_fast_exports(monkeypatch, tmp_path)
    monkeypatch.setattr(backtest, "run_backtest", lambda **_kwargs: pytest.fail("regular backtest path must not run"))
    _run_fast_cli(monkeypatch, tape_dir)
    assert backtest.main() != 0


def test_backtest_non_free_product_fails_closed(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    tape_dir = _write_tape(tmp_path)
    manifest_path = tape_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["product"] = "standard"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    _patch_fast_exports(monkeypatch, tmp_path)
    monkeypatch.setattr(backtest, "run_backtest", lambda **_kwargs: pytest.fail("regular backtest path must not run"))
    _run_fast_cli(monkeypatch, tape_dir)
    assert backtest.main() != 0


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("research_only", False),
        ("paper_live_order_execution", True),
    ],
)
def test_backtest_unsafe_summary_safety_flags_fail_closed(
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
    _patch_fast_exports(monkeypatch, tmp_path)
    monkeypatch.setattr(backtest, "run_backtest", lambda **_kwargs: pytest.fail("regular backtest path must not run"))
    _run_fast_cli(monkeypatch, tape_dir)
    assert backtest.main() != 0


@pytest.mark.parametrize("target", ["manifest", "summary", "trades"])
def test_backtest_forbidden_fields_fail_closed(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, target: str) -> None:
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

    _patch_fast_exports(monkeypatch, tmp_path)
    monkeypatch.setattr(backtest, "run_backtest", lambda **_kwargs: pytest.fail("regular backtest path must not run"))
    _run_fast_cli(monkeypatch, tape_dir)
    assert backtest.main() != 0


def test_backtest_positive_legacy_max_drawdown_fails_closed(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    tape_dir = _write_tape(tmp_path)
    unsafe_summary = {
        "research_only": True,
        "paper_live_order_execution": False,
        "max_drawdown": 1.0,
    }
    (tape_dir / "summary.json").write_text(json.dumps(unsafe_summary, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    _patch_fast_exports(monkeypatch, tmp_path)
    monkeypatch.setattr(backtest, "run_backtest", lambda **_kwargs: pytest.fail("regular backtest path must not run"))
    _run_fast_cli(monkeypatch, tape_dir)
    assert backtest.main() != 0


def test_backtest_fast_path_does_not_touch_exchange_or_order_runtime(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    tape_dir = _write_tape(tmp_path)
    _patch_fast_exports(monkeypatch, tmp_path)
    monkeypatch.setattr(backtest, "run_backtest", lambda **_kwargs: pytest.fail("regular backtest path must not run"))

    class ForbiddenExchangeClient:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            raise AssertionError("MEXC API / balance / order runtime must not be touched")

    monkeypatch.setattr(backtest, "ExchangeClient", ForbiddenExchangeClient)
    _run_fast_cli(monkeypatch, tape_dir)
    assert backtest.main() == 0


def test_fixture_contains_only_small_synthetic_tape_files(tmp_path: Path) -> None:
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
