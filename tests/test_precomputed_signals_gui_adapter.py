# BUILD_ID: 2026-05-08_free_precomputed_gui_selection_diagnostics_v1
# BUILD_ID: 2026-05-08_free_precomputed_gui_picker_wiring_v1
from __future__ import annotations

import ast
import csv
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import precomputed_signals_gui_adapter as gui_adapter
import precomputed_signals_selection as selection
import signal_tape as tape


def _source_rows(symbol: str = "BTC/USDT") -> list[dict[str, Any]]:
    return [
        {
            "symbol": symbol,
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
            "symbol": symbol,
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
        {
            "symbol": symbol,
            "direction": "long",
            "entry_ts_ms": 5000,
            "close_ts_ms": 6000,
            "entry_exec": 105.0,
            "exit_exec": 106.0,
            "qty": 1.0,
            "fee": 0.0211,
            "pnl": 1.0,
            "net": 0.9789,
            "equity_after": 1015.8519,
            "reason": "TIME_EXIT",
        },
    ]


def _manifest(
    *,
    symbol: str = "BTC/USDT",
    entry_tf: str = "5m",
    filter_tf: str = "1h",
    signal_set_id: str = "sig_gui_adapter_fixture",
) -> dict[str, Any]:
    return tape.build_manifest_template(
        product=tape.DEFAULT_PRODUCT,
        producer_script="precompute_signals.py",
        producer_build_id=tape.BUILD_ID,
        symbol=symbol,
        entry_tf=entry_tf,
        filter_tf=filter_tf,
        since_ms=1000,
        until_ms=6000,
        dataset_id=f"{tape.normalize_symbol(symbol)}_{entry_tf}_{filter_tf}_gui_adapter_unit",
        dataset_files_hash=tape.stable_json_hash({"dataset": "unit"}),
        strategy_file_hash=tape.stable_json_hash({"strategy": "read_only_reference"}),
        strategy_build_id="read_only_reference",
        config_file_hash=tape.stable_json_hash({"config": "read_only_reference"}),
        signal_config_hash=tape.stable_json_hash({"signal": "unit"}),
        accounting_config_hash=tape.stable_json_hash({"accounting": "unit"}),
        signal_set_id=signal_set_id,
    )


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_header_only_trades(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=tape.TRADE_COLUMNS)
        writer.writeheader()


def _write_valid_tape(
    root: Path,
    *,
    symbol: str = "BTC/USDT",
    entry_tf: str = "5m",
    filter_tf: str = "1h",
    signal_set_id: str = "sig_gui_adapter_fixture",
) -> Path:
    out_dir = tape.signal_tape_dir(
        product=tape.DEFAULT_PRODUCT,
        symbol=symbol,
        entry_tf=entry_tf,
        filter_tf=filter_tf,
        signal_set_id=signal_set_id,
        root=root,
    )
    trades = tape.canonicalize_trade_rows(_source_rows(symbol), symbol=symbol, initial_equity=1000.0)
    manifest = _manifest(symbol=symbol, entry_tf=entry_tf, filter_tf=filter_tf, signal_set_id=signal_set_id)
    manifest["initial_equity"] = 1000.0
    tape.write_signal_tape(out_dir, manifest=manifest, trades=trades)
    return out_dir


def _valid_selection(tmp_path: Path) -> dict[str, Any]:
    return selection.build_signal_tape_selection_contract(_write_valid_tape(tmp_path))


def _json_from_stdout(capsys: pytest.CaptureFixture[str]) -> dict[str, Any]:
    return json.loads(capsys.readouterr().out)


def test_gui_adapter_builds_picker_item_from_selection_contract(tmp_path: Path) -> None:
    contract = _valid_selection(tmp_path)
    item = gui_adapter.build_gui_picker_item(contract)

    assert item["schema_version"] == tape.SCHEMA_VERSION
    assert item["gui_adapter_schema_version"] == gui_adapter.GUI_ADAPTER_SCHEMA_VERSION
    assert item["product"] == "free"
    assert item["signal_dir"] == contract["signal_dir"]
    assert item["title"]
    assert item["subtitle"]
    assert item["symbol"] == "BTC/USDT"
    assert item["symbol_normalized"] == "BTCUSDT"
    assert item["entry_tf"] == "5m"
    assert item["filter_tf"] == "1h"
    assert item["signal_set_id"] == "sig_gui_adapter_fixture"
    assert item["selectable_for_backtest_fast_path"] is True
    assert item["selectable_for_runner_replay_fast_path"] is True
    assert item["not_selectable_for_live"] is True
    assert item["not_selectable_for_paper"] is True
    assert item["safety_research_only"] is True
    assert item["safety_paper_live_order_execution"] is False
    assert item["tape_files_present"] == {"manifest_json": True, "summary_json": True, "trades_csv": True}
    assert item["manifest_sha256"] == contract["manifest_sha256"]
    assert item["summary_sha256"] == contract["summary_sha256"]
    assert item["trades_csv_sha256_from_manifest"] == contract["trades_csv_sha256_from_manifest"]
    assert item["safe_error_code"] == ""
    assert item["status"] == "valid"
    assert item["status_reason"] == "ok"


def test_gui_adapter_builds_picker_item_from_signal_dir(tmp_path: Path) -> None:
    tape_dir = _write_valid_tape(tmp_path)

    item = gui_adapter.build_gui_picker_item_from_signal_dir(
        tape_dir,
        expected_symbol="BTC/USDT",
        expected_entry_tf="5m",
        expected_filter_tf="1h",
        expected_signal_set_id="sig_gui_adapter_fixture",
    )

    assert item["status"] == "valid"
    assert item["signal_dir"] == str(tape_dir)
    assert item["title"] == "BTC/USDT 5m/1h sig_gui_adapter_fixture"


def test_gui_adapter_dd_display_prefers_v2_display_fields(tmp_path: Path) -> None:
    contract = _valid_selection(tmp_path)
    item = gui_adapter.build_gui_picker_item(contract)

    assert item["dd_schema_version"] == tape.DD_SCHEMA_VERSION
    assert item["dd_sign_convention"] == tape.DD_SIGN_CONVENTION
    assert item["max_drawdown"] == pytest.approx(item["max_dd_signed"])
    assert item["max_drawdown"] <= 0.0
    assert item["max_dd_abs"] == pytest.approx(abs(item["max_dd_signed"]))
    assert item["max_dd_pct"] >= 0.0
    assert item["max_dd_display_abs"] == pytest.approx(item["max_dd_abs"])
    assert item["max_dd_display_pct"] == pytest.approx(item["max_dd_pct"])
    assert item["max_dd_display_label"] == "Max DD (abs, display)"
    assert item["max_drawdown_legacy_note"] == tape.MAX_DRAWDOWN_LEGACY_NOTE
    assert "Max DD (abs, display)" in item["dd_display_text"]
    assert "Max DD pct (display)" in item["dd_display_text"]
    assert "Legacy signed max_drawdown:" in item["dd_display_text"]
    assert "Legacy signed max_drawdown: 5." not in item["dd_display_text"]


def test_gui_adapter_format_drawdown_uses_display_fallbacks() -> None:
    text = gui_adapter.format_gui_drawdown_text({
        "max_drawdown": -1559.735,
        "max_dd_signed": -1559.735,
        "max_dd_abs": 1559.735,
        "max_dd_pct": 0.000244,
    })

    assert "Max DD (abs, display): 1,559.7350" in text
    assert "Max DD pct (display): 0.0244%" in text
    assert "Legacy signed max_drawdown: -1,559.7350" in text


def test_gui_adapter_positive_legacy_max_drawdown_only_fails_closed(tmp_path: Path) -> None:
    contract = _valid_selection(tmp_path)
    legacy_only = {
        "schema_version": contract["schema_version"],
        "selection_schema_version": contract["selection_schema_version"],
        "product": "free",
        "signal_dir": contract["signal_dir"],
        "status": "valid",
        "status_reason": "ok",
        "safety_research_only": True,
        "safety_paper_live_order_execution": False,
        "selectable_for_backtest_fast_path": True,
        "selectable_for_runner_replay_fast_path": True,
        "not_selectable_for_live": True,
        "not_selectable_for_paper": True,
        "max_drawdown": 100.0,
    }

    item = gui_adapter.build_gui_picker_item(legacy_only)

    assert item["status"] == "invalid"
    assert item["selectable_for_backtest_fast_path"] is False
    assert item["selectable_for_runner_replay_fast_path"] is False
    assert item["not_selectable_for_live"] is True
    assert item["not_selectable_for_paper"] is True


def test_gui_adapter_output_excludes_raw_rows_private_runtime_and_billing(tmp_path: Path) -> None:
    item = gui_adapter.build_gui_picker_item(_valid_selection(tmp_path))
    text = json.dumps(item, ensure_ascii=True, sort_keys=True).lower()

    for forbidden in (
        "trade_id",
        "entry_exec",
        "exit_exec",
        '"qty"',
        "api_key",
        "apikey",
        "secret",
        "token",
        "authorization",
        "raw_order",
        "balance",
        "raw_billing",
        "raw market data",
        "ohlcv",
    ):
        assert forbidden not in text


def test_gui_adapter_contaminated_selection_returns_safe_invalid_item(tmp_path: Path) -> None:
    contract = _valid_selection(tmp_path)
    contaminated = dict(contract)
    contaminated["raw_trades"] = [{"entry_exec": 100.0, "exit_exec": 110.0, "qty": 1.0, "trade_id": "unit"}]
    contaminated["api_key"] = "unit-redacted"

    item = gui_adapter.build_gui_picker_item(contaminated)
    text = json.dumps(item, ensure_ascii=True, sort_keys=True).lower()

    assert item["status"] == "invalid"
    assert item["selectable_for_backtest_fast_path"] is False
    assert item["selectable_for_runner_replay_fast_path"] is False
    for forbidden in ("raw_trades", "entry_exec", "exit_exec", '"qty"', "trade_id", "api_key"):
        assert forbidden not in text


def test_gui_adapter_cli_json_out_writes_safe_picker_item(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    tape_dir = _write_valid_tape(tmp_path)
    out_path = tmp_path / "out" / "gui_picker_item.json"

    assert gui_adapter.main(["--signal-dir", str(tape_dir), "--format", "json", "--out", str(out_path)]) == 0
    assert "status=valid" in capsys.readouterr().out
    payload = json.loads(out_path.read_text(encoding="utf-8"))

    assert payload["gui_adapter_schema_version"] == gui_adapter.GUI_ADAPTER_SCHEMA_VERSION
    assert payload["signal_dir"] == str(tape_dir)
    assert payload["selectable_for_backtest_fast_path"] is True
    assert payload["selectable_for_runner_replay_fast_path"] is True
    assert "entry_exec" not in out_path.read_text(encoding="utf-8")
    assert "raw_order" not in out_path.read_text(encoding="utf-8")


def test_gui_adapter_cli_text_outputs_safe_summary(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    tape_dir = _write_valid_tape(tmp_path)

    assert gui_adapter.main(["--signal-dir", str(tape_dir), "--format", "text"]) == 0
    text = capsys.readouterr().out

    assert "precomputed signal picker item product=free status=valid" in text
    assert str(tape_dir) in text
    assert "backtest_fast_path=true" in text
    assert "runner_replay_fast_path=true" in text
    assert "live_selectable=false" in text
    assert "paper_selectable=false" in text
    assert "Max DD (abs, display)" in text
    assert "entry_exec" not in text
    assert "raw_order" not in text


def test_gui_adapter_cli_without_signal_dir_fails_closed(capsys: pytest.CaptureFixture[str]) -> None:
    assert gui_adapter.main(["--format", "json"]) == 2
    payload = _json_from_stdout(capsys)

    assert payload["status"] == "invalid"
    assert payload["selectable_for_backtest_fast_path"] is False
    assert payload["selectable_for_runner_replay_fast_path"] is False
    assert payload["not_selectable_for_live"] is True
    assert payload["not_selectable_for_paper"] is True


def test_gui_adapter_module_import_boundary_and_app_version_unchanged() -> None:
    source = (REPO_ROOT / "precomputed_signals_gui_adapter.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module.split(".", 1)[0])

    forbidden_imports = {
        "strategy",
        "indicators",
        "exchange",
        "ccxt",
        "runner",
        "backtest",
        "risk",
        "precompute_signals",
        "precomputed_signals_inventory",
        "fast_backtest_signals",
    }
    assert forbidden_imports.isdisjoint(imports)

    lowered = source.lower()
    for term in ("mexc", "fetch_balance", "fetch_order", "create_order", "submit_order", "subprocess"):
        assert term not in lowered
    assert 'APP_VERSION = "1.1.3"' in (REPO_ROOT / "config.py").read_text(encoding="utf-8")


def test_gui_source_uses_adapter_through_display_only_picker_helper() -> None:
    main_source = (REPO_ROOT / "app" / "app" / "gui" / "main_window.py").read_text(encoding="utf-8")
    helper_source = (REPO_ROOT / "app" / "app" / "gui" / "precomputed_signal_picker.py").read_text(encoding="utf-8")

    assert "build_precomputed_signal_picker_state" in main_source
    assert "on_select_precomputed_signal_tape" in main_source
    assert "precomputed_signals_gui_adapter" in helper_source
    assert "build_gui_picker_item_from_signal_dir" in helper_source
    for forbidden in (
        "precomputed_signals_inventory",
        "precompute_signals",
        "launch_backtest",
        "launch_runner",
        "launch_replay",
        "fetch_balance",
        "fetch_order",
        "create_order",
        "submit_order",
    ):
        assert forbidden not in helper_source


def test_adapter_invalid_signal_dir_does_not_read_raw_trade_rows(tmp_path: Path) -> None:
    invalid_dir = tmp_path / "free" / "BTCUSDT" / "5m_1h" / "sig_missing_summary"
    _write_json(invalid_dir / "manifest.json", _manifest(signal_set_id="sig_missing_summary"))
    _write_header_only_trades(invalid_dir / "trades.csv")

    item = gui_adapter.build_gui_picker_item_from_signal_dir(invalid_dir)

    assert item["status"] == "invalid"
    assert item["status_reason"] == "missing_summary"
    assert "entry_exec" not in json.dumps(item, ensure_ascii=True, sort_keys=True)
