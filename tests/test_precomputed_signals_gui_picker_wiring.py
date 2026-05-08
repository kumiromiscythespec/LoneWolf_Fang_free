# BUILD_ID: 2026-05-08_free_precomputed_gui_picker_wiring_v1
from __future__ import annotations

import ast
import csv
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = REPO_ROOT / "app"
for path in (REPO_ROOT, APP_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import precomputed_signals_gui_adapter as gui_adapter
import signal_tape as tape
from app.gui import precomputed_signal_picker as gui_picker

MAIN_WINDOW_PATH = REPO_ROOT / "app" / "app" / "gui" / "main_window.py"
PICKER_HELPER_PATH = REPO_ROOT / "app" / "app" / "gui" / "precomputed_signal_picker.py"


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
    ]


def _manifest(
    *,
    symbol: str = "BTC/USDT",
    entry_tf: str = "5m",
    filter_tf: str = "1h",
    signal_set_id: str = "sig_gui_picker_wiring_fixture",
) -> dict[str, Any]:
    return tape.build_manifest_template(
        product=tape.DEFAULT_PRODUCT,
        producer_script="precompute_signals.py",
        producer_build_id=tape.BUILD_ID,
        symbol=symbol,
        entry_tf=entry_tf,
        filter_tf=filter_tf,
        since_ms=1000,
        until_ms=4000,
        dataset_id=f"{tape.normalize_symbol(symbol)}_{entry_tf}_{filter_tf}_gui_picker_wiring_unit",
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


def _write_valid_tape(
    root: Path,
    *,
    symbol: str = "BTC/USDT",
    entry_tf: str = "5m",
    filter_tf: str = "1h",
    signal_set_id: str = "sig_gui_picker_wiring_fixture",
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


def _function_source(path: Path, name: str) -> str:
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return ast.get_source_segment(source, node) or ""
    raise AssertionError(f"function not found: {name}")


def _app_version_from(text: str) -> str:
    match = re.search(r'^APP_VERSION\s*=\s*["\']([^"\']+)["\']', text, flags=re.MULTILINE)
    assert match is not None
    return match.group(1)


def test_gui_picker_wiring_uses_adapter_safe_picker_item(tmp_path: Path) -> None:
    tape_dir = _write_valid_tape(tmp_path)

    state = gui_picker.build_precomputed_signal_picker_state(tape_dir)
    expected = gui_adapter.build_gui_picker_item_from_signal_dir(tape_dir)

    assert state["picker_item"] == expected
    assert state["signal_dir"] == str(tape_dir)
    assert state["status"] == "valid"


def test_gui_picker_display_text_includes_dd_and_live_paper_warning(tmp_path: Path) -> None:
    state = gui_picker.build_precomputed_signal_picker_state(_write_valid_tape(tmp_path))
    text = state["display_text"]

    assert "Precomputed Signal Tape" in text
    assert "Signal dir:" in text
    assert "Symbol: BTC/USDT" in text
    assert "Timeframe: 5m / 1h" in text
    assert "Trades: 2" in text
    assert "Net total:" in text
    assert "Final equity:" in text
    assert "Max DD (abs, display)" in text
    assert "Max DD pct (display)" in text
    assert "Replay/backtest only" in text
    assert "Not selectable for LIVE/PAPER" in text


def test_gui_picker_selected_item_keeps_backtest_replay_only_flags(tmp_path: Path) -> None:
    item = gui_picker.build_precomputed_signal_picker_state(_write_valid_tape(tmp_path))["picker_item"]

    assert item["selectable_for_backtest_fast_path"] is True
    assert item["selectable_for_runner_replay_fast_path"] is True
    assert item["not_selectable_for_live"] is True
    assert item["not_selectable_for_paper"] is True
    assert "Not selectable for live: true" in gui_picker.format_precomputed_signal_picker_display_text(item)
    assert "Not selectable for paper: true" in gui_picker.format_precomputed_signal_picker_display_text(item)


def test_gui_picker_dd_display_prefers_display_abs_pct_and_keeps_signed_legacy(tmp_path: Path) -> None:
    item = gui_picker.build_precomputed_signal_picker_state(_write_valid_tape(tmp_path))["picker_item"]

    assert item["max_drawdown"] == pytest.approx(item["max_dd_signed"])
    assert item["max_drawdown"] <= 0.0
    assert item["max_dd_display_abs"] == pytest.approx(item["max_dd_abs"])
    assert item["max_dd_display_pct"] == pytest.approx(item["max_dd_pct"])
    assert item["dd_display_text"].count("Legacy signed max_drawdown") == 1


def test_gui_picker_positive_legacy_max_drawdown_only_is_not_display_dd(tmp_path: Path) -> None:
    tape_dir = _write_valid_tape(tmp_path, signal_set_id="sig_positive_legacy_gui")
    summary = json.loads((tape_dir / "summary.json").read_text(encoding="utf-8"))
    summary["max_drawdown"] = 100.0
    for key in ("max_dd_signed", "max_dd_abs", "max_dd_pct", "max_dd_display_abs", "max_dd_display_pct"):
        summary.pop(key, None)
    _write_json(tape_dir / "summary.json", summary)

    state = gui_picker.build_precomputed_signal_picker_state(tape_dir)

    assert state["picker_item"]["status"] == "invalid"
    assert "Max DD (abs, display): 100" not in state["display_text"]
    assert "100.0000" not in state["display_text"]


def test_gui_picker_output_excludes_raw_rows_private_runtime_and_billing(tmp_path: Path) -> None:
    state = gui_picker.build_precomputed_signal_picker_state(_write_valid_tape(tmp_path))
    output = json.dumps(state["picker_item"], ensure_ascii=True, sort_keys=True).lower() + "\n" + state["display_text"].lower()

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
        assert forbidden not in output


def test_gui_picker_helper_default_root_and_env_override() -> None:
    default_root = gui_picker.default_precomputed_signal_picker_root(
        env={"LOCALAPPDATA": r"C:\Users\unit\AppData\Local"}
    )
    override_root = gui_picker.default_precomputed_signal_picker_root(
        env={
            "LOCALAPPDATA": r"C:\Users\unit\AppData\Local",
            "LWF_PRECOMPUTED_SIGNALS_ROOT": r"D:\signals_override",
        }
    )

    assert default_root.endswith(r"LoneWolfFang\data\precomputed_signals")
    assert r"C:\Users\unit\AppData\Local" in default_root
    assert override_root == r"D:\signals_override"


def test_gui_source_wires_display_only_picker_without_execution_connections() -> None:
    main_source = MAIN_WINDOW_PATH.read_text(encoding="utf-8")
    helper_source = PICKER_HELPER_PATH.read_text(encoding="utf-8")

    assert "from app.gui.precomputed_signal_picker import" in main_source
    assert "build_precomputed_signal_picker_state" in main_source
    assert "precomputed_signals_gui_adapter" in helper_source
    assert "build_gui_picker_item_from_signal_dir" in helper_source

    helper_tree = ast.parse(helper_source)
    imports: set[str] = set()
    for node in ast.walk(helper_tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module.split(".", 1)[0])
    assert {
        "precompute_signals",
        "precomputed_signals_inventory",
        "runner",
        "backtest",
        "subprocess",
        "ccxt",
        "exchange",
        "risk",
        "strategy",
        "indicators",
    }.isdisjoint(imports)

    for method_name in ("on_select_precomputed_signal_tape", "_apply_precomputed_signal_tape_selection"):
        method_source = _function_source(MAIN_WINDOW_PATH, method_name)
        for forbidden in (
            "launch_backtest",
            "launch_runner",
            "launch_replay",
            "on_run_pipeline",
            "subprocess.Popen",
            "fetch_balance",
            "fetch_order",
            "create_order",
            "submit_order",
            "MEXC_API",
        ):
            assert forbidden not in method_source


def test_gui_run_paths_do_not_consume_selected_precomputed_signal_tape() -> None:
    for method_name in ("on_start", "on_run_replay", "on_run_backtest", "on_run_pipeline"):
        method_source = _function_source(MAIN_WINDOW_PATH, method_name)
        assert "precomputed_signal" not in method_source
        assert "--use-precomputed-signals" not in method_source
        assert "build_precomputed_signal_picker_state" not in method_source


def test_app_version_unchanged_and_package_artifacts_not_staged() -> None:
    current_config = (REPO_ROOT / "config.py").read_text(encoding="utf-8")
    head_config = subprocess.run(
        ["git", "show", "HEAD:config.py"],
        cwd=REPO_ROOT,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout
    assert _app_version_from(current_config) == _app_version_from(head_config)

    staged = subprocess.run(
        ["git", "diff", "--cached", "--name-only"],
        cwd=REPO_ROOT,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout.splitlines()
    forbidden_suffixes = (".zip", ".exe", ".msi", ".apk")
    forbidden_names = ("setup", "installer", "package")
    for name in staged:
        lowered = name.lower()
        assert not lowered.endswith(forbidden_suffixes)
        assert not any(token in lowered for token in forbidden_names)
