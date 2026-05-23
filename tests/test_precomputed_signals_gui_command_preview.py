# BUILD_ID: 2026-05-08_free_precomputed_gui_selection_diagnostics_v1
from __future__ import annotations

import ast
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


def _valid_picker_item(signal_dir: Path | str) -> dict[str, Any]:
    return {
        "schema_version": tape.SCHEMA_VERSION,
        "gui_adapter_schema_version": gui_adapter.GUI_ADAPTER_SCHEMA_VERSION,
        "product": tape.DEFAULT_PRODUCT,
        "signal_dir": str(signal_dir),
        "title": "BTC/USDT 5m/1h sig_gui_command_preview_fixture",
        "subtitle": "free | trades=2 | Max DD (abs, display): 10.0000",
        "symbol": "BTC/USDT",
        "symbol_normalized": "BTCUSDT",
        "entry_tf": "5m",
        "filter_tf": "1h",
        "signal_set_id": "sig_gui_command_preview_fixture",
        "dataset_id": "BTCUSDT_5m_1h_gui_command_preview_unit",
        "created_at_utc": "2026-05-08T00:00:00Z",
        "since_ms": 1000,
        "until_ms": 4000,
        "trade_count": 2,
        "net_total": 12.5,
        "final_equity": 1012.5,
        "safety_research_only": True,
        "safety_paper_live_order_execution": False,
        "dd_schema_version": tape.DD_SCHEMA_VERSION,
        "dd_sign_convention": tape.DD_SIGN_CONVENTION,
        "max_drawdown": -10.0,
        "max_dd_signed": -10.0,
        "max_dd_abs": 10.0,
        "max_dd_pct": 0.01,
        "max_dd_display_abs": 10.0,
        "max_dd_display_pct": 0.01,
        "max_dd_display_label": gui_adapter.GUI_DD_DISPLAY_LABEL,
        "max_drawdown_legacy_note": tape.MAX_DRAWDOWN_LEGACY_NOTE,
        "dd_display_text": (
            "Max DD (abs, display): 10.0000 | "
            "Max DD pct (display): 1.0000% | "
            "Legacy signed max_drawdown: -10.0000"
        ),
        "net_total_text": "12.5000",
        "final_equity_text": "1,012.5000",
        "tape_files_present": {"manifest_json": True, "summary_json": True, "trades_csv": True},
        "manifest_sha256": "a" * 64,
        "summary_sha256": "b" * 64,
        "trades_csv_sha256_from_manifest": "c" * 64,
        "safe_error_code": "",
        "selectable_for_backtest_fast_path": True,
        "selectable_for_runner_replay_fast_path": True,
        "not_selectable_for_live": True,
        "not_selectable_for_paper": True,
        "picker_warning": "Read-only research selection; not available for LIVE/PAPER.",
        "status": "valid",
        "status_reason": "ok",
    }


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


def test_valid_picker_item_builds_backtest_and_runner_replay_command_preview(tmp_path: Path) -> None:
    signal_dir = tmp_path / "signal tape with spaces"
    item = _valid_picker_item(signal_dir)

    preview = gui_picker.build_precomputed_signal_command_preview(item)
    formatted = gui_picker.format_precomputed_signal_command_preview(preview)
    display_text = gui_picker.format_precomputed_signal_picker_display_text(item)

    assert preview["preview_only"] is True
    assert preview["execution_enabled"] is False
    assert preview["live_command_available"] is False
    assert preview["paper_command_available"] is False
    assert preview["signal_dir"] == str(signal_dir)
    assert preview["source_symbol"] == "BTC/USDT"
    assert preview["source_tf_pair"] == "5m/1h"

    assert preview["backtest_argv"] == [
        "python",
        "backtest.py",
        "--use-precomputed-signals",
        "--precomputed-signals-dir",
        str(signal_dir),
        "--precomputed-signals-write-report",
    ]
    assert "backtest.py" in preview["backtest_command_text"]
    assert "--use-precomputed-signals" in preview["backtest_command_text"]
    assert "--precomputed-signals-dir" in preview["backtest_command_text"]
    assert f'"{signal_dir}"' in preview["backtest_command_text"]

    assert preview["runner_replay_argv"] == [
        "python",
        "runner.py",
        "--mode",
        "replay",
        "--use-precomputed-signals",
        "--precomputed-signals-dir",
        str(signal_dir),
        "--precomputed-signals-write-report",
    ]
    assert "runner.py" in preview["runner_replay_command_text"]
    assert "--mode replay" in preview["runner_replay_command_text"]
    assert "--use-precomputed-signals" in preview["runner_replay_command_text"]
    assert "--precomputed-signals-dir" in preview["runner_replay_command_text"]
    assert f'"{signal_dir}"' in preview["runner_replay_command_text"]

    assert "Backtest / replay preview only" in formatted
    assert "Execution is disabled in this panel" in formatted
    assert "Not selectable for LIVE/PAPER" in formatted
    assert "Backtest command preview only" in display_text
    assert "Replay command preview only" in display_text
    assert preview["backtest_command_text"] in display_text
    assert preview["runner_replay_command_text"] in display_text


def test_invalid_picker_item_disables_command_preview(tmp_path: Path) -> None:
    item = _valid_picker_item(tmp_path / "invalid signal tape")
    item["status"] = "invalid"
    item["status_reason"] = "synthetic invalid selection"
    item["selectable_for_backtest_fast_path"] = False
    item["selectable_for_runner_replay_fast_path"] = False

    preview = gui_picker.build_precomputed_signal_command_preview(item)
    formatted = gui_picker.format_precomputed_signal_command_preview(preview)

    assert preview["preview_only"] is True
    assert preview["execution_enabled"] is False
    assert preview["live_command_available"] is False
    assert preview["paper_command_available"] is False
    assert preview["backtest_command_text"] == ""
    assert preview["runner_replay_command_text"] == ""
    assert preview["backtest_argv"] == []
    assert preview["runner_replay_argv"] == []
    assert "python backtest.py" not in formatted
    assert "python runner.py" not in formatted
    assert "Command preview disabled" in formatted


@pytest.mark.parametrize("field", ["not_selectable_for_live", "not_selectable_for_paper"])
def test_command_preview_requires_not_selectable_for_live_and_paper(tmp_path: Path, field: str) -> None:
    item = _valid_picker_item(tmp_path / f"{field} false signal tape")
    item[field] = False

    preview = gui_picker.build_precomputed_signal_command_preview(item)

    assert preview["preview_only"] is True
    assert preview["execution_enabled"] is False
    assert preview["live_command_available"] is False
    assert preview["paper_command_available"] is False
    assert preview["backtest_command_text"] == ""
    assert preview["runner_replay_command_text"] == ""


def test_command_preview_excludes_raw_rows_private_runtime_and_billing(tmp_path: Path) -> None:
    item = _valid_picker_item(tmp_path / "safe signal tape")
    item.update(
        {
            "entry_exec": 100.0,
            "exit_exec": 110.0,
            "qty": 2.0,
            "trade_id": "trade-123",
            "api_key": "unit-api-key",
            "secret": "unit-secret",
            "token": "unit-token",
            "authorization": "Bearer unit",
            "raw_order": {"id": "order-123"},
            "balance": {"USDT": 1000},
            "raw_billing": {"invoice": "unit"},
        }
    )

    preview = gui_picker.build_precomputed_signal_command_preview(item)
    formatted = gui_picker.format_precomputed_signal_command_preview(preview)
    output = json.dumps(preview, ensure_ascii=True, sort_keys=True).lower() + "\n" + formatted.lower()

    assert preview["execution_enabled"] is False
    assert preview["backtest_command_text"] == ""
    assert preview["runner_replay_command_text"] == ""
    for forbidden in (
        "entry_exec",
        "exit_exec",
        '"qty"',
        "trade_id",
        "api_key",
        "apikey",
        "secret",
        "token",
        "authorization",
        "raw_order",
        "balance",
        "raw_billing",
    ):
        assert forbidden not in output


def test_command_preview_validation_fails_closed_for_execution_or_live_paper_commands(tmp_path: Path) -> None:
    preview = gui_picker.build_precomputed_signal_command_preview(_valid_picker_item(tmp_path / "safe signal tape"))

    with pytest.raises(ValueError):
        gui_picker.validate_precomputed_signal_command_preview({**preview, "execution_enabled": True})
    with pytest.raises(ValueError):
        gui_picker.validate_precomputed_signal_command_preview({**preview, "live_command_available": True})
    with pytest.raises(ValueError):
        gui_picker.validate_precomputed_signal_command_preview(
            {**preview, "runner_replay_command_text": "python runner.py --mode live"}
        )
    with pytest.raises(ValueError):
        gui_picker.validate_precomputed_signal_command_preview(
            {**preview, "runner_replay_command_text": "python runner.py --mode paper"}
        )


def test_gui_command_preview_source_has_no_execution_or_order_connections() -> None:
    helper_source = PICKER_HELPER_PATH.read_text(encoding="utf-8")
    helper_tree = ast.parse(helper_source)
    imports: set[str] = set()
    for node in ast.walk(helper_tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module.split(".", 1)[0])
    assert {
        "subprocess",
        "multiprocessing",
        "threading",
        "ccxt",
        "exchange",
        "risk",
        "strategy",
        "indicators",
        "runner",
        "backtest",
        "precompute_signals",
        "precomputed_signals_inventory",
        "precomputed_signals_selection",
    }.isdisjoint(imports)

    for function_name in (
        "build_precomputed_signal_command_preview",
        "format_precomputed_signal_command_preview",
        "validate_precomputed_signal_command_preview",
    ):
        function_source = _function_source(PICKER_HELPER_PATH, function_name)
        for forbidden in (
            "subprocess",
            "os.system",
            "QProcess",
            "Popen",
            "startDetached",
            "multiprocessing",
            "threading",
            "background worker",
            "scheduler",
            "fetch_balance",
            "fetch_order",
            "create_order",
            "submit_order",
            "MEXC_API",
        ):
            assert forbidden not in function_source


def test_gui_run_paths_do_not_consume_command_preview_or_precomputed_fast_path() -> None:
    for method_name in ("on_start", "on_run_replay", "on_run_backtest", "on_run_pipeline"):
        method_source = _function_source(MAIN_WINDOW_PATH, method_name)
        assert "build_precomputed_signal_command_preview" not in method_source
        assert "format_precomputed_signal_command_preview" not in method_source
        assert "--use-precomputed-signals" not in method_source
        assert "--precomputed-signals-dir" not in method_source
        for forbidden in ("fetch_balance", "fetch_order", "create_order", "submit_order", "MEXC_API"):
            assert forbidden not in method_source


def test_app_version_unchanged_and_package_artifacts_not_staged_for_command_preview() -> None:
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
