# BUILD_ID: 2026-05-08_free_precomputed_gui_selection_diagnostics_v1
from __future__ import annotations

import ast
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

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
DOC_PATHS = (
    REPO_ROOT / "docs" / "precomputed_signals_spec.md",
    REPO_ROOT / "docs" / "precomputed_signals_gui_picker_wiring.md",
    REPO_ROOT / "docs" / "precomputed_signals_gui_selection_diagnostics.md",
)


def _valid_picker_item(signal_dir: Path | str) -> dict[str, Any]:
    return {
        "schema_version": tape.SCHEMA_VERSION,
        "gui_adapter_schema_version": gui_adapter.GUI_ADAPTER_SCHEMA_VERSION,
        "product": tape.DEFAULT_PRODUCT,
        "signal_dir": str(signal_dir),
        "title": "BTC/USDT 5m/1h sig_gui_selection_diagnostics_fixture",
        "subtitle": "free | trades=2 | Max DD (abs, display): 10.0000",
        "symbol": "BTC/USDT",
        "symbol_normalized": "BTCUSDT",
        "entry_tf": "5m",
        "filter_tf": "1h",
        "signal_set_id": "sig_gui_selection_diagnostics_fixture",
        "dataset_id": "BTCUSDT_5m_1h_gui_selection_diagnostics_unit",
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


def _git_diff_added_lines(*paths: str) -> list[str]:
    diff = subprocess.run(
        ["git", "diff", "--", *paths],
        cwd=REPO_ROOT,
        check=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout
    return [line for line in diff.splitlines() if line.startswith("+") and not line.startswith("+++")]


def _assert_no_forbidden_diagnostics_text(output: str) -> None:
    lowered = output.lower()
    for forbidden in (
        "entry_exec",
        "exit_exec",
        '"qty"',
        "trade_id",
        "trade id",
        "order_id",
        "raw_order",
        "api_key",
        "apikey",
        "secret",
        "token",
        "authorization",
        "raw_billing",
        "raw market data",
        "raw ohlcv",
        "balance",
    ):
        assert forbidden not in lowered


def test_valid_picker_item_builds_read_only_selection_diagnostics(tmp_path: Path) -> None:
    diagnostics = gui_picker.build_precomputed_signal_diagnostics(_valid_picker_item(tmp_path / "safe tape"))
    text = gui_picker.format_precomputed_signal_diagnostics_text(diagnostics)

    assert diagnostics["status"] == "valid"
    assert diagnostics["signal_dir"] == str(tmp_path / "safe tape")
    assert diagnostics["selectable_for_backtest_fast_path"] is True
    assert diagnostics["selectable_for_runner_replay_fast_path"] is True
    assert diagnostics["not_selectable_for_live"] is True
    assert diagnostics["not_selectable_for_paper"] is True
    assert diagnostics["manifest_sha256"] == "a" * 64
    assert diagnostics["summary_sha256"] == "b" * 64
    assert diagnostics["trades_csv_sha256_from_manifest"] == "c" * 64
    assert diagnostics["max_dd_display_abs"] == 10.0
    assert diagnostics["max_dd_display_pct"] == 0.01
    assert diagnostics["safety_research_only"] is True
    assert diagnostics["safety_paper_live_order_execution"] is False

    assert "Selection diagnostics" in text
    assert "Status: valid" in text
    assert "Backtest fast path: selectable" in text
    assert "Replay fast path: selectable" in text
    assert "LIVE/PAPER: not selectable" in text
    assert "No raw trade rows are displayed" in text
    assert "This panel does not execute commands" in text
    assert "Manifest hash" in text
    assert "Summary hash" in text
    assert "Trades CSV hash from manifest" in text
    assert "max_dd_display_abs" in text
    assert "max_dd_display_pct" in text
    _assert_no_forbidden_diagnostics_text(json.dumps(diagnostics, ensure_ascii=True, sort_keys=True) + "\n" + text)


def test_invalid_picker_item_displays_safe_error_without_raw_payload(tmp_path: Path) -> None:
    invalid_item = {
        "signal_dir": str(tmp_path / "invalid tape"),
        "status": "invalid",
        "status_reason": "manifest.json is missing",
        "safe_error_code": "missing_manifest",
        "raw_payload": {"entry_exec": 100.0, "exit_exec": 110.0, "qty": 1.0, "trade_id": "unit"},
    }

    diagnostics = gui_picker.build_precomputed_signal_diagnostics(invalid_item)
    text = gui_picker.format_precomputed_signal_diagnostics_text(diagnostics)
    output = json.dumps(diagnostics, ensure_ascii=True, sort_keys=True) + "\n" + text

    assert diagnostics["status"] == "invalid"
    assert diagnostics["safe_error_code"] == "missing_manifest"
    assert diagnostics["selectable_for_backtest_fast_path"] is False
    assert diagnostics["selectable_for_runner_replay_fast_path"] is False
    assert diagnostics["not_selectable_for_live"] is True
    assert diagnostics["not_selectable_for_paper"] is True
    assert "Status: invalid" in text
    assert "Safe error code: missing_manifest" in text
    assert "LIVE/PAPER: not selectable" in text
    _assert_no_forbidden_diagnostics_text(output)


def test_picker_display_and_state_include_diagnostics_text(tmp_path: Path) -> None:
    item = _valid_picker_item(tmp_path / "safe tape")
    display_text = gui_picker.format_precomputed_signal_picker_display_text(item)

    assert "Selection diagnostics" in display_text
    assert "Manifest hash" in display_text
    assert "Summary hash" in display_text
    assert "Trades CSV hash from manifest" in display_text
    assert "No raw trade rows are displayed" in display_text
    assert "This panel does not execute commands" in display_text


def test_gui_source_adds_read_only_diagnostics_without_execution_connections() -> None:
    main_source = MAIN_WINDOW_PATH.read_text(encoding="utf-8")
    helper_source = PICKER_HELPER_PATH.read_text(encoding="utf-8")

    assert "Precomputed signal selection diagnostics" in main_source
    assert "build_precomputed_signal_diagnostics" in helper_source
    assert "format_precomputed_signal_diagnostics_text" in helper_source
    assert "validate_precomputed_signal_diagnostics" in helper_source

    helper_tree = ast.parse(helper_source)
    imports: set[str] = set()
    for node in ast.walk(helper_tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module.split(".", 1)[0])
    assert {
        "subprocess",
        "threading",
        "multiprocessing",
        "ccxt",
        "runner",
        "backtest",
        "precompute_signals",
        "precomputed_signals_inventory",
        "precomputed_signals_selection",
    }.isdisjoint(imports)

    for function_name in (
        "build_precomputed_signal_diagnostics",
        "format_precomputed_signal_diagnostics_text",
        "validate_precomputed_signal_diagnostics",
        "_refresh_precomputed_signal_picker_display",
        "_apply_precomputed_signal_tape_selection",
    ):
        source_path = PICKER_HELPER_PATH if function_name.startswith(("build_", "format_", "validate_")) else MAIN_WINDOW_PATH
        function_source = _function_source(source_path, function_name)
        for forbidden in (
            "subprocess",
            "os.system",
            "QProcess",
            "Popen",
            "startDetached",
            "threading",
            "multiprocessing",
            "background worker",
            "scheduler",
            "launch_backtest",
            "launch_runner",
            "launch_replay",
            "fetch_balance",
            "fetch_order",
            "create_order",
            "submit_order",
            "MEXC_API",
            "ccxt",
        ):
            assert forbidden not in function_source

    for line in _git_diff_added_lines("app/app/gui/main_window.py", "app/app/gui/precomputed_signal_picker.py"):
        for forbidden in (
            "subprocess.Popen",
            "os.system",
            "QProcess",
            "Popen(",
            "startDetached",
            "threading.Thread",
            "multiprocessing",
            "launch_backtest",
            "launch_runner",
            "launch_replay",
            "fetch_balance",
            "fetch_order",
            "create_order",
            "submit_order",
            "MEXC_API",
            "ccxt",
        ):
            assert forbidden not in line


def test_operator_docs_record_phase12_read_only_diagnostics_contract() -> None:
    docs_text = "\n".join(path.read_text(encoding="utf-8") for path in DOC_PATHS if path.exists())

    assert "Free Phase 12" in docs_text
    assert "read-only selection diagnostics display" in docs_text
    assert "diagnostics are read-only" in docs_text
    assert "manifest / summary / selection safe metadata" in docs_text
    assert "trades.csv raw rows are not read or displayed" in docs_text
    assert "GUI does not execute commands" in docs_text
    assert "producer / backtest / runner / inventory" in docs_text
    assert "LIVE / PAPER / order" in docs_text
    assert "MEXC API" in docs_text
    assert "max_dd_abs / max_dd_pct" in docs_text
    assert "max_drawdown" in docs_text
    assert "not_selectable_for_live=true" in docs_text
    assert "not_selectable_for_paper=true" in docs_text
    assert "APP_VERSION" in docs_text


def test_app_version_unchanged_and_package_artifacts_not_staged_for_phase12() -> None:
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
