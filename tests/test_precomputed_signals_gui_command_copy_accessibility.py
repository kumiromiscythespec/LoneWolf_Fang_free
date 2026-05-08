# BUILD_ID: 2026-05-08_free_precomputed_gui_copy_accessibility_docs_v1
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

from app.gui import precomputed_signal_picker as gui_picker

MAIN_WINDOW_PATH = REPO_ROOT / "app" / "app" / "gui" / "main_window.py"
PICKER_HELPER_PATH = REPO_ROOT / "app" / "app" / "gui" / "precomputed_signal_picker.py"
OPERATOR_DOC_PATHS = (
    REPO_ROOT / "docs" / "precomputed_signals_spec.md",
    REPO_ROOT / "docs" / "precomputed_signals_gui_picker_wiring.md",
    REPO_ROOT / "docs" / "precomputed_signals_gui_command_preview_operator_notes.md",
)


def _safe_command_preview() -> dict[str, Any]:
    signal_dir = r"C:\lwf\signals\phase11 safe tape"
    return {
        "signal_dir": signal_dir,
        "backtest_command_text": (
            'python backtest.py --use-precomputed-signals --precomputed-signals-dir '
            f'"{signal_dir}" --precomputed-signals-write-report'
        ),
        "runner_replay_command_text": (
            'python runner.py --mode replay --use-precomputed-signals --precomputed-signals-dir '
            f'"{signal_dir}" --precomputed-signals-write-report'
        ),
        "backtest_argv": [
            "python",
            "backtest.py",
            "--use-precomputed-signals",
            "--precomputed-signals-dir",
            signal_dir,
            "--precomputed-signals-write-report",
        ],
        "runner_replay_argv": [
            "python",
            "runner.py",
            "--mode",
            "replay",
            "--use-precomputed-signals",
            "--precomputed-signals-dir",
            signal_dir,
            "--precomputed-signals-write-report",
        ],
        "preview_only": True,
        "execution_enabled": False,
        "live_command_available": False,
        "paper_command_available": False,
        "warning": "Command preview only; execution is disabled in this panel.",
        "source_symbol": "BTC/USDT",
        "source_tf_pair": "5m/1h",
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


def test_copy_state_exposes_preview_only_accessibility_and_layout_text() -> None:
    state = gui_picker.build_precomputed_signal_copy_state(_safe_command_preview())

    assert state["preview_only"] is True
    assert state["execution_enabled"] is False
    assert state["live_command_available"] is False
    assert state["paper_command_available"] is False
    assert state["can_copy_backtest_command"] is True
    assert state["can_copy_runner_replay_command"] is True

    combined = json.dumps(state, ensure_ascii=True, sort_keys=True)
    assert "Preview only" in combined
    assert "Execution disabled" in combined
    assert "Not selectable for LIVE/PAPER" in combined
    assert "This panel does not execute commands" in combined
    assert "Run this command manually in a terminal if needed" in combined


def test_copy_tooltips_accessible_text_and_copied_status_are_safe_text() -> None:
    state = gui_picker.build_precomputed_signal_copy_state(_safe_command_preview())

    assert "Copy backtest command" in state["copy_backtest_tooltip"]
    assert "Copy replay command" in state["copy_replay_tooltip"]
    assert "Preview only" in state["preview_accessibility_label"]
    assert "Execution disabled" in state["execution_disabled_text"]
    assert state["live_paper_warning_text"] == "Not selectable for LIVE/PAPER"

    copied = gui_picker.mark_precomputed_command_copied(state, "backtest")
    assert copied["copy_status_text"] == "Copied"
    assert copied["copy_disabled_reason"] == ""
    assert copied["copied_backtest_command"] is True


def test_invalid_preview_returns_disabled_reason_and_safe_accessibility_text() -> None:
    state = gui_picker.build_precomputed_signal_copy_state({})

    assert state["can_copy_backtest_command"] is False
    assert state["can_copy_runner_replay_command"] is False
    assert state["copy_disabled_reason"] == "invalid_command_preview"
    assert "Copy disabled" in state["copy_status_text"]
    assert "Preview only" in state["copy_status_text"]
    assert "Execution disabled" in state["copy_status_text"]
    assert "Not selectable for LIVE/PAPER" in state["copy_status_text"]
    assert "This panel does not execute commands" in state["preview_accessibility_label"]


def test_live_paper_order_kinds_are_not_copyable() -> None:
    preview = _safe_command_preview()

    for kind in ("live", "paper", "order", "runner_live", "runner_paper"):
        assert gui_picker.get_copyable_precomputed_command(preview, kind) == ""
        copied = gui_picker.mark_precomputed_command_copied(
            gui_picker.build_precomputed_signal_copy_state(preview),
            kind,
        )
        assert copied["can_copy_backtest_command"] is False
        assert copied["can_copy_runner_replay_command"] is False


def test_copy_text_excludes_raw_rows_trades_private_order_and_billing_fields() -> None:
    preview = _safe_command_preview()
    state = gui_picker.build_precomputed_signal_copy_state(preview)
    output = json.dumps(state, ensure_ascii=True, sort_keys=True).lower()
    output += "\n" + gui_picker.get_copyable_precomputed_command(preview, "backtest").lower()
    output += "\n" + gui_picker.get_copyable_precomputed_command(preview, "runner_replay").lower()

    for forbidden in (
        "raw trades",
        "entry_exec",
        "exit_exec",
        '"qty"',
        "--qty",
        "trade_id",
        "trade id",
        "api_key",
        "apikey",
        "secret",
        "token",
        "authorization",
        "raw_order",
        "raw order",
        "balance",
        "raw_billing",
        "raw billing",
    ):
        assert forbidden not in output


def test_gui_copy_accessibility_source_adds_no_execution_button_or_worker_path() -> None:
    main_source = MAIN_WINDOW_PATH.read_text(encoding="utf-8")
    assert "setAccessibleName" in main_source
    assert "setAccessibleDescription" in main_source
    assert "setTabOrder(self.btn_select_precomputed_signal, self.btn_copy_precomputed_backtest_command)" in main_source
    assert "QApplication.clipboard().setText(command_text)" in main_source

    for function_name in (
        "_precomputed_signal_copy_accessibility_text",
        "_update_precomputed_signal_copy_ui",
        "_copy_precomputed_signal_command",
    ):
        function_source = _function_source(MAIN_WINDOW_PATH, function_name)
        for forbidden in (
            "subprocess",
            "os.system",
            "QProcess",
            "Popen",
            "startDetached",
            "threading",
            "multiprocessing",
            "background worker",
            "launch_backtest",
            "launch_runner",
            "launch_replay",
            "fetch_balance",
            "fetch_order",
            "create_order",
            "submit_order",
            "MEXC_API",
        ):
            assert forbidden not in function_source

    for line in _git_diff_added_lines("app/app/gui/main_window.py", "app/app/gui/precomputed_signal_picker.py"):
        assert not re.search(r"QPushButton\([^)]*(Run|Execute|Start)[^)]*precomputed", line, flags=re.IGNORECASE)
        for forbidden in (
            "subprocess.Popen",
            "os.system",
            "QProcess",
            "Popen(",
            "startDetached",
            "threading.Thread",
            "multiprocessing",
            "background worker",
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


def test_picker_copy_helper_imports_no_execution_or_network_modules() -> None:
    source = PICKER_HELPER_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports: set[str] = set()
    for node in ast.walk(tree):
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


def test_operator_docs_record_phase11_manual_terminal_and_no_execution_contract() -> None:
    docs_text = "\n".join(path.read_text(encoding="utf-8") for path in OPERATOR_DOC_PATHS if path.exists())

    assert "Free Phase 11" in docs_text
    assert "copy UX accessibility / layout smoke / operator docs" in docs_text
    assert "GUI command preview is preview-only" in docs_text
    assert "GUI does not execute commands" in docs_text
    assert "clipboard" in docs_text
    assert "external terminal" in docs_text
    assert "manual" in docs_text.lower()
    assert "preview_only=true" in docs_text
    assert "execution_enabled=false" in docs_text
    assert "live_command_available=false" in docs_text
    assert "paper_command_available=false" in docs_text
    assert "live / paper command" in docs_text.lower()
    assert "MEXC API" in docs_text
    assert "APP_VERSION" in docs_text


def test_app_version_unchanged_and_package_artifacts_not_staged_for_phase11() -> None:
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
