# BUILD_ID: 2026-05-08_free_precomputed_gui_command_copy_ux_v1
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

from app.gui import precomputed_signal_picker as gui_picker

MAIN_WINDOW_PATH = REPO_ROOT / "app" / "app" / "gui" / "main_window.py"
PICKER_HELPER_PATH = REPO_ROOT / "app" / "app" / "gui" / "precomputed_signal_picker.py"


def _safe_command_preview() -> dict[str, Any]:
    signal_dir = r"C:\lwf\signals\safe_tape"
    backtest_argv = [
        "python",
        "backtest.py",
        "--use-precomputed-signals",
        "--precomputed-signals-dir",
        signal_dir,
        "--precomputed-signals-write-report",
    ]
    replay_argv = [
        "python",
        "runner.py",
        "--mode",
        "replay",
        "--use-precomputed-signals",
        "--precomputed-signals-dir",
        signal_dir,
        "--precomputed-signals-write-report",
    ]
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
        "backtest_argv": backtest_argv,
        "runner_replay_argv": replay_argv,
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


def test_valid_command_preview_builds_copy_state() -> None:
    copy_state = gui_picker.build_precomputed_signal_copy_state(_safe_command_preview())

    assert copy_state["preview_only"] is True
    assert copy_state["execution_enabled"] is False
    assert copy_state["live_command_available"] is False
    assert copy_state["paper_command_available"] is False
    assert copy_state["can_copy_backtest_command"] is True
    assert copy_state["can_copy_runner_replay_command"] is True
    assert copy_state["copied_backtest_command"] is False
    assert copy_state["copied_runner_replay_command"] is False
    assert "Preview only" in copy_state["copy_status_text"]
    assert "does not execute commands" in copy_state["copy_warning"]


def test_backtest_and_runner_replay_commands_are_copyable() -> None:
    preview = _safe_command_preview()

    assert gui_picker.get_copyable_precomputed_command(preview, "backtest") == preview["backtest_command_text"]
    assert gui_picker.get_copyable_precomputed_command(preview, "runner_replay") == preview["runner_replay_command_text"]


@pytest.mark.parametrize("kind", ["live", "paper", "order", "runner_live"])
def test_live_paper_and_order_kinds_are_not_copyable(kind: str) -> None:
    assert gui_picker.get_copyable_precomputed_command(_safe_command_preview(), kind) == ""


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("preview_only", False),
        ("execution_enabled", True),
        ("live_command_available", True),
        ("paper_command_available", True),
    ],
)
def test_copy_state_requires_preview_only_and_disabled_execution_flags(field: str, value: bool) -> None:
    preview = _safe_command_preview()
    preview[field] = value

    copy_state = gui_picker.build_precomputed_signal_copy_state(preview)

    assert copy_state["can_copy_backtest_command"] is False
    assert copy_state["can_copy_runner_replay_command"] is False
    assert copy_state["copy_disabled_reason"] == "invalid_command_preview"
    assert gui_picker.get_copyable_precomputed_command(preview, "backtest") == ""
    assert gui_picker.get_copyable_precomputed_command(preview, "runner_replay") == ""


def test_invalid_or_empty_preview_disables_copy() -> None:
    empty_state = gui_picker.build_precomputed_signal_copy_state({})
    no_command_preview = {
        **_safe_command_preview(),
        "backtest_command_text": "",
        "runner_replay_command_text": "",
        "backtest_argv": [],
        "runner_replay_argv": [],
    }
    no_command_state = gui_picker.build_precomputed_signal_copy_state(no_command_preview)

    assert empty_state["can_copy_backtest_command"] is False
    assert empty_state["can_copy_runner_replay_command"] is False
    assert no_command_state["can_copy_backtest_command"] is False
    assert no_command_state["can_copy_runner_replay_command"] is False
    assert no_command_state["copy_disabled_reason"] == "empty_command_text"


def test_copied_status_is_safe_state_only() -> None:
    copy_state = gui_picker.build_precomputed_signal_copy_state(_safe_command_preview())

    copied_backtest = gui_picker.mark_precomputed_command_copied(copy_state, "backtest")
    copied_replay = gui_picker.mark_precomputed_command_copied(copy_state, "runner_replay")
    copied_live = gui_picker.mark_precomputed_command_copied(copy_state, "live")

    assert copied_backtest["copied_backtest_command"] is True
    assert copied_backtest["copied_runner_replay_command"] is False
    assert copied_backtest["copy_status_text"] == "Copied"
    assert copied_replay["copied_backtest_command"] is False
    assert copied_replay["copied_runner_replay_command"] is True
    assert copied_live["can_copy_backtest_command"] is False
    assert copied_live["can_copy_runner_replay_command"] is False


def test_copy_helper_does_not_execute_commands_or_import_execution_modules() -> None:
    helper_source = PICKER_HELPER_PATH.read_text(encoding="utf-8")
    helper_tree = ast.parse(helper_source)
    imports: set[str] = set()
    for node in ast.walk(helper_tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module.split(".", 1)[0])
    assert {"subprocess", "multiprocessing", "threading", "runner", "backtest", "precompute_signals"}.isdisjoint(imports)

    for function_name in (
        "build_precomputed_signal_copy_state",
        "get_copyable_precomputed_command",
        "mark_precomputed_command_copied",
        "validate_precomputed_signal_copy_state",
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
            "scheduler",
            "fetch_balance",
            "fetch_order",
            "create_order",
            "submit_order",
            "MEXC_API",
        ):
            assert forbidden not in function_source


def test_copy_output_excludes_raw_rows_private_runtime_and_billing_fields() -> None:
    preview = _safe_command_preview()
    copy_state = gui_picker.build_precomputed_signal_copy_state(preview)
    output = json.dumps(copy_state, ensure_ascii=True, sort_keys=True).lower()
    output += "\n" + gui_picker.get_copyable_precomputed_command(preview, "backtest").lower()
    output += "\n" + gui_picker.get_copyable_precomputed_command(preview, "runner_replay").lower()

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
        "raw market data",
        "ohlcv",
    ):
        assert forbidden not in output


def test_copy_helper_fails_closed_for_unsafe_command_text() -> None:
    preview = _safe_command_preview()
    preview["backtest_command_text"] += " entry_exec"
    preview["runner_replay_command_text"] += " entry_exec"

    copy_state = gui_picker.build_precomputed_signal_copy_state(preview)

    assert copy_state["can_copy_backtest_command"] is False
    assert copy_state["can_copy_runner_replay_command"] is False
    assert copy_state["copy_disabled_reason"] == "unsafe_command_text"


def test_copy_helper_fails_closed_for_live_or_paper_command_text() -> None:
    live_preview = {**_safe_command_preview(), "runner_replay_command_text": "python runner.py --mode live"}
    paper_preview = {**_safe_command_preview(), "runner_replay_command_text": "python runner.py --mode paper"}

    assert gui_picker.build_precomputed_signal_copy_state(live_preview)["can_copy_runner_replay_command"] is False
    assert gui_picker.build_precomputed_signal_copy_state(paper_preview)["can_copy_runner_replay_command"] is False


def test_gui_copy_source_has_only_clipboard_preview_text_path() -> None:
    main_source = MAIN_WINDOW_PATH.read_text(encoding="utf-8")
    assert "Copy backtest command" in main_source
    assert "Copy replay command" in main_source
    assert "QApplication.clipboard().setText(command_text)" in main_source
    assert "mark_precomputed_command_copied" in main_source

    for function_name in (
        "_copy_precomputed_signal_command",
        "_update_precomputed_signal_copy_ui",
        "_precomputed_signal_copy_status_text",
    ):
        function_source = _function_source(MAIN_WINDOW_PATH, function_name)
        assert "clipboard().setText(command_text)" in function_source or function_name != "_copy_precomputed_signal_command"
        for forbidden in (
            "subprocess",
            "os.system",
            "QProcess",
            "Popen",
            "startDetached",
            "multiprocessing",
            "threading",
            "scheduler",
            "fetch_balance",
            "fetch_order",
            "create_order",
            "submit_order",
            "MEXC_API",
        ):
            assert forbidden not in function_source


def test_gui_source_diff_adds_no_execution_or_order_connection() -> None:
    diff = subprocess.run(
        ["git", "diff", "--", "app/app/gui/main_window.py", "app/app/gui/precomputed_signal_picker.py"],
        cwd=REPO_ROOT,
        check=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout
    added_lines = [line for line in diff.splitlines() if line.startswith("+") and not line.startswith("+++")]
    for line in added_lines:
        for forbidden in (
            "subprocess.Popen",
            "os.system",
            "QProcess",
            "Popen(",
            "startDetached",
            "multiprocessing",
            "threading.Thread",
            "fetch_balance",
            "fetch_order",
            "create_order",
            "submit_order",
            "MEXC_API",
            "ccxt",
        ):
            assert forbidden not in line


def test_app_version_unchanged_and_package_artifacts_not_staged_for_copy_ux() -> None:
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
