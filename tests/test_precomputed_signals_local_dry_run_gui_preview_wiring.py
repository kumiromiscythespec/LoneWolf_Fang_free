# BUILD_ID: 2026-05-09_free_precomputed_local_dry_run_gui_preview_wiring_v1
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
RUNTIME_SOURCE_PATHS = (
    REPO_ROOT / "backtest.py",
    REPO_ROOT / "runner.py",
    REPO_ROOT / "precompute_signals.py",
    REPO_ROOT / "fast_backtest_signals.py",
    REPO_ROOT / "signal_tape.py",
    REPO_ROOT / "precomputed_signals_inventory.py",
    REPO_ROOT / "precomputed_signals_selection.py",
)
GUI_SOURCE_PATHS = (MAIN_WINDOW_PATH, PICKER_HELPER_PATH)


def _valid_picker_item(signal_dir: Path) -> dict[str, Any]:
    return {
        "schema_version": "lwf.precomputed.signal_tape.v1",
        "gui_adapter_schema_version": "lwf.precomputed.signal_tape.gui_picker_item.v1",
        "product": "free",
        "signal_dir": str(signal_dir),
        "title": "Precomputed Signal Tape",
        "subtitle": "Read-only research selection",
        "symbol": "BTC/USDT",
        "symbol_normalized": "BTCUSDT",
        "entry_tf": "5m",
        "filter_tf": "1h",
        "signal_set_id": "sig_phase21_gui_preview_wiring",
        "dataset_id": "BTCUSDT_5m_1h_phase21_gui_preview_wiring",
        "created_at_utc": "2026-05-09T00:00:00Z",
        "since_ms": 1000,
        "until_ms": 2000,
        "trade_count": 0,
        "net_total": 0.0,
        "final_equity": 1000.0,
        "safety_research_only": True,
        "safety_paper_live_order_execution": False,
        "dd_schema_version": "lwf.dd.v1",
        "dd_sign_convention": "signed_negative",
        "max_drawdown": 0.0,
        "max_dd_signed": 0.0,
        "max_dd_abs": 0.0,
        "max_dd_pct": 0.0,
        "max_dd_display_abs": 0.0,
        "max_dd_display_pct": 0.0,
        "max_dd_display_label": "Max DD (abs, display)",
        "max_drawdown_legacy_note": "Legacy signed max_drawdown remains signed negative.",
        "dd_display_text": "Max DD display is absolute value; legacy signed max_drawdown remains signed negative.",
        "net_total_text": "0.0000",
        "final_equity_text": "1,000.0000",
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


def _git_diff_added_lines(*paths: str) -> list[str]:
    output = subprocess.run(
        ["git", "diff", "--", *paths],
        cwd=REPO_ROOT,
        check=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout
    return [line for line in output.splitlines() if line.startswith("+") and not line.startswith("+++")]


def _git_diff_names(paths: tuple[Path, ...]) -> set[str]:
    rel_paths = [str(path.relative_to(REPO_ROOT)).replace("\\", "/") for path in paths]
    output = subprocess.run(
        ["git", "diff", "--name-only", "--", *rel_paths],
        cwd=REPO_ROOT,
        check=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout
    return {line.strip() for line in output.splitlines() if line.strip()}


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


def test_gui_wiring_helper_creates_two_local_dry_run_preview_items(tmp_path: Path) -> None:
    output_root = tmp_path / "safe_preview_root"
    item = _valid_picker_item(tmp_path / "selected_signal_tape")
    previews = gui_picker.build_precomputed_signal_local_dry_run_preview_items(
        item,
        output_root=str(output_root),
    )

    assert len(previews) == 2
    assert {preview["dry_run_mode"] for preview in previews} == {
        "backtest_fast_path_local_only",
        "runner_replay_fast_path_local_only",
    }
    assert {preview["dry_run_mode_label"] for preview in previews} == {
        "Backtest fast path local-only",
        "Runner replay fast path local-only",
    }
    assert not output_root.exists()


def test_preview_text_includes_required_phase21_display_only_labels(tmp_path: Path) -> None:
    previews = gui_picker.build_precomputed_signal_local_dry_run_preview_items(
        _valid_picker_item(tmp_path / "selected_signal_tape"),
        output_root=str(tmp_path / "safe_preview_root"),
    )
    text = gui_picker.format_precomputed_signal_local_dry_run_preview_text(previews)

    for expected in (
        "Local dry-run request preview",
        "Preview only",
        "Execution disabled",
        "Operator confirmation required",
        "Operator confirmed: false",
        "operator_confirmed=false",
        "execution_enabled_after_confirmation=false",
        "preview_only_before_confirmation=true",
        "Future execution requires separate approved phase",
        "Not LIVE/PAPER/order",
        "No private API / no balance fetch / no order fetch",
        "Mode: Backtest fast path local-only",
        "Mode: Runner replay fast path local-only",
        "signal_dir:",
        "symbol: BTC/USDT",
        "entry_tf: 5m",
        "filter_tf: 1h",
        "signal_set_id: sig_phase21_gui_preview_wiring",
        "output_dir:",
        "command_text_preview:",
        "Allowed artifacts: safe summary, equity curve, fast summary",
        "Forbidden artifacts: raw market data, raw trades rows, orders, balances, secrets",
        "Fail-closed reasons: missing operator confirmation",
        "status: valid_preview",
        "request is not executable in this panel",
    ):
        assert expected in text


def test_preview_flags_stay_unconfirmed_non_executable_and_not_live_paper(tmp_path: Path) -> None:
    previews = gui_picker.build_precomputed_signal_local_dry_run_preview_items(
        _valid_picker_item(tmp_path / "selected_signal_tape"),
        output_root=str(tmp_path / "safe_preview_root"),
    )

    for preview in previews:
        assert preview["operator_confirmed"] is False
        assert preview["execution_enabled_after_confirmation"] is False
        assert preview["preview_only_before_confirmation"] is True
        assert preview["not_selectable_for_live"] is True
        assert preview["not_selectable_for_paper"] is True
        assert preview["operator_confirmation_required"] is True
        assert preview["paper_live_order_execution"] is False


def test_command_preview_text_is_backtest_or_runner_replay_only(tmp_path: Path) -> None:
    previews = gui_picker.build_precomputed_signal_local_dry_run_preview_items(
        _valid_picker_item(tmp_path / "selected_signal_tape"),
        output_root=str(tmp_path / "safe_preview_root"),
    )
    by_mode = {preview["dry_run_mode"]: preview for preview in previews}

    assert "backtest.py" in by_mode["backtest_fast_path_local_only"]["command_text_preview"]
    assert "runner.py --mode replay" in by_mode["runner_replay_fast_path_local_only"]["command_text_preview"]
    for preview in previews:
        command = preview["command_text_preview"]
        assert "--use-precomputed-signals" in command
        assert "--precomputed-signals-dir" in command
        assert "--mode live" not in command.lower()
        assert "--mode paper" not in command.lower()


def test_invalid_picker_item_produces_no_executable_preview(tmp_path: Path) -> None:
    invalid_item = _valid_picker_item(tmp_path / "missing_signal_tape")
    invalid_item["status"] = "invalid"
    invalid_item["safe_error_code"] = "missing_signal_dir"
    invalid_item["selectable_for_backtest_fast_path"] = False
    invalid_item["selectable_for_runner_replay_fast_path"] = False

    previews = gui_picker.build_precomputed_signal_local_dry_run_preview_items(invalid_item)
    text = gui_picker.format_precomputed_signal_local_dry_run_preview_text(previews)

    assert previews == []
    assert "Status: disabled - No valid signal tape selected." in text
    assert "request is not executable in this panel" in text
    assert "Execution disabled" in text


def test_preview_display_text_contains_no_row_private_or_runtime_payload_values(tmp_path: Path) -> None:
    previews = gui_picker.build_precomputed_signal_local_dry_run_preview_items(
        _valid_picker_item(tmp_path / "selected_signal_tape"),
        output_root=str(tmp_path / "safe_preview_root"),
    )
    text = gui_picker.format_precomputed_signal_local_dry_run_preview_text(previews)
    text_without_policy_labels = "\n".join(
        line for line in text.splitlines() if not line.startswith(("Allowed artifacts:", "Forbidden artifacts:"))
    ).lower()

    for forbidden in (
        "entry_exec",
        "exit_exec",
        '"qty"',
        "'qty'",
        " qty ",
        "qty=",
        "trade_id",
        "trade id",
        "api key",
        "api_key",
        "apikey",
        "secret:",
        "token:",
        "authorization:",
        "raw order",
        "raw_order",
        '"balance"',
        "balance snapshot",
        "raw billing",
        "raw_billing",
        "raw ohlcv",
    ):
        assert forbidden not in text_without_policy_labels


def test_build_precomputed_signal_picker_state_exposes_phase21_preview_text(tmp_path: Path, monkeypatch: Any) -> None:
    item = _valid_picker_item(tmp_path / "selected_signal_tape")
    monkeypatch.setattr(gui_picker.gui_adapter, "build_gui_picker_item_from_signal_dir", lambda _signal_dir: item)

    state = gui_picker.build_precomputed_signal_picker_state(str(tmp_path / "selected_signal_tape"))

    assert len(state["local_dry_run_preview_items"]) == 2
    assert "Local dry-run request preview" in state["local_dry_run_preview_text"]
    assert "Mode: Backtest fast path local-only" in state["local_dry_run_preview_text"]
    assert "Mode: Runner replay fast path local-only" in state["local_dry_run_preview_text"]


def test_gui_source_wires_read_only_preview_section_without_buttons_or_confirmation() -> None:
    main_source = MAIN_WINDOW_PATH.read_text(encoding="utf-8")
    helper_source = PICKER_HELPER_PATH.read_text(encoding="utf-8")

    assert "precomputed_signal_local_dry_run_preview" in main_source
    assert "Local dry-run request preview" in main_source
    assert "build_precomputed_signal_local_dry_run_preview_items" in main_source
    assert "format_precomputed_signal_local_dry_run_preview_text" in main_source
    assert "build_precomputed_signal_local_dry_run_preview_items" in helper_source
    assert "validate_precomputed_signal_local_dry_run_preview_items" in helper_source

    added_lines = _git_diff_added_lines("app/app/gui/main_window.py", "app/app/gui/precomputed_signal_picker.py")
    added_source = "\n".join(added_lines)
    assert not re.search(
        r"QPushButton\([^)]*(Run|Execute|Start|Dry Run|Confirm|Approve)[^)]*precomputed",
        added_source,
        flags=re.IGNORECASE,
    )
    assert "operator_confirmation_required=True" not in added_source


def test_phase21_wiring_helpers_add_no_process_background_or_network_execution() -> None:
    for source_path, function_name in (
        (PICKER_HELPER_PATH, "build_precomputed_signal_local_dry_run_preview_items"),
        (PICKER_HELPER_PATH, "format_precomputed_signal_local_dry_run_preview_text"),
        (PICKER_HELPER_PATH, "validate_precomputed_signal_local_dry_run_preview_items"),
        (MAIN_WINDOW_PATH, "_update_precomputed_signal_local_dry_run_preview_ui"),
        (MAIN_WINDOW_PATH, "_refresh_precomputed_signal_picker_display"),
        (MAIN_WINDOW_PATH, "_apply_precomputed_signal_tape_selection"),
    ):
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


def test_gui_source_has_no_new_live_paper_order_or_private_api_connection() -> None:
    added_source = "\n".join(_git_diff_added_lines("app/app/gui/main_window.py", "app/app/gui/precomputed_signal_picker.py"))

    for forbidden in (
        "launch_backtest",
        "launch_runner",
        "launch_replay",
        "precompute_signals.py",
        "precomputed_signals_inventory.py",
        "--mode live",
        "--mode paper",
        "fetch_balance",
        "fetch_order",
        "create_order",
        "submit_order",
        "MEXC_API",
        "ccxt",
        "QProcess",
        "Popen(",
        "os.system",
        "startDetached",
    ):
        assert forbidden not in added_source


def test_runtime_sources_app_version_and_package_artifacts_remain_unchanged() -> None:
    assert _git_diff_names(RUNTIME_SOURCE_PATHS) == set()

    current_config = (REPO_ROOT / "config.py").read_text(encoding="utf-8")
    head_config = subprocess.run(
        ["git", "show", "HEAD:config.py"],
        cwd=REPO_ROOT,
        check=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout
    assert _app_version_from(current_config) == _app_version_from(head_config)

    staged = subprocess.run(
        ["git", "diff", "--cached", "--name-only"],
        cwd=REPO_ROOT,
        check=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout.splitlines()
    for name in staged:
        lowered = name.lower()
        assert not lowered.endswith((".zip", ".exe", ".msi", ".apk"))
        assert not any(token in lowered for token in ("setup", "installer", "package", "release"))


def test_phase21_changed_gui_files_are_limited_to_wiring_sources() -> None:
    assert _git_diff_names(GUI_SOURCE_PATHS).issubset(
        {"app/app/gui/main_window.py", "app/app/gui/precomputed_signal_picker.py"}
    )
    assert "free_precomputed_local_dry_run_gui_preview_wiring_v1" in PICKER_HELPER_PATH.read_text(encoding="utf-8")
    assert "free_precomputed_local_dry_run_gui_preview_wiring_v1" in MAIN_WINDOW_PATH.read_text(encoding="utf-8")
