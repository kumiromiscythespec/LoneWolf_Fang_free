# BUILD_ID: 2026-05-08_free_precomputed_gui_diagnostics_polish_v1
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
    REPO_ROOT / "docs" / "precomputed_signals_gui_smoke_procedure.md",
)


def _valid_picker_item(signal_dir: Path | str) -> dict[str, Any]:
    return {
        "schema_version": tape.SCHEMA_VERSION,
        "gui_adapter_schema_version": gui_adapter.GUI_ADAPTER_SCHEMA_VERSION,
        "product": tape.DEFAULT_PRODUCT,
        "signal_dir": str(signal_dir),
        "title": "BTC/USDT 5m/1h sig_gui_diagnostics_polish_fixture",
        "subtitle": "free | trades=2 | Max DD (abs, display): 10.0000",
        "symbol": "BTC/USDT",
        "symbol_normalized": "BTCUSDT",
        "entry_tf": "5m",
        "filter_tf": "1h",
        "signal_set_id": "sig_gui_diagnostics_polish_fixture",
        "dataset_id": "BTCUSDT_5m_1h_gui_diagnostics_polish_unit",
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
        "manifest_sha256": "8f3a21c9" + ("a" * 56),
        "summary_sha256": "d1aa7720" + ("b" * 56),
        "trades_csv_sha256_from_manifest": "74ab19e0" + ("c" * 56),
        "safe_error_code": "",
        "selectable_for_backtest_fast_path": True,
        "selectable_for_runner_replay_fast_path": True,
        "not_selectable_for_live": True,
        "not_selectable_for_paper": True,
        "picker_warning": "Read-only research selection; not available for LIVE/PAPER.",
        "status": "valid",
        "status_reason": "ok",
    }


def _invalid_picker_item(signal_dir: Path | str) -> dict[str, Any]:
    return {
        "signal_dir": str(signal_dir),
        "status": "invalid",
        "status_reason": "manifest.json is missing",
        "safe_error_code": "missing_manifest",
        "raw_payload": {"entry_exec": 100.0, "exit_exec": 110.0, "qty": 1.0, "trade_id": "unit"},
    }


def _display_for(item: dict[str, Any]) -> dict[str, Any]:
    diagnostics = gui_picker.build_precomputed_signal_diagnostics(item)
    return gui_picker.build_precomputed_signal_diagnostics_display(diagnostics)


def _function_source(path: Path, name: str) -> str:
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return ast.get_source_segment(source, node) or ""
    raise AssertionError(f"function not found: {name}")


def _git_diff_added_lines(*paths: str) -> list[str]:
    diff = subprocess.run(
        ["git", "diff", "--", *paths],
        cwd=REPO_ROOT,
        check=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout
    return [line for line in diff.splitlines() if line.startswith("+") and not line.startswith("+++")]


def _app_version_from(text: str) -> str:
    match = re.search(r'^APP_VERSION\s*=\s*["\']([^"\']+)["\']', text, flags=re.MULTILINE)
    assert match is not None
    return match.group(1)


def test_compact_hash_helper_shortens_long_hash() -> None:
    full_hash = "8f3a21c9" + ("a" * 56)
    assert gui_picker.compact_precomputed_hash(full_hash) == "8f3a21c9aa..."
    assert gui_picker.compact_precomputed_hash(full_hash, length=8) == "8f3a21c9..."


def test_compact_hash_helper_marks_empty_hash_missing() -> None:
    assert gui_picker.compact_precomputed_hash("") == "missing"
    assert gui_picker.compact_precomputed_hash("   ") == "missing"


def test_compact_signal_dir_helper_shortens_long_windows_path() -> None:
    long_path = (
        r"C:\Users\operator\AppData\Local\LoneWolfFang\data\precomputed_signals"
        r"\free\BTCUSDT\5m_1h\sig_gui_diagnostics_polish_fixture"
    )
    compact = gui_picker.compact_precomputed_signal_dir(long_path)
    assert compact == r"...\free\BTCUSDT\5m_1h\sig_gui_diagnostics_polish_fixture"
    assert "AppData" not in compact


def test_compact_signal_dir_helper_redacts_credential_like_segments() -> None:
    compact = gui_picker.compact_precomputed_signal_dir(
        r"C:\safe\api_secret_folder\free\BTCUSDT\5m_1h\sig_unit"
    )
    assert "[redacted]" in compact
    assert "secret" not in compact.lower()


def test_diagnostics_display_has_valid_operator_labels(tmp_path: Path) -> None:
    display = _display_for(_valid_picker_item(tmp_path / "free" / "BTCUSDT" / "5m_1h" / "sig_unit"))
    text = gui_picker.format_precomputed_signal_diagnostics_compact_text(display)

    assert display["diagnostics_status_label"] == "Selection diagnostics: valid"
    assert display["diagnostics_status_kind"] == "valid"
    assert display["tape_files_present_label"] == "Files: manifest OK / summary OK / trades.csv OK"
    assert display["safety_flags_label"] == "Safety: research-only OK / no live-paper execution OK"
    assert display["fast_path_availability_label"] == "Fast path: Backtest OK / Replay OK"
    assert display["live_paper_not_selectable_label"] == "LIVE/PAPER: not selectable"
    assert display["not_selectable_for_live"] is True
    assert display["not_selectable_for_paper"] is True
    assert "No raw trade rows are displayed" in display["diagnostics_warning_text"]
    assert "This panel does not execute commands" in display["diagnostics_warning_text"]
    assert "Selection diagnostics: valid" in text
    assert "Manifest hash: 8f3a21c9aa..." in text
    assert "Summary hash: d1aa7720bb..." in text
    assert "Trades CSV hash from manifest: 74ab19e0cc..." in text


def test_diagnostics_display_has_invalid_operator_labels(tmp_path: Path) -> None:
    display = _display_for(_invalid_picker_item(tmp_path / "invalid tape"))
    text = gui_picker.format_precomputed_signal_diagnostics_compact_text(display)

    assert display["diagnostics_status_label"] == "Selection diagnostics: invalid"
    assert display["diagnostics_status_kind"] == "invalid"
    assert display["safe_error_code_label"] == "Safe error code: missing_manifest"
    assert "Safe error code: missing_manifest" in text
    assert "Reason: manifest.json is missing" in text
    assert "Fast path: Backtest not selectable / Replay not selectable" in text
    assert "LIVE/PAPER: not selectable" in text


def test_diagnostics_compact_text_excludes_raw_trade_and_private_terms(tmp_path: Path) -> None:
    valid_display = _display_for(_valid_picker_item(tmp_path / "safe tape"))
    invalid_display = _display_for(_invalid_picker_item(tmp_path / "invalid tape"))
    output = "\n".join(
        (
            gui_picker.format_precomputed_signal_diagnostics_compact_text(valid_display),
            gui_picker.format_precomputed_signal_diagnostics_compact_text(invalid_display),
            json.dumps(valid_display, ensure_ascii=True, sort_keys=True),
            json.dumps(invalid_display, ensure_ascii=True, sort_keys=True),
        )
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
        "secret",
        "token",
        "authorization",
        "raw order",
        "balance",
        "raw billing",
        "private key",
    ):
        assert forbidden not in output
    assert "no raw trade rows are displayed" in output


def test_picker_state_exposes_compact_display_and_safe_tooltip(tmp_path: Path, monkeypatch: Any) -> None:
    item = _valid_picker_item(tmp_path / "free" / "BTCUSDT" / "5m_1h" / "sig_unit")
    monkeypatch.setattr(gui_adapter, "build_gui_picker_item_from_signal_dir", lambda _signal_dir: item)

    state = gui_picker.build_precomputed_signal_picker_state(str(tmp_path / "selected"))

    assert state["diagnostics_display"]["diagnostics_status_label"] == "Selection diagnostics: valid"
    assert "Full hashes: manifest " + item["manifest_sha256"] in state["diagnostics_display"]["diagnostics_details_text"]
    assert "Selection diagnostics: valid" in state["diagnostics_text"]
    assert item["manifest_sha256"] not in state["diagnostics_text"]


def test_gui_source_has_no_new_process_or_background_execution_connections() -> None:
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
        "threading",
        "multiprocessing",
        "ccxt",
        "runner",
        "backtest",
        "precompute_signals",
        "precomputed_signals_inventory",
        "precomputed_signals_selection",
    }.isdisjoint(imports)

    for source_path, function_name in (
        (PICKER_HELPER_PATH, "compact_precomputed_hash"),
        (PICKER_HELPER_PATH, "compact_precomputed_signal_dir"),
        (PICKER_HELPER_PATH, "build_precomputed_signal_diagnostics_display"),
        (PICKER_HELPER_PATH, "format_precomputed_signal_diagnostics_compact_text"),
        (PICKER_HELPER_PATH, "validate_precomputed_signal_diagnostics_display"),
        (MAIN_WINDOW_PATH, "_build_current_precomputed_signal_diagnostics_display"),
        (MAIN_WINDOW_PATH, "_update_precomputed_signal_diagnostics_status_ui"),
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
            "fetch_balance",
            "fetch_order",
            "create_order",
            "submit_order",
            "MEXC_API",
            "ccxt",
        ):
            assert forbidden not in line


def test_gui_source_has_no_new_live_paper_or_producer_auto_run_connection() -> None:
    added_gui = "\n".join(_git_diff_added_lines("app/app/gui/main_window.py", "app/app/gui/precomputed_signal_picker.py"))
    for forbidden in (
        "launch_backtest",
        "launch_runner",
        "launch_replay",
        "precompute_signals.py",
        "precomputed_signals_inventory.py",
        "build_gui_picker_item_from_signal_dir(",
        "fetch_balance",
        "fetch_order",
        "create_order",
        "submit_order",
        "--mode live",
        "--mode paper",
    ):
        assert forbidden not in added_gui


def test_operator_docs_record_phase13_gui_smoke_boundary() -> None:
    docs_text = "\n".join(path.read_text(encoding="utf-8") for path in DOC_PATHS if path.exists())

    assert "Free Phase 13" in docs_text
    assert "operator-facing diagnostics polish" in docs_text
    assert "GUI smoke is optional" in docs_text
    assert "GUI smoke does not start LIVE / PAPER / order" in docs_text
    assert "producer / backtest / runner / inventory" in docs_text
    assert "selection diagnostics visible" in docs_text
    assert "valid/invalid status visible" in docs_text
    assert "file presence compact display visible" in docs_text
    assert "hash compact display visible" in docs_text
    assert "no raw trade rows visible" in docs_text
    assert "Backtest/Replay command preview visible" in docs_text
    assert "Copy buttons visible but no execution" in docs_text
    assert "Not selectable for LIVE/PAPER visible" in docs_text
    assert "APP_VERSION" in docs_text
    assert "generated real signal tape body / raw market data" in docs_text


def test_app_version_unchanged_and_package_artifacts_not_staged_for_phase13() -> None:
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
