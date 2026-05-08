# BUILD_ID: 2026-05-09_free_precomputed_gui_smoke_checklist_docs_v1
from __future__ import annotations

import re
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
CHECKLIST_DOC = REPO_ROOT / "docs" / "precomputed_signals_gui_smoke_checklist.md"
DOC_PATHS = (
    REPO_ROOT / "docs" / "precomputed_signals_spec.md",
    REPO_ROOT / "docs" / "precomputed_signals_gui_picker_wiring.md",
    REPO_ROOT / "docs" / "precomputed_signals_gui_smoke_procedure.md",
    REPO_ROOT / "docs" / "precomputed_signals_gui_command_preview_operator_notes.md",
    REPO_ROOT / "docs" / "precomputed_signals_gui_selection_diagnostics.md",
    CHECKLIST_DOC,
)
GUI_SOURCE_PATHS = (
    REPO_ROOT / "app" / "app" / "gui" / "main_window.py",
    REPO_ROOT / "app" / "app" / "gui" / "precomputed_signal_picker.py",
    REPO_ROOT / "app" / "app" / "gui" / "chart_dialog.py",
    REPO_ROOT / "app" / "app" / "gui" / "result_chart.py",
    REPO_ROOT / "app" / "app" / "gui" / "exchange_registry.py",
    REPO_ROOT / "precomputed_signals_gui_adapter.py",
)


def _doc_text() -> str:
    return "\n".join(path.read_text(encoding="utf-8") for path in DOC_PATHS if path.exists())


def _checklist_text() -> str:
    assert CHECKLIST_DOC.exists()
    return CHECKLIST_DOC.read_text(encoding="utf-8")


def _git_diff_added_lines(paths: tuple[Path, ...]) -> list[str]:
    rel_paths = [str(path.relative_to(REPO_ROOT)).replace("\\", "/") for path in paths]
    diff = subprocess.run(
        ["git", "diff", "--", *rel_paths],
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


def test_gui_smoke_checklist_doc_exists_and_records_phase14_scope() -> None:
    text = _checklist_text()

    for expected in (
        "Free Phase 14 is GUI smoke checklist / docs-only hardening",
        "No runtime source change",
        "No GUI source change",
        "No execution path change",
        "No LIVE/PAPER/order connection",
        "GUI smoke is optional",
        "APP_VERSION unchanged",
        "Package/release not touched",
        "Generated real tape body / raw market data excluded",
        "max_drawdown` remains signed negative legacy field",
        "GUI display prefers `max_dd_abs / max_dd_pct`",
        "Raw trade rows are never displayed",
    ):
        assert expected in text


def test_gui_smoke_checklist_records_no_execution_and_no_network_boundary() -> None:
    text = _checklist_text()

    for expected in (
        "GUI smoke must not start LIVE",
        "GUI smoke must not start PAPER",
        "GUI smoke must not submit/fetch orders",
        "GUI smoke must not fetch balance",
        "GUI smoke must not call MEXC private API",
        "GUI smoke must not auto-run producer",
        "GUI smoke must not auto-run backtest",
        "GUI smoke must not auto-run runner",
        "GUI smoke must not auto-run inventory scan",
        "Command preview is preview-only",
        "Copy UX is clipboard-only",
        "No subprocess / QProcess / background worker",
        "`os.system`",
        "`Popen`",
        "`startDetached`",
        "threading",
        "multiprocessing",
    ):
        assert expected in text


def test_expected_visible_labels_are_fixed_without_screenshots() -> None:
    text = _checklist_text()

    for label in (
        "Precomputed Signal Tape",
        "Selection diagnostics",
        "Selection diagnostics: valid",
        "Selection diagnostics: invalid",
        "Safe error code",
        "Files: manifest OK / summary OK / trades.csv OK",
        "Safety: research-only OK / no live-paper execution OK",
        "Fast path: Backtest OK / Replay OK",
        "LIVE/PAPER: not selectable",
        "Max DD (abs, display)",
        "Max DD pct (display)",
        "No raw trade rows are displayed",
        "This panel does not execute commands",
        "Preview only",
        "Execution disabled",
        "Copy backtest command",
        "Copy replay command",
        "Copied",
        "Run this command manually in a terminal if needed",
    ):
        assert label in text


def test_expected_disabled_and_invalid_states_are_fixed() -> None:
    text = _checklist_text()

    for expected in (
        "live_command_available=false",
        "paper_command_available=false",
        "execution_enabled=false",
        "not_selectable_for_live=true",
        "not_selectable_for_paper=true",
        "Invalid selection disables command copy",
        "Missing manifest shows a safe error code only",
        "Missing summary shows a safe error code only",
        "Missing `trades.csv` shows a safe error code only",
        "LIVE/PAPER command text is unavailable",
    ):
        assert expected in text


def test_allowed_and_forbidden_display_fields_are_fixed() -> None:
    text = _checklist_text()

    for allowed in (
        "`signal_dir`",
        "`symbol`",
        "`entry_tf`",
        "`filter_tf`",
        "`signal_set_id`",
        "`dataset_id`",
        "`trade_count`",
        "`net_total`",
        "`final_equity`",
        "`max_dd_display_abs`",
        "`max_dd_display_pct`",
        "`max_dd_display_label`",
        "`status`",
        "`safe_error_code`",
        "manifest hash",
        "summary hash",
        "`trades.csv` hash from manifest",
        "compact path",
        "compact hash",
    ):
        assert allowed in text

    for forbidden in (
        "raw trades rows",
        "`entry_exec`",
        "`exit_exec`",
        "`qty`",
        "exact trade id",
        "order id",
        "raw order",
        "balance",
        "API key",
        "secret",
        "token",
        "authorization",
        "raw billing",
        "raw market data",
        "raw OHLCV",
        "generated real signal tape body",
    ):
        assert forbidden in text


def test_operator_pass_fail_and_future_phase_boundaries_are_fixed() -> None:
    text = _checklist_text()

    for expected in (
        "## Operator Pass Criteria",
        "## Operator Fail Criteria",
        "Pass when all of these are true",
        "Fail and stop when any of these occur",
        "Screenshots are optional",
        "if any live/paper/order path starts",
        "Static/helper tests pass even when GUI runtime smoke is not performed",
        "Future execution button is not part of Phase 14",
        "Backtest/replay execution from the GUI requires a future explicit confirmation",
        "LIVE/PAPER/order must remain separated",
    ):
        assert expected in text


def test_related_docs_reference_phase14_docs_only_smoke_policy() -> None:
    text = _doc_text()

    for expected in (
        "Phase 14 is GUI smoke checklist / docs-only hardening",
        "GUI smoke is optional",
        "GUI smoke does not start LIVE / PAPER / order execution",
        "Command preview is preview-only",
        "Copy UX is clipboard-only",
        "APP_VERSION unchanged",
        "Package/release not touched",
        "Generated real tape body / raw market data excluded",
        "producer/backtest/runner/inventory",
        "does not execute commands",
        "does not connect to LIVE/PAPER/order",
        "no subprocess / QProcess / background worker",
        "raw trades rows",
        "entry_exec",
        "exit_exec",
        "qty",
        "trade id",
        "API key",
        "secret",
        "token",
        "authorization",
        "raw order",
        "balance",
        "raw billing",
    ):
        assert expected in text


def test_gui_source_has_no_phase14_added_execution_network_or_order_connections() -> None:
    added_gui_lines = "\n".join(_git_diff_added_lines(GUI_SOURCE_PATHS))

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
        "precompute_signals.py",
        "precomputed_signals_inventory.py",
        "--mode live",
        "--mode paper",
        "LIVE",
        "PAPER",
        "order",
        "MEXC",
        "balance",
        "fetch",
        "submit",
        "fetch_balance",
        "fetch_order",
        "create_order",
        "submit_order",
    ):
        assert forbidden not in added_gui_lines

    assert not re.search(
        r"QPushButton\([^)]*(Run|Execute|Start)[^)]*precomputed",
        added_gui_lines,
        flags=re.IGNORECASE,
    )


def test_app_version_unchanged_and_package_release_artifacts_not_staged() -> None:
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
    forbidden_names = ("setup", "installer", "package", "release")
    for name in staged:
        lowered = name.lower()
        assert not lowered.endswith(forbidden_suffixes)
        assert not any(token in lowered for token in forbidden_names)
