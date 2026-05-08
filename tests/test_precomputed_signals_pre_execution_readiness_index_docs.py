# BUILD_ID: 2026-05-09_free_precomputed_pre_execution_readiness_index_docs_v1
from __future__ import annotations

import re
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
READINESS_DOC = REPO_ROOT / "docs" / "precomputed_signals_pre_execution_readiness_index.md"
HANDOFF_DOC = REPO_ROOT / "docs" / "precomputed_signals_pre_execution_handoff_summary.md"
SPEC_DOC = REPO_ROOT / "docs" / "precomputed_signals_spec.md"
DESIGN_DOC = REPO_ROOT / "docs" / "precomputed_signals_gui_local_dry_run_design.md"
APPROVAL_BOUNDARY_DOC = (
    REPO_ROOT / "docs" / "precomputed_signals_gui_local_dry_run_approval_boundary.md"
)
EXECUTION_AUDIT_DOC = (
    REPO_ROOT / "docs" / "precomputed_signals_gui_local_dry_run_execution_audit_schema.md"
)
DOC_PATHS = (
    READINESS_DOC,
    HANDOFF_DOC,
    SPEC_DOC,
    DESIGN_DOC,
    APPROVAL_BOUNDARY_DOC,
    EXECUTION_AUDIT_DOC,
)
GUI_SOURCE_PATHS = (
    REPO_ROOT / "app" / "app" / "gui" / "main_window.py",
    REPO_ROOT / "app" / "app" / "gui" / "precomputed_signal_picker.py",
    REPO_ROOT / "app" / "app" / "gui" / "chart_dialog.py",
    REPO_ROOT / "app" / "app" / "gui" / "result_chart.py",
    REPO_ROOT / "app" / "app" / "gui" / "exchange_registry.py",
    REPO_ROOT / "precomputed_signals_gui_adapter.py",
    REPO_ROOT / "precomputed_signals_local_dry_run_gui_adapter.py",
)
RUNTIME_AND_HELPER_SOURCE_PATHS = (
    REPO_ROOT / "backtest.py",
    REPO_ROOT / "runner.py",
    REPO_ROOT / "precompute_signals.py",
    REPO_ROOT / "fast_backtest_signals.py",
    REPO_ROOT / "signal_tape.py",
    REPO_ROOT / "precomputed_signals_inventory.py",
    REPO_ROOT / "precomputed_signals_selection.py",
    REPO_ROOT / "precomputed_signals_gui_adapter.py",
    REPO_ROOT / "precomputed_signals_local_dry_run_request.py",
    REPO_ROOT / "precomputed_signals_local_dry_run_gui_adapter.py",
)
EXISTING_GUARD_TESTS = (
    "tests/test_signal_tape_schema.py",
    "tests/test_fast_backtest_signals_no_network.py",
    "tests/test_fast_backtest_signals_accounting.py",
    "tests/test_precomputed_signals_dd_schema.py",
    "tests/test_precomputed_signals_backtest_cli.py",
    "tests/test_precomputed_signals_runner_replay_cli.py",
    "tests/test_precompute_signals_safe_producer.py",
    "tests/test_precomputed_signals_inventory.py",
    "tests/test_precomputed_signals_selection_contract.py",
    "tests/test_precomputed_signals_gui_adapter.py",
    "tests/test_precomputed_signals_gui_picker_wiring.py",
    "tests/test_precomputed_signals_gui_command_preview.py",
    "tests/test_precomputed_signals_gui_command_copy_ux.py",
    "tests/test_precomputed_signals_gui_command_copy_accessibility.py",
    "tests/test_precomputed_signals_gui_selection_diagnostics.py",
    "tests/test_precomputed_signals_gui_selection_diagnostics_polish.py",
    "tests/test_precomputed_signals_gui_smoke_checklist_docs.py",
    "tests/test_precomputed_signals_gui_manual_smoke_record_docs.py",
    "tests/test_precomputed_signals_gui_manual_smoke_record_schema.py",
    "tests/test_precomputed_signals_gui_local_dry_run_design_docs.py",
    "tests/test_precomputed_signals_gui_local_dry_run_request_schema_docs.py",
    "tests/test_precomputed_signals_local_dry_run_request_builder.py",
    "tests/test_precomputed_signals_local_dry_run_gui_adapter.py",
    "tests/test_precomputed_signals_local_dry_run_gui_preview_wiring.py",
    "tests/test_precomputed_signals_gui_local_dry_run_approval_boundary_docs.py",
    "tests/test_precomputed_signals_gui_local_dry_run_approval_record_docs.py",
    "tests/test_precomputed_signals_gui_local_dry_run_execution_audit_docs.py",
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _doc_text() -> str:
    return "\n".join(_read(path) for path in DOC_PATHS)


def _relative_paths(paths: tuple[Path, ...]) -> list[str]:
    return [str(path.relative_to(REPO_ROOT)) for path in paths]


def _git_changed_names(paths: tuple[Path, ...]) -> set[str]:
    return {
        line.strip()
        for line in subprocess.run(
            ["git", "diff", "--name-only", "HEAD", "--", *_relative_paths(paths)],
            cwd=REPO_ROOT,
            check=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ).stdout.splitlines()
        if line.strip()
    }


def _git_diff_added_lines(paths: tuple[Path, ...]) -> str:
    diff = subprocess.run(
        ["git", "diff", "--unified=0", "HEAD", "--", *_relative_paths(paths)],
        cwd=REPO_ROOT,
        check=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout
    added_lines = []
    for line in diff.splitlines():
        if line.startswith("+") and not line.startswith("+++"):
            added_lines.append(line[1:])
    return "\n".join(added_lines)


def _staged_names() -> list[str]:
    return subprocess.run(
        ["git", "diff", "--cached", "--name-only"],
        cwd=REPO_ROOT,
        check=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout.splitlines()


def _app_version_from(text: str) -> str:
    match = re.search(r'^APP_VERSION\s*=\s*["\']([^"\']+)["\']', text, flags=re.MULTILINE)
    assert match is not None
    return match.group(1)


def test_readiness_index_and_handoff_summary_docs_exist() -> None:
    assert READINESS_DOC.exists()
    assert HANDOFF_DOC.exists()


def test_phase25_scope_and_non_execution_boundaries_are_documented() -> None:
    text = _doc_text()

    for expected in (
        "Phase 25",
        "docs/tests-only",
        "no GUI source change",
        "no runtime source change",
        "no dry-run execution implementation",
        "no approval UI",
        "no command execution",
        "no subprocess / QProcess / background worker",
        "no LIVE/PAPER/order",
        "no MEXC private API",
        "no approval/audit record runtime generation",
        "no APP_VERSION bump",
        "no package/release/signing/upload",
    ):
        assert expected in text


def test_completed_phases_table_lists_phase1_through_phase24() -> None:
    readiness = _read(READINESS_DOC)
    handoff = _read(HANDOFF_DOC)

    assert "completed phases table" in readiness
    assert "Completed Phases Table" in handoff
    for phase_number in range(1, 25):
        assert f"| Phase {phase_number} |" in readiness
        assert f"| Phase {phase_number} |" in handoff


def test_required_sections_are_documented() -> None:
    readiness = _read(READINESS_DOC)

    for expected in (
        "## Current Non-Capabilities / Not Implemented",
        "## Required Pre-Execution Gates",
        "## Required Future Approval Flow",
        "## Required Future Execution Flow",
        "## Required Future Fail-Closed List",
        "## Allowed Artifacts Summary",
        "## Forbidden Artifacts Summary",
        "## Stop / Pause Recommendation",
        "## Handoff Summary",
    ):
        assert expected in readiness


def test_current_non_capabilities_are_fixed() -> None:
    text = _doc_text()

    for expected in (
        "no runtime local dry-run execution",
        "no GUI dry-run button",
        "no approval UI",
        "no confirm/approve/execute button",
        "no command execution from GUI",
        "no producer/backtest/runner/inventory auto-run from GUI",
        "no LIVE/PAPER/order connection",
        "no MEXC private API",
        "no balance/order fetch",
        "no subprocess / QProcess / background worker",
        "no generated approval record at runtime",
        "no generated execution audit record at runtime",
        "no packaging/release/signing/upload",
        "no APP_VERSION bump",
    ):
        assert expected in text


def test_required_pre_execution_gates_are_documented() -> None:
    text = _doc_text()

    for expected in (
        "user explicitly approves moving beyond docs/tests-only",
        "implementation phase explicitly named",
        "current branch / HEAD recorded",
        "worktree state reviewed",
        "APP_VERSION policy decided",
        "package/release artifacts excluded",
        "selected signal tape exists",
        "selection contract valid",
        "request schema valid",
        "request builder output valid",
        "GUI preview adapter output valid",
        "approval preflight passed",
        "approval record generated in future phase only",
        "approval_record_hash / request_hash / selection_hash linked",
        "output_dir safe",
        "generated artifact policy accepted",
        "execution audit schema ready",
        "no LIVE/PAPER/order path",
        "no private API path",
        "no background execution",
        "fail-closed conditions covered",
        "rollback / cleanup plan for generated local-only artifacts",
        "repo-external output location decided",
    ):
        assert expected in text


def test_required_future_approval_and_execution_flows_are_documented() -> None:
    text = _doc_text()

    for expected in (
        "approval is local_saved_tape_backtest_replay_only",
        "approval is not LIVE/PAPER/order/private API/release/package approval",
        "approval requires explicit operator confirmation",
        "approval must show signal_dir, product, symbol, timeframe, dry_run_mode, output_dir",
        "approval must show no private API / no balance fetch / no order fetch / no order submit",
        "approval must show allowed/forbidden artifacts",
        "approval must fail closed if any required field/check is missing",
        "approval record and execution audit record are separate",
        "validate selection",
        "validate request",
        "validate approval record",
        "verify approval_record_hash / request_hash / selection_hash linkage",
        "run local saved-tape fast path only",
        "write allowed artifacts only to safe output_dir",
        "generate execution audit record",
        "verify no live/paper/order/private API/background execution",
        "do not touch package/release assets",
        "do not commit generated artifacts",
    ):
        assert expected in text


def test_required_future_fail_closed_list_is_documented() -> None:
    text = _doc_text()

    for expected in (
        "missing approval",
        "missing approval_record_hash",
        "missing request_hash",
        "invalid selection",
        "invalid request",
        "invalid approval_scope",
        "operator confirmation missing",
        "live/paper requested",
        "order/balance requested",
        "private API requested",
        "background execution requested",
        "unsafe output_dir",
        "forbidden field",
        "positive legacy max_drawdown",
        "output artifact policy violation",
        "package/release path involved",
        "worktree status unexpected",
        "unknown safety violation",
    ):
        assert expected in text


def test_allowed_and_forbidden_artifacts_are_documented() -> None:
    text = _doc_text()

    for expected in (
        "safe summary JSON",
        "fast_summary.json",
        "equity_curve.csv",
        "safe-condition trades.csv",
        "local output manifest",
        "approval record",
        "execution audit record",
        "sanitized manual smoke record",
        "safe metadata log",
        "raw market data",
        "raw OHLCV",
        "raw trades rows",
        "entry_exec",
        "exit_exec",
        "qty",
        "trade_id",
        "API key",
        "secret",
        "token",
        "authorization",
        "raw billing",
        "order",
        "balance",
        "package zip",
        "exe",
        "installer",
        "release asset",
        "generated artifacts committed to repo",
    ):
        assert expected in text


def test_stop_pause_handoff_and_zip_convention_are_documented() -> None:
    text = _doc_text()

    for expected in (
        "safe to pause after Phase 25",
        "runtime dry-run execution requires explicit new approval",
        "packaging/release remains paused while Pro plan changes are in progress",
        "LoneWolf_Fang_Free_Package.zip do not stage unless packaging phase approved",
        "repo-external zip convention",
        "latest Free commit",
        "current branch",
        "worktree note",
        "publish/pairs-live-api-20260423-free",
        "free_precomputed_signals_phase25_pre_execution_readiness_index_<timestamp>.zip",
    ):
        assert expected in text


def test_gui_and_runtime_sources_have_no_phase25_changes() -> None:
    assert _git_changed_names(GUI_SOURCE_PATHS) == set()
    assert _git_changed_names(RUNTIME_AND_HELPER_SOURCE_PATHS) == set()


def test_gui_source_added_lines_have_no_execution_or_order_connection() -> None:
    added_source = _git_diff_added_lines(GUI_SOURCE_PATHS)

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
        "LIVE",
        "PAPER",
        "order",
        "MEXC",
        "balance",
        "fetch",
        "submit",
        "Run",
        "Execute",
        "Start",
        "Dry Run",
        "Confirm",
        "Approve",
    ):
        assert forbidden not in added_source


def test_existing_phase_guard_tests_are_present_for_continued_validation_coverage() -> None:
    for path in EXISTING_GUARD_TESTS:
        assert (REPO_ROOT / path).exists()


def test_app_version_unchanged_and_package_release_artifacts_not_staged() -> None:
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

    forbidden_suffixes = (".zip", ".exe", ".msi", ".apk")
    forbidden_names = ("setup", "installer", "package", "release")
    for name in _staged_names():
        lowered = name.lower()
        assert not lowered.endswith(forbidden_suffixes)
        assert not any(token in lowered for token in forbidden_names)
