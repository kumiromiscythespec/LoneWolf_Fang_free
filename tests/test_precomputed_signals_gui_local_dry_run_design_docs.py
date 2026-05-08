# BUILD_ID: 2026-05-09_free_precomputed_local_dry_run_gui_preview_wiring_v1
# BUILD_ID: 2026-05-09_free_precomputed_local_dry_run_request_schema_docs_v1
from __future__ import annotations

import re
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DESIGN_DOC = REPO_ROOT / "docs" / "precomputed_signals_gui_local_dry_run_design.md"
REQUEST_SCHEMA_DOC = REPO_ROOT / "docs" / "precomputed_signals_gui_local_dry_run_request_schema.md"
DOC_PATHS = (
    REPO_ROOT / "docs" / "precomputed_signals_spec.md",
    REPO_ROOT / "docs" / "precomputed_signals_gui_smoke_procedure.md",
    REPO_ROOT / "docs" / "precomputed_signals_gui_smoke_checklist.md",
    REPO_ROOT / "docs" / "precomputed_signals_gui_manual_smoke_record.md",
    DESIGN_DOC,
    REQUEST_SCHEMA_DOC,
)
GUI_SOURCE_PATHS = (
    REPO_ROOT / "app" / "app" / "gui" / "main_window.py",
    REPO_ROOT / "app" / "app" / "gui" / "precomputed_signal_picker.py",
    REPO_ROOT / "app" / "app" / "gui" / "chart_dialog.py",
    REPO_ROOT / "app" / "app" / "gui" / "result_chart.py",
    REPO_ROOT / "app" / "app" / "gui" / "exchange_registry.py",
    REPO_ROOT / "precomputed_signals_gui_adapter.py",
)
RUNTIME_SOURCE_PATHS = (
    REPO_ROOT / "backtest.py",
    REPO_ROOT / "runner.py",
    REPO_ROOT / "precompute_signals.py",
    REPO_ROOT / "fast_backtest_signals.py",
    REPO_ROOT / "signal_tape.py",
    REPO_ROOT / "precomputed_signals_inventory.py",
    REPO_ROOT / "precomputed_signals_selection.py",
    REPO_ROOT / "precomputed_signals_gui_adapter.py",
)


def _design_text() -> str:
    assert DESIGN_DOC.exists()
    return DESIGN_DOC.read_text(encoding="utf-8")


def _doc_text() -> str:
    return "\n".join(path.read_text(encoding="utf-8") for path in DOC_PATHS)


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


def _git_diff_names(paths: tuple[Path, ...]) -> list[str]:
    rel_paths = [str(path.relative_to(REPO_ROOT)).replace("\\", "/") for path in paths]
    return subprocess.run(
        ["git", "diff", "--name-only", "--", *rel_paths],
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


def test_local_dry_run_design_doc_exists_and_records_phase17_scope() -> None:
    text = _design_text()

    for expected in (
        "Phase 17 is future local-only dry-run design boundary",
        "docs/tests-only",
        "no local dry-run implementation",
        "no GUI source change",
        "no runtime source change",
        "no command execution",
        "no subprocess / QProcess / background worker",
        "no producer/backtest/runner/inventory auto-run",
        "no LIVE/PAPER/order",
        "no MEXC private API",
        "APP_VERSION unchanged",
        "package/release not touched",
        "generated real tape body / raw market data excluded",
        "raw trade rows are never displayed or recorded",
        "future local-only dry-run requires explicit operator confirmation",
        "future local-only dry-run is not live/paper/order",
        "future execution must be a separate phase",
    ):
        assert expected in text


def test_current_phase17_boundary_keeps_preview_copy_diagnostics_and_manual_record_read_only() -> None:
    text = _design_text()

    for expected in (
        "Current Phase 17 boundary",
        "no dry-run is executed",
        "no command is executed",
        "command preview remains preview-only",
        "copy UX remains clipboard-only",
        "GUI diagnostics remain read-only",
        "manual smoke record remains display-only",
        "manual smoke record remains synthetic fixture only",
        "Phase 17 must not create actual request files",
    ):
        assert expected in text


def test_phase18_request_schema_boundary_is_docs_tests_only_and_linked() -> None:
    text = _doc_text()

    for expected in (
        "Free Phase 18 is local-only dry-run request schema docs/test-only",
        "docs/precomputed_signals_gui_local_dry_run_request_schema.md",
        "docs/precomputed_signals_gui_local_dry_run_request_sample.json",
        "no request builder implementation",
        "no generated request file",
        "operator_confirmed=false by default",
        "execution_enabled_after_confirmation=false",
        "Phase 18 does not implement request builder",
        "Phase 18 does not create request files",
        "Phase 18 does not execute local dry-run",
        "future runtime dry-run execution must be separate and explicitly approved",
    ):
        assert expected in text


def test_future_operator_confirmation_requirements_are_fixed() -> None:
    text = _design_text()

    for expected in (
        "Future dry-run must require explicit operator confirmation",
        "must be separate from command preview copy UX",
        "must fail closed if the selection is invalid",
        "this is local-only",
        "this is not LIVE/PAPER/order",
        "`signal_dir`",
        "product `free`",
        "`symbol`",
        "`entry_tf`",
        "`filter_tf`",
        "`not_selectable_for_live=true`",
        "`not_selectable_for_paper=true`",
        "output directory",
        "The default state is unconfirmed",
    ):
        assert expected in text


def test_future_preflight_and_fail_closed_conditions_are_fixed() -> None:
    text = _design_text()

    for expected in (
        "Required Future Preflight Checks",
        "`signal_dir` exists",
        "selection contract valid",
        "`product == free`",
        "`safety_scope research_only=true`",
        "`paper_live_order_execution=false`",
        "`not_selectable_for_live=true`",
        "`not_selectable_for_paper=true`",
        "`manifest.json` present",
        "`summary.json` present",
        "`trades.csv` present",
        "manifest / summary hash metadata present",
        "positive legacy max_drawdown rejected",
        "forbidden fields rejected",
        "no raw trades rows displayed",
        "output dir is repo-external or safe runtime export dir",
        "package/release assets not touched",
        "APP_VERSION unchanged",
        "worktree state is reported",
        "generated artifacts excluded from commit/zip unless explicitly summarized",
        "fail-closed conditions",
        "missing manifest",
        "unsafe manifest",
        "forbidden field",
        "positive legacy max_drawdown",
        "order/balance/private API requested",
        "operator confirmation missing",
        "background execution requested",
    ):
        assert expected in text


def test_allowed_and_not_allowed_dry_run_modes_are_fixed() -> None:
    text = _design_text()
    allowed_section = re.search(r"## Allowed dry_run_mode enum(?P<body>.*?)## Not Allowed Modes", text, re.S)
    assert allowed_section is not None
    allowed = allowed_section.group("body")

    assert "Allowed dry_run_mode enum" in text
    assert "`backtest_fast_path_local_only`" in allowed
    assert "`runner_replay_fast_path_local_only`" in allowed

    not_allowed_section = re.search(r"## Not Allowed Modes(?P<body>.*?)## Required Future Dry-Run Request Schema", text, re.S)
    assert not_allowed_section is not None
    not_allowed = not_allowed_section.group("body")
    for mode in (
        "live",
        "paper",
        "order_submit",
        "order_fetch",
        "balance_fetch",
        "private_api",
        "producer_auto_run",
        "inventory_auto_scan",
        "background_worker",
    ):
        assert f"`{mode}` (not allowed)" in not_allowed


def test_future_dry_run_request_schema_fields_are_fixed() -> None:
    text = _design_text()

    for field in (
        "schema_version",
        "request_type",
        "phase",
        "product",
        "signal_dir",
        "symbol",
        "entry_tf",
        "filter_tf",
        "signal_set_id",
        "dry_run_mode",
        "output_dir",
        "requested_at_utc",
        "operator_confirmation_required",
        "operator_confirmed",
        "preview_only_before_confirmation",
        "execution_enabled_after_confirmation",
        "not_selectable_for_live",
        "not_selectable_for_paper",
        "safety_research_only",
        "paper_live_order_execution",
        "command_text_preview",
        "allowed_artifacts",
        "forbidden_artifacts",
        "status",
        "status_reason",
    ):
        assert f"`{field}`" in text

    for expected in (
        "precomputed_signal_local_dry_run_request",
        "operator_confirmation_required=true",
        "operator_confirmed=false by default",
        "operator_confirmed=true",
        "preview_only_before_confirmation=true",
        "not_selectable_for_live=true",
        "not_selectable_for_paper=true",
        "safety_research_only=true",
        "paper_live_order_execution=false",
        "Phase 17 must not create actual request files",
    ):
        assert expected in text


def test_allowed_and_forbidden_future_artifacts_are_fixed() -> None:
    text = _design_text()

    for allowed in (
        "Allowed Future Output Artifacts",
        "safe summary JSON",
        "`equity_curve.csv` generated by fast path",
        "`trades.csv` generated by fast path only if already redacted / synthetic",
        "`fast_summary.json`",
        "local log with safe metadata only",
        "manual smoke record with safe metadata only",
    ):
        assert allowed in text

    for forbidden in (
        "Forbidden Future Output Artifacts",
        "raw market data",
        "raw OHLCV",
        "raw trades rows",
        "`entry_exec`",
        "`exit_exec`",
        "`qty`",
        "trade id",
        "raw trades rows with entry_exec / exit_exec / qty / trade id",
        "order id",
        "raw order",
        "order",
        "balance snapshot",
        "balance",
        "API key",
        "secret",
        "token",
        "authorization",
        "raw billing",
        "package zip",
        "exe",
        "installer",
        "release assets",
        "runtime dirs copied into repo",
        "zip-in-zip",
        "API key / secret / token / authorization / raw billing / order / balance",
    ):
        assert forbidden in text


def test_related_docs_reference_phase17_boundary_and_manual_smoke_relationship() -> None:
    text = _doc_text()

    for expected in (
        "Phase 17 is future local-only dry-run design boundary",
        "docs/tests-only",
        "no local dry-run implementation",
        "no GUI source change",
        "no runtime source change",
        "no command execution",
        "no subprocess / QProcess / background worker",
        "no producer/backtest/runner/inventory auto-run",
        "no LIVE/PAPER/order",
        "no MEXC private API",
        "future local-only dry-run requires explicit operator confirmation",
        "future local-only dry-run is not live/paper/order",
        "Allowed `dry_run_mode` values are `backtest_fast_path_local_only`",
        "`runner_replay_fast_path_local_only`",
        "`live`, `paper`,",
        "`order_submit`",
        "`order_fetch`",
        "`balance_fetch`",
        "`private_api`",
        "precomputed_signal_local_dry_run_request",
        "operator_confirmed=false by default",
        "Allowed future artifacts",
        "Forbidden future artifacts",
        "future execution must be a separate phase",
        "manual smoke record remains display-only",
        "dry-run result record is a separate future artifact",
    ):
        assert expected in text


def test_gui_and_runtime_sources_have_no_phase17_added_execution_or_network_connections() -> None:
    added_gui_lines = "\n".join(_git_diff_added_lines(GUI_SOURCE_PATHS))
    added_runtime_lines = "\n".join(_git_diff_added_lines(RUNTIME_SOURCE_PATHS))
    combined_added_source = added_gui_lines + "\n" + added_runtime_lines

    assert set(_git_diff_names(GUI_SOURCE_PATHS)).issubset(
        {"app/app/gui/main_window.py", "app/app/gui/precomputed_signal_picker.py"}
    )
    assert _git_diff_names(RUNTIME_SOURCE_PATHS) == []

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
        "MEXC",
        "submit",
        "fetch_balance",
        "fetch_order",
        "create_order",
        "submit_order",
        "Execute",
        "Start",
    ):
        assert forbidden not in combined_added_source

    assert not re.search(
        r"QPushButton\([^)]*(Run|Execute|Start|Dry Run)[^)]*precomputed",
        combined_added_source,
        flags=re.IGNORECASE,
    )


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
    forbidden_suffixes = (".zip", ".exe", ".msi", ".apk")
    forbidden_names = ("setup", "installer", "package", "release")
    for name in staged:
        lowered = name.lower()
        assert not lowered.endswith(forbidden_suffixes)
        assert not any(token in lowered for token in forbidden_names)
