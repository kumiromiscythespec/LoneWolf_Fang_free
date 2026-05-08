# BUILD_ID: 2026-05-09_free_precomputed_local_dry_run_approval_boundary_docs_v1
from __future__ import annotations

import re
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
BOUNDARY_DOC = REPO_ROOT / "docs" / "precomputed_signals_gui_local_dry_run_approval_boundary.md"
DESIGN_DOC = REPO_ROOT / "docs" / "precomputed_signals_gui_local_dry_run_design.md"
REQUEST_SCHEMA_DOC = REPO_ROOT / "docs" / "precomputed_signals_gui_local_dry_run_request_schema.md"
SPEC_DOC = REPO_ROOT / "docs" / "precomputed_signals_spec.md"
DOC_PATHS = (BOUNDARY_DOC, DESIGN_DOC, REQUEST_SCHEMA_DOC, SPEC_DOC)
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
)


def _boundary_text() -> str:
    assert BOUNDARY_DOC.exists()
    return BOUNDARY_DOC.read_text(encoding="utf-8")


def _doc_text() -> str:
    return "\n".join(path.read_text(encoding="utf-8") for path in DOC_PATHS)


def _git_diff_added_lines(paths: tuple[Path, ...]) -> str:
    rel_paths = [str(path.relative_to(REPO_ROOT)).replace("\\", "/") for path in paths]
    output = subprocess.run(
        ["git", "diff", "--", *rel_paths],
        cwd=REPO_ROOT,
        check=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout
    return "\n".join(line for line in output.splitlines() if line.startswith("+") and not line.startswith("+++"))


def _git_changed_names(paths: tuple[Path, ...]) -> set[str]:
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


def test_approval_boundary_doc_exists_and_records_phase22_docs_only_scope() -> None:
    text = _boundary_text()

    for expected in (
        "Phase 22 is local-only dry-run execution approval boundary",
        "Phase 22 is docs/tests-only",
        "no approval UI implementation",
        "no dry-run execution implementation",
        "no approval record generation",
        "no execution audit record generation",
        "no GUI source change",
        "no runtime source change",
        "no command execution",
        "no subprocess / QProcess / background worker",
        "no LIVE/PAPER/order",
        "no MEXC private API",
        "APP_VERSION unchanged",
        "package/release not touched",
        "future execution requires separate explicit approval phase",
    ):
        assert expected in text


def test_approval_definition_is_local_only_and_not_live_paper_order() -> None:
    text = _boundary_text()

    for expected in (
        "explicit operator approval required",
        "approval is not live/paper/order approval",
        "approval is not LIVE approval",
        "approval is not PAPER approval",
        "approval is not order approval",
        "approval is not private API approval",
        "approval is only for local saved-tape backtest/replay fast path",
        "approval must be separate from command copy UX",
        "approval must be separate from manual smoke record",
        "approval must be separate from release/package approval",
    ):
        assert expected in text


def test_required_operator_confirmation_text_is_fixed() -> None:
    text = _boundary_text()

    for expected in (
        "Required Operator Confirmation Text",
        "This is local-only",
        "This is not LIVE/PAPER/order",
        "No MEXC private API",
        "No balance fetch",
        "No order fetch",
        "No order submit",
        "Uses selected precomputed signal tape only",
        "Product: free",
        "`signal_dir`",
        "`symbol`",
        "`entry_tf`",
        "`filter_tf`",
        "`dry_run_mode`",
        "`output_dir`",
        "operator understands generated artifacts policy",
        "operator understands execution is local saved-tape fast path only",
        "`operator_confirmed=false`",
        "`execution_enabled=false`",
    ):
        assert expected in text


def test_future_required_pre_approval_gates_are_fixed() -> None:
    text = _boundary_text()

    for expected in (
        "Future Required Pre-Approval Gates",
        "valid selection contract",
        "valid request schema",
        "`product == free`",
        "`dry_run_mode in allowed enum`",
        "`operator_confirmed=false before confirmation`",
        "`execution_enabled_after_confirmation=false before confirmation`",
        "`not_selectable_for_live=true`",
        "`not_selectable_for_paper=true`",
        "`safety_research_only=true`",
        "`paper_live_order_execution=false`",
        "manifest present",
        "summary present",
        "`trades.csv` present",
        "manifest hash present",
        "summary hash present",
        "`trades.csv` hash from manifest present",
        "forbidden fields rejected",
        "positive legacy max_drawdown rejected",
        "output_dir safe",
        "worktree status recorded",
        "package/release artifacts excluded",
        "APP_VERSION unchanged unless future release phase explicitly approves",
        "no background execution",
        "no private API",
        "no order/balance path",
    ):
        assert expected in text


def test_allowed_and_not_allowed_execution_modes_are_fixed() -> None:
    text = _boundary_text()

    for expected in (
        "Allowed Execution Modes",
        "`backtest_fast_path_local_only`",
        "`runner_replay_fast_path_local_only`",
        "Not Allowed Modes",
        "`live`",
        "`paper`",
        "`order_submit`",
        "`order_fetch`",
        "`balance_fetch`",
        "`private_api`",
        "`producer_auto_run`",
        "`inventory_auto_scan`",
        "`background_worker`",
        "`release_packaging`",
        "`external_network`",
        "`live/paper/order/private_api/background_worker` are not allowed",
    ):
        assert expected in text


def test_approval_record_schema_is_fixed_but_not_generated() -> None:
    text = _boundary_text()

    for expected in (
        "Approval Record Schema",
        "`request_type`: `precomputed_signal_local_dry_run_approval_record`",
        "`record_type`: `precomputed_signal_local_dry_run_approval_record`",
        "approval_scope enum",
        "`local_saved_tape_backtest_replay_only`",
        "approved_actions allowed",
        "`approve_backtest_fast_path_local_only`",
        "`approve_runner_replay_fast_path_local_only`",
        "forbidden_actions required",
        "`live`",
        "`paper`",
        "`order_submit`",
        "`order_fetch`",
        "`balance_fetch`",
        "`private_api`",
        "`producer_auto_run`",
        "`inventory_auto_scan`",
        "`background_worker`",
        "`package_release`",
        "status enum",
        "`approved`",
        "`rejected`",
        "`blocked`",
        "`invalid`",
        "`not_run`",
        "Phase 22 must not create approval record files",
    ):
        assert expected in text


def test_execution_audit_record_boundary_is_fixed_but_not_generated() -> None:
    text = _boundary_text()

    for expected in (
        "Execution Audit Record Boundary",
        "`record_type`: `precomputed_signal_local_dry_run_execution_record`",
        "`approval_record_hash`",
        "`request_hash`",
        "`execution_started_at_utc`",
        "`execution_finished_at_utc`",
        "`dry_run_mode`",
        "`result_status`",
        "`output_artifact_manifest`",
        "`safe_summary`",
        "`fail_closed_reason`",
        "`no_live_paper_order_assertion`",
        "`no_private_api_assertion`",
        "`no_background_execution_assertion`",
        "raw trades rows",
        "`entry_exec`",
        "`exit_exec`",
        "`qty`",
        "trade id",
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
        "Phase 22 must not create execution audit records",
    ):
        assert expected in text


def test_allowed_and_forbidden_artifacts_are_fixed() -> None:
    text = _boundary_text()

    for expected in (
        "Allowed Artifacts",
        "safe summary JSON",
        "`fast_summary.json`",
        "`equity_curve.csv`",
        "`trades.csv` only if safe-condition / saved-tape-compatible",
        "local-only output manifest",
        "approval record",
        "execution audit record",
        "sanitized manual smoke record",
        "Forbidden Artifacts",
        "raw market data",
        "raw OHLCV",
        "raw trades rows",
        "`entry_exec`",
        "`exit_exec`",
        "`qty`",
        "`trade_id`",
        "`order_id`",
        "raw_order",
        "balance_snapshot",
        "api_key",
        "secret",
        "token",
        "authorization",
        "raw_billing",
        "screenshots with secrets/balances/orders/account details",
        "package_zip",
        "exe",
        "installer",
        "release_asset",
        "runtime dirs copied into repo",
        "zip inside zip",
    ):
        assert expected in text


def test_future_fail_closed_conditions_and_existing_phase_relationship_are_fixed() -> None:
    text = _boundary_text()

    for expected in (
        "Future Fail-Closed Conditions",
        "operator confirmation missing",
        "confirmation text mismatch",
        "approval_scope invalid",
        "request invalid",
        "selection invalid",
        "`product != free`",
        "live/paper/order/private API requested",
        "background execution requested",
        "output path unsafe",
        "forbidden fields detected",
        "positive legacy max_drawdown detected",
        "approval record missing in future execution phase",
        "approval record does not match request hash",
        "approval record must match request hash",
        "worktree status unexpected",
        "package/release artifact path involved",
        "Existing Phases Relationship",
        "Phase 9 command preview remains preview-only",
        "Phase 10 copy UX remains clipboard-only",
        "Phase 15/16 manual smoke record remains display-only proof, not execution proof",
        "Phase 18 request schema is planning artifact, not execution proof",
        "Phase 19 request builder creates preview only",
        "Phase 20 GUI preview adapter creates display item only",
        "Phase 21 GUI wiring displays preview only",
        "Phase 22 defines approval boundary only",
        "future execution requires separate explicit approval phase",
    ):
        assert expected in text


def test_related_docs_reference_phase22_approval_boundary() -> None:
    text = _doc_text()

    for expected in (
        "Free Phase 22: local-only dry-run execution approval boundary",
        "docs/precomputed_signals_gui_local_dry_run_approval_boundary.md",
        "no approval UI implementation",
        "no dry-run execution implementation",
        "no approval record generation",
        "no execution audit record generation",
        "no GUI source change",
        "no runtime source change",
        "no command execution",
        "no subprocess / QProcess / background worker",
        "no LIVE/PAPER/order",
        "no MEXC private API",
        "approval is not live/paper/order approval",
        "future execution requires separate explicit approval phase",
    ):
        assert expected in text


def test_gui_runtime_and_helper_sources_have_no_phase22_execution_or_network_changes() -> None:
    assert _git_changed_names(GUI_SOURCE_PATHS) == set()
    assert _git_changed_names(RUNTIME_AND_HELPER_SOURCE_PATHS) == set()

    combined_added_source = "\n".join(
        (
            _git_diff_added_lines(GUI_SOURCE_PATHS),
            _git_diff_added_lines(RUNTIME_AND_HELPER_SOURCE_PATHS),
        )
    )
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
        "LIVE",
        "PAPER",
        "order",
        "balance",
        "fetch",
        "submit",
        "create_order",
        "submit_order",
        "Run",
        "Execute",
        "Start",
        "Dry Run",
        "Confirm",
        "Approve",
    ):
        assert forbidden not in combined_added_source


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
