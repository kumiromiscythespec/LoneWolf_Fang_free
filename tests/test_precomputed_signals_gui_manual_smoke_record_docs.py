# BUILD_ID: 2026-05-09_free_precomputed_local_dry_run_gui_preview_wiring_v1
# BUILD_ID: 2026-05-09_free_precomputed_gui_manual_smoke_record_docs_v1
from __future__ import annotations

import re
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
RECORD_DOC = REPO_ROOT / "docs" / "precomputed_signals_gui_manual_smoke_record.md"
DOC_PATHS = (
    REPO_ROOT / "docs" / "precomputed_signals_spec.md",
    REPO_ROOT / "docs" / "precomputed_signals_gui_smoke_procedure.md",
    REPO_ROOT / "docs" / "precomputed_signals_gui_smoke_checklist.md",
    RECORD_DOC,
)
GUI_SOURCE_PATHS = (
    REPO_ROOT / "app" / "app" / "gui" / "main_window.py",
    REPO_ROOT / "app" / "app" / "gui" / "precomputed_signal_picker.py",
    REPO_ROOT / "app" / "app" / "gui" / "chart_dialog.py",
    REPO_ROOT / "app" / "app" / "gui" / "result_chart.py",
    REPO_ROOT / "app" / "app" / "gui" / "exchange_registry.py",
    REPO_ROOT / "precomputed_signals_gui_adapter.py",
)


def _record_text() -> str:
    assert RECORD_DOC.exists()
    return RECORD_DOC.read_text(encoding="utf-8")


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


def _app_version_from(text: str) -> str:
    match = re.search(r'^APP_VERSION\s*=\s*["\']([^"\']+)["\']', text, flags=re.MULTILINE)
    assert match is not None
    return match.group(1)


def test_manual_smoke_record_doc_exists_and_records_phase15_scope() -> None:
    text = _record_text()

    for expected in (
        "Free Phase 15 is synthetic fixture only manual GUI runtime smoke record format",
        "Phase 15 is docs/test-only",
        "GUI runtime smoke is not executed in Phase 15",
        "No GUI source change",
        "No runtime source change",
        "No execution path change",
        "No LIVE/PAPER/order connection",
        "Manual smoke records are safe metadata only",
        "Synthetic fixture only",
        "No real market data",
        "No generated real signal tape body",
        "No command execution",
        "No producer/backtest/runner/inventory auto-run",
        "No LIVE/PAPER/order",
        "No MEXC private API",
        "No subprocess / QProcess / background worker",
        "APP_VERSION unchanged",
        "Package/release not touched",
        "Raw trade rows are never recorded",
        "max_drawdown` remains signed negative legacy field",
        "GUI display prefers `max_dd_abs / max_dd_pct`",
    ):
        assert expected in text


def test_manual_smoke_record_schema_fields_and_enums_are_fixed() -> None:
    text = _record_text()

    for field in (
        "schema_version",
        "record_type",
        "phase",
        "repo",
        "branch",
        "head_commit",
        "app_version",
        "recorded_at_utc",
        "operator",
        "smoke_mode",
        "fixture_type",
        "fixture_origin",
        "signal_dir",
        "product",
        "symbol",
        "entry_tf",
        "filter_tf",
        "signal_set_id",
        "selection_status",
        "safe_error_code",
        "expected_labels_checked",
        "disabled_states_checked",
        "command_preview_checked",
        "copy_ux_checked",
        "diagnostics_checked",
        "live_paper_boundary_checked",
        "raw_trade_rows_not_visible",
        "command_execution_observed",
        "producer_auto_run_observed",
        "backtest_auto_run_observed",
        "runner_auto_run_observed",
        "order_path_observed",
        "balance_fetch_observed",
        "private_api_observed",
        "result_status",
        "result_reason",
        "notes_sanitized",
    ):
        assert field in text

    for expected in (
        "Allowed `result_status` enum",
        "`pass`",
        "`fail`",
        "`blocked`",
        "`not_run`",
        "Allowed `smoke_mode` enum",
        "`synthetic_fixture_display_only`",
        "`docs_static_check_only`",
        "Allowed `fixture_type` enum",
        "`synthetic_signal_tape`",
        "`synthetic_selection_contract`",
        "`synthetic_picker_item`",
        "`none`",
    ):
        assert expected in text


def test_allowed_and_forbidden_record_fields_are_fixed() -> None:
    text = _record_text()

    for allowed in (
        "aggregate `net_total`",
        "aggregate `final_equity`",
        "aggregate `trade_count`",
        "DD display fields",
        "compact hashes",
        "status / warnings",
        "safe_error_code",
        "`signal_dir`",
        "branch / commit / app_version",
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
        "screenshot containing secrets / account / balances / orders",
        "entry_exec / exit_exec / qty / trade id",
        "API key / secret / token / authorization / raw order / balance / raw billing",
    ):
        assert forbidden in text


def test_expected_labels_and_disabled_states_are_fixed() -> None:
    text = _record_text()

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
        "Preview only",
        "Execution disabled",
        "This panel does not execute commands",
        "Copy backtest command",
        "Copy replay command",
        "Copied",
        "Run this command manually in a terminal if needed",
        "No raw trade rows are displayed",
    ):
        assert label in text

    for state in (
        "preview_only=true",
        "execution_enabled=false",
        "live_command_available=false",
        "paper_command_available=false",
        "not_selectable_for_live=true",
        "not_selectable_for_paper=true",
        "command_execution_observed=false",
        "producer_auto_run_observed=false",
        "backtest_auto_run_observed=false",
        "runner_auto_run_observed=false",
        "order_path_observed=false",
        "balance_fetch_observed=false",
        "private_api_observed=false",
    ):
        assert state in text


def test_pass_fail_blocked_not_run_criteria_and_screenshot_policy_are_fixed() -> None:
    text = _record_text()

    for expected in (
        "## Pass / Fail / Blocked Criteria",
        "Pass criteria",
        "expected labels visible",
        "expected disabled states confirmed",
        "command preview visible but not executed",
        "copy UX works as clipboard-only",
        "diagnostics visible",
        "no raw trade rows visible",
        "no LIVE/PAPER/order/private API observed",
        "Fail criteria",
        "any command executed by GUI",
        "producer/backtest/runner/inventory auto-runs",
        "LIVE/PAPER/order/private API path starts",
        "balance/order fetch occurs",
        "raw trade rows / `entry_exec` / `exit_exec` / `qty` / trade id displayed",
        "secrets/auth/billing/order/balance appears",
        "Blocked criteria",
        "no synthetic fixture available",
        "GUI cannot launch safely",
        "worktree contains unexpected staged files",
        "package/release artifact confusion",
        "APP_VERSION mismatch",
        "operator cannot confirm LIVE/PAPER/order disabled state",
        "Not run criteria",
        "docs/static-only validation chosen",
        "GUI runtime smoke intentionally deferred",
        "## Screenshot Policy",
        "screenshots optional",
        "screenshots must not include secrets / balances / orders",
        "screenshots must not include raw trade rows",
        "screenshots must be omitted from repo and zip unless explicitly sanitized",
        "Phase 15 does not require screenshots",
    ):
        assert expected in text


def test_related_docs_reference_phase15_policy_and_future_boundary() -> None:
    text = _doc_text()

    for expected in (
        "Phase 15 is synthetic fixture only manual GUI runtime smoke record format",
        "docs/test-only",
        "GUI runtime smoke is not executed in Phase 15",
        "no command execution",
        "no producer/backtest/runner/inventory auto-run",
        "no LIVE/PAPER/order",
        "no MEXC private API",
        "no subprocess / QProcess / background worker",
        "APP_VERSION unchanged",
        "package/release not touched",
        "safe metadata only",
        "synthetic fixture only",
        "expected labels",
        "disabled states",
        "Allowed `result_status` values are `pass`, `fail`, `blocked`, and `not_run`",
        "Allowed `smoke_mode` values are `synthetic_fixture_display_only`",
        "Allowed `fixture_type` values are",
        "future local-only dry-run is future phase",
        "Execution button is future phase",
        "LIVE/PAPER/order remains separated",
        "packaging/release remains separated",
    ):
        assert expected in text


def test_gui_source_has_no_phase15_added_execution_network_or_order_connections() -> None:
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
        "precompute_signals.py",
        "precomputed_signals_inventory.py",
        "launch_backtest",
        "launch_runner",
        "launch_replay",
        "--mode live",
        "--mode paper",
        "MEXC",
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
