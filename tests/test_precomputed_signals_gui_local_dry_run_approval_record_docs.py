# BUILD_ID: 2026-05-09_free_precomputed_local_dry_run_approval_record_docs_v1
from __future__ import annotations

import json
import re
import subprocess
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
PREFLIGHT_DOC = REPO_ROOT / "docs" / "precomputed_signals_gui_local_dry_run_approval_preflight_checklist.md"
SAMPLE_PATH = REPO_ROOT / "docs" / "precomputed_signals_gui_local_dry_run_approval_record_sample.json"
BOUNDARY_DOC = REPO_ROOT / "docs" / "precomputed_signals_gui_local_dry_run_approval_boundary.md"
DESIGN_DOC = REPO_ROOT / "docs" / "precomputed_signals_gui_local_dry_run_design.md"
SPEC_DOC = REPO_ROOT / "docs" / "precomputed_signals_spec.md"
DOC_PATHS = (PREFLIGHT_DOC, BOUNDARY_DOC, DESIGN_DOC, SPEC_DOC)
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
)

RECORD_TYPE = "precomputed_signal_local_dry_run_approval_record"
APPROVAL_SCHEMA_VERSION = "free_precomputed_local_dry_run_approval_record_v1"
APPROVAL_SCOPE = "local_saved_tape_backtest_replay_only"
PHASE = "free_precomputed_signals_phase23_local_dry_run_approval_record_docs_only"
ALLOWED_DRY_RUN_MODES = {
    "backtest_fast_path_local_only",
    "runner_replay_fast_path_local_only",
}
ALLOWED_APPROVED_ACTIONS = {
    "approve_backtest_fast_path_local_only",
    "approve_runner_replay_fast_path_local_only",
}
REQUIRED_FORBIDDEN_ACTIONS = {
    "live",
    "paper",
    "order_submit",
    "order_fetch",
    "balance_fetch",
    "private_api",
    "producer_auto_run",
    "inventory_auto_scan",
    "background_worker",
    "package_release",
}
ALLOWED_STATUS = {"draft", "approved", "rejected", "blocked", "invalid", "not_run"}
ALLOWED_FAIL_CLOSED_REASONS = {
    "missing_request_hash",
    "missing_selection_hash",
    "invalid_request",
    "invalid_selection",
    "invalid_approval_scope",
    "missing_confirmation_text",
    "missing_operator_confirmation",
    "forbidden_action_requested",
    "missing_forbidden_action",
    "live_or_paper_requested",
    "order_or_balance_requested",
    "private_api_requested",
    "background_execution_requested",
    "package_release_action_requested",
    "unsafe_output_dir",
    "forbidden_field",
    "positive_legacy_max_drawdown",
    "worktree_status_unrecorded",
    "unknown_safety_violation",
}
REQUIRED_FIELDS = (
    "schema_version",
    "approval_schema_version",
    "record_type",
    "phase",
    "repo",
    "branch",
    "head_commit",
    "app_version",
    "created_at_utc",
    "operator",
    "product",
    "signal_dir",
    "symbol",
    "entry_tf",
    "filter_tf",
    "signal_set_id",
    "dry_run_mode",
    "output_dir",
    "request_hash",
    "selection_hash",
    "confirmation_text",
    "operator_confirmed",
    "approval_scope",
    "approved_actions",
    "forbidden_actions",
    "preflight_passed",
    "preflight_summary",
    "fail_closed_reasons",
    "allowed_artifacts",
    "forbidden_artifacts",
    "status",
    "status_reason",
    "notes_sanitized",
)
POLICY_FIELDS = {
    "approved_actions",
    "forbidden_actions",
    "allowed_artifacts",
    "forbidden_artifacts",
    "fail_closed_reasons",
}
FORBIDDEN_KEYS = {
    "raw_trade_row",
    "raw_trade_rows",
    "raw_trades_rows",
    "trades_rows",
    "trades_csv_rows",
    "entry_exec",
    "exit_exec",
    "qty",
    "quantity",
    "trade_id",
    "exact_trade_id",
    "order_id",
    "raw_order",
    "raw_orders",
    "balance",
    "balance_snapshot",
    "account_balance",
    "api_key",
    "apikey",
    "secret",
    "api_secret",
    "token",
    "authorization",
    "raw_billing",
    "raw_market_data",
    "raw_ohlcv",
    "ohlcv",
    "screenshot",
    "screenshot_path",
    "screenshots",
    "account_details",
    "private_key",
}
FORBIDDEN_TEXT_PATTERNS = (
    re.compile(r"\bapi key\b", re.IGNORECASE),
    re.compile(r"\bapi[_ -]?secret\b", re.IGNORECASE),
    re.compile(r"\bsecret\s*[:=]", re.IGNORECASE),
    re.compile(r"\btoken\s*[:=]", re.IGNORECASE),
    re.compile(r"\bauthorization\s*[:=]", re.IGNORECASE),
    re.compile(r"\bbearer\s+[a-z0-9]", re.IGNORECASE),
    re.compile(r"\braw order\b", re.IGNORECASE),
    re.compile(r"\braw_order\b", re.IGNORECASE),
    re.compile(r"\bbalance snapshot\b", re.IGNORECASE),
    re.compile(r"\braw billing\b", re.IGNORECASE),
    re.compile(r"\braw market data\b", re.IGNORECASE),
    re.compile(r"\braw ohlcv\b", re.IGNORECASE),
    re.compile(r"\bentry_exec\b", re.IGNORECASE),
    re.compile(r"\bexit_exec\b", re.IGNORECASE),
    re.compile(r"\bqty\b", re.IGNORECASE),
    re.compile(r"\btrade id\b", re.IGNORECASE),
    re.compile(r"\btrade_id\b", re.IGNORECASE),
    re.compile(r"\border id\b", re.IGNORECASE),
    re.compile(r"\border_id\b", re.IGNORECASE),
    re.compile(r"screenshot[^\n]*(?:\.png|\.jpg|\.jpeg|\.webp|[\\/])", re.IGNORECASE),
)
FORBIDDEN_APPROVED_ACTIONS = {
    "live",
    "paper",
    "order",
    "order_submit",
    "order_fetch",
    "balance_fetch",
    "private_api",
}


class ApprovalRecordSchemaError(ValueError):
    pass


def load_sample_record() -> dict[str, Any]:
    assert SAMPLE_PATH.exists()
    return json.loads(SAMPLE_PATH.read_text(encoding="utf-8"))


def _doc_text() -> str:
    return "\n".join(path.read_text(encoding="utf-8") for path in DOC_PATHS)


def _normalized_key(key: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", key.lower()).strip("_")


def _scan_forbidden_text(text: str, path: str) -> None:
    for pattern in FORBIDDEN_TEXT_PATTERNS:
        if pattern.search(text):
            raise ApprovalRecordSchemaError(f"forbidden text at {path}: {pattern.pattern}")


def assert_no_forbidden_approval_record_fields(record: Any, path: str = "$") -> None:
    if isinstance(record, dict):
        for key, value in record.items():
            normalized = _normalized_key(str(key))
            if normalized in FORBIDDEN_KEYS:
                raise ApprovalRecordSchemaError(f"forbidden key at {path}: {key}")
            if str(key) in POLICY_FIELDS:
                continue
            _scan_forbidden_text(str(key), f"{path}.{key}")
            assert_no_forbidden_approval_record_fields(value, f"{path}.{key}")
        return
    if isinstance(record, list):
        for index, value in enumerate(record):
            assert_no_forbidden_approval_record_fields(value, f"{path}[{index}]")
        return
    if isinstance(record, str):
        _scan_forbidden_text(record, path)


def validate_phase23_static_approval_record(record: dict[str, Any]) -> bool:
    missing = [field for field in REQUIRED_FIELDS if field not in record]
    if missing:
        raise ApprovalRecordSchemaError(f"missing required fields: {missing}")
    if record["record_type"] != RECORD_TYPE:
        raise ApprovalRecordSchemaError("invalid record_type")
    if record["approval_schema_version"] != APPROVAL_SCHEMA_VERSION:
        raise ApprovalRecordSchemaError("invalid approval_schema_version")
    if record["phase"] != PHASE:
        raise ApprovalRecordSchemaError("invalid phase")
    if record["approval_scope"] != APPROVAL_SCOPE:
        raise ApprovalRecordSchemaError("invalid approval_scope")
    if record["product"] != "free":
        raise ApprovalRecordSchemaError("product must be free")
    if record["dry_run_mode"] not in ALLOWED_DRY_RUN_MODES:
        raise ApprovalRecordSchemaError("invalid dry_run_mode")
    if record["status"] not in ALLOWED_STATUS:
        raise ApprovalRecordSchemaError("invalid status")
    if record["status"] != "not_run":
        raise ApprovalRecordSchemaError("Phase 23 static sample status must be not_run")
    if record["operator_confirmed"] is not False:
        raise ApprovalRecordSchemaError("operator_confirmed must be false for Phase 23 static sample")
    if record["preflight_passed"] is not False:
        raise ApprovalRecordSchemaError("preflight_passed must be false for Phase 23 static sample")
    if not record["request_hash"]:
        raise ApprovalRecordSchemaError("missing_request_hash")
    if not record["selection_hash"]:
        raise ApprovalRecordSchemaError("missing_selection_hash")
    if not str(record["request_hash"]).startswith("sha256:"):
        raise ApprovalRecordSchemaError("invalid request_hash")
    if not str(record["selection_hash"]).startswith("sha256:"):
        raise ApprovalRecordSchemaError("invalid selection_hash")
    approved_actions = set(record["approved_actions"])
    if not approved_actions.issubset(ALLOWED_APPROVED_ACTIONS):
        raise ApprovalRecordSchemaError("forbidden_action_requested")
    if approved_actions & FORBIDDEN_APPROVED_ACTIONS:
        raise ApprovalRecordSchemaError("forbidden_action_requested")
    missing_forbidden = REQUIRED_FORBIDDEN_ACTIONS - set(record["forbidden_actions"])
    if missing_forbidden:
        raise ApprovalRecordSchemaError(f"missing_forbidden_action: {sorted(missing_forbidden)}")
    if not set(record["fail_closed_reasons"]).issubset(ALLOWED_FAIL_CLOSED_REASONS):
        raise ApprovalRecordSchemaError("invalid fail_closed_reasons")
    if not isinstance(record["preflight_summary"], dict):
        raise ApprovalRecordSchemaError("preflight_summary must be object")
    assert_no_forbidden_approval_record_fields(record)
    return True


def make_valid_record(**overrides: Any) -> dict[str, Any]:
    record = deepcopy(load_sample_record())
    record.update(overrides)
    return record


def make_invalid_record(**overrides: Any) -> dict[str, Any]:
    return make_valid_record(**overrides)


def _record_without_policy_fields(record: dict[str, Any]) -> dict[str, Any]:
    sanitized = deepcopy(record)
    for field in POLICY_FIELDS:
        sanitized.pop(field, None)
    return sanitized


def _git_diff_added_lines(paths: tuple[Path, ...]) -> str:
    rel_paths = [str(path.relative_to(REPO_ROOT)).replace("\\", "/") for path in paths]
    output = subprocess.run(
        ["git", "diff", "HEAD", "--", *rel_paths],
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
        ["git", "diff", "--name-only", "HEAD", "--", *rel_paths],
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


def test_approval_preflight_checklist_doc_and_sample_json_exist() -> None:
    assert PREFLIGHT_DOC.exists()
    assert SAMPLE_PATH.exists()


def test_sample_approval_record_json_is_valid_and_has_identity_fields() -> None:
    record = load_sample_record()

    assert isinstance(record, dict)
    assert record["record_type"] == RECORD_TYPE
    assert record["approval_schema_version"] == APPROVAL_SCHEMA_VERSION
    assert record["approval_scope"] == APPROVAL_SCOPE
    assert record["status"] in ALLOWED_STATUS
    assert record["status"] == "not_run"
    assert validate_phase23_static_approval_record(record)


def test_sample_approval_record_required_fields_and_static_defaults_are_fixed() -> None:
    record = load_sample_record()

    for field in REQUIRED_FIELDS:
        assert field in record
    assert record["operator_confirmed"] is False
    assert record["preflight_passed"] is False
    assert record["request_hash"]
    assert record["selection_hash"]


def test_sample_approval_record_actions_are_fixed() -> None:
    record = load_sample_record()

    assert set(record["approved_actions"]).issubset(ALLOWED_APPROVED_ACTIONS)
    assert REQUIRED_FORBIDDEN_ACTIONS.issubset(set(record["forbidden_actions"]))


def test_sample_approval_record_contains_no_forbidden_payload_fields_or_text() -> None:
    record = load_sample_record()
    payload_without_policy = _record_without_policy_fields(record)
    sample_payload_text = json.dumps(payload_without_policy, sort_keys=True).lower()

    assert_no_forbidden_approval_record_fields(record)
    for forbidden in (
        "raw_trade_rows",
        "raw trades rows",
        "entry_exec",
        "exit_exec",
        '"qty"',
        "trade id",
        "trade_id",
        "order_id",
        "raw_order",
        "balance_snapshot",
        "api key",
        "api_secret",
        "authorization:",
        "raw billing",
        "raw market data",
        "raw ohlcv",
    ):
        assert forbidden not in sample_payload_text


def test_sample_approval_record_contains_no_screenshot_path_account_or_generated_runtime_payload() -> None:
    payload_without_policy = _record_without_policy_fields(load_sample_record())
    sample_payload_text = json.dumps(payload_without_policy, sort_keys=True).lower()

    assert not re.search(r"screenshot[^\n]*(?:\.png|\.jpg|\.jpeg|\.webp|[\\/])", sample_payload_text)
    for forbidden in (
        "generated real signal tape body",
        "generated approval record",
        "generated execution audit record",
        "generated dry-run output",
        "account details",
        "private key",
        "raw order",
        "raw_order",
        "balance snapshot",
        "order id",
        "order_id",
    ):
        assert forbidden not in sample_payload_text


@pytest.mark.parametrize("bad_scope", ["live", "paper", "local", "", None])
def test_invalid_approval_scope_is_rejected(bad_scope: Any) -> None:
    with pytest.raises(ApprovalRecordSchemaError, match="approval_scope"):
        validate_phase23_static_approval_record(make_invalid_record(approval_scope=bad_scope))


@pytest.mark.parametrize("bad_status", ["ok", "success", "executed", "", None])
def test_invalid_status_is_rejected(bad_status: Any) -> None:
    with pytest.raises(ApprovalRecordSchemaError, match="status"):
        validate_phase23_static_approval_record(make_invalid_record(status=bad_status))


def test_operator_confirmed_true_is_rejected_for_phase23_static_not_run_sample() -> None:
    with pytest.raises(ApprovalRecordSchemaError, match="operator_confirmed"):
        validate_phase23_static_approval_record(make_invalid_record(operator_confirmed=True))


def test_preflight_passed_true_is_rejected_for_phase23_static_not_run_sample() -> None:
    with pytest.raises(ApprovalRecordSchemaError, match="preflight_passed"):
        validate_phase23_static_approval_record(make_invalid_record(preflight_passed=True))


@pytest.mark.parametrize("bad_action", ["live", "paper", "order", "order_submit", "order_fetch"])
def test_approved_actions_containing_live_paper_or_order_are_rejected(bad_action: str) -> None:
    record = make_invalid_record(approved_actions=["approve_backtest_fast_path_local_only", bad_action])

    with pytest.raises(ApprovalRecordSchemaError, match="forbidden_action_requested"):
        validate_phase23_static_approval_record(record)


@pytest.mark.parametrize(
    "missing_action",
    ["live", "paper", "order_submit", "order_fetch", "balance_fetch", "private_api", "background_worker"],
)
def test_missing_required_forbidden_action_is_rejected(missing_action: str) -> None:
    record = make_invalid_record()
    record["forbidden_actions"] = [
        action for action in record["forbidden_actions"] if action != missing_action
    ]

    with pytest.raises(ApprovalRecordSchemaError, match="missing_forbidden_action"):
        validate_phase23_static_approval_record(record)


def test_missing_request_hash_is_rejected() -> None:
    with pytest.raises(ApprovalRecordSchemaError, match="missing_request_hash"):
        validate_phase23_static_approval_record(make_invalid_record(request_hash=""))


def test_missing_selection_hash_is_rejected() -> None:
    with pytest.raises(ApprovalRecordSchemaError, match="missing_selection_hash"):
        validate_phase23_static_approval_record(make_invalid_record(selection_hash=""))


def test_record_containing_raw_trade_row_key_is_rejected() -> None:
    record = make_invalid_record(raw_trade_rows=[{"safe": "no"}])

    with pytest.raises(ApprovalRecordSchemaError, match="raw_trade_rows"):
        validate_phase23_static_approval_record(record)


@pytest.mark.parametrize("key", ["order_id", "raw_order", "balance"])
def test_record_containing_order_or_account_payload_key_is_rejected(key: str) -> None:
    record = make_invalid_record(**{key: "unsafe"})

    with pytest.raises(ApprovalRecordSchemaError, match=key):
        validate_phase23_static_approval_record(record)


def test_docs_mention_phase23_docs_tests_only_and_no_implementation_boundaries() -> None:
    text = _doc_text()

    for expected in (
        "Phase 23 is docs/tests-only",
        "no approval UI implementation",
        "no approval record generation at runtime",
        "no execution audit record generation",
        "no dry-run execution",
        "no GUI source change",
        "no runtime source change",
        "no command execution",
        "no subprocess / QProcess / background worker",
        "no LIVE/PAPER/order",
        "no MEXC private API",
        "APP_VERSION unchanged",
        "package/release not touched",
    ):
        assert expected in text


def test_docs_mention_approval_scope_and_not_live_paper_order_approval() -> None:
    text = _doc_text()

    for expected in (
        "approval_scope = local_saved_tape_backtest_replay_only",
        "approval is not LIVE approval",
        "approval is not PAPER approval",
        "approval is not order approval",
        "approval is not private API approval",
        "approval is not release/package approval",
        "approval only covers future local saved-tape fast-path dry-run",
        "approval is separate from command preview/copy UX",
        "approval is separate from manual GUI smoke record",
        "approval is separate from execution audit record",
    ):
        assert expected in text


def test_docs_mention_required_preflight_checklist() -> None:
    text = _doc_text()

    for expected in (
        "Required Preflight Checklist",
        "`request_type == precomputed_signal_local_dry_run_request`",
        "request schema valid",
        "request_hash present",
        "selection_hash present",
        "selection contract valid",
        "`product == free`",
        "signal_dir present",
        "symbol present",
        "entry_tf / filter_tf present",
        "dry_run_mode is allowed",
        "`not_selectable_for_live == true`",
        "`not_selectable_for_paper == true`",
        "`safety_research_only == true`",
        "`paper_live_order_execution == false`",
        "`operator_confirmation_required == true`",
        "manifest present",
        "summary present",
        "trades.csv present",
        "manifest hash present",
        "summary hash present",
        "trades.csv hash from manifest present",
        "forbidden fields rejected",
        "positive legacy max_drawdown rejected",
        "output_dir safe",
        "allowed_artifacts reviewed",
        "forbidden_artifacts reviewed",
        "worktree status recorded",
        "package/release artifacts excluded",
        "no background execution",
        "no private API",
        "no order/balance path",
        "no raw trade rows displayed or recorded",
    ):
        assert expected in text


def test_docs_mention_schema_confirmation_fail_closed_and_audit_boundary() -> None:
    text = _doc_text()

    for expected in (
        "Approval Record Schema",
        "record_type",
        "precomputed_signal_local_dry_run_approval_record",
        "free_precomputed_local_dry_run_approval_record_v1",
        "request_hash / selection_hash are synthetic placeholders",
        "Confirmation Text Requirements",
        "local-only",
        "not LIVE/PAPER/order",
        "no MEXC private API",
        "no balance fetch",
        "no order fetch",
        "no order submit",
        "selected precomputed signal tape only",
        "operator understands this is not release/package approval",
        "Fail-Closed Reasons",
        "missing_request_hash",
        "missing_selection_hash",
        "invalid_request",
        "invalid_selection",
        "invalid_approval_scope",
        "missing_confirmation_text",
        "missing_operator_confirmation",
        "forbidden_action_requested",
        "missing_forbidden_action",
        "live_or_paper_requested",
        "order_or_balance_requested",
        "private_api_requested",
        "background_execution_requested",
        "package_release_action_requested",
        "unsafe_output_dir",
        "forbidden_field",
        "positive_legacy_max_drawdown",
        "worktree_status_unrecorded",
        "unknown_safety_violation",
        "Execution Audit Boundary",
        "Execution audit record is not approval record",
        "Approval record does not prove execution",
        "Phase 23 must not create execution audit record",
        "future execution requires separate explicit approval and implementation phase",
    ):
        assert expected in text


def test_gui_runtime_and_helper_sources_have_no_phase23_execution_or_network_changes() -> None:
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
