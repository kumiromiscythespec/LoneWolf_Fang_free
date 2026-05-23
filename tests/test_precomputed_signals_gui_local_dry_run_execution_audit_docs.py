# BUILD_ID: 2026-05-09_free_precomputed_local_dry_run_execution_audit_record_docs_v1
from __future__ import annotations

import json
import re
import subprocess
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
AUDIT_DOC = REPO_ROOT / "docs" / "precomputed_signals_gui_local_dry_run_execution_audit_schema.md"
SAMPLE_PATH = REPO_ROOT / "docs" / "precomputed_signals_gui_local_dry_run_execution_audit_sample.json"
BOUNDARY_DOC = REPO_ROOT / "docs" / "precomputed_signals_gui_local_dry_run_approval_boundary.md"
PREFLIGHT_DOC = REPO_ROOT / "docs" / "precomputed_signals_gui_local_dry_run_approval_preflight_checklist.md"
DESIGN_DOC = REPO_ROOT / "docs" / "precomputed_signals_gui_local_dry_run_design.md"
SPEC_DOC = REPO_ROOT / "docs" / "precomputed_signals_spec.md"
DOC_PATHS = (AUDIT_DOC, BOUNDARY_DOC, PREFLIGHT_DOC, DESIGN_DOC, SPEC_DOC)
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
)

RECORD_TYPE = "precomputed_signal_local_dry_run_execution_record"
EXECUTION_AUDIT_SCHEMA_VERSION = "free_precomputed_local_dry_run_execution_audit_v1"
PHASE = "free_precomputed_signals_phase24_local_dry_run_execution_audit_docs_only"
APPROVAL_SCOPE = "local_saved_tape_backtest_replay_only"
ALLOWED_DRY_RUN_MODES = {
    "backtest_fast_path_local_only",
    "runner_replay_fast_path_local_only",
}
ALLOWED_RESULT_STATUS = {"pass", "fail", "blocked", "invalid", "not_run"}
ALLOWED_FAIL_CLOSED_REASONS = {
    "none",
    "approval_record_missing",
    "approval_record_hash_mismatch",
    "request_missing",
    "request_hash_mismatch",
    "selection_hash_mismatch",
    "approval_scope_mismatch",
    "invalid_request",
    "invalid_selection",
    "operator_confirmation_missing",
    "live_or_paper_requested",
    "order_or_balance_requested",
    "private_api_requested",
    "background_execution_requested",
    "unsafe_output_dir",
    "forbidden_field_detected",
    "positive_legacy_max_drawdown",
    "output_artifact_policy_violation",
    "raw_trade_rows_detected",
    "missing_no_live_paper_order_assertion",
    "missing_no_private_api_assertion",
    "missing_no_background_execution_assertion",
    "unknown_safety_violation",
}
ALLOWED_ARTIFACT_TYPES = {
    "safe_summary_json",
    "fast_summary_json",
    "equity_curve_csv",
    "fast_path_trades_csv_safe_condition",
    "safe_metadata_log",
    "sanitized_manual_smoke_record",
    "local_output_manifest",
}
FORBIDDEN_ARTIFACT_TYPES = {
    "raw_market_data",
    "raw_ohlcv",
    "raw_trades_rows",
    "entry_exec_rows",
    "exit_exec_rows",
    "qty_rows",
    "order_payload",
    "balance_snapshot",
    "api_key",
    "secret",
    "token",
    "authorization",
    "raw_billing",
    "package_zip",
    "exe",
    "installer",
    "release_asset",
    "screenshot_with_sensitive_data",
}
REQUIRED_FIELDS = (
    "schema_version",
    "execution_audit_schema_version",
    "record_type",
    "phase",
    "repo",
    "branch",
    "head_commit",
    "app_version",
    "created_at_utc",
    "approval_record_hash",
    "request_hash",
    "selection_hash",
    "approval_scope",
    "product",
    "signal_dir",
    "symbol",
    "entry_tf",
    "filter_tf",
    "signal_set_id",
    "dry_run_mode",
    "output_dir",
    "execution_started_at_utc",
    "execution_finished_at_utc",
    "execution_duration_ms",
    "result_status",
    "result_reason",
    "fail_closed_reason",
    "output_artifact_manifest",
    "safe_summary",
    "no_live_paper_order_assertion",
    "no_private_api_assertion",
    "no_background_execution_assertion",
    "no_order_or_balance_path_assertion",
    "forbidden_fields_absent",
    "raw_trade_rows_absent",
    "generated_artifacts_policy_checked",
    "approval_record_hash_verified",
    "request_hash_verified",
    "notes_sanitized",
)
REQUIRED_ASSERTION_FIELDS = (
    "no_live_paper_order_assertion",
    "no_private_api_assertion",
    "no_background_execution_assertion",
    "no_order_or_balance_path_assertion",
    "forbidden_fields_absent",
    "raw_trade_rows_absent",
    "generated_artifacts_policy_checked",
    "approval_record_hash_verified",
    "request_hash_verified",
)
MANIFEST_REQUIRED_FIELDS = (
    "artifact_type",
    "path",
    "sha256",
    "size_bytes",
    "safe_to_archive",
    "contains_raw_market_data",
    "contains_raw_trade_rows",
    "contains_order_or_balance",
    "contains_secret_or_auth",
    "notes_sanitized",
)
POLICY_FIELDS = {
    "output_artifact_manifest",
    "no_live_paper_order_assertion",
    "no_order_or_balance_path_assertion",
    "forbidden_fields_absent",
    "raw_trade_rows_absent",
    "generated_artifacts_policy_checked",
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


class ExecutionAuditSchemaError(ValueError):
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
            raise ExecutionAuditSchemaError(f"forbidden text at {path}: {pattern.pattern}")


def assert_no_forbidden_execution_audit_fields(record: Any, path: str = "$") -> None:
    if isinstance(record, dict):
        for key, value in record.items():
            normalized = _normalized_key(str(key))
            if normalized in FORBIDDEN_KEYS:
                raise ExecutionAuditSchemaError(f"forbidden key at {path}: {key}")
            if str(key) in POLICY_FIELDS or str(key) in MANIFEST_REQUIRED_FIELDS:
                continue
            _scan_forbidden_text(str(key), f"{path}.{key}")
            assert_no_forbidden_execution_audit_fields(value, f"{path}.{key}")
        return
    if isinstance(record, list):
        for index, value in enumerate(record):
            assert_no_forbidden_execution_audit_fields(value, f"{path}[{index}]")
        return
    if isinstance(record, str):
        _scan_forbidden_text(record, path)


def validate_artifact_manifest_item(item: dict[str, Any]) -> bool:
    missing = [field for field in MANIFEST_REQUIRED_FIELDS if field not in item]
    if missing:
        raise ExecutionAuditSchemaError(f"manifest item missing fields: {missing}")
    artifact_type = item["artifact_type"]
    if artifact_type in FORBIDDEN_ARTIFACT_TYPES:
        raise ExecutionAuditSchemaError(f"forbidden artifact_type: {artifact_type}")
    if artifact_type not in ALLOWED_ARTIFACT_TYPES:
        raise ExecutionAuditSchemaError(f"invalid artifact_type: {artifact_type}")
    if item["safe_to_archive"] is not True:
        raise ExecutionAuditSchemaError("artifact must be safe_to_archive")
    for field in (
        "contains_raw_market_data",
        "contains_raw_trade_rows",
        "contains_order_or_balance",
        "contains_secret_or_auth",
    ):
        if item[field] is not False:
            raise ExecutionAuditSchemaError(f"unsafe artifact field: {field}")
    path_text = str(item["path"]).lower()
    if re.search(r"screenshot[^\n]*(?:\.png|\.jpg|\.jpeg|\.webp|[\\/])", path_text):
        raise ExecutionAuditSchemaError("screenshot path is forbidden")
    if any(token in path_text for token in ("package", ".zip", ".exe", "installer", "release_asset")):
        raise ExecutionAuditSchemaError("package/release artifact path is forbidden")
    return True


def validate_phase24_static_execution_audit_record(record: dict[str, Any]) -> bool:
    missing = [field for field in REQUIRED_FIELDS if field not in record]
    if missing:
        raise ExecutionAuditSchemaError(f"missing required fields: {missing}")
    if record["record_type"] != RECORD_TYPE:
        raise ExecutionAuditSchemaError("invalid record_type")
    if record["execution_audit_schema_version"] != EXECUTION_AUDIT_SCHEMA_VERSION:
        raise ExecutionAuditSchemaError("invalid execution_audit_schema_version")
    if record["phase"] != PHASE:
        raise ExecutionAuditSchemaError("invalid phase")
    if record["approval_scope"] != APPROVAL_SCOPE:
        raise ExecutionAuditSchemaError("invalid approval_scope")
    if record["product"] != "free":
        raise ExecutionAuditSchemaError("product must be free")
    if record["dry_run_mode"] not in ALLOWED_DRY_RUN_MODES:
        raise ExecutionAuditSchemaError("invalid dry_run_mode")
    if record["result_status"] not in ALLOWED_RESULT_STATUS:
        raise ExecutionAuditSchemaError("invalid result_status")
    if record["result_status"] != "not_run":
        raise ExecutionAuditSchemaError("Phase 24 static sample result_status must be not_run")
    if record["fail_closed_reason"] not in ALLOWED_FAIL_CLOSED_REASONS:
        raise ExecutionAuditSchemaError("invalid fail_closed_reason")
    for field in ("approval_record_hash", "request_hash", "selection_hash"):
        if not record[field]:
            raise ExecutionAuditSchemaError(f"missing {field}")
        if not str(record[field]).startswith("sha256:"):
            raise ExecutionAuditSchemaError(f"invalid {field}")
    for field in REQUIRED_ASSERTION_FIELDS:
        if record[field] is not True:
            raise ExecutionAuditSchemaError(f"{field} must be true")
    if record["execution_started_at_utc"] is not None:
        raise ExecutionAuditSchemaError("Phase 24 sample execution_started_at_utc must be null")
    if record["execution_finished_at_utc"] is not None:
        raise ExecutionAuditSchemaError("Phase 24 sample execution_finished_at_utc must be null")
    if record["execution_duration_ms"] != 0:
        raise ExecutionAuditSchemaError("Phase 24 sample execution_duration_ms must be 0")
    if not isinstance(record["safe_summary"], dict):
        raise ExecutionAuditSchemaError("safe_summary must be object")
    manifest = record["output_artifact_manifest"]
    if not isinstance(manifest, list):
        raise ExecutionAuditSchemaError("output_artifact_manifest must be list")
    for item in manifest:
        validate_artifact_manifest_item(item)
    if "no execution occurred" not in str(record["notes_sanitized"]).lower():
        raise ExecutionAuditSchemaError("notes_sanitized must state no execution occurred")
    assert_no_forbidden_execution_audit_fields(record)
    return True


def make_valid_record(**overrides: Any) -> dict[str, Any]:
    record = deepcopy(load_sample_record())
    record.update(overrides)
    return record


def make_invalid_record(**overrides: Any) -> dict[str, Any]:
    return make_valid_record(**overrides)


def make_manifest_item(artifact_type: str) -> dict[str, Any]:
    return {
        "artifact_type": artifact_type,
        "path": "static-placeholder/safe-summary.json",
        "sha256": "sha256:3333333333333333333333333333333333333333333333333333333333333333",
        "size_bytes": 0,
        "safe_to_archive": True,
        "contains_raw_market_data": False,
        "contains_raw_trade_rows": False,
        "contains_order_or_balance": False,
        "contains_secret_or_auth": False,
        "notes_sanitized": "Static placeholder item; no execution occurred.",
    }


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


def test_execution_audit_schema_doc_and_sample_json_exist() -> None:
    assert AUDIT_DOC.exists()
    assert SAMPLE_PATH.exists()


def test_sample_execution_audit_record_json_is_valid_and_has_identity_fields() -> None:
    record = load_sample_record()

    assert isinstance(record, dict)
    assert record["record_type"] == RECORD_TYPE
    assert record["execution_audit_schema_version"] == EXECUTION_AUDIT_SCHEMA_VERSION
    assert record["approval_scope"] == APPROVAL_SCOPE
    assert record["dry_run_mode"] in ALLOWED_DRY_RUN_MODES
    assert record["result_status"] in ALLOWED_RESULT_STATUS
    assert record["result_status"] == "not_run"
    assert validate_phase24_static_execution_audit_record(record)


def test_sample_execution_audit_required_fields_hash_linkage_and_static_defaults_are_fixed() -> None:
    record = load_sample_record()

    for field in REQUIRED_FIELDS:
        assert field in record
    assert record["approval_record_hash"]
    assert record["request_hash"]
    assert record["selection_hash"]
    assert record["execution_started_at_utc"] is None
    assert record["execution_finished_at_utc"] is None
    assert record["execution_duration_ms"] == 0


def test_sample_execution_audit_required_assertions_are_true() -> None:
    record = load_sample_record()

    assert record["no_live_paper_order_assertion"] is True
    assert record["no_private_api_assertion"] is True
    assert record["no_background_execution_assertion"] is True
    assert record["no_order_or_balance_path_assertion"] is True
    assert record["forbidden_fields_absent"] is True
    assert record["raw_trade_rows_absent"] is True
    assert record["generated_artifacts_policy_checked"] is True
    assert record["approval_record_hash_verified"] is True
    assert record["request_hash_verified"] is True


def test_sample_execution_audit_contains_no_forbidden_payload_fields_or_text() -> None:
    record = load_sample_record()
    payload_without_policy = _record_without_policy_fields(record)
    sample_payload_text = json.dumps(payload_without_policy, sort_keys=True).lower()

    assert_no_forbidden_execution_audit_fields(record)
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


def test_sample_execution_audit_contains_no_screenshot_path_or_generated_payload() -> None:
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


def test_invalid_record_type_is_rejected() -> None:
    with pytest.raises(ExecutionAuditSchemaError, match="record_type"):
        validate_phase24_static_execution_audit_record(make_invalid_record(record_type="approval_record"))


@pytest.mark.parametrize("bad_scope", ["live", "paper", "local", "", None])
def test_invalid_approval_scope_is_rejected(bad_scope: Any) -> None:
    with pytest.raises(ExecutionAuditSchemaError, match="approval_scope"):
        validate_phase24_static_execution_audit_record(make_invalid_record(approval_scope=bad_scope))


@pytest.mark.parametrize("bad_mode", ["live", "paper", "runner", "", None])
def test_invalid_dry_run_mode_is_rejected(bad_mode: Any) -> None:
    with pytest.raises(ExecutionAuditSchemaError, match="dry_run_mode"):
        validate_phase24_static_execution_audit_record(make_invalid_record(dry_run_mode=bad_mode))


@pytest.mark.parametrize("bad_status", ["ok", "success", "executed", "", None])
def test_invalid_result_status_is_rejected(bad_status: Any) -> None:
    with pytest.raises(ExecutionAuditSchemaError, match="result_status"):
        validate_phase24_static_execution_audit_record(make_invalid_record(result_status=bad_status))


@pytest.mark.parametrize("field", ["approval_record_hash", "request_hash", "selection_hash"])
def test_missing_required_hash_linkage_is_rejected(field: str) -> None:
    with pytest.raises(ExecutionAuditSchemaError, match=field):
        validate_phase24_static_execution_audit_record(make_invalid_record(**{field: ""}))


@pytest.mark.parametrize("field", REQUIRED_ASSERTION_FIELDS)
def test_required_assertion_false_is_rejected(field: str) -> None:
    with pytest.raises(ExecutionAuditSchemaError, match=field):
        validate_phase24_static_execution_audit_record(make_invalid_record(**{field: False}))


@pytest.mark.parametrize(
    "artifact_type",
    [
        "raw_market_data",
        "raw_trades_rows",
        "order_payload",
        "balance_snapshot",
        "package_zip",
        "exe",
        "installer",
        "release_asset",
    ],
)
def test_forbidden_output_artifact_manifest_type_is_rejected(artifact_type: str) -> None:
    record = make_invalid_record(output_artifact_manifest=[make_manifest_item(artifact_type)])

    with pytest.raises(ExecutionAuditSchemaError, match="artifact_type"):
        validate_phase24_static_execution_audit_record(record)


@pytest.mark.parametrize("key", ["entry_exec", "exit_exec", "qty", "trade_id"])
def test_record_containing_trade_row_payload_key_is_rejected(key: str) -> None:
    record = make_invalid_record(**{key: "unsafe"})

    with pytest.raises(ExecutionAuditSchemaError, match=key):
        validate_phase24_static_execution_audit_record(record)


@pytest.mark.parametrize("key", ["raw_order", "balance", "secret", "token", "authorization"])
def test_record_containing_order_account_or_private_payload_key_is_rejected(key: str) -> None:
    record = make_invalid_record(**{key: "unsafe"})

    with pytest.raises(ExecutionAuditSchemaError, match=key):
        validate_phase24_static_execution_audit_record(record)


def test_docs_mention_phase24_docs_tests_only_and_no_implementation_boundaries() -> None:
    text = _doc_text()

    for expected in (
        "Phase 24 is execution audit record schema / static sample docs-tests",
        "Phase 24 is docs/tests-only",
        "no approval UI implementation",
        "no dry-run execution implementation",
        "no approval record generation at runtime",
        "no execution audit record generation at runtime",
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


def test_docs_mention_execution_audit_identity_and_separation() -> None:
    text = _doc_text()

    for expected in (
        "precomputed_signal_local_dry_run_execution_record",
        "free_precomputed_local_dry_run_execution_audit_v1",
        "execution audit record is not approval record",
        "execution audit record is not order/trading record",
        "Approval record does not prove execution",
        "Phase 24 sample is a static `not_run` sample",
        "future execution must be separate explicit approval and implementation phase",
    ):
        assert expected in text


def test_docs_mention_hash_linkage_assertions_manifest_and_fail_closed_policy() -> None:
    text = _doc_text()

    for expected in (
        "approval_record_hash / request_hash linkage",
        "approval_record_hash / request_hash / selection_hash linkage",
        "`approval_record_hash`",
        "`request_hash`",
        "`selection_hash`",
        "no LIVE/PAPER/order/private API/background execution",
        "no_private_api_assertion",
        "no_background_execution_assertion",
        "no_order_or_balance_path_assertion",
        "output artifact manifest schema",
        "allowed artifact types",
        "forbidden artifact types",
        "Fail-Closed Reasons",
        "approval_record_missing",
        "request_hash_mismatch",
        "selection_hash_mismatch",
        "approval_scope_mismatch",
        "output_artifact_policy_violation",
    ):
        assert expected in text


def test_docs_mention_allowed_and_forbidden_artifact_types() -> None:
    text = _doc_text()

    for expected in (
        "safe_summary_json",
        "fast_summary_json",
        "equity_curve_csv",
        "fast_path_trades_csv_safe_condition",
        "safe_metadata_log",
        "sanitized_manual_smoke_record",
        "local_output_manifest",
        "raw_market_data",
        "raw_ohlcv",
        "raw_trades_rows",
        "entry_exec_rows",
        "exit_exec_rows",
        "qty_rows",
        "order_payload",
        "balance_snapshot",
        "package_zip",
        "exe",
        "installer",
        "release_asset",
        "screenshot_with_sensitive_data",
    ):
        assert expected in text


def test_docs_mention_relationship_to_existing_phases() -> None:
    text = _doc_text()

    for expected in (
        "Phase 18 request schema is planning artifact",
        "Phase 19 request builder creates preview only",
        "Phase 20 GUI preview adapter creates display item only",
        "Phase 21 GUI wiring displays preview only",
        "Phase 22 approval boundary defines future approval rules",
        "Phase 23 approval static sample/checklist does not execute anything",
        "Phase 24 execution audit schema defines future audit only",
        "future execution requires separate explicit approval and implementation phase",
    ):
        assert expected in text


def test_gui_runtime_and_helper_sources_have_no_phase24_execution_or_network_changes() -> None:
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
