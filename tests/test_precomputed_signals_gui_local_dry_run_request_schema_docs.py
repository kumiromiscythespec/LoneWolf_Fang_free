# BUILD_ID: 2026-05-09_free_precomputed_local_dry_run_request_schema_docs_v1
from __future__ import annotations

import json
import re
import subprocess
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_DOC = REPO_ROOT / "docs" / "precomputed_signals_gui_local_dry_run_request_schema.md"
DESIGN_DOC = REPO_ROOT / "docs" / "precomputed_signals_gui_local_dry_run_design.md"
SPEC_DOC = REPO_ROOT / "docs" / "precomputed_signals_spec.md"
SAMPLE_PATH = REPO_ROOT / "docs" / "precomputed_signals_gui_local_dry_run_request_sample.json"
DOC_PATHS = (SCHEMA_DOC, DESIGN_DOC, SPEC_DOC)
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
)

REQUEST_TYPE = "precomputed_signal_local_dry_run_request"
PHASE = "free_precomputed_signals_phase18_local_dry_run_request_schema_docs_only"
SCHEMA_VERSION = "2026-05-09_free_precomputed_local_dry_run_request_schema_docs_v1"
ALLOWED_DRY_RUN_MODES = {
    "backtest_fast_path_local_only",
    "runner_replay_fast_path_local_only",
}
NOT_ALLOWED_MODES = {
    "live",
    "paper",
    "order_submit",
    "order_fetch",
    "balance_fetch",
    "private_api",
    "producer_auto_run",
    "inventory_auto_scan",
    "background_worker",
}
ALLOWED_STATUS = {"draft", "valid_preview", "blocked", "invalid", "not_run"}
ALLOWED_ARTIFACTS = {
    "safe_summary_json",
    "fast_path_equity_curve_csv",
    "fast_path_trades_csv_safe_condition",
    "fast_summary_json",
    "safe_metadata_log",
    "sanitized_manual_smoke_record",
}
FORBIDDEN_ARTIFACTS = {
    "raw_market_data",
    "raw_ohlcv",
    "raw_trades_rows",
    "entry_exec",
    "exit_exec",
    "qty",
    "trade_id",
    "order_id",
    "raw_order",
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
    "screenshots_with_secrets_or_balances_or_orders",
}
FAIL_CLOSED_REASONS = {
    "invalid_selection",
    "missing_signal_dir",
    "missing_manifest",
    "missing_summary",
    "missing_trades_csv",
    "unsafe_manifest",
    "unsafe_summary",
    "forbidden_field",
    "positive_legacy_max_drawdown",
    "live_or_paper_requested",
    "order_or_balance_requested",
    "private_api_requested",
    "missing_operator_confirmation",
    "background_execution_requested",
    "output_path_in_package_release_area",
    "generated_artifact_policy_violation",
    "unknown_safety_violation",
}
REQUIRED_TOP_LEVEL_FIELDS = (
    "schema_version",
    "request_schema_version",
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
    "preflight",
    "fail_closed_reasons",
    "status",
    "status_reason",
    "notes_sanitized",
)
REQUIRED_PREFLIGHT_FIELDS = (
    "selection_contract_valid",
    "manifest_present",
    "summary_present",
    "trades_csv_present",
    "manifest_hash_present",
    "summary_hash_present",
    "trades_csv_hash_from_manifest_present",
    "positive_legacy_max_drawdown_rejected",
    "forbidden_fields_rejected",
    "live_paper_order_rejected",
    "private_api_rejected",
    "background_execution_rejected",
    "package_release_artifacts_excluded",
)
SAFE_TRUE_DEFAULTS = (
    "operator_confirmation_required",
    "preview_only_before_confirmation",
    "not_selectable_for_live",
    "not_selectable_for_paper",
    "safety_research_only",
)
SAFE_FALSE_DEFAULTS = (
    "operator_confirmed",
    "execution_enabled_after_confirmation",
    "paper_live_order_execution",
)
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
COMMAND_FORBIDDEN_PATTERNS = (
    re.compile(r"\b--mode\s+live\b", re.IGNORECASE),
    re.compile(r"\b--mode\s+paper\b", re.IGNORECASE),
    re.compile(r"\blive\b", re.IGNORECASE),
    re.compile(r"\bpaper\b", re.IGNORECASE),
    re.compile(r"\border[_ -]?(?:submit|fetch)\b", re.IGNORECASE),
    re.compile(r"\bbalance[_ -]?fetch\b", re.IGNORECASE),
    re.compile(r"\bprivate[_ -]?api\b", re.IGNORECASE),
    re.compile(r"\bapi[_ -]?key\b", re.IGNORECASE),
    re.compile(r"\bsecret\b", re.IGNORECASE),
    re.compile(r"\btoken\b", re.IGNORECASE),
    re.compile(r"\bauth(?:orization)?\b", re.IGNORECASE),
)
POLICY_LIST_FIELDS = {"allowed_artifacts", "forbidden_artifacts", "fail_closed_reasons"}


class LocalDryRunRequestSchemaError(ValueError):
    pass


def load_sample_request() -> dict[str, Any]:
    assert SAMPLE_PATH.exists()
    return json.loads(SAMPLE_PATH.read_text(encoding="utf-8"))


def _doc_text() -> str:
    return "\n".join(path.read_text(encoding="utf-8") for path in DOC_PATHS)


def _normalized_key(key: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", key.lower()).strip("_")


def _scan_forbidden_text(text: str, path: str) -> None:
    for pattern in FORBIDDEN_TEXT_PATTERNS:
        if pattern.search(text):
            raise LocalDryRunRequestSchemaError(f"forbidden text at {path}: {pattern.pattern}")


def _assert_command_preview_is_safe(command_text: str) -> None:
    for pattern in COMMAND_FORBIDDEN_PATTERNS:
        if pattern.search(command_text):
            raise LocalDryRunRequestSchemaError(f"unsafe command_text_preview: {pattern.pattern}")


def assert_no_forbidden_local_dry_run_request_fields(request: Any, path: str = "$") -> None:
    if isinstance(request, dict):
        for key, value in request.items():
            normalized = _normalized_key(str(key))
            if normalized in FORBIDDEN_KEYS:
                raise LocalDryRunRequestSchemaError(f"forbidden key at {path}: {key}")
            if key == "command_text_preview":
                _assert_command_preview_is_safe(str(value))
            if key in POLICY_LIST_FIELDS:
                continue
            _scan_forbidden_text(str(key), f"{path}.{key}")
            assert_no_forbidden_local_dry_run_request_fields(value, f"{path}.{key}")
        return
    if isinstance(request, list):
        for index, value in enumerate(request):
            assert_no_forbidden_local_dry_run_request_fields(value, f"{path}[{index}]")
        return
    if isinstance(request, str):
        _scan_forbidden_text(request, path)


def assert_local_dry_run_request_defaults(request: dict[str, Any]) -> None:
    for field in SAFE_TRUE_DEFAULTS:
        if request.get(field) is not True:
            raise LocalDryRunRequestSchemaError(f"{field} must be true")
    for field in SAFE_FALSE_DEFAULTS:
        if request.get(field) is not False:
            raise LocalDryRunRequestSchemaError(f"{field} must be false")


def validate_local_dry_run_request_schema(request: dict[str, Any]) -> bool:
    missing = [field for field in REQUIRED_TOP_LEVEL_FIELDS if field not in request]
    if missing:
        raise LocalDryRunRequestSchemaError(f"missing required fields: {missing}")
    missing_preflight = [field for field in REQUIRED_PREFLIGHT_FIELDS if field not in request["preflight"]]
    if missing_preflight:
        raise LocalDryRunRequestSchemaError(f"missing preflight fields: {missing_preflight}")
    if request["request_type"] != REQUEST_TYPE:
        raise LocalDryRunRequestSchemaError("invalid request_type")
    if request["request_schema_version"] != SCHEMA_VERSION:
        raise LocalDryRunRequestSchemaError("invalid request_schema_version")
    if request["phase"] != PHASE:
        raise LocalDryRunRequestSchemaError("invalid phase")
    if request["product"] != "free":
        raise LocalDryRunRequestSchemaError("product must be free")
    if request["dry_run_mode"] not in ALLOWED_DRY_RUN_MODES:
        raise LocalDryRunRequestSchemaError("invalid dry_run_mode")
    if request["dry_run_mode"] in NOT_ALLOWED_MODES:
        raise LocalDryRunRequestSchemaError("not allowed dry_run_mode")
    if request["status"] not in ALLOWED_STATUS:
        raise LocalDryRunRequestSchemaError("invalid status")
    if not set(request["allowed_artifacts"]).issubset(ALLOWED_ARTIFACTS):
        raise LocalDryRunRequestSchemaError("invalid allowed_artifacts")
    if not set(request["forbidden_artifacts"]).issubset(FORBIDDEN_ARTIFACTS):
        raise LocalDryRunRequestSchemaError("invalid forbidden_artifacts")
    if not set(request["fail_closed_reasons"]).issubset(FAIL_CLOSED_REASONS):
        raise LocalDryRunRequestSchemaError("invalid fail_closed_reasons")
    for field in REQUIRED_PREFLIGHT_FIELDS:
        if type(request["preflight"][field]) is not bool:
            raise LocalDryRunRequestSchemaError(f"{field} must be bool")
    assert_local_dry_run_request_defaults(request)
    assert_no_forbidden_local_dry_run_request_fields(request)
    return True


def make_valid_request(**overrides: Any) -> dict[str, Any]:
    request = deepcopy(load_sample_request())
    request.update(overrides)
    return request


def make_invalid_request(**overrides: Any) -> dict[str, Any]:
    return make_valid_request(**overrides)


def _request_without_policy_lists(request: dict[str, Any]) -> dict[str, Any]:
    sanitized = deepcopy(request)
    for field in POLICY_LIST_FIELDS:
        sanitized.pop(field, None)
    return sanitized


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


def _git_added_lines(paths: tuple[Path, ...]) -> str:
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
    lines = [line for line in output.splitlines() if line.startswith("+") and not line.startswith("+++")]
    return "\n".join(lines)


def _app_version_from(text: str) -> str:
    match = re.search(r'^APP_VERSION\s*=\s*["\']([^"\']+)["\']', text, flags=re.MULTILINE)
    assert match is not None
    return match.group(1)


def test_request_schema_doc_and_sample_json_exist() -> None:
    assert SCHEMA_DOC.exists()
    assert SAMPLE_PATH.exists()


def test_sample_request_json_is_valid_and_has_identity_fields() -> None:
    request = load_sample_request()

    assert isinstance(request, dict)
    assert request["request_type"] == REQUEST_TYPE
    assert request["product"] == "free"
    assert request["dry_run_mode"] in ALLOWED_DRY_RUN_MODES
    assert request["status"] in ALLOWED_STATUS
    assert validate_local_dry_run_request_schema(request)


def test_sample_request_required_fields_and_defaults_are_fixed() -> None:
    request = load_sample_request()

    for field in REQUIRED_TOP_LEVEL_FIELDS:
        assert field in request
    for field in REQUIRED_PREFLIGHT_FIELDS:
        assert field in request["preflight"]
    assert request["operator_confirmation_required"] is True
    assert request["operator_confirmed"] is False
    assert request["preview_only_before_confirmation"] is True
    assert request["execution_enabled_after_confirmation"] is False
    assert request["not_selectable_for_live"] is True
    assert request["not_selectable_for_paper"] is True
    assert request["safety_research_only"] is True
    assert request["paper_live_order_execution"] is False


def test_sample_request_allowed_and_forbidden_policy_lists_are_schema_enums() -> None:
    request = load_sample_request()

    assert set(request["allowed_artifacts"]) == ALLOWED_ARTIFACTS
    assert set(request["forbidden_artifacts"]) == FORBIDDEN_ARTIFACTS
    assert set(request["fail_closed_reasons"]).issubset(FAIL_CLOSED_REASONS)


def test_sample_request_contains_no_forbidden_payload_fields_or_text() -> None:
    request = load_sample_request()
    request_without_policy = _request_without_policy_lists(request)
    sample_payload_text = json.dumps(request_without_policy, sort_keys=True)

    assert_no_forbidden_local_dry_run_request_fields(request)
    for forbidden in (
        "entry_exec",
        "exit_exec",
        '"qty"',
        "trade id",
        "trade_id",
        "api key",
        "api_secret",
        "authorization:",
        "raw order",
        "raw_order",
        "raw billing",
        "balance snapshot",
        "raw market data",
        "raw ohlcv",
    ):
        assert forbidden not in sample_payload_text.lower()


def test_sample_request_contains_no_screenshot_path_generated_body_order_or_balance_payload() -> None:
    request = _request_without_policy_lists(load_sample_request())
    sample_payload_text = json.dumps(request, sort_keys=True).lower()

    assert not re.search(r"screenshot[^\n]*(?:\.png|\.jpg|\.jpeg|\.webp|[\\/])", sample_payload_text)
    for forbidden in (
        "generated real signal tape body",
        "raw trades rows",
        "raw_trade_rows",
        "raw_order",
        "balance_snapshot",
        "order_id",
        "account details",
        "private key",
    ):
        assert forbidden not in sample_payload_text


@pytest.mark.parametrize("bad_mode", ["live", "paper", "private_api", "producer_auto_run", "bad_mode", "", None])
def test_invalid_dry_run_mode_is_rejected(bad_mode: Any) -> None:
    with pytest.raises(LocalDryRunRequestSchemaError, match="dry_run_mode"):
        validate_local_dry_run_request_schema(make_invalid_request(dry_run_mode=bad_mode))


@pytest.mark.parametrize("bad_status", ["ok", "success", "executed", "", None])
def test_invalid_status_is_rejected(bad_status: Any) -> None:
    with pytest.raises(LocalDryRunRequestSchemaError, match="status"):
        validate_local_dry_run_request_schema(make_invalid_request(status=bad_status))


def test_product_other_than_free_is_rejected() -> None:
    with pytest.raises(LocalDryRunRequestSchemaError, match="product"):
        validate_local_dry_run_request_schema(make_invalid_request(product="standard"))


def test_operator_confirmed_true_is_rejected_for_phase18_docs_only_sample() -> None:
    with pytest.raises(LocalDryRunRequestSchemaError, match="operator_confirmed"):
        validate_local_dry_run_request_schema(make_invalid_request(operator_confirmed=True))


def test_execution_enabled_after_confirmation_true_is_rejected() -> None:
    with pytest.raises(LocalDryRunRequestSchemaError, match="execution_enabled_after_confirmation"):
        validate_local_dry_run_request_schema(make_invalid_request(execution_enabled_after_confirmation=True))


def test_not_selectable_for_live_false_is_rejected() -> None:
    with pytest.raises(LocalDryRunRequestSchemaError, match="not_selectable_for_live"):
        validate_local_dry_run_request_schema(make_invalid_request(not_selectable_for_live=False))


def test_not_selectable_for_paper_false_is_rejected() -> None:
    with pytest.raises(LocalDryRunRequestSchemaError, match="not_selectable_for_paper"):
        validate_local_dry_run_request_schema(make_invalid_request(not_selectable_for_paper=False))


def test_safety_research_only_false_is_rejected() -> None:
    with pytest.raises(LocalDryRunRequestSchemaError, match="safety_research_only"):
        validate_local_dry_run_request_schema(make_invalid_request(safety_research_only=False))


def test_paper_live_order_execution_true_is_rejected() -> None:
    with pytest.raises(LocalDryRunRequestSchemaError, match="paper_live_order_execution"):
        validate_local_dry_run_request_schema(make_invalid_request(paper_live_order_execution=True))


@pytest.mark.parametrize(
    "unsafe_command",
    [
        "python runner.py --mode live --use-precomputed-signals",
        "python runner.py --mode paper --use-precomputed-signals",
        "python runner.py order_submit",
        "python runner.py order_fetch",
        "python runner.py balance_fetch",
    ],
)
def test_command_text_preview_unsafe_modes_are_rejected(unsafe_command: str) -> None:
    with pytest.raises(LocalDryRunRequestSchemaError, match="command_text_preview"):
        validate_local_dry_run_request_schema(make_invalid_request(command_text_preview=unsafe_command))


def test_request_containing_raw_trade_row_key_is_rejected() -> None:
    record = make_invalid_request(raw_trade_rows=[{"safe": "no"}])

    with pytest.raises(LocalDryRunRequestSchemaError, match="raw_trade_rows"):
        validate_local_dry_run_request_schema(record)


@pytest.mark.parametrize("key", ["entry_exec", "exit_exec", "qty", "trade_id"])
def test_request_containing_row_level_trade_fields_is_rejected(key: str) -> None:
    record = make_invalid_request(**{key: "unsafe"})

    with pytest.raises(LocalDryRunRequestSchemaError, match=key):
        validate_local_dry_run_request_schema(record)


def test_docs_include_required_field_groups_allowed_enums_defaults_and_artifact_policy() -> None:
    text = _doc_text()

    for expected in (
        "identity:",
        "selection:",
        "dry-run:",
        "confirmation:",
        "safety:",
        "preflight:",
        "status:",
        "Allowed `dry_run_mode` values",
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
        "Required defaults",
        "`operator_confirmation_required = true`",
        "`operator_confirmed = false`",
        "`execution_enabled_after_confirmation = false in Phase 18 sample`",
        "Allowed `allowed_artifacts` enum values",
        "`safe_summary_json`",
        "`fast_path_equity_curve_csv`",
        "`fast_path_trades_csv_safe_condition`",
        "`fast_summary_json`",
        "`safe_metadata_log`",
        "`sanitized_manual_smoke_record`",
        "Forbidden `forbidden_artifacts` enum values",
        "`raw_market_data`",
        "`raw_trades_rows`",
        "`entry_exec`",
        "`exit_exec`",
        "`qty`",
        "`trade_id`",
        "`api_key`",
        "`authorization`",
        "`raw_billing`",
    ):
        assert expected in text


def test_docs_include_fail_closed_operator_confirmation_and_phase18_future_boundary() -> None:
    text = _doc_text()

    for expected in (
        "Allowed `fail_closed_reasons` enum values",
        "`invalid_selection`",
        "`missing_operator_confirmation`",
        "`output_path_in_package_release_area`",
        "`generated_artifact_policy_violation`",
        "Future dry-run must require explicit operator confirmation",
        "operator_confirmed=false by default",
        "missing confirmation fails closed",
        "local-only",
        "not LIVE/PAPER/order",
        "no private API",
        "no order/balance fetch",
        "confirmation is separate from copy command UX",
        "Phase 18 does not implement request builder",
        "Phase 18 does not create request files",
        "Phase 18 does not execute local dry-run",
        "future runtime dry-run execution must be separate and explicitly approved",
        "LIVE/PAPER/order remains permanently separated",
        "manual smoke record remains display-only",
        "dry-run request is not execution proof",
    ):
        assert expected in text


def test_gui_and_runtime_sources_have_no_phase18_execution_or_network_changes() -> None:
    added_gui_lines = _git_added_lines(GUI_SOURCE_PATHS)
    added_runtime_lines = _git_added_lines(RUNTIME_SOURCE_PATHS)
    combined_added_source = added_gui_lines + "\n" + added_runtime_lines

    assert _git_changed_names(GUI_SOURCE_PATHS) == set()
    assert _git_changed_names(RUNTIME_SOURCE_PATHS) == set()
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
        "Dry Run",
        "Execute",
        "Start",
    ):
        assert forbidden not in combined_added_source


def test_existing_phase_guard_tests_are_present_for_continued_validation_coverage() -> None:
    for path in EXISTING_GUARD_TESTS:
        assert (REPO_ROOT / path).exists()


def test_app_version_unchanged_and_package_release_artifacts_are_not_staged() -> None:
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
