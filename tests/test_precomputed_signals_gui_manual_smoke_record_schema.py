# BUILD_ID: 2026-05-09_free_precomputed_local_dry_run_gui_preview_wiring_v1
# BUILD_ID: 2026-05-09_free_precomputed_gui_manual_smoke_record_schema_v1
from __future__ import annotations

import json
import re
import subprocess
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_PATH = REPO_ROOT / "docs" / "precomputed_signals_gui_manual_smoke_record_sample.json"
RECORD_DOC = REPO_ROOT / "docs" / "precomputed_signals_gui_manual_smoke_record.md"
DOC_PATHS = (
    RECORD_DOC,
    REPO_ROOT / "docs" / "precomputed_signals_gui_smoke_checklist.md",
    REPO_ROOT / "docs" / "precomputed_signals_gui_smoke_procedure.md",
    REPO_ROOT / "docs" / "precomputed_signals_spec.md",
)
GUI_SOURCE_PATHS = (
    REPO_ROOT / "app" / "app" / "gui" / "main_window.py",
    REPO_ROOT / "app" / "app" / "gui" / "precomputed_signal_picker.py",
    REPO_ROOT / "app" / "app" / "gui" / "chart_dialog.py",
    REPO_ROOT / "app" / "app" / "gui" / "result_chart.py",
    REPO_ROOT / "app" / "app" / "gui" / "exchange_registry.py",
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

RECORD_TYPE = "precomputed_signal_gui_manual_smoke_record"
PHASE = "free_precomputed_signals_phase15_or_later"
ALLOWED_RESULT_STATUS = {"pass", "fail", "blocked", "not_run"}
ALLOWED_SMOKE_MODE = {"synthetic_fixture_display_only", "docs_static_check_only", "not_run"}
ALLOWED_FIXTURE_TYPE = {
    "synthetic_signal_tape",
    "synthetic_selection_contract",
    "synthetic_picker_item",
    "none",
}
REQUIRED_FIELDS = (
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
)
SAFE_BOOLEAN_FIELDS = (
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
)
PASS_TRUE_FIELDS = (
    "expected_labels_checked",
    "disabled_states_checked",
    "command_preview_checked",
    "copy_ux_checked",
    "diagnostics_checked",
    "live_paper_boundary_checked",
    "raw_trade_rows_not_visible",
)
OBSERVED_FALSE_FIELDS = (
    "command_execution_observed",
    "producer_auto_run_observed",
    "backtest_auto_run_observed",
    "runner_auto_run_observed",
    "order_path_observed",
    "balance_fetch_observed",
    "private_api_observed",
)
FORBIDDEN_KEYS = {
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


class ManualSmokeRecordSchemaError(ValueError):
    pass


def load_sample_record() -> dict[str, Any]:
    assert SAMPLE_PATH.exists()
    return json.loads(SAMPLE_PATH.read_text(encoding="utf-8"))


def _doc_text() -> str:
    return "\n".join(path.read_text(encoding="utf-8") for path in DOC_PATHS)


def _git_diff_added_lines(paths: tuple[Path, ...]) -> list[str]:
    rel_paths = [str(path.relative_to(REPO_ROOT)).replace("\\", "/") for path in paths]
    diff = subprocess.run(
        ["git", "diff", "HEAD", "--", *rel_paths],
        cwd=REPO_ROOT,
        check=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout
    return [line for line in diff.splitlines() if line.startswith("+") and not line.startswith("+++")]


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


def _app_version_from(text: str) -> str:
    match = re.search(r'^APP_VERSION\s*=\s*["\']([^"\']+)["\']', text, flags=re.MULTILINE)
    assert match is not None
    return match.group(1)


def _normalized_key(key: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", key.lower()).strip("_")


def _scan_forbidden_text(text: str, path: str) -> None:
    for pattern in FORBIDDEN_TEXT_PATTERNS:
        if pattern.search(text):
            raise ManualSmokeRecordSchemaError(f"forbidden text at {path}: {pattern.pattern}")


def assert_no_forbidden_manual_smoke_fields(record: Any, path: str = "$") -> None:
    if isinstance(record, dict):
        for key, value in record.items():
            normalized = _normalized_key(str(key))
            if normalized in FORBIDDEN_KEYS:
                raise ManualSmokeRecordSchemaError(f"forbidden key at {path}: {key}")
            _scan_forbidden_text(str(key), f"{path}.{key}")
            assert_no_forbidden_manual_smoke_fields(value, f"{path}.{key}")
        return
    if isinstance(record, list):
        for index, value in enumerate(record):
            assert_no_forbidden_manual_smoke_fields(value, f"{path}[{index}]")
        return
    if isinstance(record, str):
        _scan_forbidden_text(record, path)


def assert_manual_smoke_record_pass_invariants(record: dict[str, Any]) -> None:
    for field in PASS_TRUE_FIELDS:
        if record.get(field) is not True:
            raise ManualSmokeRecordSchemaError(f"pass record requires {field}=true")
    for field in OBSERVED_FALSE_FIELDS:
        if record.get(field) is not False:
            raise ManualSmokeRecordSchemaError(f"pass record requires {field}=false")


def validate_manual_smoke_record_schema(record: dict[str, Any]) -> bool:
    missing = [field for field in REQUIRED_FIELDS if field not in record]
    if missing:
        raise ManualSmokeRecordSchemaError(f"missing required fields: {missing}")
    if record["record_type"] != RECORD_TYPE:
        raise ManualSmokeRecordSchemaError("invalid record_type")
    if record["phase"] != PHASE:
        raise ManualSmokeRecordSchemaError("invalid phase")
    if record["result_status"] not in ALLOWED_RESULT_STATUS:
        raise ManualSmokeRecordSchemaError("invalid result_status")
    if record["smoke_mode"] not in ALLOWED_SMOKE_MODE:
        raise ManualSmokeRecordSchemaError("invalid smoke_mode")
    if record["fixture_type"] not in ALLOWED_FIXTURE_TYPE:
        raise ManualSmokeRecordSchemaError("invalid fixture_type")
    for field in SAFE_BOOLEAN_FIELDS:
        if type(record[field]) is not bool:
            raise ManualSmokeRecordSchemaError(f"{field} must be bool")
    assert_no_forbidden_manual_smoke_fields(record)
    if record["result_status"] == "pass":
        assert_manual_smoke_record_pass_invariants(record)
    if record["result_status"] == "not_run":
        for field in OBSERVED_FALSE_FIELDS:
            if record.get(field) is not False:
                raise ManualSmokeRecordSchemaError(f"not_run record requires {field}=false")
    return True


def make_valid_record(**overrides: Any) -> dict[str, Any]:
    record = deepcopy(load_sample_record())
    record.update(overrides)
    return record


def make_invalid_record(**overrides: Any) -> dict[str, Any]:
    return make_valid_record(**overrides)


def test_sample_json_exists_and_is_valid_json() -> None:
    assert SAMPLE_PATH.exists()
    record = load_sample_record()
    assert isinstance(record, dict)


def test_sample_json_schema_record_type_phase_and_enums_are_valid() -> None:
    record = load_sample_record()

    assert "schema_version" in record
    assert record["record_type"] == RECORD_TYPE
    assert record["phase"] == PHASE
    assert record["result_status"] in ALLOWED_RESULT_STATUS
    assert record["smoke_mode"] in ALLOWED_SMOKE_MODE
    assert record["fixture_type"] in ALLOWED_FIXTURE_TYPE
    assert validate_manual_smoke_record_schema(record)


def test_sample_json_required_fields_and_safe_booleans_are_valid() -> None:
    record = load_sample_record()

    for field in REQUIRED_FIELDS:
        assert field in record
    for field in SAFE_BOOLEAN_FIELDS:
        assert type(record[field]) is bool


def test_sample_json_status_invariants_are_valid() -> None:
    record = load_sample_record()

    if record["result_status"] == "pass":
        assert_manual_smoke_record_pass_invariants(record)
    if record["result_status"] == "not_run":
        for field in OBSERVED_FALSE_FIELDS:
            assert record[field] is False


def test_sample_json_contains_no_forbidden_fields_or_text() -> None:
    record = load_sample_record()
    sample_text = SAMPLE_PATH.read_text(encoding="utf-8")

    assert_no_forbidden_manual_smoke_fields(record)
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
    ):
        assert forbidden not in sample_text.lower()


def test_sample_json_contains_no_screenshot_path_or_generated_payload() -> None:
    record = load_sample_record()
    sample_text = SAMPLE_PATH.read_text(encoding="utf-8").lower()

    assert "screenshot" not in {str(key).lower() for key in record}
    assert not re.search(r"screenshot[^\n]*(?:\.png|\.jpg|\.jpeg|\.webp|[\\/])", sample_text)
    for forbidden in (
        "raw market data",
        "raw ohlcv",
        "generated real signal tape body",
        "generated smoke output",
        "generated smoke record",
        "account details",
        "private key",
    ):
        assert forbidden not in sample_text


@pytest.mark.parametrize("bad_status", ["ok", "success", "", None])
def test_invalid_result_status_is_rejected(bad_status: Any) -> None:
    with pytest.raises(ManualSmokeRecordSchemaError, match="result_status"):
        validate_manual_smoke_record_schema(make_invalid_record(result_status=bad_status))


@pytest.mark.parametrize("bad_mode", ["runtime_smoke", "display", "", None])
def test_invalid_smoke_mode_is_rejected(bad_mode: Any) -> None:
    with pytest.raises(ManualSmokeRecordSchemaError, match="smoke_mode"):
        validate_manual_smoke_record_schema(make_invalid_record(smoke_mode=bad_mode))


@pytest.mark.parametrize("bad_fixture_type", ["real_signal_tape", "runtime_output", "", None])
def test_invalid_fixture_type_is_rejected(bad_fixture_type: Any) -> None:
    with pytest.raises(ManualSmokeRecordSchemaError, match="fixture_type"):
        validate_manual_smoke_record_schema(make_invalid_record(fixture_type=bad_fixture_type))


def test_missing_required_field_is_rejected() -> None:
    record = make_invalid_record()
    del record["schema_version"]

    with pytest.raises(ManualSmokeRecordSchemaError, match="missing required"):
        validate_manual_smoke_record_schema(record)


def test_non_bool_safe_boolean_field_is_rejected() -> None:
    with pytest.raises(ManualSmokeRecordSchemaError, match="command_preview_checked"):
        validate_manual_smoke_record_schema(make_invalid_record(command_preview_checked="false"))


def _passing_record(**overrides: Any) -> dict[str, Any]:
    record = make_valid_record(result_status="pass")
    for field in PASS_TRUE_FIELDS:
        record[field] = True
    for field in OBSERVED_FALSE_FIELDS:
        record[field] = False
    record.update(overrides)
    return record


def test_pass_record_with_command_execution_observed_is_rejected() -> None:
    with pytest.raises(ManualSmokeRecordSchemaError, match="command_execution_observed"):
        validate_manual_smoke_record_schema(_passing_record(command_execution_observed=True))


def test_pass_record_with_order_path_observed_is_rejected() -> None:
    with pytest.raises(ManualSmokeRecordSchemaError, match="order_path_observed"):
        validate_manual_smoke_record_schema(_passing_record(order_path_observed=True))


def test_pass_record_with_private_api_observed_is_rejected() -> None:
    with pytest.raises(ManualSmokeRecordSchemaError, match="private_api_observed"):
        validate_manual_smoke_record_schema(_passing_record(private_api_observed=True))


def test_record_containing_raw_trade_row_key_is_rejected() -> None:
    record = make_invalid_record(raw_trade_rows=[{"safe": "no"}])

    with pytest.raises(ManualSmokeRecordSchemaError, match="raw_trade_rows"):
        validate_manual_smoke_record_schema(record)


@pytest.mark.parametrize("key", ["entry_exec", "exit_exec", "qty", "trade_id"])
def test_record_containing_row_level_trade_fields_is_rejected(key: str) -> None:
    record = make_invalid_record(**{key: "unsafe"})

    with pytest.raises(ManualSmokeRecordSchemaError, match=key):
        validate_manual_smoke_record_schema(record)


def test_docs_list_schema_fields_allowed_enums_and_forbidden_fields() -> None:
    text = _doc_text()

    for field in REQUIRED_FIELDS:
        assert field in text
    for expected in (
        RECORD_TYPE,
        PHASE,
        "`pass`",
        "`fail`",
        "`blocked`",
        "`not_run`",
        "`synthetic_fixture_display_only`",
        "`docs_static_check_only`",
        "`synthetic_signal_tape`",
        "`synthetic_selection_contract`",
        "`synthetic_picker_item`",
        "`none`",
    ):
        assert expected in text
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
    ):
        assert forbidden in text


def test_docs_record_screenshot_and_phase16_runtime_boundaries() -> None:
    text = _doc_text()

    for expected in (
        "Phase 16 is docs/tests-only",
        "GUI runtime smoke is not executed in Phase 16",
        "GUI runtime smoke not executed in Phase 15/16",
        "not generated runtime output",
        "not a generated smoke record",
        "sanitized and synthetic",
        "No screenshots are required",
        "screenshots optional",
        "screenshots must be omitted from repo and zip unless explicitly sanitized",
        "screenshots are optional and must not be committed unless sanitized in a",
        "future local-only dry-run design is future phase",
        "APP_VERSION unchanged",
        "package/release not touched",
        "generated real tape body / raw market data excluded",
        "raw trade rows are never recorded",
        "max_drawdown` remains signed negative legacy field",
        "GUI display prefers `max_dd_abs / max_dd_pct`",
    ):
        assert expected in text


def test_docs_record_no_command_auto_run_network_or_background_boundaries() -> None:
    text = _doc_text()

    for expected in (
        "no command execution",
        "no producer/backtest/runner/inventory auto-run",
        "no selection or adapter auto-run",
        "no LIVE/PAPER/order",
        "no MEXC private API",
        "no order submit/fetch",
        "no balance fetch",
        "no subprocess / QProcess / background worker",
        "`os.system`",
        "`Popen`",
        "`startDetached`",
        "threading",
        "multiprocessing",
        "scheduler",
        "worker",
        "background worker",
    ):
        assert expected in text


def test_gui_source_has_no_phase16_added_execution_network_or_order_connections() -> None:
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


def test_runtime_source_and_app_version_are_unchanged() -> None:
    assert _git_changed_names(GUI_SOURCE_PATHS).issubset(
        {"app/app/gui/main_window.py", "app/app/gui/precomputed_signal_picker.py"}
    )
    assert _git_changed_names(RUNTIME_SOURCE_PATHS) == set()

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


def test_package_zip_exe_installer_and_release_artifacts_are_not_staged() -> None:
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
