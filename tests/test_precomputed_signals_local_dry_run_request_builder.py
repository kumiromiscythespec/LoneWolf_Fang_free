# BUILD_ID: 2026-05-09_free_precomputed_local_dry_run_gui_preview_wiring_v1
# BUILD_ID: 2026-05-09_free_precomputed_local_dry_run_request_builder_v1
from __future__ import annotations

import ast
import json
import re
import subprocess
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import precomputed_signals_local_dry_run_request as builder
import precomputed_signals_selection as selection
import signal_tape as tape

BUILDER_PATH = REPO_ROOT / "precomputed_signals_local_dry_run_request.py"
DOC_PATHS = (
    REPO_ROOT / "docs" / "precomputed_signals_gui_local_dry_run_request_schema.md",
    REPO_ROOT / "docs" / "precomputed_signals_gui_local_dry_run_design.md",
    REPO_ROOT / "docs" / "precomputed_signals_spec.md",
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


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_synthetic_tape(root: Path, *, signal_set_id: str = "sig_request_builder_fixture") -> Path:
    signal_dir = root / "signals" / "free" / "BTCUSDT" / "5m_1h" / signal_set_id
    signal_dir.mkdir(parents=True, exist_ok=True)

    trades_path = signal_dir / "trades.csv"
    trades_path.write_text("synthetic_header_only\n", encoding="utf-8")

    summary = tape.build_summary([], initial_equity=1000.0)
    _write_json(signal_dir / "summary.json", summary)

    manifest = tape.build_manifest_template(
        product=tape.DEFAULT_PRODUCT,
        producer_script="precompute_signals.py",
        producer_build_id=tape.BUILD_ID,
        symbol="BTC/USDT",
        entry_tf="5m",
        filter_tf="1h",
        since_ms=1000,
        until_ms=2000,
        dataset_id="BTCUSDT_5m_1h_request_builder_unit",
        dataset_files_hash=tape.stable_json_hash({"dataset": "synthetic"}),
        strategy_file_hash=tape.stable_json_hash({"strategy": "read_only_reference"}),
        strategy_build_id="read_only_reference",
        config_file_hash=tape.stable_json_hash({"config": "read_only_reference"}),
        signal_config_hash=tape.stable_json_hash({"signal": "synthetic"}),
        accounting_config_hash=tape.stable_json_hash({"accounting": "synthetic"}),
        signal_set_id=signal_set_id,
    )
    manifest["initial_equity"] = 1000.0
    manifest["files"]["trades_csv"]["sha256"] = tape.sha256_file(trades_path)
    manifest["files"]["summary_json"]["sha256"] = tape.sha256_file(signal_dir / "summary.json")
    manifest["counts"]["trades"] = 0
    manifest["counts"]["closed_trades"] = 0
    _write_json(signal_dir / "manifest.json", manifest)
    return signal_dir


def _selection(signal_dir: Path) -> dict[str, Any]:
    payload = selection.build_signal_tape_selection_contract(signal_dir)
    assert payload["status"] == "valid"
    return payload


def _request(tmp_path: Path, *, mode: str = "backtest_fast_path_local_only") -> dict[str, Any]:
    signal_dir = _write_synthetic_tape(tmp_path)
    return builder.build_local_dry_run_request(
        _selection(signal_dir),
        dry_run_mode=mode,
        output_dir=str(tmp_path / "safe_request_output"),
    )


def _command_without_paths(command: str) -> str:
    return re.sub(r'"[^"\r\n]*"', '""', command)


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


def test_builder_module_exists() -> None:
    assert BUILDER_PATH.exists()
    assert builder.BUILD_ID == "2026-05-09_free_precomputed_local_dry_run_request_builder_v1"


def test_builder_can_create_request_from_valid_synthetic_selection_contract(tmp_path: Path) -> None:
    signal_dir = _write_synthetic_tape(tmp_path)
    request = builder.build_local_dry_run_request(
        _selection(signal_dir),
        dry_run_mode="backtest_fast_path_local_only",
        output_dir=str(tmp_path / "safe_output"),
    )

    assert request["status"] == "valid_preview"
    assert request["selection_status"] == "valid"
    assert request["signal_dir"] == str(signal_dir)
    assert builder.validate_local_dry_run_request(request) == request


def test_builder_can_create_request_from_valid_synthetic_signal_dir(tmp_path: Path) -> None:
    signal_dir = _write_synthetic_tape(tmp_path)
    request = builder.build_local_dry_run_request_from_signal_dir(
        signal_dir,
        dry_run_mode="backtest_fast_path_local_only",
        output_dir=str(tmp_path / "safe_output"),
        expect_symbol="BTC/USDT",
        expect_entry_tf="5m",
        expect_filter_tf="1h",
        expect_signal_set_id="sig_request_builder_fixture",
    )

    assert request["status"] == "valid_preview"
    assert request["symbol"] == "BTC/USDT"
    assert request["symbol_normalized"] == "BTCUSDT"


def test_request_identity_schema_product_and_modes_are_fixed(tmp_path: Path) -> None:
    request = _request(tmp_path)

    assert request["request_type"] == "precomputed_signal_local_dry_run_request"
    assert request["request_schema_version"] == "free_precomputed_local_dry_run_request_v1"
    assert request["phase"] == "free_precomputed_signals_phase19_request_builder"
    assert request["product"] == "free"
    assert request["dry_run_mode"] == "backtest_fast_path_local_only"
    assert "runner_replay_fast_path_local_only" in builder.ALLOWED_DRY_RUN_MODES


def test_runner_replay_mode_is_accepted(tmp_path: Path) -> None:
    request = _request(tmp_path, mode="runner_replay_fast_path_local_only")

    assert request["dry_run_mode"] == "runner_replay_fast_path_local_only"
    assert request["status"] == "valid_preview"


def test_invalid_dry_run_mode_is_rejected(tmp_path: Path) -> None:
    signal_dir = _write_synthetic_tape(tmp_path)

    with pytest.raises(builder.LocalDryRunRequestError, match="dry_run_mode"):
        builder.build_local_dry_run_request(
            _selection(signal_dir),
            dry_run_mode="live",
            output_dir=str(tmp_path / "safe_output"),
        )


def test_required_confirmation_and_safety_defaults_are_fixed(tmp_path: Path) -> None:
    request = _request(tmp_path)

    assert request["operator_confirmation_required"] is True
    assert request["operator_confirmed"] is False
    assert request["preview_only_before_confirmation"] is True
    assert request["execution_enabled_after_confirmation"] is False
    assert request["not_selectable_for_live"] is True
    assert request["not_selectable_for_paper"] is True
    assert request["safety_research_only"] is True
    assert request["paper_live_order_execution"] is False


def test_command_previews_are_local_only_and_safe(tmp_path: Path) -> None:
    backtest_request = _request(tmp_path, mode="backtest_fast_path_local_only")
    runner_request = _request(tmp_path, mode="runner_replay_fast_path_local_only")

    backtest_command = _command_without_paths(backtest_request["command_text_preview"]).lower()
    runner_command = _command_without_paths(runner_request["command_text_preview"]).lower()
    assert "backtest.py" in backtest_command
    assert "--use-precomputed-signals" in backtest_command
    assert "runner.py" in runner_command
    assert "--mode replay" in runner_command
    assert "--use-precomputed-signals" in runner_command
    for command in (backtest_command, runner_command):
        for forbidden in ("live", "paper", "order", "balance", "private_api", "private api"):
            assert forbidden not in command


def test_request_policy_lists_output_dir_and_fail_closed_reasons_are_present(tmp_path: Path) -> None:
    request = _request(tmp_path)

    assert set(request["allowed_artifacts"]) == set(builder.ALLOWED_ARTIFACTS)
    assert set(request["forbidden_artifacts"]) == set(builder.FORBIDDEN_ARTIFACTS)
    assert set(request["fail_closed_reasons"]).issubset(set(builder.FAIL_CLOSED_REASON_OPTIONS))
    assert "missing_operator_confirmation" in request["fail_closed_reasons"]
    assert request["output_dir"] == str(tmp_path / "safe_request_output")


@pytest.mark.parametrize("unsafe_name", ["release", "release_asset", "package_zip", "installer_output", "setup_output"])
def test_unsafe_output_dir_is_rejected(tmp_path: Path, unsafe_name: str) -> None:
    signal_dir = _write_synthetic_tape(tmp_path)

    with pytest.raises(builder.LocalDryRunRequestError, match="output_path_in_package_release_area"):
        builder.build_local_dry_run_request(
            _selection(signal_dir),
            dry_run_mode="backtest_fast_path_local_only",
            output_dir=str(tmp_path / unsafe_name / "asset"),
        )


@pytest.mark.parametrize(
    ("field", "bad_value"),
    [
        ("product", "standard"),
        ("operator_confirmed", True),
        ("execution_enabled_after_confirmation", True),
        ("not_selectable_for_live", False),
        ("not_selectable_for_paper", False),
        ("paper_live_order_execution", True),
    ],
)
def test_validation_rejects_unsafe_default_mutations(tmp_path: Path, field: str, bad_value: Any) -> None:
    request = deepcopy(_request(tmp_path))
    request[field] = bad_value

    with pytest.raises(builder.LocalDryRunRequestError, match=field):
        builder.validate_local_dry_run_request(request)


def test_build_rejects_operator_confirmed_true(tmp_path: Path) -> None:
    signal_dir = _write_synthetic_tape(tmp_path)

    with pytest.raises(builder.LocalDryRunRequestError, match="operator_confirmed"):
        builder.build_local_dry_run_request(
            _selection(signal_dir),
            dry_run_mode="backtest_fast_path_local_only",
            output_dir=str(tmp_path / "safe_output"),
            operator_confirmed=True,
        )


@pytest.mark.parametrize("key", ["raw_trade_rows", "entry_exec", "exit_exec", "qty", "trade_id"])
def test_request_containing_raw_or_row_level_trade_fields_is_rejected(tmp_path: Path, key: str) -> None:
    request = deepcopy(_request(tmp_path))
    request[key] = "unsafe"

    with pytest.raises(builder.LocalDryRunRequestError, match=key):
        builder.validate_local_dry_run_request(request)


@pytest.mark.parametrize(
    "key",
    [
        "api_key",
        "secret",
        "token",
        "authorization",
        "raw_order",
        "balance_snapshot",
        "raw_billing",
    ],
)
def test_request_containing_private_runtime_or_billing_fields_is_rejected(tmp_path: Path, key: str) -> None:
    request = deepcopy(_request(tmp_path))
    request[key] = "unsafe"

    with pytest.raises(builder.LocalDryRunRequestError, match=key):
        builder.validate_local_dry_run_request(request)


def test_write_local_dry_run_request_json_writes_safe_json_to_tmp_path_only(tmp_path: Path) -> None:
    request = _request(tmp_path)
    out_path = tmp_path / "request_preview.json"

    written = builder.write_local_dry_run_request_json(out_path, request)
    payload = json.loads(written.read_text(encoding="utf-8"))
    payload_without_policy = deepcopy(payload)
    for key in ("allowed_artifacts", "forbidden_artifacts", "fail_closed_reasons"):
        payload_without_policy.pop(key, None)
    text_without_policy = json.dumps(payload_without_policy, ensure_ascii=True, sort_keys=True).lower()

    assert written == out_path
    assert payload["request_type"] == "precomputed_signal_local_dry_run_request"
    assert builder.validate_local_dry_run_request(payload) == payload
    for forbidden in ("entry_exec", "exit_exec", '"qty"', "trade_id", "api_key", "raw_order", "balance_snapshot"):
        assert forbidden not in text_without_policy


def test_cli_without_required_arguments_fails_closed(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    assert builder.main(["--dry-run-mode", "backtest_fast_path_local_only", "--output-dir", str(tmp_path)]) == 2
    assert json.loads(capsys.readouterr().out)["status"] == "invalid"

    signal_dir = _write_synthetic_tape(tmp_path)
    assert builder.main(["--signal-dir", str(signal_dir), "--dry-run-mode", "backtest_fast_path_local_only"]) == 2
    assert json.loads(capsys.readouterr().out)["status"] == "invalid"


def test_cli_invalid_mode_fails_closed(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    signal_dir = _write_synthetic_tape(tmp_path)

    assert builder.main(["--signal-dir", str(signal_dir), "--dry-run-mode", "paper", "--output-dir", str(tmp_path)]) == 2
    payload = json.loads(capsys.readouterr().out)

    assert payload["status"] == "invalid"


def test_cli_format_json_out_writes_safe_json_to_tmp_path(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    signal_dir = _write_synthetic_tape(tmp_path)
    out_path = tmp_path / "cli_request_preview.json"

    result = builder.main([
        "--signal-dir",
        str(signal_dir),
        "--dry-run-mode",
        "backtest_fast_path_local_only",
        "--output-dir",
        str(tmp_path / "safe_cli_output"),
        "--format",
        "json",
        "--out",
        str(out_path),
    ])
    payload = json.loads(out_path.read_text(encoding="utf-8"))

    assert result == 0
    assert out_path.exists()
    assert payload["status"] == "valid_preview"
    assert "precomputed local dry-run request" in capsys.readouterr().out


def test_cli_format_text_prints_safe_preview(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    signal_dir = _write_synthetic_tape(tmp_path)

    assert builder.main([
        "--signal-dir",
        str(signal_dir),
        "--dry-run-mode",
        "runner_replay_fast_path_local_only",
        "--output-dir",
        str(tmp_path / "safe_text_output"),
        "--format",
        "text",
    ]) == 0
    text = capsys.readouterr().out

    assert "precomputed local dry-run request" in text
    assert "runner.py --mode replay" in text
    assert "operator_confirmed=false" in text
    assert "execution_enabled_after_confirmation=false" in text


def test_builder_import_and_execution_boundary_is_static() -> None:
    source = BUILDER_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module.split(".", 1)[0])

    assert {"strategy", "indicators", "exchange", "ccxt", "runner", "backtest", "risk"}.isdisjoint(imports)
    for forbidden in ("subprocess", "QProcess", "Popen", "os.system", "threading", "multiprocessing", "scheduler"):
        assert forbidden not in source


def test_docs_mention_phase19_builder_preview_only_and_future_execution_boundary() -> None:
    text = "\n".join(path.read_text(encoding="utf-8") for path in DOC_PATHS)

    for expected in (
        "Free Phase 19: local-only dry-run request builder docs/helper",
        "request builder creates a safe request preview only",
        "request builder does not execute dry-run",
        "request builder does not run backtest/runner/producer/inventory",
        "request builder does not use subprocess / QProcess / background worker",
        "operator_confirmed=false by default",
        "execution_enabled_after_confirmation=false",
        "future execution still requires separate approval/phase",
        "generated request output not committed",
    ):
        assert expected in text


def test_gui_source_has_no_new_execution_connections() -> None:
    assert set(_git_diff_names(GUI_SOURCE_PATHS)).issubset(
        {"app/app/gui/main_window.py", "app/app/gui/precomputed_signal_picker.py"}
    )
    assert _git_diff_names(RUNTIME_SOURCE_PATHS) == []


def test_app_version_unchanged_and_package_artifacts_not_staged() -> None:
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
