# BUILD_ID: 2026-05-09_free_precomputed_local_dry_run_gui_preview_wiring_v1
# BUILD_ID: 2026-05-09_free_precomputed_local_dry_run_gui_preview_adapter_v1
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

import precomputed_signals_local_dry_run_gui_adapter as adapter
import precomputed_signals_local_dry_run_request as builder
import precomputed_signals_selection as selection
import signal_tape as tape

ADAPTER_PATH = REPO_ROOT / "precomputed_signals_local_dry_run_gui_adapter.py"
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


def _write_synthetic_tape(root: Path, *, signal_set_id: str = "sig_gui_preview_adapter_fixture") -> Path:
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
        dataset_id="BTCUSDT_5m_1h_gui_preview_adapter_unit",
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
        output_dir=str(tmp_path / "safe_gui_preview_output"),
    )


def _preview(tmp_path: Path, *, mode: str = "backtest_fast_path_local_only") -> dict[str, Any]:
    return adapter.build_local_dry_run_gui_preview(_request(tmp_path, mode=mode))


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


def test_gui_preview_adapter_module_exists() -> None:
    assert ADAPTER_PATH.exists()
    assert adapter.BUILD_ID == "2026-05-09_free_precomputed_local_dry_run_gui_preview_adapter_v1"


def test_adapter_can_create_preview_from_valid_request_dict(tmp_path: Path) -> None:
    request = _request(tmp_path)
    preview = adapter.build_local_dry_run_gui_preview(request)

    assert adapter.validate_local_dry_run_gui_preview(preview) == preview
    assert preview["gui_preview_schema_version"] == "free_precomputed_local_dry_run_gui_preview_v1"
    assert preview["source_request_schema_version"] == builder.REQUEST_SCHEMA_VERSION
    assert preview["title"] == "Local dry-run request preview"
    assert preview["subtitle"] == "Preview only - execution disabled"
    assert preview["status_label"] == "Preview only"


def test_adapter_can_create_preview_from_valid_synthetic_signal_dir(tmp_path: Path) -> None:
    signal_dir = _write_synthetic_tape(tmp_path)
    preview = adapter.build_local_dry_run_gui_preview_from_signal_dir(
        signal_dir,
        dry_run_mode="runner_replay_fast_path_local_only",
        output_dir=str(tmp_path / "safe_adapter_output"),
        expect_symbol="BTC/USDT",
        expect_entry_tf="5m",
        expect_filter_tf="1h",
        expect_signal_set_id="sig_gui_preview_adapter_fixture",
    )

    assert preview["status"] == "valid_preview"
    assert preview["symbol"] == "BTC/USDT"
    assert preview["symbol_normalized"] == "BTCUSDT"
    assert preview["dry_run_mode"] == "runner_replay_fast_path_local_only"


def test_preview_identity_selection_mode_and_safety_flags_are_fixed(tmp_path: Path) -> None:
    preview = _preview(tmp_path)

    assert preview["product"] == "free"
    assert preview["request_type"] == "precomputed_signal_local_dry_run_request"
    assert preview["phase"] == "free_precomputed_signals_phase20_gui_preview_adapter"
    assert preview["dry_run_mode"] == "backtest_fast_path_local_only"
    assert preview["dry_run_mode_label"] == "Backtest fast path local-only"
    assert preview["operator_confirmation_required"] is True
    assert preview["operator_confirmed"] is False
    assert preview["preview_only_before_confirmation"] is True
    assert preview["execution_enabled_after_confirmation"] is False
    assert preview["not_selectable_for_live"] is True
    assert preview["not_selectable_for_paper"] is True
    assert preview["safety_research_only"] is True
    assert preview["paper_live_order_execution"] is False


def test_runner_replay_mode_label_is_human_readable(tmp_path: Path) -> None:
    preview = _preview(tmp_path, mode="runner_replay_fast_path_local_only")

    assert preview["dry_run_mode_label"] == "Runner replay fast path local-only"


def test_preview_text_emphasizes_preview_only_disabled_and_future_phase(tmp_path: Path) -> None:
    preview = _preview(tmp_path)
    text = adapter.format_local_dry_run_gui_preview_text(preview)

    for expected in (
        "Preview only",
        "Execution disabled",
        "Not LIVE/PAPER/order",
        "No private API / no balance fetch / no order fetch",
        "This preview does not execute commands",
        "Future execution requires separate approved phase",
        "Command preview (display-only):",
    ):
        assert expected in text


def test_preview_artifact_and_fail_closed_labels_are_safe(tmp_path: Path) -> None:
    preview = _preview(tmp_path)

    assert preview["allowed_artifacts"] == list(builder.ALLOWED_ARTIFACTS)
    assert preview["forbidden_artifacts"] == list(builder.FORBIDDEN_ARTIFACTS)
    assert preview["allowed_artifacts_label"] == "Allowed artifacts: safe summary, equity curve, fast summary"
    assert preview["forbidden_artifacts_label"] == (
        "Forbidden artifacts: raw market data, raw trades rows, orders, balances, secrets"
    )
    assert preview["fail_closed_reasons"] == ["missing_operator_confirmation"]
    assert preview["fail_closed_reasons_label"] == "Fail-closed reasons: missing operator confirmation"


def test_command_preview_lines_are_display_only_and_validated(tmp_path: Path) -> None:
    preview = _preview(tmp_path)
    lines = adapter.command_preview_lines(preview)

    assert lines == preview["command_preview_lines"]
    assert "Command preview (display-only):" in lines
    assert preview["command_text_preview"] in lines


def test_preview_does_not_include_row_level_trade_payload_fields(tmp_path: Path) -> None:
    preview = _preview(tmp_path)
    unsafe_keys = {"raw_trade_rows", "entry_exec", "exit_exec", "qty", "trade_id"}

    assert unsafe_keys.isdisjoint(set(preview))
    assert unsafe_keys.isdisjoint(set(preview["command_text_preview"].lower().split()))
    payload_without_policy_labels = deepcopy(preview)
    for key in ("forbidden_artifacts", "forbidden_artifacts_label"):
        payload_without_policy_labels.pop(key, None)
    text = json.dumps(payload_without_policy_labels, ensure_ascii=True, sort_keys=True).lower()
    for forbidden in ("raw_trade_rows", "entry_exec", "exit_exec", '"qty"', "trade_id"):
        assert forbidden not in text


def test_preview_does_not_include_private_runtime_payload_fields(tmp_path: Path) -> None:
    preview = _preview(tmp_path)
    unsafe_keys = {
        "api_key",
        "secret",
        "token",
        "authorization",
        "raw_order",
        "balance_snapshot",
        "raw_billing",
    }

    assert unsafe_keys.isdisjoint(set(preview))
    for field in ("command_text_preview", "signal_dir", "output_dir", "status_reason"):
        field_text = str(preview[field]).lower()
        for forbidden in unsafe_keys:
            assert forbidden not in field_text


@pytest.mark.parametrize(
    ("field", "bad_value", "match"),
    [
        ("product", "standard", "product"),
        ("request_type", "invalid", "request_type"),
        ("dry_run_mode", "paper", "dry_run_mode"),
        ("status", "invalid", "status"),
        ("operator_confirmed", True, "operator_confirmed"),
        ("execution_enabled_after_confirmation", True, "execution_enabled_after_confirmation"),
        ("not_selectable_for_live", False, "not_selectable_for_live"),
        ("not_selectable_for_paper", False, "not_selectable_for_paper"),
        ("safety_research_only", False, "safety_research_only"),
        ("paper_live_order_execution", True, "paper_live_order_execution"),
    ],
)
def test_invalid_or_unsafe_request_mutations_are_rejected(
    tmp_path: Path,
    field: str,
    bad_value: Any,
    match: str,
) -> None:
    request = deepcopy(_request(tmp_path))
    request[field] = bad_value

    with pytest.raises((adapter.LocalDryRunGuiPreviewError, builder.LocalDryRunRequestError), match=match):
        adapter.build_local_dry_run_gui_preview(request)


@pytest.mark.parametrize(
    "command",
    [
        "python runner.py --mode live --use-precomputed-signals",
        "python runner.py --mode paper --use-precomputed-signals",
        "python backtest.py --order-submit",
        "python backtest.py --balance-fetch",
        "python backtest.py --private-api",
    ],
)
def test_unsafe_command_preview_text_is_rejected(tmp_path: Path, command: str) -> None:
    request = deepcopy(_request(tmp_path))
    request["command_text_preview"] = command

    with pytest.raises((adapter.LocalDryRunGuiPreviewError, builder.LocalDryRunRequestError), match="command"):
        adapter.build_local_dry_run_gui_preview(request)


def test_preview_validation_rejects_unknown_fail_closed_reason(tmp_path: Path) -> None:
    preview = _preview(tmp_path)
    preview["fail_closed_reasons"] = ["missing_operator_confirmation", "unsafe_unknown_value"]
    preview["fail_closed_reasons_label"] = "Fail-closed reasons: missing operator confirmation, unsafe unknown value"

    with pytest.raises(adapter.LocalDryRunGuiPreviewError, match="fail_closed_reasons"):
        adapter.validate_local_dry_run_gui_preview(preview)


def test_preview_validation_rejects_unsafe_output_dir(tmp_path: Path) -> None:
    preview = _preview(tmp_path)
    preview["output_dir"] = str(tmp_path / "release" / "package.zip")

    with pytest.raises(adapter.LocalDryRunGuiPreviewError, match="output_dir"):
        adapter.validate_local_dry_run_gui_preview(preview)


@pytest.mark.parametrize("key", ["raw_trade_rows", "entry_exec", "exit_exec", "qty", "trade_id"])
def test_request_with_row_level_payload_field_is_rejected(tmp_path: Path, key: str) -> None:
    request = deepcopy(_request(tmp_path))
    request[key] = "unsafe"

    with pytest.raises((adapter.LocalDryRunGuiPreviewError, builder.LocalDryRunRequestError), match=key):
        adapter.build_local_dry_run_gui_preview(request)


@pytest.mark.parametrize("key", ["api_key", "secret", "token", "authorization", "raw_order", "balance", "raw_billing"])
def test_request_with_private_or_order_payload_field_is_rejected(tmp_path: Path, key: str) -> None:
    request = deepcopy(_request(tmp_path))
    request[key] = "unsafe"

    with pytest.raises((adapter.LocalDryRunGuiPreviewError, builder.LocalDryRunRequestError), match=key):
        adapter.build_local_dry_run_gui_preview(request)


def test_adapter_source_import_and_execution_boundary_is_static() -> None:
    source = ADAPTER_PATH.read_text(encoding="utf-8")
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


def test_docs_mention_phase20_preview_adapter_and_non_execution_boundary() -> None:
    text = "\n".join(path.read_text(encoding="utf-8") for path in DOC_PATHS)

    for expected in (
        "Free Phase 20: request builder read-only GUI preview adapter",
        "adapter creates GUI preview item only",
        "adapter does not execute dry-run",
        "adapter does not run backtest/runner/producer/inventory",
        "adapter does not use subprocess / QProcess / background worker",
        "operator_confirmed=false",
        "execution_enabled_after_confirmation=false",
        "adapter display is preview-only",
        "adapter display is not LIVE/PAPER/order",
        "future execution still requires separate approval/phase",
        "GUI source is not connected in Phase 20",
        "raw trade rows / entry_exec / exit_exec / qty / trade id are not shown",
        "allowed / forbidden artifacts are shown as safe labels only",
        "generated GUI preview output is not committed",
    ):
        assert expected in text


def test_gui_and_runtime_source_have_no_new_execution_connections() -> None:
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
