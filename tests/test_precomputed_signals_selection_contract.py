# BUILD_ID: 2026-05-08_free_precomputed_signal_selection_contract_v1
from __future__ import annotations

import ast
import csv
import json
import sys
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import precomputed_signals_selection as selection
import signal_tape as tape


def _source_rows(symbol: str = "BTC/USDT") -> list[dict[str, Any]]:
    return [
        {
            "symbol": symbol,
            "direction": "long",
            "entry_ts_ms": 1000,
            "close_ts_ms": 2000,
            "entry_exec": 100.0,
            "exit_exec": 110.0,
            "qty": 2.0,
            "fee": 0.084,
            "pnl": 20.0,
            "net": 19.916,
            "equity_after": 1019.916,
            "reason": "TP_HIT",
        },
        {
            "symbol": symbol,
            "direction": "long",
            "entry_ts_ms": 3000,
            "close_ts_ms": 4000,
            "entry_exec": 110.0,
            "exit_exec": 105.0,
            "qty": 1.0,
            "fee": 0.043,
            "pnl": -5.0,
            "net": -5.043,
            "equity_after": 1014.873,
            "reason": "STOP_HIT_LOSS",
        },
        {
            "symbol": symbol,
            "direction": "long",
            "entry_ts_ms": 5000,
            "close_ts_ms": 6000,
            "entry_exec": 105.0,
            "exit_exec": 106.0,
            "qty": 1.0,
            "fee": 0.0211,
            "pnl": 1.0,
            "net": 0.9789,
            "equity_after": 1015.8519,
            "reason": "TIME_EXIT",
        },
    ]


def _manifest(
    *,
    symbol: str = "BTC/USDT",
    entry_tf: str = "5m",
    filter_tf: str = "1h",
    signal_set_id: str = "sig_selection_fixture",
) -> dict[str, Any]:
    return tape.build_manifest_template(
        product=tape.DEFAULT_PRODUCT,
        producer_script="precompute_signals.py",
        producer_build_id=tape.BUILD_ID,
        symbol=symbol,
        entry_tf=entry_tf,
        filter_tf=filter_tf,
        since_ms=1000,
        until_ms=6000,
        dataset_id=f"{tape.normalize_symbol(symbol)}_{entry_tf}_{filter_tf}_selection_unit",
        dataset_files_hash=tape.stable_json_hash({"dataset": "unit"}),
        strategy_file_hash=tape.stable_json_hash({"strategy": "read_only_reference"}),
        strategy_build_id="read_only_reference",
        config_file_hash=tape.stable_json_hash({"config": "read_only_reference"}),
        signal_config_hash=tape.stable_json_hash({"signal": "unit"}),
        accounting_config_hash=tape.stable_json_hash({"accounting": "unit"}),
        signal_set_id=signal_set_id,
    )


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_header_only_trades(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=tape.TRADE_COLUMNS)
        writer.writeheader()


def _write_valid_tape(
    root: Path,
    *,
    symbol: str = "BTC/USDT",
    entry_tf: str = "5m",
    filter_tf: str = "1h",
    signal_set_id: str = "sig_selection_fixture",
) -> Path:
    out_dir = tape.signal_tape_dir(
        product=tape.DEFAULT_PRODUCT,
        symbol=symbol,
        entry_tf=entry_tf,
        filter_tf=filter_tf,
        signal_set_id=signal_set_id,
        root=root,
    )
    trades = tape.canonicalize_trade_rows(_source_rows(symbol), symbol=symbol, initial_equity=1000.0)
    manifest = _manifest(symbol=symbol, entry_tf=entry_tf, filter_tf=filter_tf, signal_set_id=signal_set_id)
    manifest["initial_equity"] = 1000.0
    tape.write_signal_tape(out_dir, manifest=manifest, trades=trades)
    return out_dir


def _payload_from_stdout(capsys: pytest.CaptureFixture[str]) -> dict[str, Any]:
    return json.loads(capsys.readouterr().out)


def _assert_invalid(payload: dict[str, Any], code: str) -> None:
    assert payload["status"] == "invalid"
    assert payload["safe_error_code"] == code
    assert payload["selectable_for_backtest_fast_path"] is False
    assert payload["selectable_for_runner_replay_fast_path"] is False
    assert payload["not_selectable_for_live"] is True
    assert payload["not_selectable_for_paper"] is True


def test_selection_cli_without_signal_dir_fails_closed(capsys: pytest.CaptureFixture[str]) -> None:
    assert selection.main(["--format", "json"]) == 2
    _assert_invalid(_payload_from_stdout(capsys), "missing_signal_dir")


def test_selection_product_defaults_to_free(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    tape_dir = _write_valid_tape(tmp_path)

    assert selection.main(["--signal-dir", str(tape_dir), "--format", "json"]) == 0
    payload = _payload_from_stdout(capsys)

    assert payload["product"] == "free"
    assert payload["status"] == "valid"


def test_selection_non_free_product_fails_closed(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    tape_dir = _write_valid_tape(tmp_path)

    assert selection.main(["--signal-dir", str(tape_dir), "--product", "standard", "--format", "json"]) == 2
    _assert_invalid(_payload_from_stdout(capsys), "product_mismatch")


def test_selection_contract_from_valid_synthetic_tape_has_picker_flags(tmp_path: Path) -> None:
    tape_dir = _write_valid_tape(tmp_path)

    payload = selection.build_signal_tape_selection_contract(tape_dir)

    assert payload["schema_version"] == tape.SCHEMA_VERSION
    assert payload["selection_schema_version"] == selection.SELECTION_SCHEMA_VERSION
    assert payload["product"] == "free"
    assert payload["signal_dir"] == str(tape_dir)
    assert payload["symbol"] == "BTC/USDT"
    assert payload["symbol_normalized"] == "BTCUSDT"
    assert payload["entry_tf"] == "5m"
    assert payload["filter_tf"] == "1h"
    assert payload["signal_set_id"] == "sig_selection_fixture"
    assert payload["trade_count"] == 3
    assert payload["status"] == "valid"
    assert payload["selectable_for_backtest_fast_path"] is True
    assert payload["selectable_for_runner_replay_fast_path"] is True
    assert payload["not_selectable_for_live"] is True
    assert payload["not_selectable_for_paper"] is True
    assert payload["safety_research_only"] is True
    assert payload["safety_paper_live_order_execution"] is False
    assert payload["tape_files_present"] == {"manifest_json": True, "summary_json": True, "trades_csv": True}
    assert payload["manifest_sha256"]
    assert payload["summary_sha256"]
    assert payload["trades_csv_sha256_from_manifest"]


def test_selection_contract_preserves_dd_schema_v2_display_fields(tmp_path: Path) -> None:
    tape_dir = _write_valid_tape(tmp_path)
    payload = selection.build_signal_tape_selection_contract(tape_dir)

    assert payload["dd_schema_version"] == tape.DD_SCHEMA_VERSION
    assert payload["dd_sign_convention"] == tape.DD_SIGN_CONVENTION
    assert payload["max_drawdown"] == pytest.approx(payload["max_dd_signed"])
    assert payload["max_drawdown"] <= 0.0
    assert payload["max_dd_display_abs"] == pytest.approx(payload["max_dd_abs"])
    assert payload["max_dd_display_pct"] == pytest.approx(payload["max_dd_pct"])
    assert payload["max_dd_display_label"] == "Max DD (abs, display)"
    assert payload["max_drawdown_legacy_note"] == tape.MAX_DRAWDOWN_LEGACY_NOTE


def test_selection_expected_symbol_mismatch_fails_closed(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    tape_dir = _write_valid_tape(tmp_path)

    assert selection.main(["--signal-dir", str(tape_dir), "--expect-symbol", "ETH/USDT", "--format", "json"]) == 2
    _assert_invalid(_payload_from_stdout(capsys), "symbol_mismatch")


def test_selection_expected_timeframe_mismatch_fails_closed(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    tape_dir = _write_valid_tape(tmp_path)

    assert selection.main(["--signal-dir", str(tape_dir), "--expect-entry-tf", "15m", "--format", "json"]) == 2
    _assert_invalid(_payload_from_stdout(capsys), "timeframe_mismatch")

    assert selection.main(["--signal-dir", str(tape_dir), "--expect-filter-tf", "4h", "--format", "json"]) == 2
    _assert_invalid(_payload_from_stdout(capsys), "timeframe_mismatch")


def test_selection_expected_signal_set_id_mismatch_fails_closed(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    tape_dir = _write_valid_tape(tmp_path)

    assert selection.main(["--signal-dir", str(tape_dir), "--expect-signal-set-id", "sig_other", "--format", "json"]) == 2
    _assert_invalid(_payload_from_stdout(capsys), "signal_set_id_mismatch")


def test_selection_missing_manifest_summary_and_trades_fail_closed(tmp_path: Path) -> None:
    missing_manifest_dir = tmp_path / "free" / "BTCUSDT" / "5m_1h" / "sig_missing_manifest"
    _write_json(missing_manifest_dir / "summary.json", tape.build_summary([], initial_equity=1000.0))
    _write_header_only_trades(missing_manifest_dir / "trades.csv")

    missing_summary_dir = tmp_path / "free" / "BTCUSDT" / "5m_1h" / "sig_missing_summary"
    _write_json(missing_summary_dir / "manifest.json", _manifest(signal_set_id="sig_missing_summary"))
    _write_header_only_trades(missing_summary_dir / "trades.csv")

    missing_trades_dir = tmp_path / "free" / "BTCUSDT" / "5m_1h" / "sig_missing_trades"
    _write_json(missing_trades_dir / "manifest.json", _manifest(signal_set_id="sig_missing_trades"))
    _write_json(missing_trades_dir / "summary.json", tape.build_summary([], initial_equity=1000.0))

    _assert_invalid(selection.build_signal_tape_selection_contract(missing_manifest_dir), "missing_manifest")
    _assert_invalid(selection.build_signal_tape_selection_contract(missing_summary_dir), "missing_summary")
    _assert_invalid(selection.build_signal_tape_selection_contract(missing_trades_dir), "missing_trades_csv")


def test_selection_unsafe_manifest_summary_positive_dd_and_forbidden_fields_fail_closed(tmp_path: Path) -> None:
    unsafe_manifest_dir = _write_valid_tape(tmp_path, signal_set_id="sig_unsafe_manifest")
    unsafe_manifest = json.loads((unsafe_manifest_dir / "manifest.json").read_text(encoding="utf-8"))
    unsafe_manifest["safety_scope"]["paper_live_order_execution"] = True
    _write_json(unsafe_manifest_dir / "manifest.json", unsafe_manifest)

    forbidden_manifest_dir = _write_valid_tape(tmp_path, signal_set_id="sig_forbidden_manifest")
    forbidden_manifest = json.loads((forbidden_manifest_dir / "manifest.json").read_text(encoding="utf-8"))
    forbidden_manifest["apiKey"] = "unit-redacted"
    _write_json(forbidden_manifest_dir / "manifest.json", forbidden_manifest)

    row_level_manifest_dir = _write_valid_tape(tmp_path, signal_set_id="sig_row_level_manifest")
    row_level_manifest = json.loads((row_level_manifest_dir / "manifest.json").read_text(encoding="utf-8"))
    row_level_manifest["entry_exec"] = 100.0
    _write_json(row_level_manifest_dir / "manifest.json", row_level_manifest)

    positive_manifest_dir = _write_valid_tape(tmp_path, signal_set_id="sig_positive_manifest_dd")
    positive_manifest = json.loads((positive_manifest_dir / "manifest.json").read_text(encoding="utf-8"))
    positive_manifest["max_drawdown"] = 1.0
    _write_json(positive_manifest_dir / "manifest.json", positive_manifest)

    unsafe_summary_dir = _write_valid_tape(tmp_path, signal_set_id="sig_unsafe_summary")
    unsafe_summary = json.loads((unsafe_summary_dir / "summary.json").read_text(encoding="utf-8"))
    unsafe_summary["research_only"] = False
    _write_json(unsafe_summary_dir / "summary.json", unsafe_summary)

    forbidden_summary_dir = _write_valid_tape(tmp_path, signal_set_id="sig_forbidden_summary")
    forbidden_summary = json.loads((forbidden_summary_dir / "summary.json").read_text(encoding="utf-8"))
    forbidden_summary["raw_order"] = "unit-redacted"
    _write_json(forbidden_summary_dir / "summary.json", forbidden_summary)

    positive_dd_dir = _write_valid_tape(tmp_path, signal_set_id="sig_positive_dd")
    _write_json(
        positive_dd_dir / "summary.json",
        {"research_only": True, "paper_live_order_execution": False, "max_drawdown": 1.0},
    )

    _assert_invalid(selection.build_signal_tape_selection_contract(unsafe_manifest_dir), "unsafe_manifest")
    _assert_invalid(selection.build_signal_tape_selection_contract(forbidden_manifest_dir), "forbidden_field")
    _assert_invalid(selection.build_signal_tape_selection_contract(row_level_manifest_dir), "forbidden_field")
    _assert_invalid(selection.build_signal_tape_selection_contract(positive_manifest_dir), "positive_legacy_drawdown")
    _assert_invalid(selection.build_signal_tape_selection_contract(unsafe_summary_dir), "unsafe_summary")
    _assert_invalid(selection.build_signal_tape_selection_contract(forbidden_summary_dir), "forbidden_field")
    _assert_invalid(selection.build_signal_tape_selection_contract(positive_dd_dir), "positive_legacy_drawdown")


def test_selection_output_excludes_raw_trade_rows_and_private_runtime_material(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    tape_dir = _write_valid_tape(tmp_path)

    assert selection.main(["--signal-dir", str(tape_dir), "--format", "json"]) == 0
    text = json.dumps(_payload_from_stdout(capsys), ensure_ascii=True, sort_keys=True).lower()

    for forbidden in (
        "trade_id",
        "entry_exec",
        "exit_exec",
        '"qty"',
        "api_key",
        "apikey",
        "secret",
        "token",
        "authorization",
        "raw_order",
        "balance",
        "raw_billing",
        "raw market data",
        "ohlcv",
    ):
        assert forbidden not in text


def test_selection_json_out_writes_safe_contract(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    tape_dir = _write_valid_tape(tmp_path)
    out_path = tmp_path / "out" / "selection.json"

    assert selection.main(["--signal-dir", str(tape_dir), "--format", "json", "--out", str(out_path)]) == 0
    assert "status=valid" in capsys.readouterr().out
    payload = json.loads(out_path.read_text(encoding="utf-8"))

    assert payload["signal_dir"] == str(tape_dir)
    assert payload["selectable_for_backtest_fast_path"] is True
    assert "entry_exec" not in out_path.read_text(encoding="utf-8")
    assert "raw_order" not in out_path.read_text(encoding="utf-8")


def test_selection_text_format_outputs_safe_operator_summary(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    tape_dir = _write_valid_tape(tmp_path)

    assert selection.main(["--signal-dir", str(tape_dir), "--format", "text"]) == 0
    text = capsys.readouterr().out

    assert "precomputed signal selection product=free status=valid" in text
    assert str(tape_dir) in text
    assert "backtest_fast_path=true" in text
    assert "runner_replay_fast_path=true" in text
    assert "live_selectable=false" in text
    assert "paper_selectable=false" in text
    assert "entry_exec" not in text
    assert "raw_order" not in text


def test_selection_strict_invalid_returns_nonzero(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    invalid_dir = tmp_path / "free" / "BTCUSDT" / "5m_1h" / "sig_invalid"
    invalid_dir.mkdir(parents=True)

    assert selection.main(["--signal-dir", str(invalid_dir), "--strict", "--format", "json"]) == 2
    _assert_invalid(_payload_from_stdout(capsys), "missing_manifest")


def test_selection_module_import_boundary_and_app_version_unchanged() -> None:
    source = (REPO_ROOT / "precomputed_signals_selection.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module.split(".", 1)[0])

    assert {"strategy", "indicators", "exchange", "ccxt", "runner", "backtest", "risk"}.isdisjoint(imports)
    lowered = source.lower()
    for term in ("mexc", "fetch_balance", "fetch_order", "create_order", "submit_order", "subprocess"):
        assert term not in lowered
    assert 'APP_VERSION = "1.1.3"' in (REPO_ROOT / "config.py").read_text(encoding="utf-8")
