# BUILD_ID: 2026-05-08_free_precomputed_signal_inventory_v1
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

import precomputed_signals_inventory as inventory
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
    ]


def _manifest(
    *,
    symbol: str = "BTC/USDT",
    entry_tf: str = "5m",
    filter_tf: str = "1h",
    signal_set_id: str = "sig_inventory_fixture",
) -> dict[str, Any]:
    return tape.build_manifest_template(
        product=tape.DEFAULT_PRODUCT,
        producer_script="precompute_signals.py",
        producer_build_id=tape.BUILD_ID,
        symbol=symbol,
        entry_tf=entry_tf,
        filter_tf=filter_tf,
        since_ms=1000,
        until_ms=5000,
        dataset_id=f"{tape.normalize_symbol(symbol)}_{entry_tf}_{filter_tf}_unit",
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
    signal_set_id: str = "sig_inventory_fixture",
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


def _single_row(payload: dict[str, Any]) -> dict[str, Any]:
    assert payload["total"] == 1
    return payload["rows"][0]


def test_inventory_cli_empty_root_returns_safe_empty_result(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert inventory.main(["--root", str(tmp_path / "empty"), "--format", "json"]) == 0
    payload = _payload_from_stdout(capsys)
    assert payload["product"] == "free"
    assert payload["rows"] == []
    assert payload["valid_count"] == 0
    assert payload["invalid_count"] == 0


def test_inventory_discovers_valid_tape_and_defaults_to_free(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    tape_dir = _write_valid_tape(tmp_path)

    assert inventory.main(["--root", str(tmp_path), "--format", "json"]) == 0
    row = _single_row(_payload_from_stdout(capsys))

    assert row["product"] == "free"
    assert row["symbol"] == "BTC/USDT"
    assert row["symbol_normalized"] == "BTCUSDT"
    assert row["entry_tf"] == "5m"
    assert row["filter_tf"] == "1h"
    assert row["signal_set_id"] == "sig_inventory_fixture"
    assert row["signal_dir"] == str(tape_dir)
    assert row["status"] == "valid"
    assert row["trade_count"] == 2
    assert row["net_total"] == pytest.approx(14.873)
    assert row["final_equity"] == pytest.approx(1014.873)
    assert row["safety_research_only"] is True
    assert row["safety_paper_live_order_execution"] is False
    assert row["tape_files_present"] == {"manifest_json": True, "summary_json": True, "trades_csv": True}
    assert row["manifest_sha256"]
    assert row["summary_sha256"]
    assert row["trades_csv_sha256_from_manifest"]


def test_inventory_fails_closed_for_non_free_product(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        inventory.main(["--root", str(tmp_path), "--product", "standard"])


def test_inventory_filters_symbol_timeframes_and_signal_set_id(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    btc_dir = _write_valid_tape(tmp_path, symbol="BTC/USDT", entry_tf="5m", filter_tf="1h", signal_set_id="sig_btc")
    eth_dir = _write_valid_tape(tmp_path, symbol="ETH/USDT", entry_tf="15m", filter_tf="4h", signal_set_id="sig_eth")

    assert inventory.main(["--root", str(tmp_path), "--symbol", "btc_usdt", "--format", "json"]) == 0
    assert _single_row(_payload_from_stdout(capsys))["signal_dir"] == str(btc_dir)

    assert inventory.main(["--root", str(tmp_path), "--entry-tf", "15m", "--filter-tf", "4h", "--format", "json"]) == 0
    assert _single_row(_payload_from_stdout(capsys))["signal_dir"] == str(eth_dir)

    assert inventory.main(["--root", str(tmp_path), "--signal-set-id", "sig_eth", "--format", "json"]) == 0
    assert _single_row(_payload_from_stdout(capsys))["signal_dir"] == str(eth_dir)


def test_inventory_marks_missing_manifest_and_summary_invalid(tmp_path: Path) -> None:
    missing_manifest_dir = tmp_path / "free" / "BTCUSDT" / "5m_1h" / "sig_missing_manifest"
    _write_json(missing_manifest_dir / "summary.json", tape.build_summary([], initial_equity=1000.0))
    _write_header_only_trades(missing_manifest_dir / "trades.csv")

    missing_summary_dir = tmp_path / "free" / "BTCUSDT" / "5m_1h" / "sig_missing_summary"
    _write_json(missing_summary_dir / "manifest.json", _manifest(signal_set_id="sig_missing_summary"))
    _write_header_only_trades(missing_summary_dir / "trades.csv")

    result = inventory.build_signal_tape_inventory(tmp_path, include_invalid=True, sort="symbol")
    rows = {Path(row["signal_dir"]).name: row for row in result["rows"]}

    assert rows["sig_missing_manifest"]["status"] == "invalid"
    assert rows["sig_missing_manifest"]["safe_error_code"] == "MISSING_MANIFEST"
    assert rows["sig_missing_summary"]["status"] == "invalid"
    assert rows["sig_missing_summary"]["safe_error_code"] == "MISSING_SUMMARY"


def test_inventory_marks_unsafe_manifest_summary_positive_dd_and_forbidden_fields_invalid(tmp_path: Path) -> None:
    unsafe_manifest_dir = _write_valid_tape(tmp_path, signal_set_id="sig_unsafe_manifest")
    unsafe_manifest = json.loads((unsafe_manifest_dir / "manifest.json").read_text(encoding="utf-8"))
    unsafe_manifest["safety_scope"]["paper_live_order_execution"] = True
    _write_json(unsafe_manifest_dir / "manifest.json", unsafe_manifest)

    forbidden_manifest_dir = _write_valid_tape(tmp_path, signal_set_id="sig_forbidden_manifest")
    forbidden_manifest = json.loads((forbidden_manifest_dir / "manifest.json").read_text(encoding="utf-8"))
    forbidden_manifest["apiKey"] = "unit-redacted"
    _write_json(forbidden_manifest_dir / "manifest.json", forbidden_manifest)

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

    result = inventory.build_signal_tape_inventory(tmp_path, include_invalid=True, sort="symbol")
    rows = {Path(row["signal_dir"]).name: row for row in result["rows"]}

    assert rows["sig_unsafe_manifest"]["safe_error_code"] == "UNSAFE_SAFETY_SCOPE"
    assert rows["sig_forbidden_manifest"]["safe_error_code"] == "FORBIDDEN_FIELD"
    assert rows["sig_unsafe_summary"]["safe_error_code"] == "UNSAFE_SAFETY_SCOPE"
    assert rows["sig_forbidden_summary"]["safe_error_code"] == "FORBIDDEN_FIELD"
    assert rows["sig_positive_dd"]["safe_error_code"] == "POSITIVE_LEGACY_DD"
    for row in rows.values():
        assert row["status"] == "invalid"
        assert set(row) == {"signal_dir", "status", "status_reason", "safe_error_code"}


def test_inventory_strict_returns_nonzero_when_invalid_exists(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    invalid_dir = tmp_path / "free" / "BTCUSDT" / "5m_1h" / "sig_invalid"
    _write_json(invalid_dir / "summary.json", tape.build_summary([], initial_equity=1000.0))
    _write_header_only_trades(invalid_dir / "trades.csv")

    assert inventory.main(["--root", str(tmp_path), "--strict", "--format", "json"]) == 2
    payload = _payload_from_stdout(capsys)
    assert payload["discovered_invalid_count"] == 1
    assert payload["rows"][0]["safe_error_code"] == "MISSING_MANIFEST"


def test_inventory_valid_row_preserves_dd_schema_v2_display_fields(tmp_path: Path) -> None:
    _write_valid_tape(tmp_path)
    row = _single_row(inventory.build_signal_tape_inventory(tmp_path))

    assert row["max_drawdown"] == pytest.approx(row["max_dd_signed"])
    assert row["max_drawdown"] <= 0.0
    assert row["max_dd_abs"] >= 0.0
    assert row["max_dd_pct"] >= 0.0
    assert row["max_dd_display_abs"] == pytest.approx(row["max_dd_abs"])
    assert row["max_dd_display_pct"] == pytest.approx(row["max_dd_pct"])
    assert row["max_dd_display_label"] == tape.DD_DISPLAY_LABEL
    assert row["max_drawdown_legacy_note"] == tape.MAX_DRAWDOWN_LEGACY_NOTE


def test_inventory_output_excludes_raw_trade_rows_and_private_runtime_material(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _write_valid_tape(tmp_path)
    assert inventory.main(["--root", str(tmp_path), "--format", "json"]) == 0
    text = json.dumps(_payload_from_stdout(capsys), ensure_ascii=True, sort_keys=True).lower()

    for forbidden in (
        "trade_id",
        "entry_exec",
        "exit_exec",
        '"qty"',
        "mexc_api_key",
        "mexc_api_secret",
        "api_key",
        "apikey",
        "secret",
        "token",
        "authorization",
        "raw_order",
        "balance",
        "raw_billing",
    ):
        assert forbidden not in text


def test_inventory_json_and_csv_out_write_safe_files(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    tape_dir = _write_valid_tape(tmp_path)
    json_out = tmp_path / "out" / "inventory.json"
    csv_out = tmp_path / "out" / "inventory.csv"

    assert inventory.main(["--root", str(tmp_path), "--format", "json", "--out", str(json_out)]) == 0
    assert "valid=1" in capsys.readouterr().out
    payload = json.loads(json_out.read_text(encoding="utf-8"))
    assert _single_row(payload)["signal_dir"] == str(tape_dir)

    assert inventory.main(["--root", str(tmp_path), "--format", "csv", "--out", str(csv_out)]) == 0
    assert "valid=1" in capsys.readouterr().out
    csv_text = csv_out.read_text(encoding="utf-8")
    assert str(tape_dir) in csv_text
    assert "entry_exec" not in csv_text
    assert "raw_order" not in csv_text


def test_inventory_text_format_outputs_safe_operator_summary(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    tape_dir = _write_valid_tape(tmp_path)
    assert inventory.main(["--root", str(tmp_path), "--format", "text"]) == 0
    text = capsys.readouterr().out
    assert "precomputed signal inventory product=free" in text
    assert "valid=1" in text
    assert str(tape_dir) in text
    assert "entry_exec" not in text
    assert "raw_order" not in text


def test_inventory_root_env_override(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    env_root = tmp_path / "env_root"
    tape_dir = _write_valid_tape(env_root)
    monkeypatch.setenv("LWF_PRECOMPUTED_SIGNALS_ROOT", str(env_root))

    assert inventory.main(["--format", "json"]) == 0
    row = _single_row(_payload_from_stdout(capsys))
    assert row["signal_dir"] == str(tape_dir)


def test_inventory_module_import_boundary_and_app_version_unchanged() -> None:
    source = (REPO_ROOT / "precomputed_signals_inventory.py").read_text(encoding="utf-8")
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
