# BUILD_ID: 2026-05-25_free_backtest_offline_market_rules_v1
from __future__ import annotations

import ast
import importlib
import json
import sys
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from helper.backtest_offline_market_rules import (  # noqa: E402
    OfflineMarketRulesError,
    load_offline_market_rules,
)


def _valid_payload() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "markets": [
            {
                "symbol": "BTC/USDT",
                "aliases": ["BTCUSDT"],
                "amount_precision": 6,
                "amount_step": 0.000001,
                "price_precision": 2,
                "price_step": 0.01,
                "min_amount": 0.00001,
                "min_cost": 5.0,
            }
        ],
    }


def _write_rules(tmp_path: Path, payload: dict[str, Any] | None = None) -> Path:
    path = tmp_path / "offline_market_rules.json"
    path.write_text(json.dumps(payload if payload is not None else _valid_payload(), ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    return path


def _minimal_result(kwargs: dict[str, Any]) -> dict[str, Any]:
    initial = float(kwargs.get("initial_equity", 1000.0) or 1000.0)
    return {
        "symbols": list(kwargs.get("symbols") or ["BTC/USDT"]),
        "since_ms": kwargs.get("since_ms"),
        "since_ms_effective": kwargs.get("since_ms"),
        "since_iso_effective": "",
        "entry_tf": str(kwargs.get("entry_tf") or "5m"),
        "filter_tf": str(kwargs.get("filter_tf") or "1h"),
        "warmup_bars": int(kwargs.get("warmup_bars") or 0),
        "spread_bps_est": 0.0,
        "recent_bars_entry": kwargs.get("recent_bars_entry"),
        "recent_bars_filter": kwargs.get("recent_bars_filter"),
        "trades": 0,
        "buy_signals": 0,
        "entries_opened": 0,
        "forced_closes": 0,
        "open_positions_end": 0,
        "win": 0,
        "lose": 0,
        "final_equity": initial,
        "max_dd": 0.0,
        "max_dd_mtm": 0.0,
        "net_total": 0.0,
        "net_avg": 0.0,
        "export_dir": "",
    }


def _import_backtest_without_exchange() -> Any:
    for module_name in ("backtest", "runner", "exchange", "ccxt"):
        sys.modules.pop(module_name, None)
    module = importlib.import_module("backtest")
    assert "exchange" not in sys.modules
    assert "ccxt" not in sys.modules
    return module


def _import_roots(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module.split(".", 1)[0])
    return imports


def test_loader_accepts_valid_local_fixture(tmp_path: Path) -> None:
    provider = load_offline_market_rules(_write_rules(tmp_path), required_symbols=["BTC/USDT"])

    rules = provider.get_market_rules("BTC/USDT")
    assert rules["symbol"] == "BTC/USDT"
    assert rules["min_qty"] == pytest.approx(0.00001)
    assert rules["min_cost"] == pytest.approx(5.0)
    assert provider.amount_to_precision("BTC/USDT", 0.123456789) == pytest.approx(0.123456)


def test_missing_fixture_fails_closed(tmp_path: Path) -> None:
    with pytest.raises(OfflineMarketRulesError, match="file not found"):
        load_offline_market_rules(tmp_path / "missing.json", required_symbols=["BTC/USDT"])


def test_missing_symbol_fails_closed(tmp_path: Path) -> None:
    with pytest.raises(OfflineMarketRulesError, match="missing symbol"):
        load_offline_market_rules(_write_rules(tmp_path), required_symbols=["ETH/USDT"])


@pytest.mark.parametrize(
    "field",
    ["amount_precision", "price_precision", "min_amount", "min_cost"],
)
def test_incomplete_rules_fail_closed(tmp_path: Path, field: str) -> None:
    payload = _valid_payload()
    market = dict(payload["markets"][0])
    market.pop(field)
    if field == "amount_precision":
        market.pop("amount_step")
    if field == "price_precision":
        market.pop("price_step")
    payload["markets"] = [market]

    with pytest.raises(OfflineMarketRulesError):
        load_offline_market_rules(_write_rules(tmp_path, payload), required_symbols=["BTC/USDT"])


def test_forbidden_private_fields_fail_closed(tmp_path: Path) -> None:
    payload = _valid_payload()
    payload["markets"][0]["api_key"] = "not allowed"

    with pytest.raises(OfflineMarketRulesError, match="forbidden"):
        load_offline_market_rules(_write_rules(tmp_path, payload), required_symbols=["BTC/USDT"])


def test_btc_slash_and_compact_aliases_work(tmp_path: Path) -> None:
    provider = load_offline_market_rules(_write_rules(tmp_path), required_symbols=["BTCUSDT"])

    assert provider.market_amount_rules("BTC/USDT") == pytest.approx((0.00001, 5.0))
    assert provider.market_amount_rules("BTCUSDT") == pytest.approx((0.00001, 5.0))


def test_offline_helper_imports_no_exchange_or_ccxt() -> None:
    helper_path = REPO_ROOT / "helper" / "backtest_offline_market_rules.py"
    assert {"exchange", "ccxt", "runner", "state_store", "safety"}.isdisjoint(_import_roots(helper_path))
    source = helper_path.read_text(encoding="utf-8")
    assert "import exchange" not in source
    assert "import ccxt" not in source


def test_backtest_cli_offline_rules_passes_adapter_without_exchange_import(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    backtest = _import_backtest_without_exchange()
    called: dict[str, Any] = {}

    def fake_run_backtest(**kwargs: Any) -> dict[str, Any]:
        called.update(kwargs)
        return _minimal_result(kwargs)

    rules_path = _write_rules(tmp_path)
    monkeypatch.setattr(backtest, "run_backtest", fake_run_backtest)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backtest.py",
            "--symbols",
            "BTC/USDT",
            "--entry-tf",
            "5m",
            "--filter-tf",
            "1h",
            "--initial",
            "1000",
            "--no-csv",
            "--offline-market-rules",
            str(rules_path),
        ],
    )

    assert backtest.main() == 0
    provider = called["offline_market_rules"]
    assert provider.market_amount_rules("BTCUSDT") == pytest.approx((0.00001, 5.0))
    assert provider.amount_to_precision("BTC/USDT", 0.123456789) == pytest.approx(0.123456)
    assert "exchange" not in sys.modules
    assert "ccxt" not in sys.modules


def test_backtest_cli_offline_missing_file_fails_before_run(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    backtest = _import_backtest_without_exchange()
    monkeypatch.setattr(backtest, "run_backtest", lambda **_kwargs: pytest.fail("backtest must not run"))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backtest.py",
            "--symbols",
            "BTC/USDT",
            "--offline-market-rules",
            str(tmp_path / "missing.json"),
        ],
    )

    assert backtest.main() == 2
    assert "exchange" not in sys.modules
    assert "ccxt" not in sys.modules


def test_backtest_cli_without_offline_flag_does_not_pass_adapter(monkeypatch: pytest.MonkeyPatch) -> None:
    backtest = _import_backtest_without_exchange()
    called: dict[str, Any] = {}

    def fake_run_backtest(**kwargs: Any) -> dict[str, Any]:
        called.update(kwargs)
        return _minimal_result(kwargs)

    monkeypatch.setattr(backtest, "run_backtest", fake_run_backtest)
    monkeypatch.setattr(
        sys,
        "argv",
        ["backtest.py", "--symbols", "BTC/USDT", "--entry-tf", "5m", "--filter-tf", "1h", "--initial", "1000", "--no-csv"],
    )

    assert backtest.main() == 0
    assert "offline_market_rules" not in called


def test_schema_and_docs_state_phase3b_requires_separate_approval() -> None:
    schema = json.loads((REPO_ROOT / "schema" / "backtest_offline_market_rules.schema.json").read_text(encoding="utf-8"))
    doc = (REPO_ROOT / "docs" / "offline_backtest_market_rules.md").read_text(encoding="utf-8")

    assert schema["properties"]["schema_version"]["const"] == 1
    assert "--offline-market-rules" in doc
    assert "Phase 3B execution still requires separate owner approval" in doc
    assert "It is not an execution-ready BTC/USDT fixture" in doc
