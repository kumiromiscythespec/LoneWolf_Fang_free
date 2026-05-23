# BUILD_ID: 2026-05-08_free_precomputed_signals_foundation_v1
from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import Any, Mapping


def _import_roots(path: str) -> set[str]:
    source = Path(path).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module.split(".", 1)[0])
    return imports


def test_fast_path_imports_only_signal_tape_and_stdlib_boundaries() -> None:
    blocked = {"ccxt", "exchange", "runner", "strategy", "indicators"}
    assert blocked.isdisjoint(_import_roots("fast_backtest_signals.py"))


def test_signal_tape_imports_no_network_or_order_runtime_modules() -> None:
    blocked = {"ccxt", "exchange", "runner", "strategy", "indicators", "risk", "safety"}
    assert blocked.isdisjoint(_import_roots("signal_tape.py"))


def test_source_has_no_runtime_import_statements() -> None:
    fast_source = Path("fast_backtest_signals.py").read_text(encoding="utf-8")
    tape_source = Path("signal_tape.py").read_text(encoding="utf-8")
    for term in ("import ccxt", "import exchange", "import runner", "import strategy", "import indicators"):
        assert term not in fast_source
        assert term not in tape_source


def _flatten_payload(payload: Any) -> list[tuple[tuple[str, ...], str]]:
    values: list[tuple[tuple[str, ...], str]] = []

    def visit(value: Any, path: tuple[str, ...]) -> None:
        if isinstance(value, Mapping):
            for key, nested in value.items():
                visit(nested, path + (str(key),))
            return
        if isinstance(value, (list, tuple)):
            for index, nested in enumerate(value):
                visit(nested, path + (str(index),))
            return
        values.append((path, "" if value is None else str(value)))

    visit(payload, ())
    return values


def test_synthetic_fixture_payload_has_no_private_runtime_material() -> None:
    fixture = {
        "manifest": {
            "product": "free",
            "symbol": "BTC/USDT",
            "safety_scope": {
                "research_only": True,
                "paper_live_order_execution": False,
                "contains_api_key": False,
                "contains_secret": False,
                "contains_order_id": False,
            },
        },
        "trades": [
            {"trade_id": "1", "symbol": "BTC/USDT", "entry_exec": 100.0, "exit_exec": 110.0, "qty": 2.0},
            {"trade_id": "2", "symbol": "BTC/USDT", "entry_exec": 110.0, "exit_exec": 105.0, "qty": 1.0},
        ],
    }
    text = json.dumps(fixture, ensure_ascii=True, sort_keys=True).lower()
    for forbidden in ("mexc_api_key", "mexc_api_secret", "apikey", "apisecret", "authorization", "bearer", "raw_order", "raw_billing", "private_key"):
        assert forbidden not in text
    for path, value in _flatten_payload(fixture):
        if path[-1:] in (("contains_secret",), ("contains_order_id",)):
            assert value == "False"
        else:
            assert "unit-private-value" not in value
