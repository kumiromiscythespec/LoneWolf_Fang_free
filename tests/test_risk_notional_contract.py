from __future__ import annotations

import copy
import json
import math
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from helper import risk_notional_contract as contract


DOC = REPO_ROOT / "docs" / "fixed_notional_ceiling_contract.md"
SCHEMA = REPO_ROOT / "schema" / "fixed_notional_ceiling_contract.schema.json"
FIXTURE = (
    REPO_ROOT
    / "tests"
    / "fixtures"
    / "fixed-notional-ceiling-contract"
    / "valid_contract_payload.json"
)


def _effective(
    *,
    equity_quote: object = 10_000.0,
    cap_pct: object = 0.2,
    fixed_ceiling_enabled: bool = True,
    global_fixed_notional_ceiling: object = None,
    per_symbol_fixed_notional_ceiling: dict[str, object] | None = None,
    symbol: str = "ETH/USDT",
) -> float:
    return contract.effective_max_notional(
        equity_quote=equity_quote,
        cap_pct=cap_pct,
        fixed_ceiling_enabled=fixed_ceiling_enabled,
        global_fixed_notional_ceiling=global_fixed_notional_ceiling,
        per_symbol_fixed_notional_ceiling=per_symbol_fixed_notional_ceiling,
        symbol=symbol,
    )


def test_disabled_ceiling_equals_equity_times_cap_pct() -> None:
    assert _effective(
        fixed_ceiling_enabled=False,
        global_fixed_notional_ceiling=500.0,
        per_symbol_fixed_notional_ceiling={"ETH/USDT": 300.0},
    ) == 2_000.0


def test_enabled_global_ceiling_lower_than_percentage_cap() -> None:
    assert _effective(global_fixed_notional_ceiling=1_000.0) == 1_000.0


def test_enabled_global_ceiling_higher_than_percentage_cap() -> None:
    assert _effective(global_fixed_notional_ceiling=5_000.0) == 2_000.0


def test_per_symbol_ceiling_wins_when_lower() -> None:
    assert _effective(
        global_fixed_notional_ceiling=1_500.0,
        per_symbol_fixed_notional_ceiling={"ETH/USDT": 700.0},
    ) == 700.0


def test_per_symbol_invalid_value_ignored() -> None:
    assert _effective(
        global_fixed_notional_ceiling=1_200.0,
        per_symbol_fixed_notional_ceiling={"ETH/USDT": "bad"},
    ) == 1_200.0


@pytest.mark.parametrize("invalid_global", ["bad", 0.0, -100.0, math.inf, math.nan])
def test_global_invalid_value_ignored(invalid_global: object) -> None:
    assert _effective(global_fixed_notional_ceiling=invalid_global) == 2_000.0


def test_both_invalid_fixed_values_ignored() -> None:
    assert _effective(
        global_fixed_notional_ceiling=math.nan,
        per_symbol_fixed_notional_ceiling={"ETH/USDT": -1.0},
    ) == 2_000.0


@pytest.mark.parametrize(
    "equity_quote, cap_pct",
    [
        (0.0, 0.2),
        (-10_000.0, 0.2),
        (10_000.0, 0.0),
        (10_000.0, -0.2),
        (math.inf, 0.2),
        (10_000.0, math.nan),
    ],
)
def test_non_positive_or_non_finite_base_risk_cap_returns_zero(
    equity_quote: object,
    cap_pct: object,
) -> None:
    assert _effective(
        equity_quote=equity_quote,
        cap_pct=cap_pct,
        global_fixed_notional_ceiling=500.0,
        per_symbol_fixed_notional_ceiling={"ETH/USDT": 300.0},
    ) == 0.0


@pytest.mark.parametrize(
    "symbol, ceiling_map",
    [
        ("ETH/USDT", {"ETHUSDT": 450.0}),
        ("ETHUSDT", {"ETH/USDT": 450.0}),
    ],
)
def test_symbol_alias_behavior(symbol: str, ceiling_map: dict[str, object]) -> None:
    assert _effective(
        symbol=symbol,
        global_fixed_notional_ceiling=1_500.0,
        per_symbol_fixed_notional_ceiling=ceiling_map,
    ) == 450.0


def test_deterministic_no_side_effect_behavior() -> None:
    ceilings = {"ETHUSDT": "600", "BTCUSDT": 800.0}
    original = copy.deepcopy(ceilings)

    first = _effective(per_symbol_fixed_notional_ceiling=ceilings)
    second = _effective(per_symbol_fixed_notional_ceiling=ceilings)

    assert first == second == 600.0
    assert ceilings == original


def test_schema_fixture_and_docs_keep_safety_boundary_explicit() -> None:
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    doc = DOC.read_text(encoding="utf-8")

    assert schema["properties"]["safety_flags"]["properties"]["runtime_wiring_allowed"]["const"] is False
    assert schema["properties"]["safety_flags"]["properties"]["backtest_wiring_allowed"]["const"] is False
    assert fixture["safety_flags"]["runtime_wiring_allowed"] is False
    assert fixture["safety_flags"]["backtest_wiring_allowed"] is False
    assert fixture["semantics"]["not_wallet_balance"] is True
    assert fixture["semantics"]["applies_after_risk_sizing"] is True
    assert "- wallet balance" in doc
    assert "Runtime and backtest wiring remains blocked" in doc
