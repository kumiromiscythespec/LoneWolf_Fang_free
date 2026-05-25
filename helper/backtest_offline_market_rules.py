# BUILD_ID: 2026-05-25_free_backtest_offline_market_rules_v1
"""Backtest-only offline market rules.

This module is intentionally local and deterministic. It imports no exchange,
ccxt, config, runtime, network, order, balance, or private API code.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from decimal import Decimal, ROUND_FLOOR
from pathlib import Path
from typing import Any, Iterable, Mapping


BUILD_ID = "2026-05-25_free_backtest_offline_market_rules_v1"
SCHEMA_VERSION = 1
QUOTE_SUFFIXES: tuple[str, ...] = ("USDT", "USDC", "JPY", "USD")
FORBIDDEN_KEY_FRAGMENTS: tuple[str, ...] = (
    "apikey",
    "api_key",
    "api-secret",
    "api_secret",
    "authorization",
    "bearer",
    "private_key",
    "raw_order",
    "order_id",
    "balance",
    "secret",
)


class OfflineMarketRulesError(ValueError):
    """Raised when offline market rules are missing, unsafe, or incomplete."""


def symbol_aliases(symbol: str) -> tuple[str, ...]:
    raw = str(symbol or "").strip().upper()
    compact = "".join(ch for ch in raw if ch.isalnum())
    aliases = [raw, compact]
    if "/" in raw:
        base, quote = raw.split("/", 1)
        aliases.append(f"{base}{quote}")
    elif compact:
        for quote in QUOTE_SUFFIXES:
            if compact.endswith(quote) and len(compact) > len(quote):
                aliases.append(f"{compact[:-len(quote)]}/{quote}")
                break

    out: list[str] = []
    for item in aliases:
        value = str(item or "").strip().upper()
        if value and value not in out:
            out.append(value)
    return tuple(out)


def _safe_float(name: str, value: object, *, positive: bool) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise OfflineMarketRulesError(f"{name} must be numeric") from exc
    if not math.isfinite(number):
        raise OfflineMarketRulesError(f"{name} must be finite")
    if positive and number <= 0.0:
        raise OfflineMarketRulesError(f"{name} must be positive")
    if (not positive) and number < 0.0:
        raise OfflineMarketRulesError(f"{name} must be non-negative")
    return float(number)


def _precision_from_step(name: str, value: object) -> int:
    try:
        dec = Decimal(str(value))
    except Exception as exc:
        raise OfflineMarketRulesError(f"{name} must be decimal-like") from exc
    if not dec.is_finite() or dec <= 0:
        raise OfflineMarketRulesError(f"{name} must be positive")
    normalized = dec.normalize()
    exponent = int(normalized.as_tuple().exponent)
    return max(0, -exponent)


def _precision_value(name: str, value: object) -> int:
    try:
        precision = int(value)
    except (TypeError, ValueError) as exc:
        raise OfflineMarketRulesError(f"{name} must be an integer") from exc
    if precision < 0:
        raise OfflineMarketRulesError(f"{name} must be non-negative")
    return int(precision)


def _step_from_precision(precision: int) -> float:
    precision_i = max(0, int(precision))
    if precision_i <= 0:
        return 1.0
    return float(10.0 ** (-precision_i))


def _require_present(mapping: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        if name in mapping:
            return mapping[name]
    raise OfflineMarketRulesError(f"missing required field: {' or '.join(names)}")


def _check_forbidden_keys(value: Any, path: tuple[str, ...] = ()) -> None:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            key_text = str(key or "")
            key_l = key_text.lower().replace("-", "_")
            if any(fragment in key_l for fragment in FORBIDDEN_KEY_FRAGMENTS):
                dotted = ".".join(path + (key_text,))
                raise OfflineMarketRulesError(f"forbidden private/runtime field in offline rules: {dotted}")
            _check_forbidden_keys(nested, path + (key_text,))
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            _check_forbidden_keys(nested, path + (str(index),))


@dataclass(frozen=True)
class OfflineMarketRule:
    symbol: str
    aliases: tuple[str, ...]
    min_qty: float
    min_cost: float
    amount_precision: int
    price_precision: int
    amount_step: float
    tick_size: float

    def exchange_rules(self, exchange_id: str = "offline") -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "exchange_id": str(exchange_id or "offline"),
            "min_qty": float(self.min_qty),
            "min_cost": float(self.min_cost),
            "amount_precision": int(self.amount_precision),
            "price_precision": int(self.price_precision),
            "amount_step": float(self.amount_step),
            "tick_size": float(self.tick_size),
        }


class OfflineMarketRulesProvider:
    """Backtest sizing adapter backed only by local JSON rules."""

    offline_market_rules_provider = True

    def __init__(self, rules: Iterable[OfflineMarketRule], *, source_path: str = "") -> None:
        self.source_path = str(source_path or "")
        self._rules_by_alias: dict[str, OfflineMarketRule] = {}
        for rule in rules:
            aliases = symbol_aliases(rule.symbol)
            for alias in tuple(aliases) + tuple(rule.aliases):
                for key in symbol_aliases(alias):
                    existing = self._rules_by_alias.get(key)
                    if existing is not None and existing.symbol != rule.symbol:
                        raise OfflineMarketRulesError(f"duplicate market rule alias: {key}")
                    self._rules_by_alias[key] = rule
        if not self._rules_by_alias:
            raise OfflineMarketRulesError("offline market rules file contains no markets")

        self._replay_amount_decimals_map = {
            key: int(rule.amount_precision) for key, rule in self._rules_by_alias.items()
        }
        self._replay_amount_decimals_default = None

    def get_market_rules(self, symbol: str) -> dict[str, Any]:
        rule = self._lookup(symbol)
        return rule.exchange_rules()

    def market_amount_rules(self, symbol: str) -> tuple[float, float]:
        rule = self._lookup(symbol)
        return float(rule.min_qty), float(rule.min_cost)

    def amount_to_precision(self, symbol: str, amount: float) -> float:
        rule = self._lookup(symbol)
        return _floor_to_step(float(amount), float(rule.amount_step), int(rule.amount_precision))

    def price_to_precision(self, symbol: str, price: float) -> float:
        rule = self._lookup(symbol)
        return _floor_to_step(float(price), float(rule.tick_size), int(rule.price_precision))

    def _lookup(self, symbol: str) -> OfflineMarketRule:
        for key in symbol_aliases(symbol):
            rule = self._rules_by_alias.get(key)
            if rule is not None:
                return rule
        raise OfflineMarketRulesError(f"offline market rules missing symbol: {symbol}")


def _floor_to_step(value: float, step: float, precision: int) -> float:
    if not math.isfinite(value) or not math.isfinite(step) or step <= 0.0:
        return float(value)
    dec_value = Decimal(str(value))
    dec_step = Decimal(str(step))
    units = (dec_value / dec_step).to_integral_value(rounding=ROUND_FLOOR)
    floored = units * dec_step
    if int(precision) <= 0:
        return float(floored.quantize(Decimal("1"), rounding=ROUND_FLOOR))
    quantum = Decimal("1").scaleb(-int(precision))
    return float(floored.quantize(quantum, rounding=ROUND_FLOOR))


def _parse_rule(raw: Mapping[str, Any], index: int) -> OfflineMarketRule:
    if not isinstance(raw, Mapping):
        raise OfflineMarketRulesError(f"markets[{index}] must be an object")
    symbol = str(_require_present(raw, "symbol") or "").strip().upper()
    if not symbol:
        raise OfflineMarketRulesError(f"markets[{index}].symbol is required")

    aliases_raw = raw.get("aliases", [])
    if aliases_raw is None:
        aliases_raw = []
    if not isinstance(aliases_raw, list):
        raise OfflineMarketRulesError(f"markets[{index}].aliases must be a list")
    aliases = tuple(str(item or "").strip().upper() for item in aliases_raw if str(item or "").strip())

    amount_precision_raw = raw.get("amount_precision")
    amount_step_raw = raw.get("amount_step")
    if amount_precision_raw is None and amount_step_raw is None:
        raise OfflineMarketRulesError(f"markets[{index}] requires amount_precision or amount_step")
    if amount_precision_raw is None:
        amount_step = _safe_float(f"markets[{index}].amount_step", amount_step_raw, positive=True)
        amount_precision = _precision_from_step(f"markets[{index}].amount_step", amount_step_raw)
    else:
        amount_precision = _precision_value(f"markets[{index}].amount_precision", amount_precision_raw)
        amount_step = (
            _safe_float(f"markets[{index}].amount_step", amount_step_raw, positive=True)
            if amount_step_raw is not None
            else _step_from_precision(amount_precision)
        )

    price_precision_raw = raw.get("price_precision")
    tick_size_raw = raw.get("tick_size", raw.get("price_step"))
    if price_precision_raw is None and tick_size_raw is None:
        raise OfflineMarketRulesError(f"markets[{index}] requires price_precision or price_step")
    if price_precision_raw is None:
        tick_size = _safe_float(f"markets[{index}].price_step", tick_size_raw, positive=True)
        price_precision = _precision_from_step(f"markets[{index}].price_step", tick_size_raw)
    else:
        price_precision = _precision_value(f"markets[{index}].price_precision", price_precision_raw)
        tick_size = (
            _safe_float(f"markets[{index}].price_step", tick_size_raw, positive=True)
            if tick_size_raw is not None
            else _step_from_precision(price_precision)
        )

    min_qty = _safe_float(
        f"markets[{index}].min_amount",
        _require_present(raw, "min_amount", "min_qty"),
        positive=False,
    )
    min_cost = _safe_float(
        f"markets[{index}].min_cost",
        _require_present(raw, "min_cost"),
        positive=False,
    )

    return OfflineMarketRule(
        symbol=symbol,
        aliases=aliases,
        min_qty=float(min_qty),
        min_cost=float(min_cost),
        amount_precision=int(amount_precision),
        price_precision=int(price_precision),
        amount_step=float(amount_step),
        tick_size=float(tick_size),
    )


def load_offline_market_rules(
    path: str | Path,
    *,
    required_symbols: Iterable[str] = (),
) -> OfflineMarketRulesProvider:
    rules_path = Path(path).expanduser()
    if not rules_path.is_file():
        raise OfflineMarketRulesError(f"offline market rules file not found: {rules_path}")
    try:
        payload = json.loads(rules_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise OfflineMarketRulesError(f"offline market rules file is not valid JSON: {rules_path}") from exc
    if not isinstance(payload, Mapping):
        raise OfflineMarketRulesError("offline market rules root must be an object")
    _check_forbidden_keys(payload)
    if int(payload.get("schema_version", 0) or 0) != SCHEMA_VERSION:
        raise OfflineMarketRulesError(f"offline market rules schema_version must be {SCHEMA_VERSION}")
    markets = payload.get("markets")
    if not isinstance(markets, list) or not markets:
        raise OfflineMarketRulesError("offline market rules require a non-empty markets list")

    provider = OfflineMarketRulesProvider(
        [_parse_rule(raw, index) for index, raw in enumerate(markets)],
        source_path=str(rules_path),
    )
    for symbol in required_symbols:
        provider.get_market_rules(str(symbol))
    return provider


__all__ = [
    "BUILD_ID",
    "OfflineMarketRule",
    "OfflineMarketRulesError",
    "OfflineMarketRulesProvider",
    "SCHEMA_VERSION",
    "load_offline_market_rules",
    "symbol_aliases",
]
