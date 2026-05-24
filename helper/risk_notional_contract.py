"""Pure fixed-notional-ceiling contract helpers.

This module is intentionally standalone: it imports no runtime, exchange,
state, config, filesystem, network, order, or private API code.
"""

from __future__ import annotations

import math
from typing import Mapping


QUOTE_SUFFIXES: tuple[str, ...] = ("USDT", "USDC", "JPY", "USD")


def positive_finite(value: object) -> float | None:
    """Return a positive finite float, or None when the value is not usable."""

    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isfinite(number) and number > 0.0:
        return number
    return None


def symbol_notional_keys(symbol: str) -> tuple[str, ...]:
    """Return deterministic aliases used for per-symbol ceiling lookup."""

    raw = str(symbol or "").strip().upper()
    compact = "".join(ch for ch in raw if ch.isalnum())
    keys = [raw, compact]
    if "/" in raw:
        base, quote = raw.split("/", 1)
        keys.append(f"{base}{quote}")
    elif compact:
        for quote in QUOTE_SUFFIXES:
            if compact.endswith(quote) and len(compact) > len(quote):
                keys.append(f"{compact[:-len(quote)]}/{quote}")
                break

    out: list[str] = []
    for key in keys:
        normalized = str(key or "").strip().upper()
        if normalized and normalized not in out:
            out.append(normalized)
    return tuple(out)


def percentage_notional_cap(equity_quote: object, cap_pct: object) -> float:
    """Return the base risk percentage cap, or 0.0 for invalid inputs."""

    equity = positive_finite(equity_quote)
    pct = positive_finite(cap_pct)
    if equity is None or pct is None:
        return 0.0

    cap = equity * pct
    return cap if math.isfinite(cap) and cap > 0.0 else 0.0


def fixed_notional_ceiling_for_symbol(
    *,
    per_symbol_fixed_notional_ceiling: Mapping[str, object] | None,
    symbol: str,
) -> float | None:
    """Return the lowest valid matching per-symbol ceiling, if any."""

    if not isinstance(per_symbol_fixed_notional_ceiling, Mapping):
        return None

    wanted_keys = set(symbol_notional_keys(symbol))
    if not wanted_keys:
        return None

    matches: list[float] = []
    for raw_key, raw_value in per_symbol_fixed_notional_ceiling.items():
        if wanted_keys.intersection(symbol_notional_keys(str(raw_key))):
            ceiling = positive_finite(raw_value)
            if ceiling is not None:
                matches.append(ceiling)

    return min(matches) if matches else None


def effective_max_notional(
    *,
    equity_quote: object,
    cap_pct: object,
    fixed_ceiling_enabled: bool,
    global_fixed_notional_ceiling: object = None,
    per_symbol_fixed_notional_ceiling: Mapping[str, object] | None = None,
    symbol: str = "",
) -> float:
    """Return the effective maximum notional after applying ceiling caps.

    The percentage cap is the base risk sizing limit. Fixed notional ceilings
    can only reduce that base cap; they never create a positive notional when
    equity or cap_pct make the base cap zero.
    """

    pct_cap = percentage_notional_cap(equity_quote, cap_pct)
    if pct_cap <= 0.0:
        return 0.0
    if not fixed_ceiling_enabled:
        return float(pct_cap)

    candidates = [float(pct_cap)]
    symbol_ceiling = fixed_notional_ceiling_for_symbol(
        per_symbol_fixed_notional_ceiling=per_symbol_fixed_notional_ceiling,
        symbol=symbol,
    )
    if symbol_ceiling is not None:
        candidates.append(symbol_ceiling)

    global_ceiling = positive_finite(global_fixed_notional_ceiling)
    if global_ceiling is not None:
        candidates.append(global_ceiling)

    return float(min(candidates))


__all__ = [
    "effective_max_notional",
    "fixed_notional_ceiling_for_symbol",
    "percentage_notional_cap",
    "positive_finite",
    "symbol_notional_keys",
]
