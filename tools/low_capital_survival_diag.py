# BUILD_ID: 2026-04-27_low_capital_survival_diag_v1
from __future__ import annotations

import argparse
import json
import math
import os
import sys
from pathlib import Path
from typing import Any

BUILD_ID = "2026-04-27_low_capital_survival_diag_v1"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _bootstrap_imports() -> None:
    root = _repo_root()
    for candidate in (root, root / "app"):
        if candidate.exists():
            text = str(candidate)
            if text not in sys.path:
                sys.path.insert(0, text)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(out)


def _quote_from_symbol(symbol: str) -> str:
    raw = str(symbol or "").strip().upper()
    if "/" in raw:
        return raw.split("/", 1)[1].split(":", 1)[0].strip()
    for quote in ("USDT", "USDC", "JPY", "USD"):
        if raw.endswith(quote):
            return quote
    return "USDT"


def _resolve_quote_balance(args: argparse.Namespace, quote: str) -> float:
    if _safe_float(args.quote_balance, 0.0) > 0.0:
        return _safe_float(args.quote_balance, 0.0)
    jpy = _safe_float(args.jpy_balance, 5000.0)
    if quote == "JPY":
        return jpy
    return jpy / max(_safe_float(args.usdt_jpy, 155.0), 1e-12)


def _fetch_price(ex: Any, symbol: str, assumed_price: float) -> tuple[float, str]:
    if assumed_price > 0.0:
        return float(assumed_price), "cli_assumed_price"
    try:
        ticker = ex.fetch_ticker(symbol)
        price = _safe_float(
            (ticker or {}).get("ask")
            or (ticker or {}).get("last")
            or (ticker or {}).get("close")
            or (ticker or {}).get("bid"),
            0.0,
        )
        if price > 0.0:
            return price, "exchange_ticker"
    except Exception as exc:
        return 0.0, f"ticker_unavailable:{exc}"
    return 0.0, "ticker_unavailable"


def _market_rules(ex: Any, symbol: str) -> tuple[dict[str, Any], str]:
    try:
        if hasattr(ex, "load_markets"):
            ex.load_markets()
    except Exception:
        pass
    try:
        if hasattr(ex, "get_market_rules"):
            return dict(ex.get_market_rules(symbol)), "exchange_client_rules"
    except Exception as exc:
        return {"error": str(exc)}, "exchange_client_rules_error"
    try:
        min_qty, min_cost = ex.market_amount_rules(symbol)
        return {"min_qty": float(min_qty), "min_cost": float(min_cost)}, "market_amount_rules"
    except Exception as exc:
        return {"error": str(exc)}, "market_amount_rules_error"


def main() -> int:
    parser = argparse.ArgumentParser(description="Non-order low-capital survival diagnostics.")
    parser.add_argument("--exchange", default=os.getenv("LWF_EXCHANGE_ID", "mexc"))
    parser.add_argument("--symbol", default=os.getenv("BOT_SYMBOL", "BTC/USDT"))
    parser.add_argument("--jpy-balance", type=float, default=5000.0)
    parser.add_argument("--quote-balance", type=float, default=0.0)
    parser.add_argument("--usdt-jpy", type=float, default=155.0)
    parser.add_argument("--assumed-price", type=float, default=0.0)
    parser.add_argument("--spread-bps", type=float, default=2.0)
    parser.add_argument("--reserve-bps", type=float, default=10.0)
    args = parser.parse_args()

    os.environ["LWF_EXCHANGE_ID"] = str(args.exchange)
    os.environ["BOT_SYMBOLS"] = str(args.symbol)
    os.environ["BOT_SYMBOL"] = str(args.symbol)
    os.environ["BACKTEST_CSV_SYMBOL"] = str(args.symbol)

    _bootstrap_imports()
    import config as C  # noqa: PLC0415
    from exchange import ExchangeClient  # noqa: PLC0415
    from risk import calc_qty_from_risk  # noqa: PLC0415

    symbol = str(args.symbol).strip()
    quote = _quote_from_symbol(symbol)
    quote_balance = _resolve_quote_balance(args, quote)
    ex = ExchangeClient(str(args.exchange).strip().lower())
    price, price_source = _fetch_price(ex, symbol, _safe_float(args.assumed_price, 0.0))
    rules, rules_source = _market_rules(ex, symbol)

    min_qty = _safe_float(rules.get("min_qty"), 0.0)
    min_cost = _safe_float(rules.get("min_cost"), 0.0)
    amount_step = _safe_float(rules.get("amount_step"), 0.0)
    amount_precision = int(_safe_float(rules.get("amount_precision"), 0.0))
    min_notional = max(min_cost, min_qty * price if price > 0.0 else 0.0)

    taker_fee = _safe_float(getattr(C, "PAPER_FEE_RATE_TAKER", getattr(C, "PAPER_FEE_RATE", 0.0)), 0.0)
    slip_bps = _safe_float(getattr(C, "SLIPPAGE_BPS", 0.0), 0.0)
    spread_bps = _safe_float(args.spread_bps, 0.0)
    reserve_bps = _safe_float(args.reserve_bps, 0.0)
    exec_buffer_frac = max(0.0, taker_fee) + max(0.0, slip_bps + spread_bps + reserve_bps) / 10000.0

    cap_pct = _safe_float(getattr(C, "MAX_POSITION_NOTIONAL_PCT", 0.0), 0.0)
    configured_cap = quote_balance * cap_pct if cap_pct > 0.0 else 0.0
    spot_balance_cap = quote_balance / (1.0 + exec_buffer_frac) if quote_balance > 0.0 else 0.0

    min_stop_bps = max(_safe_float(getattr(C, "MIN_STOP_BPS", 4.0), 4.0), 0.01)
    stop_price = price * (1.0 - min_stop_bps / 10000.0) if price > 0.0 else 0.0
    raw_qty = calc_qty_from_risk(quote_balance, price, stop_price) if price > 0.0 else 0.0
    raw_notional = raw_qty * price

    pre_guard_cap = configured_cap if configured_cap > 0.0 else raw_notional
    pre_guard_notional = min(x for x in (raw_notional, pre_guard_cap) if x > 0.0) if raw_notional > 0.0 else 0.0
    guarded_notional = min(pre_guard_notional, spot_balance_cap) if pre_guard_notional > 0.0 and spot_balance_cap > 0.0 else pre_guard_notional
    requested_qty = guarded_notional / price if price > 0.0 else 0.0
    try:
        normalized_qty = float(ex.amount_to_precision(symbol, requested_qty))
    except Exception:
        normalized_qty = requested_qty
    normalized_notional = normalized_qty * price

    buffered_min_notional = min_notional * (1.0 + exec_buffer_frac)
    min_quote_by_balance = buffered_min_notional
    min_quote_by_cap = buffered_min_notional / cap_pct if cap_pct > 0.0 else buffered_min_notional
    min_quote_required = max(min_quote_by_balance, min_quote_by_cap)

    precision_step_suspect = bool(amount_step >= 1.0 and 0.0 < min_qty < 1.0 and price > 1000.0)
    entry_viable = bool(
        price > 0.0
        and min_notional > 0.0
        and normalized_qty > 0.0
        and normalized_notional + 1e-12 >= min_notional
        and guarded_notional <= spot_balance_cap + 1e-9
    )

    payload = {
        "build_id": BUILD_ID,
        "repo": _repo_root().name,
        "exchange": str(args.exchange).strip().lower(),
        "symbol": symbol,
        "quote": quote,
        "jpy_balance": _safe_float(args.jpy_balance, 5000.0),
        "usdt_jpy": _safe_float(args.usdt_jpy, 155.0),
        "quote_balance": quote_balance,
        "price": price,
        "price_source": price_source,
        "fee_taker": taker_fee,
        "spread_bps": spread_bps,
        "slippage_bps": slip_bps,
        "reserve_bps": reserve_bps,
        "market_rules": rules,
        "rules_source": rules_source,
        "min_notional": min_notional,
        "max_position_notional_pct": cap_pct,
        "configured_cap_notional": configured_cap,
        "spot_balance_cap_notional": spot_balance_cap,
        "raw_risk_qty": raw_qty,
        "raw_risk_notional": raw_notional,
        "pre_guard_notional": pre_guard_notional,
        "guarded_notional": guarded_notional,
        "normalized_qty": normalized_qty,
        "normalized_notional": normalized_notional,
        "precision_step_suspect": precision_step_suspect,
        "entry_viable": entry_viable,
        "min_quote_required": min_quote_required,
        "min_jpy_required": min_quote_required if quote == "JPY" else min_quote_required * _safe_float(args.usdt_jpy, 155.0),
        "blocking": {
            "price_unavailable": price <= 0.0,
            "min_order_not_met": normalized_notional + 1e-12 < min_notional,
            "qty_rounds_to_zero": normalized_qty <= 0.0,
            "pre_guard_exceeds_quote_balance": pre_guard_notional > quote_balance + 1e-9,
            "precision_step_suspect": precision_step_suspect,
        },
    }
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0 if entry_viable and not precision_step_suspect else 2


if __name__ == "__main__":
    raise SystemExit(main())
