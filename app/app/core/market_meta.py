# BUILD_ID: 2026-03-05_market_meta_auto_inject_v1
from __future__ import annotations

import json
import math
import os
import time
from dataclasses import asdict, dataclass
from typing import Any


BUILD_ID = "2026-03-05_market_meta_auto_inject_v1"


@dataclass
class MarketMeta:
    exchange_id: str
    symbol: str
    quote_ccy: str
    maker_fee_rate: float
    taker_fee_rate: float
    spread_bps: float
    min_qty: float
    min_cost: float
    tick_size: float
    amount_precision: int
    updated_at: float


def resolve_quote_ccy(symbol: str) -> str:
    raw = str(symbol or "").strip()
    if "/" in raw:
        quote = raw.split("/", 1)[1].strip().upper()
        if quote:
            return quote
    return ""


def compute_spread_bps_from_order_book(ob: dict | None) -> float:
    if not isinstance(ob, dict):
        return 0.0
    bids = ob.get("bids") or []
    asks = ob.get("asks") or []
    if not bids or not asks:
        return 0.0
    try:
        bid = float(bids[0][0])
        ask = float(asks[0][0])
    except Exception:
        return 0.0
    mid = (bid + ask) / 2.0
    if mid <= 0.0:
        return 0.0
    return max(0.0, (ask - bid) / mid * 10000.0)


def estimate_min_operational_balance(
    meta: MarketMeta,
    best_ask: float,
    fee_buffer: float = 0.002,
    safety_buffer_jpy: float = 2000.0,
) -> float:
    ask = float(best_ask or 0.0)
    if ask <= 0.0:
        return 0.0
    base_required = max(float(meta.min_cost or 0.0), float(meta.min_qty or 0.0) * ask)
    safety_buffer = float(safety_buffer_jpy) if str(meta.quote_ccy or "").upper() == "JPY" else 0.0
    return float(base_required) * (1.0 + float(fee_buffer or 0.0)) + safety_buffer


def market_meta_cache_path(state_dir: str, exchange_id: str, symbol: str) -> str:
    ex = str(exchange_id or "").strip().lower() or "unknown"
    sym = "".join(ch for ch in str(symbol or "").upper() if ch.isalnum()) or "UNKNOWN"
    return os.path.join(str(state_dir), f"market_meta_{ex}_{sym}.json")


def load_cached_meta(path: str) -> MarketMeta | None:
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    try:
        return MarketMeta(
            exchange_id=str(payload.get("exchange_id") or ""),
            symbol=str(payload.get("symbol") or ""),
            quote_ccy=str(payload.get("quote_ccy") or ""),
            maker_fee_rate=float(payload.get("maker_fee_rate") or 0.0),
            taker_fee_rate=float(payload.get("taker_fee_rate") or 0.0),
            spread_bps=float(payload.get("spread_bps") or 0.0),
            min_qty=float(payload.get("min_qty") or 0.0),
            min_cost=float(payload.get("min_cost") or 0.0),
            tick_size=float(payload.get("tick_size") or 0.0),
            amount_precision=int(payload.get("amount_precision") or 0),
            updated_at=float(payload.get("updated_at") or 0.0),
        )
    except Exception:
        return None


def save_cached_meta(path: str, meta: MarketMeta) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(asdict(meta), f, ensure_ascii=False, indent=2)


def _meta_is_fresh(meta: MarketMeta | None, ttl_sec: float) -> bool:
    if meta is None:
        return False
    try:
        ttl = max(0.0, float(ttl_sec))
    except Exception:
        ttl = 0.0
    if ttl <= 0.0:
        return False
    return (time.time() - float(meta.updated_at or 0.0)) <= ttl


def maybe_refresh_market_meta(
    ex: Any,
    *,
    exchange_id: str,
    symbol: str,
    state_dir: str,
    cache_ttl_sec: float = 3600.0,
    allow_refresh: bool = True,
) -> tuple[MarketMeta | None, str, str]:
    cache_path = market_meta_cache_path(state_dir, exchange_id, symbol)
    cached = load_cached_meta(cache_path)
    if _meta_is_fresh(cached, cache_ttl_sec):
        return cached, "cache", cache_path
    if (not allow_refresh) and cached is not None:
        return cached, "stale_cache", cache_path
    if not allow_refresh:
        return None, "cache_miss", cache_path
    try:
        rules = ex.get_market_rules(symbol)
        order_book = ex.fetch_order_book(symbol, limit=5)
        bid, ask = ex.fetch_best_bid_ask(symbol)
        maker, taker, fee_source = ex.fetch_market_fees(symbol)
        spread_bps = compute_spread_bps_from_order_book(order_book)
        if spread_bps <= 0.0 and bid and ask:
            mid = (float(bid) + float(ask)) / 2.0
            if mid > 0.0:
                spread_bps = max(0.0, (float(ask) - float(bid)) / mid * 10000.0)
        meta = MarketMeta(
            exchange_id=str(exchange_id or "").strip().lower(),
            symbol=str(symbol or ""),
            quote_ccy=resolve_quote_ccy(symbol),
            maker_fee_rate=float(maker),
            taker_fee_rate=float(taker),
            spread_bps=float(spread_bps),
            min_qty=float(rules.get("min_qty") or 0.0),
            min_cost=float(rules.get("min_cost") or 0.0),
            tick_size=float(rules.get("tick_size") or 0.0),
            amount_precision=int(rules.get("amount_precision") or 0),
            updated_at=float(time.time()),
        )
        save_cached_meta(cache_path, meta)
        source = f"refreshed:{fee_source}"
        return meta, source, cache_path
    except Exception:
        if cached is not None:
            return cached, "stale_cache_after_refresh_error", cache_path
        return None, "refresh_error", cache_path


def meta_to_log_dict(meta: MarketMeta | None) -> dict[str, Any]:
    if meta is None:
        return {}
    payload = asdict(meta)
    payload["updated_at_iso"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(meta.updated_at or 0.0)))
    return payload
