# BUILD_ID: 2026-04-08_free_bitbank_okx_spot_only_v1
# BUILD_ID: 2026-03-20_exchange_mexc_precision_normalize_v1
# BUILD_ID: 2026-03-09_coincheck_rest_poll_fill_v1
# exchange.py
from __future__ import annotations

from typing import Any, Optional, Sequence
import logging
import math
import os
import threading
import time
import ccxt
import config as C
from app.core.fees import resolve_paper_fees
from app.core.instrument_registry import default_quote_for_exchange
from app.core.instrument_registry import quote_for_symbol

BUILD_ID = "2026-04-08_free_bitbank_okx_spot_only_v1"
logger = logging.getLogger("exchange")



class ExchangeClient:
    """
    CCXT wrapper for spot exchanges, with:
      - fetch_ohlcv / fetch_ticker / fetch_tickers
      - scalping symbol auto-selection
      - maker-like limit placement with TTL (pseudo post-only)
      - market order for emergency exits
    """

    def __init__(self, exchange_id: str | None = None):
        exchange_id = str(
            exchange_id
            or os.getenv("LWF_EXCHANGE_ID")
            or getattr(C, "EXCHANGE_ID", "mexc")
            or "mexc"
        ).strip().lower()
        self.exchange_id = exchange_id
        self._markets_loaded = False
        self._coincheck_last_create_ts = 0.0
        self._coincheck_last_order_detail_ts = 0.0
        self._coincheck_public_trade_cache: dict[str, dict[str, Any]] = {}
        self._coincheck_ohlcv_warned: set[tuple[str, str, str]] = set()
        self._market_rules_logged: set[str] = set()
        self._coincheck_ws_threads: dict[str, Any] = {}
        self._coincheck_ws_lock = threading.Lock()
        self._coincheck_ws_last_recv_ts: dict[str, float] = {}
        self._coincheck_ws_start_ts: dict[str, float] = {}
        self._coincheck_ws_warned: set[tuple[str, str]] = set()
        self._coincheck_ws_stop = threading.Event()
        self._coincheck_rest_poll_last_ts: dict[str, float] = {}
        self._coincheck_trade_ring_by_symbol: dict[str, list[dict[str, float | int]]] = {}
        self._coincheck_ohlcv_5m_by_symbol: dict[str, dict[int, list[float]]] = {}
        self._coincheck_5m_cache_floor_by_symbol: dict[str, int] = {}
        self._coincheck_seed_done_symbols: set[str] = set()
        self._coincheck_bitbank_seed_done_symbols: set[tuple[str, str]] = set()
        self._bitbank_public_ex: Any | None = None

        if exchange_id == "binance":
            api_key = (os.getenv("BINANCE_API_KEY") or "").strip()
            api_secret = (os.getenv("BINANCE_API_SECRET") or "").strip()
            self.ex = ccxt.binance(
                {
                    "apiKey": api_key,
                    "secret": api_secret,
                    "enableRateLimit": True,
                    "options": {
                        "defaultType": "spot",
                    },
                }
            )
        elif exchange_id == "coincheck":
            api_key = (os.getenv("COINCHECK_API_KEY") or os.getenv("EXCHANGE_KEY") or "").strip()
            api_secret = (os.getenv("COINCHECK_API_SECRET") or os.getenv("EXCHANGE_SECRET") or "").strip()
            self.ex = ccxt.coincheck(
                {
                    "apiKey": api_key,
                    "secret": api_secret,
                    "enableRateLimit": True,
                }
            )
        elif exchange_id == "bitbank":
            api_key = (os.getenv("BITBANK_API_KEY") or "").strip()
            api_secret = (os.getenv("BITBANK_API_SECRET") or "").strip()
            self.ex = ccxt.bitbank(
                {
                    "apiKey": api_key,
                    "secret": api_secret,
                    "enableRateLimit": True,
                }
            )
        elif exchange_id == "okx":
            api_key = (os.getenv("OKX_API_KEY") or "").strip()
            api_secret = (os.getenv("OKX_API_SECRET") or "").strip()
            api_passphrase = (os.getenv("OKX_API_PASSPHRASE") or os.getenv("OKX_API_PASSWORD") or "").strip()
            if (api_key or api_secret) and not api_passphrase:
                raise RuntimeError("OKX requires API passphrase.")
            self.ex = ccxt.okx(
                {
                    "apiKey": api_key,
                    "secret": api_secret,
                    "password": api_passphrase,
                    "enableRateLimit": True,
                    "options": {
                        "defaultType": "spot",
                    },
                }
            )
        else:
            api_key = (os.getenv("MEXC_API_KEY") or "").strip()
            api_secret = (os.getenv("MEXC_API_SECRET") or "").strip()
            self.ex = ccxt.mexc({
                "apiKey": api_key,
                "secret": api_secret,
                "enableRateLimit": True,
                "options": {
                    "defaultType": "spot",  # 現物専用
                },
            })

        # Do not print raw credential values during debugging.
        #print("ccxt apiKey set:", bool(getattr(self.ex, "apiKey", None)))
        #print("ccxt secret set:", bool(getattr(self.ex, "secret", None)))

    def load_markets(self):
        self._ensure_markets_loaded()
        return self.ex.markets

    def _ensure_markets_loaded(self) -> None:
        if self._markets_loaded:
            return
        self._retry(self.ex.load_markets)
        self._markets_loaded = True

    def _normalize_symbol(self, symbol: str | None) -> str | None:
        if symbol is None:
            return None
        raw = str(symbol or "").strip()
        if self.exchange_id == "coincheck":
            low = raw.lower()
            if low in ("btc_jpy", "btcjpy"):
                return "BTC/JPY"
        return raw

    def _default_quote_currency(self) -> str:
        symbols = list(getattr(C, "SYMBOLS", []) or [])
        for symbol in symbols:
            quote = quote_for_symbol(str(symbol or ""), fallback="")
            if quote:
                return str(quote)
        return default_quote_for_exchange(self.exchange_id, fallback="USDT")

    def _quote_from_symbol(self, symbol: str) -> str:
        return quote_for_symbol(str(symbol or ""), fallback=self._default_quote_currency())

    def _adapt_order_params(self, params: dict | None = None) -> dict:
        out = dict(params or {})
        if self.exchange_id == "okx":
            out.setdefault("tdMode", "cash")
        if self.exchange_id != "coincheck":
            return out
        tif = str(out.get("time_in_force") or out.get("timeInForce") or "").strip().lower()
        post_only = bool(out.get("postOnly")) or tif == "post_only"
        out.pop("postOnly", None)
        out.pop("timeInForce", None)
        if post_only and not out.get("time_in_force"):
            out["time_in_force"] = "post_only"
        return out

    def _step_from_precision(self, precision: int) -> float:
        precision_i = max(0, int(precision))
        return 1.0 / (10 ** precision_i) if precision_i > 0 else 1.0

    def _round_up_to_step(self, value: float, step: float, precision: int) -> float:
        if step <= 0:
            return round(float(value), max(0, int(precision)))
        units = math.ceil((float(value) / float(step)) - 1e-12)
        return round(units * float(step), max(0, int(precision)))

    def _round_down_to_step(self, value: float, step: float, precision: int) -> float:
        if step <= 0:
            return round(float(value), max(0, int(precision)))
        units = math.floor((float(value) / float(step)) + 1e-12)
        return round(units * float(step), max(0, int(precision)))

    def _price_tick(self, price_precision: int) -> float:
        if int(price_precision) <= 0:
            return 1.0
        return self._step_from_precision(int(price_precision))

    def get_market_rules(self, symbol: str) -> dict:
        symbol = str(self._normalize_symbol(symbol) or symbol)
        self._ensure_markets_loaded()
        market = self.ex.market(symbol)
        limits = market.get("limits", {}) if market else {}
        amount_limits = limits.get("amount", {}) if limits else {}
        cost_limits = limits.get("cost", {}) if limits else {}
        min_qty = self._safe_float(amount_limits.get("min"), 0.0)
        min_cost = self._safe_float(cost_limits.get("min"), 0.0)

        price_precision, amount_precision = self._decimals_from_market(symbol)
        if self.exchange_id == "coincheck" and symbol == "BTC/JPY":
            amount_precision = max(int(amount_precision), 8)
            price_precision = max(int(price_precision), 0)
            min_qty = max(float(min_qty), 0.001)
            min_cost = max(float(min_cost), 500.0)
        elif self.exchange_id == "bitbank" and self._quote_from_symbol(symbol) == "JPY":
            min_cost = max(float(min_cost), 500.0)
        elif self.exchange_id == "mexc":
            quote_ccy = self._quote_from_symbol(symbol)
            min_cost_fallback_map = dict(getattr(C, "MEXC_MIN_COST_BY_QUOTE", {"USDT": 1.0, "USDC": 5.0}) or {})
            try:
                min_cost_fallback = float(min_cost_fallback_map.get(str(quote_ccy), 0.0) or 0.0)
            except Exception:
                min_cost_fallback = 0.0
            if min_cost_fallback > 0.0:
                min_cost = max(float(min_cost), float(min_cost_fallback))

        tick_size = self._price_tick(price_precision)
        amount_step = self._step_from_precision(amount_precision)
        rules = {
            "symbol": symbol,
            "exchange_id": self.exchange_id,
            "min_qty": float(min_qty),
            "min_cost": float(min_cost),
            "amount_precision": int(amount_precision),
            "price_precision": int(price_precision),
            "amount_step": float(amount_step),
            "tick_size": float(tick_size),
        }
        if symbol not in self._market_rules_logged:
            logger.info(
                "[MARKET_RULES] exchange_id=%s symbol=%s min_qty=%.8f min_cost=%.2f amount_precision=%s price_precision=%s tick_size=%s",
                self.exchange_id,
                symbol,
                float(rules["min_qty"]),
                float(rules["min_cost"]),
                int(rules["amount_precision"]),
                int(rules["price_precision"]),
                str(rules["tick_size"]),
            )
            self._market_rules_logged.add(symbol)
        return rules

    def _normalize_limit_order(self, symbol: str, side: str, amount: float, price: float) -> dict:
        symbol = str(self._normalize_symbol(symbol) or symbol)
        side_l = str(side or "").strip().lower()
        if side_l not in ("buy", "sell"):
            raise ValueError("side must be 'buy' or 'sell'")
        rules = self.get_market_rules(symbol)
        amount_precision = int(rules["amount_precision"])
        price_precision = int(rules["price_precision"])
        amount_step = float(rules["amount_step"])
        tick_size = float(rules["tick_size"])
        requested_amount = float(amount)
        requested_price = float(price)
        if requested_price <= 0.0:
            raise ValueError(f"limit price must be positive: {requested_price}")

        normalized_price = (
            self._round_down_to_step(requested_price, tick_size, price_precision)
            if side_l == "buy"
            else self._round_up_to_step(requested_price, tick_size, price_precision)
        )
        normalized_price = max(float(tick_size), float(normalized_price))

        min_qty = float(rules["min_qty"])
        min_cost = float(rules["min_cost"])
        min_qty_from_cost = 0.0
        if normalized_price > 0.0 and min_cost > 0.0:
            min_qty_from_cost = self._round_up_to_step(min_cost / normalized_price, amount_step, amount_precision)

        normalized_amount = self._round_up_to_step(max(requested_amount, min_qty, min_qty_from_cost), amount_step, amount_precision)
        normalized_cost = float(normalized_amount) * float(normalized_price)
        return {
            "symbol": symbol,
            "side": side_l,
            "amount": float(normalized_amount),
            "price": float(normalized_price),
            "cost": float(normalized_cost),
            "requested_amount": float(requested_amount),
            "requested_price": float(requested_price),
            "adjusted_amount": abs(float(normalized_amount) - float(requested_amount)) > 1e-12,
            "adjusted_price": abs(float(normalized_price) - float(requested_price)) > 1e-12,
            "rules": rules,
        }

    def normalize_limit_order(self, symbol: str, side: str, amount: float, price: float) -> dict:
        return self._normalize_limit_order(symbol, side, amount, price)

    def _is_coincheck_post_only_rejection(self, exc: Exception) -> bool:
        msg = str(exc or "").lower()
        needles = (
            "post_only",
            "post only",
            "time_in_force",
            "would take",
            "immediate",
            "maker",
            "cross",
        )
        return any(token in msg for token in needles)

    def _coincheck_rate_limit(self, kind: str) -> None:
        if self.exchange_id != "coincheck":
            return
        if kind == "create":
            attr = "_coincheck_last_create_ts"
            min_interval = 0.26
        else:
            attr = "_coincheck_last_order_detail_ts"
            min_interval = 1.05
        now = time.time()
        last = float(getattr(self, attr, 0.0) or 0.0)
        wait_sec = min_interval - (now - last)
        if wait_sec > 0:
            time.sleep(wait_sec)
        setattr(self, attr, time.time())

    def _coincheck_fetch_order_trades(self, order_id: str, symbol: str) -> list[dict]:
        symbol = str(self._normalize_symbol(symbol) or symbol)
        self._coincheck_rate_limit("order_detail")
        trades = self._retry(self.ex.fetch_my_trades, symbol, None, 100, {})
        out: list[dict] = []
        target = str(order_id)
        for trade in trades or []:
            info = trade.get("info") or {}
            trade_order_id = (
                info.get("order_id")
                or trade.get("order")
                or trade.get("orderId")
                or trade.get("id")
            )
            if str(trade_order_id) == target:
                out.append(trade)
        return out

    def _coincheck_fetch_order_state(
        self,
        order_id: str,
        symbol: str,
        amount: float | None = None,
        price: float | None = None,
        side: str | None = None,
    ) -> dict:
        symbol = str(self._normalize_symbol(symbol) or symbol)
        requested_amount = self._safe_float(amount, 0.0)
        requested_price = self._safe_float(price, 0.0)
        target_id = str(order_id)

        self._coincheck_rate_limit("order_detail")
        open_orders = self.fetch_open_orders(symbol)
        open_order = None
        for candidate in open_orders or []:
            if str(candidate.get("id") or "") == target_id:
                open_order = candidate
                break

        if open_order is not None:
            info = open_order.get("info") or {}
            pending_amount = self._safe_float(
                open_order.get("remaining"),
                self._safe_float(info.get("pending_amount"), 0.0),
            )
            open_amount = self._safe_float(open_order.get("amount"), pending_amount)
            known_amount = requested_amount if requested_amount > 0.0 else open_amount
            filled_qty = max(0.0, known_amount - pending_amount) if known_amount > 0.0 else 0.0
            order_price = self._safe_float(open_order.get("price"), requested_price)
            return {
                "id": target_id,
                "symbol": symbol,
                "side": side or open_order.get("side"),
                "amount": known_amount,
                "price": order_price,
                "average": order_price if filled_qty > 0.0 else None,
                "filled": filled_qty,
                "remaining": pending_amount,
                "status": "open",
                "info": info or open_order,
            }

        trades = self._coincheck_fetch_order_trades(target_id, symbol)
        filled_qty = 0.0
        total_cost = 0.0
        trade_infos = []
        for trade in trades:
            trade_amount = self._safe_float(trade.get("amount"), 0.0)
            trade_price = self._safe_float(trade.get("price"), requested_price)
            if trade_amount <= 0.0:
                continue
            filled_qty += trade_amount
            total_cost += trade_amount * trade_price
            trade_infos.append(trade.get("info") or trade)
        avg_price = (total_cost / filled_qty) if filled_qty > 0.0 else (requested_price or None)
        known_amount = requested_amount if requested_amount > 0.0 else filled_qty
        remaining = max(0.0, known_amount - filled_qty) if known_amount > 0.0 else 0.0
        status = "closed" if filled_qty > 0.0 and remaining <= 0.0 else "canceled"
        return {
            "id": target_id,
            "symbol": symbol,
            "side": side,
            "amount": known_amount,
            "price": requested_price or avg_price,
            "average": avg_price,
            "filled": filled_qty,
            "remaining": remaining,
            "status": status,
            "info": {"trades": trade_infos},
        }

    # -------------------------
    # internal retry wrapper
    # -------------------------
    def _retry(self, fn, *args, **kwargs):
        retryable = (
            ccxt.InvalidNonce,
            ccxt.RequestTimeout,
            ccxt.DDoSProtection,
            ccxt.NetworkError,
            ccxt.RateLimitExceeded,
            ccxt.ExchangeNotAvailable,
        )
        last_err: Exception | None = None
        for i in range(3):
            try:
                return fn(*args, **kwargs)
            except retryable as e:
                last_err = e
                try:
                    self.ex.load_time_difference()
                except Exception:
                    pass
                time.sleep(0.4 * (2 ** i))
            except ccxt.ExchangeError as e:
                msg = str(e).lower()
                if ("429" not in msg) and ("rate limit" not in msg) and ("too many" not in msg):
                    raise
                last_err = e
                time.sleep(0.4 * (2 ** i))

        if last_err is not None:
            raise last_err
        return fn(*args, **kwargs)

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        try:
            if value is None or value == "":
                return float(default)
            return float(value)
        except Exception:
            return float(default)

    def _safe_int(self, value: Any, default: int = 0) -> int:
        try:
            if value is None or value == "":
                return int(default)
            return int(value)
        except Exception:
            return int(default)

    def _timeframe_to_ms(self, timeframe: str) -> int:
        tf = str(timeframe or "").strip().lower()
        if tf.endswith("m"):
            return int(tf[:-1]) * 60_000
        if tf.endswith("h"):
            return int(tf[:-1]) * 3_600_000
        if tf.endswith("d"):
            return int(tf[:-1]) * 86_400_000
        raise ValueError(f"unsupported timeframe: {timeframe}")

    def _coincheck_fetch_ohlcv_unsupported(self, exc: Exception) -> bool:
        msg = str(exc or "").lower()
        return isinstance(exc, ccxt.NotSupported) or ("fetchohlcv" in msg and "not supported" in msg)

    def _coincheck_public_pair_id(self, symbol: str) -> str:
        return str(symbol).replace("/", "_").lower()

    def _coincheck_ws_pair_id(self, symbol: str) -> str:
        symbol = str(self._normalize_symbol(symbol) or symbol)
        return self._coincheck_public_pair_id(symbol)

    def _coincheck_5m_cache_max(self, symbol: str, required_bars: int | None = None) -> int:
        symbol = str(self._normalize_symbol(symbol) or symbol)
        base_limit = max(100, self._safe_int(getattr(C, "COINCHECK_WS_5M_BAR_CACHE_MAX", 1200), 1200))
        current_floor = self._safe_int(self._coincheck_5m_cache_floor_by_symbol.get(symbol), 0)
        if required_bars is not None:
            current_floor = max(int(current_floor), max(0, int(required_bars or 0)))
            if current_floor > 0:
                self._coincheck_5m_cache_floor_by_symbol[symbol] = int(current_floor)
        return max(int(base_limit), int(current_floor))

    def _coincheck_ingest_trade(self, symbol: str, trade_msg: Any) -> bool:
        symbol = str(self._normalize_symbol(symbol) or symbol)
        norm: dict[str, float | int] | None = None
        if isinstance(trade_msg, dict):
            norm = self._coincheck_normalize_public_trade(trade_msg)
        elif isinstance(trade_msg, (list, tuple)):
            if len(trade_msg) < 5:
                return False
            ts_raw = self._safe_int(trade_msg[0], 0)
            if 0 < ts_raw < 10_000_000_000:
                ts_raw *= 1000
            trade_id = self._safe_int(trade_msg[1], 0)
            pair = str(trade_msg[2] or "").strip().lower()
            expected_pair = self._coincheck_ws_pair_id(symbol)
            if pair and pair != expected_pair:
                return False
            price = self._safe_float(trade_msg[3], 0.0)
            amount = self._safe_float(trade_msg[4], 0.0)
            if ts_raw <= 0 or price <= 0.0 or amount <= 0.0:
                return False
            norm = {
                "id": int(trade_id),
                "timestamp": int(ts_raw),
                "price": float(price),
                "amount": float(amount),
            }
        if norm is None:
            return False

        trades = self._coincheck_trade_ring_by_symbol.setdefault(symbol, [])
        last = trades[-1] if trades else None
        norm_id = int(norm.get("id") or 0)
        if isinstance(last, dict):
            last_id = self._safe_int(last.get("id"), 0)
            if norm_id > 0 and last_id == norm_id:
                return False
            if norm_id <= 0:
                last_ts = self._safe_int(last.get("timestamp"), 0)
                last_px = self._safe_float(last.get("price"), 0.0)
                last_amt = self._safe_float(last.get("amount"), 0.0)
                if (
                    last_ts == int(norm["timestamp"])
                    and abs(last_px - float(norm["price"])) <= 1e-12
                    and abs(last_amt - float(norm["amount"])) <= 1e-12
                ):
                    return False
        trades.append(
            {
                "id": int(norm.get("id") or 0),
                "timestamp": int(norm["timestamp"]),
                "price": float(norm["price"]),
                "amount": float(norm["amount"]),
            }
        )
        max_trades = max(500, self._safe_int(getattr(C, "COINCHECK_WS_TRADE_RING_MAX", 5000), 5000))
        if len(trades) > max_trades:
            del trades[:-max_trades]

        bucket_ms = self._timeframe_to_ms("5m")
        bucket_ts = (int(norm["timestamp"]) // int(bucket_ms)) * int(bucket_ms)
        bars = self._coincheck_ohlcv_5m_by_symbol.setdefault(symbol, {})
        row = bars.get(int(bucket_ts))
        if row is None:
            bars[int(bucket_ts)] = [
                int(bucket_ts),
                float(norm["price"]),
                float(norm["price"]),
                float(norm["price"]),
                float(norm["price"]),
                float(norm["amount"]),
            ]
        else:
            row[2] = max(float(row[2]), float(norm["price"]))
            row[3] = min(float(row[3]), float(norm["price"]))
            row[4] = float(norm["price"])
            row[5] = float(row[5]) + float(norm["amount"])

        max_bars = self._coincheck_5m_cache_max(symbol)
        if len(bars) > max_bars:
            drop_keys = sorted(bars.keys())[:-max_bars]
            for old_key in drop_keys:
                bars.pop(old_key, None)
        return True

    def _coincheck_seed_recent_trades_once(self, symbol: str) -> int:
        if self.exchange_id != "coincheck":
            return 0
        symbol = str(self._normalize_symbol(symbol) or symbol)
        if symbol in self._coincheck_seed_done_symbols:
            return 0

        pair_id = self._coincheck_public_pair_id(symbol)
        limit_i = max(1, min(100, self._safe_int(getattr(C, "COINCHECK_WS_SEED_TRADE_LIMIT", 100), 100)))
        params: dict[str, Any] = {"pair": pair_id, "limit": int(limit_i), "order": "desc"}
        source = "none"
        raw_trades: list[Any] = []
        try:
            fetch_public = getattr(self.ex, "publicGetTrades", None)
            if callable(fetch_public):
                raw = self._retry(fetch_public, params)
                if isinstance(raw, dict):
                    raw_trades = list(raw.get("data", []) or [])
                    source = "publicGetTrades:dict"
                elif isinstance(raw, list):
                    raw_trades = list(raw)
                    source = "publicGetTrades:list"
            if not raw_trades:
                raw_trades = self._retry(self.ex.fetch_trades, symbol, None, int(limit_i), params)
                source = "fetch_trades"
        except Exception as exc:
            logger.warning("[COINCHECK][WS] seed_failed symbol=%s source=%s reason=%r", symbol, source, exc)
            return 0

        normalized: list[dict[str, float | int]] = []
        for trade in raw_trades or []:
            norm = self._coincheck_normalize_public_trade(trade)
            if norm is not None:
                normalized.append(norm)
        normalized.sort(key=lambda x: (int(x["timestamp"]), int(x.get("id") or 0), float(x["price"])))

        ingested = 0
        for norm in normalized:
            if self._coincheck_ingest_trade(symbol, norm):
                ingested += 1
        if ingested > 0:
            self._coincheck_seed_done_symbols.add(symbol)
        logger.info(
            "[COINCHECK][WS] seed symbol=%s source=%s raw=%s ingested=%s",
            symbol,
            source,
            int(len(raw_trades or [])),
            int(ingested),
        )
        return int(ingested)

    def _coincheck_poll_public_trades_if_needed(self, symbol: str, *, reason: str) -> int:
        if self.exchange_id != "coincheck":
            return 0
        symbol = str(self._normalize_symbol(symbol) or symbol)
        cooldown_sec = max(30.0, self._safe_float(getattr(C, "COINCHECK_REST_POLL_COOLDOWN_SEC", 45.0), 45.0))
        now_sec = time.time()
        last_poll_ts = self._safe_float(self._coincheck_rest_poll_last_ts.get(symbol), 0.0)
        if last_poll_ts > 0.0 and (now_sec - last_poll_ts) < cooldown_sec:
            return 0
        self._coincheck_rest_poll_last_ts[symbol] = float(now_sec)

        ring = self._coincheck_trade_ring_by_symbol.get(symbol) or []
        last_trade = ring[-1] if ring and isinstance(ring[-1], dict) else {}
        last_id = self._safe_int(last_trade.get("id"), 0) if isinstance(last_trade, dict) else 0
        last_ts = self._safe_int(last_trade.get("timestamp"), 0) if isinstance(last_trade, dict) else 0
        last_px = self._safe_float(last_trade.get("price"), 0.0) if isinstance(last_trade, dict) else 0.0
        last_amt = self._safe_float(last_trade.get("amount"), 0.0) if isinstance(last_trade, dict) else 0.0
        min_limit = max(20, min(100, self._safe_int(getattr(C, "COINCHECK_REST_POLL_TRADE_LIMIT", 100), 100)))
        fetched = 0
        ingested = 0
        skipped = 0
        try:
            polled = self._coincheck_fetch_recent_public_trades(
                symbol,
                min_limit=min_limit,
                start_ms=(int(last_ts) if last_ts > 0 else None),
            )
            fetched = int(len(polled))
            for trade in polled:
                trade_id = int(trade.get("id") or 0)
                trade_ts = int(trade.get("timestamp") or 0)
                trade_px = float(trade.get("price") or 0.0)
                trade_amt = float(trade.get("amount") or 0.0)
                if trade_ts < last_ts:
                    skipped += 1
                    continue
                if last_id > 0 and trade_id > 0 and trade_id <= last_id:
                    skipped += 1
                    continue
                if (
                    trade_ts == last_ts
                    and trade_id <= 0
                    and abs(trade_px - last_px) <= 1e-12
                    and abs(trade_amt - last_amt) <= 1e-12
                ):
                    skipped += 1
                    continue
                if self._coincheck_ingest_trade(symbol, trade):
                    ingested += 1
                else:
                    skipped += 1
            logger.info(
                "[COINCHECK][REST_POLL] symbol=%s fetched=%s ingested=%s skipped=%s reason=%s",
                symbol,
                int(fetched),
                int(ingested),
                int(skipped),
                str(reason or ""),
            )
            return int(ingested)
        except Exception as exc:
            logger.warning(
                "[COINCHECK][REST_POLL] symbol=%s fetched=%s ingested=%s skipped=%s reason=%s err=%r",
                symbol,
                int(fetched),
                int(ingested),
                int(skipped),
                str(reason or ""),
                exc,
            )
            return 0

    def _coincheck_ensure_ws(self, symbol: str) -> None:
        if self.exchange_id != "coincheck":
            return
        symbol = str(self._normalize_symbol(symbol) or symbol)
        with self._coincheck_ws_lock:
            thread = self._coincheck_ws_threads.get(symbol)
            if thread is not None and getattr(thread, "is_alive", lambda: False)():
                return
            thread = threading.Thread(
                target=self._coincheck_ws_loop,
                args=(symbol,),
                name=f"coincheck-ws-{self._coincheck_ws_pair_id(symbol)}",
                daemon=True,
            )
            self._coincheck_seed_recent_trades_once(symbol)
            self._coincheck_ws_start_ts[symbol] = time.time()
            self._coincheck_ws_threads[symbol] = thread
            thread.start()

    def _coincheck_ws_loop(self, symbol: str) -> None:
        if self.exchange_id != "coincheck":
            return
        symbol = str(self._normalize_symbol(symbol) or symbol)
        pair_id = self._coincheck_ws_pair_id(symbol)
        channel = f"{pair_id}-trades"
        url = str(getattr(C, "COINCHECK_WS_PUBLIC_URL", "wss://ws-api.coincheck.com/") or "wss://ws-api.coincheck.com/")
        connect_timeout = max(5.0, self._safe_float(getattr(C, "COINCHECK_WS_CONNECT_TIMEOUT_SEC", 10.0), 10.0))
        recv_timeout = max(5.0, self._safe_float(getattr(C, "COINCHECK_WS_RECV_TIMEOUT_SEC", 30.0), 30.0))
        reconnect_sec = max(1.0, self._safe_float(getattr(C, "COINCHECK_WS_RECONNECT_SEC", 3.0), 3.0))
        try:
            import json
            import websocket
        except Exception as exc:
            warn_key = (symbol, "ws_import")
            if warn_key not in self._coincheck_ws_warned:
                logger.warning("[COINCHECK][WS] unavailable symbol=%s reason=%r", symbol, exc)
                self._coincheck_ws_warned.add(warn_key)
            return

        while not self._coincheck_ws_stop.is_set():
            ws = None
            try:
                ws = websocket.create_connection(url, timeout=float(connect_timeout))
                try:
                    ws.settimeout(float(recv_timeout))
                except Exception:
                   pass
                ws.send(json.dumps({"type": "subscribe", "channel": channel}, separators=(",", ":")))
                logger.info("[COINCHECK][WS] started symbol=%s channel=%s url=%s", symbol, channel, url)
                while not self._coincheck_ws_stop.is_set():
                    raw = ws.recv()
                    if raw is None:
                        continue
                    self._coincheck_ws_last_recv_ts[symbol] = time.time()
                    try:
                        msg = json.loads(raw) if isinstance(raw, str) else raw
                    except Exception:
                        msg = raw
                    ingested = 0
                    if isinstance(msg, list):
                        if msg and isinstance(msg[0], (list, tuple)):
                            for item in msg:
                                if self._coincheck_ingest_trade(symbol, item):
                                    ingested += 1
                        elif len(msg) >= 5:
                            if self._coincheck_ingest_trade(symbol, msg):
                                ingested += 1
                    elif isinstance(msg, dict):
                        if self._coincheck_ingest_trade(symbol, msg):
                            ingested += 1
                    if ingested > 0:
                        logger.debug("[COINCHECK][WS] ingested symbol=%s channel=%s trades=%s", symbol, channel, int(ingested))
            except Exception as exc:
                if not self._coincheck_ws_stop.is_set():
                    logger.warning("[COINCHECK][WS] reconnect symbol=%s channel=%s reason=%r", symbol, channel, exc)
                    time.sleep(float(reconnect_sec))
            finally:
                if ws is not None:
                    try:
                        ws.close()
                    except Exception:
                        pass

    def _coincheck_normalize_public_trade(self, trade: dict[str, Any]) -> dict[str, float | int] | None:
        if not isinstance(trade, dict):
            return None
        ts_ms = self._safe_int(trade.get("timestamp"), 0)
        if ts_ms <= 0 and trade.get("created_at"):
            try:
                ts_ms = self._safe_int(self.ex.parse8601(str(trade.get("created_at"))), 0)
            except Exception:
                ts_ms = 0
        price = self._safe_float(trade.get("price", trade.get("rate")), 0.0)
        amount = self._safe_float(trade.get("amount"), 0.0)
        trade_id = self._safe_int(trade.get("id"), 0)
        if ts_ms <= 0 or price <= 0.0 or amount <= 0.0:
            return None
        return {
            "id": int(trade_id),
            "timestamp": int(ts_ms),
            "price": float(price),
            "amount": float(amount),
        }

    def _bitbank_public_client(self):
        client = self._bitbank_public_ex
        if client is None:
            client = ccxt.bitbank({"enableRateLimit": True})
            self._bitbank_public_ex = client
        return client

    def _bitbank_fetch_seed_ohlcv(self, symbol: str, bars_5m: int, since_ms: int | None = None) -> list[list[float]]:
        symbol = str(self._normalize_symbol(symbol) or symbol)
        if symbol not in ("BTC/JPY", "ETH/JPY"):
            return []
        client = self._bitbank_public_client()
        limit_i = max(1, min(1000, int(bars_5m or 0)))
        last_err: Exception | None = None
        for i in range(2):
            try:
                rows = client.fetch_ohlcv(str(symbol), "5m", since_ms, limit_i)
                return rows or []
            except Exception as exc:
                last_err = exc
                time.sleep(0.25 * (2 ** i))
        logger.warning(
            "[COINCHECK][BITBANK_SEED] fetch_failed symbol=%s bars=%s reason=%r",
            symbol,
            int(limit_i),
            last_err,
        )
        return []

    def _bitbank_fetch_seed_ohlcv_paged(
        self,
        symbol: str,
        start_ms: int,
        end_ms: int,
        max_days: int,
    ) -> tuple[list[list[float]], bool]:
        symbol = str(self._normalize_symbol(symbol) or symbol)
        if symbol not in ("BTC/JPY", "ETH/JPY"):
            return [], False
        bucket_5m_ms = self._timeframe_to_ms("5m")
        day_ms = 24 * 60 * 60 * 1000
        target_start_ms = max(0, (int(start_ms) // int(bucket_5m_ms)) * int(bucket_5m_ms))
        target_end_ms = max(target_start_ms, (int(end_ms) // int(bucket_5m_ms)) * int(bucket_5m_ms))
        cursor_day_ms = (int(target_end_ms) // int(day_ms)) * int(day_ms)
        start_day_ms = (int(target_start_ms) // int(day_ms)) * int(day_ms)
        rows_by_ts: dict[int, list[float]] = {}
        fetched_days = 0

        while fetched_days < max(1, int(max_days)) and cursor_day_ms >= start_day_ms:
            day_rows = self._bitbank_fetch_seed_ohlcv(symbol, 1000, since_ms=int(cursor_day_ms))
            fetched_days += 1
            for row in day_rows:
                if not isinstance(row, list) or len(row) < 6:
                    continue
                ts_ms = (int(row[0]) // int(bucket_5m_ms)) * int(bucket_5m_ms)
                if ts_ms < target_start_ms or ts_ms > target_end_ms:
                    continue
                rows_by_ts[int(ts_ms)] = [
                    int(ts_ms),
                    float(row[1]),
                    float(row[2]),
                    float(row[3]),
                    float(row[4]),
                    float(row[5]),
                ]
            if rows_by_ts and min(rows_by_ts.keys()) <= target_start_ms:
                break
            cursor_day_ms -= int(day_ms)

        rows = [rows_by_ts[key] for key in sorted(rows_by_ts.keys())]
        reached_start = bool(rows) and int(rows[0][0]) <= int(target_start_ms)
        return rows, reached_start

    def _coincheck_seed_from_bitbank_once(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 200,
        since: int | None = None,
    ) -> int:
        if self.exchange_id != "coincheck":
            return 0
        symbol = str(self._normalize_symbol(symbol) or symbol)
        if symbol != "BTC/JPY":
            return 0
        tf = str(timeframe or "").strip().lower()
        if tf not in ("5m", "1h"):
            return 0
        seed_key = (symbol, tf)
        if seed_key in self._coincheck_bitbank_seed_done_symbols:
            return 0

        now_ms = int(time.time() * 1000)
        bucket_5m_ms = self._timeframe_to_ms("5m")
        limit_i = max(1, int(limit or 200))
        current_rows = self._coincheck_get_cached_ohlcv(symbol, tf, limit=limit_i, since=since)
        have_rows = int(len(current_rows))
        missing_rows = max(0, int(limit_i) - int(have_rows))
        needed_5m = int(limit_i) if tf == "5m" else max(0, int(missing_rows) * 12)
        if since is not None:
            needed_5m = max(
                int(needed_5m),
                int(((max(0, now_ms - int(since))) // int(bucket_5m_ms)) + 3),
            )
        needed_5m = max(200, int(needed_5m))
        fetch_5m = min(1000, int(needed_5m + 12))

        bars = self._coincheck_ohlcv_5m_by_symbol.setdefault(symbol, {})
        cache_target_5m = int(limit_i) if tf == "5m" else int(limit_i * 12 + 24)
        max_bars = self._coincheck_5m_cache_max(symbol, required_bars=cache_target_5m if tf == "1h" else None)
        coverage_complete = False
        if tf == "1h":
            existing_ts = sorted(int(ts) for ts in bars.keys() if int(ts) <= int(now_ms))
            page_end_ms = int(existing_ts[0] - int(bucket_5m_ms)) if existing_ts else int(now_ms)
            if since is not None:
                target_start_ms = max(0, int(since))
            else:
                target_start_ms = max(0, int(page_end_ms) - (int(bucket_5m_ms) * int(needed_5m + 12)))
            day_ms = 24 * 60 * 60 * 1000
            start_day_ms = (int(target_start_ms) // int(day_ms)) * int(day_ms)
            end_day_ms = (max(0, int(page_end_ms)) // int(day_ms)) * int(day_ms)
            max_days = max(1, min(9, int(((end_day_ms - start_day_ms) // int(day_ms)) + 1)))
            fetch_5m = int(max(needed_5m + 12, int(max_days) * 288))
            rows, coverage_complete = self._bitbank_fetch_seed_ohlcv_paged(
                symbol,
                start_ms=int(target_start_ms),
                end_ms=max(0, int(page_end_ms)),
                max_days=int(max_days),
            )
        else:
            rows = self._bitbank_fetch_seed_ohlcv(symbol, fetch_5m)
        if not rows:
            return 0

        injected = 0
        for row in rows:
            if not isinstance(row, list) or len(row) < 6:
                continue
            bucket_ts = (int(row[0]) // int(bucket_5m_ms)) * int(bucket_5m_ms)
            if bucket_ts <= 0 or int(bucket_ts) > int(now_ms):
                continue
            if int(bucket_ts) in bars:
                continue
            bars[int(bucket_ts)] = [
                int(bucket_ts),
                float(row[1]),
                float(row[2]),
                float(row[3]),
                float(row[4]),
                float(row[5]),
            ]
            injected += 1
        if len(bars) > max_bars:
            drop_keys = sorted(bars.keys())[:-max_bars]
            for old_key in drop_keys:
                bars.pop(old_key, None)
        logger.info(
            "[COINCHECK][BITBANK_SEED] symbol=%s tf=%s requested_5m=%s fetched=%s injected=%s",
            symbol,
            tf,
            int(fetch_5m),
            int(len(rows)),
            int(injected),
        )
        rows_after = self._coincheck_get_cached_ohlcv(symbol, tf, limit=limit_i, since=since)
        if injected > 0 and (tf != "1h" or len(rows_after) >= limit_i or coverage_complete):
            self._coincheck_bitbank_seed_done_symbols.add(seed_key)
        return int(injected)

    def _coincheck_fetch_recent_public_trades(self, symbol: str, min_limit: int, start_ms: int | None = None) -> list[dict]:
        symbol = str(self._normalize_symbol(symbol) or symbol)
        now_sec = time.time()
        debug_paging = bool(getattr(C, "COINCHECK_OHLCV_DEBUG_PAGING", True))
        required_start_ms = max(0, int(start_ms or 0))
        cache_ttl_sec = self._safe_float(getattr(C, "COINCHECK_OHLCV_CACHE_TTL_SEC", 15.0), 15.0)
        cached = self._coincheck_public_trade_cache.get(symbol)
        if isinstance(cached, dict):
            cached_ts = self._safe_float(cached.get("ts"), 0.0)
            cached_trades = cached.get("trades")
            cached_oldest_ts = self._safe_int(cached.get("oldest_ts"), 0)
            if (
                cached_ts > 0.0
                and (now_sec - cached_ts) <= cache_ttl_sec
                and isinstance(cached_trades, list)
                and (required_start_ms <= 0 or (cached_oldest_ts > 0 and cached_oldest_ts <= required_start_ms))
            ):
                if debug_paging:
                    logger.warning(
                        "[COINCHECK][TRADES_PAGING] cache_hit symbol=%s trades=%s oldest_ts=%s required_start_ms=%s age_sec=%.3f",
                        symbol,
                        int(len(cached_trades)),
                        int(cached_oldest_ts),
                        int(required_start_ms),
                        float(max(0.0, now_sec - cached_ts)),
                    )
                return list(cached_trades)

        page_limit = max(1, min(100, self._safe_int(getattr(C, "COINCHECK_OHLCV_PUBLIC_TRADES_PAGE_LIMIT", 100), 100)))
        max_fetch = max(page_limit, self._safe_int(getattr(C, "COINCHECK_OHLCV_TRADE_FETCH_LIMIT_MAX", 50000), 50000))
        target_fetch = max(page_limit, min(int(max_fetch), max(int(min_limit or page_limit), int(page_limit))))
        max_pages_cfg = max(1, self._safe_int(getattr(C, "COINCHECK_OHLCV_TRADE_FETCH_MAX_PAGES", 40), 40))
        max_pages = max(1, min(int(max_pages_cfg), int((max_fetch + page_limit - 1) // page_limit)))
        pair_id = self._coincheck_public_pair_id(symbol)

        collected: dict[Any, dict[str, float | int]] = {}
        oldest_id = 0
        oldest_ts = 0
        last_err: Exception | None = None

        for page_idx in range(max_pages):
            params: dict[str, Any] = {
                "pair": pair_id,
                "limit": int(page_limit),
                "order": "desc",
            }
            if page_idx > 0 and oldest_id > 0:
                params["ending_before"] = int(oldest_id)
            fetch_source = "none"
            try:
                raw_trades: list[Any] = []
                fetch_public = getattr(self.ex, "publicGetTrades", None)
                if callable(fetch_public):
                    raw = self._retry(fetch_public, params)
                    if isinstance(raw, dict):
                        raw_trades = list(raw.get("data", []) or [])
                        fetch_source = "publicGetTrades:dict"
                    elif isinstance(raw, list):
                        raw_trades = list(raw)
                        fetch_source = "publicGetTrades:list"
                if not raw_trades:
                    raw_trades = self._retry(self.ex.fetch_trades, symbol, None, int(page_limit), params)
                    fetch_source = "fetch_trades"
            except Exception as exc:
                last_err = exc
                if debug_paging:
                    logger.warning(
                        "[COINCHECK][TRADES_PAGING] fetch_error symbol=%s page=%s source=%s ending_before=%s required_start_ms=%s err=%r",
                        symbol,
                        int(page_idx),
                        str(fetch_source),
                        int(params.get("ending_before", 0) or 0),
                        int(required_start_ms),
                        exc,
                    )
                if page_idx == 0:
                    continue
                break

            page_trades: list[dict[str, float | int]] = []
            for trade in raw_trades or []:
                norm = self._coincheck_normalize_public_trade(trade)
                if norm is None:
                    continue
                trade_id = int(norm.get("id") or 0)
                trade_key: Any = trade_id if trade_id > 0 else (
                    int(norm["timestamp"]),
                    float(norm["price"]),
                    float(norm["amount"]),
                )
                if trade_key in collected:
                    continue
                collected[trade_key] = norm
                page_trades.append(norm)

            if debug_paging:
                logger.warning(
                    "[COINCHECK][TRADES_PAGING] page symbol=%s page=%s source=%s raw=%s new=%s ending_before=%s collected=%s oldest_id_before=%s required_start_ms=%s",
                    symbol,
                    int(page_idx),
                    str(fetch_source),
                    int(len(raw_trades or [])),
                    int(len(page_trades)),
                    int(params.get("ending_before", 0) or 0),
                    int(len(collected)),
                    int(oldest_id),
                    int(required_start_ms),
                )
            if not page_trades:
                if debug_paging:
                    logger.warning(
                        "[COINCHECK][TRADES_PAGING] stop_no_new symbol=%s page=%s source=%s ending_before=%s collected=%s",
                        symbol,
                        int(page_idx),
                        str(fetch_source),
                        int(params.get("ending_before", 0) or 0),
                        int(len(collected)),
                    )
                break

            page_trades.sort(key=lambda x: (int(x["timestamp"]), int(x.get("id") or 0), float(x["price"])))
            page_oldest_ts = int(page_trades[0]["timestamp"])
            page_oldest_id = min(int(t.get("id") or 0) for t in page_trades)
            oldest_ts = page_oldest_ts if oldest_ts <= 0 else min(oldest_ts, page_oldest_ts)
            if page_oldest_id > 0:
                if oldest_id > 0 and page_oldest_id >= oldest_id:
                    if debug_paging:
                        logger.warning(
                            "[COINCHECK][TRADES_PAGING] stop_oldest_not_advanced symbol=%s page=%s page_oldest_id=%s oldest_id=%s page_oldest_ts=%s required_start_ms=%s",
                            symbol,
                            int(page_idx),
                            int(page_oldest_id),
                            int(oldest_id),
                            int(page_oldest_ts),
                            int(required_start_ms),
                        )
                    break
                oldest_id = page_oldest_id

            if required_start_ms > 0 and oldest_ts > 0 and oldest_ts <= required_start_ms:
                if debug_paging:
                    logger.warning(
                        "[COINCHECK][TRADES_PAGING] stop_reached_start symbol=%s page=%s oldest_ts=%s required_start_ms=%s collected=%s",
                        symbol,
                        int(page_idx),
                        int(oldest_ts),
                        int(required_start_ms),
                        int(len(collected)),
                    )
                break
            if required_start_ms <= 0 and len(collected) >= int(target_fetch):
                if debug_paging:
                    logger.warning(
                        "[COINCHECK][TRADES_PAGING] stop_target_fetch symbol=%s page=%s collected=%s target_fetch=%s",
                        symbol,
                        int(page_idx),
                        int(len(collected)),
                        int(target_fetch),
                    )
                break
            if len(collected) >= int(max_fetch):
                if debug_paging:
                    logger.warning(
                        "[COINCHECK][TRADES_PAGING] stop_max_fetch symbol=%s page=%s collected=%s max_fetch=%s",
                        symbol,
                        int(page_idx),
                        int(len(collected)),
                        int(max_fetch),
                    )
                break
            if len(page_trades) < int(page_limit):
                if debug_paging:
                    logger.warning(
                        "[COINCHECK][TRADES_PAGING] stop_short_page symbol=%s page=%s new=%s page_limit=%s collected=%s",
                        symbol,
                        int(page_idx),
                        int(len(page_trades)),
                        int(page_limit),
                        int(len(collected)),
                    )
                break
            if page_oldest_id <= 0:
                if debug_paging:
                    logger.warning(
                        "[COINCHECK][TRADES_PAGING] stop_no_oldest_id symbol=%s page=%s page_oldest_ts=%s collected=%s",
                        symbol,
                        int(page_idx),
                        int(page_oldest_ts),
                        int(len(collected)),
                    )
                break

        trades = list(collected.values())
        trades.sort(key=lambda x: (int(x["timestamp"]), int(x.get("id") or 0), float(x["price"])))
        if trades:
            self._coincheck_public_trade_cache[symbol] = {
                "ts": float(now_sec),
                "limit": int(len(trades)),
                "oldest_ts": int(trades[0]["timestamp"]),
                "trades": list(trades),
            }
            return trades
        if last_err is not None:
            raise last_err
        return []

    def _coincheck_get_cached_ohlcv(self, symbol: str, timeframe: str, limit: int = 200, since: int | None = None) -> list[list[float]]:
        symbol = str(self._normalize_symbol(symbol) or symbol)
        tf = str(timeframe or "").strip().lower()
        if tf not in ("5m", "1h"):
            raise ccxt.NotSupported(f"coincheck cached ohlcv supports only 5m/1h: {timeframe}")

        bars_map = self._coincheck_ohlcv_5m_by_symbol.get(symbol) or {}
        if not isinstance(bars_map, dict) or not bars_map:
            return []

        now_ms = int(time.time() * 1000)
        limit_i = max(1, int(limit or 200))
        bucket_5m_ms = self._timeframe_to_ms("5m")
        target_ms = self._timeframe_to_ms(tf)
        if since is None:
            lookback_5m_bars = int(limit_i) if tf == "5m" else int(limit_i) * 12
            start_ms = max(0, now_ms - (int(bucket_5m_ms) * (int(lookback_5m_bars) + 2)))
        else:
            start_ms = max(0, int(since))
        start_ms = (int(start_ms) // int(bucket_5m_ms)) * int(bucket_5m_ms)

        rows_5m: list[list[float]] = []
        for bucket_ts in sorted(bars_map.keys()):
            if int(bucket_ts) < int(start_ms) or int(bucket_ts) > int(now_ms):
                continue
            row = bars_map.get(int(bucket_ts))
            if not isinstance(row, list) or len(row) < 6:
                continue
            rows_5m.append([int(row[0]), float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5])])
        if not rows_5m:
            return []

        rows_5m = self._fill_ohlcv_gaps(rows_5m, int(bucket_5m_ms), end_ms=int(now_ms))
        rows = rows_5m if tf == "5m" else self._aggregate_ohlcv_rows(rows_5m, int(target_ms))
        if since is not None:
            rows = [row for row in rows if int(row[0]) >= int(since)]
        if limit_i > 0 and len(rows) > limit_i:
            rows = rows[-limit_i:]
        return rows

    def _fill_ohlcv_gaps(self, rows: list[list[float]], bucket_ms: int, end_ms: int | None = None) -> list[list[float]]:
        if not rows:
            return []
        out: list[list[float]] = []
        prev_ts: int | None = None
        prev_close: float | None = None
        for row in rows:
            ts_ms = int(row[0])
            if prev_ts is not None and prev_close is not None:
                fill_ts = int(prev_ts) + int(bucket_ms)
                while fill_ts < ts_ms:
                    px = float(prev_close)
                    out.append([int(fill_ts), px, px, px, px, 0.0])
                    fill_ts += int(bucket_ms)
            out.append([
                int(row[0]),
                float(row[1]),
                float(row[2]),
                float(row[3]),
                float(row[4]),
                float(row[5]),
            ])
            prev_ts = int(row[0])
            prev_close = float(row[4])
        if end_ms is not None and prev_ts is not None and prev_close is not None:
            final_bucket = (int(end_ms) // int(bucket_ms)) * int(bucket_ms)
            fill_ts = int(prev_ts) + int(bucket_ms)
            while fill_ts <= final_bucket:
                px = float(prev_close)
                out.append([int(fill_ts), px, px, px, px, 0.0])
                fill_ts += int(bucket_ms)
        return out

    def _aggregate_ohlcv_rows(self, rows: list[list[float]], bucket_ms: int) -> list[list[float]]:
        out: list[list[float]] = []
        current: list[float] | None = None
        for row in rows:
            bucket_ts = (int(row[0]) // int(bucket_ms)) * int(bucket_ms)
            if current is None or int(current[0]) != int(bucket_ts):
                current = [
                    int(bucket_ts),
                    float(row[1]),
                    float(row[2]),
                    float(row[3]),
                    float(row[4]),
                    float(row[5]),
                ]
                out.append(current)
                continue
            current[2] = max(float(current[2]), float(row[2]))
            current[3] = min(float(current[3]), float(row[3]))
            current[4] = float(row[4])
            current[5] = float(current[5]) + float(row[5])
        return [[int(row[0]), float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5])] for row in out]

    def _coincheck_trades_to_5m_ohlcv(self, trades: list[dict], start_ms: int, end_ms: int) -> list[list[float]]:
        bucket_ms = self._timeframe_to_ms("5m")
        buckets: dict[int, list[float]] = {}
        for trade in trades:
            ts_ms = self._safe_int(trade.get("timestamp"), 0)
            price = self._safe_float(trade.get("price"), 0.0)
            amount = self._safe_float(trade.get("amount"), 0.0)
            if ts_ms <= 0 or price <= 0.0 or amount <= 0.0:
                continue
            bucket_ts = (int(ts_ms) // int(bucket_ms)) * int(bucket_ms)
            if bucket_ts < int(start_ms) or bucket_ts > int(end_ms):
                continue
            row = buckets.get(int(bucket_ts))
            if row is None:
                buckets[int(bucket_ts)] = [
                    int(bucket_ts),
                    float(price),
                    float(price),
                    float(price),
                    float(price),
                    float(amount),
                ]
                continue
            row[2] = max(float(row[2]), float(price))
            row[3] = min(float(row[3]), float(price))
            row[4] = float(price)
            row[5] = float(row[5]) + float(amount)
        rows = [buckets[k] for k in sorted(buckets)]
        return self._fill_ohlcv_gaps(rows, bucket_ms, end_ms=end_ms)

    def _coincheck_fetch_ohlcv_fallback(self, symbol: str, timeframe: str, limit: int = 200, since: int | None = None) -> list[list[float]]:
        symbol = str(self._normalize_symbol(symbol) or symbol)
        tf = str(timeframe or "").strip().lower()
        if tf not in ("5m", "1h"):
            raise ccxt.NotSupported(f"coincheck fallback supports only 5m/1h: {timeframe}")

        limit_i = max(1, int(limit or 200))
        diag_enabled = bool(getattr(C, "COINCHECK_OHLCV_DIAG", True))
        stale_warn_sec = max(10.0, self._safe_float(getattr(C, "COINCHECK_WS_STALE_WARN_SEC", 90.0), 90.0))
        ws_start_grace_sec = max(5.0, self._safe_float(getattr(C, "COINCHECK_WS_START_GRACE_SEC", 45.0), 45.0))
        self._coincheck_ensure_ws(symbol)
        self._coincheck_seed_recent_trades_once(symbol)

        last_recv_ts = self._safe_float(self._coincheck_ws_last_recv_ts.get(symbol), 0.0)
        ws_start_ts = self._safe_float(self._coincheck_ws_start_ts.get(symbol), 0.0)
        ws_age_sec = max(0.0, time.time() - float(last_recv_ts)) if last_recv_ts > 0.0 else -1.0
        ws_start_age_sec = max(0.0, time.time() - float(ws_start_ts)) if ws_start_ts > 0.0 else -1.0
        ws_diag_should_warn = False
        if last_recv_ts > 0.0:
            ws_diag_should_warn = ws_age_sec >= stale_warn_sec
        elif ws_start_ts <= 0.0 or ws_start_age_sec >= ws_start_grace_sec:
            ws_diag_should_warn = True
        if diag_enabled and ws_diag_should_warn:
            logger.warning(
                "[COINCHECK][WS_DIAG] symbol=%s tf=%s last_recv_ts=%.3f age_sec=%.3f ws_start_ts=%.3f ws_start_age_sec=%.3f stale_warn_sec=%.3f start_grace_sec=%.3f",
                symbol,
                tf,
                float(last_recv_ts),
                float(ws_age_sec),
                float(ws_start_ts),
                float(ws_start_age_sec),
                float(stale_warn_sec),
                float(ws_start_grace_sec),
            )
        rest_poll_reason = ""
        if (symbol, "ws_import") in self._coincheck_ws_warned:
            rest_poll_reason = "ws_unavailable"
        elif last_recv_ts > 0.0 and ws_age_sec >= stale_warn_sec:
            rest_poll_reason = "ws_stale"
        elif last_recv_ts <= 0.0 and (ws_start_ts <= 0.0 or ws_start_age_sec >= ws_start_grace_sec):
            rest_poll_reason = "ws_unavailable"
        if rest_poll_reason:
            self._coincheck_poll_public_trades_if_needed(symbol, reason=rest_poll_reason)

        rows = self._coincheck_get_cached_ohlcv(symbol, tf, limit=limit_i, since=since)
        bars_before = int(len(rows))
        if diag_enabled:
            logger.warning(
                "[COINCHECK][OHLCV_DIAG] stage=before_seed symbol=%s tf=%s bars=%s limit=%s trade_ring=%s",
                symbol,
                tf,
                int(bars_before),
                int(limit_i),
                int(len(self._coincheck_trade_ring_by_symbol.get(symbol) or [])),
            )

        seed_injected = 0
        if len(rows) < limit_i:
            seed_injected = int(self._coincheck_seed_from_bitbank_once(symbol, tf, limit=limit_i, since=since) or 0)
            rows = self._coincheck_get_cached_ohlcv(symbol, tf, limit=limit_i, since=since)
            if diag_enabled:
                logger.warning(
                    "[COINCHECK][OHLCV_DIAG] stage=after_seed symbol=%s tf=%s bars_before=%s seed_injected=%s bars_after=%s limit=%s",
                    symbol,
                    tf,
                    int(bars_before),
                    int(seed_injected),
                    int(len(rows)),
                    int(limit_i),
                )

        if diag_enabled:
            logger.warning(
                "[COINCHECK][OHLCV_DIAG] stage=final_cached symbol=%s tf=%s bars=%s limit=%s trade_ring=%s",
                symbol,
                tf,
                int(len(rows)),
                int(limit_i),
                int(len(self._coincheck_trade_ring_by_symbol.get(symbol) or [])),
            )

        if len(rows) < limit_i:
            warn_key = (symbol, tf, "limited_history")
            if warn_key not in self._coincheck_ohlcv_warned:
                earliest_ts = int(rows[0][0]) if rows else 0
                trade_count = int(len(self._coincheck_trade_ring_by_symbol.get(symbol) or []))
                logger.warning(
                    "[COINCHECK][OHLCV_FALLBACK] limited history symbol=%s tf=%s bars=%s limit=%s trades=%s earliest_ts=%s",
                    symbol,
                    tf,
                    int(len(rows)),
                    int(limit_i),
                    int(trade_count),
                    int(earliest_ts),
                )
                self._coincheck_ohlcv_warned.add(warn_key)

        if rows:
            return rows
        raise ccxt.ExchangeError(f"coincheck ohlcv fallback returned no cached rows symbol={symbol} tf={tf}")

    # -------------------------
    # market data
    # -------------------------
    def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 200, since: int | None = None):
        symbol = str(self._normalize_symbol(symbol) or symbol)
        tf = str(timeframe or "").strip().lower()
        effective_limit = int(limit or 200)
        if self.exchange_id == "coincheck" and tf == "5m" and since is None:
            effective_limit = max(1, int(effective_limit) + 1)
        if self.exchange_id == "coincheck" and tf in ("5m", "1h"):
            self._coincheck_ensure_ws(symbol)
            self._coincheck_seed_recent_trades_once(symbol)
            has_map = getattr(self.ex, "has", {}) or {}
            has_fetch_ohlcv = has_map.get("fetchOHLCV") if isinstance(has_map, dict) else None
            if not bool(has_fetch_ohlcv):
                return self._coincheck_fetch_ohlcv_fallback(symbol, tf, limit=effective_limit, since=since)

        try:
            return self._retry(self.ex.fetch_ohlcv, symbol, timeframe, since, effective_limit)
        except Exception as exc:
            if self.exchange_id == "coincheck" and tf in ("5m", "1h") and self._coincheck_fetch_ohlcv_unsupported(exc):
                return self._coincheck_fetch_ohlcv_fallback(symbol, tf, limit=effective_limit, since=since)
            raise

    def fetch_ticker(self, symbol: str):
        symbol = str(self._normalize_symbol(symbol) or symbol)
        return self._retry(self.ex.fetch_ticker, symbol)

    def fetch_tickers(self, symbols: Optional[Sequence[str]] = None):
        # Many exchanges allow None => all tickers
        if self.exchange_id == "coincheck":
            if symbols is None:
                symbols = [self._normalize_symbol((list(getattr(C, "SYMBOLS", []) or ["BTC/JPY"]) or ["BTC/JPY"])[0])]
            return {str(symbol): self.fetch_ticker(str(symbol)) for symbol in symbols if symbol}
        return self._retry(self.ex.fetch_tickers, symbols)

    def fetch_order_book(self, symbol: str, limit: int = 20):
        symbol = str(self._normalize_symbol(symbol) or symbol)
        return self._retry(self.ex.fetch_order_book, symbol, limit)

    def fetch_best_bid_ask(self, symbol: str) -> tuple[float | None, float | None]:
        symbol = str(self._normalize_symbol(symbol) or symbol)
        ob = self.fetch_order_book(symbol, limit=5)
        bids = ob.get("bids") or []
        asks = ob.get("asks") or []
        bid = self._safe_float((bids[0][0] if bids else None), 0.0) if bids else 0.0
        ask = self._safe_float((asks[0][0] if asks else None), 0.0) if asks else 0.0
        return ((float(bid) if bid > 0.0 else None), (float(ask) if ask > 0.0 else None))

    def fetch_market_fees(self, symbol: str) -> tuple[float, float, str]:
        symbol = str(self._normalize_symbol(symbol) or symbol)
        fallback_maker, fallback_taker = resolve_paper_fees(self.exchange_id)
        fallback_source = "config"
        try:
            self._ensure_markets_loaded()
            market = self.ex.market(symbol)
            maker = market.get("maker") if isinstance(market, dict) else None
            taker = market.get("taker") if isinstance(market, dict) else None
            if isinstance(maker, (int, float)) and isinstance(taker, (int, float)):
                return (float(maker), float(taker), "ccxt_markets")
            logger.warning(
                "[MARKET_FEES] missing maker/taker in markets exchange_id=%s symbol=%s -> config fallback",
                self.exchange_id,
                symbol,
            )
        except Exception as e:
            logger.warning(
                "[MARKET_FEES] load_markets failed exchange_id=%s symbol=%s -> config fallback reason=%s",
                self.exchange_id,
                symbol,
                e,
            )
        return (float(fallback_maker), float(fallback_taker), fallback_source)

    def fetch_recent_trade_fee_hint(self, symbol: str, lookback_min: int = 60) -> dict | None:
        symbol = str(self._normalize_symbol(symbol) or symbol)
        api_key = str(getattr(self.ex, "apiKey", "") or "").strip()
        secret = str(getattr(self.ex, "secret", "") or "").strip()
        if not api_key or not secret:
            return None
        since_ms = max(0, int((time.time() - max(1, int(lookback_min)) * 60) * 1000))
        try:
            trades = self._retry(self.ex.fetch_my_trades, symbol, since_ms, 50, {})
        except Exception as e:
            logger.warning(
                "[MARKET_FEES] fetch_my_trades failed exchange_id=%s symbol=%s reason=%s",
                self.exchange_id,
                symbol,
                e,
            )
            return None
        rows = trades or []
        if not rows:
            return None
        total_fee = 0.0
        total_cost = 0.0
        sample_side = ""
        fee_currency = ""
        counted = 0
        for trade in rows:
            fee = trade.get("fee") if isinstance(trade, dict) else None
            fee_cost = None
            if isinstance(fee, dict):
                fee_cost = fee.get("cost")
                fee_currency = str(fee.get("currency") or fee_currency or "")
            cost = trade.get("cost") if isinstance(trade, dict) else None
            side = trade.get("side") if isinstance(trade, dict) else None
            try:
                fee_cost_f = float(fee_cost)
                cost_f = float(cost)
            except Exception:
                continue
            if cost_f <= 0.0 or fee_cost_f < 0.0:
                continue
            total_fee += float(fee_cost_f)
            total_cost += float(cost_f)
            counted += 1
            if side and not sample_side:
                sample_side = str(side)
        if counted <= 0 or total_cost <= 0.0:
            return None
        return {
            "symbol": symbol,
            "trade_count": int(counted),
            "fee_currency": str(fee_currency or ""),
            "side": str(sample_side or ""),
            "effective_fee_rate": float(total_fee / total_cost),
            "source": "recent_my_trades",
        }

    # -------------------------
    # precision / rules
    # -------------------------
    def amount_to_precision(self, symbol: str, amount: float) -> float:
        symbol = str(self._normalize_symbol(symbol) or symbol)
        try:
            rules = self.get_market_rules(symbol)
            min_qty = float(rules.get("min_qty") or 0.0)
            amount_step = float(rules.get("amount_step") or 0.0)
            amount_precision = int(rules.get("amount_precision") or 0)
            if self.exchange_id == "mexc" and amount_step > 0.0 and min_qty > 0.0 and min_qty < 1.0:
                return float(self._round_down_to_step(amount, amount_step, amount_precision))
        except Exception:
            pass
        try:
            self._ensure_markets_loaded()
            s = self.ex.amount_to_precision(symbol, amount)
            return float(s)
        except Exception:
            return float(amount)

    def price_to_precision(self, symbol: str, price: float) -> float:
        symbol = str(self._normalize_symbol(symbol) or symbol)
        try:
            self._ensure_markets_loaded()
            s = self.ex.price_to_precision(symbol, price)
            return float(s)
        except Exception:
            return float(price)

    def market_amount_rules(self, symbol: str) -> tuple[float, float]:
        rules = self.get_market_rules(symbol)
        return float(rules["min_qty"]), float(rules["min_cost"])

    def _decimals_from_precision(self, symbol: str) -> tuple[int, int]:
        return self._decimals_from_market(symbol)

    def _tick_size_from_precision(self, symbol: str) -> float:
        p_dec, _ = self._decimals_from_precision(symbol)
        if p_dec >= 0:
            return 10 ** (-p_dec)
        return 0.0

    # -------------------------
    # equity (LIVE)
    # -------------------------
    def fetch_balance(self):
        return self._retry(self.ex.fetch_balance)

    def get_total_equity(self, quote: str | None = None) -> float:
        quote_ccy = str(quote or getattr(C, "BASE_CURRENCY", "") or self._default_quote_currency()).strip().upper()
        bal = self.fetch_balance()
        total = bal.get("total", {}) if bal else {}
        return float(total.get(quote_ccy, 0.0) or 0.0)

    # -------------------------
    # Orders
    # -------------------------
    def create_limit_order(self, symbol: str, side: str, amount: float, price: float, params: dict | None = None):
        symbol = str(self._normalize_symbol(symbol) or symbol)
        params = self._adapt_order_params(params)
        if self.exchange_id == "coincheck":
            normalized = self._normalize_limit_order(symbol, side, amount, price)
            symbol = str(normalized["symbol"])
            amount = float(normalized["amount"])
            price = float(normalized["price"])
            self._coincheck_rate_limit("create")
        return self._retry(self.ex.create_order, symbol, "limit", side, amount, price, params)
        
    def create_maker_ttl_order(self, symbol: str, side: str,    amount: float, ttl_sec: float, params: dict | None = None):
        """
        Create a maker-first entry order with TTL handling for runner.py.
        Return the normalized fields expected by runner.py: {filled_qty, avg_price, reason, order}.
        """
        max_reprices = int(getattr(C, "MAKER_MAX_REPRICES", 6))
        poll_interval = float(getattr(C, "MAKER_POLL_INTERVAL", 0.25))
        offset_ticks = int(getattr(C, "MAKER_OFFSET_TICKS", 0))
        dryrun = bool(getattr(C, "LIVE_DRYRUN", False))

        o = self.place_maker_limit_ttl(
            symbol=symbol,
            side=side,
            amount=float(amount),
            ttl_sec=float(ttl_sec),
            max_reprices=max_reprices,
            poll_interval=poll_interval,
            offset_ticks=offset_ticks,
            dryrun=dryrun,
            params=params,
        )

        # Extract the normalized fields from the ccxt order response.
        filled = float(o.get("filled") or 0.0)
        avg = o.get("average", None)
        try:
            avg = float(avg) if avg is not None else None
        except Exception:
            avg = None

        status = str(o.get("status") or "")
        reason = str(o.get("_maker_ttl_reason") or f"MAKER_TTL:{status}")
        return {
            "filled_qty": filled,
            "avg_price": avg,
            "reason": reason,
            "order": o,
        }

    def create_market_order(self, symbol: str, side: str, amount: float, params: dict | None = None):
        symbol = str(self._normalize_symbol(symbol) or symbol)
        params = self._adapt_order_params(params)
        return self._retry(self.ex.create_order, symbol, "market", side, amount, None, params)

    def cancel_order(self, order_id: str, symbol: str):
        symbol = str(self._normalize_symbol(symbol) or symbol)
        if self.exchange_id == "coincheck":
            return self._retry(self.ex.cancel_order, order_id, None)
        return self._retry(self.ex.cancel_order, order_id, symbol)

    def fetch_order(self, order_id: str, symbol: str):
        symbol = str(self._normalize_symbol(symbol) or symbol)
        if self.exchange_id == "coincheck":
            return self._coincheck_fetch_order_state(order_id, symbol)
        return self._retry(self.ex.fetch_order, order_id, symbol)

    def fetch_open_orders(self, symbol: str | None = None, limit: int | None = None):
        # ccxt: fetch_open_orders(symbol=None, since=None, limit=None, params={})
        symbol = self._normalize_symbol(symbol)
        try:
            return self._retry(self.ex.fetch_open_orders, symbol, None, limit, {})
        except TypeError:
            # Normalize exchange and ccxt response differences here.
            return self._retry(self.ex.fetch_open_orders, symbol)

    def cancel_all_orders(self, symbol: str | None = None):
        # ccxt: cancel_all_orders(symbol=None, params={})
        try:
            return self._retry(self.ex.cancel_all_orders, symbol, {})
        except TypeError:
            return self._retry(self.ex.cancel_all_orders, symbol)


    # -------------------------
    # pseudo post-only maker price
    # -------------------------
    def _maker_price_from_order_book(self, symbol: str, side: str, order_book: dict, offset_ticks: int = 0) -> float:
        bids = order_book.get("bids") or []
        asks = order_book.get("asks") or []
        if not bids or not asks:
            raise RuntimeError(f"order_book empty: {symbol}")

        best_bid = float(bids[0][0])
        best_ask = float(asks[0][0])
        tick = self.get_market_rules(symbol)["tick_size"]

        def p(x: float) -> float:
            return self.price_to_precision(symbol, x)

        if side.lower() == "buy":
            raw = best_bid + (tick * offset_ticks if tick else 0.0)
            if raw >= best_ask:
                raw = best_ask - (tick if tick else 0.0)
            price = p(raw)
            if price >= best_ask:
                price = p(best_bid)
            return price

        if side.lower() == "sell":
            raw = best_ask - (tick * offset_ticks if tick else 0.0)
            if raw <= best_bid:
                raw = best_bid + (tick if tick else 0.0)
            price = p(raw)
            if price <= best_bid:
                price = p(best_ask)
            return price

        raise ValueError("side must be 'buy' or 'sell'")

    def _fallback_non_crossing_price(self, symbol: str, side: str, requested_price: float, order_book: dict) -> tuple[float, bool]:
        bids = order_book.get("bids") or []
        asks = order_book.get("asks") or []
        if not bids or not asks:
            raise RuntimeError(f"order_book empty: {symbol}")
        best_bid = float(bids[0][0])
        best_ask = float(asks[0][0])
        tick = float(self.get_market_rules(symbol)["tick_size"])
        requested = float(requested_price)
        adjusted = requested
        changed = False

        if str(side).lower() == "buy":
            guard_price = best_ask - tick
            if guard_price <= best_bid:
                guard_price = best_bid
            adjusted = min(requested, guard_price)
        else:
            guard_price = best_bid + tick
            if guard_price >= best_ask:
                guard_price = best_ask
            adjusted = max(requested, guard_price)
        normalized = self._normalize_limit_order(symbol, side, 0.0, adjusted)
        adjusted = float(normalized["price"])
        changed = abs(adjusted - requested) > 1e-12
        return adjusted, changed

    def _maker_price(self, symbol: str, side: str, offset_ticks: int = 0) -> float:
        ob = self.fetch_order_book(symbol, limit=5)
        return self._maker_price_from_order_book(symbol, side, ob, offset_ticks=offset_ticks)

    def place_maker_limit_ttl(
        self,
        symbol: str,
        side: str,
        amount: float,
        ttl_sec: float,
        max_reprices: int,
        poll_interval: float = 0.25,
        offset_ticks: int = 0,
        dryrun: bool = False,
        params: dict | None = None,
    ):
        symbol = str(self._normalize_symbol(symbol) or symbol)
        if self.exchange_id == "coincheck":
            poll_interval = max(float(poll_interval), 1.05)

        last_order = None
        for attempt in range(max_reprices):
            order_book = self.fetch_order_book(symbol, limit=5)
            requested_price = self._maker_price_from_order_book(symbol, side, order_book, offset_ticks=offset_ticks)
            normalized = self._normalize_limit_order(symbol, side, amount, requested_price)
            order_amount = float(normalized["amount"])
            order_price = float(normalized["price"])
            order_reason = "MAKER_TTL:post_only_ok"
            if abs(float(order_price) - float(requested_price)) > 1e-12:
                order_reason = "MAKER_TTL:post_only_cross_guard_adjusted"

            if dryrun:
                return {
                    "id": "DRYRUN",
                    "symbol": symbol,
                    "side": side,
                    "amount": order_amount,
                    "price": order_price,
                    "status": "open",
                    "info": {"attempt": attempt + 1, "dryrun": True, "normalized": normalized},
                    "_maker_ttl_reason": order_reason,
                }

            order_params = self._adapt_order_params(params)
            order = None
            if self.exchange_id == "coincheck":
                order_params["time_in_force"] = "post_only"
                try:
                    order = self.create_limit_order(symbol, side, order_amount, order_price, params=order_params)
                except Exception as exc:
                    if not self._is_coincheck_post_only_rejection(exc):
                        raise
                    fallback_price, _changed = self._fallback_non_crossing_price(symbol, side, order_price, order_book)
                    fallback_normalized = self._normalize_limit_order(symbol, side, order_amount, fallback_price)
                    fallback_params = self._adapt_order_params(params)
                    fallback_params.pop("time_in_force", None)
                    order = self.create_limit_order(
                        symbol,
                        side,
                        float(fallback_normalized["amount"]),
                        float(fallback_normalized["price"]),
                        params=fallback_params,
                    )
                    normalized = fallback_normalized
                    order_amount = float(fallback_normalized["amount"])
                    order_price = float(fallback_normalized["price"])
                    order_reason = "MAKER_TTL:post_only_rejected_fallback_gtc"
            else:
                order_params["postOnly"] = True
                order = self.create_limit_order(symbol, side, order_amount, order_price, params=order_params)

            last_order = order
            order_id = order.get("id")
            t0 = time.time()

            while True:
                if self.exchange_id == "coincheck":
                    o = self._coincheck_fetch_order_state(
                        str(order_id),
                        symbol,
                        amount=float(order_amount),
                        price=float(order_price),
                        side=side,
                    )
                else:
                    o = self.fetch_order(order_id, symbol)
                o["_maker_ttl_reason"] = order_reason
                status = (o.get("status") or "").lower()

                if status in ("closed", "filled"):
                    return o

                filled = float(o.get("filled") or 0.0)
                remaining = float(o.get("remaining") or 0.0)

                if filled > 0 and remaining <= 0:
                    return o

                if (time.time() - t0) >= ttl_sec:
                    break

                time.sleep(poll_interval)

            # TTL expired -> cancel and reprice
            try:
                self.cancel_order(order_id, symbol)
            except Exception:
                if self.exchange_id == "coincheck":
                    o2 = self._coincheck_fetch_order_state(
                        str(order_id),
                        symbol,
                        amount=float(order_amount),
                        price=float(order_price),
                        side=side,
                    )
                else:
                    o2 = self.fetch_order(order_id, symbol)
                o2["_maker_ttl_reason"] = order_reason
                st2 = (o2.get("status") or "").lower()
                if st2 in ("closed", "filled"):
                    return o2
            if self.exchange_id == "coincheck":
                last_order = self._coincheck_fetch_order_state(
                    str(order_id),
                    symbol,
                    amount=float(order_amount),
                    price=float(order_price),
                    side=side,
                )
                last_order["_maker_ttl_reason"] = order_reason

        if last_order is not None:
            return last_order
        raise RuntimeError("place_maker_limit_ttl failed before placing any order")
    def _count_decimals(self, x) -> int:
        try:
            s = str(x)
            if "e-" in s or "E-" in s:
                # scientific notation -> rough fallback
                return max(0, int(s.split("-")[-1]))
            if "." in s:
                return len(s.split(".")[1].rstrip("0"))
            return 0
        except Exception:
            return 0

    def _decimals_from_market(self, symbol: str) -> tuple[int, int]:
        """Try to infer price/amount decimals from market precision or step sizes."""
        symbol = str(self._normalize_symbol(symbol) or symbol)
        self._ensure_markets_loaded()
        m = self.ex.market(symbol)
        if not m:
            return 0, 0

        prec = m.get("precision") or {}
        p_dec = prec.get("price", None)
        a_dec = prec.get("amount", None)
        p_dec = max(int(p_dec), 0) if isinstance(p_dec, int) and not isinstance(p_dec, bool) else None
        a_dec = max(int(a_dec), 0) if isinstance(a_dec, int) and not isinstance(a_dec, bool) else None

        info = m.get("info") or {}
        for pk in ("pricePrecision", "priceScale", "quotePrecision", "price_decimal"):
            if pk in info:
                try:
                    p_dec = max(int(p_dec or 0), int(float(info[pk])))
                    break
                except Exception:
                    pass
        for ak in ("quantityPrecision", "amountPrecision", "basePrecision", "qtyScale", "amount_decimal"):
            if ak in info:
                try:
                    a_dec = max(int(a_dec or 0), int(float(info[ak])))
                    break
                except Exception:
                    pass

        for k in ("tickSize", "priceIncrement", "price_tick", "minPrice"):
            if k in info:
                d = self._count_decimals(info[k])
                if d > 0:
                    p_dec = max(int(p_dec or 0), d)
                    break

        for k in ("stepSize", "quantityIncrement", "qty_step", "minQty"):
            if k in info:
                d = self._count_decimals(info[k])
                if d > 0:
                    a_dec = max(int(a_dec or 0), d)
                    break

        limits = m.get("limits") or {}
        price_limits = limits.get("price", {}) if isinstance(limits, dict) else {}
        amount_limits = limits.get("amount", {}) if isinstance(limits, dict) else {}
        price_limit_decimals = self._count_decimals((price_limits or {}).get("min"))
        if price_limit_decimals > 0:
            p_dec = max(int(p_dec or 0), int(price_limit_decimals))
        amount_limit_decimals = self._count_decimals((amount_limits or {}).get("min"))
        if amount_limit_decimals > 0:
            a_dec = max(int(a_dec or 0), int(amount_limit_decimals))

        return int(p_dec or 0), int(a_dec or 0)

    def _ticker_bid_ask(self, t: dict) -> tuple[float | None, float | None]:
        """Extract bid/ask from ccxt ticker or raw info."""
        bid = t.get("bid", None)
        ask = t.get("ask", None)

        def f(v):
            try:
                if v is None:
                    return None
                v = float(v)
                return v if v > 0 else None
            except Exception:
                return None

        bid = f(bid)
        ask = f(ask)
        if bid and ask:
            return bid, ask

        info = t.get("info") or {}
        # common raw keys
        bid = f(info.get("bidPrice") or info.get("bid") or info.get("b"))
        ask = f(info.get("askPrice") or info.get("ask") or info.get("a"))
        return bid, ask

    def _ticker_quote_volume(self, t: dict) -> float:
        """quoteVolume if possible, else approximate baseVolume * last."""
        def f(v):
            try:
                if v is None:
                    return 0.0
                return float(v)
            except Exception:
                return 0.0

        # direct (ccxt normalized)
        for k in ("quoteVolume", "quoteVolume24h", "quote_volume"):
            if k in t and t[k] is not None:
                v = f(t[k])
                if v > 0:
                    return v

        # direct (raw info)
        info = t.get("info") or {}
        for k in ("quoteVolume", "quoteVolume24h", "quote_volume", "q", "amount"):
            if k in info and info[k] is not None:
                v = f(info[k])
                if v > 0:
                    return v

        # approximate: baseVolume * last
        base_vol = 0.0
        for k in ("baseVolume", "baseVolume24h", "base_volume"):
            if k in t and t[k] is not None:
                base_vol = f(t[k])
                break
        if base_vol <= 0:
            for k in ("baseVolume", "baseVolume24h", "base_volume", "v", "vol"):
                if k in info and info[k] is not None:
                    base_vol = f(info[k])
                    break

        last_val = t.get("last", None)
        if last_val is None:
            last_val = info.get("lastPrice") or info.get("last") or info.get("c")
        last = f(last_val)

        if base_vol > 0 and last > 0:
            return base_vol * last

        return 0.0


    # -------------------------
    # Symbol selection for scalping
    # -------------------------
    def select_symbols_for_scalping(
        self,
        quote: str = "USDT",
        top_n_by_quote_volume: int = 80,
        final_n: int = 10,
        max_spread_bps: float = 6.0,
        min_price_decimals: int = 2,
        min_amount_decimals: int = 2,
        max_min_amount: float | None = None,
        max_min_cost: float | None = None,
        exclude_leveraged_tokens: bool = True,
    ) -> list[str]:
        quote = str(quote).upper()

        tickers = self.fetch_tickers()

        def is_bad_leveraged(sym: str) -> bool:
            up = sym.upper()
            return any(x in up for x in ["3L", "3S", "5L", "5S", "UP/", "DOWN/", "BULL/", "BEAR/"])

        def is_stable_like(base: str) -> bool:
            b = base.upper()
            # Extend this stable or pseudo-stable asset list when needed.
            stable = {
                "USDT", "USDC", "DAI", "TUSD", "USDP", "BUSD",
                "FDUSD", "PYUSD", "USDE", "FRAX",
                "USD1", "USDD",
            }
            return b in stable or b.endswith("USD")


        # 1) Universe
        candidates = []
        for sym, t in tickers.items():
            if not isinstance(sym, str):
                continue
            if f"/{quote}" not in sym:
                continue
            if sym not in self.ex.markets:
                continue
            if exclude_leveraged_tokens and is_bad_leveraged(sym):
                continue

            base = sym.split("/")[0]
            if is_stable_like(base):
                continue


            qv = self._ticker_quote_volume(t)
            candidates.append((sym, qv, t))

        candidates.sort(key=lambda x: x[1], reverse=True)
        candidates = candidates[: max(1, int(top_n_by_quote_volume))]

        def run_filter(
            max_spread: float,
            min_p_dec: int,
            min_a_dec: int,
        ) -> list[str]:
            scored = []
            for sym, qv, t in candidates:
                # spread: ticker bid/ask or order book fallback
                bid, ask = self._ticker_bid_ask(t)
                if not bid or not ask:
                    try:
                        ob = self.fetch_order_book(sym, limit=5)
                        bids = ob.get("bids") or []
                        asks = ob.get("asks") or []
                        if bids and asks:
                            bid = float(bids[0][0])
                            ask = float(asks[0][0])
                    except Exception:
                        bid, ask = None, None

                if not bid or not ask or bid <= 0 or ask <= 0 or ask <= bid:
                    continue

                mid = (bid + ask) / 2.0
                spread_bps = ((ask - bid) / mid) * 10000.0
                if spread_bps > float(max_spread):
                    continue

                # precision/limits
                p_dec, a_dec = self._decimals_from_market(sym)
                if p_dec < int(min_p_dec):
                    continue
                if a_dec < int(min_a_dec):
                    continue

                min_amt, min_cost = self.market_amount_rules(sym)
                if max_min_amount is not None and min_amt > float(max_min_amount):
                    continue
                if max_min_cost is not None and min_cost > float(max_min_cost):
                    continue

                # score: prefer higher volume, tighter spread, finer precision
                score = (qv, -spread_bps, p_dec + a_dec)
                scored.append((score, sym))

            scored.sort(reverse=True, key=lambda x: x[0])
            return [x[1] for x in scored[: max(1, int(final_n))]]

        # 2) strict pass
        out = run_filter(
            max_spread=float(max_spread_bps),
            min_p_dec=int(min_price_decimals),
            min_a_dec=int(min_amount_decimals),
        )
        if out:
            return out

        # 3) relax pass #1: spread loosen, precision loosen
        out = run_filter(
            max_spread=max(20.0, float(max_spread_bps)),
            min_p_dec=0,
            min_a_dec=0,
        )
        if out:
            return out

        # 4) relax pass #2: widen universe
        # (increase top_n temporarily)
        widened = candidates
        if len(widened) < 200:
            # rebuild with wider top_n
            all_candidates = []
            for sym, t in tickers.items():
                if not isinstance(sym, str):
                    continue
                if f"/{quote}" not in sym:
                    continue
                if sym not in self.ex.markets:
                    continue
                if exclude_leveraged_tokens and is_bad_leveraged(sym):
                    continue
                qv = self._ticker_quote_volume(t)
                all_candidates.append((sym, qv, t))
            all_candidates.sort(key=lambda x: x[1], reverse=True)
            widened = all_candidates[:200]

        # temporarily swap candidates for one more run
        old_candidates = candidates
        candidates = widened
        out = run_filter(
            max_spread=50.0,
            min_p_dec=0,
            min_a_dec=0,
        )
        candidates = old_candidates
        if out:
            return out

        # 5) final fallback: majors
        majors = [
            f"BTC/{quote}",
            f"ETH/{quote}",
            f"SOL/{quote}",
            f"XRP/{quote}",
            f"DOGE/{quote}",
            f"BNB/{quote}",
            f"ADA/{quote}",
            f"TRX/{quote}",
            f"AVAX/{quote}",
            f"LINK/{quote}",
        ]
        majors = [s for s in majors if s in self.ex.markets]
        if majors:
            return majors[: max(1, int(final_n))]

        return []
