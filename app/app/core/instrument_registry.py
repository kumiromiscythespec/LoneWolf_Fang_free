# BUILD_ID: 2026-04-23_free_live_api_spot_pair_registry_v1
# BUILD_ID: 2026-04-08_free_bitbank_okx_spot_only_v1
# BUILD_ID: 2026-04-03_free_exchange_symbol_matrix_release_prep_v1
# BUILD_ID: 2026-03-05_btcusdt_first_quote_agnostic_v1
from __future__ import annotations

from dataclasses import dataclass


BUILD_ID = "2026-04-23_free_live_api_spot_pair_registry_v1"


@dataclass(frozen=True)
class InstrumentSpec:
    exchange_id: str
    symbol: str
    market_type: str
    base_ccy: str
    quote_ccy: str
    account_ccy: str
    settlement_ccy: str
    dataset_prefix: str
    visible: bool = True
    experimental: bool = False
    supported_modes: tuple[str, ...] = ("LIVE", "PAPER", "REPLAY")


INSTRUMENTS: tuple[InstrumentSpec, ...] = (
    InstrumentSpec(
        exchange_id="coincheck",
        symbol="BTC/JPY",
        market_type="spot",
        base_ccy="BTC",
        quote_ccy="JPY",
        account_ccy="JPY",
        settlement_ccy="JPY",
        dataset_prefix="BTCJPY",
        visible=True,
    ),
    InstrumentSpec(
        exchange_id="coincheck",
        symbol="ETH/JPY",
        market_type="spot",
        base_ccy="ETH",
        quote_ccy="JPY",
        account_ccy="JPY",
        settlement_ccy="JPY",
        dataset_prefix="ETHJPY",
        visible=False,
        experimental=True,
    ),
    InstrumentSpec(
        exchange_id="bitbank",
        symbol="BTC/JPY",
        market_type="spot",
        base_ccy="BTC",
        quote_ccy="JPY",
        account_ccy="JPY",
        settlement_ccy="JPY",
        dataset_prefix="BTCJPY",
        visible=True,
    ),
    InstrumentSpec(
        exchange_id="bitbank",
        symbol="ETH/JPY",
        market_type="spot",
        base_ccy="ETH",
        quote_ccy="JPY",
        account_ccy="JPY",
        settlement_ccy="JPY",
        dataset_prefix="ETHJPY",
        visible=True,
    ),
    InstrumentSpec(
        exchange_id="bitbank",
        symbol="XRP/JPY",
        market_type="spot",
        base_ccy="XRP",
        quote_ccy="JPY",
        account_ccy="JPY",
        settlement_ccy="JPY",
        dataset_prefix="XRPJPY",
        visible=True,
    ),
    InstrumentSpec(
        exchange_id="bitbank",
        symbol="BNB/JPY",
        market_type="spot",
        base_ccy="BNB",
        quote_ccy="JPY",
        account_ccy="JPY",
        settlement_ccy="JPY",
        dataset_prefix="BNBJPY",
        visible=True,
    ),
    InstrumentSpec(
        exchange_id="mexc",
        symbol="BTC/USDT",
        market_type="spot",
        base_ccy="BTC",
        quote_ccy="USDT",
        account_ccy="USDT",
        settlement_ccy="USDT",
        dataset_prefix="BTCUSDT",
        visible=True,
    ),
    InstrumentSpec(
        exchange_id="mexc",
        symbol="ETH/USDT",
        market_type="spot",
        base_ccy="ETH",
        quote_ccy="USDT",
        account_ccy="USDT",
        settlement_ccy="USDT",
        dataset_prefix="ETHUSDT",
        visible=True,
    ),
    InstrumentSpec(
        exchange_id="mexc",
        symbol="ETH/USDC",
        market_type="spot",
        base_ccy="ETH",
        quote_ccy="USDC",
        account_ccy="USDC",
        settlement_ccy="USDC",
        dataset_prefix="ETHUSDC",
        visible=True,
    ),
    InstrumentSpec(
        exchange_id="mexc",
        symbol="BTC/USDC",
        market_type="spot",
        base_ccy="BTC",
        quote_ccy="USDC",
        account_ccy="USDC",
        settlement_ccy="USDC",
        dataset_prefix="BTCUSDC",
        visible=True,
    ),
    InstrumentSpec(
        exchange_id="mexc",
        symbol="XRP/USDT",
        market_type="spot",
        base_ccy="XRP",
        quote_ccy="USDT",
        account_ccy="USDT",
        settlement_ccy="USDT",
        dataset_prefix="XRPUSDT",
        visible=True,
    ),
    InstrumentSpec(
        exchange_id="mexc",
        symbol="BNB/USDT",
        market_type="spot",
        base_ccy="BNB",
        quote_ccy="USDT",
        account_ccy="USDT",
        settlement_ccy="USDT",
        dataset_prefix="BNBUSDT",
        visible=True,
    ),
    InstrumentSpec(
        exchange_id="mexc",
        symbol="XRP/USDC",
        market_type="spot",
        base_ccy="XRP",
        quote_ccy="USDC",
        account_ccy="USDC",
        settlement_ccy="USDC",
        dataset_prefix="XRPUSDC",
        visible=True,
    ),
    InstrumentSpec(
        exchange_id="mexc",
        symbol="BNB/USDC",
        market_type="spot",
        base_ccy="BNB",
        quote_ccy="USDC",
        account_ccy="USDC",
        settlement_ccy="USDC",
        dataset_prefix="BNBUSDC",
        visible=True,
    ),
    InstrumentSpec(
        exchange_id="binance",
        symbol="ETH/USDT",
        market_type="spot",
        base_ccy="ETH",
        quote_ccy="USDT",
        account_ccy="USDT",
        settlement_ccy="USDT",
        dataset_prefix="ETHUSDT",
        visible=True,
    ),
    InstrumentSpec(
        exchange_id="binance",
        symbol="BTC/USDT",
        market_type="spot",
        base_ccy="BTC",
        quote_ccy="USDT",
        account_ccy="USDT",
        settlement_ccy="USDT",
        dataset_prefix="BTCUSDT",
        visible=True,
    ),
    InstrumentSpec(
        exchange_id="binance",
        symbol="ETH/USDC",
        market_type="spot",
        base_ccy="ETH",
        quote_ccy="USDC",
        account_ccy="USDC",
        settlement_ccy="USDC",
        dataset_prefix="ETHUSDC",
        visible=True,
    ),
    InstrumentSpec(
        exchange_id="binance",
        symbol="BTC/USDC",
        market_type="spot",
        base_ccy="BTC",
        quote_ccy="USDC",
        account_ccy="USDC",
        settlement_ccy="USDC",
        dataset_prefix="BTCUSDC",
        visible=True,
    ),
    InstrumentSpec(
        exchange_id="binance",
        symbol="XRP/USDT",
        market_type="spot",
        base_ccy="XRP",
        quote_ccy="USDT",
        account_ccy="USDT",
        settlement_ccy="USDT",
        dataset_prefix="XRPUSDT",
        visible=True,
    ),
    InstrumentSpec(
        exchange_id="binance",
        symbol="BNB/USDT",
        market_type="spot",
        base_ccy="BNB",
        quote_ccy="USDT",
        account_ccy="USDT",
        settlement_ccy="USDT",
        dataset_prefix="BNBUSDT",
        visible=True,
    ),
    InstrumentSpec(
        exchange_id="binance",
        symbol="XRP/USDC",
        market_type="spot",
        base_ccy="XRP",
        quote_ccy="USDC",
        account_ccy="USDC",
        settlement_ccy="USDC",
        dataset_prefix="XRPUSDC",
        visible=True,
    ),
    InstrumentSpec(
        exchange_id="binance",
        symbol="BNB/USDC",
        market_type="spot",
        base_ccy="BNB",
        quote_ccy="USDC",
        account_ccy="USDC",
        settlement_ccy="USDC",
        dataset_prefix="BNBUSDC",
        visible=True,
    ),
    InstrumentSpec(
        exchange_id="okx",
        symbol="BTC/USDT",
        market_type="spot",
        base_ccy="BTC",
        quote_ccy="USDT",
        account_ccy="USDT",
        settlement_ccy="USDT",
        dataset_prefix="BTCUSDT",
        visible=True,
    ),
    InstrumentSpec(
        exchange_id="okx",
        symbol="ETH/USDT",
        market_type="spot",
        base_ccy="ETH",
        quote_ccy="USDT",
        account_ccy="USDT",
        settlement_ccy="USDT",
        dataset_prefix="ETHUSDT",
        visible=True,
    ),
    InstrumentSpec(
        exchange_id="okx",
        symbol="BTC/USDC",
        market_type="spot",
        base_ccy="BTC",
        quote_ccy="USDC",
        account_ccy="USDC",
        settlement_ccy="USDC",
        dataset_prefix="BTCUSDC",
        visible=True,
    ),
    InstrumentSpec(
        exchange_id="okx",
        symbol="ETH/USDC",
        market_type="spot",
        base_ccy="ETH",
        quote_ccy="USDC",
        account_ccy="USDC",
        settlement_ccy="USDC",
        dataset_prefix="ETHUSDC",
        visible=True,
    ),
    InstrumentSpec(
        exchange_id="okx",
        symbol="XRP/USDT",
        market_type="spot",
        base_ccy="XRP",
        quote_ccy="USDT",
        account_ccy="USDT",
        settlement_ccy="USDT",
        dataset_prefix="XRPUSDT",
        visible=True,
    ),
    InstrumentSpec(
        exchange_id="okx",
        symbol="BNB/USDT",
        market_type="spot",
        base_ccy="BNB",
        quote_ccy="USDT",
        account_ccy="USDT",
        settlement_ccy="USDT",
        dataset_prefix="BNBUSDT",
        visible=True,
    ),
    InstrumentSpec(
        exchange_id="okx",
        symbol="XRP/USDC",
        market_type="spot",
        base_ccy="XRP",
        quote_ccy="USDC",
        account_ccy="USDC",
        settlement_ccy="USDC",
        dataset_prefix="XRPUSDC",
        visible=True,
    ),
    InstrumentSpec(
        exchange_id="okx",
        symbol="BNB/USDC",
        market_type="spot",
        base_ccy="BNB",
        quote_ccy="USDC",
        account_ccy="USDC",
        settlement_ccy="USDC",
        dataset_prefix="BNBUSDC",
        visible=True,
    ),
)


def _normalize_exchange_id(exchange_id: str) -> str:
    return str(exchange_id or "").strip().lower()


def _normalize_symbol(symbol: str) -> str:
    return str(symbol or "").strip().upper()


def list_exchange_ids() -> list[str]:
    out: list[str] = []
    for item in INSTRUMENTS:
        ex = _normalize_exchange_id(item.exchange_id)
        if ex and ex not in out:
            out.append(ex)
    return out


def list_instruments(exchange_id: str = "", *, include_hidden: bool = False) -> list[InstrumentSpec]:
    ex = _normalize_exchange_id(exchange_id)
    out: list[InstrumentSpec] = []
    for item in INSTRUMENTS:
        if ex and _normalize_exchange_id(item.exchange_id) != ex:
            continue
        if (not include_hidden) and (not bool(item.visible)):
            continue
        out.append(item)
    return out


def symbols_for_exchange(exchange_id: str, *, include_hidden: bool = False) -> list[str]:
    out: list[str] = []
    for item in list_instruments(exchange_id, include_hidden=include_hidden):
        sym = str(item.symbol or "").strip()
        if sym and sym not in out:
            out.append(sym)
    return out


def all_symbols(*, include_hidden: bool = False) -> list[str]:
    out: list[str] = []
    for item in list_instruments("", include_hidden=include_hidden):
        sym = str(item.symbol or "").strip()
        if sym and sym not in out:
            out.append(sym)
    return out


def get_instrument(exchange_id: str, symbol: str) -> InstrumentSpec | None:
    ex = _normalize_exchange_id(exchange_id)
    sym = _normalize_symbol(symbol)
    for item in INSTRUMENTS:
        if _normalize_exchange_id(item.exchange_id) != ex:
            continue
        if _normalize_symbol(item.symbol) == sym:
            return item
    return None


def default_symbol_for_exchange(exchange_id: str, fallback: str = "BTC/USDT") -> str:
    symbols = symbols_for_exchange(exchange_id, include_hidden=False)
    if symbols:
        return str(symbols[0])
    fb = str(fallback or "").strip()
    return fb or "BTC/USDT"


def default_quote_for_exchange(exchange_id: str, fallback: str = "USDT") -> str:
    symbols = symbols_for_exchange(exchange_id, include_hidden=False)
    for sym in symbols:
        quote = quote_for_symbol(sym, fallback="")
        if quote:
            return quote
    return str(fallback or "USDT").strip().upper()


def quote_for_symbol(symbol: str, fallback: str = "") -> str:
    raw = str(symbol or "").strip().upper()
    if "/" in raw:
        parts = raw.split("/", 1)
        quote = str(parts[1] if len(parts) > 1 else "").strip().upper()
        if quote:
            return quote
    for sep in ("-", "_", ":"):
        if sep in raw:
            parts = raw.split(sep, 1)
            quote = str(parts[1] if len(parts) > 1 else "").strip().upper()
            if quote:
                return quote
    return str(fallback or "").strip().upper()

