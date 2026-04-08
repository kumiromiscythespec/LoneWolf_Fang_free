# BUILD_ID: 2026-04-08_free_bitbank_okx_spot_only_v1
# BUILD_ID: 2026-03-05_btcusdt_first_quote_agnostic_v1
from __future__ import annotations

from dataclasses import dataclass

from app.core.instrument_registry import all_symbols as registry_all_symbols
from app.core.instrument_registry import default_symbol_for_exchange
from app.core.instrument_registry import symbols_for_exchange as registry_symbols_for_exchange

BUILD_ID = "2026-04-08_free_bitbank_okx_spot_only_v1"


@dataclass(frozen=True)
class ExchangeOption:
    id: str
    label: str
    key_env: str
    secret_env: str
    passphrase_env: str = ""


EXCHANGES: tuple[ExchangeOption, ...] = (
    ExchangeOption(
        id="coincheck",
        label="Coincheck",
        key_env="COINCHECK_API_KEY",
        secret_env="COINCHECK_API_SECRET",
    ),
    ExchangeOption(
        id="mexc",
        label="MEXC",
        key_env="MEXC_API_KEY",
        secret_env="MEXC_API_SECRET",
    ),
    ExchangeOption(
        id="binance",
        label="Binance",
        key_env="BINANCE_API_KEY",
        secret_env="BINANCE_API_SECRET",
    ),
    ExchangeOption(
        id="bitbank",
        label="bitbank",
        key_env="BITBANK_API_KEY",
        secret_env="BITBANK_API_SECRET",
    ),
    ExchangeOption(
        id="okx",
        label="OKX",
        key_env="OKX_API_KEY",
        secret_env="OKX_API_SECRET",
        passphrase_env="OKX_API_PASSPHRASE",
    ),
)


def normalize_exchange_id(raw: str, default: str = "coincheck") -> str:
    value = str(raw or "").strip().lower()
    for item in EXCHANGES:
        if item.id == value:
            return item.id
    return str(default or EXCHANGES[0].id)


def get_exchange_option(exchange_id: str) -> ExchangeOption:
    normalized = normalize_exchange_id(exchange_id)
    for item in EXCHANGES:
        if item.id == normalized:
            return item
    return EXCHANGES[0]


def symbols_for_exchange(exchange_id: str) -> list[str]:
    symbols = list(registry_symbols_for_exchange(exchange_id, include_hidden=False))
    if symbols:
        return symbols
    fallback = default_symbol_for_exchange(exchange_id, fallback="")
    return [fallback] if fallback else []


def all_symbols(*, include_hidden: bool = False) -> list[str]:
    return list(registry_all_symbols(include_hidden=bool(include_hidden)))


def normalize_symbol_for_exchange(exchange_id: str, symbol: str) -> str:
    candidates = symbols_for_exchange(exchange_id)
    value = str(symbol or "").strip()
    if value in candidates:
        return value
    return str(candidates[0] if candidates else value)
