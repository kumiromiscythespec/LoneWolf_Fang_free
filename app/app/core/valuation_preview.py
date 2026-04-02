# BUILD_ID: 2026-04-03_preview_only_valuation_settings_v1
from __future__ import annotations

import math
import os
import sqlite3
from dataclasses import dataclass
from typing import Any

from app.core.instrument_registry import default_symbol_for_exchange, get_instrument, quote_for_symbol
from app.core.paths import ensure_runtime_dirs
from app.core.state_context import build_state_context, list_registered_contexts, normalize_symbol, resolve_state_context_paths


BUILD_ID = "2026-04-03_preview_only_valuation_settings_v1"

# Preview-only helper for the GUI.
# This module must stay read-only with respect to billing:
# - no billing summary send
# - no billing transport or server calls
# - no billing local state mutation
PREVIEW_ONLY_NOTE = "Preview only. Billing is not affected."
DEFAULT_PREVIEW_BALANCE_TEXT = "300000"
SUPPORTED_PREVIEW_MODES = ("Manual JPY", "Manual FX", "Auto")
SUPPORTED_ACCOUNT_CCYS = ("JPY", "USD", "USDT", "USDC")
ENV_BILLING_USDJPY = "LWF_BILLING_USDJPY"
ENV_BILLING_USDJPY_LEGACY = "LWF_BILLING_USDJPY_RATE"
ENV_BILLING_USDTJPY = "LWF_BILLING_USDTJPY"
ENV_BILLING_USDCJPY = "LWF_BILLING_USDCJPY"


@dataclass(frozen=True)
class ValuationPreviewRequest:
    preview_mode: str = "Manual JPY"
    account_ccy: str = "JPY"
    native_balance: Any = DEFAULT_PREVIEW_BALANCE_TEXT
    manual_jpy_balance: Any = DEFAULT_PREVIEW_BALANCE_TEXT
    usdjpy: Any = ""
    usdtjpy: Any = ""
    usdcjpy: Any = ""
    exchange_id: str = ""
    symbol: str = ""
    run_mode: str = ""
    market_type: str = ""
    state_db_path: str = ""


@dataclass(frozen=True)
class ValuationPreviewResult:
    preview_mode: str
    account_ccy: str
    native_balance: float
    raw_balance_jpy: float
    fx_rate_to_jpy: float
    fx_source: str
    estimated_band: str
    note: str = PREVIEW_ONLY_NOTE
    native_balance_source: str = ""
    state_db_path: str = ""


def normalize_preview_mode(raw: Any) -> str:
    value = str(raw or "Manual JPY").strip()
    if value in SUPPORTED_PREVIEW_MODES:
        return value
    return "Manual JPY"


def normalize_account_ccy(raw: Any) -> str:
    value = str(raw or "JPY").strip().upper()
    if value in SUPPORTED_ACCOUNT_CCYS:
        return value
    return "JPY"


def normalize_run_mode(raw: Any) -> str:
    value = str(raw or "PAPER").strip().upper()
    if value in {"LIVE", "PAPER", "REPLAY", "BACKTEST"}:
        return value
    return "PAPER"


def resolve_estimated_band(raw_balance_jpy: float) -> str:
    value = float(raw_balance_jpy)
    if (not math.isfinite(value)) or value < 0.0:
        raise ValueError("preview JPY valuation must be a finite non-negative number.")
    if value > 3_000_000.0:
        return "B05"
    if value >= 1_000_000.0:
        return "B04"
    if value >= 300_000.0:
        return "B03"
    if value >= 100_000.0:
        return "B02"
    return "B01"


def format_jpy_value(value: float) -> str:
    amount = float(value)
    if (not math.isfinite(amount)) or amount < 0.0:
        raise ValueError("preview JPY valuation must be a finite non-negative number.")
    rounded = round(amount)
    if abs(amount - rounded) < 1e-9:
        return f"{rounded:,.0f} JPY"
    return f"{amount:,.2f}".rstrip("0").rstrip(".") + " JPY"


def _parse_non_negative_number(raw: Any, *, label: str) -> float:
    text = str(raw or "").strip()
    if not text:
        raise ValueError(f"{label} is required.")
    try:
        value = float(text)
    except Exception as exc:
        raise ValueError(f"{label} must be a finite non-negative number.") from exc
    if (not math.isfinite(value)) or value < 0.0:
        raise ValueError(f"{label} must be a finite non-negative number.")
    return float(value)


def _parse_positive_rate_or_none(raw: Any) -> float | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        value = float(text)
    except Exception as exc:
        raise ValueError("FX inputs must be positive finite numbers.") from exc
    if (not math.isfinite(value)) or value <= 0.0:
        raise ValueError("FX inputs must be positive finite numbers.")
    return float(value)


def _unique_values(values: list[str]) -> list[str]:
    out: list[str] = []
    for item in values:
        text = str(item or "").strip()
        if text and text not in out:
            out.append(text)
    return out


def _preview_defaults(request: ValuationPreviewRequest) -> dict[str, str]:
    exchange_id = str(request.exchange_id or "").strip().lower() or "coincheck"
    fallback_symbol = "BTC/JPY" if exchange_id == "coincheck" else "BTC/USDT"
    symbol = str(request.symbol or default_symbol_for_exchange(exchange_id, fallback=fallback_symbol)).strip() or fallback_symbol
    instrument = get_instrument(exchange_id, symbol)
    quote_ccy = normalize_account_ccy(quote_for_symbol(symbol, fallback="JPY"))
    return {
        "exchange_id": exchange_id,
        "symbol": symbol,
        "run_mode": normalize_run_mode(request.run_mode),
        "market_type": str(request.market_type or (instrument.market_type if instrument else "spot") or "spot").strip().lower() or "spot",
        "account_ccy": normalize_account_ccy(
            request.account_ccy or (instrument.account_ccy if instrument else quote_ccy) or quote_ccy
        ),
        "settlement_ccy": normalize_account_ccy((instrument.settlement_ccy if instrument else quote_ccy) or quote_ccy),
    }


def _select_registered_context(*, exchange_id: str, run_mode: str, symbol: str) -> dict[str, str] | None:
    state_dir = ensure_runtime_dirs().state_dir
    want_exchange = str(exchange_id or "").strip().lower()
    want_mode = normalize_run_mode(run_mode)
    want_symbol = normalize_symbol(symbol)
    for item in list_registered_contexts(state_dir):
        raw_account_ccy = str(item.get("account_ccy") or item.get("settlement_ccy") or "").strip().upper()
        if raw_account_ccy and raw_account_ccy not in SUPPORTED_ACCOUNT_CCYS:
            continue
        account_ccy = normalize_account_ccy(raw_account_ccy or "JPY")
        item_exchange = str(item.get("exchange_id") or "").strip().lower()
        item_mode = normalize_run_mode(item.get("run_mode") or "")
        item_symbol = normalize_symbol(item.get("symbol") or "")
        db_path = str(item.get("db_path") or "").strip()
        if want_exchange and item_exchange != want_exchange:
            continue
        if want_mode and item_mode != want_mode:
            continue
        if want_symbol and item_symbol != want_symbol:
            continue
        if not db_path or (not os.path.exists(db_path)):
            continue
        return {
            "db_path": db_path,
            "account_ccy": account_ccy,
            "source": "context_registry",
        }
    return None


def _resolve_state_db_path(request: ValuationPreviewRequest) -> tuple[str, dict[str, str]]:
    explicit = str(request.state_db_path or "").strip()
    defaults = _preview_defaults(request)
    if explicit:
        return (
            explicit,
            {
                "exchange_id": defaults["exchange_id"],
                "symbol": defaults["symbol"],
                "run_mode": defaults["run_mode"],
                "account_ccy": defaults["account_ccy"],
                "source": "explicit_state_db",
            },
        )

    selected = _select_registered_context(
        exchange_id=defaults["exchange_id"],
        run_mode=defaults["run_mode"],
        symbol=defaults["symbol"],
    )
    if selected is not None:
        return (
            selected["db_path"],
            {
                "exchange_id": defaults["exchange_id"],
                "symbol": defaults["symbol"],
                "run_mode": defaults["run_mode"],
                "account_ccy": selected["account_ccy"],
                "source": selected["source"],
            },
        )

    quote_ccy = normalize_account_ccy(quote_for_symbol(defaults["symbol"], fallback=defaults["account_ccy"]))
    candidate_ccys = _unique_values(
        [
            defaults["account_ccy"],
            defaults["settlement_ccy"],
            quote_ccy,
            "JPY",
        ]
    )
    state_dir = ensure_runtime_dirs().state_dir
    for account_ccy in candidate_ccys:
        ctx = build_state_context(
            exchange_id=defaults["exchange_id"],
            market_type=defaults["market_type"],
            run_mode=defaults["run_mode"],
            symbol=defaults["symbol"],
            account_ccy=account_ccy,
            settlement_ccy=account_ccy,
        )
        paths = resolve_state_context_paths(state_dir, ctx)
        if os.path.exists(paths.db_path):
            return (
                paths.db_path,
                {
                    "exchange_id": defaults["exchange_id"],
                    "symbol": defaults["symbol"],
                    "run_mode": defaults["run_mode"],
                    "account_ccy": account_ccy,
                    "source": "resolved_context",
                },
            )
        if os.path.exists(paths.legacy_db_path):
            return (
                paths.legacy_db_path,
                {
                    "exchange_id": defaults["exchange_id"],
                    "symbol": defaults["symbol"],
                    "run_mode": defaults["run_mode"],
                    "account_ccy": account_ccy,
                    "source": "legacy_state_db",
                },
            )
    raise FileNotFoundError(
        "Auto preview could not find a runtime state db for the selected exchange / symbol / run mode."
    )


def _read_balance_from_state_db(db_path: str, *, fallback_ccy: str) -> tuple[float, str]:
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Auto preview state db was not found: {db_path}")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        equity_state_row = None
        try:
            equity_state_row = conn.execute(
                "SELECT current_equity, equity_currency FROM equity_state WHERE id = 1"
            ).fetchone()
        except sqlite3.Error:
            equity_state_row = None
        if equity_state_row is not None:
            balance = float(equity_state_row["current_equity"] or 0.0)
            currency = normalize_account_ccy(equity_state_row["equity_currency"] or fallback_ccy)
            if (not math.isfinite(balance)) or balance < 0.0:
                raise ValueError("Auto preview found an invalid current_equity in the runtime state db.")
            return (float(balance), currency)

        legacy_row = None
        try:
            legacy_row = conn.execute(
                "SELECT equity_jpy FROM equity ORDER BY date DESC LIMIT 1"
            ).fetchone()
        except sqlite3.Error:
            legacy_row = None
        if legacy_row is not None:
            balance = float(legacy_row["equity_jpy"] or 0.0)
            if (not math.isfinite(balance)) or balance < 0.0:
                raise ValueError("Auto preview found an invalid equity_jpy in the runtime state db.")
            return (float(balance), "JPY")
    finally:
        conn.close()
    raise RuntimeError("Auto preview could not find a usable equity balance in the runtime state db.")


def _resolve_fx_rate_from_ui(account_ccy: str, request: ValuationPreviewRequest, *, source_prefix: str) -> tuple[float, str]:
    currency = normalize_account_ccy(account_ccy)
    if currency == "JPY":
        return (1.0, f"{source_prefix}_native_jpy")

    if currency == "USD":
        usd = _parse_positive_rate_or_none(request.usdjpy)
        if usd is None:
            raise ValueError("USDJPY is required for USD preview.")
        return (usd, f"{source_prefix}_usdjpy")
    if currency == "USDT":
        usdt = _parse_positive_rate_or_none(request.usdtjpy)
        if usdt is not None:
            return (usdt, f"{source_prefix}_usdtjpy")
        usd = _parse_positive_rate_or_none(request.usdjpy)
        if usd is not None:
            return (usd, f"{source_prefix}_fallback_usdjpy")
        raise ValueError("USDTJPY or USDJPY is required for USDT preview.")
    if currency == "USDC":
        usdc = _parse_positive_rate_or_none(request.usdcjpy)
        if usdc is not None:
            return (usdc, f"{source_prefix}_usdcjpy")
        usd = _parse_positive_rate_or_none(request.usdjpy)
        if usd is not None:
            return (usd, f"{source_prefix}_fallback_usdjpy")
        raise ValueError("USDCJPY or USDJPY is required for USDC preview.")
    raise ValueError(f"Unsupported account currency for preview: {currency}")


def _resolve_fx_rate_from_env(account_ccy: str, *, source_prefix: str) -> tuple[float, str] | None:
    currency = normalize_account_ccy(account_ccy)
    if currency == "JPY":
        return (1.0, f"{source_prefix}_native_jpy")

    env_values = {
        "USD": (
            _parse_positive_rate_or_none(os.getenv(ENV_BILLING_USDJPY)),
            _parse_positive_rate_or_none(os.getenv(ENV_BILLING_USDJPY_LEGACY)),
        ),
        "USDT": (
            _parse_positive_rate_or_none(os.getenv(ENV_BILLING_USDTJPY)),
            _parse_positive_rate_or_none(os.getenv(ENV_BILLING_USDJPY)),
            _parse_positive_rate_or_none(os.getenv(ENV_BILLING_USDJPY_LEGACY)),
        ),
        "USDC": (
            _parse_positive_rate_or_none(os.getenv(ENV_BILLING_USDCJPY)),
            _parse_positive_rate_or_none(os.getenv(ENV_BILLING_USDJPY)),
            _parse_positive_rate_or_none(os.getenv(ENV_BILLING_USDJPY_LEGACY)),
        ),
    }
    rates = env_values.get(currency, ())
    for index, value in enumerate(rates):
        if value is None:
            continue
        if currency == "USD":
            return (value, f"{source_prefix}_usdjpy")
        if currency == "USDT":
            return (value, f"{source_prefix}_{'usdtjpy' if index == 0 else 'fallback_usdjpy'}")
        if currency == "USDC":
            return (value, f"{source_prefix}_{'usdcjpy' if index == 0 else 'fallback_usdjpy'}")
    return None


def _calculate_auto_preview(request: ValuationPreviewRequest) -> ValuationPreviewResult:
    db_path, context = _resolve_state_db_path(request)
    native_balance, detected_ccy = _read_balance_from_state_db(
        db_path,
        fallback_ccy=str(context.get("account_ccy") or request.account_ccy or "JPY"),
    )
    account_ccy = normalize_account_ccy(detected_ccy or context.get("account_ccy") or request.account_ccy)
    try:
        fx_rate, fx_source = _resolve_fx_rate_from_ui(account_ccy, request, source_prefix="auto_ui")
    except ValueError:
        env_rate = _resolve_fx_rate_from_env(account_ccy, source_prefix="auto_env")
        if env_rate is None:
            raise ValueError(
                f"Auto preview requires a runtime FX source for {account_ccy}. "
                "Enter FX Inputs or set the existing billing FX environment variables."
            )
        fx_rate, fx_source = env_rate
    raw_balance_jpy = float(native_balance) * float(fx_rate)
    if (not math.isfinite(raw_balance_jpy)) or raw_balance_jpy < 0.0:
        raise ValueError("Auto preview produced an invalid JPY valuation.")
    return ValuationPreviewResult(
        preview_mode="Auto",
        account_ccy=account_ccy,
        native_balance=float(native_balance),
        raw_balance_jpy=float(raw_balance_jpy),
        fx_rate_to_jpy=float(fx_rate),
        fx_source=str(fx_source),
        estimated_band=resolve_estimated_band(raw_balance_jpy),
        native_balance_source=str(context.get("source") or "state_db"),
        state_db_path=db_path,
    )


def calculate_valuation_preview(request: ValuationPreviewRequest) -> ValuationPreviewResult:
    mode = normalize_preview_mode(request.preview_mode)
    account_ccy = normalize_account_ccy(request.account_ccy)
    if mode == "Manual JPY":
        raw_balance_jpy = _parse_non_negative_number(request.manual_jpy_balance, label="Manual JPY Balance")
        return ValuationPreviewResult(
            preview_mode=mode,
            account_ccy="JPY",
            native_balance=float(raw_balance_jpy),
            raw_balance_jpy=float(raw_balance_jpy),
            fx_rate_to_jpy=1.0,
            fx_source="manual_jpy",
            estimated_band=resolve_estimated_band(raw_balance_jpy),
            native_balance_source="manual_jpy_balance",
        )

    if mode == "Manual FX":
        native_balance = _parse_non_negative_number(request.native_balance, label="Native Balance")
        fx_rate, fx_source = _resolve_fx_rate_from_ui(account_ccy, request, source_prefix="manual_fx")
        raw_balance_jpy = float(native_balance) * float(fx_rate)
        return ValuationPreviewResult(
            preview_mode=mode,
            account_ccy=account_ccy,
            native_balance=float(native_balance),
            raw_balance_jpy=float(raw_balance_jpy),
            fx_rate_to_jpy=float(fx_rate),
            fx_source=str(fx_source),
            estimated_band=resolve_estimated_band(raw_balance_jpy),
            native_balance_source="manual_native_balance",
        )

    return _calculate_auto_preview(request)
