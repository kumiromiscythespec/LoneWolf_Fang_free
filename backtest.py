# BUILD_ID: 2026-03-29_free_final_polish_v1
# BUILD_ID: 2026-03-21_backtest_final_residual_comment_cleanup_v1
# BUILD_ID: 2026-03-20_backtest_ethusdc_size_sizing_diag_v1
# BUILD_ID: 2026-03-20_backtest_ethusdc_open_cost_diag_v1
# BUILD_ID: 2026-03-20_backtest_market_meta_synthetic_v1
# BUILD_ID: 2026-03-20_backtest_cli_dataset_bridge_v1
# BUILD_ID: 2026-03-20_backtest_cli_symbol_preset_bridge_v1
# BUILD_ID: 2026-03-20_backtest_cross_root_legacy_warn_v1
# BUILD_ID: 2026-03-19_market_data_root_stage1_patchfix_v2
# BUILD_ID: 2026-03-09_backtest_dataset_override_guard_v1

# BUILD_ID: 2026-03-09_backtest_entry_exec_signal_bar_open_v1
# BUILD_ID: 2026-03-14_precomputed_multiyear_diag_v1
# BUILD_ID: 2026-03-19_backtest_trail_set_manage_ts_parity_v1
# BUILD_ID: 2026-03-19_backtest_disable_posthoc_open_close_diff_trace_v1
# BUILD_ID: 2026-03-19_backtest_trail_atr_trace_diag_v1
# BUILD_ID: 2026-03-19_backtest_trail_use_atr_now_v1
# BUILD_ID: 2026-03-19_backtest_qty_precision_fallback_v1
# BUILD_ID: 2026-03-19_backtest_qty_precision_align_v1
from __future__ import annotations

import argparse
import json
import logging, csv, argparse, heapq, glob, os, re, sys
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from indicators import ema, rsi, atr, adx
from exchange import ExchangeClient
from risk import calc_qty_from_risk
from strategy import (
    detect_regime_1h,
    signal_entry,
    signal_range_entry,
    detect_regime_1h_precomputed,
    signal_entry_precomputed,
    signal_range_entry_precomputed,
    exit_signal_entry_precomputed,
)
def _bridge_backtest_dataset_from_cli(argv: list[str]) -> None:
    if str(os.getenv("BACKTEST_DATASET") or "").strip():
        return

    tokens = list(argv or [])
    for idx, token in enumerate(tokens):
        tok = str(token or "").strip()
        year = ""
        if tok == "--year" and idx + 1 < len(tokens):
            year = str(tokens[idx + 1] or "").strip()
        elif tok.startswith("--year="):
            year = str(tok.split("=", 1)[1] or "").strip()
        if re.fullmatch(r"\d{4}", year):
            os.environ["BACKTEST_DATASET"] = year
            return

    for idx, token in enumerate(tokens):
        tok = str(token or "").strip()
        since = ""
        if tok == "--since" and idx + 1 < len(tokens):
            since = str(tokens[idx + 1] or "").strip()
        elif tok.startswith("--since="):
            since = str(tok.split("=", 1)[1] or "").strip()
        m = re.fullmatch(r"(\d{4})-\d{2}-\d{2}", since)
        if m:
            os.environ["BACKTEST_DATASET"] = str(m.group(1))
            return


_bridge_backtest_dataset_from_cli(sys.argv[1:])

if os.getenv("BT_YEAR") and not os.getenv("BACKTEST_DATASET"):
    os.environ["BACKTEST_DATASET"] = os.getenv("BT_YEAR")

try:
    # When Phase A/B is owned by runner, prefer the stateful path (same update point as live).
    from strategy import signal_entry_stateful  # type: ignore
except Exception:
    signal_entry_stateful = None  # type: ignore

from collections import Counter as _Counter


def _bridge_lwf_symbol_preset_from_cli(argv: list[str]) -> None:
    if str(os.getenv("LWF_SYMBOL_PRESET") or "").strip():
        return
    symbol_map = {
        "BTC/JPY": "BTC/JPY",
        "ETH/USDC": "ETH/USDC",
    }
    for idx, token in enumerate(list(argv or [])):
        if str(token) != "--symbol":
            continue
        if idx + 1 >= len(argv):
            return
        preset_symbol = symbol_map.get(str(argv[idx + 1] or "").strip().upper())
        if preset_symbol:
            os.environ["LWF_SYMBOL_PRESET"] = preset_symbol
        return


_bridge_lwf_symbol_preset_from_cli(sys.argv[1:])

import numpy as np
import config as C
import runner as R
import math
from trace_bar_policy import attach_bar_snapshot as _trace_attach_bar_snapshot
from app.core.dataset import (
    DatasetResolutionError,
    build_missing_dataset_message,
    infer_year_from_ms,
    normalize_runtime_symbol,
    normalize_timestamp_to_ms,
    resolve_dataset,
    resolve_dataset_override_symbol,
    symbol_to_prefix,
)
from app.core.fees import resolve_paper_fees
from app.core.market_meta import MarketMeta, load_cached_meta, market_meta_cache_path, maybe_refresh_market_meta, resolve_quote_ccy
from app.core.paths import ensure_runtime_dirs
from app.core.export_paths import (
    build_run_export_dir,
    is_legacy_exports_path,
    resolve_run_id,
    write_last_run_json,
)

BUILD_ID = "2026-03-29_free_final_polish_v1"

OPEN_COST_DIAG_LIMIT_DEFAULT = 8
_CURRENT_EXPORT_DIR = ""
_CURRENT_RUN_ID = ""
_CURRENT_EXPORT_SYMBOL = ""


def _resolve_backtest_symbols(raw_symbols: str) -> tuple[list[str], str]:
    exchange_id = str(getattr(C, "EXCHANGE_ID", "") or "").strip().lower()
    cfg_symbols_seed = list(getattr(C, "SYMBOLS", []) or [])
    fallback_seed = str(
        getattr(C, "BACKTEST_CSV_SYMBOL", "")
        or (cfg_symbols_seed[0] if cfg_symbols_seed else "")
        or "BTC/USDT"
    ).strip()
    fallback_symbol = normalize_runtime_symbol(
        fallback_seed,
        exchange_id=exchange_id,
        fallback=fallback_seed or "BTC/USDT",
    )

    if str(raw_symbols or "").strip():
        cli_symbols: list[str] = []
        for tok in str(raw_symbols).split(","):
            sym = normalize_runtime_symbol(str(tok or "").strip(), exchange_id=exchange_id, fallback="")
            if sym and sym not in cli_symbols:
                cli_symbols.append(sym)
        if cli_symbols:
            return (cli_symbols, "cli")

    dataset_symbol, dataset_source = resolve_dataset_override_symbol(
        exchange_id=exchange_id,
        fallback=fallback_symbol,
    )
    if dataset_symbol:
        return ([dataset_symbol], str(dataset_source))

    cfg_symbols: list[str] = []
    for tok in cfg_symbols_seed:
        sym = normalize_runtime_symbol(str(tok or "").strip(), exchange_id=exchange_id, fallback="")
        if sym and sym not in cfg_symbols:
            cfg_symbols.append(sym)
    if cfg_symbols:
        return (cfg_symbols, "config.SYMBOLS")

    if fallback_symbol:
        source = "config.BACKTEST_CSV_SYMBOL" if str(getattr(C, "BACKTEST_CSV_SYMBOL", "") or "").strip() else "default"
        return ([fallback_symbol], source)
    return ([], "default")


def _activate_export_context(*, run_id: str, symbol: str, mode: str) -> str:
    global _CURRENT_EXPORT_DIR, _CURRENT_RUN_ID, _CURRENT_EXPORT_SYMBOL
    paths = ensure_runtime_dirs()
    rid = str(resolve_run_id(run_id, env_key="LWF_RUN_ID"))
    sym = str(symbol or "ETH/USDT")
    export_dir = build_run_export_dir(paths.exports_dir, run_id=rid, symbol=sym)
    _CURRENT_EXPORT_DIR = str(export_dir)
    _CURRENT_RUN_ID = str(rid)
    _CURRENT_EXPORT_SYMBOL = str(sym)
    os.environ["LWF_RUN_ID"] = str(rid)
    os.environ["LWF_EXPORT_DIR"] = str(export_dir)
    try:
        diff_dir_cfg = str(getattr(C, "DIFF_TRACE_DIR", "") or "").strip()
        if is_legacy_exports_path(diff_dir_cfg):
            setattr(C, "DIFF_TRACE_DIR", str(export_dir))
    except Exception:
        pass
    logger.info("[results] export_dir=%s run_id=%s symbol=%s mode=%s", str(export_dir), str(rid), str(sym), str(mode))
    return str(export_dir)


def _current_export_dir() -> str:
    if str(_CURRENT_EXPORT_DIR or "").strip():
        return str(_CURRENT_EXPORT_DIR)
    return _activate_export_context(run_id=str(resolve_run_id("", env_key="LWF_RUN_ID")), symbol="ETH/USDT", mode="BACKTEST")


def _export_path(*parts: str) -> str:
    base = Path(_current_export_dir())
    out = base.joinpath(*[str(p) for p in parts if str(p)])
    out.parent.mkdir(parents=True, exist_ok=True)
    return str(out)


def _write_last_run_reference(*, mode: str, replay_report: str = "", trade_log: str = "", extra: dict[str, Any] | None = None) -> str:
    paths = ensure_runtime_dirs()
    out = write_last_run_json(
        paths.exports_dir,
        run_id=str(_CURRENT_RUN_ID or resolve_run_id("", env_key="LWF_RUN_ID")),
        symbol=str(_CURRENT_EXPORT_SYMBOL or "ETH/USDT"),
        mode=str(mode),
        export_dir=str(_current_export_dir()),
        replay_report=str(replay_report or ""),
        trade_log=str(trade_log or ""),
        extra=extra,
    )
    return str(out)

# ======================================================================================
# Logging
# ======================================================================================

def build_arg_parser():
    p = argparse.ArgumentParser(add_help=False)

    # Range entry diagnostics stay off unless explicitly enabled.

    # Enable diagnostic logs for range-entry decisions.
    p.add_argument(
        "--debug-range-entry",
        action="store_true",
        help="Enable RANGE_ENTRY_DIAG logs (range entry diagnostics). Default: off",
    )

    # Limit how many RANGE_ENTRY_DIAG lines are emitted per run.
    p.add_argument(
        "--range-entry-diag-limit",
        type=int,
        default=20,
        help="Max number of RANGE_ENTRY_DIAG logs to print (only when --debug-range-entry is on).",
    )
    p.add_argument(
        "--perf",
        action="store_true",
        help="Enable lightweight precomputed diagnostics logging.",
    )

    return p

# --- Runner-compatible KV for pullback Phase A/B (backtest-only minimal emulation) ---
def _kv_key_pullback_ab(symbol: str) -> str:
    # Keep identical to runner.py
    return f"state.pullback_ab:{symbol}"


def _kv_get_pullback_ab(kv: dict, symbol: str) -> dict:
    # Keep identical defaults to runner.py
    k = _kv_key_pullback_ab(symbol)
    v = kv.get(k)
    if not isinstance(v, dict):
        v = {"phase": "A", "since_ms": None, "last_reason": None}
        kv[k] = v
    # ensure keys exist
    if "phase" not in v:
        v["phase"] = "A"
    if "since_ms" not in v:
        v["since_ms"] = None
    if "last_reason" not in v:
        v["last_reason"] = None
    return v


def _kv_set_pullback_ab(kv: dict, symbol: str, pb_state: dict) -> None:
    k = _kv_key_pullback_ab(symbol)
    if not isinstance(pb_state, dict):
        return
    kv[k] = pb_state

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

logger = logging.getLogger("backtest")
# --- CONFIG CHECK (debug) ---
# CFG CHECK logging can be silenced for long replays / CI:
# - env BACKTEST_CFG_CHECK=0 (or RUNNER_CFG_CHECK=0) disables these logs
# - config.py: LOG_CFG_CHECK can also override (if present)
def _cfg_check_enabled() -> bool:
    try:
        env = os.getenv("BACKTEST_CFG_CHECK", os.getenv("RUNNER_CFG_CHECK", "1"))
        env = str(env).strip().lower()
        if env in ("0", "false", "no", "off"):
            return False
    except Exception:
        pass
    cfg = getattr(C, "LOG_CFG_CHECK", None)
    if cfg is None:
        return True
    try:
        return bool(float(cfg))
    except Exception:
        return bool(cfg)


def _resolve_backtest_spread_bps() -> tuple[float, bool]:
    try:
        base = float(getattr(C, "BACKTEST_SPREAD_BPS", getattr(C, "SLIPPAGE_BPS", 0.0)) or 0.0)
    except Exception:
        try:
            base = float(getattr(C, "SLIPPAGE_BPS", 0.0) or 0.0)
        except Exception:
            base = 0.0

    env_raw = os.getenv("BACKTEST_SPREAD_BPS")
    if env_raw is None or str(env_raw).strip() == "":
        return base, False
    try:
        return float(str(env_raw).strip()), True
    except Exception:
        return base, False


def _lookup_pair_registry_meta(exchange_id: str, symbol: str) -> dict[str, Any]:
    ex = str(exchange_id or "").strip().lower()
    sym = normalize_runtime_symbol(str(symbol or "").strip(), exchange_id=ex, fallback=str(symbol or "").strip())
    registry = getattr(C, "PAIR_REGISTRY", {}) or {}
    if not isinstance(registry, dict):
        return {}
    direct = registry.get(f"{ex}:{sym}")
    if isinstance(direct, dict):
        return direct
    for item in registry.values():
        if not isinstance(item, dict):
            continue
        item_ex = str(item.get("exchange_id") or "").strip().lower()
        item_symbol = normalize_runtime_symbol(
            str(item.get("symbol") or "").strip(),
            exchange_id=item_ex or ex,
            fallback="",
        )
        if item_ex == ex and item_symbol == sym:
            return item
    return {}


def _build_synthetic_backtest_market_meta(exchange_id: str, symbol: str) -> tuple[MarketMeta | None, str]:
    ex = str(exchange_id or "").strip().lower()
    sym = normalize_runtime_symbol(str(symbol or "").strip(), exchange_id=ex, fallback=str(symbol or "").strip())
    pair_meta = _lookup_pair_registry_meta(ex, sym)
    resolved_symbol = str((pair_meta or {}).get("symbol") or sym).strip() or sym
    quote_ccy = str((pair_meta or {}).get("quote_ccy") or resolve_quote_ccy(resolved_symbol) or "").strip().upper()
    if (not pair_meta) and (not quote_ccy):
        return None, "cache_miss"

    maker_fee_rate, taker_fee_rate = resolve_paper_fees(ex)
    spread_bps, _spread_env_override = _resolve_backtest_spread_bps()
    min_qty = 0.0
    min_cost = 0.0
    tick_size = 0.0
    amount_precision = 0
    rules_applied = False

    if ex == "mexc" and quote_ccy:
        min_cost_map = dict(getattr(C, "MEXC_MIN_COST_BY_QUOTE", {}) or {})
        try:
            min_cost = float(min_cost_map.get(str(quote_ccy), 0.0) or 0.0)
        except Exception:
            min_cost = 0.0

    try:
        rules = ExchangeClient(ex).get_market_rules(resolved_symbol)
        min_qty = float(rules.get("min_qty") or 0.0)
        min_cost = max(float(min_cost), float(rules.get("min_cost") or 0.0))
        tick_size = float(rules.get("tick_size") or 0.0)
        amount_precision = int(rules.get("amount_precision") or 0)
        rules_applied = True
    except Exception:
        pass

    return (
        MarketMeta(
            exchange_id=ex,
            symbol=resolved_symbol,
            quote_ccy=quote_ccy,
            maker_fee_rate=float(maker_fee_rate),
            taker_fee_rate=float(taker_fee_rate),
            spread_bps=float(spread_bps),
            min_qty=float(min_qty),
            min_cost=float(min_cost),
            tick_size=float(tick_size),
            amount_precision=int(amount_precision),
            updated_at=float(datetime.now(timezone.utc).timestamp()),
        ),
        ("synthetic:pair_registry+market_rules" if rules_applied else "synthetic:pair_registry"),
    )


def _load_backtest_market_meta(exchange_id: str, symbol: str) -> tuple[object | None, str, str]:
    paths = ensure_runtime_dirs()
    cache_path = market_meta_cache_path(paths.state_dir, exchange_id, symbol)
    refresh = str(os.getenv("MARKET_META_REFRESH", "") or "").strip().lower() in ("1", "true", "yes", "on")
    if refresh:
        try:
            ttl_raw = float(os.getenv("MARKET_META_CACHE_TTL_SEC", "3600") or 3600.0)
        except Exception:
            ttl_raw = 3600.0
        try:
            ex = ExchangeClient(exchange_id)
            meta, source, cache_path = maybe_refresh_market_meta(
                ex,
                exchange_id=exchange_id,
                symbol=symbol,
                state_dir=paths.state_dir,
                cache_ttl_sec=max(60.0, float(ttl_raw)),
                allow_refresh=True,
            )
            return meta, source, cache_path
        except Exception as e:
            logger.warning("[MARKET_META] backtest refresh failed symbol=%s reason=%s", symbol, e)
    meta = load_cached_meta(cache_path)
    if meta is not None:
        return meta, "cache", cache_path
    synthetic_meta, synthetic_source = _build_synthetic_backtest_market_meta(exchange_id, symbol)
    if synthetic_meta is not None:
        return synthetic_meta, synthetic_source, cache_path
    return None, "cache_miss", cache_path


def _market_meta_source_kind(source: object) -> str:
    src = str(source or "").strip().lower()
    if src.startswith("synthetic"):
        return "synthetic"
    if src.startswith("refresh") or src.startswith("refreshed"):
        return "refresh"
    if "cache" in src:
        return "cache"
    return src or "unknown"


def _is_ethusdc_symbol(symbol: object, *, exchange_id: str) -> bool:
    raw = str(symbol or "").strip()
    if not raw:
        return False
    try:
        norm = normalize_runtime_symbol(raw, exchange_id=str(exchange_id or "").strip().lower(), fallback=raw)
    except Exception:
        norm = raw
    return str(norm or "").strip().upper() == "ETH/USDC"


if _cfg_check_enabled():
    # --- CONFIG CHECK (debug) ---
    log.info(
        "CFG CHECK: file=%s MIN_TP_BPS=%s MIN_RR_AFTER_ADJUST_TREND_LONG=%s",
        getattr(C, "__file__", None),
        getattr(C, "MIN_TP_BPS", None),
        getattr(C, "MIN_RR_AFTER_ADJUST_TREND_LONG", None),
    )
    log.info(
        "CFG TRADE_RANGE=%s TRADE_TREND=%s RANGE_RSI_MAX=%s RANGE_NEAR_LOW_ATR=%s",
        getattr(C, "TRADE_RANGE", None),
        getattr(C, "TRADE_TREND", None),
        getattr(C, "RANGE_RSI_MAX", None),
        getattr(C, "RANGE_NEAR_LOW_ATR", None),
    )

_cfg_spread_bps, _cfg_spread_env_override = _resolve_backtest_spread_bps()
log.info(
    "CFG CHECK "
    f"TRADE_ONLY_TREND={getattr(C,'TRADE_ONLY_TREND',None)} "
    f"RANGE_ATR_TP_MULT={getattr(C,'RANGE_ATR_TP_MULT',None)} "
    f"RANGE_ATR_SL_MULT={getattr(C,'RANGE_ATR_SL_MULT',None)} "
    f"PAPER_FEE_RATE_MAKER={getattr(C,'PAPER_FEE_RATE_MAKER',None)} "
    f"PAPER_FEE_RATE_TAKER={getattr(C,'PAPER_FEE_RATE_TAKER',None)} "
    f"SLIPPAGE_BPS={getattr(C,'SLIPPAGE_BPS',None)} "
    f"BACKTEST_SPREAD_BPS={_cfg_spread_bps}"
    f"{' (env override applied)' if _cfg_spread_env_override else ''}"
)

# (A-1) Debug: ensure range exit knobs are actually loaded/used.
def _cfg_bool(name: str, default: bool = False) -> bool:
    try:
        return bool(getattr(C, name))
    except Exception:
        return bool(default)
log.info(
    "CFG RANGE EXIT: EMA21_BREAK=%s EMA21_BUF_BPS=%.2f EARLY_LOSS_ATR=%.2f EMA9_CROSS=%s EMA9_MIN_LOSS_BPS=%.1f",
    bool(getattr(C, "RANGE_EXIT_ON_EMA21_BREAK", False)),
    float(getattr(C, "RANGE_EXIT_EMA21_BUFFER_BPS", 0.0) or 0.0),
    float(getattr(C, "RANGE_EARLY_EXIT_LOSS_ATR_MULT", 0.0) or 0.0),
    bool(getattr(C, "RANGE_EXIT_ON_EMA9_CROSS", False)),
    float(getattr(C, "RANGE_EMA9_CROSS_EXIT_MIN_LOSS_BPS", 0.0) or 0.0),
)

# diff-trace cfg normalization (must match verify_diff.py key expectations)
_DIFF_TRACE_CFG_KEYS = [
    "MIN_RR_AFTER_ADJUST_TREND_LONG",
    "MIN_TP_BPS",
    "TRADE_ONLY_TREND",
    "TRADE_RANGE",
    "TRADE_TREND",
    "TREND_ENTRY_MODE",
]


def _diff_trace_norm_cfg(cfg: Any | None) -> dict:
    """Normalize cfg dict for diff-trace key matching (stable subset only)."""
    base = cfg if isinstance(cfg, dict) else {}
    out: dict[str, Any] = {}
    for k in _DIFF_TRACE_CFG_KEYS:
        if k in base:
            out[k] = base.get(k)
        elif hasattr(C, k):
            out[k] = getattr(C, k)
    return out




def _rolling_min(a: list[float], window: int) -> list[float]:
    n = len(a)
    out = [float("nan")] * n
    if window <= 0:
        return out
    for i in range(n):
        if i + 1 < window:
            continue
        m = a[i - window + 1]
        for k in range(i - window + 2, i + 1):
            v = a[k]
            if v < m:
                m = v
        out[i] = float(m)
    return out

def _rolling_max(a: list[float], window: int) -> list[float]:
    n = len(a)
    out = [float("nan")] * n
    if window <= 0:
        return out
    for i in range(n):
        if i + 1 < window:
            continue
        m = a[i - window + 1]
        for k in range(i - window + 2, i + 1):
            v = a[k]
            if v > m:
                m = v
        out[i] = float(m)
    return out


def iso_utc(ms: int) -> str:
    """
    Safe ms->ISO UTC.
    - Accepts seconds/ms/us/ns-ish and normalizes.
    - Returns empty string if timestamp is invalid on this platform.
    """
    try:
        # ms can be weird (nan cast, ns/us, etc.)
        if ms is None:
            return ""

        ms = int(ms)

        # Heuristics to normalize units:
        # seconds (e.g., 1700000000) -> ms
        if 0 < ms < 100_000_000_000:  # < ~1973-03 in ms, but common for seconds range
            ms *= 1000

        # microseconds (e.g., 1700000000000000) -> ms
        if ms > 10_000_000_000_000:  # > 10^13
            ms //= 1000

        # nan->int often becomes huge negative; also filter absurd ranges
        # Windows datetime supports roughly 1970..far future, but guard broadly.
        if ms < 0 or ms > 253402300799000:  # up to 9999-12-31T23:59:59Z in ms
            return ""

        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()

    except (OSError, OverflowError, ValueError):
        return ""



def _parse_yyyy_mm_dd_to_ms(s: str, end_of_day_exclusive: bool = False) -> int:
    try:
        return int(R._parse_ymd_to_ms_utc(s, end_of_day_exclusive=bool(end_of_day_exclusive)))
    except Exception:
        dt = datetime.strptime(str(s).strip(), "%Y-%m-%d").replace(tzinfo=timezone.utc)
        if end_of_day_exclusive:
            dt = dt + timedelta(days=1)
        return int(dt.timestamp() * 1000)

def _apply_year_preset(year: int) -> None:
    """Apply year-based config overrides in backtest.py only.

    Example:
      python backtest.py --year 2023

    This switches CSV dirs/globs + SINCE_MS without touching config.py.

    Note: BACKTEST_CSV_GLOB_* are set to year-agnostic patterns to avoid
    accidentally filtering to a single year when using an "all" dataset.
    """
    dataset = str(int(year))

    # dataset tag (for logging / meta)
    setattr(C, "BACKTEST_DATASET", dataset)
    # Force dataset resolver to prioritize CLI year over runtime settings file.
    os.environ["BACKTEST_DATASET_YEAR"] = dataset

    # CSV dirs
    setattr(C, "BACKTEST_CSV_DIR_5M", f"binance_ethusdt_5m_{dataset}")
    setattr(C, "BACKTEST_CSV_DIR_1H", f"binance_ethusdt_1h_{dataset}")

    # CSV globs (year-agnostic)
    setattr(C, "BACKTEST_CSV_GLOB_5M", "ETHUSDT-5m-*.csv")
    setattr(C, "BACKTEST_CSV_GLOB_1H", "ETHUSDT-1h-*.csv")

    # since_ms (UTC year start)
    setattr(C, "SINCE_MS", _parse_yyyy_mm_dd_to_ms(f"{dataset}-01-01"))


def _write_yearly_report_from_csv(
    *,
    out_path: str,
    build_id: str,
    preset_name: str,
    year: int | None,
    mode: str,
    overall: Dict[str, Any] | None,
    include_yearly: bool,
    include_monthly: bool,
    risk_free_rate: float,
) -> bool:
    eq_path = _export_path("equity_curve.csv")
    tr_path = _export_path("trades.csv")
    if (not os.path.exists(eq_path)) or (not os.path.exists(tr_path)):
        legacy_eq_path = os.path.join("exports", "equity_curve.csv")
        legacy_tr_path = os.path.join("exports", "trades.csv")
        if os.path.exists(legacy_eq_path) and os.path.exists(legacy_tr_path):
            eq_path = legacy_eq_path
            tr_path = legacy_tr_path
    if (not os.path.exists(eq_path)) or (not os.path.exists(tr_path)):
        logger.info("[REPORT] skipped (missing exports/equity_curve.csv or trades.csv)")
        return False

    def _norm_ts_ms(v: Any) -> int:
        try:
            x = int(float(v))
        except Exception:
            return 0
        if 0 < x < 100_000_000_000:
            x *= 1000
        if x >= 100_000_000_000_000:
            x //= 1000
        return int(x)

    def _year_from_ts(ts_ms: int) -> str:
        try:
            return str(datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc).year)
        except Exception:
            return "unknown"

    def _month_from_ts(ts_ms: int) -> str:
        try:
            dt = datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc)
            return f"{dt.year:04d}-{dt.month:02d}"
        except Exception:
            return "unknown"

    eq_by_year: Dict[str, List[float]] = {}
    eq_by_month: Dict[str, List[float]] = {}
    eq_all: List[float] = []
    mtm_by_year: Dict[str, List[float]] = {}
    mtm_by_month: Dict[str, List[float]] = {}
    mtm_all: List[float] = []
    with open(eq_path, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                eq = float(row.get("equity", "") or 0.0)
            except Exception:
                continue
            try:
                eq_mtm = float(row.get("mtm_equity", "") or eq)
            except Exception:
                eq_mtm = float(eq)
            ts_ms = _norm_ts_ms(row.get("ts"))
            y = _year_from_ts(ts_ms)
            m = _month_from_ts(ts_ms)
            eq_by_year.setdefault(y, []).append(eq)
            eq_by_month.setdefault(m, []).append(eq)
            eq_all.append(eq)
            mtm_by_year.setdefault(y, []).append(float(eq_mtm))
            mtm_by_month.setdefault(m, []).append(float(eq_mtm))
            mtm_all.append(float(eq_mtm))

    tr_by_year: Dict[str, Dict[str, float]] = {}
    tr_by_month: Dict[str, Dict[str, float]] = {}
    with open(tr_path, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            ts_ms = _norm_ts_ms(row.get("ts"))
            y = _year_from_ts(ts_ms)
            m = _month_from_ts(ts_ms)
            try:
                net = float(row.get("net", "") or 0.0)
            except Exception:
                net = 0.0
            agg = tr_by_year.setdefault(y, {"trades": 0.0, "wins": 0.0, "net_total": 0.0})
            agg["trades"] += 1.0
            if net > 0.0:
                agg["wins"] += 1.0
            agg["net_total"] += float(net)
            agg_m = tr_by_month.setdefault(m, {"trades": 0.0, "wins": 0.0, "net_total": 0.0})
            agg_m["trades"] += 1.0
            if net > 0.0:
                agg_m["wins"] += 1.0
            agg_m["net_total"] += float(net)

    def _max_dd_from_equity(eq_list: List[float]) -> float:
        if not eq_list:
            return 0.0
        peak = float(eq_list[0])
        max_dd = 0.0
        for eq in eq_list:
            v = float(eq)
            if v > peak:
                peak = v
            if peak > 0.0:
                dd = (peak - v) / peak
                if dd > max_dd:
                    max_dd = dd
        return float(max_dd)

    def _sharpe_like(eq_list: List[float], rf: float) -> float:
        if len(eq_list) < 2:
            return 0.0
        rets: List[float] = []
        prev = float(eq_list[0])
        for eq in eq_list[1:]:
            cur = float(eq)
            if prev != 0.0:
                rets.append((cur / prev) - 1.0)
            prev = cur
        if not rets:
            return 0.0
        mean_r = float(sum(rets) / len(rets))
        if len(rets) == 1:
            return 0.0
        var = float(sum((x - mean_r) ** 2 for x in rets) / len(rets))
        std = math.sqrt(var) if var > 0.0 else 0.0
        if std <= 0.0:
            return 0.0
        return float((mean_r - float(rf)) / std)

    def _is_ym_key(v: str) -> bool:
        return bool(re.match(r"^\d{4}-\d{2}$", str(v)))

    def _monthly_returns_from_end_equity(month_end_eq: Dict[str, float], months: List[str]) -> List[float]:
        out: List[float] = []
        prev_eq: float | None = None
        for m in months:
            cur_eq = float(month_end_eq.get(m, 0.0) or 0.0)
            if not math.isfinite(cur_eq):
                prev_eq = None
                continue
            if prev_eq is not None and prev_eq != 0.0 and math.isfinite(prev_eq):
                r = (cur_eq / prev_eq) - 1.0
                if math.isfinite(r):
                    out.append(float(r))
            prev_eq = cur_eq
        return out

    def _sharpe_like_from_returns(returns: List[float], rf_monthly: float) -> float:
        if len(returns) < 2:
            return 0.0
        vals = [float(x) for x in returns if math.isfinite(float(x))]
        if len(vals) < 2:
            return 0.0
        mean_r = float(sum(vals) / len(vals))
        if not math.isfinite(mean_r):
            return 0.0
        var = float(sum((x - mean_r) ** 2 for x in vals) / len(vals))
        if (not math.isfinite(var)) or var <= 0.0:
            return 0.0
        std = math.sqrt(var)
        if (not math.isfinite(std)) or std <= 0.0:
            return 0.0
        out = (mean_r - float(rf_monthly)) / std
        if not math.isfinite(out):
            return 0.0
        return float(out)

    month_end_eq: Dict[str, float] = {}
    all_months_sorted = sorted([m for m in eq_by_month.keys() if _is_ym_key(m)])
    for m in all_months_sorted:
        eq_list_m = eq_by_month.get(m, [])
        if not eq_list_m:
            continue
        v = float(eq_list_m[-1])
        if math.isfinite(v):
            month_end_eq[str(m)] = float(v)

    overall_monthly_returns = _monthly_returns_from_end_equity(month_end_eq, all_months_sorted)

    yearly: Dict[str, Dict[str, Any]] = {}
    if include_yearly:
        years = sorted(set(eq_by_year.keys()) | set(tr_by_year.keys()))
        for y in years:
            eq_list = eq_by_year.get(y, [])
            tr = tr_by_year.get(y, {"trades": 0.0, "wins": 0.0, "net_total": 0.0})
            trades_n = int(tr.get("trades", 0.0) or 0.0)
            wins_n = int(tr.get("wins", 0.0) or 0.0)
            net_total = float(tr.get("net_total", 0.0) or 0.0)
            if trades_n == 0 and len(eq_list) >= 2:
                net_total = float(eq_list[-1]) - float(eq_list[0])
            start_eq = float(eq_list[0]) if eq_list else 0.0
            ret_pct = (net_total / start_eq * 100.0) if start_eq > 0.0 else 0.0
            win_rate = (wins_n / trades_n) if trades_n > 0 else 0.0
            yearly[str(y)] = {
                "trades": int(trades_n),
                "net_total": float(net_total),
                "return_pct_of_init": float(ret_pct),
                "max_dd": float(_max_dd_from_equity(eq_list)),
                "max_dd_mtm": float(_max_dd_from_equity(mtm_by_year.get(y, eq_list))),
                "win_rate": float(win_rate),
                "sharpe_like": float(
                    _sharpe_like_from_returns(
                        _monthly_returns_from_end_equity(
                            month_end_eq,
                            [m for m in all_months_sorted if m.startswith(f"{str(y)}-")],
                        ),
                        float(risk_free_rate),
                    )
                ),
            }

    monthly: Dict[str, Dict[str, Any]] = {}
    if include_monthly:
        months = sorted(set(eq_by_month.keys()) | set(tr_by_month.keys()))
        for m in months:
            eq_list = eq_by_month.get(m, [])
            tr = tr_by_month.get(m, {"trades": 0.0, "wins": 0.0, "net_total": 0.0})
            trades_n = int(tr.get("trades", 0.0) or 0.0)
            wins_n = int(tr.get("wins", 0.0) or 0.0)
            net_total = float(tr.get("net_total", 0.0) or 0.0)
            if trades_n == 0 and len(eq_list) >= 2:
                net_total = float(eq_list[-1]) - float(eq_list[0])
            start_eq = float(eq_list[0]) if eq_list else 0.0
            ret_pct = (net_total / start_eq * 100.0) if start_eq > 0.0 else 0.0
            win_rate = (wins_n / trades_n) if trades_n > 0 else 0.0
            monthly[str(m)] = {
                "trades": int(trades_n),
                "net_total": float(net_total),
                "return_pct_of_init": float(ret_pct),
                "max_dd": float(_max_dd_from_equity(eq_list)),
                "max_dd_mtm": float(_max_dd_from_equity(mtm_by_month.get(m, eq_list))),
                "win_rate": float(win_rate),
                "sharpe_like": 0.0,
            }

    if overall is None:
        total_trades = int(sum(int(v.get("trades", 0.0) or 0.0) for v in tr_by_year.values()))
        total_net = float(sum(float(v.get("net_total", 0.0) or 0.0) for v in tr_by_year.values()))
        init_eq = float(eq_all[0]) if eq_all else 0.0
        ret_pct = (total_net / init_eq * 100.0) if init_eq > 0.0 else 0.0
        overall = {
            "trades": int(total_trades),
            "net_total": float(total_net),
            "return_pct_of_init": float(ret_pct),
            "max_dd": float(_max_dd_from_equity(eq_all)),
            "max_dd_mtm": float(_max_dd_from_equity(mtm_all if mtm_all else eq_all)),
        }
    overall = dict(overall)
    if "max_dd_mtm" not in overall:
        overall["max_dd_mtm"] = float(_max_dd_from_equity(mtm_all if mtm_all else eq_all))
    if "max_dd_worst_bar" not in overall:
        overall["max_dd_worst_bar"] = float(overall.get("max_dd_mtm", overall.get("max_dd", 0.0)) or 0.0)
    for _k in (
        "mae_max_abs",
        "mae_max_bps",
        "mae_p50_abs",
        "mae_p90_abs",
        "mae_p99_abs",
        "mae_p50_bps",
        "mae_p90_bps",
        "mae_p99_bps",
        "mfe_max_abs",
        "mfe_max_bps",
        "mfe_p50_abs",
        "mfe_p90_abs",
        "mfe_p99_abs",
        "mfe_p50_bps",
        "mfe_p90_bps",
        "mfe_p99_bps",
        "giveback_max_abs",
        "giveback_max_bps",
        "giveback_max_pct_of_mfe",
        "giveback_max_p50_abs",
        "giveback_max_p90_abs",
        "giveback_max_p99_abs",
        "giveback_max_p50_bps",
        "giveback_max_p90_bps",
        "giveback_max_p99_bps",
        "giveback_max_p50_pct_of_mfe",
        "giveback_max_p90_pct_of_mfe",
        "giveback_max_p99_pct_of_mfe",
        "giveback_to_close_abs",
        "giveback_to_close_bps",
        "giveback_to_close_pct_of_mfe",
        "giveback_to_close_p50_abs",
        "giveback_to_close_p90_abs",
        "giveback_to_close_p99_abs",
        "giveback_to_close_p50_bps",
        "giveback_to_close_p90_bps",
        "giveback_to_close_p99_bps",
        "giveback_to_close_p50_pct_of_mfe",
        "giveback_to_close_p90_pct_of_mfe",
        "giveback_to_close_p99_pct_of_mfe",
        "kept_p50_bps",
        "kept_p90_bps",
        "kept_p99_bps",
        "kept_pct_of_mfe_p50",
        "kept_pct_of_mfe_p90",
        "kept_pct_of_mfe_p99",
        "fav_adv_ratio_p50",
        "fav_adv_ratio_p90",
        "fav_adv_ratio_p99",
    ):
        if _k not in overall:
            overall[_k] = 0.0
    if "exit_hint" not in overall:
        overall["exit_hint"] = ""
    try:
        trades_now = int(overall.get("trades", 0) or 0)
        max_dd_now = float(overall.get("max_dd", 0.0) or 0.0)
        max_dd_worst_now = float(overall.get("max_dd_worst_bar", 0.0) or 0.0)
        mae_max_now = float(overall.get("mae_max_abs", 0.0) or 0.0)
        init_eq_now = float(eq_all[0]) if eq_all else 0.0
        reasons: List[str] = []
        if trades_now > 0 and max_dd_worst_now <= 0.0 and mae_max_now > 0.0:
            reasons.append("worst_bar_dd_zero_with_positive_mae")
        if max_dd_worst_now < max_dd_now:
            reasons.append("worst_bar_dd_below_max_dd")
        if init_eq_now > 0.0:
            worst_dd_abs_est = float(init_eq_now) * float(max_dd_worst_now)
            if worst_dd_abs_est > 0.0 and mae_max_now > (worst_dd_abs_est * 5.0):
                reasons.append("mae_abs_much_larger_than_worst_dd_abs_est")
        if reasons:
            logger.warning(
                "[REPORT_SANITY] suspicious worst_bar_dd: reason=%s max_dd=%.6f max_dd_worst_bar=%.6f mae_max_abs=%.6f initial_equity=%.6f",
                ",".join(reasons),
                float(max_dd_now),
                float(max_dd_worst_now),
                float(mae_max_now),
                float(init_eq_now),
            )
    except Exception:
        pass
    overall["sharpe_like"] = float(_sharpe_like_from_returns(overall_monthly_returns, float(risk_free_rate)))

    out = {
        "meta": {
            "build_id": str(build_id),
            "preset": str(preset_name or ""),
            "year": int(year) if year else None,
            "mode": str(mode),
            "include_yearly": bool(include_yearly),
            "include_monthly": bool(include_monthly),
            "sharpe_like_basis": "monthly_returns",
        },
        "overall": dict(overall),
        "yearly": yearly if include_yearly else {},
        "monthly": monthly if include_monthly else {},
    }
    out_dir = os.path.dirname(str(out_path))
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    logger.info("[REPORT] wrote: %s", str(out_path))
    return True

def _ohlcv_to_cols(rows: List[List[float]]) -> Dict[str, List[float]]:
    out = {"ts": [], "open": [], "high": [], "low": [], "close": [], "vol": []}
    for r in rows:
        out["ts"].append(int(r[0]))
        out["open"].append(float(r[1]))
        out["high"].append(float(r[2]))
        out["low"].append(float(r[3]))
        out["close"].append(float(r[4]))
        out["vol"].append(float(r[5]))
    return out

def _ts_raw_and_ms_from_ts_list(ts_list: List[float] | List[int] | None) -> tuple[int | None, int | None, int | None, int | None, int]:
    def _norm_ts_ms(ts: int) -> int:
        x = int(ts)
        if 0 < x < 1_000_000_000_000:
            return x * 1000
        if x >= 100_000_000_000_000:
            return x // 1000
        return x

    if not ts_list:
        return (None, None, None, None, 0)
    first_raw = int(ts_list[0])
    last_raw = int(ts_list[-1])
    return (first_raw, _norm_ts_ms(first_raw), last_raw, _norm_ts_ms(last_raw), len(ts_list))

def export_equity_curve(path: str, curve: List[Tuple[int, float]], mtm_by_ts: Dict[int, float] | None = None) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["ts", "ts_iso", "equity", "mtm_equity"])
        for ts, eq in curve:
            ts_i = None
            try:
                ts_i = int(ts)
            except Exception:
                ts_i = None

            iso = iso_utc(ts_i) if ts_i is not None else ""
            if ts_i is None:
                continue

            eq_mtm = float(eq)
            if isinstance(mtm_by_ts, dict):
                try:
                    eq_mtm = float(mtm_by_ts.get(int(ts_i), float(eq)))
                except Exception:
                    eq_mtm = float(eq)
            w.writerow([ts_i, iso, f"{float(eq):.8f}", f"{float(eq_mtm):.8f}"])


def export_trades(path, trades):
    """Export a list of dicts to CSV (union of keys across rows)."""
    if not trades:
        return

    cols_set = set()
    for r in trades:
        if isinstance(r, dict):
            cols_set.update(r.keys())
    cols = sorted(cols_set)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in trades:
            if not isinstance(r, dict):
                continue
            w.writerow(r)


def export_synthetic_equity_curve(path: str, trades: List[Dict[str, Any]], start_equity: float) -> None:
    """Visualization-only synthetic compound curve from realized net per trade."""
    if not trades:
        return
    try:
        start_eq = float(start_equity)
    except Exception:
        return
    if start_eq <= 0.0:
        return

    synthetic_eq = float(start_eq)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["trade_index", "ts", "ts_iso", "net", "scale", "synthetic_equity"])
        for i, tr in enumerate(trades, start=1):
            if not isinstance(tr, dict):
                continue

            ts_i = None
            try:
                ts_v = tr.get("ts")
                if ts_v is not None and ts_v != "":
                    ts_i = int(ts_v)
            except Exception:
                ts_i = None

            try:
                net_i = float(tr.get("net"))
            except Exception:
                try:
                    net_i = float(tr.get("pnl", 0.0)) - float(tr.get("fee", 0.0))
                except Exception:
                    net_i = 0.0

            eq_ratio = float(synthetic_eq) / float(start_eq) if start_eq > 0.0 else 1.0
            if eq_ratio < 0.0:
                eq_ratio = 0.0
            scale_i = min(2.5, eq_ratio ** 0.7)
            synthetic_eq = float(synthetic_eq) + float(net_i) * float(scale_i)

            w.writerow(
                [
                    int(i),
                    (int(ts_i) if ts_i is not None else ""),
                    (iso_utc(ts_i) if ts_i is not None else ""),
                    f"{float(net_i):.8f}",
                    f"{float(scale_i):.8f}",
                    f"{float(synthetic_eq):.8f}",
                ]
            )


def _bps_to_frac(bps: float) -> float:
    return float(bps) / 10000.0


def _side_slip_mult(kind: str) -> float:
    """
    Slippage model (one-way):
      - entry_long: unfavorable -> +slip
      - exit_long : unfavorable -> -slip
    """
    slip_bps = float(getattr(C, "SLIPPAGE_BPS", 0.0))
    slip = _bps_to_frac(max(0.0, slip_bps))
    if kind == "entry_long":
        return 1.0 + slip
    if kind == "exit_long":
        return 1.0 - slip
    return 1.0


def _clamp(x: float, lo: float, hi: float) -> float:
    lo_f = float(lo)
    hi_f = float(hi)
    if hi_f < lo_f:
        lo_f, hi_f = hi_f, lo_f
    x_f = float(x)
    if x_f < lo_f:
        return lo_f
    if x_f > hi_f:
        return hi_f
    return x_f


def _resolve_size_cap_ramp_max_pct(cap_pct_base: float, cfg_value: float) -> float:
    base = max(0.0, float(cap_pct_base))
    auto_floor = 0.30 if base >= 0.20 else 0.20
    try:
        out = float(cfg_value)
    except Exception:
        out = auto_floor
    if (not math.isfinite(out)) or out <= 0.0:
        out = auto_floor
    return max(base, auto_floor, float(out))


def _resolve_size_cap_pct_eff(
    *,
    cap_pct_base: float,
    cap_ramp_enabled: bool,
    cap_ramp_k: float,
    cap_ramp_max_pct: float,
    profit_ratio: float,
    risk_mult_cap: float,
) -> tuple[float, bool]:
    base = max(0.0, float(cap_pct_base))
    eff = base
    cap_ramp_applied = False
    if bool(cap_ramp_enabled):
        k = max(1e-9, float(cap_ramp_k))
        pr = max(0.0, float(profit_ratio))
        r = 1.0 - math.exp(-pr / k)
        cap_pct_profit = base + (float(cap_ramp_max_pct) - base) * float(r)
        eff = _clamp(cap_pct_profit, base, float(cap_ramp_max_pct))
        cap_ramp_applied = True
    eff *= _clamp(float(risk_mult_cap), 0.0, 1.0)
    return max(0.0, float(eff)), bool(cap_ramp_applied)


def _infer_amount_step(ex: Any, symbol: str) -> float:
    # Replay helper (runner replay exchange shape)
    try:
        dmap = getattr(ex, "_replay_amount_decimals_map", None)
        if isinstance(dmap, dict):
            sym = str(symbol)
            d = dmap.get(sym)
            if d is None:
                d = dmap.get(sym.replace("/", ""))
            if d is None:
                d = getattr(ex, "_replay_amount_decimals_default", None)
            if d is not None:
                di = int(d)
                if di <= 0:
                    return 1.0
                return float(10.0 ** (-di))
    except Exception:
        pass

    m = None
    try:
        ex_obj = getattr(ex, "ex", None)
        if ex_obj is not None and hasattr(ex_obj, "market"):
            m = ex_obj.market(symbol)
    except Exception:
        m = None
    if not isinstance(m, dict):
        return 0.0

    info = m.get("info", {}) if isinstance(m, dict) else {}
    for k in ("stepSize", "quantityIncrement", "qty_step", "amountStep"):
        try:
            v = float((info or {}).get(k))
            if v > 0.0:
                return float(v)
        except Exception:
            pass
    try:
        prec = (m.get("precision", {}) or {}).get("amount", None)
        if isinstance(prec, int):
            if prec <= 0:
                return 1.0
            return float(10.0 ** (-int(prec)))
    except Exception:
        pass
    try:
        min_amt = float(((m.get("limits", {}) or {}).get("amount", {}) or {}).get("min", 0.0) or 0.0)
        if min_amt > 0.0:
            return float(min_amt)
    except Exception:
        pass
    return 0.0


def _ceil_to_step(qty: float, step: float) -> float:
    q = float(qty)
    s = float(step)
    if (not math.isfinite(q)) or (not math.isfinite(s)) or s <= 0.0:
        return q
    n = math.ceil((q - 1e-15) / s)
    return float(n * s)


def _floor_to_step(qty: float, step: float) -> float:
    q = float(qty)
    s = float(step)
    if (not math.isfinite(q)) or (not math.isfinite(s)) or s <= 0.0:
        return q
    n = math.floor((q + 1e-15) / s)
    return float(n * s)


def _amount_to_precision_deterministic(ex: Any, symbol: str, qty: float) -> float:
    q = float(qty)
    step = float(_infer_amount_step(ex, symbol))
    if math.isfinite(step) and step > 0.0:
        return float(_floor_to_step(q, step))
    scale = 100_000_000.0
    return float(math.floor((q + 1e-15) * scale) / scale)

def _advance_filter_ptr(filter_ts: List[int], j: int, t: int) -> int:
    """
    j: current pointer (index into filter_ts)
    returns: largest j where filter_ts[j] <= t, moving only forward (amortized O(1))
    """
    n = len(filter_ts)
    if n == 0:
        return -1

    if j < -1:
        j = -1

    # If we are before start and t is before first
    if j == -1 and t < filter_ts[0]:
        return -1

    # Move forward while next candle is <= t
    while (j + 1) < n and filter_ts[j + 1] <= t:
        j += 1
    return j



def _find_filter_index(filter_ts: List[int], t: int) -> int:
    """largest i where filter_ts[i] <= t"""
    if not filter_ts:
        return -1
    if t < filter_ts[0]:
        return -1
    if t >= filter_ts[-1]:
        return len(filter_ts) - 1
    lo = 0
    hi = len(filter_ts) - 1
    while lo <= hi:
        mid = (lo + hi) // 2
        if filter_ts[mid] <= t:
            lo = mid + 1
        else:
            hi = mid - 1
    return hi


def _fetch_ohlcv_paginated(
    ex: ExchangeClient,
    symbol: str,
    timeframe: str,
    since_ms: Optional[int],
    per_call_limit: int,
    max_total_bars: int,
    allow_since_fallback: bool = True,
) -> List[List[float]]:
    """
    Fetch OHLCV with pagination.
    If `since_ms` yields 0 rows, optionally fallback to since=None (latest candles).
    """

    def _fetch_once(_since: Optional[int]) -> List[List[float]]:
        return ex.fetch_ohlcv(symbol, timeframe, limit=int(per_call_limit), since=_since)

    out: List[List[float]] = []

    # 1) initial fetch
    first_since = int(since_ms) if since_ms is not None else None
    batch = _fetch_once(first_since)

    if not batch and since_ms is not None:
        # If the exchange returns 0 rows for a very old since, probe forward until we find data.
        step_days = int(getattr(C, "SINCE_PROBE_STEP_DAYS", 7))
        max_steps = int(getattr(C, "SINCE_PROBE_MAX_STEPS", 120))  # 7days*120=840days
        step_ms = step_days * 24 * 60 * 60 * 1000

        probed_since = first_since
        found = False

        for _ in range(max_steps):
            probed_since = int(probed_since) + int(step_ms)
            batch = _fetch_once(probed_since)
            if batch:
                logger.warning(
                    f"fetch_ohlcv returned 0 rows for {symbol} tf={timeframe} since={first_since}. "
                    f"Found data after probing since={probed_since} (step={step_days}d)."
                )
                first_since = probed_since
                found = True
                break

        if (not found) and allow_since_fallback:
            logger.warning(
                f"fetch_ohlcv returned 0 rows for {symbol} tf={timeframe} since={int(since_ms)} "
                f"even after probing. Fallback to latest candles (since=None)."
            )
            batch = _fetch_once(None)


    if not batch:
        return []

    out.extend(batch)

    # 2) paginate forward
    while True:
        if len(out) >= int(max_total_bars):
            out = out[: int(max_total_bars)]
            break

        last_ts = int(out[-1][0])
        next_since = last_ts + 1

        batch = _fetch_once(next_since)
        if not batch:
            break

        # Deduplicate overlaps
        batch = [r for r in batch if int(r[0]) > last_ts]
        if not batch:
            break

        out.extend(batch)

        # safety
        if len(out) >= 2 and int(out[-1][0]) == int(out[-2][0]):
            break

    return out


def _recent_slice(rows: List[List[float]], recent_bars: Optional[int]) -> List[List[float]]:
    if recent_bars is None or recent_bars <= 0:
        return rows
    if len(rows) <= recent_bars:
        return rows
    return rows[-recent_bars:]

def _tf_to_ms(tf: str) -> int:
    tf = str(tf).strip().lower()
    if tf.endswith("m"):
        return int(tf[:-1]) * 60 * 1000
    if tf.endswith("h"):
        return int(tf[:-1]) * 60 * 60 * 1000
    raise ValueError(f"Unsupported timeframe: {tf}")

def _read_binance_csv_dir(
    csv_dir: str,
    file_glob: str,
    since_ms: Optional[int],
    until_ms: Optional[int] = None,
    paths_override: Optional[List[str]] = None,
    diag_out: Optional[Dict[str, Any]] = None,
):
    csv_dir_abs = os.path.abspath(csv_dir)
    pattern = os.path.join(csv_dir_abs, file_glob)
    if paths_override is None:
        paths = sorted(glob.glob(pattern))
        paths = [p for p in paths if os.path.isfile(p)]
    else:
        paths = [os.path.abspath(str(p)) for p in list(paths_override or []) if os.path.isfile(str(p))]
        paths = sorted(dict.fromkeys(paths))

    logger.info(
        f"CSV SEARCH: dir={csv_dir_abs} glob={file_glob} matches={len(paths)} "
        f"explicit_paths={'yes' if paths_override is not None else 'no'}"
    )
    for p in paths[:10]:
        logger.info(f"CSV FILE: {p}")

    # If the requested glob did not match, log nearby CSV candidates before failing.
    if not paths:
        cand = sorted(glob.glob(os.path.join(csv_dir_abs, "*.csv")))
        logger.error(f"CSV NOT FOUND: pattern={pattern}")
        logger.error(f"CSV DIR LIST (*.csv)={len(cand)}")
        for p in cand[:20]:
            logger.error(f"CSV CAND: {p}")
        raise SystemExit("CSV files not found. Check BACKTEST_CSV_DIR_* and BACKTEST_CSV_GLOB_* in config.py")

    out: List[List[float]] = []
    detected_delims_all: List[str] = []
    first_line_hint = ""
    columns_seen: List[str] = []
    raw_rows = 0
    normalized_rows = 0
    filtered_rows = 0
    dropped_short_rows = 0
    dropped_ts_parse = 0
    dropped_ohlcv_parse = 0
    dropped_since = 0
    dropped_until = 0
    ts_min: Optional[int] = None
    ts_max: Optional[int] = None
    filtered_ts_min: Optional[int] = None
    filtered_ts_max: Optional[int] = None
    sample_first_ts: Optional[int] = None
    sample_last_ts: Optional[int] = None

    def _update_span(v: int, *, filtered: bool) -> None:
        nonlocal ts_min, ts_max, filtered_ts_min, filtered_ts_max, sample_first_ts, sample_last_ts
        t = int(v)
        if ts_min is None or t < ts_min:
            ts_min = t
        if ts_max is None or t > ts_max:
            ts_max = t
        if filtered:
            if filtered_ts_min is None or t < filtered_ts_min:
                filtered_ts_min = t
            if filtered_ts_max is None or t > filtered_ts_max:
                filtered_ts_max = t
            if sample_first_ts is None:
                sample_first_ts = t
            sample_last_ts = t

    for p in paths:
        # Accept UTF-8 with BOM when present.
        with open(p, "r", encoding="utf-8-sig", newline="") as f:
            sample = f.read(4096)
            if not sample.strip():
                continue
            f.seek(0)
            if not first_line_hint:
                try:
                    first_line_hint = str(sample.splitlines()[0][:180])
                except Exception:
                    first_line_hint = ""

            # Detect common delimiters first, then fall back to comma/tab/semicolon.
            detected_delims: List[str] = []
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t"])
                d0 = str(getattr(dialect, "delimiter", "") or "")
                if d0 in [",", ";", "\t"]:
                    detected_delims.append(d0)
            except Exception:
                dialect = csv.excel
            for d in [",", "\t", ";"]:
                if d in sample and d not in detected_delims:
                    detected_delims.append(d)
            for d in [",", "\t", ";"]:
                if d not in detected_delims:
                    detected_delims.append(d)
            for d in detected_delims:
                if d not in detected_delims_all:
                    detected_delims_all.append(d)

            r = csv.reader(f, dialect)
            out_before_file = len(out)

            # Try the fast positional OHLCV parse before attempting header-based fallback.
            first_row = next(r, None)
            if first_row is None:
                continue
            raw_rows += 1
            if not columns_seen and first_row:
                columns_seen = [str(x).strip() for x in first_row[:16]]

            ts0 = normalize_timestamp_to_ms(first_row[0]) if len(first_row) >= 1 else None
            # If the first row already looks like normalized OHLCV, keep it as data.
            file_any_normalized = False
            if ts0 is not None and len(first_row) >= 6:
                try:
                    o = float(first_row[1]); h = float(first_row[2]); l = float(first_row[3]); c = float(first_row[4]); v = float(first_row[5])
                except Exception:
                    dropped_ohlcv_parse += 1
                else:
                    file_any_normalized = True
                    normalized_rows += 1
                    _update_span(int(ts0), filtered=False)
                    if since_ms is not None and int(ts0) < int(since_ms):
                        dropped_since += 1
                    elif until_ms is not None and int(ts0) >= int(until_ms):
                        dropped_until += 1
                    else:
                        out.append([int(ts0), o, h, l, c, v])
                        filtered_rows += 1
                        _update_span(int(ts0), filtered=True)
            elif ts0 is None:
                dropped_ts_parse += 1

            for row in r:
                raw_rows += 1
                if not row or len(row) < 6:
                    dropped_short_rows += 1
                    continue
                ts = normalize_timestamp_to_ms(row[0])
                if ts is None:
                    dropped_ts_parse += 1
                    continue
                try:
                    o = float(row[1]); h = float(row[2]); l = float(row[3]); c = float(row[4]); v = float(row[5])
                except Exception:
                    dropped_ohlcv_parse += 1
                    continue
                file_any_normalized = True
                normalized_rows += 1
                _update_span(int(ts), filtered=False)
                if since_ms is not None and int(ts) < int(since_ms):
                    dropped_since += 1
                    continue
                # NOTE: until_ms is treated as an exclusive cutoff (ts < until_ms).
                if until_ms is not None and int(ts) >= int(until_ms):
                    dropped_until += 1
                    continue
                out.append([int(ts), o, h, l, c, v])
                filtered_rows += 1
                _update_span(int(ts), filtered=True)

            if len(out) == out_before_file and (not file_any_normalized):
                # header-based fallback for non-standard column layouts
                for delim in detected_delims:
                    try:
                        f.seek(0)
                        dr = csv.DictReader(f, delimiter=delim)
                        if not dr.fieldnames:
                            continue
                        if not columns_seen:
                            columns_seen = [str(x).strip() for x in dr.fieldnames[:16]]
                        fmap: Dict[str, str] = {}
                        for raw in dr.fieldnames:
                            k = str(raw).strip().lower()
                            if k:
                                fmap[k] = str(raw)

                        def _pick(cands: List[str]) -> Optional[str]:
                            for k in cands:
                                if k in fmap:
                                    return fmap[k]
                            return None

                        k_ts = _pick(["ts_ms", "open_time", "opentime", "ts", "time", "timestamp", "open time", "datetime", "date"])
                        k_o = _pick(["open", "o"])
                        k_h = _pick(["high", "h"])
                        k_l = _pick(["low", "l"])
                        k_c = _pick(["close", "c"])
                        k_v = _pick(["volume", "vol", "v", "base_volume", "base volume"])
                        if not (k_ts and k_o and k_h and k_l and k_c):
                            continue

                        for row in dr:
                            ts = normalize_timestamp_to_ms(row.get(k_ts, ""))
                            if ts is None:
                                continue
                            try:
                                o = float(row.get(k_o, ""))
                                h = float(row.get(k_h, ""))
                                l = float(row.get(k_l, ""))
                                c = float(row.get(k_c, ""))
                                v = float(row.get(k_v, 0.0)) if k_v else 0.0
                            except Exception:
                                continue
                            normalized_rows += 1
                            _update_span(int(ts), filtered=False)
                            if since_ms is not None and ts < int(since_ms):
                                dropped_since += 1
                                continue
                            if until_ms is not None and ts >= int(until_ms):
                                dropped_until += 1
                                continue
                            out.append([int(ts), o, h, l, c, v])
                            filtered_rows += 1
                            _update_span(int(ts), filtered=True)
                        if len(out) > out_before_file:
                            break
                    except Exception:
                        continue

    if not out:
        dshow = "|".join(detected_delims_all) if detected_delims_all else "none"
        fhint = str(first_line_hint or "").replace("\r", " ").replace("\n", " ")
        if len(fhint) > 160:
            fhint = fhint[:160] + "..."
        diag = {
            "raw_rows": int(raw_rows),
            "normalized_rows": int(normalized_rows),
            "filtered_rows": int(filtered_rows),
            "columns": list(columns_seen),
            "ts_min": int(ts_min) if ts_min is not None else None,
            "ts_max": int(ts_max) if ts_max is not None else None,
            "sample_first_ts": int(sample_first_ts) if sample_first_ts is not None else None,
            "sample_last_ts": int(sample_last_ts) if sample_last_ts is not None else None,
            "dropped_short_rows": int(dropped_short_rows),
            "dropped_ts_parse": int(dropped_ts_parse),
            "dropped_ohlcv_parse": int(dropped_ohlcv_parse),
            "dropped_since": int(dropped_since),
            "dropped_until": int(dropped_until),
            "delim_candidates": dshow,
            "first_line_hint": fhint,
        }
        if diag_out is not None:
            diag_out.clear()
            diag_out.update(diag)
        raise SystemExit(
            f"CSV parsed 0 rows. Check delimiter/columns format: dir={csv_dir_abs} glob={file_glob} "
            f"first_line='{fhint}' delim_candidates={dshow} raw_rows={raw_rows} "
            f"normalized_rows={normalized_rows} filtered_rows={filtered_rows}"
        )

    out.sort(key=lambda x: int(x[0]))

    # dedupe by ts (keep last)
    dedup: List[List[float]] = []
    last_ts = None
    for r in out:
        t = int(r[0])
        if last_ts is None or t != last_ts:
            dedup.append(r)
            last_ts = t
        else:
            dedup[-1] = r

    if dedup:
        sample_first_ts = int(dedup[0][0])
        sample_last_ts = int(dedup[-1][0])
        filtered_ts_min = int(dedup[0][0])
        filtered_ts_max = int(dedup[-1][0])
    diag = {
        "raw_rows": int(raw_rows),
        "normalized_rows": int(normalized_rows),
        "filtered_rows": int(len(dedup)),
        "columns": list(columns_seen),
        "ts_min": int(ts_min) if ts_min is not None else None,
        "ts_max": int(ts_max) if ts_max is not None else None,
        "filtered_ts_min": int(filtered_ts_min) if filtered_ts_min is not None else None,
        "filtered_ts_max": int(filtered_ts_max) if filtered_ts_max is not None else None,
        "sample_first_ts": int(sample_first_ts) if sample_first_ts is not None else None,
        "sample_last_ts": int(sample_last_ts) if sample_last_ts is not None else None,
        "dropped_short_rows": int(dropped_short_rows),
        "dropped_ts_parse": int(dropped_ts_parse),
        "dropped_ohlcv_parse": int(dropped_ohlcv_parse),
        "dropped_since": int(dropped_since),
        "dropped_until": int(dropped_until),
        "delim_candidates": "|".join(detected_delims_all) if detected_delims_all else "none",
        "first_line_hint": str(first_line_hint or ""),
    }
    if diag_out is not None:
        diag_out.clear()
        diag_out.update(diag)

    return dedup

def _parse_ts_maybe_ms(x: str) -> Optional[int]:
    try:
        ts = int(float(str(x).strip()))
    except Exception:
        return None
    # seconds -> ms
    if 0 < ts < 100_000_000_000:
        ts *= 1000
    # microseconds -> ms
    if ts > 10_000_000_000_000:
        ts //= 1000
    return int(ts)


def _load_precomputed_indicator_map(ind_csv_path: str) -> Dict[int, float]:
    """Load precomputed indicator CSV written by precompute_indicators.py.

    Expected columns: ts, ts_iso, value
    value may be empty for NaN.
    """
    out: Dict[int, float] = {}
    # BOM absorption
    with open(ind_csv_path, "r", encoding="utf-8-sig", newline="") as f:
        r = csv.reader(f)
        header = next(r, None)
        if header is None:
            return out
        # header is optional; accept both
        for row in r:
            if not row or len(row) < 1:
                continue
            ts = _parse_ts_maybe_ms(row[0])
            if ts is None:
                continue
            v: float
            if len(row) >= 3:
                s = str(row[2]).strip()
            elif len(row) >= 2:
                s = str(row[1]).strip()
            else:
                s = ""
            if s == "":
                v = float("nan")
            else:
                try:
                    v = float(s)
                except Exception:
                    v = float("nan")
            out[int(ts)] = float(v)
    return out

def _load_precomputed_indicator_series_fast(
    ts_list: List[int],
    ind_csv_paths: List[str],
    *,
    name: str,
    warmup_bars: int,
    strict: bool,
) -> List[float]:
    """Fast-path loader that avoids building a full ts->value dict.

    Assumptions (true for our precompute output):
      - monthly files are sorted by ts asc within file
      - concatenating month files yields ts asc (after filtering)
      - ts_list is the OHLCV timeline to align to

    If alignment checks fail, we fall back to the dict-based path.
    """
    if not ts_list:
        return []
    if not ind_csv_paths:
        return [float("nan")] * len(ts_list)

    # 1) Try sequential read (O(n), no dict lookups)
    out_ts: List[int] = []
    out_v: List[float] = []
    for p in ind_csv_paths:
        with open(p, "r", encoding="utf-8-sig", newline="") as f:
            r = csv.reader(f)
            _ = next(r, None)  # header (optional)
            for row in r:
                if not row or len(row) < 1:
                    continue
                ts = _parse_ts_maybe_ms(row[0])
                if ts is None:
                    continue
                if len(row) >= 3:
                    s = str(row[2]).strip()
                elif len(row) >= 2:
                    s = str(row[1]).strip()
                else:
                    s = ""
                if s == "":
                    v = float("nan")
                else:
                    try:
                        v = float(s)
                    except Exception:
                        v = float("nan")
                out_ts.append(int(ts))
                out_v.append(float(v))

    # 2) Quick alignment validation
    if len(out_ts) == len(ts_list) and out_ts and int(out_ts[0]) == int(ts_list[0]) and int(out_ts[-1]) == int(ts_list[-1]):
        wb = max(0, int(warmup_bars))
        # spot-check a few positions to avoid O(n) compare overhead
        checks: List[int] = [0, len(ts_list) - 1]
        if wb < len(ts_list):
            checks.append(wb)
        mid = len(ts_list) // 2
        checks.append(mid)
        ok = True
        for idx in sorted(set(i for i in checks if 0 <= i < len(ts_list))):
            if int(out_ts[idx]) != int(ts_list[idx]):
                ok = False
                break
        if ok:
            _validate_series_nan(name, out_v, warmup_bars, strict)
            return out_v

    # 3) Fallback: dict-based alignment (robust)
    mp: Dict[int, float] = {}
    for p in ind_csv_paths:
        mp.update(_load_precomputed_indicator_map(p))
    series = _build_series_from_map(ts_list, mp)
    _validate_series_nan(name, series, warmup_bars, strict)
    return series

def _load_precomputed_indicator_series_fast(
    ts_list: List[int],
    ind_csv_paths: List[str],
    *,
    name: str,
    warmup_bars: int,
    strict: bool,
) -> np.ndarray:
    """Fast-path loader for precomputed indicator CSVs.

    Avoids building a full ts->value dict by sequentially reading rows and
    aligning to the OHLCV ts_list. If alignment checks fail, falls back to the
    dict-based alignment (robust).

    Expected for our precompute output:
      - month files sorted by ts asc
      - concatenation yields ts asc (after month filtering)
      - ts_list is the target timeline to align to
    """
    if not ts_list:
        return np.asarray([], dtype=float)

    if not ind_csv_paths:
        out = np.full(len(ts_list), float("nan"), dtype=float)
        _validate_series_nan(name, out, warmup_bars, strict)
        return out

    out_ts: List[int] = []
    out_v: List[float] = []
    for p in ind_csv_paths:
        with open(p, "r", encoding="utf-8-sig", newline="") as f:
            r = csv.reader(f)
            _ = next(r, None)  # header (optional)
            for row in r:
                if not row:
                    continue
                ts = _parse_ts_maybe_ms(row[0])
                if ts is None:
                    continue
                if len(row) >= 3:
                    s = str(row[2]).strip()
                elif len(row) >= 2:
                    s = str(row[1]).strip()
                else:
                    s = ""
                if s == "":
                    v = float("nan")
                else:
                    try:
                        v = float(s)
                    except Exception:
                        v = float("nan")
                out_ts.append(int(ts))
                out_v.append(float(v))

    # Quick alignment validation: check length + a few spot positions
    if (
        len(out_ts) == len(ts_list)
        and out_ts
        and int(out_ts[0]) == int(ts_list[0])
        and int(out_ts[-1]) == int(ts_list[-1])
    ):
        wb = max(0, int(warmup_bars))
        checks: List[int] = [0, len(ts_list) - 1]
        if wb < len(ts_list):
            checks.append(wb)
        checks.append(len(ts_list) // 2)
        ok = True
        for idx in sorted(set(i for i in checks if 0 <= i < len(ts_list))):
            if int(out_ts[idx]) != int(ts_list[idx]):
                ok = False
                break
        if ok:
            out = np.asarray(out_v, dtype=float)
            _validate_series_nan(name, out, warmup_bars, strict)
            return out

    # Fallback (robust): dict-based alignment (last-wins across months)
    mp: Dict[int, float] = {}
    for p in ind_csv_paths:
        mp.update(_load_precomputed_indicator_map(p))
    series = _build_series_from_map(ts_list, mp)
    out = np.asarray(series, dtype=float)
    _validate_series_nan(name, out, warmup_bars, strict)
    return out


def _resolve_precomputed_root(path: str) -> str:
    raw = str(path or "").strip() or os.path.join("exports", "precomputed_indicators")
    expanded = os.path.expanduser(os.path.expandvars(raw))
    if os.path.isabs(expanded):
        return os.path.abspath(expanded)
    repo_root = os.path.dirname(os.path.abspath(__file__))
    return os.path.abspath(os.path.join(repo_root, expanded))


def _resolve_precomputed_indicator_path(
    out_root: str,
    symbol: str,
    tf: str,
    ind_name: str,
) -> List[str]:
    """Resolve indicator csv paths for given symbol/tf/ind_name.

    Convention:
      <out_root>/<symbol>/<tf>/<IND>-*.csv
      e.g. exports/precomputed_indicators/ETHUSDT/5m/EMA9-ETHUSDT-5m-2021-01.csv
    """
    # Normalize symbol for filesystem: "ETH/USDT" -> "ETHUSDT"
    symbol_fs = str(symbol).replace("/", "").replace(":", "").replace("-", "")
    out_root_abs = _resolve_precomputed_root(out_root)
    alt_root_abs = os.path.join(out_root_abs, "precomputed_indicators")
    symbol_raw = str(symbol).strip()
    candidate_dirs = [
        os.path.join(out_root_abs, symbol_raw, str(tf)),
        os.path.join(alt_root_abs, symbol_raw, str(tf)),
        os.path.join(out_root_abs, symbol_fs, str(tf)),
        os.path.join(alt_root_abs, symbol_fs, str(tf)),
    ]
    for d in candidate_dirs:
        pattern = os.path.join(d, f"{ind_name}-*.csv")
        paths = sorted(p for p in glob.glob(pattern) if os.path.isfile(p))
        if paths:
            return paths
    return []


def _handle_missing_precomputed(
    *,
    symbol: str,
    tf: str,
    missing: List[str],
    found_any: bool,
    root: str,
    strict: bool,
    scope: str,
) -> bool:
    if not missing:
        return True
    msg = (
        f"PRECOMPUTED missing for {symbol} tf={tf} indicators={','.join(missing)} "
        f"(root={root})"
    )
    if strict and found_any:
        raise RuntimeError(msg)
    reason = "no symbol-specific precomputed files found" if not found_any else "strict disabled"
    logger.warning("%s -> fallback to on-the-fly calc (%s=%s)", msg, scope, reason)
    return False

def _ym_index_from_ts_utc(ts_ms: int) -> int:
    dt = datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc)
    return int(dt.year) * 12 + int(dt.month)


def _filter_month_files_by_ts(paths: List[str], ts_start_ms: int, ts_end_ms: int) -> List[str]:
    """Filter monthly precomputed files by time window (UTC year-month).

    File naming convention includes ...-YYYY-MM.csv at the end.
    If parsing fails, keep the path (be permissive).
    """
    if not paths:
        return []
    start_ym = _ym_index_from_ts_utc(int(ts_start_ms))
    end_ym = _ym_index_from_ts_utc(int(ts_end_ms))
    matched: List[str] = []
    fallback_unparseable: List[str] = []
    saw_parseable = False
    rx = re.compile(r"-(\d{4})-(\d{2})\.csv$", re.IGNORECASE)
    for p in paths:
        base = os.path.basename(p)
        m = rx.search(base)
        if not m:
            fallback_unparseable.append(p)
            continue
        saw_parseable = True
        y = int(m.group(1))
        mo = int(m.group(2))
        ym = y * 12 + mo
        if start_ym <= ym <= end_ym:
            matched.append(p)
    if matched:
        return matched
    if saw_parseable:
        return matched
    return fallback_unparseable


def _scan_precomputed_file_head(path: str) -> dict[str, Any]:
    leading_nan_rows = 0
    first_gap_ts: int | None = None
    first_valid_ts: int | None = None
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        r = csv.reader(f)
        _ = next(r, None)
        for row in r:
            if not row or len(row) < 1:
                continue
            ts = _parse_ts_maybe_ms(row[0])
            if ts is None:
                continue
            if first_gap_ts is None:
                first_gap_ts = int(ts)
            if len(row) >= 3:
                s = str(row[2]).strip()
            elif len(row) >= 2:
                s = str(row[1]).strip()
            else:
                s = ""
            if s == "":
                leading_nan_rows += 1
                continue
            first_valid_ts = int(ts)
            break
    return {
        "path": str(path),
        "leading_nan_rows": int(leading_nan_rows),
        "first_gap_ts": first_gap_ts,
        "first_valid_ts": first_valid_ts,
    }


def _collect_precomputed_boundary_nan_issues(ind_paths: Dict[str, List[str]]) -> List[dict[str, Any]]:
    issues: List[dict[str, Any]] = []
    for indicator_name, paths in (ind_paths or {}).items():
        if not paths or len(paths) <= 1:
            continue
        for path_index, path in enumerate(paths[1:], start=1):
            head = _scan_precomputed_file_head(path)
            if int(head.get("leading_nan_rows", 0) or 0) <= 0:
                continue
            head["indicator"] = str(indicator_name)
            head["path_index"] = int(path_index)
            issues.append(head)
            break
    return issues


def _is_truthy_env_flag(name: str) -> bool:
    try:
        return str(os.getenv(name, "") or "").strip().lower() in ("1", "true", "yes", "y", "on")
    except Exception:
        return False


def _normalize_precomputed_symbol_token(symbol: str) -> str:
    return str(symbol or "").replace("/", "").replace(":", "").replace("-", "").strip().upper()


def _parse_precomputed_filename_meta(path: str) -> dict[str, Any]:
    base = os.path.basename(str(path))
    m = re.match(r"^(?P<indicator>[^-]+)-(?P<symbol>.+)-(?P<tf>\d+[mh])-(?P<year>\d{4})-(?P<month>\d{2})\.csv$", base, re.IGNORECASE)
    if not m:
        return {}
    return {
        "indicator": str(m.group("indicator") or "").strip(),
        "symbol": str(m.group("symbol") or "").strip(),
        "tf": str(m.group("tf") or "").strip().lower(),
        "year": int(m.group("year")),
        "month": int(m.group("month")),
    }


def _scan_precomputed_path_diag(path: str, *, expected_symbol: str, expected_tf: str) -> dict[str, int]:
    diag = {
        "unparseable": 0,
        "tmp_selected": 0,
        "symbol_tf_mismatch": 0,
        "required_columns_missing": 0,
        "parse_error": 0,
    }
    base = os.path.basename(str(path))
    if "tmp" in base.lower():
        diag["tmp_selected"] = 1
    meta = _parse_precomputed_filename_meta(path)
    if not meta:
        diag["unparseable"] = 1
    else:
        symbol_ok = (
            _normalize_precomputed_symbol_token(str(meta.get("symbol", "")))
            == _normalize_precomputed_symbol_token(expected_symbol)
        )
        tf_ok = str(meta.get("tf", "")).lower() == str(expected_tf or "").strip().lower()
        if (not symbol_ok) or (not tf_ok):
            diag["symbol_tf_mismatch"] = 1
    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            r = csv.reader(f)
            first = next(r, None)
    except Exception:
        diag["parse_error"] = 1
        return diag
    if first is None:
        diag["parse_error"] = 1
        return diag
    lower = [str(tok or "").strip().lower() for tok in first]
    header_like = any(tok in ("ts", "timestamp", "ts_iso", "value", "time", "open_time") for tok in lower)
    if header_like:
        has_ts = any(tok in ("ts", "timestamp", "time", "open_time") for tok in lower)
        has_value = any(tok == "value" for tok in lower)
        if (not has_ts) or (not has_value):
            diag["required_columns_missing"] = 1
    return diag


def _collect_precomputed_path_diagnostics(
    *,
    ind_paths: Dict[str, List[str]],
    symbol: str,
    tf: str,
) -> Dict[str, Dict[str, int]]:
    out: Dict[str, Dict[str, int]] = {}
    for indicator_name, paths in (ind_paths or {}).items():
        agg = {
            "unparseable": 0,
            "tmp_selected": 0,
            "symbol_tf_mismatch": 0,
            "required_columns_missing": 0,
            "parse_error": 0,
        }
        for path in (paths or []):
            diag = _scan_precomputed_path_diag(
                path,
                expected_symbol=str(symbol),
                expected_tf=str(tf),
            )
            for key in agg:
                agg[key] += int(diag.get(key, 0) or 0)
        out[str(indicator_name)] = agg
    return out


def _log_precomputed_selection_diagnostics(
    *,
    scope: str,
    symbol: str,
    tf: str,
    ind_paths: Dict[str, List[str]],
    missing: List[str],
    gap_issues: List[dict[str, Any]],
    path_diags: Dict[str, Dict[str, int]],
    enabled: bool,
) -> None:
    parts: List[str] = []
    gap_by_indicator = {
        str(issue.get("indicator", "")): issue
        for issue in (gap_issues or [])
    }
    for indicator_name in sorted(set(list((ind_paths or {}).keys()) + list(missing or []))):
        selected_paths = list((ind_paths or {}).get(indicator_name, []) or [])
        issue = gap_by_indicator.get(str(indicator_name))
        diag = dict((path_diags or {}).get(indicator_name, {}) or {})
        gap_file = os.path.basename(str(issue.get("path", ""))) if issue else ""
        gap_rows = int(issue.get("leading_nan_rows", 0) or 0) if issue else 0
        first_gap_ts = issue.get("first_gap_ts") if issue else None
        first_valid_ts = issue.get("first_valid_ts") if issue else None
        parts.append(
            (
                f"{indicator_name}:selected={len(selected_paths)}"
                f",file_missing={'1' if indicator_name in missing else '0'}"
                f",unparseable={int(diag.get('unparseable', 0) or 0)}"
                f",parse_error={int(diag.get('parse_error', 0) or 0)}"
                f",required_columns_missing={int(diag.get('required_columns_missing', 0) or 0)}"
                f",tmp_selected={int(diag.get('tmp_selected', 0) or 0)}"
                f",symbol_tf_mismatch={int(diag.get('symbol_tf_mismatch', 0) or 0)}"
                f",leading_nan_detected={'1' if issue else '0'}"
                f",boundary_gap_rows={gap_rows}"
                f",gap_file={gap_file or '-'}"
                f",first_gap_ts={int(first_gap_ts) if first_gap_ts is not None else '-'}"
                f",first_valid_ts={int(first_valid_ts) if first_valid_ts is not None else '-'}"
            )
        )
    logger.info(
        "[PRECOMPUTED][%s][DIAG] symbol=%s tf=%s enabled=%s details=%s",
        str(scope).upper(),
        str(symbol),
        str(tf),
        int(bool(enabled)),
        " | ".join(parts) if parts else "no-indicators",
    )


def _log_precomputed_boundary_nan_issues(*, scope: str, symbol: str, tf: str, issues: List[dict[str, Any]]) -> None:
    for issue in issues:
        gap_ts = issue.get("first_gap_ts")
        valid_ts = issue.get("first_valid_ts")
        logger.warning(
            "[PRECOMPUTED][%s] symbol=%s tf=%s indicator=%s file=%s path_index=%s leading_nan_rows=%s first_gap_ts=%s(%s) first_valid_ts=%s(%s) -> fallback to on-the-fly calc",
            str(scope).upper(),
            str(symbol),
            str(tf),
            str(issue.get("indicator", "")),
            os.path.basename(str(issue.get("path", ""))),
            int(issue.get("path_index", 0) or 0),
            int(issue.get("leading_nan_rows", 0) or 0),
            (int(gap_ts) if gap_ts is not None else None),
            iso_utc(int(gap_ts)) if gap_ts is not None else "",
            (int(valid_ts) if valid_ts is not None else None),
            iso_utc(int(valid_ts)) if valid_ts is not None else "",
        )

def _build_series_from_map(
    ts_list: List[int],
    mp: Dict[int, float],
) -> List[float]:
    return [float(mp.get(int(t), float("nan"))) for t in ts_list]


def _validate_series_nan(
    name: str,
    series: Union[List[float], np.ndarray],
    warmup_bars: int,
    strict: bool,
) -> None:
    if not strict:
        return

    # Fast path for numpy arrays
    if isinstance(series, np.ndarray):
        wb = max(0, int(warmup_bars))
        if wb < len(series):
            tail = series[wb:]
            if np.isnan(tail).any():
                bad = int(np.where(np.isnan(tail))[0][0] + wb)
                raise RuntimeError(
                    f"NaN encountered while writing strict output: {name} idx={bad}"
                )
        return
    wb = max(0, int(warmup_bars))
    for i, v in enumerate(series):
        if i < wb:
            continue
        if v != v:  # NaN
            raise RuntimeError(f"precomputed indicator has NaN beyond warmup: {name} idx={i} warmup={wb}")


def _resample_ohlcv(rows_1m: list[list[float]], tf: str) -> list[list[float]]:
    """
    Resample 1m rows into tf (e.g., 5m/1h).
    rows_1m must be sorted by ts asc.
    Output rows: [[bucket_ts, open, high, low, close, vol], ...]
    bucket_ts is the bucket start time (ms).
    """
    if not rows_1m:
        return []

    step = _tf_to_ms(tf)
    out: list[list[float]] = []

    cur_bucket = None
    o = h = l = c = v = None

    for r in rows_1m:
        ts = int(r[0])
        b = (ts // step) * step

        if cur_bucket is None:
            cur_bucket = b
            o = float(r[1])
            h = float(r[2])
            l = float(r[3])
            c = float(r[4])
            v = float(r[5])
            continue

        if b != cur_bucket:
            out.append([int(cur_bucket), float(o), float(h), float(l), float(c), float(v)])
            cur_bucket = b
            o = float(r[1])
            h = float(r[2])
            l = float(r[3])
            c = float(r[4])
            v = float(r[5])
        else:
            # same bucket
            hh = float(r[2])
            ll = float(r[3])
            cc = float(r[4])
            vv = float(r[5])

            if hh > float(h):
                h = hh
            if ll < float(l):
                l = ll
            c = cc
            v = float(v) + vv

    # flush last
    if cur_bucket is not None:
        out.append([int(cur_bucket), float(o), float(h), float(l), float(c), float(v)])

    return out


def _kill_switch_check(ctx: Dict[str, Any]) -> tuple[bool, str]:
    if not bool(ctx.get("enabled", False)):
        return False, ""

    def _f(v: Any, default: float = 0.0) -> float:
        try:
            x = float(v)
            if not math.isfinite(x):
                return float(default)
            return float(x)
        except Exception:
            return float(default)

    def _i(v: Any, default: int = 0) -> int:
        try:
            return int(v)
        except Exception:
            return int(default)

    equity = _f(ctx.get("equity"), 0.0)
    peak_equity = _f(ctx.get("peak_equity"), 0.0)
    day_pnl = _f(ctx.get("day_pnl"), 0.0)
    day_start_equity = _f(ctx.get("day_start_equity"), 0.0)
    consec_losses = _i(ctx.get("consec_losses"), 0)
    spread_bps_raw = ctx.get("spread_bps")
    spread_bps = _f(spread_bps_raw, 0.0) if spread_bps_raw is not None else None

    kill_max_dd_pct = max(0.0, _f(ctx.get("kill_max_dd_pct"), 0.0))
    kill_max_daily_loss_pct = max(0.0, _f(ctx.get("kill_max_daily_loss_pct"), 0.0))
    kill_max_consec_losses = max(0, _i(ctx.get("kill_max_consec_losses"), 0))
    kill_max_spread_bps = max(0.0, _f(ctx.get("kill_max_spread_bps"), 0.0))
    kill_min_equity = max(0.0, _f(ctx.get("kill_min_equity"), 0.0))

    dd_pct = 0.0
    if peak_equity > 0.0:
        dd_pct = max(0.0, (peak_equity - equity) / peak_equity)
    daily_loss_pct = 0.0
    if day_start_equity > 0.0 and day_pnl < 0.0:
        daily_loss_pct = max(0.0, (-day_pnl) / day_start_equity)
    ctx["dd_pct_calc"] = float(dd_pct)
    ctx["daily_loss_pct_calc"] = float(daily_loss_pct)

    if kill_max_dd_pct > 0.0 and dd_pct >= kill_max_dd_pct:
        return True, f"max_dd_pct({dd_pct:.6f}>={kill_max_dd_pct:.6f})"
    if kill_max_daily_loss_pct > 0.0 and daily_loss_pct >= kill_max_daily_loss_pct:
        return True, f"daily_loss_pct({daily_loss_pct:.6f}>={kill_max_daily_loss_pct:.6f})"
    if kill_max_consec_losses > 0 and consec_losses >= kill_max_consec_losses:
        return True, f"max_consec_losses({consec_losses}>={kill_max_consec_losses})"
    if kill_max_spread_bps > 0.0 and spread_bps is not None and spread_bps >= kill_max_spread_bps:
        return True, f"max_spread_bps({spread_bps:.3f}>={kill_max_spread_bps:.3f})"
    if kill_min_equity > 0.0 and equity <= kill_min_equity:
        return True, f"min_equity({equity:.6f}<={kill_min_equity:.6f})"
    return False, ""

def _ema9_break_recent_age(close_arr, ema9_arr, i: int, recent_bars: int, buf_bps: float) -> int | None:
    """
    Strict EMA9 break recent:
      prev_close <= prev_ema9 AND now_close > now_ema9*(1+buf)
    Return age in bars (0=current bar, 1=prev, ...) if found within recent_bars window.
    """
    try:
        rb = int(recent_bars)
    except Exception:
        rb = 0
    if rb <= 0:
        return None
    i = int(i)
    start = max(1, i - rb + 1)
    for k in range(i, start - 1, -1):
        try:
            c0 = float(close_arr[k - 1]); e0 = float(ema9_arr[k - 1])
            c1 = float(close_arr[k]);     e1 = float(ema9_arr[k])
            if (c0 <= e0) and (c1 > e1 * (1.0 + float(buf_bps) / 10000.0)):
                return int(i - k)
        except Exception:
            continue
    return None

@dataclass
class Position:
    symbol: str
    entry_raw: float
    entry_exec: float
    qty: float
    stop: float
    tp: float
    stop_raw: float
    tp_raw: float
    opened_ts: int
    regime: str
    direction: str
    rr0: float | None
    rr_adj: float | None
    init_stop: float
    max_fav: float  # max favorable price seen (high)
    min_adv: float  # min adverse price seen (low)
    mfe_abs: float = 0.0
    giveback_max_abs: float = 0.0
    # BE/TRAIL diagnostics (to understand BE dependency)
    be_triggered: bool = False
    be_trigger_r: float | None = None
    be_offset_bps: float | None = None
    be_stop_set: float | None = None
    trail_triggered: bool = False
    trail_start_r: float | None = None
    trail_bps_from_high: float | None = None
    start_price: float | None = None
    trail_eval_count: int = 0
    trail_candidate_stop_last: float | None = None
    trail_candidate_stop_max: float | None = None
    trail_candidate_minus_current_stop: float | None = None
    trail_candidate_minus_current_stop_max: float | None = None
    trail_candidate_from_atr_last: float | None = None
    trail_candidate_from_bps_last: float | None = None
    trail_eligible_count: int = 0
    trail_update_count: int = 0
    trail_block_reason_last: str = ""
    trail_block_reason_max: str = ""
    trail_start_price_last: float | None = None
    trail_start_price_max_context: float | None = None
    trail_bar_high_last: float | None = None
    trail_bar_high_max: float | None = None
    trail_pos_stop_before_last: float | None = None
    trail_pos_stop_before_max_context: float | None = None
    trail_risk_per_unit_last: float | None = None
    trail_mode_last: str = "none"
    qty_init: float = 0.0
    tp1_exit_notional: float = 0.0
    # Partial take-profit (TP1) bookkeeping (single trade, partial realized PnL + remaining position).
    tp1_done: bool = False
    realized_pnl: float = 0.0
    realized_fees: float = 0.0
    stop_kind: str = ""  # "", "be", "trail" (for STOP_HIT attribution)
    # Entry-time context (for conditional exits like TIMEOUT)
    ema9_break_recent: bool = False
    ema9_break_age: int | None = None
    # Diagnostics snapshots
    entry_i: int = 0
    entry_ts: int = 0
    entry_filter_j: int = 0
    entry_diag: dict | None = None
    _trail_trace_reason_last: str = ""
    _trail_check_emitted: bool = False


def _same_bar_reentry_allowed_exit_reason(exit_reason: str) -> bool:
    reason = str(exit_reason or "")
    if not reason:
        return False
    if reason in ("RANGE_EARLY_LOSS_ATR", "RANGE_EMA9_CROSS_EXIT"):
        return True
    if reason.startswith("RANGE_TIMEOUT(") or reason.startswith("TREND_TIMEOUT("):
        return True
    return False


def _normalize_position_direction(direction: object) -> str:
    d = str(direction or "").strip().lower()
    if d in ("sell", "short"):
        return "short"
    return "long"


def _position_entry_exec_or_raw(pos: object) -> float:
    try:
        entry_exec = float(getattr(pos, "entry_exec", 0.0) or 0.0)
        if entry_exec > 0.0:
            return entry_exec
    except Exception:
        pass
    try:
        return float(getattr(pos, "entry_raw", 0.0) or 0.0)
    except Exception:
        return 0.0


def _ts_fields(ts_ms: int) -> dict:
    """Return UTC/JST time fields for diagnostics."""
    try:
        ts_ms = int(ts_ms)
        dt_utc = datetime.datetime.utcfromtimestamp(ts_ms / 1000.0)
        dt_jst = dt_utc + datetime.timedelta(hours=9)
        return {
            "ts": ts_ms,
            "utc": dt_utc.isoformat() + "Z",
            "utc_hour": int(dt_utc.hour),
            "utc_wday": int(dt_utc.weekday()),
            "jst": dt_jst.isoformat() + "+09:00",
            "jst_hour": int(dt_jst.hour),
            "jst_wday": int(dt_jst.weekday()),
        }
    except Exception:
        return {"ts": ts_ms}


def _diag_snapshot_basic(
    ts_ms: int,
    close: float,
    ema9: float,
    ema21: float,
    atr: float,
    rsi: float,
    filter_regime: str | None = None,
    filter_dir: str | None = None,
    label: str | None = None,
) -> dict:
    """Small, stable snapshot used for STOP_HIT_LOSS pattern mining."""
    try:
        c = float(close)
    except Exception:
        c = 0.0

    def _bps(x: float) -> float:
        try:
            return float(x) / c * 10000.0 if c else 0.0
        except Exception:
            return 0.0

    out = {}
    if label is not None:
        out["label"] = str(label)
    out.update(_ts_fields(ts_ms))
    out.update(
        {
            "close": float(close),
            "atr": float(atr),
            "atr_bps": _bps(float(atr)),
            "rsi14": float(rsi),
            "ema9": float(ema9),
            "ema21": float(ema21),
            "ema9_over_ema21": bool(float(ema9) >= float(ema21)),
            "ema9_dist_bps": _bps(float(close) - float(ema9)),
            "ema21_dist_bps": _bps(float(close) - float(ema21)),
        }
    )
    if filter_regime is not None:
        out["filter_regime"] = str(filter_regime)
    if filter_dir is not None:
        out["filter_dir"] = str(filter_dir)
    return out


def run_backtest(
    symbols: List[str],
    since_ms: Optional[int],
    until_ms: Optional[int],
    entry_tf: str,
    filter_tf: str,
    warmup_bars: int,
    initial_equity: float,
    dataset_year: Optional[int] = None,
    since_year: Optional[int] = None,
    until_year: Optional[int] = None,
    export_csv: bool = True,
    export_diff_trace: bool = True,
    diff_trace_prefix: Optional[str] = None,
    diff_trace_source: str = "backtest",
    diff_trace_mode: str = "BACKTEST",
    recent_bars_entry: Optional[int] = None,
    recent_bars_filter: Optional[int] = None,
    DEBUG_RANGE_ENTRY: float = 0.0,
    RANGE_ENTRY_DIAG_LIMIT: float = 20.0,
    # When provided, we will *load* data from since_ms but we will *start trading* only from trade_since_ms.
    # This is used to preload enough history (warmup / filter window) while keeping the evaluation window strict.
    trade_since_ms: Optional[int] = None,
    run_id: Optional[str] = None,
    export_dir: Optional[str] = None,
    perf_debug: bool = False,
) -> Dict[str, Any]:
    global _CURRENT_EXPORT_DIR
    # --- startup identity log (once per run) ---
    precomputed_debug_enabled = bool(perf_debug or _is_truthy_env_flag("LWF_PRECOMPUTED_DEBUG"))
    spread_bps_est, _spread_bps_env_override = _resolve_backtest_spread_bps()
    try:
        logging.getLogger(__name__).info(
            f"[BUILD] backtest.py loaded: file={__file__} BUILD_ID={BUILD_ID}"
        )
    except Exception:
        pass
    context_symbol = str(symbols[0] if symbols else getattr(C, "BACKTEST_CSV_SYMBOL", "ETH/USDT") or "ETH/USDT").strip()
    if str(export_dir or "").strip():
        resolved_dir = Path(str(export_dir)).expanduser().resolve()
        resolved_dir.mkdir(parents=True, exist_ok=True)
        _activate_export_context(
            run_id=str(resolve_run_id(run_id, env_key="LWF_RUN_ID")),
            symbol=context_symbol,
            mode="BACKTEST",
        )
        _CURRENT_EXPORT_DIR = str(resolved_dir)
        os.environ["LWF_EXPORT_DIR"] = str(resolved_dir)
        logger.info("[results] export_dir=%s run_id=%s symbol=%s mode=%s", str(resolved_dir), str(_CURRENT_RUN_ID), str(context_symbol), "BACKTEST")
        try:
            diff_dir_cfg = str(getattr(C, "DIFF_TRACE_DIR", "") or "").strip()
            if is_legacy_exports_path(diff_dir_cfg):
                setattr(C, "DIFF_TRACE_DIR", str(resolved_dir))
        except Exception:
            pass
    else:
        _activate_export_context(
            run_id=str(resolve_run_id(run_id, env_key="LWF_RUN_ID")),
            symbol=context_symbol,
            mode="BACKTEST",
        )
    _write_last_run_reference(mode="BACKTEST", extra={"entry_tf": str(entry_tf), "filter_tf": str(filter_tf)})

    dataset_years: Optional[List[int]] = None
    has_since_year = since_year is not None
    has_until_year = until_year is not None
    if has_since_year != has_until_year:
        raise ValueError("--since-year and --until-year must be provided together.")
    if has_since_year and has_until_year:
        sy = int(since_year)
        uy = int(until_year)
        if sy > uy:
            raise ValueError(f"--since-year must be <= --until-year (got {sy}>{uy}).")
        if dataset_year is not None:
            raise ValueError(
                "--since-year/--until-year cannot be combined with dataset_year. "
                "Use either single-year dataset_year or a multi-year range."
            )
        if (since_ms is not None) or (until_ms is not None):
            raise ValueError(
                "--since-year/--until-year cannot be combined with since_ms/until_ms. "
                "Use one time-range method."
            )
        dataset_years = list(range(sy, uy + 1))
        since_ms = int(datetime(sy, 1, 1, tzinfo=timezone.utc).timestamp() * 1000.0)
        until_ms = int(datetime(uy + 1, 1, 1, tzinfo=timezone.utc).timestamp() * 1000.0)
        logger.info(
            "[BACKTEST][DATASET_RANGE] continuous_years=%s..%s symbols=%s since_ms=%s until_ms=%s",
            sy,
            uy,
            ",".join(map(str, symbols)),
            since_ms,
            until_ms,
        )

    logger.info(f"CONFIG_PATH: {getattr(C, '__file__', None)}")
    logger.info(
        "CONFIG_VALUES: "
        f"TREND_SL_ATR_K={getattr(C,'TREND_SL_ATR_K',None)} "
        f"TREND_TP_ATR_K={getattr(C,'TREND_TP_ATR_K',None)} "
        f"EMA_DIR_BUFFER_BPS={getattr(C,'EMA_DIR_BUFFER_BPS',None)} "
        f"TREND_BREAKOUT_BUFFER_BPS={getattr(C,'TREND_BREAKOUT_BUFFER_BPS',None)} "
        f"TREND_ENTRY_MODE={getattr(C,'TREND_ENTRY_MODE',None)} "
        f"TRADE_RANGE={getattr(C,'TRADE_RANGE',None)} "
        f"ALLOW_RANGE_TRADES={getattr(C,'ALLOW_RANGE_TRADES',None)}"
    )
    # --- determine effective since (ms) ---
    lookback_days = int(getattr(C, "BACKTEST_LOOKBACK_DAYS", 0) or 0)
    hold_counter = _Counter()
    # Preload-history mode (for runner --replay --replay-engine backtest):
    # If the caller provides a tight 1-day range, we still need enough prior history
    # to satisfy warmup and 1h filter window (BT_FILTER_WINDOW>=60).
    # We load earlier data, but start trading only from trade_since_ms.
    orig_since_ms = since_ms
    if trade_since_ms is None:
        trade_since_ms = orig_since_ms
    try:
        preload_hours = int(getattr(C, "BACKTEST_PRELOAD_HOURS", 72) or 72)
    except Exception:
        preload_hours = 72
    if (orig_since_ms is not None) and (until_ms is not None) and preload_hours > 0:
        try:
            data_since_ms = max(0, int(orig_since_ms) - int(preload_hours) * 60 * 60 * 1000)
            if int(data_since_ms) < int(orig_since_ms):
                since_ms = int(data_since_ms)
                logger.info(
                    "BACKTEST PRELOAD: trade_since_ms=%s (%s) data_since_ms=%s preload_hours=%s",
                    int(trade_since_ms or 0),
                    iso_utc(int(trade_since_ms or 0)),
                    int(since_ms or 0),
                    int(preload_hours),
                )
        except Exception:
            pass

    # Track buy-side rejects once a signal reaches entry evaluation.
    buy_reject = _Counter()
    pullback_funnel = _Counter()
    # RANGE_ENTRY_DIAG gate (from CLI kwargs)
    debug_range_entry = bool(DEBUG_RANGE_ENTRY)
    diag_limit = int(RANGE_ENTRY_DIAG_LIMIT) if RANGE_ENTRY_DIAG_LIMIT is not None else 20
    diag_limit = max(0, min(diag_limit, 1000000))
    range_entry_diag_printed = 0
    try:
        open_cost_diag_limit = int(
            getattr(C, "BACKTEST_OPEN_COST_DIAG_LIMIT", OPEN_COST_DIAG_LIMIT_DEFAULT) or OPEN_COST_DIAG_LIMIT_DEFAULT
        )
    except Exception:
        open_cost_diag_limit = int(OPEN_COST_DIAG_LIMIT_DEFAULT)
    open_cost_diag_limit = max(0, min(open_cost_diag_limit, 1000000))
    open_cost_diag_printed = 0
    size_sizing_diag_limit = 8
    size_sizing_diag_printed = 0
    state_kv: Dict[str, Dict[str, Any]] = {}
    exchange_id = (os.getenv("LWF_EXCHANGE_ID") or getattr(C, "EXCHANGE_ID", "mexc")).strip().lower() or "mexc"
    meta_symbol = str(symbols[0] if symbols else getattr(C, "BACKTEST_CSV_SYMBOL", "ETH/USDT") or "ETH/USDT").strip()
    market_meta, market_meta_source, market_meta_cache_path = _load_backtest_market_meta(exchange_id, meta_symbol)
    market_meta_source_kind = _market_meta_source_kind(market_meta_source)
    if market_meta is not None:
        try:
            spread_bps_est = float(getattr(market_meta, "spread_bps", spread_bps_est) or spread_bps_est)
            if os.getenv("BACKTEST_SPREAD_BPS") in (None, ""):
                setattr(C, "BACKTEST_SPREAD_BPS", float(spread_bps_est))
            logger.info(
                "[MARKET_META] mode=backtest exchange_id=%s symbol=%s quote=%s maker=%.6f taker=%.6f spread_bps=%.4f source_kind=%s source=%s cache=%s",
                exchange_id,
                str(getattr(market_meta, "symbol", meta_symbol) or meta_symbol),
                str(getattr(market_meta, "quote_ccy", "") or ""),
                float(getattr(market_meta, "maker_fee_rate", 0.0) or 0.0),
                float(getattr(market_meta, "taker_fee_rate", 0.0) or 0.0),
                float(spread_bps_est),
                str(market_meta_source_kind),
                str(market_meta_source),
                str(market_meta_cache_path),
            )
        except Exception:
            pass
    else:
        logger.info(
            "[MARKET_META] mode=backtest exchange_id=%s symbol=%s unavailable source_kind=%s source=%s cache=%s",
            exchange_id,
            meta_symbol,
            str(market_meta_source_kind),
            str(market_meta_source),
            str(market_meta_cache_path),
        )
    fee_maker_rate, fee_taker_rate = resolve_paper_fees(exchange_id)
    if market_meta is not None:
        try:
            fee_maker_rate = float(getattr(market_meta, "maker_fee_rate", fee_maker_rate) or fee_maker_rate)
            fee_taker_rate = float(getattr(market_meta, "taker_fee_rate", fee_taker_rate) or fee_taker_rate)
        except Exception:
            pass
    fee_maker_rate = float(fee_maker_rate)
    fee_taker_rate = float(fee_taker_rate)
    if exchange_id == "coincheck":
        if fee_taker_rate < 0.0:
            fee_taker_rate = 0.0
        if fee_maker_rate < 0.0:
            fee_maker_rate = fee_taker_rate
    elif fee_taker_rate <= 0.0:
        fee_taker_rate = 0.0002
    if exchange_id != "coincheck" and fee_maker_rate <= 0.0:
        fee_maker_rate = fee_taker_rate

    # --- Live expectancy (using realized exits so far) ---
    # We use this to gate entries (BUY_REJECT) with the same exit reasons we summarize at the end.
    exit_bps_live_sum: Dict[Tuple[str, str], float] = {}
    exit_bps_live_n: Dict[Tuple[str, str], int] = {}

    def _norm_exit_reason(reason: str) -> str:
        r = str(reason)
        if r.startswith("RANGE_TIMEOUT") or r.startswith("TREND_TIMEOUT"):
            return r.split("(")[0]
        return r

    taker_fee_rate_gate = float(fee_taker_rate)
    fee_bps_round_gate = 2.0 * taker_fee_rate_gate * 10000.0
    open_cost_slippage_bps = 2.0 * max(0.0, float(getattr(C, "SLIPPAGE_BPS", 0.0) or 0.0))

    EXPECTANCY_REASONS_RANGE = (
        "TP_HIT",
        "STOP_HIT_LOSS",
        "STOP_HIT_PROFIT",
        "RANGE_EARLY_LOSS_ATR",
        "RANGE_EMA9_CROSS_EXIT",
        "RANGE_TIMEOUT",
    )
    EXPECTANCY_REASONS_TREND = (
        "TP_HIT",
        "STOP_HIT_LOSS",
        "STOP_HIT_PROFIT",
        "TREND_TIMEOUT",
    )

    def _live_expectancy_net_bps(reg: str) -> Tuple[float, int]:
        reg0 = str(reg)
        reasons = EXPECTANCY_REASONS_RANGE if reg0 == "range" else EXPECTANCY_REASONS_TREND
        s = 0.0
        n = 0
        for rr in reasons:
            k = (reg0, rr)
            if k in exit_bps_live_n and exit_bps_live_n[k] > 0:
                s += float(exit_bps_live_sum.get(k, 0.0))
                n += int(exit_bps_live_n.get(k, 0))
        if n <= 0:
            return (float("nan"), 0)
        exp = s / float(n)
        return (float(exp - fee_bps_round_gate), int(n))

    fast_bt = bool(getattr(C, "FAST_BACKTEST", False))
    fast_skip_adj = bool(getattr(C, "FAST_SKIP_ADJUST_TP_SL", True))
    fast_skip_exp = bool(getattr(C, "FAST_SKIP_EXPECTANCY", True))

    # =========================================================
    # (B) Break-even disable controls for backtest parity.
    # - BACKTEST_DISABLE_BE keeps the legacy config path working.
    # - BACKTEST_FORCE_DISABLE_BE is the preferred explicit switch.
    # =========================================================
    force_disable_be = bool(
        getattr(C, "BACKTEST_FORCE_DISABLE_BE", getattr(C, "BACKTEST_DISABLE_BE", False))
    )
    R._log_effective_range_config(
        logger,
        label="BACKTEST",
        force_disable_be=bool(force_disable_be),
        tp1_requires_flag=False,
    )

    # Fast-path fee rate used by the lightweight equity path.
    fast_fee_rate = getattr(C, "FAST_PAPER_FEE_RATE", None)
    if fast_fee_rate is None:
        fast_fee_rate = float(fee_taker_rate)
    else:
        fast_fee_rate = float(fast_fee_rate)

    def _hold(reason: str):
        try:
            hold_counter[str(reason)] += 1
        except Exception:
            hold_counter['unknown'] += 1

    # =========================
    # Sales risk controls (Backtest)
    # =========================
    _JST = timezone(timedelta(hours=9))
    _risk_daily: dict[str, dict[str, object]] = {}
    _risk_weekly: dict[str, dict[str, object]] = {}

    def _risk_day_key_from_ts_ms(ts_ms: int) -> str:
        try:
            dt = datetime.fromtimestamp(float(ts_ms) / 1000.0, tz=_JST)
        except Exception:
            dt = datetime.now(_JST)
        return dt.strftime("%Y-%m-%d")

    def _risk_week_key_from_ts_ms(ts_ms: int) -> str:
        try:
            dt = datetime.fromtimestamp(float(ts_ms) / 1000.0, tz=_JST)
        except Exception:
            dt = datetime.now(_JST)
        iso = dt.isocalendar()
        return f"{int(iso.year):04d}-W{int(iso.week):02d}"

    def _risk_allow_new_entry(ts_ms: int) -> tuple[bool, str]:
        if not bool(getattr(C, "RISK_CONTROLS_ENABLED", True)):
            return True, ""
        day_key = _risk_day_key_from_ts_ms(int(ts_ms))
        risk = _risk_daily.get(day_key) or {"daily_pnl_jpy": 0.0, "loss_streak": 0, "halted": False, "halt_reason": ""}

        # weekly stop-loss (backtest-local)
        week_key = _risk_week_key_from_ts_ms(int(ts_ms))
        weekly = _risk_weekly.get(week_key) or {"weekly_pnl_jpy": 0.0, "halted": False, "halt_reason": ""}
        if bool(weekly.get("halted", False)):
            return False, str(weekly.get("halt_reason") or "weekly_risk_halted")
        weekly_pnl = float(weekly.get("weekly_pnl_jpy", 0.0) or 0.0)
        max_week_loss_jpy = getattr(C, "RISK_WEEKLY_STOP_LOSS_JPY", None)
        if max_week_loss_jpy is None:
            pct = float(getattr(C, "RISK_WEEKLY_STOP_LOSS_PCT", 0.0) or 0.0)
            if pct and pct > 0:
                max_week_loss_jpy = float(getattr(C, "INITIAL_EQUITY", 0.0)) * (pct / 100.0)
        if max_week_loss_jpy is not None and float(max_week_loss_jpy) > 0:
            if weekly_pnl <= -float(max_week_loss_jpy):
                weekly["halted"] = True
                weekly["halt_reason"] = f"weekly_stop_loss_hit(pnl={weekly_pnl:.2f}<=-{float(max_week_loss_jpy):.2f} week={week_key})"
                _risk_weekly[week_key] = weekly
                return False, str(weekly["halt_reason"])

        if bool(risk.get("halted", False)):
            return False, str(risk.get("halt_reason") or "risk_halted")
        daily_pnl = float(risk.get("daily_pnl_jpy", 0.0) or 0.0)

        max_loss_jpy = getattr(C, "RISK_DAILY_STOP_LOSS_JPY", None)
        if max_loss_jpy is None:
            pct = float(getattr(C, "RISK_DAILY_STOP_LOSS_PCT", 0.0) or 0.0)
            if pct and pct > 0:
                max_loss_jpy = float(getattr(C, "INITIAL_EQUITY", 0.0)) * (pct / 100.0)

        if max_loss_jpy is not None and float(max_loss_jpy) > 0:
            if daily_pnl <= -float(max_loss_jpy):
                risk["halted"] = True
                risk["halt_reason"] = f"daily_stop_loss_hit(pnl={daily_pnl:.2f}<=-{float(max_loss_jpy):.2f})"
                _risk_daily[day_key] = risk
                return False, str(risk["halt_reason"])

        max_streak = int(getattr(C, "RISK_MAX_CONSECUTIVE_LOSSES", 0) or 0)
        if max_streak and max_streak > 0:
            streak = int(risk.get("loss_streak", 0) or 0)
            if streak >= max_streak:
                risk["halted"] = True
                risk["halt_reason"] = f"max_consecutive_losses_hit(streak={streak}>=max={max_streak})"
                _risk_daily[day_key] = risk
                return False, str(risk["halt_reason"])

        _risk_daily[day_key] = risk
        return True, ""

    def _risk_on_trade_closed(ts_ms: int, net_jpy: float) -> None:
        if not bool(getattr(C, "RISK_CONTROLS_ENABLED", True)):
            return
        day_key = _risk_day_key_from_ts_ms(int(ts_ms))
        risk = _risk_daily.get(day_key) or {"daily_pnl_jpy": 0.0, "loss_streak": 0, "halted": False, "halt_reason": ""}
        risk["daily_pnl_jpy"] = float(risk.get("daily_pnl_jpy", 0.0) or 0.0) + float(net_jpy)
        if float(net_jpy) < 0:
            risk["loss_streak"] = int(risk.get("loss_streak", 0) or 0) + 1
        else:
            risk["loss_streak"] = 0
        _risk_daily[day_key] = risk

        week_key = _risk_week_key_from_ts_ms(int(ts_ms))
        weekly = _risk_weekly.get(week_key) or {"weekly_pnl_jpy": 0.0, "halted": False, "halt_reason": ""}
        weekly["weekly_pnl_jpy"] = float(weekly.get("weekly_pnl_jpy", 0.0) or 0.0) + float(net_jpy)
        _risk_weekly[week_key] = weekly

    _kill_state: dict[str, object] = {"halted": False, "reason": "", "cooldown_until_day": ""}
    _kill_day_start_equity: dict[str, float] = {}

    def _kill_add_days(day_key: str, plus_days: int) -> str:
        try:
            dt = datetime.strptime(str(day_key), "%Y-%m-%d")
            dt2 = dt + timedelta(days=max(0, int(plus_days)))
            return dt2.strftime("%Y-%m-%d")
        except Exception:
            return str(day_key)

    def _kill_should_block_new_entries(
        *,
        ts_ms: int,
        equity_now: float,
        peak_equity: float,
        spread_bps: float | None,
    ) -> tuple[bool, str]:
        if not bool(getattr(C, "KILL_SWITCH_ENABLED", False)):
            return False, ""

        day_key = _risk_day_key_from_ts_ms(int(ts_ms))

        if bool(_kill_state.get("halted", False)):
            return True, str(_kill_state.get("reason") or "halted")

        cd_until = str(_kill_state.get("cooldown_until_day") or "")
        if cd_until and day_key <= cd_until:
            return True, f"cooldown(until={cd_until})"

        if day_key not in _kill_day_start_equity:
            base_eq = float(equity_now) if float(equity_now) > 0.0 else float(getattr(C, "INITIAL_EQUITY", 0.0) or 0.0)
            _kill_day_start_equity[day_key] = float(base_eq)
        day_start_equity = float(_kill_day_start_equity.get(day_key, 0.0) or 0.0)

        risk = _risk_daily.get(day_key) or {}
        day_pnl = float(risk.get("daily_pnl_jpy", 0.0) or 0.0)
        consec_losses = int(risk.get("loss_streak", 0) or 0)

        ctx: Dict[str, Any] = {
            "enabled": True,
            "equity": float(equity_now),
            "peak_equity": float(peak_equity),
            "day_pnl": float(day_pnl),
            "day_start_equity": float(day_start_equity),
            "consec_losses": int(consec_losses),
            "spread_bps": spread_bps,
            "kill_max_dd_pct": float(getattr(C, "KILL_MAX_DD_PCT", 0.0) or 0.0),
            "kill_max_daily_loss_pct": float(getattr(C, "KILL_MAX_DAILY_LOSS_PCT", 0.0) or 0.0),
            "kill_max_consec_losses": int(getattr(C, "KILL_MAX_CONSEC_LOSSES", 0) or 0),
            "kill_max_spread_bps": float(getattr(C, "KILL_MAX_SPREAD_BPS", 0.0) or 0.0),
            "kill_min_equity": float(getattr(C, "KILL_MIN_EQUITY", 0.0) or 0.0),
        }
        halt, reason = _kill_switch_check(ctx)

        if bool(getattr(C, "KILL_DEBUG_LOG_ENABLED", False)):
            logger.debug(
                "[KILL][DEBUG] enabled=%s day=%s dd=%.6f daily_loss_pct=%.6f daily_pnl=%.6f consec=%d spread_bps=%s equity=%.6f peak=%.6f",
                True,
                day_key,
                float(ctx.get("dd_pct_calc", 0.0) or 0.0),
                float(ctx.get("daily_loss_pct_calc", 0.0) or 0.0),
                float(day_pnl),
                int(consec_losses),
                f"{float(spread_bps):.3f}" if spread_bps is not None else "na",
                float(equity_now),
                float(peak_equity),
            )

        if not halt:
            return False, ""

        mode = str(getattr(C, "KILL_SWITCH_MODE", "HALT_NEW_ENTRIES") or "HALT_NEW_ENTRIES").strip().upper()
        logger.info(
            "[KILL] triggered: mode=%s reason=%s dd=%.6f daily_pnl=%.6f consec_losses=%d spread_bps=%s",
            mode,
            str(reason),
            float(ctx.get("dd_pct_calc", 0.0) or 0.0),
            float(day_pnl),
            int(consec_losses),
            f"{float(spread_bps):.3f}" if spread_bps is not None else "na",
        )

        if mode == "EXIT_PROCESS":
            raise SystemExit(f"[KILL] EXIT_PROCESS reason={reason}")

        cooldown_days = int(getattr(C, "KILL_COOLDOWN_DAYS", 0) or 0)
        if cooldown_days > 0:
            _kill_state["cooldown_until_day"] = _kill_add_days(day_key, max(0, cooldown_days - 1))
        else:
            _kill_state["halted"] = True
        _kill_state["reason"] = str(reason)
        return True, str(reason)

    # Compat helper: some diffs call _summ_reason(counter, reason)
    # Keep a local implementation so backtest never depends on runner's helper.
    def _summ_reason(counter, reason: str):
        try:
            counter[str(reason)] += 1
        except Exception:
            counter['unknown'] += 1

    def _track_pullback_funnel(action: str, reason: str | None) -> None:
        if not reason:
            return
        r = str(reason)
        if not (r.startswith("pullback_") or r.startswith("trend_pullback_entry")):
            return

        stage_a_expired = {
            "pullback_waiting:not_near_ema": "stageA_expired_not_near_ema",
            "pullback_waiting:setup_not_bearish": "stageA_expired_setup_not_bearish",
            "pullback_waiting:setup_close_above_ema": "stageA_expired_setup_close_above_ema",
        }
        stage_a_fail = {
            "pullback_fail:too_deep",
            "pullback_fail:nan_price_or_atr",
            "pullback_fail:nan_ema",
        }
        stage_b_fail = {
            "pullback_fail:weak_break",
            "pullback_fail:not_break_prev_high",
            "pullback_fail:bad_candle",
            "pullback_waiting:need_rebound_bull",
            "pullback_waiting:rebound_close_below_ema",
            "pullback_waiting:rebound_dist_atr",
            "pullback_waiting:no_break_setup_high",
            "pullback_waiting:weak_body",
            "pullback_waiting:upper_wick",
            "pullback_waiting:need_body_cross_ema",
            "pullback_fail:nan_rsi",
            "pullback_fail:rsi_low",
            "pullback_fail:weak_rebound_body",
            "pullback_fail:upper_wick",
            "pullback_waiting:not_break_ref_high",
        }
        break_eval = {
            "pullback_fail:weak_break",
            "pullback_fail:not_break_prev_high",
            "pullback_waiting:not_break_ref_high",
        }

        for prefix, bucket in stage_a_expired.items():
            if r.startswith(prefix):
                pullback_funnel[bucket] += 1
                pullback_funnel["stageA_fail"] += 1
                return
        for prefix in stage_a_fail:
            if r.startswith(prefix):
                pullback_funnel["stageA_fail"] += 1
                return

        for prefix in stage_b_fail:
            if r.startswith(prefix):
                pullback_funnel["stageB_reached"] += 1
                pullback_funnel["stageB_fail"] += 1
                # stageB fail reason breakdown (strip params)
                tag = reason.split(':', 1)[1] if ':' in reason else reason
                tag = tag.split('(', 1)[0]
                pullback_funnel[f"stageB_fail:{tag}"] += 1
                if any(r.startswith(b) for b in break_eval):
                    pullback_funnel["break_eval"] += 1
                    # break-eval reason breakdown (strip params)
                    tag = reason.split(':', 1)[1] if ':' in reason else reason
                    tag = tag.split('(', 1)[0]
                    pullback_funnel[f"break_eval:{tag}"] += 1
                return

        if r.startswith("trend_pullback_entry"):
            pullback_funnel["stageB_reached"] += 1
            pullback_funnel["buy_fired"] += 1
            pullback_funnel["buy_fired_pullback"] += 1
            return

    effective_trade_since_ms = trade_since_ms if (trade_since_ms is not None) else since_ms
    # --- log times ---
    now_dt = datetime.now(timezone.utc)
    logger.info(f"NOW: {now_dt.isoformat()}")

    if effective_trade_since_ms is not None:
        since_dt = datetime.fromtimestamp(effective_trade_since_ms / 1000.0, tz=timezone.utc)
        extra = ""
        try:
            if (effective_since_ms is not None) and int(effective_since_ms) != int(effective_trade_since_ms):
                extra = f" data_since_ms={int(effective_since_ms)}"
        except Exception:
            extra = ""
        logger.info(f"BACKTEST START: since_ms={effective_trade_since_ms} ({since_dt.isoformat()}){extra}")
    else:
        logger.info("BACKTEST START: since_ms=None (no slicing)")


    ex = ExchangeClient()

    # Ensure markets loaded
    try:
        ex.ex.load_markets()
    except Exception:
        pass

    fee_rate = float(fee_taker_rate)
    max_pos_pct = float(getattr(C, "MAX_POSITION_NOTIONAL_PCT", 0.10))
    max_open_positions = int(getattr(C, "MAX_OPEN_POSITIONS", 1))

    per_call_limit = int(getattr(C, "BACKTEST_FETCH_LIMIT", 1000))
    max_total_entry = int(getattr(C, "BACKTEST_MAX_BARS_ENTRY", 20000))
    max_total_filter = int(getattr(C, "BACKTEST_MAX_BARS_FILTER", 10000))

    entry_data: Dict[str, Dict[str, List[float]]] = {}
    filter_data: Dict[str, Dict[str, List[float]]] = {}
    filter_ts: Dict[str, List[int]] = {}
    since_ms_effective = since_ms   # default effective-since value for reporting

    # main() may override this before result metadata is written.
    since_ms_effective = since_ms

    # CSV settings (default from config, override-able by env/runtime/settings).
    runtime_symbol = str((symbols[0] if symbols else getattr(C, "BACKTEST_CSV_SYMBOL", "ETH/USDT")) or "").strip()
    csv_symbol = str(runtime_symbol or "").strip()
    if not csv_symbol:
        csv_symbol = str(getattr(C, "BACKTEST_CSV_SYMBOL", "ETH/USDT") or "").strip()
    csv_prefix = symbol_to_prefix(csv_symbol)
    csv_symbol_norm = symbol_to_prefix(csv_symbol)

    csv_dir_5m_default = str(getattr(C, "BACKTEST_CSV_DIR_5M", "binance_ethusdt_5m_2025"))
    csv_glob_5m_default = str(getattr(C, "BACKTEST_CSV_GLOB_5M", f"{csv_prefix}-5m-*.csv"))
    csv_dir_1h_default = str(getattr(C, "BACKTEST_CSV_DIR_1H", "binance_ethusdt_1h_2025"))
    csv_glob_1h_default = str(getattr(C, "BACKTEST_CSV_GLOB_1H", f"{csv_prefix}-1h-*.csv"))

    resolved_dataset_year: Optional[int]
    if dataset_years:
        resolved_dataset_year = None
    elif dataset_year is not None:
        resolved_dataset_year = int(dataset_year)
    else:
        resolved_dataset_year = infer_year_from_ms(
            effective_trade_since_ms if effective_trade_since_ms is not None else since_ms
        )
    try:
        dataset_spec = resolve_dataset(
            dataset_root=os.path.abspath(str(getattr(ensure_runtime_dirs(), "market_data_dir", ".") or ".")),
            prefix=str(csv_prefix),
            year=resolved_dataset_year,
            years=list(dataset_years) if dataset_years else None,
            tf_dirs=("5m", "1h"),
            default_dir_5m=csv_dir_5m_default,
            default_glob_5m=csv_glob_5m_default,
            default_dir_1h=csv_dir_1h_default,
            default_glob_1h=csv_glob_1h_default,
            runtime_symbol=str(runtime_symbol),
            context="BACKTEST",
        )
    except DatasetResolutionError as e:
        logger.error("[DATASET] resolve failed diagnostics=%s", json.dumps(e.diagnostics or {}, ensure_ascii=False, sort_keys=True))
        msg = str(e)
        if (not dataset_years) and dataset_year is None and effective_trade_since_ms is None and since_ms is None:
            msg = f"{msg} Hint: pass --dataset-year, --since-ms, or --since-year/--until-year when running sweep/backtest."
        raise RuntimeError(msg) from e
    dataset_diag = dict(dataset_spec.diagnostics or {})
    csv_diag_5m: Dict[str, Any] = {}
    csv_diag_1h: Dict[str, Any] = {}

    def _attach_tf_diag(base: Dict[str, Any], tf: str, diag: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(base or {})
        if not diag:
            return out
        tf_txt = str(tf)
        out[f"raw_rows_{tf_txt}"] = int(diag.get("raw_rows", 0) or 0)
        out[f"normalized_rows_{tf_txt}"] = int(diag.get("normalized_rows", 0) or 0)
        out[f"filtered_rows_{tf_txt}"] = int(diag.get("filtered_rows", 0) or 0)
        out[f"columns_{tf_txt}"] = list(diag.get("columns", []) or [])
        out[f"ts_min_{tf_txt}"] = diag.get("ts_min", None)
        out[f"ts_max_{tf_txt}"] = diag.get("ts_max", None)
        out[f"sample_first_ts_{tf_txt}"] = diag.get("sample_first_ts", None)
        out[f"sample_last_ts_{tf_txt}"] = diag.get("sample_last_ts", None)
        return out

    csv_dir_5m = str(dataset_spec.dir_5m)
    csv_dir_1h = str(dataset_spec.dir_1h)
    csv_glob_5m = str(dataset_diag.get("glob_5m", csv_glob_5m_default))
    csv_glob_1h = str(dataset_diag.get("glob_1h", csv_glob_1h_default))
    dataset_root_used = str(dataset_spec.root)
    dataset_prefix_used = str(dataset_spec.prefix)
    dataset_year_used = int(dataset_spec.year)
    dataset_years_loaded = list(dataset_diag.get("years_loaded", []) or [])
    dataset_years_requested = list(dataset_diag.get("years_requested", []) or [])
    logger.info(
        "[BACKTEST][DATASET] symbol=%s source=%s root=%s prefix=%s year=%s years_requested=%s years_loaded=%s dir_5m=%s glob_5m=%s dir_1h=%s glob_1h=%s paths_5m=%s paths_1h=%s",
        str(runtime_symbol or csv_symbol),
        str(dataset_diag.get("source", "default")),
        dataset_root_used,
        dataset_prefix_used,
        dataset_year_used,
        dataset_years_requested,
        dataset_years_loaded,
        csv_dir_5m,
        csv_glob_5m,
        csv_dir_1h,
        csv_glob_1h,
        int(len(dataset_spec.paths_5m)),
        int(len(dataset_spec.paths_1h)),
    )
    logger.info(
        "[BACKTEST][DATASET_DIAG] symbol=%s source=%s root=%s legacy_fallback_5m_used=%s legacy_fallback_5m_reason=%s legacy_fallback_1h_used=%s legacy_fallback_1h_reason=%s searched_paths_5m=%s searched_paths_1h=%s",
        str(runtime_symbol or csv_symbol),
        str(dataset_diag.get("source", "default")),
        dataset_root_used,
        str(dataset_diag.get("legacy_fallback_5m_used", "")),
        str(dataset_diag.get("legacy_fallback_5m_reason", "")),
        str(dataset_diag.get("legacy_fallback_1h_used", "")),
        str(dataset_diag.get("legacy_fallback_1h_reason", "")),
        int(len(list(dataset_diag.get("searched_paths_5m", []) or []))),
        int(len(list(dataset_diag.get("searched_paths_1h", []) or []))),
    )
    if (
        str(dataset_diag.get("legacy_fallback_5m_reason", "")) == "cross_root_legacy"
        or str(dataset_diag.get("legacy_fallback_1h_reason", "")) == "cross_root_legacy"
    ):
        logger.warning(
            "[BACKTEST][DATASET_WARN] symbol=%s root=%s legacy_fallback_5m_used=%s legacy_fallback_5m_reason=%s legacy_fallback_1h_used=%s legacy_fallback_1h_reason=%s",
            str(runtime_symbol or csv_symbol),
            dataset_root_used,
            str(dataset_diag.get("legacy_fallback_5m_used", "")),
            str(dataset_diag.get("legacy_fallback_5m_reason", "")),
            str(dataset_diag.get("legacy_fallback_1h_used", "")),
            str(dataset_diag.get("legacy_fallback_1h_reason", "")),
        )
    if dataset_years_loaded:
        logger.info(
            "[DATASET][MULTIYEAR] years_loaded=%s paths_5m=%s paths_1h=%s",
            dataset_years_loaded,
            int(len(dataset_spec.paths_5m)),
            int(len(dataset_spec.paths_1h)),
        )

    for sym in symbols:
        rows_e = []
        rows_f = []

        use_eth = (symbol_to_prefix(sym) == csv_symbol_norm)

        entry_tf_l = str(entry_tf).lower()
        filter_tf_l = str(filter_tf).lower()

        # ----------------------------
        # ENTRY: CSV only
        # ----------------------------
        rows_e = []
        if use_eth and entry_tf_l == "5m":
            try:
                rows_5m = _read_binance_csv_dir(
                    csv_dir=csv_dir_5m,
                    file_glob=csv_glob_5m,
                    since_ms=since_ms,
                    until_ms=until_ms,
                    paths_override=list(dataset_spec.paths_5m),
                    diag_out=csv_diag_5m,
                )
            except SystemExit as e:
                diag_5m = _attach_tf_diag(dataset_diag, "5m", csv_diag_5m)
                logger.error("[DATASET][BACKTEST] diagnostics=%s", json.dumps(diag_5m, ensure_ascii=False, sort_keys=True))
                raise RuntimeError(
                    build_missing_dataset_message(
                        context="BACKTEST",
                        tf=str(entry_tf),
                        searched_dir=str(csv_dir_5m),
                        searched_glob=str(csv_glob_5m),
                        dataset_root=str(dataset_root_used),
                        prefix=str(dataset_prefix_used),
                        year=dataset_year_used,
                        tf_dirs=("5m", "1h"),
                        diagnostics=diag_5m,
                    )
                ) from e
            logger.info(
                "[DATASET][CSV_DIAG] tf=5m raw_rows=%s normalized_rows=%s filtered_rows=%s ts_min=%s ts_max=%s",
                int(csv_diag_5m.get("raw_rows", 0) or 0),
                int(csv_diag_5m.get("normalized_rows", 0) or 0),
                int(csv_diag_5m.get("filtered_rows", 0) or 0),
                csv_diag_5m.get("ts_min", None),
                csv_diag_5m.get("ts_max", None),
            )
            rows_e = _recent_slice(rows_5m, recent_bars_entry)

        elif use_eth and entry_tf_l == "1m":
            # 1m entry CSV is optional in the current dataset layout.
            # If we add 1m support later, wire it through csv_dir_1m/csv_glob_1m.
            rows_e = []

        # ----------------------------
        # FILTER: CSV only
        # ----------------------------
        rows_f = []
        if use_eth and filter_tf_l == "1h":
            try:
                rows_1h = _read_binance_csv_dir(
                    csv_dir=csv_dir_1h,
                    file_glob=csv_glob_1h,
                    since_ms=since_ms,
                    until_ms=until_ms,
                    paths_override=list(dataset_spec.paths_1h),
                    diag_out=csv_diag_1h,
                )
            except SystemExit as e:
                diag_1h = _attach_tf_diag(dataset_diag, "1h", csv_diag_1h)
                logger.error("[DATASET][BACKTEST] diagnostics=%s", json.dumps(diag_1h, ensure_ascii=False, sort_keys=True))
                raise RuntimeError(
                    build_missing_dataset_message(
                        context="BACKTEST",
                        tf=str(filter_tf),
                        searched_dir=str(csv_dir_1h),
                        searched_glob=str(csv_glob_1h),
                        dataset_root=str(dataset_root_used),
                        prefix=str(dataset_prefix_used),
                        year=dataset_year_used,
                        tf_dirs=("5m", "1h"),
                        diagnostics=diag_1h,
                    )
                ) from e
            logger.info(
                "[DATASET][CSV_DIAG] tf=1h raw_rows=%s normalized_rows=%s filtered_rows=%s ts_min=%s ts_max=%s",
                int(csv_diag_1h.get("raw_rows", 0) or 0),
                int(csv_diag_1h.get("normalized_rows", 0) or 0),
                int(csv_diag_1h.get("filtered_rows", 0) or 0),
                csv_diag_1h.get("ts_min", None),
                csv_diag_1h.get("ts_max", None),
            )
            rows_f = _recent_slice(rows_1h, recent_bars_filter)

        # If filter rows are missing, resample them from entry rows when the timeframes differ.
        if use_eth and (not rows_f) and rows_e and filter_tf_l != entry_tf_l:
            rows_f = _resample_ohlcv(rows_e, str(filter_tf))
            rows_f = _recent_slice(rows_f, recent_bars_filter)

        # ----------------------------
        # Strict diagnostics: surface empty CSV loads before continuing.
        # ----------------------------
        def _log_rows(tag: str, rows: list[list[float]]):
            if not rows:
                logger.info(f"{tag}: rows=0")
                return
            logger.info(
                f"{tag}: rows={len(rows)} ts_first={rows[0][0]}({iso_utc(int(rows[0][0]))}) "
                f"ts_last={rows[-1][0]}({iso_utc(int(rows[-1][0]))})"
            )

        _log_rows(f"[{sym}] ENTRY {entry_tf}", rows_e)
        _log_rows(f"[{sym}] FILTER {filter_tf}", rows_f)

        if not rows_e:
            diag_rows = _attach_tf_diag(_attach_tf_diag(dataset_diag, "5m", csv_diag_5m), "1h", csv_diag_1h)
            logger.error("[DATASET][BACKTEST] diagnostics=%s", json.dumps(diag_rows, ensure_ascii=False, sort_keys=True))
            logger.error("[DATASET][BACKTEST] rows entry=%s filter=%s", int(len(rows_e)), int(len(rows_f)))
            raise RuntimeError(
                build_missing_dataset_message(
                    context="BACKTEST",
                    tf=str(entry_tf),
                    searched_dir=str(csv_dir_5m),
                    searched_glob=str(csv_glob_5m),
                    dataset_root=str(dataset_root_used),
                    prefix=str(dataset_prefix_used),
                    year=dataset_year_used,
                    tf_dirs=("5m", "1h"),
                    diagnostics=diag_rows,
                )
            )

        if not rows_f:
            diag_rows = _attach_tf_diag(_attach_tf_diag(dataset_diag, "5m", csv_diag_5m), "1h", csv_diag_1h)
            logger.error("[DATASET][BACKTEST] diagnostics=%s", json.dumps(diag_rows, ensure_ascii=False, sort_keys=True))
            logger.error("[DATASET][BACKTEST] rows entry=%s filter=%s", int(len(rows_e)), int(len(rows_f)))
            raise RuntimeError(
                build_missing_dataset_message(
                    context="BACKTEST",
                    tf=str(filter_tf),
                    searched_dir=str(csv_dir_1h),
                    searched_glob=str(csv_glob_1h),
                    dataset_root=str(dataset_root_used),
                    prefix=str(dataset_prefix_used),
                    year=dataset_year_used,
                    tf_dirs=("5m", "1h"),
                    diagnostics=diag_rows,
                )
            )



        entry_data[sym] = _ohlcv_to_cols(rows_e)
        filter_data[sym] = _ohlcv_to_cols(rows_f)
        filter_ts[sym] = filter_data[sym]["ts"]

    _sym0 = str(symbols[0]) if symbols else ""
    _entry_ts = (entry_data.get(_sym0, {}) or {}).get("ts", []) or []
    _filter_ts = (filter_data.get(_sym0, {}) or {}).get("ts", []) or []
    e_fr, e_fn, e_lr, e_ln, e_cnt = _ts_raw_and_ms_from_ts_list(_entry_ts)
    f_fr, f_fn, f_lr, f_ln, f_cnt = _ts_raw_and_ms_from_ts_list(_filter_ts)
    logger.info(
        "[BACKTEST][OHLCV_RANGE] since_ms=%s until_ms=%s 5m(first_raw=%s first_ts_ms=%s last_raw=%s last_ts_ms=%s count=%s) "
        "1h(first_raw=%s first_ts_ms=%s last_raw=%s last_ts_ms=%s count=%s)",
        int(since_ms) if since_ms is not None else None,
        int(until_ms) if until_ms is not None else None,
        e_fr, e_fn, e_lr, e_ln, e_cnt,
        f_fr, f_fn, f_lr, f_ln, f_cnt,
    )

    # Per-symbol indicator caches derived from entry/filter OHLCV data.
    entry_ind: Dict[str, Dict[str, list[float]]] = {}
    filter_ind: Dict[str, Dict[str, list[float]]] = {}

    # Keep the pullback lookback aligned with strategy.py defaults.
    pb_lb = int(getattr(C, "TREND_PULLBACK_LOOKBACK", 8))
    if pb_lb < 3:
        pb_lb = 3

    brk_n = int(getattr(C, "TREND_BREAKOUT_N", 6))
    if brk_n < 2:
        brk_n = 2

    for sym in symbols:
        e = entry_data[sym]
        f = filter_data[sym]

        e_close = np.asarray(e["close"], dtype=float)
        e_high = np.asarray(e["high"], dtype=float)
        e_low = np.asarray(e["low"], dtype=float)

        f_close = np.asarray(f["close"], dtype=float)
        f_high = np.asarray(f["high"], dtype=float)
        f_low = np.asarray(f["low"], dtype=float)

        # filter(1h) indicators
        # NOTE: indicators.py exposes `adx`, so use it consistently.
        adx_period = int(getattr(C, "ADX_PERIOD_FILTER", 14))
        f_adx: Optional[np.ndarray] = None

        # entry-side indicators
        pre_enabled = bool(getattr(C, "PRECOMPUTED_INDICATORS_ENABLED", False))
        pre_root = _resolve_precomputed_root(
            str(getattr(C, "PRECOMPUTED_INDICATORS_OUT_ROOT", "exports/precomputed_indicators"))
        )
        pre_strict = bool(getattr(C, "PRECOMPUTED_INDICATORS_STRICT", True))
        # Use run_backtest() arg to keep CLI/config overrides consistent
        warmup_bars_pre = int(warmup_bars or 0)

        rsi_period = int(getattr(C, "RSI_PERIOD", 14))
        atr_period = int(getattr(C, "ATR_PERIOD", 14))

        if pre_enabled:
            entry_gap_issues: List[dict[str, Any]] = []
            entry_path_diags: Dict[str, Dict[str, int]] = {}
            # Load precomputed indicator CSVs and align by ts.
            # precompute_indicators.py writes:
            #   <root>/<symbol>/<tf>/<IND>-*.csv
            # where IND is EMA9/EMA21/RSI14/ATR14 etc.
            ind_names = [
                "EMA9",
                "EMA21",
                f"RSI{rsi_period}",
                f"ATR{atr_period}",
            ]

            # NOTE: performance: avoid dict(ts->value) and do sequential read instead.
            # Also minimize disk I/O by restricting month files to the OHLCV window.
            missing: List[str] = []
            ind_paths: Dict[str, List[str]] = {}
            found_any_entry_precomputed = False
            ts0 = int(e["ts"][0])
            ts1 = int(e["ts"][-1])
            for nm in ind_names:
                paths = _resolve_precomputed_indicator_path(pre_root, sym, entry_tf, nm)
                paths = _filter_month_files_by_ts(paths, ts0, ts1)
                if not paths:
                    missing.append(nm)
                    continue
                found_any_entry_precomputed = True
                ind_paths[nm] = paths

            if not _handle_missing_precomputed(
                symbol=sym,
                tf=str(entry_tf),
                missing=missing,
                found_any=bool(found_any_entry_precomputed),
                root=pre_root,
                strict=pre_strict,
                scope="entry",
            ):
                pre_enabled = False
            else:
                entry_gap_issues = _collect_precomputed_boundary_nan_issues(ind_paths)
                if entry_gap_issues:
                    _log_precomputed_boundary_nan_issues(
                        scope="entry",
                        symbol=str(sym),
                        tf=str(entry_tf),
                        issues=entry_gap_issues,
                    )
                    pre_enabled = False
                else:
                    logger.info(
                        "[PRECOMPUTED][ENTRY] symbol=%s tf=%s indicators=%s source=%s",
                        sym,
                        entry_tf,
                        ",".join(ind_names),
                        pre_root,
                    )
                    e_ema9 = _load_precomputed_indicator_series_fast(
                        e["ts"],
                        ind_paths["EMA9"],
                        name="ema9",
                        warmup_bars=warmup_bars_pre,
                        strict=pre_strict,
                    )
                    e_ema21 = _load_precomputed_indicator_series_fast(
                        e["ts"],
                        ind_paths["EMA21"],
                        name="ema21",
                        warmup_bars=warmup_bars_pre,
                        strict=pre_strict,
                    )
                    e_rsi14 = _load_precomputed_indicator_series_fast(
                        e["ts"],
                        ind_paths[f"RSI{rsi_period}"],
                        name="rsi14",
                        warmup_bars=warmup_bars_pre,
                        strict=pre_strict,
                    )
                    e_atr14 = _load_precomputed_indicator_series_fast(
                        e["ts"],
                        ind_paths[f"ATR{atr_period}"],
                        name="atr14",
                        warmup_bars=warmup_bars_pre,
                        strict=pre_strict,
                    )
            if precomputed_debug_enabled:
                entry_path_diags = _collect_precomputed_path_diagnostics(
                    ind_paths=ind_paths,
                    symbol=str(sym),
                    tf=str(entry_tf),
                )
                _log_precomputed_selection_diagnostics(
                    scope="entry",
                    symbol=str(sym),
                    tf=str(entry_tf),
                    ind_paths=ind_paths,
                    missing=missing,
                    gap_issues=entry_gap_issues,
                    path_diags=entry_path_diags,
                    enabled=bool(pre_enabled),
                )

        if not pre_enabled:
            # fallback: compute on the fly
            e_ema9 = ema(e_close, 9)
            e_ema21 = ema(e_close, 21)
            e_rsi14 = rsi(e_close, int(rsi_period))
            e_atr14 = atr(e_high, e_low, e_close, int(atr_period))

        # Entry-side rolling low used by pullback/range helpers.
        e_recent_min_low = _rolling_min(e["low"], pb_lb)

        # prev_high must exclude the current bar, so shift the rolling max by one.
        roll_max_high = _rolling_max(e["high"], brk_n + 1)
        prev_high_excl_cur = [float("nan")] * len(roll_max_high)
        for i in range(len(roll_max_high)):
            if i <= 0:
                continue
            prev_high_excl_cur[i] = float(roll_max_high[i - 1])
        # rolling_max(brk_n+1) includes the current bar.
        # Shifting by one gives the max over the previous breakout window.
        range_lb = int(getattr(C, "RANGE_RECENT_LOW_LOOKBACK", 20))
        if range_lb < 5:
            range_lb = 5
        range_recent_low = _rolling_min(e["low"], range_lb)
        
        entry_ind[sym] = {
            "ema9": e_ema9,
            "ema21": e_ema21,
            "rsi14": e_rsi14,
            "atr14": e_atr14,
            "recent_min_low": e_recent_min_low,
            "prev_high": prev_high_excl_cur,
            "range_recent_low": range_recent_low,
        }

        # filter-side ADX/EMA indicators
        # - If precomputed indicators exist for filter_tf, use them.
        # - Otherwise, compute on the fly (fallback).
        ema_fast_span = int(getattr(C, "EMA_FAST", 20))
        ema_slow_span = int(getattr(C, "EMA_SLOW", 50))

        pre_enabled_filter = bool(getattr(C, "PRECOMPUTED_INDICATORS_ENABLED", False))
        if pre_enabled_filter:
            filter_gap_issues: List[dict[str, Any]] = []
            filter_path_diags: Dict[str, Dict[str, int]] = {}
            f_ts0 = int(f["ts"][0])
            f_ts1 = int(f["ts"][-1])
            f_ind_names = [
                f"ADX{adx_period}",
                f"EMA{ema_fast_span}",
                f"EMA{ema_slow_span}",
            ]

            f_ind_paths: Dict[str, List[str]] = {}
            f_missing: List[str] = []
            found_any_filter_precomputed = False
            for nm in f_ind_names:
                paths = _resolve_precomputed_indicator_path(pre_root, sym, filter_tf, nm)
                paths = _filter_month_files_by_ts(paths, f_ts0, f_ts1)
                if not paths:
                    f_missing.append(nm)
                    continue
                found_any_filter_precomputed = True
                f_ind_paths[nm] = paths

            if not _handle_missing_precomputed(
                symbol=sym,
                tf=str(filter_tf),
                missing=f_missing,
                found_any=bool(found_any_filter_precomputed),
                root=pre_root,
                strict=pre_strict,
                scope="filter",
            ):
                pre_enabled_filter = False
            else:
                filter_gap_issues = _collect_precomputed_boundary_nan_issues(f_ind_paths)
                if filter_gap_issues:
                    _log_precomputed_boundary_nan_issues(
                        scope="filter",
                        symbol=str(sym),
                        tf=str(filter_tf),
                        issues=filter_gap_issues,
                    )
                    pre_enabled_filter = False
                else:
                    logger.info(
                        "[PRECOMPUTED][FILTER] symbol=%s tf=%s indicators=%s source=%s",
                        sym,
                        filter_tf,
                        ",".join(f_ind_names),
                        pre_root,
                    )
                    f_adx_list = _load_precomputed_indicator_series_fast(
                        f["ts"],
                        f_ind_paths[f"ADX{adx_period}"],
                        name="adx",
                        warmup_bars=warmup_bars_pre,
                        strict=pre_strict,
                    )
                    f_ema_fast_list = _load_precomputed_indicator_series_fast(
                        f["ts"],
                        f_ind_paths[f"EMA{ema_fast_span}"],
                        name="ema_fast",
                        warmup_bars=warmup_bars_pre,
                        strict=pre_strict,
                    )
                    f_ema_slow_list = _load_precomputed_indicator_series_fast(
                        f["ts"],
                        f_ind_paths[f"EMA{ema_slow_span}"],
                        name="ema_slow",
                        warmup_bars=warmup_bars_pre,
                        strict=pre_strict,
                    )

                    f_adx = np.asarray(f_adx_list, dtype=float)
                    f_ema_fast = np.asarray(f_ema_fast_list, dtype=float)
                    f_ema_slow = np.asarray(f_ema_slow_list, dtype=float)
            if precomputed_debug_enabled:
                filter_path_diags = _collect_precomputed_path_diagnostics(
                    ind_paths=f_ind_paths,
                    symbol=str(sym),
                    tf=str(filter_tf),
                )
                _log_precomputed_selection_diagnostics(
                    scope="filter",
                    symbol=str(sym),
                    tf=str(filter_tf),
                    ind_paths=f_ind_paths,
                    missing=f_missing,
                    gap_issues=filter_gap_issues,
                    path_diags=filter_path_diags,
                    enabled=bool(pre_enabled_filter),
                )

        if not pre_enabled_filter:
            f_adx = adx(f_high, f_low, f_close, int(adx_period))
            f_ema_fast = ema(f_close, int(ema_fast_span))
            f_ema_slow = ema(f_close, int(ema_slow_span))

        filter_ind[sym] = {
            "adx": f_adx,
            "ema_fast": f_ema_fast,
            "ema_slow": f_ema_slow,
        }
    # ============================
    # Keep only symbols that have both OHLCV data and derived indicators.
    # ============================
    symbols = [s for s in symbols if s in entry_data and s in filter_data]
    entry_ind = {s: entry_ind[s] for s in symbols if s in entry_ind}
    filter_ind = {s: filter_ind[s] for s in symbols if s in filter_ind}


    if not symbols:
        raise RuntimeError("No valid symbols after loading OHLCV data.")

    # ----------------------------
    # FAST timeline merge (heap)
    # ----------------------------
    # per symbol pointers
    entry_ptr: Dict[str, int] = {s: 0 for s in symbols}
    filter_ptr: Dict[str, int] = {s: -1 for s in symbols}

    # heap of (ts, symbol)
    heap: List[Tuple[int, str]] = []
    for s in symbols:
        ts_list = entry_data[s]["ts"]
        if not ts_list:
            continue
        heapq.heappush(heap, (int(ts_list[0]), s))

    equity = float(initial_equity)
    sizing_initial_equity = float(initial_equity) if float(initial_equity) > 0 else float(equity)
    sizing_peak_equity = float(equity) if float(equity) > 0 else float(sizing_initial_equity)
    _dd_smooth_env = str(os.getenv("DD_DELEVER_SMOOTH_ENABLED", "") or "").strip().lower()
    if _dd_smooth_env != "":
        dd_smooth_enabled = _dd_smooth_env in ("1", "true", "yes", "y", "on")
    else:
        dd_smooth_enabled = bool(getattr(C, "DD_DELEVER_SMOOTH_ENABLED", False))
    try:
        _dd_lam_env = str(os.getenv("DD_DELEVER_SMOOTH_LAMBDA", "") or "").strip()
        dd_smooth_lam = float(_dd_lam_env) if _dd_lam_env != "" else float(getattr(C, "DD_DELEVER_SMOOTH_LAMBDA", 1.0))
    except Exception:
        dd_smooth_lam = 1.0
    dd_smooth_lam = min(1.0, max(0.0, float(dd_smooth_lam)))
    try:
        _dd_init_env = str(os.getenv("DD_DELEVER_SMOOTH_INIT", "") or "").strip()
        sizing_dd_ema = float(_dd_init_env) if _dd_init_env != "" else float(getattr(C, "DD_DELEVER_SMOOTH_INIT", 0.0))
    except Exception:
        sizing_dd_ema = 0.0
    sizing_dd_ema = max(0.0, float(sizing_dd_ema))
    _profit_only_env = str(os.getenv("LEGACY_COMPOUND_PROFIT_ONLY_ENABLED", "") or "").strip().lower()
    if _profit_only_env != "":
        profit_only_enabled = _profit_only_env in ("1", "true", "yes", "y", "on")
    else:
        profit_only_enabled = bool(getattr(C, "LEGACY_COMPOUND_PROFIT_ONLY_ENABLED", False))
    try:
        _profit_w_env = str(os.getenv("LEGACY_COMPOUND_PROFIT_REINVEST_W", "") or "").strip()
        profit_reinvest_w = float(_profit_w_env) if _profit_w_env != "" else float(getattr(C, "LEGACY_COMPOUND_PROFIT_REINVEST_W", 1.0))
    except Exception:
        profit_reinvest_w = 1.0
    if not math.isfinite(profit_reinvest_w):
        profit_reinvest_w = 1.0
    _profit_floor_env = str(os.getenv("LEGACY_COMPOUND_PROFIT_ONLY_FLOOR_TO_INITIAL", "") or "").strip().lower()
    if _profit_floor_env != "":
        profit_only_floor_to_initial = _profit_floor_env in ("1", "true", "yes", "y", "on")
    else:
        profit_only_floor_to_initial = bool(getattr(C, "LEGACY_COMPOUND_PROFIT_ONLY_FLOOR_TO_INITIAL", True))
    _profit_w_ramp_env = str(os.getenv("LEGACY_COMPOUND_PROFIT_W_RAMP_ENABLED", "") or "").strip().lower()
    if _profit_w_ramp_env != "":
        profit_w_ramp_enabled = _profit_w_ramp_env in ("1", "true", "yes", "y", "on")
    else:
        profit_w_ramp_enabled = bool(getattr(C, "LEGACY_COMPOUND_PROFIT_W_RAMP_ENABLED", False))
    try:
        _profit_w_ramp_pct_env = str(os.getenv("LEGACY_COMPOUND_PROFIT_W_RAMP_PCT", "") or "").strip()
        profit_w_ramp_pct = float(_profit_w_ramp_pct_env) if _profit_w_ramp_pct_env != "" else float(getattr(C, "LEGACY_COMPOUND_PROFIT_W_RAMP_PCT", 0.30))
    except Exception:
        profit_w_ramp_pct = 0.30
    if not math.isfinite(profit_w_ramp_pct):
        profit_w_ramp_pct = 0.30
    try:
        _profit_w_ramp_shape_env = str(os.getenv("LEGACY_COMPOUND_PROFIT_W_RAMP_SHAPE", "") or "").strip()
        profit_w_ramp_shape = float(_profit_w_ramp_shape_env) if _profit_w_ramp_shape_env != "" else float(getattr(C, "LEGACY_COMPOUND_PROFIT_W_RAMP_SHAPE", 2.0))
    except Exception:
        profit_w_ramp_shape = 2.0
    if not math.isfinite(profit_w_ramp_shape):
        profit_w_ramp_shape = 2.0
    try:
        _profit_w_ramp_min_g_env = str(os.getenv("LEGACY_COMPOUND_PROFIT_W_RAMP_MIN_G", "") or "").strip()
        profit_w_ramp_min_g = float(_profit_w_ramp_min_g_env) if _profit_w_ramp_min_g_env != "" else float(getattr(C, "LEGACY_COMPOUND_PROFIT_W_RAMP_MIN_G", 0.0))
    except Exception:
        profit_w_ramp_min_g = 0.0
    if not math.isfinite(profit_w_ramp_min_g):
        profit_w_ramp_min_g = 0.0
    _size_min_bump_env = str(os.getenv("SIZE_MIN_BUMP_ENABLED", "") or "").strip().lower()
    if _size_min_bump_env != "":
        size_min_bump_enabled = _size_min_bump_env in ("1", "true", "yes", "y", "on")
    else:
        size_min_bump_enabled = bool(getattr(C, "SIZE_MIN_BUMP_ENABLED", False))
    try:
        _size_min_bump_max_env = str(os.getenv("SIZE_MIN_BUMP_MAX_PCT_OF_CAP", "") or "").strip()
        size_min_bump_max_pct_of_cap = float(_size_min_bump_max_env) if _size_min_bump_max_env != "" else float(getattr(C, "SIZE_MIN_BUMP_MAX_PCT_OF_CAP", 0.10))
    except Exception:
        size_min_bump_max_pct_of_cap = 0.10
    if not math.isfinite(size_min_bump_max_pct_of_cap):
        size_min_bump_max_pct_of_cap = 0.10
    size_min_bump_max_pct_of_cap = max(0.0, float(size_min_bump_max_pct_of_cap))
    _size_cap_ramp_env = str(os.getenv("SIZE_CAP_RAMP_ENABLED", "") or "").strip().lower()
    if _size_cap_ramp_env != "":
        size_cap_ramp_enabled = _size_cap_ramp_env in ("1", "true", "yes", "y", "on")
    else:
        size_cap_ramp_enabled = bool(getattr(C, "SIZE_CAP_RAMP_ENABLED", False))
    try:
        _size_cap_ramp_k_env = str(os.getenv("SIZE_CAP_RAMP_K", "") or "").strip()
        size_cap_ramp_k = float(_size_cap_ramp_k_env) if _size_cap_ramp_k_env != "" else float(getattr(C, "SIZE_CAP_RAMP_K", 0.50))
    except Exception:
        size_cap_ramp_k = 0.50
    if not math.isfinite(size_cap_ramp_k):
        size_cap_ramp_k = 0.50
    size_cap_ramp_k = max(1e-9, float(size_cap_ramp_k))
    try:
        _size_cap_ramp_max_env = str(os.getenv("SIZE_CAP_RAMP_MAX_PCT", "") or "").strip()
        size_cap_ramp_max_cfg = float(_size_cap_ramp_max_env) if _size_cap_ramp_max_env != "" else float(getattr(C, "SIZE_CAP_RAMP_MAX_PCT", 0.20))
    except Exception:
        size_cap_ramp_max_cfg = 0.20
    size_cap_ramp_max_pct = _resolve_size_cap_ramp_max_pct(float(max_pos_pct), float(size_cap_ramp_max_cfg))
    _size_dbg_env = str(os.getenv("SIZE_SIZING_DEBUG_LOG_ENABLED", "") or "").strip().lower()
    if _size_dbg_env != "":
        size_sizing_debug_log_enabled = _size_dbg_env in ("1", "true", "yes", "y", "on")
    else:
        size_sizing_debug_log_enabled = bool(getattr(C, "SIZE_SIZING_DEBUG_LOG_ENABLED", False))
    peak = float(equity)
    max_dd = 0.0
    peak_mtm = float(equity)
    max_dd_mtm = 0.0
    peak_worst_bar = float(equity)
    max_dd_worst_bar = 0.0
    mae_abs_values: List[float] = []
    mae_bps_values: List[float] = []
    mfe_abs_values: List[float] = []
    mfe_bps_values: List[float] = []
    giveback_max_abs_values: List[float] = []
    giveback_max_bps_values: List[float] = []
    giveback_max_pct_values: List[float] = []
    giveback_to_close_abs_values: List[float] = []
    giveback_to_close_bps_values: List[float] = []
    giveback_to_close_pct_values: List[float] = []
    kept_bps_values: List[float] = []
    kept_pct_of_mfe_values: List[float] = []
    fav_adv_ratio_values: List[float] = []
    size_sizing_open_notional_pcts: List[float] = []

    positions: Dict[str, Position] = {}

    # diff trace (jsonl) for runner/backtest comparison
    diff_trace_writers: Dict[str, Any] = {}
    trace_ohlcv_by_symbol: Dict[str, dict] = {
        str(s): {
            "timestamp": list(entry_data[s].get("ts", [])),
            "open": list(entry_data[s].get("open", [])),
            "high": list(entry_data[s].get("high", [])),
            "low": list(entry_data[s].get("low", [])),
            "close": list(entry_data[s].get("close", [])),
        }
        for s in symbols
        if s in entry_data
    }
    def _export_diff_trace_jsonl(out_dir: str, *, cfg: dict, build_id: str, mode: str, sym: str):
        """
        Create (or reuse) a jsonl writer for diff-trace events.
        Returns: (out_path, writer_fn)
        """
        os.makedirs(out_dir, exist_ok=True)

        prefix = cfg.get("diff_trace_prefix") or "diff_trace"
        written_meta_paths: set[str] = set()

        def _path_for(ts_ms: int) -> str:
            try:
                d = datetime.fromtimestamp(max(0, int(ts_ms)) / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")
            except Exception:
                d = "unknown"
            # Match runner's per-day diff-trace filename layout.
            return os.path.join(out_dir, f"{prefix}_{d}.jsonl")

        def _ensure_meta(out_path: str, ts_ms: int) -> None:
            if out_path in written_meta_paths:
                return
            meta = {
                "event": "REPLAY_META",
                "type": "REPLAY_META",
                "mode": str(mode),
                "since_ms": int(ts_ms or 0),
                "since": iso_utc(int(ts_ms or 0))[:10] if int(ts_ms or 0) > 0 else None,
                "source": str(cfg.get("diff_trace_source") or "backtest"),
                "symbol": sym,
                "tf_entry": str(entry_tf),
                "tf_filter": str(filter_tf),
                "ts_ms": int(ts_ms or 0),
                "event_ts": int(ts_ms or 0),
                "exec_ts": int(ts_ms or 0),
                "build_id": str(build_id),
                "cfg": _diff_trace_norm_cfg(cfg),
            }
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(json.dumps(meta, ensure_ascii=False, sort_keys=True) + "\n")
            written_meta_paths.add(out_path)

        def _writer(ev: dict):
            # ts_ms is optional here; default to 0 when missing.
            try:
                ts_ms = int(ev.get("ts_ms", 0) or 0)
            except Exception:
                ts_ms = 0

            # ts_ms unit normalize: expect milliseconds.
            # If we accidentally get microseconds/nanoseconds (too many digits),
            # datetime conversion will overflow and filename becomes "..._unknown.jsonl".
            try:
                if ts_ms > 10_000_000_000_000_000:      # >= 1e16 -> likely ns
                    ts_ms = ts_ms // 1_000_000
                elif ts_ms > 10_000_000_000_000:        # >= 1e13 -> likely us
                    ts_ms = ts_ms // 1_000
            except Exception:
                ts_ms = int(ev.get("ts_ms", 0) or 0) if isinstance(ev, dict) else 0

            # keep normalized ts_ms
            ev["ts_ms"] = ts_ms

            # Step1: event_ts = exec_ts (runner-compatible).
            base_ts = ts_ms
            ev.setdefault("event_ts", base_ts)
            ev.setdefault("exec_ts", base_ts)
            if ev.get("event_ts") != ev.get("exec_ts"):
                ev["event_ts"] = ev["exec_ts"] = base_ts

            # stamps
            ev.setdefault("source", cfg.get("diff_trace_source") or "backtest")
            ev.setdefault("build_id", build_id)
            ev.setdefault("mode", mode)
            ev.setdefault("sym", sym)

            out_path = _path_for(ts_ms)
            _ensure_meta(out_path, ts_ms)
            with open(out_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(ev, ensure_ascii=False, separators=(",", ":")) + "\n")

        # The writer rotates files by day, so the returned out_path is only a placeholder.
        return _path_for(0), _writer
    def _diff_trace_write(sym: str, ev: dict) -> None:
        if (not export_diff_trace) or (not getattr(C, 'DIFF_TRACE_ENABLED', False)):
            return
        # normalize optional fields for stable verify_diff comparisons
        if isinstance(ev, dict) and "open_reason" in ev:
            ev["open_reason"] = str(ev.get("open_reason") or "")
        # Step1: event_ts = exec_ts (unify schema with runner)
        base_ts = ev.get('ts_ms')
        if base_ts is None:
            base_ts = ev.get('ts')
        try:
            base_ts = int(base_ts)
        except Exception:
            base_ts = None
        if base_ts is not None:
            ev.setdefault('event_ts', base_ts)
            ev.setdefault('exec_ts', base_ts)
            if ev.get('event_ts') != ev.get('exec_ts'):
                ev['event_ts'] = ev['exec_ts'] = base_ts
        try:
            tf_ms = int(_tf_to_ms(str(ev.get("tf_entry") or entry_tf)))
        except Exception:
            tf_ms = int(_tf_to_ms(str(entry_tf)))
        ohlcv_dict = trace_ohlcv_by_symbol.get(sym)
        if isinstance(ohlcv_dict, dict):
            ev = _trace_attach_bar_snapshot(ev, ohlcv_dict, tf_ms)
        w = diff_trace_writers.get(sym)
        if w is None:
            cfg_snap = {
                'symbols': list(symbols),
                'timeframe_entry': entry_tf,
                'timeframe_filter': filter_tf,
                # Keep the prefix aligned with runner/replay diff-trace comparisons.
                'diff_trace_prefix': diff_trace_prefix or 'diff_trace_backtest',
                'diff_trace_source': 'backtest',
            }
            _out_path, w = _export_diff_trace_jsonl(
                out_dir=(
                    str(_current_export_dir())
                    if is_legacy_exports_path(str(getattr(C, "DIFF_TRACE_DIR", "exports/diff_trace") or "exports/diff_trace"))
                    else str(getattr(C, "DIFF_TRACE_DIR", "exports/diff_trace"))
                ),
                cfg=cfg_snap,
                build_id=BUILD_ID,
                mode='BACKTEST',
                sym=sym,
            )
            diff_trace_writers[sym] = w
        try:
            w(ev)
        except Exception:
            pass
    trades: List[Dict[str, Any]] = []
    stop_loss_patterns: List[Dict[str, Any]] = []
    curve: List[Tuple[int, float]] = []
    mtm_by_ts: Dict[int, float] = {}

    # ---- helpers for realized-bps analytics (exit reason stats) ----
    def _realized_bps(entry_px: float, exit_px: float, direction: str) -> float:
        try:
            e = float(entry_px)
            x = float(exit_px)
            if not (e > 0 and x > 0):
                return float("nan")
            if str(direction).lower() == "short":
                return (e - x) / e * 10000.0
            return (x - e) / e * 10000.0
        except Exception:
            return float("nan")

    def _pct(a: List[float], q: float) -> float:
        if not a:
            return float("nan")
        b = sorted(float(x) for x in a if x == x)  # drop NaN
        if not b:
            return float("nan")
        if q <= 0:
            return float(b[0])
        if q >= 100:
            return float(b[-1])
        pos = (len(b) - 1) * (q / 100.0)
        lo = int(math.floor(pos))
        hi = int(math.ceil(pos))
        if lo == hi:
            return float(b[lo])
        w = pos - lo
        return float(b[lo] * (1.0 - w) + b[hi] * w)

    # --- debug counters (for diagnosing trades==0) ---
    buy_signals = 0
    entries_opened = 0
    # Track buy-side rejects only after a buy signal reached entry evaluation.
    buy_reject = _Counter()
    forced_closes = 0
    force_close_eod = bool(getattr(C, "BACKTEST_FORCE_CLOSE_EOD", True))
    # Remember unfavorable range exits so cooldown checks can block immediate re-entry.
    last_range_unfav_exit_ts: Dict[str, int] = {}
    last_range_ema21_break_block: Dict[str, Dict[str, float]] = {}
    # entry timeframe in ms (used by cooldown/timeout gates)
    bar_ms = int(_tf_to_ms(str(entry_tf)))
    entry_bar_ms = bar_ms  # alias for clarity / backward compatibility
    filter_bar_ms = int(_tf_to_ms(str(filter_tf)))

    def _resolve_tp_sl_same_bar(
        side: str,
        bar_open: float,
        bar_high: float,
        bar_low: float,
        bar_close: float,
        tp: float,
        sl: float,
    ) -> str | None:
        side_l = str(side or "long").lower()
        hit_tp_local = (side_l == "long" and float(bar_high) >= float(tp)) or (side_l == "short" and float(bar_low) <= float(tp))
        hit_sl_local = (side_l == "long" and float(bar_low) <= float(sl)) or (side_l == "short" and float(bar_high) >= float(sl))
        if hit_tp_local and not hit_sl_local:
            return "TP_HIT"
        if hit_sl_local and not hit_tp_local:
            return "STOP_HIT"
        if not hit_tp_local and not hit_sl_local:
            return None
        bullish = float(bar_close) >= float(bar_open)
        if side_l == "long":
            return "TP_HIT" if bullish else "STOP_HIT"
        return "STOP_HIT" if bullish else "TP_HIT"

    def _update_be_trail(
        p: "Position",
        bar_high: float,
        bar_low: float,
        spread_bps_est: float,
        atr_now: float | None,
        i: int,
    ) -> bool:
        stop_updated = False
        pos_dir = str(getattr(p, "direction", "long") or "long").lower()
        entry_px = _position_entry_exec_or_raw(p)
        # update max favorable
        if bar_high > p.max_fav:
            p.max_fav = float(bar_high)
        # update max adverse
        try:
            if float(bar_low) < float(getattr(p, "min_adv", p.entry_raw)):
                p.min_adv = float(bar_low)
        except Exception:
            pass

        # Apply BE/TRAIL using runner logic (same params)
        init_risk = float(entry_px) - float(p.init_stop)
        init_risk_bps = (init_risk / float(entry_px) * 10000.0) if float(entry_px) > 0 else 0.0

        if init_risk <= 0:
            return False

        # === BE BEGIN =====================================================
        # Skip BE management entirely when force_disable_be is active.
        if R._be_effective_enabled(p.regime, force_disable_be=bool(force_disable_be)):
            min_risk_bps = float(getattr(C, "BE_MIN_INIT_RISK_BPS", 0.0))
            if init_risk_bps >= min_risk_bps:
                tr_r, be_static_off = R._be_params(p.regime)
                r_now = (p.max_fav - float(entry_px)) / init_risk
                if r_now >= float(tr_r):
                    spread_bps = float(spread_bps_est)
                    atr_bps = 0.0
                    try:
                        atr_v = float(atr_now) if atr_now is not None else 0.0
                        if atr_v > 0.0 and float(p.entry_exec) > 0.0:
                            atr_bps = (atr_v / float(p.entry_exec)) * 10000.0
                    except Exception:
                        atr_bps = 0.0

                    off_bps = R._calc_be_offset_bps(
                        spread_bps,
                        atr_bps,
                        static_off_bps=be_static_off,
                    )

                    be_stop = float(entry_px) * (1.0 + _bps_to_frac(float(off_bps)))
                    if be_stop > p.stop:
                        p.stop = float(be_stop)
                        p.stop_kind = "be"
                        stop_updated = True
                        if (pos_dir == "long" and float(be_stop) >= entry_px) or (pos_dir == "short" and float(be_stop) <= entry_px):
                            p._profit_stop_arm_i = int(i) + 1
                        try:
                            p.be_triggered = True
                            p.be_trigger_r = float(tr_r)
                            p.be_offset_bps = float(off_bps)
                            p.be_stop_set = float(be_stop)
                        except Exception:
                            pass
        # === BE END =======================================================

        # === TRAIL BEGIN ==================================================
        trail_block_reason = ""
        trail_enabled = bool(getattr(C, "TRAIL_ENABLED", False))
        if not trail_enabled:
            trail_block_reason = "trail_disabled"
        else:
            min_risk_bps = float(getattr(C, "TRAIL_MIN_INIT_RISK_BPS", 0.0))
            if init_risk_bps >= min_risk_bps:
                start_r, atr_mult, bps_from_high = R._trail_params(p.regime)
                r_now = (p.max_fav - float(entry_px)) / init_risk
                start_price = None
                current_stop_before = float(p.stop)
                new_stop = None
                cand_delta = None
                atr_candidate_stop = None
                bps_candidate_stop = None
                trail_atr_value = None
                trail_atr_index_used = None
                trail_atr_ts_used = None
                trail_atr_diag_fields = {
                    "trail_atr_value": None,
                    "trail_atr_index_used": None,
                    "trail_atr_ts_used": None,
                }
                trail_mode = "none"
                atr_missing = False
                try:
                    p.trail_start_r = float(start_r)
                    p.trail_bps_from_high = float(bps_from_high)
                    start_price = float(entry_px + (init_risk * float(start_r)))
                    p.start_price = float(start_price)
                except Exception:
                    start_price = None
                if r_now >= float(start_r):
                    p.trail_eligible_count = int(getattr(p, "trail_eligible_count", 0) or 0) + 1
                    if bool(getattr(C, "TRAIL_USE_ATR", True)):
                        a_last = None
                        try:
                            trail_atr_index_used = int(i)
                        except Exception:
                            trail_atr_index_used = None
                        try:
                            trail_atr_ts_used = int(entry_data[p.symbol]["ts"][i])
                        except Exception:
                            trail_atr_ts_used = None
                        try:
                            a_last = float(atr_now) if atr_now is not None else None
                        except Exception:
                            a_last = None
                        trail_atr_value = float(a_last) if a_last is not None else None
                        trail_atr_diag_fields = {
                            "trail_atr_value": trail_atr_value,
                            "trail_atr_index_used": trail_atr_index_used,
                            "trail_atr_ts_used": trail_atr_ts_used,
                        }
                        if a_last is not None and np.isfinite(a_last) and a_last > 0:
                            atr_candidate_stop = bar_high - a_last * atr_mult
                        else:
                            atr_missing = True
                    bps_candidate_stop = bar_high * (1.0 - _bps_to_frac(float(bps_from_high)))
                    if atr_candidate_stop is not None and bps_candidate_stop is not None:
                        if float(atr_candidate_stop) >= float(bps_candidate_stop):
                            new_stop = float(atr_candidate_stop)
                            trail_mode = "max_atr_bps_atr"
                        else:
                            new_stop = float(bps_candidate_stop)
                            trail_mode = "max_atr_bps_bps"
                    elif atr_candidate_stop is not None:
                        new_stop = float(atr_candidate_stop)
                        trail_mode = "atr"
                    elif bps_candidate_stop is not None:
                        new_stop = float(bps_candidate_stop)
                        trail_mode = "bps"
                    if new_stop is None:
                        trail_block_reason = "atr_unavailable" if atr_missing else "candidate_invalid"
                    elif float(new_stop) <= float(current_stop_before):
                        trail_block_reason = "candidate_le_current_stop"
                else:
                    trail_block_reason = "not_reached_start_price"

                cand_delta = R._trail_diag_update(
                    p.__dict__,
                    block_reason=trail_block_reason,
                    candidate_stop=new_stop,
                    candidate_from_atr=atr_candidate_stop,
                    candidate_from_bps=bps_candidate_stop,
                    pos_stop_before=current_stop_before,
                    start_price=start_price,
                    bar_high=bar_high,
                    risk_per_unit=init_risk,
                    mode=trail_mode,
                )

                if (not bool(getattr(p, "_trail_check_emitted", False))) and new_stop is not None:
                    ts_ms = int(entry_data[p.symbol]["ts"][i])
                    _diff_trace_write(
                        p.symbol,
                        {
                            "event": "TRAIL_CHECK",
                            "ts_ms": ts_ms,
                            "symbol": str(p.symbol),
                            "regime": str(p.regime),
                            "entry_exec": float(p.entry_exec),
                            "stop_current": float(current_stop_before),
                            "max_fav": float(p.max_fav),
                            "start_price": float(p.start_price) if getattr(p, "start_price", None) is not None else None,
                            "trail_candidate_stop": float(new_stop),
                            "trail_candidate_minus_current_stop": float(cand_delta) if cand_delta is not None else None,
                            "trail_candidate_from_atr": getattr(p, "trail_candidate_from_atr_last", None),
                            "trail_candidate_from_bps": getattr(p, "trail_candidate_from_bps_last", None),
                            "trail_eligible_count": int(getattr(p, "trail_eligible_count", 0) or 0),
                            "trail_eval_count": int(getattr(p, "trail_eval_count", 0) or 0),
                            "trail_update_count": int(getattr(p, "trail_update_count", 0) or 0),
                            "trail_start_r": float(start_r),
                            "trail_atr_mult": float(atr_mult),
                            **trail_atr_diag_fields,
                            "trail_bps_from_high": float(bps_from_high),
                            "trail_mode": str(getattr(p, "trail_mode_last", "none") or "none"),
                            "trail_bar_high": getattr(p, "trail_bar_high_last", None),
                            "trail_pos_stop_before": getattr(p, "trail_pos_stop_before_last", None),
                            "trail_risk_per_unit": getattr(p, "trail_risk_per_unit_last", None),
                        },
                    )
                    p._trail_check_emitted = True
                if new_stop is not None and float(new_stop) > float(current_stop_before):
                    p.stop = float(new_stop)
                    p.stop_kind = "trail"
                    stop_updated = True
                    if (pos_dir == "long" and float(new_stop) >= entry_raw) or (pos_dir == "short" and float(new_stop) <= entry_raw):
                        p._profit_stop_arm_i = int(i) + 1
                    try:
                        p.trail_triggered = True
                        p.trail_update_count = int(getattr(p, "trail_update_count", 0) or 0) + 1
                        p._trail_trace_reason_last = ""
                    except Exception:
                        pass
                    ts_ms = int(t)
                    _diff_trace_write(
                        p.symbol,
                        {
                            "event": "TRAIL_SET",
                            "ts_ms": ts_ms,
                            "symbol": str(p.symbol),
                            "regime": str(p.regime),
                            "entry_exec": float(p.entry_exec),
                            "stop_prev": float(current_stop_before),
                            "stop_new": float(p.stop),
                            "max_fav": float(p.max_fav),
                            "start_price": float(p.start_price) if getattr(p, "start_price", None) is not None else None,
                            "trail_candidate_stop": float(new_stop),
                            "trail_candidate_minus_current_stop": float(cand_delta) if cand_delta is not None else None,
                            "trail_candidate_from_atr": getattr(p, "trail_candidate_from_atr_last", None),
                            "trail_candidate_from_bps": getattr(p, "trail_candidate_from_bps_last", None),
                            "trail_eligible_count": int(getattr(p, "trail_eligible_count", 0) or 0),
                            "trail_eval_count": int(getattr(p, "trail_eval_count", 0) or 0),
                            "trail_update_count": int(getattr(p, "trail_update_count", 0) or 0),
                            "trail_start_r": float(start_r),
                            "trail_atr_mult": float(atr_mult),
                            **trail_atr_diag_fields,
                            "trail_bps_from_high": float(bps_from_high),
                            "trail_mode": str(getattr(p, "trail_mode_last", "none") or "none"),
                            "trail_bar_high": getattr(p, "trail_bar_high_last", None),
                            "trail_pos_stop_before": getattr(p, "trail_pos_stop_before_last", None),
                            "trail_risk_per_unit": getattr(p, "trail_risk_per_unit_last", None),
                        },
                    )
        if trail_block_reason:
            if int(getattr(p, "trail_eval_count", 0) or 0) <= 0:
                p.trail_block_reason_last = str(trail_block_reason)
                if not str(getattr(p, "trail_block_reason_max", "") or ""):
                    p.trail_block_reason_max = str(trail_block_reason)
                p.trail_mode_last = "none"
            if str(getattr(p, "_trail_trace_reason_last", "") or "") != str(trail_block_reason):
                ts_ms = int(entry_data[p.symbol]["ts"][i])
                _diff_trace_write(
                    p.symbol,
                    {
                        "event": "TRAIL_BLOCKED",
                        "ts_ms": ts_ms,
                        "symbol": str(p.symbol),
                        "regime": str(p.regime),
                        "entry_exec": float(p.entry_exec),
                        "stop_current": float(p.stop),
                        "max_fav": float(p.max_fav),
                        "start_price": float(p.start_price) if getattr(p, "start_price", None) is not None else None,
                        "trail_candidate_stop": (
                            float(p.trail_candidate_stop_last)
                            if getattr(p, "trail_candidate_stop_last", None) is not None
                            else None
                        ),
                        "trail_candidate_minus_current_stop": (
                            float(p.trail_candidate_minus_current_stop)
                            if getattr(p, "trail_candidate_minus_current_stop", None) is not None
                            else None
                        ),
                        "trail_candidate_from_atr": getattr(p, "trail_candidate_from_atr_last", None),
                        "trail_candidate_from_bps": getattr(p, "trail_candidate_from_bps_last", None),
                        "trail_eligible_count": int(getattr(p, "trail_eligible_count", 0) or 0),
                        "trail_eval_count": int(getattr(p, "trail_eval_count", 0) or 0),
                        "trail_update_count": int(getattr(p, "trail_update_count", 0) or 0),
                        "trail_block_reason": str(trail_block_reason),
                        "trail_start_r": (
                            float(p.trail_start_r)
                            if getattr(p, "trail_start_r", None) is not None
                            else None
                        ),
                        "trail_bps_from_high": (
                            float(p.trail_bps_from_high)
                            if getattr(p, "trail_bps_from_high", None) is not None
                            else None
                        ),
                        **trail_atr_diag_fields,
                        "trail_mode": str(getattr(p, "trail_mode_last", "none") or "none"),
                        "trail_bar_high": getattr(p, "trail_bar_high_last", None),
                        "trail_pos_stop_before": getattr(p, "trail_pos_stop_before_last", None),
                        "trail_risk_per_unit": getattr(p, "trail_risk_per_unit_last", None),
                    },
                )
                p._trail_trace_reason_last = str(trail_block_reason)
        # === TRAIL END ====================================================
        return stop_updated

    while heap:
        # pop all symbols that share the same timestamp
        t, s0 = heapq.heappop(heap)
        syms_at_t = [s0]
        while heap and int(heap[0][0]) == int(t):
            _, sx = heapq.heappop(heap)
            syms_at_t.append(sx)

        t = int(t)
        curve.append((t, float(equity)))
        equity_before_manage = float(equity)
        in_position_worst_unrealized_pnl = 0.0
        in_position_worst_samples = 0

        # ------------------------
        # 1) manage open positions (only for symbols that have a bar at t)
        # ------------------------
        to_close: List[str] = []
        closed_this_bar_reason: Dict[str, str] = {}

        for sym in list(positions.keys()):
            if sym not in syms_at_t:
                continue

            i_raw = entry_ptr[sym]
            i = i_raw - 1  # confirmed-bar-only: exclude the current bar at t
            if i < 0:
                continue
            # safety guard (should exist)
            if i >= len(entry_data[sym]["ts"]):
                continue

            e = entry_data[sym]
            # entry indicators (needed for range re-entry block bookkeeping)
            ei = entry_ind.get(sym, {})
            bar_high = float(e["high"][i])
            bar_low = float(e["low"][i])
            bar_open = float(e["open"][i])
            bar_close = float(e["close"][i])

            p = positions[sym]
            pos_dir = str(getattr(p, "direction", "long") or "long").lower()
            entry_price_for_mae = float(getattr(p, "entry_raw", 0.0) or 0.0)
            qty_mae = max(0.0, float(getattr(p, "qty_init", float(getattr(p, "qty", 0.0) or 0.0)) or float(getattr(p, "qty", 0.0) or 0.0)))
            if qty_mae > 0.0 and entry_price_for_mae > 0.0:
                if pos_dir == "short":
                    in_position_worst_unrealized_pnl += (entry_price_for_mae - float(bar_high)) * float(qty_mae)
                else:
                    in_position_worst_unrealized_pnl += (float(bar_low) - entry_price_for_mae) * float(qty_mae)
                in_position_worst_samples += 1
                if pos_dir == "short":
                    adverse_abs = max(0.0, (float(bar_high) - entry_price_for_mae) * float(qty_mae))
                    favorable_abs = max(0.0, (entry_price_for_mae - float(bar_low)) * float(qty_mae))
                else:
                    adverse_abs = max(0.0, (entry_price_for_mae - float(bar_low)) * float(qty_mae))
                    favorable_abs = max(0.0, (float(bar_high) - entry_price_for_mae) * float(qty_mae))
                prev_mae_abs = max(0.0, float(getattr(p, "mae_abs", 0.0) or 0.0))
                p.mae_abs = max(float(prev_mae_abs), float(adverse_abs))
                prev_mfe_abs = max(0.0, float(getattr(p, "mfe_abs", 0.0) or 0.0))
                p.mfe_abs = max(float(prev_mfe_abs), float(favorable_abs))
                prev_giveback_max_abs = max(0.0, float(getattr(p, "giveback_max_abs", 0.0) or 0.0))
                giveback_abs = float(prev_giveback_max_abs)
                if pos_dir == "short":
                    best_price_short = min(
                        float(getattr(p, "min_adv", entry_price_for_mae) or entry_price_for_mae),
                        float(bar_low),
                    )
                    if best_price_short < float(entry_price_for_mae):
                        giveback_abs = max(
                            float(giveback_abs),
                            max(0.0, (float(bar_high) - float(best_price_short)) * float(qty_mae)),
                        )
                else:
                    best_price_long = max(
                        float(getattr(p, "max_fav", entry_price_for_mae) or entry_price_for_mae),
                        float(bar_high),
                    )
                    if best_price_long > float(entry_price_for_mae):
                        giveback_abs = max(
                            float(giveback_abs),
                            max(0.0, (float(best_price_long) - float(bar_low)) * float(qty_mae)),
                        )
                p.giveback_max_abs = float(giveback_abs)

            # init_risk is needed by TP1 logic below.
            # NOTE: _update_be_trail() also computes init_risk internally, but that is local to the function.
            # Keep this local definition to avoid NameError when TP1 is enabled.
            entry_px = _position_entry_exec_or_raw(p)
            init_risk = float(entry_px) - float(p.init_stop)

            atr_now = None
            try:
                atr_series = ei.get("atr14", [])
                if atr_series is not None and i < len(atr_series):
                    atr_now = float(atr_series[i])
            except Exception:
                atr_now = None

            stop_updated_this_bar = _update_be_trail(
                p,
                bar_high,
                bar_low,
                float(spread_bps_est),
                atr_now,
                int(i),
            )

            # Exit decision:
            exit_reason = None
            exit_raw = None
            close_now = float(bar_close)

            # entry timeframe in ms (used by TREND timeout in bars)
            entry_bar_ms = _tf_to_ms(str(entry_tf))

            # --- NEW: timeout exit (backtest) ---
            # NOTE: timeout_bars is measured in entry-timeframe bars (for example, 5m x 12 = 60m).
            tf_entry = str(entry_tf)
            bar_ms = int(_tf_to_ms(tf_entry))

            # --- trend timeout exit ---
            if str(p.regime).lower() == "trend":
                # Optional: apply TREND timeout only when entry was "EMA9 break recent"
                # (safe default: OFF if config missing)
                only_on_recent = bool(getattr(C, "TREND_TIMEOUT_ONLY_ON_EMA9_BREAK_RECENT", False))
                ema_recent = False
                try:
                    ema_recent = bool(getattr(p, "ema9_break_recent", False))
                except Exception:
                    ema_recent = False

                # Gate: recent-only mode & not recent => completely skip TREND timeout
                if only_on_recent and (not ema_recent):
                    timeout_bars = 0
                else:
                    timeout_bars = int(getattr(C, "TREND_TIMEOUT_BARS", 0))
                    timeout_bars_recent = int(getattr(C, "TREND_TIMEOUT_BARS_RECENT", 0))
                    if ema_recent and timeout_bars_recent > 0:
                        timeout_bars = int(timeout_bars_recent)
                if timeout_bars > 0 and bar_ms > 0:
                    dt = int(t) - int(p.opened_ts)
                    dt = max(0, dt)

                    # t/opened_ts may arrive in seconds or microseconds.
                    if int(t) >= 10_000_000_000_000:  # Treat very large values as microseconds.
                        bar_unit = bar_ms * 1000
                    else:
                        bar_unit = bar_ms

                    age_bars = int(dt // max(1, bar_unit))

                    if age_bars >= timeout_bars:
                        close_now = float(entry_data[sym]["close"][i])
                        pnl_bps = (close_now - float(entry_px)) / float(entry_px) * 10000.0
                        min_keep = float(getattr(C, "TREND_TIMEOUT_MIN_PROFIT_BPS", -10.0))
                        if ema_recent:
                            # Recent-entry timeouts can override the minimum profit threshold.
                            min_keep = float(getattr(C, "TREND_TIMEOUT_MIN_PROFIT_BPS_RECENT", min_keep))
                        if pnl_bps >= min_keep:
                            require_above_ema9 = bool(getattr(C, "TREND_TIMEOUT_REQUIRE_ABOVE_EMA9", False))
                            if ema_recent:
                                require_above_ema9 = bool(
                                    getattr(C, "TREND_TIMEOUT_REQUIRE_ABOVE_EMA9_RECENT", require_above_ema9)
                                )

                            # Optional EMA9-side gate before allowing the timeout exit.
                            require_below_ema9 = bool(getattr(C, "TREND_TIMEOUT_REQUIRE_BELOW_EMA9", False))
                            if ema_recent:
                                require_below_ema9 = bool(
                                    getattr(C, "TREND_TIMEOUT_REQUIRE_BELOW_EMA9_RECENT", require_below_ema9)
                                )

                            # If both gates are enabled, skip the EMA9-side filter.
                            if require_above_ema9 and require_below_ema9:
                                pass
                            elif require_above_ema9 or require_below_ema9:
                                try:
                                    ema9_now = float(entry_ind[sym]["ema9"][i])
                                except Exception:
                                    ema9_now = float("nan")
                                if not (ema9_now == ema9_now):
                                    pass
                                else:
                                    if require_above_ema9:
                                        # Require close >= EMA9 before taking TIMEOUT.
                                        if float(close_now) < float(ema9_now):
                                            pass
                                        else:
                                            exit_reason = f"TREND_TIMEOUT({age_bars}bars pnl={pnl_bps:.1f}bps)"
                                            exit_raw = float(close_now)
                                    else:
                                        # Require close < EMA9 before taking TIMEOUT.
                                        if float(close_now) >= float(ema9_now):
                                            pass
                                        else:
                                            exit_reason = f"TREND_TIMEOUT({age_bars}bars pnl={pnl_bps:.1f}bps)"
                                            exit_raw = float(close_now)
                            else:
                                exit_reason = f"TREND_TIMEOUT({age_bars}bars pnl={pnl_bps:.1f}bps)"
                                exit_raw = float(close_now)
            # --- range timeout exit ---
            # NOTE: timeout_bars is measured in entry-timeframe bars (for example, 5m x 12 = 60m).
            if exit_reason is None and str(p.regime).lower() == "range":
                timeout_bars = int(getattr(C, "RANGE_TIMEOUT_BARS", 0))
                if timeout_bars > 0 and bar_ms > 0:
                    dt = int(t) - int(p.opened_ts)
                    dt = max(0, dt)
                    # t/opened_ts may arrive in seconds or microseconds.
                    if int(t) >= 10_000_000_000_000:  # Treat very large values as microseconds.
                        bar_unit = bar_ms * 1000
                    else:
                        bar_unit = bar_ms
                    age_bars = int(dt // max(1, bar_unit))
                    if age_bars >= timeout_bars:
                        close_now = float(entry_data[sym]["close"][i])
                        pnl_bps = (close_now - float(entry_px)) / float(entry_px) * 10000.0
                        min_keep = float(getattr(C, "RANGE_TIMEOUT_MIN_PROFIT_BPS", -5.0))
                        if pnl_bps >= min_keep:
                            exit_reason = f"RANGE_TIMEOUT({age_bars}bars pnl={pnl_bps:.1f}bps)"
                            exit_raw = float(close_now)

            # --- TP1 (partial take profit) ---
            # Range only: take partial at R-multiple, keep the rest for BE/TRAIL.
            # Uses Position fields already present: tp1_done / realized_pnl / realized_fees / tp1_exit_notional.
            if (
                exit_reason is None
                and (not bool(getattr(p, "tp1_done", False)))
                and str(getattr(p, "regime", "")).lower() == "range"
            ):
                tp1_enabled, trig_r, qty_pct = R._tp1_range_effective_params(require_flag=False)
                if tp1_enabled and float(init_risk) > 0.0:
                    tp1_px = float(entry_px) + float(init_risk) * float(trig_r)
                    if bar_high >= tp1_px:
                        sell_qty = float(p.qty) * (float(qty_pct) / 100.0)
                        if sell_qty > 0.0 and sell_qty < float(p.qty):
                            taker = float(fee_taker_rate)
                            exec_px = float(tp1_px) * _side_slip_mult("exit_long")
                            p.realized_pnl = float(getattr(p, "realized_pnl", 0.0)) + (exec_px - float(p.entry_exec)) * float(sell_qty)
                            p.tp1_exit_notional = float(getattr(p, "tp1_exit_notional", 0.0)) + exec_px * float(sell_qty)
                            p.realized_fees = float(getattr(p, "realized_fees", 0.0)) + (exec_px * float(sell_qty) * float(taker))
                            p.qty = float(p.qty) - float(sell_qty)
                            p.tp1_done = True
                            # Optional TP1 behavior: move the stop to BE after the partial exit.
                            if (not force_disable_be) and bool(getattr(C, "TP1_MOVE_STOP_TO_BE", True)):
                                if float(entry_px) > float(p.stop):
                                    p.stop = float(entry_px)
                                    try:
                                        p.stop_kind = "be"
                                    except Exception:
                                        pass

            # --- strategy-driven range exits (A-1) ---
            # Use strategy.exit_signal_entry_precomputed for ALL range exits so live/backtest stay consistent.
            # Gate only EMA9-cross exits via RANGE_EXIT_ON_EMA9_CROSS; keep other range exits intact.
            if exit_reason is None and str(p.regime).lower() == "range":
                ex_sig = None
                try:
                    ex_sig = exit_signal_entry_precomputed(
                        regime=str(p.regime),
                        direction=str(getattr(p, "direction", "long") or "long"),
                        i=int(i),
                        close=entry_data[sym]["close"],
                        ema9=entry_ind[sym].get("ema9", []),
                        ema21=entry_ind[sym].get("ema21", []),
                        atr14=entry_ind[sym].get("atr14", []),
                        rsi14=entry_ind[sym].get("rsi14", []),
                        entry_px=float(entry_px),
                    )
                except Exception:
                    ex_sig = None

                # --- strategy-driven exits (range/trend) ---
                if isinstance(ex_sig, dict) and ex_sig.get("action") == "exit":
                    r = str(ex_sig.get("reason") or "")
                    # (A-1) Range expectancy redesign:
                    #  - EMA9 cross exits are optional (flag)
                    #  - EMA21 break exits are disabled (handled via TP/SL/timeout)
                    if str(p.regime).lower() == "range":
                        if r in ("RANGE_EMA9_CROSS_DOWN", "RANGE_EMA9_CROSS_UP") and not bool(getattr(C, "RANGE_EXIT_ON_EMA9_CROSS", False)):
                            ex_sig = None
                        elif r.startswith("RANGE_EMA21_BREAK"):
                            ex_sig = None

                if isinstance(ex_sig, dict) and ex_sig.get("action") == "exit":
                    r = str(ex_sig.get("reason") or "strategy_exit")
                    if r in ("RANGE_EMA9_CROSS_DOWN", "RANGE_EMA9_CROSS_UP"):
                        exit_reason = "RANGE_EMA9_CROSS_EXIT"
                    else:
                        exit_reason = r
                    exit_raw = float(entry_data[sym]["close"][i])
            # --- range bearish dump guard (STOP_HIT_LOSS reduction) ---
            # If market turns bearish while we're in a range trade, cut early to avoid deep stop hits.
            if exit_reason is None and str(p.regime).lower() == "range" and bool(getattr(C, "RANGE_BEARISH_EXIT_ENABLED", False)):
                try:
                    close_now = float(entry_data[sym]["close"][i])
                    ema9_now = float(entry_ind[sym].get("ema9", [])[i])
                    ema21_now = float(entry_ind[sym].get("ema21", [])[i])
                    rsi_now = float(entry_ind[sym].get("rsi14", [])[i])
                    pnl_bps = (close_now - float(entry_px)) / float(entry_px) * 10000.0
                    # Support both historical and sweep-friendly names
                    min_loss_bps = float(getattr(C, "RANGE_BEARISH_EXIT_MIN_LOSS_BPS", getattr(C, "MIN_LOSS_BPS", 10.0)))
                    rsi_th = float(getattr(C, "RANGE_BEARISH_EXIT_RSI_TH", getattr(C, "RANGE_EXIT_RSI_TH", 47.0)))
                    c_lt_e9 = close_now < ema9_now
                    c_lt_e21 = close_now < ema21_now
                    e9_lt_e21 = ema9_now < ema21_now
                    if pnl_bps <= -min_loss_bps:
                        if bool(getattr(C, "RANGE_BEARISH_EXIT_REQUIRE_CLOSE_BELOW_EMA9", True)) and not c_lt_e9:
                            raise ValueError("guard_not_met")
                        if bool(getattr(C, "RANGE_BEARISH_EXIT_REQUIRE_CLOSE_BELOW_EMA21", True)) and not c_lt_e21:
                            raise ValueError("guard_not_met")
                        if bool(getattr(C, "RANGE_BEARISH_EXIT_REQUIRE_EMA9_BELOW_EMA21", True)) and not e9_lt_e21:
                            raise ValueError("guard_not_met")
                        if rsi_now < rsi_th:
                            exit_reason = f"RANGE_BEARISH_EXIT(rsi<{rsi_th:.0f})"
                            exit_raw = float(close_now)
                except Exception:
                    pass

            # --- STOP/TP ---
            stop = float(p.stop)
            tp = float(p.tp)
            entry = float(entry_px)

            # =========================================================
            # BE (Break-Even) for EMA9-break-recent entries (stall guard)
            # Role:
            #   - recent EMA9-break trend entries only
            #   - exit near BE when momentum stalls
            # Priority:
            #   TP/SL -> BE -> TIMEOUT
            # =========================================================
            if exit_reason is None and str(p.regime).lower() == "trend":
                # TREND_BE_RECENT uses the same force-disable gate in backtest.
                be_enable = (not force_disable_be) and bool(getattr(C, "TREND_BE_RECENT_ENABLE", False))
                if be_enable and bool(getattr(p, "ema9_break_recent", False)):
                    # bars held
                    if bar_ms > 0:
                        dt = int(t) - int(p.opened_ts)
                        dt = max(0, dt)
                        if int(t) >= 10_000_000_000_000:  # us
                            bar_unit = bar_ms * 1000
                        else:
                            bar_unit = bar_ms
                        age_bars = int(dt // max(1, bar_unit))
                    else:
                        age_bars = 0

                    be_delay = int(getattr(C, "TREND_BE_RECENT_DELAY_BARS", 3))
                    if age_bars >= be_delay:
                        close_now = float(entry_data[sym]["close"][i])
                        pnl_bps = (close_now - float(entry)) / float(entry) * 10000.0
                        min_pnl = float(getattr(C, "TREND_BE_RECENT_MIN_PROFIT_BPS", 5.0))

                        if pnl_bps >= min_pnl:
                            # optional momentum stall gate: close < EMA9
                            require_below = bool(getattr(C, "TREND_BE_RECENT_REQUIRE_BELOW_EMA9", False))
                            if require_below:
                                try:
                                    ema9_now = float(entry_ind[sym]["ema9"][i])
                                except Exception:
                                    ema9_now = float("nan")
                                if not (ema9_now == ema9_now) or close_now >= ema9_now:
                                    pass
                                else:
                                    exit_reason = f"TREND_BE_RECENT_STALL(pnl={pnl_bps:.1f}bps)"
                                    # exit at BE (fees handled later)
                                    exit_raw = float(entry)
                            else:
                                exit_reason = f"TREND_BE_RECENT_STALL(pnl={pnl_bps:.1f}bps)"
                                exit_raw = float(entry)

            # STOP/TP is resolved first using the intrabar high/low path.
            # If exit_reason was already set by a prior close-based range exit, keep that result.
            # This prevents later STOP/TP checks from overriding the earlier range-exit decision.
            # IMPORTANT:
            # Do NOT infer position side from stop vs entry (a trailing/BE stop can move above entry).
            # Use the position's direction as the source of truth.
            pos_dir = str(getattr(p, "direction", "long") or "long").lower()
            stop_kind = str(getattr(p, "stop_kind", "") or "init")
            hit_tp = (pos_dir == "long" and bar_high >= tp) or (pos_dir == "short" and bar_low <= tp)
            stop_is_profit = False
            if stop_kind in ("be", "trail"):
                if pos_dir == "long":
                    stop_is_profit = (stop >= entry)
                else:
                    stop_is_profit = (stop <= entry)
            hit_sl = (pos_dir == "long" and bar_low <= stop) or (pos_dir == "short" and bar_high >= stop)

            if hit_tp and hit_sl:
                exit_hit = _resolve_tp_sl_same_bar(
                    side=pos_dir,
                    bar_open=float(bar_open),
                    bar_high=float(bar_high),
                    bar_low=float(bar_low),
                    bar_close=float(bar_close),
                    tp=float(tp),
                    sl=float(stop),
                )
            elif hit_sl:
                exit_hit = "STOP_HIT"
            elif hit_tp:
                exit_hit = "TP_HIT"
            else:
                exit_hit = None
            if exit_hit == "STOP_HIT":
                exit_reason = "STOP_HIT"
                exit_raw = stop
            elif exit_hit == "TP_HIT":
                exit_reason = "TP_HIT"
                exit_raw = tp
            if exit_reason is not None and exit_raw is not None:
                # Track RANGE_EMA9_CROSS_EXIT so later entries can honor the cooldown gate.
                # NOTE: actual exit_reason used in this file is "RANGE_EMA9_CROSS_EXIT"
                if str(exit_reason) == "RANGE_EMA9_CROSS_EXIT" and str(p.regime).lower() == "range":
                    last_range_unfav_exit_ts[sym] = int(t)
                # Live/replay keeps an EMA21 reclaim block after RANGE_EARLY_LOSS_ATR.
                if str(exit_reason) in ("RANGE_EMA21_BREAK_EXIT", "RANGE_EARLY_LOSS_ATR") and str(p.regime).lower() == "range":
                    try:
                        reclaim_ema21 = float(ei["ema21"][i])
                    except Exception:
                        reclaim_ema21 = None
                    last_range_ema21_break_block[sym] = {"ts": float(int(t)), "reclaim_ema21": reclaim_ema21}
                exit_exec = float(exit_raw) * _side_slip_mult("exit_long")

                # --- analytics: split STOP_HIT into profit/loss buckets ---
                # Split STOP_HIT into profit/loss buckets after BE/TRAIL stop movement is known.
                # Use the realized exit vs entry to keep the analytics classification stable.
                if str(exit_reason) == "STOP_HIT":
                    try:
                        kind = str(getattr(p, "stop_kind", "") or "").lower()
                        if float(exit_exec) >= float(p.entry_exec):
                            if kind == "trail":
                                exit_reason = "TRAIL_STOP_PROFIT"
                            elif kind == "be":
                                exit_reason = "BE_STOP_PROFIT"
                            else:
                                exit_reason = "STOP_HIT_PROFIT"
                        else:
                            if kind == "trail":
                                exit_reason = "TRAIL_STOP_LOSS"
                            elif kind == "be":
                                exit_reason = "BE_STOP_LOSS"
                            else:
                                exit_reason = "STOP_HIT_LOSS"
                    except Exception:
                        pass

                pnl = (exit_exec - float(p.entry_exec)) * float(p.qty)

                maker = float(fee_maker_rate)
                taker = float(fee_taker_rate)

                entry_type = str(getattr(C, "PAPER_ENTRY_FEE_TYPE", "taker")).lower()
                tp_type = str(getattr(C, "PAPER_TP_FEE_TYPE", "taker")).lower()
                stop_type = str(getattr(C, "PAPER_STOP_FEE_TYPE", "taker")).lower()

                entry_fee_rate = maker if entry_type == "maker" else taker
                is_timeout = str(exit_reason).startswith(("RANGE_TIMEOUT", "TREND_TIMEOUT"))

                timeout_type = str(getattr(C, "PAPER_TIMEOUT_FEE_TYPE", stop_type))
                timeout_fee_rate = maker if timeout_type == "maker" else taker

                # Choose exit fees by exit family: timeout, TP, STOP_HIT*, or fallback.
                if is_timeout:
                    exit_fee_rate = float(timeout_fee_rate)
                elif exit_reason == "TP_HIT":
                    exit_fee_rate = float(maker if tp_type == "maker" else taker)
                elif str(exit_reason).startswith("STOP_HIT"):
                    exit_fee_rate = float(maker if stop_type == "maker" else taker)
                else:
                    # Fallback for all other exit types.
                    exit_fee_rate = float(taker)


                entry_notional = float(p.entry_exec) * float(p.qty)
                exit_notional = exit_exec * float(p.qty)
                mae_abs = max(0.0, float(getattr(p, "mae_abs", 0.0) or 0.0))
                mae_entry_notional = (
                    max(0.0, float(getattr(p, "entry_raw", 0.0) or 0.0))
                    * max(0.0, float(getattr(p, "qty_init", float(p.qty)) or float(p.qty)))
                )
                mae_bps = (float(mae_abs) / float(mae_entry_notional) * 10000.0) if float(mae_entry_notional) > 0.0 else 0.0
                mfe_abs = max(0.0, float(getattr(p, "mfe_abs", 0.0) or 0.0))
                mfe_bps = (float(mfe_abs) / float(mae_entry_notional) * 10000.0) if float(mae_entry_notional) > 0.0 else 0.0
                if pos_dir == "short":
                    best_price_for_giveback = float(getattr(p, "min_adv", entry_price_for_mae) or entry_price_for_mae)
                else:
                    best_price_for_giveback = float(getattr(p, "max_fav", entry_price_for_mae) or entry_price_for_mae)
                if (not math.isfinite(best_price_for_giveback)) or best_price_for_giveback <= 0.0:
                    best_price_for_giveback = float(entry_price_for_mae)
                giveback_max_abs = max(0.0, float(getattr(p, "giveback_max_abs", 0.0) or 0.0))
                if pos_dir == "short":
                    if best_price_for_giveback < float(entry_price_for_mae):
                        giveback_to_close_abs = max(
                            0.0,
                            (float(exit_raw) - float(best_price_for_giveback))
                            * max(0.0, float(getattr(p, "qty_init", float(p.qty)) or float(p.qty))),
                        )
                    else:
                        giveback_to_close_abs = 0.0
                else:
                    if best_price_for_giveback > float(entry_price_for_mae):
                        giveback_to_close_abs = max(
                            0.0,
                            (float(best_price_for_giveback) - float(exit_raw))
                            * max(0.0, float(getattr(p, "qty_init", float(p.qty)) or float(p.qty))),
                        )
                    else:
                        giveback_to_close_abs = 0.0
                giveback_max_bps = (float(giveback_max_abs) / float(mae_entry_notional) * 10000.0) if float(mae_entry_notional) > 0.0 else 0.0
                giveback_to_close_bps = (float(giveback_to_close_abs) / float(mae_entry_notional) * 10000.0) if float(mae_entry_notional) > 0.0 else 0.0
                if float(mfe_abs) > 0.0:
                    giveback_max_pct_of_mfe = float(giveback_max_abs) / float(max(float(mfe_abs), 1e-12))
                    giveback_to_close_pct_of_mfe = float(giveback_to_close_abs) / float(max(float(mfe_abs), 1e-12))
                else:
                    giveback_max_pct_of_mfe = 0.0
                    giveback_to_close_pct_of_mfe = 0.0
                 
                if fast_bt:
                    fee = (entry_notional + exit_notional) * float(fast_fee_rate)
                else:
                    fee = (entry_notional * entry_fee_rate) + (exit_notional * exit_fee_rate)

                net = pnl - fee
                equity += net
                sizing_peak_equity = max(float(sizing_peak_equity), float(equity))
                if equity > peak_mtm:
                    peak_mtm = float(equity)
                if peak_mtm > 0.0:
                    dd_mtm = max(0.0, (float(peak_mtm) - float(equity)) / float(peak_mtm))
                    if dd_mtm > max_dd_mtm:
                        max_dd_mtm = float(dd_mtm)
                # --- STOP_HIT_LOSS pattern mining (entry vs pre-stop vs exit) ---
                entry_diag = getattr(p, "entry_diag", None)
                prestop_diag = None
                exit_diag = None
                bars_to_exit = None
                if exit_reason == "STOP_HIT_LOSS":
                    try:
                        bars_to_exit = int(i) - int(getattr(p, "entry_i", i))
                        pre_i = max(int(i) - 1, int(getattr(p, "entry_i", i)))

                        # entry snapshot (fallback if missing)
                        if entry_diag is None:
                            ej = int(getattr(p, "entry_filter_j", j))
                            try:
                                er, ed = detect_regime_1h_precomputed(
                                    filter_data[s]["close"],
                                    ej,
                                    filter_ind[s]["adx"],
                                    filter_ind[s]["ema_fast"],
                                    filter_ind[s]["ema_slow"],
                                )
                            except Exception:
                                er, ed = None, None
                            entry_diag = _diag_snapshot_basic(
                                entry_data[s]["ts"][int(getattr(p, "entry_i", i))],
                                entry_data[s]["close"][int(getattr(p, "entry_i", i))],
                                ei["ema9"][int(getattr(p, "entry_i", i))],
                                ei["ema21"][int(getattr(p, "entry_i", i))],
                                ei["atr14"][int(getattr(p, "entry_i", i))],
                                ei["rsi14"][int(getattr(p, "entry_i", i))],
                                filter_regime=er,
                                filter_dir=ed,
                                label="entry",
                            )

                        # pre-stop snapshot: the bar right before the stop hit
                        pre_ts = int(entry_data[s]["ts"][pre_i])
                        pre_j = max(0, _find_filter_index(filter_ts[s], pre_ts))
                        try:
                            pr, pd = detect_regime_1h_precomputed(
                                filter_data[s]["close"],
                                pre_j,
                                filter_ind[s]["adx"],
                                filter_ind[s]["ema_fast"],
                                filter_ind[s]["ema_slow"],
                            )
                        except Exception:
                            pr, pd = None, None
                        prestop_diag = _diag_snapshot_basic(
                            pre_ts,
                            entry_data[s]["close"][pre_i],
                            ei["ema9"][pre_i],
                            ei["ema21"][pre_i],
                            ei["atr14"][pre_i],
                            ei["rsi14"][pre_i],
                            filter_regime=pr,
                            filter_dir=pd,
                            label="pre_stop",
                        )

                        # exit snapshot: the bar where stop is triggered
                        exit_ts = int(entry_data[s]["ts"][int(i)])
                        exit_j = max(0, int(j))
                        try:
                            xr, xd = detect_regime_1h_precomputed(
                                filter_data[s]["close"],
                                exit_j,
                                filter_ind[s]["adx"],
                                filter_ind[s]["ema_fast"],
                                filter_ind[s]["ema_slow"],
                            )
                        except Exception:
                            xr, xd = None, None
                        exit_diag = _diag_snapshot_basic(
                            exit_ts,
                            entry_data[s]["close"][int(i)],
                            ei["ema9"][int(i)],
                            ei["ema21"][int(i)],
                            ei["atr14"][int(i)],
                            ei["rsi14"][int(i)],
                            filter_regime=xr,
                            filter_dir=xd,
                            label="exit",
                        )

                        # Flatten and store
                        # Extra OHLC / wick diagnostics for the bar right before stop hit.
                        pre_extra = {}
                        try:
                            pre_i = max(0, i - 1)
                            po = float(bars_5m["open"][pre_i])
                            ph = float(bars_5m["high"][pre_i])
                            pl = float(bars_5m["low"][pre_i])
                            pc = float(bars_5m["close"][pre_i])
                            pre_extra.update({
                                "o": po,
                                "h": ph,
                                "l": pl,
                                "c": pc,
                                "hl": ph - pl,
                                "body": abs(pc - po),
                            })
                            rng = ph - pl
                            if rng > 0:
                                lower = min(po, pc) - pl
                                upper = ph - max(po, pc)
                                pre_extra.update({
                                    "body_ratio": abs(pc - po) / rng,
                                    "lower_wick_ratio": lower / rng,
                                    "upper_wick_ratio": upper / rng,
                                })
                            pe9 = float(ei["ema9"][pre_i])
                            pe21 = float(ei["ema21"][pre_i])
                            if pe9 != 0:
                                pre_extra["low_ema9_dist_bps"] = (pl - pe9) / pe9 * 10000.0
                            if pe21 != 0:
                                pre_extra["low_ema21_dist_bps"] = (pl - pe21) / pe21 * 10000.0
                        except Exception:
                            pre_extra = {}
                        flat = {
                            "symbol": s,
                            "exit_reason": exit_reason,
                            "bars_to_exit": bars_to_exit,
                        }
                        for _prefix, _d in (("entry", entry_diag), ("pre", prestop_diag), ("exit", exit_diag)):
                            if not _d:
                                continue
                            for _k, _v in _d.items():
                                flat[f"{_prefix}_{_k}"] = _v
                        for _k, _v in pre_extra.items():
                            flat[f"pre_{_k}"] = _v
                        stop_loss_patterns.append(flat)
                    except Exception:
                        # Never break the backtest on diagnostics
                        pass
                # Align CLOSE event timestamp with runner/replay:
                # runner emits OPEN/CLOSE at candle_ts_run (= current loop bar open t).
                # Using (t + bar_ms) creates a fixed +1 bar skew on 5m datasets.
                trades.append(
                    {
                        "ts": int(t),
                        "ts_iso": iso_utc(int(t)),
                        "entry_ts_ms": int(getattr(p, "opened_ts", 0) or 0),
                        "close_ts_ms": int(t),
                        "symbol": sym,
                        "reason": str(exit_reason),
                        "exit_reason": str(exit_reason),
                        "close_reason": str(exit_reason),
                        "entry_reason": str(getattr(p, "reason", "")),
                        "open_reason": str(getattr(p, "reason", "")),
                        "bars_to_exit": int(bars_to_exit) if bars_to_exit is not None else int(i) - int(getattr(p, "entry_i", i)),
                        "entry_diag": entry_diag,
                        "prestop_diag": prestop_diag,
                        "exit_diag": exit_diag,
                        "regime": str(p.regime),
                        "direction": str(p.direction),
                        "entry_raw": float(p.entry_raw),
                        "stop_raw": float(p.stop_raw),
                        "tp_raw": float(p.tp_raw),
                        "exit_raw": float(exit_raw),
                        "entry_exec": float(p.entry_exec),
                        "exit_exec": float(exit_exec),
                        "qty": float(p.qty),
                        "stop": float(p.stop),
                        "tp": float(p.tp),
                        "init_stop": float(p.init_stop),
                        "stop_bps": ((p.stop - p.entry_exec) / p.entry_exec) * 10000,
                        "tp_bps": ((p.tp - p.entry_exec) / p.entry_exec) * 10000,
                        "sl_bps_raw": ((p.stop_raw - p.entry_raw) / p.entry_raw) * 10000 if p.entry_raw else None,
                        "tp_bps_raw": ((p.tp_raw - p.entry_raw) / p.entry_raw) * 10000 if p.entry_raw else None,
                        "planned_rr": (p.tp - p.entry_exec) / max(1e-12, (p.entry_exec - p.stop)),
                        "rr0": float(p.rr0) if p.rr0 is not None else None,
                        "rr_adj": float(p.rr_adj) if p.rr_adj is not None else None,
                        "mae_abs": float(mae_abs),
                        "mae_bps": float(mae_bps),
                        "mfe_abs": float(mfe_abs),
                        "mfe_bps": float(mfe_bps),
                        "giveback_max_abs": float(giveback_max_abs),
                        "giveback_max_bps": float(giveback_max_bps),
                        "giveback_max_pct_of_mfe": float(giveback_max_pct_of_mfe),
                        "giveback_to_close_abs": float(giveback_to_close_abs),
                        "giveback_to_close_bps": float(giveback_to_close_bps),
                        "giveback_to_close_pct_of_mfe": float(giveback_to_close_pct_of_mfe),
                        "pnl": float(pnl),
                        "fee": float(fee),
                        "net": float(net),
                        "equity_after": float(equity),
                        "max_fav": float(getattr(p, "max_fav", float("nan"))),
                        "min_adv": float(getattr(p, "min_adv", float("nan"))),
                        "mfe_r": (
                            (float(getattr(p, "max_fav", float("nan"))) - float(p.entry_raw))
                            / (float(p.entry_raw) - float(p.init_stop))
                            if (float(p.entry_raw) - float(p.init_stop)) > 0
                            else None
                        ),
                        "mae_r": (
                            (float(p.entry_raw) - float(getattr(p, "min_adv", float("nan"))))
                            / (float(p.entry_raw) - float(p.init_stop))
                            if (float(p.entry_raw) - float(p.init_stop)) > 0
                            else None
                        ),
                        "tp1_done": bool(getattr(p, "tp1_done", False)),
                        "tp1_exit_notional": float(getattr(p, "tp1_exit_notional", 0.0)),
                        "stop_kind": str(getattr(p, "stop_kind", "")),
                        "be_triggered": bool(getattr(p, "be_triggered", False)),
                        "be_trigger_r": (float(getattr(p, "be_trigger_r", 0.0)) if getattr(p, "be_trigger_r", None) is not None else None),
                        "be_offset_bps": (float(getattr(p, "be_offset_bps", 0.0)) if getattr(p, "be_offset_bps", None) is not None else None),
                        "be_stop_set": (float(getattr(p, "be_stop_set", 0.0)) if getattr(p, "be_stop_set", None) is not None else None),
                        "trail_triggered": bool(getattr(p, "trail_triggered", False)),
                        "trail_start_r": (float(getattr(p, "trail_start_r", 0.0)) if getattr(p, "trail_start_r", None) is not None else None),
                        "trail_bps_from_high": (float(getattr(p, "trail_bps_from_high", 0.0)) if getattr(p, "trail_bps_from_high", None) is not None else None),
                        "start_price": (float(getattr(p, "start_price", 0.0)) if getattr(p, "start_price", None) is not None else None),
                        "opened_ts": int(p.opened_ts),
                    }
                )
                trades[-1].update(
                    R._build_stop_diag_fields(
                        stop_kind=str(getattr(p, "stop_kind", "")),
                        init_stop=float(getattr(p, "init_stop", 0.0) or 0.0),
                        final_stop=float(getattr(p, "stop", 0.0) or 0.0),
                        entry_exec=float(getattr(p, "entry_exec", 0.0) or 0.0),
                        trail_eval_count=getattr(p, "trail_eval_count", 0),
                        trail_candidate_stop_last=getattr(p, "trail_candidate_stop_last", None),
                        trail_candidate_stop_max=getattr(p, "trail_candidate_stop_max", None),
                        trail_candidate_minus_current_stop=getattr(p, "trail_candidate_minus_current_stop", None),
                        trail_candidate_minus_current_stop_max=getattr(p, "trail_candidate_minus_current_stop_max", None),
                        trail_candidate_from_atr_last=getattr(p, "trail_candidate_from_atr_last", None),
                        trail_candidate_from_bps_last=getattr(p, "trail_candidate_from_bps_last", None),
                        trail_eligible_count=getattr(p, "trail_eligible_count", 0),
                        trail_update_count=getattr(p, "trail_update_count", 0),
                        trail_block_reason_last=getattr(p, "trail_block_reason_last", ""),
                        trail_block_reason_max=getattr(p, "trail_block_reason_max", ""),
                        start_price=getattr(p, "start_price", None),
                        trail_start_price_last=getattr(p, "trail_start_price_last", None),
                        trail_start_price_max_context=getattr(p, "trail_start_price_max_context", None),
                        trail_bar_high_last=getattr(p, "trail_bar_high_last", None),
                        trail_bar_high_max=getattr(p, "trail_bar_high_max", None),
                        trail_pos_stop_before_last=getattr(p, "trail_pos_stop_before_last", None),
                        trail_pos_stop_before_max_context=getattr(p, "trail_pos_stop_before_max_context", None),
                        trail_risk_per_unit_last=getattr(p, "trail_risk_per_unit_last", None),
                        trail_mode_last=getattr(p, "trail_mode_last", None),
                        mfe_bps=float(mfe_bps),
                        giveback_to_close_bps=float(giveback_to_close_bps),
                    )
                )
                mae_abs_values.append(float(mae_abs))
                mae_bps_values.append(float(mae_bps))
                mfe_abs_values.append(float(mfe_abs))
                mfe_bps_values.append(float(mfe_bps))
                giveback_max_abs_values.append(float(giveback_max_abs))
                giveback_max_bps_values.append(float(giveback_max_bps))
                giveback_max_pct_values.append(float(giveback_max_pct_of_mfe))
                giveback_to_close_abs_values.append(float(giveback_to_close_abs))
                giveback_to_close_bps_values.append(float(giveback_to_close_bps))
                giveback_to_close_pct_values.append(float(giveback_to_close_pct_of_mfe))
                kept_bps = max(0.0, float(mfe_bps) - float(giveback_to_close_bps))
                kept_bps_values.append(float(kept_bps))
                kept_pct_of_mfe_values.append((float(kept_bps) / max(float(mfe_bps), 1e-12)) if float(mfe_bps) > 0.0 else 0.0)
                fav_adv_ratio_values.append(float(mfe_bps) / max(float(mae_bps), 1e-12))
                # diff trace: CLOSE
                ts_ms = int(t)
                _diff_trace_write(sym, {
                    "event": "CLOSE",
                    "ts_ms": ts_ms,
                    "symbol": sym,
                    "mode": "BACKTEST",
                    "tf_entry": entry_tf,
                    "tf_filter": filter_tf,
                    "bar_open": float(e["open"][i]),
                    "bar_high": float(e["high"][i]),
                    "bar_low": float(e["low"][i]),
                    "bar_close": float(e["close"][i]),
                    "stop_raw": float(getattr(p, "stop_raw", 0.0) or 0.0),
                    "tp_raw": float(getattr(p, "tp_raw", 0.0) or 0.0),
                    "stop_exec": float(getattr(p, "stop", 0.0) or 0.0),
                    "tp_exec": float(getattr(p, "tp", 0.0) or 0.0),
                    "be_triggered": bool(getattr(p, "be_triggered", False)),
                    "be_stop_set": float(getattr(p, "be_stop_set", 0.0) or 0.0) if getattr(p, "be_stop_set", None) is not None else None,
                    "candle_ts_run": int(ts_ms),
                    "candle_ts_entry": int(p.entry_ts),
                    "direction": str(getattr(p, "direction", "none")),
                    "regime": getattr(p, "regime", None),
                    "open_reason": "",
                    "exit_reason": str(exit_reason),
                    "entry_exec": p.entry_exec,
                    "exit_exec": exit_exec,
                    "qty": p.qty,
                    "pnl": pnl,
                    "fee": fee,
                    "net": net,
                    "cfg": None,
                })
                _risk_on_trade_closed(int(t), float(net))
                # update live expectancy buckets (normalized reasons)
                try:
                    bps_real = _realized_bps(
                        entry_px=float(p.entry_exec),
                        exit_px=float(exit_exec),
                        direction=str(p.direction),
                    )
                    if bps_real == bps_real:
                        r0 = _norm_exit_reason(str(exit_reason))
                        k0 = (str(p.regime), str(r0))
                        exit_bps_live_sum[k0] = float(exit_bps_live_sum.get(k0, 0.0)) + float(bps_real)
                        exit_bps_live_n[k0] = int(exit_bps_live_n.get(k0, 0)) + 1
                except Exception:
                    pass
                if is_timeout:
                    forced_closes += 1
                closed_this_bar_reason[sym] = str(exit_reason)
                to_close.append(sym)

                peak = max(peak, equity)
                dd = (peak - equity) / peak if peak > 0 else 0.0
                if dd > max_dd:
                    max_dd = dd

        if in_position_worst_samples > 0:
            in_position_worst_mtm_equity = float(equity_before_manage) + float(in_position_worst_unrealized_pnl)
        else:
            in_position_worst_mtm_equity = float(equity_before_manage)
        if in_position_worst_mtm_equity > peak_worst_bar:
            peak_worst_bar = float(in_position_worst_mtm_equity)
        if peak_worst_bar > 0.0:
            dd_worst_in_position = max(
                0.0,
                (float(peak_worst_bar) - float(in_position_worst_mtm_equity)) / float(peak_worst_bar),
            )
            if dd_worst_in_position > max_dd_worst_bar:
                max_dd_worst_bar = float(dd_worst_in_position)

        for sym in to_close:
            positions.pop(sym, None)

        # ------------------------
        # 2) entries (only symbols that have a bar at t)
        # ------------------------
        if len(positions) < max_open_positions:
            for sym in syms_at_t:
                if len(positions) >= max_open_positions:
                    break
                if sym in positions:
                    continue
                if sym in closed_this_bar_reason and not _same_bar_reentry_allowed_exit_reason(closed_this_bar_reason[sym]):
                    _hold(f"closed_this_bar_no_reentry:{closed_this_bar_reason[sym]}")
                    continue

                i_raw = entry_ptr[sym]
                i = i_raw - 1  # confirmed-bar-only: exclude the current bar at t
                if i < 0:
                    continue

                # Preload-history mode: do not allow entries before trade_since_ms.
                if effective_trade_since_ms is not None and int(t) < int(effective_trade_since_ms):
                    _hold('trade_since_ms')
                    continue

                if i < warmup_bars:
                    _hold('warmup')
                    continue

                e = entry_data[sym]
                f = filter_data[sym]

                # advance filter pointer (no binary search)
                j = filter_ptr[sym]
                # Use the last *confirmed* filter bar.
                # Runner replay fetch_ohlcv() excludes the currently-forming 1h candle,
                # so the 1h pointer must advance on (t - filter_tf_ms), not entry_tf_ms.
                j = _advance_filter_ptr(filter_ts[sym], j, t - filter_bar_ms)
                filter_ptr[sym] = j
                if j < 0:
                    _hold('filter_ptr_before_start')
                    continue

                # 1) Require a minimum active filter window before evaluating signals.
                w_f = int(getattr(C, "BT_FILTER_WINDOW", 500))
                f0 = max(0, (j + 1) - w_f)
                if (j + 1) - f0 < 60:
                    _hold('filter_window_lt60')
                    continue

                # 2) Use precomputed filter indicators for regime detection.
                ei = entry_ind[sym]
                fi = filter_ind[sym]
                regime, direction = detect_regime_1h_precomputed(
                    close=f["close"],
                    i=j,
                    adx_arr=fi["adx"],
                    ema_fast_arr=fi["ema_fast"],
                    ema_slow_arr=fi["ema_slow"],
                )
                # Align backtest with runner: if trend entries are disabled but range
                # entries are enabled, keep evaluating the range path on 1h trend bars.
                try:
                    _trade_trend = float(getattr(C, "TRADE_TREND", 0.0) or 0.0)
                except Exception:
                    _trade_trend = 0.0
                _trade_range = bool(getattr(C, "TRADE_RANGE", True))
                if _trade_trend <= 0.0 and _trade_range and str(regime).lower() == "trend":
                    regime = "range"
                # === Trend direction-none guard (quality filter) ===
                if str(regime).lower() == "trend" and str(direction).lower() == "none" and bool(getattr(C, "TREND_BLOCK_DIR_NONE", False)):
                    sig = {"action": "hold", "reason": "trend_dir_none"}
                    _summ_reason(hold_counter, sig["reason"])
                    continue

                # === Global Downtrend Filter (Spot short substitute) ===
                # Treat 1h trend + short direction as a no-entry state in spot mode.

                reg_l = str(regime).lower()
                dir_l = str(direction).lower()

                adx_th = float(getattr(C, "TREND_ADX_TH", 14.0))
                extra  = float(getattr(C, "DOWN_TREND_BLOCK_ADX_EXTRA", 3.0))

                adx_v = None
                try:
                    # j is the 1h index aligned to current entry time
                    if 0 <= int(j) < len(fi["adx"]):
                        adx_v = float(fi["adx"][int(j)])
                    else:
                        adx_v = None
                except Exception:
                    try:
                        adx_v = float(f_adx[-1])
                    except Exception:
                        adx_v = None

                block_short = (reg_l == "trend" and dir_l == "short")

                # Pullback mode can optionally relax the strong-downtrend guard.
                # The relaxation only applies when the recent EMA9-break condition is met.
                entry_mode = str(getattr(C, "TREND_ENTRY_MODE", "")).upper()

                relax_downtrend_flag = bool(
                    getattr(C, "TREND_PULLBACK_RELAX_DOWNTREND_STRONG_ON_EMA9_BREAK", False)
                )
                ema9_break_recent = False
                if entry_mode == "PULLBACK" and relax_downtrend_flag:
                    # Determine EMA9 break within a recent window (current + previous N bars).
                    # This is a frequency lever used only to relax the strong downtrend global filter.
                    recent_n = int(getattr(C, "TREND_PULLBACK_EMA9_BREAK_RECENT_BARS", 0))
                    lookback = max(0, min(int(recent_n), int(i)))
                    try:
                        relax_bps = float(getattr(C, "TREND_PULLBACK_EMA9_RELAX_BPS", 0.0))
                        body_min = float(getattr(C, "TREND_PULLBACK_EMA9_BODY_RATIO_MIN", 0.55))
                        dist_min_atr = float(getattr(C, "TREND_PULLBACK_MIN_REBOUND_DIST_ATR", 0.0))
                        for k in range(0, lookback + 1):
                            j2 = int(i) - k
                            if j2 <= 0:
                                continue

                            c = float(e["close"][j2])
                            o = float(e["open"][j2])
                            h = float(e["high"][j2])
                            l = float(e["low"][j2])
                            c_prev = float(e["close"][j2 - 1])
                            e9 = float(ei["ema9"][j2])
                            e9_prev = float(ei["ema9"][j2 - 1])

                            strict_cross = (
                                np.isfinite(c)
                                and np.isfinite(c_prev)
                                and np.isfinite(e9)
                                and np.isfinite(e9_prev)
                                and (c > e9)
                                and (c_prev <= e9_prev)
                            )

                            rng = max(1e-12, h - l)
                            body_ratio = abs(c - o) / rng
                            body_ok = (c >= o) and (body_ratio >= body_min)

                            atr_now = float(ei["atr14"][j2]) if "atr14" in ei else None
                            dist_ok = False
                            if atr_now and atr_now > 0 and np.isfinite(c) and np.isfinite(e9):
                                dist_ok = ((c - e9) / atr_now) >= dist_min_atr

                            relaxed_reclaim = (
                                relax_bps > 0.0
                                and np.isfinite(c)
                                and np.isfinite(e9)
                                and (c >= e9 * (1.0 - relax_bps / 10000.0))
                                and body_ok
                                and dist_ok
                            )

                            if strict_cross or relaxed_reclaim:
                                ema9_break_recent = True
                                break
                    except Exception:
                        ema9_break_recent = False

                strong_th = (adx_th + extra)
                if entry_mode == "PULLBACK":
                    strong_th = (adx_th + (2.0 * extra))
                    # If EMA9 break is recent, relax strong downtrend blocking (frequency lever)
                    if ema9_break_recent:
                        strong_th = (adx_th + (3.0 * extra))
                block_strong = (adx_v is not None and adx_v >= strong_th)

                # In 1h trend + short direction, block entries by default.
                # This is a global strong-downtrend guard, not a local range-exit rule.
                # Pullback + recent EMA9 break may bypass it when the relax flag is enabled.
                if block_short and block_strong:
                    # frequency lever:
                    # Allow the configured pullback + recent EMA9-break bypass.
                    if relax_downtrend_flag and ema9_break_recent:
                        pass
                    else:
                        # Decompose: why did we block?
                        if relax_downtrend_flag:
                            _hold(
                                "downtrend_global_filter_strong:"
                                f"relax_on_no_ema9_break_recent("
                                f"n={int(getattr(C, 'TREND_PULLBACK_EMA9_BREAK_RECENT_BARS', 0))},"
                                f"adx={float(adx_v):.2f},th={float(strong_th):.2f},mode={str(entry_mode)})"
                            )
                        else:
                            _hold(
                                "downtrend_global_filter_strong:"
                                f"relax_off(adx={float(adx_v):.2f},th={float(strong_th):.2f},mode={str(entry_mode)})"
                            )
                        continue
                reg_l = str(regime).lower()
                if reg_l == "range":
                    # --- trend-only / range-disable gate (IMPORTANT) ---
                    if bool(getattr(C, "TRADE_ONLY_TREND", False)) and not bool(
                        getattr(C, "ALLOW_RANGE_TRADES", False)
                    ):
                        sig = {"action": "hold", "reason": "trade_only_trend"}
                    elif not bool(getattr(C, "TRADE_RANGE", True)):
                        sig = {"action": "hold", "reason": "range_disabled"}
                    else:
                        sig = signal_range_entry_precomputed(
                            regime=str(regime),
                            direction=str(direction),
                            i=i,
                            open_=e["open"],
                            high=e["high"],
                            low=e["low"],
                            close=e["close"],
                            ema9=ei["ema9"],
                            ema21=ei["ema21"],
                            rsi14=ei["rsi14"],
                            atr14=ei["atr14"],
                            recent_low=ei["range_recent_low"],
                        )

                    if sig.get("action") != "buy":
                        _track_pullback_funnel(sig.get("action", "hold"), sig.get("reason"))
                        _hold(sig.get("reason", "unknown"))
                        continue

                    allowed, r_reason = _risk_allow_new_entry(int(t))
                    if not allowed:
                        _hold(f"risk_gate:{r_reason}")
                        continue

                else:
                    # Trend gate / Phase A-B state is maintained in runner-style KV so live/backtest match.
                    if not bool(getattr(C, "TRADE_TREND", True)):
                        sig = {"action": "hold", "reason": "trend_disabled"}
                        _track_pullback_funnel(sig.get("action", "hold"), sig.get("reason"))
                        _hold(sig.get("reason", "unknown"))
                        continue

                    pb_state = _kv_get_pullback_ab(state_kv, sym)
                    if signal_entry_stateful is not None:
                        sig, pb_state = signal_entry_stateful(
                            pb_state=pb_state,
                            # confirmed-bar-only parity with runner:
                            # treat "now" as just-before current bar open so the bar at t is NOT included.
                            now_ms=(int(t) - 1),
                            regime="trend",
                            direction=str(direction),
                            open_=e["open"],
                            high=e["high"],
                            low=e["low"],
                            close=e["close"],
                            high_=e["high"],
                            low_=e["low"],
                            close_=e["close"],
                            ema9=ei["ema9"],
                            ema21=ei["ema21"],
                            rsi14=ei["rsi14"],
                            atr14=ei["atr14"],
                        )
                    else:
                        # Fallback: keep legacy behavior, but still maintain runner-compatible KV container.
                        sig = signal_entry_precomputed(
                            open_=e["open"],
                            high=e["high"],
                            low=e["low"],
                            close=e["close"],
                            ema9=ei["ema9"],
                            ema21=ei["ema21"],
                            rsi14=ei["rsi14"],
                            atr14=ei["atr14"],
                            recent_min_low=ei["recent_min_low"],
                            prev_high=ei["prev_high"],
                            prev_low=ei["prev_low"],
                            # confirmed-bar-only parity with runner:
                            # treat "now" as just-before current bar open so the bar at t is NOT included.
                            now_ms=(int(t) - 1),
                            regime="trend",
                            direction=str(direction),
                        )
                    _kv_set_pullback_ab(state_kv, sym, pb_state)

                    if sig.get("action") != "buy":
                        _track_pullback_funnel(sig.get("action", "hold"), sig.get("reason"))
                        _hold(sig.get("reason", "unknown"))
                        continue

                buy_signals += 1

                kill_block, kill_reason = _kill_should_block_new_entries(
                    ts_ms=int(t),
                    equity_now=float(equity),
                    peak_equity=float(peak),
                    spread_bps=float(spread_bps_est),
                )
                if kill_block:
                    _hold(f"kill_switch:{kill_reason}")
                    continue

                # --- range unfavorable-exit cooldown gate ---
                # After an unfavorable RANGE_EMA9_CROSS_EXIT, block re-entry for a short cooldown.
                # Clear the block early once price reclaims EMA9.
                if reg_l == "range":
                    cd_bars = int(getattr(C, "RANGE_UNFAV_EXIT_COOLDOWN_BARS", 0))
                    if cd_bars > 0 and bar_ms > 0:
                        last_ts = int(last_range_unfav_exit_ts.get(sym, 0))
                        if last_ts > 0:
                            dt = int(t) - last_ts
                            dt = max(0, dt)
                            # t/last_ts may arrive in seconds or microseconds.
                            if int(t) >= 10_000_000_000_000:  # Treat very large values as microseconds.
                                bar_unit = bar_ms * 1000
                            else:
                                bar_unit = bar_ms
                            age_bars = int(dt // max(1, bar_unit))

                            # Early clear: reclaim once close >= EMA9.
                            try:
                                ema9_now = float(ei["ema9"][i])
                                close_now = float(e["close"][i])
                                if close_now >= ema9_now:
                                    # EMA9 reclaim cancels the remaining cooldown.
                                    last_range_unfav_exit_ts.pop(sym, None)
                                    age_bars = cd_bars  # Mark cooldown as satisfied.
                            except Exception:
                                pass

                            if age_bars < cd_bars:
                                remain = int(cd_bars - age_bars)
                                buy_reject[
                                    f"range_cooldown_after_ema9_exit(remain={remain}bars n={cd_bars})"
                                ] += 1
                                _hold(f"range_cooldown_after_ema9_exit(remain={remain}bars n={cd_bars})")
                                continue

                # --- range EMA21-break / early-loss re-entry block ---
                # After RANGE_EMA21_BREAK / RANGE_EARLY_LOSS_ATR, block re-entry until price reclaims EMA21.
                if reg_l == "range":
                    blk = last_range_ema21_break_block.get(sym)
                    if isinstance(blk, dict) and blk.get("reclaim_ema21") is not None:
                        # Safety valve: never block forever even if reclaim condition is not met.
                        # Default: 16 bars. Config override: RANGE_EMA21_BREAK_BLOCK_MAX_BARS.
                        try:
                            set_ts = int(float(blk.get("ts", 0.0) or 0.0))
                        except Exception:
                            set_ts = 0
                        max_bars = int(getattr(C, "RANGE_EMA21_BREAK_BLOCK_MAX_BARS", 16) or 16)
                        max_bars = max(1, min(max_bars, 200))
                        if set_ts > 0 and bar_ms > 0:
                            dt = int(t) - int(set_ts)
                            dt = max(0, dt)
                            if int(t) >= 10_000_000_000_000:  # us
                                bar_unit = bar_ms * 1000
                            else:
                                bar_unit = bar_ms
                            age_bars = int(dt // max(1, bar_unit))
                            if age_bars >= max_bars:
                                last_range_ema21_break_block.pop(sym, None)
                                blk = None

                    if isinstance(blk, dict) and blk.get("reclaim_ema21") is not None:
                        try:
                            reclaim = float(blk.get("reclaim_ema21"))
                        except Exception:
                            reclaim = float("nan")
                        buf_bps = float(getattr(C, "RANGE_EMA21_BREAK_BUF_BPS", 0.0) or 0.0)
                        if math.isfinite(reclaim):
                            if float(entry_data[sym]["close"][i]) >= reclaim * (1.0 + buf_bps / 10_000.0):
                                # clear block once reclaimed
                                last_range_ema21_break_block.pop(sym, None)
                            else:
                                rr = "range_block_after_ema21_break_exit(until=close_reclaim_ema21)"
                                buy_reject[rr] += 1
                                _hold(rr)
                                continue

                def _buy_reject(reason: str) -> None:
                    buy_reject[str(reason)] += 1
                    _hold(str(reason))

                entry_raw = float(sig["entry"])
                stop_raw = float(sig["stop"])
                tp_raw = float(sig["take_profit"])
                stop_raw_pre = float(stop_raw)
                tp_raw_pre = float(tp_raw)
                rr0 = None
                risk0 = entry_raw - stop_raw_pre
                reward0 = tp_raw_pre - entry_raw
                if risk0 > 0 and reward0 > 0:
                    rr0 = reward0 / risk0

                spread_bps = float(spread_bps_est)

                fcfg = R._filters_for_regime(str(regime))

                do_adj = bool(fcfg.get("ADJUST_TP_SL", True))
                do_exp = bool(fcfg.get("EXPECTANCY", True))
                # === Entry RR gate ===
                # If ADJUST_TP_SL is enabled, defer RR rejection until after the adjustment step.
                # This avoids killing otherwise-good candidates where TP/SL is improved by _adjust_tp_sl.
                # Choose RR threshold by regime.
                pre_rr_fail = False
                if reg_l == "range":
                    # Range uses its dedicated RR floor when configured.
                    min_rr_entry = float(
                        getattr(
                            C,
                            "RANGE_MIN_RR_ENTRY",
                            getattr(C, "RANGE_MIN_RR_AFTER_ADJUST", getattr(C, "MIN_RR_ENTRY", 0.0)),
                        )
                    )
                else:
                    # Trend uses regime/direction-specific RR floors.
                    base = float(getattr(C, "MIN_RR_AFTER_ADJUST_TREND_LONG", getattr(C, "MIN_RR_ENTRY", 0.0)))
                    if dir_l == "short":
                        min_rr_entry = float(getattr(C, "MIN_RR_AFTER_ADJUST_TREND_SHORT", base))
                    elif dir_l == "long":
                        min_rr_entry = float(getattr(C, "MIN_RR_AFTER_ADJUST_TREND_LONG", base))
                    else:
                        # dir=none uses the fallback trend RR floor.
                        min_rr_entry = float(getattr(C, "MIN_RR_AFTER_ADJUST_TREND_NONE", max(1.4, base)))

                if min_rr_entry and min_rr_entry > 0:
                    risk = entry_raw - stop_raw
                    reward = tp_raw - entry_raw
                    b_reason = f"entry_rr_invalid(risk={risk:.6f},reward={reward:.6f} reg={reg_l} dir={dir_l})"
                    if risk <= 0 or reward <= 0:
                        _buy_reject(b_reason)
                        continue
                    rr0 = reward / risk
                    if rr0 + 1e-9 < min_rr_entry:
                        # If we are going to adjust TP/SL, give it a chance to fix RR.
                        if do_adj and (reg_l != "range") and ((not fast_bt) or (not fast_skip_adj)):
                            pre_rr_fail = True
                        else:
                            _buy_reject(
                                f"entry_rr(rr={rr0:.2f} < {min_rr_entry:.2f} reg={reg_l} dir={dir_l})"
                            )
                            continue

                if (not fast_bt) or (not fast_skip_adj):
                    if do_adj and (reg_l != "range"):
                        w_e = int(getattr(C, "BT_ENTRY_WINDOW", 500))
                        e0 = max(0, (i + 1) - w_e)
                        e_high = e["high"][e0 : i + 1]
                        e_low  = e["low"][e0 : i + 1]
                        e_close= e["close"][e0 : i + 1]

                        ok_adj, stop_raw, tp_raw, _reason = R._adjust_tp_sl(
                            symbol=sym,
                            entry=entry_raw,
                            stop_price=stop_raw,
                            tp_price=tp_raw,
                            spread_bps=spread_bps,
                            high=e_high,
                            low=e_low,
                            close=e_close,
                            regime=str(regime),
                            direction=str(direction),
                        )
                        if not ok_adj:
                            _buy_reject(f'adjust_tp_sl({_reason})')
                            continue
                # If we deferred RR check (because adjust_tp_sl is on), enforce it now using adjusted TP/SL.
                if pre_rr_fail:
                    risk1 = entry_raw - stop_raw
                    reward1 = tp_raw - entry_raw
                    b_reason = f"entry_rr_invalid_after_adjust(risk={risk1:.6f},reward={reward1:.6f} reg={reg_l} dir={dir_l})"
                    if risk1 <= 0 or reward1 <= 0:
                        _buy_reject(b_reason)
                        continue
                    rr1 = reward1 / risk1
                    if rr1 + 1e-9 < min_rr_entry:
                        _buy_reject(f"entry_rr_after_adjust(rr={rr1:.2f} < {min_rr_entry:.2f} reg={reg_l} dir={dir_l})")
                        continue
                if (not fast_bt) or (not fast_skip_exp):
                    if do_exp:
                        ok_exp, _reason2 = R._expectancy_filter(entry_raw, stop_raw, tp_raw, spread_bps)
                        if not ok_exp:
                            _buy_reject(f'expectancy({_reason2})')
                            continue

                # Gate entries by realized net expectancy so far (includes EARLY_LOSS / EMA9_CROSS_EXIT / TIMEOUT).
                if (not fast_bt) or (not fast_skip_exp):
                    if do_exp:
                        min_n = int(getattr(C, "EXPECTANCY_MIN_EXITS_FOR_GATING", 200) or 200)
                        min_exp_net = float(getattr(C, "EXPECTANCY_MIN_NET_BPS_FOR_ENTRY", -1.0) or -1.0)
                        exp_net_live, n_live = _live_expectancy_net_bps(str(reg_l))
                        if n_live >= min_n and exp_net_live == exp_net_live and exp_net_live + 1e-9 < min_exp_net:
                            _buy_reject(
                                f"expectancy_live(exp_net={exp_net_live:.2f}bps n={n_live} < {min_exp_net:.2f})"
                            )
                            continue

                rr_adj = None
                risk_adj = entry_raw - stop_raw
                reward_adj = tp_raw - entry_raw
                if risk_adj > 0 and reward_adj > 0:
                    rr_adj = reward_adj / risk_adj

                max_spread = float(getattr(C, "MAX_SPREAD_BPS_FOR_ENTRY", 12.0))
                if str(regime).lower() == "range":
                    max_spread = float(getattr(C, "RANGE_MAX_SPREAD_BPS_FOR_ENTRY", max_spread))
                if spread_bps > max_spread:
                    _buy_reject('spread_too_wide')
                    continue

                sizing_mode = str(
                    os.getenv(
                        "POSITION_SIZING_MODE",
                        getattr(C, "POSITION_SIZING_MODE", "LEGACY"),
                    )
                ).upper()
                size_ab_enabled = bool(size_min_bump_enabled or size_cap_ramp_enabled)
                cap_pct_eff = float(max_pos_pct)
                risk_mult_cap = 1.0
                requested_qty = 0.0
                min_amt = 0.0
                min_cost = 0.0
                if not size_ab_enabled:
                    qty_legacy = float(calc_qty_from_risk(equity, entry_raw, stop_raw))
                    max_notional = float(equity) * float(max_pos_pct)
                    if entry_raw > 0 and max_notional > 0:
                        qty_legacy = min(qty_legacy, max_notional / entry_raw)

                    qty = float(qty_legacy)
                    if sizing_mode == "LEGACY_COMPOUND":
                        alpha = float(getattr(C, "LEGACY_COMPOUND_ALPHA", 0.5))
                        mult_cap = float(getattr(C, "LEGACY_COMPOUND_MULT_CAP", 2.5))
                        dd_th = float(getattr(C, "DD_DELEVER_THRESHOLD", 0.02))
                        dd_min_mult = float(getattr(C, "DD_DELEVER_MIN_MULT", 0.25))
                        eq0 = float(sizing_initial_equity) if float(sizing_initial_equity) > 0 else 1.0
                        eq_now = float(equity)
                        eq_for_scale = float(eq_now)
                        if profit_only_enabled:
                            profit = max(0.0, float(eq_now) - float(eq0))
                            w_eff = float(profit_reinvest_w)
                            if profit_w_ramp_enabled:
                                profit_cap = float(eq0) * float(profit_w_ramp_pct)
                                if profit_cap > 0.0:
                                    x = float(profit) / float(profit_cap)
                                else:
                                    x = 1.0 if float(profit) > 0.0 else 0.0
                                x = max(0.0, min(1.0, float(x)))
                                g0 = float(x) * float(x) * (3.0 - 2.0 * float(x))
                                shape = max(0.0, float(profit_w_ramp_shape))
                                g = float(g0) ** float(shape)
                                g = max(float(g), float(profit_w_ramp_min_g))
                                w_eff = float(profit_reinvest_w) + (1.0 - float(profit_reinvest_w)) * float(g)
                            eq_for_scale = float(eq0) + float(profit) * float(w_eff)
                            if profit_only_floor_to_initial:
                                eq_for_scale = max(float(eq_for_scale), float(eq0))
                        eq_ratio = float(eq_for_scale) / float(eq0)
                        if not math.isfinite(eq_ratio) or eq_ratio <= 0.0:
                            eq_ratio = 1.0
                        scale_pow = float(eq_ratio) ** float(alpha)
                        scale_pow = min(float(scale_pow), float(mult_cap))
                        peak_eq = float(sizing_peak_equity) if float(sizing_peak_equity) > 0.0 else float(eq_now)
                        dd_ratio = max(0.0, (float(peak_eq) - float(eq_now)) / float(peak_eq)) if float(peak_eq) > 0.0 else 0.0
                        if dd_smooth_enabled:
                            lam = float(dd_smooth_lam)
                            sizing_dd_ema = float(sizing_dd_ema) * (1.0 - lam) + float(dd_ratio) * lam
                            dd_eff = float(sizing_dd_ema)
                        else:
                            dd_eff = float(dd_ratio)
                        if dd_eff <= 0.0:
                            risk_mult = 1.0
                        elif float(dd_th) > 0.0 and dd_eff >= float(dd_th):
                            risk_mult = float(dd_min_mult)
                        elif float(dd_th) > 0.0:
                            risk_mult = 1.0 - (float(dd_eff) / float(dd_th)) * (1.0 - float(dd_min_mult))
                        else:
                            risk_mult = 1.0
                        risk_mult_cap = float(risk_mult)
                        final_mult = max(float(scale_pow) * float(risk_mult), 0.0)
                        if math.isfinite(final_mult) and final_mult > 0.0:
                            qty = float(qty_legacy) * float(final_mult)
                        elif final_mult == 0.0:
                            qty = 0.0

                    if qty <= 0:
                        _buy_reject("qty_zero_or_negative")
                        continue

                    requested_qty = float(qty)
                    try:
                        min_amt, min_cost = ex.market_amount_rules(sym)
                        if min_cost and (qty * entry_raw) < float(min_cost):
                            _buy_reject('below_min_cost')
                            continue
                        if min_amt and qty < float(min_amt):
                            _buy_reject('below_min_amount')
                            continue
                        qty = float(ex.amount_to_precision(sym, qty))
                    except Exception:
                        pass
                    qty = float(_amount_to_precision_deterministic(ex, sym, qty))

                    if qty <= 0:
                        _buy_reject('qty_zero_after_precision')
                        continue
                else:
                    qty0_base = float(calc_qty_from_risk(equity, entry_raw, stop_raw))
                    qty0 = float(qty0_base)
                    qty1 = float(qty0)
                    qty2 = float(qty1)
                    qty3 = float(qty2)
                    qty_cap = 0.0
                    qty_req = 0.0
                    cap_pct_base = float(max_pos_pct)
                    cap_pct_eff = float(cap_pct_base)
                    risk_mult_cap = 1.0
                    profit_ratio = 0.0
                    min_bump_applied = False
                    cap_ramp_applied = False

                    eq0 = float(sizing_initial_equity) if float(sizing_initial_equity) > 0 else 1.0
                    eq_now = float(equity)
                    if eq0 > 0.0:
                        profit_ratio = max(0.0, (float(eq_now) - float(eq0)) / float(eq0))

                    if sizing_mode == "LEGACY_COMPOUND":
                        alpha = float(getattr(C, "LEGACY_COMPOUND_ALPHA", 0.5))
                        mult_cap = float(getattr(C, "LEGACY_COMPOUND_MULT_CAP", 2.5))
                        dd_th = float(getattr(C, "DD_DELEVER_THRESHOLD", 0.02))
                        dd_min_mult = float(getattr(C, "DD_DELEVER_MIN_MULT", 0.25))
                        eq_for_scale = float(eq_now)
                        if profit_only_enabled:
                            profit = max(0.0, float(eq_now) - float(eq0))
                            w_eff = float(profit_reinvest_w)
                            if profit_w_ramp_enabled:
                                profit_cap = float(eq0) * float(profit_w_ramp_pct)
                                if profit_cap > 0.0:
                                    x = float(profit) / float(profit_cap)
                                else:
                                    x = 1.0 if float(profit) > 0.0 else 0.0
                                x = max(0.0, min(1.0, float(x)))
                                g0 = float(x) * float(x) * (3.0 - 2.0 * float(x))
                                shape = max(0.0, float(profit_w_ramp_shape))
                                g = float(g0) ** float(shape)
                                g = max(float(g), float(profit_w_ramp_min_g))
                                w_eff = float(profit_reinvest_w) + (1.0 - float(profit_reinvest_w)) * float(g)
                            eq_for_scale = float(eq0) + float(profit) * float(w_eff)
                            if profit_only_floor_to_initial:
                                eq_for_scale = max(float(eq_for_scale), float(eq0))
                        eq_ratio = float(eq_for_scale) / float(eq0)
                        if not math.isfinite(eq_ratio) or eq_ratio <= 0.0:
                            eq_ratio = 1.0
                        scale_pow = float(eq_ratio) ** float(alpha)
                        scale_pow = min(float(scale_pow), float(mult_cap))
                        peak_eq = float(sizing_peak_equity) if float(sizing_peak_equity) > 0.0 else float(eq_now)
                        dd_ratio = max(0.0, (float(peak_eq) - float(eq_now)) / float(peak_eq)) if float(peak_eq) > 0.0 else 0.0
                        if dd_smooth_enabled:
                            lam = float(dd_smooth_lam)
                            sizing_dd_ema = float(sizing_dd_ema) * (1.0 - lam) + float(dd_ratio) * lam
                            dd_eff = float(sizing_dd_ema)
                        else:
                            dd_eff = float(dd_ratio)
                        if dd_eff <= 0.0:
                            risk_mult = 1.0
                        elif float(dd_th) > 0.0 and dd_eff >= float(dd_th):
                            risk_mult = float(dd_min_mult)
                        elif float(dd_th) > 0.0:
                            risk_mult = 1.0 - (float(dd_eff) / float(dd_th)) * (1.0 - float(dd_min_mult))
                        else:
                            risk_mult = 1.0
                        risk_mult_cap = float(risk_mult)
                        final_mult = max(float(scale_pow) * float(risk_mult), 0.0)
                        if math.isfinite(final_mult) and final_mult > 0.0:
                            qty0 = float(qty0_base) * float(final_mult)
                        elif final_mult == 0.0:
                            qty0 = 0.0

                    cap_pct_eff, cap_ramp_applied = _resolve_size_cap_pct_eff(
                        cap_pct_base=float(cap_pct_base),
                        cap_ramp_enabled=bool(size_cap_ramp_enabled),
                        cap_ramp_k=float(size_cap_ramp_k),
                        cap_ramp_max_pct=float(size_cap_ramp_max_pct),
                        profit_ratio=float(profit_ratio),
                        risk_mult_cap=float(risk_mult_cap),
                    )
                    max_notional = float(equity) * float(cap_pct_eff)
                    if entry_raw > 0 and max_notional > 0.0:
                        qty_cap = float(max_notional) / float(entry_raw)
                    else:
                        qty_cap = 0.0
                    qty1 = min(float(qty0), float(qty_cap))
                    qty2 = float(qty1)

                    try:
                        min_amt, min_cost = ex.market_amount_rules(sym)
                    except Exception:
                        min_amt, min_cost = 0.0, 0.0
                    qty_req_cost = (float(min_cost) / float(entry_raw)) if (entry_raw > 0 and min_cost) else 0.0
                    qty_req_amt = float(min_amt) if min_amt else 0.0
                    qty_req = max(float(qty_req_cost), float(qty_req_amt))

                    if bool(size_min_bump_enabled) and qty_req > 0.0 and float(qty2) < float(qty_req):
                        qty_candidate = float(qty_req)
                        bump_amt = float(qty_candidate) - float(qty2)
                        bump_limit = float(qty_cap) * float(size_min_bump_max_pct_of_cap)
                        if qty_candidate <= float(qty_cap) and bump_amt <= float(bump_limit):
                            qty2 = float(qty_candidate)
                            min_bump_applied = True
                        else:
                            _buy_reject("min_bump_not_feasible_or_over_limit")
                            continue

                    if min_bump_applied:
                        step = _infer_amount_step(ex, sym)
                        qty3 = _ceil_to_step(float(qty2), float(step))
                        if qty3 > (float(qty_cap) + 1e-12):
                            _buy_reject("qty_over_cap_after_ceil_round")
                            continue
                        requested_qty = float(qty3)
                        try:
                            qty = float(ex.amount_to_precision(sym, qty3))
                        except Exception:
                            qty = float(qty3)
                    else:
                        requested_qty = float(qty2)
                        try:
                            qty = float(ex.amount_to_precision(sym, qty2))
                        except Exception:
                            qty = float(qty2)
                    qty = float(_amount_to_precision_deterministic(ex, sym, qty))

                    if qty <= 0.0:
                        _buy_reject("qty_zero_after_precision")
                        continue
                    if min_cost and (qty * entry_raw) < float(min_cost):
                        _buy_reject('below_min_cost')
                        continue
                    if min_amt and qty < float(min_amt):
                        _buy_reject('below_min_amount')
                        continue

                    if size_sizing_debug_log_enabled:
                        logger.info(
                            "[SIZE_AB] qty0=%.8f qty1=%.8f qty2=%.8f qty3=%.8f qty_cap=%.8f qty_req=%.8f cap_pct_base=%.6f cap_pct_eff=%.6f profit_ratio=%.6f risk_mult_cap=%.6f min_bump_applied=%s cap_ramp_applied=%s",
                            float(qty0),
                            float(qty1),
                            float(qty2),
                            float(qty3 if min_bump_applied else qty),
                            float(qty_cap),
                            float(qty_req),
                            float(cap_pct_base),
                            float(cap_pct_eff),
                            float(profit_ratio),
                            float(risk_mult_cap),
                            bool(min_bump_applied),
                            bool(cap_ramp_applied),
                        )

                entry_exec_src = float(e["open"][i])
                entry_exec = float(entry_exec_src) * _side_slip_mult("entry_long")

                # --- entry-time EMA9 break recent context (for conditional TIMEOUT) ---
                ema9_break_recent = False
                ema9_break_age = None
                try:
                    recent_bars = int(getattr(C, "TREND_PULLBACK_EMA9_BREAK_RECENT_BARS", 0))
                    buf_bps = float(getattr(C, "TREND_PULLBACK_BREAK_BUF_BPS", 0.0))
                    if recent_bars > 0:
                        close_arr = entry_data[sym]["close"]
                        ema9_arr = entry_ind[sym]["ema9"]
                        ema9_break_age = _ema9_break_recent_age(close_arr, ema9_arr, int(i), recent_bars, buf_bps)
                        ema9_break_recent = (ema9_break_age is not None)
                except Exception:
                    ema9_break_recent = False
                    ema9_break_age = None

                t_confirmed = int(t)
                trail_diag_defaults = R._trail_diag_defaults(
                    str(regime),
                    init_stop=float(stop_raw),
                    entry_exec=float(entry_exec),
                )
                positions[sym] = Position(
                    symbol=sym,
                    entry_raw=float(entry_raw),
                    entry_exec=float(entry_exec),
                    qty=float(qty),
                    qty_init=float(qty),
                    stop=float(stop_raw),
                    tp=float(tp_raw),
                    stop_raw=float(stop_raw_pre),
                    tp_raw=float(tp_raw_pre),
                    opened_ts=int(t_confirmed),
                    entry_i=int(i),
                    entry_ts=int(t_confirmed),
                    entry_filter_j=int(j),
                    entry_diag=_diag_snapshot_basic(int(t), float(entry_raw), float(ei["ema9"][i]), float(ei["ema21"][i]), float(ei["atr14"][i]), float(ei["rsi14"][i]), label="entry"),
                    regime=str(regime),
                    direction=_normalize_position_direction(direction),
                    rr0=rr0,
                    rr_adj=rr_adj,
                    init_stop=float(stop_raw),
                    max_fav=float(entry_exec),
                    min_adv=float(entry_exec),
                    trail_start_r=(
                        float(trail_diag_defaults.get("trail_start_r"))
                        if trail_diag_defaults.get("trail_start_r") is not None
                        else None
                    ),
                    trail_bps_from_high=(
                        float(trail_diag_defaults.get("trail_bps_from_high"))
                        if trail_diag_defaults.get("trail_bps_from_high") is not None
                        else None
                    ),
                    start_price=(
                        float(trail_diag_defaults.get("start_price"))
                        if trail_diag_defaults.get("start_price") is not None
                        else None
                    ),
                    ema9_break_recent=bool(ema9_break_recent),
                    ema9_break_age=(int(ema9_break_age) if ema9_break_age is not None else None),
                )

                if _is_ethusdc_symbol(sym, exchange_id=exchange_id):
                    try:
                        equity_before_open = float(equity)
                        rounded_qty = float(qty)
                        requested_qty_diag = float(requested_qty if requested_qty > 0.0 else rounded_qty)
                        notional = float(entry_exec) * float(rounded_qty)
                        notional_pct_of_equity = (
                            float(notional) / float(equity_before_open)
                            if float(equity_before_open) > 0.0
                            else 0.0
                        )
                        size_sizing_open_notional_pcts.append(float(notional_pct_of_equity))
                        if size_sizing_diag_printed < size_sizing_diag_limit:
                            logger.info(
                                "[SIZE_SIZING_DIAG] symbol=%s ts=%s equity_before_open=%.8f sizing_initial_equity=%.8f sizing_peak_equity=%.8f max_pos_pct=%.6f cap_pct_eff=%.6f requested_qty=%.8f rounded_qty=%.8f notional=%.8f notional_pct_of_equity=%.6f profit_only_enabled=%s risk_mult_cap=%.6f",
                                sym,
                                int(t_confirmed),
                                float(equity_before_open),
                                float(sizing_initial_equity),
                                float(sizing_peak_equity),
                                float(max_pos_pct),
                                float(cap_pct_eff),
                                float(requested_qty_diag),
                                float(rounded_qty),
                                float(notional),
                                float(notional_pct_of_equity),
                                bool(profit_only_enabled),
                                float(risk_mult_cap),
                            )
                            size_sizing_diag_printed += 1
                    except Exception:
                        pass

                if (
                    open_cost_diag_printed < open_cost_diag_limit
                    and _is_ethusdc_symbol(sym, exchange_id=exchange_id)
                ):
                    try:
                        rounded_qty = float(qty)
                        requested_qty_diag = float(requested_qty if requested_qty > 0.0 else rounded_qty)
                        notional = float(entry_exec) * float(rounded_qty)
                        expected_cost_abs = float(notional) * (
                            float(fee_bps_round_gate) + float(spread_bps) + float(open_cost_slippage_bps)
                        ) / 10000.0
                        logger.info(
                            "[OPEN_COST_DIAG] symbol=%s ts=%s requested_qty=%.8f rounded_qty=%.8f notional=%.8f min_cost=%.8f fee_bps_round=%.4f spread_bps=%.4f slippage_bps=%.4f expected_cost_abs=%.8f",
                            sym,
                            int(t_confirmed),
                            float(requested_qty_diag),
                            float(rounded_qty),
                            float(notional),
                            float(min_cost),
                            float(fee_bps_round_gate),
                            float(spread_bps),
                            float(open_cost_slippage_bps),
                            float(expected_cost_abs),
                        )
                        open_cost_diag_printed += 1
                    except Exception:
                        pass

                # diff trace: OPEN
                # runner/replay emits OPEN at candle_ts_run (= current loop bar open t).
                # Using +entry_tf_ms here introduces a deterministic +1 bar skew.
                ts_ms = int(t_confirmed)
                ts_ms_run = ts_ms
                _dir = str(direction).lower()
                if _dir in ("buy", "long"):
                    _dir = "long"
                elif _dir in ("sell", "short"):
                    _dir = "short"
                else:
                    _dir = "none"
                _diff_trace_write(sym, {
                    "event": "OPEN",
                    "ts_ms": ts_ms_run,
                    # OHLC of the *current 5m bar* that produced this OPEN decision.
                    "bar_open": float(e["open"][i]),
                    "bar_high": float(e["high"][i]),
                    "bar_low": float(e["low"][i]),
                    "bar_close": float(e["close"][i]),
                    "symbol": sym,
                    "mode": "BACKTEST",
                    "tf_entry": entry_tf,
                    "tf_filter": filter_tf,
                    "candle_ts_run": int(ts_ms_run),
                    "candle_ts_entry": int(ts_ms),
                    "direction": str(direction if direction is not None else "none"),
                    "regime": (str(regime).lower() if regime is not None else None),
                    "open_reason": "PAPER_OPEN",
                    "qty": qty,
                    "entry_exec": entry_exec,
                    "entry_raw": entry_raw,
                    "stop_raw": stop_raw,
                    "tp_raw": tp_raw,
                    "rr0": rr0,
                    "rr_adj": rr_adj,
                    "cfg": None,
                })

                # Range SL/TP diagnostics reflect the config-adjusted RANGE_ATR_* inputs.
                if str(regime).lower() == "range" and debug_range_entry and (range_entry_diag_printed < diag_limit):
                    try:
                        range_entry_diag_printed += 1
                        stop_bps = (float(stop_raw_pre) / float(entry_raw) - 1.0) * 10000.0
                        tp_bps = (float(tp_raw_pre) / float(entry_raw) - 1.0) * 10000.0
                        logger.info(
                            "RANGE_ENTRY_DIAG: entry=%.2f stop=%.2f tp=%.2f stop_bps=%.2f tp_bps=%.2f "
                            "RANGE_ATR_TP_MULT=%.3f RANGE_ATR_SL_MULT=%.3f",
                            float(entry_raw),
                            float(stop_raw_pre),
                            float(tp_raw_pre),
                            float(stop_bps),
                            float(tp_bps),
                            float(getattr(C, "RANGE_ATR_TP_MULT", float("nan"))),
                            float(getattr(C, "RANGE_ATR_SL_MULT", float("nan"))),
                        )
                    except Exception:
                        logger.exception("RANGE_ENTRY_DIAG failed")

                entries_opened += 1

        unrealized_pnl = 0.0
        unrealized_worst_pnl = 0.0
        for sym, p in positions.items():
            i_mark = int(entry_ptr.get(sym, 0)) - 1
            if i_mark < 0:
                continue
            e_mark = entry_data.get(sym)
            if not isinstance(e_mark, dict):
                continue
            close_arr = e_mark.get("close")
            low_arr = e_mark.get("low")
            high_arr = e_mark.get("high")
            if (not isinstance(close_arr, list)) or (not isinstance(low_arr, list)) or (not isinstance(high_arr, list)):
                continue
            if i_mark >= len(close_arr) or i_mark >= len(low_arr) or i_mark >= len(high_arr):
                continue
            try:
                bar_close = float(close_arr[i_mark])
                bar_low = float(low_arr[i_mark])
                bar_high = float(high_arr[i_mark])
                entry_price = float(getattr(p, "entry_raw", 0.0) or 0.0)
                qty = float(getattr(p, "qty", 0.0) or 0.0)
            except Exception:
                continue
            if qty <= 0.0 or entry_price <= 0.0:
                continue
            side = str(getattr(p, "direction", "long") or "long").lower()
            if side == "short":
                unrealized_pnl += (entry_price - bar_close) * qty
                unrealized_worst_pnl += (entry_price - bar_high) * qty
            else:
                unrealized_pnl += (bar_close - entry_price) * qty
                unrealized_worst_pnl += (bar_low - entry_price) * qty
        mtm_equity = float(equity) + float(unrealized_pnl)
        worst_mtm_equity = float(equity) + float(unrealized_worst_pnl)
        mtm_by_ts[int(t)] = float(mtm_equity)
        if mtm_equity > peak_mtm:
            peak_mtm = float(mtm_equity)
        if peak_mtm > 0.0:
            dd_mtm = max(0.0, (float(peak_mtm) - float(mtm_equity)) / float(peak_mtm))
            if dd_mtm > max_dd_mtm:
                max_dd_mtm = float(dd_mtm)
        if worst_mtm_equity > peak_worst_bar:
            peak_worst_bar = float(worst_mtm_equity)
        if peak_worst_bar > 0.0:
            dd_worst = max(0.0, (float(peak_worst_bar) - float(worst_mtm_equity)) / float(peak_worst_bar))
            if dd_worst > max_dd_worst_bar:
                max_dd_worst_bar = float(dd_worst)

        # ------------------------
        # advance entry pointers & push next ts to heap
        # ------------------------
        for sym in syms_at_t:
            entry_ptr[sym] += 1
            i2 = entry_ptr[sym]
            ts_list = entry_data[sym]["ts"]
            if i2 < len(ts_list):
                heapq.heappush(heap, (int(ts_list[i2]), sym))
    # ------------------------------------------------------------
    # EOD force close (optional) - avoids "trades==0" when positions remain open
    # ------------------------------------------------------------
    if force_close_eod and positions:
        for sym, p in list(positions.items()):
            try:
                e = entry_data[sym]
                if not e["ts"]:
                    continue
                last_i = len(e["ts"]) - 1
                t_last = int(e["ts"][last_i])
                exit_raw = float(e["close"][last_i])
                exit_exec = float(exit_raw) * _side_slip_mult("exit_long")

                pnl = float(getattr(p, 'realized_pnl', 0.0)) + (exit_exec - float(p.entry_exec)) * float(p.qty)
                entry_qty = float(getattr(p, 'qty_init', p.qty) or p.qty)
                entry_notional = float(p.entry_exec) * float(entry_qty)
                exit_notional = exit_exec * float(p.qty)
                mae_abs = max(0.0, float(getattr(p, "mae_abs", 0.0) or 0.0))
                mae_entry_notional = (
                    max(0.0, float(getattr(p, "entry_raw", 0.0) or 0.0))
                    * max(0.0, float(getattr(p, "qty_init", entry_qty) or entry_qty))
                )
                mae_bps = (float(mae_abs) / float(mae_entry_notional) * 10000.0) if float(mae_entry_notional) > 0.0 else 0.0
                mfe_abs = max(0.0, float(getattr(p, "mfe_abs", 0.0) or 0.0))
                mfe_bps = (float(mfe_abs) / float(mae_entry_notional) * 10000.0) if float(mae_entry_notional) > 0.0 else 0.0
                pos_dir = str(getattr(p, "direction", "long") or "long").lower()
                entry_price_for_giveback = max(0.0, float(getattr(p, "entry_raw", 0.0) or 0.0))
                qty_for_giveback = max(0.0, float(getattr(p, "qty_init", entry_qty) or entry_qty))
                if pos_dir == "short":
                    best_price_for_giveback = float(getattr(p, "min_adv", entry_price_for_giveback) or entry_price_for_giveback)
                else:
                    best_price_for_giveback = float(getattr(p, "max_fav", entry_price_for_giveback) or entry_price_for_giveback)
                if (not math.isfinite(best_price_for_giveback)) or best_price_for_giveback <= 0.0:
                    best_price_for_giveback = float(entry_price_for_giveback)
                giveback_max_abs = max(0.0, float(getattr(p, "giveback_max_abs", 0.0) or 0.0))
                if pos_dir == "short":
                    if best_price_for_giveback < float(entry_price_for_giveback):
                        giveback_to_close_abs = max(
                            0.0,
                            (float(exit_raw) - float(best_price_for_giveback)) * float(qty_for_giveback),
                        )
                    else:
                        giveback_to_close_abs = 0.0
                else:
                    if best_price_for_giveback > float(entry_price_for_giveback):
                        giveback_to_close_abs = max(
                            0.0,
                            (float(best_price_for_giveback) - float(exit_raw)) * float(qty_for_giveback),
                        )
                    else:
                        giveback_to_close_abs = 0.0
                giveback_max_bps = (float(giveback_max_abs) / float(mae_entry_notional) * 10000.0) if float(mae_entry_notional) > 0.0 else 0.0
                giveback_to_close_bps = (float(giveback_to_close_abs) / float(mae_entry_notional) * 10000.0) if float(mae_entry_notional) > 0.0 else 0.0
                if float(mfe_abs) > 0.0:
                    giveback_max_pct_of_mfe = float(giveback_max_abs) / float(max(float(mfe_abs), 1e-12))
                    giveback_to_close_pct_of_mfe = float(giveback_to_close_abs) / float(max(float(mfe_abs), 1e-12))
                else:
                    giveback_max_pct_of_mfe = 0.0
                    giveback_to_close_pct_of_mfe = 0.0

                # Fee calc (local scope; do not rely on variables defined inside the main loop)
                maker = float(fee_maker_rate)
                taker = float(fee_taker_rate)
                entry_type = str(getattr(C, "PAPER_ENTRY_FEE_TYPE", "taker")).lower()
                entry_fee_rate = maker if entry_type == "maker" else taker
                # force_close_eod is treated as a TIMEOUT-style exit, so taker fees apply.
                exit_fee_rate = float(taker)

                if fast_bt:
                    fee = (entry_notional + exit_notional) * float(fast_fee_rate)
                else:
                    fee = (entry_notional + exit_notional + float(getattr(p, 'tp1_exit_notional', 0.0))) * float(fast_fee_rate)

                net = pnl - fee
                equity += net
                sizing_peak_equity = max(float(sizing_peak_equity), float(equity))
                if equity > peak_mtm:
                    peak_mtm = float(equity)
                if peak_mtm > 0.0:
                    dd_mtm = max(0.0, (float(peak_mtm) - float(equity)) / float(peak_mtm))
                    if dd_mtm > max_dd_mtm:
                        max_dd_mtm = float(dd_mtm)

                trades.append(
                    {
                        "ts": int(t_last),
                        "ts_iso": iso_utc(int(t_last)),
                        "symbol": sym,
                        "reason": "force_close_eod",
                        "regime": str(p.regime),
                        "direction": str(p.direction),
                        "entry_raw": float(p.entry_raw),
                        "stop_raw": float(p.stop_raw),
                        "tp_raw": float(p.tp_raw),
                        "exit_raw": float(exit_raw),
                        "entry_exec": float(p.entry_exec),
                        "exit_exec": float(exit_exec),
                        "qty": float(p.qty),
                        "mae_abs": float(mae_abs),
                        "mae_bps": float(mae_bps),
                        "mfe_abs": float(mfe_abs),
                        "mfe_bps": float(mfe_bps),
                        "giveback_max_abs": float(giveback_max_abs),
                        "giveback_max_bps": float(giveback_max_bps),
                        "giveback_max_pct_of_mfe": float(giveback_max_pct_of_mfe),
                        "giveback_to_close_abs": float(giveback_to_close_abs),
                        "giveback_to_close_bps": float(giveback_to_close_bps),
                        "giveback_to_close_pct_of_mfe": float(giveback_to_close_pct_of_mfe),
                        "pnl": float(pnl),
                        "fee": float(fee),
                        "net": float(net),
                        "rr0": float(p.rr0) if p.rr0 is not None else None,
                        "rr_adj": float(p.rr_adj) if p.rr_adj is not None else None,
                        "max_fav": float(p.max_fav),
                        "min_adv": float(getattr(p, "min_adv", float("nan"))),
                        "mfe_r": (
                            (float(getattr(p, "max_fav", float("nan"))) - float(p.entry_raw))
                            / (float(p.entry_raw) - float(p.init_stop))
                            if (float(p.entry_raw) - float(p.init_stop)) > 0
                            else None
                        ),
                        "mae_r": (
                            (float(p.entry_raw) - float(getattr(p, "min_adv", float("nan"))))
                            / (float(p.entry_raw) - float(p.init_stop))
                            if (float(p.entry_raw) - float(p.init_stop)) > 0
                            else None
                        ),
                        "tp1_done": bool(getattr(p, "tp1_done", False)),
                        "tp1_exit_notional": float(getattr(p, "tp1_exit_notional", 0.0)),
                        "stop_kind": str(getattr(p, "stop_kind", "")),
                        "be_triggered": bool(getattr(p, "be_triggered", False)),
                        "be_trigger_r": (float(getattr(p, "be_trigger_r", 0.0)) if getattr(p, "be_trigger_r", None) is not None else None),
                        "be_offset_bps": (float(getattr(p, "be_offset_bps", 0.0)) if getattr(p, "be_offset_bps", None) is not None else None),
                        "be_stop_set": (float(getattr(p, "be_stop_set", 0.0)) if getattr(p, "be_stop_set", None) is not None else None),
                        "trail_triggered": bool(getattr(p, "trail_triggered", False)),
                        "trail_start_r": (float(getattr(p, "trail_start_r", 0.0)) if getattr(p, "trail_start_r", None) is not None else None),
                        "trail_bps_from_high": (float(getattr(p, "trail_bps_from_high", 0.0)) if getattr(p, "trail_bps_from_high", None) is not None else None),
                        "start_price": (float(getattr(p, "start_price", 0.0)) if getattr(p, "start_price", None) is not None else None),
                        "opened_ts": int(p.opened_ts),
                    }
                )
                trades[-1].update(
                    R._build_stop_diag_fields(
                        stop_kind=str(getattr(p, "stop_kind", "")),
                        init_stop=float(getattr(p, "init_stop", 0.0) or 0.0),
                        final_stop=float(getattr(p, "stop", 0.0) or 0.0),
                        entry_exec=float(getattr(p, "entry_exec", 0.0) or 0.0),
                        trail_eval_count=getattr(p, "trail_eval_count", 0),
                        trail_candidate_stop_last=getattr(p, "trail_candidate_stop_last", None),
                        trail_candidate_stop_max=getattr(p, "trail_candidate_stop_max", None),
                        trail_candidate_minus_current_stop=getattr(p, "trail_candidate_minus_current_stop", None),
                        trail_candidate_minus_current_stop_max=getattr(p, "trail_candidate_minus_current_stop_max", None),
                        trail_candidate_from_atr_last=getattr(p, "trail_candidate_from_atr_last", None),
                        trail_candidate_from_bps_last=getattr(p, "trail_candidate_from_bps_last", None),
                        trail_eligible_count=getattr(p, "trail_eligible_count", 0),
                        trail_update_count=getattr(p, "trail_update_count", 0),
                        trail_block_reason_last=getattr(p, "trail_block_reason_last", ""),
                        trail_block_reason_max=getattr(p, "trail_block_reason_max", ""),
                        start_price=getattr(p, "start_price", None),
                        trail_start_price_last=getattr(p, "trail_start_price_last", None),
                        trail_start_price_max_context=getattr(p, "trail_start_price_max_context", None),
                        trail_bar_high_last=getattr(p, "trail_bar_high_last", None),
                        trail_bar_high_max=getattr(p, "trail_bar_high_max", None),
                        trail_pos_stop_before_last=getattr(p, "trail_pos_stop_before_last", None),
                        trail_pos_stop_before_max_context=getattr(p, "trail_pos_stop_before_max_context", None),
                        trail_risk_per_unit_last=getattr(p, "trail_risk_per_unit_last", None),
                        trail_mode_last=getattr(p, "trail_mode_last", None),
                        mfe_bps=float(mfe_bps),
                        giveback_to_close_bps=float(giveback_to_close_bps),
                    )
                )
                mae_abs_values.append(float(mae_abs))
                mae_bps_values.append(float(mae_bps))
                mfe_abs_values.append(float(mfe_abs))
                mfe_bps_values.append(float(mfe_bps))
                giveback_max_abs_values.append(float(giveback_max_abs))
                giveback_max_bps_values.append(float(giveback_max_bps))
                giveback_max_pct_values.append(float(giveback_max_pct_of_mfe))
                giveback_to_close_abs_values.append(float(giveback_to_close_abs))
                giveback_to_close_bps_values.append(float(giveback_to_close_bps))
                giveback_to_close_pct_values.append(float(giveback_to_close_pct_of_mfe))
                kept_bps = max(0.0, float(mfe_bps) - float(giveback_to_close_bps))
                kept_bps_values.append(float(kept_bps))
                kept_pct_of_mfe_values.append((float(kept_bps) / max(float(mfe_bps), 1e-12)) if float(mfe_bps) > 0.0 else 0.0)
                fav_adv_ratio_values.append(float(mfe_bps) / max(float(mae_bps), 1e-12))
                forced_closes += 1
                del positions[sym]
            except Exception:
                # If anything goes wrong, leave the position as-is
                continue

    final_equity = float(equity)
    net_total = final_equity - float(initial_equity)
    win = sum(1 for tr in trades if float(tr.get("net", 0.0)) > 0)
    lose = sum(1 for tr in trades if float(tr.get("net", 0.0)) <= 0)

    def _percentile(values: List[float], q: float) -> float:
        vals = [float(v) for v in values if math.isfinite(float(v))]
        if not vals:
            return 0.0
        vals.sort()
        q_clamped = min(1.0, max(0.0, float(q)))
        pos = (len(vals) - 1) * q_clamped
        lo = int(math.floor(pos))
        hi = int(math.ceil(pos))
        if lo == hi:
            return float(vals[lo])
        w = float(pos) - float(lo)
        return float(vals[lo] + (vals[hi] - vals[lo]) * w)

    mae_max_abs = max([float(v) for v in mae_abs_values], default=0.0)
    mae_max_bps = max([float(v) for v in mae_bps_values], default=0.0)
    mae_p50_abs = _percentile(mae_abs_values, 0.50)
    mae_p90_abs = _percentile(mae_abs_values, 0.90)
    mae_p99_abs = _percentile(mae_abs_values, 0.99)
    mae_p50_bps = _percentile(mae_bps_values, 0.50)
    mae_p90_bps = _percentile(mae_bps_values, 0.90)
    mae_p99_bps = _percentile(mae_bps_values, 0.99)
    mfe_max_abs = max([float(v) for v in mfe_abs_values], default=0.0)
    mfe_max_bps = max([float(v) for v in mfe_bps_values], default=0.0)
    mfe_p50_abs = _percentile(mfe_abs_values, 0.50)
    mfe_p90_abs = _percentile(mfe_abs_values, 0.90)
    mfe_p99_abs = _percentile(mfe_abs_values, 0.99)
    mfe_p50_bps = _percentile(mfe_bps_values, 0.50)
    mfe_p90_bps = _percentile(mfe_bps_values, 0.90)
    mfe_p99_bps = _percentile(mfe_bps_values, 0.99)
    giveback_max_abs = max([float(v) for v in giveback_max_abs_values], default=0.0)
    giveback_max_bps = max([float(v) for v in giveback_max_bps_values], default=0.0)
    giveback_max_pct_of_mfe = max([float(v) for v in giveback_max_pct_values], default=0.0)
    giveback_max_p50_abs = _percentile(giveback_max_abs_values, 0.50)
    giveback_max_p90_abs = _percentile(giveback_max_abs_values, 0.90)
    giveback_max_p99_abs = _percentile(giveback_max_abs_values, 0.99)
    giveback_max_p50_bps = _percentile(giveback_max_bps_values, 0.50)
    giveback_max_p90_bps = _percentile(giveback_max_bps_values, 0.90)
    giveback_max_p99_bps = _percentile(giveback_max_bps_values, 0.99)
    giveback_max_p50_pct_of_mfe = _percentile(giveback_max_pct_values, 0.50)
    giveback_max_p90_pct_of_mfe = _percentile(giveback_max_pct_values, 0.90)
    giveback_max_p99_pct_of_mfe = _percentile(giveback_max_pct_values, 0.99)
    giveback_to_close_abs = max([float(v) for v in giveback_to_close_abs_values], default=0.0)
    giveback_to_close_bps = max([float(v) for v in giveback_to_close_bps_values], default=0.0)
    giveback_to_close_pct_of_mfe = max([float(v) for v in giveback_to_close_pct_values], default=0.0)
    giveback_to_close_p50_abs = _percentile(giveback_to_close_abs_values, 0.50)
    giveback_to_close_p90_abs = _percentile(giveback_to_close_abs_values, 0.90)
    giveback_to_close_p99_abs = _percentile(giveback_to_close_abs_values, 0.99)
    giveback_to_close_p50_bps = _percentile(giveback_to_close_bps_values, 0.50)
    giveback_to_close_p90_bps = _percentile(giveback_to_close_bps_values, 0.90)
    giveback_to_close_p99_bps = _percentile(giveback_to_close_bps_values, 0.99)
    giveback_to_close_p50_pct_of_mfe = _percentile(giveback_to_close_pct_values, 0.50)
    giveback_to_close_p90_pct_of_mfe = _percentile(giveback_to_close_pct_values, 0.90)
    giveback_to_close_p99_pct_of_mfe = _percentile(giveback_to_close_pct_values, 0.99)
    kept_p50_bps = _percentile(kept_bps_values, 0.50)
    kept_p90_bps = _percentile(kept_bps_values, 0.90)
    kept_p99_bps = _percentile(kept_bps_values, 0.99)
    kept_pct_of_mfe_p50 = _percentile(kept_pct_of_mfe_values, 0.50)
    kept_pct_of_mfe_p90 = _percentile(kept_pct_of_mfe_values, 0.90)
    kept_pct_of_mfe_p99 = _percentile(kept_pct_of_mfe_values, 0.99)
    fav_adv_ratio_p50 = _percentile(fav_adv_ratio_values, 0.50)
    fav_adv_ratio_p90 = _percentile(fav_adv_ratio_values, 0.90)
    fav_adv_ratio_p99 = _percentile(fav_adv_ratio_values, 0.99)
    exit_hint = "balanced_exit_profile"
    if float(giveback_to_close_p50_pct_of_mfe) > 0.5:
        exit_hint = "giveback_heavy_consider_protective_trail"
    elif float(mfe_p50_bps) > 0.0 and float(kept_pct_of_mfe_p50) < 0.35:
        exit_hint = "mfe_available_but_kept_low_consider_earlier_take"
    elif float(fav_adv_ratio_p50) <= 1.0 and float(mae_max_bps) > 0.0:
        exit_hint = "fav_adv_ratio_low_review_entry_or_stop"
    elif float(mfe_p50_bps) <= 0.0:
        exit_hint = "mfe_small_no_early_take_needed"
    max_dd_worst_bar = max(float(max_dd_worst_bar), float(max_dd_mtm))
    if int(len(trades)) > 0:
        reasons: List[str] = []
        if float(max_dd_worst_bar) <= 0.0 and float(mae_max_abs) > 0.0:
            reasons.append("worst_bar_dd_zero_with_positive_mae")
        if float(max_dd_worst_bar) < float(max_dd):
            reasons.append("worst_bar_dd_below_max_dd")
        if float(initial_equity) > 0.0:
            worst_dd_abs_est = float(initial_equity) * float(max_dd_worst_bar)
            if float(worst_dd_abs_est) > 0.0 and float(mae_max_abs) > (float(worst_dd_abs_est) * 5.0):
                reasons.append("mae_abs_much_larger_than_worst_dd_abs_est")
        if reasons:
            logger.warning(
                "[REPORT_SANITY] suspicious worst_bar_dd: reason=%s max_dd=%.6f max_dd_worst_bar=%.6f mae_max_abs=%.6f initial_equity=%.6f",
                ",".join(reasons),
                float(max_dd),
                float(max_dd_worst_bar),
                float(mae_max_abs),
                float(initial_equity),
            )

    result = {
        "since_ms_effective": int(since_ms_effective) if since_ms_effective is not None else None,
        "since_iso_effective": iso_utc(int(since_ms_effective)) if since_ms_effective is not None else None,

        "trades": len(trades),
        "win": win,
        "lose": lose,
        "final_equity": final_equity,
        "net_total": float(net_total),
        "net_avg": float(net_total) / max(1, len(trades)),
        "max_dd": float(max_dd),
        "max_dd_mtm": float(max_dd_mtm),
        "max_dd_worst_bar": float(max_dd_worst_bar),
        "mae_max_abs": float(mae_max_abs),
        "mae_max_bps": float(mae_max_bps),
        "mae_p50_abs": float(mae_p50_abs),
        "mae_p90_abs": float(mae_p90_abs),
        "mae_p99_abs": float(mae_p99_abs),
        "mae_p50_bps": float(mae_p50_bps),
        "mae_p90_bps": float(mae_p90_bps),
        "mae_p99_bps": float(mae_p99_bps),
        "mfe_max_abs": float(mfe_max_abs),
        "mfe_max_bps": float(mfe_max_bps),
        "mfe_p50_abs": float(mfe_p50_abs),
        "mfe_p90_abs": float(mfe_p90_abs),
        "mfe_p99_abs": float(mfe_p99_abs),
        "mfe_p50_bps": float(mfe_p50_bps),
        "mfe_p90_bps": float(mfe_p90_bps),
        "mfe_p99_bps": float(mfe_p99_bps),
        "giveback_max_abs": float(giveback_max_abs),
        "giveback_max_bps": float(giveback_max_bps),
        "giveback_max_pct_of_mfe": float(giveback_max_pct_of_mfe),
        "giveback_max_p50_abs": float(giveback_max_p50_abs),
        "giveback_max_p90_abs": float(giveback_max_p90_abs),
        "giveback_max_p99_abs": float(giveback_max_p99_abs),
        "giveback_max_p50_bps": float(giveback_max_p50_bps),
        "giveback_max_p90_bps": float(giveback_max_p90_bps),
        "giveback_max_p99_bps": float(giveback_max_p99_bps),
        "giveback_max_p50_pct_of_mfe": float(giveback_max_p50_pct_of_mfe),
        "giveback_max_p90_pct_of_mfe": float(giveback_max_p90_pct_of_mfe),
        "giveback_max_p99_pct_of_mfe": float(giveback_max_p99_pct_of_mfe),
        "giveback_to_close_abs": float(giveback_to_close_abs),
        "giveback_to_close_bps": float(giveback_to_close_bps),
        "giveback_to_close_pct_of_mfe": float(giveback_to_close_pct_of_mfe),
        "giveback_to_close_p50_abs": float(giveback_to_close_p50_abs),
        "giveback_to_close_p90_abs": float(giveback_to_close_p90_abs),
        "giveback_to_close_p99_abs": float(giveback_to_close_p99_abs),
        "giveback_to_close_p50_bps": float(giveback_to_close_p50_bps),
        "giveback_to_close_p90_bps": float(giveback_to_close_p90_bps),
        "giveback_to_close_p99_bps": float(giveback_to_close_p99_bps),
        "giveback_to_close_p50_pct_of_mfe": float(giveback_to_close_p50_pct_of_mfe),
        "giveback_to_close_p90_pct_of_mfe": float(giveback_to_close_p90_pct_of_mfe),
        "giveback_to_close_p99_pct_of_mfe": float(giveback_to_close_p99_pct_of_mfe),
        "kept_p50_bps": float(kept_p50_bps),
        "kept_p90_bps": float(kept_p90_bps),
        "kept_p99_bps": float(kept_p99_bps),
        "kept_pct_of_mfe_p50": float(kept_pct_of_mfe_p50),
        "kept_pct_of_mfe_p90": float(kept_pct_of_mfe_p90),
        "kept_pct_of_mfe_p99": float(kept_pct_of_mfe_p99),
        "fav_adv_ratio_p50": float(fav_adv_ratio_p50),
        "fav_adv_ratio_p90": float(fav_adv_ratio_p90),
        "fav_adv_ratio_p99": float(fav_adv_ratio_p99),
        "exit_hint": str(exit_hint),
        "symbols": symbols,
        "since_ms": int(since_ms) if since_ms is not None else None,
        "entry_tf": str(entry_tf),
        "filter_tf": str(filter_tf),
        "warmup_bars": int(warmup_bars),
        "spread_bps_est": float(spread_bps_est),
        "recent_bars_entry": int(recent_bars_entry) if recent_bars_entry else None,
        "recent_bars_filter": int(recent_bars_filter) if recent_bars_filter else None,
        # debug: trade==0 diagnosis
        "buy_signals": int(buy_signals),
        "entries_opened": int(entries_opened),
        "forced_closes": int(forced_closes),
        "open_positions_end": int(len(positions)),
        "pullback_funnel": dict(pullback_funnel),
        "hold_reasons_top12": hold_counter.most_common(12),
        "run_id": str(_CURRENT_RUN_ID or resolve_run_id("", env_key="LWF_RUN_ID")),
        "export_dir": str(_current_export_dir()),
    }

    if export_csv:
        os.makedirs(_current_export_dir(), exist_ok=True)

        export_equity_curve(_export_path("equity_curve.csv"), curve, mtm_by_ts=mtm_by_ts)
        export_trades(_export_path("trades.csv"), trades)
        export_synthetic_equity_curve(
            _export_path("synthetic_equity_curve.csv"),
            trades,
            float(initial_equity),
        )

        logger.info("CSV exported: %s / %s", _export_path("equity_curve.csv"), _export_path("trades.csv"))
        if stop_loss_patterns:
            export_trades(_export_path("stop_loss_patterns.csv"), stop_loss_patterns)
            logger.info("CSV exported: %s (STOP_HIT_LOSS diagnostics)", _export_path("stop_loss_patterns.csv"))

    # ------------------------------------------------------------
    # Shared live/backtest diff-trace export.
    # Keep the jsonl layout aligned with runner.py.
    # ------------------------------------------------------------
    # Write one REPLAY_META record per file, then append OPEN/CLOSE events.
    # ------------------------------------------------------------
    try:
        diff_enabled = bool(getattr(C, "DIFF_TRACE_ENABLED", False))
    except Exception:
        diff_enabled = False

    if bool(export_diff_trace) and diff_enabled:
        try:
            out_dir_cfg = str(getattr(C, "DIFF_TRACE_DIR", "") or "").strip()
            out_dir = str(_current_export_dir()) if is_legacy_exports_path(out_dir_cfg) else out_dir_cfg
            prefix = str(diff_trace_prefix or getattr(C, "DIFF_TRACE_PREFIX_BACKTEST", "diff_trace_backtest") or "diff_trace_backtest")
            # Strip mode/symbol suffixes so filenames stay aligned with runner output.
            try:
                _sym0 = (symbols[0] if symbols else "")
                _safe_sym = str(_sym0).replace("/", "")
                _suffix = f"_{str(diff_trace_mode)}_{_safe_sym}" if _safe_sym else f"_{str(diff_trace_mode)}"
                if prefix.endswith(_suffix):
                    prefix = prefix[: -len(_suffix)]
            except Exception:
                pass
            os.makedirs(out_dir, exist_ok=True)

            def _norm_ts_ms(ts: int) -> int:
                """Normalize seconds or microseconds timestamps to milliseconds."""
                try:
                    ts = int(ts)
                except Exception:
                    return 0
                if ts <= 0:
                    return 0
                # seconds (e.g. 1736162400) -> ms
                if ts < 10_000_000_000:  # < ~2001-09-09 in ms
                    return ts * 1000
                # microseconds (e.g. 1736162400000000) -> ms
                if ts > 10_000_000_000_000:  # > ~2286-11-20 in ms
                    return ts // 1000
                return ts

            def _path_for(ts_ms: int) -> str:
                try:
                    ts_ms = _norm_ts_ms(ts_ms)
                    dt = datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc)
                    day = dt.strftime("%Y-%m-%d")
                except Exception:
                    day = "unknown"
                return os.path.join(out_dir, f"{prefix}_{day}.jsonl")

            base_since_ms = _norm_ts_ms(int(effective_trade_since_ms or 0))
            base_path = _path_for(base_since_ms)

            # Write REPLAY_META once per file so the jsonl layout matches live traces.
            meta_template = {
                "event": "REPLAY_META",
                "mode": str(diff_trace_mode),
                "since_ms": int(base_since_ms or 0),
                "since": iso_utc(int(base_since_ms or 0))[:10] if (base_since_ms or 0) else None,
                "source": str(diff_trace_source),
                "symbol": symbols[0] if symbols else None,
                "tf_entry": str(entry_tf),
                "tf_filter": str(filter_tf),
                "ts_ms": int(base_since_ms or 0),
                "build_id": str(BUILD_ID),
                # Keep the cfg snapshot aligned with runner.py for trace comparison.
                "cfg": (
                    R._cfg_snapshot(
                        keys=[
                            "TRADE_TREND",
                            "TRADE_RANGE",
                            "TRADE_ONLY_TREND",
                            "TREND_ENTRY_MODE",
                            "MIN_TP_BPS",
                            "MIN_RR_AFTER_ADJUST_TREND_LONG",
                        ]
                    )
                    if hasattr(R, "_cfg_snapshot")
                    else {}
                ),
            }
            written_meta_paths: set[str] = set()

            def _ensure_meta(path: str, ts_ms_for_file: int) -> None:
                if path in written_meta_paths:
                    return
                meta = dict(meta_template)
                meta["type"] = "REPLAY_META"
                meta["ts_ms"] = int(ts_ms_for_file or meta_template.get("ts_ms") or 0)

                # Step1: event_ts = exec_ts (runner-compatible).
                base_ts = int(ts_ms_for_file) if ts_ms_for_file else 0
                meta.setdefault("event_ts", base_ts)
                meta.setdefault("exec_ts", base_ts)
                if meta.get("event_ts") != meta.get("exec_ts"):
                    meta["event_ts"] = meta["exec_ts"] = base_ts
                meta["cfg"] = _diff_trace_norm_cfg(meta.get("cfg") or {})
                existing_lines: list[str] = []
                if os.path.exists(path):
                    try:
                        with open(path, "r", encoding="utf-8") as f:
                            existing_lines = [ln.rstrip("\n") for ln in f if (ln or "").strip()]
                    except Exception:
                        existing_lines = []
                if existing_lines:
                    try:
                        first_obj = json.loads(existing_lines[0])
                    except Exception:
                        first_obj = None
                    if isinstance(first_obj, dict) and str(first_obj.get("event") or first_obj.get("type") or "") == "REPLAY_META":
                        written_meta_paths.add(path)
                        return
                with open(path, "w", encoding="utf-8") as f:
                    f.write(json.dumps(meta, ensure_ascii=False, sort_keys=True) + "\n")
                    for ln in existing_lines:
                        f.write(str(ln) + "\n")
                written_meta_paths.add(path)

            # Ensure the daily jsonl exists even when there are 0 trades in the sliced range.
            _ensure_meta(base_path, int(base_since_ms or 0))

            def _write(ev: dict) -> None:
                ts_ms = _norm_ts_ms(int(ev.get("ts_ms") or 0))
                # Step1: event_ts = exec_ts (runner-compatible).
                base_ts = ts_ms
                ev.setdefault("event_ts", base_ts)
                ev.setdefault("exec_ts", base_ts)
                if ev.get("event_ts") != ev.get("exec_ts"):
                    ev["event_ts"] = ev["exec_ts"] = base_ts
                ev["ts_ms"] = int(ts_ms or 0)
                # Step1: event_ts = exec_ts (runner-compatible).
                base_ts = ts_ms
                ev.setdefault("event_ts", base_ts)
                ev.setdefault("exec_ts", base_ts)
                if ev.get("event_ts") != ev.get("exec_ts"):
                    ev["event_ts"] = ev["exec_ts"] = base_ts
                ev.setdefault("source", str(diff_trace_source))
                ev.setdefault("build_id", str(BUILD_ID))
                ev.setdefault("mode", str(diff_trace_mode))
                ev.setdefault("cfg", meta_template.get("cfg") or {})
                ev.setdefault("trade_range", bool(getattr(C, "TRADE_RANGE", False)))
                ev.setdefault("trade_trend", float(getattr(C, "TRADE_TREND", 0.0) or 0.0))
                if "ts_iso" not in ev:
                    try:
                        ev["ts_iso"] = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()
                    except Exception:
                        ev["ts_iso"] = ""
                path = _path_for(ts_ms if ts_ms else int(base_since_ms or 0))
                _ensure_meta(path, ts_ms if ts_ms else int(base_since_ms or 0))
                with open(path, "a", encoding="utf-8") as f:
                    f.write(json.dumps(ev, ensure_ascii=False, sort_keys=True) + "\n")

            # OPEN/CLOSE are already emitted inline in the trading loop.
            # Skip post-hoc duplicate export so diff_trace matches replay/live counts.
        except Exception as _e_diff:
            logger.warning(f"DIFF_TRACE export failed: {_e_diff}")

    if hold_counter:
        logger.info(f"HOLD REASONS TOP12: {hold_counter.most_common(12)}")
    # Emit BUY_REJECT only when buy signals actually reached entry evaluation.
    if int(buy_signals) > 0 and buy_reject:
        logger.info(f"BUY_REJECT TOP12 (buy_signals={buy_signals}): {buy_reject.most_common(12)}")
    if size_sizing_open_notional_pcts:
        try:
            size_sizing_open_notional_pct_arr = np.asarray(size_sizing_open_notional_pcts, dtype=float)
            logger.info(
                "[SIZE_SIZING_DIAG_SUMMARY] avg_open_notional_pct=%.6f median_open_notional_pct=%.6f p90_open_notional_pct=%.6f max_open_notional_pct=%.6f",
                float(np.mean(size_sizing_open_notional_pct_arr)),
                float(np.median(size_sizing_open_notional_pct_arr)),
                float(np.percentile(size_sizing_open_notional_pct_arr, 90)),
                float(np.max(size_sizing_open_notional_pct_arr)),
            )
        except Exception:
            pass

    # ------------------------------------------------------------
    # Exit reason + realized-bps stats (by regime)
    # ------------------------------------------------------------
    if trades:
        try:
            exit_counter = _Counter()
            exit_counter_range = _Counter()
            exit_counter_trend = _Counter()
            for tr in trades:
                r = str(tr.get("reason", "unknown"))
                exit_counter[r] += 1
                reg = str(tr.get("regime", "")).lower()
                if reg == "range":
                    exit_counter_range[r] += 1
                elif reg == "trend":
                    exit_counter_trend[r] += 1

            logger.info(f"EXIT REASONS TOP12: {exit_counter.most_common(12)}")
            logger.info(f"EXIT REASONS TOP12 (range): {exit_counter_range.most_common(12)}")
            logger.info(f"EXIT REASONS TOP12 (trend): {exit_counter_trend.most_common(12)}")

            # realized bps stats by (regime, reason) for expectancy gating (includes EARLY_LOSS / EMA9_CROSS_EXIT / TIMEOUT)
            bucket: Dict[Tuple[str, str], List[float]] = {}
            for tr in trades:
                reason = str(tr.get("reason", "unknown"))
                reason0 = _norm_exit_reason(reason)
                reg = str(tr.get("regime", "")).lower()
                if reg not in ("range", "trend"):
                    continue
                allow = EXPECTANCY_REASONS_RANGE if reg == "range" else EXPECTANCY_REASONS_TREND
                if reason0 not in allow:
                    continue
                bps = _realized_bps(
                    entry_px=float(tr.get("entry_exec", 0.0) or 0.0),
                    exit_px=float(tr.get("exit_exec", 0.0) or 0.0),
                    direction=str(tr.get("direction", "long")),
                )
                if not (bps == bps):
                    continue
                bucket.setdefault((reg, reason0), []).append(float(bps))

            for (reg, reason), arr in sorted(bucket.items(), key=lambda kv: (-len(kv[1]), kv[0][0], kv[0][1])):
                n = len(arr)
                avg = float(sum(arr)) / max(1, n)
                med = _pct(arr, 50)
                p10 = _pct(arr, 10)
                p90 = _pct(arr, 90)
                mn = min(arr) if arr else float("nan")
                mx = max(arr) if arr else float("nan")
                logger.info(
                    "EXIT_BPS (%s/%s): n=%d avg=%.2f med=%.2f p10=%.2f p90=%.2f min=%.2f max=%.2f",
                    reg,
                    reason,
                    n,
                    avg,
                    med,
                    p10,
                    p90,
                    mn,
                    mx,
                )

            # Derived expectancy (bps) from realized TP/STOP buckets (fee separated).
            def _avg(a: List[float]) -> float:
                if not a:
                    return float("nan")
                return float(sum(a)) / float(len(a))

            taker_fee_rate = float(fee_taker_rate)
            fee_bps_round = 2.0 * taker_fee_rate * 10000.0

            for reg in ("range", "trend"):
                tp = bucket.get((reg, "TP_HIT"), [])
                sl_loss = bucket.get((reg, "STOP_HIT_LOSS"), [])
                sl_profit = bucket.get((reg, "STOP_HIT_PROFIT"), [])

                extra: List[float] = []
                if reg == "range":
                    extra += bucket.get((reg, "RANGE_EARLY_LOSS_ATR"), [])
                    extra += bucket.get((reg, "RANGE_EMA9_CROSS_EXIT"), [])
                    extra += bucket.get((reg, "RANGE_TIMEOUT"), [])
                else:
                    extra += bucket.get((reg, "TREND_TIMEOUT"), [])

                n = len(tp) + len(sl_loss) + len(sl_profit) + len(extra)
                if n <= 0:
                    continue

                avg_tp = _avg(tp)
                avg_sl_loss = _avg(sl_loss)
                avg_sl_profit = _avg(sl_profit)

                exp_sum = float(sum(tp)) + float(sum(sl_loss)) + float(sum(sl_profit)) + float(sum(extra))
                exp_bps = exp_sum / float(n)

                # Winrate threshold to break even (TP vs STOP_LOSS only)
                req_wr = float("nan")
                req_wr_net = float("nan")
                if avg_tp == avg_tp and avg_sl_loss == avg_sl_loss and avg_tp > 0 and avg_sl_loss < 0:
                    req_wr = abs(float(avg_sl_loss)) / (abs(float(avg_sl_loss)) + float(avg_tp))
                    tp_net = float(avg_tp) - fee_bps_round
                    sl_net = float(avg_sl_loss) - fee_bps_round
                    if tp_net > 0 and sl_net < 0:
                        req_wr_net = abs(float(sl_net)) / (abs(float(sl_net)) + float(tp_net))

                exp_net = float(exp_bps) - float(fee_bps_round)
                wr = float(sum(1 for x in (tp + sl_loss + sl_profit + extra) if float(x) > 0.0)) / float(n)

                logger.info(
                    "EXPECTANCY_BPS (%s): n=%d exp=%.2f exp_net=%.2f fee_round=%.2f wr=%.3f avg_tp=%.2f avg_sl_loss=%.2f avg_sl_profit=%.2f req_wr=%.3f req_wr_net=%.3f",
                    reg,
                    n,
                    float(exp_bps),
                    float(exp_net),
                    float(fee_bps_round),
                    float(wr),
                    float(avg_tp) if avg_tp == avg_tp else float("nan"),
                    float(avg_sl_loss) if avg_sl_loss == avg_sl_loss else float("nan"),
                    float(avg_sl_profit) if avg_sl_profit == avg_sl_profit else float("nan"),
                    float(req_wr) if req_wr == req_wr else float("nan"),
                    float(req_wr_net) if req_wr_net == req_wr_net else float("nan"),
                )
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Legacy / duplicate exit summary block
    # NOTE:
    #   EXIT_BPS / EXPECTANCY_BPS are already emitted earlier, so keep this duplicate block disabled.
    # ------------------------------------------------------------------
    # Exit reasons summary (from trades.csv "reason" field)
    # NOTE: Legacy duplicate block disabled (kept earlier block which also prints EXIT_BPS/EXPECTANCY_BPS).
    if False and trades:
        try:
            exit_counter = _Counter()
            exit_counter_range = _Counter()
            exit_counter_trend = _Counter()
            for tr in trades:
                r0 = str(tr.get("reason", "unknown"))
                reg = str(tr.get("regime", "")).lower()
                # Split STOP_HIT into LOSS/PROFIT using realized bps (for expectancy diagnosis).
                r = r0
                if r0 == "STOP_HIT":
                    bps0 = _realized_bps(
                        entry_px=float(tr.get("entry_exec", 0.0) or 0.0),
                        exit_px=float(tr.get("exit_exec", 0.0) or 0.0),
                        direction=str(tr.get("direction", "long")),
                    )
                    if bps0 == bps0:
                        r = "STOP_HIT_LOSS" if float(bps0) < 0.0 else "STOP_HIT_PROFIT"
                exit_counter[r] += 1
                if reg == "range":
                    exit_counter_range[r] += 1
                elif reg == "trend":
                    exit_counter_trend[r] += 1
            logger.info(f"EXIT REASONS TOP12: {exit_counter.most_common(12)}")
            logger.info(f"EXIT REASONS TOP12 (range): {exit_counter_range.most_common(12)}")
            logger.info(f"EXIT REASONS TOP12 (trend): {exit_counter_trend.most_common(12)}")

            # (debug) Realized bps stats by exit reason/regime (expectancy diagnosis)
            # - uses entry_exec/exit_exec (post-spread/slippage in this backtest)
            # - does NOT include fees (fees are applied separately in expectancy summary)
            bucket: Dict[Tuple[str, str], List[float]] = {}
            for tr in trades:
                r0 = str(tr.get("reason", "unknown"))
                reg = str(tr.get("regime", "")).lower()
                if reg not in ("range", "trend"):
                    continue
                if r0 not in ("TP_HIT", "STOP_HIT"):
                    continue
                bps = _realized_bps(
                    entry_px=float(tr.get("entry_exec", 0.0) or 0.0),
                    exit_px=float(tr.get("exit_exec", 0.0) or 0.0),
                    direction=str(tr.get("direction", "long")),
                )
                if not (bps == bps):
                    continue
                r = r0
                if r0 == "STOP_HIT":
                    r = "STOP_HIT_LOSS" if float(bps) < 0.0 else "STOP_HIT_PROFIT"
                bucket.setdefault((reg, r), []).append(float(bps))

            for (reg, reason), arr in sorted(bucket.items(), key=lambda kv: (-len(kv[1]), kv[0][0], kv[0][1])):
                n = len(arr)
                avg = float(sum(arr)) / max(1, n)
                med = _pct(arr, 50)
                p10 = _pct(arr, 10)
                p90 = _pct(arr, 90)
                mn = min(arr) if arr else float("nan")
                mx = max(arr) if arr else float("nan")
                logger.info(
                    "EXIT_BPS (%s/%s): n=%d avg=%.2f med=%.2f p10=%.2f p90=%.2f min=%.2f max=%.2f",
                    reg,
                    reason,
                    n,
                    avg,
                    med,
                    p10,
                    p90,
                    mn,
                    mx,
                )

            # Derived expectancy (bps) from realized TP/STOP buckets.
            # Goal: make the range-side math explicit (TP vs SL vs fees), without changing trading logic.
            def _avg(a: List[float]) -> float:
                if not a:
                    return float("nan")
                return float(sum(a)) / float(len(a))

            taker_fee_rate = float(fee_taker_rate)
            fee_bps_round = 2.0 * taker_fee_rate * 10000.0

            for reg in ("range", "trend"):
                tp = bucket.get((reg, "TP_HIT"), [])
                sl_loss = bucket.get((reg, "STOP_HIT_LOSS"), [])
                sl_profit = bucket.get((reg, "STOP_HIT_PROFIT"), [])

                n = len(tp) + len(sl_loss) + len(sl_profit)
                if n <= 0:
                    continue

                avg_tp = _avg(tp)
                avg_sl_loss = _avg(sl_loss)
                avg_sl_profit = _avg(sl_profit)

                exp_bps = 0.0
                if len(tp) > 0:
                    exp_bps += len(tp) * float(avg_tp)
                if len(sl_loss) > 0:
                    exp_bps += len(sl_loss) * float(avg_sl_loss)
                if len(sl_profit) > 0:
                    exp_bps += len(sl_profit) * float(avg_sl_profit)
                exp_bps /= float(n)

                # Winrate threshold to break even (TP vs STOP_LOSS only). Uses absolute loss size.
                req_wr = float("nan")
                req_wr_net = float("nan")
                if avg_tp == avg_tp and avg_sl_loss == avg_sl_loss and avg_tp > 0 and avg_sl_loss < 0:
                    req_wr = abs(float(avg_sl_loss)) / (abs(float(avg_sl_loss)) + float(avg_tp))
                    tp_net = float(avg_tp) - fee_bps_round
                    sl_net = float(avg_sl_loss) - fee_bps_round
                    if tp_net > 0 and sl_net < 0:
                        req_wr_net = abs(float(sl_net)) / (abs(float(sl_net)) + float(tp_net))

                exp_net = float(exp_bps) - float(fee_bps_round)
                wr = float(sum(1 for x in (tp + sl_loss + sl_profit + extra) if float(x) > 0.0)) / float(n)

                logger.info(
                    "EXPECTANCY_BPS (%s): n=%d exp=%.2f exp_net=%.2f fee_round=%.2f wr=%.3f avg_tp=%.2f avg_sl_loss=%.2f avg_sl_profit=%.2f req_wr=%.3f req_wr_net=%.3f",
                    reg,
                    n,
                    float(exp_bps),
                    float(exp_net),
                    float(fee_bps_round),
                    float(wr),
                    float(avg_tp) if avg_tp == avg_tp else float("nan"),
                    float(avg_sl_loss) if avg_sl_loss == avg_sl_loss else float("nan"),
                    float(avg_sl_profit) if avg_sl_profit == avg_sl_profit else float("nan"),
                    float(req_wr) if req_wr == req_wr else float("nan"),
                    float(req_wr_net) if req_wr_net == req_wr_net else float("nan"),
                )
        except Exception:
            pass

    def _realized_bps(entry_px: float, exit_px: float, direction: str) -> float:
        try:
            e = float(entry_px)
            x = float(exit_px)
            if not (e > 0 and x > 0):
                return float("nan")
            if str(direction).lower() == "short":
                return (e - x) / e * 10000.0
            return (x - e) / e * 10000.0
        except Exception:
            return float("nan")

    def _pct(a: List[float], q: float) -> float:
        if not a:
            return float("nan")
        b = sorted(float(x) for x in a if x == x)  # drop NaN
        if not b:
            return float("nan")
        if q <= 0:
            return float(b[0])
        if q >= 100:
            return float(b[-1])
        pos = (len(b) - 1) * (q / 100.0)
        lo = int(math.floor(pos))
        hi = int(math.ceil(pos))
        if lo == hi:
            return float(b[lo])
        w = pos - lo
        return float(b[lo] * (1.0 - w) + b[hi] * w)

    # Exit reasons summary (from trades.csv "reason" field)
    # NOTE:
    #   Duplicate of the main EXIT_BPS / EXPECTANCY_BPS block above.
    #   Disabled to keep a single source of truth for EXIT analytics.
    if False and trades:
        try:
            exit_counter = _Counter()
            exit_counter_range = _Counter()
            exit_counter_trend = _Counter()
            for tr in trades:
                reason = str(tr.get("reason", "unknown"))
                reg = str(tr.get("regime", "")).lower()

                # B-1a: classify overly wide range stops as a dedicated exit reason
                if (
                    reg == "range"
                    and reason == "STOP_HIT_LOSS"
                    and getattr(C, "RANGE_MAX_STOP_BPS", None) is not None
                ):
                    bps0 = _realized_bps(
                        entry_px=float(tr.get("entry_exec", 0.0) or 0.0),
                        exit_px=float(tr.get("exit_exec", 0.0) or 0.0),
                        direction=str(tr.get("direction", "long")),
                    )
                    if bps0 == bps0 and abs(float(bps0)) > float(C.RANGE_MAX_STOP_BPS):
                        reason = "RANGE_STOP_TOO_WIDE"

                exit_counter[r] += 1
                if reg == "range":
                    exit_counter_range[r] += 1
                elif reg == "trend":
                    exit_counter_trend[r] += 1
            logger.info(f"EXIT REASONS TOP12: {exit_counter.most_common(12)}")
            logger.info(f"EXIT REASONS TOP12 (range): {exit_counter_range.most_common(12)}")
            logger.info(f"EXIT REASONS TOP12 (trend): {exit_counter_trend.most_common(12)}")

            # (debug) Realized bps stats by exit reason/regime (expectancy diagnosis)
            # - uses entry_exec/exit_exec (post-spread/slippage in this backtest)
            # - does NOT include fees (fees are in "fee" column)
            bucket: Dict[Tuple[str, str], List[float]] = {}
            for tr in trades:
                reason = str(tr.get("reason", "unknown"))
                reg = str(tr.get("regime", "")).lower()
                if reg not in ("range", "trend"):
                    continue
                if reason != "TP_HIT" and not str(reason).startswith("STOP_HIT"):
                    continue
                bps = _realized_bps(
                    entry_px=float(tr.get("entry_exec", 0.0) or 0.0),
                    exit_px=float(tr.get("exit_exec", 0.0) or 0.0),
                    direction=str(tr.get("direction", "long")),
                )
                if not (bps == bps):
                    continue
                bucket.setdefault((reg, reason), []).append(float(bps))

            for (reg, reason), arr in sorted(bucket.items(), key=lambda kv: (-len(kv[1]), kv[0][0], kv[0][1])):
                n = len(arr)
                avg = float(sum(arr)) / max(1, n)
                med = _pct(arr, 50)
                p10 = _pct(arr, 10)
                p90 = _pct(arr, 90)
                mn = min(arr) if arr else float("nan")
                mx = max(arr) if arr else float("nan")
                logger.info(
                    "EXIT_BPS (%s/%s): n=%d avg=%.2f med=%.2f p10=%.2f p90=%.2f min=%.2f max=%.2f",
                    reg,
                    reason,
                    n,
                    avg,
                    med,
                    p10,
                    p90,
                    mn,
                    mx,
                )
            # Derived expectancy (bps) from realized TP/STOP buckets.
            # Goal: make the range-side math explicit (TP vs SL vs fees), without changing trading logic.
            def _avg(a: List[float]) -> float:
                if not a:
                    return float("nan")
                return float(sum(a)) / float(len(a))

            taker_fee_rate = float(fee_taker_rate)
            fee_bps_round = 2.0 * taker_fee_rate * 10000.0

            for reg in ("range", "trend"):
                tp = bucket.get((reg, "TP_HIT"), [])
                sl_loss = bucket.get((reg, "STOP_HIT_LOSS"), [])
                sl_profit = bucket.get((reg, "STOP_HIT_PROFIT"), [])

                n = len(tp) + len(sl_loss) + len(sl_profit)
                if n <= 0:
                    continue

                avg_tp = _avg(tp)
                avg_sl_loss = _avg(sl_loss)
                avg_sl_profit = _avg(sl_profit)

                exp_bps = 0.0
                if len(tp) > 0:
                    exp_bps += len(tp) * float(avg_tp)
                if len(sl_loss) > 0:
                    exp_bps += len(sl_loss) * float(avg_sl_loss)
                if len(sl_profit) > 0:
                    exp_bps += len(sl_profit) * float(avg_sl_profit)
                exp_bps /= float(n)

                # Winrate threshold to break even (TP vs STOP_LOSS only). Uses absolute loss size.
                req_wr = float("nan")
                req_wr_net = float("nan")
                if avg_tp == avg_tp and avg_sl_loss == avg_sl_loss and avg_tp > 0 and avg_sl_loss < 0:
                    req_wr = abs(float(avg_sl_loss)) / (abs(float(avg_sl_loss)) + float(avg_tp))
                    tp_net = float(avg_tp) - fee_bps_round
                    sl_net = float(avg_sl_loss) - fee_bps_round
                    if tp_net > 0 and sl_net < 0:
                        req_wr_net = abs(float(sl_net)) / (abs(float(sl_net)) + float(tp_net))

                logger.info(
                    "EXPECTANCY_BPS (%s): n=%d exp=%.2f fee_round=%.2f avg_tp=%.2f avg_sl_loss=%.2f avg_sl_profit=%.2f req_wr=%.3f req_wr_net=%.3f",
                    reg,
                    n,
                    float(exp_bps),
                    float(fee_bps_round),
                    float(avg_tp) if avg_tp == avg_tp else float("nan"),
                    float(avg_sl_loss) if avg_sl_loss == avg_sl_loss else float("nan"),
                    float(avg_sl_profit) if avg_sl_profit == avg_sl_profit else float("nan"),
                    float(req_wr) if req_wr == req_wr else float("nan"),
                    float(req_wr_net) if req_wr_net == req_wr_net else float("nan"),
                )
        except Exception:
            pass

    # STOP_HIT_LOSS summary (range -> trend burn diagnostics)
    try:
        sl_n = len(stop_loss_patterns)
        if sl_n > 0:
            sl_bars = 0.0
            sl_pre_atr_bps = 0.0
            sl_pre_rsi = 0.0
            sl_pre_ema9_dist_bps = 0.0
            for p in stop_loss_patterns:
                sl_bars += float(p.get("bars_to_exit", 0.0) or 0.0)
                sl_pre_atr_bps += float(p.get("pre_atr_bps", 0.0) or 0.0)
                sl_pre_rsi += float(p.get("pre_rsi14", 0.0) or 0.0)
                sl_pre_ema9_dist_bps += float(p.get("pre_ema9_dist_bps", 0.0) or 0.0)
            logger.info(
                "STOP_HIT_LOSS: n=%d avg_bars_to_exit=%.2f avg_pre_atr_bps=%.2f avg_pre_rsi=%.2f avg_pre_ema9_dist_bps=%.2f",
                sl_n,
                sl_bars / sl_n,
                sl_pre_atr_bps / sl_n,
                sl_pre_rsi / sl_n,
                sl_pre_ema9_dist_bps / sl_n,
            )
        by_reg = _Counter()
        by_hour = _Counter()
        by_burn = _Counter()
        for p in stop_loss_patterns:
            try:
                reg = p.get("pre_filter_regime")
                direc = p.get("pre_filter_dir")
                if reg or direc:
                    by_reg[f"{reg}/{direc}"] += 1
                h = p.get("pre_jst_hour")
                if h is not None and h != "":
                    by_hour[int(h)] += 1
                burn_flags = []
                if p.get("pre_ema9_over_ema21") is False:
                    burn_flags.append("e9<e21")
                if (p.get("pre_ema9_dist_bps") is not None) and (p.get("pre_ema9_dist_bps") < 0):
                    burn_flags.append("c<e9")
                if (p.get("pre_ema21_dist_bps") is not None) and (p.get("pre_ema21_dist_bps") < 0):
                    burn_flags.append("c<e21")
                if (p.get("pre_rsi14") is not None) and (p.get("pre_rsi14") < 45):
                    burn_flags.append("rsi<45")
                if burn_flags:
                    by_burn["+".join(burn_flags)] += 1
            except Exception:
                continue
        logger.info(f"STOP_HIT_LOSS: TOP pre_regime/dir={by_reg.most_common(10)}")
        logger.info(f"STOP_HIT_LOSS: TOP pre_JST_hour={sorted(by_hour.items(), key=lambda x: (-x[1], x[0]))[:10]}")
        logger.info(f"STOP_HIT_LOSS: TOP burn_signature={by_burn.most_common(10)}")
    except Exception:
        pass

    if pullback_funnel:
        logger.info(f"PULLBACK FUNNEL: {dict(pullback_funnel)}")

    return result

def main() -> int:
    print("DEBUG TREND_TP_ATR_K =", C.TREND_TP_ATR_K)
    parser = build_arg_parser()
    args_dbg, _ = parser.parse_known_args()
    kwargs = {}
    kwargs["DEBUG_RANGE_ENTRY"] = 1.0 if args_dbg.debug_range_entry else 0.0
    kwargs["RANGE_ENTRY_DIAG_LIMIT"] = float(args_dbg.range_entry_diag_limit)
    parser = argparse.ArgumentParser(
        description="Backtest (runner-compatible, MEXC spot scalping).",
        parents=[build_arg_parser()],
    )
    parser.add_argument(
        "--year",
        type=int,
        default=0,
        choices=[2021, 2022, 2023, 2024, 2025],
        help="Apply year preset (dataset dirs/globs + SINCE_MS) without editing config.py.",
    )
    parser.add_argument(
        "--preset",
        type=str,
        default="",
        help="Opt-in preset name (e.g. SELL_SAFE). CLI takes precedence over BOT_PRESET env.",
    )
    parser.add_argument("--run-id", type=str, default="", help="Optional run identifier. If empty, uses LWF_RUN_ID or auto-generated id.")
    parser.add_argument("--report", action="store_true", help="Write opt-in report.json using run-scoped equity_curve.csv and trades.csv")
    parser.add_argument("--report-out", type=str, default="", help="Report output path override")
    parser.add_argument("--symbol", type=str, default="", help="Single symbol override. Accepts BTC/JPY or ETH/USDC.")
    parser.add_argument("--symbols", type=str, default="", help="Comma-separated symbols. Empty uses config.SYMBOLS")
    parser.add_argument("--since", type=str, default="", help="YYYY-MM-DD (UTC) inclusive. Empty -> recent-only mode")
    parser.add_argument("--until", type=str, default="", help="YYYY-MM-DD (UTC) inclusive day; internally treated as next-day exclusive")
    parser.add_argument("--entry-tf", type=str, default=str(getattr(C, "ENTRY_TF", getattr(C, "TIMEFRAME_ENTRY", "1m"))))
    parser.add_argument("--filter-tf", type=str, default=str(getattr(C, "FILTER_TF", getattr(C, "TIMEFRAME_FILTER", "1h"))))

    parser.add_argument("--warmup", type=int, default=int(getattr(C, "BACKTEST_WARMUP_BARS", 300)))
    parser.add_argument("--initial", type=float, default=float(getattr(C, "BACKTEST_INITIAL_EQUITY", 300000.0)))
    parser.add_argument("--no-csv", action="store_true", help="Do not export CSVs")
    parser.add_argument(
        "--recent-entry",
        type=int,
        default=int(getattr(C, "BACKTEST_RECENT_BARS_ENTRY", 0)),
        help="Use only latest N entry bars (0 disables). Recommended for MEXC stability.",
    )
    parser.add_argument(
        "--recent-filter",
        type=int,
        default=int(getattr(C, "BACKTEST_RECENT_BARS_FILTER", 0)),
        help="Use only latest N filter bars (0 disables).",
    )
    args = parser.parse_args()

    if int(getattr(args, "year", 0) or 0) > 0:
        _apply_year_preset(int(args.year))
    preset_name = str(getattr(args, "preset", "") or "").strip()
    if not preset_name:
        preset_name = str(os.getenv("BOT_PRESET", "") or "").strip()
    if preset_name:
        C.apply_preset(preset_name)

    raw_symbols = str(getattr(args, "symbol", "") or "").strip() or str(getattr(args, "symbols", "") or "")
    symbols, symbol_source = _resolve_backtest_symbols(raw_symbols)
    if symbols:
        logger.info("[BACKTEST][SYMBOL] symbol=%s source=%s symbols=%s", str(symbols[0]), str(symbol_source), list(symbols))

    if not symbols:
        logger.error("No symbols. Set config.SYMBOLS or pass --symbols.")
        return 1

    since_ms: Optional[int] = getattr(C, "SINCE_MS", None)
    until_ms: Optional[int] = None
    if args.since.strip():
        since_ms = _parse_yyyy_mm_dd_to_ms(args.since.strip(), end_of_day_exclusive=False)
    if args.until.strip():
        until_ms = _parse_yyyy_mm_dd_to_ms(args.until.strip(), end_of_day_exclusive=True)


    recent_entry = int(args.recent_entry) if int(args.recent_entry) > 0 else None
    recent_filter = int(args.recent_filter) if int(args.recent_filter) > 0 else None

    res = run_backtest(
        symbols=symbols,
        since_ms=since_ms,
        until_ms=until_ms,
        entry_tf=str(args.entry_tf),
        filter_tf=str(args.filter_tf),
        warmup_bars=int(args.warmup),
        initial_equity=float(args.initial),
        export_csv=(not bool(args.no_csv)),
        recent_bars_entry=recent_entry,
        recent_bars_filter=recent_filter,
        run_id=str(getattr(args, "run_id", "") or ""),
        perf_debug=bool(getattr(args, "perf", False)),
        **kwargs,
    )

    if res is None:
        logger.error("run_backtest returned None (unexpected). Check previous logs for the root cause.")
        return 1

    logger.info("===== BACKTEST RESULT (runner-compatible / MEXC spot) =====")
    logger.info(f"Symbols: {res['symbols']}")
    logger.info(f"Since(ms): {res['since_ms']} -> effective={res.get('since_ms_effective')} ({res.get('since_iso_effective')})")

    logger.info(f"TF: entry={res['entry_tf']} filter={res['filter_tf']} warmup={res['warmup_bars']}")
    logger.info(f"Spread estimate: {res['spread_bps_est']:.2f} bps")
    logger.info(f"Recent bars: entry={res['recent_bars_entry']} filter={res['recent_bars_filter']}")
    logger.info(f"Trades: {res['trades']}")
    if int(res.get("trades", 0)) == 0:
        logger.info(
            "DIAG trades==0: "
            f"buy_signals={res.get('buy_signals')} entries_opened={res.get('entries_opened')} "
            f"forced_closes={res.get('forced_closes')} open_positions_end={res.get('open_positions_end')}"
        )
    logger.info(f"Win: {res['win']} Lose: {res['lose']}")
    logger.info(f"Final equity: {res['final_equity']:.2f}")
    logger.info(f"Max DD: {res['max_dd']:.4f}")
    logger.info(f"Max DD MTM: {res.get('max_dd_mtm', 0.0):.4f}")
    logger.info(
        "[REPORT] max_dd_worst_bar=%.4f mae_max_abs=%.6f mae_max_bps=%.2f mfe_max_abs=%.6f mfe_max_bps=%.2f giveback_max_abs=%.6f giveback_max_bps=%.2f giveback_to_close_abs=%.6f mae_p50_bps=%.2f mfe_p50_bps=%.2f giveback_to_close_p50_pct_of_mfe=%.4f kept_pct_of_mfe_p50=%.4f",
        float(res.get("max_dd_worst_bar", res.get("max_dd_mtm", 0.0)) or 0.0),
        float(res.get("mae_max_abs", 0.0) or 0.0),
        float(res.get("mae_max_bps", 0.0) or 0.0),
        float(res.get("mfe_max_abs", 0.0) or 0.0),
        float(res.get("mfe_max_bps", 0.0) or 0.0),
        float(res.get("giveback_max_abs", 0.0) or 0.0),
        float(res.get("giveback_max_bps", 0.0) or 0.0),
        float(res.get("giveback_to_close_abs", 0.0) or 0.0),
        float(res.get("mae_p50_bps", 0.0) or 0.0),
        float(res.get("mfe_p50_bps", 0.0) or 0.0),
        float(res.get("giveback_to_close_p50_pct_of_mfe", 0.0) or 0.0),
        float(res.get("kept_pct_of_mfe_p50", 0.0) or 0.0),
    )
    logger.info(f"Net total: {res['net_total']:.2f}  Net avg/trade: {res['net_avg']:.2f}")

    report_enabled = bool(getattr(args, "report", False))
    if report_enabled:
        report_out_default = os.path.join(str(res.get("export_dir") or _current_export_dir()), "report.json")
        report_out_cfg = str(getattr(C, "REPORT_OUT_PATH", "") or "").strip()
        if is_legacy_exports_path(report_out_cfg):
            report_out_cfg = ""
        report_out = str(getattr(args, "report_out", "") or "").strip() or report_out_cfg or report_out_default
        include_yearly = bool(getattr(C, "REPORT_INCLUDE_YEARLY", True))
        include_monthly = True
        risk_free_rate = float(getattr(C, "REPORT_RISK_FREE_RATE", 0.0) or 0.0)
        overall_report = {
            "trades": int(res.get("trades", 0) or 0),
            "net_total": float(res.get("net_total", 0.0) or 0.0),
            "return_pct_of_init": (
                float(res.get("net_total", 0.0) or 0.0) / float(args.initial) * 100.0
                if float(args.initial) > 0.0
                else 0.0
            ),
            "max_dd": float(res.get("max_dd", 0.0) or 0.0),
            "max_dd_mtm": float(res.get("max_dd_mtm", 0.0) or 0.0),
            "max_dd_worst_bar": float(res.get("max_dd_worst_bar", res.get("max_dd_mtm", 0.0)) or 0.0),
            "mae_max_abs": float(res.get("mae_max_abs", 0.0) or 0.0),
            "mae_max_bps": float(res.get("mae_max_bps", 0.0) or 0.0),
            "mae_p50_abs": float(res.get("mae_p50_abs", 0.0) or 0.0),
            "mae_p90_abs": float(res.get("mae_p90_abs", 0.0) or 0.0),
            "mae_p99_abs": float(res.get("mae_p99_abs", 0.0) or 0.0),
            "mae_p50_bps": float(res.get("mae_p50_bps", 0.0) or 0.0),
            "mae_p90_bps": float(res.get("mae_p90_bps", 0.0) or 0.0),
            "mae_p99_bps": float(res.get("mae_p99_bps", 0.0) or 0.0),
            "mfe_max_abs": float(res.get("mfe_max_abs", 0.0) or 0.0),
            "mfe_max_bps": float(res.get("mfe_max_bps", 0.0) or 0.0),
            "mfe_p50_abs": float(res.get("mfe_p50_abs", 0.0) or 0.0),
            "mfe_p90_abs": float(res.get("mfe_p90_abs", 0.0) or 0.0),
            "mfe_p99_abs": float(res.get("mfe_p99_abs", 0.0) or 0.0),
            "mfe_p50_bps": float(res.get("mfe_p50_bps", 0.0) or 0.0),
            "mfe_p90_bps": float(res.get("mfe_p90_bps", 0.0) or 0.0),
            "mfe_p99_bps": float(res.get("mfe_p99_bps", 0.0) or 0.0),
            "giveback_max_abs": float(res.get("giveback_max_abs", 0.0) or 0.0),
            "giveback_max_bps": float(res.get("giveback_max_bps", 0.0) or 0.0),
            "giveback_max_pct_of_mfe": float(res.get("giveback_max_pct_of_mfe", 0.0) or 0.0),
            "giveback_max_p50_abs": float(res.get("giveback_max_p50_abs", 0.0) or 0.0),
            "giveback_max_p90_abs": float(res.get("giveback_max_p90_abs", 0.0) or 0.0),
            "giveback_max_p99_abs": float(res.get("giveback_max_p99_abs", 0.0) or 0.0),
            "giveback_max_p50_bps": float(res.get("giveback_max_p50_bps", 0.0) or 0.0),
            "giveback_max_p90_bps": float(res.get("giveback_max_p90_bps", 0.0) or 0.0),
            "giveback_max_p99_bps": float(res.get("giveback_max_p99_bps", 0.0) or 0.0),
            "giveback_max_p50_pct_of_mfe": float(res.get("giveback_max_p50_pct_of_mfe", 0.0) or 0.0),
            "giveback_max_p90_pct_of_mfe": float(res.get("giveback_max_p90_pct_of_mfe", 0.0) or 0.0),
            "giveback_max_p99_pct_of_mfe": float(res.get("giveback_max_p99_pct_of_mfe", 0.0) or 0.0),
            "giveback_to_close_abs": float(res.get("giveback_to_close_abs", 0.0) or 0.0),
            "giveback_to_close_bps": float(res.get("giveback_to_close_bps", 0.0) or 0.0),
            "giveback_to_close_pct_of_mfe": float(res.get("giveback_to_close_pct_of_mfe", 0.0) or 0.0),
            "giveback_to_close_p50_abs": float(res.get("giveback_to_close_p50_abs", 0.0) or 0.0),
            "giveback_to_close_p90_abs": float(res.get("giveback_to_close_p90_abs", 0.0) or 0.0),
            "giveback_to_close_p99_abs": float(res.get("giveback_to_close_p99_abs", 0.0) or 0.0),
            "giveback_to_close_p50_bps": float(res.get("giveback_to_close_p50_bps", 0.0) or 0.0),
            "giveback_to_close_p90_bps": float(res.get("giveback_to_close_p90_bps", 0.0) or 0.0),
            "giveback_to_close_p99_bps": float(res.get("giveback_to_close_p99_bps", 0.0) or 0.0),
            "giveback_to_close_p50_pct_of_mfe": float(res.get("giveback_to_close_p50_pct_of_mfe", 0.0) or 0.0),
            "giveback_to_close_p90_pct_of_mfe": float(res.get("giveback_to_close_p90_pct_of_mfe", 0.0) or 0.0),
            "giveback_to_close_p99_pct_of_mfe": float(res.get("giveback_to_close_p99_pct_of_mfe", 0.0) or 0.0),
            "kept_p50_bps": float(res.get("kept_p50_bps", 0.0) or 0.0),
            "kept_p90_bps": float(res.get("kept_p90_bps", 0.0) or 0.0),
            "kept_p99_bps": float(res.get("kept_p99_bps", 0.0) or 0.0),
            "kept_pct_of_mfe_p50": float(res.get("kept_pct_of_mfe_p50", 0.0) or 0.0),
            "kept_pct_of_mfe_p90": float(res.get("kept_pct_of_mfe_p90", 0.0) or 0.0),
            "kept_pct_of_mfe_p99": float(res.get("kept_pct_of_mfe_p99", 0.0) or 0.0),
            "fav_adv_ratio_p50": float(res.get("fav_adv_ratio_p50", 0.0) or 0.0),
            "fav_adv_ratio_p90": float(res.get("fav_adv_ratio_p90", 0.0) or 0.0),
            "fav_adv_ratio_p99": float(res.get("fav_adv_ratio_p99", 0.0) or 0.0),
            "exit_hint": str(res.get("exit_hint", "") or ""),
        }
        _write_yearly_report_from_csv(
            out_path=report_out,
            build_id=str(BUILD_ID),
            preset_name=str(preset_name or ""),
            year=int(args.year) if int(getattr(args, "year", 0) or 0) > 0 else None,
            mode="BACKTEST",
            overall=overall_report,
            include_yearly=bool(include_yearly),
            include_monthly=bool(include_monthly),
            risk_free_rate=float(risk_free_rate),
        )
        _write_last_run_reference(mode="BACKTEST", replay_report=str(report_out), extra={"report_written": True})
    return 0
    
if __name__ == "__main__":
    if os.getenv("BT_YEAR"):
        C.BACKTEST_DATASET = os.getenv("BT_YEAR")
    raise SystemExit(main())
