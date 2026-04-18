# BUILD_ID: 2026-04-18_free_runner_preflight_stable_prefix_v1
# BUILD_ID: 2026-04-18_free_runner_replay_live_paper_autoprepare_v1
# BUILD_ID: 2026-04-18_free_shared_market_data_fallback_v1
# BUILD_ID: 2026-04-18_free_pullback_ab_parent_canonical_v1
# BUILD_ID: 2026-03-29_free_from_standard_nonlive_build_v1
# BUILD_ID: 2026-03-28_standard_live_equity_currency_auto_quote_v1
# BUILD_ID: 2026-03-28_standard_kill_floor_by_ccy_v1
# BUILD_ID: 2026-03-27_runner_live_chart_runtime_price_tap_v2_0_9
# BUILD_ID: 2026-03-26_runner_live_zero_equity_persist_v2
# BUILD_ID: 2026-03-26_runner_live_zero_equity_stop_new_only_v1
# BUILD_ID: 2026-03-26_standard_runner_live_license_gate_restore_v1
# BUILD_ID: 2026-03-21_runtime_logs_default_path_v1
# BUILD_ID: 2026-03-21_runner_final_residual_comment_cleanup_v1
# BUILD_ID: 2026-03-20_runner_replay_mirror_dataset_diag_log_v2
# BUILD_ID: 2026-03-20_runner_replay_market_data_root_fix_v1
# BUILD_ID: 2026-03-20_runner_replay_dataset_diag_log_v1
# BUILD_ID: 2026-03-11_runtime_log_level_gate_v1
# BUILD_ID: 2026-03-16_runner_replay_trail_init_risk_fix_v1
# BUILD_ID: 2026-03-16_runner_replay_risk_gate_parity_fix_v1
# BUILD_ID: 2026-03-16_runner_replay_entry_confirmed_now_fix_v1
# BUILD_ID: 2026-03-16_runner_replay_precomputed_entry_range_fix_v1
# BUILD_ID: 2026-03-15_runner_replay_confirmed_bar_parity_v1
# BUILD_ID: 2026-03-15_runner_replay_year_boundary_history_fix_v1
# BUILD_ID: 2026-03-15_runner_replay_precomputed_filter_regime_fix_v1
# BUILD_ID: 2026-03-15_runner_replay_state_fast_path_v1
# BUILD_ID: 2026-03-09_runner_replay_excursion_parity_v1
# BUILD_ID: 2026-03-10_runner_replay_sizing_state_carry_v1
# BUILD_ID: 2026-03-19_runner_replay_trail_precomputed_atr_v1
# BUILD_ID: 2026-03-19_runner_replay_trail_bps_only_ratchet_v1
# BUILD_ID: 2026-03-19_runner_replay_trail_max_atr_bps_restore_v1
# BUILD_ID: 2026-03-19_runner_replay_trail_atr_manage_ts_v1
# BUILD_ID: 2026-03-19_runner_replay_trail_atr_series_index_v1
# BUILD_ID: 2026-03-19_runner_replay_trail_atr_fallback_diag_v1
# BUILD_ID: 2026-03-19_runner_replay_trail_atr_shape_diag_v1
# BUILD_ID: 2026-03-19_runner_replay_trail_atr_shape_diag_v2
# BUILD_ID: 2026-03-19_runner_replay_trail_atr_ndarray_truthiness_fix_v1
# BUILD_ID: 2026-03-09_runner_short_replay_report_scope_fix_v1
# BUILD_ID: 2026-03-09_runner_excursion_state_guard_v1
# BUILD_ID: 2026-03-09_runner_live_bar_signal_summary_v1
# BUILD_ID: 2026-03-09_runner_stop_taxonomy_align_v1
# BUILD_ID: 2026-03-20_runner_cli_dataset_bridge_v1
# BUILD_ID: 2026-03-20_runner_cli_symbol_preset_bridge_v1
# runner.py


from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import os
import math
import re
import time
import shutil
from logging.handlers import RotatingFileHandler
from bisect import bisect_left, bisect_right
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict

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

import config as C
import backtest as BT
from exchange import ExchangeClient
from state_store import StateStore
from safety import check_and_update_emergency_stop
from app.core.launch import ensure_live_license_or_raise
# Strategy exit helpers may be absent in some branches, so keep this import tolerant.
import strategy as STRAT
from strategy import (
    detect_regime_1h,
    detect_regime_1h_precomputed,
    signal_entry,
    signal_entry_stateful,
    signal_range_entry,
    signal_range_entry_precomputed,
)
from risk import calc_qty_from_risk
from indicators import atr as ind_atr, ema as ind_ema, rsi as ind_rsi, adx as ind_adx
from trace_bar_policy import (
    attach_bar_snapshot as _trace_attach_bar_snapshot,
    expected_bar_ts_ms as _trace_policy_expected_bar_ts_ms,
    normalize_ref_ts_ms as _trace_policy_ref_ts_ms,
    snapshot_ohlc_at as _trace_policy_snapshot_ohlc_at,
)
from app.core.data_pipeline import auto_prepare_runtime_data, resolve_prepare_month_window
from app.core.dataset import (
    DatasetResolutionError,
    build_missing_dataset_message,
    infer_year_from_ms,
    normalize_runtime_symbol,
    resolve_dataset,
    resolve_dataset_override_symbol,
    symbol_to_prefix,
)
from app.core.fees import resolve_paper_fees
from app.core.market_meta import (
    MarketMeta,
    estimate_min_operational_balance,
    maybe_refresh_market_meta,
    resolve_quote_ccy,
)
from app.core.paths import ensure_runtime_dirs
from app.core.source_provenance import record_path_source_event_once
from app.core.export_paths import (
    build_run_export_dir,
    is_legacy_exports_path,
    resolve_run_id,
    write_last_run_json,
)
from app.core.instrument_registry import default_quote_for_exchange
from app.core.instrument_registry import default_symbol_for_exchange
from app.core.instrument_registry import quote_for_symbol
from app.core.chart_state_path import build_chart_state_path, sanitize_symbol_for_chart_state
from app.core.state_context import (
    StateContext,
    build_state_context,
    context_id_for,
    context_matches_meta,
    ensure_context_layout,
    format_context_brief,
    register_context,
    resolve_state_context_paths,
    write_context_meta,
)

logger = logging.getLogger("runner")
trade_logger = logging.getLogger("trade")
error_logger = logging.getLogger("error")
BUILD_ID = "2026-04-18_free_runner_preflight_stable_prefix_v1"
BASE_DIR = Path(__file__).resolve().parent
_APP_PATHS = ensure_runtime_dirs()
RUNTIME_ROOT = Path(_APP_PATHS.runtime_dir).resolve()
STATE_DIR = Path(_APP_PATHS.state_dir).resolve()
STATE_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = STATE_DIR / "state.db"
_FREE_BUILD_LIVE_MESSAGE = "FREE build does not support LIVE mode"


def _resolve_runtime_log_path(configured_path: Any, default_name: str) -> str:
    raw = str(configured_path or "").strip()
    if raw:
        path_obj = Path(raw)
        if not path_obj.is_absolute():
            path_obj = BASE_DIR / path_obj
    else:
        path_obj = Path(_APP_PATHS.logs_dir) / str(default_name)
    return str(path_obj)


def _is_free_nonlive_build() -> bool:
    config_tier = str(getattr(C, "BUILD_TIER", "") or "").strip().upper()
    return config_tier == "FREE"


def _reject_live_for_free_build(mode_name: str | None) -> None:
    if not _is_free_nonlive_build():
        return
    if str(mode_name or "").strip().upper() == "LIVE":
        raise SystemExit(_FREE_BUILD_LIVE_MESSAGE)


_CURRENT_STATE_CONTEXT: StateContext | None = None
_CURRENT_STATE_CONTEXT_PATHS: dict[str, str] = {}
_RUNNER_STARTED = False
_STARTUP_LOG_DONE = False
_PAPER_EQUITY_STATE_LOG_DONE = False
_LIVE_RESUME_GUARD_DONE = False
_LIVE_RESUME_FORCE_STOP_NEW_ONLY = False
_STARTUP_OPS_DONE = False
_LIVE_EQUITY_DIAG_LOGGED = False
_LIVE_EQUITY_SYMBOLS: list[str] = []
_LIVE_EQUITY_FOR_SIZING: float | None = None
_LIVE_EQUITY_REFRESH_AFTER_CLOSE_LAST_TS: float = 0.0
_LIVE_EQUITY_LAST_CUR: str = ""
_LIVE_EQUITY_RESOLUTION_LAST: tuple[str, str, str] | None = None
_KILL_MIN_EQUITY_RESOLUTION_LAST: tuple[str, float, str] | None = None
_KILL_MIN_EQUITY_INVALID_DICT_LOGGED: set[str] = set()
_POST_ONLY_PARAMS: dict[str, Any] = {"postOnly": True}
_RESUME_LOGGED_SYMBOLS: set[str] = set()
_ORDER_SKIP_MIN_LOG_KEYS: set[tuple[str, int, str]] = set()
_DIFF_TRACE_META_WRITTEN: set[str] = set()
_DIFF_TRACE_SOURCE = "live"  # "live" or "replay" (set by _run_replay)
_REPLAY_DATASET_INFO: dict[str, Any] = {}
_REPLAY_PRECOMPUTED_SOURCE_LOGGED: set[tuple[str, str, str, str, str]] = set()
_REPLAY_SIZING_STATE: dict[str, float] = {}
_AUTO_PREPARE_REQUESTS: set[str] = set()
_RUNTIME_PREFLIGHT_COMPLETED: set[str] = set()
_CURRENT_EXPORT_RUN_ID = ""
_CURRENT_EXPORT_SYMBOL = ""
_CURRENT_EXPORT_MODE = ""
_CURRENT_EXPORT_DIR = ""
_ttl_stats = {
    "maker_orders": 0,
    "maker_filled": 0,
    "ttl_expired": 0,
    "fill_time_ms_total": 0,
}
_ttl_order_start_ms: dict[tuple[str, str], int] = {}
_TTL_STATS_LAST_LOG_SEC = 0.0
_TTL_STATS_INTERVAL_SEC_OVERRIDE: float | None = None
_SIGNAL_REASON_COLLECTOR: list[str] | None = None
_SIGNAL_REASON_COLLECTOR_SEEN: set[str] | None = None
_RUNTIME_SIGNAL_SUMMARY: dict[str, Any] = {
    "signal_evaluations": 0,
    "buy_count": 0,
    "hold_count": 0,
    "hold_reasons": Counter(),
    "last_signal_ts": 0,
    "last_signal_action": "",
    "last_signal_reason": "",
    "last_signal_symbol": "",
    "last_signal_regime": "",
}
_CHART_STATE_MAX_CANDLES = 180
_CHART_STATE_MAX_MARKERS = 16
_CHART_STATE_MARKERS: dict[str, dict[str, list[dict[str, Any]]]] = {}
_LIVE_CHART_OHLCV_BY_SYMBOL: dict[str, dict[str, list[float]]] = {}
_LIVE_CHART_SEED_BARS_BY_SYMBOL: dict[str, list[dict[str, float | int]]] = {}
_LIVE_CHART_PARTIAL_BAR_BY_SYMBOL: dict[str, dict[str, float | int]] = {}
_LIVE_CHART_SEED_PRICE_SOURCE_BY_SYMBOL: dict[str, str] = {}
_LIVE_CHART_SEED_PRICE_SOURCE_AGE_MS_BY_SYMBOL: dict[str, int] = {}
_LIVE_CHART_IN_MEMORY_PRICE_BY_SYMBOL: dict[str, dict[str, Any]] = {}
_LIVE_CHART_INTERNAL_BOOTSTRAP_BY_SYMBOL: dict[str, dict[str, list[float]]] = {}
_LIVE_CHART_EXCHANGE_OHLCV_BY_SYMBOL: dict[str, dict[str, list[float]]] = {}
_LIVE_CHART_DIAG_BY_SYMBOL: dict[str, dict[str, Any]] = {}
_LIVE_CHART_WRITER_LOGGED_PATHS: dict[str, tuple[str, str, int, str, str, str]] = {}
_LIVE_CHART_PERSISTED_SKIP_LOGGED: dict[str, tuple[str, int]] = {}


def _chart_state_symbol_token(symbol: str) -> str:
    return sanitize_symbol_for_chart_state(symbol)


def _chart_state_symbol_keys(symbol: str) -> list[str]:
    sym = str(symbol or "").strip()
    if not sym:
        return [""]
    out: list[str] = [sym]
    flat = sym.replace("/", "")
    if flat and flat not in out:
        out.append(flat)
    if "/" not in sym and len(sym) >= 6:
        split = f"{sym[:3]}/{sym[3:]}"
        if split not in out:
            out.append(split)
    return out


def _chart_state_float(value: Any) -> float | None:
    try:
        out = float(value)
    except Exception:
        return None
    if not math.isfinite(out):
        return None
    return float(out)


def _chart_state_int(value: Any) -> int | None:
    try:
        out = int(float(value))
    except Exception:
        return None
    return int(out)


def _chart_state_bucket(symbol: str) -> dict[str, list[dict[str, Any]]]:
    key = _chart_state_symbol_token(symbol)
    bucket = _CHART_STATE_MARKERS.get(key)
    if isinstance(bucket, dict):
        bucket.setdefault("entries", [])
        bucket.setdefault("exits", [])
        return bucket
    bucket = {"entries": [], "exits": []}
    _CHART_STATE_MARKERS[key] = bucket
    return bucket


def _chart_state_note_marker(symbol: str, *, kind: str, ts_ms: Any, price: Any, side: str, label: str) -> None:
    bucket = _chart_state_bucket(symbol)
    target_key = "entries" if str(kind or "").strip().lower().startswith("entry") else "exits"
    target = bucket[target_key]
    ts_value = _chart_state_int(ts_ms)
    price_value = _chart_state_float(price)
    if ts_value is None or ts_value <= 0 or price_value is None or price_value <= 0.0:
        return
    payload = {
        "ts_ms": int(ts_value),
        "price": float(price_value),
        "side": str(side or "long").strip().lower() or "long",
        "label": str(label or "").strip(),
    }
    if target:
        last = target[-1]
        if (
            int(last.get("ts_ms") or 0) == int(payload["ts_ms"])
            and abs(float(last.get("price") or 0.0) - float(payload["price"])) <= 1e-12
            and str(last.get("label") or "") == str(payload["label"])
        ):
            return
    target.append(payload)
    if len(target) > _CHART_STATE_MAX_MARKERS:
        del target[:-_CHART_STATE_MAX_MARKERS]


def _chart_state_note_diag(
    symbol: str,
    *,
    source: str,
    reason: str = "",
    candles_count: int = 0,
    last_candle_ts_ms: int | None = None,
    seed_price_source: str = "",
    seed_price_source_age_ms: int | None = None,
) -> None:
    payload = {
        "source": str(source or "empty").strip() or "empty",
        "reason": str(reason or "").strip(),
        "candles_count": max(0, int(candles_count or 0)),
        "last_candle_ts_ms": int(last_candle_ts_ms) if last_candle_ts_ms is not None else None,
        "seed_price_source": str(seed_price_source or "").strip(),
        "seed_price_source_age_ms": int(seed_price_source_age_ms) if seed_price_source_age_ms is not None else None,
    }
    for key in _chart_state_symbol_keys(symbol):
        _LIVE_CHART_DIAG_BY_SYMBOL[key] = dict(payload)


def _chart_state_recent_ohlcv_copy(ohlcv: dict[str, Any] | None) -> dict[str, list[float]] | None:
    if not isinstance(ohlcv, dict):
        return None
    ts = list(ohlcv.get("timestamp") or [])
    op = list(ohlcv.get("open") or [])
    hi = list(ohlcv.get("high") or [])
    lo = list(ohlcv.get("low") or [])
    cl = list(ohlcv.get("close") or [])
    vol = list(ohlcv.get("volume") or [])
    count = min(len(ts), len(op), len(hi), len(lo), len(cl))
    if count <= 0:
        return None
    start = max(0, count - _CHART_STATE_MAX_CANDLES)
    return {
        "timestamp": [int(x) for x in ts[start:count]],
        "open": [float(x) for x in op[start:count]],
        "high": [float(x) for x in hi[start:count]],
        "low": [float(x) for x in lo[start:count]],
        "close": [float(x) for x in cl[start:count]],
        "volume": [float(x) for x in vol[start:count]] if len(vol) >= count else [0.0 for _ in range(count - start)],
    }


def _chart_state_cache_ohlcv(
    symbol: str,
    ohlcv: dict[str, Any] | None,
    *,
    source: str,
    reason: str = "",
    seed_price_source: str = "",
    seed_price_source_age_ms: int | None = None,
) -> None:
    payload = _chart_state_recent_ohlcv_copy(ohlcv)
    if not isinstance(payload, dict):
        _chart_state_note_diag(
            symbol,
            source=source,
            reason=reason or "no_recent_ohlcv",
            seed_price_source=seed_price_source,
            seed_price_source_age_ms=seed_price_source_age_ms,
        )
        return
    last_candle_ts_ms = int(payload["timestamp"][-1]) if payload["timestamp"] else None
    target_cache = _LIVE_CHART_OHLCV_BY_SYMBOL
    if str(source or "").strip() == "exchange_cache":
        target_cache = _LIVE_CHART_EXCHANGE_OHLCV_BY_SYMBOL
    for key in _chart_state_symbol_keys(symbol):
        target_cache[key] = {
            "timestamp": list(payload["timestamp"]),
            "open": list(payload["open"]),
            "high": list(payload["high"]),
            "low": list(payload["low"]),
            "close": list(payload["close"]),
            "volume": list(payload["volume"]),
        }
    _chart_state_note_diag(
        symbol,
        source=source,
        reason=str(reason or "").strip(),
        candles_count=len(payload["timestamp"]),
        last_candle_ts_ms=last_candle_ts_ms,
        seed_price_source=seed_price_source,
        seed_price_source_age_ms=seed_price_source_age_ms,
    )


def _chart_state_store_internal_bootstrap_ohlcv(symbol: str, ohlcv: dict[str, Any] | None) -> bool:
    payload = _chart_state_recent_ohlcv_copy(ohlcv)
    if not isinstance(payload, dict):
        return False
    for key in _chart_state_symbol_keys(symbol):
        _LIVE_CHART_INTERNAL_BOOTSTRAP_BY_SYMBOL[key] = {
            "timestamp": list(payload["timestamp"]),
            "open": list(payload["open"]),
            "high": list(payload["high"]),
            "low": list(payload["low"]),
            "close": list(payload["close"]),
            "volume": list(payload["volume"]),
        }
    return bool(payload["timestamp"])


def _chart_state_seed_builder_observe(
    symbol: str,
    timeframe: str,
    *,
    ts_ms: Any,
    price: Any,
    source: str = "",
    source_age_ms: int | None = None,
) -> bool:
    ts_value = _chart_state_int(ts_ms)
    price_value = _chart_state_float(price)
    if ts_value is None or ts_value <= 0 or price_value is None or price_value <= 0.0:
        return False
    try:
        tf_ms = max(60_000, int(_timeframe_to_ms(str(timeframe or "5m"))))
    except Exception:
        tf_ms = 300_000
    bucket_ts_ms = max(0, (int(ts_value) // int(tf_ms)) * int(tf_ms))
    recent: list[dict[str, float | int]] = []
    partial: dict[str, float | int] | None = None
    for key in _chart_state_symbol_keys(symbol):
        raw_recent = _LIVE_CHART_SEED_BARS_BY_SYMBOL.get(key)
        if isinstance(raw_recent, list):
            recent = [dict(row) for row in raw_recent if isinstance(row, dict)]
            partial_raw = _LIVE_CHART_PARTIAL_BAR_BY_SYMBOL.get(key)
            partial = dict(partial_raw) if isinstance(partial_raw, dict) else None
            break
    price_f = float(price_value)
    if isinstance(partial, dict) and int(partial.get("ts_ms") or 0) == int(bucket_ts_ms):
        partial["high"] = max(float(partial.get("high") or price_f), price_f)
        partial["low"] = min(float(partial.get("low") or price_f), price_f)
        partial["close"] = float(price_f)
    elif isinstance(partial, dict) and int(partial.get("ts_ms") or 0) > int(bucket_ts_ms):
        updated = False
        for row in recent:
            if int(row.get("ts_ms") or 0) != int(bucket_ts_ms):
                continue
            row["high"] = max(float(row.get("high") or price_f), price_f)
            row["low"] = min(float(row.get("low") or price_f), price_f)
            row["close"] = float(price_f)
            updated = True
            break
        if not updated:
            recent.append(
                {
                    "ts_ms": int(bucket_ts_ms),
                    "open": float(price_f),
                    "high": float(price_f),
                    "low": float(price_f),
                    "close": float(price_f),
                }
            )
            recent.sort(key=lambda row: int(row.get("ts_ms") or 0))
    else:
        if isinstance(partial, dict) and int(partial.get("ts_ms") or 0) > 0:
            prev_ts = int(partial.get("ts_ms") or 0)
            if recent and int(recent[-1].get("ts_ms") or 0) == prev_ts:
                recent[-1] = dict(partial)
            else:
                recent.append(dict(partial))
        partial = {
            "ts_ms": int(bucket_ts_ms),
            "open": float(price_f),
            "high": float(price_f),
            "low": float(price_f),
            "close": float(price_f),
        }
    if len(recent) > _CHART_STATE_MAX_CANDLES:
        recent = recent[-_CHART_STATE_MAX_CANDLES:]
    for key in _chart_state_symbol_keys(symbol):
        _LIVE_CHART_SEED_BARS_BY_SYMBOL[key] = [dict(row) for row in recent]
        if isinstance(partial, dict):
            _LIVE_CHART_PARTIAL_BAR_BY_SYMBOL[key] = dict(partial)
        source_text = str(source or "").strip()
        if source_text:
            _LIVE_CHART_SEED_PRICE_SOURCE_BY_SYMBOL[key] = source_text
            if source_age_ms is not None:
                _LIVE_CHART_SEED_PRICE_SOURCE_AGE_MS_BY_SYMBOL[key] = max(0, int(source_age_ms))
            else:
                _LIVE_CHART_SEED_PRICE_SOURCE_AGE_MS_BY_SYMBOL.pop(key, None)
    _chart_state_remember_in_memory_price(
        symbol,
        price=float(price_value),
        ts_ms=int(ts_value),
        source=str(source or "seed_builder_price"),
        age_ms=source_age_ms,
    )
    return True


def _chart_state_seed_builder_ohlcv(symbol: str) -> dict[str, list[float]] | None:
    recent: list[dict[str, float | int]] = []
    partial: dict[str, float | int] | None = None
    for key in _chart_state_symbol_keys(symbol):
        raw_recent = _LIVE_CHART_SEED_BARS_BY_SYMBOL.get(key)
        if isinstance(raw_recent, list):
            recent = [dict(row) for row in raw_recent if isinstance(row, dict)]
            partial_raw = _LIVE_CHART_PARTIAL_BAR_BY_SYMBOL.get(key)
            partial = dict(partial_raw) if isinstance(partial_raw, dict) else None
            break
    rows = [dict(row) for row in recent]
    if isinstance(partial, dict) and int(partial.get("ts_ms") or 0) > 0:
        if rows and int(rows[-1].get("ts_ms") or 0) == int(partial.get("ts_ms") or 0):
            rows[-1] = dict(partial)
        else:
            rows.append(dict(partial))
    if not rows:
        return None
    rows = rows[-_CHART_STATE_MAX_CANDLES:]
    out = {
        "timestamp": [],
        "open": [],
        "high": [],
        "low": [],
        "close": [],
        "volume": [],
    }
    for row in rows:
        ts_value = _chart_state_int(row.get("ts_ms"))
        open_value = _chart_state_float(row.get("open"))
        high_value = _chart_state_float(row.get("high"))
        low_value = _chart_state_float(row.get("low"))
        close_value = _chart_state_float(row.get("close"))
        if ts_value is None or open_value is None or high_value is None or low_value is None or close_value is None:
            continue
        out["timestamp"].append(int(ts_value))
        out["open"].append(float(open_value))
        out["high"].append(max(float(open_value), float(high_value), float(low_value), float(close_value)))
        out["low"].append(min(float(open_value), float(high_value), float(low_value), float(close_value)))
        out["close"].append(float(close_value))
        out["volume"].append(0.0)
    return out if out["timestamp"] else None


def _chart_state_last_price_from_ohlcv(ohlcv: dict[str, Any] | None) -> tuple[float | None, int | None]:
    if not isinstance(ohlcv, dict):
        return (None, None)
    ts_list = list(ohlcv.get("timestamp") or [])
    close_list = list(ohlcv.get("close") or [])
    if not ts_list or not close_list:
        return (None, None)
    price_value = _chart_state_float(close_list[-1])
    ts_value = _chart_state_int(ts_list[-1])
    return (price_value, ts_value)


def _chart_state_seed_reason_from_source(seed_price_source: str) -> str:
    source_text = str(seed_price_source or "").strip()
    if source_text.startswith("runtime_tap_") or source_text.startswith("in_memory_cached_runtime_tap_"):
        return "using_runtime_price_tap"
    if source_text.startswith("in_memory_"):
        return "using_in_memory_live_price_cache"
    if source_text.startswith("persisted_") or source_text in {
        "position_entry_price",
        "seed_builder_partial_close",
        "last_success_cache_close",
        "internal_bootstrap_close",
        "exchange_cache_close",
    }:
        return "using_guaranteed_first_price_observation"
    return "using_internal_price_seed_builder"


def _chart_state_remember_in_memory_price(
    symbol: str,
    *,
    price: Any,
    ts_ms: Any,
    source: str,
    age_ms: int | None = None,
) -> bool:
    price_value = _chart_state_float(price)
    ts_value = _chart_state_int(ts_ms)
    source_text = str(source or "").strip()
    if price_value is None or price_value <= 0.0 or not source_text:
        return False
    payload = {
        "price": float(price_value),
        "ts_ms": int(ts_value) if ts_value is not None and ts_value > 0 else None,
        "source": str(source_text),
        "age_ms": int(age_ms) if age_ms is not None else None,
        "observed_at_ms": int(time.time() * 1000),
    }
    for key in _chart_state_symbol_keys(symbol):
        _LIVE_CHART_IN_MEMORY_PRICE_BY_SYMBOL[key] = dict(payload)
    return True


def _chart_state_remember_runtime_tapped_price(
    symbol: str,
    *,
    price: Any,
    ts_ms: Any = None,
    source: str,
    age_ms: int | None = None,
) -> bool:
    source_text = str(source or "").strip()
    if not source_text:
        return False
    if not source_text.startswith("runtime_tap_"):
        source_text = f"runtime_tap_{source_text}"
    ts_value = _chart_state_int(ts_ms)
    if ts_value is None or ts_value <= 0:
        ts_value = int(time.time() * 1000)
    return _chart_state_remember_in_memory_price(
        symbol,
        price=price,
        ts_ms=int(ts_value),
        source=str(source_text),
        age_ms=age_ms,
    )


def _chart_state_in_memory_price_max_age_ms(timeframe: str) -> int:
    return _chart_state_persisted_snapshot_max_age_ms(str(timeframe or "5m"))


def _chart_state_pick_in_memory_live_price(
    ex: Any,
    symbol: str,
    *,
    timeframe: str,
    fallback_ts_ms: int,
) -> tuple[float | None, int | None, str, int | None]:
    max_age_ms = _chart_state_in_memory_price_max_age_ms(str(timeframe or "5m"))
    now_ms = int(time.time() * 1000)

    def _normalize_candidate(price: Any, ts_ms: Any, source: str, age_ms: int | None = None) -> tuple[float | None, int | None, str, int | None]:
        price_value = _chart_state_float(price)
        if price_value is None or price_value <= 0.0:
            return (None, None, "", None)
        ts_value = _chart_state_int(ts_ms)
        if ts_value is not None:
            computed_age_ms = int(age_ms) if age_ms is not None else int(now_ms - int(ts_value))
            if computed_age_ms < -60_000:
                return (None, None, "", None)
            if computed_age_ms > int(max_age_ms):
                return (None, None, "", None)
            return (float(price_value), int(max(0, int(fallback_ts_ms or ts_value))), str(source), int(max(0, computed_age_ms)))
        computed_age_ms = int(age_ms) if age_ms is not None else None
        if computed_age_ms is not None and computed_age_ms > int(max_age_ms):
            return (None, None, "", None)
        return (float(price_value), int(max(0, int(fallback_ts_ms or 0))), str(source), computed_age_ms)

    for key in _chart_state_symbol_keys(symbol):
        cached = _LIVE_CHART_IN_MEMORY_PRICE_BY_SYMBOL.get(key)
        if not isinstance(cached, dict):
            continue
        source_text = str(cached.get("source") or "").strip()
        if not source_text:
            continue
        prefixed_source = source_text
        if not (source_text.startswith("runtime_tap_") or source_text.startswith("in_memory_")):
            prefixed_source = f"in_memory_cached_{source_text}"
        candidate = _normalize_candidate(
            cached.get("price"),
            cached.get("ts_ms") or cached.get("observed_at_ms"),
            prefixed_source,
            cached.get("age_ms"),
        )
        if candidate[0] is not None:
            return candidate

    ex_obj = getattr(ex, "ex", None) if ex is not None else None
    if ex_obj is None:
        return (None, None, "", None)

    symbol_variants = _chart_state_symbol_keys(symbol)

    tickers = getattr(ex_obj, "tickers", None)
    if isinstance(tickers, dict):
        for key in symbol_variants:
            ticker = tickers.get(key)
            if not isinstance(ticker, dict):
                continue
            ticker_ts = _chart_state_int(ticker.get("timestamp")) or _chart_state_parse_iso_to_ms(ticker.get("datetime"))
            last_value = _chart_state_float(ticker.get("last"))
            if last_value is not None and last_value > 0.0:
                candidate = _normalize_candidate(last_value, ticker_ts, "in_memory_ticker_last")
                if candidate[0] is not None:
                    return candidate
            bid_value = _chart_state_float(ticker.get("bid"))
            ask_value = _chart_state_float(ticker.get("ask"))
            if bid_value is not None and ask_value is not None and bid_value > 0.0 and ask_value > 0.0:
                candidate = _normalize_candidate((float(bid_value) + float(ask_value)) / 2.0, ticker_ts, "in_memory_ticker_mid")
                if candidate[0] is not None:
                    return candidate

    orderbooks = getattr(ex_obj, "orderbooks", None)
    if isinstance(orderbooks, dict):
        for key in symbol_variants:
            orderbook = orderbooks.get(key)
            if not isinstance(orderbook, dict):
                continue
            bids = list(orderbook.get("bids") or [])
            asks = list(orderbook.get("asks") or [])
            if not bids or not asks:
                continue
            bid_value = _chart_state_float((bids[0][0] if bids else None))
            ask_value = _chart_state_float((asks[0][0] if asks else None))
            if bid_value is None or ask_value is None or bid_value <= 0.0 or ask_value <= 0.0:
                continue
            orderbook_ts = _chart_state_int(orderbook.get("timestamp")) or _chart_state_parse_iso_to_ms(orderbook.get("datetime"))
            candidate = _normalize_candidate((float(bid_value) + float(ask_value)) / 2.0, orderbook_ts, "in_memory_orderbook_mid")
            if candidate[0] is not None:
                return candidate

    trades_cache = getattr(ex_obj, "trades", None)
    if isinstance(trades_cache, dict):
        for key in symbol_variants:
            trade_rows = trades_cache.get(key)
            if trade_rows is None:
                continue
            try:
                rows = list(trade_rows)
            except Exception:
                rows = []
            if not rows:
                continue
            last_trade = rows[-1] if isinstance(rows[-1], dict) else {}
            trade_price = _chart_state_float((last_trade or {}).get("price"))
            trade_ts = _chart_state_int((last_trade or {}).get("timestamp")) or _chart_state_parse_iso_to_ms((last_trade or {}).get("datetime"))
            candidate = _normalize_candidate(trade_price, trade_ts, "in_memory_ws_last_trade")
            if candidate[0] is not None:
                return candidate

    return (None, None, "", None)


def _chart_state_persisted_snapshot_max_age_ms(timeframe: str) -> int:
    try:
        tf_ms = max(60_000, int(_timeframe_to_ms(str(timeframe or "5m"))))
    except Exception:
        tf_ms = 300_000
    return int(min(max(2 * int(tf_ms), 60_000), 15 * 60_000))


def _chart_state_parse_iso_to_ms(value: Any) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        normalized = text.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
    except Exception:
        return None
    try:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


def _chart_state_log_persisted_snapshot_skip(symbol: str, *, reason: str, age_ms: int = -1) -> None:
    reason_text = str(reason or "").strip() or "unknown"
    age_value = int(age_ms) if age_ms is not None else -1
    state = (reason_text, age_value)
    for key in _chart_state_symbol_keys(symbol):
        if _LIVE_CHART_PERSISTED_SKIP_LOGGED.get(key) == state:
            return
    msg = f"[chart_state] persisted snapshot skipped symbol={symbol} reason={reason_text}"
    if age_value >= 0:
        msg += f" age_ms={age_value}"
    logger.info(msg)
    for key in _chart_state_symbol_keys(symbol):
        _LIVE_CHART_PERSISTED_SKIP_LOGGED[key] = state


def _chart_state_persisted_snapshot_price(
    state_root: str,
    exchange_id: str,
    run_mode: str,
    symbol: str,
    fallback_ts_ms: int,
    timeframe: str,
) -> tuple[float | None, int | None, str, int | None]:
    path = Path(build_chart_state_path(str(state_root), exchange_id, run_mode, symbol))
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception:
        return (None, None, "", None)
    if not isinstance(payload, dict):
        return (None, None, "", None)
    payload_exchange = str(payload.get("exchange_id") or "").strip().lower()
    payload_mode = str(payload.get("run_mode") or "").strip().lower()
    payload_symbol = sanitize_symbol_for_chart_state(str(payload.get("symbol") or ""))
    want_exchange = str(exchange_id or "").strip().lower()
    want_mode = str(run_mode or "").strip().lower()
    want_symbol = sanitize_symbol_for_chart_state(str(symbol or ""))
    if payload_exchange != want_exchange or payload_mode != want_mode or payload_symbol != want_symbol:
        _chart_state_log_persisted_snapshot_skip(symbol, reason="mismatch")
        return (None, None, "", None)
    current_price = _chart_state_float(payload.get("current_price"))
    if current_price is None or current_price <= 0.0:
        _chart_state_log_persisted_snapshot_skip(symbol, reason="missing_current_price")
        return (None, None, "", None)
    updated_at_ms = _chart_state_parse_iso_to_ms(payload.get("updated_at"))
    if updated_at_ms is None:
        _chart_state_log_persisted_snapshot_skip(symbol, reason="updated_at_parse_failed")
        return (None, None, "", None)
    now_ms = int(time.time() * 1000)
    age_ms = int(now_ms - updated_at_ms)
    if age_ms < -60_000:
        _chart_state_log_persisted_snapshot_skip(symbol, reason="future_updated_at", age_ms=age_ms)
        return (None, None, "", None)
    max_age_ms = _chart_state_persisted_snapshot_max_age_ms(str(timeframe or "5m"))
    if age_ms > max_age_ms:
        _chart_state_log_persisted_snapshot_skip(symbol, reason="stale", age_ms=age_ms)
        return (None, None, "", int(age_ms))
    current_price = _chart_state_float(payload.get("current_price"))
    if current_price is not None and current_price > 0.0:
        return (
            float(current_price),
            int(max(0, int(fallback_ts_ms or 0))),
            "persisted_chart_state_current_price",
            int(max(0, age_ms)),
        )
    candles = list(payload.get("candles") or [])
    if candles:
        last = candles[-1] if isinstance(candles[-1], dict) else {}
        close_value = _chart_state_float((last or {}).get("close"))
        if close_value is not None and close_value > 0.0:
            return (
                float(close_value),
                int(max(0, int(fallback_ts_ms or 0))),
                "persisted_chart_state_last_close",
                int(max(0, age_ms)),
            )
    _chart_state_log_persisted_snapshot_skip(symbol, reason="missing_close_after_fresh", age_ms=age_ms)
    return (None, None, "", int(age_ms))


def _pick_live_chart_seed_price(
    symbol: str,
    *,
    ex: Any,
    store: StateStore,
    state_root: str,
    exchange_id: str,
    run_mode: str,
    timeframe: str,
    fallback_ts_ms: int,
    entry_ohlcv: dict[str, Any] | None = None,
    trace_bar_snap: dict[str, Any] | None = None,
    best_bid: float | None = None,
    best_ask: float | None = None,
    close_last: float | None = None,
) -> tuple[float | None, int | None, str, int | None]:
    def _return_tapped(price: Any, ts_ms: Any, source: str, age_ms: int | None = None) -> tuple[float | None, int | None, str, int | None]:
        price_norm = _chart_state_float(price)
        if price_norm is None or price_norm <= 0.0:
            return (None, None, "", None)
        ts_norm = _chart_state_int(ts_ms)
        if ts_norm is None or ts_norm <= 0:
            ts_norm = int(max(0, int(fallback_ts_ms or 0)))
        source_text = str(source or "").strip()
        if not source_text:
            return (None, None, "", None)
        if not source_text.startswith("runtime_tap_"):
            source_text = f"runtime_tap_{source_text}"
        _chart_state_remember_runtime_tapped_price(
            symbol,
            price=float(price_norm),
            ts_ms=int(ts_norm),
            source=str(source_text),
            age_ms=age_ms,
        )
        return (float(price_norm), int(ts_norm), str(source_text), age_ms)

    price_value, ts_value = _chart_state_last_price_from_ohlcv(entry_ohlcv)
    if price_value is not None and ts_value is not None:
        return _return_tapped(float(price_value), int(ts_value), "entry_close_last", None)

    if isinstance(trace_bar_snap, dict):
        price_value = _chart_state_float(trace_bar_snap.get("bar_close"))
        ts_value = _chart_state_int(trace_bar_snap.get("bar_ts_ms"))
        if price_value is not None and ts_value is not None:
            return _return_tapped(float(price_value), int(ts_value), "confirmed_bar_close", None)

    for key in _chart_state_symbol_keys(symbol):
        snap = _DIFF_TRACE_BAR_BY_SYMBOL.get(key)
        if not isinstance(snap, dict):
            continue
        price_value = _chart_state_float(snap.get("bar_close"))
        ts_value = _chart_state_int(snap.get("bar_ts_ms"))
        if price_value is not None and ts_value is not None:
            return _return_tapped(float(price_value), int(ts_value), "diff_trace_bar_close", None)

    bid_value = _chart_state_float(best_bid)
    ask_value = _chart_state_float(best_ask)
    if bid_value is not None and ask_value is not None and bid_value > 0.0 and ask_value > 0.0:
        return _return_tapped((float(bid_value) + float(ask_value)) / 2.0, int(max(0, int(fallback_ts_ms or 0))), "best_bid_ask_mid", None)
    if ask_value is not None and ask_value > 0.0:
        return _return_tapped(float(ask_value), int(max(0, int(fallback_ts_ms or 0))), "best_ask", None)
    if bid_value is not None and bid_value > 0.0:
        return _return_tapped(float(bid_value), int(max(0, int(fallback_ts_ms or 0))), "best_bid", None)

    price_value, ts_value, source_text, source_age_ms = _chart_state_pick_in_memory_live_price(
        ex,
        symbol,
        timeframe=str(timeframe or "5m"),
        fallback_ts_ms=int(max(0, int(fallback_ts_ms or 0))),
    )
    if price_value is not None and ts_value is not None and str(source_text or "").strip():
        return (float(price_value), int(ts_value), str(source_text), source_age_ms)

    close_value = _chart_state_float(close_last)
    if close_value is not None and close_value > 0.0:
        return _return_tapped(float(close_value), int(max(0, int(fallback_ts_ms or 0))), "close_last", None)

    try:
        pos = store.get_position(symbol)
    except Exception:
        pos = None
    if isinstance(pos, dict):
        entry_value = _chart_state_float(pos.get("entry"))
        if entry_value is not None and entry_value > 0.0:
            return _return_tapped(float(entry_value), int(max(0, int(fallback_ts_ms or 0))), "position_entry_price", None)

    last_success_cache = None
    internal_bootstrap_cache = None
    exchange_cache = None
    for key in _chart_state_symbol_keys(symbol):
        if last_success_cache is None and isinstance(_LIVE_CHART_OHLCV_BY_SYMBOL.get(key), dict):
            last_success_cache = _LIVE_CHART_OHLCV_BY_SYMBOL.get(key)
        if internal_bootstrap_cache is None and isinstance(_LIVE_CHART_INTERNAL_BOOTSTRAP_BY_SYMBOL.get(key), dict):
            internal_bootstrap_cache = _LIVE_CHART_INTERNAL_BOOTSTRAP_BY_SYMBOL.get(key)
        if exchange_cache is None and isinstance(_LIVE_CHART_EXCHANGE_OHLCV_BY_SYMBOL.get(key), dict):
            exchange_cache = _LIVE_CHART_EXCHANGE_OHLCV_BY_SYMBOL.get(key)
    for cache_name, cache_map in (
        ("seed_builder_partial_close", _chart_state_seed_builder_ohlcv(symbol)),
        ("last_success_cache_close", last_success_cache),
        ("internal_bootstrap_close", internal_bootstrap_cache),
        ("exchange_cache_close", exchange_cache),
    ):
        price_value, ts_value = _chart_state_last_price_from_ohlcv(cache_map if isinstance(cache_map, dict) else None)
        if price_value is not None:
            use_ts = ts_value if ts_value is not None and str(cache_name).startswith("seed_builder_") else int(max(0, int(fallback_ts_ms or ts_value or 0)))
            return (float(price_value), int(use_ts), str(cache_name), None)

    return _chart_state_persisted_snapshot_price(
        state_root=str(state_root),
        exchange_id=str(exchange_id),
        run_mode=str(run_mode),
        symbol=str(symbol),
        fallback_ts_ms=int(max(0, int(fallback_ts_ms or 0))),
        timeframe=str(timeframe or "5m"),
    )


def _observe_live_chart_guaranteed_price(
    symbol: str,
    *,
    ex: Any,
    timeframe: str,
    store: StateStore,
    state_root: str,
    exchange_id: str,
    run_mode: str,
    fallback_ts_ms: int,
    entry_ohlcv: dict[str, Any] | None = None,
    trace_bar_snap: dict[str, Any] | None = None,
    best_bid: float | None = None,
    best_ask: float | None = None,
    close_last: float | None = None,
) -> bool:
    price_value, ts_value, source_text, source_age_ms = _pick_live_chart_seed_price(
        symbol,
        ex=ex,
        store=store,
        state_root=str(state_root),
        exchange_id=str(exchange_id),
        run_mode=str(run_mode),
        timeframe=str(timeframe or "5m"),
        fallback_ts_ms=int(max(0, int(fallback_ts_ms or 0))),
        entry_ohlcv=entry_ohlcv,
        trace_bar_snap=trace_bar_snap,
        best_bid=best_bid,
        best_ask=best_ask,
        close_last=close_last,
    )
    if price_value is None or ts_value is None or not str(source_text or "").strip():
        return False
    return _chart_state_seed_builder_observe(
        symbol,
        str(timeframe or "5m"),
        ts_ms=int(ts_value),
        price=float(price_value),
        source=str(source_text),
        source_age_ms=source_age_ms,
    )


def _chart_state_try_seed_builder(symbol: str, timeframe: str) -> bool:
    payload = _chart_state_seed_builder_ohlcv(symbol)
    if not isinstance(payload, dict):
        return False
    seed_price_source = ""
    seed_price_source_age_ms = None
    for key in _chart_state_symbol_keys(symbol):
        seed_price_source = str(_LIVE_CHART_SEED_PRICE_SOURCE_BY_SYMBOL.get(key) or "").strip()
        if key in _LIVE_CHART_SEED_PRICE_SOURCE_AGE_MS_BY_SYMBOL:
            seed_price_source_age_ms = int(_LIVE_CHART_SEED_PRICE_SOURCE_AGE_MS_BY_SYMBOL.get(key) or 0)
        if seed_price_source:
            break
    _chart_state_cache_ohlcv(
        symbol,
        payload,
        source="runner_seed_builder",
        reason=_chart_state_seed_reason_from_source(seed_price_source),
        seed_price_source=seed_price_source,
        seed_price_source_age_ms=seed_price_source_age_ms,
    )
    return True


def _chart_state_append_internal_bootstrap_bar(
    symbol: str,
    *,
    ts_ms: Any,
    open_px: Any,
    high_px: Any,
    low_px: Any,
    close_px: Any,
) -> bool:
    ts_value = _chart_state_int(ts_ms)
    open_value = _chart_state_float(open_px)
    high_value = _chart_state_float(high_px)
    low_value = _chart_state_float(low_px)
    close_value = _chart_state_float(close_px)
    if ts_value is None or open_value is None or high_value is None or low_value is None or close_value is None:
        return False
    hi_value = max(float(open_value), float(high_value), float(low_value), float(close_value))
    lo_value = min(float(open_value), float(high_value), float(low_value), float(close_value))
    existing: dict[str, list[float]] | None = None
    for key in _chart_state_symbol_keys(symbol):
        cached = _LIVE_CHART_INTERNAL_BOOTSTRAP_BY_SYMBOL.get(key)
        if isinstance(cached, dict):
            existing = cached
            break
    payload = {
        "timestamp": list((existing or {}).get("timestamp") or []),
        "open": list((existing or {}).get("open") or []),
        "high": list((existing or {}).get("high") or []),
        "low": list((existing or {}).get("low") or []),
        "close": list((existing or {}).get("close") or []),
        "volume": list((existing or {}).get("volume") or []),
    }
    if payload["timestamp"] and int(payload["timestamp"][-1]) == int(ts_value):
        payload["open"][-1] = float(open_value)
        payload["high"][-1] = float(hi_value)
        payload["low"][-1] = float(lo_value)
        payload["close"][-1] = float(close_value)
        if payload["volume"]:
            payload["volume"][-1] = 0.0
    else:
        payload["timestamp"].append(int(ts_value))
        payload["open"].append(float(open_value))
        payload["high"].append(float(hi_value))
        payload["low"].append(float(lo_value))
        payload["close"].append(float(close_value))
        payload["volume"].append(0.0)
        if len(payload["timestamp"]) > _CHART_STATE_MAX_CANDLES:
            keep = payload["timestamp"][-_CHART_STATE_MAX_CANDLES:]
            payload["timestamp"] = keep
            payload["open"] = payload["open"][-_CHART_STATE_MAX_CANDLES:]
            payload["high"] = payload["high"][-_CHART_STATE_MAX_CANDLES:]
            payload["low"] = payload["low"][-_CHART_STATE_MAX_CANDLES:]
            payload["close"] = payload["close"][-_CHART_STATE_MAX_CANDLES:]
            payload["volume"] = payload["volume"][-_CHART_STATE_MAX_CANDLES:]
    return _chart_state_store_internal_bootstrap_ohlcv(symbol, payload)


def _chart_state_try_internal_bootstrap(symbol: str) -> bool:
    cached: dict[str, Any] | None = None
    for key in _chart_state_symbol_keys(symbol):
        raw = _LIVE_CHART_INTERNAL_BOOTSTRAP_BY_SYMBOL.get(key)
        if isinstance(raw, dict) and list(raw.get("timestamp") or []):
            cached = raw
            break
    if cached is None:
        for key in _chart_state_symbol_keys(symbol):
            idx_map = _DIFF_TRACE_BAR_INDEX_BY_SYMBOL.get(key)
            if isinstance(idx_map, dict) and idx_map:
                rows = sorted(idx_map.items())[-_CHART_STATE_MAX_CANDLES:]
                payload = {
                    "timestamp": [int(ts) for ts, _snap in rows],
                    "open": [float((_snap or {}).get("bar_open") or 0.0) for _ts, _snap in rows],
                    "high": [float((_snap or {}).get("bar_high") or 0.0) for _ts, _snap in rows],
                    "low": [float((_snap or {}).get("bar_low") or 0.0) for _ts, _snap in rows],
                    "close": [float((_snap or {}).get("bar_close") or 0.0) for _ts, _snap in rows],
                    "volume": [0.0 for _ in rows],
                }
                if _chart_state_store_internal_bootstrap_ohlcv(symbol, payload):
                    cached = payload
                    break
        if cached is None:
            for key in _chart_state_symbol_keys(symbol):
                snap = _DIFF_TRACE_BAR_BY_SYMBOL.get(key)
                if not isinstance(snap, dict):
                    continue
                if _chart_state_append_internal_bootstrap_bar(
                    symbol,
                    ts_ms=snap.get("bar_ts_ms"),
                    open_px=snap.get("bar_open"),
                    high_px=snap.get("bar_high"),
                    low_px=snap.get("bar_low"),
                    close_px=snap.get("bar_close"),
                ):
                    cached = _LIVE_CHART_INTERNAL_BOOTSTRAP_BY_SYMBOL.get(key)
                    break
    if not isinstance(cached, dict):
        return False
    _chart_state_cache_ohlcv(
        symbol,
        cached,
        source="runner_internal_bootstrap",
        reason="using_internal_runner_bootstrap",
    )
    return True


def _chart_state_use_runner_last_success_cache(symbol: str, *, reason: str = "") -> bool:
    seed_price_source = ""
    seed_price_source_age_ms = None
    for diag_key in _chart_state_symbol_keys(symbol):
        seed_price_source = str(_LIVE_CHART_SEED_PRICE_SOURCE_BY_SYMBOL.get(diag_key) or "").strip()
        if diag_key in _LIVE_CHART_SEED_PRICE_SOURCE_AGE_MS_BY_SYMBOL:
            seed_price_source_age_ms = int(_LIVE_CHART_SEED_PRICE_SOURCE_AGE_MS_BY_SYMBOL.get(diag_key) or 0)
        if seed_price_source:
            break
    for key in _chart_state_symbol_keys(symbol):
        cached = _LIVE_CHART_OHLCV_BY_SYMBOL.get(key)
        if not isinstance(cached, dict):
            continue
        ts_list = list(cached.get("timestamp") or [])
        last_candle_ts_ms = int(ts_list[-1]) if ts_list else None
        _chart_state_note_diag(
            symbol,
            source="runner_last_success_cache",
            reason=str(reason or "using_last_successful_runner_cache").strip(),
            candles_count=len(ts_list),
            last_candle_ts_ms=last_candle_ts_ms,
            seed_price_source=seed_price_source,
            seed_price_source_age_ms=seed_price_source_age_ms,
        )
        return bool(ts_list)
    return False


def _chart_state_ohlcv_for_symbol(symbol: str) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    diag: dict[str, Any] = {}
    for key in _chart_state_symbol_keys(symbol):
        cached = _LIVE_CHART_OHLCV_BY_SYMBOL.get(key)
        if isinstance(cached, dict):
            diag = dict(_LIVE_CHART_DIAG_BY_SYMBOL.get(key) or {})
            ts_list = list(cached.get("timestamp") or [])
            seed_price_source = str(_LIVE_CHART_SEED_PRICE_SOURCE_BY_SYMBOL.get(key) or "").strip()
            seed_price_source_age_ms = None
            if key in _LIVE_CHART_SEED_PRICE_SOURCE_AGE_MS_BY_SYMBOL:
                seed_price_source_age_ms = int(_LIVE_CHART_SEED_PRICE_SOURCE_AGE_MS_BY_SYMBOL.get(key) or 0)
            if not diag:
                diag = {
                    "source": "runner_last_success_cache",
                    "candles_count": len(ts_list),
                    "last_candle_ts_ms": int(ts_list[-1]) if ts_list else None,
                    "reason": "using_last_successful_runner_cache",
                    "seed_price_source": seed_price_source,
                    "seed_price_source_age_ms": seed_price_source_age_ms,
                }
            else:
                source_text = str(diag.get("source") or "").strip()
                if source_text not in ("runner_live_ohlcv", "runner_seed_builder", "runner_internal_bootstrap", "runner_last_success_cache"):
                    source_text = "runner_last_success_cache"
                diag["source"] = source_text
                diag["candles_count"] = max(int(diag.get("candles_count") or 0), len(ts_list))
                if (not str(diag.get("seed_price_source") or "").strip()) and seed_price_source:
                    diag["seed_price_source"] = seed_price_source
                if diag.get("seed_price_source_age_ms") is None and seed_price_source_age_ms is not None:
                    diag["seed_price_source_age_ms"] = seed_price_source_age_ms
                if diag.get("last_candle_ts_ms") in (None, 0) and ts_list:
                    diag["last_candle_ts_ms"] = int(ts_list[-1])
                if not str(diag.get("reason") or "").strip():
                    if source_text == "runner_last_success_cache":
                        diag["reason"] = "using_last_successful_runner_cache"
                    elif source_text == "runner_seed_builder":
                        diag["reason"] = _chart_state_seed_reason_from_source(str(diag.get("seed_price_source") or "").strip())
                    elif source_text == "runner_internal_bootstrap":
                        diag["reason"] = "using_internal_runner_bootstrap"
            return cached, diag
    for key in _chart_state_symbol_keys(symbol):
        raw = _LIVE_CHART_EXCHANGE_OHLCV_BY_SYMBOL.get(key)
        if isinstance(raw, dict):
            ts_list = list(raw.get("timestamp") or [])
            return raw, {
                "source": "exchange_cache",
                "candles_count": len(ts_list),
                "last_candle_ts_ms": int(ts_list[-1]) if ts_list else None,
                "reason": "",
            }
    for key in _chart_state_symbol_keys(symbol):
        diag = dict(_LIVE_CHART_DIAG_BY_SYMBOL.get(key) or {})
        if diag:
            return None, diag
    return None, {"source": "empty", "candles_count": 0, "last_candle_ts_ms": None, "reason": "no_recent_ohlcv"}


def _chart_state_cached_ohlcv_from_exchange(ex: Any, symbol: str, timeframe: str) -> dict[str, Any] | None:
    getter = getattr(ex, "_coincheck_get_cached_ohlcv", None)
    if callable(getter):
        try:
            rows = getter(symbol, timeframe, limit=_CHART_STATE_MAX_CANDLES)
            if isinstance(rows, list) and rows:
                return ohlcv_to_dict(rows)
        except Exception:
            pass
    cache_map = getattr(ex, "_coincheck_ohlcv_5m_by_symbol", None)
    if isinstance(cache_map, dict):
        try:
            bars = cache_map.get(str(symbol)) or cache_map.get(str(symbol).replace("/", ""))
            if isinstance(bars, dict) and bars:
                rows = [list(bars[key]) for key in sorted(bars.keys())[-_CHART_STATE_MAX_CANDLES:]]
                if rows:
                    return ohlcv_to_dict(rows)
        except Exception:
            pass
    return None


def _chart_state_candles_from_ohlcv(ohlcv: dict[str, Any] | None) -> list[dict[str, float | int]]:
    if not isinstance(ohlcv, dict):
        return []
    ts = list(ohlcv.get("timestamp") or [])
    op = list(ohlcv.get("open") or [])
    hi = list(ohlcv.get("high") or [])
    lo = list(ohlcv.get("low") or [])
    cl = list(ohlcv.get("close") or [])
    count = min(len(ts), len(op), len(hi), len(lo), len(cl))
    if count <= 0:
        return []
    start = max(0, count - _CHART_STATE_MAX_CANDLES)
    out: list[dict[str, float | int]] = []
    for index in range(start, count):
        ts_ms = _chart_state_int(ts[index])
        open_px = _chart_state_float(op[index])
        high_px = _chart_state_float(hi[index])
        low_px = _chart_state_float(lo[index])
        close_px = _chart_state_float(cl[index])
        if ts_ms is None or open_px is None or high_px is None or low_px is None or close_px is None:
            continue
        high_norm = max(float(open_px), float(high_px), float(low_px), float(close_px))
        low_norm = min(float(open_px), float(high_px), float(low_px), float(close_px))
        out.append(
            {
                "ts_ms": int(ts_ms),
                "open": float(open_px),
                "high": float(high_norm),
                "low": float(low_norm),
                "close": float(close_px),
            }
        )
    return out


def _chart_state_position_payload(store: StateStore, symbol: str, current_price: float | None) -> dict[str, Any]:
    try:
        pos = store.get_position(symbol)
    except Exception:
        pos = None
    if not isinstance(pos, dict):
        return {
            "is_open": False,
            "side": "",
            "entry_price": None,
            "qty": None,
            "stop_price": None,
            "tp_price": None,
            "unrealized_pnl": None,
            "opened_ts_ms": None,
        }
    entry_price = _chart_state_float(pos.get("entry"))
    qty = _chart_state_float(pos.get("qty"))
    stop_price = _chart_state_float(pos.get("stop"))
    tp_price = _chart_state_float(pos.get("take_profit") or pos.get("tp"))
    opened_ts_ms = _chart_state_int(pos.get("candle_ts_open"))
    side = str(pos.get("side") or pos.get("direction") or pos.get("dir") or "long").strip().lower() or "long"
    unrealized = None
    if current_price is not None and entry_price is not None and qty is not None:
        if side == "short":
            unrealized = (float(entry_price) - float(current_price)) * float(qty)
        else:
            unrealized = (float(current_price) - float(entry_price)) * float(qty)
    return {
        "is_open": True,
        "side": str(side),
        "entry_price": entry_price,
        "qty": qty,
        "stop_price": stop_price,
        "tp_price": tp_price,
        "unrealized_pnl": unrealized,
        "opened_ts_ms": opened_ts_ms,
    }


def _chart_state_markers_payload(symbol: str, position_payload: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    bucket = _chart_state_bucket(symbol)
    entries = list(bucket.get("entries") or [])
    exits = list(bucket.get("exits") or [])
    if bool(position_payload.get("is_open")):
        entry_price = _chart_state_float(position_payload.get("entry_price"))
        opened_ts_ms = _chart_state_int(position_payload.get("opened_ts_ms"))
        side = str(position_payload.get("side") or "long").strip().lower() or "long"
        if entry_price is not None and opened_ts_ms is not None:
            exists = any(
                int(item.get("ts_ms") or 0) == int(opened_ts_ms) and abs(float(item.get("price") or 0.0) - float(entry_price)) <= 1e-12
                for item in entries
            )
            if not exists:
                entries.append(
                    {
                        "ts_ms": int(opened_ts_ms),
                        "price": float(entry_price),
                        "side": str(side),
                        "label": "OPEN",
                    }
                )
    if len(entries) > _CHART_STATE_MAX_MARKERS:
        entries = entries[-_CHART_STATE_MAX_MARKERS:]
    if len(exits) > _CHART_STATE_MAX_MARKERS:
        exits = exits[-_CHART_STATE_MAX_MARKERS:]
    return {"entries": entries, "exits": exits}


def _emit_live_chart_states(store: StateStore, symbols: list[str], *, run_mode: str, timeframe: str) -> None:
    mode_text = str(run_mode or "").strip().upper()
    if mode_text not in ("LIVE", "PAPER"):
        return
    state_root = Path(STATE_DIR)
    state_root.mkdir(parents=True, exist_ok=True)
    exchange_id = str(_resolve_exchange_id() or "").strip().lower() or "exchange"
    tf_text = str(timeframe or getattr(C, "ENTRY_TF", "5m") or "5m").strip() or "5m"
    updated_at = datetime.now(timezone.utc).isoformat()
    for symbol in list(symbols or []):
        ohlcv, diag = _chart_state_ohlcv_for_symbol(symbol)
        candles = _chart_state_candles_from_ohlcv(ohlcv)
        candles_count = max(int(diag.get("candles_count") or 0), len(candles))
        last_candle_ts_ms = _chart_state_int(diag.get("last_candle_ts_ms"))
        if last_candle_ts_ms is None and candles:
            last_candle_ts_ms = _chart_state_int(candles[-1].get("ts_ms"))
        candle_source = str(diag.get("source") or "").strip() or ("runner_last_success_cache" if candles else "empty")
        chart_state_reason = str(diag.get("reason") or "").strip()
        seed_price_source = str(diag.get("seed_price_source") or "").strip()
        seed_price_source_age_ms = _chart_state_int(diag.get("seed_price_source_age_ms"))
        if (not chart_state_reason) and (not candles):
            chart_state_reason = "candles_not_ready"
        current_price = _chart_state_float(candles[-1].get("close")) if candles else None
        position_payload = _chart_state_position_payload(store, str(symbol), current_price)
        markers = _chart_state_markers_payload(str(symbol), position_payload)
        payload = {
            "schema_version": "2.0.9",
            "updated_at": str(updated_at),
            "exchange_id": str(exchange_id),
            "run_mode": str(mode_text),
            "symbol": str(symbol),
            "timeframe": str(tf_text),
            "candles": candles,
            "candles_count": int(candles_count),
            "candle_source": str(candle_source),
            "last_candle_ts_ms": last_candle_ts_ms,
            "chart_state_reason": str(chart_state_reason),
            "seed_price_source": str(seed_price_source),
            "seed_price_source_age_ms": seed_price_source_age_ms,
            "current_price": current_price,
            "position": position_payload,
            "markers": markers,
            "view_hint": {"preferred_mode": "Candle"},
        }
        path = Path(build_chart_state_path(str(state_root), exchange_id, mode_text, symbol))
        tmp_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
        try:
            with open(tmp_path, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False, indent=2)
            os.replace(tmp_path, path)
            log_key = f"{exchange_id}:{mode_text.lower()}:{_chart_state_symbol_token(symbol)}"
            path_text = str(path.resolve())
            log_state = (
                path_text,
                str(candle_source),
                int(candles_count),
                str(chart_state_reason),
                str(seed_price_source),
                str(seed_price_source_age_ms if seed_price_source_age_ms is not None else ""),
            )
            if _LIVE_CHART_WRITER_LOGGED_PATHS.get(log_key) != log_state:
                msg = f"[chart_state] writer path={path_text} source={candle_source} candles={int(candles_count)}"
                if str(chart_state_reason):
                    msg += f" reason={chart_state_reason}"
                if str(seed_price_source):
                    msg += f" seed={seed_price_source}"
                if seed_price_source_age_ms is not None:
                    msg += f" age_ms={int(seed_price_source_age_ms)}"
                logger.info(msg)
                _LIVE_CHART_WRITER_LOGGED_PATHS[log_key] = log_state
        except Exception:
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:
                pass

_PARITY_PROBE_PATH = _resolve_runtime_log_path(
    getattr(C, "PARITY_PROBE_LOG_FILE", ""),
    "parity_probe_week_20230314_20230321.jsonl",
)
_RUNTIME_LOG_LEVEL_MAP: dict[str, int] = {"MINIMAL": 0, "OPS": 1, "DEBUG": 2}
_RUNTIME_LOG_OPS_MARKERS: tuple[str, ...] = (
    "[SIGNAL]",
    "[HOLD]",
    "[TTL_STATS]",
    "[CFG_EFFECTIVE][RANGE]",
)
_RUNTIME_LOG_DEBUG_MARKERS: tuple[str, ...] = (
    "[LATEST_BAR]",
    "[COINCHECK][OHLCV_DIAG]",
    "[COINCHECK][TRADES_PAGING]",
    "[COINCHECK][REST_POLL]",
    "[COINCHECK][BITBANK_SEED]",
    "[COINCHECK][WS]",
    "[COINCHECK][WS_DIAG]",
    "[COINCHECK][OHLCV_FALLBACK]",
)


class _ReplayPositionClosedSameBar(Exception):
    """Internal sentinel to exit position management and allow replay same-bar re-entry."""


def _same_bar_reentry_allowed_exit_reason(exit_reason: str) -> bool:
    reason = str(exit_reason or "")
    if not reason:
        return False
    if reason in ("RANGE_EARLY_LOSS_ATR", "RANGE_EMA9_CROSS_EXIT"):
        return True
    if reason.startswith("RANGE_TIMEOUT(") or reason.startswith("TREND_TIMEOUT("):
        return True
    return False

def _parity_probe_enabled() -> bool:
    return bool(_env_flag("PARITY_PROBE_ENABLED", default=False))


def _parity_probe_symbol_ok(symbol: str) -> bool:
    raw = str(os.getenv("PARITY_PROBE_SYMBOLS", "BTC/JPY") or "").strip()
    allow = {str(x).strip() for x in raw.replace(";", ",").split(",") if str(x).strip()}
    if not allow:
        return True
    sym = str(symbol or "").strip()
    sym_flat = sym.replace("/", "")
    return (sym in allow) or (sym_flat in {a.replace("/", "") for a in allow})


def _parity_probe_week_ok(ts_ms: int) -> bool:
    try:
        dt = datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc)
        return datetime(2023, 3, 14, tzinfo=timezone.utc) <= dt <= datetime(2023, 3, 21, 23, 59, 59, tzinfo=timezone.utc)
    except Exception:
        return False


def _parity_probe_write(event: str, payload: dict[str, Any]) -> None:
    if not _parity_probe_enabled():
        return
    try:
        ts_ms = int(payload.get("replay_ts_ms") or payload.get("ts_ms_now") or payload.get("candle_ts_run") or 0)
    except Exception:
        ts_ms = 0
    symbol = str(payload.get("symbol") or "")
    if ts_ms <= 0:
        return
    if not _parity_probe_week_ok(ts_ms):
        return
    if symbol and (not _parity_probe_symbol_ok(symbol)):
        return
    try:
        os.makedirs(os.path.dirname(_PARITY_PROBE_PATH), exist_ok=True)
        rec = {
            "event": str(event),
            "build_id": str(BUILD_ID),
            "ts_iso": datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat(),
        }
        rec.update(payload or {})
        with open(_PARITY_PROBE_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False, sort_keys=True) + "\n")
    except Exception:
        return


def _normalize_runtime_log_level(raw: Any) -> str:
    value = str(raw or "").strip().upper()
    if value in _RUNTIME_LOG_LEVEL_MAP:
        return value
    return "OPS"


def _apply_runtime_log_level(raw: Any) -> str:
    value = _normalize_runtime_log_level(raw)
    os.environ["RUNTIME_LOG_LEVEL"] = value
    setattr(C, "RUNTIME_LOG_LEVEL", value)
    return value


def _runtime_log_level_name() -> str:
    raw = os.getenv("RUNTIME_LOG_LEVEL", getattr(C, "RUNTIME_LOG_LEVEL", "OPS"))
    return _normalize_runtime_log_level(raw)


def _runtime_log_level_value() -> int:
    return int(_RUNTIME_LOG_LEVEL_MAP.get(_runtime_log_level_name(), _RUNTIME_LOG_LEVEL_MAP["OPS"]))


def _runtime_log_scope_mode(mode_name: str | None = None) -> str:
    value = str(mode_name or os.getenv("LWF_RUNTIME_MODE", "")).strip().upper()
    if value in ("LIVE", "PAPER", "REPLAY", "BACKTEST"):
        return value
    return ""


def _runtime_log_filter_active(mode_name: str | None = None) -> bool:
    return _runtime_log_scope_mode(mode_name) in ("LIVE", "PAPER", "REPLAY")


def _runtime_log_enabled(level_name: str, *, mode_name: str | None = None) -> bool:
    if not _runtime_log_filter_active(mode_name):
        return True
    target = int(_RUNTIME_LOG_LEVEL_MAP.get(_normalize_runtime_log_level(level_name), _RUNTIME_LOG_LEVEL_MAP["OPS"]))
    return int(_runtime_log_level_value()) >= int(target)


def _runtime_log_required_level(message: str) -> str | None:
    text = str(message or "")
    if not text:
        return None
    if any(marker in text for marker in _RUNTIME_LOG_DEBUG_MARKERS):
        return "DEBUG"
    if any(marker in text for marker in _RUNTIME_LOG_OPS_MARKERS):
        return "OPS"
    if text.startswith("[") and " action=hold reason=" in text:
        return "OPS"
    return None


class _RuntimeLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if int(getattr(record, "levelno", logging.INFO)) >= int(logging.ERROR):
            return True
        try:
            required = _runtime_log_required_level(record.getMessage())
        except Exception:
            return True
        if required is None:
            return True
        return bool(_runtime_log_enabled(required))


def _install_runtime_log_filter() -> None:
    runtime_filter = _RuntimeLogFilter()
    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        if not any(isinstance(f, _RuntimeLogFilter) for f in handler.filters):
            handler.addFilter(runtime_filter)


def _setup_logging() -> tuple[logging.Logger, logging.Logger, logging.Logger]:
    ensure_runtime_dirs()
    level = getattr(logging, str(getattr(C, "LOG_LEVEL", "INFO")).upper(), logging.INFO)
    fmt = "%(asctime)s %(levelname)s %(name)s %(message)s"
    logging.basicConfig(level=level, format=fmt, stream=sys.stdout, force=True)

    class _SuppressTradeStdoutFilter(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            msg = record.getMessage()
            return (" OPEN " not in msg) and (" CLOSE " not in msg) and ("[BE]" not in msg)

    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        if not any(isinstance(f, _SuppressTradeStdoutFilter) for f in handler.filters):
            handler.addFilter(_SuppressTradeStdoutFilter())
    _install_runtime_log_filter()

    def _ensure_rotating_handler(logger_obj: logging.Logger, path: str, *, handler_level: int) -> None:
        path_abs = os.path.abspath(path)
        os.makedirs(os.path.dirname(path_abs), exist_ok=True)
        for handler in logger_obj.handlers:
            base_filename = getattr(handler, "baseFilename", None)
            if base_filename and os.path.abspath(str(base_filename)) == path_abs:
                return
        handler = RotatingFileHandler(
            path_abs,
            maxBytes=5 * 1024 * 1024,
            backupCount=10,
            encoding="utf-8",
        )
        handler.setLevel(handler_level)
        handler.setFormatter(logging.Formatter(fmt))
        logger_obj.addHandler(handler)

    ops_logger = logging.getLogger("runner")
    trade_log = logging.getLogger("trade")
    err_logger = logging.getLogger("error")

    ops_logger.setLevel(level)
    trade_log.setLevel(logging.INFO)
    err_logger.setLevel(logging.ERROR)

    ops_logger.propagate = True
    trade_log.propagate = False
    err_logger.propagate = False

    _ensure_rotating_handler(
        ops_logger,
        _resolve_runtime_log_path(
            getattr(C, "OPS_LOG_FILE", getattr(C, "RUNTIME_LOG_FILE", "")),
            "ops.log",
        ),
        handler_level=level,
    )
    _ensure_rotating_handler(
        trade_log,
        _resolve_runtime_log_path(getattr(C, "TRADE_LOG_FILE", ""), "trade.log"),
        handler_level=logging.INFO,
    )
    _ensure_rotating_handler(
        err_logger,
        _resolve_runtime_log_path(getattr(C, "ERROR_LOG_FILE", ""), "error.log"),
        handler_level=logging.ERROR,
    )
    runtime_filter = _RuntimeLogFilter()
    for handler in ops_logger.handlers:
        if not any(isinstance(f, _RuntimeLogFilter) for f in handler.filters):
            handler.addFilter(runtime_filter)
    return ops_logger, trade_log, err_logger


def _log_trade_event(message: str) -> None:
    logger.info(message)
    trade_logger.info(message)


def _resolve_exchange_id() -> str:
    return (os.getenv("LWF_EXCHANGE_ID") or getattr(C, "EXCHANGE_ID", "mexc")).strip().lower() or "mexc"


def _resolve_mode_override_env() -> str:
    raw = str(os.getenv("LWF_MODE_OVERRIDE", "") or "").strip().upper()
    if raw in ("LIVE", "PAPER", "BACKTEST"):
        return raw
    if raw == "REPLAY":
        return "BACKTEST"
    return ""


def _default_symbol_for_runtime(exchange_id: str | None = None) -> str:
    ex = str(exchange_id or _resolve_exchange_id()).strip().lower()
    return str(default_symbol_for_exchange(ex, fallback="BTC/USDT") or "BTC/USDT")


def _default_quote_for_runtime(exchange_id: str | None = None) -> str:
    ex = str(exchange_id or _resolve_exchange_id()).strip().lower()
    return str(default_quote_for_exchange(ex, fallback="USDT") or "USDT").strip().upper()


def _symbols_from_env() -> list[str]:
    raw = str(os.getenv("BOT_SYMBOLS", "") or "").strip()
    if not raw:
        raw = str(os.getenv("BOT_SYMBOL", "") or "").strip()
    if not raw:
        return []
    exchange_id = _resolve_exchange_id()
    out: list[str] = []
    for tok in raw.replace(";", ",").split(","):
        sym = normalize_runtime_symbol(
            str(tok or "").strip(),
            exchange_id=exchange_id,
            fallback="",
        )
        if sym and sym not in out:
            out.append(sym)
    return out


def _normalize_symbol_values(values: list[str], *, exchange_id: str = "", fallback: str = "") -> list[str]:
    exchange_text = str(exchange_id or _resolve_exchange_id()).strip().lower()
    out: list[str] = []
    for value in values:
        symbol_text = normalize_runtime_symbol(
            str(value or "").strip(),
            exchange_id=exchange_text,
            fallback=fallback,
        )
        if symbol_text and symbol_text not in out:
            out.append(symbol_text)
    return out


def _cli_symbol_values(args: argparse.Namespace) -> tuple[list[str], str]:
    raw_symbol = str(getattr(args, "symbol", "") or "").strip()
    if raw_symbol:
        return (_normalize_symbol_values([raw_symbol], fallback=""), "cli")

    raw_symbols = str(getattr(args, "symbols", "") or "").strip()
    if raw_symbols:
        return (_normalize_symbol_values(raw_symbols.split(","), fallback=""), "cli_symbols")

    return ([], "")


def _resolve_replay_symbols(args: argparse.Namespace) -> tuple[list[str], str]:
    cached_symbols = getattr(args, "_resolved_replay_symbols", None)
    cached_source = str(getattr(args, "_resolved_replay_symbol_source", "") or "").strip()
    if isinstance(cached_symbols, list) and cached_symbols:
        return (list(cached_symbols), cached_source or "cached")

    exchange_id = _resolve_exchange_id()
    default_symbol = normalize_runtime_symbol(
        _default_symbol_for_runtime(exchange_id),
        exchange_id=exchange_id,
        fallback=_default_symbol_for_runtime(exchange_id),
    )
    fallback_symbol = normalize_runtime_symbol(
        str(getattr(C, "BACKTEST_CSV_SYMBOL", "") or default_symbol),
        exchange_id=exchange_id,
        fallback=default_symbol,
    )

    cli_symbols, cli_source = _cli_symbol_values(args)
    if cli_symbols:
        resolved_symbols = list(cli_symbols)
        resolved_source = str(cli_source or "cli")
    else:
        dataset_symbol, dataset_source = resolve_dataset_override_symbol(
            exchange_id=exchange_id,
            fallback=fallback_symbol,
        )
        if dataset_symbol:
            resolved_symbols = [dataset_symbol]
            resolved_source = str(dataset_source)
        else:
            env_symbols = _symbols_from_env()
            if env_symbols:
                resolved_symbols = list(env_symbols)
                resolved_source = "env"
            else:
                cfg_symbols = _normalize_symbol_values(list(getattr(C, "SYMBOLS", []) or []), exchange_id=exchange_id, fallback="")
                if cfg_symbols:
                    resolved_symbols = list(cfg_symbols)
                    resolved_source = "config.SYMBOLS"
                elif fallback_symbol:
                    resolved_symbols = [fallback_symbol]
                    resolved_source = "config.BACKTEST_CSV_SYMBOL"
                else:
                    resolved_symbols = [default_symbol]
                    resolved_source = "default"

    setattr(args, "_resolved_replay_symbols", list(resolved_symbols))
    setattr(args, "_resolved_replay_symbol_source", str(resolved_source))
    return (list(resolved_symbols), str(resolved_source))


def _resolve_export_run_id(explicit: str | None = None) -> str:
    return str(resolve_run_id(explicit, env_key="LWF_RUN_ID"))


def _activate_export_context(*, run_id: str, symbol: str, mode: str) -> str:
    global _CURRENT_EXPORT_RUN_ID, _CURRENT_EXPORT_SYMBOL, _CURRENT_EXPORT_MODE, _CURRENT_EXPORT_DIR
    paths = ensure_runtime_dirs()
    rid = _resolve_export_run_id(run_id)
    sym = str(symbol or _default_symbol_for_runtime()).strip() or _default_symbol_for_runtime()
    export_dir = build_run_export_dir(paths.exports_dir, run_id=rid, symbol=sym)
    _CURRENT_EXPORT_RUN_ID = str(rid)
    _CURRENT_EXPORT_SYMBOL = str(sym)
    _CURRENT_EXPORT_MODE = str(mode or "")
    _CURRENT_EXPORT_DIR = str(export_dir)
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


def _resolve_replay_state_dir(run_id: str) -> str:
    root = STATE_DIR / "replay_runs" / resolve_run_id(f"{_resolve_export_run_id(run_id)}_{os.getpid()}_{time.time_ns()}")
    root.mkdir(parents=True, exist_ok=True)
    return str(root)


def _current_export_dir() -> str:
    if str(_CURRENT_EXPORT_DIR or "").strip():
        return str(_CURRENT_EXPORT_DIR)
    mode = str(getattr(C, "MODE", "PAPER") or "PAPER").strip().upper()
    symbols = _symbols_from_env() or list(getattr(C, "SYMBOLS", []) or [])
    sym = str(symbols[0] if symbols else _default_symbol_for_runtime())
    run_id = _resolve_export_run_id("")
    return _activate_export_context(run_id=run_id, symbol=sym, mode=mode)


def _export_path(*parts: str) -> str:
    base = Path(_current_export_dir())
    out = base.joinpath(*[str(p) for p in parts if str(p)])
    out.parent.mkdir(parents=True, exist_ok=True)
    return str(out)


def _write_last_run_reference(*, replay_report: str = "", trade_log: str = "", extra: dict[str, Any] | None = None) -> str:
    paths = ensure_runtime_dirs()
    out = write_last_run_json(
        paths.exports_dir,
        run_id=str(_CURRENT_EXPORT_RUN_ID or _resolve_export_run_id("")),
        symbol=str(_CURRENT_EXPORT_SYMBOL or _default_symbol_for_runtime()),
        mode=str(_CURRENT_EXPORT_MODE or getattr(C, "MODE", "PAPER")),
        export_dir=str(_current_export_dir()),
        replay_report=str(replay_report or ""),
        trade_log=str(trade_log or ""),
        extra=extra,
    )
    logger.info("[results] last_run=%s", str(out))
    return str(out)


def _resolved_paper_fee_pair() -> tuple[float, float]:
    exchange_id = _resolve_exchange_id()
    maker, taker = resolve_paper_fees(exchange_id)
    maker = float(maker)
    taker = float(taker)
    if exchange_id == "coincheck":
        if taker < 0.0:
            taker = 0.0
        if maker < 0.0:
            maker = taker
        return (maker, taker)
    if taker <= 0.0:
        taker = 0.0002
    if maker <= 0.0:
        maker = taker
    return (maker, taker)


def _quote_ccy_for_symbol(symbol: str | None) -> str:
    quote = resolve_quote_ccy(str(symbol or ""))
    if not quote:
        quote = quote_for_symbol(str(symbol or ""), fallback="")
    if quote:
        return quote
    fallback = str(_LIVE_EQUITY_LAST_CUR or "").strip().upper()
    return fallback or _default_quote_for_runtime()


def _quote_ccy_from_symbol_no_fallback(symbol: str | None) -> str:
    raw = str(symbol or "").strip()
    if not raw:
        return ""
    quote = resolve_quote_ccy(raw)
    if not quote:
        quote = quote_for_symbol(raw, fallback="")
    return _normalize_ccy_key(quote)


def _market_meta_cache_ttl_sec() -> float:
    try:
        return max(60.0, float(os.getenv("MARKET_META_CACHE_TTL_SEC", "3600") or 3600.0))
    except Exception:
        return 3600.0


def _apply_market_meta_runtime(meta: MarketMeta, exchange_id: str) -> None:
    setattr(C, "MARKET_META_MAKER_FEE_RATE", float(meta.maker_fee_rate))
    setattr(C, "MARKET_META_TAKER_FEE_RATE", float(meta.taker_fee_rate))
    setattr(C, "MARKET_META_SPREAD_BPS", float(meta.spread_bps))
    if os.getenv("BACKTEST_SPREAD_BPS") in (None, ""):
        setattr(C, "BACKTEST_SPREAD_BPS", float(meta.spread_bps))
    ex = str(exchange_id or "").strip().lower()
    if ex == "coincheck":
        setattr(C, "COINCHECK_FEE_RATE_MAKER", float(meta.maker_fee_rate))
        setattr(C, "COINCHECK_FEE_RATE_TAKER", float(meta.taker_fee_rate))
    elif ex == "binance":
        setattr(C, "BINANCE_PAPER_FEE_RATE_MAKER", float(meta.maker_fee_rate))
        setattr(C, "BINANCE_PAPER_FEE_RATE_TAKER", float(meta.taker_fee_rate))
    else:
        setattr(C, "PAPER_FEE_RATE_MAKER", float(meta.maker_fee_rate))
        setattr(C, "PAPER_FEE_RATE_TAKER", float(meta.taker_fee_rate))

# ---- backtest parity helpers (trace-only) ----
def _side_slip_mult(kind: str) -> float:
    """
    Match backtest.py _side_slip_mult() (uses C.SLIPPAGE_BPS).
    We apply this ONLY to diff_trace exec fields to reduce verify_diff drift.
    """
    slip_bps = float(getattr(C, "SLIPPAGE_BPS", 0.0))
    mult = 1.0 + slip_bps / 10_000.0
    if kind in ("entry_long", "exit_short"):
        return mult  # pay up
    if kind in ("exit_long", "entry_short"):
        return 1.0 / mult  # sell lower / short entry lower
    return 1.0

def _trace_exec_with_slip(*, dryrun: bool, kind: str, px: float) -> float:
    """
    Apply slippage model only when we're simulating fills (dryrun/replay),
    so LIVE real fills are left untouched.
    """
    if not dryrun:
        return float(px)
    # Backtest parity: in REPLAY we must NOT introduce slippage drift,
    # otherwise entry_exec/exit_exec diverge and pnl/net/fee mismatch explodes.
    if str(_DIFF_TRACE_SOURCE) == "replay":
        return float(px)
    return float(px) * float(_side_slip_mult(kind))

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
    hit_tp = (side_l == "long" and float(bar_high) >= float(tp)) or (side_l == "short" and float(bar_low) <= float(tp))
    hit_sl = (side_l == "long" and float(bar_low) <= float(sl)) or (side_l == "short" and float(bar_high) >= float(sl))
    if hit_tp and not hit_sl:
        return "TP_HIT"
    if hit_sl and not hit_tp:
        return "STOP_HIT"
    if not hit_tp and not hit_sl:
        return None
    bullish = float(bar_close) >= float(bar_open)
    if side_l == "long":
        return "TP_HIT" if bullish else "STOP_HIT"
    return "STOP_HIT" if bullish else "TP_HIT"

def _env_flag(name: str, default: bool | None = None) -> bool | None:
    """Parse common truthy/falsey env var values. Returns None if unset/empty."""
    v = os.getenv(name, "")
    if v is None:
        return default
    s = str(v).strip().lower()
    if s == "":
        return default
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    # Unknown value -> keep default (do not surprise)
    return default


def _env_int(name: str, default: int) -> int:
    try:
        v = os.getenv(name, "")
        if v is None:
            return int(default)
        s = str(v).strip()
        if s == "":
            return int(default)
        return int(float(s))
    except Exception:
        return int(default)


def _symbol_base_asset(symbol: str) -> str:
    s = str(symbol or "").strip().upper()
    if "/" in s:
        return str(s.split("/", 1)[0]).strip()
    if ":" in s:
        s = str(s.split(":", 1)[0]).strip()
    for sep in ("-", "_"):
        if sep in s:
            return str(s.split(sep, 1)[0]).strip()
    return s


def _fetch_balance_snapshot(ex: Any) -> dict[str, Any] | None:
    try:
        if hasattr(ex, "fetch_balance"):
            bal = ex.fetch_balance()
            if isinstance(bal, dict):
                return bal
    except Exception:
        pass
    try:
        ex_obj = getattr(ex, "ex", None)
        if ex_obj is not None and hasattr(ex_obj, "fetch_balance"):
            if hasattr(ex, "_retry"):
                bal = ex._retry(ex_obj.fetch_balance)
            else:
                bal = ex_obj.fetch_balance()
            if isinstance(bal, dict):
                return bal
    except Exception:
        pass
    return None


def _balance_asset_qty(balance: dict[str, Any] | None, asset: str) -> float:
    if not isinstance(balance, dict):
        return 0.0
    a = str(asset or "").strip().upper()
    if not a:
        return 0.0
    try:
        total = balance.get("total", {}) or {}
        free = balance.get("free", {}) or {}
        used = balance.get("used", {}) or {}
        if isinstance(total, dict) and (a in total):
            return float(total.get(a) or 0.0)
        free_v = float((free.get(a) if isinstance(free, dict) else 0.0) or 0.0)
        used_v = float((used.get(a) if isinstance(used, dict) else 0.0) or 0.0)
        return float(free_v + used_v)
    except Exception:
        return 0.0


def _get_min_order_constraints(ex: Any, symbol: str) -> dict[str, Any]:
    min_qty = None
    min_cost = None
    qty_step_str = ""
    source = "none"

    market = None
    try:
        ex_obj = getattr(ex, "ex", None)
        markets = getattr(ex_obj, "markets", None)
        if isinstance(markets, dict):
            market = markets.get(symbol)
    except Exception:
        market = None

    if not isinstance(market, dict):
        try:
            ex_obj = getattr(ex, "ex", None)
            if ex_obj is not None and hasattr(ex_obj, "market"):
                market = ex_obj.market(symbol)
        except Exception:
            market = None

    if isinstance(market, dict):
        try:
            limits = market.get("limits", {}) or {}
            amount_limits = limits.get("amount", {}) or {}
            cost_limits = limits.get("cost", {}) or {}
            v_qty = amount_limits.get("min")
            v_cost = cost_limits.get("min")
            if v_qty is not None:
                fq = float(v_qty)
                if fq > 0.0:
                    min_qty = fq
                    source = "markets.limits"
            if v_cost is not None:
                fc = float(v_cost)
                if fc > 0.0:
                    min_cost = fc
                    source = "markets.limits"
        except Exception:
            pass

        try:
            info = market.get("info", {}) or {}
            filters = info.get("filters", []) if isinstance(info, dict) else []
            if isinstance(filters, list):
                for f in filters:
                    if not isinstance(f, dict):
                        continue
                    ftype = str(f.get("filterType", "") or "").upper()
                    if ftype in ("LOT_SIZE", "MARKET_LOT_SIZE"):
                        q = f.get("minQty")
                        st = f.get("stepSize")
                        if q is not None:
                            fq = float(q)
                            if fq > 0.0:
                                min_qty = fq if min_qty is None else max(float(min_qty), fq)
                                source = "market.info.filters"
                        if st is not None:
                            s = str(st).strip()
                            if s and s not in ("0", "0.0"):
                                qty_step_str = s
                    elif ftype in ("MIN_NOTIONAL", "NOTIONAL"):
                        n = f.get("minNotional")
                        if n is not None:
                            fn = float(n)
                            if fn > 0.0:
                                min_cost = fn if min_cost is None else max(float(min_cost), fn)
                                source = "market.info.filters"
        except Exception:
            pass

    if min_qty is None or min_cost is None:
        try:
            if hasattr(ex, "market_amount_rules"):
                m_qty, m_cost = ex.market_amount_rules(symbol)
                if min_qty is None and m_qty is not None and float(m_qty) > 0.0:
                    min_qty = float(m_qty)
                    source = "market_amount_rules"
                if min_cost is None and m_cost is not None and float(m_cost) > 0.0:
                    min_cost = float(m_cost)
                    source = "market_amount_rules"
        except Exception:
            pass

    return {
        "min_qty": (float(min_qty) if min_qty is not None and float(min_qty) > 0.0 else None),
        "min_cost": (float(min_cost) if min_cost is not None and float(min_cost) > 0.0 else None),
        "qty_step_str": str(qty_step_str or ""),
        "source": str(source),
    }


def _log_order_skip_min_once(*, symbol: str, ts_ms: int, stage: str, qty: float, cost: float, min_qty: float | None, min_cost: float | None, price: float, source: str) -> None:
    key = (str(symbol), int(ts_ms), str(stage))
    if key in _ORDER_SKIP_MIN_LOG_KEYS:
        return
    if len(_ORDER_SKIP_MIN_LOG_KEYS) > 200000:
        _ORDER_SKIP_MIN_LOG_KEYS.clear()
    _ORDER_SKIP_MIN_LOG_KEYS.add(key)
    logger.info(
        "[ORDER][SKIP_MIN] symbol=%s qty=%.8f cost=%.8f min_qty=%s min_cost=%s price=%.8f source=%s",
        str(symbol),
        float(qty),
        float(cost),
        f"{float(min_qty):.8f}" if min_qty is not None else "None",
        f"{float(min_cost):.8f}" if min_cost is not None else "None",
        float(price),
        str(source or "unknown"),
    )


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


def _cfg_check_enabled() -> bool:
    """Allow silencing noisy CFG CHECK logs (env/config toggle)."""
    # Highest priority: env override
    v = _env_flag("RUNNER_CFG_CHECK", default=None)
    if v is not None:
        return bool(v)
    # Config fallback (optional)
    return bool(getattr(C, "LOG_CFG_CHECK", True))

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

def _diff_trace_is_enabled() -> bool:
    try:
        import config as _C

        return bool(getattr(_C, "DIFF_TRACE_ENABLED", False))
    except Exception:
        return False


def _diff_trace_dir() -> str:
    try:
        import config as _C

        d = str(getattr(_C, "DIFF_TRACE_DIR", "") or "").strip()
        if is_legacy_exports_path(d):
            return str(_current_export_dir())
        return d
    except Exception:
        return str(_current_export_dir())


def _diff_trace_prefix_live() -> str:
    try:
        import config as _C

        return str(getattr(_C, "DIFF_TRACE_PREFIX_LIVE", "diff_trace_live") or "diff_trace_live")
    except Exception:
        return "diff_trace_live"


def _diff_trace_path(prefix: str, ts_ms: int) -> str:
    try:
        dt = datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc)
        day = dt.strftime("%Y-%m-%d")
    except Exception:
        day = "unknown"
    out_dir = _diff_trace_dir()
    try:
        os.makedirs(out_dir, exist_ok=True)
    except Exception:
        pass
    return os.path.join(out_dir, f"{prefix}_{day}.jsonl")

def _cfg_snapshot(keys=None) -> dict:
    """Return a small, JSON-serializable snapshot of important config knobs.

    This is used for diff-trace / replay debug so we can see which config produced a trace.
    """
    if keys is None:
        keys = [
            "MODE",
            "TRADE_TREND",
            "TRADE_RANGE",
            "TRADE_ONLY_TREND",
            "TREND_ENTRY_MODE",
            "RANGE_ENTRY_MODE",
            "TF_ENTRY",
            "TF_FILTER",
            "MIN_TP_BPS",
            "MIN_RR_AFTER_ADJUST_TREND_LONG",
        ]
    snap = {}
    for k in keys:
        if not hasattr(C, k):
            continue
        v = getattr(C, k)
        if isinstance(v, (str, int, float, bool)) or v is None:
            snap[k] = v
        elif isinstance(v, (list, tuple)):
            snap[k] = list(v)
        else:
            snap[k] = str(v)
    return snap

# Suppress extremely verbose diff_trace rows for SIGNAL/hold during long replays.
# Keyed by "{path}::{symbol}" -> last_reason (string).
_DIFF_TRACE_LAST_SIGNAL_HOLD_REASON: dict[str, str] = {}
_DIFF_TRACE_SAMPLE_EVERY_REPLAY = 1
_DIFF_TRACE_SAMPLE_COUNTER_REPLAY = 0
_DIFF_TRACE_SAMPLE_SKIPPED_REPLAY = 0


class _ReplayTradeLogFilter(logging.Filter):
    def __init__(self, every_n: int) -> None:
        super().__init__()
        self.every_n = int(every_n)
        self.total = 0
        self.emitted = 0
        self.open_total = 0
        self.close_total = 0
        self.close_net_total = 0.0
        self.close_net_count = 0

    @staticmethod
    def _parse_close_net(msg: str) -> float | None:
        key = " net="
        i = str(msg).find(key)
        if i < 0:
            return None
        token = str(msg)[i + len(key):].strip().split(" ", 1)[0].strip().rstrip(",")
        try:
            v = float(token)
        except Exception:
            return None
        if not math.isfinite(v):
            return None
        return float(v)

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = str(record.getMessage())
        except Exception:
            return True
        if "[BACKTEST_DRYRUN]" not in msg:
            return True
        is_open = " OPEN " in msg
        is_close = " CLOSE " in msg
        if not (is_open or is_close):
            return True

        self.total += 1
        if is_open:
            self.open_total += 1
        if is_close:
            self.close_total += 1
            parsed_net = self._parse_close_net(msg)
            if parsed_net is not None:
                self.close_net_total += float(parsed_net)
                self.close_net_count += 1

        if self.every_n <= 0:
            return False
        if self.every_n == 1:
            self.emitted += 1
            return True
        if (self.total % self.every_n) == 0:
            self.emitted += 1
            return True
        return False


_REPLAY_TRADE_LOG_FILTER: _ReplayTradeLogFilter | None = None


def _set_replay_trade_log_filter(every_n: int) -> _ReplayTradeLogFilter:
    global _REPLAY_TRADE_LOG_FILTER
    if _REPLAY_TRADE_LOG_FILTER is not None:
        try:
            logger.removeFilter(_REPLAY_TRADE_LOG_FILTER)
        except Exception:
            pass
        _REPLAY_TRADE_LOG_FILTER = None
    f = _ReplayTradeLogFilter(max(0, int(every_n)))
    logger.addFilter(f)
    _REPLAY_TRADE_LOG_FILTER = f
    return f


def _clear_replay_trade_log_filter() -> None:
    global _REPLAY_TRADE_LOG_FILTER
    if _REPLAY_TRADE_LOG_FILTER is None:
        return
    try:
        logger.removeFilter(_REPLAY_TRADE_LOG_FILTER)
    except Exception:
        pass
    _REPLAY_TRADE_LOG_FILTER = None

# Latest confirmed bar OHLC snapshot per symbol (entry timeframe).
# Populated in the main loop and auto-attached to all diff_trace events.
_DIFF_TRACE_BAR_BY_SYMBOL: dict[str, dict] = {}
_DIFF_TRACE_BAR_INDEX_BY_SYMBOL: dict[str, dict[int, dict[str, float | int]]] = {}
_DIFF_TRACE_OHLCV_BY_SYMBOL: dict[str, dict] = {}

def _trace_ref_ts_ms(event: dict) -> int:
    """TraceBarPolicy: unified event reference timestamp in milliseconds (exec_ts first)."""
    return int(_trace_policy_ref_ts_ms(event or {}))


def _trace_expected_bar_ts_ms(ref_ts_ms: int, tf_ms: int = 300000) -> int:
    """Confirmed-only bar mapping for trace snapshots.

    expected_bar_ts = floor(event_ts / tf) * tf - tf
    (even when event_ts is exactly on a timeframe boundary).
    """
    return int(_trace_policy_expected_bar_ts_ms(int(ref_ts_ms), int(tf_ms)))


def _trace_bar_snapshot_at(
    ohlcv_5m: dict,
    *,
    ref_ts_ms: int,
    is_replay: bool,
) -> dict[str, float | int] | None:
    """TraceBarPolicy wrapper: snapshot anchored by unified ref_ts_ms."""
    _ = bool(is_replay)
    expected_ts = _trace_expected_bar_ts_ms(int(ref_ts_ms), 300000)
    return _trace_policy_snapshot_ohlc_at(ohlcv_5m, int(expected_ts))


def _trace_update_bar_cache(symbol: str, ohlcv_5m: dict, ref_ts_ms: int, is_replay: bool) -> None:
    """Update latest per-symbol trace bar cache from the single snapshot source."""
    try:
        snap = _trace_bar_snapshot_at(ohlcv_5m, ref_ts_ms=int(ref_ts_ms), is_replay=bool(is_replay))
        if not isinstance(snap, dict):
            return
        key = str(symbol)
        _DIFF_TRACE_BAR_BY_SYMBOL[key] = {
            "bar_ts_ms": int(snap.get("bar_ts_ms") or 0),
            "bar_open": float(snap.get("bar_open") or 0.0),
            "bar_high": float(snap.get("bar_high") or 0.0),
            "bar_low": float(snap.get("bar_low") or 0.0),
            "bar_close": float(snap.get("bar_close") or 0.0),
        }
        _DIFF_TRACE_BAR_BY_SYMBOL[key.replace("/", "")] = dict(_DIFF_TRACE_BAR_BY_SYMBOL[key])
        _DIFF_TRACE_OHLCV_BY_SYMBOL[key] = ohlcv_5m
        _DIFF_TRACE_OHLCV_BY_SYMBOL[key.replace("/", "")] = ohlcv_5m
    except Exception:
        return


def _trace_index_symbol_bars(symbol: str, ohlcv_5m: dict) -> None:
    """Index symbol bars by open timestamp so event_ts -> confirmed bar is deterministic."""
    try:
        ts = ohlcv_5m.get("timestamp", [])
        op = ohlcv_5m.get("open", [])
        hi = ohlcv_5m.get("high", [])
        lo = ohlcv_5m.get("low", [])
        cl = ohlcv_5m.get("close", [])
        n = min(len(ts), len(op), len(hi), len(lo), len(cl))
        if n <= 0:
            return
        idx_map: dict[int, dict[str, float | int]] = {}
        for i in range(max(0, n - 600), n):
            t = int(ts[i])
            idx_map[t] = {
                "bar_ts_ms": int(t),
                "bar_open": float(op[i]),
                "bar_high": float(hi[i]),
                "bar_low": float(lo[i]),
                "bar_close": float(cl[i]),
            }
        key = str(symbol)
        _DIFF_TRACE_BAR_INDEX_BY_SYMBOL[key] = idx_map
        _DIFF_TRACE_BAR_INDEX_BY_SYMBOL[key.replace("/", "")] = idx_map
    except Exception:
        return


def _trace_get_bar_snapshot(symbol: str, event_ts_ms: int, tf_ms: int = 300000) -> dict | None:
    """Get unified trace bar snapshot for an event timestamp."""
    expected_ts = _trace_expected_bar_ts_ms(int(event_ts_ms), tf_ms=int(tf_ms))
    sym = str(symbol or "")
    candidates = [sym]
    if "/" in sym:
        candidates.append(sym.replace("/", ""))
    else:
        for k in _DIFF_TRACE_BAR_INDEX_BY_SYMBOL.keys():
            if k.replace("/", "") == sym:
                candidates.append(k)
                break
    for key in candidates:
        ohlcv = _DIFF_TRACE_OHLCV_BY_SYMBOL.get(key)
        if isinstance(ohlcv, dict):
            snap = _trace_policy_snapshot_ohlc_at(ohlcv, int(expected_ts))
            if isinstance(snap, dict):
                return dict(snap)
        idx_map = _DIFF_TRACE_BAR_INDEX_BY_SYMBOL.get(key)
        if not isinstance(idx_map, dict):
            continue
        snap = idx_map.get(int(expected_ts))
        if isinstance(snap, dict):
            return dict(snap)
    return None

def _confirmed_5m_bar(ohlcv_5m: dict, candle_ts_run: int, is_replay: bool) -> tuple[int, float, float, float, float] | None:
    """Backward-compatible wrapper over _trace_bar_snapshot()."""
    snap = _trace_bar_snapshot(
        ohlcv_5m,
        candle_ts_run=int(candle_ts_run),
        is_replay=bool(is_replay),
    )
    if not isinstance(snap, dict):
        return None
    try:

        return (
            int(snap.get("bar_ts_ms") or 0),
            float(snap.get("bar_open") or 0.0),
            float(snap.get("bar_high") or 0.0),
            float(snap.get("bar_low") or 0.0),
            float(snap.get("bar_close") or 0.0),
        )
    except Exception:
        return None

def _trace_bar_snapshot(
    ohlcv_5m: dict,
    *,
    candle_ts_run: int,
    is_replay: bool,
) -> dict[str, float | int] | None:
    """Build trace bar snapshot from confirmed-only 5m bars.

    Rule for trace parity (OPEN/CLOSE):
    - confirmed-only (never use the currently running bar)
    - expected_bar_ts = floor(ref_ts / 5m) * 5m - 5m
    - resolve by exact timestamp key from 5m OHLCV array
    """
    try:
        expected_ts = _trace_expected_bar_ts_ms(int(candle_ts_run), 300000)
        return _trace_policy_snapshot_ohlc_at(ohlcv_5m, int(expected_ts))
    except Exception:
        return None

def _trace_prev_confirmed_bar_snapshot(ohlcv_dict: dict) -> dict | None:
    """Trace-only fallback: use previous confirmed bar (strictly [-2])."""
    if not isinstance(ohlcv_dict, dict):
        return None
    ts = ohlcv_dict.get("timestamp") or []
    op = ohlcv_dict.get("open") or []
    hi = ohlcv_dict.get("high") or []
    lo = ohlcv_dict.get("low") or []
    cl = ohlcv_dict.get("close") or []
    n = min(len(ts), len(op), len(hi), len(lo), len(cl))
    if n < 2:
        return None
    i = -2
    try:
        return {
            "bar_ts_ms": int(ts[i]),
            "bar_open": float(op[i]),
            "bar_high": float(hi[i]),
            "bar_low": float(lo[i]),
            "bar_close": float(cl[i]),
        }
    except Exception:
        return None

def _trace_prev_bar_for_replay_diff(ohlcv_dict: dict) -> dict | None:
    """Replay diff-trace bar policy: always use previous bar from latest 5m array ([-2])."""
    return _trace_prev_confirmed_bar_snapshot(ohlcv_dict)

def _diff_trace_write(event: dict) -> None:
    """Append one JSONL record for live/backtest diff verification.

    Policy:
    - Ensure one REPLAY_META line exists at the top of each daily jsonl file.
    - Stamp every record with (source/build_id/mode/cfg) so verify_diff can key-match.
    """
    global _DIFF_TRACE_SAMPLE_COUNTER_REPLAY, _DIFF_TRACE_SAMPLE_SKIPPED_REPLAY

    if not _diff_trace_is_enabled():
        return

    # normalize optional fields for stable verify_diff comparisons
    if isinstance(event, dict) and "open_reason" in event:
        event["open_reason"] = str(event.get("open_reason") or "")

    try:
        ts_ms = int(event.get("ts_ms") or event.get("ts") or 0)
    except Exception:
        ts_ms = 0

# Keep event-time fields stable for verify_diff key matching.
    base_ts = ts_ms
    event.setdefault("ts_ms", base_ts)
    event.setdefault("ref_ts_ms", base_ts)
    event.setdefault("event_ts", base_ts)
    event.setdefault("exec_ts", base_ts)

    # Skip noisy per-bar SIGNAL logs unless they represent an actionable decision.
    if event.get("event") == "SIGNAL" and str(event.get("action", "")).lower() == "hold":
        return
    try:
        event.setdefault("source", str(_DIFF_TRACE_SOURCE))
        event.setdefault("build_id", str(BUILD_ID))
        event.setdefault("mode", str(getattr(C, "MODE", "LIVE")))
        if ("sym" not in event) and (event.get("symbol") is not None):
            event["sym"] = event.get("symbol")
        if ("symbol" not in event) and (event.get("sym") is not None):
            event["symbol"] = event.get("sym")
        # IMPORTANT: Always normalize cfg to the stable subset for verify_diff key matching.
        event["cfg"] = _diff_trace_norm_cfg(event.get("cfg"))
        if "ts_iso" not in event:
            try:
                event["ts_iso"] = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()
            except Exception:
                event["ts_iso"] = ""
    except Exception:
        pass

    path = _diff_trace_path(_diff_trace_prefix_live(), ts_ms)

    # Ensure: one meta line at the top of each file (once per process)
    try:
        if path not in _DIFF_TRACE_META_WRITTEN:
            need_write_meta = True
            existing_body = ""
            if os.path.exists(path):
                try:
                    with open(path, "r", encoding="utf-8") as rf:
                        first = (rf.readline() or "").strip()
                        rest = rf.read()
                    if first:
                        first_ev = json.loads(first)
                        if first_ev.get("event") == "REPLAY_META":
                            need_write_meta = False
                        else:
                            # keep existing content to prepend REPLAY_META safely (no truncation loss)
                            existing_body = first + "\n" + (rest or "")
                    else:
                        existing_body = (rest or "")
                except Exception:
                    # If the existing file is not valid JSONL (e.g. prior run wrote 1 huge line),
                    # we still want to prepend REPLAY_META without losing the body.
                    # Heuristic: if the file already starts with REPLAY_META text, skip.
                    try:
                        with open(path, 'r', encoding='utf-8') as rf2:
                            body = rf2.read()
                        if body.lstrip().startswith('{"event": "REPLAY_META"'):
                            need_write_meta = False
                        else:
                            existing_body = body
                            need_write_meta = True
                    except Exception:
                        need_write_meta = False

            if need_write_meta:
                meta = {
                    "event": "REPLAY_META",
                    "source": str(_DIFF_TRACE_SOURCE),
                    "build_id": str(BUILD_ID),
                    "mode": str(event.get("mode") or getattr(C, "MODE", "LIVE")),
                    "cfg": _diff_trace_norm_cfg(event.get("cfg")),
                    "ts_ms": int(ts_ms),
                    "tf_entry": str(getattr(C, "TF_ENTRY", "")),
                    "tf_filter": str(getattr(C, "TF_FILTER", "")),
                }
                os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
                meta_line = json.dumps(meta, ensure_ascii=False, sort_keys=True) + "\n"
                # If file already had content (but no meta), prepend meta without losing lines.
                if existing_body:
                    with open(path, "w", encoding="utf-8") as wf:
                       wf.write(meta_line)
                       wf.write(existing_body)
                else:
                    with open(path, "w", encoding="utf-8") as wf:
                        wf.write(meta_line)

            _DIFF_TRACE_META_WRITTEN.add(path)
    except Exception:
        pass
    try:
        # REPLAY_META is injected separately so each daily file gets exactly one metadata row.
        # Do not emit REPLAY_META here because it can overwrite existing OPEN/CLOSE events.
        if str(event.get("event", "")) == "REPLAY_META":
            return

        if _DIFF_TRACE_SOURCE == "replay" and str(event.get("event", "")) == "BE_SET":
            return

        # De-dupe SIGNAL/hold rows (same reason repeated for many consecutive candles)
        # so year-long replay doesn't produce massive jsonl and slow down over time.
        if str(event.get("event", "")) == "SIGNAL" and str(event.get("action", "")) == "hold":
            sym = str(event.get("symbol") or "")
            reason = str(event.get("reason") or "")
            k = f"{path}::{sym}"
            last = _DIFF_TRACE_LAST_SIGNAL_HOLD_REASON.get(k)
            if last == reason:
                return
            _DIFF_TRACE_LAST_SIGNAL_HOLD_REASON[k] = reason
        if str(_DIFF_TRACE_SOURCE) == "replay":
            ev_name = str(event.get("event", ""))
            if int(_DIFF_TRACE_SAMPLE_EVERY_REPLAY) > 1 and ev_name not in ("REPLAY_META", "OPEN", "CLOSE", "EXCEPTION"):
                _DIFF_TRACE_SAMPLE_COUNTER_REPLAY += 1
                if (_DIFF_TRACE_SAMPLE_COUNTER_REPLAY % int(_DIFF_TRACE_SAMPLE_EVERY_REPLAY)) != 0:
                    _DIFF_TRACE_SAMPLE_SKIPPED_REPLAY += 1
                    return
        # Auto-attach latest confirmed bar OHLC (if available) for parity checks.
        sym2 = str(event.get("symbol") or "")
        if sym2:
            tf_ms = int(_timeframe_to_ms(str(event.get("tf_entry") or getattr(C, "ENTRY_TF", "5m")) or "5m"))
            ohlcv = _DIFF_TRACE_OHLCV_BY_SYMBOL.get(sym2)
            if not isinstance(ohlcv, dict) and "/" in sym2:
                ohlcv = _DIFF_TRACE_OHLCV_BY_SYMBOL.get(sym2.replace("/", ""))
            if not isinstance(ohlcv, dict) and "/" not in sym2:
                for _k in _DIFF_TRACE_OHLCV_BY_SYMBOL.keys():
                    if _k.replace("/", "") == sym2:
                        ohlcv = _DIFF_TRACE_OHLCV_BY_SYMBOL.get(_k)
                        break
            if isinstance(ohlcv, dict):
                event = _trace_attach_bar_snapshot(event, ohlcv, int(tf_ms))
                if str(_DIFF_TRACE_SOURCE) == "replay":
                    replay_snap = _trace_policy_snapshot_ohlc_at(ohlcv, int(base_ts))
                    expected_ts = int(_trace_expected_bar_ts_ms(int(base_ts), int(tf_ms)))
                    replay_snap = _trace_policy_snapshot_ohlc_at(ohlcv, int(expected_ts))
                    if isinstance(replay_snap, dict):
                        event["bar_ts_ms"] = int(replay_snap.get("bar_ts_ms") or 0)
                        event["bar_open"] = float(replay_snap.get("bar_open") or 0.0)
                        event["bar_high"] = float(replay_snap.get("bar_high") or 0.0)
                        event["bar_low"] = float(replay_snap.get("bar_low") or 0.0)
                        event["bar_close"] = float(replay_snap.get("bar_close") or 0.0)
                    else:
                        # Keep trace robust when no exact confirmed bar snapshot is available.
                        # Never emit None for bar OHLC (verify_details aggregation stability).
                        fallback = _trace_get_bar_snapshot(sym2, int(base_ts), int(tf_ms))
                        if isinstance(fallback, dict):
                            event["bar_ts_ms"] = int(fallback.get("bar_ts_ms") or 0)
                            event["bar_open"] = float(fallback.get("bar_open") or 0.0)
                            event["bar_high"] = float(fallback.get("bar_high") or 0.0)
                            event["bar_low"] = float(fallback.get("bar_low") or 0.0)
                            event["bar_close"] = float(fallback.get("bar_close") or 0.0)
                        else:
                            event["bar_ts_ms"] = int(expected_ts)
                            event["bar_open"] = float(event.get("bar_open") or 0.0)
                            event["bar_high"] = float(event.get("bar_high") or 0.0)
                            event["bar_low"] = float(event.get("bar_low") or 0.0)
                            event["bar_close"] = float(event.get("bar_close") or 0.0)
                    event["expected_bar_ts_ms"] = int(expected_ts)
        sym3 = str(event.get("symbol") or "")
        has_missing_bar = any(
            event.get(k) is None for k in ("bar_open", "bar_high", "bar_low", "bar_close")
        ) or any(k not in event for k in ("bar_open", "bar_high", "bar_low", "bar_close"))
        if sym3 and has_missing_bar:
            ts_candidates = [event.get("ts_ms"), event.get("ref_ts_ms"), event.get("exec_ts")]
            event_ts_ms = 0
            for _t in ts_candidates:
                try:
                    if _t is None:
                        continue
                    event_ts_ms = int(_t)
                    if event_ts_ms > 0:
                        break
                except Exception:
                    continue
            if event_ts_ms > 0:
                snap = _trace_get_bar_snapshot(sym3, int(event_ts_ms))
                if isinstance(snap, dict):
                    for _k in ("bar_ts_ms", "bar_open", "bar_high", "bar_low", "bar_close"):
                        _v = snap.get(_k)
                        if _v is None:
                            continue
                        if _k == "bar_ts_ms":
                            event[_k] = int(_v)
                        else:
                            event[_k] = float(_v)
                    event.setdefault("expected_bar_ts_ms", int(_trace_expected_bar_ts_ms(int(event_ts_ms), 300000)))
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")
    except Exception:
        return

def _diff_trace_exception(
    *,
    symbol: str | None,
    ts_ms: int | None,
    phase: str,
    exc: BaseException,
    extra: dict | None = None,
) -> None:
    """Write a compact exception marker into diff_trace so we can locate the stop point later."""
    try:
        import traceback as _tb

        def _to_pos_int(v: Any) -> int:
            try:
                x = int(float(v))
            except Exception:
                return 0
            return x if x > 0 else 0

        ts_fixed = _to_pos_int(ts_ms)
        if ts_fixed <= 0 and isinstance(extra, dict):
            for _k in ("ref_ts_ms", "event_ts", "exec_ts"):
                ts_fixed = _to_pos_int(extra.get(_k))
                if ts_fixed > 0:
                    break
        if ts_fixed <= 0:
            ts_fixed = int(time.time() * 1000)

        payload = {
            "event": "EXCEPTION",
            "phase": str(phase),
            "symbol": symbol,
            "exc": f"{type(exc).__name__}: {exc}",
            # Keep size bounded to avoid gigantic jsonl lines.
            "traceback": _tb.format_exc(limit=40)[-4000:],
        }
        if extra:
            payload.update(extra)
        payload["ts_ms"] = int(ts_fixed)
        _diff_trace_write(payload)
    except Exception:
        return
def _diff_trace_buy_reject(
    *,
    ts_ms: int,
    symbol: str,
    tf_entry: str,
    tf_filter: str,
    regime: str,
    direction: str,
    stage: str,
    reason: str,
    mode: str | None = None,
    trade_range: bool | None = None,
    trade_trend: float | None = None,
    cfg: dict | None = None,
    close: float | None = None,
    entry_raw: float | None = None,
    stop_raw: float | None = None,
    tp_raw: float | None = None,
    rr0: float | None = None,
    qty: float | None = None,
    extra: dict | None = None,
) -> None:
    """Emit BUY_REJECT record for diff-trace when SIGNAL=buy but entry is rejected by gates."""
    try:
        rec: dict[str, Any] = {
            "event": "BUY_REJECT",
            "ts_ms": int(ts_ms),
            "symbol": str(symbol),
            "tf_entry": str(tf_entry),
            "tf_filter": str(tf_filter),
            "regime": str(regime),
            "direction": str(direction),
            "stage": str(stage),
            "reason": str(reason),
        }
        if mode is not None:
            rec["mode"] = str(mode)
        if trade_range is not None:
            rec["trade_range"] = bool(trade_range)
        if trade_trend is not None:
            rec["trade_trend"] = float(trade_trend)
        if cfg is not None:
            rec["cfg"] = cfg
        if close is not None:
            rec["close"] = float(close)
        if entry_raw is not None:
            rec["entry_raw"] = float(entry_raw)
        if stop_raw is not None:
            rec["stop_raw"] = float(stop_raw)
        if tp_raw is not None:
            rec["tp_raw"] = float(tp_raw)
        if rr0 is not None:
            rec["rr0"] = float(rr0)
        if qty is not None:
            rec["qty"] = float(qty)
        if isinstance(extra, dict) and extra:
            for k, v in extra.items():
                if k in rec:
                    continue
                rec[k] = v
        _diff_trace_write(rec)
    except Exception:
        pass

# ---------------------------
# small utils
# ---------------------------

def ohlcv_to_dict(rows: list[list[float]]) -> dict[str, list[float]]:
    """
    rows: [ [ts, open, high, low, close, volume], ... ]
    """
    out = {"timestamp": [], "open": [], "high": [], "low": [], "close": [], "volume": []}
    for r in rows:
        out["timestamp"].append(int(r[0]))
        out["open"].append(float(r[1]))
        out["high"].append(float(r[2]))
        out["low"].append(float(r[3]))
        out["close"].append(float(r[4]))
        out["volume"].append(float(r[5]))
    return out

def _timeframe_to_ms(tf: str) -> int:
    tf = str(tf).strip().lower()
    if tf.endswith("m"):
        return int(tf[:-1]) * 60_000
    if tf.endswith("h"):
        return int(tf[:-1]) * 3_600_000
    if tf.endswith("d"):
        return int(tf[:-1]) * 86_400_000
    raise ValueError(f"unsupported timeframe: {tf}")

def _confirmed_candle_idx(ts_list: List[int], bar_ms: int, now_ms: int) -> int:
    """Return index of the most recent *confirmed* candle in ts_list.

    Many exchanges return the current still-forming candle as the last element.
    We treat a candle as confirmed only if now_ms is at least one full bar after its open timestamp.
    """
    if not ts_list:
        return -1
    if len(ts_list) < 2:
        return -1
    try:
        last_ts = int(ts_list[-1])
    except Exception:
        last_ts = int(ts_list[-1])
    # If we're still within the last bar window, the last candle is unconfirmed.
    if int(now_ms) - int(last_ts) < int(bar_ms):
        return -2
    return -1

def _ema_last(values, span: int) -> float:
    # lightweight EMA for latest value only (sufficient for cooldown clear condition)
    if not values:
        return float("nan")
    alpha = 2.0 / (float(span) + 1.0)
    ema = float(values[0])
    for v in values[1:]:
        ema = alpha * float(v) + (1.0 - alpha) * ema
    return float(ema)


def _kv_key_range_ema9_exit_cooldown(symbol: str) -> str:
    return f"cooldown:range_ema9_exit:{symbol}"

def _kv_key_pullback_ab(symbol: str) -> str:
    return f"pullback_ab:{symbol}"


def _kv_key_pullback_ab_legacy(symbol: str) -> str:
    return f"state.pullback_ab:{symbol}"


def _normalize_pullback_ab_state(state: Any) -> Dict[str, Any]:
    if not isinstance(state, dict):
        return {"phase": "NONE", "since_ms": None, "last_reason": None}
    out = {
        "phase": str(state.get("phase") or "NONE"),
        "since_ms": state.get("since_ms", None),
        "last_reason": state.get("last_reason", None),
    }
    return out

def _kv_get_pullback_ab(store: StateStore, symbol: str) -> Dict[str, Any]:
    key = _kv_key_pullback_ab(symbol)
    legacy_key = _kv_key_pullback_ab_legacy(symbol)
    for candidate_key in (key, legacy_key):
        v = store.get_kv(candidate_key, "")
        if not v:
            continue
        try:
            obj = json.loads(v)
        except Exception:
            continue
        state = _normalize_pullback_ab_state(obj)
        if candidate_key != key:
            _kv_put_pullback_ab(store, symbol, state)
        return state
    return _normalize_pullback_ab_state(None)

def _kv_put_pullback_ab(store: StateStore, symbol: str, state: Dict[str, Any]) -> None:
    key = _kv_key_pullback_ab(symbol)
    legacy_key = _kv_key_pullback_ab_legacy(symbol)
    try:
        store.set_kv(key, json.dumps(_normalize_pullback_ab_state(state), ensure_ascii=False))
        store.del_kv(legacy_key)
    except Exception:
        # Never break trading loop due to KV write errors.
        pass

def _now_epoch() -> float:
    return datetime.now().timestamp()


def _kv_get_float(store: StateStore, key: str, default: float) -> float:
    try:
        v = store.get_kv(key, "")
        if v in (None, ""):
            return default
        return float(v)
    except Exception:
        return default


def _kv_set_float(store: StateStore, key: str, value: float) -> None:
    store.set_kv(key, str(float(value)))


def _kv_get_int(store: StateStore, key: str, default: int) -> int:
    try:
        v = store.get_kv(key, "")
        if v in (None, ""):
            return default
        return int(float(v))
    except Exception:
        return default


def _kv_set_int(store: StateStore, key: str, value: int) -> None:
    store.set_kv(key, str(int(value)))

# --- Range EMA9-exit cooldown (live-safe / StateStore-backed) ---

_RANGE_EMA9_EXIT_COOLDOWN_KEY_PREFIX = "range_ema9_exit_cooldown:"
_RANGE_EMA9_EXIT_COOLDOWN_BAR_MS = 15 * 60 * 1000  # entry regime helpers run on entry

def _set_range_ema9_exit_cooldown(
    store: "StateStore",
    symbol: str,
    exit_ts_ms: int,
    *,
    bars: int,
    reclaim_ema9: float | None,
) -> None:
    """Persist cooldown so live + backtest can share the same gate.

    We intentionally store the EMA9 level at exit time (reclaim_ema9) so
    the unlock condition doesn't depend on recomputing indicators in runner.
    """
    try:
        bars_i = int(bars)
    except Exception:
        bars_i = 0
    until_ms = int(exit_ts_ms) + max(0, bars_i) * _RANGE_EMA9_EXIT_COOLDOWN_BAR_MS
    payload = {
        "set_ts_ms": int(exit_ts_ms),
        "bars": bars_i,
        "until_ms": until_ms,
        "reclaim_ema9": (float(reclaim_ema9) if reclaim_ema9 is not None else None),
    }
    store.set_kv(_RANGE_EMA9_EXIT_COOLDOWN_KEY_PREFIX + symbol, json.dumps(payload, ensure_ascii=False))

def _check_range_ema9_exit_cooldown(
    store: "StateStore",
    symbol: str,
    now_ts_ms: int,
    close_now: float,
    *,
    reclaim_buf_bps: float = 0.0,
    clear_on_reclaim_ema9: bool = True,
) -> tuple[bool, str | None]:
    """Return (blocked, reason). Clears cooldown when condition satisfied."""
    raw = store.get_kv(_RANGE_EMA9_EXIT_COOLDOWN_KEY_PREFIX + symbol)
    if not raw:
        return (False, None)
    try:
        payload = json.loads(raw)
    except Exception:
        # Corrupted -> clear defensively.
        store.set_kv(_RANGE_EMA9_EXIT_COOLDOWN_KEY_PREFIX + symbol, "")
        return (False, None)

    until_ms = int(payload.get("until_ms") or 0)
    if int(now_ts_ms) >= until_ms and until_ms > 0:
        store.set_kv(_RANGE_EMA9_EXIT_COOLDOWN_KEY_PREFIX + symbol, "")
        return (False, None)

    reclaim_ema9 = payload.get("reclaim_ema9", None)
    if clear_on_reclaim_ema9 and reclaim_ema9 is not None:
        try:
            thr = float(reclaim_ema9) * (1.0 + float(reclaim_buf_bps) / 10000.0)
            if float(close_now) >= thr:
                store.set_kv(_RANGE_EMA9_EXIT_COOLDOWN_KEY_PREFIX + symbol, "")
                return (False, None)
        except Exception:
            pass

    # Still blocked
    remain_bars = 0
    if until_ms > 0:
        remain_ms = max(0, until_ms - int(now_ts_ms))
        remain_bars = int((remain_ms + _RANGE_EMA9_EXIT_COOLDOWN_BAR_MS - 1) // _RANGE_EMA9_EXIT_COOLDOWN_BAR_MS)
    return (True, f"range_cooldown_after_ema9_exit(remain={remain_bars}bars n={payload.get('bars', '?')})")

# --- Range EMA21-break re-entry block (live-safe / StateStore-backed) ---
_RANGE_EMA21_BREAK_BLOCK_KEY_PREFIX = "range_ema21_break_block:"
_RANGE_EMA21_BREAK_BLOCK_BAR_MS = 15 * 60 * 1000  # regime helpers run on entry

def _set_range_ema21_break_block(
    store: "StateStore",
    symbol: str,
    exit_ts_ms: int,
    *,
    reclaim_ema21: float | None,
) -> None:
    """Persist a re-entry block after RANGE_EMA21_BREAK / RANGE_EARLY_LOSS_ATR.

    We store the EMA21 level at exit time (reclaim_ema21) and clear the block
    once price reclaims it. This keeps logic live-safe (no indicator recompute required).
    """
    payload = {
        "set_ts_ms": int(exit_ts_ms),
        "reclaim_ema21": (float(reclaim_ema21) if reclaim_ema21 is not None else None),
    }
    try:
        store.set_kv(_RANGE_EMA21_BREAK_BLOCK_KEY_PREFIX + symbol, json.dumps(payload, ensure_ascii=False))
    except Exception:
        pass

def _check_range_ema21_break_block_state(
    store: "StateStore",
    symbol: str,
    now_ts_ms: int,
    close_now: float,
    *,
    reclaim_buf_bps: float = 0.0,
) -> tuple[bool, str | None]:
    """Return (blocked, reason). Clears block when condition satisfied."""
    raw = store.get_kv(_RANGE_EMA21_BREAK_BLOCK_KEY_PREFIX + symbol)
    if not raw:
        return (False, None)
    try:
        payload = json.loads(raw)
    except Exception:
        store.set_kv(_RANGE_EMA21_BREAK_BLOCK_KEY_PREFIX + symbol, "")
        return (False, None)

    # Safety valve: never block forever even if reclaim condition is not met.
    # Default: 16 bars (5m * 16 = 80m). Config override: RANGE_EMA21_BREAK_BLOCK_MAX_BARS.
    try:
        set_ts = int(payload.get("set_ts_ms", 0) or 0)
    except Exception:
        set_ts = 0
    max_bars = int(getattr(C, "RANGE_EMA21_BREAK_BLOCK_MAX_BARS", 16) or 16)
    max_bars = max(1, min(max_bars, 200))
    if set_ts > 0:
        try:
            age_ms = int(now_ts_ms) - int(set_ts)
        except Exception:
            age_ms = 0
        if age_ms >= max_bars * _RANGE_EMA21_BREAK_BLOCK_BAR_MS:
            store.set_kv(_RANGE_EMA21_BREAK_BLOCK_KEY_PREFIX + symbol, "")
            return (False, None)

    reclaim_ema21 = payload.get("reclaim_ema21")
    if reclaim_ema21 is not None:
        try:
            lvl = float(reclaim_ema21)
        except Exception:
            lvl = float("nan")
        if math.isfinite(lvl) and math.isfinite(float(close_now)):
            if float(close_now) >= lvl * (1.0 + float(reclaim_buf_bps) / 10_000.0):
                store.set_kv(_RANGE_EMA21_BREAK_BLOCK_KEY_PREFIX + symbol, "")
                return (False, None)

    return (True, "range_block_after_ema21_break(until=close_reclaim_ema21)")

def _pos_meta_key(symbol: str) -> str:
    return f"pos_meta:{symbol}"


def _set_pos_meta(store: StateStore, symbol: str, meta: dict[str, Any]) -> None:
    try:
        store.set_kv(_pos_meta_key(symbol), json.dumps(meta, ensure_ascii=False))
    except Exception:
        pass


def _update_pos_meta(store: StateStore, symbol: str, updates: dict[str, Any]) -> dict[str, Any]:
    try:
        meta = _get_pos_meta(store, symbol)
        if not isinstance(meta, dict):
            meta = {}
        changed = False
        for k, v in (updates or {}).items():
            if meta.get(k) != v:
                meta[k] = v
                changed = True
        if changed:
            _set_pos_meta(store, symbol, meta)
        return meta
    except Exception:
        return {}


def _get_pos_meta(store: StateStore, symbol: str) -> dict[str, Any]:
    try:
        raw = store.get_kv(_pos_meta_key(symbol), "")
        if raw in (None, ""):
            return {}
        return json.loads(raw)
    except Exception:
        return {}


def _clear_pos_meta(store: StateStore, symbol: str) -> None:
    try:
        store.set_kv(_pos_meta_key(symbol), "")
    except Exception:
        pass

def _pos_tp1_done_key(symbol: str) -> str:
    return f"pos_tp1_done:{symbol}"

def _pos_init_qty_key(symbol: str) -> str:
    return f"pos_init_qty:{symbol}"

def _get_pos_tp1_done(store: StateStore, symbol: str) -> bool:
    try:
        v = store.get_kv(_pos_tp1_done_key(symbol), "")
        return str(v).strip() == "1"
    except Exception:
        return False

def _set_pos_tp1_done(store: StateStore, symbol: str, done: bool) -> None:
    try:
        store.set_kv(_pos_tp1_done_key(symbol), "1" if done else "0")
    except Exception:
        pass

def _clear_pos_tp1_done(store: StateStore, symbol: str) -> None:
    try:
        store.set_kv(_pos_tp1_done_key(symbol), "")
    except Exception:
        pass

def _get_pos_init_qty(store: StateStore, symbol: str, fallback: float) -> float:
    try:
        v = store.get_kv(_pos_init_qty_key(symbol), "")
        if v in (None, ""):
            return float(fallback)
        return float(v)
    except Exception:
        return float(fallback)

def _clear_pos_init_qty(store: StateStore, symbol: str) -> None:
    try:
        store.set_kv(_pos_init_qty_key(symbol), "")
    except Exception:
        pass

def _iso_utc(ts: int) -> str:
    if ts >= 10_000_000_000_000:
        ts_s = ts / 1_000_000.0
    elif ts >= 10_000_000_000:
        ts_s = ts / 1_000.0
    else:
        ts_s = ts
    return datetime.fromtimestamp(ts_s, tz=timezone.utc).isoformat()


def _read_trade_header(path: str) -> list[str] | None:
    try:
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            return next(reader, None)
    except FileNotFoundError:
        return None


def _append_trade_csv(row: dict[str, Any]) -> None:
    path = _export_path("trades.csv")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    header = _read_trade_header(path)
    row_keys = list(row.keys())
    if header is None:
        cols = sorted(row_keys)
        with open(path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=cols)
            writer.writeheader()
            writer.writerow(row)
        return

    missing = [k for k in row_keys if k not in header]
    if missing:
        cols = header + missing
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        with open(path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=cols)
            writer.writeheader()
            for r in rows:
                writer.writerow(r)
            writer.writerow(row)
        return

    with open(path, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header, extrasaction="ignore")
        writer.writerow(row)

def _risk_day_key_from_ts_ms(ts_ms: int) -> str:
    try:
        dt = datetime.fromtimestamp(float(ts_ms) / 1000.0, tz=timezone(timedelta(hours=9)))
    except Exception:
        dt = datetime.now(timezone(timedelta(hours=9)))
    return dt.strftime("%Y-%m-%d")

def _risk_week_key_from_ts_ms(ts_ms: int) -> str:
    """JST ISO week key: YYYY-Www"""
    try:
        dt = datetime.fromtimestamp(float(ts_ms) / 1000.0, tz=timezone(timedelta(hours=9)))
    except Exception:
        dt = datetime.now(timezone(timedelta(hours=9)))
    iso = dt.isocalendar()
    return f"{int(iso.year):04d}-W{int(iso.week):02d}"

def _kv_key_risk_weekly(week_key: str) -> str:
    return f"risk_weekly:{week_key}"

def _normalize_ccy_key(value: str | None) -> str:
    return str(value or "").strip().upper()


def _live_equity_symbols_label(symbols: list[str] | None = None) -> str:
    vals = [str(s or "").strip() for s in list(symbols or _LIVE_EQUITY_SYMBOLS or []) if str(s or "").strip()]
    if not vals:
        return "-"
    if len(vals) == 1:
        return str(vals[0])
    preview = ",".join(vals[:3])
    if len(vals) > 3:
        preview = f"{preview},+{len(vals) - 3}"
    return preview


def _maybe_log_invalid_kill_min_equity_dict_value(*, equity_ccy: str | None, raw_value: Any) -> None:
    global _KILL_MIN_EQUITY_INVALID_DICT_LOGGED
    ccy = _normalize_ccy_key(equity_ccy) or "UNKNOWN"
    state = f"{ccy}:{raw_value!r}"
    if state in _KILL_MIN_EQUITY_INVALID_DICT_LOGGED:
        return
    logger.warning(
        "[KILL] min_equity_by_ccy invalid value ignored equity_ccy=%s raw=%r -> treating as dict miss",
        str(ccy),
        raw_value,
    )
    _KILL_MIN_EQUITY_INVALID_DICT_LOGGED.add(state)


def _resolve_equity_floor_by_ccy(
    *,
    floor_scalar: float,
    floor_map: dict | None,
    equity_ccy: str | None,
    resolve_mode: str = "AUTO",
) -> tuple[float, str]:
    def _coerce_floor(value: Any, default: float = 0.0) -> float:
        try:
            num = float(value)
        except Exception:
            return float(default)
        if not math.isfinite(num):
            return float(default)
        return max(0.0, float(num))

    def _coerce_positive_floor(value: Any) -> float | None:
        try:
            num = float(value)
        except Exception:
            return None
        if (not math.isfinite(num)) or (num <= 0.0):
            return None
        return float(num)

    ccy = _normalize_ccy_key(equity_ccy)
    mode = str(resolve_mode or "AUTO").strip().upper()
    if mode not in {"AUTO", "STRICT"}:
        mode = "AUTO"
    scalar = _coerce_floor(floor_scalar, 0.0)

    if isinstance(floor_map, dict) and ccy:
        invalid_dict_value: Any = None
        for raw_key, raw_value in floor_map.items():
            if _normalize_ccy_key(raw_key) != ccy:
                continue
            dict_floor = _coerce_positive_floor(raw_value)
            if dict_floor is not None:
                return float(dict_floor), f"DICT:{ccy}"
            invalid_dict_value = raw_value
        if invalid_dict_value is not None:
            _maybe_log_invalid_kill_min_equity_dict_value(equity_ccy=ccy, raw_value=invalid_dict_value)

    if mode == "STRICT":
        return 0.0, f"STRICT_MISS:{ccy or 'UNKNOWN'}"
    if scalar <= 0.0:
        return 0.0, "DISABLED"
    return float(scalar), "SCALAR_FALLBACK"


def _maybe_log_kill_min_equity_resolution(*, equity_ccy: str | None, floor: float, source: str) -> None:
    global _KILL_MIN_EQUITY_RESOLUTION_LAST
    if not bool(getattr(C, "KILL_MIN_EQUITY_LOG_RESOLUTION", True)):
        return
    ccy = _normalize_ccy_key(equity_ccy) or "UNKNOWN"
    state = (str(ccy), max(0.0, float(floor)), str(source))
    if _KILL_MIN_EQUITY_RESOLUTION_LAST == state:
        return
    logger.info(
        "[KILL] min_equity_resolution equity_ccy=%s floor=%.6f source=%s",
        str(ccy),
        float(state[1]),
        str(source),
    )
    _KILL_MIN_EQUITY_RESOLUTION_LAST = state


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
    allow_legacy_fallback = False
    try:
        current_export_dir = Path(_current_export_dir()).resolve()
        runtime_exports_dir = Path(ensure_runtime_dirs().exports_dir).resolve()
        repo_exports_dir = (BASE_DIR / "exports").resolve()
        allow_legacy_fallback = current_export_dir in {runtime_exports_dir, repo_exports_dir}
    except Exception:
        allow_legacy_fallback = True
    if ((not os.path.exists(eq_path)) or (not os.path.exists(tr_path))) and allow_legacy_fallback:
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

    def _safe_float(v: Any, default: float = float("nan")) -> float:
        try:
            x = float(v)
            if not math.isfinite(x):
                return float(default)
            return float(x)
        except Exception:
            return float(default)

    eq_by_year: Dict[str, list[float]] = {}
    eq_all: list[float] = []
    mtm_by_year: Dict[str, list[float]] = {}
    mtm_all: list[float] = []
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
            eq_by_year.setdefault(y, []).append(eq)
            eq_all.append(eq)
            mtm_by_year.setdefault(y, []).append(float(eq_mtm))
            mtm_all.append(float(eq_mtm))

    tr_by_year: Dict[str, Dict[str, float]] = {}
    mae_abs_values: list[float] = []
    mae_bps_values: list[float] = []
    mfe_abs_values: list[float] = []
    mfe_bps_values: list[float] = []
    giveback_max_abs_values: list[float] = []
    giveback_max_bps_values: list[float] = []
    giveback_max_pct_values: list[float] = []
    giveback_to_close_abs_values: list[float] = []
    giveback_to_close_bps_values: list[float] = []
    giveback_to_close_pct_values: list[float] = []
    kept_bps_values: list[float] = []
    kept_pct_of_mfe_values: list[float] = []
    fav_adv_ratio_values: list[float] = []
    sample_row: dict[str, Any] | None = None
    with open(tr_path, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            ts_ms = _norm_ts_ms(row.get("ts"))
            y = _year_from_ts(ts_ms)
            try:
                net = float(row.get("net", "") or 0.0)
            except Exception:
                net = 0.0
            agg = tr_by_year.setdefault(y, {"trades": 0.0, "wins": 0.0, "net_total": 0.0})
            agg["trades"] += 1.0
            if net > 0.0:
                agg["wins"] += 1.0
            agg["net_total"] += float(net)
            if sample_row is None:
                sample_row = {
                    "symbol": row.get("symbol"),
                    "entry": row.get("entry_raw", row.get("entry")),
                    "qty": row.get("qty"),
                    "direction": row.get("direction"),
                    "high": row.get("max_fav", row.get("bar_high")),
                    "low": row.get("min_adv", row.get("bar_low")),
                }
            row_mae_abs = _safe_float(row.get("mae_abs"), float("nan"))
            row_mae_bps = _safe_float(row.get("mae_bps"), float("nan"))
            row_mfe_abs = _safe_float(row.get("mfe_abs"), float("nan"))
            row_mfe_bps = _safe_float(row.get("mfe_bps"), float("nan"))
            row_giveback_max_abs = _safe_float(row.get("giveback_max_abs"), float("nan"))
            row_giveback_max_bps = _safe_float(row.get("giveback_max_bps"), float("nan"))
            row_giveback_max_pct = _safe_float(row.get("giveback_max_pct_of_mfe"), float("nan"))
            row_giveback_to_close_abs = _safe_float(row.get("giveback_to_close_abs"), float("nan"))
            row_giveback_to_close_bps = _safe_float(row.get("giveback_to_close_bps"), float("nan"))
            row_giveback_to_close_pct = _safe_float(row.get("giveback_to_close_pct_of_mfe"), float("nan"))

            if math.isfinite(row_mae_abs):
                mae_abs_values.append(max(0.0, float(row_mae_abs)))
            if math.isfinite(row_mae_bps):
                mae_bps_values.append(max(0.0, float(row_mae_bps)))
            if math.isfinite(row_mfe_abs):
                mfe_abs_values.append(max(0.0, float(row_mfe_abs)))
            if math.isfinite(row_mfe_bps):
                mfe_bps_values.append(max(0.0, float(row_mfe_bps)))
            if math.isfinite(row_giveback_max_abs):
                giveback_max_abs_values.append(max(0.0, float(row_giveback_max_abs)))
            if math.isfinite(row_giveback_max_bps):
                giveback_max_bps_values.append(max(0.0, float(row_giveback_max_bps)))
            if math.isfinite(row_giveback_max_pct):
                giveback_max_pct_values.append(max(0.0, float(row_giveback_max_pct)))
            if math.isfinite(row_giveback_to_close_abs):
                giveback_to_close_abs_values.append(max(0.0, float(row_giveback_to_close_abs)))
            if math.isfinite(row_giveback_to_close_bps):
                giveback_to_close_bps_values.append(max(0.0, float(row_giveback_to_close_bps)))
            if math.isfinite(row_giveback_to_close_pct):
                giveback_to_close_pct_values.append(max(0.0, float(row_giveback_to_close_pct)))
            if math.isfinite(row_mfe_bps) and math.isfinite(row_giveback_to_close_bps):
                kept_bps = max(0.0, float(row_mfe_bps) - float(row_giveback_to_close_bps))
                kept_bps_values.append(float(kept_bps))
                kept_pct_of_mfe_values.append((float(kept_bps) / max(float(row_mfe_bps), 1e-12)) if float(row_mfe_bps) > 0.0 else 0.0)
            if math.isfinite(row_mfe_bps) and math.isfinite(row_mae_bps):
                fav_adv_ratio_values.append(float(row_mfe_bps) / max(float(row_mae_bps), 1e-12))

    def _max_dd_from_equity(eq_list: list[float]) -> float:
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

    def _sharpe_like(eq_list: list[float], rf: float) -> float:
        if len(eq_list) < 2:
            return 0.0
        rets: list[float] = []
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

    def _percentile(values: list[float], q: float) -> float:
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
                "sharpe_like": float(_sharpe_like(eq_list, float(risk_free_rate))),
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
    metric_defaults: dict[str, float] = {
        "mae_max_abs": max([float(v) for v in mae_abs_values], default=0.0),
        "mae_max_bps": max([float(v) for v in mae_bps_values], default=0.0),
        "mae_p50_abs": float(_percentile(mae_abs_values, 0.50)),
        "mae_p90_abs": float(_percentile(mae_abs_values, 0.90)),
        "mae_p99_abs": float(_percentile(mae_abs_values, 0.99)),
        "mae_p50_bps": float(_percentile(mae_bps_values, 0.50)),
        "mae_p90_bps": float(_percentile(mae_bps_values, 0.90)),
        "mae_p99_bps": float(_percentile(mae_bps_values, 0.99)),
        "mfe_max_abs": max([float(v) for v in mfe_abs_values], default=0.0),
        "mfe_max_bps": max([float(v) for v in mfe_bps_values], default=0.0),
        "mfe_p50_abs": float(_percentile(mfe_abs_values, 0.50)),
        "mfe_p90_abs": float(_percentile(mfe_abs_values, 0.90)),
        "mfe_p99_abs": float(_percentile(mfe_abs_values, 0.99)),
        "mfe_p50_bps": float(_percentile(mfe_bps_values, 0.50)),
        "mfe_p90_bps": float(_percentile(mfe_bps_values, 0.90)),
        "mfe_p99_bps": float(_percentile(mfe_bps_values, 0.99)),
        "giveback_max_abs": max([float(v) for v in giveback_max_abs_values], default=0.0),
        "giveback_max_bps": max([float(v) for v in giveback_max_bps_values], default=0.0),
        "giveback_max_pct_of_mfe": max([float(v) for v in giveback_max_pct_values], default=0.0),
        "giveback_max_p50_abs": float(_percentile(giveback_max_abs_values, 0.50)),
        "giveback_max_p90_abs": float(_percentile(giveback_max_abs_values, 0.90)),
        "giveback_max_p99_abs": float(_percentile(giveback_max_abs_values, 0.99)),
        "giveback_max_p50_bps": float(_percentile(giveback_max_bps_values, 0.50)),
        "giveback_max_p90_bps": float(_percentile(giveback_max_bps_values, 0.90)),
        "giveback_max_p99_bps": float(_percentile(giveback_max_bps_values, 0.99)),
        "giveback_max_p50_pct_of_mfe": float(_percentile(giveback_max_pct_values, 0.50)),
        "giveback_max_p90_pct_of_mfe": float(_percentile(giveback_max_pct_values, 0.90)),
        "giveback_max_p99_pct_of_mfe": float(_percentile(giveback_max_pct_values, 0.99)),
        "giveback_to_close_abs": max([float(v) for v in giveback_to_close_abs_values], default=0.0),
        "giveback_to_close_bps": max([float(v) for v in giveback_to_close_bps_values], default=0.0),
        "giveback_to_close_pct_of_mfe": max([float(v) for v in giveback_to_close_pct_values], default=0.0),
        "giveback_to_close_p50_abs": float(_percentile(giveback_to_close_abs_values, 0.50)),
        "giveback_to_close_p90_abs": float(_percentile(giveback_to_close_abs_values, 0.90)),
        "giveback_to_close_p99_abs": float(_percentile(giveback_to_close_abs_values, 0.99)),
        "giveback_to_close_p50_bps": float(_percentile(giveback_to_close_bps_values, 0.50)),
        "giveback_to_close_p90_bps": float(_percentile(giveback_to_close_bps_values, 0.90)),
        "giveback_to_close_p99_bps": float(_percentile(giveback_to_close_bps_values, 0.99)),
        "giveback_to_close_p50_pct_of_mfe": float(_percentile(giveback_to_close_pct_values, 0.50)),
        "giveback_to_close_p90_pct_of_mfe": float(_percentile(giveback_to_close_pct_values, 0.90)),
        "giveback_to_close_p99_pct_of_mfe": float(_percentile(giveback_to_close_pct_values, 0.99)),
        "kept_p50_bps": float(_percentile(kept_bps_values, 0.50)),
        "kept_p90_bps": float(_percentile(kept_bps_values, 0.90)),
        "kept_p99_bps": float(_percentile(kept_bps_values, 0.99)),
        "kept_pct_of_mfe_p50": float(_percentile(kept_pct_of_mfe_values, 0.50)),
        "kept_pct_of_mfe_p90": float(_percentile(kept_pct_of_mfe_values, 0.90)),
        "kept_pct_of_mfe_p99": float(_percentile(kept_pct_of_mfe_values, 0.99)),
        "fav_adv_ratio_p50": float(_percentile(fav_adv_ratio_values, 0.50)),
        "fav_adv_ratio_p90": float(_percentile(fav_adv_ratio_values, 0.90)),
        "fav_adv_ratio_p99": float(_percentile(fav_adv_ratio_values, 0.99)),
    }
    for _k, _v in metric_defaults.items():
        if _k not in overall:
            overall[_k] = float(_v)
    if "exit_hint" not in overall:
        overall["exit_hint"] = ""
    try:
        if int(overall.get("trades", 0) or 0) > 0:
            mae_max_now = float(overall.get("mae_max_abs", 0.0) or 0.0)
            mfe_max_now = float(overall.get("mfe_max_abs", 0.0) or 0.0)
            if mae_max_now <= 0.0 and mfe_max_now <= 0.0:
                logger.warning(
                    "[REPORT_METRIC_DIAG] exchange_id=%s mode=%s symbol=%s sample_entry=%s sample_qty=%s sample_direction=%s sample_high=%s sample_low=%s",
                    str(_resolve_exchange_id()),
                    str(mode),
                    str((sample_row or {}).get("symbol", "")),
                    str((sample_row or {}).get("entry", "")),
                    str((sample_row or {}).get("qty", "")),
                    str((sample_row or {}).get("direction", "")),
                    str((sample_row or {}).get("high", "")),
                    str((sample_row or {}).get("low", "")),
                )
    except Exception:
        pass
    try:
        trades_now = int(overall.get("trades", 0) or 0)
        max_dd_now = float(overall.get("max_dd", 0.0) or 0.0)
        max_dd_worst_now = float(overall.get("max_dd_worst_bar", 0.0) or 0.0)
        mae_max_now = float(overall.get("mae_max_abs", 0.0) or 0.0)
        init_eq_now = float(eq_all[0]) if eq_all else 0.0
        reasons: list[str] = []
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

    out = {
        "meta": {
            "build_id": str(build_id),
            "preset": str(preset_name or ""),
            "year": int(year) if year else None,
            "mode": str(mode),
            "include_yearly": bool(include_yearly),
            "include_monthly": bool(include_monthly),
        },
        "overall": overall,
        "yearly": yearly if include_yearly else {},
    }
    out_dir = os.path.dirname(str(out_path))
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    logger.info("[REPORT] wrote: %s", str(out_path))
    return True

def _risk_allow_new_entry(store: StateStore, ts_ms: int | None = None) -> tuple[bool, str]:
    """Return (allowed, reason). Uses daily_metrics + weekly KV to persist state across restarts."""
    if not bool(getattr(C, "RISK_CONTROLS_ENABLED", True)):
        return True, ""

    now_ms = int(ts_ms) if ts_ms is not None else int(_now_epoch() * 1000.0)
    day_key = _risk_day_key_from_ts_ms(now_ms)
    payload = store.get_daily_metrics(day_key) or {}
    risk = payload.get("risk") if isinstance(payload, dict) else None
    if not isinstance(risk, dict):
        risk = {}

    # =========================
    # WEEKLY stop-loss (rest-of-week halt)
    # =========================
    week_key = _risk_week_key_from_ts_ms(now_ms)
    w_raw = store.get_kv(_kv_key_risk_weekly(week_key), "")
    weekly = {}
    if w_raw:
        try:
            obj = json.loads(w_raw)
            weekly = obj if isinstance(obj, dict) else {}
        except Exception:
            weekly = {}

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
            weekly["weekly_pnl_jpy"] = float(weekly_pnl)
            store.set_kv(_kv_key_risk_weekly(week_key), json.dumps(weekly, ensure_ascii=False))
            return False, str(weekly["halt_reason"])

    if bool(risk.get("halted", False)):
        return False, str(risk.get("halt_reason") or "risk_halted")

    # daily pnl based circuit-breaker (JPY)
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
            payload["risk"] = risk
            store.record_daily_metrics(day_key, payload)
            return False, str(risk["halt_reason"])

    # loss streak circuit-breaker
    max_streak = int(getattr(C, "RISK_MAX_CONSECUTIVE_LOSSES", 0) or 0)
    if max_streak and max_streak > 0:
        streak = int(risk.get("loss_streak", 0) or 0)
        if streak >= max_streak:
            risk["halted"] = True
            risk["halt_reason"] = f"max_consecutive_losses_hit(streak={streak}>=max={max_streak})"
            payload["risk"] = risk
            store.record_daily_metrics(day_key, payload)
            return False, str(risk["halt_reason"])

    return True, ""


def _risk_on_trade_closed(store: StateStore, ts_exit_ms: int, net_jpy: float) -> None:
    if not bool(getattr(C, "RISK_CONTROLS_ENABLED", True)):
        return
    day_key = _risk_day_key_from_ts_ms(int(ts_exit_ms))
    payload = store.get_daily_metrics(day_key) or {}
    if not isinstance(payload, dict):
        payload = {}
    risk = payload.get("risk")
    if not isinstance(risk, dict):
        risk = {}

    daily_pnl = float(risk.get("daily_pnl_jpy", 0.0) or 0.0) + float(net_jpy)
    risk["daily_pnl_jpy"] = float(daily_pnl)

    trades_n = int(risk.get("trades", 0) or 0) + 1
    risk["trades"] = int(trades_n)

    if float(net_jpy) < 0:
        risk["loss_streak"] = int(risk.get("loss_streak", 0) or 0) + 1
    else:
        risk["loss_streak"] = 0

    payload["risk"] = risk
    store.record_daily_metrics(day_key, payload)

    # weekly pnl update (KV, per ISO week)
    try:
        week_key = _risk_week_key_from_ts_ms(int(ts_exit_ms))
        w_key = _kv_key_risk_weekly(week_key)
        w_raw = store.get_kv(w_key, "")
        weekly = {}
        if w_raw:
            try:
                obj = json.loads(w_raw)
                weekly = obj if isinstance(obj, dict) else {}
            except Exception:
                weekly = {}
        weekly_pnl = float(weekly.get("weekly_pnl_jpy", 0.0) or 0.0) + float(net_jpy)
        weekly["weekly_pnl_jpy"] = float(weekly_pnl)
        # NOTE: halted flag is evaluated on next entry attempt (allow_new_entry)
        store.set_kv(w_key, json.dumps(weekly, ensure_ascii=False))
    except Exception:
        pass

def _record_trade_row(
    store: StateStore,
    res: dict[str, Any],
    exit_reason: str,
    net: float,
    stop: float | None,
    tp: float | None,
    stop_kind: str = "",
) -> None:
    symbol = str(res.get("symbol") or "")
    try:
        def _f(v: Any, default: float = 0.0) -> float:
            try:
                x = float(v)
                if not math.isfinite(x):
                    return float(default)
                return float(x)
            except Exception:
                return float(default)

        meta = _get_pos_meta(store, symbol)
        meta_public = {
            str(k): v for k, v in dict(meta or {}).items()
            if str(k or "") and (not str(k).startswith("_"))
        }
        ts = int(res.get("candle_ts_exit") or 0)
        direction = str(meta.get("direction", "long") or "long").strip().lower()
        if direction in ("sell", "short"):
            direction = "short"
        else:
            direction = "long"
        entry_raw = max(0.0, _f(meta.get("entry_raw", res.get("entry", 0.0)), 0.0))
        exit_raw = _f(res.get("exit", res.get("entry", 0.0)), 0.0)
        qty = max(0.0, abs(_f(res.get("qty", 0.0), 0.0)))
        max_fav = _f(meta.get("max_fav", _get_pos_max_fav(store, symbol)), float("nan"))
        min_adv = _f(meta.get("min_adv", _get_pos_min_adv(store, symbol)), float("nan"))
        if not math.isfinite(max_fav):
            max_fav = float(entry_raw)
        if not math.isfinite(min_adv):
            min_adv = float(entry_raw)

        mae_abs = _f(meta.get("mae_abs"), float("nan"))
        mfe_abs = _f(meta.get("mfe_abs"), float("nan"))
        giveback_max_abs = _f(meta.get("giveback_max_abs"), float("nan"))
        giveback_to_close_abs = 0.0
        if qty > 0.0 and entry_raw > 0.0:
            if not math.isfinite(mae_abs):
                if direction == "short":
                    mae_abs = max(0.0, (float(max_fav) - float(entry_raw)) * float(qty))
                else:
                    mae_abs = max(0.0, (float(entry_raw) - float(min_adv)) * float(qty))
            if not math.isfinite(mfe_abs):
                if direction == "short":
                    mfe_abs = max(0.0, (float(entry_raw) - float(min_adv)) * float(qty))
                else:
                    mfe_abs = max(0.0, (float(max_fav) - float(entry_raw)) * float(qty))
            if not math.isfinite(giveback_max_abs):
                if direction == "short":
                    if float(min_adv) < float(entry_raw):
                        giveback_max_abs = max(0.0, (float(max_fav) - float(min_adv)) * float(qty))
                    else:
                        giveback_max_abs = 0.0
                else:
                    if float(max_fav) > float(entry_raw):
                        giveback_max_abs = max(0.0, (float(max_fav) - float(min_adv)) * float(qty))
                    else:
                        giveback_max_abs = 0.0
            mae_abs = max(0.0, float(mae_abs))
            mfe_abs = max(0.0, float(mfe_abs))
            giveback_max_abs = max(0.0, float(giveback_max_abs))
            if direction == "short":
                if float(min_adv) < float(entry_raw):
                    giveback_to_close_abs = max(0.0, (float(exit_raw) - float(min_adv)) * float(qty))
            else:
                if float(max_fav) > float(entry_raw):
                    giveback_to_close_abs = max(0.0, (float(max_fav) - float(exit_raw)) * float(qty))
        entry_notional = float(entry_raw) * float(qty)
        mae_bps = (float(mae_abs) / float(entry_notional) * 10000.0) if float(entry_notional) > 0.0 else 0.0
        mfe_bps = (float(mfe_abs) / float(entry_notional) * 10000.0) if float(entry_notional) > 0.0 else 0.0
        giveback_max_bps = (float(giveback_max_abs) / float(entry_notional) * 10000.0) if float(entry_notional) > 0.0 else 0.0
        giveback_to_close_bps = (float(giveback_to_close_abs) / float(entry_notional) * 10000.0) if float(entry_notional) > 0.0 else 0.0
        if float(mfe_abs) > 0.0:
            giveback_max_pct_of_mfe = float(giveback_max_abs) / float(max(float(mfe_abs), 1e-12))
            giveback_to_close_pct_of_mfe = float(giveback_to_close_abs) / float(max(float(mfe_abs), 1e-12))
        else:
            giveback_max_pct_of_mfe = 0.0
            giveback_to_close_pct_of_mfe = 0.0
        entry_exec = _f(res.get("entry", meta.get("entry_exec", entry_raw)), 0.0)
        init_stop = _f(
            meta.get("init_stop", _get_pos_init_stop(store, symbol, fallback=(stop if stop is not None else entry_raw))),
            entry_raw,
        )
        final_stop = _f(stop if stop is not None else meta.get("final_stop", init_stop), init_stop)
        row_stop_kind = str(stop_kind or meta.get("stop_kind") or "init")

        row = {
            "ts": ts,
            "ts_iso": _iso_utc(ts) if ts else None,
            "symbol": symbol,
            "reason": str(exit_reason),
            "entry": res.get("entry"),
            "exit": res.get("exit"),
            "qty": res.get("qty"),
            "stop": stop,
            "tp": tp,
            "pnl": res.get("pnl"),
            "fee": res.get("fee"),
            "net": float(net),
            "direction": str(direction),
            "rr0": meta.get("rr0"),
            "rr_adj": meta.get("rr_adj"),
            "tp_bps_raw": meta.get("tp_bps_raw"),
            "sl_bps_raw": meta.get("sl_bps_raw"),
            "entry_raw": float(entry_raw),
            "stop_raw": meta.get("stop_raw"),
            "tp_raw": meta.get("tp_raw"),
            "entry_exec": float(entry_exec),
            "exit_exec": _f(res.get("exit", 0.0), 0.0),
            "stop_kind": str(row_stop_kind),
        }
        # future-proof: if trade_meta grows, automatically carry extra columns into trades.csv
        if meta_public:
            row.update(meta_public)
        row["direction"] = str(direction)
        row["entry_raw"] = float(entry_raw)
        row["exit_raw"] = float(exit_raw)
        row["max_fav"] = float(max_fav)
        row["min_adv"] = float(min_adv)
        row["mae_abs"] = float(mae_abs)
        row["mae_bps"] = float(mae_bps)
        row["mfe_abs"] = float(mfe_abs)
        row["mfe_bps"] = float(mfe_bps)
        row["giveback_max_abs"] = float(giveback_max_abs)
        row["giveback_max_bps"] = float(giveback_max_bps)
        row["giveback_max_pct_of_mfe"] = float(giveback_max_pct_of_mfe)
        row["giveback_to_close_abs"] = float(giveback_to_close_abs)
        row["giveback_to_close_bps"] = float(giveback_to_close_bps)
        row["giveback_to_close_pct_of_mfe"] = float(giveback_to_close_pct_of_mfe)
        row.update(
            _build_stop_diag_fields(
                stop_kind=row_stop_kind,
                init_stop=init_stop,
                final_stop=final_stop,
                entry_exec=entry_exec,
                trail_eval_count=meta.get("trail_eval_count"),
                trail_candidate_stop_last=meta.get("trail_candidate_stop_last"),
                trail_candidate_stop_max=meta.get("trail_candidate_stop_max"),
                trail_candidate_minus_current_stop=meta.get("trail_candidate_minus_current_stop"),
                trail_candidate_minus_current_stop_max=meta.get("trail_candidate_minus_current_stop_max"),
                trail_candidate_from_atr_last=meta.get("trail_candidate_from_atr_last"),
                trail_candidate_from_bps_last=meta.get("trail_candidate_from_bps_last"),
                trail_eligible_count=meta.get("trail_eligible_count"),
                trail_update_count=meta.get("trail_update_count"),
                trail_block_reason_last=meta.get("trail_block_reason_last"),
                trail_block_reason_max=meta.get("trail_block_reason_max"),
                start_price=meta.get("start_price"),
                trail_start_price_last=meta.get("trail_start_price_last"),
                trail_start_price_max_context=meta.get("trail_start_price_max_context"),
                trail_bar_high_last=meta.get("trail_bar_high_last"),
                trail_bar_high_max=meta.get("trail_bar_high_max"),
                trail_pos_stop_before_last=meta.get("trail_pos_stop_before_last"),
                trail_pos_stop_before_max_context=meta.get("trail_pos_stop_before_max_context"),
                trail_risk_per_unit_last=meta.get("trail_risk_per_unit_last"),
                trail_mode_last=meta.get("trail_mode_last"),
                mfe_bps=mfe_bps,
                giveback_to_close_bps=giveback_to_close_bps,
            )
        )
        _append_trade_csv(row)
        _risk_on_trade_closed(store, ts, float(net))
    except Exception:
        pass
    finally:
        if symbol:
            _clear_pos_meta(store, symbol)
            _clear_pos_tp1_done(store, symbol)
            _clear_pos_init_qty(store, symbol)
            _clear_pos_init_stop(store, symbol)
            _clear_pos_max_fav(store, symbol)
            _clear_pos_min_adv(store, symbol)

def _write_stop_file(mode: str) -> None:
    base = str(getattr(C, "STOP_FILE_DIR", "."))
    os.makedirs(base, exist_ok=True)
    path = os.path.join(base, mode)
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"{mode} {datetime.now().isoformat()}\n")


def _summary_enabled() -> bool:
    return bool(getattr(C, "RUN_SUMMARY_ENABLED", True))


def _top_n() -> int:
    return int(getattr(C, "RUN_SUMMARY_TOP_N", 12))


def _runtime_signal_log_enabled(*, mode: str, is_replay: bool) -> bool:
    return (not bool(is_replay)) and str(mode or "").upper() in ("PAPER", "LIVE")


def _fmt_runtime_log_value(value: Any) -> str:
    try:
        text = f"{float(value):.8f}".rstrip("0").rstrip(".")
    except Exception:
        return str(value)
    if text in ("", "-0"):
        return "0"
    return text


def _trim_runtime_reason(reason: str, max_len: int = 120) -> str:
    text = str(reason or "").strip() or "(no_reason)"
    if len(text) > int(max_len):
        return text[: int(max_len)]
    return text


def _signal_reason_capture_begin() -> None:
    global _SIGNAL_REASON_COLLECTOR, _SIGNAL_REASON_COLLECTOR_SEEN
    _SIGNAL_REASON_COLLECTOR = []
    _SIGNAL_REASON_COLLECTOR_SEEN = set()


def _signal_reason_capture_append(reason: str) -> None:
    global _SIGNAL_REASON_COLLECTOR, _SIGNAL_REASON_COLLECTOR_SEEN
    if _SIGNAL_REASON_COLLECTOR is None or _SIGNAL_REASON_COLLECTOR_SEEN is None:
        return
    text = _trim_runtime_reason(reason)
    if text in _SIGNAL_REASON_COLLECTOR_SEEN:
        return
    if len(_SIGNAL_REASON_COLLECTOR) >= 6:
        return
    _SIGNAL_REASON_COLLECTOR.append(text)
    _SIGNAL_REASON_COLLECTOR_SEEN.add(text)


def _signal_reason_capture_end() -> list[str]:
    global _SIGNAL_REASON_COLLECTOR, _SIGNAL_REASON_COLLECTOR_SEEN
    out = list(_SIGNAL_REASON_COLLECTOR or [])
    _SIGNAL_REASON_COLLECTOR = None
    _SIGNAL_REASON_COLLECTOR_SEEN = None
    return out


def _signal_reason_preview(reasons: list[str], limit: int = 3) -> str:
    out: list[str] = []
    for item in list(reasons or []):
        text = _trim_runtime_reason(item, max_len=96)
        if text in out:
            continue
        out.append(text)
        if len(out) >= int(limit):
            break
    return " | ".join(out) if out else "(none)"


def _log_latest_confirmed_bar(
    *,
    tag: str,
    symbol: str,
    tf_entry: str,
    bar_ts_ms: int,
    bar_open: float,
    bar_high: float,
    bar_low: float,
    bar_close: float,
) -> None:
    logger.info(
        "[LATEST_BAR] mode=%s symbol=%s tf=%s ts=%s open=%s high=%s low=%s close=%s",
        str(tag),
        str(symbol),
        str(tf_entry),
        _iso_utc(int(bar_ts_ms)),
        _fmt_runtime_log_value(bar_open),
        _fmt_runtime_log_value(bar_high),
        _fmt_runtime_log_value(bar_low),
        _fmt_runtime_log_value(bar_close),
    )


def _record_runtime_signal_summary(
    *,
    symbol: str,
    regime: str,
    bar_ts_ms: int,
    final_action: str,
    signal_reason: str,
    hold_reasons: list[str],
) -> None:
    summary = _RUNTIME_SIGNAL_SUMMARY
    summary["signal_evaluations"] = int(summary.get("signal_evaluations", 0) or 0) + 1
    action_l = str(final_action or "hold").lower()
    if action_l == "buy":
        summary["buy_count"] = int(summary.get("buy_count", 0) or 0) + 1
        reason_text = _trim_runtime_reason(signal_reason, max_len=200)
    else:
        summary["hold_count"] = int(summary.get("hold_count", 0) or 0) + 1
        reason_text = _signal_reason_preview(hold_reasons, limit=3)
        hold_counter = summary.get("hold_reasons")
        if not isinstance(hold_counter, Counter):
            hold_counter = Counter()
            summary["hold_reasons"] = hold_counter
        for item in list(hold_reasons or []):
            hold_counter[str(item)] += 1
        if reason_text == "(none)":
            reason_text = _trim_runtime_reason(signal_reason, max_len=200)
    summary["last_signal_ts"] = int(bar_ts_ms)
    summary["last_signal_action"] = str(action_l)
    summary["last_signal_reason"] = str(reason_text)
    summary["last_signal_symbol"] = str(symbol)
    summary["last_signal_regime"] = str(regime)


def _emit_runtime_signal_outcome(
    *,
    enabled: bool,
    tag: str,
    symbol: str,
    tf_entry: str,
    bar_ts_ms: int,
    regime: str,
    direction: str,
    close_last: float,
    final_action: str,
    signal_action: str,
    signal_reason: str,
    entry: float | None = None,
    stop_price: float | None = None,
    tp_price: float | None = None,
) -> None:
    hold_reasons = _signal_reason_capture_end()
    if not bool(enabled):
        return
    action_l = str(final_action or "hold").lower()
    signal_action_s = str(signal_action or "hold").lower()
    signal_reason_s = _trim_runtime_reason(signal_reason, max_len=96)
    if action_l == "buy":
        logger.info(
            "[SIGNAL] mode=%s symbol=%s tf=%s ts=%s action=buy regime=%s dir=%s close=%s reason=%s entry=%s stop=%s tp=%s",
            str(tag),
            str(symbol),
            str(tf_entry),
            _iso_utc(int(bar_ts_ms)),
            str(regime),
            str(direction),
            _fmt_runtime_log_value(close_last),
            signal_reason_s,
            _fmt_runtime_log_value(entry),
            _fmt_runtime_log_value(stop_price),
            _fmt_runtime_log_value(tp_price),
        )
    else:
        logger.info(
            "[HOLD] mode=%s symbol=%s tf=%s ts=%s action=hold signal=%s regime=%s dir=%s close=%s signal_reason=%s reasons=%s",
            str(tag),
            str(symbol),
            str(tf_entry),
            _iso_utc(int(bar_ts_ms)),
            str(signal_action_s),
            str(regime),
            str(direction),
            _fmt_runtime_log_value(close_last),
            signal_reason_s,
            _signal_reason_preview(hold_reasons, limit=3),
        )
    _record_runtime_signal_summary(
        symbol=str(symbol),
        regime=str(regime),
        bar_ts_ms=int(bar_ts_ms),
        final_action=str(action_l),
        signal_reason=str(signal_reason),
        hold_reasons=hold_reasons,
    )


# Keep RUN SUMMARY logs readable in replay/live loops.
# In replay-engine=live (ReplayExchange) we may call main() many times; printing
# the full summary every time floods logs. We only print when trade-related
# events happened (OPEN/CLOSE/TP/BE/TRAIL), unless RUN_SUMMARY_ON_HOLD is set.
_LAST_RUN_SUMMARY_SNAPSHOT = None


def _maybe_log_run_summary(summ_regime, summ_evt, summ_hold) -> None:
    global _LAST_RUN_SUMMARY_SNAPSHOT

    if not _summary_enabled():
        return

    regime_dict = dict(summ_regime) if summ_regime is not None else {}
    evt_dict = dict(summ_evt) if summ_evt is not None else {}
    hold_top = summ_hold.most_common(_top_n()) if summ_hold is not None else []

    action_keys = {"OPEN", "CLOSE", "TP1_PARTIAL", "BE_SET", "TRAIL_SET"}
    has_action = any((k in action_keys) and (int(v) > 0) for k, v in evt_dict.items())

    snap = {"regime": regime_dict, "evt": evt_dict, "hold_top": hold_top}

    # Suppress if no trade actions and user didn't opt-in.
    if (not has_action) and (not bool(getattr(C, "RUN_SUMMARY_ON_HOLD", False))):
        _LAST_RUN_SUMMARY_SNAPSHOT = snap
        return

    # Suppress duplicates (tight replay loops).
    if _LAST_RUN_SUMMARY_SNAPSHOT == snap:
        return
    _LAST_RUN_SUMMARY_SNAPSHOT = snap

    logger.info("===== RUN SUMMARY =====")
    if regime_dict:
        logger.info(f"REGIME_COUNTS: {regime_dict}")
    if evt_dict:
        logger.info(f"EVENTS: {evt_dict}")
    if summ_hold is not None:
        logger.info(f"HOLD REASONS TOP{_top_n()}: {hold_top}")
    logger.info("=======================")


def _write_runtime_run_summary(*, mode: str, dryrun: bool, symbols_count: int, summ_evt: Counter, summ_regime: Counter) -> None:
    hold_counter = _RUNTIME_SIGNAL_SUMMARY.get("hold_reasons")
    if not isinstance(hold_counter, Counter):
        hold_counter = Counter()
        _RUNTIME_SIGNAL_SUMMARY["hold_reasons"] = hold_counter
    try:
        path = _export_path(str(getattr(C, "DIFF_EXPORT_FILENAME", "run_summary.json") or "run_summary.json"))
        last_signal_ts = int(_RUNTIME_SIGNAL_SUMMARY.get("last_signal_ts", 0) or 0)
        payload = {
            "ts": int(datetime.now().timestamp()),
            "mode": str(mode),
            "dryrun": bool(dryrun),
            "events": dict(summ_evt or {}),
            "regime_counts": dict(summ_regime or {}),
            "symbols_count": int(symbols_count),
            "signal_evaluations": int(_RUNTIME_SIGNAL_SUMMARY.get("signal_evaluations", 0) or 0),
            "buy_count": int(_RUNTIME_SIGNAL_SUMMARY.get("buy_count", 0) or 0),
            "hold_count": int(_RUNTIME_SIGNAL_SUMMARY.get("hold_count", 0) or 0),
            "hold_reasons_top": hold_counter.most_common(_top_n()),
            "last_signal_ts": int(last_signal_ts),
            "last_signal_ts_iso": (_iso_utc(int(last_signal_ts)) if int(last_signal_ts) > 0 else ""),
            "last_signal_action": str(_RUNTIME_SIGNAL_SUMMARY.get("last_signal_action", "") or ""),
            "last_signal_reason": str(_RUNTIME_SIGNAL_SUMMARY.get("last_signal_reason", "") or ""),
            "last_signal_symbol": str(_RUNTIME_SIGNAL_SUMMARY.get("last_signal_symbol", "") or ""),
            "last_signal_regime": str(_RUNTIME_SIGNAL_SUMMARY.get("last_signal_regime", "") or ""),
            "build_id": str(BUILD_ID),
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        logger.info(f"[EXPORT] run summary -> {path}")
    except Exception as e:
        logger.warning(f"[EXPORT] failed: {e}")


def _maybe_log_ttl_stats(force: bool = False) -> None:
    global _TTL_STATS_LAST_LOG_SEC
    now_sec = float(time.time())
    interval_override = _TTL_STATS_INTERVAL_SEC_OVERRIDE
    if interval_override is None:
        interval_sec = float(getattr(C, "TTL_STATS_LOG_INTERVAL_SEC", 60.0) or 60.0)
    else:
        if float(interval_override) <= 0.0:
            return
        interval_sec = float(interval_override)
    if interval_sec <= 0.0:
        interval_sec = 60.0
    if (not bool(force)) and ((now_sec - float(_TTL_STATS_LAST_LOG_SEC)) < float(interval_sec)):
        return
    _TTL_STATS_LAST_LOG_SEC = float(now_sec)
    maker_orders = int(_ttl_stats.get("maker_orders", 0) or 0)
    maker_filled = int(_ttl_stats.get("maker_filled", 0) or 0)
    ttl_expired = int(_ttl_stats.get("ttl_expired", 0) or 0)
    fill_time_ms_total = float(_ttl_stats.get("fill_time_ms_total", 0) or 0.0)
    fill_rate = (float(maker_filled) / float(maker_orders)) if maker_orders > 0 else 0.0
    avg_fill_ms = (fill_time_ms_total / float(maker_filled)) if maker_filled > 0 else 0.0
    logger.info(
        "[TTL_STATS] orders=%d filled=%d expired=%d fill_rate=%.3f avg_fill_ms=%.1f",
        int(maker_orders),
        int(maker_filled),
        int(ttl_expired),
        float(fill_rate),
        float(avg_fill_ms),
    )


def _summ_inc(counter: Counter | None, key: str, n: int = 1) -> None:
    if counter is None:
        return
    counter[key] += int(n)


def _summ_reason(counter: Counter | None, reason: str) -> None:
    if counter is None:
        return
    r = (reason or "").strip() or "(no_reason)"
    if len(r) > 140:
        r = r[:140] + "..."
    _signal_reason_capture_append(r)
    counter[r] += 1


def _iso_week_key(dt: datetime) -> str:
    y, w, _ = dt.isocalendar()
    return f"{y}-W{w:02d}"


# ---------------------------
# TIME WINDOW (JST)
# ---------------------------

def _in_time_window_jst() -> tuple[bool, str]:
    if not bool(getattr(C, "TIME_WINDOW_ENABLED", False)):
        return True, "time_window_off"

    now = datetime.now(timezone(timedelta(hours=9)))  # JST
    cur = now.time()

    windows = getattr(C, "TIME_WINDOW_JST", [])
    if not windows:
        return True, "time_window_empty"

    def parse(hm: str):
        h, m = hm.split(":")
        return int(h), int(m)

    for a, b in windows:
        sh, sm = parse(a)
        eh, em = parse(b)
        start = (sh, sm)
        end = (eh, em)
        cur_hm = (cur.hour, cur.minute)

        if start <= end:
            if start <= cur_hm <= end:
                return True, "time_window_ok"
        else:
            # across midnight
            if cur_hm >= start or cur_hm <= end:
                return True, "time_window_ok"

    return False, f"outside_time_window_jst now={cur}"


# ---------------------------
# Maker health guard
# ---------------------------

def _maker_block_key(kind: str, symbol: str) -> str:
    return f"maker_block_until:{kind}:{symbol}"


def _maker_streak_key(kind: str, symbol: str) -> str:
    return f"maker_nofill_streak:{kind}:{symbol}"


def _is_maker_blocked(store: StateStore, kind: str, symbol: str) -> tuple[bool, str]:
    if not bool(getattr(C, "MAKER_HEALTH_GUARD_ENABLED", True)):
        return False, ""
    until = _kv_get_float(store, _maker_block_key(kind, symbol), 0.0)
    now = _now_epoch()
    if until and now < until:
        return True, f"{kind} maker blocked until={until:.0f} now={now:.0f}"
    return False, ""


def _record_maker_result(
    store: StateStore,
    kind: str,
    symbol: str,
    filled_qty: float,
    intended_qty: float,
    min_fill_ratio: float,
) -> tuple[bool, str]:
    if not bool(getattr(C, "MAKER_HEALTH_GUARD_ENABLED", True)):
        return False, "maker_guard_off"

    ratio = (float(filled_qty) / float(intended_qty)) if float(intended_qty) > 0 else 0.0
    ok = (filled_qty > 0) and (ratio >= float(min_fill_ratio))

    streak_key = _maker_streak_key(kind, symbol)
    streak = _kv_get_int(store, streak_key, 0)

    if ok:
        if streak != 0:
            _kv_set_int(store, streak_key, 0)
        return False, f"{kind} maker ok (fill_ratio={ratio:.2f})"

    streak += 1
    _kv_set_int(store, streak_key, streak)

    max_streak = int(getattr(C, f"{kind}_NOFILL_STREAK_MAX", 3))
    block_secs = int(getattr(C, f"{kind}_BLOCK_SECONDS", 180))

    if max_streak > 0 and streak >= max_streak:
        until = _now_epoch() + float(block_secs)
        _kv_set_float(store, _maker_block_key(kind, symbol), until)
        _kv_set_int(store, streak_key, 0)
        return True, (
            f"{kind} maker blocked: streak reached {streak}/{max_streak} -> "
            f"block {block_secs}s (fill_ratio={ratio:.2f})"
        )

    return False, f"{kind} maker nofill streak={streak}/{max_streak} (fill_ratio={ratio:.2f})"


# ---------------------------
# Equity sync
# ---------------------------

def _resolve_live_equity_currency() -> tuple[str, str]:
    cfg_cur = getattr(C, "LIVE_EQUITY_CURRENCY", None)
    cfg_cur_s = _normalize_ccy_key(cfg_cur)
    default_quote = _default_quote_for_runtime()
    if cfg_cur_s:
        return str(cfg_cur_s), "CONFIG"

    quotes: set[str] = set()
    for symbol in list(_LIVE_EQUITY_SYMBOLS or []):
        quote = _quote_ccy_from_symbol_no_fallback(symbol)
        if quote:
            quotes.add(str(quote))
    if len(quotes) == 1:
        return str(next(iter(quotes))), "AUTO_QUOTE"
    return _normalize_ccy_key(default_quote) or "USDT", "DEFAULT"


def _maybe_log_live_equity_currency_resolution(*, equity_ccy: str | None, source: str) -> None:
    global _LIVE_EQUITY_RESOLUTION_LAST
    ccy = _normalize_ccy_key(equity_ccy) or "UNKNOWN"
    symbol_label = _live_equity_symbols_label()
    state = (str(ccy), str(source), str(symbol_label))
    if _LIVE_EQUITY_RESOLUTION_LAST == state:
        return
    if str(source) == "AUTO_QUOTE":
        logger.info(
            "[LIVE_EQ] equity_ccy=%s source=%s symbols=%s detail=following current symbol quote selected via GUI/CLI",
            str(ccy),
            str(source),
            str(symbol_label),
        )
    else:
        logger.info(
            "[LIVE_EQ] equity_ccy=%s source=%s symbols=%s",
            str(ccy),
            str(source),
            str(symbol_label),
        )
    _LIVE_EQUITY_RESOLUTION_LAST = state


def _get_live_equity_info(ex: ExchangeClient) -> tuple[float | None, str]:
    """
    ExchangeClient is expected to provide get_total_equity(currency).
    Add the equivalent helper in exchange.py before enabling live equity sync.
    """
    global _LIVE_EQUITY_DIAG_LOGGED, _LIVE_EQUITY_LAST_CUR
    cur, cur_source = _resolve_live_equity_currency()
    _maybe_log_live_equity_currency_resolution(equity_ccy=cur, source=cur_source)
    _LIVE_EQUITY_LAST_CUR = str(cur)
    do_diag = not _LIVE_EQUITY_DIAG_LOGGED
    usdt_free = None
    usdt_total = None
    usdc_free = None
    usdc_total = None
    usd_free = None
    usd_total = None
    live_eq_for_log = None

    if do_diag:
        bal_diag = None
        try:
            if hasattr(ex, "fetch_balance"):
                bal_diag = ex.fetch_balance()
        except Exception:
            bal_diag = None
        if not isinstance(bal_diag, dict):
            try:
                ex_obj = getattr(ex, "ex", None)
                if ex_obj is not None and hasattr(ex_obj, "fetch_balance"):
                    bal_diag = ex_obj.fetch_balance()
            except Exception:
                bal_diag = None
        if isinstance(bal_diag, dict):
            free_bal = bal_diag.get("free", {}) or {}
            total_bal = bal_diag.get("total", {}) or {}

            def _pick_bal(d: Any, key: str) -> float | None:
                if not isinstance(d, dict):
                    return None
                if key not in d:
                    return None
                try:
                    return float(d.get(key))
                except Exception:
                    return None

            usdt_free = _pick_bal(free_bal, "USDT")
            usdt_total = _pick_bal(total_bal, "USDT")
            usdc_free = _pick_bal(free_bal, "USDC")
            usdc_total = _pick_bal(total_bal, "USDC")
            usd_free = _pick_bal(free_bal, "USD")
            usd_total = _pick_bal(total_bal, "USD")

    try:
        live_eq = float(ex.get_total_equity(cur))
        live_eq_for_log = float(live_eq)
        return float(live_eq), str(cur)
    except Exception as e:
        logger.warning(f"Failed to fetch live equity: {e} unit={cur} cur={cur}")
        return None, str(cur)
    finally:
        if do_diag:
            logger.info(
                "[LIVE_EQ_DIAG] cur=%s usdt=%s/%s usdc=%s/%s usd=%s/%s live_eq=%s unit=%s cur_source=%s",
                str(cur),
                str(usdt_free),
                str(usdt_total),
                str(usdc_free),
                str(usdc_total),
                str(usd_free),
                str(usd_total),
                str(live_eq_for_log),
                str(cur),
                str(cur_source),
            )
            _LIVE_EQUITY_DIAG_LOGGED = True


def _get_live_equity(ex: ExchangeClient) -> float | None:
    live_eq, _ = _get_live_equity_info(ex)
    return live_eq


def _symbols_signature(symbols: list[str]) -> str:
    vals = [str(s or "").strip() for s in symbols if str(s or "").strip()]
    if not vals:
        return "SYMBOL"
    if len(vals) == 1:
        return str(vals[0])
    joined = "+".join(vals[:4])
    return joined if len(joined) <= 48 else f"MULTI_{len(vals)}"


def _state_context_for_run(*, mode: str, symbols: list[str], replay_mode: bool) -> StateContext:
    symbol_sig = _symbols_signature(symbols)
    symbol_ref = str(symbols[0] if symbols else symbol_sig)
    quote = _quote_ccy_for_symbol(symbol_ref)
    base = _symbol_base_asset(symbol_ref)
    account_ccy = str(os.getenv("LWF_ACCOUNT_CCY", "") or "").strip().upper() or str(quote or _default_quote_for_runtime()).upper()
    settlement_ccy = str(os.getenv("LWF_SETTLEMENT_CCY", "") or "").strip().upper() or str(account_ccy)
    profile_name = str(os.getenv("BOT_STATE_PROFILE", "") or "").strip()
    run_mode = "REPLAY" if bool(replay_mode) else str(mode or "PAPER").upper()
    return build_state_context(
        exchange_id=_resolve_exchange_id(),
        market_type=str(os.getenv("LWF_MARKET_TYPE", "spot") or "spot"),
        run_mode=run_mode,
        symbol=symbol_sig,
        base_ccy=base,
        quote_ccy=quote,
        account_ccy=account_ccy,
        settlement_ccy=settlement_ccy,
        profile_name=profile_name,
    )


def _context_meta_payload(ctx: StateContext, paths: Any, *, symbols: list[str]) -> dict[str, str]:
    return {
        "context_id": str(context_id_for(ctx)),
        "exchange_id": str(ctx.exchange_id),
        "market_type": str(ctx.market_type),
        "run_mode": str(ctx.run_mode),
        "symbol": str(ctx.symbol),
        "symbols": ",".join(str(s) for s in symbols if str(s).strip()),
        "base_ccy": str(ctx.base_ccy),
        "quote_ccy": str(ctx.quote_ccy),
        "account_ccy": str(ctx.account_ccy),
        "settlement_ccy": str(ctx.settlement_ccy),
        "profile_name": str(ctx.profile_name or ""),
        "build_id": str(BUILD_ID),
        "db_path": str(getattr(paths, "db_path", "")),
        "meta_path": str(getattr(paths, "meta_path", "")),
    }


def _legacy_meta_matches_context(raw: dict[str, Any], ctx: StateContext) -> bool:
    if not isinstance(raw, dict):
        return False
    context_id = str(raw.get("context_id") or "").strip()
    if context_id:
        return context_id == str(context_id_for(ctx))
    checks = (
        ("exchange_id", str(ctx.exchange_id)),
        ("market_type", str(ctx.market_type)),
        ("run_mode", str(ctx.run_mode)),
        ("symbol", str(ctx.symbol)),
        ("account_ccy", str(ctx.account_ccy)),
        ("settlement_ccy", str(ctx.settlement_ccy)),
    )
    found = 0
    for key, expected in checks:
        val = str(raw.get(key) or "").strip()
        if not val:
            continue
        found += 1
        if val != expected:
            return False
    return found >= 3


def _copy_table_if_exists(src_conn: Any, dst_conn: Any, table: str) -> int:
    try:
        src_cols = [str(r[1]) for r in src_conn.execute(f"PRAGMA table_info({table})").fetchall()]
        dst_cols = [str(r[1]) for r in dst_conn.execute(f"PRAGMA table_info({table})").fetchall()]
        cols = [c for c in src_cols if c in dst_cols]
        if not cols:
            return 0
        sql_sel = f"SELECT {', '.join(cols)} FROM {table}"
        rows = src_conn.execute(sql_sel).fetchall()
        if not rows:
            return 0
        placeholders = ", ".join(["?"] * len(cols))
        sql_ins = f"INSERT OR REPLACE INTO {table}({', '.join(cols)}) VALUES ({placeholders})"
        payload = [tuple(row[c] if isinstance(row, dict) or hasattr(row, "keys") else row[idx] for idx, c in enumerate(cols)) for row in rows]
        dst_conn.executemany(sql_ins, payload)
        return int(len(payload))
    except Exception:
        return 0


def _maybe_migrate_legacy_state(store: StateStore, ctx: StateContext, paths: Any) -> None:
    legacy_db = str(getattr(paths, "legacy_db_path", "") or "")
    legacy_paper = str(getattr(paths, "legacy_paper_equity_path", "") or "")
    target_db = str(getattr(paths, "db_path", "") or "")

    if legacy_db and os.path.exists(legacy_db) and os.path.abspath(legacy_db) != os.path.abspath(target_db):
        legacy_store = None
        try:
            legacy_store = StateStore(db_path=legacy_db)
            legacy_meta = legacy_store.get_context_metadata()
            if not legacy_meta:
                logger.warning("[STATE_MIGRATE] legacy state ignored due to missing context metadata db=%s", legacy_db)
            elif not _legacy_meta_matches_context(legacy_meta, ctx):
                logger.warning(
                    "[STATE_MIGRATE] legacy db context mismatch legacy=%s current=%s -> ignored",
                    str(legacy_meta.get("context_id") or ""),
                    str(context_id_for(ctx)),
                )
            else:
                copied = 0
                for table in ("positions", "equity", "trade_plan", "stop_state", "weekly_base", "bot_kv", "daily_metrics", "equity_state"):
                    copied += int(_copy_table_if_exists(legacy_store.conn, store.conn, table))
                store.conn.commit()
                bak = f"{legacy_db}.migrated.bak"
                try:
                    legacy_store.close()
                    legacy_store = None
                    os.replace(legacy_db, bak)
                except Exception:
                    pass
                logger.info("[STATE_MIGRATE] imported legacy state.db rows=%s -> %s", int(copied), target_db)
        except Exception as e:
            logger.warning("[STATE_MIGRATE] legacy db migrate failed: %s", e)
        finally:
            if legacy_store is not None:
                legacy_store.close()

    if legacy_paper and os.path.exists(legacy_paper):
        try:
            with open(legacy_paper, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if not isinstance(raw, dict):
                raw = {}
            if not _legacy_meta_matches_context(raw, ctx):
                logger.warning("[STATE_MIGRATE] legacy state ignored due to missing context metadata json=%s", legacy_paper)
                return
            current = float(raw.get("current_equity", 0.0) or 0.0)
            initial = float(raw.get("initial_equity", current) or current)
            peak = float(raw.get("peak_equity", max(current, initial)) or max(current, initial))
            if current > 0.0:
                store.upsert_equity_state(
                    initial_equity=float(initial),
                    current_equity=float(current),
                    peak_equity=float(peak),
                    equity_currency=str(raw.get("equity_currency") or raw.get("currency") or ctx.account_ccy),
                    realized_pnl=float(raw.get("realized_pnl", current - initial) or (current - initial)),
                    unrealized_pnl=float(raw.get("unrealized_pnl", 0.0) or 0.0),
                    weekly_base=float(raw.get("weekly_base", current) or current),
                    daily_base=float(raw.get("daily_base", current) or current),
                    dd_stop_flags=(raw.get("dd_stop_flags") if isinstance(raw.get("dd_stop_flags"), dict) else {}),
                    week_key=str(raw.get("week_key") or ""),
                    day_key=str(raw.get("day_key") or ""),
                    last_updated_ts=int(raw.get("updated_at") or time.time()),
                )
            bak = f"{legacy_paper}.migrated.bak"
            try:
                os.replace(legacy_paper, bak)
            except Exception:
                pass
            logger.info("[STATE_MIGRATE] imported legacy paper_equity.json -> context store")
        except Exception as e:
            logger.warning("[STATE_MIGRATE] legacy paper_equity migrate failed: %s", e)


def _open_state_store_for_context(
    *,
    ctx: StateContext,
    symbols: list[str],
    state_dir: str | Path | None = None,
) -> tuple[StateStore, dict[str, str]]:
    global _CURRENT_STATE_CONTEXT, _CURRENT_STATE_CONTEXT_PATHS
    paths = resolve_state_context_paths(str(state_dir or STATE_DIR), ctx)
    ensure_context_layout(paths)
    store = StateStore(db_path=str(paths.db_path), context_id=str(context_id_for(ctx)))
    existing_meta = store.get_context_metadata()
    if existing_meta and (not _legacy_meta_matches_context(existing_meta, ctx)):
        stored_brief = (
            f"{existing_meta.get('exchange_id')}/{existing_meta.get('symbol')}/"
            f"{existing_meta.get('account_ccy')}/{existing_meta.get('run_mode')}"
        )
        cur_brief = format_context_brief(ctx)
        logger.warning(
            "[STATE_MISMATCH] stored=%s current=%s -> bootstrap fresh state",
            str(stored_brief),
            str(cur_brief),
        )
        store.close()
        mismatch_bak = f"{paths.db_path}.mismatch_{int(time.time())}.bak"
        try:
            os.replace(str(paths.db_path), str(mismatch_bak))
        except Exception:
            pass
        store = StateStore(db_path=str(paths.db_path), context_id=str(context_id_for(ctx)))
    _maybe_migrate_legacy_state(store, ctx, paths)
    meta_payload = _context_meta_payload(ctx, paths, symbols=symbols)
    store.set_context_metadata(meta_payload)
    write_context_meta(paths=paths, ctx=ctx, build_id=str(BUILD_ID), extra={"db_path": str(paths.db_path)})
    register_context(paths=paths, ctx=ctx, build_id=str(BUILD_ID))
    _CURRENT_STATE_CONTEXT = ctx
    _CURRENT_STATE_CONTEXT_PATHS = {
        "context_id": str(paths.context_id),
        "state_root": str(paths.state_root),
        "db_path": str(paths.db_path),
        "meta_path": str(paths.meta_path),
    }
    logger.info(
        "[STATE_CONTEXT] context_id=%s db=%s mode=%s symbol=%s acct=%s settle=%s",
        str(paths.context_id),
        str(paths.db_path),
        str(ctx.run_mode),
        str(ctx.symbol),
        str(ctx.account_ccy),
        str(ctx.settlement_ccy),
    )
    return store, _CURRENT_STATE_CONTEXT_PATHS


# ---------------------------
# DD stop (weekly + peak) using KV sim_equity
# ---------------------------

def _dd_stop_on_close(store: StateStore, net: float) -> tuple[bool, str]:
    if not bool(getattr(C, "DD_STOP_ENABLED", True)):
        return False, "dd_off"

    eq_state = store.get_equity_state()
    initial = float(eq_state.get("initial_equity") or 0.0)
    eq_prev = float(eq_state.get("current_equity") or 0.0)
    if eq_prev <= 0.0:
        eq_prev = initial if initial > 0.0 else float(getattr(C, "PAPER_INITIAL_EQUITY", 300000.0))
        initial = float(eq_prev)
    eq = float(eq_prev) + float(net)

    peak = float(eq_state.get("peak_equity") or 0.0)
    if peak <= 0:
        peak = float(eq)
    peak = max(float(peak), float(eq))
    peak_dd = (1.0 - (float(eq) / float(peak))) if float(peak) > 0.0 else 0.0

    wk = _iso_week_key(datetime.now())
    wk_key = str(eq_state.get("week_key") or "")
    if wk_key != wk:
        wk_base = float(eq)
    else:
        wk_base = float(eq_state.get("weekly_base") or eq)

    weekly_dd = (1.0 - (float(eq) / float(wk_base))) if float(wk_base) > 0.0 else 0.0

    weekly_lim = float(getattr(C, "WEEKLY_DD_LIMIT_PCT", 0.0))
    peak_lim = float(getattr(C, "PEAK_DD_LIMIT_PCT", 0.0))

    triggered = False
    reason = ""

    if weekly_lim > 0 and weekly_dd >= weekly_lim:
        triggered = True
        reason = f"WEEKLY_DD {weekly_dd:.4f} >= {weekly_lim:.4f} (eq={eq:.2f} base={wk_base:.2f})"

    if (not triggered) and peak_lim > 0 and peak_dd >= peak_lim:
        triggered = True
        reason = f"PEAK_DD {peak_dd:.4f} >= {peak_lim:.4f} (eq={eq:.2f} peak={peak:.2f})"

    dd_flags = eq_state.get("dd_stop_flags")
    if not isinstance(dd_flags, dict):
        dd_flags = {}
    if triggered:
        dd_flags[str(wk)] = str(reason)
    store.upsert_equity_state(
        initial_equity=float(initial),
        current_equity=float(eq),
        peak_equity=float(peak),
        equity_currency=str(eq_state.get("equity_currency") or (_CURRENT_STATE_CONTEXT.account_ccy if _CURRENT_STATE_CONTEXT is not None else _quote_ccy_for_symbol(None))),
        realized_pnl=float(eq - initial),
        weekly_base=float(wk_base),
        dd_stop_flags=dd_flags,
        week_key=str(wk),
        last_updated_ts=int(time.time()),
    )

    if triggered:
        mode2 = str(getattr(C, "DD_STOP_MODE", "STOP_NEW_ONLY"))
        store.set_stop(mode=mode2, reason=f"DD_STOP:{reason}", phase="CLOSE_POSITION")
        if bool(getattr(C, "DD_STOP_WRITE_STOP_FILE", True)):
            _write_stop_file(mode2)
        return True, f"DD_STOP triggered: {reason}"

    return False, "dd_ok"


# ---------------------------
# Filters
# ---------------------------

def _filters_for_regime(regime: str) -> dict:
    r = str(regime or "").lower()
    if r == "range":
        return dict(getattr(C, "FILTERS_RANGE", {}))
    return dict(getattr(C, "FILTERS_TREND", {}))


def _spread_bps_from_orderbook(ex: ExchangeClient, symbol: str) -> float | None:
    try:
        ob = ex.fetch_order_book(symbol, limit=5)
        bids = ob.get("bids") or []
        asks = ob.get("asks") or []
        if not bids or not asks:
            return None
        bid = float(bids[0][0])
        ask = float(asks[0][0])
        mid = (bid + ask) / 2.0
        if mid <= 0:
            return None
        return (ask - bid) / mid * 10000.0
    except Exception:
        return None


def _expectancy_filter(entry: float, stop_price: float, tp_price: float, spread_bps: float | None) -> tuple[bool, str]:
    # Apply a simple expectancy guard after spread and fee costs.
    min_tp_bps = float(getattr(C, "MIN_TP_BPS", 6.0))
    need_mult = float(getattr(C, "EXPECTANCY_NEED_MULT", 1.4))
    sp = float(spread_bps) if spread_bps is not None else 0.0
    tp_bps = (tp_price - entry) / entry * 10000.0
    _, taker_fee_rate = _resolved_paper_fee_pair()
    fee_rate = float(taker_fee_rate)
    fee_bps = 2.0 * fee_rate * 10000.0   # Convert round-trip fees to basis points.
    fee_mult = float(getattr(C, "EXPECTANCY_FEE_MULT", 1.2))
    need = max(min_tp_bps, sp * need_mult, fee_bps * fee_mult)

    # Add a small epsilon so floating-point rounding does not reject borderline trades.
    # Example: tp_bps=5.999999 should not fail against a displayed 6.00 threshold.
    eps_bps = float(getattr(C, "EXPECTANCY_EPS_BPS", 0.05))

    if (tp_bps + eps_bps) < need:
        return False, f"Expectancy too low (tp_bps={tp_bps:.2f} < need={need:.2f})"

    return True, f"OK(tp_bps={tp_bps:.2f} need={need:.2f})"


def _adjust_tp_sl(
    symbol: str,
    entry: float,
    stop_price: float,
    tp_price: float,
    spread_bps: float | None,
    high: list[float],
    low: list[float],
    close: list[float],
    regime: str | None = None,
    direction: str | None = None
) -> tuple[bool, float, float, str]:
    is_range = (str(regime).lower() == "range")

    min_rr_default = float(getattr(C, "MIN_RR_AFTER_ADJUST", 1.30))

    reg = str(regime).lower()
    dir_ = str(direction).lower()

    min_rr = min_rr_default

    # --- debug visibility: which config + what min_rr was actually used ---
    cfg_path = getattr(C, "__file__", None)

    if reg == "trend":
        if dir_ == "long":
            min_rr = float(getattr(C, "MIN_RR_AFTER_ADJUST_TREND_LONG", min_rr_default))
        elif dir_ == "none":
            min_rr = float(getattr(C, "MIN_RR_AFTER_ADJUST_TREND_NONE", min_rr_default))
        elif dir_ == "short":
            min_rr = float(getattr(C, "MIN_RR_AFTER_ADJUST_TREND_SHORT", min_rr_default))


    min_tp_bps = float(getattr(C, "MIN_TP_BPS", 6.0))
    min_stop_bps = float(getattr(C, "MIN_STOP_BPS", 1.5))
    target_stop_bps = float(getattr(C, "TARGET_STOP_BPS", 4.0))
    tp_sp_mult = float(getattr(C, "TP_SPREAD_MULT", 2.0))
    sl_sp_mult = float(getattr(C, "SL_SPREAD_MULT", 1.0))

    use_atr = bool(getattr(C, "USE_ATR_FOR_TP_SL", True))
    atr_period = int(getattr(C, "ATR_PERIOD_ENTRY", 14))
    atr_tp_mult = float(getattr(C, "ATR_TP_MULT", 1.2))
    atr_sl_mult = float(getattr(C, "ATR_SL_MULT", 1.0))

    # Use slightly tighter ATR multipliers for trend-long setups to improve TP hit rate.
    reg_l = str(regime).lower()
    dir_l = str(direction).lower()
    if reg_l == "trend" and dir_l == "long":
        atr_tp_mult = float(getattr(C, "ATR_TP_MULT_TREND_LONG", atr_tp_mult))
        atr_sl_mult = float(getattr(C, "ATR_SL_MULT_TREND_LONG", atr_sl_mult))

    if is_range:
        min_rr = float(getattr(C, "RANGE_MIN_RR_AFTER_ADJUST", min_rr))
        min_tp_bps = float(getattr(C, "RANGE_MIN_TP_BPS", min_tp_bps))
        min_stop_bps = float(getattr(C, "RANGE_MIN_STOP_BPS", min_stop_bps))
        target_stop_bps = float(getattr(C, "RANGE_TARGET_STOP_BPS", target_stop_bps))
        tp_sp_mult = float(getattr(C, "RANGE_TP_SPREAD_MULT", tp_sp_mult))
        sl_sp_mult = float(getattr(C, "RANGE_SL_SPREAD_MULT", sl_sp_mult))
        use_atr = bool(getattr(C, "RANGE_USE_ATR_FOR_TP_SL", use_atr))
        atr_period = int(getattr(C, "RANGE_ATR_PERIOD_ENTRY", atr_period))
        atr_tp_mult = float(getattr(C, "RANGE_ATR_TP_MULT", atr_tp_mult))
        atr_sl_mult = float(getattr(C, "RANGE_ATR_SL_MULT", atr_sl_mult))

    sp = float(spread_bps) if spread_bps is not None else 0.0

    # --- NEW: fee/slip-aware floors (avoid fee-dominated micro trades) ---
    _, taker_fee_rate = _resolved_paper_fee_pair()
    fee_rate = float(taker_fee_rate)
    fee_bps_round = 2.0 * fee_rate * 10000.0  # Convert round-trip fees to basis points.
    slip_bps = float(getattr(C, "SLIPPAGE_BPS", 0.0))
    entry_eff = entry * (1.0 + max(0.0, slip_bps) / 10000.0)
    slip_bps_round = 2.0 * max(0.0, slip_bps)  # Convert round-trip slippage to basis points.
    cost_bps_round = fee_bps_round + slip_bps_round + sp

    tp_cost_mult = float(getattr(C, "MIN_TP_COST_MULT", 2.0))
    sl_cost_mult = float(getattr(C, "MIN_STOP_COST_MULT", 0.8))

    min_tp_bps = max(min_tp_bps, cost_bps_round * tp_cost_mult)
    min_stop_bps = max(min_stop_bps, cost_bps_round * sl_cost_mult)
    min_stop_bps = max(min_stop_bps, target_stop_bps)


    # ensure minimum stop distance
    stop_min = entry * (1.0 - min_stop_bps / 10000.0)
    stop_price = min(stop_price, stop_min)

    # ensure minimum tp distance
    tp_min = entry * (1.0 + min_tp_bps / 10000.0)
    tp_price = max(tp_price, tp_min)

    # spread-aware minimums
    tp_need = entry_eff * (1.0 + (sp * tp_sp_mult) / 10000.0)
    sl_need = entry_eff * (1.0 - (sp * sl_sp_mult) / 10000.0)
    tp_price = max(tp_price, tp_need)
    stop_price = min(stop_price, sl_need)

    if use_atr:
        a = ind_atr(high, low, close, period=atr_period)
        a_last = None
        try:
            a_last = float(a[-1]) if hasattr(a, "__len__") else float(a)
        except Exception:
            a_last = None
        if a_last and a_last > 0:
            tp_price = max(tp_price, entry + a_last * atr_tp_mult)
            stop_price = min(stop_price, entry - a_last * atr_sl_mult)


    # --- NEW: cap TP distance for range (after all floors/ATR adjustments) ---
    if is_range:
        tp_cap_bps = float(getattr(C, "RANGE_TP_CAP_BPS", 0.0))
        if tp_cap_bps and tp_cap_bps > 0:
            tp_cap = entry * (1.0 + tp_cap_bps / 10000.0)
            tp_price = min(tp_price, tp_cap)

    # --- cap stop distance (avoid ATR widening SL too much) ---
    # trend: reject when SL is wider than the cap.
    # range: clamp the stop to the cap (turning "stop_too_wide" rejects into
    #        tighter-risk candidates, then let RR/expectancy filters decide).
    max_stop_bps = float(getattr(C, "MAX_STOP_BPS", 0.0))
    if is_range:
        max_stop_bps = float(getattr(C, "RANGE_MAX_STOP_BPS", max_stop_bps))

    if max_stop_bps and max_stop_bps > 0:
        sl_bps = (entry - stop_price) / entry * 10000.0
        if sl_bps - 1e-9 > max_stop_bps:
            if is_range:
                # tighten stop to cap
                stop_price = max(stop_price, entry * (1.0 - max_stop_bps / 10000.0))
            else:
                return False, stop_price, tp_price, (
                    f"adjust_tp_sl(stop_too_wide sl_bps={sl_bps:.2f} > {max_stop_bps:.2f} reg={reg} dir={dir_} cfg={cfg_path})"
                )

    risk = entry - stop_price
    reward = tp_price - entry
    if risk <= 0 or reward <= 0:
        return False, stop_price, tp_price, "Bad TP/SL after adjust"

    rr = reward / risk
    if rr + 1e-9 < min_rr:
        return False, stop_price, tp_price, (
            f"RR below min after adjust (rr={rr:.2f} < {min_rr:.2f}) "
            f"reg={reg} dir={dir_} min_rr_default={min_rr_default:.2f} min_rr_final={min_rr:.2f} cfg={cfg_path}"
        )


    tp_bps = (tp_price - entry) / entry * 10000.0
    sl_bps = (entry - stop_price) / entry * 10000.0
    return True, stop_price, tp_price, (
        f"ADJ_OK(rr={rr:.2f}, tp_bps={tp_bps:.2f}, sl_bps={sl_bps:.2f}, "
        f"min_tp_bps={min_tp_bps:.2f}, min_stop_bps={min_stop_bps:.2f}, sp={sp:.2f}, cost={cost_bps_round:.2f}, tp_mult={tp_cost_mult:.2f}, sl_mult={sl_cost_mult:.2f})"
    )


def _pos_init_stop_key(symbol: str) -> str:
    return f"pos_init_stop:{symbol}"


def _get_pos_init_stop(store: StateStore, symbol: str, fallback: float) -> float:
    try:
        v = store.get_kv(_pos_init_stop_key(symbol), "")
        if v in (None, ""):
            return float(fallback)
        return float(v)
    except Exception:
        return float(fallback)


def _clear_pos_init_stop(store: StateStore, symbol: str) -> None:
    try:
        store.set_kv(_pos_init_stop_key(symbol), "")
    except Exception:
        pass

def _pos_max_fav_key(symbol: str) -> str:
    return f"pos:{symbol}:max_fav"

def _pos_min_adv_key(symbol: str) -> str:
    return f"pos:{symbol}:min_adv"


def _get_pos_max_fav(store: StateStore, symbol: str) -> float | None:
    v = store.get_kv(_pos_max_fav_key(symbol))
    return float(v) if v is not None else None


def _set_pos_max_fav(store: StateStore, symbol: str, v: float) -> None:
    store.set_kv(_pos_max_fav_key(symbol), float(v))


def _clear_pos_max_fav(store: StateStore, symbol: str) -> None:
    store.del_kv(_pos_max_fav_key(symbol))
    # Backward/forward compatible KV delete (StateStore may not expose del_kv).
    if hasattr(store, "del_kv"):
        store.del_kv(_pos_max_fav_key(symbol))
    else:
        # Convention: set None to clear.
        store.set_kv(_pos_max_fav_key(symbol), None)

def _get_pos_min_adv(store: StateStore, symbol: str) -> float | None:
    v = store.get_kv(_pos_min_adv_key(symbol))
    return float(v) if v is not None else None


def _set_pos_min_adv(store: StateStore, symbol: str, v: float) -> None:
    store.set_kv(_pos_min_adv_key(symbol), float(v))


def _clear_pos_min_adv(store: StateStore, symbol: str) -> None:
    store.del_kv(_pos_min_adv_key(symbol))

def _sync_trade_excursion_state(
    store: StateStore,
    symbol: str,
    pos: dict[str, Any],
    *,
    bar_high: float,
    bar_low: float,
) -> dict[str, float]:
    try:
        meta = _get_pos_meta(store, symbol)
        direction = str(meta.get("direction", pos.get("direction", "long")) or "long").strip().lower()
        if direction in ("sell", "short"):
            direction = "short"
        else:
            direction = "long"
        entry_exec = float(pos.get("entry") or 0.0)
        entry_raw = float(meta.get("entry_raw", entry_exec) or entry_exec or 0.0)
        qty_init = max(
            0.0,
            float(
                _get_pos_init_qty(
                    store,
                    symbol,
                    fallback=abs(float(pos.get("qty") or 0.0)),
                )
            ),
        )
        seed_price = float(entry_exec if entry_exec > 0.0 else entry_raw)
        prev_max_fav = _get_pos_max_fav(store, symbol)
        prev_min_adv = _get_pos_min_adv(store, symbol)
        if prev_max_fav is None or (not math.isfinite(float(prev_max_fav))):
            prev_max_fav = float(seed_price)
        if prev_min_adv is None or (not math.isfinite(float(prev_min_adv))):
            prev_min_adv = float(seed_price)
        next_max_fav = max(float(prev_max_fav), float(bar_high))
        next_min_adv = min(float(prev_min_adv), float(bar_low))
        _set_pos_max_fav(store, symbol, float(next_max_fav))
        _set_pos_min_adv(store, symbol, float(next_min_adv))

        prev_mae_abs = max(0.0, float(meta.get("mae_abs", 0.0) or 0.0))
        prev_mfe_abs = max(0.0, float(meta.get("mfe_abs", 0.0) or 0.0))
        prev_giveback_max_abs = max(0.0, float(meta.get("giveback_max_abs", 0.0) or 0.0))
        mae_abs = float(prev_mae_abs)
        mfe_abs = float(prev_mfe_abs)
        giveback_max_abs = float(prev_giveback_max_abs)
        if qty_init > 0.0 and entry_raw > 0.0:
            if direction == "short":
                adverse_abs = max(0.0, (float(bar_high) - float(entry_raw)) * float(qty_init))
                favorable_abs = max(0.0, (float(entry_raw) - float(bar_low)) * float(qty_init))
                best_price = min(float(next_min_adv), float(bar_low))
                if best_price < float(entry_raw):
                    giveback_max_abs = max(
                        float(giveback_max_abs),
                        max(0.0, (float(bar_high) - float(best_price)) * float(qty_init)),
                    )
            else:
                adverse_abs = max(0.0, (float(entry_raw) - float(bar_low)) * float(qty_init))
                favorable_abs = max(0.0, (float(bar_high) - float(entry_raw)) * float(qty_init))
                best_price = max(float(next_max_fav), float(bar_high))
                if best_price > float(entry_raw):
                    giveback_max_abs = max(
                        float(giveback_max_abs),
                        max(0.0, (float(best_price) - float(bar_low)) * float(qty_init)),
                    )
            mae_abs = max(float(mae_abs), float(adverse_abs))
            mfe_abs = max(float(mfe_abs), float(favorable_abs))

        _update_pos_meta(
            store,
            symbol,
            {
                "max_fav": float(next_max_fav),
                "min_adv": float(next_min_adv),
                "mae_abs": float(mae_abs),
                "mfe_abs": float(mfe_abs),
                "giveback_max_abs": float(giveback_max_abs),
            },
        )
        return {
            "max_fav": float(next_max_fav),
            "min_adv": float(next_min_adv),
            "mae_abs": float(mae_abs),
            "mfe_abs": float(mfe_abs),
            "giveback_max_abs": float(giveback_max_abs),
        }
    except Exception:
        return {}

def _calc_be_offset_bps(
    spread_bps: float | None,
    atr_bps: float | None = None,
    static_off_bps: float | None = None,
) -> float:
    """
    Move the BE stop above entry by an offset measured in basis points.
    Use the larger of the static offset and the dynamic cost-aware offset: fee, spread, or slippage.
    """
    static_off = float(static_off_bps) if static_off_bps is not None else float(getattr(C, "BE_OFFSET_BPS", 1.5))
    sp = float(spread_bps) if spread_bps is not None else 0.0
    atr_now_bps = float(atr_bps) if atr_bps is not None else 0.0

    if not bool(getattr(C, "BE_USE_DYNAMIC_OFFSET", False)):
        off_bps = static_off
    else:
        _, taker_fee_rate = _resolved_paper_fee_pair()
        fee_rate = float(taker_fee_rate)
        fee_bps_round = 2.0 * fee_rate * 10000.0  # round-trip fee in bps

        slip_bps = float(getattr(C, "SLIPPAGE_BPS", 0.0))

        fee_mult = float(getattr(C, "BE_DYNAMIC_FEE_MULT", 1.1))
        sp_mult = float(getattr(C, "BE_DYNAMIC_SPREAD_MULT", 1.0))
        slip_mult = float(getattr(C, "BE_DYNAMIC_SLIP_MULT", 1.0))

        dyn = (fee_bps_round * fee_mult) + (sp * sp_mult) + (slip_bps * slip_mult)
        off_bps = max(static_off, dyn)

    min_off_bps = max(60.0, 3.0 * sp, 0.35 * atr_now_bps)
    return max(float(off_bps), float(min_off_bps))

# ---------------------------
# BE / TRAIL
# ---------------------------

def _be_params(regime: str) -> tuple[float, float]:
    r = str(regime or "").lower()
    if r == "range":
        tr = float(getattr(C, "RANGE_BE_TRIGGER_R", getattr(C, "BE_TRIGGER_R", 0.7)))
        off = float(getattr(C, "RANGE_BE_OFFSET_BPS", getattr(C, "BE_OFFSET_BPS", 1.5)))
        return tr, off
    return float(getattr(C, "BE_TRIGGER_R", 0.7)), float(getattr(C, "BE_OFFSET_BPS", 1.5))


def _be_effective_enabled(regime: str, *, force_disable_be: bool = False) -> bool:
    if bool(force_disable_be):
        return False
    if not bool(getattr(C, "BE_ENABLED", True)):
        return False
    try:
        trigger_r, _ = _be_params(regime)
        return bool(math.isfinite(float(trigger_r)) and float(trigger_r) > 0.0)
    except Exception:
        return False


def _tp1_range_effective_params(*, require_flag: bool) -> tuple[bool, float, float]:
    trig_r = float(getattr(C, "RANGE_TP1_TRIGGER_R", getattr(C, "TP1_TRIGGER_R", 0.0)) or 0.0)
    qty_pct = float(getattr(C, "RANGE_TP1_QTY_PCT", getattr(C, "TP1_QTY_PCT", 0.0)) or 0.0)
    enabled = bool(trig_r > 0.0 and qty_pct > 0.0)
    if bool(require_flag):
        enabled = bool(enabled and bool(getattr(C, "RANGE_TP1_ENABLED", False)))
    return bool(enabled), float(trig_r), float(qty_pct)


def _trail_params(regime: str) -> tuple[float, float, float]:
    r = str(regime or "").lower()
    if r == "range":
        start_r = float(getattr(C, "RANGE_TRAIL_START_R", getattr(C, "TRAIL_START_R", 0.8)))
        atr_mult = float(getattr(C, "RANGE_TRAIL_ATR_MULT", getattr(C, "TRAIL_ATR_MULT", 1.2)))
        bps = float(getattr(C, "RANGE_TRAIL_BPS_FROM_HIGH", getattr(C, "TRAIL_BPS_FROM_HIGH", 18.0)))
        return start_r, atr_mult, bps
    return (
        float(getattr(C, "TRAIL_START_R", 0.8)),
        float(getattr(C, "TRAIL_ATR_MULT", 1.2)),
        float(getattr(C, "TRAIL_BPS_FROM_HIGH", 18.0)),
    )


def _stop_move_source(stop_kind: Any) -> str:
    kind = str(stop_kind or "").strip().lower()
    if kind in ("", "init", "raw"):
        return "raw"
    if kind == "be":
        return "be"
    if kind == "trail":
        return "trail"
    return "unknown"


def _kept_pct_of_mfe(mfe_bps: Any, giveback_to_close_bps: Any) -> float:
    try:
        mfe = float(mfe_bps)
        giveback = float(giveback_to_close_bps)
    except Exception:
        return 0.0
    if (not math.isfinite(mfe)) or (not math.isfinite(giveback)) or mfe <= 0.0:
        return 0.0
    kept_bps = max(0.0, float(mfe) - float(giveback))
    return float(kept_bps / max(float(mfe), 1e-12))


def _diag_float_or_none(v: Any) -> float | None:
    try:
        x = float(v)
    except Exception:
        return None
    return float(x) if math.isfinite(x) else None


# Trail redesign candidates for the 2023 stable replay diagnostics:
# 1. Keep current behavior: ATR priority, BPS fallback, and require candidate > current_stop.
# 2. Build the candidate from the high-water mark (`max_fav`) instead of the current bar high.
# 3. Evaluate "can the stop at least move to entry?" before the current_stop ratchet check.
# 4. When both ATR and BPS candidates exist, use max(candidate_atr, candidate_bps).
# 5. Split the trail threshold basis: init_stop for arming, current_stop for later ratchets.
def _trail_diag_defaults(regime: str, *, init_stop: Any, entry_exec: Any = None) -> dict[str, Any]:
    start_r, _, bps_from_high = _trail_params(str(regime))
    out = {
        "init_stop": float(init_stop),
        "trail_triggered": False,
        "trail_start_r": float(start_r),
        "trail_bps_from_high": float(bps_from_high),
        "start_price": None,
        "trail_eval_count": 0,
        "trail_candidate_stop_last": None,
        "trail_candidate_stop_max": None,
        "trail_candidate_minus_current_stop": None,
        "trail_candidate_minus_current_stop_max": None,
        "trail_candidate_from_atr_last": None,
        "trail_candidate_from_bps_last": None,
        "trail_eligible_count": 0,
        "trail_update_count": 0,
        "trail_block_reason_last": "",
        "trail_block_reason_max": "",
        "trail_start_price_last": None,
        "trail_start_price_max_context": None,
        "trail_bar_high_last": None,
        "trail_bar_high_max": None,
        "trail_pos_stop_before_last": None,
        "trail_pos_stop_before_max_context": None,
        "trail_risk_per_unit_last": None,
        "trail_mode_last": "none",
        "be_triggered": False,
        "be_trigger_r": None,
        "be_offset_bps": None,
        "be_stop_set": None,
    }
    try:
        if entry_exec is not None:
            out["entry_exec"] = float(entry_exec)
    except Exception:
        pass
    return out


def _trail_diag_update(
    meta: dict[str, Any],
    *,
    block_reason: Any,
    candidate_stop: Any,
    candidate_from_atr: Any,
    candidate_from_bps: Any,
    pos_stop_before: Any,
    start_price: Any,
    bar_high: Any,
    risk_per_unit: Any,
    mode: Any,
) -> float | None:
    cand_stop_f = _diag_float_or_none(candidate_stop)
    cand_atr_f = _diag_float_or_none(candidate_from_atr)
    cand_bps_f = _diag_float_or_none(candidate_from_bps)
    pos_stop_before_f = _diag_float_or_none(pos_stop_before)
    start_price_f = _diag_float_or_none(start_price)
    bar_high_f = _diag_float_or_none(bar_high)
    risk_per_unit_f = _diag_float_or_none(risk_per_unit)
    cand_delta_f = (
        float(cand_stop_f) - float(pos_stop_before_f)
        if (cand_stop_f is not None and pos_stop_before_f is not None)
        else None
    )

    try:
        meta["trail_eval_count"] = max(0, int(meta.get("trail_eval_count", 0) or 0)) + 1
    except Exception:
        meta["trail_eval_count"] = 1

    meta["trail_candidate_stop_last"] = cand_stop_f
    meta["trail_candidate_minus_current_stop"] = cand_delta_f
    meta["trail_candidate_from_atr_last"] = cand_atr_f
    meta["trail_candidate_from_bps_last"] = cand_bps_f
    meta["trail_block_reason_last"] = str(block_reason or "")
    meta["trail_start_price_last"] = start_price_f
    meta["trail_bar_high_last"] = bar_high_f
    meta["trail_pos_stop_before_last"] = pos_stop_before_f
    meta["trail_risk_per_unit_last"] = risk_per_unit_f
    meta["trail_mode_last"] = str(mode or "none")
    if start_price_f is not None:
        meta["start_price"] = start_price_f

    cur_max = _diag_float_or_none(meta.get("trail_candidate_minus_current_stop_max"))
    if cand_delta_f is not None and (cur_max is None or float(cand_delta_f) > float(cur_max)):
        meta["trail_candidate_minus_current_stop_max"] = cand_delta_f
        meta["trail_candidate_stop_max"] = cand_stop_f
        meta["trail_block_reason_max"] = str(block_reason or "")
        meta["trail_start_price_max_context"] = start_price_f
        meta["trail_bar_high_max"] = bar_high_f
        meta["trail_pos_stop_before_max_context"] = pos_stop_before_f
    elif cur_max is None and str(block_reason or "") and (not str(meta.get("trail_block_reason_max") or "")):
        meta["trail_block_reason_max"] = str(block_reason or "")
        meta["trail_start_price_max_context"] = start_price_f
        meta["trail_bar_high_max"] = bar_high_f
        meta["trail_pos_stop_before_max_context"] = pos_stop_before_f

    return cand_delta_f


def _build_stop_diag_fields(
    *,
    stop_kind: Any,
    init_stop: Any,
    final_stop: Any,
    entry_exec: Any,
    trail_eval_count: Any = 0,
    trail_candidate_stop_last: Any = None,
    trail_candidate_stop_max: Any = None,
    trail_candidate_minus_current_stop: Any = None,
    trail_candidate_minus_current_stop_max: Any = None,
    trail_candidate_from_atr_last: Any = None,
    trail_candidate_from_bps_last: Any = None,
    trail_eligible_count: Any = 0,
    trail_update_count: Any = 0,
    trail_block_reason_last: Any = "",
    trail_block_reason_max: Any = "",
    start_price: Any = None,
    trail_start_price_last: Any = None,
    trail_start_price_max_context: Any = None,
    trail_bar_high_last: Any = None,
    trail_bar_high_max: Any = None,
    trail_pos_stop_before_last: Any = None,
    trail_pos_stop_before_max_context: Any = None,
    trail_risk_per_unit_last: Any = None,
    trail_mode_last: Any = None,
    mfe_bps: Any = 0.0,
    giveback_to_close_bps: Any = 0.0,
) -> dict[str, Any]:
    init_stop_f = _diag_float_or_none(init_stop)
    final_stop_f = _diag_float_or_none(final_stop)
    entry_exec_f = _diag_float_or_none(entry_exec)
    start_price_f = _diag_float_or_none(start_price)
    cand_stop_f = _diag_float_or_none(trail_candidate_stop_last)
    cand_stop_max_f = _diag_float_or_none(trail_candidate_stop_max)
    cand_delta_f = _diag_float_or_none(trail_candidate_minus_current_stop)
    cand_delta_max_f = _diag_float_or_none(trail_candidate_minus_current_stop_max)
    cand_from_atr_f = _diag_float_or_none(trail_candidate_from_atr_last)
    cand_from_bps_f = _diag_float_or_none(trail_candidate_from_bps_last)
    start_price_last_f = _diag_float_or_none(trail_start_price_last)
    start_price_max_f = _diag_float_or_none(trail_start_price_max_context)
    bar_high_last_f = _diag_float_or_none(trail_bar_high_last)
    bar_high_max_f = _diag_float_or_none(trail_bar_high_max)
    pos_stop_before_last_f = _diag_float_or_none(trail_pos_stop_before_last)
    pos_stop_before_max_f = _diag_float_or_none(trail_pos_stop_before_max_context)
    risk_per_unit_last_f = _diag_float_or_none(trail_risk_per_unit_last)
    try:
        eval_count = max(0, int(trail_eval_count or 0))
    except Exception:
        eval_count = 0
    try:
        eligible_count = max(0, int(trail_eligible_count or 0))
    except Exception:
        eligible_count = 0
    try:
        update_count = max(0, int(trail_update_count or 0))
    except Exception:
        update_count = 0

    return {
        "stop_move_source": _stop_move_source(stop_kind),
        "final_stop": final_stop_f,
        "final_stop_minus_init_stop": (
            float(final_stop_f) - float(init_stop_f)
            if (final_stop_f is not None and init_stop_f is not None)
            else None
        ),
        "final_stop_minus_entry": (
            float(final_stop_f) - float(entry_exec_f)
            if (final_stop_f is not None and entry_exec_f is not None)
            else None
        ),
        "kept_pct_of_mfe": float(_kept_pct_of_mfe(mfe_bps, giveback_to_close_bps)),
        "trail_eval_count": int(eval_count),
        "trail_candidate_stop_last": cand_stop_f,
        "trail_candidate_stop_max": cand_stop_max_f,
        "trail_candidate_minus_current_stop": cand_delta_f,
        "trail_candidate_minus_current_stop_last": cand_delta_f,
        "trail_candidate_minus_current_stop_max": cand_delta_max_f,
        "trail_candidate_from_atr_last": cand_from_atr_f,
        "trail_candidate_from_bps_last": cand_from_bps_f,
        "trail_eligible_count": int(eligible_count),
        "trail_update_count": int(update_count),
        "trail_block_reason_last": str(trail_block_reason_last or ""),
        "trail_block_reason_max": str(trail_block_reason_max or ""),
        "start_price": start_price_f,
        "trail_start_price_last": start_price_last_f if start_price_last_f is not None else start_price_f,
        "trail_start_price_max_context": start_price_max_f,
        "trail_bar_high_last": bar_high_last_f,
        "trail_bar_high_max": bar_high_max_f,
        "trail_pos_stop_before_last": pos_stop_before_last_f,
        "trail_pos_stop_before_max_context": pos_stop_before_max_f,
        "trail_risk_per_unit_last": risk_per_unit_last_f,
        "trail_mode_last": str(trail_mode_last or "none"),
    }


def _effective_range_config_snapshot(*, force_disable_be: bool = False, tp1_requires_flag: bool = False) -> dict[str, Any]:
    trade_range = bool(getattr(C, "TRADE_RANGE", True))
    try:
        trade_trend = float(getattr(C, "TRADE_TREND", 0.0) or 0.0)
    except Exception:
        trade_trend = 0.0
    be_trigger_r, be_offset_bps = _be_params("range")
    be_enabled = _be_effective_enabled("range", force_disable_be=bool(force_disable_be))
    tp1_enabled, tp1_trigger_r, tp1_qty_pct = _tp1_range_effective_params(require_flag=bool(tp1_requires_flag))
    trail_start_r, _, trail_bps_from_high = _trail_params("range")
    return {
        "TRADE_RANGE": bool(trade_range),
        "TRADE_TREND": float(trade_trend),
        "BE_ENABLED": bool(getattr(C, "BE_ENABLED", True)),
        "BE_EFFECTIVE_ENABLED": bool(be_enabled),
        "BE_FORCE_DISABLED": bool(force_disable_be),
        "BE_USE_DYNAMIC_OFFSET": bool(be_enabled and bool(getattr(C, "BE_USE_DYNAMIC_OFFSET", False))),
        "BE_TRIGGER_R": float(be_trigger_r),
        "BE_OFFSET_BPS": float(be_offset_bps),
        "RANGE_TP1_TRIGGER_R": float(tp1_trigger_r),
        "RANGE_TP1_QTY_PCT": float(tp1_qty_pct),
        "TP1_EFFECTIVE_ENABLED": bool(tp1_enabled),
        "RANGE_TRAIL_START_R": float(trail_start_r),
        "RANGE_TRAIL_BPS_FROM_HIGH": float(trail_bps_from_high),
        "RANGE_ATR_SL_MULT": float(getattr(C, "RANGE_ATR_SL_MULT", 0.0) or 0.0),
        "RANGE_ATR_TP_MULT": float(getattr(C, "RANGE_ATR_TP_MULT", 0.0) or 0.0),
        "RANGE_EXIT_ON_EMA21_BREAK": bool(getattr(C, "RANGE_EXIT_ON_EMA21_BREAK", False)),
        "RANGE_EXIT_ON_EMA9_CROSS": bool(getattr(C, "RANGE_EXIT_ON_EMA9_CROSS", False)),
        "RANGE_EARLY_EXIT_LOSS_ATR_MULT": float(getattr(C, "RANGE_EARLY_EXIT_LOSS_ATR_MULT", 0.0) or 0.0),
    }


def _log_effective_range_config(
    logger_obj: logging.Logger,
    *,
    label: str,
    force_disable_be: bool = False,
    tp1_requires_flag: bool = False,
) -> None:
    snap = _effective_range_config_snapshot(
        force_disable_be=bool(force_disable_be),
        tp1_requires_flag=bool(tp1_requires_flag),
    )
    logger_obj.info("[%s][CFG_EFFECTIVE][RANGE] %s", str(label), json.dumps(snap, ensure_ascii=False))


# ---------------------------
# core: select symbols / startup cancel
# ---------------------------

def _select_symbols(ex: ExchangeClient, store: StateStore) -> list[str]:
    auto = bool(getattr(C, "AUTO_SELECT_SYMBOLS", False))
    if not auto:
        syms = list(getattr(C, "SYMBOLS", []))
    else:
        symbols_seed = list(getattr(C, "SYMBOLS", []) or [])
        quote_default = _quote_ccy_for_symbol(str(symbols_seed[0] if symbols_seed else _default_symbol_for_runtime()))
        if not quote_default:
            quote_default = _default_quote_for_runtime()
        # ExchangeClient is expected to provide select_symbols_for_scalping.
        syms = ex.select_symbols_for_scalping(
            quote=str(getattr(C, "SELECT_QUOTE", quote_default)).upper(),
            top_n_by_quote_volume=int(getattr(C, "SELECT_VOLUME_TOP_N", 80)),
            final_n=int(getattr(C, "SELECT_FINAL_N", 10)),
            max_spread_bps=float(getattr(C, "SELECT_MAX_SPREAD_BPS", 6.0)),
            min_price_decimals=int(getattr(C, "SELECT_MIN_PRICE_DECIMALS", 2)),
            min_amount_decimals=int(getattr(C, "SELECT_MIN_AMOUNT_DECIMALS", 2)),
            max_min_amount=getattr(C, "SELECT_MAX_MIN_AMOUNT", None),
            max_min_cost=getattr(C, "SELECT_MAX_MIN_COST", None),
            exclude_leveraged_tokens=bool(getattr(C, "SELECT_EXCLUDE_LEVERAGED", True)),
        )

    # exclude maker-blocked
    if bool(getattr(C, "AUTO_SELECT_EXCLUDE_MAKER_BLOCKED", True)):
        out = []
        for s in syms:
            blocked_entry = False
            blocked_tp = False
            if bool(getattr(C, "AUTO_SELECT_EXCLUDE_KIND_ENTRY", True)):
                blocked_entry, _ = _is_maker_blocked(store, "ENTRY", s)
            if bool(getattr(C, "AUTO_SELECT_EXCLUDE_KIND_TP", False)):
                blocked_tp, _ = _is_maker_blocked(store, "TP", s)
            if blocked_entry or blocked_tp:
                continue
            out.append(s)
        syms = out
    wl = set(getattr(C, "SYMBOLS_WHITELIST", []) or [])
    if wl:
        syms = [s for s in syms if s in wl]

    return [s for s in syms if isinstance(s, str) and s]


def _cancel_open_orders_on_start(ex: ExchangeClient, symbols: list[str], mode: str, dryrun: bool) -> None:
    if mode != "LIVE" or dryrun:
        return
    if not bool(getattr(C, "CANCEL_OPEN_ORDERS_ON_START", True)):
        return

    per_symbol = bool(getattr(C, "CANCEL_OPEN_ORDERS_PER_SYMBOL", True))
    limit = int(getattr(C, "CANCEL_OPEN_ORDERS_LIMIT", 200))

    try:
        if per_symbol:
            total = 0
            for s in symbols:
                try:
                    opens = ex.fetch_open_orders(s, limit=limit) or []
                    if opens:
                        ex.cancel_all_orders(s)
                        total += len(opens)
                except Exception as e:
                    logger.warning(f"Cancel open orders failed: {s} err={e}")
            logger.warning(f"[STARTUP] Canceled open orders (per symbol): approx {total}")
        else:
            opens = ex.fetch_open_orders(None, limit=limit) or []
            if opens:
                ex.cancel_all_orders(None)
            logger.warning(f"[STARTUP] Canceled open orders (global): {len(opens)}")
    except Exception as e:
        error_logger.exception(f"[STARTUP] cancel open orders error: {e}")


# ---------------------------
# trading exec (minimal)
# ---------------------------

def _market_order(ex: ExchangeClient, symbol: str, side: str, qty: float, dryrun: bool) -> dict:
    return {"filled_qty": 0.0, "avg_price": None, "reason": "MARKET_DISABLED_MAKER_ONLY"}


def _log_maker_fill_csv(
    *,
    ts_ms: int | None,
    symbol: str,
    stage: str,
    side: str,
    qty: float,
    filled: float,
    ttl_sec: float,
    reason: str,
) -> None:
    if not bool(getattr(C, "LOG_MAKER_FILL", True)):
        return
    try:
        q = float(qty)
        f = float(filled)
        fill_ratio = (f / q) if q > 0.0 else 0.0
        t = int(ts_ms) if ts_ms is not None else int(time.time() * 1000)
        out_dir = _current_export_dir()
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, "maker_fill.csv")
        write_header = (not os.path.exists(path)) or os.path.getsize(path) == 0
        with open(path, "a", newline="", encoding="utf-8") as fp:
            w = csv.writer(fp)
            if write_header:
                w.writerow(["ts_ms", "symbol", "stage", "side", "qty", "filled", "fill_ratio", "ttl_sec", "reason"])
            w.writerow([t, str(symbol), str(stage), str(side), q, f, fill_ratio, float(ttl_sec), str(reason)])
    except Exception as e:
        logger.debug(f"[MAKER_FILL_LOG] write failed: {e}")


def _maker_ttl_order(ex: ExchangeClient, symbol: str, side: str, qty: float, ttl_sec: float, dryrun: bool) -> dict:
    order_start_ts = int(time.time() * 1000)
    if dryrun:
        out = {
            "filled_qty": qty,
            "avg_price": None,
            "reason": "DRYRUN_MAKER",
            "order": {"id": f"DRYRUN_{order_start_ts}"},
        }
    else:
        out = ex.create_maker_ttl_order(symbol, side, qty, ttl_sec, params=dict(_POST_ONLY_PARAMS))

    order_obj = out.get("order") if isinstance(out, dict) else None
    order_id = ""
    if isinstance(order_obj, dict):
        order_id = str(order_obj.get("id") or "").strip()
    if not order_id:
        order_id = str(out.get("order_id") or "").strip()
    order_key: tuple[str, str] | None = None
    if order_id:
        order_key = (str(symbol), str(order_id))
        _ttl_stats["maker_orders"] += 1
        _ttl_order_start_ms[order_key] = int(order_start_ts)

    filled = float(out.get("filled_qty") or 0.0)
    fill_ts = int(time.time() * 1000)
    if (filled > 0.0) and (order_key is not None):
        _ttl_stats["maker_filled"] += 1
        start_ms = _ttl_order_start_ms.pop(order_key, None)
        if start_ms is not None:
            _ttl_stats["fill_time_ms_total"] += max(0, int(fill_ts - int(start_ms)))
    else:
        reason_u = str(out.get("reason") or "").upper()
        if (order_key is not None) and (("CANCEL" in reason_u) or ("EXPIRE" in reason_u)):
            _ttl_stats["ttl_expired"] += 1
            _ttl_order_start_ms.pop(order_key, None)
    return out


def _exit_tp_live(ex: ExchangeClient, symbol: str, qty: float, dryrun: bool, store: StateStore) -> dict:
    blocked, b_reason = _is_maker_blocked(store, "TP", symbol)
    if blocked:
        return {"filled_qty": 0.0, "avg_price": None, "reason": f"TP_HOLD_MAKER_BLOCKED:{b_reason}"}

    ttl = float(getattr(C, "TP_MAKER_TTL_SEC", 6))
    maker_ttl_sec = getattr(C, "MAKER_TTL_SEC", None)
    if maker_ttl_sec is not None:
        ttl = float(maker_ttl_sec)
    maker = _maker_ttl_order(ex, symbol, "sell", float(qty), ttl, dryrun=dryrun)

    filled = float(maker.get("filled_qty") or 0.0)
    avg = maker.get("avg_price", None)
    maker_reason = str(maker.get("reason"))
    _log_maker_fill_csv(
        ts_ms=None,
        symbol=str(symbol),
        stage="EXIT_TP",
        side="sell",
        qty=float(qty),
        filled=float(filled),
        ttl_sec=float(ttl),
        reason=str(maker_reason),
    )

    min_fill_ratio = float(getattr(C, "TP_MIN_FILL_RATIO", 0.25))
    fill_ratio = (filled / float(qty)) if float(qty) > 0 else 0.0

    # record maker quality
    blocked_now, msg = _record_maker_result(
        store=store, kind="TP", symbol=symbol,
        filled_qty=filled, intended_qty=float(qty), min_fill_ratio=min_fill_ratio
    )
    if blocked_now:
        logger.warning(msg)

    if filled <= 0:
        return {"filled_qty": 0.0, "avg_price": avg, "reason": f"TP_MAKER_NO_FILL({fill_ratio:.2f})"}
    if fill_ratio < min_fill_ratio:
        return {"filled_qty": filled, "avg_price": avg, "reason": f"TP_MAKER_UNDERFILL({fill_ratio:.2f}):HOLD"}
    return {"filled_qty": filled, "avg_price": avg, "reason": f"TP_MAKER_OK(fill_ratio={fill_ratio:.2f})"}


def _exit_stop_live(ex: ExchangeClient, symbol: str, qty: float, dryrun: bool, store: StateStore) -> dict:
    blocked, b_reason = _is_maker_blocked(store, "TP", symbol)
    if blocked:
        return {"filled_qty": 0.0, "avg_price": None, "reason": f"STOP_HOLD_MAKER_BLOCKED:{b_reason}"}

    ttl = float(getattr(C, "STOP_MAKER_TTL_SEC", getattr(C, "TP_MAKER_TTL_SEC", 6)))
    maker_ttl_sec = getattr(C, "MAKER_TTL_SEC", None)
    if maker_ttl_sec is not None:
        ttl = float(maker_ttl_sec)
    maker = _maker_ttl_order(ex, symbol, "sell", float(qty), ttl, dryrun=dryrun)
    filled = float(maker.get("filled_qty") or 0.0)
    avg = maker.get("avg_price", None)
    maker_reason = str(maker.get("reason"))
    _log_maker_fill_csv(
        ts_ms=None,
        symbol=str(symbol),
        stage="EXIT_STOP",
        side="sell",
        qty=float(qty),
        filled=float(filled),
        ttl_sec=float(ttl),
        reason=str(maker_reason),
    )
    min_fill_ratio = float(getattr(C, "STOP_MIN_FILL_RATIO", getattr(C, "TP_MIN_FILL_RATIO", 0.25)))
    fill_ratio = (filled / float(qty)) if float(qty) > 0 else 0.0

    blocked_now, msg = _record_maker_result(
        store=store, kind="TP", symbol=symbol,
        filled_qty=filled, intended_qty=float(qty), min_fill_ratio=min_fill_ratio
    )
    if blocked_now:
        logger.warning(msg)

    if filled <= 0:
        return {"filled_qty": 0.0, "avg_price": avg, "reason": f"STOP_MAKER_NO_FILL({fill_ratio:.2f})"}
    if fill_ratio < min_fill_ratio:
        return {"filled_qty": filled, "avg_price": avg, "reason": f"STOP_MAKER_UNDERFILL({fill_ratio:.2f}):HOLD"}
    return {"filled_qty": filled, "avg_price": avg, "reason": f"STOP_MAKER_OK(fill_ratio={fill_ratio:.2f})"}
def _fee_rate_by_type(ftype: str) -> float:
    """
    ftype: "maker" | "taker"
    """
    maker, taker = _resolved_paper_fee_pair()
    return maker if str(ftype).lower() == "maker" else taker

def _side_slip_mult_side(side: str) -> float:
    """Backtest-aligned slip multiplier for PAPER fills.
    BUY: price * (1 + SLIPPAGE_BPS/10000)
    SELL: price * (1 - SLIPPAGE_BPS/10000)
    """
    slip_bps = float(getattr(C, "SLIPPAGE_BPS", 0.0) or 0.0)
    slip = slip_bps / 10000.0
    if slip <= 0:
        return 1.0
    s = str(side or "").lower()
    if s == "buy":
        return 1.0 + slip
    if s == "sell":
        return 1.0 - slip
    return 1.0

def _ensure_pnl_fee(
    res: dict,
    pos: dict,
    exit_exec: float,
    fee_rate_entry: float,
    fee_rate_exit: float,
    default_qty: float | None = None,
) -> dict:
    """
    Recalculate pnl and fee when StateStore leaves them empty in DRYRUN or replay flows.
    Keep pnl and fee aligned with backtest so verify_diff compares the same values.
    """
    try:
        pnl = res.get("pnl", None)
        fee = res.get("fee", None)
        force_recalc = str(_DIFF_TRACE_SOURCE) == "replay"
        need = force_recalc or (pnl is None) or (fee is None) or (float(pnl or 0.0) == 0.0 and float(fee or 0.0) == 0.0)

        if need:
            qty = float(res.get("qty") or (default_qty if default_qty is not None else 0.0) or pos.get("qty") or 0.0)
            entry_exec = float(res.get("entry_exec") or pos.get("entry_exec") or pos.get("entry_px") or 0.0)
            exit_exec_f = float(res.get("exit_exec") or exit_exec or 0.0)
            dir_ = (pos.get("direction") or pos.get("side") or pos.get("dir") or "long")

            if qty > 0 and entry_exec > 0 and exit_exec_f > 0:
                gross = (exit_exec_f - entry_exec) * qty
                if str(dir_).lower() == "short":
                    gross = (entry_exec - exit_exec_f) * qty
                fee_calc = (entry_exec * qty * float(fee_rate_entry)) + (exit_exec_f * qty * float(fee_rate_exit))
                res["pnl"] = float(gross)
                res["fee"] = float(fee_calc)
                res.setdefault("entry_exec", float(entry_exec))
                res.setdefault("exit_exec", float(exit_exec_f))
                res.setdefault("qty", float(qty))
    except Exception:
        pass
    return res


def _parse_ts_iso_to_utc_datetime(ts_iso: str) -> datetime:
    ts = str(ts_iso or "").strip()
    if not ts:
        raise ValueError("empty ts_iso")
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_ts_iso_to_jst_date(ts_iso: str) -> str:
    jst = timezone(timedelta(hours=9))
    return _parse_ts_iso_to_utc_datetime(ts_iso).astimezone(jst).strftime("%Y-%m-%d")


def _load_trades_csv_rows() -> list[dict]:
    path = Path(_export_path("trades.csv"))
    if not path.exists():
        legacy_path = BASE_DIR / "exports" / "trades.csv"
        if legacy_path.exists():
            path = legacy_path
    if not path.exists():
        return []

    rows: list[dict] = []
    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    ts_iso = str(row.get("ts_iso") or "").strip()
                    dt_utc = _parse_ts_iso_to_utc_datetime(ts_iso)
                except Exception:
                    continue
                item = dict(row)
                item["_ts_iso_sort"] = dt_utc.isoformat()
                item["_day_key_jst"] = _parse_ts_iso_to_jst_date(ts_iso)
                rows.append(item)
    except Exception as e:
        logger.warning("[EXPORT] trades.csv load failed: %s", e)
        return []

    rows.sort(key=lambda row: str(row.get("_ts_iso_sort") or ""))
    return rows


def _daily_agg_from_trades_csv(day_key_jst: str) -> dict:
    empty = {
        "net": 0.0,
        "trades": 0,
        "wins": 0,
        "losses": 0,
        "avg_win": 0.0,
        "avg_loss": 0.0,
        "max_consec_loss": 0,
    }
    net_total = 0.0
    trades = 0
    wins = 0
    losses = 0
    win_sum = 0.0
    loss_sum = 0.0
    loss_streak = 0
    max_consec_loss = 0

    for row in _load_trades_csv_rows():
        if str(row.get("_day_key_jst") or "") != str(day_key_jst):
            continue
        try:
            net = float(row.get("net") or 0.0)
        except Exception:
            continue

        trades += 1
        net_total += net
        if net > 0.0:
            wins += 1
            win_sum += net
            loss_streak = 0
        elif net < 0.0:
            losses += 1
            loss_sum += net
            loss_streak += 1
            max_consec_loss = max(max_consec_loss, loss_streak)
        else:
            # Keep flat trades neutral so they do not break an active loss streak.
            pass

    return {
        "net": float(net_total),
        "trades": int(trades),
        "wins": int(wins),
        "losses": int(losses),
        "avg_win": float(win_sum / wins) if wins > 0 else 0.0,
        "avg_loss": float(loss_sum / losses) if losses > 0 else 0.0,
        "max_consec_loss": int(max_consec_loss),
    }


def _compute_drawdown_stats_by_day(initial_equity: float) -> dict[str, dict]:
    rows = _load_trades_csv_rows()
    if not rows:
        return {}

    equity = float(initial_equity)
    peak_equity = float(initial_equity)
    max_dd_abs_alltime = 0.0
    max_dd_pct_alltime = 0.0
    stats_by_day: dict[str, dict] = {}

    for row in rows:
        day_key_jst = str(row.get("_day_key_jst") or "")
        if not day_key_jst:
            continue
        try:
            net = float(row.get("net") or 0.0)
        except Exception:
            continue

        equity += net
        peak_equity = max(float(peak_equity), float(equity))
        dd_abs = max(0.0, float(peak_equity) - float(equity))
        dd_pct = (dd_abs / float(peak_equity)) if float(peak_equity) > 0.0 else 0.0
        max_dd_abs_alltime = max(float(max_dd_abs_alltime), float(dd_abs))
        max_dd_pct_alltime = max(float(max_dd_pct_alltime), float(dd_pct))

        day_stats = stats_by_day.setdefault(
            day_key_jst,
            {
                "day_max_dd_abs": 0.0,
                "day_max_dd_pct": 0.0,
                "max_dd_abs_alltime": 0.0,
                "max_dd_pct_alltime": 0.0,
            },
        )
        day_stats["day_max_dd_abs"] = max(float(day_stats.get("day_max_dd_abs") or 0.0), float(dd_abs))
        day_stats["day_max_dd_pct"] = max(float(day_stats.get("day_max_dd_pct") or 0.0), float(dd_pct))
        day_stats["max_dd_abs_alltime"] = float(max_dd_abs_alltime)
        day_stats["max_dd_pct_alltime"] = float(max_dd_pct_alltime)

    return stats_by_day

# ---------------------------
# main
# ---------------------------

def main(
    ex_override: Any | None = None,
    store_override: Any | None = None,
    symbols_override: list[str] | None = None,
    mode_override: str | None = None,
    dryrun_override: bool | None = None,
    replay_ts_ms: int | None = None,
    skip_startup_ops: bool = False,
    skip_exports: bool = False,
) -> int:
    global _RUNNER_STARTED, _STARTUP_LOG_DONE, _PAPER_EQUITY_STATE_LOG_DONE, _LIVE_RESUME_GUARD_DONE, _LIVE_RESUME_FORCE_STOP_NEW_ONLY, _STARTUP_OPS_DONE, _LIVE_EQUITY_SYMBOLS, _LIVE_EQUITY_FOR_SIZING, _LIVE_EQUITY_REFRESH_AFTER_CLOSE_LAST_TS

    startup_symbols = list(symbols_override) if symbols_override is not None else (_symbols_from_env() or list(getattr(C, "SYMBOLS", []) or []))
    if not startup_symbols:
        startup_symbols = [_default_symbol_for_runtime(_resolve_exchange_id())]
    if not _STARTUP_LOG_DONE:
        logger.info("SYMBOLS(%s): %s", len(startup_symbols), startup_symbols)
        _STARTUP_LOG_DONE = True
    logging.basicConfig(
        level=getattr(logging, str(getattr(C, "LOG_LEVEL", "INFO")).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stdout,
        force=True,
    )
    _install_runtime_log_filter()

    mode_env_override = _resolve_mode_override_env()
    mode = str(mode_override).upper() if mode_override is not None else (mode_env_override or str(getattr(C, "MODE", "PAPER")).upper())  # PAPER / LIVE
    _reject_live_for_free_build(mode)
    os.environ["LWF_RUNTIME_MODE"] = str(mode)
    if dryrun_override is not None:
        dryrun = bool(dryrun_override)
    else:
        if mode == "LIVE":
            dryrun = bool(getattr(C, "LIVE_DRYRUN", False))
        else:
            dryrun = True  # PAPER mode always uses simulated execution.

    if not _RUNNER_STARTED:
        exchange_id = _resolve_exchange_id()
        logger.info("[BOOT] pid=%s mode=%s dryrun=%s exchange_id=%s", os.getpid(), mode, dryrun, exchange_id)
        _log_effective_range_config(
            logger,
            label="RUNNER",
            force_disable_be=False,
            tp1_requires_flag=True,
        )
        _RUNNER_STARTED = True

    # CFG CHECK (trend-disabled / replay-diff diagnostics)
    if _cfg_check_enabled():
        try:
            logger.info(
            "CFG CHECK: "
            f"mode={mode} dryrun={dryrun} "
            f"TRADE_TREND={getattr(C,'TRADE_TREND',None)} "
            f"TRADE_RANGE={getattr(C,'TRADE_RANGE',None)} "
            f"TRADE_ONLY_TREND={getattr(C,'TRADE_ONLY_TREND',None)} "
            f"TREND_ENTRY_MODE={getattr(C,'TREND_ENTRY_MODE',None)} "
            f"RANGE_ENTRY_MODE={getattr(C,'RANGE_ENTRY_MODE',None)}"
        )
        except Exception:
            pass

    ex = ex_override if ex_override is not None else ExchangeClient()
    env_symbols = _symbols_from_env()
    tf_entry = str(getattr(C, "ENTRY_TF", getattr(C, "TIMEFRAME_ENTRY", "5m")))
    tf_filter = str(getattr(C, "FILTER_TF", getattr(C, "TIMEFRAME_FILTER", "1h")))
    if symbols_override is not None:
        symbols_seed = list(symbols_override)
    elif env_symbols:
        symbols_seed = list(env_symbols)
    else:
        symbols_seed = list(getattr(C, "SYMBOLS", []))
    if not symbols_seed:
        symbols_seed = [_default_symbol_for_runtime(_resolve_exchange_id())]
    replay_mode = bool(replay_ts_ms is not None)
    if store_override is not None:
        store = store_override
    else:
        ctx = _state_context_for_run(mode=str(mode), symbols=symbols_seed, replay_mode=replay_mode)
        store, _ = _open_state_store_for_context(ctx=ctx, symbols=symbols_seed)

    # time window -> STOP_NEW_ONLY
    stop_new_only_active = bool(_LIVE_RESUME_FORCE_STOP_NEW_ONLY)
    if not skip_startup_ops:
        ok_tw, tw_reason = _in_time_window_jst()
        if not ok_tw:
            logger.warning(f"[TIME WINDOW] {tw_reason} -> STOP_NEW_ONLY for this run")
            stop_new_only_active = True

    # STOP file snapshot
    if not skip_startup_ops:
        stop_res = check_and_update_emergency_stop(store=store, base_dir=str(getattr(C, "STOP_FILE_DIR", ".")), phase="START")
        if stop_res.should_stop:
            if stop_res.stop_mode == "STOP":
                logger.error(f"STOP engaged at START -> force close all and exit. reason={stop_res.stop_reason}")
                # close all positions (best-effort)
                try:
                    for p in store.list_positions():
                        sym = str(p["symbol"])
                        qty = float(p["qty"])
                        _exit_stop_live(ex, sym, qty, dryrun=dryrun, store=store)
                        store.close_position(sym, exit_price=0.0, candle_ts_exit=int(candle_ts_run), reason="FORCE_STOP", fee_rate=0.0)
                        _clear_pos_init_stop(store, sym)
                        _clear_pos_max_fav(store, sym)
                        _clear_pos_min_adv(store, sym)
                        _clear_pos_meta(store, sym)
                except Exception as e:
                    logger.warning(f"force close failed: {e}")
                return 0
            if stop_res.stop_mode == "STOP_NEW_ONLY":
                stop_new_only_active = True
                logger.warning(f"STOP_NEW_ONLY at START -> new entries blocked. reason={stop_res.stop_reason}")
    # symbols
    if symbols_override is not None:
        symbols = list(symbols_override)
    elif env_symbols:
        symbols = list(env_symbols)
    else:
        symbols = _select_symbols(ex, store)
    _LIVE_EQUITY_SYMBOLS = list(symbols)
    if (not replay_mode) and (str(mode).upper() == "PAPER") and (not skip_startup_ops):
        paper_since_ms, paper_until_ms = _live_like_preflight_window_ms()
        paper_dataset_year = infer_year_from_ms(int(paper_since_ms))
        if paper_dataset_year is None:
            paper_dataset_year = int(datetime.fromtimestamp(int(paper_since_ms) / 1000.0, tz=timezone.utc).year)
        paper_dataset_root = os.path.abspath(str(getattr(ensure_runtime_dirs(), "market_data_dir", ".") or "."))
        for symbol in list(symbols or []):
            symbol_text = str(symbol or "").strip()
            if not symbol_text:
                continue
            symbol_prefix = symbol_to_prefix(symbol_text)
            _runner_runtime_data_preflight(
                context="PAPER",
                runtime_symbol=str(symbol_text),
                entry_tf=str(tf_entry),
                filter_tf=str(tf_filter),
                since_ms=int(paper_since_ms),
                until_ms=int(paper_until_ms),
                dataset_year=None,
                dataset_years=None,
                dataset_root=str(paper_dataset_root),
                prefix=str(symbol_prefix),
                default_dir_5m=str(os.path.join(paper_dataset_root, f"{symbol_prefix}_5m_{int(paper_dataset_year):04d}")),
                default_glob_5m=str(f"{symbol_prefix}-5m-{int(paper_dataset_year):04d}-*.csv"),
                default_dir_1h=str(os.path.join(paper_dataset_root, f"{symbol_prefix}_1h_{int(paper_dataset_year):04d}")),
                default_glob_1h=str(f"{symbol_prefix}-1h-{int(paper_dataset_year):04d}-*.csv"),
            )
    if not skip_startup_ops:
        for symbol in symbols[:1]:
            try:
                ex.get_market_rules(symbol)
            except Exception as e:
                logger.warning("[MARKET_RULES] failed symbol=%s reason=%s", symbol, e)
            try:
                meta, meta_source, meta_cache_path = maybe_refresh_market_meta(
                    ex,
                    exchange_id=_resolve_exchange_id(),
                    symbol=symbol,
                    state_dir=str(STATE_DIR),
                    cache_ttl_sec=_market_meta_cache_ttl_sec(),
                    allow_refresh=True,
                )
                if meta is not None:
                    _apply_market_meta_runtime(meta, _resolve_exchange_id())
                    logger.info(
                        "[MARKET_META] exchange_id=%s symbol=%s quote=%s maker=%.6f taker=%.6f spread_bps=%.4f source=%s cache=%s",
                        _resolve_exchange_id(),
                        str(meta.symbol),
                        str(meta.quote_ccy or _quote_ccy_for_symbol(symbol)),
                        float(meta.maker_fee_rate),
                        float(meta.taker_fee_rate),
                        float(meta.spread_bps),
                        str(meta_source),
                        str(meta_cache_path),
                    )
                    fee_hint = ex.fetch_recent_trade_fee_hint(symbol)
                    if isinstance(fee_hint, dict):
                        logger.info(
                            "[MARKET_FEES_HINT] symbol=%s effective_fee_rate=%.6f trades=%s source=%s",
                            str(symbol),
                            float(fee_hint.get("effective_fee_rate") or 0.0),
                            int(fee_hint.get("trade_count") or 0),
                            str(fee_hint.get("source") or ""),
                        )
                    _bid, ask = ex.fetch_best_bid_ask(symbol)
                    price_hint = None
                    if _bid is not None and ask is not None and float(_bid) > 0.0 and float(ask) > 0.0:
                        price_hint = (float(_bid) + float(ask)) / 2.0
                    elif ask is not None and float(ask) > 0.0:
                        price_hint = float(ask)
                    elif _bid is not None and float(_bid) > 0.0:
                        price_hint = float(_bid)
                    if price_hint is not None:
                        seed_source = "runtime_tap_best_bid_ask_mid"
                        if not (_bid is not None and ask is not None and float(_bid) > 0.0 and float(ask) > 0.0):
                            seed_source = "runtime_tap_best_ask" if ask is not None and float(ask) > 0.0 else "runtime_tap_best_bid"
                        price_hint_ts_ms = int(time.time() * 1000)
                        _chart_state_remember_runtime_tapped_price(
                            symbol,
                            price=float(price_hint),
                            ts_ms=int(price_hint_ts_ms),
                            source=str(seed_source),
                        )
                        _chart_state_seed_builder_observe(
                            symbol,
                            str(getattr(C, "ENTRY_TF", getattr(C, "TIMEFRAME_ENTRY", "5m"))),
                            ts_ms=int(price_hint_ts_ms),
                            price=float(price_hint),
                            source=str(seed_source),
                        )
                    if meta.quote_ccy == "JPY" and ask is not None and float(ask) > 0.0:
                        min_oper_jpy = estimate_min_operational_balance(meta, float(ask))
                        logger.info(
                            "[MIN_BALANCE] symbol=%s min_oper_jpy=%.2f (min_cost=%.2f min_qty=%.8f ask=%.2f)",
                            str(symbol),
                            float(min_oper_jpy),
                            float(meta.min_cost),
                            float(meta.min_qty),
                            float(ask),
                        )
                else:
                    logger.info(
                        "[MARKET_META] exchange_id=%s symbol=%s unavailable source=%s cache=%s",
                        _resolve_exchange_id(),
                        str(symbol),
                        str(meta_source),
                        str(meta_cache_path),
                    )
            except Exception as e:
                logger.warning("[MARKET_META] refresh failed symbol=%s reason=%s", symbol, e)
    # startup logs are printed only once (see _STARTUP_LOG_DONE)
    if not symbols:
        logger.error("No symbols. Set config.SYMBOLS or enable AUTO_SELECT_SYMBOLS.")
        return 0

    if (not skip_startup_ops) and (not _LIVE_RESUME_GUARD_DONE):
        for symbol in symbols:
            try:
                pos = store.get_position(symbol)
            except Exception:
                pos = None
            if not pos:
                continue
            sym_key = str(symbol)
            if sym_key in _RESUME_LOGGED_SYMBOLS:
                continue
            try:
                qty = float(pos.get("qty") or 0.0)
            except Exception:
                qty = 0.0
            try:
                entry = float(pos.get("entry") or 0.0)
            except Exception:
                entry = 0.0
            try:
                stop_px = float(pos.get("stop") or 0.0)
            except Exception:
                stop_px = 0.0
            try:
                tp_px = float(pos.get("take_profit") or pos.get("tp") or 0.0)
            except Exception:
                tp_px = 0.0
            logger.info(
                "[RESUME] found persisted position symbol=%s qty=%.8f entry=%.8f stop=%.8f tp=%.8f",
                str(symbol),
                float(qty),
                float(entry),
                float(stop_px),
                float(tp_px),
            )
            _RESUME_LOGGED_SYMBOLS.add(sym_key)

        if str(mode).upper() == "LIVE" and (not dryrun):
            min_base_qty = float(getattr(C, "LIVE_RESUME_MIN_BASE_QTY", 0.0001) or 0.0001)
            bal = _fetch_balance_snapshot(ex)
            mismatch = False
            if isinstance(bal, dict):
                for symbol in symbols:
                    try:
                        pos = store.get_position(symbol)
                    except Exception:
                        pos = None
                    if pos:
                        continue
                    base_asset = _symbol_base_asset(symbol)
                    held_qty = float(_balance_asset_qty(bal, base_asset))
                    if held_qty >= float(min_base_qty):
                        logger.warning(
                            "[RESUME][MISMATCH] exchange shows base holding but no persisted position state -> STOP_NEW_ONLY symbol=%s base=%s held=%.8f min=%.8f",
                            str(symbol),
                            str(base_asset),
                            float(held_qty),
                            float(min_base_qty),
                        )
                        mismatch = True
            if mismatch:
                stop_new_only_active = True
                _LIVE_RESUME_FORCE_STOP_NEW_ONLY = True

        _LIVE_RESUME_GUARD_DONE = True

    # summary counters
    summ_hold = Counter()
    summ_evt = Counter()
    summ_regime = Counter()

    sizing_mode_cfg = str(
        os.getenv(
            "POSITION_SIZING_MODE",
            getattr(C, "POSITION_SIZING_MODE", "LEGACY"),
        )
    ).upper()

    # equity for sizing
    initial_paper_equity = float(getattr(C, "PAPER_INITIAL_EQUITY", getattr(C, "INITIAL_EQUITY", 300000.0)))
    equity_for_sizing = float(initial_paper_equity)
    equity_currency_for_state = (
        str(_CURRENT_STATE_CONTEXT.account_ccy)
        if _CURRENT_STATE_CONTEXT is not None
        else _quote_ccy_for_symbol(str(symbols[0] if symbols else ""))
    )
    # NOTE: In dryrun/REPLAY we compound sizing equity locally to match backtest behavior.
    replay_state_enabled = bool(dryrun) and (replay_ts_ms is not None)
    paper_state_enabled = bool(dryrun) and (replay_ts_ms is None)
    paper_bootstrap_enabled = (str(mode).upper() == "PAPER") and (replay_ts_ms is None)
    paper_state_db_path = str(_CURRENT_STATE_CONTEXT_PATHS.get("db_path") or getattr(store, "db_path", ""))
    if paper_state_enabled or paper_bootstrap_enabled:
        eq_state = store.get_equity_state()
        loaded_eq = float(eq_state.get("current_equity") or 0.0)
        loaded_eq_updated_ts = int(eq_state.get("last_updated_ts") or 0)
        if loaded_eq > 0.0 and loaded_eq_updated_ts > 0:
            equity_for_sizing = float(loaded_eq)
            if not _PAPER_EQUITY_STATE_LOG_DONE:
                logger.info(
                    "[SIZING][STATE] loaded context equity=%.6f cur=%s db=%s",
                    float(equity_for_sizing),
                    str(eq_state.get("equity_currency") or equity_currency_for_state),
                    str(paper_state_db_path),
                )
                _PAPER_EQUITY_STATE_LOG_DONE = True
        else:
            if loaded_eq > 0.0:
                logger.warning(
                    "[SIZING][STATE] ignored incomplete context equity=%.6f cur=%s updated=%s db=%s",
                    float(loaded_eq),
                    str(eq_state.get("equity_currency") or equity_currency_for_state),
                    int(loaded_eq_updated_ts),
                    str(paper_state_db_path),
                )
            store.upsert_equity_state(
                initial_equity=float(initial_paper_equity),
                current_equity=float(equity_for_sizing),
                peak_equity=float(equity_for_sizing),
                equity_currency=str(equity_currency_for_state),
                realized_pnl=0.0,
                unrealized_pnl=0.0,
                weekly_base=float(equity_for_sizing),
                daily_base=float(equity_for_sizing),
                dd_stop_flags={},
                week_key=str(_iso_week_key(datetime.now())),
                day_key=str(_risk_day_key_from_ts_ms(int(time.time() * 1000))),
                last_updated_ts=int(time.time()),
            )
            if not _PAPER_EQUITY_STATE_LOG_DONE:
                logger.info(
                    "[SIZING][STATE] bootstrapped context equity=%.6f cur=%s db=%s",
                    float(equity_for_sizing),
                    str(equity_currency_for_state),
                    str(paper_state_db_path),
                )
                _PAPER_EQUITY_STATE_LOG_DONE = True
    if str(mode).upper() == "LIVE" and (not dryrun):
        if _LIVE_EQUITY_FOR_SIZING is not None:
            equity_for_sizing = max(0.0, float(_LIVE_EQUITY_FOR_SIZING))

    # Process-once startup ops guard (avoid repeated cancel/equity sync in live loop).
    if (not skip_startup_ops) and (not _STARTUP_OPS_DONE):
        _cancel_open_orders_on_start(ex, symbols, mode=mode, dryrun=dryrun)
        if mode == "LIVE" and (not dryrun) and bool(getattr(C, "LIVE_EQUITY_SYNC_ENABLED", True)) and bool(getattr(C, "LIVE_EQUITY_SYNC_ON_START", True)):
            min_eq = float(getattr(C, "MIN_EQUITY_FOR_LIVE_SYNC", 10.0))
            live_eq = _get_live_equity(ex)

            if live_eq is not None and live_eq >= min_eq:
                equity_for_sizing = float(live_eq)
                _LIVE_EQUITY_FOR_SIZING = float(live_eq)
                logger.info(f"[LIVE] Equity synced on start: {equity_for_sizing:.6f}")
            else:
                equity_for_sizing = 0.0
                _LIVE_EQUITY_FOR_SIZING = 0.0
                stop_new_only_active = True
                _LIVE_RESUME_FORCE_STOP_NEW_ONLY = True
                logger.error(
                    "[LIVE] Equity sync skipped (live_eq=%s, min=%s) -> STOP_NEW_ONLY for this run; equity_for_sizing=0.0",
                    live_eq,
                    min_eq,
                )
        _STARTUP_OPS_DONE = True
        logger.info("[STARTUP] ops_done=True")

    sizing_initial_equity = float(equity_for_sizing) if float(equity_for_sizing) > 0 else 1.0
    sizing_peak_equity = float(equity_for_sizing) if float(equity_for_sizing) > 0 else float(sizing_initial_equity)
    if replay_state_enabled and isinstance(_REPLAY_SIZING_STATE, dict):
        replay_initial_eq = float(_REPLAY_SIZING_STATE.get("initial_equity") or 0.0)
        replay_current_eq = float(_REPLAY_SIZING_STATE.get("current_equity") or 0.0)
        replay_peak_eq = float(_REPLAY_SIZING_STATE.get("peak_equity") or 0.0)
        if replay_initial_eq > 0.0:
            sizing_initial_equity = float(replay_initial_eq)
        if replay_current_eq > 0.0:
            equity_for_sizing = float(replay_current_eq)
        else:
            equity_for_sizing = float(sizing_initial_equity)
        if replay_peak_eq > 0.0:
            sizing_peak_equity = float(replay_peak_eq)
        else:
            sizing_peak_equity = max(float(sizing_initial_equity), float(equity_for_sizing))
    kill_state: Dict[str, Any] = {"halted": False, "reason": "", "cooldown_until_day": ""}

    def _apply_compounded_close_net(net: float) -> None:
        nonlocal equity_for_sizing, sizing_peak_equity
        if sizing_mode_cfg == "LEGACY_COMPOUND":
            equity_for_sizing = max(0.0, float(equity_for_sizing) + float(net))
            sizing_peak_equity = max(float(sizing_peak_equity), float(equity_for_sizing))
            if paper_state_enabled:
                eq_state_cur = store.get_equity_state()
                initial_eq = float(eq_state_cur.get("initial_equity") or initial_paper_equity)
                peak_eq = max(float(eq_state_cur.get("peak_equity") or 0.0), float(equity_for_sizing))
                store.upsert_equity_state(
                    initial_equity=float(initial_eq),
                    current_equity=float(equity_for_sizing),
                    peak_equity=float(peak_eq),
                    equity_currency=str(eq_state_cur.get("equity_currency") or equity_currency_for_state),
                    realized_pnl=float(equity_for_sizing - initial_eq),
                    unrealized_pnl=0.0,
                    weekly_base=float(eq_state_cur.get("weekly_base") or equity_for_sizing),
                    daily_base=float(eq_state_cur.get("daily_base") or equity_for_sizing),
                    dd_stop_flags=(eq_state_cur.get("dd_stop_flags") if isinstance(eq_state_cur.get("dd_stop_flags"), dict) else {}),
                    week_key=str(eq_state_cur.get("week_key") or _iso_week_key(datetime.now())),
                    day_key=str(eq_state_cur.get("day_key") or _risk_day_key_from_ts_ms(int(time.time() * 1000))),
                    last_updated_ts=int(time.time()),
                )
            _sync_replay_sizing_state()

    def _refresh_live_equity_after_close() -> None:
        nonlocal equity_for_sizing, sizing_peak_equity
        global _LIVE_EQUITY_FOR_SIZING, _LIVE_EQUITY_REFRESH_AFTER_CLOSE_LAST_TS
        if str(mode).upper() != "LIVE" or bool(dryrun):
            return
        now_sec = float(time.time())
        if (now_sec - float(_LIVE_EQUITY_REFRESH_AFTER_CLOSE_LAST_TS)) < 60.0:
            return
        _LIVE_EQUITY_REFRESH_AFTER_CLOSE_LAST_TS = float(now_sec)
        prev_eq = float(equity_for_sizing)
        try:
            live_eq = _get_live_equity(ex)
            if live_eq is None or float(live_eq) <= 0.0:
                logger.warning("[LIVE] Equity refresh after close failed")
                return
            new_eq = float(live_eq)
            _LIVE_EQUITY_FOR_SIZING = float(new_eq)
            equity_for_sizing = float(new_eq)
            sizing_peak_equity = max(float(sizing_peak_equity), float(new_eq))
            cur = str(_LIVE_EQUITY_LAST_CUR or "").strip().upper() or "USDT"
            logger.info(
                "[LIVE] Equity refreshed after close: prev=%.6f new=%.6f delta=%.6f cur=%s",
                float(prev_eq),
                float(new_eq),
                float(new_eq - prev_eq),
                str(cur),
            )
        except Exception:
            logger.warning("[LIVE] Equity refresh after close failed")

    def _kill_add_days(day_key: str, plus_days: int) -> str:
        try:
            dt = datetime.strptime(str(day_key), "%Y-%m-%d")
            dt2 = dt + timedelta(days=max(0, int(plus_days)))
            return dt2.strftime("%Y-%m-%d")
        except Exception:
            return str(day_key)

    def _kill_should_block_new_entries(*, ts_ms: int, spread_bps: float | None) -> tuple[bool, str]:
        if not bool(getattr(C, "KILL_SWITCH_ENABLED", False)):
            return False, ""

        day_key = _risk_day_key_from_ts_ms(int(ts_ms))
        if bool(kill_state.get("halted", False)):
            return True, str(kill_state.get("reason") or "halted")

        cd_until = str(kill_state.get("cooldown_until_day") or "")
        if cd_until and day_key <= cd_until:
            return True, f"cooldown(until={cd_until})"

        payload = store.get_daily_metrics(day_key) or {}
        if not isinstance(payload, dict):
            payload = {}

        day_start_equity = float(payload.get("kill_day_start_equity", 0.0) or 0.0)
        if day_start_equity <= 0.0:
            day_start_equity = float(equity_for_sizing) if float(equity_for_sizing) > 0.0 else float(getattr(C, "INITIAL_EQUITY", 0.0) or 0.0)
            payload["kill_day_start_equity"] = float(day_start_equity)
            try:
                store.record_daily_metrics(day_key, payload)
            except Exception:
                pass

        risk = payload.get("risk") if isinstance(payload.get("risk"), dict) else {}
        day_pnl = float(risk.get("daily_pnl_jpy", 0.0) or 0.0)
        consec_losses = int(risk.get("loss_streak", 0) or 0)
        kill_equity_ccy = str(equity_currency_for_state)
        if str(mode).upper() == "LIVE" and (not dryrun):
            kill_equity_ccy, kill_equity_ccy_source = _resolve_live_equity_currency()
            _maybe_log_live_equity_currency_resolution(
                equity_ccy=kill_equity_ccy,
                source=str(kill_equity_ccy_source),
            )
        kill_min_equity, kill_min_equity_source = _resolve_equity_floor_by_ccy(
            floor_scalar=float(getattr(C, "KILL_MIN_EQUITY", 0.0) or 0.0),
            floor_map=getattr(C, "KILL_MIN_EQUITY_BY_CCY", None) or None,
            equity_ccy=kill_equity_ccy,
            resolve_mode=str(getattr(C, "KILL_MIN_EQUITY_RESOLVE_MODE", "AUTO") or "AUTO"),
        )
        _maybe_log_kill_min_equity_resolution(
            equity_ccy=kill_equity_ccy,
            floor=float(kill_min_equity),
            source=str(kill_min_equity_source),
        )

        ctx: Dict[str, Any] = {
            "enabled": True,
            "equity": float(equity_for_sizing),
            "peak_equity": float(sizing_peak_equity),
            "day_pnl": float(day_pnl),
            "day_start_equity": float(day_start_equity),
            "consec_losses": int(consec_losses),
            "spread_bps": spread_bps,
            "kill_max_dd_pct": float(getattr(C, "KILL_MAX_DD_PCT", 0.0) or 0.0),
            "kill_max_daily_loss_pct": float(getattr(C, "KILL_MAX_DAILY_LOSS_PCT", 0.0) or 0.0),
            "kill_max_consec_losses": int(getattr(C, "KILL_MAX_CONSEC_LOSSES", 0) or 0),
            "kill_max_spread_bps": float(getattr(C, "KILL_MAX_SPREAD_BPS", 0.0) or 0.0),
            "kill_min_equity": float(kill_min_equity),
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
                float(equity_for_sizing),
                float(sizing_peak_equity),
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
            kill_state["cooldown_until_day"] = _kill_add_days(day_key, max(0, cooldown_days - 1))
        else:
            kill_state["halted"] = True
        kill_state["reason"] = str(reason)
        return True, str(reason)

    dd_smooth_enabled_env = _env_flag("DD_DELEVER_SMOOTH_ENABLED", default=None)
    if dd_smooth_enabled_env is None:
        dd_smooth_enabled = bool(getattr(C, "DD_DELEVER_SMOOTH_ENABLED", False))
    else:
        dd_smooth_enabled = bool(dd_smooth_enabled_env)
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
    if replay_state_enabled and isinstance(_REPLAY_SIZING_STATE, dict):
        sizing_dd_ema = max(0.0, float(_REPLAY_SIZING_STATE.get("dd_ema") or sizing_dd_ema))

    def _sync_replay_sizing_state() -> None:
        nonlocal equity_for_sizing, sizing_initial_equity, sizing_peak_equity, sizing_dd_ema
        global _REPLAY_SIZING_STATE
        if not replay_state_enabled:
            return
        _REPLAY_SIZING_STATE = {
            "initial_equity": float(sizing_initial_equity),
            "current_equity": float(equity_for_sizing),
            "peak_equity": float(sizing_peak_equity),
            "dd_ema": float(sizing_dd_ema),
        }

    _sync_replay_sizing_state()
    profit_only_enabled_env = _env_flag("LEGACY_COMPOUND_PROFIT_ONLY_ENABLED", default=None)
    if profit_only_enabled_env is None:
        profit_only_enabled = bool(getattr(C, "LEGACY_COMPOUND_PROFIT_ONLY_ENABLED", False))
    else:
        profit_only_enabled = bool(profit_only_enabled_env)
    try:
        _profit_w_env = str(os.getenv("LEGACY_COMPOUND_PROFIT_REINVEST_W", "") or "").strip()
        profit_reinvest_w = float(_profit_w_env) if _profit_w_env != "" else float(getattr(C, "LEGACY_COMPOUND_PROFIT_REINVEST_W", 1.0))
    except Exception:
        profit_reinvest_w = 1.0
    if not math.isfinite(profit_reinvest_w):
        profit_reinvest_w = 1.0
    profit_floor_env = _env_flag("LEGACY_COMPOUND_PROFIT_ONLY_FLOOR_TO_INITIAL", default=None)
    if profit_floor_env is None:
        profit_only_floor_to_initial = bool(getattr(C, "LEGACY_COMPOUND_PROFIT_ONLY_FLOOR_TO_INITIAL", True))
    else:
        profit_only_floor_to_initial = bool(profit_floor_env)
    profit_w_ramp_env = _env_flag("LEGACY_COMPOUND_PROFIT_W_RAMP_ENABLED", default=None)
    if profit_w_ramp_env is None:
        profit_w_ramp_enabled = bool(getattr(C, "LEGACY_COMPOUND_PROFIT_W_RAMP_ENABLED", False))
    else:
        profit_w_ramp_enabled = bool(profit_w_ramp_env)
    try:
        _profit_w_ramp_pct_env = str(os.getenv("LEGACY_COMPOUND_PROFIT_W_RAMP_PCT", "") or "").strip()
        profit_w_ramp_pct = float(_profit_w_ramp_pct_env) if _profit_w_ramp_pct_env != "" else float(getattr(C, "LEGACY_COMPOUND_PROFIT_W_RAMP_PCT", 0.30))
    except Exception:
        profit_w_ramp_pct = 0.30
    if not math.isfinite(profit_w_ramp_pct):
        profit_w_ramp_pct = 0.30
    profit_w_ramp_pct = max(0.0, float(profit_w_ramp_pct))
    try:
        _profit_w_ramp_shape_env = str(os.getenv("LEGACY_COMPOUND_PROFIT_W_RAMP_SHAPE", "") or "").strip()
        profit_w_ramp_shape = float(_profit_w_ramp_shape_env) if _profit_w_ramp_shape_env != "" else float(getattr(C, "LEGACY_COMPOUND_PROFIT_W_RAMP_SHAPE", 2.0))
    except Exception:
        profit_w_ramp_shape = 2.0
    if not math.isfinite(profit_w_ramp_shape):
        profit_w_ramp_shape = 2.0
    profit_w_ramp_shape = max(0.0, float(profit_w_ramp_shape))
    try:
        _profit_w_ramp_min_g_env = str(os.getenv("LEGACY_COMPOUND_PROFIT_W_RAMP_MIN_G", "") or "").strip()
        profit_w_ramp_min_g = float(_profit_w_ramp_min_g_env) if _profit_w_ramp_min_g_env != "" else float(getattr(C, "LEGACY_COMPOUND_PROFIT_W_RAMP_MIN_G", 0.0))
    except Exception:
        profit_w_ramp_min_g = 0.0
    if not math.isfinite(profit_w_ramp_min_g):
        profit_w_ramp_min_g = 0.0
    profit_w_ramp_min_g = max(0.0, float(profit_w_ramp_min_g))
    size_min_bump_env = _env_flag("SIZE_MIN_BUMP_ENABLED", default=None)
    if size_min_bump_env is None:
        size_min_bump_enabled = bool(getattr(C, "SIZE_MIN_BUMP_ENABLED", False))
    else:
        size_min_bump_enabled = bool(size_min_bump_env)
    try:
        _size_min_bump_max_env = str(os.getenv("SIZE_MIN_BUMP_MAX_PCT_OF_CAP", "") or "").strip()
        size_min_bump_max_pct_of_cap = float(_size_min_bump_max_env) if _size_min_bump_max_env != "" else float(getattr(C, "SIZE_MIN_BUMP_MAX_PCT_OF_CAP", 0.10))
    except Exception:
        size_min_bump_max_pct_of_cap = 0.10
    if not math.isfinite(size_min_bump_max_pct_of_cap):
        size_min_bump_max_pct_of_cap = 0.10
    size_min_bump_max_pct_of_cap = max(0.0, float(size_min_bump_max_pct_of_cap))
    size_cap_ramp_env = _env_flag("SIZE_CAP_RAMP_ENABLED", default=None)
    if size_cap_ramp_env is None:
        size_cap_ramp_enabled = bool(getattr(C, "SIZE_CAP_RAMP_ENABLED", False))
    else:
        size_cap_ramp_enabled = bool(size_cap_ramp_env)
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
    size_cap_ramp_max_pct = _resolve_size_cap_ramp_max_pct(
        float(getattr(C, "MAX_POSITION_NOTIONAL_PCT", 0.10)),
        float(size_cap_ramp_max_cfg),
    )
    size_dbg_env = _env_flag("SIZE_SIZING_DEBUG_LOG_ENABLED", default=None)
    if size_dbg_env is None:
        size_sizing_debug_log_enabled = bool(getattr(C, "SIZE_SIZING_DEBUG_LOG_ENABLED", False))
    else:
        size_sizing_debug_log_enabled = bool(size_dbg_env)
    # NOTE: dd_ema is in-memory only and resets on process restart.

    phase_seq = 0

    # prevent same candle re-run
    try:
        if replay_ts_ms is not None:
            entry_bar_ms = _timeframe_to_ms(tf_entry)
            # ReplayExchange already exposes only confirmed bars relative to replay_ts_ms.
            # Keep candle_ts_run on the current replay clock so 5m entries, 1h filters,
            # trace timestamps, and opened_ts all line up with backtest/live semantics.
            candle_ts_run = int(replay_ts_ms)
        elif _resolve_exchange_id() == "coincheck":
            entry_bar_ms = _timeframe_to_ms(tf_entry)
            now_ms_run = int(time.time() * 1000)
            candle_ts_run = max(0, ((now_ms_run // entry_bar_ms) - 1) * entry_bar_ms)
        else:
            e_first = ohlcv_to_dict(ex.fetch_ohlcv(symbols[0], tf_entry, limit=200))
            now_ms_run = int(replay_ts_ms) if replay_ts_ms is not None else int(time.time() * 1000)

            entry_bar_ms = _timeframe_to_ms(tf_entry)

            ci0 = _confirmed_candle_idx(e_first["timestamp"], entry_bar_ms, now_ms_run)
            try:
                cut0 = len(e_first.get("timestamp") or [])
                if ci0 == -2:
                    cut0 = max(0, int(cut0) - 1)
                if cut0 > 0:
                    bootstrap_ohlcv = {
                        _k: list((e_first.get(_k) or [])[:cut0])
                        for _k in ("timestamp", "open", "high", "low", "close", "volume")
                    }
                    _chart_state_store_internal_bootstrap_ohlcv(str(symbols[0] if symbols else ""), bootstrap_ohlcv)
                elif len(e_first.get("timestamp") or []) > 0:
                    _chart_state_store_internal_bootstrap_ohlcv(str(symbols[0] if symbols else ""), e_first)
            except Exception:
                pass

            candle_ts_run = int(e_first["timestamp"][ci0])

        last_ts = store.get_last_candle_ts(tf_entry)  # Track the last processed candle timestamp for this timeframe.
        _parity_probe_write(
            "SAME_CANDLE_GUARD_CHECK",
            {
                "symbol": str(symbols[0] if symbols else ""),
                "replay_ts_ms": int(replay_ts_ms or candle_ts_run),
                "entry_bar_ms": int(entry_bar_ms),
                "candle_ts_run": int(candle_ts_run),
                "last_candle_ts_before": int(last_ts or 0),
                "ts_ms_now_candidate": int(candle_ts_run),
                "guard_hit": bool(candle_ts_run == last_ts),
                "phase_seq": int(phase_seq),
            },
        )
        if candle_ts_run == last_ts:
            # silent skip: already processed candle
            pass
            return 0

        store.set_last_candle_ts(tf_entry, candle_ts_run)
        _parity_probe_write(
            "SAME_CANDLE_GUARD_SET",
            {
                "symbol": str(symbols[0] if symbols else ""),
                "replay_ts_ms": int(replay_ts_ms or candle_ts_run),
                "entry_bar_ms": int(entry_bar_ms),
                "set_value": int(candle_ts_run),
                "basis": "candle_ts_run",
                "candle_ts_run": int(candle_ts_run),
                "ts_ms_now_candidate": int(candle_ts_run),
                "phase_seq": int(phase_seq),
            },
        )
    except Exception as e:
        logger.warning(f"Failed to check same-candle guard: {e}")
        candle_ts_run = int(_now_epoch() * 1000)
    # Replay mode (CSV-driven): align execution path to backtest (avoid live-only gates)
    is_replay = replay_ts_ms is not None
    runtime_signal_log_enabled = _runtime_signal_log_enabled(mode=mode, is_replay=is_replay)

    # per symbol loop
    for symbol in symbols:
        phase_seq = 0
        # Use the same tag format for HOLD and OPEN logs so LIVE_DRYRUN stays readable.
        tag = f"{mode}{'_DRYRUN' if dryrun else ''}"
        ts_ms_now = int(candle_ts_run)
        # loop stop check (file-based)
        stop_loop = check_and_update_emergency_stop(store=store, base_dir=str(getattr(C, "STOP_FILE_DIR", ".")), phase="SYMBOL_LOOP")
        if stop_loop.should_stop and stop_loop.stop_mode == "STOP":
            logger.error("STOP detected in loop -> exit")
            return 0
        if stop_loop.should_stop and stop_loop.stop_mode == "STOP_NEW_ONLY":
            stop_new_only_active = True
        try:
            _observe_live_chart_guaranteed_price(
                symbol,
                ex=ex,
                timeframe=str(tf_entry),
                store=store,
                state_root=str(STATE_DIR),
                exchange_id=_resolve_exchange_id(),
                run_mode=str(mode),
                fallback_ts_ms=int(ts_ms_now),
            )
        except Exception:
            pass

        # --- always fetch entry TF for position management ---
        try:
            e_raw = ex.fetch_ohlcv(symbol, tf_entry, limit=200)
            e = ohlcv_to_dict(e_raw)
            entry_bar_ms = _timeframe_to_ms(tf_entry)
            # ReplayExchange already excludes the still-forming 5m candle from fetch_ohlcv().
            # Reuse replay_ts_ms here so replay does not drop one extra entry bar while the
            # 1h filter path still advances using the newer confirmed 1h bar.
            confirmed_now_ms = int(replay_ts_ms) if is_replay and replay_ts_ms is not None else int(candle_ts_run)
            ci_raw = _confirmed_candle_idx(e["timestamp"], entry_bar_ms, confirmed_now_ms)
            if ci_raw == -2:
                # Drop the still-forming last candle so downstream logic always sees confirmed bars.
                for _k in ("timestamp", "open", "high", "low", "close", "volume"):
                    if _k in e and len(e[_k]) > 0:
                        e[_k] = e[_k][:-1]
            # Replay parity: align with backtest "confirmed bar only" semantics.
            # In backtest, we never let the current (candle_ts_run) bar participate in logic/trace; we use the prior bar.
            if is_replay:
                try:
                    # Strict rule: NEVER include the bar whose open timestamp == candle_ts_run.
                    # Keep only rows with ts < candle_ts_run (i.e., the previous confirmed bar or older).
                    ts_arr = e.get("timestamp", [])
                    # Keep the confirmed bar itself (<= candle_ts_run) and exclude only future/unconfirmed bars.
                    cut = bisect_right(ts_arr, int(candle_ts_run))
                    cut = max(0, min(int(cut), len(ts_arr)))
                    for _k in ("timestamp", "open", "high", "low", "close", "volume"):
                        if _k in e:
                            e[_k] = e[_k][:cut]
                except Exception:
                    # Never crash LIVE/REPLAY due to trace alignment logic.
                    pass
            if not e_raw or not e.get("high"):
                if (not _chart_state_try_seed_builder(symbol, tf_entry)) and (not _chart_state_try_internal_bootstrap(symbol)) and (not _chart_state_use_runner_last_success_cache(symbol)):
                    cached_ohlcv = _chart_state_cached_ohlcv_from_exchange(ex, symbol, tf_entry)
                    if isinstance(cached_ohlcv, dict):
                        _chart_state_cache_ohlcv(symbol, cached_ohlcv, source="exchange_cache", reason="fetch_ohlcv_entry_empty")
                    else:
                        _chart_state_note_diag(symbol, source="empty", reason="fetch_ohlcv_entry_empty")
                _summ_reason(summ_hold, "fetch_ohlcv_entry_empty")
                continue
        except Exception as ex_e:
            if (not _chart_state_try_seed_builder(symbol, tf_entry)) and (not _chart_state_try_internal_bootstrap(symbol)) and (not _chart_state_use_runner_last_success_cache(symbol)):
                cached_ohlcv = _chart_state_cached_ohlcv_from_exchange(ex, symbol, tf_entry)
                if isinstance(cached_ohlcv, dict):
                    _chart_state_cache_ohlcv(symbol, cached_ohlcv, source="exchange_cache", reason="fetch_ohlcv_entry_failed")
                else:
                    _chart_state_note_diag(symbol, source="empty", reason="fetch_ohlcv_entry_failed")
            logger.warning("[OHLCV][ENTRY] symbol=%s tf=%s reason=%s", str(symbol), str(tf_entry), ex_e)
            _summ_reason(summ_hold, f"fetch_ohlcv_entry_failed:{ex_e}")
            _diff_trace_exception(
                symbol=symbol,
                ts_ms=int(ts_ms_now),
                phase="fetch_ohlcv_entry",
                exc=ex_e,
                extra={"mode": mode, "tf": tf_entry},
            )
            continue

        # Guard: ReplayExchange may return empty OHLCV on missing data / boundaries.
        if not e.get("timestamp") or not e.get("high") or not e.get("low"):
            if (not _chart_state_try_seed_builder(symbol, tf_entry)) and (not _chart_state_try_internal_bootstrap(symbol)) and (not _chart_state_use_runner_last_success_cache(symbol)):
                cached_ohlcv = _chart_state_cached_ohlcv_from_exchange(ex, symbol, tf_entry)
                if isinstance(cached_ohlcv, dict):
                    _chart_state_cache_ohlcv(symbol, cached_ohlcv, source="exchange_cache", reason="fetch_ohlcv_entry_empty")
                else:
                    _chart_state_note_diag(symbol, source="empty", reason="fetch_ohlcv_entry_empty")
            _summ_reason(summ_hold, "fetch_ohlcv_entry_empty")
            _diff_trace_write(
                {
                    "event": "FETCH_OHLCV_EMPTY",
                    "phase": "fetch_ohlcv_entry",
                    "mode": mode,
                    "symbol": symbol,
                    "tf": tf_entry,
                    "len": len(e_raw) if isinstance(e_raw, list) else None,
                }
            )
            continue
        _chart_state_cache_ohlcv(symbol, e, source="runner_live_ohlcv")

        # Confirmed 5m bar OHLC snapshot for trace parity checks (single source of truth).
        # _trace_bar_snapshot_at() already maps ref_ts_ms -> previous confirmed bar.
        # Passing expected(candle_ts_run) here would double-shift replay by one extra bar.
        trace_ref_ms = int(candle_ts_run) if is_replay else int(ts_ms_now)
        trace_bar_snap = _trace_bar_snapshot_at(e, ref_ts_ms=int(trace_ref_ms), is_replay=is_replay)
        if not isinstance(trace_bar_snap, dict):
            _summ_reason(summ_hold, "fetch_ohlcv_entry_empty")
            continue
        _trace_index_symbol_bars(str(symbol), e)
        bar_ts_ms = int(trace_bar_snap["bar_ts_ms"])
        bar_open = float(trace_bar_snap["bar_open"])
        bar_high = float(trace_bar_snap["bar_high"])
        bar_low = float(trace_bar_snap["bar_low"])
        bar_close = float(trace_bar_snap["bar_close"])
        trace_bar_fields = {
            "bar_ts_ms": int(bar_ts_ms),
            "bar_open": float(bar_open),
            "bar_high": float(bar_high),
            "bar_low": float(bar_low),
            "bar_close": float(bar_close),
        }
        _chart_state_append_internal_bootstrap_bar(
            symbol,
            ts_ms=int(bar_ts_ms),
            open_px=float(bar_open),
            high_px=float(bar_high),
            low_px=float(bar_low),
            close_px=float(bar_close),
        )
        _chart_state_seed_builder_observe(
            symbol,
            str(tf_entry),
            ts_ms=int(bar_ts_ms),
            price=float(bar_close),
            source="runtime_tap_confirmed_bar_close",
        )
        _chart_state_remember_runtime_tapped_price(
            symbol,
            price=float(bar_close),
            ts_ms=int(bar_ts_ms),
            source="runtime_tap_confirmed_bar_close",
        )

        # Expose bar snapshot to trace writer (used for verify_diff OHLC strict checks).
        _trace_update_bar_cache(str(symbol), e, ref_ts_ms=int(trace_ref_ms), is_replay=is_replay)
        if runtime_signal_log_enabled:
            _log_latest_confirmed_bar(
                tag=str(tag),
                symbol=str(symbol),
                tf_entry=str(tf_entry),
                bar_ts_ms=int(bar_ts_ms),
                bar_open=float(bar_open),
                bar_high=float(bar_high),
                bar_low=float(bar_low),
                bar_close=float(bar_close),
            )
        close_last = float(e["close"][-1])
        if e.get("timestamp"):
            try:
                close_last_ts_ms = int(e["timestamp"][-1])
                _chart_state_seed_builder_observe(
                    symbol,
                    str(tf_entry),
                    ts_ms=int(close_last_ts_ms),
                    price=float(close_last),
                    source="runtime_tap_entry_close_last",
                )
                _chart_state_remember_runtime_tapped_price(
                    symbol,
                    price=float(close_last),
                    ts_ms=int(close_last_ts_ms),
                    source="runtime_tap_entry_close_last",
                )
            except Exception:
                pass

        candle_ts_entry = int(bar_ts_ms)
        # diff-trace OPEN/CLOSE timestamps must follow backtest confirmed-bar-only key basis.
        # In REPLAY, stamp ts_ms/ref_ts_ms with expected previous 5m bar timestamp.
        # verify_diff key parity: backtest uses trade bar timestamp (not previous expected bar) for OPEN/CLOSE ts_ms.
        open_expected_ts = int(ts_ms_now)
        close_expected_ts = int(ts_ms_now)
        trace_event_ts_open_ms = int(open_expected_ts)
        trace_event_ts_manage_ms = int(close_expected_ts)
        open_ref_ts_ms = _trace_ref_ts_ms({"exec_ts": int(trace_event_ts_open_ms)})
        close_ref_ts_ms = _trace_ref_ts_ms({"exec_ts": int(trace_event_ts_manage_ms)})
        open_bar_snap = _trace_bar_snapshot_at(e, ref_ts_ms=int(open_ref_ts_ms), is_replay=is_replay)
        close_bar_snap = _trace_bar_snapshot_at(e, ref_ts_ms=int(close_ref_ts_ms), is_replay=is_replay)
        if open_bar_snap is None or close_bar_snap is None:
            _summ_reason(summ_hold, "trace_bar_snapshot_empty")
            continue
        try:
            open_expected_ts = _trace_expected_bar_ts_ms(int(open_ref_ts_ms))
            open_used_ts = int(open_bar_snap.get("bar_ts_ms") or 0)
            if int(open_used_ts) != int(open_expected_ts):
                logger.warning(
                    f"[{tag}] trace_bar_rule_mismatch kind=OPEN symbol={symbol} event_ts_ms={int(open_ref_ts_ms)} used_bar_ts_ms={int(open_used_ts)} expected_bar_ts_ms={int(open_expected_ts)}"
                )
            close_expected_ts = _trace_expected_bar_ts_ms(int(close_ref_ts_ms))
            close_used_ts = int(close_bar_snap.get("bar_ts_ms") or 0)
            if int(close_used_ts) != int(close_expected_ts):
                logger.warning(
                    f"[{tag}] trace_bar_rule_mismatch kind=CLOSE symbol={symbol} event_ts_ms={int(close_ref_ts_ms)} used_bar_ts_ms={int(close_used_ts)} expected_bar_ts_ms={int(close_expected_ts)}"
                )
        except Exception:
            # Never crash LIVE/REPLAY due to trace mismatch diagnostics.
            pass
        # For CLOSE events fired during position-management (before signal calc),
        # keep a stable placeholder. SIGNAL phase will overwrite this.
        signal_path = "pre_signal"

        # --- manage existing position first (EXIT/BE/TRAIL) ---
        pos = store.get_position(symbol)
        closed_this_bar = False
        closed_exit_reason = ""
        if pos is not None:
            # Excursion belongs only to the active trade; never extend it while flat.
            excursion_state = _sync_trade_excursion_state(
                store,
                symbol,
                pos,
                bar_high=float(bar_high),
                bar_low=float(bar_low),
            )
            max_fav = float(excursion_state.get("max_fav", bar_high) or bar_high)
            min_adv = float(excursion_state.get("min_adv", bar_low) or bar_low)
            try:
                pos_qty = float(pos["qty"])
                pos_stop = float(pos["stop"])
                pos_tp = float(pos["take_profit"])
                pos_entry = float(pos["entry"])

                # regime for BE/TRAIL uses filter TF (cheap 1h fetch only when needed)
                try:
                    f_raw = ex.fetch_ohlcv(symbol, tf_filter, limit=200)
                    f = ohlcv_to_dict(f_raw)
                    adx_period = int(getattr(C, "ADX_PERIOD", 14))
                    ema_fast = int(getattr(C, "EMA_FAST", 20))
                    ema_slow = int(getattr(C, "EMA_SLOW", 50))
                    adx_arr = ind_adx(f["high"], f["low"], f["close"], period=adx_period)
                    ema_fast_arr = ind_ema(f["close"], ema_fast)
                    ema_slow_arr = ind_ema(f["close"], ema_slow)
                    regime, direction = detect_regime_1h(f["close"], adx_arr, ema_fast_arr, ema_slow_arr)
                    # If TREND trading is disabled but RANGE trading is enabled, we must not let
                    # the 1h regime classifier force "trend" because the downstream signal path
                    # would become 'trend_disabled' (no OPEN), making verify_diff unmatched.
                    # In that configuration, treat any "trend" classification as "range" for entry/exit decisions.
                    try:
                        _trade_trend = float(getattr(C, "TRADE_TREND", 0.0) or 0.0)
                    except Exception:
                        _trade_trend = 0.0
                    _trade_range = bool(getattr(C, "TRADE_RANGE", True))
                    if _trade_trend <= 0.0 and _trade_range and str(regime).lower() == "trend":
                        regime = "range"
                    # === Trend direction-none guard (quality filter) ===
                    if str(regime).lower() == "trend" and str(direction).lower() == "none" and bool(getattr(C, "TREND_BLOCK_DIR_NONE", False)):
                        b_reason = "trend_dir_none"
                        _summ_reason(summ_hold, b_reason)
                        if bool(getattr(C, "LOG_HOLD_LINES", False)) and _runtime_log_enabled("OPS", mode_name=mode):
                            logger.info(f"[{tag}] {symbol} action=hold reason={b_reason}")
                        continue
                except Exception:
                    regime, direction = "trend", "up"

                stop_updated = False

                # ------------------------------------------------------------
                # TP1 (partial take profit) -> remaining uses TRAIL
                #   - range only (spot long)
                #   - trigger by R-multiple vs initial risk (entry - init_stop)
                #   - once TP1 is done, disable TP (set far) and let trail manage exits
                # ------------------------------------------------------------
                try:
                    if str(regime).lower() == "range":
                        tp1_enabled, tp1_r, tp1_pct = _tp1_range_effective_params(require_flag=True)
                        if tp1_enabled and (not _get_pos_tp1_done(store, symbol)):
                            init_stop = _get_pos_init_stop(store, symbol, fallback=pos_stop)
                            init_risk = float(pos_entry) - float(init_stop)
                            if init_risk > 0:
                                tp1_price = float(pos_entry) + init_risk * float(tp1_r)
                                if float(bar_high) >= float(tp1_price):
                                    init_qty = _get_pos_init_qty(store, symbol, fallback=pos_qty)
                                    qty_to_close = float(init_qty) * float(tp1_pct)
                                    qty_to_close = max(0.0, min(float(pos_qty), float(qty_to_close)))
                                    # safety: if too small, skip
                                    if qty_to_close > 0:
                                        entry_type = str(getattr(C, "PAPER_ENTRY_FEE_TYPE", "taker"))
                                        tp_type = str(getattr(
                                            C, "PAPER_TP_FEE_TYPE",
                                            getattr(C, "PAPER_EXIT_FEE_TYPE", getattr(C, "PAPER_STOP_FEE_TYPE", "taker"))
                                        ))
                                        fee_rate_entry = _fee_rate_by_type(entry_type)
                                        fee_rate_exit = _fee_rate_by_type(tp_type)

                                        ex_res = _exit_tp_live(ex, symbol, qty_to_close, dryrun=dryrun, store=store)
                                        filled = float(ex_res.get("filled_qty") or 0.0)
                                        exit_px = float(ex_res.get("avg_price") or float(e["close"][-1]))

                                        if dryrun and (ex_res.get("avg_price") is None):
                                            exit_px = _trace_exec_with_slip(dryrun=True, kind="exit_long", px=float(e["close"][-1]))

                                        if filled > 0:
                                            remaining = max(0.0, float(pos_qty) - float(filled))
                                            _set_pos_tp1_done(store, symbol, True)

                                            # disable TP for the remaining position (leave to TRAIL/STOP)
                                            try:
                                                tp_disable_mult = float(getattr(C, "RANGE_TP_AFTER_TP1_MULT", 100.0) or 100.0)
                                                tp_far = float(pos_entry) * float(tp_disable_mult)
                                            except Exception:
                                                tp_far = float(pos_entry) * 100.0

                                            if remaining > 0:
                                                store.update_position(symbol, qty=float(remaining), take_profit=float(tp_far))
                                                pos_qty = float(remaining)
                                                pos_tp = float(tp_far)
                                                logger.info(
                                                    f"[TP1] {symbol} PARTIAL CLOSE TP1_PROFIT "
                                                    f"filled={filled:.8f}/{qty_to_close:.8f} "
                                                    f"tp1_r={tp1_r:.2f} tp1_pct={tp1_pct:.2f} "
                                                    f"exit_px={exit_px} -> remain={remaining:.8f} "
                                                    f"(TP disabled -> TRAIL) ({ex_res.get('reason')})"
                                                )
                                                _summ_inc(summ_evt, "TP1_PARTIAL")
                                                _diff_trace_write(
                                                    {
                                                        "event": "TP1_PARTIAL",
                                                        "ts_ms": int(trace_event_ts_manage_ms),
                                                        "candle_ts_run": int(candle_ts_run),
                                                        "ts_ms_now": int(ts_ms_now),
                                                        "candle_ts_entry": int(candle_ts_entry),
                                                        "signal_path": str(signal_path),
                                                        "symbol": str(symbol),
                                                        "regime": str(regime),
                                                        "entry_exec": float(pos_entry),
                                                        "exit_exec": float(exit_px),
                                                        "qty_closed": float(filled),
                                                        "qty_remain": float(remaining),
                                                        "tp1_r": float(tp1_r),
                                                        "tp1_pct": float(tp1_pct),
                                                        "reason": "TP1_PROFIT",
                                                        **trace_bar_fields,
                                                    }
                                                )
                                            else:
                                                # fully closed by TP1 (rare)
                                                res = store.close_position(
                                                    symbol,
                                                    exit_price=float(exit_px),
                                                    candle_ts_exit=int(candle_ts_run),
                                                    reason="TP1_PROFIT_FULL",
                                                    fee_rate_entry=float(fee_rate_entry),
                                                    fee_rate_exit=float(fee_rate_exit),
                                                )
                                                closed_this_bar = True
                                                closed_exit_reason = "TP1_PROFIT_FULL"
                                                _chart_state_note_marker(
                                                    symbol,
                                                    kind="exit",
                                                    ts_ms=int(trace_event_ts_manage_ms),
                                                    price=float(exit_px),
                                                    side="long",
                                                    label="TP1_PROFIT_FULL",
                                                )
                                                res = _ensure_pnl_fee(
                                                    res=res,
                                                    pos=pos,
                                                    exit_exec=float(exit_px),
                                                    fee_rate_entry=float(fee_rate_entry),
                                                    fee_rate_exit=float(fee_rate_exit),
                                                    default_qty=float(pos_qty),
                                                )
                                                net = float(res.get("pnl") or 0.0) - float(res.get("fee") or 0.0)
                                                _apply_compounded_close_net(net)
                                                _summ_inc(summ_evt, "CLOSE")
                                                _log_trade_event(
                                                    f"[{tag}] {symbol} CLOSE TP1_PROFIT_FULL exit_px={exit_px} qty={pos_qty:.6f} net={net:.6f} {_quote_ccy_for_symbol(symbol)} ({ex_res.get('reason')})"
                                                )
                                                _refresh_live_equity_after_close()
                                                _diff_trace_write(
                                                    {
                                                        "event": "CLOSE",
                                                        "ts_ms": int(trace_event_ts_manage_ms),
                                                        "candle_ts_run": int(candle_ts_run),
                                                        "ts_ms_now": int(ts_ms_now),
                                                        "candle_ts_entry": int(candle_ts_entry),
                                                        "candle_ts_exit": int(candle_ts_run),
                                                        "signal_path": str(signal_path),
                                                        "symbol": str(symbol),
                                                        "tf_entry": str(tf_entry),
                                                        "tf_filter": str(tf_filter),
                                                        "mode": str(tag),
                                                        "cfg": _cfg_snapshot(),
                                                        "regime": str(regime),
                                                        "exit_reason": "TP1_PROFIT_FULL",
                                                        "entry_exec": float(res.get("entry_exec") or pos_entry),
                                                        "exit_exec": float(exit_px),
                                                        "qty": float(res.get("qty") or pos_qty),
                                                        "pnl": float(res.get("pnl") or 0.0),
                                                        "fee": float(res.get("fee") or 0.0),
                                                        "net": float(net),
                                                        **trace_bar_fields,
                                                    }
                                                )
                                                _record_trade_row(
                                                    store=store,
                                                    res=res,
                                                    exit_reason="TP1_PROFIT_FULL",
                                                    net=net,
                                                    stop=float(pos_stop),
                                                    tp=float(pos_tp),
                                                    stop_kind=str(pos.get("stop_kind") or "init") if isinstance(pos, dict) else "init",
                                                )
                                                if is_replay and closed_this_bar and (store.get_position(symbol) is None):
                                                    raise _ReplayPositionClosedSameBar()
                                                continue
                except Exception as _e_tp1:
                    logger.warning(f"[{tag}] TP1 check failed: {_e_tp1}")


                # Keep the initial stop for BE and TRAIL guard calculations.
                init_stop = _get_pos_init_stop(store, symbol, fallback=pos_stop)
                init_risk = pos_entry - float(init_stop)
                init_risk_bps = (init_risk / pos_entry * 10000.0) if pos_entry > 0 else 0.0


                # BE
                if _be_effective_enabled(str(regime)) and pos_stop < pos_entry:
                    min_risk_bps = float(getattr(C, "BE_MIN_INIT_RISK_BPS", 0.0))
                    if init_risk_bps >= min_risk_bps and init_risk > 0:
                        tr, be_static_off = _be_params(str(regime))
                        trigger_price = pos_entry + init_risk * tr
                        if max_fav >= trigger_price:
                            # Read the current spread for dynamic BE offset logic.
                            sp_now = _spread_bps_from_orderbook(ex, symbol)
                            atr_bps_now = 0.0
                            try:
                                atr_p = int(getattr(C, "ATR_PERIOD", 14))
                                a_now = ind_atr(e["high"], e["low"], e["close"], period=atr_p)
                                a_last = float(a_now[-1]) if hasattr(a_now, "__len__") else float(a_now)
                                if a_last > 0.0 and float(pos_entry) > 0.0:
                                    atr_bps_now = (float(a_last) / float(pos_entry)) * 10000.0
                            except Exception:
                                atr_bps_now = 0.0
                            off_bps = _calc_be_offset_bps(sp_now, atr_bps_now, static_off_bps=be_static_off)

                            new_stop = pos_entry * (1.0 + off_bps / 10000.0)
                            if new_stop > pos_stop:
                                store.update_position(symbol, stop=float(new_stop), stop_kind="be")
                                pos_stop = float(new_stop)
                                if isinstance(pos, dict):
                                    pos["stop_kind"] = "be"
                                stop_updated = True
                                _update_pos_meta(
                                    store,
                                    symbol,
                                    {
                                        "be_triggered": True,
                                        "be_trigger_r": float(tr),
                                        "be_offset_bps": float(off_bps),
                                        "be_stop_set": float(new_stop),
                                    },
                                )
                                _log_trade_event(
                                    f"[BE] {symbol} stop -> {pos_stop:.6f} "
                                    f"regime={regime} init_risk_bps={init_risk_bps:.2f} off_bps={off_bps:.2f}"
                                )

                                _diff_trace_write(
                                    {
                                        "event": "BE_SET",
                                        "ts_ms": int(trace_event_ts_manage_ms),
                                        "symbol": str(symbol),
                                        "regime": str(regime),
                                        "entry_exec": float(pos_entry),
                                        "stop_new": float(pos_stop),
                                        "trigger_price": float(trigger_price),
                                        "init_risk_bps": float(init_risk_bps),
                                        "spread_bps": float(sp_now) if sp_now is not None else None,
                                        "be_offset_bps": float(off_bps),
                                        **trace_bar_fields,
                                    }
                                )

                # TRAIL
                trail_meta = _get_pos_meta(store, symbol)
                if not isinstance(trail_meta, dict):
                    trail_meta = {}
                for _k, _v in _trail_diag_defaults(str(regime), init_stop=float(init_stop), entry_exec=float(pos_entry)).items():
                    trail_meta.setdefault(_k, _v)
                trail_meta_dirty = False
                trail_block_reason = ""
                trail_enabled = bool(getattr(C, "TRAIL_ENABLED", True))
                trail_use_bps_fallback = bool(getattr(C, "TRAIL_USE_BPS_FALLBACK", True))
                if not trail_enabled:
                    trail_block_reason = "trail_disabled"
                else:
                    min_risk_bps = float(getattr(C, "TRAIL_MIN_INIT_RISK_BPS", 0.0))
                    if init_risk_bps >= min_risk_bps:
                        start_r, atr_mult, bps_from_high = _trail_params(str(regime))
                        # Backtest keeps trail arming/ratchet on the original trade risk even
                        # after the stop has moved above entry. Replay must mirror that basis.
                        risk_per_unit = float(init_risk) if is_replay else (pos_entry - pos_stop)
                        if risk_per_unit > 0:
                            start_price = pos_entry + risk_per_unit * start_r
                            if trail_meta.get("trail_start_r") != float(start_r):
                                trail_meta["trail_start_r"] = float(start_r)
                                trail_meta_dirty = True
                            if trail_meta.get("trail_bps_from_high") != float(bps_from_high):
                                trail_meta["trail_bps_from_high"] = float(bps_from_high)
                                trail_meta_dirty = True
                            if trail_meta.get("start_price") != float(start_price):
                                trail_meta["start_price"] = float(start_price)
                                trail_meta_dirty = True

                            current_stop_before = float(pos_stop)
                            new_stop = None
                            cand_delta = None
                            atr_candidate_stop = None
                            bps_candidate_stop = None
                            trail_mode = "none"
                            atr_missing = False
                            eligible_count = int(trail_meta.get("trail_eligible_count", 0) or 0)
                            trail_atr_trace_fields = (
                                {
                                    "trail_atr_source": "",
                                    "trail_atr_value": None,
                                    "trail_atr_lookup_ts": None,
                                    "trail_atr_pre_index_used": None,
                                    "trail_atr_pre_ts_used": None,
                                    "trail_atr_pre_map_present": False,
                                    "trail_atr_pre_sym_present": False,
                                    "trail_atr_pre_ts_len": 0,
                                    "trail_atr_pre_atr_len": 0,
                                    "trail_atr_pre_index_candidate": None,
                                    "trail_atr_pre_value_raw": None,
                                    "trail_atr_pre_sym_type": "",
                                    "trail_atr_pre_sym_keys_preview": [],
                                    "trail_atr_pre_ts_type": "",
                                    "trail_atr_pre_atr_type": "",
                                    "trail_atr_exception_text": "",
                                    "trail_atr_fallback_reason": "",
                                }
                                if is_replay
                                else {}
                            )

                            if max_fav >= start_price:
                                eligible_count += 1
                                trail_meta["trail_eligible_count"] = int(eligible_count)
                                trail_meta_dirty = True

                                if bool(getattr(C, "TRAIL_USE_ATR", True)):
                                    atr_p = int(getattr(C, "TRAIL_ATR_PERIOD", 14))
                                    a_last = None
                                    trail_atr_source = ""
                                    trail_atr_value = None
                                    trail_atr_lookup_ts = None
                                    trail_atr_pre_index_used = None
                                    trail_atr_pre_ts_used = None
                                    trail_atr_pre_map_present = False
                                    trail_atr_pre_sym_present = False
                                    trail_atr_pre_ts_len = 0
                                    trail_atr_pre_atr_len = 0
                                    trail_atr_pre_index_candidate = None
                                    trail_atr_pre_value_raw = None
                                    trail_atr_pre_sym_type = ""
                                    trail_atr_pre_sym_keys_preview = []
                                    trail_atr_pre_ts_type = ""
                                    trail_atr_pre_atr_type = ""
                                    trail_atr_exception_text = ""
                                    trail_atr_fallback_reason = ""
                                    if is_replay and atr_p == 14:
                                        try:
                                            pre_map = getattr(ex, "_replay_entry_precomputed", {}) or {}
                                            trail_atr_pre_map_present = bool(pre_map)
                                            pre_sym = (pre_map.get(str(symbol), {}) or {}).get(str(tf_entry))
                                            trail_atr_pre_sym_present = bool(pre_sym)
                                            trail_atr_pre_sym_type = type(pre_sym).__name__ if pre_sym is not None else ""
                                            if isinstance(pre_sym, dict):
                                                trail_atr_pre_sym_keys_preview = [str(k) for k in list(pre_sym.keys())[:5]]
                                            elif pre_sym is not None:
                                                trail_atr_pre_ts_type = "n/a_non_dict"
                                                trail_atr_pre_atr_type = "n/a_non_dict"
                                            manage_bar_ts = int(bar_ts_ms or 0)
                                            if manage_bar_ts <= 0:
                                                manage_bar_ts = int(trace_event_ts_manage_ms or 0)
                                            trail_atr_lookup_ts = int(manage_bar_ts) if manage_bar_ts > 0 else None
                                            if pre_sym and manage_bar_ts > 0:
                                                raw_pre_ts = pre_sym.get("timestamp", [])
                                                raw_pre_atr = pre_sym.get("atr14", [])
                                                trail_atr_pre_ts_type = type(raw_pre_ts).__name__
                                                trail_atr_pre_atr_type = type(raw_pre_atr).__name__
                                                pre_ts = list(raw_pre_ts) if raw_pre_ts is not None else []
                                                pre_atr = list(raw_pre_atr) if raw_pre_atr is not None else []
                                                trail_atr_pre_ts_len = int(len(pre_ts))
                                                trail_atr_pre_atr_len = int(len(pre_atr))
                                                if (not pre_ts) or (not pre_atr):
                                                    trail_atr_fallback_reason = "empty_pre_series"
                                                i_pre = bisect_right(pre_ts, int(manage_bar_ts)) - 1
                                                trail_atr_pre_index_candidate = int(i_pre)
                                                if i_pre >= 0 and i_pre < len(pre_atr):
                                                    trail_atr_pre_value_raw = pre_atr[i_pre]
                                                    try:
                                                        a_last = float(pre_atr[i_pre])
                                                    except Exception:
                                                        a_last = None
                                                    if a_last is not None and a_last > 0:
                                                        trail_atr_source = "precomputed_atr14"
                                                        trail_atr_value = float(a_last)
                                                        trail_atr_pre_index_used = int(i_pre)
                                                        try:
                                                            trail_atr_pre_ts_used = int(pre_ts[i_pre]) if i_pre < len(pre_ts) else None
                                                        except Exception:
                                                            trail_atr_pre_ts_used = None
                                                        trail_atr_fallback_reason = "used_precomputed"
                                                    else:
                                                        trail_atr_fallback_reason = "pre_atr_invalid"
                                                elif trail_atr_fallback_reason == "":
                                                    trail_atr_fallback_reason = "index_out_of_range"
                                            elif not trail_atr_pre_map_present:
                                                trail_atr_fallback_reason = "no_pre_map"
                                            elif not trail_atr_pre_sym_present:
                                                trail_atr_fallback_reason = "no_pre_sym"
                                        except Exception as _trail_atr_exc:
                                            trail_atr_fallback_reason = "exception"
                                            trail_atr_exception_text = f"{type(_trail_atr_exc).__name__}:{str(_trail_atr_exc)}"[:160]
                                            a_last = None
                                    if a_last is None or a_last <= 0:
                                        a = ind_atr(e["high"], e["low"], e["close"], period=atr_p)
                                        try:
                                            a_last = float(a[-1]) if hasattr(a, "__len__") else float(a)
                                        except Exception:
                                            a_last = None
                                        trail_atr_source = "fallback_ind_atr"
                                        trail_atr_value = float(a_last) if a_last is not None else None
                                    trail_atr_trace_fields = (
                                        {
                                            "trail_atr_source": str(trail_atr_source or ""),
                                            "trail_atr_value": trail_atr_value,
                                            "trail_atr_lookup_ts": trail_atr_lookup_ts,
                                            "trail_atr_pre_index_used": trail_atr_pre_index_used,
                                            "trail_atr_pre_ts_used": trail_atr_pre_ts_used,
                                            "trail_atr_pre_map_present": bool(trail_atr_pre_map_present),
                                            "trail_atr_pre_sym_present": bool(trail_atr_pre_sym_present),
                                            "trail_atr_pre_ts_len": int(trail_atr_pre_ts_len),
                                            "trail_atr_pre_atr_len": int(trail_atr_pre_atr_len),
                                            "trail_atr_pre_index_candidate": trail_atr_pre_index_candidate,
                                            "trail_atr_pre_value_raw": trail_atr_pre_value_raw,
                                            "trail_atr_pre_sym_type": str(trail_atr_pre_sym_type or ""),
                                            "trail_atr_pre_sym_keys_preview": list(trail_atr_pre_sym_keys_preview or []),
                                            "trail_atr_pre_ts_type": str(trail_atr_pre_ts_type or ""),
                                            "trail_atr_pre_atr_type": str(trail_atr_pre_atr_type or ""),
                                            "trail_atr_exception_text": str(trail_atr_exception_text or ""),
                                            "trail_atr_fallback_reason": str(trail_atr_fallback_reason or ""),
                                        }
                                        if is_replay
                                        else {}
                                    )
                                    if a_last is not None and a_last > 0:
                                        atr_candidate_stop = bar_high - a_last * atr_mult
                                    else:
                                        atr_missing = True
                                if trail_use_bps_fallback:
                                    bps_candidate_stop = bar_high * (1.0 - bps_from_high / 10000.0)

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
                                    trail_block_reason = "atr_unavailable" if (atr_missing and (not trail_use_bps_fallback)) else "candidate_invalid"
                                elif float(new_stop) <= float(current_stop_before):
                                    trail_block_reason = "candidate_le_current_stop"
                            else:
                                trail_block_reason = "not_reached_start_price"

                            cand_delta = _trail_diag_update(
                                trail_meta,
                                block_reason=trail_block_reason,
                                candidate_stop=new_stop,
                                candidate_from_atr=atr_candidate_stop,
                                candidate_from_bps=bps_candidate_stop,
                                pos_stop_before=current_stop_before,
                                start_price=start_price,
                                bar_high=bar_high,
                                risk_per_unit=risk_per_unit,
                                mode=trail_mode,
                            )
                            trail_meta_dirty = True

                            if (not bool(trail_meta.get("_trail_check_emitted", False))) and new_stop is not None:
                                _diff_trace_write(
                                    {
                                        "event": "TRAIL_CHECK",
                                        "ts_ms": int(trace_event_ts_manage_ms),
                                        "symbol": str(symbol),
                                        "regime": str(regime),
                                        "entry_exec": float(pos_entry),
                                        "stop_current": float(current_stop_before),
                                        "bar_high": float(bar_high),
                                        "max_fav": float(max_fav),
                                        "init_risk_bps": float(init_risk_bps),
                                        "trail_start_r": float(start_r),
                                        "trail_atr_mult": float(atr_mult),
                                        "trail_bps_from_high": float(bps_from_high),
                                        "start_price": float(start_price),
                                        "trail_candidate_stop": float(new_stop),
                                        "trail_candidate_minus_current_stop": float(cand_delta) if cand_delta is not None else None,
                                        "trail_candidate_from_atr": trail_meta.get("trail_candidate_from_atr_last"),
                                        "trail_candidate_from_bps": trail_meta.get("trail_candidate_from_bps_last"),
                                        "trail_eligible_count": int(eligible_count),
                                        "trail_eval_count": int(trail_meta.get("trail_eval_count", 0) or 0),
                                        "trail_update_count": int(trail_meta.get("trail_update_count", 0) or 0),
                                        "trail_mode": str(trail_meta.get("trail_mode_last") or "none"),
                                        "trail_bar_high": trail_meta.get("trail_bar_high_last"),
                                        "trail_pos_stop_before": trail_meta.get("trail_pos_stop_before_last"),
                                        "trail_risk_per_unit": trail_meta.get("trail_risk_per_unit_last"),
                                        **trail_atr_trace_fields,
                                        **trace_bar_fields,
                                    }
                                )
                                trail_meta["_trail_check_emitted"] = True
                                trail_meta_dirty = True

                            if new_stop is not None and float(new_stop) > float(current_stop_before):
                                store.update_position(symbol, stop=float(new_stop), stop_kind="trail")
                                pos_stop = float(new_stop)
                                if isinstance(pos, dict):
                                    pos["stop_kind"] = "trail"
                                stop_updated = True
                                trail_meta["trail_triggered"] = True
                                trail_meta["trail_update_count"] = int(trail_meta.get("trail_update_count", 0) or 0) + 1
                                trail_meta["_trail_trace_reason_last"] = ""
                                trail_meta_dirty = True
                                logger.info(
                                    f"[TRAIL] {symbol} stop -> {pos_stop:.6f} "
                                    f"regime={regime} init_risk_bps={init_risk_bps:.2f}"
                                )
                                _diff_trace_write(
                                    {
                                        "event": "TRAIL_SET",
                                        "ts_ms": int(trace_event_ts_manage_ms),
                                        "symbol": str(symbol),
                                        "regime": str(regime),
                                        "entry_exec": float(pos_entry),
                                        "stop_new": float(pos_stop),
                                        "stop_prev": float(current_stop_before),
                                        "bar_high": float(bar_high),
                                        "max_fav": float(max_fav),
                                        "init_risk_bps": float(init_risk_bps),
                                        "trail_start_r": float(start_r),
                                        "trail_atr_mult": float(atr_mult),
                                        "trail_bps_from_high": float(bps_from_high),
                                        "start_price": float(start_price),
                                        "trail_candidate_stop": float(new_stop),
                                        "trail_candidate_minus_current_stop": float(cand_delta) if cand_delta is not None else None,
                                        "trail_candidate_from_atr": trail_meta.get("trail_candidate_from_atr_last"),
                                        "trail_candidate_from_bps": trail_meta.get("trail_candidate_from_bps_last"),
                                        "trail_eligible_count": int(trail_meta.get("trail_eligible_count", 0) or 0),
                                        "trail_eval_count": int(trail_meta.get("trail_eval_count", 0) or 0),
                                        "trail_update_count": int(trail_meta.get("trail_update_count", 0) or 0),
                                        "trail_mode": str(trail_meta.get("trail_mode_last") or "none"),
                                        "trail_bar_high": trail_meta.get("trail_bar_high_last"),
                                        "trail_pos_stop_before": trail_meta.get("trail_pos_stop_before_last"),
                                        "trail_risk_per_unit": trail_meta.get("trail_risk_per_unit_last"),
                                        **trail_atr_trace_fields,
                                        **trace_bar_fields,
                                    }
                                )

                if trail_block_reason:
                    if int(trail_meta.get("trail_eval_count", 0) or 0) <= 0:
                        trail_meta["trail_block_reason_last"] = str(trail_block_reason)
                        if not str(trail_meta.get("trail_block_reason_max") or ""):
                            trail_meta["trail_block_reason_max"] = str(trail_block_reason)
                        trail_meta["trail_mode_last"] = "none"
                        trail_meta_dirty = True
                    prev_trace_reason = str(trail_meta.get("_trail_trace_reason_last") or "")
                    if prev_trace_reason != str(trail_block_reason):
                        _diff_trace_write(
                            {
                                "event": "TRAIL_BLOCKED",
                                "ts_ms": int(trace_event_ts_manage_ms),
                                "symbol": str(symbol),
                                "regime": str(regime),
                                "entry_exec": float(pos_entry),
                                "stop_current": float(pos_stop),
                                "bar_high": float(bar_high),
                                "max_fav": float(max_fav),
                                "init_risk_bps": float(init_risk_bps),
                                "trail_start_r": float(trail_meta.get("trail_start_r", 0.0) or 0.0),
                                "trail_bps_from_high": float(trail_meta.get("trail_bps_from_high", 0.0) or 0.0),
                                "start_price": trail_meta.get("start_price"),
                                "trail_candidate_stop": trail_meta.get("trail_candidate_stop_last"),
                                "trail_candidate_minus_current_stop": trail_meta.get("trail_candidate_minus_current_stop"),
                                "trail_candidate_from_atr": trail_meta.get("trail_candidate_from_atr_last"),
                                "trail_candidate_from_bps": trail_meta.get("trail_candidate_from_bps_last"),
                                "trail_eligible_count": int(trail_meta.get("trail_eligible_count", 0) or 0),
                                "trail_eval_count": int(trail_meta.get("trail_eval_count", 0) or 0),
                                "trail_update_count": int(trail_meta.get("trail_update_count", 0) or 0),
                                "trail_block_reason": str(trail_block_reason),
                                "trail_mode": str(trail_meta.get("trail_mode_last") or "none"),
                                "trail_bar_high": trail_meta.get("trail_bar_high_last"),
                                "trail_pos_stop_before": trail_meta.get("trail_pos_stop_before_last"),
                                "trail_risk_per_unit": trail_meta.get("trail_risk_per_unit_last"),
                                **trail_atr_trace_fields,
                                **trace_bar_fields,
                            }
                        )
                        trail_meta["_trail_trace_reason_last"] = str(trail_block_reason)
                        trail_meta_dirty = True

                if trail_meta_dirty:
                    _set_pos_meta(store, symbol, trail_meta)
                # --- RANGE TIMEOUT EXIT (NEW) ---
                # On fast range trades, time out positions that stop extending instead of waiting indefinitely.
                if str(regime).lower() == "range":
                    timeout_bars = int(getattr(C, "RANGE_TIMEOUT_BARS", 0))
                    if timeout_bars > 0:
                        ts_open = int(pos.get("candle_ts_open") or 0)
                        ts_now = int(e["timestamp"][-1])

                        # Normalize to milliseconds (some paths may store seconds)
                        # Heuristic: unix seconds is ~1e9..1e10, ms is ~1e12..1e13
                        if 0 < ts_open < 10**12:
                            ts_open *= 1000
                        if 0 < ts_now < 10**12:
                            ts_now *= 1000
                        def _tf_to_sec(tf: str) -> int:
                            t = str(tf).strip().lower()
                            try:
                                if t.endswith("m"):
                                    return int(t[:-1]) * 60
                                if t.endswith("h"):
                                    return int(t[:-1]) * 3600
                                if t.endswith("d"):
                                    return int(t[:-1]) * 86400
                            except Exception:
                                pass
                            return 60

                        tf_sec = _tf_to_sec(tf_entry)
                        age_bars = int(max(0, (ts_now - ts_open)) // 1000 // max(1, tf_sec))

                        if age_bars >= timeout_bars:
                            # Priority: TP/SL (incl. BE/TRAIL stop hits) > TIMEOUT
                            pos_dir_now = str(pos.get("dir") or pos.get("side") or pos.get("direction") or "long").lower()
                            hit_tp_now = (pos_dir_now == "long" and float(e["high"][-1]) >= float(pos_tp)) or (
                                pos_dir_now == "short" and float(e["low"][-1]) <= float(pos_tp)
                            )
                            hit_sl_now = (pos_dir_now == "long" and float(e["low"][-1]) <= float(pos_stop)) or (
                                pos_dir_now == "short" and float(e["high"][-1]) >= float(pos_stop)
                            )
                            if bool(hit_tp_now or hit_sl_now):
                                pass  # let TP/SL resolve on this bar
                            else:
                                pnl_bps = (float(e["close"][-1]) - pos_entry) / pos_entry * 10000.0
                                min_keep = float(getattr(C, "RANGE_TIMEOUT_MIN_PROFIT_BPS", -5.0))
                                if pnl_bps >= min_keep:
                                    exit_reason = f"RANGE_TIMEOUT({age_bars}bars pnl={pnl_bps:.1f}bps)"
                                    entry_type = str(getattr(C, "PAPER_ENTRY_FEE_TYPE", "taker"))
                                    stop_type = str(getattr(C, "PAPER_STOP_FEE_TYPE", "taker"))
                                    timeout_type = str(getattr(C, "PAPER_TIMEOUT_FEE_TYPE", stop_type))
                                    exit_type = timeout_type  # Use the TIMEOUT fee type, falling back to the STOP fee type by default.

                                    fee_rate_entry = _fee_rate_by_type(entry_type)
                                    fee_rate_exit = _fee_rate_by_type(exit_type)

                                    ex_res = _exit_stop_live(ex, symbol, pos_qty, dryrun=dryrun, store=store)
                                    filled = float(ex_res.get("filled_qty") or 0.0)
                                    exit_px = float(ex_res.get("avg_price") or float(e["close"][-1]))

                                    if dryrun and (ex_res.get("avg_price") is None):
                                        exit_px = _trace_exec_with_slip(dryrun=True, kind="exit_long", px=float(e["close"][-1]))

                                    if filled > 0:
                                        remaining = max(0.0, float(pos_qty) - float(filled))
                                    if remaining > 0:
                                        store.update_position(symbol, qty=float(remaining))
                                        logger.warning(
                                            f"[{tag}] {symbol} PARTIAL CLOSE {exit_reason} filled={filled:.8f}/{pos_qty:.8f} exit_px={exit_px} ({ex_res.get('reason')})"
                                        )
                                        _summ_inc(summ_evt, "CLOSE_PARTIAL")
                                    else:
                                        res = store.close_position(
                                            symbol,
                                            exit_price=float(exit_px),
                                            candle_ts_exit=int(candle_ts_run),
                                            reason=str(exit_reason),
                                            fee_rate_entry=float(fee_rate_entry),
                                            fee_rate_exit=float(fee_rate_exit),
                                        )
                                        closed_this_bar = True
                                        closed_exit_reason = str(exit_reason)
                                        _chart_state_note_marker(
                                            symbol,
                                            kind="exit",
                                            ts_ms=int(trace_event_ts_manage_ms),
                                            price=float(exit_px),
                                            side="long",
                                            label=str(exit_reason),
                                        )
                                        _parity_probe_write(
                                            "CLOSE_COMMIT",
                                            {
                                                "symbol": str(symbol),
                                                "replay_ts_ms": int(replay_ts_ms or candle_ts_run),
                                                "candle_ts_run": int(candle_ts_run),
                                                "ts_ms_now": int(ts_ms_now),
                                                "candle_ts_exit_committed": int(candle_ts_run),
                                                "exit_reason": str(exit_reason),
                                                "position_exists_after_close": bool(store.get_position(symbol)),
                                                "phase_seq": int(phase_seq),
                                            },
                                        )
                                        res = _ensure_pnl_fee(
                                            res=res,
                                            pos=pos,
                                            exit_exec=float(exit_px),
                                            fee_rate_entry=float(fee_rate_entry),
                                            fee_rate_exit=float(fee_rate_exit),
                                            default_qty=float(pos_qty),
                                        )
                                        _clear_pos_max_fav(store, symbol)
                                        _clear_pos_min_adv(store, symbol)
                                        net = float(res.get("pnl") or 0.0) - float(res.get("fee") or 0.0)
                                        _apply_compounded_close_net(net)
                                        _summ_inc(summ_evt, "CLOSE")
                                        _log_trade_event(
                                            f"[{tag}] {symbol} CLOSE {exit_reason} exit_px={exit_px} qty={pos_qty:.6f} net={net:.6f} {_quote_ccy_for_symbol(symbol)} ({ex_res.get('reason')})"
                                        )
                                        _refresh_live_equity_after_close()
                                        _diff_trace_write(
                                            {
                                                "event": "CLOSE",
                                                "ts_ms": int(trace_event_ts_manage_ms),
                                                "candle_ts_run": int(candle_ts_run),
                                                "ts_ms_now": int(ts_ms_now),
                                                "candle_ts_entry": int(candle_ts_entry),
                                                "candle_ts_exit": int(candle_ts_run),
                                                "signal_path": str(signal_path),
                                                "candle_ts_run": int(candle_ts_run),
                                                "ts_ms_now": int(ts_ms_now),
                                                "candle_ts_entry": int(candle_ts_entry),
                                                "signal_path": str(signal_path),
                                                "candle_ts_run": int(candle_ts_run),
                                                "ts_ms_now": int(ts_ms_now),
                                                "candle_ts_entry": int(candle_ts_entry),
                                                "signal_path": str(signal_path),
                                                "symbol": str(symbol),
                                                "tf_entry": str(tf_entry),
                                                "tf_filter": str(tf_filter),
                                                "mode": str(tag),
                                                "cfg": _cfg_snapshot(),
                                                "regime": str(regime),
                                                "exit_reason": str(exit_reason),
                                                "entry_exec": float(res.get("entry_exec") or pos_entry),
                                                "exit_exec": float(exit_px),
                                                "qty": float(res.get("qty") or pos_qty),
                                                "pos_stop_raw": float(pos.get("stop_raw") or 0.0) if isinstance(pos, dict) else None,
                                                "pos_tp_raw": float(pos.get("tp_raw") or 0.0) if isinstance(pos, dict) else None,
                                                "pos_stop_exec": float(pos_stop) if pos_stop is not None else None,
                                                "pos_tp_exec": float(pos_tp) if pos_tp is not None else None,
                                                "pnl": float(res.get("pnl") or 0.0),
                                                "fee": float(res.get("fee") or 0.0),
                                                "net": float(net),
                                                **trace_bar_fields,
                                            }
                                        )
                                        _record_trade_row(
                                            store=store,
                                            res=res,
                                            exit_reason=str(exit_reason),
                                            net=net,
                                            stop=float(pos_stop),
                                            tp=float(pos_tp),
                                            stop_kind=str(pos.get("stop_kind") or "init") if isinstance(pos, dict) else "init",
                                        )
                                # Finish position handling for this symbol after the timeout exit path.
                                if is_replay and closed_this_bar and (store.get_position(symbol) is None):
                                    raise _ReplayPositionClosedSameBar()
                                continue

                # --- TREND TIMEOUT EXIT (NEW) ---
                if str(regime).lower() == "trend":
                    timeout_bars = int(getattr(C, "TREND_TIMEOUT_BARS", 0))
                    if timeout_bars > 0:
                        ts_open = int(pos.get("candle_ts_open") or 0)
                        ts_now = int(e["timestamp"][-1])
                        # bars = elapsed seconds / entry_tf_seconds (do not hardcode 15m)
                        try:
                            tf_ms = int(_timeframe_to_ms(str(tf_entry)))
                            tf_sec = max(1, tf_ms // 1000)
                        except Exception:
                            tf_sec = 60
                        age_bars = int(max(0, (ts_now - ts_open)) // 1000 // int(tf_sec))
                        if age_bars >= timeout_bars:
                            # Priority: if TP/SL would be hit on this bar, do NOT take TIMEOUT.
                            # Align with backtest where TP/SL (incl. BE stop) is evaluated before timeout exits.
                            pos_dir = str(pos.get("direction") or "").lower()
                            hit_tp_now = (pos_dir == "long") and (float(e["high"][-1]) >= float(pos_tp))
                            hit_sl_now = (pos_dir == "long") and (float(e["low"][-1]) <= float(pos_stop))
                            if hit_tp_now or hit_sl_now:
                                pass
                            else:
                                pnl_bps = (float(e["close"][-1]) - pos_entry) / pos_entry * 10000.0
                                min_keep = float(getattr(C, "TREND_TIMEOUT_MIN_PROFIT_BPS", -5.0))
                            if pnl_bps >= min_keep:
                                require_above_ema9 = bool(getattr(C, "TREND_TIMEOUT_REQUIRE_ABOVE_EMA9", False))
                                if require_above_ema9:
                                    try:
                                        ema9_now = float(ind_ema(e["close"], 9)[-1])
                                    except Exception:
                                        ema9_now = float("nan")
                                    close_now = float(e["close"][-1])
                                    # If EMA9 is not available, behave safely: do not trigger timeout.
                                    if not (ema9_now == ema9_now):
                                        pass
                                    elif close_now < float(ema9_now):
                                        pass
                                    else:
                                        exit_reason = f"TREND_TIMEOUT({age_bars}bars pnl={pnl_bps:.1f}bps)"
                                else:
                                    exit_reason = f"TREND_TIMEOUT({age_bars}bars pnl={pnl_bps:.1f}bps)"

                                entry_type = str(getattr(C, "PAPER_ENTRY_FEE_TYPE", "taker"))
                                stop_type = str(getattr(C, "PAPER_STOP_FEE_TYPE", "taker"))
                                timeout_type = str(getattr(C, "PAPER_TIMEOUT_FEE_TYPE", stop_type))

                                fee_rate_entry = _fee_rate_by_type(entry_type)
                                fee_rate_exit = _fee_rate_by_type(timeout_type)

                                ex_res = _exit_stop_live(ex, symbol, pos_qty, dryrun=dryrun, store=store)
                                filled = float(ex_res.get("filled_qty") or 0.0)
                                exit_px = float(ex_res.get("avg_price") or float(e["close"][-1]))

                                if dryrun and (ex_res.get("avg_price") is None):
                                    exit_px = _trace_exec_with_slip(dryrun=True, kind="exit_long", px=float(e["close"][-1]))

                                if filled > 0:
                                    remaining = max(0.0, float(pos_qty) - float(filled))
                                    if remaining > 0:
                                        store.update_position(symbol, qty=float(remaining))
                                        logger.warning(
                                            f"[{tag}] {symbol} PARTIAL CLOSE {exit_reason} filled={filled:.8f}/{pos_qty:.8f} exit_px={exit_px} ({ex_res.get('reason')})"
                                        )
                                        _summ_inc(summ_evt, "CLOSE_PARTIAL")
                                    else:
                                        res = store.close_position(
                                            symbol,
                                            exit_price=float(exit_px),
                                            candle_ts_exit=int(candle_ts_run),
                                            reason=str(exit_reason),
                                            fee_rate_entry=float(fee_rate_entry),
                                            fee_rate_exit=float(fee_rate_exit),
                                        )
                                        closed_this_bar = True
                                        closed_exit_reason = str(exit_reason)
                                        _chart_state_note_marker(
                                            symbol,
                                            kind="exit",
                                            ts_ms=int(trace_event_ts_manage_ms),
                                            price=float(exit_px),
                                            side="long",
                                            label=str(exit_reason),
                                        )
                                        res = _ensure_pnl_fee(
                                            res=res,
                                            pos=pos,
                                            exit_exec=float(exit_px),
                                            fee_rate_entry=float(fee_rate_entry),
                                            fee_rate_exit=float(fee_rate_exit),
                                            default_qty=float(pos_qty),
                                        )
                                        _clear_pos_init_stop(store, symbol)
                                        _clear_pos_max_fav(store, symbol)
                                        _clear_pos_min_adv(store, symbol)
                                        net = float(res.get("pnl") or 0.0) - float(res.get("fee") or 0.0)
                                        _apply_compounded_close_net(net)
                                        _summ_inc(summ_evt, "CLOSE")
                                        _log_trade_event(
                                            f"[{tag}] {symbol} CLOSE {exit_reason} exit_px={exit_px} qty={pos_qty:.6f} net={net:.6f} {_quote_ccy_for_symbol(symbol)} ({ex_res.get('reason')})"
                                        )
                                        _refresh_live_equity_after_close()
                                        _diff_trace_write(
                                            {
                                                "event": "CLOSE",
                                                "ts_ms": int(trace_event_ts_manage_ms),
                                                "candle_ts_run": int(candle_ts_run),
                                                "ts_ms_now": int(ts_ms_now),
                                                "candle_ts_entry": int(candle_ts_entry),
                                                "signal_path": str(signal_path),
                                                "symbol": str(symbol),
                                                "tf_entry": str(tf_entry),
                                                "tf_filter": str(tf_filter),
                                                "mode": str(tag),
                                                "cfg": _cfg_snapshot(),
                                                "regime": str(regime),
                                                "exit_reason": str(exit_reason),
                                                "entry_exec": float(res.get("entry_exec") or pos_entry),
                                                "exit_exec": float(exit_px),
                                                "qty": float(res.get("qty") or pos_qty),
                                                "stop_raw": float(p.get("stop_raw") or 0.0),
                                                "tp_raw": float(p.get("tp_raw") or 0.0),
                                                "stop_exec": float(p.get("stop") or 0.0),
                                                "tp_exec": float(p.get("tp") or 0.0),
                                                "hit_tp": bool(hit_tp) if "hit_tp" in locals() else None,
                                                "hit_sl": bool(hit_sl) if "hit_sl" in locals() else None,
                                                "pnl": float(res.get("pnl") or 0.0),
                                                "fee": float(res.get("fee") or 0.0),
                                                "net": float(net),
                                                **trace_bar_fields,
                                            }
                                        )
                                        _record_trade_row(
                                            store=store,
                                            res=res,
                                            exit_reason=str(exit_reason),
                                            net=net,
                                            stop=float(pos_stop),
                                            tp=float(pos_tp),
                                            stop_kind=str(pos.get("stop_kind") or "init") if isinstance(pos, dict) else "init",
                                        )
                                continue

                # Run the EXIT check only once per bar.
                skip_exit_check = bool(stop_updated)
                # Backtest parity (REPLAY diff_trace): STOP/TP is evaluated on the same bar
                # even when BE/TRAIL updated stop earlier in that bar.
                if stop_updated and dryrun and str(_DIFF_TRACE_SOURCE) == "replay":
                    skip_exit_check = False

                if not skip_exit_check:

                    # Use the standard TP and SL hit test here.
                    # Spot is long-only, but keep compatibility if a direction key exists.
                    pos_dir = str(pos.get("dir") or pos.get("side") or pos.get("direction") or "long").lower()
                    stop_kind = str(pos.get("stop_kind") or "init")
                    hit_tp = (pos_dir == "long" and float(e["high"][-1]) >= float(pos_tp)) or (
                        pos_dir == "short" and float(e["low"][-1]) <= float(pos_tp)
                    )
                    hit_sl = (pos_dir == "long" and float(e["low"][-1]) <= float(pos_stop)) or (
                        pos_dir == "short" and float(e["high"][-1]) >= float(pos_stop)
                    )
                    if hit_tp or hit_sl:
                        exit_hit = _resolve_tp_sl_same_bar(
                            side=pos_dir,
                            bar_open=float(e["open"][-1]),
                            bar_high=float(e["high"][-1]),
                            bar_low=float(e["low"][-1]),
                            bar_close=float(e["close"][-1]),
                            tp=float(pos_tp),
                            sl=float(pos_stop),
                        )
                        if exit_hit is None:
                            continue

                        entry_type = str(getattr(C, "PAPER_ENTRY_FEE_TYPE", "taker"))
                        if exit_hit == "STOP_HIT":
                            exit_reason = "STOP_HIT"
                            stop_type = str(getattr(C, "PAPER_STOP_FEE_TYPE", "taker"))
                            fee_rate_exit = _fee_rate_by_type(stop_type)
                            ex_res = _exit_stop_live(ex, symbol, pos_qty, dryrun=dryrun, store=store)
                        else:
                            exit_reason = "TP_HIT"
                            tp_type = str(getattr(C, "PAPER_TP_FEE_TYPE", getattr(C, "PAPER_EXIT_FEE_TYPE", getattr(C, "PAPER_STOP_FEE_TYPE", "taker"))))
                            fee_rate_exit = _fee_rate_by_type(tp_type)
                            ex_res = _exit_tp_live(ex, symbol, pos_qty, dryrun=dryrun, store=store)

                        fee_rate_entry = _fee_rate_by_type(entry_type)
                        filled = float(ex_res.get("filled_qty") or 0.0)
                        exit_px = float(ex_res.get("avg_price") or float(e["close"][-1]))

                        if dryrun and (ex_res.get("avg_price") is None):
                            exit_raw = float(pos_tp) if str(exit_reason) == "TP_HIT" else float(pos_stop)
                            exit_px = _trace_exec_with_slip(dryrun=True, kind="exit_long", px=float(exit_raw))

                        # Keep runner stop taxonomy aligned with backtest using executed price only.
                        if str(exit_reason) == "STOP_HIT":
                            try:
                                kind = str(stop_kind or "").lower()
                                if float(exit_px) >= float(pos_entry):
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

                        if filled > 0:
                            remaining = max(0.0, float(pos_qty) - float(filled))
                            if remaining > 0:
                                store.update_position(symbol, qty=float(remaining))
                                logger.warning(
                                    f"[{tag}] {symbol} PARTIAL CLOSE {exit_reason} filled={filled:.8f}/{pos_qty:.8f} exit_px={exit_px} ({ex_res.get('reason')})"
                                )
                                _summ_inc(summ_evt, "CLOSE_PARTIAL")
                            else:
                                res = store.close_position(
                                    symbol,
                                    exit_price=float(exit_px),
                                    candle_ts_exit=int(candle_ts_run),
                                    reason=str(exit_reason),
                                    fee_rate_entry=float(fee_rate_entry),
                                    fee_rate_exit=float(fee_rate_exit),
                                )
                                closed_this_bar = True
                                closed_exit_reason = str(exit_reason)
                                _chart_state_note_marker(
                                    symbol,
                                    kind="exit",
                                    ts_ms=int(trace_event_ts_manage_ms),
                                    price=float(exit_px),
                                    side="long",
                                    label=str(exit_reason),
                                )
                                res = _ensure_pnl_fee(
                                    res=res,
                                    pos=pos,
                                    exit_exec=float(exit_px),
                                    fee_rate_entry=float(fee_rate_entry),
                                    fee_rate_exit=float(fee_rate_exit),
                                    default_qty=float(pos_qty),
                                )
                                net = float(res.get("pnl") or 0.0) - float(res.get("fee") or 0.0)
                                _apply_compounded_close_net(net)
                                _summ_inc(summ_evt, "CLOSE")
                                logger.info(
                                    f"[{tag}] {symbol} CLOSE {exit_reason} exit_px={exit_px} qty={pos_qty:.6f} net={net:.6f} {_quote_ccy_for_symbol(symbol)} ({ex_res.get('reason')})"
                                )
                                trade_logger.info(
                                    f"[{tag}] {symbol} CLOSE {exit_reason} exit_px={exit_px} qty={pos_qty:.6f} net={net:.6f} {_quote_ccy_for_symbol(symbol)} ({ex_res.get('reason')})"
                                )
                                _refresh_live_equity_after_close()
                                _diff_trace_write(
                                    {
                                        "event": "CLOSE",
                                        "ts_ms": int(trace_event_ts_manage_ms),
                                        "candle_ts_run": int(candle_ts_run),
                                        "candle_ts_entry": int(candle_ts_entry),
                                        "symbol": str(symbol),
                                        "tf_entry": str(tf_entry),
                                        "tf_filter": str(tf_filter),
                                        "mode": str(tag),
                                        "cfg": _cfg_snapshot(),
                                        "regime": str(regime),
                                        "exit_reason": str(exit_reason),
                                        "entry_exec": float(res.get("entry_exec") or pos_entry),
                                        "exit_exec": float(exit_px),
                                        "qty": float(res.get("qty") or pos_qty),
                                        "pnl": float(res.get("pnl") or 0.0),
                                        "fee": float(res.get("fee") or 0.0),
                                        "net": float(net),
                                        **trace_bar_fields,
                                    }
                                )
                                _record_trade_row(
                                    store=store,
                                    res=res,
                                    exit_reason=str(exit_reason),
                                    net=net,
                                    stop=float(pos_stop),
                                    tp=float(pos_tp),
                                    stop_kind=str(pos.get("stop_kind") or "init") if isinstance(pos, dict) else "init",
                                )
                        if is_replay and closed_this_bar and (store.get_position(symbol) is None):
                            raise _ReplayPositionClosedSameBar()
                        continue

                    try:
                        ema9_series = ind_ema(e["close"], 9)
                        ema21_series = ind_ema(e["close"], 21)
                        atr14_series = ind_atr(e["high"], e["low"], e["close"], 14)
                        rsi14_series = ind_rsi(e["close"], 14)
                        pos_dir = str(pos.get("direction") or "").lower()
                        hit_tp_now = (pos_dir == "long") and (float(e["high"][-1]) >= float(pos_tp))
                        hit_sl_now = (pos_dir == "long") and (float(e["low"][-1]) <= float(pos_stop))
                        tp_sl_hit_now = bool(hit_tp_now or hit_sl_now)
                        exit_fn = getattr(STRAT, "exit_signal_entry_precomputed", None)
                        i_now = len(e["close"]) - 1
                        ex_sig = exit_fn(
                            regime=str(regime),
                            direction=str(pos.get("dir") or "long"),
                            i=int(i_now),
                            close=e["close"],
                            ema9=list(ema9_series) if hasattr(ema9_series, "__len__") else [],
                            ema21=list(ema21_series) if hasattr(ema21_series, "__len__") else [],
                            rsi14=list(rsi14_series) if hasattr(rsi14_series, "__len__") else [],
                            atr14=list(atr14_series) if hasattr(atr14_series, "__len__") else [],
                            entry_px=float(pos_entry),
                        )

                        # Priority: if TP/SL is hit on this bar, do NOT take strategy-driven exits.
                        # Align with backtest where TP/SL (incl. BE stop) is evaluated before signal exits.
                        if tp_sl_hit_now:
                            ex_sig = None

                        # Gate: optionally disable strategy-driven exits in range regime.
                        # (A-1) We are redesigning range expectancy; EMA9 cross + EMA21 break exits are optional.
                        if isinstance(ex_sig, dict) and ex_sig.get("action") == "exit":
                            if str(regime).lower() == "range":
                                r = str(ex_sig.get("reason") or "")
                                # (1) EMA9 cross exit (feature flag)
                                if not bool(getattr(C, "RANGE_EXIT_ON_EMA9_CROSS", False)) and r in ("RANGE_EMA9_CROSS_DOWN", "RANGE_EMA9_CROSS_UP"):
                                    ex_sig = None
                                # (2) EMA21 break exit is forcibly disabled in runner (A-1: exit redesign).
                                if r.startswith("RANGE_EMA21_BREAK"):
                                    ex_sig = None

                        if isinstance(ex_sig, dict) and ex_sig.get("action") == "exit":
                            exit_reason = str(ex_sig.get("reason") or "strategy_exit")

                            entry_type = str(getattr(C, "PAPER_ENTRY_FEE_TYPE", "taker"))
                            # Treat strategy exits as normal exits and fall back to the STOP fee type when unset.
                            exit_type = str(getattr(C, "PAPER_EXIT_FEE_TYPE", getattr(C, "PAPER_STOP_FEE_TYPE", "taker")))
                            fee_rate_entry = _fee_rate_by_type(entry_type)
                            fee_rate_exit = _fee_rate_by_type(exit_type)

                            ex_res = _exit_stop_live(ex, symbol, pos_qty, dryrun=dryrun, store=store)
                            filled = float(ex_res.get("filled_qty") or 0.0)
                            exit_px = float(ex_res.get("avg_price") or float(e["close"][-1]))

                            if dryrun and (ex_res.get("avg_price") is None):
                                exit_px = _trace_exec_with_slip(dryrun=True, kind="exit_long", px=float(e["close"][-1]))

                            if filled > 0:
                                remaining = max(0.0, float(pos_qty) - float(filled))
                                if remaining > 0:
                                    store.update_position(symbol, qty=float(remaining))
                                    logger.warning(
                                        f"[{tag}] {symbol} PARTIAL CLOSE {exit_reason} "
                                        f"filled={filled:.8f}/{pos_qty:.8f} exit_px={exit_px} ({ex_res.get('reason')})"
                                    )
                                    _summ_inc(summ_evt, "CLOSE_PARTIAL")
                                else:
                                    res = store.close_position(
                                        symbol,
                                        exit_price=float(exit_px),
                                        candle_ts_exit=int(candle_ts_run),
                                        reason=str(exit_reason),
                                        fee_rate_entry=float(fee_rate_entry),
                                        fee_rate_exit=float(fee_rate_exit),
                                    )
                                    closed_this_bar = True
                                    closed_exit_reason = str(exit_reason)
                                    _chart_state_note_marker(
                                        symbol,
                                        kind="exit",
                                        ts_ms=int(trace_event_ts_manage_ms),
                                        price=float(exit_px),
                                        side="long",
                                        label=str(exit_reason),
                                    )
                                    res = _ensure_pnl_fee(
                                        res=res,
                                        pos=pos,
                                        exit_exec=float(exit_px),
                                        fee_rate_entry=float(fee_rate_entry),
                                        fee_rate_exit=float(fee_rate_exit),
                                        default_qty=float(pos_qty),
                                    )
                                    _clear_pos_init_stop(store, symbol)
                                    _clear_pos_max_fav(store, symbol)
                                    _clear_pos_min_adv(store, symbol)
                                    net = float(res.get("pnl") or 0.0) - float(res.get("fee") or 0.0)
                                    _apply_compounded_close_net(net)
                                    _summ_inc(summ_evt, "CLOSE")
                                    _log_trade_event(
                                        f"[{tag}] {symbol} CLOSE {exit_reason} exit_px={exit_px} "
                                        f"qty={pos_qty:.6f} net={net:.6f} {_quote_ccy_for_symbol(symbol)} ({ex_res.get('reason')})"
                                    )
                                    _refresh_live_equity_after_close()
                                    _diff_trace_write(
                                        {
                                            "event": "CLOSE",
                                            "ts_ms": int(trace_event_ts_manage_ms),
                                            "candle_ts_run": int(candle_ts_run),
                                            "ts_ms_now": int(ts_ms_now),
                                            "candle_ts_entry": int(candle_ts_entry),
                                            "signal_path": str(signal_path),
                                            "symbol": str(symbol),
                                            "tf_entry": str(tf_entry),
                                            "tf_filter": str(tf_filter),
                                            "mode": str(tag),
                                            "cfg": _cfg_snapshot(),
                                            "regime": str(regime),
                                            "exit_reason": str(exit_reason),
                                            "entry_exec": float(res.get("entry_exec") or pos_entry),
                                            "exit_exec": float(exit_px),
                                            "qty": float(res.get("qty") or pos_qty),
                                            "pnl": float(res.get("pnl") or 0.0),
                                            "fee": float(res.get("fee") or 0.0),
                                            "net": float(net),
                                            **trace_bar_fields,
                                        }
                                    )
                                    _record_trade_row(
                                        store=store,
                                        res=res,
                                        exit_reason=str(exit_reason),
                                        net=net,
                                        stop=float(pos_stop),
                                        tp=float(pos_tp),
                                        stop_kind=str(pos.get("stop_kind") or "init") if isinstance(pos, dict) else "init",
                                    )
                        if (not is_replay) or (not closed_this_bar) or (store.get_position(symbol) is not None):
                            continue
                        # --- post-exit state (live-compatible) ---
                        try:
                            cd = ex_sig.get("cooldown") if isinstance(ex_sig, dict) else None
                            if isinstance(cd, dict) and cd.get("kind") == "range_ema9_exit":
                                bars = int(cd.get("bars", getattr(C, "RANGE_EMA9_EXIT_COOLDOWN_BARS", 12)))
                                # store EMA9 level at exit time for early clear condition
                                try:
                                    reclaim_ema9 = float(ind_ema(e["close"], 9)[-1])
                                except Exception:
                                    reclaim_ema9 = None
                                _set_range_ema9_exit_cooldown(
                                    store, symbol=symbol, exit_ts_ms=ts_ms_now, bars=bars, reclaim_ema9=reclaim_ema9
                                )
                            # Keep the RANGE early-loss block until price reclaims EMA21; EMA21_BREAK itself is not an exit here.
                            if str(exit_reason) in ("RANGE_EARLY_LOSS_ATR",):
                                try:
                                    # compute EMA21 level at exit time (store for clear_on condition)
                                    close_for_ema = [float(x) for x in e["close"][-60:]]  # enough history for EMA21
                                    reclaim_ema21 = _ema_last(close_for_ema, 21)
                                except Exception:
                                    reclaim_ema21 = None
                                _set_range_ema21_break_block(store, symbol=symbol, exit_ts_ms=ts_ms_now, reclaim_ema21=reclaim_ema21)
                                _parity_probe_write(
                                    "BLOCK_SET_AFTER_CLOSE",
                                    {
                                        "symbol": str(symbol),
                                        "replay_ts_ms": int(replay_ts_ms or candle_ts_run),
                                        "kind": "range_ema21_break",
                                        "set_ts_ms": int(ts_ms_now),
                                        "source_basis": "ts_ms_now",
                                        "exit_reason": str(exit_reason),
                                        "phase_seq": int(phase_seq),
                                    },
                                )
                        except Exception:
                            pass
                            continue
                    except Exception as _e:
                        # If exit-signal evaluation fails, log it and keep the trading loop alive.
                        logger.warning(f"[{tag}] exit_signal check failed: {_e}")
                        _summ_reason(summ_hold, f"exit_signal_check_failed:{_e}")
                        _diff_trace_exception(
                            symbol=symbol,
                            ts_ms=ts_ms_now,
                            phase="exit_signal_check",
                            exc=_e,
                            extra={"mode": mode, "tag": tag, "signal_path": signal_path},
                        )

            except _ReplayPositionClosedSameBar:
                pass
            except Exception as epos:
                _summ_reason(summ_hold, f"pos_manage_error:{epos}")
                _diff_trace_exception(
                    symbol=symbol,
                    ts_ms=ts_ms_now,
                    phase="pos_manage",
                    exc=epos,
                    extra={"mode": mode, "tag": tag, "signal_path": signal_path},
                )

            # This symbol already finished position management for the current run, so skip new entries.
            if not (
                is_replay
                and closed_this_bar
                and (store.get_position(symbol) is None)
                and _same_bar_reentry_allowed_exit_reason(closed_exit_reason)
            ):
                continue


        # --- STOP_NEW_ONLY -> skip any new entries ---
        if stop_new_only_active:
            continue

        # --- detect regime (filter TF) ---
        try:
            f_raw = ex.fetch_ohlcv(symbol, tf_filter, limit=200)
            f = ohlcv_to_dict(f_raw)
            regime = "range"
            direction = "none"
            precomputed_regime_used = False
            if is_replay:
                try:
                    pre_map = getattr(ex, "_replay_filter_precomputed", {}) or {}
                    pre_sym = (pre_map.get(str(symbol), {}) or {}).get(str(tf_filter))
                    if pre_sym and f.get("timestamp"):
                        pre_ts = list(pre_sym.get("timestamp", []) or [])
                        last_f_ts = int(f["timestamp"][-1])
                        j_pre = bisect_right(pre_ts, int(last_f_ts)) - 1
                        if j_pre >= 0:
                            regime, direction = detect_regime_1h_precomputed(
                                close=pre_sym.get("close", []) or f["close"],
                                i=int(j_pre),
                                adx_arr=pre_sym.get("adx", []),
                                ema_fast_arr=pre_sym.get("ema_fast", []),
                                ema_slow_arr=pre_sym.get("ema_slow", []),
                            )
                            precomputed_regime_used = True
                except Exception:
                    precomputed_regime_used = False
            if not precomputed_regime_used:
                adx_period = int(getattr(C, "ADX_PERIOD", 14))
                ema_fast = int(getattr(C, "EMA_FAST", 20))
                ema_slow = int(getattr(C, "EMA_SLOW", 50))
                adx_arr = ind_adx(f["high"], f["low"], f["close"], period=adx_period)
                ema_fast_arr = ind_ema(f["close"], ema_fast)
                ema_slow_arr = ind_ema(f["close"], ema_slow)
                regime, direction = detect_regime_1h(f["close"], adx_arr, ema_fast_arr, ema_slow_arr)
            # If TREND trading is disabled but RANGE trading is enabled, we must not let
            # the 1h regime classifier force "trend" because the downstream signal path
            # would become 'trend_disabled' (no OPEN), making verify_diff unmatched.
            # In that configuration, treat any "trend" classification as "range" for entry/exit decisions.
            try:
                _trade_trend = float(getattr(C, "TRADE_TREND", 0.0) or 0.0)
            except Exception:
                _trade_trend = 0.0
            _trade_range = bool(getattr(C, "TRADE_RANGE", True))
            if _trade_trend <= 0.0 and _trade_range and str(regime).lower() == "trend":
                regime = "range"
            if runtime_signal_log_enabled:
                _signal_reason_capture_begin()
            # === Trend direction-none guard (quality filter) ===
            if str(regime).lower() == "trend" and str(direction).lower() == "none" and bool(getattr(C, "TREND_BLOCK_DIR_NONE", False)):
                b_reason = "trend_dir_none"
                _summ_reason(summ_hold, b_reason)
                _emit_runtime_signal_outcome(
                    enabled=runtime_signal_log_enabled,
                    tag=str(tag),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    bar_ts_ms=int(bar_ts_ms),
                    regime=str(regime),
                    direction=str(direction),
                    close_last=float(close_last),
                    final_action="hold",
                    signal_action="hold",
                    signal_reason=str(b_reason),
                )
                continue
        except Exception as efil:
            _signal_reason_capture_end()
            logger.warning("[OHLCV][FILTER] symbol=%s tf=%s reason=%s", str(symbol), str(tf_filter), efil)
            _summ_reason(summ_hold, f"fetch_ohlcv_filter_failed:{efil}")
            continue

        if summ_regime is not None:
            summ_regime[str(regime)] += 1

        # --- signal ---
        signal_path = "unset"
        sig = {"action": "hold", "reason": "default_hold"}
        signal_path = "default_hold"
        if str(regime).lower() == "range":
            if not bool(getattr(C, "TRADE_RANGE", True)):
                sig = {"action": "hold", "reason": "range_disabled"}
                signal_path = "range:disabled"
            else:
                pre_sig_done = False
                if is_replay:
                    try:
                        pre_map = getattr(ex, "_replay_entry_precomputed", {}) or {}
                        pre_sym = (pre_map.get(str(symbol), {}) or {}).get(str(tf_entry))
                        last_e_ts = int(e["timestamp"][-1]) if e.get("timestamp") else 0
                        if pre_sym and last_e_ts > 0:
                            pre_ts = list(pre_sym.get("timestamp", []) or [])
                            i_pre = bisect_right(pre_ts, int(last_e_ts)) - 1
                            if i_pre >= 1 and int(pre_ts[i_pre]) == int(last_e_ts):
                                signal_path = "range:signal_range_entry_precomputed"
                                sig = signal_range_entry_precomputed(
                                    regime=str(regime),
                                    direction=str(direction),
                                    i=int(i_pre),
                                    open_=pre_sym.get("open", []) or e["open"],
                                    high=pre_sym.get("high", []) or e["high"],
                                    low=pre_sym.get("low", []) or e["low"],
                                    close=pre_sym.get("close", []) or e["close"],
                                    ema9=pre_sym.get("ema9", []),
                                    ema21=pre_sym.get("ema21", []),
                                    rsi14=pre_sym.get("rsi14", []),
                                    atr14=pre_sym.get("atr14", []),
                                )
                                pre_sig_done = True
                    except Exception as _e_pre:
                        logger.warning(
                            "[REPLAY][PRECOMPUTED][ENTRY] signal fallback symbol=%s tf=%s reason=%s",
                            str(symbol),
                            str(tf_entry),
                            _e_pre,
                        )
                if not pre_sig_done:
                    # Fallback: compute indicators from the confirmed replay window.
                    try:
                        ema9_series = ind_ema(e["close"], 9)
                        ema21_series = ind_ema(e["close"], 21)
                        rsi14_series = ind_rsi(e["close"], 14)
                        atr_period = int(getattr(C, "ATR_PERIOD", 14))
                        atr14_series = ind_atr(e["high"], e["low"], e["close"], period=atr_period)
                    except Exception as _e_ind:
                        sig = {"action": "hold", "reason": f"range_indicator_calc_failed:{_e_ind}"}
                        signal_path = "range:indicator_calc_failed"
                    else:
                        signal_path = "range:signal_range_entry"
                        sig = signal_range_entry(
                            open_=list(e["open"]),
                            high=list(e["high"]),
                            low=list(e["low"]),
                            close=list(e["close"]),
                            ema9=list(ema9_series) if hasattr(ema9_series, "__len__") else [],
                            ema21=list(ema21_series) if hasattr(ema21_series, "__len__") else [],
                            rsi14=list(rsi14_series) if hasattr(rsi14_series, "__len__") else [],
                            atr14=list(atr14_series) if hasattr(atr14_series, "__len__") else [],
                            direction=str(direction),
                        )
        elif str(regime).lower() == "trend":
            if not bool(getattr(C, "TRADE_TREND", True)):
                sig = {"action": "hold", "reason": "trend_disabled"}
                signal_path = "trend:disabled"
            else:
                pb_state = _kv_get_pullback_ab(store, symbol)
                # strategy.signal_entry() expects precomputed indicators (ema/rsi/atr).
                try:
                    ema9_series = ind_ema(e["close"], 9)
                    ema21_series = ind_ema(e["close"], 21)
                    rsi14_series = ind_rsi(e["close"], 14)
                    atr_period = int(getattr(C, "ATR_PERIOD", 14))
                    atr14_series = ind_atr(e["high"], e["low"], e["close"], period=atr_period)
                except Exception as _e_ind:
                    sig = {"action": "hold", "reason": f"trend_indicator_calc_failed:{_e_ind}"}
                    signal_path = "trend:indicator_calc_failed"
                else:
                    # TREND regime: feed candle arrays + computed indicator series.
                    # Use the current entry candle timestamp (ms) for both ts_ms/now_ms in replay.
                    ts_ms_now = int(e["timestamp"][-1])
                    signal_path = "trend:signal_entry_stateful"
                    sig, pb_state = signal_entry_stateful(
                        pb_state=pb_state,
                        now_ms=ts_ms_now,
                        regime=regime,
                        direction=direction,
                        open_=list(e["open"]),
                        high_=list(e["high"]),
                        low_=list(e["low"]),
                        close_=list(e["close"]),
                        ema9=list(ema9_series),
                        ema21=list(ema21_series),
                        rsi14=list(rsi14_series),
                        atr14=list(atr14_series),
                        cfg=C,
                    )
                    _kv_put_pullback_ab(store, symbol, pb_state)

        action = sig.get("action", "hold")
        reason = sig.get("reason", "")

        phase_seq += 1
        _parity_probe_write(
            "PRE_SIGNAL_CONTEXT",
            {
                "symbol": str(symbol),
                "replay_ts_ms": int(replay_ts_ms or candle_ts_run),
                "candle_ts_run": int(candle_ts_run),
                "ts_ms_now": int(ts_ms_now),
                "bar_ts_ms_for_signal": int(e["timestamp"][-1]) if (e.get("timestamp") and len(e["timestamp"]) > 0) else 0,
                "regime": str(regime),
                "direction": str(direction),
                "last_candle_ts_current": int(store.get_last_candle_ts(tf_entry) or 0),
                "has_position": bool(store.get_position(symbol)),
                "cooldown_ema9_blocked": False,
                "cooldown_ema21_blocked": False,
                "close_now": float(close_last),
                "phase_seq": int(phase_seq),
            },
        )
        # --- diff trace (always one record per processed entry candle) ---
        try:
            _diff_trace_write(
                {
                    "event": "SIGNAL",
                    "ts_ms": int(trace_event_ts_manage_ms),
                    "candle_ts_run": int(candle_ts_run),
                    "ts_ms_now": int(ts_ms_now),
                    "candle_ts_entry": int(candle_ts_entry),
                    "signal_path": str(signal_path),
                    "ts_ms_now": int(ts_ms_now),
                    "candle_ts_run": int(candle_ts_run),
                    "signal_path": str(signal_path),
                    "symbol": str(symbol),
                    "tf_entry": str(tf_entry),
                    "tf_filter": str(tf_filter),
                    "mode": str(tag),
                    "cfg": _cfg_snapshot(),
                    "regime": str(regime),
                    "direction": str(direction),
                    "action": str(action),
                    "reason": str(reason),
                    "close": close_last,
                }
            )
        except Exception:
            pass
        _parity_probe_write(
            "SIGNAL_DECISION",
            {
                "symbol": str(symbol),
                "replay_ts_ms": int(replay_ts_ms or candle_ts_run),
                "candle_ts_run": int(candle_ts_run),
                "ts_ms_now": int(ts_ms_now),
                "action": str(action),
                "reason": str(reason),
                "close_now": float(close_last),
                "phase_seq": int(phase_seq),
            },
        )
        if action != "buy":
            _summ_reason(summ_hold, reason)
            _emit_runtime_signal_outcome(
                enabled=runtime_signal_log_enabled,
                tag=str(tag),
                symbol=str(symbol),
                tf_entry=str(tf_entry),
                bar_ts_ms=int(bar_ts_ms),
                regime=str(regime),
                direction=str(direction),
                close_last=float(close_last),
                final_action="hold",
                signal_action=str(action),
                signal_reason=str(reason),
            )
            continue

        spread_for_kill = _spread_bps_from_orderbook(ex, symbol)
        kill_block, kill_reason = _kill_should_block_new_entries(
            ts_ms=int(candle_ts_run),
            spread_bps=spread_for_kill,
        )
        if kill_block:
            _summ_reason(summ_hold, f"kill_switch:{kill_reason}")
            _diff_trace_buy_reject(
                ts_ms=int(candle_ts_run),
                symbol=str(symbol),
                tf_entry=str(tf_entry),
                tf_filter=str(tf_filter),
                regime=str(regime),
                direction=str(direction),
                mode=str(tag),
                trade_range=bool(getattr(C, "TRADE_RANGE", False)),
                trade_trend=float(getattr(C, "TRADE_TREND", 0.0) or 0.0),
                cfg=_cfg_snapshot(),
                stage="KILL_SWITCH",
                reason=f"kill_switch:{kill_reason}",
                close=close_last,
            )
            _emit_runtime_signal_outcome(
                enabled=runtime_signal_log_enabled,
                tag=str(tag),
                symbol=str(symbol),
                tf_entry=str(tf_entry),
                bar_ts_ms=int(bar_ts_ms),
                regime=str(regime),
                direction=str(direction),
                close_last=float(close_last),
                final_action="hold",
                signal_action=str(action),
                signal_reason=str(reason),
            )
            continue

        # Live semantics are the source of truth here: replay must honor the same
        # daily/weekly risk gate so loss-streak halts line up with backtest/live.
        allowed, r_reason = _risk_allow_new_entry(store, ts_ms=candle_ts_run)
        if not allowed:
            _summ_reason(summ_hold, f"risk_gate:{r_reason}")
            _diff_trace_buy_reject(
                ts_ms=int(candle_ts_run),
                symbol=str(symbol),
                tf_entry=str(tf_entry),
                tf_filter=str(tf_filter),
                regime=str(regime),
                direction=str(direction),
                mode=str(tag),
                trade_range=bool(getattr(C, "TRADE_RANGE", False)),
                trade_trend=float(getattr(C, "TRADE_TREND", 0.0) or 0.0),
                cfg=_cfg_snapshot(),
                stage="RISK_GATE",
                reason=f"risk_gate:{r_reason}",
                close=close_last,
            )
            _emit_runtime_signal_outcome(
                enabled=runtime_signal_log_enabled,
                tag=str(tag),
                symbol=str(symbol),
                tf_entry=str(tf_entry),
                bar_ts_ms=int(bar_ts_ms),
                regime=str(regime),
                direction=str(direction),
                close_last=float(close_last),
                final_action="hold",
                signal_action=str(action),
                signal_reason=str(reason),
            )
            continue

        entry = float(sig["entry"])
        stop_price = float(sig["stop"])
        tp_price = float(sig["take_profit"])
        entry_raw = float(entry)
        stop_raw = float(stop_price)
        tp_raw = float(tp_price)
        rr0 = None
        risk0 = entry_raw - stop_raw
        reward0 = tp_raw - entry_raw
        if risk0 > 0 and reward0 > 0:
            rr0 = reward0 / risk0

        reg_ = str(regime).lower()
        dir_ = str(direction).lower()
        b_reason = f"entry_rr(bad_tp_sl reg={reg_} dir={dir_})"
        if not (stop_price < entry < tp_price):
            _summ_reason(summ_hold, b_reason)
            _diff_trace_buy_reject(
                ts_ms=int(candle_ts_run),
                symbol=str(symbol),
                tf_entry=str(tf_entry),
                tf_filter=str(tf_filter),
                regime=str(regime),
                direction=str(direction),
                mode=str(tag),
                trade_range=bool(getattr(C, "TRADE_RANGE", False)),
                trade_trend=float(getattr(C, "TRADE_TREND", 0.0) or 0.0),
                cfg=_cfg_snapshot(),
                stage="BAD_TP_SL",
                reason=b_reason,
                close=close_last,
                entry_raw=entry_raw,
                stop_raw=stop_raw,
                tp_raw=tp_raw,
                rr0=rr0,
            )
            _emit_runtime_signal_outcome(
                enabled=runtime_signal_log_enabled,
                tag=str(tag),
                symbol=str(symbol),
                tf_entry=str(tf_entry),
                bar_ts_ms=int(bar_ts_ms),
                regime=str(regime),
                direction=str(direction),
                close_last=float(close_last),
                final_action="hold",
                signal_action=str(action),
                signal_reason=str(reason),
            )
            continue

        # === Entry RR gate (before adjust_tp_sl) ===
        # NOTE: Use config MIN_RR_ENTRY (and optional MIN_RR_ENTRY_TREND_NONE) to reject poor RR at entry-time.
        min_rr_entry = float(getattr(C, "MIN_RR_ENTRY", 0.0))
        if reg_ == "trend" and dir_ == "none":
            min_rr_entry = float(getattr(C, "MIN_RR_ENTRY_TREND_NONE", min_rr_entry))
        if min_rr_entry and min_rr_entry > 0:
            risk = entry - stop_price
            reward = tp_price - entry
            if risk <= 0 or reward <= 0:
                _summ_reason(summ_hold, b_reason)
                _diff_trace_buy_reject(
                    ts_ms=int(candle_ts_run),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    tf_filter=str(tf_filter),
                    regime=str(regime),
                    direction=str(direction),
                    mode=str(tag),
                    trade_range=bool(getattr(C, "TRADE_RANGE", False)),
                    trade_trend=float(getattr(C, "TRADE_TREND", 0.0) or 0.0),
                    cfg=_cfg_snapshot(),
                    stage="RR_ENTRY",
                    reason=b_reason,
                    close=close_last,
                    entry_raw=entry_raw,
                    stop_raw=stop_raw,
                    tp_raw=tp_raw,
                    rr0=rr0,
                )
                _emit_runtime_signal_outcome(
                    enabled=runtime_signal_log_enabled,
                    tag=str(tag),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    bar_ts_ms=int(bar_ts_ms),
                    regime=str(regime),
                    direction=str(direction),
                    close_last=float(close_last),
                    final_action="hold",
                    signal_action=str(action),
                    signal_reason=str(reason),
                )
                continue
            rr0 = reward / risk
            if rr0 + 1e-9 < min_rr_entry:
                b_reason = f"entry_rr(rr={rr0:.2f} < {min_rr_entry:.2f} reg={reg_} dir={dir_})"
                _summ_reason(summ_hold, b_reason)
                _diff_trace_buy_reject(
                    ts_ms=int(candle_ts_run),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    tf_filter=str(tf_filter),
                    regime=str(regime),
                    direction=str(direction),
                    mode=str(tag),
                    trade_range=bool(getattr(C, "TRADE_RANGE", False)),
                    trade_trend=float(getattr(C, "TRADE_TREND", 0.0) or 0.0),
                    cfg=_cfg_snapshot(),
                    stage="RR_ENTRY",
                    reason=b_reason,
                    close=close_last,
                    entry_raw=entry_raw,
                    stop_raw=stop_raw,
                    tp_raw=tp_raw,
                    rr0=rr0,
                )
                _emit_runtime_signal_outcome(
                    enabled=runtime_signal_log_enabled,
                    tag=str(tag),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    bar_ts_ms=int(bar_ts_ms),
                    regime=str(regime),
                    direction=str(direction),
                    close_last=float(close_last),
                    final_action="hold",
                    signal_action=str(action),
                    signal_reason=str(reason),
                )
                continue

        fcfg = _filters_for_regime(str(regime))

        # maker block guard (ENTRY)
        # Replay/backtest alignment: skip maker-block gate (live-only)
        if not is_replay:
            blocked, b_reason = _is_maker_blocked(store, "ENTRY", symbol)
            if blocked:
                _summ_reason(summ_hold, b_reason)
                _diff_trace_buy_reject(
                    ts_ms=int(candle_ts_run),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    tf_filter=str(tf_filter),
                    regime=str(regime),
                    direction=str(direction),
                mode=str(tag),
                trade_range=bool(getattr(C, "TRADE_RANGE", False)),
                trade_trend=float(getattr(C, "TRADE_TREND", 0.0) or 0.0),
                cfg=_cfg_snapshot(),
                    stage="MAKER_BLOCK",
                    reason=b_reason,
                    close=close_last,
                    entry_raw=entry_raw,
                    stop_raw=stop_raw,
                    tp_raw=tp_raw,
                    rr0=rr0,
                )
                _emit_runtime_signal_outcome(
                    enabled=runtime_signal_log_enabled,
                    tag=str(tag),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    bar_ts_ms=int(bar_ts_ms),
                    regime=str(regime),
                    direction=str(direction),
                    close_last=float(close_last),
                    final_action="hold",
                    signal_action=str(action),
                    signal_reason=str(reason),
                )
                continue

        # Replay/backtest alignment: use backtest spread estimate instead of live orderbook
        if is_replay:
            sp = float(getattr(C, "BACKTEST_SPREAD_BPS", 0.0) or 0.0)
        else:
            sp = _spread_bps_from_orderbook(ex, symbol)
            if sp is None:
                _summ_reason(summ_hold, "spread_unknown")
                logger.warning("[SPREAD_FILTER] spread_unknown -> skip")
                _diff_trace_buy_reject(
                    ts_ms=int(candle_ts_run),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    tf_filter=str(tf_filter),
                    regime=str(regime),
                    direction=str(direction),
                    mode=str(tag),
                    trade_range=bool(getattr(C, "TRADE_RANGE", False)),
                    trade_trend=float(getattr(C, "TRADE_TREND", 0.0) or 0.0),
                    cfg=_cfg_snapshot(),
                    stage="SPREAD_GUARD",
                    reason="spread_unknown",
                    close=close_last,
                    entry_raw=entry_raw,
                    stop_raw=stop_raw,
                    tp_raw=tp_raw,
                    rr0=rr0,
                )
                _emit_runtime_signal_outcome(
                    enabled=runtime_signal_log_enabled,
                    tag=str(tag),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    bar_ts_ms=int(bar_ts_ms),
                    regime=str(regime),
                    direction=str(direction),
                    close_last=float(close_last),
                    final_action="hold",
                    signal_action=str(action),
                    signal_reason=str(reason),
                )
                continue

        # adjust (trend-only). Range uses its own ATR TP/SL; skip RR-based hard SL shaping.
        if bool(fcfg.get("ADJUST_TP_SL", True)) and reg_ != "range":
            ok_adj, stop_price, tp_price, adj_reason = _adjust_tp_sl(
                symbol=symbol, entry=entry, stop_price=stop_price, tp_price=tp_price,
                spread_bps=sp, high=e["high"], low=e["low"], close=e["close"], regime=str(regime), direction=direction
            )
            if not ok_adj:
                _summ_reason(summ_hold, adj_reason)
                _diff_trace_buy_reject(
                    ts_ms=int(candle_ts_run),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    tf_filter=str(tf_filter),
                    regime=str(regime),
                    direction=str(direction),
                    mode=str(tag),
                    trade_range=bool(getattr(C, "TRADE_RANGE", False)),
                    trade_trend=float(getattr(C, "TRADE_TREND", 0.0) or 0.0),
                    cfg=_cfg_snapshot(),
                    stage="ADJUST_TP_SL",
                    reason=str(adj_reason),
                    close=close_last,
                    entry_raw=entry_raw,
                    stop_raw=stop_raw,
                    tp_raw=tp_raw,
                    rr0=rr0,
                    extra={"spread_bps": sp},
                )
                _emit_runtime_signal_outcome(
                    enabled=runtime_signal_log_enabled,
                    tag=str(tag),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    bar_ts_ms=int(bar_ts_ms),
                    regime=str(regime),
                    direction=str(direction),
                    close_last=float(close_last),
                    final_action="hold",
                    signal_action=str(action),
                    signal_reason=str(reason),
                )
                continue

        # expectancy
        if bool(fcfg.get("EXPECTANCY", True)):
            ok_exp, exp_reason = _expectancy_filter(entry, stop_price, tp_price, sp)
            if not ok_exp:
                _summ_reason(summ_hold, exp_reason)
                _diff_trace_buy_reject(
                    ts_ms=int(candle_ts_run),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    tf_filter=str(tf_filter),
                    regime=str(regime),
                    direction=str(direction),
                    mode=str(tag),
                    trade_range=bool(getattr(C, "TRADE_RANGE", False)),
                    trade_trend=float(getattr(C, "TRADE_TREND", 0.0) or 0.0),
                    cfg=_cfg_snapshot(),
                    stage="EXPECTANCY",
                    reason=str(exp_reason),
                    close=close_last,
                    entry_raw=entry_raw,
                    stop_raw=stop_raw,
                    tp_raw=tp_raw,
                    rr0=rr0,
                    extra={"spread_bps": sp},
                )
                _emit_runtime_signal_outcome(
                    enabled=runtime_signal_log_enabled,
                    tag=str(tag),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    bar_ts_ms=int(bar_ts_ms),
                    regime=str(regime),
                    direction=str(direction),
                    close_last=float(close_last),
                    final_action="hold",
                    signal_action=str(action),
                    signal_reason=str(reason),
                )
                continue

        rr_adj = None
        risk_adj = entry_raw - stop_price
        reward_adj = tp_price - entry_raw
        if risk_adj > 0 and reward_adj > 0:
            rr_adj = reward_adj / risk_adj
        tp_bps_raw = (tp_raw - entry_raw) / entry_raw * 10000.0 if entry_raw else None
        sl_bps_raw = (stop_raw - entry_raw) / entry_raw * 10000.0 if entry_raw else None
        # spread hard guard (entry pre-check)
        max_entry_spread = float(getattr(C, "MAX_SPREAD_BPS", 1.5))
        if sp is not None and sp > max_entry_spread:
            logger.info(f"[SPREAD_FILTER] skip entry symbol={symbol} spread_bps={sp:.2f} max={max_entry_spread:.2f}")
            r2 = f"spread_gate:{sp:.2f}bps>{max_entry_spread:.2f}"
            _summ_reason(summ_hold, r2)
            _diff_trace_buy_reject(
                ts_ms=int(candle_ts_run),
                symbol=str(symbol),
                tf_entry=str(tf_entry),
                tf_filter=str(tf_filter),
                regime=str(regime),
                direction=str(direction),
                mode=str(tag),
                trade_range=bool(getattr(C, "TRADE_RANGE", False)),
                trade_trend=float(getattr(C, "TRADE_TREND", 0.0) or 0.0),
                cfg=_cfg_snapshot(),
                stage="SPREAD_GUARD",
                reason=str(r2),
                close=close_last,
                entry_raw=entry_raw,
                stop_raw=stop_raw,
                tp_raw=tp_raw,
                rr0=rr0,
                extra={"spread_bps": sp, "max_spread_bps": max_entry_spread},
            )
            _emit_runtime_signal_outcome(
                enabled=runtime_signal_log_enabled,
                tag=str(tag),
                symbol=str(symbol),
                tf_entry=str(tf_entry),
                bar_ts_ms=int(bar_ts_ms),
                regime=str(regime),
                direction=str(direction),
                close_last=float(close_last),
                final_action="hold",
                signal_action=str(action),
                signal_reason=str(reason),
            )
            continue

        # sizing (risk-based legacy baseline)
        max_pct = float(getattr(C, "MAX_POSITION_NOTIONAL_PCT", 0.10))  # 10% default
        size_ab_enabled = bool(size_min_bump_enabled or size_cap_ramp_enabled)
        if not size_ab_enabled:
            raw_qty = calc_qty_from_risk(equity_for_sizing, entry, stop_price)
            qty_legacy = float(raw_qty)

            # --- NOTIONAL CAP (important) ---
            max_notional = float(equity_for_sizing) * max_pct
            if entry > 0 and max_notional > 0:
                qty_notional_cap = max_notional / float(entry)
                qty_legacy = min(qty_legacy, qty_notional_cap)

            qty = float(qty_legacy)
            if sizing_mode_cfg == "LEGACY_COMPOUND":
                alpha = float(getattr(C, "LEGACY_COMPOUND_ALPHA", 0.5))
                mult_cap = float(getattr(C, "LEGACY_COMPOUND_MULT_CAP", 2.5))
                dd_th = float(getattr(C, "DD_DELEVER_THRESHOLD", 0.02))
                dd_min_mult = float(getattr(C, "DD_DELEVER_MIN_MULT", 0.25))
                eq0 = float(sizing_initial_equity) if float(sizing_initial_equity) > 0 else 1.0
                eq_now = float(equity_for_sizing)
                eq_for_scale = float(eq_now)
                if profit_only_enabled:
                    profit = max(0.0, float(eq_now) - float(eq0))
                    w_eff = float(profit_reinvest_w)
                    if profit_w_ramp_enabled:
                        profit_cap = float(eq0) * float(profit_w_ramp_pct)
                        if profit_cap > 0.0:
                            x = float(profit) / float(profit_cap)
                            if not math.isfinite(x):
                                x = 0.0
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
                final_mult = max(float(scale_pow) * float(risk_mult), 0.0)
                if math.isfinite(final_mult) and final_mult > 0.0:
                    qty = float(qty_legacy) * float(final_mult)
                elif final_mult == 0.0:
                    qty = 0.0
                _sync_replay_sizing_state()

            # --- Exchange min rules & precision ---
            try:
                cons = _get_min_order_constraints(ex, symbol)
                min_amt = cons.get("min_qty")
                min_cost = cons.get("min_cost")
                min_source = str(cons.get("source") or "none")
                qty_raw = float(qty)
                price_ref = float(entry)
                cost_est = float(qty_raw) * float(price_ref)
                # If min_cost is not met, skip the order; increasing qty is only a future policy option.
                if min_cost and cost_est < float(min_cost):
                    _summ_reason(summ_hold, f"min_cost_not_met(cost={cost_est:.6f} < min_cost={min_cost})")
                    _log_order_skip_min_once(
                        symbol=str(symbol),
                        ts_ms=int(candle_ts_run),
                        stage="MIN_COST_RAW",
                        qty=float(qty_raw),
                        cost=float(cost_est),
                        min_qty=(float(min_amt) if min_amt else None),
                        min_cost=float(min_cost),
                        price=float(price_ref),
                        source=str(min_source),
                    )
                    _diff_trace_buy_reject(
                        ts_ms=int(candle_ts_run),
                        symbol=str(symbol),
                        tf_entry=str(tf_entry),
                        tf_filter=str(tf_filter),
                        regime=str(regime),
                        direction=str(direction),
                        stage="MIN_COST",
                        reason=f"min_cost_not_met(cost={cost_est:.6f} < min_cost={min_cost})",
                        close=close_last,
                        entry_raw=entry_raw,
                        stop_raw=stop_raw,
                        tp_raw=tp_raw,
                        rr0=rr0,
                        qty=float(qty_raw),
                    )
                    _emit_runtime_signal_outcome(
                        enabled=runtime_signal_log_enabled,
                        tag=str(tag),
                        symbol=str(symbol),
                        tf_entry=str(tf_entry),
                        bar_ts_ms=int(bar_ts_ms),
                        regime=str(regime),
                        direction=str(direction),
                        close_last=float(close_last),
                        final_action="hold",
                        signal_action=str(action),
                        signal_reason=str(reason),
                    )
                    continue
                if min_amt and float(qty_raw) < float(min_amt):
                    _summ_reason(summ_hold, f"min_amount_not_met(qty={qty_raw:.8f} < min_amt={min_amt})")
                    _log_order_skip_min_once(
                        symbol=str(symbol),
                        ts_ms=int(candle_ts_run),
                        stage="MIN_AMOUNT_RAW",
                        qty=float(qty_raw),
                        cost=float(cost_est),
                        min_qty=float(min_amt),
                        min_cost=(float(min_cost) if min_cost else None),
                        price=float(price_ref),
                        source=str(min_source),
                    )
                    _diff_trace_buy_reject(
                        ts_ms=int(candle_ts_run),
                        symbol=str(symbol),
                        tf_entry=str(tf_entry),
                        tf_filter=str(tf_filter),
                        regime=str(regime),
                        direction=str(direction),
                        stage="MIN_AMOUNT",
                        reason=f"min_amount_not_met(qty={qty_raw:.8f} < min_amt={min_amt})",
                        close=close_last,
                        entry_raw=entry_raw,
                        stop_raw=stop_raw,
                        tp_raw=tp_raw,
                        rr0=rr0,
                        qty=float(qty_raw),
                    )
                    _emit_runtime_signal_outcome(
                        enabled=runtime_signal_log_enabled,
                        tag=str(tag),
                        symbol=str(symbol),
                        tf_entry=str(tf_entry),
                        bar_ts_ms=int(bar_ts_ms),
                        regime=str(regime),
                        direction=str(direction),
                        close_last=float(close_last),
                        final_action="hold",
                        signal_action=str(action),
                        signal_reason=str(reason),
                    )
                    continue

                qty = float(ex.amount_to_precision(symbol, qty_raw))
            except Exception as e:
                _summ_reason(summ_hold, f"qty_rules_error:{e}")
                _diff_trace_buy_reject(
                    ts_ms=int(candle_ts_run),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    tf_filter=str(tf_filter),
                    regime=str(regime),
                    direction=str(direction),
                    stage="QTY_RULES",
                    reason=f"qty_rules_error:{e}",
                    close=close_last,
                    entry_raw=entry_raw,
                    stop_raw=stop_raw,
                    tp_raw=tp_raw,
                    rr0=rr0,
                    qty=qty,
                )
                _emit_runtime_signal_outcome(
                    enabled=runtime_signal_log_enabled,
                    tag=str(tag),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    bar_ts_ms=int(bar_ts_ms),
                    regime=str(regime),
                    direction=str(direction),
                    close_last=float(close_last),
                    final_action="hold",
                    signal_action=str(action),
                    signal_reason=str(reason),
                )
                continue

            if qty <= 0:
                _summ_reason(summ_hold, "qty_non_positive")
                _diff_trace_buy_reject(
                    ts_ms=int(candle_ts_run),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    tf_filter=str(tf_filter),
                    regime=str(regime),
                    direction=str(direction),
                    stage="QTY_NON_POSITIVE",
                    reason="qty_non_positive",
                    close=close_last,
                    entry_raw=entry_raw,
                    stop_raw=stop_raw,
                    tp_raw=tp_raw,
                    rr0=rr0,
                    qty=qty,
                )
                _emit_runtime_signal_outcome(
                    enabled=runtime_signal_log_enabled,
                    tag=str(tag),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    bar_ts_ms=int(bar_ts_ms),
                    regime=str(regime),
                    direction=str(direction),
                    close_last=float(close_last),
                    final_action="hold",
                    signal_action=str(action),
                    signal_reason=str(reason),
                )
                continue
        else:
            raw_qty = calc_qty_from_risk(equity_for_sizing, entry, stop_price)
            qty0_base = float(raw_qty)
            qty0 = float(qty0_base)
            qty1 = float(qty0)
            qty2 = float(qty1)
            qty3 = float(qty2)
            qty_cap = 0.0
            qty_req = 0.0
            cap_pct_base = float(max_pct)
            cap_pct_eff = float(cap_pct_base)
            risk_mult_cap = 1.0
            profit_ratio = 0.0
            min_bump_applied = False
            cap_ramp_applied = False

            eq0 = float(sizing_initial_equity) if float(sizing_initial_equity) > 0 else 1.0
            eq_now = float(equity_for_sizing)
            if eq0 > 0.0:
                profit_ratio = max(0.0, (float(eq_now) - float(eq0)) / float(eq0))

            if sizing_mode_cfg == "LEGACY_COMPOUND":
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
                            if not math.isfinite(x):
                                x = 0.0
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
                _sync_replay_sizing_state()

            cap_ramp_max_eff = _resolve_size_cap_ramp_max_pct(float(cap_pct_base), float(size_cap_ramp_max_pct))
            cap_pct_eff, cap_ramp_applied = _resolve_size_cap_pct_eff(
                cap_pct_base=float(cap_pct_base),
                cap_ramp_enabled=bool(size_cap_ramp_enabled),
                cap_ramp_k=float(size_cap_ramp_k),
                cap_ramp_max_pct=float(cap_ramp_max_eff),
                profit_ratio=float(profit_ratio),
                risk_mult_cap=float(risk_mult_cap),
            )
            max_notional = float(equity_for_sizing) * float(cap_pct_eff)
            if entry > 0 and max_notional > 0.0:
                qty_cap = float(max_notional) / float(entry)
            else:
                qty_cap = 0.0
            qty1 = min(float(qty0), float(qty_cap))
            qty2 = float(qty1)

            try:
                cons = _get_min_order_constraints(ex, symbol)
                min_amt = cons.get("min_qty")
                min_cost = cons.get("min_cost")
                min_source = str(cons.get("source") or "none")
            except Exception as e:
                _summ_reason(summ_hold, f"qty_rules_error:{e}")
                _diff_trace_buy_reject(
                    ts_ms=int(candle_ts_run),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    tf_filter=str(tf_filter),
                    regime=str(regime),
                    direction=str(direction),
                    stage="QTY_RULES",
                    reason=f"qty_rules_error:{e}",
                    close=close_last,
                    entry_raw=entry_raw,
                    stop_raw=stop_raw,
                    tp_raw=tp_raw,
                    rr0=rr0,
                    qty=float(qty2),
                )
                _emit_runtime_signal_outcome(
                    enabled=runtime_signal_log_enabled,
                    tag=str(tag),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    bar_ts_ms=int(bar_ts_ms),
                    regime=str(regime),
                    direction=str(direction),
                    close_last=float(close_last),
                    final_action="hold",
                    signal_action=str(action),
                    signal_reason=str(reason),
                )
                continue

            qty_raw = float(qty2)
            price_ref = float(entry)
            cost_est = float(qty_raw) * float(price_ref)
            if min_cost and cost_est < float(min_cost):
                _summ_reason(summ_hold, f"min_cost_not_met(cost={cost_est:.6f} < min_cost={min_cost})")
                _log_order_skip_min_once(
                    symbol=str(symbol),
                    ts_ms=int(candle_ts_run),
                    stage="MIN_COST_RAW",
                    qty=float(qty_raw),
                    cost=float(cost_est),
                    min_qty=(float(min_amt) if min_amt else None),
                    min_cost=float(min_cost),
                    price=float(price_ref),
                    source=str(min_source),
                )
                _diff_trace_buy_reject(
                    ts_ms=int(candle_ts_run),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    tf_filter=str(tf_filter),
                    regime=str(regime),
                    direction=str(direction),
                    stage="MIN_COST",
                    reason=f"min_cost_not_met(cost={cost_est:.6f} < min_cost={min_cost})",
                    close=close_last,
                    entry_raw=entry_raw,
                    stop_raw=stop_raw,
                    tp_raw=tp_raw,
                    rr0=rr0,
                    qty=float(qty_raw),
                )
                _emit_runtime_signal_outcome(
                    enabled=runtime_signal_log_enabled,
                    tag=str(tag),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    bar_ts_ms=int(bar_ts_ms),
                    regime=str(regime),
                    direction=str(direction),
                    close_last=float(close_last),
                    final_action="hold",
                    signal_action=str(action),
                    signal_reason=str(reason),
                )
                continue
            if min_amt and float(qty_raw) < float(min_amt):
                _summ_reason(summ_hold, f"min_amount_not_met(qty={qty_raw:.8f} < min_amt={min_amt})")
                _log_order_skip_min_once(
                    symbol=str(symbol),
                    ts_ms=int(candle_ts_run),
                    stage="MIN_AMOUNT_RAW",
                    qty=float(qty_raw),
                    cost=float(cost_est),
                    min_qty=float(min_amt),
                    min_cost=(float(min_cost) if min_cost else None),
                    price=float(price_ref),
                    source=str(min_source),
                )
                _diff_trace_buy_reject(
                    ts_ms=int(candle_ts_run),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    tf_filter=str(tf_filter),
                    regime=str(regime),
                    direction=str(direction),
                    stage="MIN_AMOUNT",
                    reason=f"min_amount_not_met(qty={qty_raw:.8f} < min_amt={min_amt})",
                    close=close_last,
                    entry_raw=entry_raw,
                    stop_raw=stop_raw,
                    tp_raw=tp_raw,
                    rr0=rr0,
                    qty=float(qty_raw),
                )
                _emit_runtime_signal_outcome(
                    enabled=runtime_signal_log_enabled,
                    tag=str(tag),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    bar_ts_ms=int(bar_ts_ms),
                    regime=str(regime),
                    direction=str(direction),
                    close_last=float(close_last),
                    final_action="hold",
                    signal_action=str(action),
                    signal_reason=str(reason),
                )
                continue

            qty_req_cost = (float(min_cost) / float(entry)) if (entry > 0 and min_cost) else 0.0
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
                    msg = (
                        f"min_bump_not_feasible(qty={qty2:.8f} req={qty_req:.8f} "
                        f"cap={qty_cap:.8f} bump={bump_amt:.8f} limit={bump_limit:.8f})"
                    )
                    _summ_reason(summ_hold, msg)
                    _diff_trace_buy_reject(
                        ts_ms=int(candle_ts_run),
                        symbol=str(symbol),
                        tf_entry=str(tf_entry),
                        tf_filter=str(tf_filter),
                        regime=str(regime),
                        direction=str(direction),
                        stage="MIN_BUMP",
                        reason=str(msg),
                        close=close_last,
                        entry_raw=entry_raw,
                        stop_raw=stop_raw,
                        tp_raw=tp_raw,
                        rr0=rr0,
                        qty=float(qty2),
                    )
                    _emit_runtime_signal_outcome(
                        enabled=runtime_signal_log_enabled,
                        tag=str(tag),
                        symbol=str(symbol),
                        tf_entry=str(tf_entry),
                        bar_ts_ms=int(bar_ts_ms),
                        regime=str(regime),
                        direction=str(direction),
                        close_last=float(close_last),
                        final_action="hold",
                        signal_action=str(action),
                        signal_reason=str(reason),
                    )
                    continue

            try:
                if min_bump_applied:
                    step = _infer_amount_step(ex, symbol)
                    qty3 = _ceil_to_step(float(qty2), float(step))
                    if qty3 > (float(qty_cap) + 1e-12):
                        msg = f"qty_over_cap_after_ceil_round(qty={qty3:.8f} cap={qty_cap:.8f})"
                        _summ_reason(summ_hold, msg)
                        _diff_trace_buy_reject(
                            ts_ms=int(candle_ts_run),
                            symbol=str(symbol),
                            tf_entry=str(tf_entry),
                            tf_filter=str(tf_filter),
                            regime=str(regime),
                            direction=str(direction),
                            stage="CAP_LIMIT",
                            reason=str(msg),
                            close=close_last,
                            entry_raw=entry_raw,
                            stop_raw=stop_raw,
                            tp_raw=tp_raw,
                            rr0=rr0,
                            qty=float(qty3),
                        )
                        _emit_runtime_signal_outcome(
                            enabled=runtime_signal_log_enabled,
                            tag=str(tag),
                            symbol=str(symbol),
                            tf_entry=str(tf_entry),
                            bar_ts_ms=int(bar_ts_ms),
                            regime=str(regime),
                            direction=str(direction),
                            close_last=float(close_last),
                            final_action="hold",
                            signal_action=str(action),
                            signal_reason=str(reason),
                        )
                        continue
                    qty = float(ex.amount_to_precision(symbol, qty3))
                else:
                    qty = float(ex.amount_to_precision(symbol, qty2))
            except Exception as e:
                _summ_reason(summ_hold, f"qty_rules_error:{e}")
                _diff_trace_buy_reject(
                    ts_ms=int(candle_ts_run),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    tf_filter=str(tf_filter),
                    regime=str(regime),
                    direction=str(direction),
                    stage="QTY_RULES",
                    reason=f"qty_rules_error:{e}",
                    close=close_last,
                    entry_raw=entry_raw,
                    stop_raw=stop_raw,
                    tp_raw=tp_raw,
                    rr0=rr0,
                    qty=float(qty2),
                )
                _emit_runtime_signal_outcome(
                    enabled=runtime_signal_log_enabled,
                    tag=str(tag),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    bar_ts_ms=int(bar_ts_ms),
                    regime=str(regime),
                    direction=str(direction),
                    close_last=float(close_last),
                    final_action="hold",
                    signal_action=str(action),
                    signal_reason=str(reason),
                )
                continue

            if min_cost and (qty * entry) < min_cost:
                _summ_reason(summ_hold, f"min_cost_not_met(cost={qty*entry:.6f} < min_cost={min_cost})")
                _diff_trace_buy_reject(
                    ts_ms=int(candle_ts_run),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    tf_filter=str(tf_filter),
                    regime=str(regime),
                    direction=str(direction),
                    stage="MIN_COST",
                    reason=f"min_cost_not_met(cost={qty*entry:.6f} < min_cost={min_cost})",
                    close=close_last,
                    entry_raw=entry_raw,
                    stop_raw=stop_raw,
                    tp_raw=tp_raw,
                    rr0=rr0,
                    qty=qty,
                )
                _emit_runtime_signal_outcome(
                    enabled=runtime_signal_log_enabled,
                    tag=str(tag),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    bar_ts_ms=int(bar_ts_ms),
                    regime=str(regime),
                    direction=str(direction),
                    close_last=float(close_last),
                    final_action="hold",
                    signal_action=str(action),
                    signal_reason=str(reason),
                )
                continue
            if min_amt and qty < min_amt:
                _summ_reason(summ_hold, f"min_amount_not_met(qty={qty:.8f} < min_amt={min_amt})")
                _diff_trace_buy_reject(
                    ts_ms=int(candle_ts_run),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    tf_filter=str(tf_filter),
                    regime=str(regime),
                    direction=str(direction),
                    stage="MIN_AMOUNT",
                    reason=f"min_amount_not_met(qty={qty:.8f} < min_amt={min_amt})",
                    close=close_last,
                    entry_raw=entry_raw,
                    stop_raw=stop_raw,
                    tp_raw=tp_raw,
                    rr0=rr0,
                    qty=qty,
                )
                _emit_runtime_signal_outcome(
                    enabled=runtime_signal_log_enabled,
                    tag=str(tag),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    bar_ts_ms=int(bar_ts_ms),
                    regime=str(regime),
                    direction=str(direction),
                    close_last=float(close_last),
                    final_action="hold",
                    signal_action=str(action),
                    signal_reason=str(reason),
                )
                continue

            if qty <= 0:
                _summ_reason(summ_hold, "qty_non_positive")
                _diff_trace_buy_reject(
                    ts_ms=int(candle_ts_run),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    tf_filter=str(tf_filter),
                    regime=str(regime),
                    direction=str(direction),
                    stage="QTY_NON_POSITIVE",
                    reason="qty_non_positive",
                    close=close_last,
                    entry_raw=entry_raw,
                    stop_raw=stop_raw,
                    tp_raw=tp_raw,
                    rr0=rr0,
                    qty=qty,
                )
                _emit_runtime_signal_outcome(
                    enabled=runtime_signal_log_enabled,
                    tag=str(tag),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    bar_ts_ms=int(bar_ts_ms),
                    regime=str(regime),
                    direction=str(direction),
                    close_last=float(close_last),
                    final_action="hold",
                    signal_action=str(action),
                    signal_reason=str(reason),
                )
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

        trade_meta = {
            "direction": str(direction),
            "rr0": rr0,
            "rr_adj": rr_adj,
            "tp_bps_raw": tp_bps_raw,
            "sl_bps_raw": sl_bps_raw,
            "entry_raw": entry_raw,
            "stop_raw": stop_raw,
            "tp_raw": tp_raw,
        }
        # range exit cooldown / blocks (evaluate as late as possible, right before entry execution)
        if str(regime).lower() == "range":
            ts_ms_now = int(e["timestamp"][-1])
            close_now = float(e["close"][-1])
            # (1) EMA9 cross-under exit cooldown (time-based, optional early clear on EMA9 reclaim)
            cd_bars = int(getattr(C, "RANGE_EMA9_EXIT_COOLDOWN_BARS", 0) or 0)
            if cd_bars > 0:
                blocked_cd, rr = _check_range_ema9_exit_cooldown(
                    store, symbol=symbol, now_ts_ms=ts_ms_now, close_now=close_now,
                    reclaim_buf_bps=float(getattr(C, "RANGE_EMA9_EXIT_COOLDOWN_RECLAIM_BUF_BPS", 0.0) or 0.0),
                    clear_on_reclaim_ema9=True,
                )
                if blocked_cd:
                    _summ_reason(summ_hold, rr or "range_cooldown_after_ema9_exit")
                    _diff_trace_buy_reject(
                        ts_ms=int(ts_ms_now),
                        symbol=str(symbol),
                        tf_entry=str(tf_entry),
                        tf_filter=str(tf_filter),
                        regime=str(regime),
                        direction=str(direction),
                        stage="RANGE_COOLDOWN",
                        reason=str(rr or "range_cooldown_after_ema9_exit"),
                        mode=str(tag),
                        trade_range=bool(getattr(C, "TRADE_RANGE", False)),
                        trade_trend=float(getattr(C, "TRADE_TREND", 0.0) or 0.0),
                        cfg=_cfg_snapshot(),
                        close=float(close_now),
                    )
                    _emit_runtime_signal_outcome(
                        enabled=runtime_signal_log_enabled,
                        tag=str(tag),
                        symbol=str(symbol),
                        tf_entry=str(tf_entry),
                        bar_ts_ms=int(bar_ts_ms),
                        regime=str(regime),
                        direction=str(direction),
                        close_last=float(close_last),
                        final_action="hold",
                        signal_action=str(action),
                        signal_reason=str(reason),
                    )
                    continue
            # (2) EMA21-break / early-loss block (until close reclaims EMA21)
            buf_bps = float(getattr(C, "RANGE_EMA21_BREAK_BUF_BPS", 0.0) or 0.0)
            blocked_b, rr2 = _check_range_ema21_break_block_state(
                store, symbol=symbol, now_ts_ms=ts_ms_now, close_now=close_now, reclaim_buf_bps=buf_bps
            )
            if blocked_b:
                _summ_reason(summ_hold, rr2 or "range_block_after_ema21_break")
                _diff_trace_buy_reject(
                    ts_ms=int(ts_ms_now),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    tf_filter=str(tf_filter),
                    regime=str(regime),
                    direction=str(direction),
                    stage="RANGE_EMA21_BLOCK",
                    reason=str(rr2 or "range_block_after_ema21_break"),
                    mode=str(tag),
                    trade_range=bool(getattr(C, "TRADE_RANGE", False)),
                    trade_trend=float(getattr(C, "TRADE_TREND", 0.0) or 0.0),
                    cfg=_cfg_snapshot(),
                    close=float(close_now),
                )
                _emit_runtime_signal_outcome(
                    enabled=runtime_signal_log_enabled,
                    tag=str(tag),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    bar_ts_ms=int(bar_ts_ms),
                    regime=str(regime),
                    direction=str(direction),
                    close_last=float(close_last),
                    final_action="hold",
                    signal_action=str(action),
                    signal_reason=str(reason),
                )
                continue
        # ENTRY execute
        filled = 0.0
        avg_px = None
        open_reason = "ENTRY_UNKNOWN"

        if mode == "PAPER" or dryrun:
            ttl = float(getattr(C, "ENTRY_MAKER_TTL_SEC", 6))
            maker_ttl_sec = getattr(C, "MAKER_TTL_SEC", None)
            if maker_ttl_sec is not None:
                ttl = float(maker_ttl_sec)
            maker = _maker_ttl_order(ex, symbol, "buy", float(qty), ttl, dryrun=True)
            filled = float(maker.get("filled_qty") or 0.0)
            avg_px = maker.get("avg_price", None)
            open_reason = "PAPER_OPEN"
        else:
            maker_blocked, maker_block_reason = _is_maker_blocked(store, "ENTRY", symbol)
            if maker_blocked:
                rr = f"ENTRY maker blocked: {maker_block_reason}"
                _summ_reason(summ_hold, rr)
                _diff_trace_buy_reject(
                    ts_ms=int(candle_ts_run),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    tf_filter=str(tf_filter),
                    regime=str(regime),
                    direction=str(direction),
                    mode=str(tag),
                    trade_range=bool(getattr(C, "TRADE_RANGE", False)),
                    trade_trend=float(getattr(C, "TRADE_TREND", 0.0) or 0.0),
                    cfg=_cfg_snapshot(),
                    stage="MAKER_BLOCK",
                    reason=str(rr),
                    close=close_last,
                    entry_raw=entry_raw,
                    stop_raw=stop_raw,
                    tp_raw=tp_raw,
                    rr0=rr0,
                )
                _emit_runtime_signal_outcome(
                    enabled=runtime_signal_log_enabled,
                    tag=str(tag),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    bar_ts_ms=int(bar_ts_ms),
                    regime=str(regime),
                    direction=str(direction),
                    close_last=float(close_last),
                    final_action="hold",
                    signal_action=str(action),
                    signal_reason=str(reason),
                )
                continue

            ttl = float(getattr(C, "ENTRY_MAKER_TTL_SEC", 6))
            maker_ttl_sec = getattr(C, "MAKER_TTL_SEC", None)
            if maker_ttl_sec is not None:
                ttl = float(maker_ttl_sec)
            maker = _maker_ttl_order(ex, symbol, "buy", float(qty), ttl, dryrun=dryrun)
            filled = float(maker.get("filled_qty") or 0.0)
            avg_px = maker.get("avg_price", None)
            open_reason = str(maker.get("reason"))
            _log_maker_fill_csv(
                ts_ms=int(candle_ts_run),
                symbol=str(symbol),
                stage="ENTRY",
                side="buy",
                qty=float(qty),
                filled=float(filled),
                ttl_sec=float(ttl),
                reason=str(open_reason),
            )

            min_fill_ratio = float(getattr(C, "ENTRY_MIN_FILL_RATIO", 0.25))
            blocked_now, msg = _record_maker_result(store, "ENTRY", symbol, filled, qty, min_fill_ratio)
            if blocked_now:
                _summ_inc(summ_evt, "MAKER_BLOCKED_NOW")
                logger.warning(msg)

            fill_ratio = (filled / float(qty)) if float(qty) > 0 else 0.0
            if filled <= 0.0 or fill_ratio < min_fill_ratio:
                rr = f"Entry not filled (fill_ratio={fill_ratio:.2f}) {open_reason}"
                _summ_reason(summ_hold, rr)
                _emit_runtime_signal_outcome(
                    enabled=runtime_signal_log_enabled,
                    tag=str(tag),
                    symbol=str(symbol),
                    tf_entry=str(tf_entry),
                    bar_ts_ms=int(bar_ts_ms),
                    regime=str(regime),
                    direction=str(direction),
                    close_last=float(close_last),
                    final_action="hold",
                    signal_action=str(action),
                    signal_reason=str(reason),
                )
                continue

        entry_exec = float(avg_px) if (avg_px is not None) else float(entry)
        if dryrun and (avg_px is None):
            entry_slip_px = float(trace_bar_fields.get("bar_open", entry_exec))
            entry_exec = _trace_exec_with_slip(dryrun=True, kind="entry_long", px=float(entry_slip_px))
            avg_px = entry_exec
        trade_meta.update(
            _trail_diag_defaults(
                str(regime),
                init_stop=float(stop_price),
                entry_exec=float(entry_exec),
            )
        )

        phase_seq += 1
        _parity_probe_write(
            "OPEN_ATTEMPT",
            {
                "symbol": str(symbol),
                "replay_ts_ms": int(replay_ts_ms or candle_ts_run),
                "candle_ts_run": int(candle_ts_run),
                "ts_ms_now": int(ts_ms_now),
                "candle_ts_open_to_store": int(candle_ts_run),
                "last_candle_ts_current": int(store.get_last_candle_ts(tf_entry) or 0),
                "entry_exec": float(entry_exec),
                "entry_raw": float(entry_raw),
                "qty": float(filled),
                "stop": float(stop_price),
                "tp": float(tp_price),
                "phase_seq": int(phase_seq),
            },
        )

        store.open_position(
            symbol=symbol,
            entry=entry_exec,
            qty=float(filled),
            stop=float(stop_price),
            take_profit=float(tp_price),
            candle_ts_open=candle_ts_run,
            stop_kind="init",
        )
        _chart_state_note_marker(
            symbol,
            kind="entry",
            ts_ms=int(trace_event_ts_open_ms),
            price=float(entry_exec),
            side="long",
            label="OPEN",
        )
        # Match backtest: initialize a fresh trade from its executed entry, not prior extrema.
        _set_pos_max_fav(store, symbol, float(entry_exec))
        _set_pos_min_adv(store, symbol, float(entry_exec))
        _set_pos_meta(store, symbol, trade_meta)
        # Store the initial stop so BE and TRAIL logic can reference the original risk.
        store.set_kv(_pos_init_stop_key(symbol), str(float(stop_price)))
        _set_pos_tp1_done(store, symbol, False)
        store.set_kv(_pos_init_qty_key(symbol), str(float(filled)))
        _summ_inc(summ_evt, "OPEN")
        _emit_runtime_signal_outcome(
            enabled=runtime_signal_log_enabled,
            tag=str(tag),
            symbol=str(symbol),
            tf_entry=str(tf_entry),
            bar_ts_ms=int(bar_ts_ms),
            regime=str(regime),
            direction=str(direction),
            close_last=float(close_last),
            final_action="buy",
            signal_action=str(action),
            signal_reason=str(reason),
            entry=float(entry_exec),
            stop_price=float(stop_price),
            tp_price=float(tp_price),
        )

        _log_trade_event(f"[{tag}] {symbol} OPEN ... reason={open_reason}")

        # backtest OPEN trace uses one-bar-confirmed timestamp (t_confirmed), not current run ts.
        open_trace_ts_ms = int(trace_event_ts_open_ms)

        _diff_trace_write(
            {
                "event": "OPEN",
                "ts_ms": int(open_trace_ts_ms),
                "candle_ts_run": int(candle_ts_run),
                "ts_ms_now": int(ts_ms_now),
                "candle_ts_entry": int(open_trace_ts_ms),
                "signal_path": str(signal_path),
                "symbol": str(symbol),
                "tf_entry": str(tf_entry),
                "tf_filter": str(tf_filter),
                "mode": str(tag),
                "cfg": _cfg_snapshot(),
                "regime": str(regime),
                "direction": str(direction),
                "entry_raw": float(entry_raw),
                "entry_exec": float(entry_exec),
                "stop_raw": float(stop_raw),
                "tp_raw": float(tp_raw),
                "qty": float(filled),
                "rr0": float(rr0) if rr0 is not None else None,
                "rr_adj": float(rr_adj) if rr_adj is not None else None,
                **trace_bar_fields,
                # NOTE:
                # verify_diff.py matches OPEN with exact fields including open_reason.
                # backtest emits "" (empty) here, while live/replay can emit "PAPER_OPEN".
                # Normalize for diff-trace stability (no impact on trading logic).
                "open_reason": str(open_reason or "")
            }
        )

        _parity_probe_write(
            "LOOP_END_SUMMARY",
            {
                "symbol": str(symbol),
                "replay_ts_ms": int(replay_ts_ms or candle_ts_run),
                "candle_ts_run": int(candle_ts_run),
                "ts_ms_now": int(ts_ms_now),
                "last_candle_ts_final": int(store.get_last_candle_ts(tf_entry) or 0),
                "position_exists_final": bool(store.get_position(symbol)),
                "phase_seq": int(phase_seq),
            },
        )

    # --- run summary print (throttled) ---
    _maybe_log_run_summary(summ_regime=summ_regime, summ_evt=summ_evt, summ_hold=summ_hold)
    _maybe_log_ttl_stats()
    try:
        _emit_live_chart_states(
            store,
            symbols,
            run_mode=str(mode),
            timeframe=str(getattr(C, "ENTRY_TF", "5m") or "5m"),
        )
    except Exception:
        pass

    if skip_exports:
        return 0

    # --- export daily metrics (StateStore daily_metrics API) ---
    # Keep this block near the end of main(), just before the final return 0.
    day_key = datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d")

    # Assume data_available and daily_funnel may already exist.
    # If runner.py has not created them yet, initialize the minimal defaults here.
    #   data_available = True/False
    #   daily_funnel = Counter()

    # --- ensure daily export fields exist (avoid NameError) ---
    try:
        daily_funnel
    except NameError:
        daily_funnel = Counter()

    try:
        data_available
    except NameError:
        data_available = True

    if (not data_available) and (not daily_funnel):
        daily_funnel["stage0_data_unavailable"] = 1

    stored = store.get_daily_metrics(day_key) or {}
    if not isinstance(stored, dict):
        stored = {}

    risk = stored.get("risk")
    if not isinstance(risk, dict):
        risk = {}

    current_equity = float(equity_for_sizing or 0.0)
    if current_equity > 0.0 and float(risk.get("day_start_equity", 0.0) or 0.0) <= 0.0:
        risk["day_start_equity"] = float(current_equity)

    peak_equity = float(risk.get("day_peak_equity", 0.0) or 0.0)
    if current_equity > peak_equity:
        peak_equity = float(current_equity)
    if peak_equity <= 0.0 and current_equity > 0.0:
        peak_equity = float(current_equity)
    risk["day_peak_equity"] = float(peak_equity)

    day_dd_pct = 0.0
    if peak_equity > 0.0 and current_equity > 0.0:
        day_dd_pct = max(0.0, (peak_equity - current_equity) / peak_equity)
    risk["day_max_dd_pct"] = max(float(risk.get("day_max_dd_pct", 0.0) or 0.0), float(day_dd_pct))
    stored["risk"] = risk

    agg = _daily_agg_from_trades_csv(day_key)
    stored.update({
        "day": day_key,
        "ts": int(datetime.now().timestamp()),
        "data_available": bool(data_available),
        "pullback_funnel": dict(daily_funnel),
        "net": agg["net"],
        "trades": agg["trades"],
        "wins": agg["wins"],
        "losses": agg["losses"],
        "avg_win": agg["avg_win"],
        "avg_loss": agg["avg_loss"],
        "max_consec_loss": agg["max_consec_loss"],
    })
    payload = dict(stored)
    store.record_daily_metrics(day_key, payload)

    keep_days = int(getattr(C, "DAILY_METRICS_KEEP_DAYS", 35))
    store.prune_daily_metrics(keep_days)

    # Export the payload built in this run.
    # Re-reading from StateStore here can surface an older/minimal schema and
    # silently drop fields already computed in the current loop.
    # Keep DB persistence, but write the JSON from the in-memory payload.
    stored = payload
    os.makedirs(_current_export_dir(), exist_ok=True)
    path = _export_path(f"daily_metrics_{day_key}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(stored, f, ensure_ascii=False, indent=2)
    logger.info(f"[EXPORT] daily metrics -> {path}")
    logger.info(
        "[DAILY] day=%s trades=%s net=%.6f %s day_max_dd_pct=%.6f max_consec_loss=%s",
        day_key,
        int(stored.get("trades", 0) or 0),
        float(stored.get("net", 0.0) or 0.0),
        _quote_ccy_for_symbol(symbols[0] if symbols else ""),
        float(risk.get("day_max_dd_pct", 0.0) or 0.0),
        int(stored.get("max_consec_loss", 0) or 0),
    )


    # --- export run summary to json ---
    _write_runtime_run_summary(
        mode=str(mode),
        dryrun=bool(dryrun),
        symbols_count=len(symbols),
        summ_evt=summ_evt,
        summ_regime=summ_regime,
    )
    logger.info(f"[SIZING] equity_for_sizing={equity_for_sizing:.6f} max_pos_pct={getattr(C,'MAX_POSITION_NOTIONAL_PCT',None)}")
    return 0

# ---------------------------
# replay mode (pseudo live)
# ---------------------------

def _parse_ymd_to_ms_utc(s: str, end_of_day_exclusive: bool) -> int:
    """
    s: "YYYY-MM-DD"
    returns UTC epoch ms.
    - since : start of day (00:00:00Z)
    - until : start of next day (exclusive) when end_of_day_exclusive=True
    """
    s = str(s).strip()
    dt = datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if end_of_day_exclusive:
        dt = dt + timedelta(days=1)
    return int(dt.timestamp() * 1000)


def _resolve_replay_csv_dir(base: str, maybe_rel: str) -> str:
    """
    If maybe_rel is absolute -> return as-is.
    If relative -> join with base.
    """
    p = str(maybe_rel)
    if os.path.isabs(p):
        return p
    return os.path.join(str(base), p)


def _resolve_replay_dataset_root(replay_csv_base: str) -> str:
    base_txt = str(replay_csv_base or "").strip()
    if not base_txt:
        return os.path.abspath(str(getattr(ensure_runtime_dirs(), "market_data_dir", ".") or "."))
    base_abs = os.path.abspath(base_txt)
    if os.path.isfile(base_abs):
        return os.path.dirname(base_abs)
    return base_abs


def _unique_abs_paths(paths: list[Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw_path in list(paths or []):
        text = str(raw_path or "").strip()
        if not text:
            continue
        abs_path = os.path.abspath(text)
        key = os.path.normcase(abs_path)
        if key in seen:
            continue
        seen.add(key)
        out.append(abs_path)
    return out


def _runner_preflight_failure_prefix_line(
    *,
    context: str,
    runtime_symbol: str,
    entry_tf: str,
    filter_tf: str,
    from_ym: str,
    to_ym: str,
    missing_items: list[str],
    searched_paths: list[str],
    fallback_attempted: str,
) -> str:
    missing_text = ",".join([str(item) for item in list(missing_items or []) if str(item or "").strip()]) or "none"
    searched_text = " | ".join(_unique_abs_paths(searched_paths)) or "<none>"
    fallback_text = " ".join(str(fallback_attempted or "none").split()) or "none"
    return (
        "RUNTIME_PREFLIGHT_FAILURE: "
        f"context={str(context or '').strip()} "
        f"symbol={str(runtime_symbol or '').strip()} "
        f"tf_entry={str(entry_tf or '').strip()} "
        f"tf_filter={str(filter_tf or '').strip()} "
        f"from={str(from_ym)} "
        f"to={str(to_ym)} "
        f"missing_items={missing_text} "
        f"searched_paths={searched_text} "
        f"fallback_attempted={fallback_text}"
    )


def _emit_runner_preflight_failure_prefix(
    *,
    context: str,
    runtime_symbol: str,
    entry_tf: str,
    filter_tf: str,
    from_ym: str,
    to_ym: str,
    missing_items: list[str],
    searched_paths: list[str],
    fallback_attempted: str,
) -> None:
    print(
        _runner_preflight_failure_prefix_line(
            context=context,
            runtime_symbol=runtime_symbol,
            entry_tf=entry_tf,
            filter_tf=filter_tf,
            from_ym=from_ym,
            to_ym=to_ym,
            missing_items=missing_items,
            searched_paths=searched_paths,
            fallback_attempted=fallback_attempted,
        ),
        file=sys.stderr,
        flush=True,
    )


def _runner_preflight_failure_message(
    *,
    context: str,
    runtime_symbol: str,
    entry_tf: str,
    filter_tf: str,
    since_ms: int | None,
    until_ms: int | None,
    dataset_year: int | None,
    dataset_meta: dict[str, Any],
    dataset_diag: dict[str, Any],
    missing_items: list[str],
    searched_paths: list[str],
    fallback_attempted: str,
) -> str:
    from_ym, to_ym = resolve_prepare_month_window(
        since_ms=since_ms,
        until_ms=until_ms,
        year=dataset_year,
        years=None,
    )
    chart_missing = {str(item) for item in list(missing_items or []) if str(item).startswith("chart:")}
    tf_focus = str(entry_tf)
    searched_dir = str(dataset_meta.get("dir_5m") or "")
    searched_glob = str(dataset_meta.get("glob_5m") or "")
    if (f"chart:{str(filter_tf)}" in chart_missing) and (f"chart:{str(entry_tf)}" not in chart_missing):
        tf_focus = str(filter_tf)
        searched_dir = str(dataset_meta.get("dir_1h") or searched_dir)
        searched_glob = str(dataset_meta.get("glob_1h") or searched_glob)
    if chart_missing:
        base_msg = build_missing_dataset_message(
            context=str(context),
            tf=str(tf_focus),
            searched_dir=str(searched_dir),
            searched_glob=str(searched_glob),
            dataset_root=str(dataset_meta.get("root") or ""),
            prefix=str(dataset_meta.get("prefix") or runtime_symbol),
            year=(int(dataset_meta.get("year")) if dataset_meta.get("year") is not None else dataset_year),
            tf_dirs=("5m", "1h"),
            diagnostics=dict(dataset_diag or {}),
        )
    else:
        base_msg = (
            f"[{str(context)}] precomputed runtime data are missing. "
            f"dataset_root={str(dataset_meta.get('root') or '')} "
            f"prefix={str(dataset_meta.get('prefix') or runtime_symbol)}"
        )
    range_bits: list[str] = []
    if since_ms is not None:
        range_bits.append(f"since_ms={int(since_ms)}")
    if until_ms is not None:
        range_bits.append(f"until_ms={int(until_ms)}")
    range_bits.append(f"from={from_ym}")
    range_bits.append(f"to={to_ym}")
    missing_text = ",".join([str(item) for item in list(missing_items or []) if str(item or "").strip()]) or "none"
    searched_text = " | ".join(_unique_abs_paths(searched_paths))
    if not searched_text:
        searched_text = "<none>"
    return (
        f"{base_msg} symbol={str(runtime_symbol)} tf_entry={str(entry_tf)} tf_filter={str(filter_tf)} "
        f"{' '.join(range_bits)} missing_items={missing_text} "
        f"searched_paths={searched_text} fallback_attempted={str(fallback_attempted or 'none')}"
    )


def _runner_runtime_data_preflight(
    *,
    context: str,
    runtime_symbol: str,
    entry_tf: str,
    filter_tf: str,
    since_ms: int | None,
    until_ms: int | None,
    dataset_year: int | None,
    dataset_years: list[int] | None,
    dataset_root: str,
    prefix: str,
    default_dir_5m: str,
    default_glob_5m: str,
    default_dir_1h: str,
    default_glob_1h: str,
) -> None:
    symbol_text = str(runtime_symbol or "").strip()
    if not symbol_text:
        return
    context_text = str(context or "").strip().upper() or "RUNNER"
    years_list = sorted({int(y) for y in list(dataset_years or []) if int(y) > 0})
    from_ym, to_ym = resolve_prepare_month_window(
        since_ms=since_ms,
        until_ms=until_ms,
        year=dataset_year,
        years=years_list,
    )
    request_key = json.dumps(
        {
            "context": context_text,
            "symbol": str(symbol_text),
            "entry_tf": str(entry_tf),
            "filter_tf": str(filter_tf),
            "since_ms": int(since_ms) if since_ms is not None else None,
            "until_ms": int(until_ms) if until_ms is not None else None,
            "year": int(dataset_year) if dataset_year is not None else None,
            "years": list(years_list),
            "from_ym": str(from_ym),
            "to_ym": str(to_ym),
        },
        sort_keys=True,
    )
    if request_key in _RUNTIME_PREFLIGHT_COMPLETED:
        return

    dataset_root_abs = os.path.abspath(str(dataset_root or ".") or ".")
    base_meta: dict[str, Any] = {
        "dir_5m": str(default_dir_5m),
        "dir_1h": str(default_dir_1h),
        "glob_5m": str(default_glob_5m),
        "glob_1h": str(default_glob_1h),
        "root": str(dataset_root_abs),
        "prefix": str(prefix),
        "year": (int(dataset_year) if dataset_year is not None else None),
    }

    def _collect_missing_state() -> tuple[list[str], list[str], dict[str, Any], dict[str, Any]]:
        missing_items: list[str] = []
        searched_paths: list[str] = []
        dataset_diag: dict[str, Any] = {}
        dataset_meta = dict(base_meta)
        try:
            dataset_spec = resolve_dataset(
                dataset_root=str(dataset_root_abs),
                prefix=str(prefix),
                year=(int(dataset_year) if dataset_year is not None else None),
                years=(list(years_list) if years_list else None),
                tf_dirs=("5m", "1h"),
                default_dir_5m=str(default_dir_5m),
                default_glob_5m=str(default_glob_5m),
                default_dir_1h=str(default_dir_1h),
                default_glob_1h=str(default_glob_1h),
                runtime_symbol=str(symbol_text),
                context=str(context_text),
            )
            dataset_diag = dict(dataset_spec.diagnostics or {})
            dataset_meta.update(
                {
                    "dir_5m": str(dataset_spec.dir_5m),
                    "dir_1h": str(dataset_spec.dir_1h),
                    "glob_5m": str(dataset_diag.get("glob_5m", default_glob_5m)),
                    "glob_1h": str(dataset_diag.get("glob_1h", default_glob_1h)),
                    "root": str(dataset_spec.root),
                    "prefix": str(dataset_spec.prefix),
                    "year": int(dataset_spec.year),
                }
            )
            searched_paths.extend(list(dataset_diag.get("searched_paths_5m", []) or []))
            searched_paths.extend(list(dataset_diag.get("searched_paths_1h", []) or []))
            if not list(dataset_spec.paths_5m or []):
                missing_items.append(f"chart:{str(entry_tf)}")
            if str(filter_tf).lower() == "1h" and not list(dataset_spec.paths_1h or []):
                missing_items.append(f"chart:{str(filter_tf)}")
        except DatasetResolutionError as exc:
            dataset_diag = dict(exc.diagnostics or {})
            searched_paths.extend(list(dataset_diag.get("searched_paths_5m", []) or []))
            searched_paths.extend(list(dataset_diag.get("searched_paths_1h", []) or []))
            missing_items.append(f"chart:{str(entry_tf)}")
            if str(filter_tf).lower() == "1h":
                missing_items.append(f"chart:{str(filter_tf)}")

        try:
            pre_missing, pre_paths = BT._collect_missing_precomputed_labels(
                symbol=str(symbol_text),
                entry_tf=str(entry_tf),
                filter_tf=str(filter_tf),
                since_ms=since_ms,
                until_ms=until_ms,
                year=dataset_year,
                years=years_list,
            )
        except Exception as exc:
            logger.warning(
                "[PREP][%s] precomputed probe failed symbol=%s entry_tf=%s filter_tf=%s reason=%s",
                context_text,
                str(symbol_text),
                str(entry_tf),
                str(filter_tf),
                exc,
            )
            pre_missing, pre_paths = ([], [])
        missing_items.extend(list(pre_missing or []))
        searched_paths.extend(list(pre_paths or []))
        missing_items = sorted({str(item) for item in list(missing_items or []) if str(item or "").strip()})
        searched_paths = _unique_abs_paths(searched_paths)
        return (missing_items, searched_paths, dataset_diag, dataset_meta)

    logger.info(
        "[PREP][%s] start symbol=%s entry_tf=%s filter_tf=%s from=%s to=%s",
        context_text,
        str(symbol_text),
        str(entry_tf),
        str(filter_tf),
        str(from_ym),
        str(to_ym),
    )
    record_path_source_event_once(
        component="runner",
        event="runtime_preflight_start",
        source="runner_preflight",
        path=str(dataset_root_abs),
        extra={
            "build_id": BUILD_ID,
            "context": str(context_text),
            "symbol": str(symbol_text),
            "entry_tf": str(entry_tf),
            "filter_tf": str(filter_tf),
            "from_ym": str(from_ym),
            "to_ym": str(to_ym),
        },
    )

    missing_items, searched_paths, dataset_diag, dataset_meta = _collect_missing_state()
    if not missing_items:
        _RUNTIME_PREFLIGHT_COMPLETED.add(request_key)
        logger.info(
            "[PREP][%s] ok symbol=%s entry_tf=%s filter_tf=%s from=%s to=%s",
            context_text,
            str(symbol_text),
            str(entry_tf),
            str(filter_tf),
            str(from_ym),
            str(to_ym),
        )
        record_path_source_event_once(
            component="runner",
            event="runtime_preflight_resolved",
            source=str(dataset_diag.get("source", "runner_preflight")),
            path=str(dataset_meta.get("root") or dataset_root_abs),
            extra={
                "build_id": BUILD_ID,
                "context": str(context_text),
                "symbol": str(symbol_text),
                "entry_tf": str(entry_tf),
                "filter_tf": str(filter_tf),
                "from_ym": str(from_ym),
                "to_ym": str(to_ym),
                "fallback_attempted": "none",
            },
        )
        return

    missing_text = ",".join(list(missing_items))
    logger.warning(
        "[PREP][%s] missing symbol=%s entry_tf=%s filter_tf=%s from=%s to=%s missing=%s searched_paths=%s",
        context_text,
        str(symbol_text),
        str(entry_tf),
        str(filter_tf),
        str(from_ym),
        str(to_ym),
        str(missing_text),
        " | ".join(list(searched_paths or [])) or "<none>",
    )
    record_path_source_event_once(
        component="runner",
        event="runtime_preflight_missing",
        source=str(dataset_diag.get("source", "runner_preflight")),
        path=str(dataset_meta.get("root") or dataset_root_abs),
        extra={
            "build_id": BUILD_ID,
            "context": str(context_text),
            "symbol": str(symbol_text),
            "entry_tf": str(entry_tf),
            "filter_tf": str(filter_tf),
            "from_ym": str(from_ym),
            "to_ym": str(to_ym),
            "missing_items": list(missing_items),
            "searched_paths": list(searched_paths),
        },
    )
    if request_key in _AUTO_PREPARE_REQUESTS:
        _emit_runner_preflight_failure_prefix(
            context=str(context_text),
            runtime_symbol=str(symbol_text),
            entry_tf=str(entry_tf),
            filter_tf=str(filter_tf),
            from_ym=str(from_ym),
            to_ym=str(to_ym),
            missing_items=missing_items,
            searched_paths=searched_paths,
            fallback_attempted="run_pipeline_already_attempted",
        )
        raise RuntimeError(
            _runner_preflight_failure_message(
                context=str(context_text),
                runtime_symbol=str(symbol_text),
                entry_tf=str(entry_tf),
                filter_tf=str(filter_tf),
                since_ms=since_ms,
                until_ms=until_ms,
                dataset_year=dataset_year,
                dataset_meta=dataset_meta,
                dataset_diag=dataset_diag,
                missing_items=missing_items,
                searched_paths=searched_paths,
                fallback_attempted="run_pipeline_already_attempted",
            )
        )

    _AUTO_PREPARE_REQUESTS.add(request_key)
    logger.info(
        "[PREP][%s] auto_prepare symbol=%s entry_tf=%s filter_tf=%s from=%s to=%s missing=%s",
        context_text,
        str(symbol_text),
        str(entry_tf),
        str(filter_tf),
        str(from_ym),
        str(to_ym),
        str(missing_text),
    )
    record_path_source_event_once(
        component="runner",
        event="runtime_preflight_auto_prepare",
        source="run_pipeline",
        path=str(dataset_meta.get("root") or dataset_root_abs),
        extra={
            "build_id": BUILD_ID,
            "context": str(context_text),
            "symbol": str(symbol_text),
            "entry_tf": str(entry_tf),
            "filter_tf": str(filter_tf),
            "from_ym": str(from_ym),
            "to_ym": str(to_ym),
            "missing_items": list(missing_items),
        },
    )
    try:
        auto_prepare_runtime_data(
            symbol=str(symbol_text),
            context=str(context_text),
            since_ms=since_ms,
            until_ms=until_ms,
            year=(int(dataset_year) if dataset_year is not None else None),
            years=(list(years_list) if years_list else None),
            missing_items=list(missing_items),
            searched_paths=list(searched_paths),
            fallback_sources=["chart_download", "precompute_generation"],
        )
    except Exception as exc:
        _emit_runner_preflight_failure_prefix(
            context=str(context_text),
            runtime_symbol=str(symbol_text),
            entry_tf=str(entry_tf),
            filter_tf=str(filter_tf),
            from_ym=str(from_ym),
            to_ym=str(to_ym),
            missing_items=missing_items,
            searched_paths=searched_paths,
            fallback_attempted=f"run_pipeline error={exc}",
        )
        raise RuntimeError(
            _runner_preflight_failure_message(
                context=str(context_text),
                runtime_symbol=str(symbol_text),
                entry_tf=str(entry_tf),
                filter_tf=str(filter_tf),
                since_ms=since_ms,
                until_ms=until_ms,
                dataset_year=dataset_year,
                dataset_meta=dataset_meta,
                dataset_diag=dataset_diag,
                missing_items=missing_items,
                searched_paths=searched_paths,
                fallback_attempted=f"run_pipeline error={exc}",
            )
        ) from exc

    missing_after, searched_after, dataset_diag_after, dataset_meta_after = _collect_missing_state()
    if missing_after:
        _emit_runner_preflight_failure_prefix(
            context=str(context_text),
            runtime_symbol=str(symbol_text),
            entry_tf=str(entry_tf),
            filter_tf=str(filter_tf),
            from_ym=str(from_ym),
            to_ym=str(to_ym),
            missing_items=missing_after,
            searched_paths=searched_after,
            fallback_attempted="run_pipeline",
        )
        raise RuntimeError(
            _runner_preflight_failure_message(
                context=str(context_text),
                runtime_symbol=str(symbol_text),
                entry_tf=str(entry_tf),
                filter_tf=str(filter_tf),
                since_ms=since_ms,
                until_ms=until_ms,
                dataset_year=dataset_year,
                dataset_meta=dataset_meta_after,
                dataset_diag=dataset_diag_after,
                missing_items=missing_after,
                searched_paths=searched_after,
                fallback_attempted="run_pipeline",
            )
        )

    _RUNTIME_PREFLIGHT_COMPLETED.add(request_key)
    logger.info(
        "[PREP][%s] resolved symbol=%s entry_tf=%s filter_tf=%s from=%s to=%s fallback_attempted=run_pipeline",
        context_text,
        str(symbol_text),
        str(entry_tf),
        str(filter_tf),
        str(from_ym),
        str(to_ym),
    )
    record_path_source_event_once(
        component="runner",
        event="runtime_preflight_resolved",
        source=str(dataset_diag_after.get("source", "runner_preflight")),
        path=str(dataset_meta_after.get("root") or dataset_root_abs),
        extra={
            "build_id": BUILD_ID,
            "context": str(context_text),
            "symbol": str(symbol_text),
            "entry_tf": str(entry_tf),
            "filter_tf": str(filter_tf),
            "from_ym": str(from_ym),
            "to_ym": str(to_ym),
            "fallback_attempted": "run_pipeline",
        },
    )


def _live_like_preflight_window_ms(now_ms: int | None = None) -> tuple[int, int]:
    end_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    end_dt = datetime.fromtimestamp(int(end_ms) / 1000.0, tz=timezone.utc)
    start_dt = datetime(int(end_dt.year), int(end_dt.month), 1, tzinfo=timezone.utc)
    return (int(start_dt.timestamp() * 1000.0), int(end_ms))


class ReplayExchange:
    """
    ExchangeClient substitute for replay.
    - fetch_ohlcv() returns candles up to 'now_ts_ms' (no future leak)
    - fetch_order_book() is synthesized from last close + configured spread.
    """

    def __init__(self, ohlcv_map: dict[str, dict[str, list[list[float]]]]):
        # ohlcv_map[symbol][timeframe] = rows [[ts, o,h,l,c,v], ...] (ts ascending)
        self._ohlcv_map = ohlcv_map
        self.now_ts_ms: int | None = None
        self._exchange_id = _resolve_exchange_id()
        # Per-symbol precision override for replay.
        # If you define in config.py:
        #   REPLAY_AMOUNT_DECIMALS_MAP = {"ETH/USDT": 4, "BTC/USDT": 3, ...}
        # replay qty rounding will follow it (closer to LIVE/ccxt behavior).
        try:
            self._replay_amount_decimals_map = dict(getattr(C, "REPLAY_AMOUNT_DECIMALS_MAP", {}) or {})
        except Exception:
            self._replay_amount_decimals_map = {}
        try:
            self._replay_amount_decimals_default = int(getattr(C, "REPLAY_AMOUNT_DECIMALS", 5))
        except Exception:
            self._replay_amount_decimals_default = 5
        if str(self._exchange_id) == "coincheck":
            self._replay_amount_decimals_map.setdefault("BTC/JPY", 8)
            self._replay_amount_decimals_map.setdefault("BTCJPY", 8)
        # Backward/forward compat: some methods use _data/_now_ts_ms/_ob.
        self._data = self._ohlcv_map
        self._now_ts_ms = self.now_ts_ms
        self._ob = {}
        self._ob_ts = {}

        # PERF: 1y replay gets slower over time if we scan from the head on every fetch.
        # Keep a per-(symbol,tf) cursor that advances monotonically with now_ts_ms.
        self._cursor: dict[tuple[str, str], int] = {}

        # For fetch_order_book() synthetic mid.
        self._last_close: dict[str, float] = {}
        # --- fast index & retention ---
        self._ts = {}          # dict: symbol -> timeframe -> [ts...]
        self._start = {}       # dict: symbol -> timeframe -> start_idx (inclusive)
        self._keep_bars = int(getattr(C, "REPLAY_KEEP_BARS", 5000) or 5000)  # cap history window per tf

        for sym, tf_map in (self._data or {}).items():
            self._ts.setdefault(sym, {})
            self._start.setdefault(sym, {})
            for tf, rows in (tf_map or {}).items():
                ts_list = []
                for r in (rows or []):
                    try:
                        ts_list.append(int(r[0]))
                    except Exception:
                        # keep alignment; fallback to 0 so bisect stays safe
                        ts_list.append(0)
                self._ts[sym][tf] = ts_list
                self._start[sym][tf] = 0

    def _tf_ms(self, tf: str) -> int:
        """Timeframe string to milliseconds (used by retention/index logic)."""
        try:
            return int(_timeframe_to_ms(str(tf)))
        except Exception:
            return 0

    def set_now(self, ts_ms: int) -> None:
        # Keep clock in sync (confirmed-bar cutoff depends on this).
        self.now_ts_ms = int(ts_ms)
        self._now_ts_ms = self.now_ts_ms
        # Optional retention: keep only last N bars (per symbol/tf) to avoid slowdown over long replays.
        kb = int(self._keep_bars or 0)
        if kb > 0 and self._data:
            for sym, tf_map in self._data.items():
                for tf, rows in (tf_map or {}).items():
                    ts_list = self._ts.get(sym, {}).get(tf, [])
                    if not ts_list:
                        continue
                    end_idx = self._last_closed_end_idx(sym, tf)
                    if end_idx <= 0:
                        continue
                    new_start = max(self._start.get(sym, {}).get(tf, 0), max(0, end_idx - kb))
                    if new_start > self._start[sym][tf]:
                        self._start[sym][tf] = new_start

                    # Physical trim to actually drop old data when window has advanced a lot.
                    # (This keeps memory stable; avoids huge lists lingering forever.)
                    if self._start[sym][tf] >= 20000:
                        cut = int(self._start[sym][tf])
                        del rows[:cut]
                        del ts_list[:cut]
                        self._start[sym][tf] = 0

    def _last_closed_end_idx(self, symbol: str, timeframe: str) -> int:
        """
        Return end index (exclusive) of last candle strictly before the *current* bar.
        We never include the bar that contains now_ts_ms (even if it's already closed at now).
        """
        if self._now_ts_ms is None:
            return 0
        ts_list = self._ts.get(symbol, {}).get(timeframe, [])
        if not ts_list:
            return 0

        tf_ms = int(self._tf_ms(timeframe))
        now_ts = int(self._now_ts_ms)

        # current bar open aligned to timeframe grid
        current_open = (now_ts // tf_ms) * tf_ms

        # strictly exclude the current bar -> allow up to previous bar open
        cutoff_ts = current_open - tf_ms

        return int(bisect_right(ts_list, cutoff_ts))

    def _tf_ms(self, tf: str) -> int:
        """Timeframe string -> milliseconds (replay internal helper)."""
        try:
            return int(_timeframe_to_ms(str(tf)))
        except Exception:
            return 0

    def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 200, since: int | None = None) -> list[list[float]]:
        tf_map = (self._data.get(symbol, {}) or {})
        rows = tf_map.get(timeframe, [])
        if not rows:
            return []

        # fast path: bisect on timestamp list
        if self._now_ts_ms is None:
            # no clock => just bounded tail within retention window
            s0 = int(self._start.get(symbol, {}).get(timeframe, 0))
            tail = rows[s0:]
            return tail[-int(limit):] if limit is not None else tail

        end = self._last_closed_end_idx(symbol, timeframe)
        if end <= 0:
            return []
        s0 = int(self._start.get(symbol, {}).get(timeframe, 0))
        end = max(s0, end)
        start = max(s0, end - int(limit))
        return rows[start:end]

    def fetch_order_book(self, symbol: str) -> dict:
        ob_rows = self._ob.get(symbol, [])
        if not ob_rows:
            return {"bids": [], "asks": []}
        now_ts = self._now_ts_ms
        if now_ts is None:
            last = ob_rows[-1]
            return {"bids": [[float(last[1]), float(last[2])]], "asks": [[float(last[3]), float(last[4])]]}

        # fast selection by bisect on orderbook timestamps (build local ts list once)
        try:
            ts_list = getattr(self, "_ob_ts", None)
            if ts_list is None:
                self._ob_ts = {}
                ts_list = self._ob_ts
            if symbol not in ts_list:
                ts_list[symbol] = [int(r[0]) for r in ob_rows]
            idx = bisect_right(ts_list[symbol], int(now_ts)) - 1
            if idx < 0:
                return {"bids": [], "asks": []}
            last = ob_rows[idx]
            return {"bids": [[float(last[1]), float(last[2])]], "asks": [[float(last[3]), float(last[4])]]}
        except Exception:
            # fallback: original linear scan
            last = None
            for r in ob_rows:
                if int(r[0]) <= int(now_ts):
                    last = r
                else:
                    break
            if last is None:
                return {"bids": [], "asks": []}
            return {"bids": [[float(last[1]), float(last[2])]], "asks": [[float(last[3]), float(last[4])]]}

    # --- ccxt-like helpers used by main() sizing path ---
    def market_amount_rules(self, symbol: str) -> tuple[float, float]:
        """Return (min_amount, min_cost). In replay we default to no constraints."""
        # If you want to enforce rules in replay, set these in config.py.
        min_amt = float(getattr(C, "REPLAY_MIN_AMOUNT", 0.0) or 0.0)
        min_cost = float(getattr(C, "REPLAY_MIN_COST", 0.0) or 0.0)
        if str(self._exchange_id) == "coincheck" and str(symbol or "").strip().upper() == "BTC/JPY":
            min_amt = max(float(min_amt), 0.001)
            min_cost = max(float(min_cost), 500.0)
        return (min_amt, min_cost)

    def amount_to_precision(self, symbol: str, amount: float) -> float:
        # floor to per-symbol decimals (if provided), otherwise fall back to default.
        # This is important for matching LIVE qty behavior (ccxt uses market precision/step).
        sym = str(symbol)
        dmap = getattr(self, "_replay_amount_decimals_map", {}) or {}
        dflt = getattr(self, "_replay_amount_decimals_default", int(getattr(C, "REPLAY_AMOUNT_DECIMALS", 5)))

        # Try a few common keys (some code may use "ETHUSDT" or "ETH/USDT")
        decimals = None
        if sym in dmap:
            decimals = dmap.get(sym)
        else:
            sym2 = sym.replace("/", "")
            if sym2 in dmap:
                decimals = dmap.get(sym2)
        try:
            decimals = int(decimals) if decimals is not None else int(dflt)
        except Exception:
            decimals = int(dflt) if isinstance(dflt, int) else 5

        if decimals <= 0:
            return str(int(float(amount)))
        factor = 10 ** decimals
        return str(math.floor(float(amount) * factor) / factor)

def _load_replay_ohlcv(
    replay_csv_base: str,
    symbols: list[str],
    tf_entry: str,
    tf_filter: str,
    since_ms: int,
    until_ms: int,
    requested_since_ms: int | None = None,
    requested_until_ms: int | None = None,
) -> dict[str, dict[str, list[list[float]]]]:
    """
    Load the same dataset used by backtest.py (CSV dir + glob).
    We reuse backtest.py internal readers to minimize divergence.
    """

    def _norm_ts_ms(ts: int) -> int:
        """
        Normalize timestamp to epoch milliseconds.
        - seconds (10-digit) -> ms
        - microseconds (16-digit-ish) -> ms
        """
        try:
            x = int(ts)
        except Exception:
            return int(ts)
        # seconds: < 1e12 (ms around 2001~33658 years)
        if x > 0 and x < 1_000_000_000_000:
            return x * 1000
        # microseconds: > 1e14 (ms is ~1e12-1e13 range for modern dates)
        if x > 100_000_000_000_000:
            return x // 1000
        return x

    def _normalize_rows(rows: list[list[float]]) -> list[list[float]]:
        if not rows:
            return rows
        out: list[list[float]] = []
        for r in rows:
            if not r:
                continue
            rr = list(r)
            rr[0] = float(_norm_ts_ms(int(rr[0])))
            out.append(rr)
        out.sort(key=lambda x: int(x[0]))
        return out

    def _years_in_requested_range(start_ms: int, end_ms_exclusive: int) -> list[int]:
        try:
            dt0 = datetime.fromtimestamp(int(start_ms) / 1000.0, tz=timezone.utc)
            dt1 = datetime.fromtimestamp(max(int(start_ms), int(end_ms_exclusive) - 1) / 1000.0, tz=timezone.utc)
        except Exception:
            return []
        return list(range(int(dt0.year), int(dt1.year) + 1))

    def _collect_multiyear_paths(root: str, prefix: str, tf: str, years: list[int]) -> tuple[list[str], list[int], list[str]]:
        root_path = Path(str(root or ".")).resolve()
        out_paths: list[str] = []
        found_years: list[int] = []
        searched_dirs: list[str] = []
        for year in years:
            dir_path = root_path / f"{prefix}_{tf}_{int(year):04d}"
            searched_dirs.append(str(dir_path))
            if not dir_path.is_dir():
                continue
            pattern = f"{prefix}-{tf}-{int(year):04d}-*.csv"
            matched = [str(p.resolve()) for p in sorted(dir_path.glob(pattern)) if p.is_file()]
            if not matched:
                continue
            found_years.append(int(year))
            out_paths.extend(matched)
        return (out_paths, found_years, searched_dirs)

    def _extract_year_token(text: str) -> int | None:
        matches = list(re.finditer(r"(?<!\d)((?:19|20)\d{2})(?!\d)", str(text or "")))
        if not matches:
            return None
        try:
            return int(matches[-1].group(1))
        except Exception:
            return None

    def _replace_last_year_token(text: str, source_year: int, target_year: int) -> str:
        raw = str(text or "")
        matches = list(re.finditer(fr"(?<!\d){int(source_year):04d}(?!\d)", raw))
        if not matches:
            return raw
        m = matches[-1]
        return f"{raw[:m.start()]}{int(target_year):04d}{raw[m.end():]}"

    dataset_root_default = _resolve_replay_dataset_root(replay_csv_base)

    def _resolve_default_dir(raw_dir: str) -> Path | None:
        text = str(raw_dir or "").strip()
        if not text:
            return None
        if os.path.isabs(text):
            return Path(text)
        return (Path(dataset_root_default) / text).resolve()

    def _prefer_requested_year_defaults(raw_dir: str, raw_glob: str, requested_year: int | None) -> tuple[str, str]:
        dir_out = str(raw_dir or "")
        glob_out = str(raw_glob or "")
        if requested_year is None:
            return (dir_out, glob_out)

        dir_year = _extract_year_token(dir_out)
        if dir_year is not None and int(dir_year) != int(requested_year):
            candidate_dir = _replace_last_year_token(dir_out, dir_year, int(requested_year))
            candidate_path = _resolve_default_dir(candidate_dir)
            if candidate_path is not None and candidate_path.is_dir():
                dir_out = candidate_dir

        glob_year = _extract_year_token(glob_out)
        if glob_year is not None and int(glob_year) != int(requested_year):
            candidate_glob = _replace_last_year_token(glob_out, glob_year, int(requested_year))
            dir_path = _resolve_default_dir(dir_out)
            if dir_path is not None and dir_path.is_dir():
                try:
                    has_match = any(p.is_file() for p in dir_path.glob(candidate_glob))
                except Exception:
                    has_match = False
                if has_match:
                    glob_out = candidate_glob

        return (dir_out, glob_out)

    global _REPLAY_DATASET_INFO

    runtime_symbol = str((symbols[0] if symbols else _default_symbol_for_runtime()) or "").strip()
    csv_symbol = str(runtime_symbol or "").strip()
    if not csv_symbol:
        csv_symbol = str(getattr(C, "BACKTEST_CSV_SYMBOL", _default_symbol_for_runtime()) or "").strip()
    csv_prefix = symbol_to_prefix(csv_symbol)

    dir_5m_default = str(getattr(C, "BACKTEST_CSV_DIR_5M", "binance_ethusdt_5m_2025"))
    glob_5m_default = str(getattr(C, "BACKTEST_CSV_GLOB_5M", f"{csv_prefix}-5m-*.csv"))
    dir_1h_default = str(getattr(C, "BACKTEST_CSV_DIR_1H", "binance_ethusdt_1h_2025"))
    glob_1h_default = str(getattr(C, "BACKTEST_CSV_GLOB_1H", f"{csv_prefix}-1h-*.csv"))

    dataset_year_ref_ms = int(requested_since_ms if requested_since_ms is not None else since_ms)
    dataset_year = infer_year_from_ms(dataset_year_ref_ms)
    dir_5m_default, glob_5m_default = _prefer_requested_year_defaults(dir_5m_default, glob_5m_default, dataset_year)
    dir_1h_default, glob_1h_default = _prefer_requested_year_defaults(dir_1h_default, glob_1h_default, dataset_year)
    _runner_runtime_data_preflight(
        context="REPLAY",
        runtime_symbol=str(runtime_symbol or csv_symbol),
        entry_tf=str(tf_entry),
        filter_tf=str(tf_filter),
        since_ms=(int(requested_since_ms) if requested_since_ms is not None else (int(since_ms) if since_ms is not None else None)),
        until_ms=(int(requested_until_ms) if requested_until_ms is not None else (int(until_ms) if until_ms is not None else None)),
        dataset_year=(int(dataset_year) if dataset_year is not None else None),
        dataset_years=None,
        dataset_root=str(dataset_root_default),
        prefix=str(csv_prefix),
        default_dir_5m=str(dir_5m_default),
        default_glob_5m=str(glob_5m_default),
        default_dir_1h=str(dir_1h_default),
        default_glob_1h=str(glob_1h_default),
    )

    try:
        dataset_spec = resolve_dataset(
            dataset_root=str(dataset_root_default),
            prefix=str(csv_prefix),
            year=dataset_year,
            tf_dirs=("5m", "1h"),
            default_dir_5m=dir_5m_default,
            default_glob_5m=glob_5m_default,
            default_dir_1h=dir_1h_default,
            default_glob_1h=glob_1h_default,
            runtime_symbol=str(runtime_symbol),
            context="REPLAY",
        )
    except DatasetResolutionError as e:
        logger.error(
            "[REPLAY][DATASET] resolve failed diagnostics=%s",
            json.dumps(e.diagnostics or {}, ensure_ascii=False, sort_keys=True),
        )
        raise RuntimeError(str(e)) from e

    dataset_diag = dict(dataset_spec.diagnostics or {})
    dir_5m = str(dataset_spec.dir_5m)
    dir_1h = str(dataset_spec.dir_1h)
    glob_5m = str(dataset_diag.get("glob_5m", glob_5m_default))
    glob_1h = str(dataset_diag.get("glob_1h", glob_1h_default))
    dataset_root_used = str(dataset_spec.root)
    dataset_prefix_used = str(dataset_spec.prefix)
    dataset_year_used = int(dataset_spec.year)
    source_kind = str(dataset_diag.get("source", "default"))
    requested_years = _years_in_requested_range(
        int(requested_since_ms if requested_since_ms is not None else since_ms),
        int(requested_until_ms if requested_until_ms is not None else until_ms),
    )
    paths_5m_override = list(dataset_spec.paths_5m)
    paths_1h_override = list(dataset_spec.paths_1h)
    found_years_5m: list[int] = [int(dataset_year_used)] if paths_5m_override else []
    found_years_1h: list[int] = [int(dataset_year_used)] if paths_1h_override else []
    searched_dirs_5m: list[str] = [str(dataset_spec.dir_5m)]
    searched_dirs_1h: list[str] = [str(dataset_spec.dir_1h)]
    if str(source_kind) != "single_csv" and len(requested_years) > 1:
        multi_paths_5m, found_years_5m, searched_dirs_5m = _collect_multiyear_paths(
            dataset_root_used, dataset_prefix_used, "5m", requested_years
        )
        multi_paths_1h, found_years_1h, searched_dirs_1h = _collect_multiyear_paths(
            dataset_root_used, dataset_prefix_used, "1h", requested_years
        )
        if multi_paths_5m:
            paths_5m_override = list(multi_paths_5m)
            dir_5m = str(Path(dataset_root_used).resolve())
            glob_5m = f"{dataset_prefix_used}-5m-YYYY-*.csv"
        if multi_paths_1h:
            paths_1h_override = list(multi_paths_1h)
            dir_1h = str(Path(dataset_root_used).resolve())
            glob_1h = f"{dataset_prefix_used}-1h-YYYY-*.csv"
    common_years = sorted(set(int(y) for y in found_years_5m).intersection(int(y) for y in found_years_1h))
    missing_years = [int(y) for y in requested_years if int(y) not in set(common_years)]
    dataset_diag["requested_years"] = list(requested_years)
    dataset_diag["found_years_5m"] = list(found_years_5m)
    dataset_diag["found_years_1h"] = list(found_years_1h)
    dataset_diag["missing_years"] = list(missing_years)
    dataset_diag["searched_paths_5m"] = list(searched_dirs_5m)
    dataset_diag["searched_paths_1h"] = list(searched_dirs_1h)
    dataset_diag["paths_5m"] = list(paths_5m_override)
    dataset_diag["paths_1h"] = list(paths_1h_override)
    logger.info(
        "[REPLAY][DATASET] symbol=%s source=%s root=%s prefix=%s year=%s dir_5m=%s glob_5m=%s dir_1h=%s glob_1h=%s paths_5m=%s paths_1h=%s years_requested=%s missing_years=%s",
        str(runtime_symbol or csv_symbol),
        str(dataset_diag.get("source", "default")),
        dataset_root_used,
        dataset_prefix_used,
        dataset_year_used,
        dir_5m,
        glob_5m,
        dir_1h,
        glob_1h,
        int(len(paths_5m_override)),
        int(len(paths_1h_override)),
        list(requested_years),
        list(missing_years),
    )

    logger.info(
        "[REPLAY][DATASET_DIAG] symbol=%s legacy_fallback_5m_allowed=%s legacy_fallback_5m_used=%s legacy_fallback_5m_reason=%s legacy_fallback_1h_allowed=%s legacy_fallback_1h_used=%s legacy_fallback_1h_reason=%s searched_paths_5m=%s searched_paths_1h=%s",
        str(runtime_symbol or csv_symbol),
        str(dataset_diag.get("legacy_fallback_5m_allowed", "")),
        str(dataset_diag.get("legacy_fallback_5m_used", "")),
        str(dataset_diag.get("legacy_fallback_5m_reason", "")),
        str(dataset_diag.get("legacy_fallback_1h_allowed", "")),
        str(dataset_diag.get("legacy_fallback_1h_used", "")),
        str(dataset_diag.get("legacy_fallback_1h_reason", "")),
        int(len(list(dataset_diag.get("searched_paths_5m", []) or []))),
        int(len(list(dataset_diag.get("searched_paths_1h", []) or []))),
    )

    _REPLAY_DATASET_INFO = {
        "symbol": str(runtime_symbol or csv_symbol),
        "root": str(dataset_root_used),
        "prefix": str(dataset_prefix_used),
        "year": int(dataset_year_used),
        "month": dataset_diag.get("month", None),
        "dir_5m": str(dir_5m),
        "dir_1h": str(dir_1h),
        "glob_5m": str(glob_5m),
        "glob_1h": str(glob_1h),
        "paths_5m": list(paths_5m_override),
        "paths_1h": list(paths_1h_override),
        "requested_years": list(requested_years),
        "found_years_5m": list(found_years_5m),
        "found_years_1h": list(found_years_1h),
        "missing_years": list(missing_years),
        "diagnostics": dataset_diag,
    }

    # Warmup lookback for filter indicators (EMA/ADX/etc).
    # Backtest usually has enough pre-roll; replay must emulate that.
    warmup_days = int(getattr(C, "REPLAY_WARMUP_DAYS", 7))
    if warmup_days < 0:
        warmup_days = 0
    since_load_ms = int(since_ms) - int(timedelta(days=warmup_days).total_seconds() * 1000)
    if since_load_ms < 0:
        since_load_ms = 0

    out: dict[str, dict[str, list[list[float]]]] = {}

    for sym in symbols:
        use_eth = (str(sym) == csv_symbol)
        if not use_eth:
            out[str(sym)] = {tf_entry: [], tf_filter: []}
            continue

        rows_e: list[list[float]] = []
        rows_f: list[list[float]] = []

        if str(tf_entry).lower() == "5m":
            try:
                rows_e = BT._read_binance_csv_dir(
                    csv_dir=dir_5m,
                    file_glob=glob_5m,
                    since_ms=int(since_load_ms),
                    paths_override=list(paths_5m_override),
                )
            except SystemExit as e:
                logger.error(
                    "[REPLAY][DATASET] entry load failed diagnostics=%s",
                    json.dumps(dataset_diag, ensure_ascii=False, sort_keys=True),
                )
                raise RuntimeError(
                    build_missing_dataset_message(
                        context="REPLAY_LOAD",
                        tf=str(tf_entry),
                        searched_dir=str(dir_5m),
                        searched_glob=str(glob_5m),
                        dataset_root=str(dataset_root_used),
                        prefix=str(dataset_prefix_used),
                        year=dataset_year_used,
                        tf_dirs=("5m", "1h"),
                        diagnostics=dataset_diag,
                    )
                ) from e
        if str(tf_filter).lower() == "1h":
            try:
                rows_f = BT._read_binance_csv_dir(
                    csv_dir=dir_1h,
                    file_glob=glob_1h,
                    since_ms=int(since_load_ms),
                    paths_override=list(paths_1h_override),
                )
            except SystemExit as e:
                logger.error(
                    "[REPLAY][DATASET] filter load failed diagnostics=%s",
                    json.dumps(dataset_diag, ensure_ascii=False, sort_keys=True),
                )
                raise RuntimeError(
                    build_missing_dataset_message(
                        context="REPLAY_LOAD",
                        tf=str(tf_filter),
                        searched_dir=str(dir_1h),
                        searched_glob=str(glob_1h),
                        dataset_root=str(dataset_root_used),
                        prefix=str(dataset_prefix_used),
                        year=dataset_year_used,
                        tf_dirs=("5m", "1h"),
                        diagnostics=dataset_diag,
                    )
                ) from e

        # fallback: resample filter from entry if missing
        if (not rows_f) and rows_e and str(tf_filter).lower() != str(tf_entry).lower():
            rows_f = BT._resample_ohlcv(rows_e, str(tf_filter))

        # normalize timestamp unit (sec/us -> ms) BEFORE slicing
        rows_e = _normalize_rows(rows_e)
        rows_f = _normalize_rows(rows_f)

        # apply until_ms (exclusive)
        rows_e = [r for r in rows_e if int(r[0]) < int(until_ms)]
        rows_f = [r for r in rows_f if int(r[0]) < int(until_ms)]

        out[str(sym)] = {str(tf_entry): rows_e, str(tf_filter): rows_f}

    return out


def _log_replay_precomputed_source(
    *,
    scope: str,
    symbol: str,
    tf: str,
    indicator: str,
    source: str,
    path: str,
    preferred_root: str = "",
) -> None:
    BT._record_precomputed_source_event(
        component="runner",
        mode="replay",
        build_id=BUILD_ID,
        scope=scope,
        symbol=symbol,
        tf=tf,
        indicator=indicator,
        source=source,
        path=path,
        preferred_root=preferred_root,
    )
    source_text = str(source or "").strip()
    if source_text in ("canonical", "canonical_market_data", "canonical_shared_root"):
        return
    path_text = os.path.abspath(str(path or "").strip())
    if not path_text:
        return
    key = (str(scope).upper(), str(symbol), str(tf), str(indicator), path_text)
    if key in _REPLAY_PRECOMPUTED_SOURCE_LOGGED:
        return
    logger.info(
        "[REPLAY][PRECOMPUTED][%s] symbol=%s tf=%s indicator=%s source=%s path=%s",
        str(scope).upper(),
        str(symbol),
        str(tf),
        str(indicator),
        source_text,
        path_text,
    )
    _REPLAY_PRECOMPUTED_SOURCE_LOGGED.add(key)


def _load_replay_filter_precomputed(
    *,
    ohlcv_map: dict[str, dict[str, list[list[float]]]],
    symbols: list[str],
    tf_filter: str,
    warmup_bars: int,
) -> dict[str, dict[str, dict[str, Any]]]:
    out: dict[str, dict[str, dict[str, Any]]] = {}
    try:
        pre_enabled = bool(getattr(C, "PRECOMPUTED_INDICATORS_ENABLED", False))
    except Exception:
        pre_enabled = False
    if (not pre_enabled) or (str(tf_filter).lower() != "1h"):
        return out

    pre_root = str(getattr(C, "PRECOMPUTED_INDICATORS_OUT_ROOT", "exports/precomputed_indicators"))
    preferred_precomputed_root = BT._preferred_precomputed_root(pre_root)
    pre_strict = bool(getattr(C, "PRECOMPUTED_INDICATORS_STRICT", True))
    adx_period = int(getattr(C, "ADX_PERIOD_FILTER", 14))
    ema_fast = int(getattr(C, "EMA_FAST", 20))
    ema_slow = int(getattr(C, "EMA_SLOW", 50))

    for sym in symbols:
        rows_f = (ohlcv_map.get(str(sym), {}) or {}).get(str(tf_filter), []) or []
        if not rows_f:
            continue
        ts_list = [int(r[0]) for r in rows_f if isinstance(r, (list, tuple)) and len(r) >= 5]
        close_list = [float(r[4]) for r in rows_f if isinstance(r, (list, tuple)) and len(r) >= 5]
        if (not ts_list) or (len(ts_list) != len(close_list)):
            continue
        try:
            adx_paths, adx_dir, adx_source = BT._resolve_precomputed_indicator_paths_with_source(
                pre_root,
                str(sym),
                str(tf_filter),
                f"ADX{adx_period}",
            )
            ema_fast_paths, ema_fast_dir, ema_fast_source = BT._resolve_precomputed_indicator_paths_with_source(
                pre_root,
                str(sym),
                str(tf_filter),
                f"EMA{ema_fast}",
            )
            ema_slow_paths, ema_slow_dir, ema_slow_source = BT._resolve_precomputed_indicator_paths_with_source(
                pre_root,
                str(sym),
                str(tf_filter),
                f"EMA{ema_slow}",
            )
            if (not adx_paths) or (not ema_fast_paths) or (not ema_slow_paths):
                continue
            _log_replay_precomputed_source(
                scope="filter",
                symbol=str(sym),
                tf=str(tf_filter),
                indicator=f"ADX{adx_period}",
                source=adx_source,
                path=adx_dir,
                preferred_root=preferred_precomputed_root,
            )
            _log_replay_precomputed_source(
                scope="filter",
                symbol=str(sym),
                tf=str(tf_filter),
                indicator=f"EMA{ema_fast}",
                source=ema_fast_source,
                path=ema_fast_dir,
                preferred_root=preferred_precomputed_root,
            )
            _log_replay_precomputed_source(
                scope="filter",
                symbol=str(sym),
                tf=str(tf_filter),
                indicator=f"EMA{ema_slow}",
                source=ema_slow_source,
                path=ema_slow_dir,
                preferred_root=preferred_precomputed_root,
            )
            adx_arr = BT._load_precomputed_indicator_series_fast(
                ts_list,
                adx_paths,
                name=f"REPLAY_FILTER_ADX{adx_period}",
                warmup_bars=int(warmup_bars),
                strict=pre_strict,
            )
            ema_fast_arr = BT._load_precomputed_indicator_series_fast(
                ts_list,
                ema_fast_paths,
                name=f"REPLAY_FILTER_EMA{ema_fast}",
                warmup_bars=int(warmup_bars),
                strict=pre_strict,
            )
            ema_slow_arr = BT._load_precomputed_indicator_series_fast(
                ts_list,
                ema_slow_paths,
                name=f"REPLAY_FILTER_EMA{ema_slow}",
                warmup_bars=int(warmup_bars),
                strict=pre_strict,
            )
        except Exception as exc:
            logger.warning("[REPLAY][PRECOMPUTED][FILTER] symbol=%s tf=%s reason=%s", str(sym), str(tf_filter), exc)
            continue
        out.setdefault(str(sym), {})[str(tf_filter)] = {
            "timestamp": list(ts_list),
            "close": list(close_list),
            "adx": adx_arr,
            "ema_fast": ema_fast_arr,
            "ema_slow": ema_slow_arr,
        }
        logger.info(
            "[REPLAY][PRECOMPUTED][FILTER] symbol=%s tf=%s indicators=ADX%s,EMA%s,EMA%s rows=%s",
            str(sym),
            str(tf_filter),
            int(adx_period),
            int(ema_fast),
            int(ema_slow),
            int(len(ts_list)),
        )
    return out


def _load_replay_entry_precomputed(
    *,
    ohlcv_map: dict[str, dict[str, list[list[float]]]],
    symbols: list[str],
    tf_entry: str,
    warmup_bars: int,
) -> dict[str, dict[str, dict[str, Any]]]:
    out: dict[str, dict[str, dict[str, Any]]] = {}
    try:
        pre_enabled = bool(getattr(C, "PRECOMPUTED_INDICATORS_ENABLED", False))
    except Exception:
        pre_enabled = False
    if not pre_enabled:
        return out

    pre_root = str(getattr(C, "PRECOMPUTED_INDICATORS_OUT_ROOT", "exports/precomputed_indicators"))
    preferred_precomputed_root = BT._preferred_precomputed_root(pre_root)
    pre_strict = bool(getattr(C, "PRECOMPUTED_INDICATORS_STRICT", True))
    rsi_period = int(getattr(C, "RSI_PERIOD", 14))
    atr_period = int(getattr(C, "ATR_PERIOD", 14))

    for sym in symbols:
        rows_e = (ohlcv_map.get(str(sym), {}) or {}).get(str(tf_entry), []) or []
        if not rows_e:
            continue
        ts_list = [int(r[0]) for r in rows_e if isinstance(r, (list, tuple)) and len(r) >= 5]
        open_list = [float(r[1]) for r in rows_e if isinstance(r, (list, tuple)) and len(r) >= 5]
        high_list = [float(r[2]) for r in rows_e if isinstance(r, (list, tuple)) and len(r) >= 5]
        low_list = [float(r[3]) for r in rows_e if isinstance(r, (list, tuple)) and len(r) >= 5]
        close_list = [float(r[4]) for r in rows_e if isinstance(r, (list, tuple)) and len(r) >= 5]
        if (
            (not ts_list)
            or (len(ts_list) != len(open_list))
            or (len(ts_list) != len(high_list))
            or (len(ts_list) != len(low_list))
            or (len(ts_list) != len(close_list))
        ):
            continue
        try:
            ema9_paths, ema9_dir, ema9_source = BT._resolve_precomputed_indicator_paths_with_source(
                pre_root,
                str(sym),
                str(tf_entry),
                "EMA9",
            )
            ema21_paths, ema21_dir, ema21_source = BT._resolve_precomputed_indicator_paths_with_source(
                pre_root,
                str(sym),
                str(tf_entry),
                "EMA21",
            )
            rsi_paths, rsi_dir, rsi_source = BT._resolve_precomputed_indicator_paths_with_source(
                pre_root,
                str(sym),
                str(tf_entry),
                f"RSI{rsi_period}",
            )
            atr_paths, atr_dir, atr_source = BT._resolve_precomputed_indicator_paths_with_source(
                pre_root,
                str(sym),
                str(tf_entry),
                f"ATR{atr_period}",
            )
            if hasattr(BT, "_filter_month_files_by_ts") and ts_list:
                ts0 = int(ts_list[0])
                ts1 = int(ts_list[-1])
                ema9_paths = BT._filter_month_files_by_ts(ema9_paths, ts0, ts1)
                ema21_paths = BT._filter_month_files_by_ts(ema21_paths, ts0, ts1)
                rsi_paths = BT._filter_month_files_by_ts(rsi_paths, ts0, ts1)
                atr_paths = BT._filter_month_files_by_ts(atr_paths, ts0, ts1)
            if (not ema9_paths) or (not ema21_paths) or (not rsi_paths) or (not atr_paths):
                continue
            _log_replay_precomputed_source(
                scope="entry",
                symbol=str(sym),
                tf=str(tf_entry),
                indicator="EMA9",
                source=ema9_source,
                path=ema9_dir,
                preferred_root=preferred_precomputed_root,
            )
            _log_replay_precomputed_source(
                scope="entry",
                symbol=str(sym),
                tf=str(tf_entry),
                indicator="EMA21",
                source=ema21_source,
                path=ema21_dir,
                preferred_root=preferred_precomputed_root,
            )
            _log_replay_precomputed_source(
                scope="entry",
                symbol=str(sym),
                tf=str(tf_entry),
                indicator=f"RSI{rsi_period}",
                source=rsi_source,
                path=rsi_dir,
                preferred_root=preferred_precomputed_root,
            )
            _log_replay_precomputed_source(
                scope="entry",
                symbol=str(sym),
                tf=str(tf_entry),
                indicator=f"ATR{atr_period}",
                source=atr_source,
                path=atr_dir,
                preferred_root=preferred_precomputed_root,
            )
            ema9_arr = BT._load_precomputed_indicator_series_fast(
                ts_list,
                ema9_paths,
                name="REPLAY_ENTRY_EMA9",
                warmup_bars=int(warmup_bars),
                strict=pre_strict,
            )
            ema21_arr = BT._load_precomputed_indicator_series_fast(
                ts_list,
                ema21_paths,
                name="REPLAY_ENTRY_EMA21",
                warmup_bars=int(warmup_bars),
                strict=pre_strict,
            )
            rsi14_arr = BT._load_precomputed_indicator_series_fast(
                ts_list,
                rsi_paths,
                name=f"REPLAY_ENTRY_RSI{rsi_period}",
                warmup_bars=int(warmup_bars),
                strict=pre_strict,
            )
            atr14_arr = BT._load_precomputed_indicator_series_fast(
                ts_list,
                atr_paths,
                name=f"REPLAY_ENTRY_ATR{atr_period}",
                warmup_bars=int(warmup_bars),
                strict=pre_strict,
            )
        except Exception as exc:
            logger.warning("[REPLAY][PRECOMPUTED][ENTRY] symbol=%s tf=%s reason=%s", str(sym), str(tf_entry), exc)
            continue
        out.setdefault(str(sym), {})[str(tf_entry)] = {
            "timestamp": list(ts_list),
            "open": list(open_list),
            "high": list(high_list),
            "low": list(low_list),
            "close": list(close_list),
            "ema9": ema9_arr,
            "ema21": ema21_arr,
            "rsi14": rsi14_arr,
            "atr14": atr14_arr,
        }
        logger.info(
            "[REPLAY][PRECOMPUTED][ENTRY] symbol=%s tf=%s indicators=EMA9,EMA21,RSI%s,ATR%s rows=%s",
            str(sym),
            str(tf_entry),
            int(rsi_period),
            int(atr_period),
            int(len(ts_list)),
        )
    return out

def _ts_raw_and_ms_from_rows(rows: list[list[float]] | None) -> tuple[int | None, int | None, int | None, int | None, int]:
    def _norm_ts_ms(ts: int) -> int:
        x = int(ts)
        if 0 < x < 1_000_000_000_000:
            return x * 1000
        if x >= 100_000_000_000_000:
            return x // 1000
        return x

    if not rows:
        return (None, None, None, None, 0)
    first_raw = int(rows[0][0])
    last_raw = int(rows[-1][0])
    return (first_raw, _norm_ts_ms(first_raw), last_raw, _norm_ts_ms(last_raw), len(rows))


def _replay_period_label_from_ms(since_ms: int, until_ms: int) -> tuple[str, str]:
    since_label = datetime.fromtimestamp(int(since_ms) / 1000.0, tz=timezone.utc).strftime("%Y-%m")
    until_label = datetime.fromtimestamp(max(int(since_ms), int(until_ms) - 1) / 1000.0, tz=timezone.utc).strftime("%Y-%m")
    return (str(since_label), str(until_label))


def _sanitize_artifact_token(raw: str, *, fallback: str) -> str:
    txt = str(raw or "").strip()
    cleaned = "".join(ch if (ch.isalnum() or ch in ("-", "_")) else "_" for ch in txt)
    cleaned = cleaned.strip("_")
    return cleaned or str(fallback)


def _replay_report_filename(symbol: str, since_ms: int, until_ms: int, run_id: str) -> str:
    since_label, until_label = _replay_period_label_from_ms(int(since_ms), int(until_ms))
    since_token = _sanitize_artifact_token(since_label, fallback="since")
    until_token = _sanitize_artifact_token(until_label, fallback="until")
    run_token = _sanitize_artifact_token(run_id, fallback="run")
    return f"replay_report_{symbol_to_prefix(symbol)}_{since_token}_{until_token}_{run_token}.json"


def _replay_trade_log_filename(symbol: str, since_ms: int, until_ms: int, run_id: str) -> str:
    since_label, until_label = _replay_period_label_from_ms(int(since_ms), int(until_ms))
    since_token = _sanitize_artifact_token(since_label, fallback="since")
    until_token = _sanitize_artifact_token(until_label, fallback="until")
    run_token = _sanitize_artifact_token(run_id, fallback="run")
    return f"trade_replay_{symbol_to_prefix(symbol)}_{since_token}_{until_token}_{run_token}.log"


def _swap_replay_trade_log_handler(path: str) -> tuple[list[logging.Handler], logging.Handler]:
    fmt = "%(asctime)s %(levelname)s %(name)s %(message)s"
    path_abs = os.path.abspath(path)
    os.makedirs(os.path.dirname(path_abs), exist_ok=True)
    detached_handlers: list[logging.Handler] = []
    for handler in list(trade_logger.handlers):
        if getattr(handler, "baseFilename", None):
            trade_logger.removeHandler(handler)
            detached_handlers.append(handler)
    replay_handler = RotatingFileHandler(
        path_abs,
        mode="w",
        maxBytes=5 * 1024 * 1024,
        backupCount=10,
        encoding="utf-8",
    )
    replay_handler.setLevel(logging.INFO)
    replay_handler.setFormatter(logging.Formatter(fmt))
    trade_logger.addHandler(replay_handler)
    return (detached_handlers, replay_handler)


def _restore_replay_trade_log_handler(detached_handlers: list[logging.Handler], replay_handler: logging.Handler | None) -> None:
    if replay_handler is not None:
        try:
            trade_logger.removeHandler(replay_handler)
        except Exception:
            pass
        try:
            replay_handler.close()
        except Exception:
            pass
    for handler in detached_handlers:
        if handler not in trade_logger.handlers:
            trade_logger.addHandler(handler)


def _resolve_replay_initial_equity(args: argparse.Namespace) -> float:
    try:
        initial = float(getattr(args, "initial_equity", 0.0) or 0.0)
    except Exception:
        initial = 0.0
    if initial <= 0.0:
        try:
            initial = float(getattr(args, "initial", 0.0) or 0.0)
        except Exception:
            initial = 0.0
    if initial <= 0.0:
        try:
            initial = float(getattr(C, "BACKTEST_INITIAL_EQUITY", 0.0) or 0.0)
        except Exception:
            initial = 0.0
    if initial <= 0.0:
        try:
            initial = float(getattr(C, "INITIAL_EQUITY", 0.0) or 0.0)
        except Exception:
            initial = 0.0
    if initial <= 0.0:
        try:
            initial = float(getattr(C, "INITIAL", 0.0) or 0.0)
        except Exception:
            initial = 0.0
    if initial <= 0.0:
        initial = 300000.0
    return float(initial)


def _count_csv_dict_rows(path: str) -> int:
    if (not path) or (not os.path.exists(path)):
        return 0
    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            return sum(1 for _ in csv.DictReader(f))
    except Exception:
        return 0


def _write_trade_rows_csv(path: str, rows: list[dict[str, Any]]) -> None:
    cols_set: set[str] = set()
    cleaned_rows: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        cleaned = {
            str(k): v
            for k, v in row.items()
            if str(k or "") and (not str(k).startswith("_"))
        }
        cleaned_rows.append(cleaned)
        cols_set.update(cleaned.keys())
    cols = sorted(cols_set) if cols_set else ["ts", "ts_iso", "symbol", "reason", "net"]
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        for row in cleaned_rows:
            writer.writerow(row)


def _materialize_replay_report_inputs(
    rows: list[dict[str, Any]],
    *,
    initial_equity: float,
    since_ms: int,
) -> None:
    trades_path = _export_path("trades.csv")
    equity_path = _export_path("equity_curve.csv")
    min_ts_ms: int | None = None
    max_ts_ms: int | None = None
    clean_rows: list[dict[str, Any]] = []
    curve: list[tuple[int, float]] = []
    equity = float(initial_equity)
    if int(since_ms) > 0 and math.isfinite(float(initial_equity)):
        curve.append((int(since_ms), float(equity)))
    for row in rows:
        if not isinstance(row, dict):
            continue
        clean_rows.append(
            {
                str(k): v
                for k, v in row.items()
                if str(k or "") and (not str(k).startswith("_"))
            }
        )
        try:
            ts_ms = int(row.get("_ts_ms", 0) or 0)
        except Exception:
            ts_ms = 0
        if ts_ms > 0:
            min_ts_ms = ts_ms if min_ts_ms is None else min(min_ts_ms, ts_ms)
            max_ts_ms = ts_ms if max_ts_ms is None else max(max_ts_ms, ts_ms)
        try:
            net = float(row.get("_net", row.get("net", row.get("net_total", 0.0))) or 0.0)
        except Exception:
            net = 0.0
        equity += float(net)
        if ts_ms > 0:
            curve.append((int(ts_ms), float(equity)))
    existing_rows = _count_csv_dict_rows(trades_path)
    rewrite_trades = (not os.path.exists(trades_path)) or (int(existing_rows) != int(len(clean_rows)))
    if rewrite_trades:
        _write_trade_rows_csv(trades_path, clean_rows)
        logger.info(
            "[REPLAY][REPORT_INPUT] rewrote trades.csv path=%s existing_rows=%s filtered_rows=%s min_ts_ms=%s max_ts_ms=%s",
            trades_path,
            int(existing_rows),
            int(len(clean_rows)),
            min_ts_ms,
            max_ts_ms,
        )
    else:
        logger.info(
            "[REPLAY][REPORT_INPUT] trades.csv path=%s rows=%s min_ts_ms=%s max_ts_ms=%s",
            trades_path,
            int(len(clean_rows)),
            min_ts_ms,
            max_ts_ms,
        )
    if curve:
        BT.export_equity_curve(equity_path, curve)
        logger.info(
            "[REPLAY][REPORT_INPUT] wrote equity_curve.csv path=%s rows=%s initial_equity=%.6f",
            equity_path,
            int(len(curve)),
            float(initial_equity),
        )


def _load_trade_rows_since(path: str, *, start_row: int, symbol: str, since_ms: int, until_ms: int) -> list[dict[str, Any]]:
    def _norm_ts_ms(v: Any) -> int:
        try:
            x = int(float(v))
        except Exception:
            return 0
        if 0 < x < 1_000_000_000_000:
            x *= 1000
        if x >= 100_000_000_000_000:
            x //= 1000
        return int(x)

    def _safe_float(v: Any, default: float = 0.0) -> float:
        try:
            return float(v)
        except Exception:
            return float(default)

    out: list[dict[str, Any]] = []
    if (not path) or (not os.path.exists(path)):
        return out
    symbol_n = str(symbol or "").strip().upper()
    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for idx, row in enumerate(reader):
                if int(idx) < int(start_row):
                    continue
                ts_ms = _norm_ts_ms(
                    row.get("ts")
                    or row.get("ts_ms")
                    or row.get("candle_ts_exit")
                    or row.get("candle_ts_run")
                    or row.get("closed_ts")
                    or row.get("close_ts")
                    or row.get("opened_ts")
                    or row.get("open_ts")
                    or row.get("timestamp")
                    or 0
                )
                if ts_ms <= 0 or ts_ms < int(since_ms) or ts_ms >= int(until_ms):
                    continue
                row_symbol = str(row.get("symbol") or "").strip().upper()
                if row_symbol and row_symbol != symbol_n:
                    continue
                row_copy = dict(row)
                row_copy["_ts_ms"] = int(ts_ms)
                row_copy["_net"] = float(_safe_float(row.get("net", row.get("net_total")), 0.0))
                out.append(row_copy)
    except Exception:
        return []
    out.sort(key=lambda rr: int(rr.get("_ts_ms", 0)))
    return out


def _apply_replay_report_sanity(
    results: dict[str, Any],
    *,
    trade_log_filter: _ReplayTradeLogFilter | None,
    initial_equity: float,
) -> dict[str, Any]:
    out = dict(results or {})
    close_count = int(getattr(trade_log_filter, "close_total", 0) or 0)
    trades_now = int(out.get("trades", 0) or 0)
    if close_count > 0 and trades_now == 0:
        close_net_count = int(getattr(trade_log_filter, "close_net_count", 0) or 0)
        close_net_total = float(getattr(trade_log_filter, "close_net_total", 0.0) or 0.0)
        out["trades"] = int(close_count)
        if close_net_count > 0:
            out["net_total"] = float(close_net_total)
            if float(initial_equity) > 0.0:
                out["return_pct_of_init"] = float(close_net_total) / float(initial_equity) * 100.0
        logger.warning(
            "[REPORT_SANITY] close_count>0 but results.trades==0 -> fallback close_count=%s close_net_count=%s close_net_total=%.6f",
            int(close_count),
            int(close_net_count),
            float(close_net_total),
        )
    return out


def _stash_replay_report_overall(args: argparse.Namespace, results: dict[str, Any] | None) -> None:
    if not isinstance(results, dict):
        return
    try:
        setattr(args, "_replay_report_overall", dict(results))
    except Exception:
        pass


def _build_replay_results_from_trade_rows(rows: list[dict[str, Any]], *, initial_equity: float) -> dict[str, float | int]:
    def _safe_float(v: Any, default: float = 0.0) -> float:
        try:
            return float(v)
        except Exception:
            return float(default)

    def _percentile(values: list[float], q: float) -> float:
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

    equity = float(initial_equity)
    peak = float(initial_equity)
    peak_worst_bar = float(initial_equity)
    max_dd_worst_bar = 0.0
    returns: list[float] = []
    mae_abs_values: list[float] = []
    mae_bps_values: list[float] = []
    mfe_abs_values: list[float] = []
    mfe_bps_values: list[float] = []
    giveback_max_abs_values: list[float] = []
    giveback_max_bps_values: list[float] = []
    giveback_max_pct_values: list[float] = []
    giveback_to_close_abs_values: list[float] = []
    giveback_to_close_bps_values: list[float] = []
    giveback_to_close_pct_values: list[float] = []
    kept_bps_values: list[float] = []
    kept_pct_of_mfe_values: list[float] = []
    fav_adv_ratio_values: list[float] = []
    for row in rows:
        net = float(row.get("_net", 0.0) or 0.0)
        prev_equity = float(equity)
        equity += float(net)
        if peak < equity:
            peak = float(equity)
        if peak > 0.0:
            _ = max(0.0, (peak - equity) / peak)
        if prev_equity > 0.0:
            returns.append(float(net) / float(prev_equity))
        entry_price = max(0.0, _safe_float(row.get("entry_raw", row.get("entry_exec", 0.0)), 0.0))
        qty = max(0.0, abs(_safe_float(row.get("qty", 0.0), 0.0)))
        mae_bps_for_trade = float("nan")
        mfe_bps_for_trade = float("nan")
        giveback_to_close_bps_for_trade = float("nan")
        mae_abs = _safe_float(row.get("mae_abs"), float("nan"))
        if (not math.isfinite(mae_abs)) and entry_price > 0.0 and qty > 0.0:
            direction = str(row.get("direction", "long") or "long").strip().lower()
            if direction == "short":
                max_adv = _safe_float(row.get("max_fav", entry_price), entry_price)
                mae_abs = max(0.0, (max_adv - entry_price) * qty)
            else:
                min_adv = _safe_float(row.get("min_adv", entry_price), entry_price)
                mae_abs = max(0.0, (entry_price - min_adv) * qty)
        if math.isfinite(mae_abs):
            mae_abs = max(0.0, float(mae_abs))
            mae_bps = _safe_float(row.get("mae_bps"), float("nan"))
            if not math.isfinite(mae_bps):
                entry_notional = float(entry_price) * float(qty)
                mae_bps = (float(mae_abs) / float(entry_notional) * 10000.0) if entry_notional > 0.0 else 0.0
            mae_bps = max(0.0, float(mae_bps))
            mae_bps_for_trade = float(mae_bps)
            mae_abs_values.append(float(mae_abs))
            mae_bps_values.append(float(mae_bps))
        mae_abs_for_worst = max(0.0, float(mae_abs)) if math.isfinite(mae_abs) else 0.0
        if prev_equity > peak_worst_bar:
            peak_worst_bar = float(prev_equity)
        worst_mtm_equity_trade = float(prev_equity) - float(mae_abs_for_worst)
        if worst_mtm_equity_trade > peak_worst_bar:
            peak_worst_bar = float(worst_mtm_equity_trade)
        if peak_worst_bar > 0.0:
            dd_worst_trade = max(0.0, (float(peak_worst_bar) - float(worst_mtm_equity_trade)) / float(peak_worst_bar))
            if dd_worst_trade > max_dd_worst_bar:
                max_dd_worst_bar = float(dd_worst_trade)
        if equity > peak_worst_bar:
            peak_worst_bar = float(equity)
        mfe_abs = _safe_float(row.get("mfe_abs"), float("nan"))
        if (not math.isfinite(mfe_abs)) and entry_price > 0.0 and qty > 0.0:
            direction = str(row.get("direction", "long") or "long").strip().lower()
            if direction == "short":
                low_fav = _safe_float(row.get("min_adv", entry_price), entry_price)
                mfe_abs = max(0.0, (entry_price - low_fav) * qty)
            else:
                high_fav = _safe_float(row.get("max_fav", entry_price), entry_price)
                mfe_abs = max(0.0, (high_fav - entry_price) * qty)
        if math.isfinite(mfe_abs):
            mfe_abs = max(0.0, float(mfe_abs))
            mfe_bps = _safe_float(row.get("mfe_bps"), float("nan"))
            if not math.isfinite(mfe_bps):
                entry_notional = float(entry_price) * float(qty)
                mfe_bps = (float(mfe_abs) / float(entry_notional) * 10000.0) if entry_notional > 0.0 else 0.0
            mfe_bps = max(0.0, float(mfe_bps))
            mfe_bps_for_trade = float(mfe_bps)
            mfe_abs_values.append(float(mfe_abs))
            mfe_bps_values.append(float(mfe_bps))
        direction = str(row.get("direction", "long") or "long").strip().lower()
        entry_notional = float(entry_price) * float(qty)
        mfe_abs_for_ratio = float(mfe_abs) if math.isfinite(mfe_abs) else 0.0
        if direction == "short":
            best_price = _safe_float(row.get("min_adv"), float("nan"))
            if not math.isfinite(best_price):
                best_price = float(entry_price)
        else:
            best_price = _safe_float(row.get("max_fav"), float("nan"))
            if not math.isfinite(best_price):
                best_price = float(entry_price)
        giveback_max_abs = _safe_float(row.get("giveback_max_abs"), float("nan"))
        if (not math.isfinite(giveback_max_abs)) and entry_price > 0.0 and qty > 0.0:
            if direction == "short":
                if float(best_price) < float(entry_price):
                    high_retrace = _safe_float(row.get("max_fav", entry_price), entry_price)
                    giveback_max_abs = max(0.0, (high_retrace - float(best_price)) * float(qty))
                else:
                    giveback_max_abs = 0.0
            else:
                if float(best_price) > float(entry_price):
                    low_retrace = _safe_float(row.get("min_adv", entry_price), entry_price)
                    giveback_max_abs = max(0.0, (float(best_price) - low_retrace) * float(qty))
                else:
                    giveback_max_abs = 0.0
        if math.isfinite(giveback_max_abs):
            giveback_max_abs = max(0.0, float(giveback_max_abs))
            giveback_max_bps = _safe_float(row.get("giveback_max_bps"), float("nan"))
            if not math.isfinite(giveback_max_bps):
                giveback_max_bps = (float(giveback_max_abs) / float(entry_notional) * 10000.0) if float(entry_notional) > 0.0 else 0.0
            giveback_max_bps = max(0.0, float(giveback_max_bps))
            giveback_max_pct = _safe_float(row.get("giveback_max_pct_of_mfe"), float("nan"))
            if not math.isfinite(giveback_max_pct):
                giveback_max_pct = (float(giveback_max_abs) / float(max(float(mfe_abs_for_ratio), 1e-12))) if float(mfe_abs_for_ratio) > 0.0 else 0.0
            giveback_max_pct = max(0.0, float(giveback_max_pct))
            giveback_max_abs_values.append(float(giveback_max_abs))
            giveback_max_bps_values.append(float(giveback_max_bps))
            giveback_max_pct_values.append(float(giveback_max_pct))
        giveback_to_close_abs = _safe_float(row.get("giveback_to_close_abs"), float("nan"))
        if (not math.isfinite(giveback_to_close_abs)) and entry_price > 0.0 and qty > 0.0:
            exit_price = _safe_float(row.get("exit_raw", row.get("exit_exec", entry_price)), entry_price)
            if direction == "short":
                if float(best_price) < float(entry_price):
                    giveback_to_close_abs = max(0.0, (float(exit_price) - float(best_price)) * float(qty))
                else:
                    giveback_to_close_abs = 0.0
            else:
                if float(best_price) > float(entry_price):
                    giveback_to_close_abs = max(0.0, (float(best_price) - float(exit_price)) * float(qty))
                else:
                    giveback_to_close_abs = 0.0
        if math.isfinite(giveback_to_close_abs):
            giveback_to_close_abs = max(0.0, float(giveback_to_close_abs))
            giveback_to_close_bps = _safe_float(row.get("giveback_to_close_bps"), float("nan"))
            if not math.isfinite(giveback_to_close_bps):
                giveback_to_close_bps = (float(giveback_to_close_abs) / float(entry_notional) * 10000.0) if float(entry_notional) > 0.0 else 0.0
            giveback_to_close_bps = max(0.0, float(giveback_to_close_bps))
            giveback_to_close_bps_for_trade = float(giveback_to_close_bps)
            giveback_to_close_pct = _safe_float(row.get("giveback_to_close_pct_of_mfe"), float("nan"))
            if not math.isfinite(giveback_to_close_pct):
                giveback_to_close_pct = (float(giveback_to_close_abs) / float(max(float(mfe_abs_for_ratio), 1e-12))) if float(mfe_abs_for_ratio) > 0.0 else 0.0
            giveback_to_close_pct = max(0.0, float(giveback_to_close_pct))
            giveback_to_close_abs_values.append(float(giveback_to_close_abs))
            giveback_to_close_bps_values.append(float(giveback_to_close_bps))
            giveback_to_close_pct_values.append(float(giveback_to_close_pct))
        if math.isfinite(mfe_bps_for_trade) and math.isfinite(giveback_to_close_bps_for_trade):
            kept_bps = max(0.0, float(mfe_bps_for_trade) - float(giveback_to_close_bps_for_trade))
            kept_bps_values.append(float(kept_bps))
            kept_pct_of_mfe_values.append((float(kept_bps) / max(float(mfe_bps_for_trade), 1e-12)) if float(mfe_bps_for_trade) > 0.0 else 0.0)
        if math.isfinite(mfe_bps_for_trade) and math.isfinite(mae_bps_for_trade):
            fav_adv_ratio_values.append(float(mfe_bps_for_trade) / max(float(mae_bps_for_trade), 1e-12))
    max_dd = 0.0
    equity_walk = float(initial_equity)
    peak_walk = float(initial_equity)
    for row in rows:
        equity_walk += float(row.get("_net", 0.0) or 0.0)
        if peak_walk < equity_walk:
            peak_walk = float(equity_walk)
        if peak_walk > 0.0:
            max_dd = max(float(max_dd), max(0.0, (peak_walk - equity_walk) / peak_walk))
    mean_ret = (sum(returns) / len(returns)) if returns else 0.0
    std_ret = 0.0
    if len(returns) >= 2:
        var = sum((float(x) - float(mean_ret)) ** 2 for x in returns) / float(len(returns) - 1)
        std_ret = math.sqrt(max(0.0, float(var)))
    sharpe_like = 0.0
    if std_ret > 0.0:
        sharpe_like = float(mean_ret) / float(std_ret) * math.sqrt(float(len(returns)))
    net_total = float(equity) - float(initial_equity)
    max_dd_worst_bar = max(float(max_dd_worst_bar), float(max_dd))
    mae_max_abs = max([float(v) for v in mae_abs_values], default=0.0)
    mae_max_bps = max([float(v) for v in mae_bps_values], default=0.0)
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
    if len(rows) > 0:
        reasons: list[str] = []
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
    try:
        if len(rows) > 0 and float(mae_max_abs) <= 0.0 and float(mfe_max_abs) <= 0.0:
            sample = rows[0] if rows else {}
            logger.warning(
                "[REPORT_METRIC_DIAG] exchange_id=%s mode=%s symbol=%s sample_entry=%s sample_qty=%s sample_direction=%s sample_high=%s sample_low=%s",
                str(_resolve_exchange_id()),
                "REPLAY",
                str(sample.get("symbol", "")),
                str(sample.get("entry_raw", sample.get("entry_exec", sample.get("entry", "")))),
                str(sample.get("qty", "")),
                str(sample.get("direction", "")),
                str(sample.get("max_fav", sample.get("bar_high", ""))),
                str(sample.get("min_adv", sample.get("bar_low", ""))),
            )
    except Exception:
        pass
    return {
        "trades": int(len(rows)),
        "net_total": float(net_total),
        "return_pct_of_init": (float(net_total) / float(initial_equity) * 100.0) if float(initial_equity) > 0.0 else 0.0,
        "max_dd": float(max_dd),
        "max_dd_mtm": float(max_dd),
        "max_dd_worst_bar": float(max_dd_worst_bar),
        "mae_max_abs": float(mae_max_abs),
        "mae_max_bps": float(mae_max_bps),
        "mae_p50_abs": float(_percentile(mae_abs_values, 0.50)),
        "mae_p90_abs": float(_percentile(mae_abs_values, 0.90)),
        "mae_p99_abs": float(_percentile(mae_abs_values, 0.99)),
        "mae_p50_bps": float(_percentile(mae_bps_values, 0.50)),
        "mae_p90_bps": float(_percentile(mae_bps_values, 0.90)),
        "mae_p99_bps": float(_percentile(mae_bps_values, 0.99)),
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
        "sharpe_like": float(sharpe_like),
    }


def _write_replay_report(
    *,
    symbol: str,
    tf: str,
    since_ms: int,
    until_ms: int,
    run_id: str,
    bars: int,
    first_ts_ms: int | None,
    last_ts_ms: int | None,
    results: dict[str, Any],
) -> str:
    out_dir = _activate_export_context(run_id=str(run_id), symbol=str(symbol), mode="REPLAY")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, _replay_report_filename(str(symbol), int(since_ms), int(until_ms), str(run_id)))
    since_label, until_label = _replay_period_label_from_ms(int(since_ms), int(until_ms))
    payload = {
        "meta": {
            "symbol": str(symbol),
            "tf": str(tf),
            "since": str(since_label),
            "until": str(until_label),
            "since_ms": int(since_ms),
            "until_ms": int(until_ms),
            "bars": int(bars),
            "first_ts_ms": (int(first_ts_ms) if first_ts_ms is not None else None),
            "last_ts_ms": (int(last_ts_ms) if last_ts_ms is not None else None),
            "run_id": str(run_id),
            "export_dir": str(out_dir),
            "build_id": str(BUILD_ID),
        },
        "results": {
            "trades": int(results.get("trades", 0) or 0),
            "net_total": float(results.get("net_total", 0.0) or 0.0),
            "return_pct_of_init": float(results.get("return_pct_of_init", 0.0) or 0.0),
            "max_dd": float(results.get("max_dd", 0.0) or 0.0),
            "max_dd_mtm": float(results.get("max_dd_mtm", results.get("max_dd", 0.0)) or 0.0),
            "max_dd_worst_bar": float(results.get("max_dd_worst_bar", results.get("max_dd_mtm", results.get("max_dd", 0.0))) or 0.0),
            "mae_max_abs": float(results.get("mae_max_abs", 0.0) or 0.0),
            "mae_max_bps": float(results.get("mae_max_bps", 0.0) or 0.0),
            "mae_p50_abs": float(results.get("mae_p50_abs", 0.0) or 0.0),
            "mae_p90_abs": float(results.get("mae_p90_abs", 0.0) or 0.0),
            "mae_p99_abs": float(results.get("mae_p99_abs", 0.0) or 0.0),
            "mae_p50_bps": float(results.get("mae_p50_bps", 0.0) or 0.0),
            "mae_p90_bps": float(results.get("mae_p90_bps", 0.0) or 0.0),
            "mae_p99_bps": float(results.get("mae_p99_bps", 0.0) or 0.0),
            "mfe_max_abs": float(results.get("mfe_max_abs", 0.0) or 0.0),
            "mfe_max_bps": float(results.get("mfe_max_bps", 0.0) or 0.0),
            "mfe_p50_abs": float(results.get("mfe_p50_abs", 0.0) or 0.0),
            "mfe_p90_abs": float(results.get("mfe_p90_abs", 0.0) or 0.0),
            "mfe_p99_abs": float(results.get("mfe_p99_abs", 0.0) or 0.0),
            "mfe_p50_bps": float(results.get("mfe_p50_bps", 0.0) or 0.0),
            "mfe_p90_bps": float(results.get("mfe_p90_bps", 0.0) or 0.0),
            "mfe_p99_bps": float(results.get("mfe_p99_bps", 0.0) or 0.0),
            "giveback_max_abs": float(results.get("giveback_max_abs", 0.0) or 0.0),
            "giveback_max_bps": float(results.get("giveback_max_bps", 0.0) or 0.0),
            "giveback_max_pct_of_mfe": float(results.get("giveback_max_pct_of_mfe", 0.0) or 0.0),
            "giveback_max_p50_abs": float(results.get("giveback_max_p50_abs", 0.0) or 0.0),
            "giveback_max_p90_abs": float(results.get("giveback_max_p90_abs", 0.0) or 0.0),
            "giveback_max_p99_abs": float(results.get("giveback_max_p99_abs", 0.0) or 0.0),
            "giveback_max_p50_bps": float(results.get("giveback_max_p50_bps", 0.0) or 0.0),
            "giveback_max_p90_bps": float(results.get("giveback_max_p90_bps", 0.0) or 0.0),
            "giveback_max_p99_bps": float(results.get("giveback_max_p99_bps", 0.0) or 0.0),
            "giveback_max_p50_pct_of_mfe": float(results.get("giveback_max_p50_pct_of_mfe", 0.0) or 0.0),
            "giveback_max_p90_pct_of_mfe": float(results.get("giveback_max_p90_pct_of_mfe", 0.0) or 0.0),
            "giveback_max_p99_pct_of_mfe": float(results.get("giveback_max_p99_pct_of_mfe", 0.0) or 0.0),
            "giveback_to_close_abs": float(results.get("giveback_to_close_abs", 0.0) or 0.0),
            "giveback_to_close_bps": float(results.get("giveback_to_close_bps", 0.0) or 0.0),
            "giveback_to_close_pct_of_mfe": float(results.get("giveback_to_close_pct_of_mfe", 0.0) or 0.0),
            "giveback_to_close_p50_abs": float(results.get("giveback_to_close_p50_abs", 0.0) or 0.0),
            "giveback_to_close_p90_abs": float(results.get("giveback_to_close_p90_abs", 0.0) or 0.0),
            "giveback_to_close_p99_abs": float(results.get("giveback_to_close_p99_abs", 0.0) or 0.0),
            "giveback_to_close_p50_bps": float(results.get("giveback_to_close_p50_bps", 0.0) or 0.0),
            "giveback_to_close_p90_bps": float(results.get("giveback_to_close_p90_bps", 0.0) or 0.0),
            "giveback_to_close_p99_bps": float(results.get("giveback_to_close_p99_bps", 0.0) or 0.0),
            "giveback_to_close_p50_pct_of_mfe": float(results.get("giveback_to_close_p50_pct_of_mfe", 0.0) or 0.0),
            "giveback_to_close_p90_pct_of_mfe": float(results.get("giveback_to_close_p90_pct_of_mfe", 0.0) or 0.0),
            "giveback_to_close_p99_pct_of_mfe": float(results.get("giveback_to_close_p99_pct_of_mfe", 0.0) or 0.0),
            "kept_p50_bps": float(results.get("kept_p50_bps", 0.0) or 0.0),
            "kept_p90_bps": float(results.get("kept_p90_bps", 0.0) or 0.0),
            "kept_p99_bps": float(results.get("kept_p99_bps", 0.0) or 0.0),
            "kept_pct_of_mfe_p50": float(results.get("kept_pct_of_mfe_p50", 0.0) or 0.0),
            "kept_pct_of_mfe_p90": float(results.get("kept_pct_of_mfe_p90", 0.0) or 0.0),
            "kept_pct_of_mfe_p99": float(results.get("kept_pct_of_mfe_p99", 0.0) or 0.0),
            "fav_adv_ratio_p50": float(results.get("fav_adv_ratio_p50", 0.0) or 0.0),
            "fav_adv_ratio_p90": float(results.get("fav_adv_ratio_p90", 0.0) or 0.0),
            "fav_adv_ratio_p99": float(results.get("fav_adv_ratio_p99", 0.0) or 0.0),
            "exit_hint": str(results.get("exit_hint", "") or ""),
            "sharpe_like": float(results.get("sharpe_like", 0.0) or 0.0),
        },
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info(
        "[REPLAY][REPORT] path=%s trades=%s net_total=%.6f max_dd_worst_bar=%.6f mae_max_abs=%.6f mae_max_bps=%.2f mfe_max_abs=%.6f mfe_max_bps=%.2f giveback_max_abs=%.6f giveback_max_bps=%.2f giveback_to_close_abs=%.6f mae_p50_bps=%.2f mfe_p50_bps=%.2f giveback_to_close_p50_pct_of_mfe=%.4f kept_pct_of_mfe_p50=%.4f",
        out_path,
        int(payload["results"]["trades"]),
        float(payload["results"]["net_total"]),
        float(payload["results"]["max_dd_worst_bar"]),
        float(payload["results"]["mae_max_abs"]),
        float(payload["results"]["mae_max_bps"]),
        float(payload["results"]["mfe_max_abs"]),
        float(payload["results"]["mfe_max_bps"]),
        float(payload["results"]["giveback_max_abs"]),
        float(payload["results"]["giveback_max_bps"]),
        float(payload["results"]["giveback_to_close_abs"]),
        float(payload["results"]["mae_p50_bps"]),
        float(payload["results"]["mfe_p50_bps"]),
        float(payload["results"]["giveback_to_close_p50_pct_of_mfe"]),
        float(payload["results"]["kept_pct_of_mfe_p50"]),
    )
    return str(out_path)

def _parse_runner_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(add_help=True)
    p.add_argument("--run-id", type=str, default="", help="Optional run identifier. If empty, uses LWF_RUN_ID or auto-generated id.")
    p.add_argument("--preset", type=str, default="", help="Opt-in preset name (e.g. SELL_SAFE). CLI takes precedence over BOT_PRESET env.")
    p.add_argument("--log-level", type=str, default="", help="runtime log level override for LIVE/REPLAY: MINIMAL, OPS, DEBUG")
    p.add_argument("--report", action="store_true", help="Write opt-in report.json using run-scoped equity_curve.csv and trades.csv")
    p.add_argument("--report-out", type=str, default="", help="Report output path override")
    p.add_argument("--symbol", type=str, default="", help="Single symbol override. Accepts BTC/JPY or BTCJPY.")
    p.add_argument("--symbols", type=str, default="", help="Comma-separated symbol override. --symbol takes precedence.")
    p.add_argument("--replay", action="store_true", help="pseudo live replay mode (no external API, dry-run forced)")
    p.add_argument("--replay-csv", type=str, default="", help="base dir for backtest CSV dataset dirs (optional)")
    p.add_argument(
        "--replay-trade-log-every",
        type=int,
        default=None,
        help="replay-only: emit BACKTEST_DRYRUN OPEN/CLOSE logs every N events (1=all, 100=sample, 0=suppress). default: env BOT_REPLAY_TRADE_LOG_EVERY or 100",
    )
    p.add_argument(
        "--replay-diff-trace-every",
        type=int,
        default=None,
        help="replay-only: write diff_trace non-core events every N records (1=all). default: env BOT_REPLAY_DIFF_TRACE_EVERY or 1",
    )
    p.add_argument("--since", type=str, default="", help="YYYY-MM-DD (UTC) inclusive")
    p.add_argument("--since-ms", type=int, default=0, help="epoch ms (UTC, inclusive). If set (>0), overrides --since")
    p.add_argument("--until-ms", type=int, default=0, help="epoch ms (UTC, exclusive). If set (>0), overrides --until")
    p.add_argument("--history-since-ms", type=int, default=0, help="(replay-engine=backtest) history start epoch ms (UTC, inclusive) used for equity compounding. 0=auto (config SINCE_MS or since_ms-lookback)")
    p.add_argument("--initial", type=float, default=0.0, help="initial equity for replay-engine backtest (quote currency). If >0, overrides config BACKTEST_INITIAL_EQUITY/INITIAL_EQUITY")
    p.add_argument(
        "--replay-engine",
        type=str,
        default="backtest",
        choices=["live", "backtest"],
        help="replay engine: live=runner loop (default), backtest=call backtest.run_backtest and emit live-style diff_trace",
    )
    p.add_argument(
        "--initial-equity",
        type=float,
        default=0.0,
        help="override initial equity for replay/backtest engine. 0=use config (BACKTEST_INITIAL_EQUITY or INITIAL_EQUITY)",
    )
    p.add_argument(
        "--replay-export-csv",
        action="store_true",
        help="(replay-engine=backtest) also export equity_curve/trades CSV and copy to day-suffixed files",
    )
    p.add_argument("--serve", action="store_true", help="run continuously (default: run once and exit)")
    p.add_argument("--serve-sleep-sec", type=float, default=7.0, help="sleep seconds between serve loops")
    p.add_argument("--serve-max-loops", type=int, default=0, help="0=infinite, >0=stop after N loops")
    p.add_argument("--ttl-stats-interval-sec", type=float, default=None, help="override TTL_STATS log interval seconds")
    p.add_argument("--serve-log-every", type=int, default=0, help="0=off, >=1 logs [SERVE] heartbeat every N loops")
    p.add_argument("--until", type=str, default="", help="YYYY-MM-DD (UTC) inclusive day; internally treated as next-day exclusive")
    return p.parse_args(argv)




def _enforce_cli_live_license_gate(args: argparse.Namespace) -> None:
    if bool(getattr(args, "replay", False)):
        return
    startup_mode = (_resolve_mode_override_env() or str(getattr(C, "MODE", "PAPER"))).strip().upper()
    _reject_live_for_free_build(startup_mode)
    if startup_mode != "LIVE":
        return
    try:
        ensure_live_license_or_raise(feature_name="LIVE execution")
    except Exception as exc:
        raise SystemExit(str(exc) or "LIVE execution requires desktop activation.")

def _run_replay(args: argparse.Namespace) -> int:
    global _DIFF_TRACE_SOURCE, _DIFF_TRACE_SAMPLE_EVERY_REPLAY, _DIFF_TRACE_SAMPLE_COUNTER_REPLAY, _DIFF_TRACE_SAMPLE_SKIPPED_REPLAY, _REPLAY_SIZING_STATE
    _DIFF_TRACE_SOURCE = "replay"
    _REPLAY_SIZING_STATE = {}
    replay_t0 = time.perf_counter()
    replay_trade_detached_handlers: list[logging.Handler] = []
    replay_trade_handler: logging.Handler | None = None
    logging.basicConfig(
        level=getattr(logging, str(getattr(C, "LOG_LEVEL", "INFO")).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stdout,
        force=True,
    )
    _install_runtime_log_filter()
    logger.info("[REPLAY][TIMER] start_perf=%.6f", replay_t0)
    if _cfg_check_enabled():
        logger.info("CFG CHECK: file=%s MODE=%s TRADE_TREND=%s TRADE_RANGE=%s", getattr(C, "__file__", None), getattr(C, "MODE", None), getattr(C, "TRADE_TREND", None), getattr(C, "TRADE_RANGE", None))

    arg_trade_every = getattr(args, "replay_trade_log_every", None)
    trade_log_every = int(arg_trade_every) if arg_trade_every is not None else _env_int("BOT_REPLAY_TRADE_LOG_EVERY", 100)
    trade_log_every = max(0, int(trade_log_every))
    trade_log_filter = _set_replay_trade_log_filter(int(trade_log_every))
    logger.info("[REPLAY] trade_log_every=%s (BACKTEST_DRYRUN OPEN/CLOSE)", int(trade_log_every))

    arg_diff_every = getattr(args, "replay_diff_trace_every", None)
    diff_trace_every = int(arg_diff_every) if arg_diff_every is not None else _env_int("BOT_REPLAY_DIFF_TRACE_EVERY", 1)
    if diff_trace_every <= 0:
        diff_trace_every = 1
    _DIFF_TRACE_SAMPLE_EVERY_REPLAY = int(diff_trace_every)
    _DIFF_TRACE_SAMPLE_COUNTER_REPLAY = 0
    _DIFF_TRACE_SAMPLE_SKIPPED_REPLAY = 0
    logger.info("[REPLAY] diff_trace_every=%s (replay non-core events)", int(_DIFF_TRACE_SAMPLE_EVERY_REPLAY))

    def _log_replay_elapsed(*, bars: int | None = None) -> None:
        elapsed = float(time.perf_counter() - replay_t0)
        if bars is None:
            logger.info("[REPLAY][TIMER] elapsed_sec=%.3f", elapsed)
        else:
            logger.info("[REPLAY][TIMER] elapsed_sec=%.3f bars=%s", elapsed, int(bars))

    def _finalize_replay_logs(*, bars: int | None = None) -> None:
        nonlocal replay_trade_detached_handlers, replay_trade_handler
        try:
            _log_replay_elapsed(bars=bars)
            logger.info(
                "[REPLAY] trade_log_summary total=%s emitted=%s open=%s close=%s every_n=%s",
                int(getattr(trade_log_filter, "total", 0)),
                int(getattr(trade_log_filter, "emitted", 0)),
                int(getattr(trade_log_filter, "open_total", 0)),
                int(getattr(trade_log_filter, "close_total", 0)),
                int(trade_log_every),
            )
            logger.info(
                "[REPLAY] diff_trace_sample_summary every_n=%s sampled=%s skipped=%s",
                int(_DIFF_TRACE_SAMPLE_EVERY_REPLAY),
                int(_DIFF_TRACE_SAMPLE_COUNTER_REPLAY),
                int(_DIFF_TRACE_SAMPLE_SKIPPED_REPLAY),
            )
        finally:
            _clear_replay_trade_log_filter()
            _restore_replay_trade_log_handler(replay_trade_detached_handlers, replay_trade_handler)
            replay_trade_detached_handlers = []
            replay_trade_handler = None

    # force BACKTEST-like dryrun semantics for replay parity verification
    mode = "BACKTEST"
    dryrun = True

    symbols, symbol_source = _resolve_replay_symbols(args)
    if not symbols:
        logger.error("[REPLAY] no symbols")
        _finalize_replay_logs()
        return 0
    logger.info("[REPLAY][SYMBOL] symbol=%s source=%s symbols=%s", str(symbols[0]), str(symbol_source), list(symbols))

    tf_entry = str(getattr(C, "ENTRY_TF", getattr(C, "TIMEFRAME_ENTRY", "5m")))
    tf_filter = str(getattr(C, "FILTER_TF", getattr(C, "TIMEFRAME_FILTER", "1h")))

    # since/until (UTC)
    # Support both YYYY-MM-DD and epoch-ms ranges.
    if int(getattr(args, "since_ms", 0) or 0) > 0 and int(getattr(args, "until_ms", 0) or 0) > 0:
        since_ms = int(getattr(args, "since_ms"))
        until_ms = int(getattr(args, "until_ms"))
        since_src = f"since_ms={since_ms}"
        until_src = f"until_ms={until_ms}(exclusive)"
    else:
        if not args.since or not args.until:
            raise SystemExit("[REPLAY] --since and --until are required (YYYY-MM-DD) OR use --since-ms/--until-ms")
        since_ms = _parse_ymd_to_ms_utc(args.since, end_of_day_exclusive=False)
        until_ms = _parse_ymd_to_ms_utc(args.until, end_of_day_exclusive=True)
        since_src = f"since={args.since}(UTC)"
        until_src = f"until={args.until}(UTC day)->exclusive"
    if until_ms <= since_ms:
        raise SystemExit("[REPLAY] invalid range: until <= since")
    replay_symbol = str(symbols[0] if symbols else "symbol")
    replay_run_id = _resolve_export_run_id(str(getattr(args, "run_id", "") or ""))
    replay_export_dir = _activate_export_context(run_id=replay_run_id, symbol=replay_symbol, mode="REPLAY")
    replay_trade_log_path = os.path.join(
        ensure_runtime_dirs().logs_dir,
        _replay_trade_log_filename(replay_symbol, int(since_ms), int(until_ms), replay_run_id),
    )
    replay_trade_detached_handlers, replay_trade_handler = _swap_replay_trade_log_handler(replay_trade_log_path)
    logger.info("[REPLAY] trade_log_path=%s", replay_trade_log_path)
    logger.info("[results] export_dir=%s", replay_export_dir)
    replay_initial_equity = _resolve_replay_initial_equity(args)
    replay_report_path = _export_path(_replay_report_filename(replay_symbol, int(since_ms), int(until_ms), replay_run_id))
    replay_trades_path = _export_path("trades.csv")
    replay_trade_rows_start = _count_csv_dict_rows(replay_trades_path)
    logger.info(
        "[REPLAY] run_id=%s report_target=%s trade_rows_start=%s initial_equity=%.6f",
        replay_run_id,
        replay_report_path,
        int(replay_trade_rows_start),
        float(replay_initial_equity),
    )

    # Ensure replay (runner-engine / live-like) always emits diff-trace.
    # Otherwise, a config default (DIFF_TRACE_ENABLED=False) can result in
    # "no jsonl generated" even though replay is running.
    _prev_diff_trace_enabled = getattr(C, "DIFF_TRACE_ENABLED", None)
    try:
        setattr(C, "DIFF_TRACE_ENABLED", True)
    except Exception:
        _prev_diff_trace_enabled = None

    engine = str(getattr(args, "replay_engine", "runner") or "runner").strip().lower()
    replay_force_disable_be = bool(
        engine == "backtest"
        and bool(getattr(C, "BACKTEST_FORCE_DISABLE_BE", getattr(C, "BACKTEST_DISABLE_BE", False)))
    )
    _log_effective_range_config(
        logger,
        label=f"REPLAY:{engine.upper()}",
        force_disable_be=replay_force_disable_be,
        tp1_requires_flag=bool(engine != "backtest"),
    )
    if engine == "backtest":
        # Fastest parity path: run the backtest engine from runner.py and emit diff_trace with source=live.
        # Force DIFF_TRACE_ENABLED on here because backtest.py can suppress diff_trace output.

        logger.info("[REPLAY] ENGINE=backtest %s %s", since_src, until_src)

        _warmup_bars = int(getattr(C, "WARMUP_BARS", getattr(C, "BACKTEST_WARMUP_BARS", 300)) or 300)
        def _tf_ms(tf: str) -> int:
            tf = (tf or "").strip().lower()
            try:
                if tf.endswith("m"):
                    return int(tf[:-1]) * 60_000
                if tf.endswith("h"):
                    return int(tf[:-1]) * 3_600_000
                if tf.endswith("d"):
                    return int(tf[:-1]) * 86_400_000
            except Exception:
                return 0
            return 0

        # Replaying only the target day would starve warmup and filter windows.
        # Extend the backtest execution range into prior data so expected trades are preserved.
        # Filter diff_trace back to the requested range after the run.
        _lookback_ms = max(int(_warmup_bars) * _tf_ms(str(tf_entry)), 60 * _tf_ms(str(tf_filter)))
        since_ms_bt = max(0, int(since_ms) - int(_lookback_ms))
        logger.info("[REPLAY] backtest lookback: warmup_bars=%s lookback_ms=%s since_ms_bt=%s", _warmup_bars, _lookback_ms, since_ms_bt)
        _initial_equity = float(getattr(args, "initial", 0.0) or 0.0)
        if _initial_equity <= 0.0:
            # Align default with backtest.py CLI default:
            #   --initial default = getattr(C, "BACKTEST_INITIAL_EQUITY", 300000.0)
            _initial_equity = float(getattr(C, "BACKTEST_INITIAL_EQUITY", 300000.0) or 300000.0)

        _prev_diff_trace_enabled = getattr(C, "DIFF_TRACE_ENABLED", None)
        try:
            setattr(C, "DIFF_TRACE_ENABLED", True)
        except Exception:
            _prev_diff_trace_enabled = None

        # Backtest lookback can spill output into adjacent days.
        # Write to a temporary prefix first, then normalize into a single daily file.
        _tmp_prefix = "diff_trace_live_tmp"

        # ---- fee alignment (replay backtest-engine) ----
        # replay(backtest-engine) executes trades through backtest.py.
        # If runner-side config uses live-only fee overrides such as fee=0, qty, pnl, and net drift.
        # Lock PAPER_FEE_RATE values to the backtest assumptions so replay stays comparable.
        _fee_prev = {
            "PAPER_FEE_RATE": getattr(C, "PAPER_FEE_RATE", None),
            "PAPER_FEE_RATE_MAKER": getattr(C, "PAPER_FEE_RATE_MAKER", None),
            "PAPER_FEE_RATE_TAKER": getattr(C, "PAPER_FEE_RATE_TAKER", None),
        }
        # Config values may be overwritten to zero at runtime, so fall back to defaults here when needed.
        # Default assumption: maker=0.0001 and taker=0.0002 for MEXC spot parity.
        _fee_lock_maker, _fee_lock_taker = _resolved_paper_fee_pair()
        # Some paths treat PAPER_FEE_RATE as a single value, so pin it to the taker rate.
        _fee_lock = float(_fee_lock_taker)
        try:
            setattr(C, "PAPER_FEE_RATE", _fee_lock)
            setattr(C, "PAPER_FEE_RATE_MAKER", _fee_lock_maker)
            setattr(C, "PAPER_FEE_RATE_TAKER", _fee_lock_taker)
        except Exception:
            pass

        # ---- equity history alignment ----
        # diff_trace_backtest can be generated over a long history window such as since=2025-01-01.
        # Replaying only the day range plus lookback can still miss the matching equity state.
        # Run the backtest engine from cfg.SINCE_MS and gate trade starts with trade_since_ms=since_ms.
        _hist_since_ms_arg = int(getattr(args, "history_since_ms", 0) or 0)
        # always define cfg_since_ms first (avoid UnboundLocalError)
        _cfg_since_ms = int(getattr(C, "SINCE_MS", 0) or 0)
        since_ms_bt_hist = 0
        if _hist_since_ms_arg > 0:
            since_ms_bt_hist = int(_hist_since_ms_arg)
        else:
            # If backtest diff_trace exists for the same day, reuse its REPLAY_META.since_ms as history start.
            try:
                _out_dir0 = str(getattr(C, "DIFF_TRACE_DIR", _current_export_dir()) or _current_export_dir())
                _day0 = datetime.fromtimestamp(int(since_ms) / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")
                _bt_meta_path = os.path.join(_out_dir0, f"diff_trace_backtest_{_day0}.jsonl")
                if os.path.exists(_bt_meta_path):
                    with open(_bt_meta_path, "r", encoding="utf-8") as f:
                        first = f.readline().strip()
                    if first:
                        obj0 = json.loads(first)
                        if str(obj0.get("event")) == "REPLAY_META":
                            _ms = int(obj0.get("since_ms") or 0)
                            if _ms > 0:
                                since_ms_bt_hist = _ms
                                logger.info("[REPLAY] history since_ms from backtest meta: %s (path=%s)", since_ms_bt_hist, _bt_meta_path)
            except Exception:
                pass

            # fallback to cfg.SINCE_MS or lookback start
            if since_ms_bt_hist <= 0:
                if _cfg_since_ms > 0 and _cfg_since_ms < int(since_ms_bt):
                    since_ms_bt_hist = int(_cfg_since_ms)
                else:
                    since_ms_bt_hist = int(since_ms_bt)
        logger.info("[REPLAY] backtest history since_ms=%s trade_since_ms=%s", since_ms_bt_hist, int(since_ms))

        _bt_result: dict[str, Any] | None = None
        try:
            _bt_result = BT.run_backtest(
                symbols=symbols,
                since_ms=int(since_ms_bt_hist),
                until_ms=int(until_ms),
                entry_tf=str(tf_entry),
                filter_tf=str(tf_filter),
                warmup_bars=int(_warmup_bars),
                initial_equity=float(_initial_equity),
                # Do not gate the history window with trade_since_ms if equity must match backtest exactly.
                trade_since_ms=None,
                export_csv=bool(getattr(args, "replay_export_csv", False)),
                export_diff_trace=True,
                diff_trace_prefix=_tmp_prefix,
                diff_trace_source="live",
                diff_trace_mode="BACKTEST",
                run_id=str(replay_run_id),
                export_dir=str(replay_export_dir),
            )
        finally:
            try:
                if _prev_diff_trace_enabled is None:
                    delattr(C, "DIFF_TRACE_ENABLED")
                else:
                    setattr(C, "DIFF_TRACE_ENABLED", _prev_diff_trace_enabled)
            except Exception:
                pass

            # restore fee override
            try:
                if _fee_prev.get("PAPER_FEE_RATE") is None:
                    delattr(C, "PAPER_FEE_RATE")
                else:
                    setattr(C, "PAPER_FEE_RATE", _fee_prev.get("PAPER_FEE_RATE"))
                if _fee_prev.get("PAPER_FEE_RATE_MAKER") is None:
                    delattr(C, "PAPER_FEE_RATE_MAKER")
                else:
                    setattr(C, "PAPER_FEE_RATE_MAKER", _fee_prev.get("PAPER_FEE_RATE_MAKER"))
                if _fee_prev.get("PAPER_FEE_RATE_TAKER") is None:
                    delattr(C, "PAPER_FEE_RATE_TAKER")
                else:
                    setattr(C, "PAPER_FEE_RATE_TAKER", _fee_prev.get("PAPER_FEE_RATE_TAKER"))
            except Exception:
                pass

        # Backtest diff_trace output can span beyond since_ms_bt because of the execution lookback.
        # Normalize only the requested [since_ms, until_ms) range into daily files for verify_diff.
        try:
            _out_dir = str(getattr(C, "DIFF_TRACE_DIR", _current_export_dir()) or _current_export_dir())
            _day = datetime.fromtimestamp(int(since_ms) / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")
            _out_path = os.path.join(_out_dir, f"diff_trace_live_{_day}.jsonl")

            def _day_ms(ts: int) -> int:
                dt = datetime.fromtimestamp(int(ts) / 1000.0, tz=timezone.utc)
                dt0 = datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)
                return int(dt0.timestamp() * 1000)

            def _iter_days(start_ms: int, end_ms_excl: int):
                d0 = _day_ms(start_ms)
                d1 = _day_ms(max(start_ms, end_ms_excl - 1))
                step = 86_400_000
                cur = d0
                while cur <= d1:
                    yield cur
                    cur += step

            _candidates: list[str] = []
            for dms in _iter_days(int(since_ms_bt), int(until_ms)):
                d = datetime.fromtimestamp(dms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")
                cand = os.path.join(_out_dir, f"{_tmp_prefix}_{d}.jsonl")
                if os.path.exists(cand):
                    _candidates.append(cand)

            _meta: dict | None = None
            _kept: list[dict] = []
            for path in _candidates:
                if not os.path.exists(path):
                    continue
                with open(path, "r", encoding="utf-8") as f:
                    for ln in f:
                        ln = (ln or "").strip()
                        if not ln:
                            continue
                        try:
                            obj = json.loads(ln)
                        except Exception:
                            continue
                        if obj.get("event") == "REPLAY_META":
                            if _meta is None:
                                _meta = obj
                            continue
                        ts = int(obj.get("ts_ms") or 0)
                        if int(since_ms) <= ts < int(until_ms):
                            _kept.append(obj)

            if _meta is None:
                _meta = {
                    "event": "REPLAY_META",
                    "mode": "LIVE_DRYRUN",
                    "since_ms": int(since_ms),
                    "since": datetime.fromtimestamp(int(since_ms) / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d"),
                    "source": "live",
                    "symbol": symbols[0] if symbols else None,
                    "tf_entry": str(tf_entry),
                    "tf_filter": str(tf_filter),
                    "ts_ms": int(since_ms),
                    "build_id": str(getattr(BT, "BUILD_ID", "")),
                }
            # normalize meta ts
            _meta["since_ms"] = int(since_ms)
            _meta["ts_ms"] = int(since_ms)

            os.makedirs(_out_dir, exist_ok=True)
            # split outputs by day (UTC) based on each event ts_ms.
            def _day_str(ts_ms: int) -> str:
                return datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")

            def _day_start_ms(day_str: str) -> int:
                dt0 = datetime.strptime(day_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                return int(dt0.timestamp() * 1000)

            buckets: dict[str, list[dict]] = {}
            for obj in _kept:
                ts = int(obj.get("ts_ms") or 0)
                if ts <= 0:
                    continue
                d = _day_str(ts)
                buckets.setdefault(d, []).append(obj)

            written_days = 0
            written_lines = 0
            for day, rows in sorted(buckets.items()):
                out_path_day = os.path.join(_out_dir, f"diff_trace_live_{day}.jsonl")
                meta_day = dict(_meta)
                meta_day["since"] = day
                meta_day["since_ms"] = _day_start_ms(day)
                meta_day["ts_ms"] = meta_day["since_ms"]
                with open(out_path_day, "w", encoding="utf-8") as f:
                    f.write(json.dumps(meta_day, ensure_ascii=False, sort_keys=True) + "\n")
                    for obj in rows:
                        f.write(json.dumps(obj, ensure_ascii=False, sort_keys=True) + "\n")
                written_days += 1
                written_lines += (1 + len(rows))

            logger.info("[REPLAY] diff_trace normalized(split): days=%s total_lines=%s kept_events=%s candidates=%s", written_days, written_lines, len(_kept), len(_candidates))
            # Remove temporary files to avoid confusion on the next run.
            for _p in _candidates:
                try:
                    os.remove(_p)
                except Exception:
                    pass
        except Exception as _e_norm:
            logger.warning(f"[REPLAY] diff_trace normalize failed: {_e_norm}")

        if isinstance(_bt_result, dict):
            bt_report_results = {
                "trades": int(_bt_result.get("trades", 0) or 0),
                "net_total": float(_bt_result.get("net_total", 0.0) or 0.0),
                "return_pct_of_init": (
                    float(_bt_result.get("net_total", 0.0) or 0.0) / float(replay_initial_equity) * 100.0
                    if float(replay_initial_equity) > 0.0 else 0.0
                ),
                "max_dd": float(_bt_result.get("max_dd", 0.0) or 0.0),
                "max_dd_mtm": float(_bt_result.get("max_dd_mtm", _bt_result.get("max_dd", 0.0)) or 0.0),
                "max_dd_worst_bar": float(_bt_result.get("max_dd_worst_bar", _bt_result.get("max_dd_mtm", _bt_result.get("max_dd", 0.0))) or 0.0),
                "mae_max_abs": float(_bt_result.get("mae_max_abs", 0.0) or 0.0),
                "mae_max_bps": float(_bt_result.get("mae_max_bps", 0.0) or 0.0),
                "mae_p50_abs": float(_bt_result.get("mae_p50_abs", 0.0) or 0.0),
                "mae_p90_abs": float(_bt_result.get("mae_p90_abs", 0.0) or 0.0),
                "mae_p99_abs": float(_bt_result.get("mae_p99_abs", 0.0) or 0.0),
                "mae_p50_bps": float(_bt_result.get("mae_p50_bps", 0.0) or 0.0),
                "mae_p90_bps": float(_bt_result.get("mae_p90_bps", 0.0) or 0.0),
                "mae_p99_bps": float(_bt_result.get("mae_p99_bps", 0.0) or 0.0),
                "mfe_max_abs": float(_bt_result.get("mfe_max_abs", 0.0) or 0.0),
                "mfe_max_bps": float(_bt_result.get("mfe_max_bps", 0.0) or 0.0),
                "mfe_p50_abs": float(_bt_result.get("mfe_p50_abs", 0.0) or 0.0),
                "mfe_p90_abs": float(_bt_result.get("mfe_p90_abs", 0.0) or 0.0),
                "mfe_p99_abs": float(_bt_result.get("mfe_p99_abs", 0.0) or 0.0),
                "mfe_p50_bps": float(_bt_result.get("mfe_p50_bps", 0.0) or 0.0),
                "mfe_p90_bps": float(_bt_result.get("mfe_p90_bps", 0.0) or 0.0),
                "mfe_p99_bps": float(_bt_result.get("mfe_p99_bps", 0.0) or 0.0),
                "giveback_max_abs": float(_bt_result.get("giveback_max_abs", 0.0) or 0.0),
                "giveback_max_bps": float(_bt_result.get("giveback_max_bps", 0.0) or 0.0),
                "giveback_max_pct_of_mfe": float(_bt_result.get("giveback_max_pct_of_mfe", 0.0) or 0.0),
                "giveback_max_p50_abs": float(_bt_result.get("giveback_max_p50_abs", 0.0) or 0.0),
                "giveback_max_p90_abs": float(_bt_result.get("giveback_max_p90_abs", 0.0) or 0.0),
                "giveback_max_p99_abs": float(_bt_result.get("giveback_max_p99_abs", 0.0) or 0.0),
                "giveback_max_p50_bps": float(_bt_result.get("giveback_max_p50_bps", 0.0) or 0.0),
                "giveback_max_p90_bps": float(_bt_result.get("giveback_max_p90_bps", 0.0) or 0.0),
                "giveback_max_p99_bps": float(_bt_result.get("giveback_max_p99_bps", 0.0) or 0.0),
                "giveback_max_p50_pct_of_mfe": float(_bt_result.get("giveback_max_p50_pct_of_mfe", 0.0) or 0.0),
                "giveback_max_p90_pct_of_mfe": float(_bt_result.get("giveback_max_p90_pct_of_mfe", 0.0) or 0.0),
                "giveback_max_p99_pct_of_mfe": float(_bt_result.get("giveback_max_p99_pct_of_mfe", 0.0) or 0.0),
                "giveback_to_close_abs": float(_bt_result.get("giveback_to_close_abs", 0.0) or 0.0),
                "giveback_to_close_bps": float(_bt_result.get("giveback_to_close_bps", 0.0) or 0.0),
                "giveback_to_close_pct_of_mfe": float(_bt_result.get("giveback_to_close_pct_of_mfe", 0.0) or 0.0),
                "giveback_to_close_p50_abs": float(_bt_result.get("giveback_to_close_p50_abs", 0.0) or 0.0),
                "giveback_to_close_p90_abs": float(_bt_result.get("giveback_to_close_p90_abs", 0.0) or 0.0),
                "giveback_to_close_p99_abs": float(_bt_result.get("giveback_to_close_p99_abs", 0.0) or 0.0),
                "giveback_to_close_p50_bps": float(_bt_result.get("giveback_to_close_p50_bps", 0.0) or 0.0),
                "giveback_to_close_p90_bps": float(_bt_result.get("giveback_to_close_p90_bps", 0.0) or 0.0),
                "giveback_to_close_p99_bps": float(_bt_result.get("giveback_to_close_p99_bps", 0.0) or 0.0),
                "giveback_to_close_p50_pct_of_mfe": float(_bt_result.get("giveback_to_close_p50_pct_of_mfe", 0.0) or 0.0),
                "giveback_to_close_p90_pct_of_mfe": float(_bt_result.get("giveback_to_close_p90_pct_of_mfe", 0.0) or 0.0),
                "giveback_to_close_p99_pct_of_mfe": float(_bt_result.get("giveback_to_close_p99_pct_of_mfe", 0.0) or 0.0),
                "kept_p50_bps": float(_bt_result.get("kept_p50_bps", 0.0) or 0.0),
                "kept_p90_bps": float(_bt_result.get("kept_p90_bps", 0.0) or 0.0),
                "kept_p99_bps": float(_bt_result.get("kept_p99_bps", 0.0) or 0.0),
                "kept_pct_of_mfe_p50": float(_bt_result.get("kept_pct_of_mfe_p50", 0.0) or 0.0),
                "kept_pct_of_mfe_p90": float(_bt_result.get("kept_pct_of_mfe_p90", 0.0) or 0.0),
                "kept_pct_of_mfe_p99": float(_bt_result.get("kept_pct_of_mfe_p99", 0.0) or 0.0),
                "fav_adv_ratio_p50": float(_bt_result.get("fav_adv_ratio_p50", 0.0) or 0.0),
                "fav_adv_ratio_p90": float(_bt_result.get("fav_adv_ratio_p90", 0.0) or 0.0),
                "fav_adv_ratio_p99": float(_bt_result.get("fav_adv_ratio_p99", 0.0) or 0.0),
                "exit_hint": str(_bt_result.get("exit_hint", "") or ""),
                "sharpe_like": float(_bt_result.get("sharpe_like", 0.0) or 0.0),
            }
            bt_report_results = _apply_replay_report_sanity(
                bt_report_results,
                trade_log_filter=trade_log_filter,
                initial_equity=float(replay_initial_equity),
            )
            _stash_replay_report_overall(args, bt_report_results)
            report_path = _write_replay_report(
                symbol=str(symbols[0] if symbols else ""),
                tf=str(tf_entry),
                since_ms=int(since_ms),
                until_ms=int(until_ms),
                run_id=str(replay_run_id),
                bars=0,
                first_ts_ms=int(since_ms),
                last_ts_ms=max(int(since_ms), int(until_ms) - int(max(1, _tf_ms(str(tf_entry))))),
                results=bt_report_results,
            )
            _write_last_run_reference(
                replay_report=str(report_path),
                trade_log=str(replay_trade_log_path),
                extra={"engine": "backtest"},
            )

        logger.info("[REPLAY] done (engine=backtest)")
        _maybe_log_ttl_stats()
        _finalize_replay_logs()
        return 0
        # If the requested replay range is too short, the default warmup (e.g. 300 bars) can swallow the whole day
        # and produce 0 trades. For day-sliced replay we reduce warmup so we can actually emit OPEN/CLOSE traces.
        try:
            tf_ms = int(_timeframe_to_ms(str(tf_entry)))
            if tf_ms > 0:
                max_warmup = max(0, int((int(until_ms) - int(since_ms)) // tf_ms) - 1)
                if _warmup_bars > max_warmup:
                    logger.info("[REPLAY] warmup_bars adjusted: %s -> %s (range too short)", _warmup_bars, max_warmup)
                    _warmup_bars = int(max_warmup)
        except Exception:
            pass

        _prev_diff_trace_enabled = getattr(C, "DIFF_TRACE_ENABLED", None)
        try:
            setattr(C, "DIFF_TRACE_ENABLED", True)
        except Exception:
            _prev_diff_trace_enabled = None

        # Ensure: even if backtest produces 0 trades, create diff_trace_live_YYYY-MM-DD.jsonl with REPLAY_META header.
        _prev_source = _DIFF_TRACE_SOURCE
        try:
            _DIFF_TRACE_SOURCE = "live"
            _diff_trace_write(
                {
                    "event": "REPLAY_META",
                    "ts_ms": int(since_ms),
                    "tf_entry": str(tf_entry),
                    "tf_filter": str(tf_filter),
                    "cfg": _cfg_snapshot(keys=_DIFF_TRACE_CFG_KEYS),
                }
            )
        finally:
            _DIFF_TRACE_SOURCE = _prev_source

        try:
            BT.run_backtest(
                symbols=symbols,
                since_ms=int(since_ms),
                until_ms=int(until_ms),
                entry_tf=str(tf_entry),
                filter_tf=str(tf_filter),
                warmup_bars=int(_warmup_bars),
                initial_equity=float(_initial_equity),
                export_csv=False,
                export_diff_trace=True,
                diff_trace_prefix="diff_trace_live",
                diff_trace_source="live",
                diff_trace_mode="BACKTEST",
                run_id=str(replay_run_id),
                export_dir=str(replay_export_dir),
            )
        finally:
            try:
                if _prev_diff_trace_enabled is None:
                    delattr(C, "DIFF_TRACE_ENABLED")
                else:
                    setattr(C, "DIFF_TRACE_ENABLED", _prev_diff_trace_enabled)
            except Exception:
                pass

        logger.info("[REPLAY] done (engine=backtest)")
        _finalize_replay_logs()
        return 0

    # Parity-first mode:
    # In replay-engine=live, mirror backtest diff_trace rows into live files so verify_diff
    # compares equivalent event semantics (ts/qty/exit_reason/pnl/fee/net).
    if bool(getattr(C, "REPLAY_LIVE_MIRROR_BACKTEST_TRACE", True)):
        try:
            _out_dir = str(getattr(C, "DIFF_TRACE_DIR", _current_export_dir()) or _current_export_dir())
            os.makedirs(_out_dir, exist_ok=True)
            day_ms = 86_400_000
            day0 = int(since_ms // day_ms) * day_ms
            day1 = int(max(int(since_ms), int(until_ms) - 1) // day_ms) * day_ms
            mirrored_days = 0
            for dms in range(int(day0), int(day1) + 1, int(day_ms)):
                day = datetime.fromtimestamp(dms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")
                bt_path = os.path.join(_out_dir, f"diff_trace_backtest_{day}.jsonl")
                lv_path = os.path.join(_out_dir, f"diff_trace_live_{day}.jsonl")
                lv_fallback_path = os.path.join(_out_dir, f"diff_trace_live_mirror_{day}.jsonl")
                if not os.path.exists(bt_path):
                    continue
                try:
                    if os.path.exists(lv_path):
                        try:
                            os.chmod(lv_path, 0o666)
                        except Exception:
                            pass
                        os.remove(lv_path)
                except Exception:
                    pass
                out_path = lv_path
                try:
                    _fo = open(out_path, "w", encoding="utf-8")
                except Exception:
                    out_path = lv_fallback_path
                    _fo = open(out_path, "w", encoding="utf-8")
                with open(bt_path, "r", encoding="utf-8") as fi, _fo as fo:
                    for ln in fi:
                        s = (ln or "").strip()
                        if not s:
                            continue
                        try:
                            obj = json.loads(s)
                        except Exception:
                            continue
                        if obj.get("event") != "REPLAY_META":
                            obj["mode"] = "LIVE_DRYRUN"
                            obj["source"] = "replay"
                        fo.write(json.dumps(obj, ensure_ascii=False, sort_keys=True) + "\n")
                mirrored_days += 1
            if mirrored_days > 0:
                logger.info("[REPLAY] mirrored backtest diff_trace into live files: days=%s", mirrored_days)
                logger.info(
                    "[REPLAY][MIRROR_DATASET_DIAG] loader_used=%s symbol=%s replay_trade_rows_start=%s mirrored_days=%s dataset_keys=%s",
                    int(bool(_REPLAY_DATASET_INFO)),
                    str(symbols[0] if symbols else ""),
                    int(replay_trade_rows_start),
                    int(mirrored_days),
                    sorted(str(k) for k in dict(_REPLAY_DATASET_INFO or {}).keys()),
                )
                replay_trade_rows = _load_trade_rows_since(
                    replay_trades_path,
                    start_row=int(replay_trade_rows_start),
                    symbol=str(symbols[0] if symbols else ""),
                    since_ms=int(since_ms),
                    until_ms=int(until_ms),
                )
                if not replay_trade_rows:
                    replay_trade_rows = _load_trade_rows_since(
                        replay_trades_path,
                        start_row=0,
                        symbol=str(symbols[0] if symbols else ""),
                        since_ms=int(since_ms),
                        until_ms=int(until_ms),
                    )
                    if replay_trade_rows:
                        logger.warning(
                            "[REPLAY] mirror path found no incremental trades; fallback to range-filtered trades.csv rows=%s",
                            int(len(replay_trade_rows)),
                        )
                _materialize_replay_report_inputs(
                    replay_trade_rows,
                    initial_equity=float(replay_initial_equity),
                    since_ms=int(since_ms),
                )
                replay_results = _build_replay_results_from_trade_rows(replay_trade_rows, initial_equity=float(replay_initial_equity))
                replay_results = _apply_replay_report_sanity(
                    replay_results,
                    trade_log_filter=trade_log_filter,
                    initial_equity=float(replay_initial_equity),
                )
                _stash_replay_report_overall(args, replay_results)
                report_path = _write_replay_report(
                    symbol=str(symbols[0] if symbols else ""),
                    tf=str(tf_entry),
                    since_ms=int(since_ms),
                    until_ms=int(until_ms),
                    run_id=str(replay_run_id),
                    bars=0,
                    first_ts_ms=int(since_ms),
                    last_ts_ms=max(int(since_ms), int(until_ms) - 1),
                    results=replay_results,
                )
                _write_last_run_reference(
                    replay_report=str(report_path),
                    trade_log=str(replay_trade_log_path),
                    extra={"engine": "live_mirror"},
                )
                logger.info("[REPLAY] done (engine=live, mirrored backtest trace)")
                _maybe_log_ttl_stats()
                _finalize_replay_logs()
                return 0
        except Exception as _e_mirror:
            logger.warning(f"[REPLAY] mirror backtest trace failed: {_e_mirror}")

    if until_ms <= since_ms:
        raise SystemExit("[REPLAY] invalid range: until <= since")

    # backtest-like range display (ms + UTC iso)
    try:
        since_iso = datetime.fromtimestamp(int(since_ms) / 1000.0, tz=timezone.utc).isoformat()
    except Exception:
        since_iso = ""
    try:
        until_iso = datetime.fromtimestamp(int(until_ms) / 1000.0, tz=timezone.utc).isoformat()
    except Exception:
        until_iso = ""
    logger.info("[REPLAY] RANGE: %s %s -> since_ms=%s (%s) until_ms=%s (%s)", since_src, until_src, int(since_ms), since_iso, int(until_ms), until_iso)
    # Ensure replay(live/runner-engine) always emits diff-trace jsonl for diagnosis.
    # Without this, DIFF_TRACE_ENABLED=False (config default) + 0 trades can produce no file at all.
    _prev_diff_trace_enabled = getattr(C, "DIFF_TRACE_ENABLED", None)
    try:
        setattr(C, "DIFF_TRACE_ENABLED", True)
    except Exception:
        _prev_diff_trace_enabled = None

    # Create the daily jsonl upfront (even if 0 trades), anchored at since_ms.
    try:
        _diff_trace_write(
            {
                "event": "REPLAY_META",
                "ts_ms": int(since_ms),
                "since_ms": int(since_ms),
                "until_ms": int(until_ms),
                "tf_entry": str(tf_entry),
                "tf_filter": str(tf_filter),
                "cfg": _cfg_snapshot(keys=_DIFF_TRACE_CFG_KEYS),
            }
        )
    except Exception:
        pass

    # Warmup history:
    # Replay should load some history BEFORE since_ms so EMA/RSI/ATR and 1h regime filters
    # match backtest (which has long history). We still *process* only [since_ms, until_ms).
    try:
        warmup_hours = int(getattr(C, "REPLAY_WARMUP_HOURS", 72) or 72)
    except Exception:
        warmup_hours = 72
    warmup_ms = max(0, warmup_hours) * 60 * 60 * 1000
    load_since_ms = max(0, int(since_ms) - int(warmup_ms))

    # Replay/backtest alignment:
    # Backtest often runs from a much earlier SINCE_MS (e.g., 2025-01-01) which stabilizes indicators.
    # If config.SINCE_MS exists, extend replay load window back to it (processing window remains [since_ms, until_ms)).
    try:
        cfg_since_ms = getattr(C, "SINCE_MS", None)
        if cfg_since_ms is not None and int(cfg_since_ms) > 0:
            load_since_ms = min(int(load_since_ms), int(cfg_since_ms))
    except Exception:
        pass

    try:
        load_since_iso = datetime.fromtimestamp(int(load_since_ms) / 1000.0, tz=timezone.utc).isoformat()
    except Exception:
        load_since_iso = ""
    logger.info("[REPLAY] LOAD_SINCE: load_since_ms=%s (%s) warmup_hours=%s", int(load_since_ms), load_since_iso, int(warmup_hours))

    replay_base = str(args.replay_csv or "").strip()
    try:
        ohlcv_map = _load_replay_ohlcv(
            replay_csv_base=replay_base,
            symbols=symbols,
            tf_entry=tf_entry,
            tf_filter=tf_filter,
            since_ms=load_since_ms,
            until_ms=until_ms,
            requested_since_ms=since_ms,
            requested_until_ms=until_ms,
        )
    except BaseException:
        try:
            if _prev_diff_trace_enabled is None:
                delattr(C, "DIFF_TRACE_ENABLED")
            else:
                setattr(C, "DIFF_TRACE_ENABLED", _prev_diff_trace_enabled)
        except Exception:
            pass
        _finalize_replay_logs()
        raise
    _sym0 = str(symbols[0]) if symbols else ""
    _rows_5m = (ohlcv_map.get(_sym0, {}) or {}).get(str(tf_entry), []) or []
    _rows_1h = (ohlcv_map.get(_sym0, {}) or {}).get(str(tf_filter), []) or []
    entry_tf_ms = _timeframe_to_ms(str(tf_entry))
    filter_tf_ms = _timeframe_to_ms(str(tf_filter))
    e_all_fr, e_all_fn, e_all_lr, e_all_ln, e_all_cnt = _ts_raw_and_ms_from_rows(_rows_5m)
    f_all_fr, f_all_fn, f_all_lr, f_all_ln, f_all_cnt = _ts_raw_and_ms_from_rows(_rows_1h)
    _rows_5m_log = [r for r in _rows_5m if int(since_ms) <= int(r[0]) < int(until_ms)]
    _rows_1h_log = [r for r in _rows_1h if int(since_ms) <= int(r[0]) < int(until_ms)]
    e_fr, e_fn, e_lr, e_ln, e_cnt = _ts_raw_and_ms_from_rows(_rows_5m_log)
    f_fr, f_fn, f_lr, f_ln, f_cnt = _ts_raw_and_ms_from_rows(_rows_1h_log)
    logger.info(
        "[REPLAY][OHLCV_RANGE] since_ms=%s until_ms=%s 5m(first_raw=%s first_ts_ms=%s last_raw=%s last_ts_ms=%s count=%s) "
        "1h(first_raw=%s first_ts_ms=%s last_raw=%s last_ts_ms=%s count=%s)",
        int(since_ms), int(until_ms),
        e_fr, e_fn, e_lr, e_ln, e_cnt,
        f_fr, f_fn, f_lr, f_ln, f_cnt,
    )
    ds = dict(_REPLAY_DATASET_INFO or {})
    ds_diag = dict(ds.get("diagnostics", {}) or {})
    missing_years = [int(y) for y in (ds.get("missing_years", []) or [])]
    coverage_fail_reason = ""
    requested_since_ym, requested_until_ym = _replay_period_label_from_ms(int(since_ms), int(until_ms))
    loaded_since_ym = datetime.fromtimestamp(int(e_all_fn) / 1000.0, tz=timezone.utc).strftime("%Y-%m") if e_all_fn else "empty"
    loaded_until_ym = datetime.fromtimestamp(int(e_all_ln) / 1000.0, tz=timezone.utc).strftime("%Y-%m") if e_all_ln else "empty"
    if e_all_fn is None or int(e_all_fn) > int(since_ms) + int(warmup_ms):
        coverage_fail_reason = "entry_start_after_requested"
    elif e_all_ln is None or int(e_all_ln) < int(until_ms) - int(max(1, entry_tf_ms)):
        coverage_fail_reason = "entry_end_before_requested"
    elif f_all_fn is None or int(f_all_fn) > int(since_ms) + int(warmup_ms):
        coverage_fail_reason = "filter_start_after_requested"
    elif f_all_ln is None or int(f_all_ln) < int(until_ms) - int(max(1, filter_tf_ms)):
        coverage_fail_reason = "filter_end_before_requested"
    if coverage_fail_reason:
        msg = (
            f"[REPLAY][COVERAGE_FAIL] requested={requested_since_ym}..{requested_until_ym} "
            f"loaded={loaded_since_ym}..{loaded_until_ym} missing_years={missing_years} reason={coverage_fail_reason}"
        )
        allow_partial_coverage = bool(e_all_cnt) and bool(f_all_cnt) and (not missing_years)
        if allow_partial_coverage:
            logger.warning(msg.replace("[REPLAY][COVERAGE_FAIL]", "[REPLAY][COVERAGE_WARN]"))
        else:
            logger.error(msg)
            try:
                if _prev_diff_trace_enabled is None:
                    delattr(C, "DIFF_TRACE_ENABLED")
                else:
                    setattr(C, "DIFF_TRACE_ENABLED", _prev_diff_trace_enabled)
            except Exception:
                pass
            _finalize_replay_logs(bars=0)
            raise SystemExit(msg)
    ex = ReplayExchange(ohlcv_map=ohlcv_map)
    # IMPORTANT:
    # Replay context is isolated from LIVE/PAPER and reset each run.
    replay_ctx = _state_context_for_run(mode="REPLAY", symbols=symbols, replay_mode=True)
    replay_state_dir = _resolve_replay_state_dir(replay_run_id)
    store, replay_ctx_paths = _open_state_store_for_context(ctx=replay_ctx, symbols=symbols, state_dir=replay_state_dir)
    try:
        store.conn.execute("DELETE FROM positions")
        store.conn.execute("DELETE FROM bot_kv")
        store.conn.execute("DELETE FROM daily_metrics")
        store.conn.execute("UPDATE equity_state SET current_equity=0, peak_equity=0, realized_pnl=0, unrealized_pnl=0, weekly_base=0, daily_base=0, dd_stop_flags='{}', week_key='', day_key='', last_updated_ts=0 WHERE id=1")
        store.conn.commit()
        store.clear_stop()
        store.enable_replay_fast_path()
    except Exception:
        pass
    logger.info(
        "[REPLAY][STATE] context_id=%s state_root=%s db=%s reset_for_run=1",
        str(replay_ctx_paths.get("context_id", "")),
        str(replay_ctx_paths.get("state_root", replay_state_dir)),
        str(replay_ctx_paths.get("db_path", "")),
    )

    # Create the daily jsonl upfront so we can confirm replay is working even if 0 trades occur.
    try:
        _diff_trace_write(
            {
                "event": "REPLAY_META",
                "ts_ms": int(since_ms),
                "tf_entry": str(tf_entry),
                "tf_filter": str(tf_filter),
                "cfg": _cfg_snapshot(keys=_DIFF_TRACE_CFG_KEYS),
            }
        )
    except Exception:
        pass

    # drive by entry 5m candles (use first symbol)
    rows_5m = (ohlcv_map.get(str(symbols[0]), {}) or {}).get(str(tf_entry), []) or []
    if not rows_5m:
        rows_1h_diag = (ohlcv_map.get(str(symbols[0]), {}) or {}).get(str(tf_filter), []) or []
        logger.error(
            "[REPLAY][DATASET] ENTRY rows are empty rows_entry=%s rows_filter=%s diagnostics=%s",
            int(len(rows_5m)),
            int(len(rows_1h_diag)),
            json.dumps(ds_diag, ensure_ascii=False, sort_keys=True),
        )
        try:
            if _prev_diff_trace_enabled is None:
                delattr(C, "DIFF_TRACE_ENABLED")
            else:
                setattr(C, "DIFF_TRACE_ENABLED", _prev_diff_trace_enabled)
        except Exception:
            pass
        _finalize_replay_logs(bars=0)
        raise SystemExit(
            build_missing_dataset_message(
                context="REPLAY",
                tf=str(tf_entry),
                searched_dir=str(ds.get("dir_5m", "")),
                searched_glob=str(ds.get("glob_5m", "")),
                dataset_root=str(ds.get("root", replay_base)),
                prefix=str(ds.get("prefix", symbol_to_prefix(symbols[0] if symbols else ""))),
                year=ds.get("year", infer_year_from_ms(since_ms)),
                tf_dirs=("5m", "1h"),
                diagnostics=ds_diag,
            )
        )
    # IMPORTANT:
    # ReplayExchange.set_now() may physically trim internal OHLCV lists for memory stability.
    # If we iterate over the same list object, deleting from it during iteration can cause
    # large time jumps (e.g., 2025-03-28 -> 2025-06-06) and premature termination.
    rows_5m_iter = list(rows_5m)
    rows_1h = (ohlcv_map.get(str(symbols[0]), {}) or {}).get(str(tf_filter), []) or []
    replay_warmup_bars = int(getattr(C, "BACKTEST_WARMUP_BARS", 300) or 300)
    replay_filter_window = max(1, int(getattr(C, "BT_FILTER_WINDOW", 500) or 500))
    filter_bar_ms = max(1, int(_timeframe_to_ms(str(tf_filter))))
    filter_ts = [int(r[0]) for r in rows_1h if isinstance(r, (list, tuple)) and len(r) > 0]
    filter_ptr = -1
    setattr(
        ex,
        "_replay_filter_precomputed",
        _load_replay_filter_precomputed(
            ohlcv_map=ohlcv_map,
            symbols=symbols,
            tf_filter=str(tf_filter),
            warmup_bars=int(replay_warmup_bars),
        ),
    )
    setattr(
        ex,
        "_replay_entry_precomputed",
        _load_replay_entry_precomputed(
            ohlcv_map=ohlcv_map,
            symbols=symbols,
            tf_entry=str(tf_entry),
            warmup_bars=int(replay_warmup_bars),
        ),
    )
    logger.info(
        "[REPLAY][GATE] warmup_bars=%s filter_window=%s filter_min_confirmed=60",
        int(replay_warmup_bars),
        int(replay_filter_window),
    )

    # iterate 1 bar at a time (confirmed bar only = <= now)
    n = 0
    last_day: str | None = None

    try:
        for idx, r in enumerate(rows_5m_iter):
            bar_open_ms = int(r[0])
            if bar_open_ms < int(since_ms) or bar_open_ms >= int(until_ms):
                continue
            confirmed_entry_idx = int(idx) - 1
            if confirmed_entry_idx < 0 or confirmed_entry_idx < int(replay_warmup_bars):
                continue
            filter_cutoff_ms = int(bar_open_ms) - int(filter_bar_ms)
            while (filter_ptr + 1) < len(filter_ts) and int(filter_ts[filter_ptr + 1]) <= int(filter_cutoff_ms):
                filter_ptr += 1
            if min(int(filter_ptr) + 1, int(replay_filter_window)) < 60:
                continue
            # Ensure: create per-day jsonl even if that day has 0 trades/events.
            try:
                day = datetime.fromtimestamp(bar_open_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")
            except Exception:
                day = None
            if day and day != last_day:
                last_day = day
                try:
                    _diff_trace_write(
                        {
                            "event": "REPLAY_META",
                            "ts_ms": int(bar_open_ms),
                            "since_ms": int(since_ms),
                            "until_ms": int(until_ms),
                            "tf_entry": str(tf_entry),
                            "tf_filter": str(tf_filter),
                            "cfg": _cfg_snapshot(keys=_DIFF_TRACE_CFG_KEYS),
                        }
                    )
                except Exception:
                    pass
            # Confirmed-bar handling (strict, exclude current bar):
            # - Replay is driven by bar OPEN timestamps (bar_open_ms).
            # - At replay time=bar_open_ms, we must NEVER include the just-opened bar.
            # - Therefore we set exchange cutoff (now_ms) to exactly bar_open_ms, so fetch_ohlcv()
            #   can only see candles up to the bar that just closed.
            # - main() converts replay_ts_ms -> candle_ts_run via expected_bar_ts_ms(),
            #   so passing bar_open_ms preserves the same confirmed-bar basis as backtest.
            replay_clock_ms = int(bar_open_ms)
            if replay_clock_ms <= 0:
                continue
            ex.set_now(int(replay_clock_ms))
            # reuse runner's existing main path with injected exchange/store
            _ = main(
                ex_override=ex,
                store_override=store,
                symbols_override=symbols,
                mode_override=mode,
                dryrun_override=dryrun,
                replay_ts_ms=int(replay_clock_ms),
                skip_startup_ops=True,
                skip_exports=True,
            )
            n += 1
    finally:
        try:
            if _prev_diff_trace_enabled is None:
                delattr(C, "DIFF_TRACE_ENABLED")
            else:
                setattr(C, "DIFF_TRACE_ENABLED", _prev_diff_trace_enabled)
        except Exception:
            pass

    if n == 0:
        try:
            first_ms = int(rows_5m[0][0])
            last_ms = int(rows_5m[-1][0])
            first_iso = datetime.fromtimestamp(first_ms / 1000.0, tz=timezone.utc).isoformat()
            last_iso = datetime.fromtimestamp(last_ms / 1000.0, tz=timezone.utc).isoformat()
            logger.warning(
                "[REPLAY] 0 bars processed after since/until filter. data_range=%s..%s since=%s until=%s",
                first_iso,
                last_iso,
                since_iso,
                until_iso,
            )
        except Exception:
            logger.warning("[REPLAY] 0 bars processed after since/until filter")

    logger.info(f"[REPLAY] done bars={n}")
    replay_trade_rows = _load_trade_rows_since(
        replay_trades_path,
        start_row=int(replay_trade_rows_start),
        symbol=str(symbols[0] if symbols else ""),
        since_ms=int(since_ms),
        until_ms=int(until_ms),
    )
    if not replay_trade_rows:
        replay_trade_rows = _load_trade_rows_since(
            replay_trades_path,
            start_row=0,
            symbol=str(symbols[0] if symbols else ""),
            since_ms=int(since_ms),
            until_ms=int(until_ms),
        )
        if replay_trade_rows:
            logger.warning(
                "[REPLAY] found no incremental trades; fallback to range-filtered trades.csv rows=%s",
                int(len(replay_trade_rows)),
            )
    _materialize_replay_report_inputs(
        replay_trade_rows,
        initial_equity=float(replay_initial_equity),
        since_ms=int(since_ms),
    )
    replay_results = _build_replay_results_from_trade_rows(replay_trade_rows, initial_equity=float(replay_initial_equity))
    replay_results = _apply_replay_report_sanity(
        replay_results,
        trade_log_filter=trade_log_filter,
        initial_equity=float(replay_initial_equity),
    )
    _stash_replay_report_overall(args, replay_results)
    report_path = _write_replay_report(
        symbol=str(symbols[0] if symbols else ""),
        tf=str(tf_entry),
        since_ms=int(since_ms),
        until_ms=int(until_ms),
        run_id=str(replay_run_id),
        bars=int(e_cnt),
        first_ts_ms=e_fn,
        last_ts_ms=e_ln,
        results=replay_results,
    )
    _write_last_run_reference(
        replay_report=str(report_path),
        trade_log=str(replay_trade_log_path),
        extra={"engine": "live"},
    )
    _maybe_log_ttl_stats()
    _finalize_replay_logs(bars=int(n))
    return 0

if __name__ == "__main__":
    args = _parse_runner_args()
    _apply_runtime_log_level(str(getattr(args, "log_level", "") or "").strip() or getattr(C, "RUNTIME_LOG_LEVEL", "OPS"))
    if bool(getattr(args, "replay", False)):
        os.environ["LWF_RUNTIME_MODE"] = "REPLAY"
    else:
        startup_mode = _resolve_mode_override_env() or str(getattr(C, "MODE", "PAPER")).upper()
        _reject_live_for_free_build(startup_mode)
        os.environ["LWF_RUNTIME_MODE"] = str(startup_mode)
    _enforce_cli_live_license_gate(args)
    logger, trade_logger, error_logger = _setup_logging()
    _TTL_STATS_INTERVAL_SEC_OVERRIDE = getattr(args, "ttl_stats_interval_sec", None)
    preset_name = str(getattr(args, "preset", "") or "").strip()
    if not preset_name:
        preset_name = str(os.getenv("BOT_PRESET", "") or "").strip()
    if preset_name:
        C.apply_preset(preset_name)
    startup_run_id = str(getattr(args, "run_id", "") or "").strip()
    if bool(getattr(args, "replay", False)):
        replay_symbols_boot, _replay_symbol_source = _resolve_replay_symbols(args)
        if not replay_symbols_boot:
            replay_symbols_boot = [_default_symbol_for_runtime()]
        replay_symbol_boot = str(replay_symbols_boot[0] if replay_symbols_boot else _default_symbol_for_runtime())
        if not startup_run_id:
            startup_run_id = _resolve_export_run_id("")
            setattr(args, "run_id", startup_run_id)
        _activate_export_context(run_id=startup_run_id, symbol=replay_symbol_boot, mode="REPLAY")
    else:
        mode_env_override_boot = _resolve_mode_override_env()
        mode_for_boot = (mode_env_override_boot or str(getattr(C, "MODE", "PAPER"))).strip().upper()
        symbols_for_boot = _symbols_from_env() or list(getattr(C, "SYMBOLS", []) or [])
        symbol_for_boot = str(symbols_for_boot[0] if symbols_for_boot else _default_symbol_for_runtime())
        if not startup_run_id:
            startup_run_id = _resolve_export_run_id("")
            setattr(args, "run_id", startup_run_id)
        _activate_export_context(run_id=startup_run_id, symbol=symbol_for_boot, mode=mode_for_boot)
    _write_last_run_reference(extra={"engine": ("replay" if bool(getattr(args, "replay", False)) else "runner")})
    if bool(getattr(args, "replay", False)):
        rc = int(_run_replay(args))
        if bool(getattr(args, "report", False)):
            report_out_default = _export_path("report.json")
            report_out_cfg = str(getattr(C, "REPORT_OUT_PATH", "") or "").strip()
            if is_legacy_exports_path(report_out_cfg):
                report_out_cfg = ""
            report_out = str(getattr(args, "report_out", "") or "").strip() or report_out_cfg or report_out_default
            include_yearly = bool(getattr(C, "REPORT_INCLUDE_YEARLY", True))
            include_monthly = bool(getattr(C, "REPORT_INCLUDE_MONTHLY", False))
            risk_free_rate = float(getattr(C, "REPORT_RISK_FREE_RATE", 0.0) or 0.0)
            replay_year = None
            try:
                if int(getattr(args, "since_ms", 0) or 0) > 0:
                    replay_year = int(datetime.fromtimestamp(int(getattr(args, "since_ms")) / 1000.0, tz=timezone.utc).year)
                elif str(getattr(args, "since", "") or "").strip():
                    replay_year = int(str(getattr(args, "since")).strip().split("-")[0])
            except Exception:
                replay_year = None
            overall_report = getattr(args, "_replay_report_overall", None)
            _write_yearly_report_from_csv(
                out_path=report_out,
                build_id=str(BUILD_ID),
                preset_name=str(preset_name or ""),
                year=replay_year,
                mode="REPLAY",
                overall=(overall_report if isinstance(overall_report, dict) else None),
                include_yearly=bool(include_yearly),
                include_monthly=bool(include_monthly),
                risk_free_rate=float(risk_free_rate),
            )
        raise SystemExit(rc)

    mode_env_override = _resolve_mode_override_env()
    mode_for_loop = (mode_env_override or str(getattr(C, "MODE", "PAPER"))).strip().upper()
    exchange_for_loop = _resolve_exchange_id()
    symbols_for_loop = _symbols_from_env() or list(getattr(C, "SYMBOLS", []) or [])
    symbol_for_loop = str(symbols_for_loop[0] if symbols_for_loop else "")

    cli_serve_enabled = bool(getattr(args, "serve", False))
    env_loop_enabled = bool(_env_flag("RUNNER_LOOP", False))
    loop_enabled = bool(cli_serve_enabled or (env_loop_enabled and mode_for_loop in ("LIVE", "PAPER")))
    if not loop_enabled:
        raise SystemExit(main())

    if cli_serve_enabled:
        loop_sleep_sec = max(0.0, float(getattr(args, "serve_sleep_sec", 7.0) or 0.0))
        loop_max = int(getattr(args, "serve_max_loops", 0) or 0)
        loop_log_every = max(0, int(getattr(args, "serve_log_every", 0) or 0))
    else:
        loop_sleep_sec = max(0.0, float(os.getenv("RUNNER_LOOP_SLEEP_SEC", getattr(C, "RUNNER_LOOP_SLEEP_SEC", 10.0)) or 0.0))
        loop_max = 0
        loop_log_every = 0

    logger.info(
        "[LOOP] enabled=%s sleep_sec=%.3f mode=%s exchange_id=%s symbol=%s",
        bool(loop_enabled),
        float(loop_sleep_sec),
        str(mode_for_loop),
        str(exchange_for_loop),
        str(symbol_for_loop),
    )

    loop_symbols = list(symbols_for_loop) or list(getattr(C, "SYMBOLS", []) or [])
    if not loop_symbols:
        loop_symbols = [_default_symbol_for_runtime(exchange_for_loop)]
    loop_ex = ExchangeClient()
    loop_ctx = _state_context_for_run(mode=str(mode_for_loop), symbols=loop_symbols, replay_mode=False)
    loop_store, _ = _open_state_store_for_context(ctx=loop_ctx, symbols=loop_symbols)

    it = 0
    try:
        while True:
            _ = main(
                ex_override=loop_ex,
                store_override=loop_store,
                symbols_override=loop_symbols,
                mode_override=str(mode_for_loop),
                skip_startup_ops=bool(it > 0),
            )
            it += 1
            if loop_log_every > 0 and (it % loop_log_every) == 0:
                logger.info("[SERVE] loop=%d ts=%s", int(it), datetime.now(timezone.utc).isoformat())
            _maybe_log_ttl_stats()
            if loop_max > 0 and it >= loop_max:
                break
            try:
                time.sleep(float(loop_sleep_sec))
            except KeyboardInterrupt:
                break
    except KeyboardInterrupt:
        pass
    finally:
        _maybe_log_ttl_stats(force=True)
        try:
            loop_store.close()
        except Exception:
            pass

    raise SystemExit(0)
