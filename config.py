# BUILD_ID: 2026-03-29_free_from_standard_nonlive_build_v1
# BUILD_ID: 2026-03-21_runtime_logs_default_path_v1
# BUILD_ID: 2026-03-21_runtime_layout_contract_v1
# BUILD_ID: 2026-03-21_config_user_preset_search_root_v1
# BUILD_ID: 2026-03-21_config_comment_cleanup_v1
# BUILD_ID: 2026-03-20_config_ethusdc_exchange_meta_align_v1
# BUILD_ID: 2026-03-20_config_btcjpy_market_data_root_v1
# BUILD_ID: 2026-03-20_config_btcjpy_preset_backtest_defaults_v1
# BUILD_ID: 2026-03-19_market_data_root_stage1_v1
# -*- coding: utf-8 -*-

import importlib.util
import os
import logging
import re
from typing import Any
from app.core.paths import get_paths
from app.core.instrument_registry import default_symbol_for_exchange
from app.core.instrument_registry import symbols_for_exchange as registry_symbols_for_exchange

BUILD_ID = "2026-03-29_free_from_standard_nonlive_build_v1"

BUILD_TIER = "FREE"
FREE_ALLOWED_MODES = ("PAPER", "REPLAY", "BACKTEST")
FREE_LIVE_SUPPORTED = False

_RUNTIME_LAYOUT_PATHS = get_paths()


def _repo_relative_path(path_value: str) -> str:
    try:
        rel_path = os.path.relpath(str(path_value or ""), str(_RUNTIME_LAYOUT_PATHS.repo_root or os.path.dirname(__file__)))
    except Exception:
        rel_path = str(path_value or "")
    return str(rel_path).replace("\\", "/")


def _runtime_exports_path(*parts: str) -> str:
    target = os.path.join(
        str(_RUNTIME_LAYOUT_PATHS.exports_dir or os.path.join(os.path.dirname(__file__), "runtime", "exports")),
        *[str(p) for p in parts if str(p or "").strip()],
    )
    return _repo_relative_path(target)


def _runtime_logs_path(*parts: str) -> str:
    target = os.path.join(
        str(_RUNTIME_LAYOUT_PATHS.logs_dir or os.path.join(os.path.dirname(__file__), "runtime", "logs")),
        *[str(p) for p in parts if str(p or "").strip()],
    )
    return _repo_relative_path(target)


# Runtime layout contract:
# - runtime/exports: reports, CSVs, diff traces, support bundles
# - runtime/state: runtime state, caches, market meta, last state
# - runtime/logs: runtime and operations logs
RUNTIME_ROOT = _repo_relative_path(str(_RUNTIME_LAYOUT_PATHS.runtime_dir or os.path.join(os.path.dirname(__file__), "runtime")))
RUNTIME_EXPORTS_DIR = _repo_relative_path(str(_RUNTIME_LAYOUT_PATHS.exports_dir or os.path.join(os.path.dirname(__file__), "runtime", "exports")))
RUNTIME_STATE_DIR = _repo_relative_path(str(_RUNTIME_LAYOUT_PATHS.state_dir or os.path.join(os.path.dirname(__file__), "runtime", "state")))
RUNTIME_LOGS_DIR = _repo_relative_path(str(_RUNTIME_LAYOUT_PATHS.logs_dir or os.path.join(os.path.dirname(__file__), "runtime", "logs")))
RUNTIME_LOG_FILE = _runtime_logs_path("ops.log")
OPS_LOG_FILE = RUNTIME_LOG_FILE
TRADE_LOG_FILE = _runtime_logs_path("trade.log")
ERROR_LOG_FILE = _runtime_logs_path("error.log")
PARITY_PROBE_LOG_FILE = _runtime_logs_path("parity_probe_week_20230314_20230321.jsonl")

BACKTEST_DATASET = str(os.getenv("BACKTEST_DATASET") or "all")
# Symbol preset helpers
def _symbol_to_prefix(symbol: str) -> str:
    s = str(symbol or "").strip().upper()
    s = s.replace("/", "").replace("-", "").replace("_", "")
    s = "".join(ch for ch in s if ch.isalnum())
    return s or "BTCUSDT"


def _quote_from_symbol(symbol: str) -> str:
    raw = str(symbol or "").strip().upper()
    if "/" in raw:
        parts = raw.split("/", 1)
        if len(parts) > 1 and str(parts[1]).strip():
            return str(parts[1]).strip().upper()
    for sep in ("-", "_", ":"):
        if sep in raw:
            parts = raw.split(sep, 1)
            if len(parts) > 1 and str(parts[1]).strip():
                return str(parts[1]).strip().upper()
    return ""


def _split_symbols_env(raw: str) -> list[str]:
    text = str(raw or "").strip()
    if not text:
        return []
    out: list[str] = []
    for tok in text.replace(";", ",").split(","):
        sym = str(tok or "").strip()
        if sym and sym not in out:
            out.append(sym)
    return out


def _first_symbol_from_raw(raw: str) -> str:
    symbols = _split_symbols_env(raw)
    if symbols:
        return symbols[0]
    return str(raw or "").strip()


_symbol_preset_env_raw = str(os.getenv("LWF_SYMBOL_PRESET") or "").strip()
_SYMBOL_PRESET_DEFAULT_SYMBOLS = {
    "BTCJPY": "BTC/JPY",
    "ETHUSDC": "ETH/USDC",
}
_SYMBOL_PRESET_DEFAULT_EXCHANGE_IDS = {
    "BTCJPY": "coincheck",
    "ETHUSDC": "mexc",
}
_symbol_preset_env_symbol = _first_symbol_from_raw(_symbol_preset_env_raw)
_symbol_preset_env_key = _symbol_to_prefix(_symbol_preset_env_symbol)
_symbol_preset_default_symbol = str(_SYMBOL_PRESET_DEFAULT_SYMBOLS.get(_symbol_preset_env_key, "") or "").strip()
EXCHANGE_ID = str(
    os.getenv("LWF_EXCHANGE_ID")
    or _SYMBOL_PRESET_DEFAULT_EXCHANGE_IDS.get(_symbol_preset_env_key, "coincheck")
).strip().lower()
_symbols_env_raw = str(os.getenv("BOT_SYMBOLS") or os.getenv("BOT_SYMBOL") or "").strip()
_symbols_env = _split_symbols_env(_symbols_env_raw)
_registry_symbols = list(registry_symbols_for_exchange(EXCHANGE_ID, include_hidden=False))
_default_symbol = default_symbol_for_exchange(EXCHANGE_ID, fallback="BTC/USDT")

if _symbols_env:
    SYMBOLS = list(_symbols_env)
elif _symbol_preset_default_symbol:
    SYMBOLS = [str(_symbol_preset_default_symbol)]
elif _registry_symbols:
    SYMBOLS = list(_registry_symbols)
else:
    SYMBOLS = [str(_default_symbol)]

# Canonical shipped presets live under configs/; configs/user/ is reserved for future user presets.
_SYMBOL_PRESET_CANONICAL_DIR = os.path.join(os.path.dirname(__file__), "configs")
_SYMBOL_PRESET_USER_DIR = os.path.join(_SYMBOL_PRESET_CANONICAL_DIR, "user")
_SYMBOL_PRESET_SEARCH_DIRS = (
    _SYMBOL_PRESET_USER_DIR,
    _SYMBOL_PRESET_CANONICAL_DIR,
)


def _symbol_preset_filename_key(symbol: str) -> str:
    return _symbol_to_prefix(_first_symbol_from_raw(symbol))


def _resolve_standard_symbol_preset_path(symbol: str) -> str:
    key = _symbol_preset_filename_key(symbol)
    if not key:
        return ""
    preset_filename = f"config_standard_{key}.py"
    for preset_root in _SYMBOL_PRESET_SEARCH_DIRS:
        preset_path = os.path.join(preset_root, preset_filename)
        if os.path.isfile(preset_path):
            return preset_path
    return ""


# Resolve the preset selector before falling back to SYMBOLS[0].
def _resolve_symbol_preset_selector_symbol() -> str:
    for raw in (
        _symbol_preset_env_raw,
        str(os.getenv("BOT_SYMBOLS") or os.getenv("BOT_SYMBOL") or ""),
        str(os.getenv("BACKTEST_CSV_SYMBOL") or ""),
        str(SYMBOLS[0]),
    ):
        symbol = _first_symbol_from_raw(raw)
        if symbol:
            return symbol
    return str(SYMBOLS[0])


_ACTIVE_SYMBOL_PRESET_KEY = _symbol_preset_filename_key(_resolve_symbol_preset_selector_symbol())
_ACTIVE_SYMBOL_PRESET = _resolve_standard_symbol_preset_path(_ACTIVE_SYMBOL_PRESET_KEY)
# Only these names may be overridden by symbol presets.
_SYMBOL_PRESET_ALLOWED_NAMES = {
    "RANGE_ATR_TP_MULT",
    "RANGE_ATR_SL_MULT",
    "RANGE_ENTRY_MIN_ATR_BPS",
    "RANGE_RSI_BUY_MAX",
    "RANGE_ENTRY_MAX_EMA21_DIST_BPS",
    "RANGE_TIMEOUT_BARS",
    "RANGE_TRAIL_START_R",
    "RANGE_TRAIL_BPS_FROM_HIGH",
    "RANGE_UNFAV_EXIT_COOLDOWN_BARS",
    "MAX_POSITION_PCT_OF_EQUITY",
    "MAX_POSITION_NOTIONAL_PCT",
    "LEGACY_COMPOUND_PROFIT_ONLY_ENABLED",
    "LEGACY_COMPOUND_PROFIT_REINVEST_W",
    "SIZE_CAP_RAMP_ENABLED",
    "SIZE_CAP_RAMP_K",
    "SIZE_CAP_RAMP_MAX_PCT",
}


def _load_symbol_preset_module(preset_path: str) -> Any:
    if not preset_path:
        return None
    spec = importlib.util.spec_from_file_location(
        f"_config_symbol_preset_{_ACTIVE_SYMBOL_PRESET_KEY}",
        preset_path,
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"failed to load symbol preset: {preset_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _apply_symbol_preset(preset_path: str) -> None:
    preset_module = _load_symbol_preset_module(preset_path)
    if preset_module is None:
        return
    for name in _SYMBOL_PRESET_ALLOWED_NAMES:
        if not hasattr(preset_module, name):
            continue
        value = getattr(preset_module, name)
        if callable(value) or isinstance(value, type(os)):
            continue
        globals()[name] = value

PAIR_REGISTRY = {
    "coincheck:BTC/JPY": {
        "exchange_id": "coincheck",
        "symbol": "BTC/JPY",
        "market_type": "spot",
        "base_ccy": "BTC",
        "quote_ccy": "JPY",
        "account_ccy": "JPY",
        "settlement_ccy": "JPY",
        "visible": True,
    },
    "mexc:BTC/USDT": {
        "exchange_id": "mexc",
        "symbol": "BTC/USDT",
        "market_type": "spot",
        "base_ccy": "BTC",
        "quote_ccy": "USDT",
        "account_ccy": "USDT",
        "settlement_ccy": "USDT",
        "visible": True,
    },
    "mexc:BTC/USDC": {
        "exchange_id": "mexc",
        "symbol": "BTC/USDC",
        "market_type": "spot",
        "base_ccy": "BTC",
        "quote_ccy": "USDC",
        "account_ccy": "USDC",
        "settlement_ccy": "USDC",
        "visible": False,
        "experimental": True,
    },
    "mexc:ETH/USDT": {
        "exchange_id": "mexc",
        "symbol": "ETH/USDT",
        "market_type": "spot",
        "base_ccy": "ETH",
        "quote_ccy": "USDT",
        "account_ccy": "USDT",
        "settlement_ccy": "USDT",
        "visible": True,
    },
    "mexc:ETH/USDC": {
        "exchange_id": "mexc",
        "symbol": "ETH/USDC",
        "market_type": "spot",
        "base_ccy": "ETH",
        "quote_ccy": "USDC",
        "account_ccy": "USDC",
        "settlement_ccy": "USDC",
        "visible": True,
    },
}

MEXC_MIN_COST_BY_QUOTE = {
    "USDT": 1.0,
    "USDC": 5.0,
}
SELECT_QUOTE = str(os.getenv("SELECT_QUOTE") or _quote_from_symbol(SYMBOLS[0]) or "USDT").strip().upper()
BASE_CURRENCY = str(os.getenv("BASE_CURRENCY") or _quote_from_symbol(SYMBOLS[0]) or "USDT").strip().upper()

# =========================
# RUN SUMMARY
# =========================
LOG_CFG_CHECK = False  # Config summary log
RUN_SUMMARY_ON_HOLD = True
RUN_SUMMARY_ENABLED = False
# =========================
# Live/Backtest diff verification trace (JSONL)
# =========================
# When enabled, runner.py appends one JSONL record per processed entry candle.
# This is used to compare a live day vs backtest day.
DIFF_TRACE_ENABLED = True
DIFF_TRACE_DIR = RUNTIME_EXPORTS_DIR
DIFF_TRACE_PREFIX_LIVE = "diff_trace_live"
DIFF_TRACE_PREFIX_BACKTEST = "diff_trace_backtest"
# =========================
# Runner loop (live daemon)
# =========================
RUNNER_LOOP_SLEEP_SEC = 10.0
# =========================
# Backtest dataset switch
# =========================

_DATASET_CFG = {
    "2021": {
        "since_ms": 1609459200000,  # 2021-01-01 00:00:00 UTC
        "until_ms": 1640995199000,  # 2021-12-31 23:59:59 UTC
        "csv_dir_5m": "binance_ethusdt_5m_2021",
        "csv_dir_1h": "binance_ethusdt_1h_2021",
    },
    "2022": {
        "since_ms": 1640995200000,  # 2022-01-01 00:00:00 UTC
        "until_ms": 1672531199000,  # 2022-12-31 23:59:59 UTC
        "csv_dir_5m": "binance_ethusdt_5m_2022",
        "csv_dir_1h": "binance_ethusdt_1h_2022",
    },
    "2023": {
        "since_ms": 1672531200000,  # 2023-01-01 00:00:00 UTC
        "until_ms": 1704067199000,  # 2023-12-31 23:59:59 UTC
        "csv_dir_5m": "binance_ethusdt_5m_2023",
        "csv_dir_1h": "binance_ethusdt_1h_2023",
    },
    "2024": {
        "since_ms": 1704067200000,  # 2024-01-01 00:00:00 UTC
        "until_ms": 1735689599000,  # 2024-12-31 23:59:59 UTC
        "csv_dir_5m": "binance_ethusdt_5m_2024",
        "csv_dir_1h": "binance_ethusdt_1h_2024",
    },
    "2025": {
        "since_ms": 1735689600000,  # 2025-01-01 00:00:00 UTC
        "until_ms": 1767225599000,  # 2025-12-31 23:59:59 UTC
        "csv_dir_5m": "binance_ethusdt_5m_2025",
        "csv_dir_1h": "binance_ethusdt_1h_2025",
    },
    "all": {
        "since_ms": 1609459200000,  # 2021-01-01 00:00:00 UTC
        "until_ms": 1767225599000,  # 2025-12-31 23:59:59 UTC
        "csv_dir_5m": "binance_ethusdt_5m_all",
        "csv_dir_1h": "binance_ethusdt_1h_all",
    },
}

_ds = _DATASET_CFG.get(str(BACKTEST_DATASET), _DATASET_CFG["2025"])

# Backtest slicing range
SINCE_MS = int(_ds["since_ms"])
UNTIL_MS = int(_ds["until_ms"])


def _default_market_data_bt_dir(prefix: str, tf: str, dataset_cfg: dict[str, Any]) -> str:
    raw = str(dataset_cfg.get(f"csv_dir_{tf}", "") or "").strip()
    m = re.search(rf"_{re.escape(str(tf))}_(.+)$", raw)
    suffix = str(m.group(1)).strip() if m else "all"
    return os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "market_data",
            f"{str(prefix)}_{str(tf)}_{suffix}",
        )
    )


def _default_backtest_csv_defaults(prefix: str, dataset_cfg: dict[str, Any]) -> tuple[str, str, str, str]:
    prefix = str(prefix)
    if prefix == "BTCJPY":
        return (
            _default_market_data_bt_dir(prefix, "5m", dataset_cfg),
            _default_market_data_bt_dir(prefix, "1h", dataset_cfg),
            f"{prefix}-5m-*.csv",
            f"{prefix}-1h-*.csv",
        )
    return (
        f"{prefix}_5m_all",
        f"{prefix}_1h_all",
        f"{prefix}-5m-*.csv",
        f"{prefix}-1h-*.csv",
    )


# Backtest CSV defaults (used by backtest.py / replay).
# BTCJPY defaults follow market_data stage2 naming.
# Legacy GMO ranged exports remain available via BACKTEST_CSV_DIR_5M/1H env overrides when needed.
_default_backtest_csv_symbol = str(os.getenv("BACKTEST_CSV_SYMBOL") or "").strip()
if (not _default_backtest_csv_symbol) and _ACTIVE_SYMBOL_PRESET_KEY == "BTCJPY":
    # Keep BTC/JPY backtest defaults anchored to market_data root when only the preset selector is used.
    _default_backtest_csv_symbol = _resolve_symbol_preset_selector_symbol()
BACKTEST_CSV_SYMBOL = str(_default_backtest_csv_symbol or SYMBOLS[0])
_BACKTEST_PREFIX = _symbol_to_prefix(BACKTEST_CSV_SYMBOL)
(
    _default_bt_dir_5m,
    _default_bt_dir_1h,
    _default_bt_glob_5m,
    _default_bt_glob_1h,
) = _default_backtest_csv_defaults(_BACKTEST_PREFIX, _ds)
BACKTEST_CSV_DIR_5M = str(os.getenv("BACKTEST_CSV_DIR_5M") or _default_bt_dir_5m)
BACKTEST_CSV_DIR_1H = str(os.getenv("BACKTEST_CSV_DIR_1H") or _default_bt_dir_1h)
BACKTEST_CSV_GLOB_5M = str(os.getenv("BACKTEST_CSV_GLOB_5M") or _default_bt_glob_5m)
BACKTEST_CSV_GLOB_1H = str(os.getenv("BACKTEST_CSV_GLOB_1H") or _default_bt_glob_1h)

ENTRY_TF = "5m"
FILTER_TF = "1h"

INITIAL_EQUITY = 300000.0
WARMUP_BARS = 200

# =========================
# Sales risk controls (for long-term stability / bot distribution)
# =========================
# If enabled, runner will stop taking NEW entries for the day when:
# - daily net PnL <= -RISK_DAILY_STOP_LOSS (pct or absolute JPY)
# - consecutive losses >= RISK_MAX_CONSECUTIVE_LOSSES
# Also (NEW): stop taking NEW entries for the rest of the week when:
# - weekly net PnL <= -RISK_WEEKLY_STOP_LOSS (pct or absolute JPY)
RISK_CONTROLS_ENABLED = True
RISK_DAILY_STOP_LOSS_PCT = 2.0      # % of INITIAL_EQUITY (used if *_JPY is None)
RISK_DAILY_STOP_LOSS_JPY = None     # absolute JPY (overrides pct when set)
RISK_MAX_CONSECUTIVE_LOSSES = 3     #3 # block new entries after N losses in a row (same day)
RISK_WEEKLY_STOP_LOSS_PCT = 4.0     # % of INITIAL_EQUITY (used if *_JPY is None, 0 disables)
RISK_WEEKLY_STOP_LOSS_JPY = None    # absolute JPY (overrides pct when set)
# Kill Switch (opt-in, default OFF; backward compatible)
KILL_SWITCH_ENABLED = False
KILL_SWITCH_MODE = "HALT_NEW_ENTRIES"  # "HALT_NEW_ENTRIES" or "EXIT_PROCESS"
KILL_MAX_DD_PCT = 0.0
KILL_MAX_DAILY_LOSS_PCT = 0.0
KILL_MAX_CONSEC_LOSSES = 0
KILL_MAX_SPREAD_BPS = 0.0
KILL_MIN_EQUITY = 0.0
KILL_COOLDOWN_DAYS = 0
KILL_DEBUG_LOG_ENABLED = False
# Report (opt-in, default OFF; backward compatible)
REPORT_ENABLED = False
REPORT_OUT_PATH = _runtime_exports_path("report.json")
REPORT_INCLUDE_YEARLY = True
REPORT_INCLUDE_MONTHLY = False
REPORT_RISK_FREE_RATE = 0.0
# =========================
# Precomputed indicators (optional)
# =========================
PRECOMPUTED_INDICATORS_ENABLED = True
PRECOMPUTED_INDICATORS_OUT_ROOT = "market_data/precomputed_indicators"
PRECOMPUTED_INDICATORS_STRICT = False
# maker fee
PAPER_FEE_RATE_MAKER = 0.0001  # 0.01%
PAPER_FEE_RATE_TAKER = 0.0002  # 0.02%
BINANCE_PAPER_FEE_RATE_MAKER = 0.001  # 0.10%
BINANCE_PAPER_FEE_RATE_TAKER = 0.001  # 0.10%
COINCHECK_FEE_RATE_MAKER = float(os.getenv("COINCHECK_FEE_RATE_MAKER") or 0.0)
COINCHECK_FEE_RATE_TAKER = float(os.getenv("COINCHECK_FEE_RATE_TAKER") or 0.0)

# Spot fee table for cost estimation tools (bps). Override by account tier as needed.
FEE_BPS = {
    "mexc": {"maker": 1.0, "taker": 2.0},
    "binance": {"maker": 10.0, "taker": 10.0},
    "coincheck": {"maker": 0.0, "taker": 0.0},
}

PAPER_ENTRY_FEE_TYPE = "taker"
PAPER_TP_FEE_TYPE = "maker"
PAPER_STOP_FEE_TYPE = "taker"
# backtest spread estimate (bps)
BACKTEST_SPREAD_BPS = 2.0  # 2.0
SLIPPAGE_BPS = 0.0

# =========================
# Fast backtest shortcuts
FAST_BACKTEST = False
FAST_SKIP_ADJUST_TP_SL = False
FAST_SKIP_EXPECTANCY = False

# -------------------------
# TREND TIMEOUT (exit tuning)
# -------------------------
# Base timeout for trend trades (0 disables)
TREND_TIMEOUT_BARS = 3
# Apply TIMEOUT only when entry was EMA9-break-recent (default OFF / safe)
TREND_TIMEOUT_ONLY_ON_EMA9_BREAK_RECENT = False

# =========================
# Trend ATR / momentum thresholds
TREND_PULLBACK_TP_ATR_K = 0.95
TREND_TP_ATR_K = 1.05
TREND_SL_ATR_K = 0.75
RSI_TREND_ENTRY_TH = 58.5

TREND_ADX_TH = 14.0
TREND_EMA_SLOPE_BPS = 0.08
EMA_DIR_BUFFER_BPS = 8.0

EMA_FAST = 20
EMA_SLOW = 50

# =========================
# BE / TRAIL
# =========================
BACKTEST_DISABLE_BE = True
BE_ENABLED = False  # Standard baseline: BE is hard-disabled in runner/replay.
RANGE_ATR_TP_MULT = 0.88423
RANGE_ATR_SL_MULT = 0.00625

# Minimum EMA9 distance for range entry.
RANGE_MIN_E9_DIST_BPS = 0.0
RANGE_ENTRY_MIN_ATR_BPS = 0.0  # Standard range baseline: require ATR/price*1e4 >= this before entering
RANGE_RSI_BUY_MAX = 71.0
RANGE_ENTRY_MAX_EMA21_DIST_BPS = 999.0  # 0 disables the gate.

# =========================
# Range quality filters (entry gate)
# =========================
# Disabled when *_MAX <= 0 or *_MULT <= 0.

# Hold when ATR exceeds its moving average by this multiple.
RANGE_QUALITY_ATR_EXPAND_MA_BARS = 24
RANGE_QUALITY_ATR_EXPAND_MULT = 0.0

# Hold when abs(EMA9 - EMA21) / ATR exceeds this ceiling.
RANGE_QUALITY_EMA_SPREAD_ATR_MAX = 999.0
# =========================
# Optional time window filter
TIME_WINDOW_ENABLED = False
# TIME_WINDOW_JST = [("09:00", "12:00"), ("21:00", "01:00")]

# =========================
# Position sizing and deleveraging
MAX_RISK_PCT = 0.01
POSITION_SIZING_MODE = "LEGACY_COMPOUND"  # Compatibility default.
BASE_RISK_PCT = 0.01
MAX_POSITION_PCT_OF_EQUITY = 0.15
MAX_POSITION_NOTIONAL_PCT = MAX_POSITION_PCT_OF_EQUITY
DD_DELEVER_THRESHOLD = 0.02
DD_DELEVER_MIN_MULT = 0.25
DD_DELEVER_SMOOTH_ENABLED = False
DD_DELEVER_SMOOTH_LAMBDA = 1.0   # 1.0 = no smoothing
DD_DELEVER_SMOOTH_INIT = 0.0
LEGACY_COMPOUND_PROFIT_ONLY_ENABLED = False
LEGACY_COMPOUND_PROFIT_REINVEST_W = 1.0
LEGACY_COMPOUND_PROFIT_ONLY_FLOOR_TO_INITIAL = True
LEGACY_COMPOUND_PROFIT_W_RAMP_ENABLED = False
LEGACY_COMPOUND_PROFIT_W_RAMP_PCT = 0.30
LEGACY_COMPOUND_PROFIT_W_RAMP_SHAPE = 2.0
LEGACY_COMPOUND_PROFIT_W_RAMP_MIN_G = 0.0
LEGACY_COMPOUND_ALPHA = 0.5
LEGACY_COMPOUND_MULT_CAP = 2.5
SIZE_MIN_BUMP_ENABLED = True
SIZE_MIN_BUMP_MAX_PCT_OF_CAP = 0.45
SIZE_CAP_RAMP_ENABLED = True
SIZE_CAP_RAMP_K = 1.25
SIZE_CAP_RAMP_MAX_PCT = 0.22
SIZE_SIZING_DEBUG_LOG_ENABLED = False
PRESETS: dict[str, dict[str, Any]] = {
    "OFF": {},
    "SELL_SAFE": {
        "POSITION_SIZING_MODE": "LEGACY_COMPOUND",
        "SIZE_MIN_BUMP_ENABLED": True,
        "SIZE_MIN_BUMP_MAX_PCT_OF_CAP": 0.35,
        "SIZE_CAP_RAMP_ENABLED": True,
        "SIZE_CAP_RAMP_K": 1.25,
        "SIZE_CAP_RAMP_MAX_PCT": 0.22,
        "SIZE_SIZING_DEBUG_LOG_ENABLED": False,
        "KILL_SWITCH_ENABLED": True,
        "KILL_SWITCH_MODE": "HALT_NEW_ENTRIES",
        "KILL_MAX_DD_PCT": 0.02,
        "KILL_MAX_DAILY_LOSS_PCT": 0.01,
        "KILL_MAX_CONSEC_LOSSES": 6,
        "KILL_MAX_SPREAD_BPS": 15.0,
        "KILL_MIN_EQUITY": 0.0,
        "KILL_COOLDOWN_DAYS": 3,
        "KILL_DEBUG_LOG_ENABLED": False,
    },
}

def apply_preset(name: str) -> None:
    preset_raw = str(name or "")
    preset_name = preset_raw.strip().upper()
    if not preset_name:
        return
    preset_values = PRESETS.get(preset_name)
    if preset_values is None:
        available = ", ".join(sorted(PRESETS.keys()))
        raise SystemExit(f"[PRESET] unknown preset: {preset_raw!r}. available={available}")
    for key, value in preset_values.items():
        globals()[str(key)] = value
    logging.getLogger("config").info("[PRESET] applied: %s", preset_name)
PAPER_TIMEOUT_FEE_TYPE = "taker"
# =========================
# Break-even (BE)
# =========================
BE_TRIGGER_R = 999.0
BE_OFFSET_BPS = 0.0
BE_USE_DYNAMIC_OFFSET = False
RANGE_BE_TRIGGER_R = BE_TRIGGER_R
RANGE_BE_OFFSET_BPS = BE_OFFSET_BPS

MIN_RR_ENTRY = 1.3
MIN_RR_AFTER_ADJUST = 1.2
# Direction-none trend RR floors
MIN_RR_ENTRY_TREND_NONE = 1.40
MIN_RR_AFTER_ADJUST_TREND_NONE = 1.40
MIN_RR_AFTER_ADJUST_TREND_SHORT = 1.50
MIN_TP_COST_MULT = 3.0
MIN_TP_BPS = 10.0
MIN_STOP_BPS = 4.0
MAX_STOP_BPS = 30.0  # cap SL distance; trades needing wider SL are skipped
RANGE_MAX_STOP_BPS = 80.0  # disable hard cap so RANGE_ATR_SL_MULT can actually affect stop distance

# Range stop placement policy:
# - When True: use the tighter stop between (recent_low-based) and (ATR-based) to reduce fat-tail losses.
# - When False: keep legacy behavior (wider of the two).
RANGE_STOP_USE_TIGHTER = True
TP_SPREAD_MULT = 2.0
SL_SPREAD_MULT = 1.0

# =========================
# Range timeout
RANGE_TIMEOUT_BARS = 24
RANGE_TIMEOUT_MIN_PROFIT_BPS = -5.0

# Range exits (early-loss protection)
# - EMA21 break is a "range invalidation" signal (helps avoid large stop-hit losses).
# - loss ATR exit is a soft stop that exits before hard SL when price moves too far against entry.
RANGE_EXIT_ON_EMA21_BREAK = True
RANGE_EXIT_EMA21_BUFFER_BPS = 0.0
RANGE_EARLY_EXIT_LOSS_ATR_MULT = 0.80

# Range EMA9 cross exit is supplemental. Primary range exit is EMA21 break.
RANGE_EXIT_ON_EMA9_CROSS = True

# --- RANGE bearish dump guard (STOP_HIT_LOSS reduction) ---
# When price dumps below both EMA9 and EMA21 with weak RSI, we cut early (market close) to avoid deep stop hits.
RANGE_BEARISH_EXIT_ENABLED = True
RANGE_BEARISH_EXIT_RSI_TH = 50.0
RANGE_BEARISH_EXIT_MIN_LOSS_BPS = 16.0
RANGE_BEARISH_EXIT_REQUIRE_CLOSE_BELOW_EMA9 = True
RANGE_BEARISH_EXIT_REQUIRE_CLOSE_BELOW_EMA21 = True
RANGE_BEARISH_EXIT_REQUIRE_EMA9_BELOW_EMA21 = True
# After RANGE_EMA21_BREAK / RANGE_EARLY_LOSS_ATR, block range re-entry
# until close reclaims EMA21 (+buffer), with a safety max duration (15m bars).
RANGE_EMA21_BREAK_BUF_BPS = 10.0
RANGE_EMA21_BREAK_BLOCK_MAX_BARS = 48
# =========================
# Range: TP redesign (TP1 partial -> remaining TRAIL)
# =========================
# TP1 trigger at +tp1_r * initial_risk (initial_risk = entry - init_stop)
RANGE_TP1_ENABLED = False
RANGE_TP1_TRIGGER_R = 10.0
# close this fraction of initial qty at TP1 (0.5 = half)
RANGE_TP1_QTY_PCT = 0.0
TP1_MOVE_STOP_TO_BE = False
# after TP1, set take_profit = entry * MULT (very far) to effectively disable TP
RANGE_TP_AFTER_TP1_MULT = 100.0
# EMA9-break-recent timeout overrides
# If True, apply TREND TIMEOUT only when the entry was an EMA9-break-recent pullback.
TREND_TIMEOUT_ONLY_ON_EMA9_BREAK_RECENT = True
# If >0, override TREND_TIMEOUT_BARS only for EMA9-break-recent entries.
TREND_TIMEOUT_BARS_RECENT = 6
# Optional: separate keep-profit threshold for EMA9-break-recent entries (bps).
TREND_TIMEOUT_MIN_PROFIT_BPS_RECENT = 0.0
# Optional: separate EMA9-above requirement for EMA9-break-recent entries.
TREND_TIMEOUT_REQUIRE_ABOVE_EMA9_RECENT = True
TREND_TIMEOUT_REQUIRE_BELOW_EMA9_RECENT = False
# =========================
# Logging / CSV export
# =========================
EXPORT_CSV = True
LOG_LEVEL = "INFO"
RUNTIME_LOG_LEVEL = "OPS"

# =========================
# Trend breakout filters
# =========================

TREND_BREAKOUT_BUFFER_BPS = 30.0

TREND_EMA21_FLAT_TOL_BPS = 100.0
TREND_E9_OVER_E21_TOL_BPS = 20.0
# Relax the strong-downtrend filter only on EMA9-break pullback entries.
TREND_PULLBACK_RELAX_DOWNTREND_STRONG_ON_EMA9_BREAK = False
TREND_EMA9_RISE_TOL_BPS = 10

# =========================
# Time-based exits (timeout)
# =========================
RANGE_TIMEOUT_MIN_PROFIT_BPS = -2.0
# =========================
# Trade mode
# =========================
TREND_REQUIRE_SLOPE = False
TREND_BLOCK_DIR_SHORT = False
TRADE_TREND_IF_PULLBACK_STRONG = True
TRADE_RANGE = True
TRADE_ONLY_TREND = False
ALLOW_RANGE_TRADES = True
TREND_RSI_RISE_MIN_DELTA = 0.25
TREND_FOLLOW_BODY_RATIO_MIN = 0.62
TREND_UPPER_WICK_RATIO_MAX = 0.30

# =========================
# Trend entry mode
# =========================
# "BREAKOUT" or "PULLBACK"
TREND_ENTRY_MODE = "PULLBACK"

# =========================
# Pullback continuation (trend)
# =========================
# --- Trend Pullback FT ---
TREND_PULLBACK_REQUIRE_DIR_LONG = False
TREND_PULLBACK_FT_BODY_RATIO_MIN = 0.5
TREND_PULLBACK_FT_UPPER_WICK_RATIO_MAX = 0.3

TREND_PULLBACK_BUY_UPPER_WICK_RATIO_MAX = 0.30
TREND_PULLBACK_BUY_RSI_MIN = 62.0
TREND_PULLBACK_BUY_BODY_RATIO_MIN = 0.65
# Pullback reference EMA: "EMA9" or "EMA21".
TREND_PULLBACK_EMA = "EMA21"

# Follow-through filter (pullback breakout confirmation)
TREND_PULLBACK_FT_UPPER_WICK_RATIO_MAX = 0.45

# Two-stage pullback filter
TREND_PULLBACK_TWO_STAGE = True

# Pullback break mode selector
PULLBACK_BREAK_MODE = "TREND_PULLBACK_BREAK_MODE"
TREND_PULLBACK_BREAK_LOOKBACK = 1        # 1,2,3...
# EMA9 break recent window (bars).
# 2 means current or previous 2 bars.
TREND_PULLBACK_EMA9_BREAK_RECENT_BARS = 48
TREND_PULLBACK_BREAK_ATR_MIN = 0.0
TREND_PULLBACK_BREAK_ATR_FLOOR = 0.00   # absolute minimum break size (ATR). 0 disables.
TREND_PULLBACK_BREAK_ATR_MAX = 999.00   # maximum break size (ATR) to avoid chasing. large disables.
TREND_PULLBACK_BREAK_BUF_BPS = 0.0      # require ref_high*(1+buf_bps/10000) break (0 disables)
TREND_PULLBACK_BREAK_DYNAMIC = True
# When dynamic break is enabled, require breakout distance to exceed an ATR-normalized estimate
# of all-in trading costs (spread/slippage/fees), but keep it within [FLOOR, BREAK_ATR_MIN].
TREND_PULLBACK_BREAK_COST_ATR_MULT = 1.2
# Optional: override estimated cost in bps (if None, uses BACKTEST_SPREAD_BPS/SLIPPAGE_BPS/fees)
# TREND_PULLBACK_BREAK_COST_BPS = None
# HOLD reason priority (smaller = higher priority)
HOLD_REASON_PRIORITY = {
    # Safety / global filters
    "downtrend_global_filter": 0,

    # Trend regime gates (entry feasibility)
    "trend_fail:e9_le_e21": 10,
    "trend_fail:slope": 11,
    "trend_fail:breakout": 12,

    # Risk / execution gates
    "adjust_tp_sl": 20,

    # Momentum confirmations
    "trend_fail:rsi_delta": 30,
    "trend_fail:weak_follow_body": 31,

    # fallback
    "no_entry_condition": 90,
}
HOLD_REASON_CANONICALIZE = True
HOLD_REASON_PRIORITY_TOPN = 20

DOWN_TREND_BLOCK_ADX_EXTRA = 5.0
# === Trend regime: direction none handling ===
# If True, keep blocking entries when 1h regime=trend but direction=none (legacy behavior).
# If False, allow it and rely on stricter breakout/entry RR/adjust gates downstream.
TREND_HOLD_DIR_NONE = True

ATR_TP_MULT_TREND_LONG = 1.60

# Pullback Stage B rebound candle tolerance
TREND_PULLBACK_REBOUND_REQUIRE_BULL = False
TREND_PULLBACK_REBOUND_BULL_TOL_BPS = 10.0
# Pullback body cross EMA requirement
TREND_PULLBACK_REBOUND_REQUIRE_BODY_CROSS_EMA = False

# EMA9 break body ratio threshold
TREND_PULLBACK_EMA9_BODY_RATIO_MIN = 0.60
TREND_PULLBACK_UPPER_WICK_RATIO_MAX = 0.25
TREND_PULLBACK_FT_UPPER_WICK_RATIO_MAX = 0.35

TREND_PULLBACK_MAX_STOP_BPS = 8.0
TRAIL_ENABLED = True
# Trail tuning
TRAIL_START_R = 0.3
TRAIL_BPS_FROM_HIGH = 12.0
TRAIL_ATR_MULT = 1.0
# Pullback break settings
TREND_PULLBACK_BREAK_MODE = "ema9|high"
TREND_PULLBACK_BREAK_LOOKBACK = 1
TREND_PULLBACK_BREAK_BUF_BPS = 0.0

# Pullback frequency control

# EMA9 break relaxation (ATR units, negative = allow below EMA9)
TREND_PULLBACK_BREAK_EMA9_RELAX = 0.0

# Minimum rebound distance after pullback (ATR units)
TREND_PULLBACK_REBOUND_ATR_MIN = 0.30

# Pullback frequency levers

# Minimum EMA9 break distance in ATR (0 = disabled)
TREND_PULLBACK_EMA9_DIST_ATR_MIN = 0.0
TREND_PULLBACK_EMA9_MIN_BREAK_ATR = 0.0  # default
TREND_PULLBACK_RELAX_E9_E21_POS = 1.0
# -------------------------
# Range exits
# -------------------------
RANGE_TIMEOUT_MIN_PROFIT_BPS = -5.0

# -------------------------
# Trend exits (time-based)
# -------------------------
# If True, TREND timeout exits only trigger when close >= EMA9 (avoid cutting during pullback dip).
TREND_TIMEOUT_REQUIRE_ABOVE_EMA9 = True

# -------------------------
# Trend exits (time-based)
# -------------------------
TREND_TIMEOUT_MIN_PROFIT_BPS = 0.0
BE_DYNAMIC_FEE_MULT = 9.0

RANGE_MIN_RR_ENTRY = 0.00
RANGE_EARLY_LOSS_ATR = 0.8
RANGE_EMA21_BUF_BPS = 0.0
RANGE_EMA9_MIN_LOSS_BPS = 10.0
MIN_LOSS_BPS = RANGE_EMA9_MIN_LOSS_BPS
RANGE_EMA9_CROSS_EXIT_MIN_LOSS_BPS = RANGE_EMA9_MIN_LOSS_BPS
RANGE_ATR_EXPAND_N = 20
RANGE_ATR_EXPAND_RATIO = 1.35
TRADE_TREND = 0.0
RANGE_MIN_ATR_BPS = RANGE_ENTRY_MIN_ATR_BPS
RANGE_EXIT_EMA21_BREAK = RANGE_EXIT_ON_EMA21_BREAK
RANGE_EMA9_CROSS_EXIT = RANGE_EXIT_ON_EMA9_CROSS
RANGE_RSI_MAX = RANGE_RSI_BUY_MAX
RANGE_EXIT_RSI_TH = RANGE_BEARISH_EXIT_RSI_TH
RANGE_EMA21_BREAK = RANGE_EXIT_ON_EMA21_BREAK
RANGE_EMA9_CROSS = RANGE_EXIT_ON_EMA9_CROSS
EMA9_MIN_LOSS_BPS = RANGE_EMA9_MIN_LOSS_BPS
RANGE_EXIT_EMA21_BUF_BPS = RANGE_EXIT_EMA21_BUFFER_BPS
RANGE_TRAIL_START_R = 0.90
RANGE_TRAIL_BPS_FROM_HIGH = 8.0
TP1_TRIGGER_R = RANGE_TP1_TRIGGER_R
TP1_QTY_PCT = RANGE_TP1_QTY_PCT
export_diff_trace=True
diff_trace_prefix="diff_trace_live"
diff_trace_source="live"

MAKER_TTL_SEC = 1.0
TTL_STATS_LOG_INTERVAL_SEC = 60
MAX_SPREAD_BPS = 5.0
RANGE_NEAR_LOW_ATR_MULT = 7.5
RANGE_ENTRY_MIN_EMA9_GAP_BPS = 0.0

_apply_symbol_preset(_ACTIVE_SYMBOL_PRESET)
