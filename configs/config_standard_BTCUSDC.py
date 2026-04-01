# BUILD_ID: 2026-04-01_configs_canonical_btcusdt_standard_preset_v2
# -*- coding: utf-8 -*-

BUILD_ID = "2026-04-01_configs_canonical_btcusdt_standard_preset_v2"

# BTC/USDT standard preset values mirrored from config.py.
# Keep this file limited to the names that the symbol preset bridge applies.

RANGE_ATR_TP_MULT = 0.90
RANGE_ATR_SL_MULT = 0.045
RANGE_ENTRY_MIN_ATR_BPS = 28.0
RANGE_RSI_BUY_MAX = 98.0
RANGE_ENTRY_MAX_EMA21_DIST_BPS = 865.0
RANGE_TIMEOUT_BARS = 24
RANGE_TRAIL_START_R = 0.6
RANGE_TRAIL_BPS_FROM_HIGH = 6.0

MAX_POSITION_PCT_OF_EQUITY = 0.15
MAX_POSITION_NOTIONAL_PCT = MAX_POSITION_PCT_OF_EQUITY

LEGACY_COMPOUND_PROFIT_ONLY_ENABLED = False
LEGACY_COMPOUND_PROFIT_REINVEST_W = 1.0

SIZE_CAP_RAMP_ENABLED = True
SIZE_CAP_RAMP_K = 1.25
SIZE_CAP_RAMP_MAX_PCT = 0.22

# ----------------------------------------------------------------------
# Operational notes for BTC/USDT standard preset
# ----------------------------------------------------------------------
# Final proxy frontier (2021-2025, BACKTEST_DATASET=all) was checked with:
#   RANGE_ENTRY_MIN_ATR_BPS = 28.0
#   RANGE_RSI_BUY_MAX = 98.0
#   RANGE_ENTRY_MAX_EMA21_DIST_BPS = 865.0
#   RANGE_TRAIL_START_R = 0.6
#   RANGE_TRAIL_BPS_FROM_HIGH = 6.0
#
# Proxy slippage interpretation under the tested assumptions:
# - Strong acceptable zone: SLIPPAGE_BPS <= 6
# - Degraded but still acceptable zone: SLIPPAGE_BPS = 7 to 8
# - Severe degradation zone: SLIPPAGE_BPS >= 9
#
# Note:
# Even at SLIPPAGE_BPS = 9, net stayed positive in the final proxy frontier,
# but performance was materially degraded versus lower-slippage cases.
#
# Before taking this preset into live or paper trading, it is recommended to:
# 1) monitor real spread and real slippage continuously, and
# 2) define a stop condition for degraded execution quality.