# BUILD_ID: 2026-04-28_config_free_xrpusdt_range_fixed_ceiling_v1
# -*- coding: utf-8 -*-

BUILD_ID = "2026-04-28_config_free_xrpusdt_range_fixed_ceiling_v1"

# XRP/USDT Free range fixed-ceiling preset.
# Ported from standard XRP/USDT confirmed preset after full-period,
# 2024-2025 holdout, 2025-only, and runtime config smoke.
# Fixed executable ceiling is 500 USDT.
# USDC will later copy USDT settings and is not configured here.
# Free port does not enable live.

TRADE_TREND = 0.0
TRADE_RANGE = 1.0
RANGE_ENTRY_MIN_ATR_BPS = 20.0
RANGE_RSI_BUY_MAX = 98.0
RANGE_ATR_TP_MULT = 0.75
RANGE_ATR_SL_MULT = 0.035
RANGE_ENTRY_MAX_EMA21_DIST_BPS = 865.0
RANGE_TIMEOUT_BARS = 24.0
RANGE_TIMEOUT_MIN_PROFIT_BPS = -5.0
RANGE_EARLY_EXIT_LOSS_ATR_MULT = 0.80
RANGE_TRAIL_START_R = 0.6
RANGE_TRAIL_BPS_FROM_HIGH = 6.0
FIXED_NOTIONAL_CEILING_ENABLED = True
FIXED_NOTIONAL_CEILING_BY_SYMBOL = {"XRP/USDT": 500.0}
