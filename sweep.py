# BUILD_ID: 2026-03-31_sweep_metrics_fallback_and_per_run_artifacts_v1
# FILE_TOKEN: strategy 2026-02-03T15:46JST v7

#python sweep.py --mode EXIT_GEOMETRY --out exports\sweep_exit_geometry.csv
#python sweep.py --mode PB_BREAK_FILTER --out exports\sweep_pb_break_filter.csv
#python sweep.py --mode PB_BREAK_FILTER_WIDE --out exports\sweep_pb_break_filter_wide.csv
#python sweep.py --mode PB_FT_FILTER --out exports/sweep_pb_ft_filter.csv
#python sweep.py --mode BE_OFFSET_BPS
#python sweep.py --mode PB_STOP_X_BE --out exports\sweep_pb_stop_x_be.csv
#python sweep.py --mode TREND_E9_OVER_E21_TOL_BPS
#python sweep.py --mode TRADE_FREQ_100 --out exports\sweep_trade_freq_100.csv
#python sweep.py --mode TRADE_FREQ_100_V2 --out exports\sweep_trade_freq_100_v2.csv
#python sweep.py --mode ENTRY_RR_TUNING_FOR_FREQ --out exports\sweep_entry_rr_turning_for_freq.csv
#python sweep.py --mode EMA_BREAK_PULLBACK --out exports\sweep_ema_break_pullback.csv
#python sweep.py --mode EMA9_RELAX_X_REBOUND_DIST --out exports\sweep_ema9_relax_x_rebound_dist.csv --include-debug
#python sweep.py --mode EMA9_RELAX_X_REBOUND_DIST_X_RANGE --out exports/sweep_ema9_relax_x_rebound_dist_x_range.csv --include-debug
#python sweep.py --mode EMA9_RELAX_X_REBOUND_DIST_X_RANGE_X_TIMEOUT --out exports/sweep_ema9_relax_x_rebound_dist_x_range_x_timeout.csv --include-debug
#python sweep.py --mode EMA9_BREAK_RECENT_ONLY --out exports/sweep_ema9_break_recent_only.csv --include-debug
#python sweep.py --mode TREND_EXIT_REFINED --out exports/sweep_Trend_exit_refined.csv --include-debug
#python sweep.py --mode BE_MINI_12 --out exports/sweep_be_mini_12.csv --include-debug
#python sweep.py --mode EXIT_TUNE_TREND_TIMEOUT_X_TRAIL --out exports/sweep_exit_tune_trend_Timeout_x_trail.csv --include-debug
#python sweep.py --mode TIMEOUT_ONLY_TREND_COARSE --out exports/sweep_timeout_only_trend_coarse.csv --include-debug
#python sweep.py --mode TIMEOUT_ONLY_TREND_FINE --out exports/sweep_timeout_only_trend_fine.csv --include-debug
#python sweep.py --mode TIMEOUT_TREND_REQABOVE_X_MINPROFIT --out exports/sweep_timeout_reqabove_x_minprofit.csv --include-debug
#python sweep.py --mode TRAIL_START_R_TUNE --out exports/sweep_trail_start_r_tune.csv --include-debug
#python sweep.py --mode TRAIL_FIRE_CONFIRM_BPS --out exports/sweep_trail_fire_confirm_bps.csv --include-debug
#python sweep.py --mode TRAIL_BPS_LIVE_VALIDATE --out exports/sweep_trail_bps_live_validate.csv --include-debug
#python sweep.py --mode TRAIL_EXIT_REASON_SHIFT --out exports/sweep_trail_exit_reason_shift.csv --export-csv  # +(--include-debug optional)
#python sweep.py --mode RANGE_MAX_STOP_BPS --out exports/sweep_range_max_stop_bps.csv --include-debug
#python sweep.py --mode SWEEP_RANGE_V2_R80 --out exports/sweep_range_v2_r80.csv --include-debug
#python sweep.py --mode SWEEP_RANGE_R_MAXSTOP_FINE --out exports/sweep_range_r_maxstop_fine.csv --include-debug
#python sweep.py --mode SWEEP_RANGE_R_EARLY_LOSS --out exports/sweep_range_r_early_loss.csv --include-debug
#python sweep.py --mode SWEEP_RANGE_R_EARLY_LOSS_FINE --out exports/sweep_range_r_early_loss_fine.csv --include-debug
#python sweep.py --mode RANGE_EXIT_RSI_X_MINLOSS_MINI16 --out exports/sweep_range_exit_rsi_x_minloss_mini16.csv --include-debug
#python sweep.py --mode RANGE_EXIT_RSI_X_MINLOSS_FINE30 --out exports/sweep_range_exit_rsi_x_minloss_fine30.csv --include-debug
#python sweep.py --mode RANGE_EXIT_RSI_X_MINLOSS_FINE12 --out exports/sweep_range_exit_rsi_x_minloss_fine12.csv --include-debug
#python sweep.py --mode RANGE_EXIT_RSI_X_MINLOSS_ROUTEA_GRID24 --out exports/sweep_range_exit_rsi_x_minloss_routeA_grid24.csv --include-debug
#python sweep.py --mode RANGE_EXIT_RSI_X_MINLOSS_ROUTEA_FINE24 --out exports/sweep_range_exit_rsi_x_minloss_routeA_fine24.csv --include-debug
#python sweep.py --mode RANGE_EXIT_RSI_X_MINLOSS_ROUTEA_FINE30_NEXT --out exports/sweep_range_exit_rsi_x_minloss_routeA_fine30_next.csv --include-debug
#python sweep.py --mode RANGE_EXIT_RSI_X_MINLOSS_ROUTEA_FINE30 --out exports/sweep_range_exit_rsi_x_minloss_routeA_fine30.csv --include-debug
#python sweep.py --mode RANGE_EXIT_RSI_X_MINLOSS_ROUTEA_NEXT24 --out exports/sweep_range_exit_rsi_x_minloss_routeA_next24.csv --include-debug
#python sweep.py --mode RANGE_EXIT_RSI_X_MINLOSS_ROUTEA_NEXT36 --out exports/sweep_range_exit_rsi_x_minloss_routeA_next36.csv --include-debug
#python sweep.py --mode RANGE_EXIT_RSI_X_MINLOSS_ROUTEA_GRID24_2D --out exports/sweep_range_exit_rsi_x_minloss_routeA_grid24_2d.csv --include-debug
#python sweep.py --mode RANGE_EXIT_RSI_X_MINLOSS_ROUTEA_GRID36_2D --out exports/sweep_range_exit_rsi_x_minloss_routeA_grid36_2d.csv --include-debug
#python sweep.py --mode RANGE_EXIT_RSI_X_MINLOSS_ROUTEA_GRID36_STOP0 --out exports/sweep_range_exit_rsi_x_minloss_routeA_grid36_stop0.csv --include-debug
#python sweep.py --mode SWEEP_RANGE_UNFAV_EXIT_COOLDOWN_BARS --out exports/sweep_range_unfav_exit_cooldown_bars.csv --include-debug
#python sweep.py --mode RANGE_SL_X_EARLYLOSS_GRID36_STOP0 --out exports/sweep_range_sl_x_earlyloss_grid36_stop0.csv --include-debug
#python sweep.py --mode RANGE_SL_X_EARLYLOSS_GRID36 --out exports/sweep_range_sl_x_earlyloss_grid36.csv --include-debug
#python sweep.py --mode RANGE_SL_X_EARLYLOSS_GRID36_MAXSTOP70 --out exports/sweep_range_sl_x_earlyloss_grid36_maxstop70.csv --include-debug
#python sweep.py --mode RANGE_MAXSTOP_X_EARLYLOSS_GRID36 --out exports/sweep_range_maxstop_x_earlyloss_grid36.csv --include-debug
#python sweep.py --mode RANGE_SL_X_EARLYLOSS_ULTRA_GRID36_MAXSTOP70 --out exports/sweep_range_sl_x_earlyloss_ultra_grid36_maxstop70.csv --include-debug
#python sweep.py --mode RANGE_SL_X_EARLYLOSS_ULTRA_GRID24_MAXSTOP70 --out exports/sweep_range_sl_x_earlyloss_ultra_grid24_maxstop70.csv --include-debug
#python sweep.py --mode RANGE_SL_X_EARLYLOSS_GRID24_MAXSTOP70_80 --out exports/sweep_range_sl_x_earlyloss_grid24_maxstop70_80.csv --include-debug
#python sweep.py --mode RANGE_SL_X_EARLYLOSS_FINE24 --out exports/sweep_range_sl_x_earlyloss_fine24.csv --include-debug
#python sweep.py --mode RANGE_QUALITY_FILTER_GRID24 --out exports/sweep_range_quality_filter_grid24.csv --include-debug
#python sweep.py --mode SWEEP_RANGE_ATR_TP_MULT --out exports/sweep_range_atr_tp_mult.csv --include-debug
#python sweep.py --mode SWEEP_RANGE_ATR_SL_MULT --out exports/sweep_range_atr_sl_mult.csv --include-debug
#python sweep.py --mode SWEEP_RANGE_ATR_TP_FINE --out exports/sweep_range_atr_tp_fine.csv --include-debug
#python sweep.py --mode SWEEP_RANGE_ATR_SL_FINE --out exports/sweep_range_atr_sl_fine.csv --include-debug
#python sweep.py --mode SWEEP_RANGE_ATR_TP_ULTRA_FINE --out exports/sweep_range_atr_tp_ultra_fine.csv --include-debug
#python sweep.py --mode SWEEP_RANGE_ATR_SL_ULTRA_FINE --out exports/sweep_range_atr_sl_ultra_fine.csv --include-debug
#python sweep.py --mode SWEEP_RANGE_MIN_ATR_BPS --out exports/sweep_range_min_atr_bps.csv --include-debug
#python sweep.py --mode SWEEP_RANGE_NEAR_LOW_ATR_MULT --out exports/sweep_range_near_low_atr_mult.csv --include-debug
#python sweep.py --mode SWEEP_RANGE_RSI_MAX --out exports/sweep_range_rsi_max.csv --include-debug
#python sweep.py --mode SWEEP_RANGE_EARLY_LOSS_ATR --out exports/sweep_range_early_loss_atr.csv --include-debug
#python sweep.py --mode SWEEP_RANGE_EMA21_BUF_BPS --out exports/sweep_range_ema21_buf_bps.csv --include-debug
#python sweep.py --mode SWEEP_RANGE_EMA9_MIN_LOSS_BPS --out exports/sweep_range_ema9_min_loss.csv --include-debug
#python sweep.py --mode SWEEP_RANGE_EARLY_LOSS_ATR_FINE --out exports/sweep_range_early_loss_atr_fine.csv --include-debug
#python sweep.py --mode RANGE_FINALIZE_EARLYLOSS_X_MINATR_X_TP --out exports/sweep_range_finalize_earlyloss_x_minatr_x_tp.csv --include-debug
#python sweep.py --mode RANGE_SL_ATR_X_TP_ATR_ROUTEA_V1 --out exports/sweep_range_sl_atr_x_tp_atr_routeA_v1.csv --include-debug
#python sweep.py --mode RANGE_SL_ATR_X_TP_ATR_ROUTEA_V2 --out exports/sweep_range_sl_atr_x_tp_atr_routeA_v2.csv --include-debug
#python sweep.py --mode RANGE_SL_ATR_X_TP_ATR_ROUTEA_V3_FINE25 --out exports/sweep_range_sl_atr_x_tp_atr_routeA_v3_fine25.csv --include-debug
#python sweep.py --mode RANGE_TP_EFFECT_DIAG_EXIT_COUNTS --out exports/sweep_range_tp_effect_diag_exit_counts.csv --include-debug --export-csv
#python sweep.py --mode RANGE_EMA9_CROSS_MINLOSS_ONLY_V1 --out exports/sweep_range_ema9_cross_minloss_only_v1.csv --include-debug
#python sweep.py --mode RANGE_EMA9_CROSS_MINLOSS_ONLY_V2 --out exports/sweep_range_ema9_cross_minloss_only_v2.csv --include-debug
#python sweep.py --mode RANGE_EMA9_CROSS_MINLOSS_ONLY_V3 --out exports/sweep_range_ema9_cross_minloss_only_v3.csv --include-debug
#python sweep.py --mode RANGE_EMA9_CROSS_EXIT_MIN_LOSS_BPS_SWEEP_A --out exports/sweep_range_ema9_cross_exit_minloss_sweep_a.csv --include-debug
#python sweep.py --mode RANGE_EMA9_CROSS_EXIT_MIN_LOSS_BPS_SWEEP_A_WIDE --out exports/sweep_range_ema9_cross_exit_minloss_sweep_a_wide.csv --include-debug
#python sweep.py --mode RANGE_EMA9_CROSS_EXIT_MIN_LOSS_BPS_DIAG_ULTRAWIDE --out exports/sweep_range_ema9_cross_exit_minloss_diag_ultrawide.csv --include-debug --export-csv
#python sweep.py --mode RANGE_RSI_X_EMA21DIST_MINI16 --out exports\sweep_range_rsi_x_ema21dist_mini16.csv --include-debug --export-csv
#python sweep.py --mode RANGE_E9DIST_X_RSI_BUY_MAX_MINI16 --out exports\sweep_range_e9dist_x_rsi_buymax_mini16.csv --include-debug --export-csv
#python sweep.py --mode RANGE_E9DIST_HIGH_X_RSI_BUY_MAX_M12 --out exports\sweep_range_e9dist_high_x_rsi_buymax_m12.csv --include-debug --export-csv
#python sweep.py --mode RANGE_E9DIST_ULTRA_X_RSI_BUY_MAX_M12 --out exports\sweep_range_e9dist_ultra_x_rsi_buymax_m12.csv --include-debug --export-csv
#python sweep.py --mode RANGE_SL_X_EMA21DIST_RSI62_M20 --out exports\sweep_range_sl_x_ema21dist_rsi62_m20.csv --include-debug --export-csv
#python sweep.py --mode RANGE_TP_X_EMA21DIST_RSI62_M12 --out exports\sweep_range_tp_x_ema21dist_rsi62_m12.csv --include-debug --export-csv
#python sweep.py --mode RANGE_MINATR_X_RSI_BUYMAX_M16 --out exports/sweep_range_minatr_x_rsi_buymax_m16.csv --include-debug
#python sweep.py --mode RANGE_MINATR_X_RSI_BUYMAX_FINE9 --out exports/sweep_range_minatr_x_rsi_buymax_fine9.csv --include-debug
#python sweep.py --mode RANGE_MINATR_X_E9DIST_FINE12 --out exports/sweep_range_minatr_x_e9dist_fine12.csv --include-debug --export-csv
#python sweep.py --mode RANGE_ENTRY_EMA21DIST_X_RSI_BUYMAX_MATR13_L12 --out exports/sweep_range_entry_emadist_x_rsi_buymax_matr13_l12.csv --include-debug --export-csv
#python sweep.py --mode RANGE_EARLYLOSS_ATR_X_EMA9MINLOSS_M20 --out exports/sweep_range_earlyloss_atr_x_ema9minloss_m20.csv --include-debug --export-csv
#python sweep.py --mode RANGE_TRAIL_START_R_X_TRAIL_BPS_FROM_HIGH_L16 --out exports/sweep_range_trail_start_r_x_trail_bps_from_high_l16.csv --include-debug --export-csv
#python sweep.py --mode RANGE_TRAIL_START_R_HIGH_X_TRAIL_BPS_FROM_HIGH_L16 --out exports/sweep_range_trail_start_r_high_x_trail_bps_from_high_l16.csv --include-debug --export-csv
#python sweep.py --mode RANGE_TRAIL_FIRE_CONFIRM_START_R_X_BPS_L12 --out exports/sweep_range_trail_fire_confirm_start_r_x_bps_l12.csv --include-debug --export-csv
#python sweep.py --mode RANGE_TRAIL_START_R_BIND_X_BPS_L12 --out exports/sweep_range_trail_start_r_bind_x_bps_l12.csv --include-debug --export-csv
#python sweep.py --mode RANGE_TP1_TRIGGER_R_X_TP1_QTY_PCT_L12 --out exports/sweep_range_tp1_trigger_r_x_tp1_qty_pct_l12.csv --include-debug --export-csv
#python sweep.py --mode RANGE_TP1_TRIGGER_R_FINE_X_TP1_QTY_PCT_FINE_L16 --out exports/sweep_range_tp1_trigger_r_fine_x_tp1_qty_pct_fine_l16.csv --include-debug --export-csv
#python sweep.py --mode RANGE_BE_TRIGGER_R_X_BE_OFFSET_BPS_L16 --out exports/sweep_range_be_trigger_r_x_be_offset_bps_l16.csv --include-debug --export-csv
#python sweep.py --mode RANGE_TP1_TRIGGER_R_HIGH_X_TP1_QTY_PCT_FINE_L16 --out exports/sweep_range_tp1_trigger_r_high_x_tp1_qty_pct_fine_l16.csv --include-debug --export-csv
#python sweep.py --mode RANGE_BE_DYNAMIC_FEE_MULT_X_BE_TRIGGER_R_L16 --out exports/sweep_range_be_dynamic_fee_mult_x_be_trigger_r_l16.csv --include-debug --export-csv
#python sweep.py --mode STRESS_TEST --out exports/sweep_stress_test.csv --include-debug --export-csv
#python sweep.py --mode RANGE_NO_BE_STRUCTURE_TEST_TP1_L2304 --out exports/sweep_range_no_be_structure_test_tp1_l2304.csv --include-debug --export-csv
#python sweep.py --mode RANGE_NO_BE_STRUCTURE_TEST_TP1_L9216 --out exports/sweep_range_no_be_structure_test_tp1_l9216.csv --include-debug --export-csv
#python sweep.py --mode RANGE_NO_BE_STRUCTURE_TEST_TP1_TRAILKEYS_L2304 --out exports/sweep_range_no_be_structure_test_tp1_trailkeys_l2304.csv --include-debug --export-csv
#python sweep.py --mode RANGE_NO_BE_STRUCTURE_TEST_TP1_TRAILKEYS_L9216 --out exports/sweep_range_no_be_structure_test_tp1_trailkeys_l9216.csv --include-debug --export-csv
#python sweep.py --mode RANGE_NO_BE_PRIORITY_STAGE1_TRAIL_L36 --out exports/sweep_range_no_be_priority_stage1_trail_l36.csv --include-debug
#python sweep.py --mode RANGE_NO_BE_PRIORITY_STAGE3_TP1_L16 --out exports/sweep_range_no_be_priority_stage3_tp1_l16.csv --include-debug
#python sweep.py --mode RANGE_NO_BE_PRIORITY_STAGE2_TP_SL_L16 --out exports/sweep_range_no_be_priority_stage2_tp_sl_l16.csv --include-debug
    # 追加（起きてる間に回す短時間優先度順）:
    #   python sweep.py --mode RANGE_NO_BE_P1_TRAILKEYS_L36 --out exports/sweep_range_no_be_p1_trailkeys_l36.csv --include-debug
    #   python sweep.py --mode RANGE_NO_BE_P2_SL_X_EARLYLOSS_L12 --out exports/sweep_range_no_be_p2_sl_x_earlyloss_l12.csv --include-debug
    #   python sweep.py --mode RANGE_NO_BE_P3_TP1_NARROW_L12 --out exports/sweep_range_no_be_p3_tp1_narrow_l12.csv --include-debug
    #   python sweep.py --mode RANGE_NO_BE_P4_EMA9MINLOSS_X_EMA21BUF_L12 --out exports/sweep_range_no_be_p4_ema9minloss_x_ema21buf_l12.csv --include-debug
    #   python sweep.py --mode RANGE_NO_BE_P5_TP1_X_TRAILBPS_L36 --out exports/sweep_range_no_be_p5_tp1_x_trailbps_l36.csv --include-debug
    #   python sweep.py --mode RANGE_NO_BE_P6_VALIDATE_TOP_L12 --out exports/sweep_range_no_be_p6_validate_top_l12.csv --include-debug
    # ------------------------------------------------------------
#python sweep.py --mode RANGE_NO_BE_P1_TRAILKEYS_MICRO_L36 --out exports\sweep_range_no_be_p1_trailkeys_micro_l36.csv --include-debug
#python sweep.py --mode RANGE_NO_BE_P3_TP1_QTYHIGH_L12 --out exports\sweep_range_no_be_p3_tp1_qtyhigh_l12.csv --include-debug
#python sweep.py --mode RANGE_NO_BE_P4_BEARISH_EXIT_RSI_X_MINLOSS_L12 --out exports\sweep_range_no_be_p4_bearish_exit_rsi_x_minloss_l12.csv --include-debug
#python sweep.py --mode SPREAD_SLIPPAGE_RESISTANCE_TEST --out exports/sweep_spread_slippage_resistance_test.csv --include-debug --export-csv
#python sweep.py --mode SWEEP_MAX_SPREAD_BPS --out exports/SWEEP_MAX_SPREAD_BPS.csv --include-debug --export-csv
#python sweep.py --mode LIVE_LONG_GRID_SL_TIGHT_L8 --out exports/live_long_grid_sl_tight_l8.csv --include-debug --export-csv
#python sweep.py --mode LIVE_LONG_GRID_SL_TIGHT_FINE_L6 --out exports/live_long_grid_sl_tight_fine_l6.csv --include-debug --export-csv
#python sweep.py --mode LIVE_LONG_GRID_SL_MICRO_EDGE_L4 --out exports/live_long_grid_sl_micro_edge_l4.csv --include-debug --export-csv

#--include-debug
#--min-trades 100
from __future__ import annotations

import argparse
import csv
import hashlib
import inspect
import json
import os
import itertools
import shutil
import time
from typing import Any

import config as C
import app.core.dataset as dataset_mod
from backtest import run_backtest

BUILD_ID = "2026-03-31_sweep_metrics_fallback_and_per_run_artifacts_v1"
DD_PENALTY_MULT = 100000.0
_SWEEP_RESERVED_KEYS = {"__preset__", "__combos__"}
_RUN_BACKTEST_PARAM_NAMES = set(inspect.signature(run_backtest).parameters.keys())

from sweep_presets import SWEEP_PRESETS, _file_meta_json

try:
    from sweep_presets_local import SWEEP_PRESETS_LOCAL
except ImportError:
    SWEEP_PRESETS_LOCAL = {}

if SWEEP_PRESETS_LOCAL:
    SWEEP_PRESETS = {**SWEEP_PRESETS, **SWEEP_PRESETS_LOCAL}


def resolve_latest_dataset_timestamp(
    *,
    dataset_root: str,
    prefix: str,
    tf: str = "5m",
) -> int | None:
    resolver = getattr(dataset_mod, "resolve_latest_dataset_timestamp", None)
    if callable(resolver):
        return resolver(dataset_root=dataset_root, prefix=prefix, tf=tf)

    root = dataset_mod._resolve_dataset_root(dataset_root)
    prefix_n = dataset_mod.symbol_to_prefix(prefix)
    tf_n = dataset_mod._normalize_tf(tf, "5m")
    years = dataset_mod._available_years(root, prefix_n, tf_n)
    for year in reversed(years):
        dir_path = os.path.join(root, f"{prefix_n}_{int(year)}")
        dir_path = dataset_mod._resolve_tf_dir(dir_path, tf_n)
        paths = dataset_mod._collect_tf_paths(
            dir_path=dir_path,
            prefix=prefix_n,
            tf=tf_n,
            year=int(year),
            month=None,
        )
        for path in reversed(paths):
            last_ts = dataset_mod._read_last_timestamp_from_csv(path)
            if last_ts is not None:
                return int(last_ts)
    return None

def _validate_preset_keys(mode: str, *, allow_new_keys: bool) -> None:
    """
    Validate that preset keys exist in config.py.
    If allow_new_keys is True, skip validation (not recommended).
    """
    if allow_new_keys:
        return

    if mode not in SWEEP_PRESETS:
        raise ValueError(f"Unknown mode: {mode}. Available: {list(SWEEP_PRESETS.keys())}")

    preset = SWEEP_PRESETS[mode]
    combo_keys: list[str] = []
    raw_combos = preset.get("__combos__")
    if raw_combos is not None:
        if not isinstance(raw_combos, list):
            raise ValueError(f"[SWEEP] mode={mode} __combos__ must be a list")
        for combo in raw_combos:
            if not isinstance(combo, dict):
                raise ValueError(f"[SWEEP] mode={mode} __combos__ entries must be dict")
            for key in combo.keys():
                if key not in combo_keys:
                    combo_keys.append(str(key))

    missing: list[str] = []
    keys_to_validate = combo_keys or [k for k in preset.keys() if k not in _SWEEP_RESERVED_KEYS]
    for k in keys_to_validate:
        if not hasattr(C, k):
            missing.append(k)

    if missing:
        available = [x for x in dir(C) if x.isupper()]
        msg = (
            f"[SWEEP] config.py 縺ｫ蟄伜惠縺励↑縺・く繝ｼ縺・preset 縺ｫ蜷ｫ縺ｾ繧後※縺・∪縺・ {missing}\n"
            f"mode={mode}\n"
            f"蟇ｾ蜃ｦ:\n"
            f"  1) config.py 縺ｫ隧ｲ蠖薙く繝ｼ繧定ｿｽ蜉縺吶ｋ・域耳螂ｨ・噂n"
            f"  2) 縺ｩ縺・＠縺ｦ繧ゆｸ譎ゅく繝ｼ繧定ｨｱ蜿ｯ縺吶ｋ縺ｪ繧・--allow-new-keys 繧剃ｻ倥￠繧具ｼ磯撼謗ｨ螂ｨ・噂n"
            f"蜿り・ config.py 縺ｮ螟ｧ譁・ｭ励く繝ｼ萓・ {available[:40]}{'...' if len(available) > 40 else ''}"
        )
        raise ValueError(msg)

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sweep presets for backtest runs.")
    parser.add_argument(
        "--mode",
        default="BE_OFFSET_BPS",
        choices=list(SWEEP_PRESETS.keys()),
        help="Preset mode for sweep.",
    )
    parser.add_argument(
        "--allow-new-keys",
        action="store_true",
        help="Allow preset keys that do not exist in config.py (NOT recommended).",
    )
    parser.add_argument(
        "--out",
        default="exports/sweep_results.csv",
        help="Output CSV path.",
    )
    parser.add_argument(
        "--include-debug",
        action="store_true",
        help="Add JSON columns for pullback_funnel / hold_reasons_top12 to diagnose sweeps.",
    )
    parser.add_argument(
        "--min-trades",
        type=int,
        default=0,
        help="Minimum trades threshold (0 disables).",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=[str(getattr(C, "BACKTEST_CSV_SYMBOL", "BTC/JPY") or "BTC/JPY")],
        help="Symbols to backtest.",
    )
    parser.add_argument(
        "--since-ms",
        default="None",
        help="Start timestamp in ms (use 'None' to disable).",
    )
    parser.add_argument(
        "--until-ms",
        default="None",
        help="End timestamp in ms; when omitted, sweep resolves to the latest dataset timestamp.",
    )
    parser.add_argument(
        "--entry-tf",
        default="5m",
        help="Entry timeframe.",
    )
    parser.add_argument(
        "--filter-tf",
        default="1h",
        help="Filter timeframe.",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=300,
        help="Warmup bars.",
    )
    parser.add_argument(
        "--initial-equity",
        type=float,
        default=300000.0,
        help="Initial equity.",
    )
    parser.add_argument(
        "--export-csv",
        action="store_true",
        help="Export CSV from backtest.",
    )
    parser.add_argument(
        "--filter-stop-hit-loss-zero",
        action="store_true",
        help="Only keep sweep rows where STOP_HIT_LOSS count is 0 (requires trades.csv export).",
    )
    parser.add_argument(
        "--recent-bars-entry",
        default="None",
        help="Recent bars for entry (use 'None' to disable).",
    )
    parser.add_argument(
        "--recent-bars-filter",
        default="None",
        help="Recent bars for filter (use 'None' to disable).",
    )
    return parser

def _parse_optional_int(value: str | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, str) and value.lower() == "none":
        return None
    return int(value)


def _resolve_sweep_until_ms(
    *,
    symbols: list[str],
    entry_tf: str,
    until_ms: int | None,
) -> tuple[int | None, bool]:
    if until_ms is not None:
        return int(until_ms), False
    latest_candidates: list[int] = []
    for symbol in list(symbols or []):
        latest_ts = resolve_latest_dataset_timestamp(
            dataset_root=os.path.abspath("."),
            prefix=str(symbol),
            tf=str(entry_tf or "5m"),
        )
        if latest_ts is not None:
            latest_candidates.append(int(latest_ts))
    if not latest_candidates:
        return None, True
    return max(latest_candidates), True

def _coerce_combo_value(v: Any) -> Any:
    """
    Sweep preset values may include non-floats (e.g., TREND_ENTRY_MODE="PULLBACK").
    - Keep bool as bool (avoid float(True)=1.0)
    - Convert int/float to float for numeric params
    - Keep others (str, etc.) as-is
    """
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return float(v)
    return v


def _iter_combos(preset: dict[str, list[Any]]) -> list[dict[str, Any]]:
    raw_combos = preset.get("__combos__")
    if raw_combos is not None:
        combos: list[dict[str, Any]] = []
        for combo in raw_combos:
            row: dict[str, Any] = {}
            for key, val in dict(combo).items():
                row[str(key)] = _coerce_combo_value(val)
            combos.append(row)
        return combos

    keys = [key for key in preset.keys() if key not in _SWEEP_RESERVED_KEYS]
    values = [preset[key] for key in keys]
    combos: list[dict[str, Any]] = []
    for product in itertools.product(*values):
        row = {}
        for key, val in zip(keys, product):
            # 譌｢蟄・sweep 縺ｫ縺ｯ "PULLBACK" 縺ｮ繧医≧縺ｪ譁・ｭ怜・縺悟・繧九％縺ｨ縺後≠繧九◆繧√・
            # 菴輔〒繧・float() 縺ｯ縺励↑縺・ｼ・rade=0 險ｺ譁ｭ縺ｮ蜑肴署繧貞｣翫＆縺ｪ縺・ｼ・
            if isinstance(val, (int, float)):
                row[key] = float(val)
                continue
            try:
                row[key] = float(val)
            except Exception:
                row[key] = val
        combos.append(row)
    return combos


def _param_columns_for_preset(preset: dict[str, Any], combos: list[dict[str, Any]]) -> list[str]:
    direct_keys = [key for key in preset.keys() if key not in _SWEEP_RESERVED_KEYS]
    if direct_keys:
        return direct_keys
    ordered: list[str] = []
    for combo in combos:
        for key in combo.keys():
            if key not in ordered:
                ordered.append(str(key))
    return ordered

_MISSING = object()


def _apply_overrides(overrides: dict[str, Any]) -> dict[str, Any]:
    """
    Apply overrides into config module (config as C).
    Returns a dict of old values so we can restore later.
    If a key didn't exist on config, we store a sentinel and delete on restore.
    """
    # Backward-compat aliases (older sweep presets / CSVs)
    # NOTE: backtest/strategy expects RANGE_EARLY_EXIT_LOSS_ATR_MULT.
    canon = dict(overrides)
    if "RANGE_EARLY_LOSS_ATR" in canon and "RANGE_EARLY_EXIT_LOSS_ATR_MULT" not in canon:
        canon["RANGE_EARLY_EXIT_LOSS_ATR_MULT"] = canon["RANGE_EARLY_LOSS_ATR"]

    old_values: dict[str, Any] = {}
    for k, v in canon.items():
        old_values[k] = getattr(C, k, _MISSING)
        setattr(C, k, v)
    return old_values


def _restore_overrides(old_values: dict[str, Any]) -> None:
    """
    Restore config values saved by _apply_overrides().
    If a key was missing before, delete it.
    """
    for k, old in old_values.items():
        if old is _MISSING:
            # Only delete if it was created by override
            if hasattr(C, k):
                delattr(C, k)
        else:
            setattr(C, k, old)


def _apply_named_preset(preset_name: str) -> dict[str, Any]:
    preset_key = str(preset_name or "").strip().upper()
    if not preset_key:
        return {}
    preset_values = dict(getattr(C, "PRESETS", {}).get(preset_key) or {})
    if not preset_values:
        raise ValueError(f"[SWEEP] unknown preset for mode: {preset_name}")
    old_values: dict[str, Any] = {}
    for key in preset_values.keys():
        old_values[key] = getattr(C, key, _MISSING)
    C.apply_preset(preset_key)
    return old_values

def _ensure_exports_dir(path: str) -> None:
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)


def _stable_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _make_run_id(index: int, params: dict[str, Any]) -> str:
    digest = hashlib.sha1(_stable_json(params).encode("utf-8")).hexdigest()[:8]
    return f"run_{int(index):03d}_{digest}"


def _write_sweep_run_report(
    *,
    mode: str,
    run_id: str,
    preset_name: str,
    params: dict[str, Any],
    result: dict[str, Any],
    exit_counts: dict[str, int],
) -> str:
    run_dir = os.path.join("exports", "sweeps", str(mode), str(run_id))
    os.makedirs(run_dir, exist_ok=True)
    report_path = os.path.join(run_dir, "report.json")
    raw_stop_count_by_year = result.get("stop_count_by_year") or {}
    stop_count_by_year: dict[str, int] = {}
    if isinstance(raw_stop_count_by_year, dict):
        for year_key, year_val in raw_stop_count_by_year.items():
            try:
                stop_count_by_year[str(year_key)] = int(year_val)
            except Exception:
                stop_count_by_year[str(year_key)] = 0
    pos_metrics = dict(result.get("pos_metrics") or {})
    payload = {
        "meta": {
            "build_id": BUILD_ID,
            "mode": str(mode),
            "run_id": str(run_id),
            "preset": str(preset_name or ""),
        },
        "params": dict(params),
        "result": {
            "trades": int(result.get("trades", 0) or 0),
            "win": int(result.get("win", 0) or 0),
            "lose": int(result.get("lose", 0) or 0),
            "net_total": float(result.get("net_total", 0.0) or 0.0),
            "max_dd": float(result.get("max_dd", 0.0) or 0.0),
            "final_equity": float(result.get("final_equity", 0.0) or 0.0),
            "stop_count_total": int(result.get("stop_count_total", 0) or 0),
            "risk_state_max_dd_all_ratio": float(result.get("risk_state_max_dd_all_ratio", 0.0) or 0.0),
            "risk_state_max_dd_all_pct": float(result.get("risk_state_max_dd_all_pct", 0.0) or 0.0),
            "risk_state_stop_count": int(result.get("risk_state_stop_count", 0) or 0),
            "stop_count_by_year": dict(stop_count_by_year),
            "pos_metrics": dict(pos_metrics),
        },
        "exit_counts": {str(k): int(v) for k, v in dict(exit_counts).items()},
    }
    with open(report_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    return report_path

def _load_exit_reason_counts(trades_csv_path: str) -> dict[str, int]:
    """
    Read trades.csv exported by backtest and count exit reasons.
    Robust to column naming (reason / exit_reason).
    """
    counts = {
        "exit_trail": 0,
        "exit_tp": 0,
        "exit_timeout": 0,
        "exit_stop": 0,
        "stop_hit_loss": 0,
        "stop_hit_profit": 0,
        "exit_be": 0,
        "exit_other": 0,
    }
    if not trades_csv_path:
        return counts
    if not os.path.exists(trades_csv_path):
        return counts
    try:
        with open(trades_csv_path, "r", encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                reason = (row.get("reason") or row.get("exit_reason") or "").strip()
                r = reason.lower()
                if "stop_hit_loss" in r:
                    counts["stop_hit_loss"] += 1
                if "stop_hit_profit" in r:
                    counts["stop_hit_profit"] += 1
                if not r:
                    counts["exit_other"] += 1
                    continue
                if "trail" in r:
                    counts["exit_trail"] += 1
                elif "timeout" in r:
                    counts["exit_timeout"] += 1
                elif ("tp" in r) or (("take" in r) and ("profit" in r)):
                    counts["exit_tp"] += 1
                elif ("stop" in r) or ("sl" in r):
                    counts["exit_stop"] += 1
                elif ("be" in r) or (("break" in r) and ("even" in r)):
                    counts["exit_be"] += 1
                else:
                    counts["exit_other"] += 1
    except Exception:
        return counts
    return counts


def _sweep_run_dir(mode: str, run_id: str) -> str:
    run_dir = os.path.join("exports", "sweeps", str(mode), str(run_id))
    os.makedirs(run_dir, exist_ok=True)
    return run_dir


def _copy_sweep_run_artifacts(mode: str, run_id: str) -> dict[str, str]:
    run_dir = _sweep_run_dir(mode, run_id)
    copied: dict[str, str] = {}
    artifact_names = (
        "trades.csv",
        "equity_curve.csv",
        "synthetic_equity_curve.csv",
        "stop_loss_patterns.csv",
    )
    for name in artifact_names:
        src = os.path.join("exports", name)
        if not os.path.exists(src):
            continue
        dst = os.path.join(run_dir, name)
        try:
            shutil.copy2(src, dst)
            copied[name] = dst
        except Exception:
            continue
    return copied


def _resolve_trades_csv_path(
    *,
    mode: str,
    run_id: str,
    copied_artifacts: dict[str, str] | None = None,
) -> str:
    copied = dict(copied_artifacts or {})
    copied_path = str(copied.get("trades.csv") or "").strip()
    if copied_path and os.path.exists(copied_path):
        return copied_path

    run_dir = _sweep_run_dir(mode, run_id)
    run_local = os.path.join(run_dir, "trades.csv")
    if os.path.exists(run_local):
        return run_local

    legacy = os.path.join("exports", "trades.csv")
    if os.path.exists(legacy):
        return legacy
    return ""


def _safe_float(value: Any) -> float | None:
    try:
        out = float(value)
    except Exception:
        return None
    if out != out:
        return None
    return float(out)


def _safe_percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    vals = sorted(float(v) for v in values)
    if len(vals) == 1:
        return float(vals[0])
    qf = max(0.0, min(100.0, float(q))) / 100.0
    pos = qf * float(len(vals) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(vals) - 1)
    if lo == hi:
        return float(vals[lo])
    weight = pos - float(lo)
    return float(vals[lo] * (1.0 - weight) + vals[hi] * weight)


def _safe_mean(values: list[float]) -> float | None:
    if not values:
        return None
    return float(sum(float(v) for v in values) / float(len(values)))


def _fallback_pos_metrics_from_trades_csv(trades_csv_path: str) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    if not trades_csv_path or not os.path.exists(trades_csv_path):
        return metrics

    qty_values: list[float] = []
    entry_notional_values: list[float] = []

    try:
        with open(trades_csv_path, "r", encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                qty = _safe_float(row.get("qty"))
                if qty is None or qty <= 0.0:
                    continue
                qty_values.append(float(qty))

                entry_px = _safe_float(row.get("entry_exec"))
                if entry_px is None:
                    entry_px = _safe_float(row.get("entry_raw"))
                if entry_px is None or entry_px <= 0.0:
                    continue
                entry_notional_values.append(float(entry_px) * float(qty))
    except Exception:
        return {}

    avg_qty = _safe_mean(qty_values)
    if avg_qty is not None:
        metrics["avg_qty"] = float(avg_qty)
    p50_qty = _safe_percentile(qty_values, 50.0)
    if p50_qty is not None:
        metrics["p50_qty"] = float(p50_qty)

    avg_entry_notional = _safe_mean(entry_notional_values)
    if avg_entry_notional is not None:
        metrics["avg_entry_notional"] = float(avg_entry_notional)
    p50_entry_notional = _safe_percentile(entry_notional_values, 50.0)
    if p50_entry_notional is not None:
        metrics["p50_entry_notional"] = float(p50_entry_notional)
    p90_entry_notional = _safe_percentile(entry_notional_values, 90.0)
    if p90_entry_notional is not None:
        metrics["p90_entry_notional"] = float(p90_entry_notional)

    return metrics

def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    # Some modes require trades.csv to be exported so we can analyze exit reasons.
    mode_needs_trades_csv = (
        (
            args.mode in (
                "TRAIL_EXIT_REASON_SHIFT",
                "RANGE_TP_EFFECT_DIAG_EXIT_COUNTS",
                "RANGE_EMA9_CROSS_MINLOSS_ONLY_V2",
                "RANGE_EMA9_CROSS_MINLOSS_ONLY_V3",
                "RANGE_EMA9_CROSS_EXIT_MIN_LOSS_BPS_SWEEP_A",
                "RANGE_EMA9_CROSS_EXIT_MIN_LOSS_BPS_SWEEP_A_WIDE",
            )
        )
        or bool(getattr(args, "filter_stop_hit_loss_zero", False))
        or ("STOP0" in str(args.mode).upper())
    )
    if mode_needs_trades_csv and not args.export_csv:
        # Force ON (so the mode is user-proof).
        args.export_csv = True

    # Meta embedding (for stable debugging / provenance)
    config_meta_json = ""
    strategy_meta_json = ""
    if args.include_debug:
        config_path = getattr(C, "__file__", "") or ""
        if config_path:
            config_meta_json = _file_meta_json(config_path)
        # strategy is not imported in this file by default; import lazily
        try:
            import strategy as S  # type: ignore
            strat_path = getattr(S, "__file__", "") or ""
            if strat_path:
                strategy_meta_json = _file_meta_json(strat_path)
        except Exception:
            strategy_meta_json = ""

    _validate_preset_keys(args.mode, allow_new_keys=bool(args.allow_new_keys))
    since_ms = _parse_optional_int(args.since_ms)
    until_ms, until_auto = _resolve_sweep_until_ms(
        symbols=list(args.symbols),
        entry_tf=str(args.entry_tf),
        until_ms=_parse_optional_int(args.until_ms),
    )
    recent_bars_entry = _parse_optional_int(args.recent_bars_entry)
    recent_bars_filter = _parse_optional_int(args.recent_bars_filter)
    print(
        f"[SWEEP] resolved range since_ms={since_ms} until_ms={until_ms} "
        f"(until_auto={1 if until_auto else 0})"
    )

    preset = SWEEP_PRESETS[args.mode]
    preset_name = str(preset.get("__preset__", "") or "").strip()
    combos = _iter_combos(preset)

    t0 = time.time()
    results: list[dict[str, Any]] = []

    for idx, overrides in enumerate(combos, start=1):
        preset_old_values = _apply_named_preset(preset_name)
        old_values = _apply_overrides(overrides)
        run_id = _make_run_id(idx, overrides)
        state_events_out_path = os.path.join("exports", "sweeps", str(args.mode), str(run_id), "state_events.csv")
        backtest_kwargs: dict[str, Any] = {
            "symbols": list(args.symbols),
            "since_ms": since_ms,
            "until_ms": until_ms,
            "entry_tf": args.entry_tf,
            "filter_tf": args.filter_tf,
            "warmup_bars": args.warmup,
            "initial_equity": args.initial_equity,
            "export_csv": bool(args.export_csv),
            "recent_bars_entry": recent_bars_entry,
            "recent_bars_filter": recent_bars_filter,
        }
        if "run_id" in _RUN_BACKTEST_PARAM_NAMES:
            backtest_kwargs["run_id"] = str(run_id)
        if "state_events_out_path" in _RUN_BACKTEST_PARAM_NAMES:
            backtest_kwargs["state_events_out_path"] = state_events_out_path
        try:
            res = run_backtest(**backtest_kwargs)
        finally:
            _restore_overrides(old_values)
            _restore_overrides(preset_old_values)

        copied_artifacts = _copy_sweep_run_artifacts(str(args.mode), str(run_id)) if bool(args.export_csv) else {}
        trades_csv_path = _resolve_trades_csv_path(
            mode=str(args.mode),
            run_id=str(run_id),
            copied_artifacts=copied_artifacts,
        )

        # exit reason counters (must always be defined to avoid NameError)
        exit_counts = {
            "exit_trail": 0,
            "exit_tp": 0,
            "exit_timeout": 0,
            "exit_stop": 0,
            "stop_hit_loss": 0,
            "stop_hit_profit": 0,
            "exit_be": 0,
            "exit_other": 0,
        }
        if trades_csv_path:
            exit_counts = _load_exit_reason_counts(trades_csv_path)
        filter_stop0 = bool(getattr(args, "filter_stop_hit_loss_zero", False)) or ("STOP0" in str(args.mode).upper())
        if filter_stop0 and int(exit_counts.get("stop_hit_loss", 0)) > 0:
            continue

        annual = res.get("annual") or res
        trades = int(annual.get("trades", 0))
        win = int(annual.get("win", 0))
        lose = int(annual.get("lose", 0))
        net_total = float(annual.get("net_total", 0.0))
        max_dd = float(annual.get("max_dd", 0.0))
        final_equity = float(annual.get("final_equity", 0.0))
        stop_count_total = int(annual.get("stop_count_total", 0) or 0)
        pm = dict(annual.get("pos_metrics") or {})
        if (not pm) and trades_csv_path:
            pm = _fallback_pos_metrics_from_trades_csv(trades_csv_path)
        if stop_count_total <= 0 and trades_csv_path:
            stop_count_total = int(exit_counts.get("exit_stop", 0) or 0)

        def _pm_value_or_blank(key: str) -> Any:
            if key not in pm:
                return ""
            try:
                return float(pm.get(key, 0.0) or 0.0)
            except Exception:
                return ""

        avg_entry_notional = _pm_value_or_blank("avg_entry_notional")
        p50_entry_notional = _pm_value_or_blank("p50_entry_notional")
        p90_entry_notional = _pm_value_or_blank("p90_entry_notional")
        avg_qty = _pm_value_or_blank("avg_qty")
        p50_qty = _pm_value_or_blank("p50_qty")
        cap_hit_rate = _pm_value_or_blank("cap_hit_rate")
        avg_f_used = _pm_value_or_blank("avg_f_used")
        days_in_defense = _pm_value_or_blank("days_in_defense")
        days_in_caution = _pm_value_or_blank("days_in_caution")
        days_in_stop = _pm_value_or_blank("days_in_stop")

        annual_for_report = dict(annual)
        annual_for_report["stop_count_total"] = int(stop_count_total)
        if pm:
            annual_for_report["pos_metrics"] = dict(pm)

        report_path = _write_sweep_run_report(
            mode=str(args.mode),
            run_id=str(run_id),
            preset_name=str(preset_name),
            params=overrides,
            result=annual_for_report,
            exit_counts=exit_counts,
        )
        pullback_funnel_json = ""
        hold_reasons_json = ""
        buy_signals = 0
        entries_opened = 0
        forced_closes = 0
        open_positions_end = 0
        if args.include_debug:
            pullback_funnel = res.get("pullback_funnel") or {}
            hold_reasons = res.get("hold_reasons_top12") or []
            pullback_funnel_json = json.dumps(pullback_funnel, ensure_ascii=False, sort_keys=True)
            hold_reasons_json = json.dumps(hold_reasons, ensure_ascii=False)
            buy_signals = int(annual.get("buy_signals", 0))
            entries_opened = int(annual.get("entries_opened", 0))
            forced_closes = int(annual.get("forced_closes", 0))
            open_positions_end = int(annual.get("open_positions_end", 0))
        score = net_total - (max_dd * DD_PENALTY_MULT)
        if args.min_trades and trades < args.min_trades:
            score = -1e18

        results.append(
            {
                "params": overrides,
                "run_id": str(run_id),
                "preset": str(preset_name),
                "report_path": str(report_path),
                "trades": trades,
                "win": win,
                "lose": lose,
                "net_total": net_total,
                "max_dd": max_dd,
                "final_equity": final_equity,
                "stop_count_total": stop_count_total,
                "score": score,
                "avg_entry_notional": avg_entry_notional,
                "p50_entry_notional": p50_entry_notional,
                "p90_entry_notional": p90_entry_notional,
                "avg_qty": avg_qty,
                "p50_qty": p50_qty,
                "cap_hit_rate": cap_hit_rate,
                "avg_f_used": avg_f_used,
                "days_in_defense": days_in_defense,
                "days_in_caution": days_in_caution,
                "days_in_stop": days_in_stop,
                "exit_trail": int(exit_counts.get("exit_trail", 0)) if mode_needs_trades_csv else 0,
                "exit_tp": int(exit_counts.get("exit_tp", 0)) if mode_needs_trades_csv else 0,
                "exit_timeout": int(exit_counts.get("exit_timeout", 0)) if mode_needs_trades_csv else 0,
                "exit_stop": int(exit_counts.get("exit_stop", 0)) if mode_needs_trades_csv else 0,
                "exit_be": int(exit_counts.get("exit_be", 0)) if mode_needs_trades_csv else 0,
                "exit_other": int(exit_counts.get("exit_other", 0)) if mode_needs_trades_csv else 0,
                "stop_hit_loss": int(exit_counts.get("stop_hit_loss", 0)) if mode_needs_trades_csv else 0,
                "stop_hit_profit": int(exit_counts.get("stop_hit_profit", 0)) if mode_needs_trades_csv else 0,
                "pullback_funnel_json": pullback_funnel_json,
                "hold_reasons_json": hold_reasons_json,
                "buy_signals": buy_signals,
                "entries_opened": entries_opened,
                "forced_closes": forced_closes,
                "open_positions_end": open_positions_end,
            }
        )

    results_sorted = sorted(results, key=lambda item: item["score"], reverse=True)

    _ensure_exports_dir(args.out)
    param_columns = _param_columns_for_preset(preset, combos)
    fieldnames = (
        ["run_id", "preset", "report_path"]
        + param_columns
        + [
            "trades",
            "win",
            "lose",
            "net_total",
            "max_dd",
            "final_equity",
            "stop_count_total",
            "score",
            "avg_entry_notional",
            "p50_entry_notional",
            "p90_entry_notional",
            "avg_qty",
            "p50_qty",
            "cap_hit_rate",
            "avg_f_used",
            "days_in_defense",
            "days_in_caution",
            "days_in_stop",
        ]
    )
    if mode_needs_trades_csv:
        fieldnames = fieldnames + [
            "exit_trail",
            "exit_tp",
            "exit_timeout",
            "exit_stop",
            "exit_be",
            "exit_other",
            "stop_hit_loss",
            "stop_hit_profit",
        ]
    if args.include_debug:
        fieldnames = fieldnames + [
            "config_meta_json",
            "strategy_meta_json",
            "pullback_funnel_json",
            "hold_reasons_json",
            "buy_signals",
            "entries_opened",
            "forced_closes",
            "open_positions_end",
        ]

    with open(args.out, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for item in results_sorted:
            row = {key: item["params"].get(key) for key in param_columns}
            row.update(
                {
                    "run_id": item["run_id"],
                    "preset": item["preset"],
                    "report_path": item["report_path"],
                    "trades": item["trades"],
                    "win": item["win"],
                    "lose": item["lose"],
                    "net_total": item["net_total"],
                    "max_dd": item["max_dd"],
                    "final_equity": item["final_equity"],
                    "stop_count_total": item["stop_count_total"],
                    "score": item["score"],
                    "avg_entry_notional": item.get("avg_entry_notional", 0.0),
                    "p50_entry_notional": item.get("p50_entry_notional", 0.0),
                    "p90_entry_notional": item.get("p90_entry_notional", 0.0),
                    "avg_qty": item.get("avg_qty", 0.0),
                    "p50_qty": item.get("p50_qty", 0.0),
                    "cap_hit_rate": item.get("cap_hit_rate", 0.0),
                    "avg_f_used": item.get("avg_f_used", 0.0),
                    "days_in_defense": item.get("days_in_defense", 0.0),
                    "days_in_caution": item.get("days_in_caution", 0.0),
                    "days_in_stop": item.get("days_in_stop", 0.0),
                }
            )
            if mode_needs_trades_csv:
                row.update({
                    "exit_trail": item.get("exit_trail", 0),
                    "exit_tp": item.get("exit_tp", 0),
                    "exit_timeout": item.get("exit_timeout", 0),
                    "exit_stop": item.get("exit_stop", 0),
                    "exit_be": item.get("exit_be", 0),
                    "exit_other": item.get("exit_other", 0),
                    "stop_hit_loss": item.get("stop_hit_loss", 0),
                    "stop_hit_profit": item.get("stop_hit_profit", 0),
                })
            if args.include_debug:
                row.update({
                    "config_meta_json": config_meta_json,
                    "strategy_meta_json": strategy_meta_json,
                    "pullback_funnel_json": item.get("pullback_funnel_json", ""),
                    "hold_reasons_json": item.get("hold_reasons_json", ""),
                    "buy_signals": item.get("buy_signals", 0),
                    "entries_opened": item.get("entries_opened", 0),
                    "forced_closes": item.get("forced_closes", 0),
                    "open_positions_end": item.get("open_positions_end", 0),
                })
            writer.writerow(row)

    dt = time.time() - t0

    best = results_sorted[0] if results_sorted else None
    best_score = best["score"] if best else 0.0
    print(
        f"[SWEEP] mode={args.mode} runs={len(results_sorted)} "
        f"best_score={best_score:.6f} dt={dt:.2f}s out={args.out}"
    )
    if best:
        print(
            f"[SWEEP] best_params={best['params']} trades={best['trades']} "
            f"net_total={best['net_total']:.2f} max_dd={best['max_dd']:.6f}"
        )
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
