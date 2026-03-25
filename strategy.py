# BUILD_ID: 2026-03-21_strategy_comment_cleanup_v1
# BUILD_ID: 2026-03-07_runner_range_direction_forward_v1
# NOTE:
#   Used to verify that the expected strategy.py was loaded.
#   It has no effect on trading logic.

from __future__ import annotations

import logging
import numpy as np
import config as C

BUILD_ID = "2026-03-21_strategy_comment_cleanup_v1"

logger = logging.getLogger(__name__)
logging.getLogger(__name__).info(
    f"[BUILD] strategy.py loaded: file={__file__} BUILD_ID={BUILD_ID}"
)

if '_hold' not in globals():
    def _hold(reason: str) -> dict:
        return {"action": "hold", "reason": reason}
# Pullback Phase A/B state helper for runner/backtest parity.
# Keep the mapping explicit and serializable.
#
def _range_entry_diag_should_log(state, debug_flag: float, limit: float) -> bool:
    """
    state: state dict used by the stateful entry flow (pb_state / StateStore)
    debug_flag: DEBUG_RANGE_ENTRY (0.0/1.0)
    limit: RANGE_ENTRY_DIAG_LIMIT
    """
    if not debug_flag:
        return False

    lim = int(limit) if limit is not None else 0
    if lim <= 0:
        return False

    # Store the counter in the caller-provided state dict.
    key = "_range_entry_diag_count"
    n = int(state.get(key, 0))
    if n >= lim:
        return False

    state[key] = n + 1
    return True


def _log_range_entry_diag(state, debug_flag: float, limit: float, msg: str) -> None:
    if _range_entry_diag_should_log(state, debug_flag, limit):
        logger.info(msg)

def pullback_ab_state_update(pb_state: dict | None, sig: dict, now_ms: int | None = None) -> dict:
    """
    Update Pullback Phase A/B state from signal dict.
    - pb_state: JSON-serializable dict (or None)
    - sig: output from signal_entry / signal_entry_precomputed
    - now_ms: optional timestamp for bookkeeping
    """
    if pb_state is None:
        pb_state = {"phase": "NONE", "since_ms": None, "last_reason": None}

    # BUY fires => reset
    if sig.get("action") == "buy":
        return {"phase": "NONE", "since_ms": None, "last_reason": "buy_fired"}

    reason = sig.get("reason") or sig.get("hold_reason") or ""
    pb_state["last_reason"] = reason

    # Heuristic mapping (keep minimal + stable)
    # Phase A: setup/waiting for reclaim/rebound (pre-break)
    if reason.startswith("pullback_waiting:"):
        if pb_state.get("phase") == "NONE":
            pb_state["phase"] = "A"
            pb_state["since_ms"] = now_ms
        return pb_state

    # If we want a stricter separation later, we can map specific sub-reasons to B.
    # For now keep it A unless buy happens.
    return pb_state


def signal_entry_stateful(
    *,
    pb_state,
    now_ms: int,
    regime: str,
    direction: str,
    open_,
    high_,
    low_,
    close_,
    ema9,
    ema21,
    rsi14,
    atr14,
    cfg=None,
    **kwargs,
):
    # signal_entry() accepts a strict input set, so drop runner-only extras here.
    call_kwargs = dict(
        regime=regime,
        direction=direction,
        open_=open_,
        high_=high_,
        low_=low_,
        close_=close_,
        ema9=ema9,
        ema21=ema21,
        rsi14=rsi14,
        atr14=atr14,
    )

    # Normalize key names for signal_entry() signature.
    if "high_" in call_kwargs and "high" not in call_kwargs:
        call_kwargs["high"] = call_kwargs.pop("high_")
    if "low_" in call_kwargs and "low" not in call_kwargs:
        call_kwargs["low"] = call_kwargs.pop("low_")
    if "close_" in call_kwargs and "close" not in call_kwargs:
        call_kwargs["close"] = call_kwargs.pop("close_")

    sig = signal_entry(**call_kwargs)

    # Update pullback AB state using only what the updater expects.
    pb_state = pullback_ab_state_update(pb_state, sig, now_ms=now_ms)

    return sig, pb_state

def _is_finite(x: float) -> bool:
    try:
        return bool(np.isfinite(float(x)))
    except Exception:
        return False

def _ema9_break_at(
    *,
    k: int,
    open_: list[float],
    high: list[float],
    low: list[float],
    close: list[float],
    ema9: list[float],
    atr_now: float,
) -> bool:
    """
    EMA9 break detector (no lookahead).
    Returns True if at bar k the EMA9 reclaim is satisfied:
      - strict_cross: trig_now > ema9[k] and trig_prev <= ema9[k-1]
      - relaxed_reclaim: trig_now > ema9[k] and previous bar touches EMA9 band (+/- relax_bps)
    trig is max(close, high) to allow wick breaks (frequency lever).
    """
    if k <= 0:
        return False
    try:
        c = float(close[k])
        h = float(high[k])
        e = float(ema9[k])
        c_prev = float(close[k - 1])
        h_prev = float(high[k - 1])
        e_prev = float(ema9[k - 1])
        o_prev = float(open_[k - 1])
        l_prev = float(low[k - 1])
    except Exception:
        return False

    if not (_is_finite(c) and _is_finite(h) and _is_finite(e) and _is_finite(c_prev) and _is_finite(h_prev) and _is_finite(e_prev)):
        return False

    trig_now = max(c, h)
    trig_prev = max(c_prev, h_prev)
    strict_cross = (trig_now > e) and (trig_prev <= e_prev)

    relax_bps = float(getattr(C, "TREND_PULLBACK_EMA9_RELAX_BPS", 0.0))
    ema_dist_min = float(getattr(C, "TREND_PULLBACK_EMA9_DIST_ATR_MIN", 0.0))
    dist_ok = True
    if ema_dist_min > 0.0 and _is_finite(atr_now) and atr_now > 0:
        ema_dist_atr = (trig_now - e) / float(atr_now)
        dist_ok = (ema_dist_atr + 1e-12) >= ema_dist_min

    tol = e_prev * (relax_bps / 10000.0)
    prev_min = min(float(l_prev), float(o_prev), float(c_prev), float(h_prev))
    prev_max = max(float(l_prev), float(o_prev), float(c_prev), float(h_prev))
    prev_touch_or_near = (prev_min <= (e_prev + tol)) and (prev_max >= (e_prev - tol))
    relaxed_reclaim = (relax_bps > 0.0) and (trig_now > e) and prev_touch_or_near and dist_ok

    return bool(strict_cross or relaxed_reclaim)

def _ema9_break_recent(
    *,
    i: int,
    open_: list[float],
    high: list[float],
    low: list[float],
    close: list[float],
    ema9: list[float],
    atr_now: float,
) -> bool:
    """
    Recent-window EMA9 break:
      TREND_PULLBACK_EMA9_BREAK_RECENT_BARS = 0 -> current bar only
      = N -> current bar or previous N bars
    No lookahead.
    """
    n = int(getattr(C, "TREND_PULLBACK_EMA9_BREAK_RECENT_BARS", 0))
    n = max(0, min(n, 50))
    if i <= 0:
        return False
    for k in range(i, max(1, i - n) - 1, -1):
        if _ema9_break_at(
            k=k,
            open_=open_,
            high=high,
            low=low,
            close=close,
            ema9=ema9,
            atr_now=atr_now,
        ):
            return True
    return False

def _pick_pullback_ema(e9: float, e21: float) -> float:
    k = str(getattr(C, "TREND_PULLBACK_EMA", "EMA21")).upper()
    return float(e9) if k == "EMA9" else float(e21)


def _trend_pullback_precomputed(
    *,
    regime: str | None = None,
    direction: str | None = None,
    i: int,
    open_: list[float],
    high: list[float],
    low: list[float],
    close: list[float],
    ema9: list[float],
    ema21: list[float],
    rsi14: list[float],
    atr14: list[float],
) -> dict:
    """
    Trend Pullback Continuation (2-stage):
      Stage A: 直前足(i-1)で「押し目到達」を検出（まだ入らない）
      Stage B: 現在足(i)で「反発確認」したらエントリー
    目的:
      - pullback_fail:not_bull_candle を激減させる（押し目中の陰線を当然として扱う）
      - ノイズエントリーを削減して DD/コスト負けを落とす
    """

    if i <= 2:
        return {"action": "hold", "reason": "warmup"}

    two_stage = bool(getattr(C, "TREND_PULLBACK_TWO_STAGE", True))

    # ------------------------------------------------------------
    # Direction gate (profit-first)
    #   - trend なのに direction が none/short の買いは捨てる
    #   - 既存ログ/集計との整合のため、理由は "trend_dir_none" を優先
    # ------------------------------------------------------------
    reg_l = str(regime).lower() if regime is not None else ""
    dir_l = str(direction).lower() if direction is not None else ""
    require_dir_long = bool(getattr(C, "TREND_PULLBACK_REQUIRE_DIR_LONG", True))
    # 年100回優先：pullbackでは direction=none を許可
    if require_dir_long and reg_l == "trend" and dir_l == "short":
        return {"action": "hold", "reason": "trend_dir_short"}

    price = float(close[i])
    atr_now = float(atr14[i])
    if not (_is_finite(price) and _is_finite(atr_now) and atr_now > 0):
        return {"action": "hold", "reason": "pullback_fail:nan_price_or_atr"}

    # ------------------------------------------------------------
    # Volatility gate (profit-first)
    #   ATRが小さすぎる局面はコスト負けしやすいので、エントリー自体を拒否する
    #   (例) atr_pct_min=0.0008 => ATRが価格の0.08%未満なら hold
    # ------------------------------------------------------------
    atr_pct_min = float(getattr(C, "TREND_PULLBACK_ATR_PCT_MIN", 0.0))
    if atr_pct_min and atr_pct_min > 0:
        atr_pct = atr_now / max(price, 1e-12)
        if atr_pct + 1e-12 < atr_pct_min:
            return {
                "action": "hold",
                "reason": f"pullback_waiting:atr_too_small(pct={atr_pct:.5f} < {atr_pct_min:.5f})",
            }

    e9 = float(ema9[i])
    e21 = float(ema21[i])
    ema_ref = _pick_pullback_ema(e9, e21)
    if not (_is_finite(ema_ref) and _is_finite(e9) and _is_finite(e21)):
        return {"action": "hold", "reason": "pullback_fail:nan_ema"}

    near_mult = float(getattr(C, "TREND_PULLBACK_NEAR_ATR_MULT", 0.35))
    deep_mult = float(getattr(C, "TREND_PULLBACK_DEEP_ATR_MULT", 1.10))

    # ------------------------------------------------------------
    # StageA relax (frequency lever)
    #   EMA9 break recent window 中だけ「浅い押し目」でも StageA を通す
    #   - config/sweepは変更しない（既存の recent bars を利用）
    # ------------------------------------------------------------
    pb_break_mode = str(getattr(C, "TREND_PULLBACK_BREAK_MODE", "high")).lower()
    pb_modes = [m.strip() for m in pb_break_mode.split("|") if m.strip()]
    ema_mode_enabled = any(m.startswith("ema") for m in pb_modes)
    ema9_break_recent_stagea = False
    if ema_mode_enabled:
        ema9_break_recent_stagea = _ema9_break_recent(
            i=i, open_=open_, high=high, low=low, close=close, ema9=ema9, atr_now=atr_now
        )
    near_mult_eff = float(near_mult) * (2.00 if ema9_break_recent_stagea else 1.00)

    # -----------------------
    # Stage A: previous bar
    # -----------------------
    if two_stage:
        j = i - 1
        # 押し目判定：前足lowがEMA近傍まで到達
        if float(low[j]) > ema_ref + atr_now * near_mult_eff:
            dist = float(low[j]) - ema_ref
            return {
                "action": "hold",
                "reason": f"pullback_waiting:not_near_ema(dist={dist:.2f} atr={atr_now:.2f} near={near_mult_eff:.2f} relax={int(ema9_break_recent_stagea)})",
            }

        # 深すぎる押し（トレンド崩れ）除外：前足lowがEMA - atr*deep を割る
        if float(low[j]) < ema_ref - atr_now * deep_mult:
            dist = ema_ref - float(low[j])
            return {
                "action": "hold",
                "reason": f"pullback_fail:too_deep(dist={dist:.2f} atr={atr_now:.2f} deep={deep_mult:.2f})",
            }

        # 押し目“らしさ”を要求（ノイズ削減）
        req_bear = bool(getattr(C, "TREND_PULLBACK_SETUP_REQUIRE_BEARISH", True))
        req_close_below = bool(getattr(C, "TREND_PULLBACK_SETUP_REQUIRE_CLOSE_BELOW_EMA", True))
        # setup足が陰線でないなら「押し目として弱い」→除外
        if req_bear and not (float(close[j]) < float(open_[j])):
            return {"action": "hold", "reason": "pullback_waiting:setup_not_bearish"}

        # setup足の終値がEMA以下なら押し目として扱う
        if req_close_below and not (float(close[j]) <= ema_ref):
            d = float(close[j]) - ema_ref
            return {"action": "hold", "reason": f"pullback_waiting:setup_close_above_ema(d={d:.2f})"}

        # Stage A passed, so confirm the rebound on the current bar.
        # -----------------------
        # Stage B: current bar
        # -----------------------
        # Stage B uses the current bar for the break confirmation.
        # Break mode accepts the current config key and the legacy fallback key.
        _raw_break_mode = getattr(C, "TREND_PULLBACK_BREAK_MODE", getattr(C, "PULLBACK_BREAK_MODE", "close"))
        # Sweep jobs may pass numeric flags here: 1/True => "high", 0/False => "close".
        if isinstance(_raw_break_mode, (int, float)):
            break_mode_raw = "high" if float(_raw_break_mode) >= 1.0 else "close"
        else:
            break_mode_raw = str(_raw_break_mode).lower()

        modes = [m.strip() for m in break_mode_raw.split("|") if m.strip()]
        if not modes:
            modes = ["close"]

        lookback = int(getattr(C, "TREND_PULLBACK_BREAK_LOOKBACK", 1))
        # allow wider sweep (yearly 100+ trades needs flexibility)
        lookback = max(1, min(lookback, 50))
        # Use the highest high across the previous lookback bars.
        ref_high = max(float(high[i - j]) for j in range(1, lookback + 1))

        # Support close/high/both trigger modes (for example, "close|high").
        triggers: list[float] = []
        if "close" in modes:
            triggers.append(float(close[i]))
        if "high" in modes:
            triggers.append(float(high[i]))
        # fallback
        if not triggers:
            triggers = [float(close[i])]
        trigger = max(triggers)

        # ブレイク要求：ref_high に対して buffer を加えた水準を抜く（0なら無効）
        buf_bps = float(getattr(C, "TREND_PULLBACK_BREAK_BUF_BPS", 0.0))
        ref_high_adj = ref_high * (1.0 + (buf_bps / 10000.0))


        floor_break = float(getattr(C, "TREND_PULLBACK_BREAK_ATR_FLOOR", getattr(C, "TREND_PULLBACK_BREAK_FLOOR_ATR", 0.0)))
        max_break_atr = float(getattr(C, "TREND_PULLBACK_BREAK_ATR_MAX", 999.0))
        max_break = max_break_atr
        cost_mult = float(getattr(C, "PULLBACK_BREAK_COST_MULT", 0.0))
        dynamic = bool(getattr(C, "TREND_PULLBACK_BREAK_DYNAMIC", False))
        min_break_base = float(getattr(C, "TREND_PULLBACK_BREAK_ATR_MIN", 0.10))
        
        if dynamic:
            # Estimate all-in cost in bps (spread + slippage + round-trip fee if present)
            spread_bps = float(getattr(C, "BACKTEST_SPREAD_BPS", getattr(C, "SPREAD_BPS_ESTIMATE", 2.0)))
            slippage_bps = float(getattr(C, "SLIPPAGE_BPS", 0.0))
            fee_rate_maker = float(getattr(C, "PAPER_FEE_RATE_MAKER", getattr(C, "PAPER_FEE_RATE", 0.0)))
            fee_rate_taker = float(getattr(C, "PAPER_FEE_RATE_TAKER", getattr(C, "PAPER_FEE_RATE", 0.0)))
            # Conservative: assume taker on entry+exit if both exist, else maker+maker.
            fee_bps_round = (fee_rate_taker + fee_rate_taker) * 10000.0
            cost_bps = float(getattr(C, "TREND_PULLBACK_BREAK_COST_BPS", spread_bps + slippage_bps + fee_bps_round))
            # Convert bps cost into ATR units at current price level
            cost_move = float(ref_high) * (cost_bps / 10000.0)
            cost_atr = cost_move / float(atr_now)
            cost_mult = float(getattr(C, "TREND_PULLBACK_BREAK_COST_ATR_MULT", 1.2))
            floor_atr = float(getattr(C, "TREND_PULLBACK_BREAK_ATR_FLOOR", 0.03))
            min_break = max(floor_atr, min(min_break_base, cost_atr * cost_mult))
        else:
            min_break = max(floor_break, min_break_base)

        min_req = max(min_break, floor_break)

        # Break gate (mode-aware):
        # TREND_PULLBACK_BREAK_MODE の指定に従って ref_high ブレイクを判定する。
        # 例:
        #   - "close"        => close が ref_high_adj を超える必要
        #   - "high"         => high  が ref_high_adj を超える必要
        #   - "close|high"   => close または high
        #   - "ema9"         => ref_high ブレイクでは通さない（EMA ブレイク側で通す）
        allow_ref_break = False
        if "close" in modes:
            if float(close[i]) > ref_high_adj:
                allow_ref_break = True
        if "high" in modes:
            if float(high[i]) > ref_high_adj:
                allow_ref_break = True

        # EMA break mode:
        # 押し目後の“復帰”を ref_high ブレイクに限定すると trades が極端に減るため、
        # ema9 クロスでも StageB を通せるようにする（年100回以上の土台づくり）
        ema_break_enabled = any(m.startswith("ema") for m in modes)
        allow_ema_break = False
        ema9_break_recent = False
        if ema_break_enabled and i >= 1:
            # ema9 を上抜け（厳密クロス）
            c = float(close[i])
            h = float(high[i])
            e = float(ema9[i])
            c_prev = float(close[i - 1])
            h_prev = float(high[i - 1])
            e_prev = float(ema9[i - 1])
            o_prev = float(open_[i - 1])
            l_prev = float(low[i - 1])

            # frequency lever:
            # close だけに限定すると「ヒゲだけ上抜け→終値で戻る」足を全て捨ててしまう。
            # ここでは close|high のどちらかで EMA9 を上抜けたら break 扱いにする。
            trig_now = max(c, h)
            trig_prev = max(c_prev, h_prev)

            # Recent-window aware EMA9 break (frequency lever):
            # - allow break if current bar breaks EMA9
            # - or if previous N bars had EMA9 break and we are still above EMA9 now
            ema9_break_recent = _ema9_break_recent(
                i=i,
                open_=open_,
                high=high,
                low=low,
                close=close,
                ema9=ema9,
                atr_now=atr_now,
            )
            # Default: require trig_now > EMA9 (wick ok)
            allow_ema_break = bool(ema9_break_recent and (trig_now > e))
            # Relax not_break_ema ONLY during recent-window:
            # If we already had an EMA9 break recently, allow StageB to proceed when
            # the current close is at/above EMA9 (avoid pure-wick reclaim).
            # This targets "pullback_waiting:not_break_ema(...)" without globally loosening.
            allow_ema_break_soft = bool(ema9_break_recent and (float(close[i]) >= e))
            if (not allow_ema_break) and allow_ema_break_soft:
                allow_ema_break = True

        # break の基準レベル（距離評価で使う）
        break_level = ref_high_adj
        break_level_kind = "ref_high"
        if not allow_ref_break and allow_ema_break:
            break_level = float(ema9[i])
            break_level_kind = "ema9"

        if not (allow_ref_break or allow_ema_break):
            if any(m.startswith("ema") for m in modes):
                return {
                    "action": "hold",
                    "reason": f"pullback_waiting:not_break_ema(kind=ema9,lb={lookback},buf_bps={buf_bps:.1f},recent={int(getattr(C,'TREND_PULLBACK_EMA9_BREAK_RECENT_BARS',0))})",
                }
            else:
                return {
                    "action": "hold",
                    "reason": f"pullback_waiting:not_break_ref_high(mode={break_mode_raw},lb={lookback},buf_bps={buf_bps:.1f})",
                }

        # ------------------------------------------------------------
        # Break distance / wick-aware min break requirement
        # ------------------------------------------------------------
        # upper_wick_ratio (0..1): 0=上ヒゲなし, 1=ほぼ全て上ヒゲ
        rng_i = max(1e-12, float(high[i]) - float(low[i]))
        upper_wick = float(high[i]) - max(float(open_[i]), float(close[i]))
        upper_wick_ratio = max(0.0, upper_wick / rng_i)

        # break distance in ATR
        break_dist_atr = (trigger - break_level) / float(atr_now) if atr_now else 0.0

        min_req = max(floor_break, min_break)
        # --- Relax break distance requirement for EMA9 reclaim ---
        # Rationale:
        # EMA9 break is used as a trade-frequency lever.
        # Requiring the same ATR-distance as ref_high break
        # kills most relaxed EMA9 reclaim entries.
        if break_level_kind == "ema9":
            min_req = min(min_req, float(getattr(C, "TREND_PULLBACK_EMA9_MIN_BREAK_ATR", 0.0)))

        if dynamic and cost_mult > 0.0:
            # ヒゲが長いほど「弱いブレイク」に見えるので min_req を少し引き上げる
            min_req = max(min_req, min_break + (cost_mult * upper_wick_ratio))

        if min_req and break_dist_atr < min_req:
            return {
                "action": "hold",
                "reason": f"pullback_waiting:weak_break(kind={break_level_kind},dist_atr={break_dist_atr:.3f} < req={min_req:.3f})",
            }

        if max_break_atr and break_dist_atr > max_break_atr:
            return {
                "action": "hold",
                "reason": f"pullback_waiting:over_break(dist_atr={break_dist_atr:.3f} > max={max_break_atr:.3f})",
            }

        # ------------------------------------------------------------
        # Break candle quality gates (make PB_BREAK_FILTER sweep effective)
        # ------------------------------------------------------------
        body_i = abs(float(close[i]) - float(open_[i]))
        body_ratio_i = body_i / rng_i
        body_min = float(getattr(C, "TREND_PULLBACK_BODY_RATIO_MIN", getattr(C, "TREND_FOLLOW_BODY_RATIO_MIN", 0.60)))
        # EMA9 break: relax break candle body threshold (frequency lever)
        # Rationale:
        # EMA9 reclaim entries tend to have weaker bodies than ref_high break.
        # Using the same body_min for both flattens EMA9_RELAX sweeps via break_weak_body.
        if break_level_kind == "ema9":
            body_min = float(getattr(C, "TREND_PULLBACK_EMA9_BODY_RATIO_MIN", body_min))
            # Further relax ONLY during EMA9 break recent window:
            # allow slightly weaker break bodies for "recent" reclaim flows.
            # No new config knob: fixed small decrement.
            if bool(ema9_break_recent):
                body_min = max(0.0, float(body_min) - 0.05)
        if body_ratio_i < body_min:
            return {
                "action": "hold",
                "reason": f"pullback_waiting:break_weak_body(r={body_ratio_i:.2f} < {body_min:.2f})",
            }

        uw_max = float(getattr(C, "TREND_PULLBACK_UPPER_WICK_RATIO_MAX", getattr(C, "TREND_UPPER_WICK_RATIO_MAX", 0.15)))
        if upper_wick_ratio > uw_max:
            return {
                "action": "hold",
                "reason": f"pullback_waiting:break_upper_wick(r={upper_wick_ratio:.2f} > {uw_max:.2f})",
            }
        # Rebound candle gate (configurable)
        # Default: require bullish candle (close > open)
        # Optionally allow small red/doji candles by tolerance (bps).
        require_bull = bool(getattr(C, "TREND_PULLBACK_REBOUND_REQUIRE_BULL", True))
        bull_tol_bps = float(getattr(C, "TREND_PULLBACK_REBOUND_BULL_TOL_BPS", 0.0))
        # rebound bullish requirement relaxed by config
        if require_bull:
            if float(close[i]) < float(open_[i]) * (1.0 - bull_tol_bps / 10000.0):
                return {"action": "hold", "reason": "pullback_waiting:need_rebound_bull"}
        # Rebound-close gate:
        # Default: require close >= pullback EMA reference.
        # Relax ONLY during EMA9 break recent window:
        # allow close to be slightly below EMA by epsilon (reuse TREND_PULLBACK_EMA9_RELAX_BPS).
        if float(close[i]) < ema_ref:
            relax_bps = float(getattr(C, "TREND_PULLBACK_EMA9_RELAX_BPS", 0.0))
            eps = float(ema_ref) * (relax_bps / 10000.0)
            if bool(ema9_break_recent) and (float(close[i]) >= float(ema_ref) - eps):
                pass
            else:
                d = ema_ref - float(close[i])
                return {
                    "action": "hold",
                    "reason": f"pullback_waiting:rebound_close_below_ema(d={d:.2f},relax={int(bool(ema9_break_recent))},eps={eps:.2f})",
                }

        # Require the rebound candle to recover from ema_ref by a minimum ATR distance.
        min_dist_atr = float(getattr(C, "TREND_PULLBACK_MIN_REBOUND_DIST_ATR", 0.0))
        # rebound distance gate (sweep lever)
        # NOTE:
        # - We measure distance from the pullback EMA reference (ema_ref),
        #   because this is the level used for StageA "near/deep" checks.
        # - This gate must exist for TREND_PULLBACK_MIN_REBOUND_DIST_ATR sweeps
        #   (EMA9_RELAX_X_REBOUND_DIST) to have any effect.
        if min_dist_atr and min_dist_atr > 0.0:
            rebound_dist_atr = (float(close[i]) - float(ema_ref)) / float(atr_now)
            if rebound_dist_atr + 1e-12 < min_dist_atr:
                return {
                    "action": "hold",
                    "reason": f"pullback_waiting:rebound_dist_atr(dist={rebound_dist_atr:.3f} < min={min_dist_atr:.3f})",
                }

        # Current candle geometry (used by follow-through / momentum filters)
        rng_i = float(high[i]) - float(low[i])
        if not (_is_finite(rng_i) and rng_i > 0):
            return {"action": "hold", "reason": "pullback_fail:bad_candle"}
        body_i = abs(float(close[i]) - float(open_[i]))
        if not _is_finite(body_i):
            return {"action": "hold", "reason": "pullback_fail:bad_candle"}
        body_ratio_i = body_i / rng_i
        upper_wick_i = float(high[i]) - max(float(open_[i]), float(close[i]))
        if not _is_finite(upper_wick_i) or upper_wick_i < 0:
            return {"action": "hold", "reason": "pullback_fail:bad_candle"}
        upper_wick_ratio_i = upper_wick_i / rng_i
        # ensure body_ratio is defined on all paths
        rng_ft = float(high[i]) - float(low[i])
        if not _is_finite(rng_ft) or rng_ft <= 0:
            return {"action": "hold", "reason": "pullback_fail:bad_ft_candle"}

        body_ft = abs(float(close[i]) - float(open_[i]))
        body_ratio = body_ft / rng_ft
        # Follow-through filter (NO lookahead)
        # Backtestで未来足(i+1)を見るのは lookahead になるため、
        # 「現在足(i)」のローソク形状で follow-through を評価する。
        ft_body_ratio_min = float(getattr(C, "TREND_PULLBACK_FT_BODY_RATIO_MIN", 0.30))
        ft_uw_ratio_max = float(getattr(C, "TREND_PULLBACK_FT_UPPER_WICK_RATIO_MAX", 0.45))

        if body_ratio_i < ft_body_ratio_min:
            return {
                "action": "hold",
                "reason": f"pullback_waiting:ft_weak_body(r={body_ratio_i:.2f} < {ft_body_ratio_min:.2f})",
            }

        if upper_wick_ratio_i > ft_uw_ratio_max:
            return {
                "action": "hold",
                "reason": f"pullback_waiting:ft_upper_wick(r={upper_wick_ratio_i:.2f} > {ft_uw_ratio_max:.2f})",
            }
        # 反発足の“勢い”フィルタ（breakoutで効いた価格アクションをpullbackにも適用）
        body_min = float(getattr(C, "TREND_PULLBACK_BODY_RATIO_MIN", getattr(C, "TREND_FOLLOW_BODY_RATIO_MIN", 0.60)))
        if body_ratio_i < body_min:
            return {"action": "hold", "reason": f"pullback_waiting:weak_body(r={body_ratio_i:.2f} < {body_min:.2f})"}
        uw_max = float(getattr(C, "TREND_PULLBACK_UPPER_WICK_RATIO_MAX", getattr(C, "TREND_UPPER_WICK_RATIO_MAX", 0.15)))
        if upper_wick_ratio_i > uw_max:
            return {"action": "hold", "reason": f"pullback_waiting:upper_wick(r={upper_wick_ratio_i:.2f} > {uw_max:.2f})"}
        # Close position filter（終値が高値寄り＝勢い維持）
        close_pos_i = (float(close[i]) - float(low[i])) / rng_i
        if not _is_finite(close_pos_i):
            return {"action": "hold", "reason": "pullback_fail:bad_candle"}
        close_pos_min = float(getattr(C, "TREND_PULLBACK_CLOSE_POS_MIN", 0.75))
        if close_pos_i < close_pos_min:
            return {"action": "hold", "reason": f"pullback_waiting:close_pos(p={close_pos_i:.2f} < {close_pos_min:.2f})"}
        # Body-cross-EMA gate (configurable)
        require_body_cross = bool(getattr(C, "TREND_PULLBACK_REBOUND_REQUIRE_BODY_CROSS_EMA", True))
        if require_body_cross:
            if not (float(open_[i]) < ema_ref and float(close[i]) > ema_ref):
                return {"action": "hold", "reason": "pullback_waiting:need_body_cross_ema"}

    else:
        # 旧1-stage（必要なら戻せる）: 現在足で押し目+反発まで完結
        if float(low[i]) > ema_ref + atr_now * near_mult:
            dist = float(low[i]) - ema_ref
            return {"action": "hold", "reason": f"pullback_fail:not_near_ema(dist={dist:.2f} atr={atr_now:.2f} near={near_mult:.2f})"}
        if float(low[i]) < ema_ref - atr_now * deep_mult:
            dist = ema_ref - float(low[i])
            return {"action": "hold", "reason": f"pullback_fail:too_deep(dist={dist:.2f} atr={atr_now:.2f} deep={deep_mult:.2f})"}
        # Rebound candle gate (configurable)
        # Default: require bullish candle (close > open)
        # Optionally allow small red/doji candles by tolerance (bps).
        require_bull = bool(getattr(C, "TREND_PULLBACK_REBOUND_REQUIRE_BULL", True))
        bull_tol_bps = float(getattr(C, "TREND_PULLBACK_REBOUND_BULL_TOL_BPS", 0.0))
        # rebound bullish requirement relaxed by config
        if require_bull:
            if float(close[i]) < float(open_[i]) * (1.0 - bull_tol_bps / 10000.0):
                return {"action": "hold", "reason": "pullback_waiting:need_rebound_bull"}
        # Rebound-close gate:
        # Default: require close >= pullback EMA reference.
        # Relax ONLY during EMA9 break recent window:
        # allow close to be slightly below EMA by epsilon (reuse TREND_PULLBACK_EMA9_RELAX_BPS).
        if float(close[i]) < ema_ref:
            relax_bps = float(getattr(C, "TREND_PULLBACK_EMA9_RELAX_BPS", 0.0))
            eps = float(ema_ref) * (relax_bps / 10000.0)
            if bool(ema9_break_recent) and (float(close[i]) >= float(ema_ref) - eps):
                pass
            else:
                d = ema_ref - float(close[i])
                return {
                    "action": "hold",
                    "reason": f"pullback_waiting:rebound_close_below_ema(d={d:.2f},relax={int(bool(ema9_break_recent))},eps={eps:.2f})",
                }

    # RSI下限（反発確認後に見る）
    rsi_now = float(rsi14[i])
    rsi_min = float(getattr(C, "TREND_PULLBACK_RSI_MIN", 50.0))
    if not _is_finite(rsi_now):
        return {"action": "hold", "reason": "pullback_fail:nan_rsi"}
    if rsi_now < rsi_min:
        return {"action": "hold", "reason": f"pullback_fail:rsi_low(rsi={rsi_now:.2f} < {rsi_min:.2f})"}

    # 反発足の形状（実体/値幅、上ヒゲ/値幅）
    rng = float(high[i]) - float(low[i])
    body = abs(float(close[i]) - float(open_[i]))
    if not (_is_finite(rng) and _is_finite(body)) or rng <= 0:
        return {"action": "hold", "reason": "pullback_fail:bad_candle"}

    body_ratio = body / rng
    body_min = float(getattr(C, "TREND_PULLBACK_REBOUND_BODY_RATIO_MIN", 0.35))
    if body_ratio < body_min:
        return {"action": "hold", "reason": f"pullback_fail:weak_rebound_body(r={body_ratio:.2f} < {body_min:.2f})"}

    upper_wick = float(high[i]) - max(float(open_[i]), float(close[i]))
    if not _is_finite(upper_wick) or upper_wick < 0:
        return {"action": "hold", "reason": "pullback_fail:bad_candle"}
    upper_wick_ratio = upper_wick / rng
    uw_max = float(getattr(C, "TREND_PULLBACK_REBOUND_UPPER_WICK_RATIO_MAX", 0.35))
    # EMA9 break は frequency lever:
    # 反発初動は上ヒゲが出やすく、ここを ref_high と同じ閾値にすると多くが死ぬ。
    # EMA9 break（break_level_kind=="ema9"）のときだけ上限を緩和する。
    if break_level_kind == "ema9":
        uw_max = max(uw_max, 0.30)
    if upper_wick_ratio > uw_max:
        return {"action": "hold", "reason": f"pullback_fail:upper_wick(r={upper_wick_ratio:.2f} > {uw_max:.2f})"}
    # Extra quality gates (aim: reduce overtrading, improve expectancy)
    # defaults tightened slightly to reduce low-quality churn
    buy_rsi_min = float(getattr(C, "TREND_PULLBACK_BUY_RSI_MIN", rsi_min + 10.0))

    if rsi_now < buy_rsi_min:
        return {"action": "hold", "reason": f"pullback_waiting:buy_rsi_low(rsi={rsi_now:.2f} < {buy_rsi_min:.2f})"}
    rsi_prev = float(rsi14[i - 1])
    require_buy_rsi_rising = bool(getattr(C, "TREND_PULLBACK_BUY_REQUIRE_RSI_RISING", True))
    if require_buy_rsi_rising:
        if _is_finite(rsi_prev) and rsi_now < rsi_prev:
            return {"action": "hold", "reason": f"pullback_waiting:buy_rsi_not_rising(rsi={rsi_now:.2f} prev={rsi_prev:.2f})"}

    buy_body_min = float(getattr(C, "TREND_PULLBACK_BUY_BODY_RATIO_MIN", 0.78))
    if body_ratio < buy_body_min:
        return {"action": "hold", "reason": f"pullback_waiting:buy_weak_body(r={body_ratio:.2f} < {buy_body_min:.2f})"}
    buy_uw_max = float(getattr(C, "TREND_PULLBACK_BUY_UPPER_WICK_RATIO_MAX", 0.12))
    # EMA9 break は frequency lever:
    # buy 足の上ヒゲ制約も同様に緩和しないと stageB を抜けても買いで落ちる。
    if break_level_kind == "ema9":
        buy_uw_max = max(buy_uw_max, 0.30)
    if upper_wick_ratio > buy_uw_max:
        return {"action": "hold", "reason": f"pullback_waiting:buy_upper_wick(r={upper_wick_ratio:.2f} > {buy_uw_max:.2f})"}

    # SL/TP（既存の固定ロジックを踏襲）
    sl_k = float(getattr(C, "TREND_PULLBACK_SL_ATR_K", getattr(C, "TREND_SL_ATR_K", 0.55)))
    tp_k = float(getattr(C, "TREND_PULLBACK_TP_ATR_K", getattr(C, "TREND_TP_ATR_K", 0.75)))
    entry = float(price)
    stop = float(entry - atr_now * sl_k)
    tp = float(entry + atr_now * tp_k)
    if not (stop < entry < tp):
        return {"action": "hold", "reason": "invalid_sl_tp"}

    # --- NEW: reject overly wide stop (expectancy protection) ---
    # Too-wide stops tended to have very low win rate in practice.
    max_stop_bps = float(getattr(C, "TREND_PULLBACK_MAX_STOP_BPS", 0.0))
    if max_stop_bps and max_stop_bps > 0:
        sl_bps = (entry - stop) / entry * 10000.0
        if sl_bps - 1e-9 > max_stop_bps:
            return {
                "action": "hold",
                "reason": f"pullback_waiting:stop_too_wide(sl_bps={sl_bps:.2f} > {max_stop_bps:.2f})",
            }

    return {
        "action": "buy",
        "entry": float(entry),
        "stop": float(stop),
        "take_profit": float(tp),
        "reason": "trend_pullback_entry_2stage" if two_stage else "trend_pullback_entry",
        }
# ============================================================
# Regime / Direction (1h filter)
# ============================================================

def detect_regime_1h_precomputed(
    close: list[float] | np.ndarray,
    i: int,
    adx_arr: list[float] | np.ndarray,
    ema_fast_arr: list[float] | np.ndarray,
    ema_slow_arr: list[float] | np.ndarray,
) -> tuple[str, str]:
    """
    1hのprecomputed指標から、regime と direction を返す。
    backtest.py 側がこれを使用し、direction=="short" を global gate に使う。

    Returns:
        regime: "trend" | "range"
        direction: "long" | "short" | "none"
    """
    th_adx = float(getattr(C, "TREND_ADX_TH", 14.0))

    try:
        adx = float(adx_arr[i])
        ef = float(ema_fast_arr[i])
        es = float(ema_slow_arr[i])
        c = float(close[i])
    except Exception:
        return ("range", "none")

    if not (_is_finite(adx) and _is_finite(ef) and _is_finite(es) and _is_finite(c)):
        return ("range", "none")

    regime = "trend" if adx >= th_adx else "range"

    # direction: EMA fast vs slow
    # In trend regime, reduce the buffer so direction becomes "long/short" more often
    # (helps avoid excessive "none" that blocks entries).
    base = float(getattr(C, "EMA_DIR_BUFFER_BPS", 0.0))
    buf_bps = base * 0.5 if regime == "trend" else base

    if ef > es * (1.0 + buf_bps / 10000.0):
        direction = "long"
    elif ef < es * (1.0 - buf_bps / 10000.0):
        direction = "short"
    else:
        direction = "none"
    return (regime, direction)


def detect_regime_1h(
    close: list[float] | np.ndarray,
    adx_arr: list[float] | np.ndarray,
    ema_fast_arr: list[float] | np.ndarray,
    ema_slow_arr: list[float] | np.ndarray,
) -> tuple[str, str]:
    """
    slice版（runner互換）。最後の足で判定。
    """
    if close is None or len(close) < 2:
        return ("range", "none")
    i = len(close) - 1
    return detect_regime_1h_precomputed(
        close=close,
        i=i,
        adx_arr=adx_arr,
        ema_fast_arr=ema_fast_arr,
        ema_slow_arr=ema_slow_arr,
    )

def _annotate_pullback_stage(sig: dict, *, two_stage: bool) -> dict:
    """Attach pullback stage info (A/B/BUY) to signal dict for diagnostics.

    This is intentionally non-invasive: it does not change existing 'reason' strings.
    Callers (runner/backtest) can aggregate stageA/stageB behavior consistently.
    """
    try:
        if not isinstance(sig, dict):
            return sig
        r = str(sig.get("reason", ""))
        stage = None

        # BUY (entry fired)
        if r.startswith("trend_pullback_entry"):
            stage = "BUY"
        else:
            # Stage A: preconditions (near EMA, depth, setup alignment) before waiting for breakout.
            if two_stage and (
                r.startswith("pullback_waiting:not_near_ema")
                or r.startswith("pullback_fail:too_deep")
                or r.startswith("pullback_waiting:setup_")
                or r.startswith("pullback_waiting:atr_too_small")
                or r.startswith("pullback_fail:nan")
            ):
                stage = "A"
            # Stage B: waiting for breakout/rebound/follow-through or failing those checks.
            elif (
                r.startswith("pullback_waiting:")
                or r.startswith("pullback_fail:")
                or r.startswith("pullback_stageA_")
                or r.startswith("pullback_stageB_")
            ):
                stage = "B"

        if stage is not None and "pb_stage" not in sig:
            sig["pb_stage"] = stage
        if two_stage and "pb_two_stage" not in sig:
            sig["pb_two_stage"] = True
        return sig
    except Exception:
        return sig

# ============================================================
# Range signals (entry entry / entry execution idea)
# ============================================================

def signal_range_entry_precomputed(
    *,
    regime: str | None = None,
    direction: str | None = None,
    i: int,
    open_,
    high,
    low,
    close,
    ema9,
    ema21,
    rsi14,
    atr14,
    recent_min_low=None,
    prev_high=None,
    **kwargs,
):
    """
    レンジ局面のエントリー判定（precomputed）。
    既存のbacktest構造に合わせ、"buy"/"hold" を返す。
    """
    if i <= 1:
        return {"action": "hold", "reason": "warmup"}

    # Guard: avoid range longs when higher-timeframe direction is clearly short
    if str(direction).lower() == "short":
        return {"action": "hold", "reason": "range_disabled_short_dir"}

    c = float(close[i])
    e9 = float(ema9[i])
    e21 = float(ema21[i])
    e9_prev = float(ema9[i - 1])
    rsi_now = float(rsi14[i])
    rsi_prev = float(rsi14[i - 1])
    atr_now = float(atr14[i]) if i < len(atr14) else float("nan")

    if not (_is_finite(c) and _is_finite(e9) and _is_finite(e21) and _is_finite(rsi_now)):
        return {"action": "hold", "reason": "range_nan"}

    # Optional gate: require minimum ATR (avoid too-low volatility where fees dominate / whipsaw)
    atr_bps = (atr_now / max(c, 1e-12)) * 10000.0 if _is_finite(atr_now) else float('nan')
    min_atr_bps = float(getattr(C, 'RANGE_ENTRY_MIN_ATR_BPS', 0.0) or 0.0)
    if min_atr_bps > 0.0 and (_is_finite(atr_bps) and atr_bps < min_atr_bps):
        return _hold(f"range_atr_too_low(atr_bps={atr_bps:.2f} th={min_atr_bps:.2f})")
    # Entry safety gate: avoid buying when price is too far from EMA9/EMA21 (optional; set thresholds in config)
    ema9_dist_bps = abs(c - e9) / max(c, 1e-12) * 10000.0
    max_ema9 = float(getattr(C, 'RANGE_ENTRY_MAX_EMA9_DIST_BPS', 0.0) or 0.0)
    if max_ema9 > 0.0 and ema9_dist_bps > max_ema9:
        return _hold(f"range_ema9_dist_too_far(dist_bps={ema9_dist_bps:.2f} th={max_ema9:.2f})")
    ema21_dist_bps = abs(c - e21) / max(c, 1e-12) * 10000.0
    max_ema21 = float(getattr(C, 'RANGE_ENTRY_MAX_EMA21_DIST_BPS', 0.0) or 0.0)
    if max_ema21 > 0.0 and ema21_dist_bps > max_ema21:
        return _hold(f"range_ema21_dist_too_far(dist_bps={ema21_dist_bps:.2f} th={max_ema21:.2f})")
    max_rsi = float(getattr(C, 'RANGE_ENTRY_MAX_RSI14', 0.0) or 0.0)
    if max_rsi > 0.0 and rsi_now > max_rsi:
        return _hold(f"range_rsi_too_high(rsi={rsi_now:.2f} th={max_rsi:.2f})")
    # RSI rising
    if not (rsi_now > rsi_prev):
        return {"action": "hold", "reason": "range_rsi_not_rising"}

    # RSI high ceiling
    rsi_max = float(getattr(C, "RANGE_RSI_BUY_MAX", 64.0))
    if rsi_now >= rsi_max:
        return {"action": "hold", "reason": "range_rsi_high"}

    # Price above EMA9 and EMA9 rising
    if not (c >= e9):
        return {"action": "hold", "reason": "range_below_ema9"}
    # EMA9 minimum gap gate: avoid entries hugging EMA9 (whipsaw -> stop).
    # If you want to tune, add RANGE_ENTRY_MIN_EMA9_GAP_BPS to config.py.
    min_ema9_gap_bps = float(getattr(C, "RANGE_ENTRY_MIN_EMA9_GAP_BPS", 4.0) or 0.0)
    if min_ema9_gap_bps > 0.0:
        ema9_gap_bps = (c - e9) / max(c, 1e-12) * 10000.0
        if ema9_gap_bps < min_ema9_gap_bps:
            return _hold(f"range_ema9_gap_too_small(gap_bps={ema9_gap_bps:.2f} th={min_ema9_gap_bps:.2f})")
    if not (e9 > e9_prev):
        return {"action": "hold", "reason": "range_ema9_not_rising"}
    if not (e9 >= e21):
        return {"action": "hold", "reason": "range_ema9_below_ema21"}

    # --- Range quality filters (entry gate) ---
    # 目的: レンジ→トレンド化の初動や急拡大ボラ局面でのレンジ買いを抑止する
    if _is_finite(atr_now) and atr_now > 0:
        # (1) ATR expansion filter: ATR_now > MA(N) * MULT
        atr_mult = float(getattr(C, "RANGE_QUALITY_ATR_EXPAND_MULT", 0.0) or 0.0)
        if atr_mult > 0.0:
            n = int(getattr(C, "RANGE_QUALITY_ATR_EXPAND_MA_BARS", 24) or 24)
            n = max(3, min(n, 200))
            if i >= n + 1 and i < len(atr14):
                try:
                    ma = float(np.mean(np.asarray(atr14[i - n:i], dtype=float)))
                except Exception:
                    ma = float("nan")
                if _is_finite(ma) and ma > 0 and atr_now > ma * atr_mult:
                    return {
                        "action": "hold",
                        "reason": f"range_quality_atr_expand(atr={atr_now:.6f} ma={ma:.6f} mult={atr_mult:.3f})",
                    }

        # (2) EMA spread filter: abs(EMA9-EMA21)/ATR <= MAX
        spread_max = float(getattr(C, "RANGE_QUALITY_EMA_SPREAD_ATR_MAX", 0.0) or 0.0)
        if spread_max > 0.0:
            spread = abs(e9 - e21) / float(atr_now)
            if _is_finite(spread) and spread > spread_max:
                return {
                    "action": "hold",
                    "reason": f"range_quality_ema_spread(spread={spread:.4f} th={spread_max:.4f})",
                }

    # Near low (ATR based)
    if _is_finite(atr_now) and atr_now > 0:
        dist = c - float(low[i])
        th = float(getattr(C, "RANGE_NEAR_LOW_ATR_MULT", 2.0))
        if dist > th * atr_now:
            return {"action": "hold", "reason": "range_not_near_low"}

    # --- build order params (required by backtest/runner) ---
    if not (_is_finite(atr_now) and atr_now > 0):
        return {"action": "hold", "reason": "bad_atr"}
    # Minimum ATR (in bps) gate for range entries (config: RANGE_MIN_ATR_BPS).
    # Backward-compat: if a legacy key exists, honor it too.
    min_atr_bps = float(getattr(C, "RANGE_MIN_ATR_BPS", 0.0))
    if min_atr_bps <= 0.0:
        min_atr_bps = float(getattr(C, "RANGE_ENTRY_MIN_ATR_BPS", 0.0))
    if min_atr_bps > 0.0 and c > 0.0:
        atr_bps = (atr_now / c) * 10000.0
        if atr_bps < min_atr_bps:
            return {
                "action": "hold",
                "reason": f"range_min_atr_bps(atr_bps={atr_bps:.2f} th={min_atr_bps:.2f})",
            }
    entry = c
    sl_mult = float(getattr(C, "RANGE_ATR_SL_MULT", 1.0))
    tp_mult = float(getattr(C, "RANGE_ATR_TP_MULT", 1.0))
    stop_below_low_atr = float(getattr(C, "RANGE_STOP_BELOW_LOW_ATR_MULT", 0.5))
    lb = int(getattr(C, "RANGE_RECENT_LOW_LOOKBACK", 20))
    if lb < 5:
        lb = 5
    recent_low = float(min(low[max(0, i - lb): i + 1]))
    base_stop = recent_low - atr_now * stop_below_low_atr
    atr_stop = entry - atr_now * sl_mult
    use_tighter = bool(getattr(C, "RANGE_STOP_USE_TIGHTER", False))
    stop = float(max(base_stop, atr_stop) if use_tighter else min(base_stop, atr_stop))
    # Cap range stop distance to avoid fat-tail losses (expectancy fix).
    # If RANGE_MAX_STOP_BPS > 0, do not allow stop deeper than entry - max_bps.
    max_stop_bps = float(getattr(C, "RANGE_MAX_STOP_BPS", 0.0) or 0.0)
    if max_stop_bps > 0:
        stop_floor = float(entry) * (1.0 - max_stop_bps / 10000.0)
        stop = float(max(stop, stop_floor))
    take_profit = float(entry + atr_now * tp_mult)
    if stop <= 0 or take_profit <= entry or not (stop < entry < take_profit):
        return {"action": "hold", "reason": "invalid_sl_tp"}
    return {
        "action": "buy",
        "entry": float(entry),
        "stop": float(stop),
        "take_profit": float(take_profit),
        "reason": "range_entry",
    }

def exit_signal_entry_precomputed(
    *,
    regime: str,
    direction: str,
    i: int,
    close,
    ema9,
    ema21=None,
    rsi14=None,
    atr14=None,
    state: dict | None = None,
    entry_px: float | None = None,
):
    """
    Live-compatible exit signal (used by runner):
      - range regime: EMA21 break (primary), ATR soft stop, optional EMA9 cross (supplement)
    Returns:
      - {"action":"exit","reason":...} or None
    """
    if i is None or i <= 0:
        return None

    if str(regime) != "range":
        return None

    # Default to long (spot). direction may be "none" depending on upstream regime detector.
    dir_s = str(direction or "long").lower()

    try:
        c0 = float(close[i - 1])
        c1 = float(close[i])
        e0 = float(ema9[i - 1])
        e1 = float(ema9[i])
    except Exception:
        return None

    if str(regime) == "range":
         # --- range: bearish RSI exit ---
         # NOTE: Uses precomputed rsi14 if provided by caller (runner/backtest).
         if dir_s == "long" and rsi14 is not None and i < len(rsi14):
             rsi_th = float(getattr(C, "RANGE_BEARISH_EXIT_RSI_TH", 50.0))
             min_loss_bps = float(getattr(C, "RANGE_BEARISH_EXIT_MIN_LOSS_BPS", 11.0))
             pnl_bps = (float(close[i]) / float(entry_px) - 1.0) * 10_000.0
             if float(rsi14[i]) < rsi_th and pnl_bps <= -abs(min_loss_bps):
                 cd_bars = int(getattr(C, "RANGE_UNFAV_EXIT_COOLDOWN_BARS", 0))
                 cd = {"kind": "range_unfav_exit", "bars": cd_bars} if cd_bars > 0 else None
                 out = {"action": "sell", "reason": f"RANGE_BEARISH_EXIT(rsi<{int(rsi_th)})"}
                 if cd is not None:
                     out["cooldown"] = cd
                 return out
    # ------------------------------------------------------------
    # Primary exits for range regime:
    #   1) EMA21 break (range invalidation)
    #   2) ATR soft stop (exit before hard SL when loss grows)
    # ------------------------------------------------------------
    # Range: EMA21 break is regime invalidation only (no exit here)
    if getattr(C, "RANGE_EXIT_ON_EMA21_BREAK", False) and ema21 is not None:
        buf_bps = float(getattr(C, "RANGE_EXIT_EMA21_BUFFER_BPS", 0.0) or 0.0)
        thr = float(ema21[i]) * (1.0 - buf_bps / 10000.0)
        if float(close[i]) < thr:
            # regime invalidation marker (optional; caller may pass a mutable dict)
            if state is not None:
                state["range_ema21_broken"] = True

    if entry_px is not None and atr14 is not None:
        mult = float(getattr(C, "RANGE_EARLY_EXIT_LOSS_ATR_MULT", 0.0) or 0.0)
        if mult > 0 and (float(entry_px) - float(close[i])) > float(atr14[i]) * mult:
            return {"action": "exit", "reason": "RANGE_EARLY_LOSS_ATR"}

    # ------------------------------------------------------------
    # Range bearish RSI exit (RouteA main):
    #   Exit when RSI drops below threshold AND unrealized loss exceeds min_loss_bps.
    #   - Preferred keys:
    #       RANGE_BEARISH_EXIT_RSI_TH
    #       RANGE_BEARISH_EXIT_MIN_LOSS_BPS
    #   - Backward compatible aliases:
    #       RANGE_EXIT_RSI_TH
    #       RANGE_EMA9_CROSS_EXIT_MIN_LOSS_BPS / MIN_LOSS_BPS
    # ------------------------------------------------------------
    try:
        rsi_th = getattr(C, "RANGE_BEARISH_EXIT_RSI_TH", None)
        if rsi_th is None:
            rsi_th = getattr(C, "RANGE_EXIT_RSI_TH", None)
        rsi_th = float(rsi_th) if rsi_th is not None else 0.0
    except Exception:
        rsi_th = 0.0

    if rsi_th > 0.0 and (rsi14 is not None) and (entry_px is not None):
        try:
            rsi_now = float(rsi14[i])
        except Exception:
            rsi_now = float("nan")

        if _is_finite(rsi_now) and rsi_now <= rsi_th:
            # min loss (bps)
            _ml = getattr(C, "RANGE_BEARISH_EXIT_MIN_LOSS_BPS", None)
            if _ml is None:
                _ml = getattr(C, "RANGE_EMA9_CROSS_EXIT_MIN_LOSS_BPS", None)
            if _ml is None:
                _ml = getattr(C, "MIN_LOSS_BPS", 0.0)
            min_loss_bps = float(_ml or 0.0)

            # long loss: price moved down against entry
            u_bps = (float(close[i]) / float(entry_px) - 1.0) * 10000.0
            if (min_loss_bps <= 0.0) or (u_bps <= -min_loss_bps):
                cd_bars = int(getattr(
                    C,
                    "RANGE_UNFAV_EXIT_COOLDOWN_BARS",
                    getattr(C, "RANGE_EMA9_EXIT_COOLDOWN_BARS", 12),
                ))
                out = {"action": "exit", "reason": "RANGE_BEARISH_RSI"}
                if cd_bars > 0:
                    out["cooldown"] = {"kind": "range_unfav_exit", "bars": int(cd_bars), "clear_on": "close_reclaim_ema9"}
                return out

    # Cross logic:
    #  - long: close crosses down EMA9
    #  - short: close crosses up EMA9 (future-proof)
    # Optional supplement (default disabled): EMA9 cross exit
    if not bool(getattr(C, "RANGE_EXIT_ON_EMA9_CROSS", False)):
        return None
    if dir_s == "short":
        if c0 <= e0 and c1 > e1:
            # Optional: gate EMA9 cross exit to avoid over-cutting winners.
            # For short (future-proof): only exit if unrealized loss exceeds threshold.
            # Allow sweeping with either the new dedicated key or the legacy generic key.
            # - Preferred: RANGE_EMA9_CROSS_EXIT_MIN_LOSS_BPS
            # - Fallback:  MIN_LOSS_BPS
            _ml = getattr(C, "RANGE_EMA9_CROSS_EXIT_MIN_LOSS_BPS", None)
            if _ml is None:
                _ml = getattr(C, "MIN_LOSS_BPS", 0.0)
            min_loss_bps = float(_ml or 0.0)

            # Cooldown key alias:
            # - Preferred: RANGE_UNFAV_EXIT_COOLDOWN_BARS
            # - Fallback:  RANGE_EMA9_EXIT_COOLDOWN_BARS
            cd_bars = int(getattr(
                C,
                "RANGE_UNFAV_EXIT_COOLDOWN_BARS",
                getattr(C, "RANGE_EMA9_EXIT_COOLDOWN_BARS", 12),
            ))
            cooldown = None
            if cd_bars > 0:
                cooldown = {"kind": "range_ema9_exit", "bars": int(cd_bars), "clear_on": "close_reclaim_ema9"}
            if entry_px is None or min_loss_bps <= 0:
                out = {"action": "exit", "reason": "RANGE_EMA9_CROSS_UP"}
                if cooldown is not None:
                    out["cooldown"] = cooldown
                return out
            # short loss: price moved up against entry
            u_bps = (float(close[i]) / float(entry_px) - 1.0) * 10000.0
            if u_bps >= min_loss_bps:
                out = {"action": "exit", "reason": "RANGE_EMA9_CROSS_UP"}
                if cooldown is not None:
                    out["cooldown"] = cooldown
                return out

        return None

    if c0 >= e0 and c1 < e1:
        # Optional: gate EMA9 cross exit to avoid over-cutting range longs.
        # Exit only if unrealized loss exceeds threshold (bps). If threshold <= 0, behave as before.
        # Allow sweeping with either the new dedicated key or the legacy generic key.
        # - Preferred: RANGE_EMA9_CROSS_EXIT_MIN_LOSS_BPS
        # - Fallback:  MIN_LOSS_BPS
        _ml = getattr(C, "RANGE_EMA9_CROSS_EXIT_MIN_LOSS_BPS", None)
        if _ml is None:
            _ml = getattr(C, "MIN_LOSS_BPS", 0.0)
        min_loss_bps = float(_ml or 0.0)

        # Cooldown key alias:
        # - Preferred: RANGE_UNFAV_EXIT_COOLDOWN_BARS
        # - Fallback:  RANGE_EMA9_EXIT_COOLDOWN_BARS
        cd_bars = int(getattr(
            C,
            "RANGE_UNFAV_EXIT_COOLDOWN_BARS",
            getattr(C, "RANGE_EMA9_EXIT_COOLDOWN_BARS", 12),
        ))
        cooldown = None
        if cd_bars > 0:
            cooldown = {"kind": "range_ema9_exit", "bars": int(cd_bars), "clear_on": "close_reclaim_ema9"}
        if entry_px is None or min_loss_bps <= 0:
            out = {"action": "exit", "reason": "RANGE_EMA9_CROSS_DOWN"}
            if cooldown is not None:
                out["cooldown"] = cooldown
            return out
        u_bps = (float(close[i]) / float(entry_px) - 1.0) * 10000.0
        if u_bps <= -min_loss_bps:
            out = {"action": "exit", "reason": "RANGE_EMA9_CROSS_DOWN"}
            if cooldown is not None:
                out["cooldown"] = cooldown
            return out

    return None

def signal_range_entry(
    open_: list[float],
    high: list[float],
    low: list[float],
    close: list[float],
    ema9: list[float],
    ema21: list[float],
    rsi14: list[float],
    atr14: list[float],
    direction: str | None = None,
) -> dict:
    """
    slice版レンジシグナル（最後の足）。
    """
    if close is None or len(close) < 2:
        return {"action": "hold", "reason": "warmup"}
    i = len(close) - 1
    return signal_range_entry_precomputed(
        i=i,
        direction=direction,
        open_=open_,
        high=high,
        low=low,
        close=close,
        ema9=ema9,
        ema21=ema21,
        rsi14=rsi14,
        atr14=atr14,
    )

# ============================================================
# Trend signals (entry precomputed)
# ============================================================

def signal_entry_precomputed(
    regime: str,
    direction: str,
    i: int,
    open_: list[float],
    high: list[float],
    low: list[float],
    close: list[float],
    ema9: list[float],
    ema21: list[float],
    rsi14: list[float],
    atr14: list[float],
    recent_min_low: list[float],
    prev_high: list[float],
) -> dict:

    if i <= 1:
        return {"action": "hold", "reason": "warmup"}

    if str(regime).lower() == "range":
        return signal_range_entry_precomputed(
            i=i,
            open_=open_,
            high=high,
            low=low,
            close=close,
            ema9=ema9,
            ema21=ema21,
            rsi14=rsi14,
            atr14=atr14,
        )
    # Decide trend entry mode early (so global gates can be mode-aware)
    entry_mode = str(getattr(C, "TREND_ENTRY_MODE", "BREAKOUT")).upper()

    # Pullback 2-stage flag (used by diagnostics annotation)
    two_stage = bool(getattr(C, "TREND_PULLBACK_TWO_STAGE", True))

    # --- Global downtrend filter ---
    reg = str(regime).lower()
    dir_ = str(direction).lower()
    # In pullback mode, don't hard-block "dir==none" here.
    # (Let TREND_PULLBACK_REQUIRE_DIR_LONG control it in _trend_pullback_precomputed.)
    if entry_mode != "PULLBACK":
        if reg == "trend" and dir_ == "none" and bool(getattr(C, "TREND_BLOCK_DIR_NONE", True)):
            return {"action": "hold", "reason": "trend_dir_none"}
    if reg == "trend" and dir_ == "short":
        # spot safety valve: allow disabling this for trade-frequency sweeps
        #
        # NOTE:
        # - breakout: use TREND_BLOCK_DIR_SHORT (default True)
        # - pullback: prefer TREND_PULLBACK_REQUIRE_DIR_LONG (sweep-able) so we can
        #   optionally allow long pullback entries even when filter direction is short.
        if entry_mode == "PULLBACK":
            if bool(getattr(C, "TREND_PULLBACK_REQUIRE_DIR_LONG", True)):
                # Optional relaxation: allow PULLBACK entries even when dir==short
                # only if EMA9 break is "recent" AND slope conditions pass (with EMA21 slope delayed) (frequency lever, no lookahead).
                pb_break_mode = str(getattr(C, "TREND_PULLBACK_BREAK_MODE", "high")).lower()
                pb_modes = [m.strip() for m in pb_break_mode.split("|") if m.strip()]
                pb_uses_ema9_break = ("ema9" in pb_modes)
                relax_flag = (
                    pb_uses_ema9_break
                    and bool(getattr(C, "TREND_PULLBACK_RELAX_DOWNTREND_STRONG_ON_EMA9_BREAK", False))
                )

                if relax_flag:
                    atr_now_for_break = 0.0
                    try:
                        atr_now_for_break = float(atr14[i])
                    except Exception:
                        atr_now_for_break = 0.0

                    # Determine recent break + "age" (bars since last EMA9 break within window)
                    n = int(getattr(C, "TREND_PULLBACK_EMA9_BREAK_RECENT_BARS", 0))
                    n = max(0, min(n, 50))
                    ema_recent_ok = False
                    ema_break_age = 9999
                    if i > 0:
                        for k in range(i, max(1, i - n) - 1, -1):
                            if _ema9_break_at(
                                k=k,
                                open_=open_,
                                high=high,
                                low=low,
                                close=close,
                                ema9=ema9,
                                atr_now=atr_now_for_break,
                            ):
                                ema_recent_ok = True
                                ema_break_age = i - k
                                break

                    # EMA21 slope delay (A案):
                    # allow a short grace window after EMA9 break where EMA21 doesn't need to be flat/up yet.
                    delay_bars = min(2, n)

                    # slope conditions (no lookahead)
                    e9_up = False
                    e21_ok = False
                    try:
                        e9 = float(ema9[i])
                        e9_prev = float(ema9[i - 1])
                        e21 = float(ema21[i])
                        e21_prev = float(ema21[i - 1])
                        e9_tol_eff = float(
                            getattr(C, "TREND_PULLBACK_EMA9_RISE_TOL_BPS", getattr(C, "TREND_EMA9_RISE_TOL_BPS", 0.0))
                        )
                        tol_bps = float(getattr(C, "TREND_EMA21_FLAT_TOL_BPS", 0.0))
                        e9_up = bool(e9 >= e9_prev * (1.0 - e9_tol_eff / 10000.0))
                        e21_ok = bool(e21 >= e21_prev * (1.0 - tol_bps / 10000.0))
                    except Exception:
                        e9_up = False
                        e21_ok = False

                    e21_ok_eff = bool(e21_ok or (ema_break_age <= delay_bars))

                    if ema_recent_ok and e9_up and e21_ok_eff:
                        # pass (do not block by dir==short)
                        pass
                    else:

                        if not ema_recent_ok:
                            return {
                                "action": "hold",
                                "reason": (
                                    "downtrend_global_filter:pullback_require_dir_long:"
                                    f"relax_on_no_ema9_break_recent(n={n})"
                                ),
                            }
                        return {
                            "action": "hold",
                            "reason": (
                                "downtrend_global_filter:pullback_require_dir_long:"
                                f"relax_slope_fail(e9_up={e9_up},e21_ok={e21_ok},age={ema_break_age},delay={delay_bars},n={n})"
                            ),
                        }
                else:
                    return {
                        "action": "hold",
                        "reason": "downtrend_global_filter:pullback_require_dir_long",
                    }
        else:
            if bool(getattr(C, "TREND_BLOCK_DIR_SHORT", True)):
                return {"action": "hold", "reason": "downtrend_global_filter:block_dir_short"}

    # ---------- trend branch ----------
    price = float(close[i])
    atr_now = float(atr14[i])

    if not (_is_finite(price) and _is_finite(atr_now) and atr_now > 0):
        return {"action": "hold", "reason": "trend_nan_price_or_atr"}

    e9 = float(ema9[i])
    e21 = float(ema21[i])
    e9_prev = float(ema9[i - 1])
    e21_prev = float(ema21[i - 1])

    tol_bps = float(getattr(C, "TREND_EMA21_FLAT_TOL_BPS", 0.0))
    pos_tol_bps = float(getattr(C, "TREND_E9_OVER_E21_TOL_BPS", 0.0))
    e9_tol_bps = float(getattr(C, "TREND_EMA9_RISE_TOL_BPS", 0.0))

    is_pullback = (entry_mode == "PULLBACK")
    # Pullback は「押してから回帰」を狙うため、e9/e21 の厳格な一致で候補が枯れやすい。
    # pullback時は別tol（未設定ならデフォルトで緩め）を使う。
    pos_tol_eff = pos_tol_bps
    e9_tol_eff = e9_tol_bps
    if is_pullback:
        pos_tol_eff = float(getattr(C, "TREND_PULLBACK_E9_OVER_E21_TOL_BPS", max(pos_tol_bps, 20.0)))
        e9_tol_eff = float(getattr(C, "TREND_PULLBACK_EMA9_RISE_TOL_BPS", e9_tol_bps))

    cond_trend_pos = e9 >= e21 * (1.0 - pos_tol_eff / 10000.0)
    cond_trend_fast = e9 >= e9_prev * (1.0 - e9_tol_eff / 10000.0)
    cond_trend_slow = e21 >= e21_prev * (1.0 - tol_bps / 10000.0)

    # EMA9 break pullback is a frequency lever:
    # relax E9/E21 position constraint ONLY when:
    #   - entry_mode == PULLBACK
    #   - break_mode includes "ema9"
    #   - explicit flag enabled
    pb_break_mode = str(getattr(C, "TREND_PULLBACK_BREAK_MODE", "high")).lower()
    pb_modes = [m.strip() for m in pb_break_mode.split("|") if m.strip()]
    pb_uses_ema9_break = ("ema9" in pb_modes)
    relax_flag = (
        entry_mode == "PULLBACK"
        and pb_uses_ema9_break
        and bool(getattr(C, "TREND_PULLBACK_RELAX_E9_E21_ON_EMA9_BREAK", False))
    )
    # IMPORTANT:
    # "EMA9 break を使う pullback のときだけ" 緩和したいので、
    # break_mode に ema9 が含まれるだけではなく「当該足で EMA9 break が成立」した場合に限定する。
    ema9_break_now = False
    ema9_break_recent_trend = False
    if relax_flag and i >= 1:
        c = float(close[i])
        h = float(high[i])
        e = float(ema9[i])
        c_prev = float(close[i - 1])
        e_prev = float(ema9[i - 1])
        o_prev = float(open_[i - 1])
        l_prev = float(low[i - 1])

        strict_cross = (c > e) and (c_prev <= e_prev)

        # relaxed reclaim (same logic as pullback stage)
        relax_bps = float(getattr(C, "TREND_PULLBACK_EMA9_RELAX_BPS", 0.0))
        ema_dist_min = float(getattr(C, "TREND_PULLBACK_EMA9_DIST_ATR_MIN", 0.0))
        dist_ok = True
        if ema_dist_min > 0.0 and _is_finite(atr_now) and atr_now > 0:
            ema_dist_atr = (c - e) / float(atr_now)
            dist_ok = (ema_dist_atr + 1e-12) >= ema_dist_min

        tol = e_prev * (relax_bps / 10000.0)
        prev_min = min(float(l_prev), float(o_prev), float(c_prev))
        prev_max = max(float(l_prev), float(o_prev), float(c_prev))
        prev_touch_or_near = (prev_min <= (e_prev + tol)) and (prev_max >= (e_prev - tol))
        relaxed_reclaim = (relax_bps > 0.0) and (c > e) and prev_touch_or_near and dist_ok

        ema9_break_now = strict_cross or relaxed_reclaim

        # Further relaxation (frequency lever):
        # allow recent-window EMA9 break too, BUT only if we are still above EMA9 now
        # (prevents "old break" from bypassing trend position without current momentum)
        ema9_break_recent_trend = _ema9_break_recent(
            i=i,
            open_=open_,
            high=high,
            low=low,
            close=close,
            ema9=ema9,
            atr_now=atr_now,
        ) and (max(c, h) > e)

    # relax E9/E21 position constraint when EMA9 break is active:
    #  - break now (strict_cross/relaxed_reclaim), OR
    #  - break recent window and still above EMA9
    relax_e9_over_e21 = relax_flag and (ema9_break_now or ema9_break_recent_trend)
    if not ((cond_trend_pos or relax_e9_over_e21) and cond_trend_fast and cond_trend_slow):
        if not cond_trend_pos:
            return {"action": "hold", "reason": f"trend_fail:e9_le_e21(tol={pos_tol_eff:.1f})"}
        return {
            "action": "hold",
            "reason": f"trend_fail:slope(e9_up={cond_trend_fast}, e21_ok={cond_trend_slow})",
        }

    # ======================================================
    # Trend entry mode switch
    # ======================================================
    if entry_mode == "PULLBACK":
        # Pullback precomputed path does not use breakout-only candle filters.
        sig = _trend_pullback_precomputed(
            regime=regime,
            direction=direction,
            i=i,
            open_=open_,
            high=high,
            low=low,
            close=close,
            ema9=ema9,
            ema21=ema21,
            rsi14=rsi14,
            atr14=atr14,
        )
        # Pullback 2-stage flag (used by diagnostics annotation)
        two_stage = bool(getattr(C, "TREND_PULLBACK_TWO_STAGE", True))
        sig = _annotate_pullback_stage(sig, two_stage=two_stage)
        return sig

    # RSI rising (delta)
    rsi_last = float(rsi14[i])
    rsi_prev = float(rsi14[i - 1])
    min_d = float(getattr(C, "TREND_RSI_RISE_MIN_DELTA", 0.0))
    if (rsi_last - rsi_prev) < min_d:
        return {
            "action": "hold",
            "reason": f"trend_fail:rsi_delta(d={rsi_last - rsi_prev:.2f} < {min_d:.2f})",
        }

    # breakout
    ph = float(prev_high[i])
    base = float(getattr(C, "TREND_BREAKOUT_BUFFER_BPS", 0.0))
    eff = base
    reg = str(regime).lower()
    dir_ = str(direction).lower()
    if reg == "trend" and dir_ == "long":
        eff = base * 0.5
    if price < ph * (1.0 + eff / 10000.0):
        return {
            "action": "hold",
            "reason": f"trend_fail:breakout(buf_eff={eff:.2f} base={base:.2f} reg={reg} dir={dir_})",
        }

    # follow-through body filter（★ sweep 対象 ★）
    body = abs(close[i] - open_[i])
    rng = high[i] - low[i]
    if not (_is_finite(body) and _is_finite(rng)) or rng <= 0:
        return {"action": "hold", "reason": "trend_fail:bad_candle"}

    body_ratio = body / rng
    body_min = float(getattr(C, "TREND_FOLLOW_BODY_RATIO_MIN", 0.40))
    if body_ratio < body_min:
        return {
            "action": "hold",
            "reason": f"trend_fail:weak_follow_body(r={body_ratio:.2f} < {body_min:.2f})",
        }

    # upper-wick filter（ダマシの叩き戻しを除外）
    upper_wick = high[i] - max(open_[i], close[i])
    if not _is_finite(upper_wick) or upper_wick < 0:
        return {"action": "hold", "reason": "trend_fail:bad_candle"}
    upper_wick_ratio = upper_wick / rng
    upper_wick_max = float(getattr(C, "TREND_UPPER_WICK_RATIO_MAX", 0.30))
    if upper_wick_ratio > upper_wick_max:
        return {
            "action": "hold",
            "reason": f"trend_fail:upper_wick(r={upper_wick_ratio:.2f} > {upper_wick_max:.2f})",
        }

    # close position filter（終値が高値寄り＝勢い維持）
    close_pos = (close[i] - low[i]) / rng
    if not _is_finite(close_pos):
        return {"action": "hold", "reason": "trend_fail:bad_candle"}

    close_pos_min = float(getattr(C, "TREND_PULLBACK_CLOSE_POS_MIN", 0.75))
    if close_pos < close_pos_min:
        return {
            "action": "hold",
            "reason": f"trend_fail:close_pos(p={close_pos:.2f} < {close_pos_min:.2f})",
        }

    # SL / TP
    sl_k = float(getattr(C, "TREND_SL_ATR_K", 0.55))
    tp_k = float(getattr(C, "TREND_PULLBACK_TP_ATR_K", getattr(C, "TREND_TP_ATR_K", 0.75)))

    entry = price
    stop = entry - atr_now * sl_k
    tp = entry + atr_now * tp_k

    if not (stop < entry < tp):
        return {"action": "hold", "reason": "invalid_sl_tp"}

    return {
        "action": "buy",
        "entry": float(entry),
        "stop": float(stop),
        "take_profit": float(tp),
        "reason": "trend_entry",
    }



def signal_entry(
    regime: str,
    direction: str,
    open_: list[float],
    high: list[float],
    low: list[float],
    close: list[float],
    ema9: list[float],
    ema21: list[float],
    rsi14: list[float],
    atr14: list[float],
) -> dict:
    """
    slice版（runner互換）。trend/range を regime で分岐し、最後の足で判断。
    ここでは precomputedで必要な prev_high/recent_min_low が無いので、簡易版。
    """
    entry_mode = str(getattr(C, "TREND_ENTRY_MODE", "BREAKOUT")).upper()
    # Global downtrend filter (PULLBACK では dir==none をここで落とさない。強い下落のブロックは backtest 側で担保)
    reg = str(regime).lower()
    dir_ = str(direction).lower()
    if entry_mode != "PULLBACK":
        if reg == "trend" and dir_ == "none" and bool(getattr(C, "TREND_BLOCK_DIR_NONE", True)):
            return {"action": "hold", "reason": "trend_dir_none"}
    if reg == "trend" and dir_ == "short":
        # Decompose reason to see which gate blocked (breakout vs pullback switch)
        if entry_mode == "PULLBACK":
            if bool(getattr(C, "TREND_PULLBACK_REQUIRE_DIR_LONG", True)):
                # Keep slice-version consistent with precomputed:
                # allow bypass only when EMA9 break is recent AND slope passes (EMA21 slope delayed).
                pb_break_mode = str(getattr(C, "TREND_PULLBACK_BREAK_MODE", "high")).lower()
                pb_modes = [m.strip() for m in pb_break_mode.split("|") if m.strip()]
                pb_uses_ema9_break = ("ema9" in pb_modes)
                relax_flag = (
                    pb_uses_ema9_break
                    and bool(getattr(C, "TREND_PULLBACK_RELAX_DOWNTREND_STRONG_ON_EMA9_BREAK", False))
                )

                if relax_flag and close is not None and len(close) >= 2:
                    i = len(close) - 1
                    atr_now_for_break = 0.0
                    try:
                        atr_now_for_break = float(atr14[i])
                    except Exception:
                        atr_now_for_break = 0.0

                    # Determine recent break + "age" (bars since last EMA9 break within window)
                    n = int(getattr(C, "TREND_PULLBACK_EMA9_BREAK_RECENT_BARS", 0))
                    n = max(0, min(n, 50))
                    ema_recent_ok = False
                    ema_break_age = 9999
                    if i > 0:
                        for k in range(i, max(1, i - n) - 1, -1):
                            if _ema9_break_at(
                                k=k,
                                open_=open_,
                                high=high,
                                low=low,
                                close=close,
                                ema9=ema9,
                                atr_now=atr_now_for_break,
                            ):
                                ema_recent_ok = True
                                ema_break_age = i - k
                                break

                    delay_bars = min(2, n)

                    e9_up = False
                    e21_ok = False
                    try:
                        e9 = float(ema9[i])
                        e9_prev = float(ema9[i - 1])
                        e21 = float(ema21[i])
                        e21_prev = float(ema21[i - 1])
                        e9_tol_eff = float(
                            getattr(C, "TREND_PULLBACK_EMA9_RISE_TOL_BPS", getattr(C, "TREND_EMA9_RISE_TOL_BPS", 0.0))
                        )
                        tol_bps = float(getattr(C, "TREND_EMA21_FLAT_TOL_BPS", 0.0))
                        e9_up = bool(e9 >= e9_prev * (1.0 - e9_tol_eff / 10000.0))
                        e21_ok = bool(e21 >= e21_prev * (1.0 - tol_bps / 10000.0))
                    except Exception:
                        e9_up = False
                        e21_ok = False
                    e21_ok_eff = bool(e21_ok or (ema_break_age <= delay_bars))

                    if ema_recent_ok and e9_up and e21_ok_eff:
                        pass
                    else:

                        if not ema_recent_ok:
                            return {"action": "hold", "reason": f"downtrend_global_filter:pullback_require_dir_long:relax_on_no_ema9_break_recent(n={n})"}
                        return {"action": "hold", "reason": f"downtrend_global_filter:pullback_require_dir_long:relax_slope_fail(e9_up={e9_up},e21_ok={e21_ok},age={ema_break_age},delay={delay_bars},n={n})"}
                return {"action": "hold", "reason": "downtrend_global_filter:pullback_require_dir_long"}
        else:
            if bool(getattr(C, "TREND_BLOCK_DIR_SHORT", True)):
                return {
                    "action": "hold",
                    "reason": "downtrend_global_filter:block_dir_short",
                }

    if close is None or len(close) < 2:
        return {"action": "hold", "reason": "warmup"}

    i = len(close) - 1

    if str(regime).lower() == "range":
        return signal_range_entry(
            open_=open_,
            high=high,
            low=low,
            close=close,
            ema9=ema9,
            ema21=ema21,
            rsi14=rsi14,
            atr14=atr14,
            direction=direction,
        )

    if entry_mode == "PULLBACK":
        return _trend_pullback_precomputed(
            regime=regime,
            direction=direction,
            i=i,
            open_=open_,
            high=high,
            low=low,
            close=close,
            ema9=ema9,
            ema21=ema21,
            rsi14=rsi14,
            atr14=atr14,
        )

    # Trend: minimal
    e9 = float(ema9[i])
    e21 = float(ema21[i])
    e9_prev = float(ema9[i - 1])
    e21_prev = float(ema21[i - 1])
    if not (_is_finite(e9) and _is_finite(e21) and _is_finite(e9_prev) and _is_finite(e21_prev)):
        return {"action": "hold", "reason": "trend_nan_ema"}

    tol_bps = float(getattr(C, "TREND_EMA21_FLAT_TOL_BPS", 0.0))
    pos_tol_bps = float(getattr(C, "TREND_E9_OVER_E21_TOL_BPS", 0.0))
    e9_tol_bps = float(getattr(C, "TREND_EMA9_RISE_TOL_BPS", 0.0))

    cond_trend_pos = (e9 >= e21 * (1.0 - pos_tol_bps / 10000.0))
    cond_trend_fast_up = (e9 >= e9_prev * (1.0 - e9_tol_bps / 10000.0))
    cond_trend_slow_ok = (e21 >= e21_prev * (1.0 - tol_bps / 10000.0))
    cond_trend = cond_trend_pos and cond_trend_fast_up and cond_trend_slow_ok

    if not cond_trend:
        if not cond_trend_pos:
            return {"action": "hold", "reason": f"trend_fail:trend:e9_le_e21(pos_tol_bps={pos_tol_bps:.1f})"}
        return {
            "action": "hold",
            "reason": (
                "trend_fail:trend:slope("
                f"e9_up={cond_trend_fast_up}, "
                f"e21_ok={cond_trend_slow_ok}, "
                f"tol_bps={tol_bps:.1f}, e9_tol_bps={e9_tol_bps:.1f})"
            ),
        }

    # RSI rising (trend side) - runner compat
    rsi_last = float(rsi14[i])
    rsi_prev = float(rsi14[i - 1])
    if not (_is_finite(rsi_last) and _is_finite(rsi_prev)):
        return {"action": "hold", "reason": "trend_nan_rsi"}
    min_d = float(getattr(C, "TREND_RSI_RISE_MIN_DELTA", 0.0))
    if not ((rsi_last - rsi_prev) >= min_d):
        return {
            "action": "hold",
            "reason": f"trend_fail:rsi_not_rising(d={rsi_last - rsi_prev:.2f} < {min_d:.2f}, rsi={rsi_last:.2f} prev={rsi_prev:.2f})",
        }



    # breakout (simple: close vs recent high)
    base = float(getattr(C, "TREND_BREAKOUT_BUFFER_BPS", 0.0))
    eff = base
    reg = str(regime).lower()
    dir_ = str(direction).lower()
    if reg == "trend" and dir_ == "long":
        eff = base * 0.5
    ph = float(max(high[:-1])) if len(high) > 1 else float(high[-1])
    price = float(close[i])
    if not (price >= ph * (1.0 + eff / 10000.0)):
        return {
            "action": "hold",
            "reason": f"trend_fail:breakout(buf_eff={eff:.2f} base={base:.2f} reg={reg} dir={dir_})",
        }

    # follow-through body filter (after breakout) - reduce fake breakouts
    body = abs(close[i] - open_[i])
    rng = high[i] - low[i]
    if not (_is_finite(body) and _is_finite(rng)) or rng <= 0:
        return {"action": "hold", "reason": "trend_fail:bad_candle_range"}

    body_ratio = body / rng if rng > 0 else 0.0
    body_min = float(getattr(C, "TREND_FOLLOW_BODY_RATIO_MIN", 0.40))
    if body_ratio < body_min:
        return {
            "action": "hold",
            "reason": f"trend_fail:weak_follow_body(r={body_ratio:.2f} < {body_min:.2f})",
        }

    # --- build order params (required by backtest/runner) ---
    atr_now = float(atr14[i]) if i < len(atr14) else float("nan")
    if not (_is_finite(atr_now) and atr_now > 0):
        return {"action": "hold", "reason": "bad_atr"}
    # Minimum ATR (in bps) gate (NOTE: currently applied in this entry path).
    min_atr_bps = float(getattr(C, "RANGE_MIN_ATR_BPS", 0.0) or 0.0)
    if min_atr_bps > 0:
        entry = float(price)
        atr_bps = (atr_now / entry) * 10000.0 if entry > 0 else 0.0
        if atr_bps < min_atr_bps:
            return {"action": "hold", "reason": "range_atr_bps_below_min"}
    sl_k = float(getattr(C, "TREND_SL_ATR_K", 0.55))
    tp_k = float(getattr(C, "TREND_PULLBACK_TP_ATR_K", getattr(C, "TREND_TP_ATR_K", 0.75)))
    entry = float(price)
    stop = float(entry - atr_now * sl_k)
    take_profit = float(entry + atr_now * tp_k)
    if stop <= 0 or take_profit <= entry or not (stop < entry < take_profit):
        return {"action": "hold", "reason": "invalid_sl_tp"}
    return {
        "action": "buy",
        "entry": float(entry),
        "stop": float(stop),
        "take_profit": float(take_profit),
        "reason": "trend_entry",
    }
