from __future__ import annotations

import math
import config as C


def _bps_to_frac(bps: float) -> float:
    return float(bps) / 10000.0


def calc_qty_from_risk(equity_quote: float, entry: float, stop: float) -> float:
    """Calculate position quantity from fixed risk.

    Designed for MEXC spot scalping (quote is usually USDT).

    Notes
    -----
    - Risk budget = equity_quote * RISK_PER_TRADE
    - Per-unit loss is based on (entry - stop) but:
        * Enforces MIN_STOP_BPS as a floor
        * Adds conservative buffer for fees and slippage
    """

    try:
        eq = float(equity_quote)
        entry_f = float(entry)
        stop_f = float(stop)
    except Exception:
        return 0.0

    if not math.isfinite(eq) or not math.isfinite(entry_f) or not math.isfinite(stop_f):
        return 0.0
    if eq <= 0 or entry_f <= 0 or stop_f <= 0:
        return 0.0

    risk_pct = float(getattr(C, "RISK_PER_TRADE", 0.01))
    risk_budget = eq * risk_pct
    if risk_budget <= 0:
        return 0.0

    # Conservative buffer: expected one-way fee + one-way slippage
    fee_rate = float(getattr(C, "PAPER_FEE_RATE", 0.0))
    slip_bps = float(getattr(C, "SLIPPAGE_BPS", 0.0))
    cost_frac = max(0.0, fee_rate) + max(0.0, _bps_to_frac(slip_bps))

    effective_entry = entry_f * (1.0 + cost_frac)
    effective_stop = stop_f * (1.0 - cost_frac)

    per_unit_loss = effective_entry - effective_stop

    # Floor on stop distance (too-tight SL makes qty explode)
    min_stop_bps = float(getattr(C, "MIN_STOP_BPS", 3.0))
    min_loss = entry_f * _bps_to_frac(max(0.0, min_stop_bps))
    if per_unit_loss < min_loss:
        per_unit_loss = min_loss

    if per_unit_loss <= 0 or not math.isfinite(per_unit_loss):
        return 0.0

    qty = risk_budget / per_unit_loss
    if qty <= 0 or not math.isfinite(qty):
        return 0.0

    max_notional = float(getattr(C, "MAX_POSITION_NOTIONAL", 0.0))
    if max_notional > 0:
        qty_cap = max_notional / entry_f
        qty = min(qty, qty_cap)


    return float(qty)
