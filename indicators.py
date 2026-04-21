# BUILD_ID: 2026-04-21_free_indicator_audit_rsi_filter_precompute_v1
# BUILD_ID: 2026-03-07_adx_warning_guard_v1
# indicators.py
from __future__ import annotations

import numpy as np


def ema(close: np.ndarray, period: int) -> np.ndarray:
    close = np.asarray(close, dtype=float)
    n = close.size
    out = np.full(n, np.nan, dtype=float)
    if period <= 0 or n == 0:
        return out
    if n < period:
        return out

    alpha = 2.0 / (period + 1.0)

    # seed: SMA
    seed = np.nanmean(close[:period])
    out[period - 1] = seed
    prev = seed

    for i in range(period, n):
        prev = alpha * close[i] + (1.0 - alpha) * prev
        out[i] = prev

    return out


def rsi(close: np.ndarray, period: int) -> np.ndarray:
    close = np.asarray(close, dtype=float)
    n = close.size
    out = np.full(n, np.nan, dtype=float)
    if period <= 0 or n < period + 1:
        return out

    diff = np.diff(close)
    gains = np.where(diff > 0, diff, 0.0)
    losses = np.where(diff < 0, -diff, 0.0)

    avg_gain = np.full(n, np.nan, dtype=float)
    avg_loss = np.full(n, np.nan, dtype=float)

    avg_gain[period] = np.mean(gains[:period])
    avg_loss[period] = np.mean(losses[:period])

    for i in range(period + 1, n):
        avg_gain[i] = (avg_gain[i - 1] * (period - 1) + gains[i - 1]) / period
        avg_loss[i] = (avg_loss[i - 1] * (period - 1) + losses[i - 1]) / period

    valid = np.isfinite(avg_gain) & np.isfinite(avg_loss)
    normal = valid & (avg_gain > 0.0) & (avg_loss > 0.0)

    out[valid & (avg_gain > 0.0) & (avg_loss == 0.0)] = 100.0
    out[valid & (avg_gain == 0.0) & (avg_loss > 0.0)] = 0.0
    out[valid & (avg_gain == 0.0) & (avg_loss == 0.0)] = 50.0
    out[normal] = 100.0 - (100.0 / (1.0 + (avg_gain[normal] / avg_loss[normal])))
    return out


def bollinger(close: np.ndarray, period: int, std_mult: float):
    close = np.asarray(close, dtype=float)
    n = close.size
    mid = np.full(n, np.nan, dtype=float)
    up = np.full(n, np.nan, dtype=float)
    dn = np.full(n, np.nan, dtype=float)
    if period <= 0 or n < period:
        return mid, up, dn

    for i in range(period - 1, n):
        w = close[i - period + 1 : i + 1]
        m = np.nanmean(w)
        s = np.nanstd(w, ddof=0)
        mid[i] = m
        up[i] = m + std_mult * s
        dn[i] = m - std_mult * s

    return mid, up, dn


def atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int) -> np.ndarray:
    high = np.asarray(high, dtype=float)
    low = np.asarray(low, dtype=float)
    close = np.asarray(close, dtype=float)
    m = int(min(high.size, low.size, close.size))
    if m <= 0:
        return np.full(close.size, np.nan, dtype=float)
    if (high.size != m) or (low.size != m) or (close.size != m):
        high = high[:m]
        low = low[:m]
        close = close[:m]

    n = close.size
    out = np.full(n, np.nan, dtype=float)
    if period <= 0 or n < period + 1:
        return out

    prev_close = np.roll(close, 1)
    prev_close[0] = np.nan

    tr = np.maximum(high - low, np.maximum(np.abs(high - prev_close), np.abs(low - prev_close)))

    # Wilder smoothing
    out[period] = np.nanmean(tr[1 : period + 1])  # first ATR uses period TRs (excluding tr[0])
    for i in range(period + 1, n):
        out[i] = (out[i - 1] * (period - 1) + tr[i]) / period

    return out


def adx(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int) -> np.ndarray:
    """
    ADX (Wilder). Returns np.ndarray same length as input close.
    If input lengths differ, aligns to the shortest length for calculation and pads with NaN.
    """
    high = np.asarray(high, dtype=float)
    low = np.asarray(low, dtype=float)
    close = np.asarray(close, dtype=float)

    orig_n = close.size
    m = int(min(high.size, low.size, close.size))
    if m <= 0:
        return np.full(orig_n, np.nan, dtype=float)

    # align inputs to the shortest length
    if high.size != m:
        high = high[:m]
    if low.size != m:
        low = low[:m]
    if close.size != m:
        close = close[:m]

    n = m
    adx_out = np.full(n, np.nan, dtype=float)

    if period <= 0:
        # pad to original close length
        if orig_n != n:
            padded = np.full(orig_n, np.nan, dtype=float)
            padded[:n] = adx_out
            return padded
        return adx_out

    # needs at least (2*period + 1)
    if n < (2 * period + 1):
        if orig_n != n:
            padded = np.full(orig_n, np.nan, dtype=float)
            padded[:n] = adx_out
            return padded
        return adx_out

    # True Range
    prev_close = np.roll(close, 1)
    prev_close[0] = np.nan
    tr = np.maximum(high - low, np.maximum(np.abs(high - prev_close), np.abs(low - prev_close)))

    # Directional Movement
    up_move = high - np.roll(high, 1)
    down_move = np.roll(low, 1) - low
    up_move[0] = np.nan
    down_move[0] = np.nan

    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    # Wilder smoothing for TR, +DM, -DM
    tr_s = np.full(n, np.nan, dtype=float)
    p_dm_s = np.full(n, np.nan, dtype=float)
    m_dm_s = np.full(n, np.nan, dtype=float)

    tr_s[period] = np.nansum(tr[1: period + 1])
    p_dm_s[period] = np.nansum(plus_dm[1: period + 1])
    m_dm_s[period] = np.nansum(minus_dm[1: period + 1])

    for i in range(period + 1, n):
        tr_s[i] = tr_s[i - 1] - (tr_s[i - 1] / period) + tr[i]
        p_dm_s[i] = p_dm_s[i - 1] - (p_dm_s[i - 1] / period) + plus_dm[i]
        m_dm_s[i] = m_dm_s[i - 1] - (m_dm_s[i - 1] / period) + minus_dm[i]

    # DI and DX
    plus_di = np.full(n, np.nan, dtype=float)
    minus_di = np.full(n, np.nan, dtype=float)
    valid_tr = np.isfinite(tr_s) & (tr_s != 0.0)
    np.divide(p_dm_s, tr_s, out=plus_di, where=valid_tr)
    np.divide(m_dm_s, tr_s, out=minus_di, where=valid_tr)
    plus_di *= 100.0
    minus_di *= 100.0

    di_sum = plus_di + minus_di
    dx = np.full(n, np.nan, dtype=float)
    valid_di = np.isfinite(di_sum) & (di_sum != 0.0)
    np.divide(np.abs(plus_di - minus_di), di_sum, out=dx, where=valid_di)
    dx *= 100.0

    # First ADX = average of DX over next period
    first_adx_window = dx[period + 1: 2 * period + 1]
    if np.isfinite(first_adx_window).any():
        adx_out[2 * period] = np.nanmean(first_adx_window)

    # Wilder smoothing for ADX
    for i in range(2 * period + 1, n):
        adx_out[i] = ((adx_out[i - 1] * (period - 1)) + dx[i]) / period

    # pad to original close length if close was longer
    if orig_n != n:
        padded = np.full(orig_n, np.nan, dtype=float)
        padded[:n] = adx_out
        return padded

    return adx_out

