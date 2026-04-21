# BUILD_ID: 2026-04-21_free_adx_impl_version_v1_contract_test
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from indicators import ADX_IMPL_VERSION, adx


PERIOD = 14


def _sample_ohlc(length: int = 80) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    x = np.arange(length, dtype=float)
    close = 100.0 + np.cumsum(
        np.sin(x / 3.0) * 1.7
        + np.cos(x / 5.0) * 0.9
        + np.where((x % 9) < 4, 0.6, -0.4)
    )
    high = close + 1.2 + (np.sin(x / 4.0) ** 2) * 0.7
    low = close - 1.1 - (np.cos(x / 6.0) ** 2) * 0.6
    return high, low, close


def _adx_components(
    high: np.ndarray,
    low: np.ndarray,
    close: np.ndarray,
    period: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    prev_close = np.roll(close, 1)
    prev_close[0] = np.nan
    tr = np.maximum(high - low, np.maximum(np.abs(high - prev_close), np.abs(low - prev_close)))

    up_move = high - np.roll(high, 1)
    down_move = np.roll(low, 1) - low
    up_move[0] = np.nan
    down_move[0] = np.nan

    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    tr_s = np.full(close.size, np.nan, dtype=float)
    p_dm_s = np.full(close.size, np.nan, dtype=float)
    m_dm_s = np.full(close.size, np.nan, dtype=float)
    tr_s[period] = np.nansum(tr[1 : period + 1])
    p_dm_s[period] = np.nansum(plus_dm[1 : period + 1])
    m_dm_s[period] = np.nansum(minus_dm[1 : period + 1])

    for i in range(period + 1, close.size):
        tr_s[i] = tr_s[i - 1] - (tr_s[i - 1] / period) + tr[i]
        p_dm_s[i] = p_dm_s[i - 1] - (p_dm_s[i - 1] / period) + plus_dm[i]
        m_dm_s[i] = m_dm_s[i - 1] - (m_dm_s[i - 1] / period) + minus_dm[i]

    plus_di = np.full(close.size, np.nan, dtype=float)
    minus_di = np.full(close.size, np.nan, dtype=float)
    valid_tr = np.isfinite(tr_s) & (tr_s != 0.0)
    np.divide(p_dm_s, tr_s, out=plus_di, where=valid_tr)
    np.divide(m_dm_s, tr_s, out=minus_di, where=valid_tr)
    plus_di *= 100.0
    minus_di *= 100.0

    di_sum = plus_di + minus_di
    dx = np.full(close.size, np.nan, dtype=float)
    valid_di = np.isfinite(di_sum) & (di_sum != 0.0)
    np.divide(np.abs(plus_di - minus_di), di_sum, out=dx, where=valid_di)
    dx *= 100.0
    return plus_di, minus_di, dx


def test_adx_impl_version_is_historical_v1() -> None:
    assert ADX_IMPL_VERSION == 1


def test_adx_v1_seed_contract_first_finite_index_and_window() -> None:
    high, low, close = _sample_ohlc()
    plus_di, minus_di, dx = _adx_components(high, low, close, PERIOD)
    adx_values = adx(high, low, close, PERIOD)

    assert int(np.flatnonzero(np.isfinite(plus_di))[0]) == PERIOD
    assert int(np.flatnonzero(np.isfinite(minus_di))[0]) == PERIOD
    assert int(np.flatnonzero(np.isfinite(dx))[0]) == PERIOD
    assert int(np.flatnonzero(np.isfinite(adx_values))[0]) == 2 * PERIOD

    assert np.isnan(adx_values[(2 * PERIOD) - 1])
    assert np.isclose(
        adx_values[2 * PERIOD],
        np.mean(dx[PERIOD + 1 : 2 * PERIOD + 1]),
        rtol=0.0,
        atol=1e-12,
    )


def test_adx_v1_seed_is_not_the_canonical_2p_minus_1_variant() -> None:
    high, low, close = _sample_ohlc()
    _, _, dx = _adx_components(high, low, close, PERIOD)
    adx_values = adx(high, low, close, PERIOD)

    canonical_seed = float(np.mean(dx[PERIOD : 2 * PERIOD]))
    current_seed = float(adx_values[2 * PERIOD])

    assert abs(current_seed - canonical_seed) > 1e-6


if __name__ == "__main__":
    test_adx_impl_version_is_historical_v1()
    test_adx_v1_seed_contract_first_finite_index_and_window()
    test_adx_v1_seed_is_not_the_canonical_2p_minus_1_variant()
    print("ADX v1 contract checks passed.")
