# BUILD_ID: 2026-03-14_backtest_trace_snapshot_cache_v1
from __future__ import annotations

from bisect import bisect_left
from typing import Any

_SNAPSHOT_META_CACHE: dict[int, tuple[int, int, int, int, bool, int]] = {}


def _normalize_ts_to_ms(value: Any) -> tuple[int | None, str | None]:
    try:
        ts = int(value)
    except Exception:
        return None, "invalid"
    if ts <= 0:
        return None, "non_positive"
    if ts < 1_000_000_000_000:
        return ts * 1000, "sec_to_ms"
    if ts > 10_000_000_000_000_000:
        return ts // 1_000_000, "ns_to_ms"
    if ts > 10_000_000_000_000:
        return ts // 1_000, "us_to_ms"
    return ts, None


def normalize_ref_ts_ms(event_dict: dict) -> int:
    if not isinstance(event_dict, dict):
        return 0
    for k in ("exec_ts_ms", "exec_ts", "event_ts_ms", "event_ts", "ts_ms", "ts"):
        if k not in event_dict:
            continue
        ts_ms, _note = _normalize_ts_to_ms(event_dict.get(k))
        if ts_ms is not None:
            return int(ts_ms)
    return 0


def expected_bar_ts_ms(ref_ts_ms: int, tf_ms: int) -> int:
    tf = max(1, int(tf_ms))
    ref = int(ref_ts_ms)
    return (ref // tf) * tf - tf


def _snapshot_meta(ohlcv_dict: dict, n: int) -> tuple[list[Any], bool, int] | None:
    ts = ohlcv_dict.get("timestamp") or []
    if n <= 0:
        return None
    try:
        first_ts = int(ts[0])
        last_ts = int(ts[n - 1])
    except Exception:
        return None
    cache_key = id(ohlcv_dict)
    cache_sig = (id(ts), n, first_ts, last_ts)
    cached = _SNAPSHOT_META_CACHE.get(cache_key)
    if cached and cached[:4] == cache_sig:
        return ts, bool(cached[4]), int(cached[5])
    last_ts_ms, _ = _normalize_ts_to_ms(last_ts)
    try:
        prev = first_ts
        for i in range(1, n):
            cur = int(ts[i])
            if cur < prev:
                _SNAPSHOT_META_CACHE[cache_key] = (
                    cache_sig[0],
                    cache_sig[1],
                    cache_sig[2],
                    cache_sig[3],
                    False,
                    int(last_ts_ms or 0),
                )
                return ts, False, int(last_ts_ms or 0)
            prev = cur
    except Exception:
        _SNAPSHOT_META_CACHE[cache_key] = (
            cache_sig[0],
            cache_sig[1],
            cache_sig[2],
            cache_sig[3],
            False,
            int(last_ts_ms or 0),
        )
        return None
    _SNAPSHOT_META_CACHE[cache_key] = (
        cache_sig[0],
        cache_sig[1],
        cache_sig[2],
        cache_sig[3],
        True,
        int(last_ts_ms or 0),
    )
    return ts, True, int(last_ts_ms or 0)


def snapshot_ohlc_at(ohlcv_dict: dict, bar_ts_ms: int) -> dict | None:
    if not isinstance(ohlcv_dict, dict):
        return None
    op = ohlcv_dict.get("open") or []
    hi = ohlcv_dict.get("high") or []
    lo = ohlcv_dict.get("low") or []
    cl = ohlcv_dict.get("close") or []
    ts = ohlcv_dict.get("timestamp") or []
    n = min(len(ts), len(op), len(hi), len(lo), len(cl))
    if n <= 0:
        return None
    meta = _snapshot_meta(ohlcv_dict, n)
    if meta is None:
        return None
    ts, is_sorted, _last_ts_ms = meta
    if not is_sorted:
        return None
    target = int(bar_ts_ms)
    idx = bisect_left(ts, target, 0, n)
    if idx < 0 or idx >= n:
        return None
    if int(ts[idx]) != target:
        return None
    return {
        "bar_ts_ms": int(ts[idx]),
        "bar_open": float(op[idx]),
        "bar_high": float(hi[idx]),
        "bar_low": float(lo[idx]),
        "bar_close": float(cl[idx]),
    }


def attach_bar_snapshot(event_dict: dict, ohlcv_dict: dict, tf_ms: int) -> dict:
    ev = dict(event_dict) if isinstance(event_dict, dict) else {}
    ref_ts = normalize_ref_ts_ms(ev)
    exp_ts = expected_bar_ts_ms(ref_ts, int(tf_ms)) if ref_ts > 0 else 0
    snap = snapshot_ohlc_at(ohlcv_dict, exp_ts) if exp_ts > 0 else None
    last_ts = 0
    try:
        ts = (ohlcv_dict or {}).get("timestamp") or []
        n = len(ts)
        if n:
            meta = _snapshot_meta(ohlcv_dict, n)
            if meta is not None:
                _ts, _is_sorted, last_ts = meta
            else:
                last_ts, _ = _normalize_ts_to_ms(ts[-1])
                last_ts = int(last_ts or 0)
    except Exception:
        last_ts = 0
    ev["ref_ts_ms"] = int(ref_ts)
    ev["expected_bar_ts_ms"] = int(exp_ts)
    ev["tf_ms"] = int(max(1, int(tf_ms)))
    ev["last_ohlcv_ts_ms"] = int(last_ts)
    ev["bar_ts_ms"] = int(snap["bar_ts_ms"]) if isinstance(snap, dict) else None
    for k in ("bar_open", "bar_high", "bar_low", "bar_close"):
        ev[k] = (float(snap[k]) if isinstance(snap, dict) and (k in snap) else None)
    return ev
