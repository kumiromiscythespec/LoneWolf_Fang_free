# BUILD_ID: 2026-03-29_free_port_standard_gui_nonlive_improvements_v1
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path


BUILD_ID = "2026-03-29_free_port_standard_gui_nonlive_improvements_v1"
BITBANK_BASE_URL = "https://public.bitbank.cc"
GMO_KLINES_URL = "https://api.coin.z.com/public/v1/klines"
_DEFAULT_TIMEOUT_SEC = 30
_DEFAULT_SLEEP_SEC = 0.25
_DEFAULT_MAX_RETRIES = 5
_INTERVAL_MAP = {
    "5m": ("5min", 300_000, "5m"),
    "5min": ("5min", 300_000, "5m"),
    "1h": ("1hour", 3_600_000, "1h"),
    "1hour": ("1hour", 3_600_000, "1h"),
}


def _parse_provider(raw: str) -> str:
    provider = str(raw or "").strip().upper() or "BITBANK"
    if provider == "AUTO":
        return "BITBANK"
    if provider not in ("BITBANK", "GMO"):
        raise ValueError(f"unsupported provider: {raw}")
    return provider


def _parse_pair(raw: str) -> tuple[str, str, str]:
    pair = str(raw or "").strip().upper()
    if pair != "BTC/JPY":
        raise ValueError(f"unsupported pair: {raw}")
    return ("BTC/JPY", "btc_jpy", "BTC")


def _normalize_interval(raw: str) -> tuple[str, int, str]:
    hit = _INTERVAL_MAP.get(str(raw or "").strip().lower())
    if hit is None:
        raise ValueError(f"unsupported interval: {raw}")
    api_interval, tf_ms, file_tf = hit
    return (str(api_interval), int(tf_ms), str(file_tf))


def _parse_date(raw: str) -> date:
    text = str(raw or "").strip()
    if not text:
        raise ValueError("date is required")
    return date.fromisoformat(text)


def _date_start_ms(day: date) -> int:
    return int(datetime.combine(day, dt_time.min, tzinfo=timezone.utc).timestamp() * 1000.0)


def _range_ms(since_date: date, until_date: date) -> tuple[int, int]:
    since_ms = _date_start_ms(since_date)
    until_ms_exclusive = _date_start_ms(until_date + timedelta(days=1))
    return (int(since_ms), int(until_ms_exclusive))


def _daterange(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur
        cur = cur + timedelta(days=1)


def _request_json(
    url: str,
    *,
    params: dict[str, str] | None,
    timeout_sec: int,
    max_retries: int,
    sleep_sec: float,
) -> dict:
    query = dict(params or {})
    request_url = url
    if query:
        request_url = f"{url}?{urllib.parse.urlencode(query)}"
    req = urllib.request.Request(request_url, headers={"User-Agent": "Mozilla/5.0"})
    last_exc: Exception | None = None
    for attempt in range(max(0, int(max_retries)) + 1):
        try:
            with urllib.request.urlopen(req, timeout=max(1, int(timeout_sec))) as resp:
                payload = json.load(resp)
            return dict(payload or {}) if isinstance(payload, dict) else {}
        except urllib.error.HTTPError as exc:
            if int(exc.code) == 404:
                return {"status": 404, "success": 0, "data": []}
            last_exc = exc
            if int(exc.code) not in (429, 500, 502, 503, 504) or attempt >= int(max_retries):
                break
        except Exception as exc:
            last_exc = exc
            if attempt >= int(max_retries):
                break
        time.sleep(max(0.2, float(sleep_sec)) * (2 ** attempt))
    raise RuntimeError(f"http request failed url={request_url} error={last_exc}")


def _merge_rows(rows: list[list[float]]) -> list[list[float]]:
    by_ts: dict[int, list[float]] = {}
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) < 6:
            continue
        try:
            ts = int(float(row[0]))
            opn = float(row[1])
            high = float(row[2])
            low = float(row[3])
            close = float(row[4])
            volume = float(row[5])
        except Exception:
            continue
        by_ts[int(ts)] = [int(ts), float(opn), float(high), float(low), float(close), float(volume)]
    return [by_ts[ts] for ts in sorted(by_ts.keys())]


def _fill_missing_rows(
    rows: list[list[float]],
    *,
    tf_ms: int,
    since_ms: int,
    until_ms_exclusive: int,
) -> tuple[list[list[float]], int]:
    merged = _merge_rows(rows)
    if not merged:
        return ([], 0)
    by_ts: dict[int, list[float]] = {}
    carry_close: float | None = None
    first_in_range_close: float | None = None
    for row in merged:
        ts = int(row[0])
        if int(ts) < int(since_ms):
            carry_close = float(row[4])
            continue
        if int(ts) >= int(until_ms_exclusive):
            break
        by_ts[int(ts)] = row
        if first_in_range_close is None:
            first_in_range_close = float(row[4])
    if first_in_range_close is None:
        return ([], 0)
    prev_close = carry_close if carry_close is not None else first_in_range_close
    out: list[list[float]] = []
    for ts in range(int(since_ms), int(until_ms_exclusive), int(tf_ms)):
        row = by_ts.get(int(ts))
        if row is not None:
            prev_close = float(row[4])
            out.append(row)
            continue
        fill = float(prev_close)
        out.append([int(ts), fill, fill, fill, fill, 0.0])
    return (out, len(by_ts))


def _finalize_rows(
    rows: list[list[float]],
    *,
    tf_ms: int,
    since_ms: int,
    until_ms_exclusive: int,
) -> tuple[list[list[float]], int]:
    filtered = [
        row for row in _merge_rows(rows)
        if int(row[0]) < int(until_ms_exclusive)
    ]
    return _fill_missing_rows(
        filtered,
        tf_ms=int(tf_ms),
        since_ms=int(since_ms),
        until_ms_exclusive=int(until_ms_exclusive),
    )


def _format_success_summary(
    *,
    provider: str,
    symbol: str,
    interval: str,
    tf: str,
    rows: list[list[float]],
    raw_in_range_rows: int,
    out_path: str,
) -> str:
    final_rows = len(rows)
    ts_first = int(rows[0][0]) if rows else 0
    ts_last = int(rows[-1][0]) if rows else 0
    filled = max(0, int(final_rows) - int(raw_in_range_rows))
    resolved_out = os.path.abspath(str(out_path))
    return (
        f"[JPY_DATASET] provider={provider} symbol={symbol} interval={interval} tf={tf} "
        f"rows={final_rows} ts_first={ts_first} ts_last={ts_last} filled={filled} out={resolved_out}"
    )


def _parse_bitbank_rows(blob: dict) -> list[list[float]]:
    if int(blob.get("success") or 0) != 1:
        return []
    data = blob.get("data") or {}
    candles = data.get("candlestick") or []
    if not candles:
        return []
    ohlcv = candles[0].get("ohlcv") or []
    out: list[list[float]] = []
    for row in ohlcv:
        if not isinstance(row, (list, tuple)) or len(row) < 6:
            continue
        try:
            opn = float(row[0])
            high = float(row[1])
            low = float(row[2])
            close = float(row[3])
            volume = float(row[4])
            ts_ms = int(row[5])
        except Exception:
            continue
        out.append([int(ts_ms), float(opn), float(high), float(low), float(close), float(volume)])
    return _merge_rows(out)


def _parse_gmo_rows(blob: dict) -> list[list[float]]:
    raw_rows = blob.get("data") or []
    out: list[list[float]] = []
    for row in raw_rows:
        try:
            ts_ms = int(row.get("openTime"))
            opn = float(row.get("open"))
            high = float(row.get("high"))
            low = float(row.get("low"))
            close = float(row.get("close"))
            volume = float(row.get("volume"))
        except Exception:
            continue
        out.append([int(ts_ms), float(opn), float(high), float(low), float(close), float(volume)])
    return _merge_rows(out)


def _download_bitbank_rows(
    *,
    pair: str,
    interval: str,
    since_date: date,
    until_date: date,
    timeout_sec: int,
    max_retries: int,
    sleep_sec: float,
) -> tuple[list[list[float]], int]:
    pair_norm, pair_api, _gmo_symbol = _parse_pair(pair)
    candle_type, tf_ms, _file_tf = _normalize_interval(interval)
    since_ms, until_ms_exclusive = _range_ms(since_date, until_date)
    today_utc = datetime.now(timezone.utc).date()
    rows: list[list[float]] = []
    for day in _daterange(since_date - timedelta(days=1), until_date + timedelta(days=1)):
        if day > today_utc:
            continue
        payload = _request_json(
            f"{BITBANK_BASE_URL}/{pair_api}/candlestick/{candle_type}/{day:%Y%m%d}",
            params=None,
            timeout_sec=int(timeout_sec),
            max_retries=int(max_retries),
            sleep_sec=float(sleep_sec),
        )
        if int(payload.get("success") or 0) != 1 and int(payload.get("status") or 0) == 404:
            continue
        rows.extend(_parse_bitbank_rows(payload))
        time.sleep(max(0.0, float(sleep_sec)))
    finalized, raw_in_range_rows = _finalize_rows(
        rows,
        tf_ms=int(tf_ms),
        since_ms=int(since_ms),
        until_ms_exclusive=int(until_ms_exclusive),
    )
    if not finalized:
        raise RuntimeError(
            f"no_rows provider=BITBANK symbol={pair_norm} interval={interval} "
            f"since={since_date.isoformat()} until={until_date.isoformat()}"
        )
    return (finalized, int(raw_in_range_rows))


def _download_gmo_rows(
    *,
    pair: str,
    interval: str,
    since_date: date,
    until_date: date,
    timeout_sec: int,
    max_retries: int,
    sleep_sec: float,
) -> tuple[list[list[float]], int]:
    pair_norm, _bitbank_pair, gmo_symbol = _parse_pair(pair)
    api_interval, tf_ms, _file_tf = _normalize_interval(interval)
    since_ms, until_ms_exclusive = _range_ms(since_date, until_date)
    rows: list[list[float]] = []
    for day in _daterange(since_date - timedelta(days=1), until_date + timedelta(days=1)):
        payload = _request_json(
            GMO_KLINES_URL,
            params={"symbol": gmo_symbol, "interval": api_interval, "date": day.strftime("%Y%m%d")},
            timeout_sec=int(timeout_sec),
            max_retries=int(max_retries),
            sleep_sec=float(sleep_sec),
        )
        status = str(payload.get("status", "0"))
        if status == "404":
            continue
        if status not in ("0", "OK", "ok", ""):
            raise RuntimeError(
                f"gmo status error symbol={pair_norm} interval={api_interval} day={day.isoformat()} "
                f"payload={json.dumps(payload, ensure_ascii=False)}"
            )
        rows.extend(_parse_gmo_rows(payload))
        time.sleep(max(0.0, float(sleep_sec)))
    finalized, raw_in_range_rows = _finalize_rows(
        rows,
        tf_ms=int(tf_ms),
        since_ms=int(since_ms),
        until_ms_exclusive=int(until_ms_exclusive),
    )
    if not finalized:
        raise RuntimeError(
            f"no_rows provider=GMO symbol={pair_norm} interval={interval} "
            f"since={since_date.isoformat()} until={until_date.isoformat()}"
        )
    return (finalized, int(raw_in_range_rows))


def _write_rows_atomic(path: str, rows: list[list[float]]) -> None:
    out_path = Path(str(path or "")).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = Path(str(out_path) + ".tmp")
    try:
        with open(tmp_path, "w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            for row in _merge_rows(rows):
                writer.writerow(row)
        os.replace(tmp_path, out_path)
    finally:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass


def run_download(
    *,
    provider: str,
    symbol: str,
    interval: str,
    since: str,
    until: str,
    out_path: str,
    timeout_sec: int = _DEFAULT_TIMEOUT_SEC,
    sleep_sec: float = _DEFAULT_SLEEP_SEC,
    max_retries: int = _DEFAULT_MAX_RETRIES,
) -> int:
    provider_name = _parse_provider(provider)
    symbol_name, _pair_api, _gmo_symbol = _parse_pair(symbol)
    interval_name, _tf_ms, file_tf = _normalize_interval(interval)
    since_date = _parse_date(since)
    until_date = _parse_date(until)
    if until_date < since_date:
        raise ValueError(f"until must be >= since: since={since} until={until}")
    if provider_name == "BITBANK":
        rows, raw_in_range_rows = _download_bitbank_rows(
            pair=str(symbol_name),
            interval=str(interval_name),
            since_date=since_date,
            until_date=until_date,
            timeout_sec=int(timeout_sec),
            max_retries=int(max_retries),
            sleep_sec=float(sleep_sec),
        )
    elif provider_name == "GMO":
        rows, raw_in_range_rows = _download_gmo_rows(
            pair=str(symbol_name),
            interval=str(interval_name),
            since_date=since_date,
            until_date=until_date,
            timeout_sec=int(timeout_sec),
            max_retries=int(max_retries),
            sleep_sec=float(sleep_sec),
        )
    else:
        raise ValueError(f"unsupported provider: {provider}")
    _write_rows_atomic(str(out_path), rows)
    print(
        _format_success_summary(
            provider=provider_name,
            symbol=symbol_name,
            interval=interval_name,
            tf=file_tf,
            rows=rows,
            raw_in_range_rows=int(raw_in_range_rows),
            out_path=str(out_path),
        ),
        flush=True,
    )
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Download JPY monthly dataset rows for free pipeline.")
    ap.add_argument("--provider", type=str, default="BITBANK")
    ap.add_argument("--pair", type=str, required=True)
    ap.add_argument("--interval", type=str, required=True)
    ap.add_argument("--since", type=str, required=True)
    ap.add_argument("--until", type=str, required=True)
    ap.add_argument("--out", type=str, required=True)
    ap.add_argument("--timeout", type=int, default=_DEFAULT_TIMEOUT_SEC)
    ap.add_argument("--sleep", type=float, default=_DEFAULT_SLEEP_SEC)
    ap.add_argument("--retries", type=int, default=_DEFAULT_MAX_RETRIES)
    args = ap.parse_args()
    try:
        return int(
            run_download(
                provider=str(args.provider),
                symbol=str(args.pair),
                interval=str(args.interval),
                since=str(args.since),
                until=str(args.until),
                out_path=str(args.out),
                timeout_sec=int(args.timeout),
                sleep_sec=float(args.sleep),
                max_retries=int(args.retries),
            )
        )
    except Exception as e:
        print(
            f"[JPY_DATASET][ERROR] provider={str(args.provider).upper()} symbol={args.pair} "
            f"interval={args.interval} since={args.since} until={args.until} reason={e}",
            file=sys.stderr,
            flush=True,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
