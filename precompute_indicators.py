# BUILD_ID: 2026-04-21_free_indicator_audit_rsi_filter_precompute_v1
# BUILD_ID: 2026-04-18_free_precompute_shared_root_v1
# BUILD_ID: 2026-03-14_precompute_multiyear_boundary_safe_v1
# BUILD_ID=2026-02-26_precompute_support_ETHUSDC_dir_v1

from __future__ import annotations

import argparse
import csv
import glob
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np
from app.core.paths import get_paths
from app.core.source_provenance import (
    is_legacy_source,
    iter_source_root_candidates,
    record_path_source_event_once,
)

# Project indicators (same as backtest.py uses)
try:
    import config as C  # type: ignore
except Exception:
    C = None

from indicators import ema, rsi, atr, adx  # type: ignore

BUILD_ID="2026-04-21_free_indicator_audit_rsi_filter_precompute_v1"

# ======================================================================================
# Logging
# ======================================================================================
log = logging.getLogger("precompute_indicators")
_SOURCE_DIAG_ONCE: set[str] = set()


def _app_repo_root() -> str:
    try:
        return os.path.abspath(str(get_paths().repo_root or os.getcwd()))
    except Exception:
        return os.path.abspath(os.getcwd())


def _canonical_market_data_dir() -> str:
    try:
        return os.path.abspath(str(get_paths().market_data_dir or os.path.join(_app_repo_root(), "market_data")))
    except Exception:
        return os.path.abspath(os.path.join(_app_repo_root(), "market_data"))


def _canonical_precomputed_out_root() -> str:
    try:
        return os.path.abspath(
            str(
                get_paths().precomputed_indicators_dir
                or os.path.join(_canonical_market_data_dir(), "precomputed_indicators")
            )
        )
    except Exception:
        return os.path.abspath(os.path.join(_canonical_market_data_dir(), "precomputed_indicators"))


def _runtime_filter_ema_spans() -> List[int]:
    spans: list[int] = []
    for attr, fallback in (("EMA_FAST", 20), ("EMA_SLOW", 50)):
        try:
            value = int(getattr(C, attr, fallback) if C is not None else fallback)
        except Exception:
            value = int(fallback)
        if value > 0:
            spans.append(int(value))
    return spans


def _resolve_ema_spans(requested_spans: Optional[List[int]]) -> List[int]:
    spans = {int(x) for x in list(requested_spans or []) if int(x) > 0}
    spans.update(int(x) for x in _runtime_filter_ema_spans() if int(x) > 0)
    return sorted(spans)


def _source_diag_once(level: str, key: str, message: str) -> None:
    once_key = str(key or "").strip()
    if not once_key or once_key in _SOURCE_DIAG_ONCE:
        return
    _SOURCE_DIAG_ONCE.add(once_key)
    getattr(log, str(level).lower(), log.info)(message)


def _record_precompute_source_event(
    *,
    event: str,
    source: str,
    path: str,
    symbol: str,
    tf: str,
    file_glob: str = "",
) -> None:
    extra = {
        "build_id": BUILD_ID,
        "symbol": str(symbol or "").strip(),
        "tf": str(tf or "").strip(),
    }
    if file_glob:
        extra["file_glob"] = str(file_glob)
    record_path_source_event_once(
        component="precompute_indicators",
        event=event,
        source=source,
        path=path,
        extra=extra,
    )


def _candidate_layout_search_roots_with_sources(src_dir: str, *, include_parent: bool) -> List[Tuple[str, str]]:
    return [
        (root_path, source)
        for source, root_path in iter_source_root_candidates(
            str(src_dir),
            _canonical_market_data_dir(),
            _app_repo_root(),
            include_explicit_parent=include_parent,
            include_cwd=True,
        )
    ]


def _iter_source_files_with_provenance(
    *,
    src_dir: str,
    file_glob: str,
    symbol: str,
    tf: str,
    since_ms: Optional[int],
    until_ms: Optional[int],
) -> Tuple[List[str], str, str]:
    explicit_root = os.path.abspath(str(src_dir or "").strip())
    direct_matches = _iter_source_files(explicit_root, file_glob)
    if direct_matches:
        _record_precompute_source_event(
            event="precompute_source_resolved",
            source="explicit_src_dir",
            path=explicit_root,
            symbol=symbol,
            tf=tf,
            file_glob=file_glob,
        )
        return (direct_matches, explicit_root, "explicit_src_dir")

    if since_ms is None or until_ms is None:
        return ([], explicit_root, "explicit_src_dir")
    try:
        y0 = int(datetime.fromtimestamp(int(since_ms) / 1000.0, tz=timezone.utc).year)
        y1 = int(datetime.fromtimestamp(int(until_ms) / 1000.0, tz=timezone.utc).year)
    except Exception:
        return ([], explicit_root, "explicit_src_dir")
    if y0 > y1:
        y0, y1 = y1, y0

    symbol_dir_prefix = str(symbol or "").replace("/", "").replace(":", "").replace("-", "").strip().upper()
    tf_s = str(tf or "").strip()
    if not symbol_dir_prefix or not tf_s:
        return ([], explicit_root, "explicit_src_dir")

    for root, source in _candidate_layout_search_roots_with_sources(src_dir, include_parent=False):
        matches: List[str] = []
        for year in range(int(y0), int(y1) + 1):
            year_dir = os.path.join(root, f"{symbol_dir_prefix}_{tf_s}_{int(year)}")
            if not os.path.isdir(year_dir):
                continue
            matches.extend(sorted([p for p in glob.glob(os.path.join(year_dir, "*.csv")) if os.path.isfile(p)]))
        if matches:
            _record_precompute_source_event(
                event="precompute_source_resolved",
                source=source,
                path=root,
                symbol=symbol_dir_prefix,
                tf=tf_s,
                file_glob=file_glob,
            )
            if is_legacy_source(source):
                _record_precompute_source_event(
                    event="precompute_source_legacy_used",
                    source=source,
                    path=root,
                    symbol=symbol_dir_prefix,
                    tf=tf_s,
                    file_glob=file_glob,
                )
                _source_diag_once(
                    "warning",
                    f"layout_source:{source}:{root}:{symbol_dir_prefix}:{tf_s}",
                    f"SOURCE PROVENANCE: source={source} root={root} symbol={symbol_dir_prefix} tf={tf_s}",
                )
            return (sorted(matches), root, source)
    return ([], explicit_root, "explicit_src_dir")

def _setup_logging(level: str) -> None:
    lvl = getattr(logging, str(level).upper(), logging.INFO)
    logging.basicConfig(
        level=lvl,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    log.info("[BUILD] precompute_indicators.py loaded: file=%s BUILD_ID=%s", __file__, BUILD_ID)

# ======================================================================================
# Time helpers
# ======================================================================================
def _parse_yyyy_mm_dd_utc_to_ms_start(s: str) -> int:
    """
    Parse YYYY-MM-DD as UTC 00:00:00 -> ms.
    """
    dt = datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

def _parse_yyyy_mm_dd_utc_to_ms_end(s: str) -> int:
    """
    Parse YYYY-MM-DD as UTC 23:59:59.999 -> ms (inclusive end-of-day).
    """
    dt = datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = dt.replace(hour=23, minute=59, second=59, microsecond=999_000)
    return int(end.timestamp() * 1000)

def _parse_ts_maybe_ms(x: str) -> Optional[int]:
    """
    Accept seconds/ms/us-ish timestamps as in backtest.py CSV reader.
    Returns ms int or None if invalid.
    """
    try:
        ts = int(float(str(x).strip()))
    except Exception:
        return None
    # seconds -> ms
    if 0 < ts < 100_000_000_000:
        ts *= 1000
    # microseconds -> ms
    if ts > 10_000_000_000_000:
        ts //= 1000
    return int(ts)


def _iso_utc(ms: int) -> str:
    try:
        ms = int(ms)
        if ms < 0:
            return ""
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).isoformat()
    except Exception:
        return ""


def _parse_since_until(value: str, *, is_until: bool) -> int:
    """
    Accept:
      - integer-like (ms)
      - YYYY-MM-DD (UTC)
    """
    v = str(value).strip()
    if not v:
        raise ValueError("empty date")
    if re.fullmatch(r"-?\d+", v):
        return int(v)
    # YYYY-MM-DD
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", v):
        return _parse_yyyy_mm_dd_utc_to_ms_end(v) if is_until else _parse_yyyy_mm_dd_utc_to_ms_start(v)
    raise ValueError(f"Unsupported date format: {value!r} (use ms or YYYY-MM-DD)")


# ======================================================================================
# CSV reader (matches backtest behavior closely, but standalone)
# ======================================================================================
@dataclass
class Ohlcv:
    ts: List[int]
    open: List[float]
    high: List[float]
    low: List[float]
    close: List[float]
    vol: List[float]


def _read_binance_csv_dir(csv_dir: str, file_glob: str, since_ms: Optional[int], until_ms: Optional[int]) -> List[List[float]]:
    """
    Read OHLCV rows from many CSV files:
      row = [ts, open, high, low, close, vol]

    - Detect delimiter
    - Skip header if first column isn't numeric
    - Normalize ts to ms
    - Apply since/until slicing (inclusive bounds)
    - Dedupe by ts (keep last)
    """
    csv_dir_abs = os.path.abspath(csv_dir)
    pattern = os.path.join(csv_dir_abs, file_glob)
    paths = sorted(glob.glob(pattern))
    paths = [p for p in paths if os.path.isfile(p)]

    log.info("CSV SEARCH: dir=%s glob=%s matches=%d", csv_dir_abs, file_glob, len(paths))
    for p in paths[:10]:
        log.info("CSV FILE: %s", p)

    if not paths:
        cand = sorted(glob.glob(os.path.join(csv_dir_abs, "*.csv")))
        log.error("CSV NOT FOUND: pattern=%s", pattern)
        log.error("CSV DIR LIST (*.csv)=%d", len(cand))
        for p in cand[:20]:
            log.error("CSV CAND: %s", p)
        raise SystemExit("CSV files not found. Check --src-dir/--glob.")

    out: List[List[float]] = []

    for p in paths:
        with open(p, "r", encoding="utf-8-sig", newline="") as f:
            sample = f.read(4096)
            if not sample.strip():
                continue
            f.seek(0)

            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|"])
            except Exception:
                dialect = csv.excel

            r = csv.reader(f, dialect)
            first_row = next(r, None)
            if first_row is None:
                continue

            def _accept_row(row: List[str]) -> None:
                if not row or len(row) < 6:
                    return
                ts = _parse_ts_maybe_ms(row[0])
                if ts is None:
                    return
                if since_ms is not None and ts < int(since_ms):
                    return
                if until_ms is not None and ts > int(until_ms):
                    return
                try:
                    o = float(row[1]); h = float(row[2]); l = float(row[3]); c = float(row[4]); v = float(row[5])
                except Exception:
                    return
                out.append([int(ts), float(o), float(h), float(l), float(c), float(v)])

            # header auto-skip
            ts0 = _parse_ts_maybe_ms(first_row[0]) if len(first_row) >= 1 else None
            if ts0 is None:
                # treat as header; continue
                pass
            else:
                _accept_row(first_row)

            for row in r:
                _accept_row(row)

    if not out:
        raise SystemExit(f"CSV parsed 0 rows. Check slicing/format: dir={csv_dir_abs} glob={file_glob}")

    out.sort(key=lambda x: int(x[0]))

    # dedupe by ts (keep last)
    dedup: List[List[float]] = []
    last_ts = None
    for rr in out:
        t = int(rr[0])
        if last_ts is None or t != last_ts:
            dedup.append(rr)
            last_ts = t
        else:
            dedup[-1] = rr

    return dedup


def _rows_to_ohlcv(rows: List[List[float]]) -> Ohlcv:
    ts: List[int] = []
    o: List[float] = []
    h: List[float] = []
    l: List[float] = []
    c: List[float] = []
    v: List[float] = []
    for r in rows:
        ts.append(int(r[0]))
        o.append(float(r[1]))
        h.append(float(r[2]))
        l.append(float(r[3]))
        c.append(float(r[4]))
        v.append(float(r[5]))
    return Ohlcv(ts=ts, open=o, high=h, low=l, close=c, vol=v)


# ======================================================================================
# Output naming / directory convention
# ======================================================================================
def _ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def _base_stem_from_path(path: str) -> str:
    """
    ETHUSDT-5m-2025-01.csv -> ETHUSDT-5m-2025-01
    """
    b = os.path.basename(path)
    if b.lower().endswith(".csv"):
        b = b[:-4]
    return b


def _iter_source_files(src_dir: str, file_glob: str) -> List[str]:
    src_dir_abs = os.path.abspath(src_dir)
    pattern = os.path.join(src_dir_abs, file_glob)
    paths = sorted(glob.glob(pattern))
    paths = [p for p in paths if os.path.isfile(p)]
    return paths


def _iter_source_files_layout_fallback(
    src_dir: str,
    symbol: str,
    tf: str,
    since_ms: Optional[int],
    until_ms: Optional[int],
) -> List[str]:
    # Fallback is enabled only when year range is explicitly given.
    if since_ms is None or until_ms is None:
        return []
    try:
        y0 = int(datetime.fromtimestamp(int(since_ms) / 1000.0, tz=timezone.utc).year)
        y1 = int(datetime.fromtimestamp(int(until_ms) / 1000.0, tz=timezone.utc).year)
    except Exception:
        return []
    if y0 > y1:
        y0, y1 = y1, y0

    symbol_dir_prefix = str(symbol or "").replace("/", "").replace(":", "").replace("-", "").strip().upper()
    tf_s = str(tf or "").strip()
    if not symbol_dir_prefix or not tf_s:
        return []

    roots: List[str] = []
    src_root = os.path.abspath(str(src_dir or "").strip())
    if os.path.isdir(src_root):
        roots.append(src_root)
    cwd_root = os.path.abspath(".")
    if cwd_root not in roots:
        roots.append(cwd_root)

    out: List[str] = []
    for root in roots:
        for year in range(int(y0), int(y1) + 1):
            d = os.path.join(root, f"{symbol_dir_prefix}_{tf_s}_{int(year)}")
            if not os.path.isdir(d):
                continue
            out.extend(sorted([p for p in glob.glob(os.path.join(d, "*.csv")) if os.path.isfile(p)]))
    return sorted(out)


def _iter_history_source_files(
    *,
    src_dir: str,
    symbol: str,
    tf: str,
    before_year: int,
) -> List[str]:
    symbol_dir_prefix = str(symbol or "").replace("/", "").replace(":", "").replace("-", "").strip().upper()
    tf_s = str(tf or "").strip().lower()
    if (not symbol_dir_prefix) or (not tf_s):
        return []
    roots: List[str] = []
    src_root = os.path.abspath(str(src_dir or "").strip())
    if os.path.isdir(src_root):
        roots.append(src_root)
        parent_root = os.path.abspath(os.path.join(src_root, os.pardir))
        if parent_root not in roots:
            roots.append(parent_root)
    cwd_root = os.path.abspath(".")
    if cwd_root not in roots:
        roots.append(cwd_root)

    seen: set[str] = set()
    out: List[str] = []
    dir_rx = re.compile(rf"^{re.escape(symbol_dir_prefix)}_{re.escape(tf_s)}_(\d{{4}})$", re.IGNORECASE)
    for root in roots:
        if not os.path.isdir(root):
            continue
        for cand in sorted(glob.glob(os.path.join(root, f"{symbol_dir_prefix}_{tf_s}_*"))):
            if not os.path.isdir(cand):
                continue
            m = dir_rx.match(os.path.basename(cand))
            if not m:
                continue
            year = int(m.group(1))
            if int(year) >= int(before_year):
                continue
            for path in sorted(glob.glob(os.path.join(cand, "*.csv"))):
                if os.path.isfile(path) and path not in seen:
                    seen.add(path)
                    out.append(path)
    return out


def _augment_source_files_with_history(
    *,
    src_files: List[str],
    src_dir: str,
    symbol: str,
    tf: str,
    since_ms: Optional[int],
) -> List[str]:
    if (not src_files) or (since_ms is None):
        return list(src_files or [])
    start_year = int(datetime.fromtimestamp(int(since_ms) / 1000.0, tz=timezone.utc).year)
    history_files = _iter_history_source_files(
        src_dir=str(src_dir),
        symbol=str(symbol),
        tf=str(tf),
        before_year=int(start_year),
    )
    if not history_files:
        return list(src_files)
    merged = sorted({os.path.abspath(p) for p in list(history_files) + list(src_files or [])})
    log.info(
        "SOURCE HISTORY: added=%d start_year=%d symbol=%s tf=%s",
        len([p for p in merged if p not in set(os.path.abspath(x) for x in (src_files or []))]),
        int(start_year),
        str(symbol),
        str(tf),
    )
    return merged


def _write_indicator_csv(path: str, ts: List[int], values: List[float], strict: bool, allow_nan_prefix_bars: int) -> None:
    _ensure_dir(os.path.dirname(path))
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["ts", "ts_iso", "value"])
        for i, (t, v) in enumerate(zip(ts, values)):
            if strict:
                # strict: allow NaN only in the warmup prefix
                if v != v:  # NaN
                    if int(i) >= int(allow_nan_prefix_bars):
                        raise RuntimeError(
                            f"NaN encountered while writing strict output: "
                            f"{os.path.basename(path)} idx={i} ts={t} (allowed NaN prefix bars={allow_nan_prefix_bars})"
                        )
            w.writerow([int(t), _iso_utc(int(t)), "" if (v != v) else f"{float(v):.12f}"])


def _write_indicator_csv_atomic(path: str, ts: List[int], values: List[float], strict: bool, allow_nan_prefix_bars: int) -> None:
    _ensure_dir(os.path.dirname(path))
    tmp_path = f"{path}.tmp-{os.getpid()}-{int(datetime.now(timezone.utc).timestamp() * 1000)}"
    try:
        _write_indicator_csv(
            tmp_path,
            ts,
            values,
            strict=bool(strict),
            allow_nan_prefix_bars=int(allow_nan_prefix_bars),
        )
        os.replace(tmp_path, path)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


def _validate_boundary_series(
    *,
    indicator_name: str,
    out_name: str,
    file_ts_out: List[int],
    series: List[float],
    merged_first_ts: int,
) -> None:
    if (not file_ts_out) or (not series):
        return
    leading_nan_rows = 0
    for v in series:
        if v == v:
            break
        leading_nan_rows += 1
    if leading_nan_rows <= 0:
        return
    if int(file_ts_out[0]) == int(merged_first_ts):
        log.warning(
            "BOUNDARY CHECK: earliest_output indicator=%s file=%s leading_nan_rows=%d first_ts=%s",
            str(indicator_name),
            str(out_name),
            int(leading_nan_rows),
            _iso_utc(int(file_ts_out[0])),
        )
        return
    raise RuntimeError(
        f"boundary leading NaN detected: indicator={indicator_name} file={out_name} "
        f"leading_nan_rows={leading_nan_rows} first_ts={file_ts_out[0]}({_iso_utc(int(file_ts_out[0]))})"
    )


def _slice_warmup(values: List[float], warmup_bars: int) -> List[float]:
    """
    For indicator series, keep full-length (align by index) but allow warmup-only NaNs.
    We do not drop rows because backtest aligns by ts index; dropping breaks alignment.
    """
    # No-op: keep alignment.
    _ = warmup_bars
    return values


# ======================================================================================
# Main
# ======================================================================================
def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Precompute indicators from OHLCV CSVs.")
    p.add_argument("--src-dir", type=str, default="binance_ethusdt_5m_all", help="Source directory containing OHLCV CSV files.")
    p.add_argument("--glob", type=str, default="ETHUSDT-5m-20*.csv", help="Glob pattern of OHLCV CSV files.")
    p.add_argument("--out-root", type=str, default=_canonical_precomputed_out_root(), help="Root output directory.")
    p.add_argument("--tf", type=str, required=True, help="Timeframe label used in filenames (e.g., 5m).")
    p.add_argument("--symbol", type=str, required=True, help="Symbol label used in filenames (e.g., ETHUSDT).")

    p.add_argument("--rsi-period", type=int, default=14)
    p.add_argument("--atr-period", type=int, default=14)
    p.add_argument("--adx-period", type=int, default=14)
    p.add_argument("--ema-spans", type=int, nargs="+", default=[9, 21], help="EMA spans to compute (e.g., 9 21).")

    # since/until: accept ms OR YYYY-MM-DD
    # since/until (preferred): YYYY-MM-DD or ms
    p.add_argument("--since", type=str, default="", help="Start bound (inclusive). Accept ms or YYYY-MM-DD (UTC).")
    p.add_argument("--until", type=str, default="", help="End bound (inclusive). Accept ms or YYYY-MM-DD (UTC; date means end-of-day).")
    # backward compatibility
    p.add_argument("--since-ms", type=str, default="", help="[DEPRECATED] use --since. Accept ms or YYYY-MM-DD (UTC).")
    p.add_argument("--until-ms", type=str, default="", help="[DEPRECATED] use --until. Accept ms or YYYY-MM-DD (UTC; date means end-of-day).")

    p.add_argument("--warmup-bars", type=int, default=500, help="Warmup bars hint (kept for compatibility; alignment preserved).")
    p.add_argument("--force", action="store_true", help="Overwrite existing output files.")
    p.add_argument("--strict", action="store_true", help="Fail if NaN appears in outputs.")
    p.add_argument("--log-level", type=str, default="INFO", help="Logging level (INFO/DEBUG/...).")
    return p


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()
    _setup_logging(args.log_level)

    # Parse since/until (optional)
    since_ms: Optional[int] = None
    until_ms: Optional[int] = None
    since_raw = str(args.since).strip() or str(args.since_ms).strip()
    until_raw = str(args.until).strip() or str(args.until_ms).strip()
    if str(args.since_ms).strip() and not str(args.since).strip():
        log.warning("DEPRECATED: --since-ms is deprecated. Use --since instead.")
    if str(args.until_ms).strip() and not str(args.until).strip():
        log.warning("DEPRECATED: --until-ms is deprecated. Use --until instead.")

    if since_raw:
        since_ms = _parse_since_until(since_raw, is_until=False)
    if until_raw:
        until_ms = _parse_since_until(until_raw, is_until=True)
    if since_ms is not None and until_ms is not None and int(since_ms) > int(until_ms):
        raise SystemExit("--since must be <= --until")

    # Determine file list first (we keep per-file outputs, matching your example naming)
    src_files, selected_src_root, selected_source = _iter_source_files_with_provenance(
        src_dir=str(args.src_dir),
        file_glob=str(args.glob),
        symbol=str(args.symbol),
        tf=str(args.tf),
        since_ms=since_ms,
        until_ms=until_ms,
    )
    src_files = _augment_source_files_with_history(
        src_files=src_files,
        src_dir=str(selected_src_root or args.src_dir),
        symbol=str(args.symbol),
        tf=str(args.tf),
        since_ms=since_ms,
    )
    if not src_files:
        raise SystemExit(f"No source files matched: dir={args.src_dir} glob={args.glob}")

    out_root = os.path.abspath(str(args.out_root or _canonical_precomputed_out_root()))
    tf = str(args.tf).strip()
    sym = str(args.symbol).strip()

    log.info(
        "RUN: src_dir=%s glob=%s out_root=%s tf=%s symbol=%s source=%s selected_src_root=%s",
        args.src_dir,
        args.glob,
        out_root,
        tf,
        sym,
        selected_source,
        selected_src_root,
    )
    log.info("RUN: since_ms=%s (%s) until_ms=%s (%s)",
             str(since_ms) if since_ms is not None else "",
             _iso_utc(int(since_ms)) if since_ms is not None else "",
             str(until_ms) if until_ms is not None else "",
             _iso_utc(int(until_ms)) if until_ms is not None else "")
    log.info("RUN: src_files=%d first=%s last=%s", len(src_files), os.path.basename(src_files[0]), os.path.basename(src_files[-1]))

    ema_spans = _resolve_ema_spans(getattr(args, "ema_spans", None))
    if not ema_spans:
        raise SystemExit("--ema-spans must include >=1 positive integer")

    rsi_period = int(args.rsi_period)
    atr_period = int(args.atr_period)
    adx_period = int(getattr(args, "adx_period", 14))

    written = 0
    skipped = 0

    # Output dir convention (repo-aligned): exports/precomputed_indicators/<symbol>/<tf>/
    out_dir = os.path.join(out_root, sym, tf)
    _ensure_dir(out_dir)
    wrote_any = False

    # ----------------------------------------------------------------------------------
    # Concat-then-slice design:
    #  1) Read all matched month files in full (no since/until slicing for computation).
    #  2) Merge by ts into one chronological stream (duplicate ts -> last file wins).
    #  3) Compute indicators once on the merged stream.
    #  4) For each source file, slice indicator values by its ts list and then apply
    #     since/until for output rows only.
    #
    # This prevents "NaN at start of every month" artifacts (ADX, etc.).
    # ----------------------------------------------------------------------------------
    per_file_rows: Dict[str, List[List[float]]] = {}
    merged_by_ts: Dict[int, List[float]] = {}
    total_overwrites = 0

    for src_path in src_files:
        rows_full = _read_binance_csv_dir(os.path.dirname(src_path), os.path.basename(src_path), None, None)
        if not rows_full:
            log.warning("SKIP file=%s reason=0 rows (read_full)", os.path.basename(src_path))
            continue

        overwrites = 0
        for rr in rows_full:
            t = int(rr[0])
            if t in merged_by_ts:
                overwrites += 1
            merged_by_ts[t] = rr  # last-write-wins

        total_overwrites += overwrites
        per_file_rows[src_path] = rows_full
        if overwrites > 0:
            log.info("MERGE overlap: file=%s overwrites=%d (last-write-wins)", os.path.basename(src_path), overwrites)

    if not merged_by_ts:
        raise SystemExit("No OHLCV rows were loaded. Check --src-dir/--glob.")

    merged_rows = [merged_by_ts[t] for t in sorted(merged_by_ts.keys())]
    merged_ohlcv = _rows_to_ohlcv(merged_rows)

    log.info(
        "MERGED: files=%d rows=%d ts_first=%s ts_last=%s overwrites_total=%d",
        len(per_file_rows),
        len(merged_ohlcv.ts),
        _iso_utc(merged_ohlcv.ts[0]),
        _iso_utc(merged_ohlcv.ts[-1]),
        total_overwrites,
    )

    # index for fast slicing (ts -> idx in merged arrays)
    ts_to_idx: Dict[int, int] = {int(t): int(i) for i, t in enumerate(merged_ohlcv.ts)}

    close_all = np.asarray(merged_ohlcv.close, dtype=float)
    high_all = np.asarray(merged_ohlcv.high, dtype=float)
    low_all = np.asarray(merged_ohlcv.low, dtype=float)

    # Compute once on merged stream
    ema_all: Dict[int, np.ndarray] = {}
    for span in ema_spans:
        ema_all[int(span)] = ema(close_all, int(span))

    rsi_all = rsi(close_all, int(rsi_period))
    atr_all = atr(high_all, low_all, close_all, int(atr_period))
    adx_all = adx(high_all, low_all, close_all, int(adx_period))

    # Slice back into per-file outputs (apply since/until only for writing)
    for src_path in src_files:
        rows_full = per_file_rows.get(src_path, [])
        if not rows_full:
            continue

        base = _base_stem_from_path(src_path)
        file_ts_full = [int(r[0]) for r in rows_full]

        file_ts_out: List[int] = []
        file_idx_out: List[int] = []
        for t in file_ts_full:
            if since_ms is not None and int(t) < int(since_ms):
                continue
            if until_ms is not None and int(t) > int(until_ms):
                continue
            idx = ts_to_idx.get(int(t))
            if idx is None:
                continue
            file_ts_out.append(int(t))
            file_idx_out.append(int(idx))

        if not file_ts_out:
            log.warning(
                "SKIP file=%s reason=0 rows after output slicing (since=%s until=%s)",
                os.path.basename(src_path),
                str(since_ms) if since_ms is not None else "",
                str(until_ms) if until_ms is not None else "",
            )
            continue

        tasks_out: List[Tuple[str, List[float]]] = []
        for span in sorted(ema_all.keys()):
            arr = ema_all[int(span)]
            series = [float(arr[i]) for i in file_idx_out]
            tasks_out.append((f"EMA{span}", _slice_warmup(series, int(args.warmup_bars))))

        rsi_series = [float(rsi_all[i]) for i in file_idx_out]
        atr_series = [float(atr_all[i]) for i in file_idx_out]
        adx_series = [float(adx_all[i]) for i in file_idx_out]

        tasks_out.append((f"RSI{rsi_period}", _slice_warmup(rsi_series, int(args.warmup_bars))))
        tasks_out.append((f"ATR{atr_period}", _slice_warmup(atr_series, int(args.warmup_bars))))
        tasks_out.append((f"ADX{adx_period}", _slice_warmup(adx_series, int(args.warmup_bars))))

        for ind_name, series in tasks_out:
            out_name = f"{ind_name}-{base}.csv"
            out_path = os.path.join(out_dir, out_name)

            if (not bool(args.force)) and os.path.exists(out_path):
                skipped += 1
                continue

            _validate_boundary_series(
                indicator_name=str(ind_name),
                out_name=str(out_name),
                file_ts_out=file_ts_out,
                series=series,
                merged_first_ts=int(merged_ohlcv.ts[0]),
            )
            _write_indicator_csv_atomic(
                out_path,
                file_ts_out,
                series,
                strict=bool(args.strict),
                allow_nan_prefix_bars=int(args.warmup_bars),
            )
            written += 1
            wrote_any = True

        log.info("DONE file=%s rows_out=%d out_dir=%s", os.path.basename(src_path), len(file_ts_out), out_dir)
    if not wrote_any:
        raise SystemExit("No outputs written (all files empty after slicing or all skipped). Check --since/--until and --glob.")

    log.info("SUMMARY: written=%d skipped=%d out_dir=%s", written, skipped, out_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
