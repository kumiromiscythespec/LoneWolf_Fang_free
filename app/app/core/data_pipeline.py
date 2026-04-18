# BUILD_ID: 2026-04-18_free_shared_market_data_fallback_v1
# BUILD_ID: 2026-03-29_free_port_standard_gui_nonlive_improvements_v1
# BUILD_ID: 2026-03-20_pipeline_precompute_lookback_diag_v1
# BUILD_ID: 2026-03-20_pipeline_precompute_until_bound_v1
# BUILD_ID: 2026-03-20_market_data_stage2_chart_path_cleanup_v1
# BUILD_ID: 2026-03-14_pipeline_multiyear_precompute_v1
# BUILD_ID: 2026-03-07_coinbase_exchange_fallback_v1
from __future__ import annotations

import argparse
import calendar
import csv
import glob
import io
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request
import urllib.error
import urllib.parse
import zipfile
import json
from datetime import datetime, timezone
from typing import Any

from app.core.dataset import symbol_to_prefix
from app.core.paths import get_paths
from app.core.source_provenance import iter_source_root_candidates, record_path_source_event_once


BUILD_ID = "2026-04-18_free_shared_market_data_fallback_v1"

_PIPELINE_DIAG_ONCE: set[str] = set()


def _parse_yyyy_mm(raw: str) -> tuple[int, int]:
    s = str(raw or "").strip()
    if not re.fullmatch(r"\d{4}-\d{2}", s):
        raise ValueError(f"invalid YYYY-MM: {raw}")
    y = int(s[:4])
    m = int(s[5:7])
    if y < 2000 or y > 2100 or m < 1 or m > 12:
        raise ValueError(f"invalid YYYY-MM: {raw}")
    return y, m


def _iter_months(start: tuple[int, int], end: tuple[int, int]) -> list[tuple[int, int]]:
    sy, sm = start
    ey, em = end
    out: list[tuple[int, int]] = []
    y = int(sy)
    m = int(sm)
    while (y < ey) or (y == ey and m <= em):
        out.append((int(y), int(m)))
        m += 1
        if m > 12:
            y += 1
            m = 1
    return out


def _month_ordinal(year: int, month: int) -> int:
    return int(year) * 12 + (int(month) - 1)


def _shift_month(year: int, month: int, delta_months: int) -> tuple[int, int]:
    ordinal = _month_ordinal(int(year), int(month)) + int(delta_months)
    return (int(ordinal // 12), int(ordinal % 12) + 1)


def _tf_minutes(tf: str) -> int:
    s = str(tf or "").strip().lower()
    m = re.fullmatch(r"(\d+)([mhd])", s)
    if not m:
        return 0
    value = int(m.group(1))
    unit = str(m.group(2))
    if unit == "m":
        return int(value)
    if unit == "h":
        return int(value) * 60
    if unit == "d":
        return int(value) * 1440
    return 0


def _precompute_history_lookback_months(tf: str) -> int:
    tf_minutes = _tf_minutes(tf)
    if tf_minutes <= 0:
        return 1
    warmup_bars = 500
    bars_per_day = max(1, 1440 // int(tf_minutes))
    warmup_days = max(1, (int(warmup_bars) + int(bars_per_day) - 1) // int(bars_per_day))
    return max(1, (int(warmup_days) + 27) // 28)


def _is_jpy_symbol(symbol: str) -> bool:
    return str(symbol or "").strip().upper().endswith("/JPY")


def _month_to_ymd(year: int, month: int) -> tuple[str, str]:
    dom = int(calendar.monthrange(int(year), int(month))[1])
    return (f"{int(year):04d}-{int(month):02d}-01", f"{int(year):04d}-{int(month):02d}-{int(dom):02d}")


def _count_csv_rows(path: str) -> int:
    if (not path) or (not os.path.exists(path)):
        return 0
    cnt = 0
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        for row in csv.reader(fh):
            if row and len(row) >= 6:
                cnt += 1
    return int(cnt)


def _safe_remove(path: str) -> None:
    try:
        if path and os.path.exists(path):
            os.remove(path)
    except Exception:
        pass


def _pipeline_diag_once(key: str, message: str) -> None:
    once_key = str(key or "").strip()
    if not once_key or once_key in _PIPELINE_DIAG_ONCE:
        return
    _PIPELINE_DIAG_ONCE.add(once_key)
    print(message, flush=True)


def _record_pipeline_path_event(
    *,
    event: str,
    source: str,
    path: str,
    extra: dict | None = None,
) -> None:
    try:
        payload = dict(extra or {})
        payload.setdefault("build_id", BUILD_ID)
        record_path_source_event_once(
            component="data_pipeline",
            event=str(event or "").strip(),
            source=str(source or "").strip(),
            path=os.path.abspath(str(path or "").strip() or os.getcwd()),
            extra=payload,
        )
    except Exception:
        return


def _resolve_pipeline_paths(repo_root: str) -> tuple[str, str, str]:
    repo_raw = str(repo_root or "").strip()
    cwd_abs = os.path.abspath(os.getcwd())
    try:
        p = get_paths()
        market_root = str(getattr(p, "market_data_dir", "") or "").strip()
        if market_root:
            market_abs = os.path.abspath(market_root)
            chart_cache_root = str(getattr(p, "chart_cache_dir", "") or "").strip()
            precomputed_root = str(getattr(p, "precomputed_indicators_dir", "") or "").strip()
            chart_cache_abs = (
                os.path.abspath(chart_cache_root)
                if chart_cache_root
                else os.path.abspath(os.path.join(market_abs, "chart_cache"))
            )
            precomputed_abs = (
                os.path.abspath(precomputed_root)
                if precomputed_root
                else os.path.abspath(os.path.join(market_abs, "precomputed_indicators"))
            )
            _pipeline_diag_once(
                "pipeline_paths:canonical_market_data",
                f"[PIPELINE][DIAG] source=canonical_market_data market_data_root={market_abs} "
                f"chart_cache_root={chart_cache_abs} precomputed_root={precomputed_abs}",
            )
            _record_pipeline_path_event(
                event="pipeline_root_resolved",
                source="canonical_market_data",
                path=market_abs,
                extra={
                    "chart_cache_root": chart_cache_abs,
                    "precomputed_root": precomputed_abs,
                },
            )
            return (market_abs, chart_cache_abs, precomputed_abs)
    except Exception:
        pass

    if repo_raw:
        repo_abs = os.path.abspath(repo_raw)
        market_abs = os.path.abspath(os.path.join(repo_abs, "market_data"))
        chart_cache_abs = os.path.abspath(os.path.join(market_abs, "chart_cache"))
        precomputed_abs = os.path.abspath(os.path.join(market_abs, "precomputed_indicators"))
        _pipeline_diag_once(
            "pipeline_paths:legacy_repo_root",
            f"[PIPELINE][DIAG] source=legacy_repo_root repo_root={repo_abs} market_data_root={market_abs} "
            f"chart_cache_root={chart_cache_abs} precomputed_root={precomputed_abs}",
        )
        _record_pipeline_path_event(
            event="pipeline_root_resolved",
            source="legacy_repo_root",
            path=market_abs,
            extra={
                "chart_cache_root": chart_cache_abs,
                "precomputed_root": precomputed_abs,
                "repo_root": repo_abs,
            },
        )
        return (market_abs, chart_cache_abs, precomputed_abs)

    market_abs = os.path.abspath(os.path.join(cwd_abs, "market_data"))
    chart_cache_abs = os.path.abspath(os.path.join(market_abs, "chart_cache"))
    precomputed_abs = os.path.abspath(os.path.join(market_abs, "precomputed_indicators"))
    _pipeline_diag_once(
        "pipeline_paths:cwd_fallback",
        f"[PIPELINE][DIAG] source=cwd_fallback cwd={cwd_abs} market_data_root={market_abs} "
        f"chart_cache_root={chart_cache_abs} precomputed_root={precomputed_abs}",
    )
    _record_pipeline_path_event(
        event="pipeline_root_resolved",
        source="cwd_fallback",
        path=market_abs,
        extra={
            "chart_cache_root": chart_cache_abs,
            "precomputed_root": precomputed_abs,
            "cwd": cwd_abs,
        },
    )
    return (market_abs, chart_cache_abs, precomputed_abs)


def _resolve_pipeline_roots(repo_root: str) -> tuple[str, str]:
    market_abs, _chart_cache_abs, precomputed_abs = _resolve_pipeline_paths(repo_root)
    return (market_abs, precomputed_abs)


def _market_data_root(repo_root: str) -> str:
    return _resolve_pipeline_paths(repo_root)[0]


def _chart_cache_root(repo_root: str) -> str:
    return _resolve_pipeline_paths(repo_root)[1]


def _precomputed_out_root(repo_root: str) -> str:
    return _resolve_pipeline_paths(repo_root)[2]


def _source_root_has_month_dirs(root: str, prefix: str, tf: str, years: list[int]) -> bool:
    root_abs = os.path.abspath(str(root or "").strip() or os.getcwd())
    return any(os.path.isdir(os.path.join(root_abs, f"{prefix}_{tf}_{int(year)}")) for year in (years or []))


def _resolve_precompute_source_root(
    *,
    repo_root: str,
    prefix: str,
    tf: str,
    years_in_range: list[int],
) -> tuple[str, str]:
    market_root = _market_data_root(repo_root)
    repo_raw = str(repo_root or "").strip()
    repo_abs = os.path.abspath(repo_raw or os.getcwd())
    repo_candidate = repo_abs if repo_raw else ""
    try:
        candidates = iter_source_root_candidates(
            "",
            market_root,
            repo_candidate,
            include_explicit_parent=False,
            include_cwd=True,
        )
    except Exception:
        candidates = [("canonical_market_data", market_root)]
        if repo_candidate:
            candidates.append(("legacy_repo_root", repo_abs))
        candidates.append(("cwd_fallback", os.path.abspath(os.getcwd())))

    for source, root in candidates:
        if not _source_root_has_month_dirs(root, prefix, tf, years_in_range):
            continue
        if source in ("legacy_repo_root", "cwd_fallback", "legacy_env_root", "legacy_product_root"):
            _pipeline_diag_once(
                f"precompute_source:{source}:{prefix}:{tf}",
                f"[PIPELINE][DIAG] precompute source={source} tf={tf} prefix={prefix} "
                f"src_dir={root} preferred_src_dir={market_root}",
            )
        _record_pipeline_path_event(
            event="precompute_source_resolved",
            source=source,
            path=root,
            extra={
                "prefix": prefix,
                "tf": tf,
                "preferred_src_dir": market_root,
            },
        )
        return (root, source)

    _record_pipeline_path_event(
        event="precompute_source_resolved",
        source="canonical_market_data",
        path=market_root,
        extra={
            "prefix": prefix,
            "tf": tf,
            "preferred_src_dir": market_root,
        },
    )
    return (market_root, "canonical_market_data")


def _legacy_month_dirs(repo_root: str, prefix: str, tf: str, years: list[int]) -> list[str]:
    repo_abs = os.path.abspath(str(repo_root or "").strip() or os.getcwd())
    out: list[str] = []
    seen: set[str] = set()
    for year in years:
        cand = os.path.abspath(os.path.join(repo_abs, f"{prefix}_{tf}_{int(year)}"))
        if os.path.isdir(cand) and cand not in seen:
            seen.add(cand)
            out.append(cand)
    return out


def _warn_legacy_chart_paths(
    *,
    repo_root: str,
    prefix: str,
    tf: str,
    years_in_range: list[int],
    using_src_dir: str,
    using_out_root: str,
) -> None:
    legacy_month_dirs = _legacy_month_dirs(repo_root, prefix, tf, years_in_range)
    market_root = _market_data_root(repo_root)
    if legacy_month_dirs:
        src_abs = os.path.abspath(str(using_src_dir or "").strip() or market_root)
        level = "WARN" if src_abs == os.path.abspath(market_root) else "DIAG"
        print(
            f"[PIPELINE][{level}] legacy_month_root_detected tf={tf} prefix={prefix} "
            f"legacy_count={len(legacy_month_dirs)} using_src_dir={src_abs} preferred_src_dir={market_root}",
            flush=True,
        )
        for legacy_dir in legacy_month_dirs[:4]:
            print(f"[PIPELINE][{level}] legacy_month_dir={legacy_dir}", flush=True)
    legacy_pre_root = os.path.abspath(os.path.join(str(repo_root or "").strip() or os.getcwd(), "exports", "precomputed_indicators"))
    using_out_abs = os.path.abspath(str(using_out_root or "").strip() or _precomputed_out_root(repo_root))
    if os.path.isdir(legacy_pre_root) and legacy_pre_root != using_out_abs:
        print(
            f"[PIPELINE][WARN] legacy_precomputed_root_detected path={legacy_pre_root} "
            f"preferred_out_root={using_out_abs}",
            flush=True,
        )


def _parse_precompute_input_month(path: str, *, prefix: str, tf: str) -> tuple[int, int] | None:
    name = os.path.basename(str(path or "").strip())
    m = re.fullmatch(rf"{re.escape(prefix)}-{re.escape(tf)}-(\d{{4}})-(\d{{2}})\.csv", name, re.IGNORECASE)
    if not m:
        return None
    year = int(m.group(1))
    month = int(m.group(2))
    if year < 2000 or year > 2100 or month < 1 or month > 12:
        return None
    return (int(year), int(month))


def _iter_precompute_input_files(src_dir: str, prefix: str, tf: str) -> list[str]:
    src_abs = os.path.abspath(str(src_dir or "").strip() or os.getcwd())
    direct_matches: list[str] = []
    for path in sorted(glob.glob(os.path.join(src_abs, f"{prefix}-{tf}-20*.csv"))):
        path_abs = os.path.abspath(path)
        if os.path.isfile(path_abs):
            direct_matches.append(path_abs)
    if direct_matches:
        return direct_matches

    seen: set[str] = set()
    out: list[str] = []
    year_dir_rx = re.compile(rf"^{re.escape(prefix)}_{re.escape(tf)}_(\d{{4}})$", re.IGNORECASE)
    for year_dir in sorted(glob.glob(os.path.join(src_abs, f"{prefix}_{tf}_*"))):
        if (not os.path.isdir(year_dir)) or (not year_dir_rx.match(os.path.basename(year_dir))):
            continue
        for path in sorted(glob.glob(os.path.join(year_dir, "*.csv"))):
            path_abs = os.path.abspath(path)
            if os.path.isfile(path_abs) and path_abs not in seen:
                seen.add(path_abs)
                out.append(path_abs)
    return out


def _link_or_copy_precompute_input(src_path: str, dst_path: str) -> None:
    _safe_remove(dst_path)
    try:
        os.link(src_path, dst_path)
        return
    except Exception:
        pass
    shutil.copy2(src_path, dst_path)


def _stage_precompute_input_dir(
    *,
    src_dir: str,
    prefix: str,
    tf: str,
    start: tuple[int, int],
    end: tuple[int, int],
    historical_lookback_months: int,
) -> tuple[str, int, int, int]:
    candidates = _iter_precompute_input_files(src_dir=src_dir, prefix=prefix, tf=tf)
    if not candidates:
        return ("", 0, 0, 0)

    selected: list[str] = []
    selected_history_months: set[tuple[int, int]] = set()
    skipped_future = 0
    skipped_history = 0
    start_ordinal = _month_ordinal(int(start[0]), int(start[1]))
    end_ordinal = _month_ordinal(int(end[0]), int(end[1]))
    history_start = _shift_month(int(start[0]), int(start[1]), -max(0, int(historical_lookback_months)))
    history_floor_ordinal = _month_ordinal(int(history_start[0]), int(history_start[1]))
    for path in candidates:
        ym = _parse_precompute_input_month(path, prefix=prefix, tf=tf)
        if ym is not None:
            ym_ordinal = _month_ordinal(int(ym[0]), int(ym[1]))
            if int(ym_ordinal) > int(end_ordinal):
                skipped_future += 1
                continue
            if int(ym_ordinal) < int(history_floor_ordinal):
                skipped_history += 1
                continue
            if int(ym_ordinal) < int(start_ordinal):
                selected_history_months.add((int(ym[0]), int(ym[1])))
        selected.append(path)

    adopted_history_months = len(selected_history_months)
    if skipped_future <= 0 and skipped_history <= 0 and adopted_history_months <= 0:
        return ("", len(selected), 0, 0)
    if not selected:
        return ("", 0, int(skipped_future), 0)

    stage_dir = tempfile.mkdtemp(prefix=f"pipeline_precompute_{prefix}_{tf}_")
    try:
        for path in selected:
            dst_path = os.path.join(stage_dir, os.path.basename(path))
            _link_or_copy_precompute_input(path, dst_path)
    except Exception:
        shutil.rmtree(stage_dir, ignore_errors=True)
        raise
    return (stage_dir, len(selected), int(skipped_future), int(adopted_history_months))


def _resolve_jpy_data_source() -> str:
    raw = str(os.getenv("LWF_JPY_DATA_SOURCE", "BITBANK") or "").strip().upper()
    if raw in ("AUTO", "BITBANK", ""):
        return "BITBANK"
    if raw == "GMO":
        return "GMO"
    return "BITBANK"


def _pipeline_force_enabled() -> bool:
    return _env_flag("LWF_PIPELINE_FORCE", False)


def _validate_generated_csv(path: str) -> int:
    if (not path) or (not os.path.exists(path)):
        raise RuntimeError(f"csv missing: {path}")
    if os.path.getsize(path) <= 0:
        raise RuntimeError(f"csv empty: {path}")
    with open(path, "rb") as fb:
        head = fb.read(512)
    if head[:4] == b"\xD0\xCF\x11\xE0":
        raise RuntimeError(f"csv invalid binary OLE detected: {path}")
    head_text = head.decode("utf-8", errors="ignore").lstrip().lower()
    if head_text.startswith("<html") or head_text.startswith("<!doctype html") or head_text.startswith("<body"):
        raise RuntimeError(f"csv invalid HTML detected: {path}")

    prev_ts: int | None = None
    data_rows = 0
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh)
        first = next(reader, None)
        if first is None:
            raise RuntimeError(f"csv no rows: {path}")
        rows_iter = reader
        if first:
            first0 = str(first[0]).strip().lower()
            if ("ts_ms" in first0) or (first0 in ("timestamp", "ts", "time", "open_time")):
                rows_iter = reader
            else:
                rows_iter = iter([first] + list(reader))
        for row in rows_iter:
            if not row:
                continue
            if len(row) < 6:
                raise RuntimeError(f"csv invalid column count path={path} row={row}")
            try:
                ts = int(float(row[0]))
                _ = float(row[1]); _ = float(row[2]); _ = float(row[3]); _ = float(row[4]); _ = float(row[5])
            except Exception as e:
                raise RuntimeError(f"csv invalid numeric row path={path} row={row} reason={e}")
            if prev_ts is not None and int(ts) <= int(prev_ts):
                raise RuntimeError(f"csv ts not strictly increasing path={path} prev={prev_ts} cur={ts}")
            prev_ts = int(ts)
            data_rows += 1
    if data_rows <= 0:
        raise RuntimeError(f"csv 0 data rows: {path}")
    return int(data_rows)


def _month_expected_rows(year: int, month: int, tf: str) -> int:
    bars_per_day = 288 if str(tf) == "5m" else 24 if str(tf) == "1h" else 0
    if bars_per_day <= 0:
        return 0
    dom = int(calendar.monthrange(int(year), int(month))[1])
    return int(dom) * int(bars_per_day)


def _month_floor_rows(tf: str) -> int:
    if str(tf) == "5m":
        return 5000
    if str(tf) == "1h":
        return 600
    return 1


def _is_current_utc_month(year: int, month: int) -> bool:
    now = datetime.now(timezone.utc)
    return int(year) == int(now.year) and int(month) == int(now.month)


def _inspect_month_csv(path: str, *, tf: str, year: int, month: int) -> tuple[bool, str, int, int]:
    expected = _month_expected_rows(int(year), int(month), str(tf))
    threshold = 0.80 if _is_current_utc_month(int(year), int(month)) else 0.95
    try:
        rows = _validate_generated_csv(path)
    except Exception as e:
        return (False, f"corrupt:{e}", 0, int(expected))
    floor_rows = _month_floor_rows(str(tf))
    if int(rows) < int(floor_rows):
        return (False, f"too_few_rows rows={rows} floor={floor_rows}", int(rows), int(expected))
    if int(expected) > 0:
        required = int(max(1, round(float(expected) * float(threshold))))
        if int(rows) < int(required):
            return (False, f"short_rows rows={rows} expected={expected} threshold={threshold:.2f}", int(rows), int(expected))
    return (True, "already_valid", int(rows), int(expected))


def _prepare_month_csv_target(path: str, *, tf: str, year: int, month: int, force: bool) -> tuple[bool, int, int]:
    if (not path) or (not os.path.exists(path)) or os.path.getsize(path) <= 0:
        return (False, 0, _month_expected_rows(int(year), int(month), str(tf)))
    valid, reason, rows, expected = _inspect_month_csv(path, tf=str(tf), year=int(year), month=int(month))
    ym = f"{int(year):04d}-{int(month):02d}"
    if (not force) and bool(valid):
        print(
            f"[PIPELINE][SKIP] tf={tf} ym={ym} reason={reason} rows={rows} expected={expected} path={path}",
            flush=True,
        )
        return (True, int(rows), int(expected))
    regen_reason = "force_redownload" if bool(force) else str(reason)
    print(
        f"[PIPELINE][REGEN] tf={tf} ym={ym} reason={regen_reason} rows={rows} expected={expected} path={path}",
        flush=True,
    )
    _safe_remove(path)
    return (False, int(rows), int(expected))


def _format_subprocess_cmd(cmd: list[str]) -> str:
    parts = [str(x) for x in (cmd or [])]
    if not parts:
        return ""
    try:
        return str(subprocess.list2cmdline(parts))
    except Exception:
        return " ".join(parts)


def _summarize_subprocess_text(text: str, *, limit: int = 600) -> str:
    raw = str(text or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if not raw:
        return ""
    lines = [line.strip() for line in raw.split("\n") if line.strip()]
    summary = " | ".join(lines[-4:]) if lines else raw
    if len(summary) <= int(limit):
        return summary
    return "..." + summary[-int(limit):]


def _run_jpy_month_csv(repo_root: str, symbol: str, prefix: str, tf: str, year: int, month: int, provider: str) -> str:
    out_dir = os.path.join(_market_data_root(repo_root), f"{prefix}_{tf}_{int(year)}")
    os.makedirs(out_dir, exist_ok=True)
    out_csv = os.path.join(out_dir, f"{prefix}-{tf}-{int(year)}-{int(month):02d}.csv")
    force = _pipeline_force_enabled()
    skip, _rows, _expected = _prepare_month_csv_target(out_csv, tf=str(tf), year=int(year), month=int(month), force=force)
    if skip:
        return out_csv
    since_ymd, until_ymd = _month_to_ymd(int(year), int(month))
    interval = "5min" if str(tf) == "5m" else "1hour" if str(tf) == "1h" else str(tf)
    run_cwd = os.path.abspath(str(repo_root or "").strip() or os.getcwd())
    tmp_out = out_csv + ".tmp"
    _safe_remove(tmp_out)
    cmd = [
        sys.executable,
        "-u",
        "-m",
        "app.core.jpy_dataset_downloader",
        "--provider",
        str(provider),
        "--pair",
        str(symbol),
        "--interval",
        str(interval),
        "--since",
        str(since_ymd),
        "--until",
        str(until_ymd),
        "--out",
        tmp_out,
    ]
    cmd_text = _format_subprocess_cmd(cmd)
    try:
        cp = subprocess.run(
            cmd,
            cwd=run_cwd,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
    except Exception as e:
        _safe_remove(tmp_out)
        print(
            f"[PIPELINE][ERROR] provider={str(provider).upper()} symbol={symbol} tf={tf} "
            f"ym={year:04d}-{month:02d} command={cmd_text}",
            flush=True,
        )
        print(
            f"[PIPELINE][ERROR] cwd={run_cwd} spawn_error={e}",
            flush=True,
        )
        raise RuntimeError(f"{provider} dataset download failed tf={tf} ym={year:04d}-{month:02d} code=spawn")
    if int(cp.returncode) != 0:
        stdout_summary = _summarize_subprocess_text(str(getattr(cp, "stdout", "") or ""))
        stderr_summary = _summarize_subprocess_text(str(getattr(cp, "stderr", "") or ""))
        print(
            f"[PIPELINE][ERROR] provider={str(provider).upper()} symbol={symbol} tf={tf} "
            f"ym={year:04d}-{month:02d} command={cmd_text}",
            flush=True,
        )
        print(
            f"[PIPELINE][ERROR] cwd={run_cwd} returncode={int(cp.returncode)}",
            flush=True,
        )
        if stdout_summary:
            print(f"[PIPELINE][ERROR] stdout={stdout_summary}", flush=True)
        if stderr_summary:
            print(f"[PIPELINE][ERROR] stderr={stderr_summary}", flush=True)
        _safe_remove(tmp_out)
        raise RuntimeError(f"{provider} dataset download failed tf={tf} ym={year:04d}-{month:02d} code={cp.returncode}")
    try:
        valid, reason, rows, expected = _inspect_month_csv(tmp_out, tf=str(tf), year=int(year), month=int(month))
        if not valid:
            raise RuntimeError(f"{reason} rows={rows} expected={expected}")
        os.replace(tmp_out, out_csv)
    except Exception:
        _safe_remove(tmp_out)
        _safe_remove(out_csv)
        raise
    stdout_summary = _summarize_subprocess_text(str(getattr(cp, "stdout", "") or ""))
    if stdout_summary:
        print(f"[PIPELINE]{stdout_summary}", flush=True)
    print(
        f"[PIPELINE][{str(provider).upper()}] symbol={symbol} tf={tf} ym={year:04d}-{month:02d} rows={rows} out={out_csv}",
        flush=True,
    )
    return out_csv


def _download(url: str, out_path: str) -> None:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = resp.read()
    with open(out_path, "wb") as f:
        f.write(data)


def _download_bytes(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return bytes(resp.read())


def _env_flag(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, "1" if default else "0") or "").strip().lower()
    if raw in ("1", "true", "yes", "on"):
        return True
    if raw in ("0", "false", "no", "off"):
        return False
    return bool(default)


def _tf_ms(tf: str) -> int:
    s = str(tf or "").strip().lower()
    if s.endswith("m"):
        return int(s[:-1]) * 60_000
    if s.endswith("h"):
        return int(s[:-1]) * 3_600_000
    raise ValueError(f"unsupported tf: {tf}")


def _month_range_ms(year: int, month: int) -> tuple[int, int]:
    start = datetime(int(year), int(month), 1, tzinfo=timezone.utc)
    if int(month) == 12:
        end = datetime(int(year) + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end = datetime(int(year), int(month) + 1, 1, tzinfo=timezone.utc)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def _tf_to_coinbase_granularity(tf: str) -> int:
    s = str(tf or "").strip().lower()
    if s == "5m":
        return 300
    if s == "1h":
        return 3600
    raise ValueError(f"unsupported coinbase tf: {tf}")


def _symbol_to_coinbase_product_id(symbol: str) -> str:
    raw = str(symbol or "").strip().upper().replace("_", "/").replace("-", "/")
    parts = [p for p in raw.split("/") if p]
    if len(parts) >= 2:
        return f"{parts[0]}-{parts[1]}"
    return raw.replace("/", "-")


def _ms_to_iso8601(ms: int) -> str:
    dt = datetime.fromtimestamp(float(ms) / 1000.0, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _iter_coinbase_windows(start_ms: int, end_ms: int, granularity_s: int) -> list[tuple[int, int]]:
    max_per_req = 300
    step_ms = int(granularity_s) * 1000 * int(max_per_req)
    out: list[tuple[int, int]] = []
    cur = int(start_ms)
    while cur < int(end_ms):
        nxt = min(int(end_ms), int(cur + step_ms))
        out.append((int(cur), int(nxt)))
        cur = int(nxt)
    return out


def _coinbase_download_json(url: str, *, retries: int = 3) -> list | None:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        },
    )
    last_err: Exception | None = None
    for attempt in range(int(retries) + 1):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                payload = json.loads(resp.read().decode("utf-8", errors="ignore"))
            if isinstance(payload, list):
                return payload
            return []
        except urllib.error.HTTPError as e:
            if int(getattr(e, "code", 0)) == 404:
                return []
            if int(getattr(e, "code", 0)) in (429, 500, 502, 503, 504) and attempt < int(retries):
                time.sleep(0.4 * float(attempt + 1))
                continue
            last_err = e
            break
        except Exception as e:
            if attempt < int(retries):
                time.sleep(0.4 * float(attempt + 1))
                continue
            last_err = e
            break
    if last_err is not None:
        raise RuntimeError(f"coinbase request failed: {last_err}")
    return []


def _coinbase_month_rows(symbol: str, tf: str, year: int, month: int) -> list[list[float]]:
    granularity = _tf_to_coinbase_granularity(str(tf))
    product_id = _symbol_to_coinbase_product_id(str(symbol))
    start_ms, end_ms = _month_range_ms(int(year), int(month))
    rows: list[list[float]] = []
    windows = _iter_coinbase_windows(int(start_ms), int(end_ms), int(granularity))
    for ws, we in windows:
        params = urllib.parse.urlencode(
            {
                "start": _ms_to_iso8601(int(ws)),
                "end": _ms_to_iso8601(int(we)),
                "granularity": int(granularity),
            }
        )
        url = f"https://api.exchange.coinbase.com/products/{urllib.parse.quote(product_id)}/candles?{params}"
        payload = _coinbase_download_json(url, retries=3)
        if payload:
            for item in payload:
                if (not isinstance(item, (list, tuple))) or len(item) < 6:
                    continue
                try:
                    ts_ms = int(float(item[0])) * 1000
                    low = float(item[1])
                    high = float(item[2])
                    opn = float(item[3])
                    cls = float(item[4])
                    vol = float(item[5])
                except Exception:
                    continue
                if int(ts_ms) < int(start_ms) or int(ts_ms) >= int(end_ms):
                    continue
                rows.append([int(ts_ms), float(opn), float(high), float(low), float(cls), float(vol)])
        time.sleep(0.12)
    return _merge_rows(rows)


def _rows_from_csv_bytes(blob: bytes) -> list[list[float]]:
    out: list[list[float]] = []
    text = blob.decode("utf-8-sig", errors="ignore")
    reader = csv.reader(io.StringIO(text))
    for row in reader:
        if not row or len(row) < 6:
            continue
        t0 = str(row[0]).strip().lower()
        if t0 in ("open_time", "timestamp", "time", "ts"):
            continue
        try:
            ts = int(float(row[0]))
            o = float(row[1])
            h = float(row[2])
            l = float(row[3])
            c = float(row[4])
            v = float(row[5])
        except Exception:
            continue
        out.append([int(ts), float(o), float(h), float(l), float(c), float(v)])
    return out


def _merge_rows(rows: list[list[float]]) -> list[list[float]]:
    by_ts: dict[int, list[float]] = {}
    for rr in rows:
        try:
            ts = int(rr[0])
        except Exception:
            continue
        by_ts[int(ts)] = [int(ts), float(rr[1]), float(rr[2]), float(rr[3]), float(rr[4]), float(rr[5])]
    return [by_ts[t] for t in sorted(by_ts.keys())]


def _write_rows_csv(path: str, rows: list[list[float]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    merged = _merge_rows(rows)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for rr in merged:
            w.writerow(rr)


def _read_rows_csv(path: str) -> list[list[float]]:
    if not os.path.exists(path):
        return []
    out: list[list[float]] = []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        r = csv.reader(f)
        for row in r:
            if not row or len(row) < 6:
                continue
            try:
                ts = int(float(row[0]))
                o = float(row[1])
                h = float(row[2])
                l = float(row[3])
                c = float(row[4])
                v = float(row[5])
            except Exception:
                continue
            out.append([int(ts), float(o), float(h), float(l), float(c), float(v)])
    return _merge_rows(out)


def _extract_csv(zip_path: str, out_csv: str) -> None:
    want = os.path.basename(out_csv).lower()
    with zipfile.ZipFile(zip_path, "r") as zf:
        names = [n for n in zf.namelist() if str(n).lower().endswith(".csv")]
        if not names:
            raise RuntimeError(f"zip has no csv: {zip_path}")
        pick = None
        for name in names:
            if os.path.basename(name).lower() == want:
                pick = name
                break
        if pick is None:
            pick = names[0]
        with zf.open(pick, "r") as src, open(out_csv, "wb") as dst:
            shutil.copyfileobj(src, dst)


def _download_month_csv(repo_root: str, prefix: str, tf: str, year: int, month: int) -> str:
    out_dir = os.path.join(_market_data_root(repo_root), f"{prefix}_{tf}_{int(year)}")
    os.makedirs(out_dir, exist_ok=True)
    stem = f"{prefix}-{tf}-{int(year)}-{int(month):02d}"
    out_csv = os.path.join(out_dir, f"{stem}.csv")
    if os.path.exists(out_csv) and os.path.getsize(out_csv) > 0:
        return out_csv
    zip_path = os.path.join(out_dir, f"{stem}.zip")
    url = (
        f"https://data.binance.vision/data/spot/monthly/klines/"
        f"{prefix}/{tf}/{stem}.zip"
    )
    _download(url=url, out_path=zip_path)
    _extract_csv(zip_path=zip_path, out_csv=out_csv)
    try:
        os.remove(zip_path)
    except Exception:
        pass
    return out_csv


def _download_daily_month_rows(prefix: str, tf: str, year: int, month: int) -> list[list[float]]:
    dom = int(calendar.monthrange(int(year), int(month))[1])
    rows: list[list[float]] = []
    for day in range(1, dom + 1):
        ymd = f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
        stem = f"{prefix}-{tf}-{ymd}"
        url = (
            f"https://data.binance.vision/data/spot/daily/klines/"
            f"{prefix}/{tf}/{stem}.zip"
        )
        try:
            blob = _download_bytes(url)
            with zipfile.ZipFile(io.BytesIO(blob), "r") as zf:
                names = [n for n in zf.namelist() if str(n).lower().endswith(".csv")]
                if not names:
                    continue
                with zf.open(names[0], "r") as fp:
                    rows.extend(_rows_from_csv_bytes(fp.read()))
        except Exception:
            continue
    return _merge_rows(rows)


def _ccxt_fill_missing_rows(
    *,
    symbol: str,
    tf: str,
    year: int,
    month: int,
    existing_rows: list[list[float]],
) -> list[list[float]]:
    try:
        import ccxt  # type: ignore
    except Exception as e:
        raise RuntimeError(f"ccxt import failed: {e}")

    tf_ms = int(_tf_ms(tf))
    start_ms, end_ms = _month_range_ms(int(year), int(month))
    expected = list(range(int(start_ms), int(end_ms), int(tf_ms)))
    exists = {int(rr[0]) for rr in existing_rows}
    missing = [t for t in expected if int(t) not in exists]
    if not missing:
        return _merge_rows(existing_rows)

    ranges: list[tuple[int, int]] = []
    st = int(missing[0])
    pv = int(missing[0])
    for t in missing[1:]:
        tt = int(t)
        if tt == pv + int(tf_ms):
            pv = tt
            continue
        ranges.append((int(st), int(pv + tf_ms)))
        st = tt
        pv = tt
    ranges.append((int(st), int(pv + tf_ms)))

    ex = ccxt.binance({"enableRateLimit": True})
    merged_rows = list(existing_rows)
    try:
        for rs, re in ranges:
            since = int(rs)
            while since < int(re):
                batch = ex.fetch_ohlcv(str(symbol), timeframe=str(tf), since=int(since), limit=1000)
                if not batch:
                    break
                last_ts = int(batch[-1][0])
                for rr in batch:
                    try:
                        ts = int(rr[0])
                    except Exception:
                        continue
                    if ts < int(rs) or ts >= int(re):
                        continue
                    try:
                        merged_rows.append([int(ts), float(rr[1]), float(rr[2]), float(rr[3]), float(rr[4]), float(rr[5])])
                    except Exception:
                        continue
                nxt = int(last_ts + tf_ms)
                if nxt <= int(since):
                    break
                since = int(nxt)
    finally:
        try:
            ex.close()
        except Exception:
            pass
    return _merge_rows(merged_rows)


def _ensure_month_csv_with_fallback(
    *,
    repo_root: str,
    symbol: str,
    prefix: str,
    tf: str,
    year: int,
    month: int,
    use_daily: bool,
    use_coinbase: bool,
    use_ccxt: bool,
) -> str:
    out_dir = os.path.join(_market_data_root(repo_root), f"{prefix}_{tf}_{int(year)}")
    os.makedirs(out_dir, exist_ok=True)
    ym = f"{int(year):04d}-{int(month):02d}"
    out_csv = os.path.join(out_dir, f"{prefix}-{tf}-{ym}.csv")
    force = _pipeline_force_enabled()
    skip, _rows, _expected = _prepare_month_csv_target(out_csv, tf=str(tf), year=int(year), month=int(month), force=force)
    if skip:
        return out_csv

    try:
        _download_month_csv(repo_root=repo_root, prefix=prefix, tf=tf, year=year, month=month)
        valid, reason, rows, expected = _inspect_month_csv(out_csv, tf=str(tf), year=int(year), month=int(month))
        if not valid:
            _safe_remove(out_csv)
            raise RuntimeError(f"{reason} rows={rows} expected={expected}")
        print(f"[PIPELINE][BINANCE] tf={tf} ym={ym} rows={rows} out={out_csv}", flush=True)
        return out_csv
    except Exception:
        pass

    if use_daily:
        print(f"[UI] monthly failed ym={ym} tf={tf} -> daily fallback", flush=True)
        daily_rows = _download_daily_month_rows(prefix=prefix, tf=tf, year=year, month=month)
        if daily_rows:
            _write_rows_csv(out_csv, daily_rows)
            valid, reason, rows, expected = _inspect_month_csv(out_csv, tf=str(tf), year=int(year), month=int(month))
            if not valid:
                print(f"[UI] daily invalid ym={ym} tf={tf} reason={reason} -> next fallback", flush=True)
            else:
                return out_csv

    if use_coinbase:
        print(f"[UI] daily failed ym={ym} tf={tf} -> coinbase fallback", flush=True)
        current_rows = _read_rows_csv(out_csv)
        try:
            cb_rows = _coinbase_month_rows(symbol=str(symbol), tf=str(tf), year=int(year), month=int(month))
        except Exception:
            cb_rows = []
        if cb_rows:
            merged = _merge_rows(list(current_rows) + list(cb_rows))
            _write_rows_csv(out_csv, merged)
            valid, reason, rows, expected = _inspect_month_csv(out_csv, tf=str(tf), year=int(year), month=int(month))
            if not valid:
                print(f"[UI] coinbase invalid ym={ym} tf={tf} reason={reason} -> next fallback", flush=True)
            else:
                return out_csv

    if use_ccxt:
        print(f"[UI] coinbase failed ym={ym} tf={tf} -> ccxt fallback", flush=True)
        current = _read_rows_csv(out_csv)
        filled = _ccxt_fill_missing_rows(
            symbol=str(symbol),
            tf=tf,
            year=year,
            month=month,
            existing_rows=current,
        )
        if filled:
            _write_rows_csv(out_csv, filled)
            valid, reason, rows, expected = _inspect_month_csv(out_csv, tf=str(tf), year=int(year), month=int(month))
            if not valid:
                _safe_remove(out_csv)
                raise RuntimeError(f"{reason} rows={rows} expected={expected}")
            return out_csv

    raise RuntimeError(f"monthly data not available ym={ym} tf={tf}")


def _run_precompute_for_range(
    repo_root: str,
    prefix: str,
    tf: str,
    since_ymd: str,
    until_ymd: str,
) -> None:
    market_root = _market_data_root(repo_root)
    file_glob = f"{prefix}-{tf}-20*.csv"
    start = _parse_yyyy_mm(since_ymd[:7])
    end = _parse_yyyy_mm(until_ymd[:7])
    years_in_range = sorted({int(year) for year, _month in _iter_months(start, end)})
    src_dir, src_source = _resolve_precompute_source_root(
        repo_root=repo_root,
        prefix=prefix,
        tf=tf,
        years_in_range=years_in_range,
    )
    script = os.path.join(repo_root, "precompute_indicators.py")
    out_root = _precomputed_out_root(repo_root)
    _warn_legacy_chart_paths(
        repo_root=repo_root,
        prefix=prefix,
        tf=tf,
        years_in_range=years_in_range,
        using_src_dir=src_dir,
        using_out_root=out_root,
    )
    if not _source_root_has_month_dirs(src_dir, prefix, tf, years_in_range):
        print(
            f"[PIPELINE][DIAG] precompute skipped tf={tf} prefix={prefix} "
            f"reason=source_dirs_missing checked_market_root={market_root} checked_repo_root={os.path.abspath(repo_root)} "
            f"resolved_source={src_source} resolved_src_dir={src_dir}",
            flush=True,
        )
        return
    cmd_src_dir = src_dir
    staged_src_dir = ""
    staged_count = 0
    skipped_future = 0
    historical_lookback_months = _precompute_history_lookback_months(tf=tf)
    adopted_history_months = 0
    try:
        staged_src_dir, staged_count, skipped_future, adopted_history_months = _stage_precompute_input_dir(
            src_dir=src_dir,
            prefix=prefix,
            tf=tf,
            start=start,
            end=end,
            historical_lookback_months=historical_lookback_months,
        )
    except Exception as e:
        staged_src_dir = ""
        print(
            f"[PIPELINE][DIAG] precompute staged_input_fallback tf={tf} prefix={prefix} "
            f"reason={e} src_dir={os.path.abspath(src_dir)}",
            flush=True,
        )
    print(
        f"[PIPELINE][DIAG] precompute historical_lookback tf={tf} prefix={prefix} "
        f"requested_months={historical_lookback_months} adopted_months={adopted_history_months} "
        f"since={start[0]:04d}-{start[1]:02d} until={end[0]:04d}-{end[1]:02d}",
        flush=True,
    )
    if adopted_history_months > 0:
        print(
            f"[PIPELINE][DIAG] precompute historical_lookback_note tf={tf} prefix={prefix} "
            f"reason=pre_since_months_staged_for_indicator_warmup",
            flush=True,
        )
    if staged_src_dir:
        cmd_src_dir = staged_src_dir
        print(
            f"[PIPELINE][DIAG] precompute staged_input tf={tf} prefix={prefix} "
            f"selected={staged_count} skipped_future={skipped_future} until={end[0]:04d}-{end[1]:02d} "
            f"stage_dir={staged_src_dir}",
            flush=True,
        )
    print(
        f"[PIPELINE][PRECOMPUTE] tf={tf} prefix={prefix} src_dir={os.path.abspath(src_dir)} "
        f"out_root={out_root} glob={file_glob}",
        flush=True,
    )
    cmd = [
        sys.executable,
        "-u",
        script,
        "--src-dir",
        cmd_src_dir,
        "--glob",
        file_glob,
        "--out-root",
        out_root,
        "--tf",
        tf,
        "--symbol",
        prefix,
        "--since",
        str(since_ymd),
        "--until",
        str(until_ymd),
        "--ema-spans",
        "9",
        "21",
        "20",
        "50",
        "--strict",
        "--force",
    ]
    run_cwd = staged_src_dir if staged_src_dir else repo_root
    try:
        cp = subprocess.run(cmd, cwd=run_cwd, check=False)
    finally:
        if staged_src_dir:
            shutil.rmtree(staged_src_dir, ignore_errors=True)
    if int(cp.returncode) != 0:
        raise RuntimeError(f"precompute failed tf={tf} since={since_ymd} until={until_ymd} code={cp.returncode}")


def _ym_from_ts_ms(ts_ms: int, *, exclusive_end: bool = False) -> str:
    ref_ms = int(ts_ms)
    if exclusive_end:
        ref_ms = max(0, int(ref_ms) - 1)
    dt = datetime.fromtimestamp(int(ref_ms) / 1000.0, tz=timezone.utc)
    return f"{int(dt.year):04d}-{int(dt.month):02d}"


def resolve_prepare_month_window(
    *,
    since_ms: int | None = None,
    until_ms: int | None = None,
    year: int | None = None,
    years: list[int] | None = None,
) -> tuple[str, str]:
    years_list = sorted({int(y) for y in list(years or []) if int(y) > 0})
    if years_list:
        return (f"{int(years_list[0]):04d}-01", f"{int(years_list[-1]):04d}-12")
    if year is not None and int(year) > 0:
        return (f"{int(year):04d}-01", f"{int(year):04d}-12")

    since_value = int(since_ms) if since_ms is not None else None
    until_value = int(until_ms) if until_ms is not None else None
    if since_value is not None and until_value is not None:
        return (
            _ym_from_ts_ms(int(since_value), exclusive_end=False),
            _ym_from_ts_ms(int(until_value), exclusive_end=True),
        )
    if since_value is not None:
        ym = _ym_from_ts_ms(int(since_value), exclusive_end=False)
        return (ym, ym)
    if until_value is not None:
        ym = _ym_from_ts_ms(int(until_value), exclusive_end=True)
        return (ym, ym)

    now = datetime.now(timezone.utc)
    ym_now = f"{int(now.year):04d}-{int(now.month):02d}"
    return (ym_now, ym_now)


def auto_prepare_runtime_data(
    *,
    symbol: str,
    context: str,
    since_ms: int | None = None,
    until_ms: int | None = None,
    year: int | None = None,
    years: list[int] | None = None,
    missing_items: list[str] | None = None,
    searched_paths: list[str] | None = None,
    fallback_sources: list[str] | None = None,
) -> dict[str, Any]:
    from_ym, to_ym = resolve_prepare_month_window(
        since_ms=since_ms,
        until_ms=until_ms,
        year=year,
        years=years,
    )
    missing_text = ",".join([str(item) for item in list(missing_items or []) if str(item or "").strip()]) or "unspecified"
    searched_text = " | ".join([os.path.abspath(str(path)) for path in list(searched_paths or []) if str(path or "").strip()])
    fallback_text = ", ".join([str(item) for item in list(fallback_sources or []) if str(item or "").strip()]) or "pipeline_download+precompute"
    print(
        f"[AUTO_PREP][{str(context or 'runtime').upper()}] symbol={symbol} from={from_ym} to={to_ym} "
        f"missing={missing_text} fallback={fallback_text}",
        flush=True,
    )
    if searched_text:
        print(
            f"[AUTO_PREP][{str(context or 'runtime').upper()}] searched_paths={searched_text}",
            flush=True,
        )
    run_pipeline(symbol=str(symbol), from_ym=str(from_ym), to_ym=str(to_ym))
    return {
        "symbol": str(symbol),
        "context": str(context or "").upper(),
        "from_ym": str(from_ym),
        "to_ym": str(to_ym),
        "missing_items": list(missing_items or []),
        "searched_paths": [os.path.abspath(str(path)) for path in list(searched_paths or []) if str(path or "").strip()],
        "fallback_sources": list(fallback_sources or []),
    }


def run_pipeline(*, symbol: str, from_ym: str, to_ym: str) -> int:
    start = _parse_yyyy_mm(from_ym)
    end = _parse_yyyy_mm(to_ym)
    if start > end:
        raise ValueError("from must be <= to")
    p = get_paths()
    repo_root = str(p.repo_root)
    prefix = symbol_to_prefix(symbol)
    months = _iter_months(start, end)
    use_daily = _env_flag("BOT_DL_FALLBACK_DAILY", True)
    use_coinbase = _env_flag("BOT_DL_FALLBACK_COINBASE", True)
    use_ccxt = _env_flag("BOT_DL_FALLBACK_CCXT", False)
    since_ymd = f"{int(start[0]):04d}-{int(start[1]):02d}-01"
    until_dom = int(calendar.monthrange(int(end[0]), int(end[1]))[1])
    until_ymd = f"{int(end[0]):04d}-{int(end[1]):02d}-{int(until_dom):02d}"
    print(
        f"[PIPELINE] start symbol={prefix} from={from_ym} to={to_ym} months={len(months)}",
        flush=True,
    )
    use_jpy_provider = _is_jpy_symbol(symbol)
    jpy_source = _resolve_jpy_data_source()
    if use_jpy_provider:
        print(f"[PIPELINE] source={jpy_source} symbol={symbol}", flush=True)
    for year, month in months:
        if use_jpy_provider:
            _run_jpy_month_csv(
                repo_root=repo_root,
                symbol=symbol,
                prefix=prefix,
                tf="5m",
                year=year,
                month=month,
                provider=jpy_source,
            )
            _run_jpy_month_csv(
                repo_root=repo_root,
                symbol=symbol,
                prefix=prefix,
                tf="1h",
                year=year,
                month=month,
                provider=jpy_source,
            )
        else:
            out_5m = _ensure_month_csv_with_fallback(
                repo_root=repo_root,
                symbol=symbol,
                prefix=prefix,
                tf="5m",
                year=year,
                month=month,
                use_daily=bool(use_daily),
                use_coinbase=bool(use_coinbase),
                use_ccxt=bool(use_ccxt),
            )
            _validate_generated_csv(out_5m)
            out_1h = _ensure_month_csv_with_fallback(
                repo_root=repo_root,
                symbol=symbol,
                prefix=prefix,
                tf="1h",
                year=year,
                month=month,
                use_daily=bool(use_daily),
                use_coinbase=bool(use_coinbase),
                use_ccxt=bool(use_ccxt),
            )
            _validate_generated_csv(out_1h)
    _run_precompute_for_range(
        repo_root=repo_root,
        prefix=prefix,
        tf="5m",
        since_ymd=since_ymd,
        until_ymd=until_ymd,
    )
    _run_precompute_for_range(
        repo_root=repo_root,
        prefix=prefix,
        tf="1h",
        since_ymd=since_ymd,
        until_ymd=until_ymd,
    )
    print("[PIPELINE] done", flush=True)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Download monthly klines and run precompute indicators.")
    ap.add_argument("--symbol", type=str, default="BTC/JPY")
    ap.add_argument("--from", dest="from_ym", type=str, required=True)
    ap.add_argument("--to", dest="to_ym", type=str, required=True)
    args = ap.parse_args()
    try:
        return int(run_pipeline(symbol=str(args.symbol), from_ym=str(args.from_ym), to_ym=str(args.to_ym)))
    except Exception as e:
        print(f"[PIPELINE] error {e}", flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
