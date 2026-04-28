# BUILD_ID: 2026-04-29_free_dataset_canonical_shared_root_priority_v1
# BUILD_ID: 2026-04-18_free_dataset_shared_root_fallback_v1
# BUILD_ID: 2026-03-20_settings_bridge_effective_defaults_v1
# BUILD_ID: 2026-03-20_market_data_stage2_strict_cross_root_guard_v1
# BUILD_ID: 2026-03-20_market_data_stage2_legacy_fallback_diag_v2
# BUILD_ID: 2026-03-20_market_data_stage2_legacy_fallback_diag_v1
# BUILD_ID: 2026-03-11_symbol_resolution_align_v1
# BUILD_ID: 2026-03-09_dataset_override_symbol_guard_v1
from __future__ import annotations

import glob
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

from app.core.instrument_registry import list_instruments
from app.core.paths import get_paths
from app.core.source_provenance import record_path_source_event_once


BUILD_ID = "2026-04-29_free_dataset_canonical_shared_root_priority_v1"

_DIR_RE = re.compile(r"^([A-Za-z0-9]+)_(5m|1h)_(\d{4})$", flags=re.IGNORECASE)
_FILE_RE = re.compile(r"^([A-Za-z0-9]+)-(5m|1h)-(\d{4})-(\d{2})\.csv$", flags=re.IGNORECASE)


@dataclass(frozen=True)
class DatasetLayout:
    root: str
    prefix: str
    year: int | None
    tf_entry: str
    tf_filter: str
    dir_entry: str
    dir_filter: str
    glob_entry: str
    glob_filter: str
    reason: str


@dataclass(frozen=True)
class DatasetSpec:
    root: str
    prefix: str
    year: int
    month: int | None
    dir_5m: str
    dir_1h: str
    paths_5m: list[str]
    paths_1h: list[str]
    diagnostics: dict[str, Any]


class DatasetResolutionError(RuntimeError):
    def __init__(self, message: str, diagnostics: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.diagnostics = diagnostics or {}


def symbol_to_prefix(symbol: str) -> str:
    s = str(symbol or "").strip().upper()
    if not s:
        return "BTCJPY"
    s = s.replace("/", "").replace("-", "").replace("_", "")
    s = "".join(ch for ch in s if ch.isalnum())
    return s or "BTCJPY"


def normalize_symbol_token(symbol: Any) -> str:
    raw = str(symbol or "").strip().upper()
    if not raw:
        return ""
    raw = raw.replace("/", "").replace("-", "").replace("_", "").replace(":", "")
    return "".join(ch for ch in raw if ch.isalnum())


def dataset_prefix_to_symbol(prefix: str, *, exchange_id: str = "", fallback: str = "") -> str:
    token = normalize_symbol_token(prefix)
    fallback_text = str(fallback or "").strip()
    if not token:
        return fallback_text

    exchange_text = str(exchange_id or "").strip().lower()
    seen: set[str] = set()
    search_groups = []
    if exchange_text:
        search_groups.append(list_instruments(exchange_text, include_hidden=True))
    search_groups.append(list_instruments("", include_hidden=True))
    for group in search_groups:
        for item in group:
            symbol_text = str(getattr(item, "symbol", "") or "").strip()
            if (not symbol_text) or (symbol_text in seen):
                continue
            seen.add(symbol_text)
            if token in (
                normalize_symbol_token(symbol_text),
                normalize_symbol_token(getattr(item, "dataset_prefix", "") or symbol_text),
            ):
                return symbol_text

    for quote in ("USDT", "USDC", "JPY", "USD", "BTC", "ETH"):
        if token.endswith(quote) and len(token) > len(quote):
            return f"{token[:-len(quote)]}/{quote}"
    return fallback_text or token


def normalize_runtime_symbol(symbol: str, *, exchange_id: str = "", fallback: str = "") -> str:
    raw = str(symbol or "").strip()
    fallback_text = str(fallback or "").strip()
    if not raw:
        return fallback_text

    raw_upper = raw.upper()
    if "/" in raw_upper:
        base, quote = raw_upper.split("/", 1)
        base = base.strip()
        quote = quote.strip()
        if base and quote:
            return f"{base}/{quote}"

    for sep in ("-", "_", ":"):
        if sep not in raw_upper:
            continue
        base, quote = raw_upper.split(sep, 1)
        base = base.strip()
        quote = quote.strip()
        if base and quote:
            return f"{base}/{quote}"

    return dataset_prefix_to_symbol(raw_upper, exchange_id=exchange_id, fallback=fallback_text or raw_upper)


def resolve_dataset_override_symbol(*, exchange_id: str = "", fallback: str = "") -> tuple[str, str]:
    override = load_runtime_dataset_override()
    if not isinstance(override, dict):
        return ("", "")

    dataset_prefix = str(override.get("dataset_prefix", "") or "").strip()
    if dataset_prefix:
        symbol = normalize_runtime_symbol(dataset_prefix, exchange_id=exchange_id, fallback=fallback)
        if symbol:
            return (symbol, f"dataset_prefix={symbol_to_prefix(dataset_prefix)}")

    dataset_csv = str(override.get("dataset_csv", "") or "").strip()
    if dataset_csv:
        csv_prefix, _csv_year = infer_prefix_year_from_source(dataset_csv, "")
        if csv_prefix:
            symbol = normalize_runtime_symbol(csv_prefix, exchange_id=exchange_id, fallback=fallback)
            if symbol:
                return (symbol, f"dataset_csv={os.path.basename(dataset_csv)}")

    return ("", "")


def _env_text(name: str) -> str:
    return str(os.getenv(name, "") or "").strip()


def _infer_tf_from_name(name: str) -> str:
    base = os.path.basename(str(name or "")).strip()
    if not base:
        return ""
    lower = base.lower()
    for tf in ("5m", "1h"):
        if (f"-{tf}-" in lower) or (f"_{tf}_" in lower):
            return tf
    return ""


def _infer_prefix_from_name(name: str, tf_hint: str = "") -> str:
    base = os.path.basename(str(name or "")).strip()
    if not base:
        return ""
    lower = base.lower()
    tf_tokens: list[str] = []
    tf_norm = str(tf_hint or "").strip().lower()
    if tf_norm:
        tf_tokens.append(tf_norm)
    for tf in ("5m", "1h"):
        if tf not in tf_tokens:
            tf_tokens.append(tf)
    for tf in tf_tokens:
        for marker in (f"-{tf}-", f"_{tf}_"):
            idx = lower.find(marker)
            if idx > 0:
                return symbol_to_prefix(base[:idx])
    return ""


def _infer_year_from_name(name: str, tf_hint: str = "") -> int | None:
    base = os.path.basename(str(name or "")).strip()
    if not base:
        return None
    lower = base.lower()
    tf_tokens: list[str] = []
    tf_norm = str(tf_hint or "").strip().lower()
    if tf_norm:
        tf_tokens.append(tf_norm)
    for tf in ("5m", "1h"):
        if tf not in tf_tokens:
            tf_tokens.append(tf)
    for tf in tf_tokens:
        for marker in (f"-{tf}-", f"_{tf}_"):
            idx = lower.find(marker)
            if idx < 0:
                continue
            tail = base[idx + len(marker):]
            m = re.search(r"((?:19|20)\d{2})", tail)
            if m:
                return _safe_year(m.group(1))
    return None


def _path_is_under_root(root: str, path: str) -> bool:
    root_abs = os.path.abspath(str(root or ""))
    path_abs = os.path.abspath(str(path or ""))
    if not root_abs or not path_abs:
        return False
    try:
        return os.path.commonpath([root_abs, path_abs]) == root_abs
    except Exception:
        return False


def _env_flag(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "1" if default else "0") or "").strip().lower()
    if raw in ("1", "true", "yes", "on"):
        return True
    if raw in ("0", "false", "no", "off"):
        return False
    return bool(default)


def _looks_like_market_data_root(path: str) -> bool:
    parts = [str(x).strip().lower() for x in os.path.abspath(str(path or "")).split(os.sep) if str(x).strip()]
    return bool(parts) and (parts[-1] == "market_data")


def _legacy_fallback_reason(*, root_default: str, default_dir_abs: str) -> str:
    if not str(default_dir_abs or "").strip():
        return "no_default_dir"
    if _path_is_under_root(root_default, default_dir_abs):
        return "same_root"
    return "cross_root_legacy"


def _legacy_fallback_allowed(*, root_default: str, default_dir_abs: str, strict_market_data_root: bool = False) -> bool:
    reason = _legacy_fallback_reason(root_default=root_default, default_dir_abs=default_dir_abs)
    if strict_market_data_root and reason == "cross_root_legacy":
        return False
    return bool(str(default_dir_abs or "").strip())


def _normalize_epoch_to_ms(ts_raw: int) -> int:
    ts = int(ts_raw)
    abs_ts = abs(ts)
    # seconds
    if 0 < abs_ts < 100_000_000_000:
        return int(ts * 1000)
    # nanoseconds
    if abs_ts >= 10_000_000_000_000_000:
        return int(ts // 1_000_000)
    # microseconds
    if abs_ts >= 10_000_000_000_000:
        return int(ts // 1_000)
    # milliseconds
    return int(ts)


def normalize_timestamp_to_ms(value: Any) -> int | None:
    if value is None:
        return None

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            return _normalize_epoch_to_ms(int(float(value)))
        except Exception:
            return None

    s = str(value).strip()
    if not s:
        return None

    # numeric string
    try:
        return _normalize_epoch_to_ms(int(float(s)))
    except Exception:
        pass

    s_iso = s
    if s_iso.endswith("Z"):
        s_iso = s_iso[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s_iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000.0)
    except Exception:
        pass

    dt_formats = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    )
    for fmt in dt_formats:
        try:
            dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000.0)
        except Exception:
            continue
    return None


def _safe_year(v: Any) -> int | None:
    try:
        y = int(str(v).strip())
    except Exception:
        return None
    if 1900 <= y <= 9999:
        return int(y)
    return None


def _safe_month(v: Any) -> int | None:
    try:
        m = int(str(v).strip())
    except Exception:
        return None
    if 1 <= m <= 12:
        return int(m)
    return None


def _normalize_tf(tf: str, fallback: str) -> str:
    s = str(tf or "").strip().lower()
    return s or str(fallback or "").strip().lower() or "5m"


def _available_years(root: str, prefix: str, tf: str) -> list[int]:
    out: list[int] = []
    if not os.path.isdir(root):
        return out
    want = f"{prefix}_{tf}_"
    for name in os.listdir(root):
        p = os.path.join(root, str(name))
        if not os.path.isdir(p):
            continue
        n = str(name).strip()
        if not n.lower().startswith(want.lower()):
            continue
        m = _DIR_RE.match(n)
        if not m:
            continue
        y = _safe_year(m.group(3))
        if y is not None:
            out.append(int(y))
    return sorted(set(out))


def infer_prefix_year_from_source(path: str, fallback_prefix: str = "") -> tuple[str, int | None]:
    p = str(path or "").strip()
    if not p:
        return (symbol_to_prefix(fallback_prefix), None)
    if ":" in p and (not os.path.exists(p)):
        p = p.split(":", 1)[0]
    p = os.path.abspath(p)

    if os.path.isfile(p):
        pfx_csv, y_csv, _m_csv, _tf_csv = _infer_csv_meta(p)
        if pfx_csv:
            return (symbol_to_prefix(pfx_csv), _safe_year(y_csv))
        d = os.path.basename(os.path.dirname(p))
        dm = _DIR_RE.match(d)
        if dm:
            return (symbol_to_prefix(dm.group(1)), _safe_year(dm.group(3)))
    if os.path.isdir(p):
        dn = os.path.basename(p)
        dm = _DIR_RE.match(dn)
        if dm:
            return (symbol_to_prefix(dm.group(1)), _safe_year(dm.group(3)))
    return (symbol_to_prefix(fallback_prefix), None)


def _infer_csv_meta(path: str) -> tuple[str, int | None, int | None, str]:
    fn = os.path.basename(str(path or ""))
    m = _FILE_RE.match(fn)
    if m:
        return (
            symbol_to_prefix(m.group(1)),
            _safe_year(m.group(3)),
            _safe_month(m.group(4)),
            str(m.group(2)).lower(),
        )
    d = os.path.basename(os.path.dirname(str(path or "")))
    dm = _DIR_RE.match(d)
    if dm:
        return (
            symbol_to_prefix(dm.group(1)),
            _safe_year(dm.group(3)),
            None,
            str(dm.group(2)).lower(),
        )
    legacy_prefix = _infer_prefix_from_name(fn)
    legacy_year = _infer_year_from_name(fn)
    legacy_tf = _infer_tf_from_name(fn)
    if legacy_prefix or legacy_tf:
        return (symbol_to_prefix(legacy_prefix), _safe_year(legacy_year), None, str(legacy_tf).lower())
    return ("", None, None, "")


def resolve_dataset_layout(
    dataset_root: str,
    prefix: str,
    year: int | None,
    tf_dirs: Iterable[str] = ("5m", "1h"),
) -> DatasetLayout:
    tf_list = [str(x or "").strip().lower() for x in tf_dirs]
    tf_entry = _normalize_tf(tf_list[0] if tf_list else "5m", "5m")
    tf_filter = _normalize_tf(tf_list[1] if len(tf_list) > 1 else "1h", "1h")

    root = os.path.abspath(str(dataset_root or "."))
    if os.path.isfile(root):
        root = os.path.dirname(root)
    prefix_n = symbol_to_prefix(prefix)

    y = _safe_year(year)
    reason = "explicit_year" if y is not None else "auto_latest_common_year"
    if y is None:
        ys_e = _available_years(root, prefix_n, tf_entry)
        ys_f = _available_years(root, prefix_n, tf_filter)
        common = sorted(set(ys_e).intersection(ys_f))
        if common:
            y = int(common[-1])
        else:
            y = int((ys_e or ys_f or [0])[-1]) or None
            reason = "auto_latest_partial_year"

    if y is not None:
        dir_entry = os.path.join(root, f"{prefix_n}_{tf_entry}_{int(y)}")
        dir_filter = os.path.join(root, f"{prefix_n}_{tf_filter}_{int(y)}")
    else:
        dir_entry = os.path.join(root, f"{prefix_n}_{tf_entry}")
        dir_filter = os.path.join(root, f"{prefix_n}_{tf_filter}")

    glob_entry = f"{prefix_n}-{tf_entry}-*.csv"
    glob_filter = f"{prefix_n}-{tf_filter}-*.csv"

    return DatasetLayout(
        root=root,
        prefix=prefix_n,
        year=y,
        tf_entry=tf_entry,
        tf_filter=tf_filter,
        dir_entry=dir_entry,
        dir_filter=dir_filter,
        glob_entry=glob_entry,
        glob_filter=glob_filter,
        reason=reason,
    )


def infer_year_from_ms(ms: int | None) -> int | None:
    if ms is None:
        return None
    try:
        return int(datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc).year)
    except Exception:
        return None


def infer_month_from_ms(ms: int | None) -> int | None:
    if ms is None:
        return None
    try:
        return int(datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc).month)
    except Exception:
        return None


def load_runtime_dataset_override() -> dict[str, Any]:
    env_root = str(os.getenv("BACKTEST_DATASET_ROOT", os.getenv("BOT_REPLAY_DATASET_ROOT", "")) or "").strip()
    env_prefix = str(os.getenv("BACKTEST_DATASET_PREFIX", os.getenv("BOT_REPLAY_DATASET_PREFIX", "")) or "").strip()
    env_year = _safe_year(os.getenv("BACKTEST_DATASET_YEAR", os.getenv("BOT_REPLAY_DATASET_YEAR", "")))
    env_month = _safe_month(os.getenv("BACKTEST_DATASET_MONTH", os.getenv("BOT_REPLAY_DATASET_MONTH", "")))
    env_csv = str(os.getenv("BACKTEST_DATASET_CSV", os.getenv("BOT_REPLAY_DATASET_CSV", "")) or "").strip()
    if env_root or env_prefix or (env_year is not None) or (env_month is not None) or env_csv:
        return {
            "source": "env",
            "dataset_root": os.path.abspath(env_root) if env_root else "",
            "dataset_prefix": symbol_to_prefix(env_prefix) if env_prefix else "",
            "dataset_year": env_year,
            "dataset_month": env_month,
            "dataset_csv": os.path.abspath(env_csv) if env_csv else "",
        }

    p = get_paths().settings_path
    if not os.path.exists(p):
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if not isinstance(raw, dict):
            return {}
        root = str(raw.get("dataset_root", raw.get("replay_dataset_root", "")) or "").strip()
        pref_raw = str(raw.get("dataset_prefix", raw.get("replay_dataset_prefix", "")) or "").strip()
        year = _safe_year(raw.get("dataset_year", raw.get("replay_dataset_year", None)))
        month = _safe_month(raw.get("dataset_month", raw.get("replay_dataset_month", None)))
        csv_path = str(raw.get("dataset_csv", raw.get("replay_dataset_csv", "")) or "").strip()
        if (not root) and (not pref_raw) and (year is None) and (month is None) and (not csv_path):
            return {}
        return {
            "source": "settings",
            "dataset_root": os.path.abspath(root) if root else "",
            "dataset_prefix": symbol_to_prefix(pref_raw) if pref_raw else "",
            "dataset_year": year,
            "dataset_month": month,
            "dataset_csv": os.path.abspath(csv_path) if csv_path else "",
            "settings_path": p,
        }
    except Exception:
        return {}


def _resolve_dataset_root(dataset_root: str) -> str:
    p = os.path.abspath(str(dataset_root or "."))
    if os.path.isfile(p):
        return os.path.dirname(p)
    return p


def _canonical_market_data_root() -> str:
    try:
        market_root = str(getattr(get_paths(), "market_data_dir", "") or "").strip()
        if market_root:
            return _resolve_dataset_root(market_root)
    except Exception:
        pass
    return ""


def _dataset_root_is_generic_for_canonical(root: str) -> bool:
    root_abs = _resolve_dataset_root(root)
    if not root_abs:
        return False
    try:
        paths = get_paths()
    except Exception:
        paths = None

    candidates: list[str] = []
    if paths is not None:
        for attr in ("repo_root", "market_data_dir", "product_data_root", "shared_data_root", "shared_root"):
            value = str(getattr(paths, attr, "") or "").strip()
            if value:
                candidates.append(value)
                candidates.append(os.path.join(value, "market_data"))
        for _source, value in tuple(getattr(paths, "market_data_candidates", ()) or ()):
            if value:
                candidates.append(str(value))
    candidates.append(os.getcwd())
    candidates.append(os.path.join(os.getcwd(), "market_data"))

    root_key = os.path.normcase(root_abs)
    for candidate in candidates:
        candidate_text = str(candidate or "").strip()
        if not candidate_text:
            continue
        if os.path.normcase(_resolve_dataset_root(candidate_text)) == root_key:
            return True
    return False


def _resolve_dir_with_root(root: str, maybe_dir: str) -> str:
    s = str(maybe_dir or "").strip()
    if not s:
        return ""
    if os.path.isabs(s):
        return os.path.abspath(s)
    return os.path.abspath(os.path.join(root, s))


def _infer_year_from_dir_name(path: str) -> int | None:
    s = str(path or "").strip().rstrip("/\\")
    if not s:
        return None
    m = _DIR_RE.match(os.path.basename(s))
    if not m:
        return None
    return _safe_year(m.group(3))


def _collect_tf_paths(
    *,
    dir_path: str,
    prefix: str,
    tf: str,
    year: int,
    month: int | None,
) -> list[str]:
    d = os.path.abspath(str(dir_path or "."))
    if not os.path.isdir(d):
        return []
    out: list[str] = []
    for p in sorted(glob.glob(os.path.join(d, "*.csv"))):
        if not os.path.isfile(p):
            continue
        m = _FILE_RE.match(os.path.basename(p))
        if not m:
            continue
        pfx = symbol_to_prefix(m.group(1))
        tf_v = str(m.group(2)).lower()
        y_v = _safe_year(m.group(3))
        m_v = _safe_month(m.group(4))
        if pfx != symbol_to_prefix(prefix):
            continue
        if tf_v != str(tf).lower():
            continue
        if y_v != int(year):
            continue
        if (month is not None) and (m_v != int(month)):
            continue
        out.append(os.path.abspath(p))
    return out


def _collect_glob_paths(*, dir_path: str, pattern: str) -> list[str]:
    d = os.path.abspath(str(dir_path or "."))
    pat = str(pattern or "").strip()
    if (not pat) or (not os.path.isdir(d)):
        return []
    out: list[str] = []
    for p in sorted(glob.glob(os.path.join(d, pat))):
        if os.path.isfile(p):
            out.append(os.path.abspath(p))
    return out


def _coalesce_month(*values: Any) -> int | None:
    for v in values:
        m = _safe_month(v)
        if m is not None:
            return int(m)
    return None


def _coalesce_year(*values: Any) -> int | None:
    for v in values:
        y = _safe_year(v)
        if y is not None:
            return int(y)
    return None


def _coalesce_text(*values: Any) -> str:
    for v in values:
        s = str(v or "").strip()
        if s:
            return s
    return ""


def _infer_prefix_from_dir_name(path: str) -> str:
    s = str(path or "").strip().rstrip("/\\")
    if not s:
        return ""
    m = _DIR_RE.match(os.path.basename(s))
    if not m:
        return ""
    return symbol_to_prefix(m.group(1))


def _resolve_effective_default_dataset_hints(
    *,
    root_default: str,
    prefix_default: str,
    year_default: int | None,
    default_dir_5m: str,
    default_glob_5m: str,
    default_dir_1h: str,
    default_glob_1h: str,
) -> dict[str, Any]:
    dir_5m_abs = _resolve_dir_with_root(root_default, default_dir_5m)
    dir_1h_abs = _resolve_dir_with_root(root_default, default_dir_1h)
    parent_candidates: list[str] = []
    for dir_abs in (dir_5m_abs, dir_1h_abs):
        dir_txt = str(dir_abs or "").strip().rstrip("/\\")
        if not dir_txt:
            continue
        parent = os.path.dirname(os.path.abspath(dir_txt))
        if parent:
            parent_candidates.append(parent)

    root_hint = ""
    unique_parents = sorted(set(parent_candidates))
    if len(unique_parents) == 1:
        root_hint = unique_parents[0]

    env_dataset_year = _safe_year(os.getenv("BACKTEST_DATASET", os.getenv("BOT_REPLAY_DATASET_YEAR", "")))
    prefix_hint = symbol_to_prefix(
        _coalesce_text(
            prefix_default,
            _infer_prefix_from_dir_name(default_dir_5m),
            _infer_prefix_from_dir_name(default_dir_1h),
            _infer_prefix_from_name(default_glob_5m, "5m"),
            _infer_prefix_from_name(default_glob_1h, "1h"),
        )
    )
    year_hint = _coalesce_year(
        env_dataset_year,
        year_default,
        _infer_year_from_dir_name(default_dir_5m),
        _infer_year_from_dir_name(default_dir_1h),
        _infer_year_from_name(default_glob_5m, "5m"),
        _infer_year_from_name(default_glob_1h, "1h"),
    )
    return {
        "active": bool(root_hint or prefix_hint or (year_hint is not None)),
        "root": root_hint,
        "prefix": prefix_hint,
        "year": year_hint,
        "dir_5m": dir_5m_abs,
        "dir_1h": dir_1h_abs,
    }


def _market_data_root_candidates(root_default: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: set[str] = set()

    def _add(source: str, root: str) -> None:
        root_txt = str(root or "").strip()
        if not root_txt:
            return
        root_abs = _resolve_dataset_root(root_txt)
        root_key = os.path.normcase(root_abs)
        if root_key in seen:
            return
        seen.add(root_key)
        out.append((str(source or "").strip() or "dataset_root", root_abs))

    try:
        for source, root in tuple(getattr(get_paths(), "market_data_candidates", ()) or ()):
            source_text = "canonical_market_data" if str(source or "").strip() == "canonical_shared_root" else str(source or "").strip()
            _add(source_text, str(root or ""))
    except Exception:
        pass
    _add("dataset_root", root_default)
    return out


def _candidate_default_dir_for_root(default_dir_abs: str, candidate_root: str) -> str:
    dir_txt = str(default_dir_abs or "").strip().rstrip("/\\")
    candidate_root_abs = _resolve_dataset_root(candidate_root)
    if (not dir_txt) or (not candidate_root_abs):
        return ""
    if _path_is_under_root(candidate_root_abs, dir_txt):
        return os.path.abspath(dir_txt)
    base_name = os.path.basename(dir_txt)
    if not base_name:
        return ""
    return os.path.abspath(os.path.join(candidate_root_abs, base_name))


def _search_candidate_tf_paths(
    *,
    candidate_root: str,
    prefix: str,
    tf: str,
    years: list[int],
    month: int | None,
    default_dir_abs: str,
    default_glob: str,
    searched_dirs: list[str],
) -> tuple[list[str], list[int], list[int], str]:
    candidate_root_abs = _resolve_dataset_root(candidate_root)
    if not candidate_root_abs:
        return ([], [], list(years or []), "")

    selected_dir = ""
    found_years: list[int] = []
    missing_years: list[int] = []
    paths: list[str] = []
    for year_it in list(years or []):
        dir_year = os.path.abspath(os.path.join(candidate_root_abs, f"{symbol_to_prefix(prefix)}_{str(tf).lower()}_{int(year_it)}"))
        if dir_year not in searched_dirs:
            searched_dirs.append(dir_year)
        found = _collect_tf_paths(
            dir_path=dir_year,
            prefix=prefix,
            tf=tf,
            year=int(year_it),
            month=month,
        )
        if found:
            paths.extend(found)
            found_years.append(int(year_it))
            if not selected_dir:
                selected_dir = dir_year if len(list(years or [])) == 1 else candidate_root_abs
        else:
            missing_years.append(int(year_it))

    if paths or len(list(years or [])) != 1:
        return (paths, found_years, missing_years, selected_dir)

    fallback_dir = _candidate_default_dir_for_root(default_dir_abs, candidate_root_abs)
    if fallback_dir and fallback_dir not in searched_dirs:
        searched_dirs.append(fallback_dir)
    year_single = int(list(years or [0])[0] or 0)
    fallback_paths = _collect_tf_paths(
        dir_path=fallback_dir,
        prefix=prefix,
        tf=tf,
        year=year_single,
        month=month,
    )
    if (not fallback_paths) and str(default_glob or "").strip():
        fallback_paths = _collect_glob_paths(dir_path=fallback_dir, pattern=default_glob)
    if fallback_paths:
        return (list(fallback_paths), [year_single], [], fallback_dir)
    return ([], [], list(years or []), fallback_dir)


def _record_dataset_root_event(
    *,
    context: str,
    source: str,
    tf: str,
    path: str,
    prefix: str,
    years_requested: Iterable[int],
) -> None:
    path_text = str(path or "").strip()
    source_text = str(source or "").strip()
    if (not path_text) or (not source_text):
        return
    try:
        record_path_source_event_once(
            component="dataset",
            event="dataset_root_resolved",
            source=source_text,
            path=os.path.abspath(path_text),
            extra={
                "build_id": BUILD_ID,
                "context": str(context or "DATASET").upper(),
                "tf": str(tf or "").strip().lower(),
                "prefix": symbol_to_prefix(prefix),
                "years_requested": [int(y) for y in list(years_requested or [])],
            },
        )
    except Exception:
        return


def _settings_root_is_generic_for_defaults(settings_root: str, *, root_default: str, default_root: str) -> bool:
    settings_root_abs = _resolve_dataset_root(settings_root) if str(settings_root or "").strip() else ""
    if not settings_root_abs:
        return True
    paths = get_paths()
    generic_roots = {
        _resolve_dataset_root(root_default),
        _resolve_dataset_root(default_root),
        _resolve_dataset_root(getattr(paths, "repo_root", "")),
        _resolve_dataset_root(getattr(paths, "market_data_dir", "")),
    }
    for _source, candidate_root in tuple(getattr(paths, "market_data_candidates", ()) or ()):
        generic_roots.add(_resolve_dataset_root(str(candidate_root or "")))
    return settings_root_abs in {str(x or "").strip() for x in generic_roots if str(x or "").strip()}


def _bridge_settings_to_effective_defaults(
    override: dict[str, Any],
    *,
    context: str,
    root_default: str,
    default_hints: dict[str, Any],
    explicit_csv_override_active: bool,
) -> dict[str, Any]:
    if str(override.get("source", "") or "") != "settings":
        return override
    if explicit_csv_override_active:
        return override
    if str(override.get("dataset_csv", "") or "").strip():
        return override
    if str(context or "").strip().upper() not in ("BACKTEST", "REPLAY"):
        return override

    default_root = str(default_hints.get("root", "") or "").strip()
    default_prefix = symbol_to_prefix(str(default_hints.get("prefix", "") or ""))
    default_year = _safe_year(default_hints.get("year", None))
    if (not default_root) and (not default_prefix) and (default_year is None):
        return override
    if default_prefix != "BTCJPY" and (not _looks_like_market_data_root(default_root)):
        return override
    if not _settings_root_is_generic_for_defaults(
        str(override.get("dataset_root", "") or ""),
        root_default=root_default,
        default_root=default_root,
    ):
        return override

    out = dict(override)
    if default_root:
        out["dataset_root"] = default_root
    if default_prefix:
        out["dataset_prefix"] = default_prefix
    if default_year is not None:
        out["dataset_year"] = int(default_year)
    out["settings_bridge"] = "config_effective_defaults"
    return out


def _scan_dir_prefixes(dir_path: str, tf: str) -> list[str]:
    d = os.path.abspath(str(dir_path or "."))
    if not os.path.isdir(d):
        return []
    tf_norm = str(tf or "").strip().lower()
    out: set[str] = set()
    for p in sorted(glob.glob(os.path.join(d, "*.csv")))[:256]:
        if not os.path.isfile(p):
            continue
        pref, _year, _month, tf_seen = _infer_csv_meta(p)
        if tf_norm and tf_seen and tf_seen != tf_norm:
            continue
        if pref:
            out.add(pref)
    return sorted(out)


def _resolve_explicit_csv_env(
    *,
    root_default: str,
    default_dir_5m: str,
    default_glob_5m: str,
    default_dir_1h: str,
    default_glob_1h: str,
) -> dict[str, Any]:
    raw_dir_5m = _env_text("BACKTEST_CSV_DIR_5M")
    raw_dir_1h = _env_text("BACKTEST_CSV_DIR_1H")
    raw_glob_5m = _env_text("BACKTEST_CSV_GLOB_5M")
    raw_glob_1h = _env_text("BACKTEST_CSV_GLOB_1H")
    active_5m = bool(raw_dir_5m or raw_glob_5m)
    active_1h = bool(raw_dir_1h or raw_glob_1h)
    eff_dir_5m_raw = str(raw_dir_5m or default_dir_5m or "").strip()
    eff_dir_1h_raw = str(raw_dir_1h or default_dir_1h or "").strip()
    eff_glob_5m = str(raw_glob_5m or default_glob_5m or "").strip()
    eff_glob_1h = str(raw_glob_1h or default_glob_1h or "").strip()
    dir_5m_abs = _resolve_dir_with_root(root_default, eff_dir_5m_raw)
    dir_1h_abs = _resolve_dir_with_root(root_default, eff_dir_1h_raw)
    paths_5m = _collect_glob_paths(dir_path=dir_5m_abs, pattern=eff_glob_5m)
    paths_1h = _collect_glob_paths(dir_path=dir_1h_abs, pattern=eff_glob_1h)
    prefixes_5m = sorted(
        set(
            [pref for pref, _year, _month, tf in (_infer_csv_meta(p) for p in paths_5m) if pref and ((not tf) or (tf == "5m"))]
        )
    )
    prefixes_1h = sorted(
        set(
            [pref for pref, _year, _month, tf in (_infer_csv_meta(p) for p in paths_1h) if pref and ((not tf) or (tf == "1h"))]
        )
    )
    if (not prefixes_5m) and dir_5m_abs:
        prefixes_5m = _scan_dir_prefixes(dir_5m_abs, "5m")
    if (not prefixes_1h) and dir_1h_abs:
        prefixes_1h = _scan_dir_prefixes(dir_1h_abs, "1h")
    glob_prefix_5m = _infer_prefix_from_name(eff_glob_5m, "5m")
    glob_prefix_1h = _infer_prefix_from_name(eff_glob_1h, "1h")
    if glob_prefix_5m and glob_prefix_5m not in prefixes_5m:
        prefixes_5m.append(glob_prefix_5m)
    if glob_prefix_1h and glob_prefix_1h not in prefixes_1h:
        prefixes_1h.append(glob_prefix_1h)
    return {
        "active": bool(active_5m or active_1h),
        "active_5m": bool(active_5m),
        "active_1h": bool(active_1h),
        "dir_5m_raw": raw_dir_5m,
        "dir_1h_raw": raw_dir_1h,
        "glob_5m_raw": raw_glob_5m,
        "glob_1h_raw": raw_glob_1h,
        "dir_5m": dir_5m_abs,
        "dir_1h": dir_1h_abs,
        "glob_5m": eff_glob_5m,
        "glob_1h": eff_glob_1h,
        "paths_5m": list(paths_5m),
        "paths_1h": list(paths_1h),
        "prefixes_5m": sorted(set(prefixes_5m)),
        "prefixes_1h": sorted(set(prefixes_1h)),
    }


def build_dataset_mismatch_message(*, context: str, details: dict[str, Any]) -> str:
    reason = "; ".join([str(x) for x in (details.get("mismatch_reasons", []) or []) if str(x).strip()])
    if not reason:
        reason = "dataset override values are inconsistent"
    return f"[{str(context or 'DATASET').upper()}][DATASET_MISMATCH] {reason} diagnostics={format_dataset_diagnostics(details)}"


def format_dataset_diagnostics(diagnostics: dict[str, Any] | None) -> str:
    try:
        return json.dumps(diagnostics or {}, ensure_ascii=False, sort_keys=True)
    except Exception:
        return str(diagnostics or {})


def resolve_dataset(
    *,
    dataset_root: str,
    prefix: str,
    year: int | None,
    years: Iterable[int] | None = None,
    month: int | None = None,
    tf_dirs: Iterable[str] = ("5m", "1h"),
    single_csv: str = "",
    default_dir_5m: str = "",
    default_glob_5m: str = "",
    default_dir_1h: str = "",
    default_glob_1h: str = "",
    runtime_symbol: str = "",
    context: str = "DATASET",
) -> DatasetSpec:
    tf_list = [str(x or "").strip().lower() for x in tf_dirs]
    tf_5m = _normalize_tf(tf_list[0] if tf_list else "5m", "5m")
    tf_1h = _normalize_tf(tf_list[1] if len(tf_list) > 1 else "1h", "1h")

    root_default = _resolve_dataset_root(dataset_root)
    prefix_default = symbol_to_prefix(prefix)
    year_default = _safe_year(year)
    years_default: list[int] = []
    if years is not None:
        for yv in years:
            ysafe = _safe_year(yv)
            if ysafe is not None:
                years_default.append(int(ysafe))
    years_default = sorted(set(years_default))
    month_default = _safe_month(month)

    ov = load_runtime_dataset_override()
    default_dir_5m_abs = _resolve_dir_with_root(root_default, default_dir_5m)
    default_dir_1h_abs = _resolve_dir_with_root(root_default, default_dir_1h)
    default_hints = _resolve_effective_default_dataset_hints(
        root_default=root_default,
        prefix_default=prefix_default,
        year_default=year_default,
        default_dir_5m=default_dir_5m,
        default_glob_5m=default_glob_5m,
        default_dir_1h=default_dir_1h,
        default_glob_1h=default_glob_1h,
    )
    explicit_csv_env = _resolve_explicit_csv_env(
        root_default=root_default,
        default_dir_5m=default_dir_5m,
        default_glob_5m=default_glob_5m,
        default_dir_1h=default_dir_1h,
        default_glob_1h=default_glob_1h,
    )
    if isinstance(ov, dict):
        ov = _bridge_settings_to_effective_defaults(
            ov,
            context=context,
            root_default=root_default,
            default_hints=default_hints,
            explicit_csv_override_active=bool(explicit_csv_env.get("active", False)),
        )
    source = "default"

    source_csv = str(single_csv or "").strip()
    if not source_csv:
        source_csv = str(ov.get("dataset_csv", "") if isinstance(ov, dict) else "").strip()
    if not source_csv:
        src_root = str(dataset_root or "").strip()
        if src_root and os.path.isfile(src_root) and str(src_root).lower().endswith(".csv"):
            source_csv = src_root

    resolved_root = root_default
    resolved_prefix = prefix_default
    resolved_year = year_default
    resolved_years = list(years_default)
    resolved_month = month_default
    csv_abs = ""

    if source_csv:
        source = "single_csv"
        csv_abs = os.path.abspath(str(source_csv))
        if not os.path.isfile(csv_abs):
            diag = {
                "source": source,
                "single_csv": csv_abs,
                "dataset_root_input": str(dataset_root or ""),
                "prefix_input": str(prefix or ""),
                "year_input": year_default,
                "years_input": list(years_default),
                "month_input": month_default,
            }
            raise DatasetResolutionError(
                f"[DATASET] single csv is not found: {csv_abs}",
                diagnostics=diag,
            )
        pfx_csv, y_csv, m_csv, _tf_csv = _infer_csv_meta(csv_abs)
        parent_dir = os.path.basename(os.path.dirname(csv_abs))
        if _DIR_RE.match(parent_dir):
            resolved_root = os.path.abspath(os.path.join(os.path.dirname(csv_abs), ".."))
        else:
            resolved_root = os.path.abspath(os.path.dirname(csv_abs))
        resolved_prefix = symbol_to_prefix(pfx_csv or str(ov.get("dataset_prefix", "") or prefix_default))
        resolved_year = _coalesce_year(y_csv, ov.get("dataset_year", None), year_default)
        resolved_month = _coalesce_month(m_csv, ov.get("dataset_month", None), month_default)
    elif ov:
        source = str(ov.get("source", "env") or "env")
        resolved_root = _resolve_dataset_root(str(ov.get("dataset_root", "") or root_default))
        resolved_prefix = symbol_to_prefix(str(ov.get("dataset_prefix", "") or prefix_default))
        resolved_year = _coalesce_year(ov.get("dataset_year", None), year_default)
        resolved_month = _coalesce_month(ov.get("dataset_month", None), month_default)

    if not resolved_years:
        if resolved_year is None and source == "default":
            resolved_year = _coalesce_year(
                _infer_year_from_dir_name(default_dir_5m),
                _infer_year_from_dir_name(default_dir_1h),
                year_default,
            )
        if resolved_year is not None:
            resolved_years = [int(resolved_year)]

    resolved_root = _resolve_dataset_root(resolved_root)
    canonical_market_root = _canonical_market_data_root()
    if (
        canonical_market_root
        and str(source or "").strip() in ("default", "settings")
        and _dataset_root_is_generic_for_canonical(resolved_root)
    ):
        resolved_root = canonical_market_root
    resolved_prefix = symbol_to_prefix(resolved_prefix or prefix_default)
    resolved_month = _safe_month(resolved_month)

    if not resolved_years:
        diag = {
            "source": source,
            "dataset_root": resolved_root,
            "dataset_prefix": resolved_prefix,
            "dataset_year": None,
            "dataset_years": [],
            "dataset_month": resolved_month,
            "single_csv": csv_abs,
            "naming_rule": "<root>/<PREFIX>_5m_<YEAR>/<PREFIX>-5m-YYYY-MM.csv and <root>/<PREFIX>_1h_<YEAR>/<PREFIX>-1h-YYYY-MM.csv",
        }
        raise DatasetResolutionError(
            "[DATASET] dataset_year is required (year fallback is disabled).",
            diagnostics=diag,
        )

    resolved_years = sorted(set(int(yv) for yv in resolved_years))
    y = int(resolved_years[0])
    dir_5m = os.path.abspath(os.path.join(resolved_root, f"{resolved_prefix}_{tf_5m}_{y}"))
    dir_1h = os.path.abspath(os.path.join(resolved_root, f"{resolved_prefix}_{tf_1h}_{y}"))
    searched_5m: list[str] = []
    searched_1h: list[str] = []
    paths_5m: list[str] = []
    paths_1h: list[str] = []
    found_years_5m: list[int] = []
    found_years_1h: list[int] = []
    missing_years_5m: list[int] = []
    missing_years_1h: list[int] = []

    for year_it in resolved_years:
        y_it = int(year_it)
        dir_5m_y = os.path.abspath(os.path.join(resolved_root, f"{resolved_prefix}_{tf_5m}_{y_it}"))
        dir_1h_y = os.path.abspath(os.path.join(resolved_root, f"{resolved_prefix}_{tf_1h}_{y_it}"))
        searched_5m.append(dir_5m_y)
        searched_1h.append(dir_1h_y)
        p5 = _collect_tf_paths(dir_path=dir_5m_y, prefix=resolved_prefix, tf=tf_5m, year=y_it, month=resolved_month)
        p1 = _collect_tf_paths(dir_path=dir_1h_y, prefix=resolved_prefix, tf=tf_1h, year=y_it, month=resolved_month)
        if p5:
            paths_5m.extend(p5)
            found_years_5m.append(y_it)
        else:
            missing_years_5m.append(y_it)
        if p1:
            paths_1h.extend(p1)
            found_years_1h.append(y_it)
        else:
            missing_years_1h.append(y_it)

    default_dir_5m_abs = _resolve_dir_with_root(resolved_root, default_dir_5m)
    default_dir_1h_abs = _resolve_dir_with_root(resolved_root, default_dir_1h)
    strict_market_data_root = _env_flag("LWF_STRICT_MARKET_DATA_ROOT", False)
    legacy_fallback_5m_reason = _legacy_fallback_reason(root_default=resolved_root, default_dir_abs=default_dir_5m_abs)
    legacy_fallback_1h_reason = _legacy_fallback_reason(root_default=resolved_root, default_dir_abs=default_dir_1h_abs)
    legacy_fallback_5m_allowed = _legacy_fallback_allowed(
        root_default=resolved_root,
        default_dir_abs=default_dir_5m_abs,
        strict_market_data_root=strict_market_data_root,
    )
    legacy_fallback_1h_allowed = _legacy_fallback_allowed(
        root_default=resolved_root,
        default_dir_abs=default_dir_1h_abs,
        strict_market_data_root=strict_market_data_root,
    )
    legacy_fallback_5m_used = False
    legacy_fallback_1h_used = False
    dir_5m_source = str(source or "canonical_market_data")
    dir_1h_source = str(source or "canonical_market_data")

    if (
        source == "default"
        and len(resolved_years) == 1
        and (not paths_5m)
        and default_dir_5m_abs
        and legacy_fallback_5m_allowed
    ):
        if default_dir_5m_abs not in searched_5m:
            searched_5m.append(default_dir_5m_abs)
        fallback_5m = _collect_tf_paths(
            dir_path=default_dir_5m_abs,
            prefix=resolved_prefix,
            tf=tf_5m,
            year=y,
            month=resolved_month,
        )
        if fallback_5m:
            dir_5m = default_dir_5m_abs
            paths_5m = fallback_5m
            found_years_5m = [y]
            missing_years_5m = []
            legacy_fallback_5m_used = True
        elif default_glob_5m:
            fallback_5m = _collect_glob_paths(dir_path=default_dir_5m_abs, pattern=default_glob_5m)
            if fallback_5m:
                dir_5m = default_dir_5m_abs
                paths_5m = fallback_5m
                found_years_5m = [y]
                missing_years_5m = []
                legacy_fallback_5m_used = True
                dir_5m_source = "default_fallback"

    if (
        source == "default"
        and len(resolved_years) == 1
        and (not paths_1h)
        and default_dir_1h_abs
        and legacy_fallback_1h_allowed
    ):
        if default_dir_1h_abs not in searched_1h:
            searched_1h.append(default_dir_1h_abs)
        fallback_1h = _collect_tf_paths(
            dir_path=default_dir_1h_abs,
            prefix=resolved_prefix,
            tf=tf_1h,
            year=y,
            month=resolved_month,
        )
        if fallback_1h:
            dir_1h = default_dir_1h_abs
            paths_1h = fallback_1h
            found_years_1h = [y]
            missing_years_1h = []
            legacy_fallback_1h_used = True
        elif default_glob_1h:
            fallback_1h = _collect_glob_paths(dir_path=default_dir_1h_abs, pattern=default_glob_1h)
            if fallback_1h:
                dir_1h = default_dir_1h_abs
                paths_1h = fallback_1h
                found_years_1h = [y]
                missing_years_1h = []
                legacy_fallback_1h_used = True
                dir_1h_source = "default_fallback"

    if source == "default":
        for candidate_source, candidate_root in _market_data_root_candidates(resolved_root):
            if os.path.normcase(_resolve_dataset_root(candidate_root)) == os.path.normcase(resolved_root):
                continue
            if (not paths_5m) or missing_years_5m:
                candidate_paths_5m, candidate_found_5m, candidate_missing_5m, candidate_dir_5m = _search_candidate_tf_paths(
                    candidate_root=candidate_root,
                    prefix=resolved_prefix,
                    tf=tf_5m,
                    years=resolved_years,
                    month=resolved_month,
                    default_dir_abs=default_dir_5m_abs,
                    default_glob=default_glob_5m,
                    searched_dirs=searched_5m,
                )
                if candidate_paths_5m:
                    dir_5m = str(candidate_dir_5m or candidate_root)
                    paths_5m = list(candidate_paths_5m)
                    found_years_5m = list(candidate_found_5m)
                    missing_years_5m = list(candidate_missing_5m)
                    legacy_fallback_5m_used = str(candidate_source) != "canonical_market_data"
                    legacy_fallback_5m_reason = "cross_root_legacy"
                    dir_5m_source = str(candidate_source)
            if (not paths_1h) or missing_years_1h:
                candidate_paths_1h, candidate_found_1h, candidate_missing_1h, candidate_dir_1h = _search_candidate_tf_paths(
                    candidate_root=candidate_root,
                    prefix=resolved_prefix,
                    tf=tf_1h,
                    years=resolved_years,
                    month=resolved_month,
                    default_dir_abs=default_dir_1h_abs,
                    default_glob=default_glob_1h,
                    searched_dirs=searched_1h,
                )
                if candidate_paths_1h:
                    dir_1h = str(candidate_dir_1h or candidate_root)
                    paths_1h = list(candidate_paths_1h)
                    found_years_1h = list(candidate_found_1h)
                    missing_years_1h = list(candidate_missing_1h)
                    legacy_fallback_1h_used = str(candidate_source) != "canonical_market_data"
                    legacy_fallback_1h_reason = "cross_root_legacy"
                    dir_1h_source = str(candidate_source)
            if paths_5m and paths_1h and (not missing_years_5m) and (not missing_years_1h):
                break

    if len(resolved_years) == 1:
        glob_5m = f"{resolved_prefix}-{tf_5m}-{y:04d}-*.csv"
        glob_1h = f"{resolved_prefix}-{tf_1h}-{y:04d}-*.csv"
        if resolved_month is not None:
            glob_5m = f"{resolved_prefix}-{tf_5m}-{y:04d}-{int(resolved_month):02d}.csv"
            glob_1h = f"{resolved_prefix}-{tf_1h}-{y:04d}-{int(resolved_month):02d}.csv"
    else:
        glob_5m = f"{resolved_prefix}-{tf_5m}-YYYY-*.csv"
        glob_1h = f"{resolved_prefix}-{tf_1h}-YYYY-*.csv"

    def _years_from_paths(paths: Iterable[str]) -> list[int]:
        out: list[int] = []
        for p in paths:
            _pref, y_v, _month, _tf = _infer_csv_meta(p)
            if y_v is not None:
                out.append(int(y_v))
        return sorted(set(out))

    runtime_symbol_text = str(runtime_symbol or "").strip()
    runtime_prefix = symbol_to_prefix(runtime_symbol_text) if runtime_symbol_text else ""
    env_csv_symbol = _env_text("BACKTEST_CSV_SYMBOL")
    env_csv_prefix = symbol_to_prefix(env_csv_symbol) if env_csv_symbol else ""
    explicit_csv_override_active = bool(explicit_csv_env.get("active", False)) and (source != "single_csv")
    explicit_prefixes_5m = list(explicit_csv_env.get("prefixes_5m", []) or []) if explicit_csv_override_active else []
    explicit_prefixes_1h = list(explicit_csv_env.get("prefixes_1h", []) or []) if explicit_csv_override_active else []
    effective_source = "env_csv_override" if explicit_csv_override_active else source
    mismatch_reasons: list[str] = []
    mismatch_details: dict[str, Any] = {
        "context": str(context or "DATASET").upper(),
        "dataset_source": effective_source,
        "dataset_root": resolved_root,
        "dataset_prefix": resolved_prefix,
        "runtime_symbol": runtime_symbol_text,
        "runtime_prefix": runtime_prefix,
        "csv_symbol_env": env_csv_symbol,
        "csv_prefix_env": env_csv_prefix,
        "dir_5m": str((explicit_csv_env.get("dir_5m", "") if explicit_csv_override_active else dir_5m) or dir_5m),
        "glob_5m": str((explicit_csv_env.get("glob_5m", "") if explicit_csv_override_active else glob_5m) or glob_5m),
        "dir_1h": str((explicit_csv_env.get("dir_1h", "") if explicit_csv_override_active else dir_1h) or dir_1h),
        "glob_1h": str((explicit_csv_env.get("glob_1h", "") if explicit_csv_override_active else glob_1h) or glob_1h),
        "explicit_prefixes_5m": list(explicit_prefixes_5m),
        "explicit_prefixes_1h": list(explicit_prefixes_1h),
    }

    if runtime_prefix and runtime_prefix != resolved_prefix:
        mismatch_reasons.append(
            f"runtime symbol {runtime_symbol_text} (prefix={runtime_prefix}) does not match dataset prefix {resolved_prefix}"
        )
    if env_csv_prefix and env_csv_prefix != resolved_prefix:
        mismatch_reasons.append(
            f"BACKTEST_CSV_SYMBOL {env_csv_symbol} (prefix={env_csv_prefix}) does not match dataset prefix {resolved_prefix}"
        )

    if explicit_csv_override_active:
        if len(explicit_prefixes_5m) > 1:
            mismatch_reasons.append(f"BACKTEST_CSV_DIR/GLOB_5M resolved multiple prefixes {explicit_prefixes_5m}")
        if len(explicit_prefixes_1h) > 1:
            mismatch_reasons.append(f"BACKTEST_CSV_DIR/GLOB_1H resolved multiple prefixes {explicit_prefixes_1h}")
        if explicit_prefixes_5m and (len(explicit_prefixes_5m) == 1) and (explicit_prefixes_5m[0] != resolved_prefix):
            mismatch_reasons.append(
                f"BACKTEST_CSV_DIR/GLOB_5M points to prefix {explicit_prefixes_5m[0]} but dataset prefix is {resolved_prefix}"
            )
        if explicit_prefixes_1h and (len(explicit_prefixes_1h) == 1) and (explicit_prefixes_1h[0] != resolved_prefix):
            mismatch_reasons.append(
                f"BACKTEST_CSV_DIR/GLOB_1H points to prefix {explicit_prefixes_1h[0]} but dataset prefix is {resolved_prefix}"
            )
        if (
            explicit_prefixes_5m
            and explicit_prefixes_1h
            and (len(explicit_prefixes_5m) == 1)
            and (len(explicit_prefixes_1h) == 1)
            and (explicit_prefixes_5m[0] != explicit_prefixes_1h[0])
        ):
            mismatch_reasons.append(
                f"BACKTEST_CSV_DIR/GLOB 5m prefix {explicit_prefixes_5m[0]} and 1h prefix {explicit_prefixes_1h[0]} disagree"
            )
        if source in ("env", "settings"):
            if bool(explicit_csv_env.get("active_5m", False)) and str(explicit_csv_env.get("dir_5m", "")).strip():
                if not _path_is_under_root(resolved_root, str(explicit_csv_env.get("dir_5m", ""))):
                    mismatch_reasons.append(
                        f"BACKTEST_CSV_DIR_5M {explicit_csv_env.get('dir_5m', '')} is outside dataset root {resolved_root}"
                    )
            if bool(explicit_csv_env.get("active_1h", False)) and str(explicit_csv_env.get("dir_1h", "")).strip():
                if not _path_is_under_root(resolved_root, str(explicit_csv_env.get("dir_1h", ""))):
                    mismatch_reasons.append(
                        f"BACKTEST_CSV_DIR_1H {explicit_csv_env.get('dir_1h', '')} is outside dataset root {resolved_root}"
                    )

    if mismatch_reasons:
        mismatch_details["mismatch_reasons"] = list(mismatch_reasons)
        raise DatasetResolutionError(
            build_dataset_mismatch_message(context=context, details=mismatch_details),
            diagnostics=mismatch_details,
        )

    if explicit_csv_override_active:
        if bool(explicit_csv_env.get("active_5m", False)):
            dir_5m = str(explicit_csv_env.get("dir_5m", dir_5m) or dir_5m)
            glob_5m = str(explicit_csv_env.get("glob_5m", glob_5m) or glob_5m)
            paths_5m = list(explicit_csv_env.get("paths_5m", []) or [])
            searched_5m = [str(dir_5m)]
            found_years_5m = _years_from_paths(paths_5m)
            missing_years_5m = [int(yv) for yv in resolved_years if int(yv) not in set(found_years_5m)] if found_years_5m else list(resolved_years)
        if bool(explicit_csv_env.get("active_1h", False)):
            dir_1h = str(explicit_csv_env.get("dir_1h", dir_1h) or dir_1h)
            glob_1h = str(explicit_csv_env.get("glob_1h", glob_1h) or glob_1h)
            paths_1h = list(explicit_csv_env.get("paths_1h", []) or [])
            searched_1h = [str(dir_1h)]
            found_years_1h = _years_from_paths(paths_1h)
            missing_years_1h = [int(yv) for yv in resolved_years if int(yv) not in set(found_years_1h)] if found_years_1h else list(resolved_years)

    years_loaded = sorted(set(found_years_5m).intersection(found_years_1h))
    settings_bridge = str(ov.get("settings_bridge", "") if isinstance(ov, dict) else "").strip()
    diagnostics: dict[str, Any] = {
        "source": effective_source,
        "single_csv": csv_abs,
        "root": resolved_root,
        "prefix": resolved_prefix,
        "year": y,
        "years_requested": list(resolved_years),
        "years_loaded": list(years_loaded),
        "missing_years_5m": list(sorted(set(missing_years_5m))),
        "missing_years_1h": list(sorted(set(missing_years_1h))),
        "month": resolved_month,
        "dir_5m": dir_5m,
        "dir_1h": dir_1h,
        "dir_5m_source": str(dir_5m_source),
        "dir_1h_source": str(dir_1h_source),
        "paths_5m": list(paths_5m),
        "paths_1h": list(paths_1h),
        "paths_5m_count": int(len(paths_5m)),
        "paths_1h_count": int(len(paths_1h)),
        "searched_paths_5m": list(searched_5m),
        "searched_paths_1h": list(searched_1h),
        "default_dir_5m": default_dir_5m_abs,
        "default_dir_1h": default_dir_1h_abs,
        "default_glob_5m": str(default_glob_5m or ""),
        "default_glob_1h": str(default_glob_1h or ""),
        "strict_market_data_root": bool(strict_market_data_root),
        "legacy_fallback_5m_allowed": bool(legacy_fallback_5m_allowed),
        "legacy_fallback_1h_allowed": bool(legacy_fallback_1h_allowed),
        "legacy_fallback_5m_used": bool(legacy_fallback_5m_used),
        "legacy_fallback_1h_used": bool(legacy_fallback_1h_used),
        "legacy_fallback_5m_reason": str(legacy_fallback_5m_reason),
        "legacy_fallback_1h_reason": str(legacy_fallback_1h_reason),
        "glob_5m": glob_5m,
        "glob_1h": glob_1h,
        "runtime_symbol": runtime_symbol_text,
        "runtime_prefix": runtime_prefix,
        "csv_symbol_env": env_csv_symbol,
        "csv_prefix_env": env_csv_prefix,
        "explicit_csv_override_active": bool(explicit_csv_override_active),
        "explicit_prefixes_5m": list(explicit_prefixes_5m),
        "explicit_prefixes_1h": list(explicit_prefixes_1h),
        "market_data_candidates": [
            {"source": str(source_name), "path": str(root_path)}
            for source_name, root_path in _market_data_root_candidates(resolved_root)
        ],
        "expected_dirs": [os.path.join(resolved_root, f"{resolved_prefix}_{tf_5m}_{yy:04d}") for yy in resolved_years]
        + [os.path.join(resolved_root, f"{resolved_prefix}_{tf_1h}_{yy:04d}") for yy in resolved_years],
        "expected_files": [
            f"{resolved_prefix}-{tf_5m}-YYYY-MM.csv",
            f"{resolved_prefix}-{tf_1h}-YYYY-MM.csv",
        ],
        "naming_rule": "<root>/<PREFIX>_5m_<YEAR>/<PREFIX>-5m-YYYY-MM.csv and <root>/<PREFIX>_1h_<YEAR>/<PREFIX>-1h-YYYY-MM.csv",
    }
    if settings_bridge:
        diagnostics["settings_bridge"] = settings_bridge

    _record_dataset_root_event(
        context=context,
        source=dir_5m_source,
        tf=tf_5m,
        path=dir_5m,
        prefix=resolved_prefix,
        years_requested=resolved_years,
    )
    _record_dataset_root_event(
        context=context,
        source=dir_1h_source,
        tf=tf_1h,
        path=dir_1h,
        prefix=resolved_prefix,
        years_requested=resolved_years,
    )

    missing_tf = ""
    searched_dir = ""
    searched_glob = ""
    if (not paths_5m) or missing_years_5m:
        missing_tf = tf_5m
        y_missing = int(missing_years_5m[0] if missing_years_5m else y)
        searched_dir = os.path.join(resolved_root, f"{resolved_prefix}_{tf_5m}_{y_missing:04d}")
        searched_glob = glob_5m
    elif (not paths_1h) or missing_years_1h:
        missing_tf = tf_1h
        y_missing = int(missing_years_1h[0] if missing_years_1h else y)
        searched_dir = os.path.join(resolved_root, f"{resolved_prefix}_{tf_1h}_{y_missing:04d}")
        searched_glob = glob_1h

    if missing_tf:
        raise DatasetResolutionError(
            build_missing_dataset_message(
                context="DATASET_RESOLVE",
                tf=missing_tf,
                searched_dir=searched_dir,
                searched_glob=searched_glob,
                dataset_root=resolved_root,
                prefix=resolved_prefix,
                year=y,
                tf_dirs=(tf_5m, tf_1h),
                diagnostics=diagnostics,
            ),
            diagnostics=diagnostics,
        )

    return DatasetSpec(
        root=resolved_root,
        prefix=resolved_prefix,
        year=int(y),
        month=resolved_month,
        dir_5m=dir_5m,
        dir_1h=dir_1h,
        paths_5m=list(paths_5m),
        paths_1h=list(paths_1h),
        diagnostics=diagnostics,
    )


def build_missing_dataset_message(
    *,
    context: str,
    tf: str,
    searched_dir: str,
    searched_glob: str,
    dataset_root: str,
    prefix: str,
    year: int | None,
    tf_dirs: Iterable[str] = ("5m", "1h"),
    diagnostics: dict[str, Any] | None = None,
) -> str:
    tf_list = [str(x or "").strip().lower() for x in tf_dirs]
    tf_entry = _normalize_tf(tf_list[0] if tf_list else "5m", "5m")
    tf_filter = _normalize_tf(tf_list[1] if len(tf_list) > 1 else "1h", "1h")
    root = os.path.abspath(str(dataset_root or "."))
    pfx = symbol_to_prefix(prefix or "")
    y_txt = str(int(year)) if year is not None else "<year>"
    expected_5m_dir = os.path.join(root, f"{pfx}_{tf_entry}_{y_txt}")
    expected_1h_dir = os.path.join(root, f"{pfx}_{tf_filter}_{y_txt}")
    expected_5m_file = f"{pfx}-{tf_entry}-YYYY-MM.csv"
    expected_1h_file = f"{pfx}-{tf_filter}-YYYY-MM.csv"
    naming_rule = (
        "<root>/<PREFIX>_5m_<YEAR>/<PREFIX>-5m-YYYY-MM.csv ; "
        "<root>/<PREFIX>_1h_<YEAR>/<PREFIX>-1h-YYYY-MM.csv"
    )
    parts = [
        f"[{context}] {tf} rows are empty.",
        f"searched_dir={searched_dir}",
        f"searched_glob={searched_glob}",
        f"expected_dirs=({expected_5m_dir}, {expected_1h_dir})",
        f"expected_files=({expected_5m_file}, {expected_1h_file})",
        f"naming_rule={naming_rule}",
    ]
    if diagnostics:
        parts.append(f"diagnostics={format_dataset_diagnostics(diagnostics)}")
    return " ".join(parts)
