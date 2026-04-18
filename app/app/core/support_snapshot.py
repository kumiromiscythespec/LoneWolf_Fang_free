# BUILD_ID: 2026-04-18_free_bundle_preflight_failures_sidecar_meta_v1
# BUILD_ID: 2026-04-18_free_bundle_preflight_failures_and_snapshot_align_v1
# BUILD_ID: 2026-04-18_free_support_snapshot_preflight_failures_v1
# BUILD_ID: 2026-04-09_free_support_snapshot_v1
from __future__ import annotations

import json
import os
import platform
import re
from datetime import datetime, timezone
from typing import Any, Iterable, Sequence

from app.core.paths import AppPaths


BUILD_ID = "2026-04-18_free_bundle_preflight_failures_sidecar_meta_v1"
SUPPORT_SNAPSHOT_VERSION = 1
SUPPORT_SNAPSHOT_TYPE = "support_snapshot"
SUPPORT_SNAPSHOT_DIRNAME = "support_snapshots"
SUPPORT_SNAPSHOT_SCREENSHOT_FILE = "screenshot.png"
SUPPORT_SNAPSHOT_META_FILE = "meta.json"
SUPPORT_SNAPSHOT_LOG_TAIL_FILE = "log_tail.txt"
SUPPORT_SNAPSHOT_RUNTIME_PREFLIGHT_FAILURES_FILE = "runtime_preflight_failures.json"
_KNOWN_APP_TIERS = ("free", "standard", "aggressive")
_RUNTIME_PREFLIGHT_FAILURE_PREFIX = "RUNTIME_PREFLIGHT_FAILURE:"
_RUNTIME_PREFLIGHT_FAILURE_COLLECT_STATUS_OK = "ok"
_RUNTIME_PREFLIGHT_FAILURE_COLLECT_STATUS_ERROR = "error"
_RUNTIME_PREFLIGHT_FAILURE_COLLECT_ERROR_MAX_LEN = 240


def support_snapshot_root_dir(paths: AppPaths) -> str:
    target = os.path.abspath(os.path.join(str(paths.runtime_dir or ""), SUPPORT_SNAPSHOT_DIRNAME))
    os.makedirs(target, exist_ok=True)
    return target


def create_support_snapshot_dir(paths: AppPaths, *, now_local: datetime | None = None) -> tuple[str, str, datetime, datetime]:
    local_dt = now_local.astimezone() if isinstance(now_local, datetime) else datetime.now().astimezone()
    utc_dt = local_dt.astimezone(timezone.utc)
    base_id = local_dt.strftime("%Y-%m-%d_%H%M%S")
    root_dir = support_snapshot_root_dir(paths)
    snapshot_id = base_id
    target_dir = os.path.join(root_dir, snapshot_id)
    suffix = 1
    while os.path.exists(target_dir):
        snapshot_id = f"{base_id}_{suffix:02d}"
        target_dir = os.path.join(root_dir, snapshot_id)
        suffix += 1
    os.makedirs(target_dir, exist_ok=True)
    return target_dir, snapshot_id, local_dt, utc_dt


def latest_support_snapshot_dir(paths: AppPaths) -> str:
    root_dir = support_snapshot_root_dir(paths)
    try:
        entries = [entry.path for entry in os.scandir(root_dir) if entry.is_dir()]
    except Exception:
        return ""
    if not entries:
        return ""
    try:
        return str(max(entries, key=lambda path: os.path.getmtime(path)))
    except Exception:
        return ""


def resolve_support_snapshot_open_dir(paths: AppPaths) -> str:
    latest_dir = latest_support_snapshot_dir(paths)
    return latest_dir or support_snapshot_root_dir(paths)


def infer_support_snapshot_app_info(display_name: str) -> tuple[str, str]:
    normalized = re.sub(r"\s+", " ", str(display_name or "").strip()) or "LoneWolf Fang"
    lower_name = normalized.lower()
    for tier in _KNOWN_APP_TIERS:
        suffix = f" {tier}"
        if lower_name.endswith(suffix):
            base_name = normalized[: -len(suffix)].strip() or normalized
            return base_name, tier
    return normalized, "unknown"


def _normalize_app_version(app_version: str) -> str:
    value = str(app_version or "").strip()
    if not value:
        return ""
    return value if value.lower().startswith("v") else f"v{value}"


def _normalize_platform_key() -> str:
    value = str(platform.system() or "").strip().lower()
    if value:
        return value
    return str(os.name or "").strip().lower() or "unknown"


def _normalize_os_label() -> str:
    system_name = str(platform.system() or "").strip()
    release_name = str(platform.release() or "").strip()
    if system_name and release_name:
        return f"{system_name} {release_name}"
    if system_name:
        return system_name
    return "unknown"


def _format_iso_local(dt: datetime) -> str:
    return dt.astimezone().replace(microsecond=0).isoformat()


def _format_iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _extract_runtime_preflight_token(text: str, key: str) -> str:
    match = re.search(rf"(?:^|\s){re.escape(str(key or ''))}=(?P<value>\S+)", str(text or ""))
    if not match:
        return ""
    return str(match.group("value") or "").strip()


def _extract_runtime_preflight_segment(text: str, start_marker: str, end_marker: str | None = None) -> str:
    source = str(text or "")
    start = source.find(str(start_marker or ""))
    if start < 0:
        return ""
    start += len(str(start_marker or ""))
    if end_marker is None:
        return str(source[start:] or "").strip()
    end = source.find(str(end_marker or ""), start)
    if end < 0:
        return ""
    return str(source[start:end] or "").strip()


def _normalize_runtime_preflight_failure_line(raw_line: str) -> str:
    compact_line = str(raw_line or "").strip()
    if compact_line.startswith("[stderr] "):
        compact_line = str(compact_line[len("[stderr] "):] or "").strip()
    return compact_line


def _parse_runtime_preflight_failure_line(raw_line: str) -> dict[str, Any] | None:
    compact_line = _normalize_runtime_preflight_failure_line(raw_line)
    if not compact_line.startswith(_RUNTIME_PREFLIGHT_FAILURE_PREFIX):
        return None

    record: dict[str, Any] = {
        "raw_line": compact_line,
    }
    symbol = _extract_runtime_preflight_token(compact_line, "symbol")
    tf_entry = _extract_runtime_preflight_token(compact_line, "tf_entry")
    tf_filter = _extract_runtime_preflight_token(compact_line, "tf_filter")
    from_ym = _extract_runtime_preflight_token(compact_line, "from")
    to_ym = _extract_runtime_preflight_token(compact_line, "to")
    missing_items = _extract_runtime_preflight_segment(compact_line, "missing_items=", " searched_paths=")
    searched_paths_text = _extract_runtime_preflight_segment(compact_line, "searched_paths=", " fallback_attempted=")
    fallback_attempted = _extract_runtime_preflight_segment(compact_line, "fallback_attempted=", None)

    if symbol:
        record["symbol"] = symbol
    if tf_entry:
        record["tf_entry"] = tf_entry
    if tf_filter:
        record["tf_filter"] = tf_filter
    if from_ym:
        record["from_ym"] = from_ym
    if to_ym:
        record["to_ym"] = to_ym
    if missing_items:
        record["missing_items"] = missing_items
    if searched_paths_text:
        record["searched_paths"] = [
            str(part or "").strip()
            for part in str(searched_paths_text or "").split(" | ")
            if str(part or "").strip() and str(part or "").strip() != "<none>"
        ]
    if fallback_attempted:
        record["fallback_attempted"] = fallback_attempted
    return record


def _copy_runtime_preflight_failure_record(record: dict[str, Any]) -> dict[str, Any]:
    copied: dict[str, Any] = {}
    for key, value in dict(record or {}).items():
        if isinstance(value, list):
            copied[key] = list(value)
        else:
            copied[key] = value
    return copied


def merge_runtime_preflight_failures(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: dict[str, dict[str, Any]] = {}
    for record in records:
        raw_line = str(dict(record or {}).get("raw_line", "") or "").strip()
        if not raw_line:
            continue
        existing = seen.get(raw_line)
        if existing is None:
            copied = _copy_runtime_preflight_failure_record(dict(record or {}))
            copied["raw_line"] = raw_line
            merged.append(copied)
            seen[raw_line] = copied
            continue
        for key, value in dict(record or {}).items():
            if key == "raw_line":
                continue
            if key not in existing or existing.get(key) in (None, "", []):
                existing[key] = list(value) if isinstance(value, list) else value
    return merged


def collect_runtime_preflight_failures_from_text(
    text: str,
    *,
    context: str,
    source_path: str = "",
) -> list[dict[str, Any]]:
    collected: list[dict[str, Any]] = []
    for raw_line in str(text or "").splitlines():
        record = _parse_runtime_preflight_failure_line(raw_line)
        if record is None:
            continue
        record["context"] = str(context or "").strip()
        if str(source_path or "").strip():
            record["source_path"] = os.path.abspath(str(source_path or "").strip())
        collected.append(record)
    return merge_runtime_preflight_failures(collected)


def collect_runtime_preflight_failures_from_file(path: str, *, context: str) -> list[dict[str, Any]]:
    target_path = os.path.abspath(str(path or "").strip())
    if not target_path or (not os.path.isfile(target_path)):
        return []
    with open(target_path, "r", encoding="utf-8", errors="replace") as handle:
        return collect_runtime_preflight_failures_from_text(
            handle.read(),
            context=context,
            source_path=target_path,
        )


def collect_runtime_preflight_failures(
    *,
    text_sources: Sequence[tuple[str, str]] = (),
    file_sources: Sequence[tuple[str, str]] = (),
) -> list[dict[str, Any]]:
    collected: list[dict[str, Any]] = []
    for context, text in tuple(text_sources or ()):
        collected.extend(
            collect_runtime_preflight_failures_from_text(
                text,
                context=str(context or "").strip(),
            )
        )
    for context, path in tuple(file_sources or ()):
        collected.extend(
            collect_runtime_preflight_failures_from_file(
                str(path or "").strip(),
                context=str(context or "").strip(),
            )
        )
    return merge_runtime_preflight_failures(collected)


def _normalize_runtime_preflight_failures_collect_status(status: str) -> str:
    return (
        _RUNTIME_PREFLIGHT_FAILURE_COLLECT_STATUS_ERROR
        if str(status or "").strip().lower() == _RUNTIME_PREFLIGHT_FAILURE_COLLECT_STATUS_ERROR
        else _RUNTIME_PREFLIGHT_FAILURE_COLLECT_STATUS_OK
    )


def _truncate_runtime_preflight_failures_collect_error(
    error: Any,
    *,
    max_len: int = _RUNTIME_PREFLIGHT_FAILURE_COLLECT_ERROR_MAX_LEN,
) -> str:
    text = str(error or "").strip()
    if (not text) and (error is not None):
        text = str(error.__class__.__name__ or "").strip()
    text = re.sub(r"\s+", " ", text)
    if max_len <= 0:
        return ""
    if len(text) <= max_len:
        return text
    if max_len <= 3:
        return text[:max_len]
    return f"{text[: max_len - 3].rstrip()}..."


def build_runtime_preflight_failure_collect_meta(
    *,
    collect_status: str = _RUNTIME_PREFLIGHT_FAILURE_COLLECT_STATUS_OK,
    collect_error: str = "",
) -> dict[str, str]:
    normalized_status = _normalize_runtime_preflight_failures_collect_status(collect_status)
    normalized_error = (
        _truncate_runtime_preflight_failures_collect_error(collect_error)
        if normalized_status == _RUNTIME_PREFLIGHT_FAILURE_COLLECT_STATUS_ERROR
        else ""
    )
    return {
        "runtime_preflight_failures_collect_status": normalized_status,
        "runtime_preflight_failures_collect_error": normalized_error,
    }


def collect_runtime_preflight_failures_artifact(
    *,
    text_sources: Sequence[tuple[str, str]] = (),
    file_sources: Sequence[tuple[str, str]] = (),
) -> dict[str, Any]:
    try:
        records = collect_runtime_preflight_failures(
            text_sources=text_sources,
            file_sources=file_sources,
        )
    except Exception as exc:
        return {
            "records": [],
            "collect_status": _RUNTIME_PREFLIGHT_FAILURE_COLLECT_STATUS_ERROR,
            "collect_error": _truncate_runtime_preflight_failures_collect_error(exc),
        }
    return {
        "records": records,
        "collect_status": _RUNTIME_PREFLIGHT_FAILURE_COLLECT_STATUS_OK,
        "collect_error": "",
    }


def serialize_runtime_preflight_failures_json(records: Sequence[dict[str, Any]] | None) -> str:
    payload = merge_runtime_preflight_failures(records or ())
    return json.dumps(payload, ensure_ascii=False, indent=2)


def build_runtime_preflight_failure_summary(records: Sequence[dict[str, Any]] | None) -> dict[str, Any]:
    failures = list(records or [])
    latest = failures[-1] if failures else {}
    return {
        "runtime_preflight_failure_count": len(failures),
        "runtime_preflight_failure_latest_symbol": str(latest.get("symbol", "") or "").strip(),
        "runtime_preflight_failure_latest_from": str(latest.get("from_ym", "") or "").strip(),
        "runtime_preflight_failure_latest_to": str(latest.get("to_ym", "") or "").strip(),
    }


def build_runtime_preflight_failures_bundle_meta(
    records: Sequence[dict[str, Any]] | None,
    *,
    collect_status: str = _RUNTIME_PREFLIGHT_FAILURE_COLLECT_STATUS_OK,
    collect_error: str = "",
) -> dict[str, Any]:
    meta = build_runtime_preflight_failure_summary(records)
    meta.update(
        build_runtime_preflight_failure_collect_meta(
            collect_status=collect_status,
            collect_error=collect_error,
        )
    )
    return meta


def serialize_runtime_preflight_failures_bundle_meta_json(
    records: Sequence[dict[str, Any]] | None,
    *,
    collect_status: str = _RUNTIME_PREFLIGHT_FAILURE_COLLECT_STATUS_OK,
    collect_error: str = "",
) -> str:
    payload = build_runtime_preflight_failures_bundle_meta(
        records,
        collect_status=collect_status,
        collect_error=collect_error,
    )
    return json.dumps(payload, ensure_ascii=False, indent=2)


def build_support_snapshot_meta(
    *,
    snapshot_id: str,
    timestamp_local: datetime,
    timestamp_utc: datetime,
    app_display_name: str,
    app_version: str,
    build_id: str,
    mode: str,
    exchange: str,
    symbol: str,
    preset: str,
    language: str,
    qt_version: str,
    window_title: str,
    window_width: int,
    window_height: int,
    runtime_preflight_failures: Sequence[dict[str, Any]] | None = None,
    runtime_preflight_failures_collect_status: str = _RUNTIME_PREFLIGHT_FAILURE_COLLECT_STATUS_OK,
    runtime_preflight_failures_collect_error: str = "",
) -> dict[str, Any]:
    app_name, app_tier = infer_support_snapshot_app_info(app_display_name)
    meta = {
        "snapshot_version": SUPPORT_SNAPSHOT_VERSION,
        "snapshot_type": SUPPORT_SNAPSHOT_TYPE,
        "snapshot_id": str(snapshot_id or "").strip(),
        "timestamp_local": _format_iso_local(timestamp_local),
        "timestamp_utc": _format_iso_utc(timestamp_utc),
        "app_name": str(app_name or "").strip(),
        "app_tier": str(app_tier or "").strip(),
        "app_version": _normalize_app_version(app_version),
        "build_id": str(build_id or "").strip(),
        "mode": str(mode or "").strip(),
        "exchange": str(exchange or "").strip(),
        "symbol": str(symbol or "").strip(),
        "preset": str(preset or "").strip(),
        "language": str(language or "").strip(),
        "platform": _normalize_platform_key(),
        "os": _normalize_os_label(),
        "python_version": str(platform.python_version() or "").strip(),
        "qt_version": str(qt_version or "").strip(),
        "window_title": str(window_title or "").strip(),
        "window_width": int(window_width or 0),
        "window_height": int(window_height or 0),
        "files": {
            "screenshot": SUPPORT_SNAPSHOT_SCREENSHOT_FILE,
            "log_tail": SUPPORT_SNAPSHOT_LOG_TAIL_FILE,
            "runtime_preflight_failures": SUPPORT_SNAPSHOT_RUNTIME_PREFLIGHT_FAILURES_FILE,
        },
    }
    meta.update(build_runtime_preflight_failure_summary(runtime_preflight_failures))
    meta.update(
        build_runtime_preflight_failure_collect_meta(
            collect_status=runtime_preflight_failures_collect_status,
            collect_error=runtime_preflight_failures_collect_error,
        )
    )
    return meta


def write_support_snapshot_meta(snapshot_dir: str, meta: dict[str, Any]) -> str:
    target_path = os.path.join(str(snapshot_dir or "").strip(), SUPPORT_SNAPSHOT_META_FILE)
    with open(target_path, "w", encoding="utf-8") as handle:
        json.dump(meta, handle, ensure_ascii=False, indent=2)
    return target_path


def write_support_snapshot_log_tail(snapshot_dir: str, text: str, *, max_lines: int = 200) -> str:
    lines = str(text or "").splitlines()
    tail_text = "\n".join(lines[-max(1, int(max_lines or 200)) :]).strip()
    if tail_text:
        tail_text += "\n"
    target_path = os.path.join(str(snapshot_dir or "").strip(), SUPPORT_SNAPSHOT_LOG_TAIL_FILE)
    with open(target_path, "w", encoding="utf-8") as handle:
        handle.write(tail_text)
    return target_path


def write_support_snapshot_runtime_preflight_failures(
    snapshot_dir: str,
    records: Sequence[dict[str, Any]] | None,
) -> str:
    target_path = os.path.join(str(snapshot_dir or "").strip(), SUPPORT_SNAPSHOT_RUNTIME_PREFLIGHT_FAILURES_FILE)
    with open(target_path, "w", encoding="utf-8") as handle:
        handle.write(serialize_runtime_preflight_failures_json(records))
    return target_path
