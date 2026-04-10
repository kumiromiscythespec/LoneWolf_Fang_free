# BUILD_ID: 2026-04-09_free_support_snapshot_v1
from __future__ import annotations

import json
import os
import platform
import re
from datetime import datetime, timezone
from typing import Any

from app.core.paths import AppPaths


BUILD_ID = "2026-04-09_free_support_snapshot_v1"
SUPPORT_SNAPSHOT_VERSION = 1
SUPPORT_SNAPSHOT_TYPE = "support_snapshot"
SUPPORT_SNAPSHOT_DIRNAME = "support_snapshots"
SUPPORT_SNAPSHOT_SCREENSHOT_FILE = "screenshot.png"
SUPPORT_SNAPSHOT_META_FILE = "meta.json"
SUPPORT_SNAPSHOT_LOG_TAIL_FILE = "log_tail.txt"
_KNOWN_APP_TIERS = ("free", "standard", "aggressive")


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
) -> dict[str, Any]:
    app_name, app_tier = infer_support_snapshot_app_info(app_display_name)
    return {
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
        },
    }


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
