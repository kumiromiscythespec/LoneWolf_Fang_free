# BUILD_ID: 2026-04-18_free_source_provenance_shared_root_v1
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

from app.core.paths import get_paths


BUILD_ID = "2026-04-18_free_source_provenance_shared_root_v1"

_SOURCE_EVENT_ONCE: set[str] = set()
_CANONICAL_SOURCES = {
    "canonical",
    "canonical_chart_cache",
    "canonical_market_data",
    "canonical_shared_root",
}
_LEGACY_SOURCES = {
    "legacy_env_root",
    "legacy_exports_root",
    "legacy_fallback",
    "legacy_product_root",
    "legacy_repo_root",
}


def _normalized_dir(path_value: str) -> str:
    raw = str(path_value or "").strip()
    if not raw:
        return ""
    return os.path.abspath(raw)


def iter_source_root_candidates(
    explicit_root: str,
    canonical_root: str,
    repo_root: str,
    *,
    include_explicit_parent: bool = False,
    include_cwd: bool = True,
) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: set[str] = set()

    def _add(source: str, path_value: str) -> None:
        root_abs = _normalized_dir(path_value)
        if not root_abs:
            return
        root_key = os.path.normcase(root_abs)
        if root_key in seen:
            return
        seen.add(root_key)
        out.append((str(source), root_abs))

    explicit_abs = _normalized_dir(explicit_root)
    _add("explicit_src_dir", explicit_abs)
    if include_explicit_parent and explicit_abs:
        _add("explicit_src_parent", os.path.abspath(os.path.join(explicit_abs, os.pardir)))
    _add("canonical_market_data", canonical_root)

    try:
        for source, root in tuple(getattr(get_paths(), "market_data_candidates", ()) or ()):
            label = "canonical_market_data" if str(source or "").strip() == "canonical_shared_root" else str(source or "")
            _add(label, str(root or ""))
    except Exception:
        pass

    _add("legacy_repo_root", repo_root)
    if include_cwd:
        _add("cwd_fallback", os.getcwd())
    return out


def path_source_events_path() -> str:
    exports_dir = os.path.abspath(str(get_paths().exports_dir or ""))
    return os.path.abspath(os.path.join(exports_dir, "path_source_events.jsonl"))


def is_legacy_source(source: str) -> bool:
    return str(source or "").strip() in _LEGACY_SOURCES


def is_noncanonical_source(source: str) -> bool:
    source_text = str(source or "").strip()
    return bool(source_text) and source_text not in _CANONICAL_SOURCES


def record_path_source_event_once(
    *,
    component: str,
    event: str,
    source: str,
    path: str,
    extra: dict | None = None,
) -> None:
    try:
        payload_extra = dict(extra or {})
        event_build_id = str(payload_extra.pop("build_id", BUILD_ID) or BUILD_ID)
        payload: dict[str, Any] = {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "build_id": event_build_id,
            "component": str(component or "").strip(),
            "event": str(event or "").strip(),
            "source": str(source or "").strip(),
            "path": os.path.abspath(str(path or "").strip() or os.getcwd()),
        }
        for key, value in payload_extra.items():
            payload[str(key)] = value

        dedupe_payload = {key: value for key, value in payload.items() if key != "ts_utc"}
        once_key = json.dumps(dedupe_payload, ensure_ascii=True, sort_keys=True, default=str)
        if once_key in _SOURCE_EVENT_ONCE:
            return

        out_path = path_source_events_path()
        out_dir = os.path.dirname(out_path)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
        with open(out_path, "a", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, ensure_ascii=True, sort_keys=True, default=str)
            handle.write("\n")
        _SOURCE_EVENT_ONCE.add(once_key)
    except Exception:
        return
