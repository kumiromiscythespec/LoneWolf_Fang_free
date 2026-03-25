# BUILD_ID: 2026-03-05_exports_runid_symbol_isolation_v1
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


_LEGACY_EXPORT_DIR_TOKENS = {
    "exports",
    "runtime/exports",
}


def normalize_symbol_token(symbol: str | None) -> str:
    raw = str(symbol or "").strip().upper()
    token = "".join(ch for ch in raw if ch.isalnum())
    return token or "SYMBOL"


def generate_run_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    return f"{stamp}_{os.getpid()}"


def resolve_run_id(explicit: str | None = None, *, env_key: str = "LWF_RUN_ID") -> str:
    raw = str(explicit or "").strip()
    if not raw:
        raw = str(os.getenv(env_key, "") or "").strip()
    if not raw:
        raw = generate_run_id()
    safe = "".join(ch if (ch.isalnum() or ch in ("-", "_")) else "_" for ch in raw)
    return safe or generate_run_id()


def is_legacy_exports_path(path_value: str | None) -> bool:
    raw = str(path_value or "").strip().replace("\\", "/").strip("/")
    raw_l = raw.lower()
    if not raw_l:
        return True
    if raw_l in _LEGACY_EXPORT_DIR_TOKENS:
        return True
    if raw_l.startswith("exports/"):
        return True
    if raw_l.startswith("runtime/exports/"):
        return True
    return False


def build_run_export_dir(root_dir: str | Path, *, run_id: str, symbol: str | None) -> Path:
    root = Path(root_dir)
    path = root / "runs" / resolve_run_id(run_id) / normalize_symbol_token(symbol)
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_last_run_json(
    root_dir: str | Path,
    *,
    run_id: str,
    symbol: str,
    mode: str,
    export_dir: str | Path,
    replay_report: str = "",
    trade_log: str = "",
    extra: dict[str, Any] | None = None,
) -> Path:
    root = Path(root_dir)
    root.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {
        "run_id": str(run_id),
        "symbol": str(symbol),
        "symbolnorm": normalize_symbol_token(symbol),
        "mode": str(mode),
        "export_dir": str(export_dir),
        "replay_report": str(replay_report or ""),
        "trade_log": str(trade_log or ""),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    if isinstance(extra, dict):
        payload.update(extra)
    out_path = root / "last_run.json"
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path
