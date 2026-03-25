# BUILD_ID: 2026-03-25_free_settings_normalization_v1
from __future__ import annotations

import json
import os
from dataclasses import dataclass, asdict
from typing import Any, Dict

from app.core.paths import ensure_runtime_dirs
from app.core.tier import get_build_tier


BUILD_ID = "2026-03-25_free_settings_normalization_v1"
_FREE_RUN_MODES = ("PAPER", "REPLAY", "BACKTEST")


def _is_free_build() -> bool:
    return get_build_tier() == "RESEARCH"


def _normalize_run_mode(raw: str) -> str:
    value = str(raw or "").strip().upper()
    if _is_free_build():
        return value if value in _FREE_RUN_MODES else "PAPER"
    if value in ("LIVE", "PAPER", "REPLAY", "BACKTEST"):
        return value
    return "LIVE"


def _normalize_runtime_log_level(raw: str) -> str:
    if _is_free_build():
        return "MINIMAL"
    value = str(raw or "").strip().upper()
    if value in ("MINIMAL", "OPS", "DEBUG"):
        return value
    return "OPS"


@dataclass
class AppSettings:
    # Secrets are NOT stored here (keyring only).
    preset: str = "OFF"  # OFF / SELL_SAFE
    symbol: str = "BTC/JPY"
    run_mode: str = "PAPER"
    report_enabled: bool = False
    report_out: str = ""  # empty -> default exports/report.json (runner/backtest side)
    exchange_id: str = "coincheck"
    dataset_root: str = ""
    dataset_prefix: str = ""
    dataset_year: int = 0
    log_level: str = ""


def load_settings() -> AppSettings:
    p = ensure_runtime_dirs()
    if not os.path.exists(p.settings_path):
        return AppSettings()
    try:
        with open(p.settings_path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if not isinstance(raw, dict):
            return AppSettings()
        s = AppSettings()
        s.preset = str(raw.get("preset", s.preset))
        s.symbol = str(raw.get("symbol", s.symbol))
        s.run_mode = _normalize_run_mode(raw.get("run_mode", s.run_mode))
        s.report_enabled = bool(raw.get("report_enabled", s.report_enabled))
        s.report_out = str(raw.get("report_out", s.report_out))
        ex = str(raw.get("exchange_id", s.exchange_id) or s.exchange_id).strip().lower()
        if ex in ("coincheck", "mexc", "binance"):
            s.exchange_id = ex
        else:
            s.exchange_id = "coincheck"
        s.dataset_root = str(raw.get("dataset_root", s.dataset_root))
        s.dataset_prefix = str(raw.get("dataset_prefix", s.dataset_prefix))
        try:
            s.dataset_year = int(raw.get("dataset_year", s.dataset_year) or 0)
        except Exception:
            s.dataset_year = 0
        s.log_level = _normalize_runtime_log_level(raw.get("log_level", s.log_level))
        return s
    except Exception:
        return AppSettings()


def save_settings(s: AppSettings) -> None:
    p = ensure_runtime_dirs()
    data: Dict[str, Any] = asdict(s)
    data["run_mode"] = _normalize_run_mode(data.get("run_mode", "PAPER"))
    data["log_level"] = _normalize_runtime_log_level(data.get("log_level", ""))
    with open(p.settings_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
