# BUILD_ID: 2026-03-29_free_from_standard_nonlive_build_v1
# BUILD_ID: 2026-03-27_settings_collapsible_sections_v1
# BUILD_ID: 2026-03-27_settings_live_chart_v2_0
# BUILD_ID: 2026-03-27_settings_window_size_v1_2
# BUILD_ID: 2026-03-27_settings_chart_panel_v1_1
# BUILD_ID: 2026-03-12_gui_log_level_settings_v1
from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from typing import Any, Dict

from app.core.paths import ensure_runtime_dirs


BUILD_ID = "2026-03-27_settings_collapsible_sections_v1"


_CHART_MODES = {"Equity", "Net", "Max DD", "Trades", "Combined", "Candle"}
_DEFAULT_CHART_MODE = "Equity"
_DEFAULT_MAIN_WINDOW_WIDTH = 840
_DEFAULT_MAIN_WINDOW_HEIGHT = 620
_MIN_MAIN_WINDOW_WIDTH = 840
_MIN_MAIN_WINDOW_HEIGHT = 620


def _normalize_runtime_log_level(raw: str) -> str:
    value = str(raw or "").strip().upper()
    if value in ("MINIMAL", "OPS", "DEBUG"):
        return value
    return "OPS"


def _normalize_chart_mode(raw: str) -> str:
    value = str(raw or "").strip()
    if value in _CHART_MODES:
        return value
    return _DEFAULT_CHART_MODE


def _normalize_chart_splitter_sizes(raw: Any) -> list[int]:
    if not isinstance(raw, (list, tuple)):
        return []
    out: list[int] = []
    for item in list(raw)[:2]:
        try:
            value = int(item)
        except Exception:
            return []
        if value <= 0:
            return []
        out.append(value)
    return out if len(out) == 2 else []


def _normalize_window_dimension(raw: Any, *, default: int, minimum: int) -> int:
    try:
        value = int(raw)
    except Exception:
        value = int(default)
    if value < int(minimum):
        return int(default)
    return int(value)


def _normalize_bool(raw: Any, *, default: bool = False) -> bool:
    if isinstance(raw, bool):
        return raw
    if raw is None:
        return bool(default)
    if isinstance(raw, (int, float)):
        return bool(raw)
    value = str(raw).strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off", ""}:
        return False
    return bool(default)


@dataclass
class AppSettings:
    # Secrets are NOT stored here (keyring only).
    preset: str = "OFF"  # OFF / SELL_SAFE
    symbol: str = "BTC/JPY"
    report_enabled: bool = False
    report_out: str = ""  # empty -> default exports/report.json (runner/backtest side)
    exchange_id: str = "coincheck"
    dataset_root: str = ""
    dataset_prefix: str = ""
    dataset_year: int = 0
    log_level: str = ""
    last_chart_mode: str = ""
    chart_splitter_sizes: list[int] = field(default_factory=list)
    main_window_width: int = _DEFAULT_MAIN_WINDOW_WIDTH
    main_window_height: int = _DEFAULT_MAIN_WINDOW_HEIGHT
    gui_section_api_expanded: bool = False
    gui_section_activation_expanded: bool = False
    gui_section_diagnostics_expanded: bool = False


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
        log_level_raw = str(raw.get("log_level", s.log_level) or "").strip()
        s.log_level = _normalize_runtime_log_level(log_level_raw) if log_level_raw else ""
        chart_mode_raw = str(raw.get("last_chart_mode", s.last_chart_mode) or "").strip()
        s.last_chart_mode = _normalize_chart_mode(chart_mode_raw) if chart_mode_raw else ""
        s.chart_splitter_sizes = _normalize_chart_splitter_sizes(raw.get("chart_splitter_sizes", s.chart_splitter_sizes))
        s.main_window_width = _normalize_window_dimension(
            raw.get("main_window_width", s.main_window_width),
            default=_DEFAULT_MAIN_WINDOW_WIDTH,
            minimum=_MIN_MAIN_WINDOW_WIDTH,
        )
        s.main_window_height = _normalize_window_dimension(
            raw.get("main_window_height", s.main_window_height),
            default=_DEFAULT_MAIN_WINDOW_HEIGHT,
            minimum=_MIN_MAIN_WINDOW_HEIGHT,
        )
        s.gui_section_api_expanded = _normalize_bool(
            raw.get("gui_section_api_expanded", s.gui_section_api_expanded),
            default=False,
        )
        s.gui_section_activation_expanded = _normalize_bool(
            raw.get("gui_section_activation_expanded", s.gui_section_activation_expanded),
            default=False,
        )
        s.gui_section_diagnostics_expanded = _normalize_bool(
            raw.get("gui_section_diagnostics_expanded", s.gui_section_diagnostics_expanded),
            default=False,
        )
        return s
    except Exception:
        return AppSettings()


def save_settings(s: AppSettings) -> None:
    p = ensure_runtime_dirs()
    data: Dict[str, Any] = asdict(s)
    log_level_raw = str(data.get("log_level", "") or "").strip()
    data["log_level"] = _normalize_runtime_log_level(log_level_raw) if log_level_raw else ""
    chart_mode_raw = str(data.get("last_chart_mode", "") or "").strip()
    data["last_chart_mode"] = _normalize_chart_mode(chart_mode_raw) if chart_mode_raw else ""
    data["chart_splitter_sizes"] = _normalize_chart_splitter_sizes(data.get("chart_splitter_sizes"))
    data["main_window_width"] = _normalize_window_dimension(
        data.get("main_window_width"),
        default=_DEFAULT_MAIN_WINDOW_WIDTH,
        minimum=_MIN_MAIN_WINDOW_WIDTH,
    )
    data["main_window_height"] = _normalize_window_dimension(
        data.get("main_window_height"),
        default=_DEFAULT_MAIN_WINDOW_HEIGHT,
        minimum=_MIN_MAIN_WINDOW_HEIGHT,
    )
    data["gui_section_api_expanded"] = _normalize_bool(data.get("gui_section_api_expanded"), default=False)
    data["gui_section_activation_expanded"] = _normalize_bool(data.get("gui_section_activation_expanded"), default=False)
    data["gui_section_diagnostics_expanded"] = _normalize_bool(data.get("gui_section_diagnostics_expanded"), default=False)
    with open(p.settings_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_gui_chart_state(settings: AppSettings | None = None) -> Dict[str, Any]:
    target = settings if isinstance(settings, AppSettings) else AppSettings()
    mode_raw = str(getattr(target, "last_chart_mode", "") or "").strip()
    return {
        "last_chart_mode": _normalize_chart_mode(mode_raw) if mode_raw else _DEFAULT_CHART_MODE,
        "chart_splitter_sizes": _normalize_chart_splitter_sizes(getattr(target, "chart_splitter_sizes", [])),
    }


def set_gui_chart_state(
    settings: AppSettings,
    *,
    last_chart_mode: str | None = None,
    chart_splitter_sizes: list[int] | tuple[int, int] | None = None,
) -> AppSettings:
    target = settings if isinstance(settings, AppSettings) else AppSettings()
    if last_chart_mode is not None:
        target.last_chart_mode = _normalize_chart_mode(last_chart_mode)
    if chart_splitter_sizes is not None:
        target.chart_splitter_sizes = _normalize_chart_splitter_sizes(chart_splitter_sizes)
    return target
