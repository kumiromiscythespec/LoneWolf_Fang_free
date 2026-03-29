# BUILD_ID: 2026-03-29_free_final_polish_v1
# BUILD_ID: 2026-03-29_free_port_standard_gui_nonlive_improvements_v1
# BUILD_ID: 2026-03-29_free_from_standard_nonlive_build_v1
# BUILD_ID: 2026-03-27_result_chart_snapshot_folder_align_v1
# BUILD_ID: 2026-03-27_result_chart_candle_visual_finish_v2_1_1
# BUILD_ID: 2026-03-27_result_chart_candle_visual_improve_v2_1_0
# BUILD_ID: 2026-03-27_result_chart_state_absent_fix_v2_0_2
# BUILD_ID: 2026-03-27_result_chart_candle_mode_v2_0
# BUILD_ID: 2026-03-27_result_chart_combo_popup_dark_v1_2_1
# BUILD_ID: 2026-03-27_result_chart_axis_scroll_v1_2
# BUILD_ID: 2026-03-27_result_chart_widget_v1_1
# BUILD_ID: 2026-03-27_result_chart_widget_v1
from __future__ import annotations

from bisect import bisect_left
from copy import deepcopy
import csv
import glob
import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from PySide6.QtCore import QPointF, QRectF, QSize, Qt, Signal
from PySide6.QtGui import QColor, QFontMetricsF, QImage, QPainter, QPainterPath, QPalette, QPen
from PySide6.QtWidgets import (
    QComboBox,
    QFrame,
    QHBoxLayout,
    QLabel,
    QLayout,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QVBoxLayout,
    QWidget,
)

from app.core.chart_state_path import build_chart_state_path, sanitize_symbol_for_chart_state


BUILD_ID = "2026-03-29_free_final_polish_v1"

CHART_MODE_EQUITY = "Equity"
CHART_MODE_NET = "Net"
CHART_MODE_MAX_DD = "Max DD"
CHART_MODE_TRADES = "Trades"
CHART_MODE_COMBINED = "Combined"
CHART_MODE_CANDLE = "Candle"
CHART_MODES = (
    CHART_MODE_EQUITY,
    CHART_MODE_NET,
    CHART_MODE_MAX_DD,
    CHART_MODE_TRADES,
    CHART_MODE_COMBINED,
    CHART_MODE_CANDLE,
)
_CHART_MODE_TEXT_KEYS = {
    CHART_MODE_EQUITY: "chart.mode.equity",
    CHART_MODE_NET: "chart.mode.net",
    CHART_MODE_MAX_DD: "chart.mode.max_dd",
    CHART_MODE_TRADES: "chart.mode.trades",
    CHART_MODE_COMBINED: "chart.mode.combined",
    CHART_MODE_CANDLE: "chart.mode.candle",
}
_RESULT_EMPTY_MESSAGE_TEXT_KEYS = {
    "No result data": "result.empty.no_result_data",
    "No result data yet": "result.empty.no_result_data_yet",
    "No chart-ready data": "result.empty.no_chart_ready_data",
    "live/paper chart state parse failed": "result.empty.live_parse_failed",
    "paper chart state parse failed": "result.empty.live_parse_failed",
    "live/paper chart state present but candles are not ready": "result.empty.live_waiting_ready",
    "paper chart state present but candles are not ready": "result.empty.live_waiting_ready",
    "Waiting for live/paper candles...": "result.empty.live_waiting",
    "Waiting for paper candles...": "result.empty.live_waiting",
}

_COLOR_BG = QColor("#050608")
_COLOR_PANEL = QColor("#0c0f12")
_COLOR_GRID = QColor("#1d2227")
_COLOR_BORDER = QColor("#2a3037")
_COLOR_TEXT = QColor("#d7dde3")
_COLOR_MUTED = QColor("#7f8994")
_COLOR_EQUITY = QColor("#f2f5f7")
_COLOR_NET = QColor("#33d17a")
_COLOR_NET_NEGATIVE = QColor("#ff5c5c")
_COLOR_DD = QColor("#ff5c5c")
_COLOR_TRADES = QColor("#4aa3ff")
_COLOR_CANDLE_UP = QColor("#2dd4bf")
_COLOR_CANDLE_DOWN = QColor("#ff6b6b")
_COLOR_PRICE_LINE = QColor("#7dd3fc")
_COLOR_STOP = QColor("#ff7b72")
_COLOR_TP = QColor("#67e8f9")
_COLOR_ENTRY = QColor("#34d399")
_COLOR_EXIT = QColor("#f87171")
_COLOR_POSITION_GAIN = QColor("#22c55e")
_COLOR_POSITION_LOSS = QColor("#ef4444")
_COLOR_LABEL_BG = QColor("#0f1419")
_CHART_MIN_WIDTH = 380
_CHART_MIN_HEIGHT = 240
_Y_AXIS_TICK_COUNT = 5
_Y_AXIS_GUTTER = 8.0
_Y_AXIS_MIN_WIDTH = 56.0
_Y_AXIS_MAX_WIDTH_RATIO = 0.28
_CANDLE_MAX_BARS = 240
_CANDLE_X_AXIS_HEIGHT = 26.0
_CANDLE_RIGHT_PAD_RATIO = 0.055
_CANDLE_LEFT_PAD_RATIO = 0.012
_CANDLE_MIN_TICK_PX = 110.0
_CANDLE_TICK_STEPS_MS = (
    60_000,
    2 * 60_000,
    5 * 60_000,
    10 * 60_000,
    15 * 60_000,
    30 * 60_000,
    60 * 60 * 1000,
    2 * 60 * 60 * 1000,
    4 * 60 * 60 * 1000,
    6 * 60 * 60 * 1000,
    12 * 60 * 60 * 1000,
    24 * 60 * 60 * 1000,
    2 * 24 * 60 * 60 * 1000,
    7 * 24 * 60 * 60 * 1000,
)
_COLOR_COMBO_HIGHLIGHT = QColor("#1b2430")
_DARK_COMBO_POPUP_STYLESHEET = """
QComboBox {
    color: #d7dde3;
    background-color: #0c0f12;
    border: 1px solid #2a3037;
    border-radius: 4px;
    padding: 4px 28px 4px 8px;
}
QComboBox::drop-down {
    border: none;
    background: transparent;
    width: 24px;
}
QComboBox QAbstractItemView {
    background-color: #0c0f12;
    color: #d7dde3;
    border: 1px solid #2a3037;
    outline: none;
    selection-background-color: #1b2430;
    selection-color: #f2f5f7;
}
QComboBox QAbstractItemView::item {
    min-height: 22px;
    padding: 4px 8px;
    background-color: #0c0f12;
    color: #d7dde3;
}
QComboBox QAbstractItemView::item:selected {
    background-color: #1b2430;
    color: #f2f5f7;
}
QComboBox QAbstractItemView::item:hover {
    background-color: #1b2430;
    color: #f2f5f7;
}
"""


@dataclass
class ResultChartData:
    export_dir: str = ""
    report_path: str = ""
    equity_path: str = ""
    trades_path: str = ""
    chart_state_path: str = ""
    source_name: str = ""
    symbol: str = ""
    first_label: str = ""
    last_label: str = ""
    net_total: float | None = None
    initial_equity: float | None = None
    net_return_pct: float | None = None
    max_dd: float | None = None
    trades_count: int | None = None
    equity_points: list[float] = field(default_factory=list)
    net_points: list[float] = field(default_factory=list)
    drawdown_points: list[float] = field(default_factory=list)
    trade_points: list[float] = field(default_factory=list)
    live_chart_state: LiveChartState | None = None
    empty_message: str = "No result data"

    @property
    def has_any_result(self) -> bool:
        return bool(
            self.report_path
            or self.equity_path
            or self.trades_path
            or self.equity_points
            or self.net_points
            or self.drawdown_points
            or self.trade_points
            or self.live_chart_state is not None
        )

    @property
    def has_chart_data(self) -> bool:
        return bool(
            self.equity_points
            or self.net_points
            or self.drawdown_points
            or self.trade_points
            or (self.live_chart_state is not None and self.live_chart_state.has_data)
        )


@dataclass
class ChartCandle:
    ts_ms: int = 0
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0


@dataclass
class ChartMarker:
    ts_ms: int = 0
    price: float = 0.0
    side: str = ""
    label: str = ""


@dataclass
class ChartPositionState:
    is_open: bool = False
    side: str = ""
    entry_price: float | None = None
    qty: float | None = None
    stop_price: float | None = None
    tp_price: float | None = None
    unrealized_pnl: float | None = None
    opened_ts_ms: int | None = None


@dataclass
class LiveChartState:
    path: str = ""
    schema_version: str = ""
    updated_at: str = ""
    exchange_id: str = ""
    run_mode: str = ""
    symbol: str = ""
    timeframe: str = ""
    candles: list[ChartCandle] = field(default_factory=list)
    current_price: float | None = None
    candles_count: int = 0
    candle_source: str = ""
    last_candle_ts_ms: int | None = None
    chart_state_reason: str = ""
    state_present: bool = False
    parse_error: bool = False
    position: ChartPositionState = field(default_factory=ChartPositionState)
    entry_markers: list[ChartMarker] = field(default_factory=list)
    exit_markers: list[ChartMarker] = field(default_factory=list)
    preferred_mode: str = CHART_MODE_CANDLE

    @property
    def has_data(self) -> bool:
        return bool(self.candles)


@dataclass
class _ChartSeries:
    label: str
    values: list[float]
    color: QColor
    fill: bool = False


@dataclass
class _ChartViewState:
    x_margin_ratio: float = 0.015
    y_padding_ratio: float = 0.08


@dataclass
class _PriceTagRequest:
    y: float
    text: str
    color: QColor
    priority: int = 0
    emphasized: bool = False
    allow_hide: bool = False


def normalize_chart_mode(mode: str) -> str:
    text = str(mode or "").strip()
    return text if text in CHART_MODES else CHART_MODE_EQUITY


def _lookup_ui_text(ui_texts: dict[str, str] | None, key: str, default: str) -> str:
    if isinstance(ui_texts, dict):
        value = str(ui_texts.get(key, "") or "")
        if value:
            return value
    return str(default or "")


def _display_chart_mode_text(mode: str, ui_texts: dict[str, str] | None = None) -> str:
    normalized = normalize_chart_mode(mode)
    key = _CHART_MODE_TEXT_KEYS.get(normalized, "")
    return _lookup_ui_text(ui_texts, key, normalized) if key else normalized


def _translate_result_message(
    message: str,
    ui_texts: dict[str, str] | None = None,
    *,
    default_key: str = "",
    default_text: str = "",
) -> str:
    text = str(message or "").strip()
    key = _RESULT_EMPTY_MESSAGE_TEXT_KEYS.get(text, "")
    if key:
        return _lookup_ui_text(ui_texts, key, text)
    if text:
        return text
    if default_key:
        return _lookup_ui_text(ui_texts, default_key, default_text)
    return str(default_text or "")


def _abs_path(path: str) -> str:
    return os.path.abspath(str(path or ""))


def _unique_paths(paths: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for path in paths:
        value = _abs_path(path)
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _safe_float(value: Any) -> float | None:
    try:
        out = float(value)
    except Exception:
        return None
    if out != out:
        return None
    return out


def _safe_int(value: Any) -> int | None:
    try:
        return int(float(value))
    except Exception:
        return None


def _safe_ts_ms(value: Any) -> int | None:
    out = _safe_int(value)
    if out is None:
        return None
    ts_ms = int(out)
    if 0 < ts_ms < 100_000_000_000:
        ts_ms *= 1000
    elif ts_ms >= 100_000_000_000_000:
        ts_ms //= 1000
    return ts_ms if ts_ms > 0 else None


def _sanitize_chart_state_symbol(symbol: str) -> str:
    return sanitize_symbol_for_chart_state(symbol)


def sanitize_snapshot_symbol(symbol: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "", str(symbol or "").strip().lower())
    return text or "unknown"


def infer_result_chart_snapshot_symbol(data: ResultChartData | None) -> str:
    if not isinstance(data, ResultChartData):
        return "unknown"
    candidates: list[str] = []
    state = data.live_chart_state if isinstance(data.live_chart_state, LiveChartState) else None
    if state is not None:
        candidates.append(str(state.symbol or ""))
    candidates.append(str(data.symbol or ""))
    source_name = str(data.source_name or "").strip()
    if source_name:
        if " / " in source_name:
            source_name = source_name.rsplit(" / ", 1)[0].strip()
        candidates.append(source_name)
    for candidate in candidates:
        safe = sanitize_snapshot_symbol(candidate)
        if safe != "unknown":
            return safe
    return "unknown"


def _chart_state_filename(exchange_id: str, run_mode: str, symbol: str) -> str:
    exchange = "".join(ch for ch in str(exchange_id or "").strip().lower() if ch.isalnum()) or "exchange"
    mode = "".join(ch for ch in str(run_mode or "").strip().lower() if ch.isalnum()) or "paper"
    return f"chart_state_{exchange}_{mode}_{_sanitize_chart_state_symbol(symbol)}.json"


def build_live_chart_state_path(state_dir: str, exchange_id: str, run_mode: str, symbol: str) -> str:
    return build_chart_state_path(state_dir, exchange_id, run_mode, symbol)


def _load_live_chart_candles(raw: Any) -> list[ChartCandle]:
    if not isinstance(raw, list):
        return []
    out: list[ChartCandle] = []
    for item in raw[-_CANDLE_MAX_BARS:]:
        if not isinstance(item, dict):
            continue
        ts_ms = _safe_ts_ms(item.get("ts_ms"))
        open_px = _safe_float(item.get("open"))
        high_px = _safe_float(item.get("high"))
        low_px = _safe_float(item.get("low"))
        close_px = _safe_float(item.get("close"))
        if ts_ms is None or open_px is None or high_px is None or low_px is None or close_px is None:
            continue
        hi = max(float(open_px), float(high_px), float(low_px), float(close_px))
        lo = min(float(open_px), float(high_px), float(low_px), float(close_px))
        out.append(
            ChartCandle(
                ts_ms=int(ts_ms),
                open=float(open_px),
                high=float(hi),
                low=float(lo),
                close=float(close_px),
            )
        )
    return out


def _load_live_chart_markers(raw: Any) -> list[ChartMarker]:
    if not isinstance(raw, list):
        return []
    out: list[ChartMarker] = []
    for item in raw[-16:]:
        if not isinstance(item, dict):
            continue
        ts_ms = _safe_ts_ms(item.get("ts_ms"))
        price = _safe_float(item.get("price"))
        if ts_ms is None or price is None:
            continue
        out.append(
            ChartMarker(
                ts_ms=int(ts_ms),
                price=float(price),
                side=str(item.get("side", "") or "").strip(),
                label=str(item.get("label", "") or "").strip(),
            )
        )
    return out


def _empty_live_chart_state(
    path: str,
    *,
    reason: str,
    state_present: bool,
    parse_error: bool = False,
) -> LiveChartState:
    return LiveChartState(
        path=path,
        candle_source="empty",
        chart_state_reason=str(reason or "").strip(),
        state_present=bool(state_present),
        parse_error=bool(parse_error),
        candles_count=0,
        last_candle_ts_ms=None,
    )


def load_live_chart_state(path: str) -> LiveChartState:
    target = _abs_path(path)
    if not target:
        return _empty_live_chart_state("", reason="state_path_missing", state_present=False)
    if not os.path.isfile(target):
        return _empty_live_chart_state(target, reason="state_file_absent", state_present=False)
    try:
        with open(target, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception:
        return _empty_live_chart_state(target, reason="state_parse_failed", state_present=True, parse_error=True)
    if not isinstance(payload, dict):
        return _empty_live_chart_state(target, reason="state_parse_failed", state_present=True, parse_error=True)
    position_raw = payload.get("position")
    markers_raw = payload.get("markers")
    pos = ChartPositionState()
    if isinstance(position_raw, dict):
        pos = ChartPositionState(
            is_open=bool(position_raw.get("is_open")),
            side=str(position_raw.get("side", "") or "").strip().lower(),
            entry_price=_safe_float(position_raw.get("entry_price")),
            qty=_safe_float(position_raw.get("qty")),
            stop_price=_safe_float(position_raw.get("stop_price")),
            tp_price=_safe_float(position_raw.get("tp_price")),
            unrealized_pnl=_safe_float(position_raw.get("unrealized_pnl")),
            opened_ts_ms=_safe_ts_ms(position_raw.get("opened_ts_ms")),
        )
    view_hint = payload.get("view_hint")
    preferred_mode = CHART_MODE_CANDLE
    if isinstance(view_hint, dict):
        preferred_mode = normalize_chart_mode(str(view_hint.get("preferred_mode", CHART_MODE_CANDLE) or CHART_MODE_CANDLE))
    candles = _load_live_chart_candles(payload.get("candles"))
    candles_count = max(int(_safe_int(payload.get("candles_count")) or 0), len(candles))
    last_candle_ts_ms = _safe_ts_ms(payload.get("last_candle_ts_ms"))
    if last_candle_ts_ms is None and candles:
        last_candle_ts_ms = int(candles[-1].ts_ms)
    candle_source = str(payload.get("candle_source", "") or "").strip()
    chart_state_reason = str(payload.get("chart_state_reason", "") or "").strip()
    if not candle_source:
        candle_source = "runner_buffer" if candles else "empty"
    if (not chart_state_reason) and (not candles):
        chart_state_reason = "candles_not_ready"
    state = LiveChartState(
        path=target,
        schema_version=str(payload.get("schema_version", "") or ""),
        updated_at=str(payload.get("updated_at", "") or ""),
        exchange_id=str(payload.get("exchange_id", "") or "").strip(),
        run_mode=str(payload.get("run_mode", "") or "").strip().upper(),
        symbol=str(payload.get("symbol", "") or "").strip(),
        timeframe=str(payload.get("timeframe", "") or "").strip(),
        candles=candles,
        current_price=_safe_float(payload.get("current_price")),
        candles_count=int(candles_count),
        candle_source=str(candle_source),
        last_candle_ts_ms=last_candle_ts_ms,
        chart_state_reason=str(chart_state_reason),
        state_present=True,
        parse_error=False,
        position=pos,
        entry_markers=_load_live_chart_markers((markers_raw or {}).get("entries") if isinstance(markers_raw, dict) else None),
        exit_markers=_load_live_chart_markers((markers_raw or {}).get("exits") if isinstance(markers_raw, dict) else None),
        preferred_mode=preferred_mode,
    )
    return state


def load_live_chart_state_for_runtime(state_dir: str, exchange_id: str, run_mode: str, symbol: str) -> LiveChartState:
    path = build_live_chart_state_path(state_dir, exchange_id, run_mode, symbol)
    return load_live_chart_state(path)


def with_live_chart_state(data: ResultChartData | None, state: LiveChartState | None) -> ResultChartData:
    out = deepcopy(data) if isinstance(data, ResultChartData) else ResultChartData()
    if isinstance(state, LiveChartState):
        out.live_chart_state = state
        out.chart_state_path = str(state.path or "")
        if state.symbol:
            out.symbol = str(state.symbol)
        if state.symbol and state.run_mode:
            out.source_name = f"{state.symbol} / {state.run_mode}"
        elif state.symbol and (not out.source_name):
            out.source_name = str(state.symbol)
        if not state.has_data:
            if bool(state.parse_error):
                out.empty_message = "live/paper chart state parse failed"
            elif bool(state.state_present):
                out.empty_message = "live/paper chart state present but candles are not ready"
            else:
                out.empty_message = "Waiting for live/paper candles..."
        return out
    out.live_chart_state = None
    out.chart_state_path = ""
    return out


def _format_iso_label(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    if len(text) > 19 and "T" in text:
        return text[:19].replace("T", " ")
    return text[:19]


def _format_ts_label(raw: Any) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    ts_ms = _safe_ts_ms(text)
    if ts_ms is None:
        return text[:19]
    try:
        dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return text[:19]


def _format_chart_time_label(ts_ms: int | None) -> str:
    dt = _local_chart_dt(ts_ms)
    if dt is None:
        return ""
    return dt.strftime("%m-%d %H:%M")


def _parse_chart_dt(raw: Any) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    ts_ms = _safe_ts_ms(text)
    if ts_ms is not None:
        return _local_chart_dt(ts_ms)
    candidate = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        dt = datetime.fromisoformat(candidate)
    except Exception:
        return None
    try:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone()
    except Exception:
        return None


def _format_candle_updated_label(raw: str, reference_ts_ms: int | None = None) -> str:
    dt = _parse_chart_dt(raw)
    if dt is not None:
        reference_dt = _local_chart_dt(reference_ts_ms) if reference_ts_ms is not None else datetime.now().astimezone()
        if reference_dt is not None and reference_dt.date() == dt.date():
            return f"updated {dt.strftime('%H:%M')}"
        if reference_dt is not None and reference_dt.year == dt.year:
            return f"updated {dt.strftime('%m-%d %H:%M')}"
        return f"updated {dt.strftime('%Y-%m-%d %H:%M')}"
    text = str(raw or "").strip().replace("T", " ")
    if not text:
        return ""
    compact = text
    if len(text) >= 16 and text[4:5] == "-":
        compact = text[5:16]
    else:
        compact = text[:16]
    return f"updated {compact}".strip()


def _local_chart_dt(ts_ms: int | None) -> datetime | None:
    if ts_ms is None or int(ts_ms) <= 0:
        return None
    try:
        return datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc).astimezone()
    except Exception:
        return None


def _align_ts_to_time_step(ts_ms: int, step_ms: int) -> int:
    if step_ms <= 0:
        return int(ts_ms)
    dt = _local_chart_dt(ts_ms)
    if dt is None:
        return int(int(ts_ms) // int(step_ms)) * int(step_ms)
    offset = dt.utcoffset()
    offset_ms = int(offset.total_seconds() * 1000.0) if offset is not None else 0
    return int(((int(ts_ms) + offset_ms) // int(step_ms)) * int(step_ms) - offset_ms)


def _count_time_ticks(min_ts: int, max_ts: int, step_ms: int) -> int:
    if step_ms <= 0 or max_ts < min_ts:
        return 0
    first_tick = _align_ts_to_time_step(int(min_ts), int(step_ms))
    if first_tick < int(min_ts):
        first_tick += int(step_ms)
    if first_tick > int(max_ts):
        return 0
    return int(((int(max_ts) - first_tick) // int(step_ms)) + 1)


def _format_time_tick(ts_ms: int, min_ts: int, max_ts: int, step_ms: int) -> str:
    dt = _local_chart_dt(ts_ms)
    start_dt = _local_chart_dt(min_ts)
    end_dt = _local_chart_dt(max_ts)
    if dt is None:
        return ""
    same_day = bool(start_dt is not None and end_dt is not None and start_dt.date() == end_dt.date())
    span_ms = max(0, int(max_ts) - int(min_ts))
    if same_day and span_ms <= (18 * 60 * 60 * 1000) and int(step_ms) < (24 * 60 * 60 * 1000):
        return dt.strftime("%H:%M")
    if int(step_ms) >= (24 * 60 * 60 * 1000):
        return dt.strftime("%m-%d")
    return dt.strftime("%m-%d %H:%M")


def _estimate_time_tick_label_width(label: str) -> float:
    return max(38.0, (len(str(label or "")) * 7.2) + 10.0)


def _build_time_ticks(min_ts: int, max_ts: int, plot_width: float) -> list[tuple[int, str]]:
    start_ts = int(min(min_ts, max_ts))
    end_ts = int(max(min_ts, max_ts))
    if end_ts <= start_ts:
        label = _format_chart_time_label(start_ts)
        return [(start_ts, label)] if label else []
    desired_count = max(3, min(6, int(round(max(1.0, float(plot_width)) / _CANDLE_MIN_TICK_PX))))
    best_step = int(_CANDLE_TICK_STEPS_MS[0])
    best_score: float | None = None
    for candidate in _CANDLE_TICK_STEPS_MS:
        step_ms = int(candidate)
        tick_count = _count_time_ticks(start_ts, end_ts, step_ms)
        if tick_count <= 0:
            continue
        score = abs(float(tick_count) - float(desired_count))
        first_tick = _align_ts_to_time_step(start_ts, step_ms)
        if first_tick < start_ts:
            first_tick += step_ms
        sample_labels = [
            _format_time_tick(first_tick, start_ts, end_ts, step_ms) if first_tick <= end_ts else "",
            _format_time_tick(end_ts, start_ts, end_ts, step_ms),
        ]
        estimated_label_width = max((_estimate_time_tick_label_width(label) for label in sample_labels if label), default=38.0)
        spacing = float(plot_width) / float(max(1, tick_count - 1))
        required_spacing = estimated_label_width + 14.0
        if spacing < required_spacing:
            score += ((required_spacing - spacing) / 9.0)
        if tick_count < 3 or tick_count > 6:
            score += 10.0
        if best_score is None or score < best_score:
            best_score = score
            best_step = step_ms
    ticks: list[tuple[int, str]] = []
    seen: set[int] = set()
    tick_ts = _align_ts_to_time_step(start_ts, best_step)
    if tick_ts < start_ts:
        tick_ts += best_step
    while tick_ts <= end_ts and len(ticks) < 16:
        label = _format_time_tick(tick_ts, start_ts, end_ts, best_step)
        if label and tick_ts not in seen:
            seen.add(int(tick_ts))
            ticks.append((int(tick_ts), label))
        tick_ts += best_step
    if len(ticks) >= 2:
        return ticks
    for fallback_ts in (start_ts, end_ts):
        label = _format_time_tick(fallback_ts, start_ts, end_ts, best_step) or _format_chart_time_label(fallback_ts)
        if label and fallback_ts not in seen:
            seen.add(int(fallback_ts))
            ticks.append((int(fallback_ts), label))
    return ticks


def _resolve_existing_path(raw: str, *, repo_root: str = "", export_dir: str = "", base_dir: str = "") -> str:
    text = str(raw or "").strip().strip("\"' ")
    if not text:
        return ""
    candidates: list[str] = []
    if os.path.isabs(text):
        candidates.append(text)
    else:
        if export_dir:
            candidates.append(os.path.join(export_dir, text))
        if base_dir:
            candidates.append(os.path.join(base_dir, text))
        if repo_root:
            candidates.append(os.path.join(repo_root, text))
        candidates.append(text)
    for candidate in candidates:
        ab = _abs_path(candidate)
        if os.path.exists(ab):
            return ab
    return ""


def _file_candidates(*patterns: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for pattern in patterns:
        text = str(pattern or "").strip()
        if not text:
            continue
        for value in glob.glob(text):
            ab = _abs_path(value)
            if ab in seen or not os.path.isfile(ab):
                continue
            seen.add(ab)
            out.append(ab)
    return out


def _dir_has_result_artifacts(path: str) -> bool:
    directory = _abs_path(path)
    if not os.path.isdir(directory):
        return False
    candidates = _file_candidates(
        os.path.join(directory, "report.json"),
        os.path.join(directory, "*report*.json"),
        os.path.join(directory, "equity_curve.csv"),
        os.path.join(directory, "synthetic_equity_curve.csv"),
        os.path.join(directory, "trades.csv"),
    )
    return bool(candidates)


def _artifact_score(path: str) -> float:
    try:
        return float(os.path.getmtime(path))
    except Exception:
        return 0.0


def _candidate_export_dirs(runtime_exports_dir: str, repo_root: str, last_run_payload: dict[str, Any] | None) -> list[str]:
    runtime_root = _abs_path(runtime_exports_dir)
    repo_exports = _abs_path(os.path.join(repo_root, "exports")) if repo_root else ""
    candidates: list[str] = []
    if isinstance(last_run_payload, dict):
        export_dir = _resolve_existing_path(str(last_run_payload.get("export_dir", "") or ""), repo_root=repo_root, base_dir=runtime_root)
        if export_dir and os.path.isdir(export_dir):
            candidates.append(export_dir)
        replay_report = _resolve_existing_path(
            str(last_run_payload.get("replay_report", "") or ""),
            repo_root=repo_root,
            export_dir=export_dir,
            base_dir=runtime_root,
        )
        if replay_report:
            candidates.append(os.path.dirname(replay_report))
    if _dir_has_result_artifacts(runtime_root):
        candidates.append(runtime_root)
    if repo_exports and _dir_has_result_artifacts(repo_exports):
        candidates.append(repo_exports)

    scored_dirs: list[tuple[float, str]] = []
    for pattern in (
        os.path.join(runtime_root, "runs", "*", "*"),
        os.path.join(repo_exports, "runs", "*", "*") if repo_exports else "",
    ):
        if not str(pattern or "").strip():
            continue
        for value in glob.glob(pattern):
            directory = _abs_path(value)
            if not _dir_has_result_artifacts(directory):
                continue
            artifacts = _file_candidates(
                os.path.join(directory, "report.json"),
                os.path.join(directory, "*report*.json"),
                os.path.join(directory, "equity_curve.csv"),
                os.path.join(directory, "synthetic_equity_curve.csv"),
                os.path.join(directory, "trades.csv"),
            )
            score = max((_artifact_score(path) for path in artifacts), default=_artifact_score(directory))
            scored_dirs.append((score, directory))
    for _score, directory in sorted(scored_dirs, key=lambda item: item[0], reverse=True):
        candidates.append(directory)
    return _unique_paths(candidates)


def _pick_report_path(export_dir: str, *, runtime_exports_dir: str, repo_root: str, last_run_payload: dict[str, Any] | None) -> str:
    hinted: list[str] = []
    if isinstance(last_run_payload, dict):
        replay_report = _resolve_existing_path(
            str(last_run_payload.get("replay_report", "") or ""),
            repo_root=repo_root,
            export_dir=export_dir,
            base_dir=runtime_exports_dir,
        )
        if replay_report:
            hinted.append(replay_report)
    patterns = [
        os.path.join(export_dir, "report.json") if export_dir else "",
        os.path.join(export_dir, "*report*.json") if export_dir else "",
        os.path.join(export_dir, "*.json") if export_dir else "",
    ]
    candidates = _unique_paths(hinted + _file_candidates(*patterns))
    if not candidates:
        return ""
    candidates.sort(key=_artifact_score, reverse=True)
    return candidates[0]


def _pick_equity_path(export_dir: str) -> str:
    candidates = _file_candidates(
        os.path.join(export_dir, "equity_curve.csv") if export_dir else "",
        os.path.join(export_dir, "synthetic_equity_curve.csv") if export_dir else "",
    )
    if not candidates:
        return ""
    candidates.sort(key=_artifact_score, reverse=True)
    preferred = [path for path in candidates if os.path.basename(path).lower() == "equity_curve.csv"]
    return preferred[0] if preferred else candidates[0]


def _pick_trades_path(export_dir: str) -> str:
    candidates = _file_candidates(os.path.join(export_dir, "trades.csv") if export_dir else "")
    if not candidates:
        return ""
    candidates.sort(key=_artifact_score, reverse=True)
    return candidates[0]


def _load_report_metrics(data: ResultChartData, report_path: str) -> None:
    with open(report_path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        return
    meta = payload.get("meta")
    metrics = payload.get("results")
    if not isinstance(metrics, dict):
        metrics = payload.get("overall")
    if not isinstance(metrics, dict):
        metrics = payload
    if isinstance(meta, dict):
        export_dir = _resolve_existing_path(str(meta.get("export_dir", "") or ""), export_dir=data.export_dir)
        if export_dir:
            data.export_dir = export_dir
        symbol = str(meta.get("symbol", "") or meta.get("preset", "") or "").strip()
        if symbol:
            data.symbol = symbol
        mode = str(meta.get("mode", "") or "").strip()
        if symbol and mode:
            data.source_name = f"{symbol} / {mode}"
        elif symbol:
            data.source_name = symbol
        elif mode:
            data.source_name = mode
    net_total = _safe_float(metrics.get("net_total"))
    initial_equity = None
    for raw in (
        metrics.get("initial_equity"),
        metrics.get("initial_balance"),
        metrics.get("start_equity"),
        metrics.get("starting_equity"),
        (meta.get("initial_equity") if isinstance(meta, dict) else None),
        payload.get("initial_equity"),
        payload.get("initial"),
    ):
        initial_equity = _safe_float(raw)
        if initial_equity is not None:
            break
    return_pct = _safe_float(metrics.get("return_pct_of_init"))
    max_dd = _safe_float(metrics.get("max_dd"))
    if max_dd is None:
        max_dd = _safe_float(metrics.get("max_dd_mtm"))
    if max_dd is None:
        max_dd = _safe_float(metrics.get("max_dd_worst_bar"))
    trades = _safe_int(metrics.get("trades"))
    if net_total is not None:
        data.net_total = net_total
    if initial_equity is not None:
        data.initial_equity = initial_equity
    if return_pct is None and net_total is not None and initial_equity is not None and abs(float(initial_equity)) > 1e-12:
        return_pct = (float(net_total) / float(initial_equity)) * 100.0
    if return_pct is not None:
        data.net_return_pct = return_pct
    if max_dd is not None:
        data.max_dd = max_dd
    if trades is not None:
        data.trades_count = trades


def _load_equity_points(data: ResultChartData, equity_path: str) -> None:
    points: list[float] = []
    labels: list[str] = []
    with open(equity_path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if not isinstance(row, dict):
                continue
            value = None
            for key in ("mtm_equity", "equity", "synthetic_equity"):
                value = _safe_float(row.get(key))
                if value is not None:
                    break
            if value is None:
                continue
            points.append(float(value))
            iso = _format_iso_label(str(row.get("ts_iso", "") or ""))
            labels.append(iso or _format_ts_label(row.get("ts")))
    if not points:
        return
    data.equity_points = points
    baseline = float(points[0])
    if data.initial_equity is None and abs(float(baseline)) > 1e-12:
        data.initial_equity = baseline
    data.net_points = [float(value) - baseline for value in points]
    peak = float(points[0])
    drawdowns: list[float] = []
    for value in points:
        current = float(value)
        if current > peak:
            peak = current
        if peak > 0.0:
            drawdown = (current - peak) / peak
        else:
            drawdown = 0.0
        drawdowns.append(float(drawdown))
    data.drawdown_points = drawdowns
    if labels:
        data.first_label = labels[0]
        data.last_label = labels[-1]
    if data.net_total is None and data.net_points:
        data.net_total = float(data.net_points[-1])
    if data.max_dd is None and data.drawdown_points:
        data.max_dd = abs(min(data.drawdown_points))


def _load_trade_points(data: ResultChartData, trades_path: str) -> list[float]:
    trade_points: list[float] = []
    cumulative_net: list[float] = []
    labels: list[str] = []
    running_net = 0.0
    with open(trades_path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for index, row in enumerate(reader, start=1):
            if not isinstance(row, dict):
                continue
            trade_points.append(float(index))
            net_value = _safe_float(row.get("net"))
            if net_value is None:
                pnl_value = _safe_float(row.get("pnl"))
                fee_value = _safe_float(row.get("fee"))
                if pnl_value is not None:
                    net_value = float(pnl_value) - float(fee_value or 0.0)
            if net_value is not None:
                running_net += float(net_value)
                cumulative_net.append(float(running_net))
            label = ""
            for key in ("ts_iso", "exit_ts_iso", "close_ts_iso"):
                label = _format_iso_label(str(row.get(key, "") or ""))
                if label:
                    break
            if not label:
                for key in ("ts", "exit_ts", "close_ts", "open_ts"):
                    label = _format_ts_label(row.get(key))
                    if label:
                        break
            labels.append(label)
    if trade_points:
        data.trade_points = trade_points
        if cumulative_net and (not data.net_points):
            data.net_points = cumulative_net
        if data.trades_count is None:
            data.trades_count = int(trade_points[-1])
        if (not data.first_label) and labels:
            data.first_label = labels[0]
            data.last_label = labels[-1]
    return cumulative_net


def _linear_series(length: int, end_value: float) -> list[float]:
    if length <= 1:
        return [float(end_value)]
    step = float(end_value) / float(length - 1)
    return [float(step * idx) for idx in range(length)]


def _ensure_fallback_series(data: ResultChartData) -> None:
    length = max(
        len(data.equity_points),
        len(data.net_points),
        len(data.drawdown_points),
        len(data.trade_points),
        int(data.trades_count or 0),
        2,
    )
    if (not data.trade_points) and int(data.trades_count or 0) > 0:
        if data.equity_points:
            scale = float(data.trades_count or 0)
            denom = max(1, len(data.equity_points) - 1)
            data.trade_points = [float(scale * idx / denom) for idx in range(len(data.equity_points))]
        else:
            data.trade_points = _linear_series(length, float(data.trades_count or 0))
    if (not data.net_points) and data.net_total is not None:
        data.net_points = _linear_series(length, float(data.net_total))
    if (not data.drawdown_points) and data.max_dd is not None:
        end_value = -abs(float(data.max_dd))
        data.drawdown_points = _linear_series(length, end_value)


def load_latest_result_chart_data(
    runtime_exports_dir: str,
    *,
    repo_root: str = "",
    last_run_payload: dict[str, Any] | None = None,
) -> tuple[ResultChartData, list[str]]:
    messages: list[str] = []
    data = ResultChartData()
    payload = last_run_payload if isinstance(last_run_payload, dict) else None
    candidate_dirs = _candidate_export_dirs(runtime_exports_dir, repo_root, payload)
    for export_dir in candidate_dirs:
        report_path = _pick_report_path(
            export_dir,
            runtime_exports_dir=runtime_exports_dir,
            repo_root=repo_root,
            last_run_payload=payload,
        )
        equity_path = _pick_equity_path(export_dir)
        trades_path = _pick_trades_path(export_dir)
        if report_path or equity_path or trades_path:
            data.export_dir = export_dir
            data.report_path = report_path
            data.equity_path = equity_path
            data.trades_path = trades_path
            break

    if not data.export_dir and candidate_dirs:
        data.export_dir = candidate_dirs[0]

    if data.report_path:
        try:
            _load_report_metrics(data, data.report_path)
        except Exception as exc:
            messages.append(f"report read error={exc}")
    if data.equity_path:
        try:
            _load_equity_points(data, data.equity_path)
        except Exception as exc:
            messages.append(f"equity curve read error={exc}")
    if data.trades_path:
        try:
            _load_trade_points(data, data.trades_path)
        except Exception as exc:
            messages.append(f"trades read error={exc}")

    if (not data.source_name) and data.export_dir:
        data.source_name = os.path.basename(data.export_dir)
    if (not data.source_name) and data.report_path:
        data.source_name = os.path.basename(data.report_path)

    _ensure_fallback_series(data)
    if not data.has_any_result:
        data.empty_message = "No result data yet"
    elif not data.has_chart_data:
        data.empty_message = "No chart-ready data"
    return data, messages


def _format_net_value(value: float | None) -> str:
    if value is None:
        return "--"
    return f"{float(value):+,.2f}"


def _net_value_color(value: float | None) -> QColor:
    if value is None:
        return _COLOR_TEXT
    numeric = float(value)
    if abs(numeric) <= 1e-12:
        return _COLOR_TEXT
    return _COLOR_NET if numeric > 0.0 else _COLOR_NET_NEGATIVE


def _resolve_net_return_pct(data: ResultChartData) -> float | None:
    if not isinstance(data, ResultChartData):
        return None
    if data.net_total is not None and data.initial_equity is not None and abs(float(data.initial_equity)) > 1e-12:
        return (float(data.net_total) / float(data.initial_equity)) * 100.0
    if data.net_return_pct is None:
        return None
    return float(data.net_return_pct)


def _format_net_with_pct(data: ResultChartData) -> str:
    net_text = _format_net_value(data.net_total)
    return_pct = _resolve_net_return_pct(data)
    if return_pct is None:
        return net_text
    if abs(float(return_pct)) <= 1e-12:
        pct_text = "0.0%"
    else:
        pct_text = f"{_trim_decimal_text(f'{float(return_pct):+.1f}')}%"
    return f"{net_text} <span style='font-size:12px; font-weight:600;'>({pct_text})</span>"


def _format_drawdown_value(value: float | None) -> str:
    if value is None:
        return "--"
    drawdown = abs(float(value))
    if drawdown <= 1.5:
        return f"{drawdown * 100.0:.2f}%"
    return f"{drawdown:,.2f}"


def _format_trades_value(value: int | None) -> str:
    if value is None:
        return "--"
    return f"{int(value):,d}"


def _normalized_points(values: list[float], *, absolute: bool = False) -> list[float]:
    if not values:
        return []
    raw = [abs(float(value)) if absolute else float(value) for value in values]
    lo = min(raw)
    hi = max(raw)
    if abs(hi - lo) <= 1e-12:
        return [0.5 for _ in raw]
    return [float((value - lo) / (hi - lo)) for value in raw]


def _trim_decimal_text(text: str) -> str:
    if "." not in text:
        return text
    return text.rstrip("0").rstrip(".")


def _format_decimal_value(value: float, decimals: int) -> str:
    if abs(float(value)) <= 1e-12:
        return "0"
    places = max(0, int(decimals))
    if places <= 0:
        return str(int(round(float(value))))
    return _trim_decimal_text(f"{float(value):.{places}f}")


def _plain_value_decimals(span: float) -> int:
    scale = abs(float(span))
    if scale >= 200.0:
        return 0
    if scale >= 20.0:
        return 1
    if scale >= 2.0:
        return 2
    if scale >= 0.2:
        return 3
    return 4


def _format_short_value(value: float, *, span: float = 0.0) -> str:
    numeric = float(value)
    abs_value = abs(numeric)
    for threshold, divisor, suffix in (
        (1_000_000_000.0, 1_000_000_000.0, "B"),
        (1_000_000.0, 1_000_000.0, "M"),
        (1_000.0, 1_000.0, "K"),
    ):
        if abs_value >= threshold:
            scaled = numeric / divisor
            decimals = 0 if abs(scaled) >= 100.0 else 1
            return f"{_format_decimal_value(scaled, decimals)}{suffix}"
    return _format_decimal_value(numeric, _plain_value_decimals(span if span else abs_value))


def _format_axis_value(value: float, mode: str, *, span: float) -> str:
    current = normalize_chart_mode(mode)
    if current == CHART_MODE_COMBINED:
        return _format_decimal_value(value, 2)
    if current == CHART_MODE_MAX_DD and abs(float(value)) <= 1.5:
        percent_span = abs(float(span)) * 100.0
        decimals = 0 if percent_span >= 50.0 else 1 if percent_span >= 5.0 else 2
        return f"{_format_decimal_value(float(value) * 100.0, decimals)}%"
    return _format_short_value(value, span=span)


def _apply_dark_combo_popup(combo: QComboBox) -> None:
    if not isinstance(combo, QComboBox):
        return
    combo.setStyleSheet(_DARK_COMBO_POPUP_STYLESHEET)
    view = combo.view()
    if view is None:
        return
    palette = view.palette()
    palette.setColor(QPalette.ColorRole.Base, _COLOR_PANEL)
    palette.setColor(QPalette.ColorRole.Text, _COLOR_TEXT)
    palette.setColor(QPalette.ColorRole.Window, _COLOR_PANEL)
    palette.setColor(QPalette.ColorRole.WindowText, _COLOR_TEXT)
    palette.setColor(QPalette.ColorRole.Highlight, _COLOR_COMBO_HIGHLIGHT)
    palette.setColor(QPalette.ColorRole.HighlightedText, _COLOR_EQUITY)
    view.setPalette(palette)
    view.setStyleSheet(
        """
        QAbstractItemView {
            background-color: #0c0f12;
            color: #d7dde3;
            border: 1px solid #2a3037;
            outline: none;
        }
        QAbstractItemView::item {
            min-height: 22px;
            padding: 4px 8px;
            background-color: #0c0f12;
            color: #d7dde3;
        }
        QAbstractItemView::item:selected {
            background-color: #1b2430;
            color: #f2f5f7;
        }
        QAbstractItemView::item:hover {
            background-color: #1b2430;
            color: #f2f5f7;
        }
        """
    )


class ResultChartWidget(QWidget):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._mode = CHART_MODE_EQUITY
        self._data = ResultChartData()
        self._ui_texts: dict[str, str] = {}
        self._initial_view_state = self._default_view_state_for_mode(self._mode)
        self._view_state = _ChartViewState(
            x_margin_ratio=self._initial_view_state.x_margin_ratio,
            y_padding_ratio=self._initial_view_state.y_padding_ratio,
        )
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.setMinimumSize(_CHART_MIN_WIDTH, _CHART_MIN_HEIGHT)

    def sizeHint(self) -> QSize:
        return QSize(640, 320)

    def minimumSizeHint(self) -> QSize:
        return QSize(_CHART_MIN_WIDTH, _CHART_MIN_HEIGHT)

    def set_result_data(self, data: ResultChartData) -> None:
        self._data = data if isinstance(data, ResultChartData) else ResultChartData()
        self.update()

    def result_data(self) -> ResultChartData:
        return self._data

    def set_ui_texts(self, texts: dict[str, str] | None) -> None:
        self._ui_texts = dict(texts or {})
        self.update()

    def set_chart_mode(self, mode: str) -> None:
        self._mode = normalize_chart_mode(mode)
        self._reset_view_state_for_mode()
        self.update()

    def chart_mode(self) -> str:
        return self._mode

    def fit_all(self) -> None:
        self._view_state = _ChartViewState(x_margin_ratio=0.0, y_padding_ratio=0.03)
        self.update()

    def reset_view(self) -> None:
        self._view_state = _ChartViewState(
            x_margin_ratio=self._initial_view_state.x_margin_ratio,
            y_padding_ratio=self._initial_view_state.y_padding_ratio,
        )
        self.update()

    def save_png(self, path: str, size: QSize | None = None) -> bool:
        target = _abs_path(path)
        parent = os.path.dirname(target)
        if parent:
            os.makedirs(parent, exist_ok=True)
        image_size = size if isinstance(size, QSize) and size.isValid() else QSize(max(self.width(), 1280), max(self.height(), 720))
        image = QImage(image_size, QImage.Format.Format_ARGB32)
        image.fill(_COLOR_BG)
        painter = QPainter(image)
        try:
            self._render_chart(painter, QRectF(0.0, 0.0, float(image.width()), float(image.height())))
        finally:
            painter.end()
        return bool(image.save(target, "PNG"))

    def paintEvent(self, event) -> None:  # type: ignore[override]
        painter = QPainter(self)
        try:
            self._render_chart(painter, QRectF(self.rect()))
        finally:
            painter.end()

    def _render_chart(self, painter: QPainter, rect: QRectF) -> None:
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        painter.fillRect(rect, _COLOR_BG)
        outer = rect.adjusted(1.0, 1.0, -1.0, -1.0)
        painter.setPen(QPen(_COLOR_BORDER, 1.0))
        painter.drawRoundedRect(outer, 6.0, 6.0)

        if self._mode == CHART_MODE_CANDLE:
            self._render_candle_chart(painter, outer)
            return

        title_rect = QRectF(outer.left() + 14.0, outer.top() + 10.0, outer.width() - 28.0, 20.0)
        painter.setPen(_COLOR_TEXT)
        painter.drawText(
            title_rect,
            Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
            _display_chart_mode_text(self._mode, self._ui_texts),
        )
        if self._data.source_name:
            painter.setPen(_COLOR_MUTED)
            painter.drawText(title_rect, Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter, self._data.source_name)

        series = self._series_for_mode(self._mode)
        chart_top = 54.0 if len(series) > 1 else 38.0
        chart_rect = QRectF(outer.left() + 14.0, outer.top() + chart_top, outer.width() - 28.0, outer.height() - (chart_top + 34.0))
        if chart_rect.width() <= 10.0 or chart_rect.height() <= 10.0:
            return
        plot_rect = self._plot_rect(chart_rect)

        if not series:
            painter.setPen(_COLOR_MUTED)
            painter.drawText(
                chart_rect,
                Qt.AlignmentFlag.AlignCenter,
                _translate_result_message(
                    self._data.empty_message,
                    self._ui_texts,
                    default_key="result.empty.no_result_data",
                    default_text="No result data",
                ),
            )
            self._draw_footer(painter, outer)
            return

        min_y, max_y = self._range_for_series(series, self._mode)
        if abs(max_y - min_y) <= 1e-12:
            max_y += 1.0
            min_y -= 1.0
        ticks = self._build_y_axis_ticks(min_y, max_y, self._mode)
        axis_width = self._axis_width(painter, ticks, chart_rect)
        plot_rect = self._plot_rect(chart_rect, axis_width)
        if plot_rect.width() <= 10.0 or plot_rect.height() <= 10.0:
            return
        axis_rect = QRectF(chart_rect.left(), plot_rect.top(), max(0.0, plot_rect.left() - chart_rect.left() - _Y_AXIS_GUTTER), plot_rect.height())
        self._draw_y_axis(painter, axis_rect, plot_rect, ticks, min_y, max_y)
        painter.setPen(QPen(_COLOR_BORDER, 1.0))
        painter.drawRect(plot_rect)

        zero_line_y = self._map_y(0.0, plot_rect, min_y, max_y)
        if min_y <= 0.0 <= max_y:
            painter.setPen(QPen(_COLOR_BORDER, 1.0, Qt.PenStyle.DashLine))
            painter.drawLine(plot_rect.left(), zero_line_y, plot_rect.right(), zero_line_y)

        for item in series:
            points = self._build_points(plot_rect, item.values, min_y, max_y)
            if len(points) >= 2 and item.fill:
                fill_path = QPainterPath()
                fill_path.moveTo(points[0].x(), zero_line_y)
                for point in points:
                    fill_path.lineTo(point)
                fill_path.lineTo(points[-1].x(), zero_line_y)
                fill_path.closeSubpath()
                fill_color = QColor(item.color)
                fill_color.setAlpha(60)
                painter.fillPath(fill_path, fill_color)
            if len(points) >= 2:
                path = QPainterPath(points[0])
                for point in points[1:]:
                    path.lineTo(point)
                painter.setPen(QPen(item.color, 2.1))
                painter.drawPath(path)
            elif len(points) == 1:
                painter.setPen(QPen(item.color, 2.1))
                painter.setBrush(item.color)
                painter.drawEllipse(points[0], 3.0, 3.0)

        if len(series) > 1:
            self._draw_legend(painter, outer, series)
        self._draw_footer(painter, outer)

    def _draw_legend(self, painter: QPainter, rect: QRectF, series: list[_ChartSeries]) -> None:
        x = rect.left() + 14.0
        y = rect.top() + 34.0
        for item in series:
            painter.setPen(QPen(item.color, 2.0))
            painter.drawLine(x, y, x + 14.0, y)
            painter.setPen(_COLOR_MUTED)
            painter.drawText(QRectF(x + 18.0, y - 8.0, 64.0, 16.0), Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter, item.label)
            x += 72.0

    def _draw_footer(self, painter: QPainter, rect: QRectF) -> None:
        footer_rect = QRectF(rect.left() + 14.0, rect.bottom() - 24.0, rect.width() - 28.0, 16.0)
        painter.setPen(_COLOR_MUTED)
        painter.drawText(footer_rect, Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter, self._data.first_label or "")
        painter.drawText(footer_rect, Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter, self._data.last_label or "")

    def _default_view_state_for_mode(self, mode: str) -> _ChartViewState:
        if mode == CHART_MODE_CANDLE:
            return _ChartViewState(x_margin_ratio=0.01, y_padding_ratio=0.05)
        if mode == CHART_MODE_COMBINED:
            return _ChartViewState(x_margin_ratio=0.015, y_padding_ratio=0.05)
        if mode == CHART_MODE_EQUITY:
            return _ChartViewState(x_margin_ratio=0.018, y_padding_ratio=0.08)
        return _ChartViewState(x_margin_ratio=0.015, y_padding_ratio=0.07)

    def _reset_view_state_for_mode(self) -> None:
        self._initial_view_state = self._default_view_state_for_mode(self._mode)
        self._view_state = _ChartViewState(
            x_margin_ratio=self._initial_view_state.x_margin_ratio,
            y_padding_ratio=self._initial_view_state.y_padding_ratio,
        )

    def _build_y_axis_ticks(self, min_y: float, max_y: float, mode: str) -> list[tuple[float, str]]:
        span = float(max_y - min_y)
        if _Y_AXIS_TICK_COUNT <= 1:
            return [(float(max_y), _format_axis_value(float(max_y), mode, span=span))]
        out: list[tuple[float, str]] = []
        last_index = _Y_AXIS_TICK_COUNT - 1
        for index in range(_Y_AXIS_TICK_COUNT):
            ratio = float(index) / float(last_index)
            value = float(max_y - (span * ratio))
            out.append((value, _format_axis_value(value, mode, span=span)))
        return out

    def _axis_width(self, painter: QPainter, ticks: list[tuple[float, str]], chart_rect: QRectF) -> float:
        metrics = QFontMetricsF(painter.font())
        label_width = max((metrics.horizontalAdvance(label) for _value, label in ticks), default=0.0)
        desired = max(_Y_AXIS_MIN_WIDTH, label_width + 8.0)
        maximum = max(_Y_AXIS_MIN_WIDTH, chart_rect.width() * _Y_AXIS_MAX_WIDTH_RATIO)
        return min(desired, maximum)

    def _draw_y_axis(
        self,
        painter: QPainter,
        axis_rect: QRectF,
        plot_rect: QRectF,
        ticks: list[tuple[float, str]],
        min_y: float,
        max_y: float,
    ) -> None:
        metrics = QFontMetricsF(painter.font())
        text_height = metrics.height() + 2.0
        grid_color = QColor(_COLOR_GRID)
        grid_color.setAlpha(220)
        painter.setPen(QPen(grid_color, 1.0))
        for value, label in ticks:
            y = self._map_y(value, plot_rect, min_y, max_y)
            painter.drawLine(plot_rect.left(), y, plot_rect.right(), y)
            painter.setPen(QPen(_COLOR_BORDER, 1.0))
            painter.drawLine(plot_rect.left() - 4.0, y, plot_rect.left(), y)
            label_rect = QRectF(axis_rect.left(), y - (text_height / 2.0), axis_rect.width(), text_height)
            painter.setPen(_COLOR_MUTED)
            painter.drawText(label_rect, Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter, label)
            painter.setPen(QPen(grid_color, 1.0))

    def _plot_rect(self, chart_rect: QRectF, axis_width: float = 0.0) -> QRectF:
        x_margin = max(0.0, min(chart_rect.width() * self._view_state.x_margin_ratio, chart_rect.width() * 0.2))
        left_inset = max(0.0, axis_width) + _Y_AXIS_GUTTER + x_margin
        plot_rect = chart_rect.adjusted(left_inset, 0.0, -x_margin, 0.0)
        if plot_rect.width() <= 10.0:
            return chart_rect
        return plot_rect

    def _series_for_mode(self, mode: str) -> list[_ChartSeries]:
        if mode == CHART_MODE_EQUITY:
            if self._data.equity_points:
                return [_ChartSeries(_display_chart_mode_text(CHART_MODE_EQUITY, self._ui_texts), self._data.equity_points, _COLOR_EQUITY)]
            return []
        if mode == CHART_MODE_NET:
            if self._data.net_points:
                return [_ChartSeries(_display_chart_mode_text(CHART_MODE_NET, self._ui_texts), self._data.net_points, _COLOR_NET)]
            return []
        if mode == CHART_MODE_MAX_DD:
            if self._data.drawdown_points:
                return [_ChartSeries(_display_chart_mode_text(CHART_MODE_MAX_DD, self._ui_texts), self._data.drawdown_points, _COLOR_DD, fill=True)]
            return []
        if mode == CHART_MODE_TRADES:
            if self._data.trade_points:
                return [_ChartSeries(_display_chart_mode_text(CHART_MODE_TRADES, self._ui_texts), self._data.trade_points, _COLOR_TRADES)]
            return []
        combined: list[_ChartSeries] = []
        if self._data.net_points:
            combined.append(_ChartSeries(_display_chart_mode_text(CHART_MODE_NET, self._ui_texts), _normalized_points(self._data.net_points), _COLOR_NET))
        if self._data.drawdown_points:
            combined.append(_ChartSeries(_display_chart_mode_text(CHART_MODE_MAX_DD, self._ui_texts), _normalized_points(self._data.drawdown_points, absolute=True), _COLOR_DD))
        if self._data.trade_points:
            combined.append(_ChartSeries(_display_chart_mode_text(CHART_MODE_TRADES, self._ui_texts), _normalized_points(self._data.trade_points), _COLOR_TRADES))
        return combined

    def _range_for_series(self, series: list[_ChartSeries], mode: str) -> tuple[float, float]:
        values = [float(value) for item in series for value in item.values]
        if not values:
            return (0.0, 1.0)
        min_y = min(values)
        max_y = max(values)
        padding = max(0.0, float(self._view_state.y_padding_ratio))
        if mode == CHART_MODE_COMBINED:
            return (0.0, 1.0)
        if mode == CHART_MODE_MAX_DD:
            min_y = min(min_y, 0.0)
            max_y = max(max_y, 0.0)
            if abs(max_y - min_y) > 1e-12:
                pad = (max_y - min_y) * padding
                return (min_y - pad, max_y + pad)
            return (min_y - 1.0, max_y + 1.0)
        if mode in (CHART_MODE_NET, CHART_MODE_TRADES):
            if min_y > 0.0:
                min_y = 0.0
            if max_y < 0.0:
                max_y = 0.0
        if abs(max_y - min_y) > 1e-12:
            pad = (max_y - min_y) * padding
            return (min_y - pad, max_y + pad)
        pad = abs(max_y) * max(padding, 0.03) if max_y else 1.0
        return (min_y - pad, max_y + pad)

    def _build_points(self, rect: QRectF, values: list[float], min_y: float, max_y: float) -> list[QPointF]:
        if not values:
            return []
        if len(values) == 1:
            x = rect.left() + (rect.width() / 2.0)
            return [QPointF(x, self._map_y(float(values[0]), rect, min_y, max_y))]
        points: list[QPointF] = []
        last_index = len(values) - 1
        for index, value in enumerate(values):
            x = rect.left() + (rect.width() * index / last_index)
            y = self._map_y(float(value), rect, min_y, max_y)
            points.append(QPointF(x, y))
        return points

    def _map_y(self, value: float, rect: QRectF, min_y: float, max_y: float) -> float:
        if abs(max_y - min_y) <= 1e-12:
            return rect.center().y()
        ratio = (float(value) - min_y) / (max_y - min_y)
        ratio = max(0.0, min(1.0, ratio))
        return rect.bottom() - (rect.height() * ratio)

    def _render_candle_chart(self, painter: QPainter, outer: QRectF) -> None:
        state = self._data.live_chart_state if isinstance(self._data.live_chart_state, LiveChartState) else None
        title_rect = QRectF(outer.left() + 14.0, outer.top() + 10.0, outer.width() - 28.0, 20.0)
        painter.setPen(_COLOR_TEXT)
        painter.drawText(
            title_rect,
            Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
            _display_chart_mode_text(CHART_MODE_CANDLE, self._ui_texts),
        )
        if state is not None and state.symbol:
            painter.setPen(_COLOR_MUTED)
            painter.drawText(title_rect, Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter, state.symbol)

        chart_top = 40.0
        chart_rect = QRectF(outer.left() + 14.0, outer.top() + chart_top, outer.width() - 28.0, outer.height() - (chart_top + 34.0))
        if chart_rect.width() <= 10.0 or chart_rect.height() <= 10.0:
            return

        candles = list((state.candles if state is not None else [])[-_CANDLE_MAX_BARS:])
        if not candles:
            painter.setPen(_COLOR_MUTED)
            painter.drawText(
                chart_rect,
                Qt.AlignmentFlag.AlignCenter,
                _translate_result_message(
                    self._data.empty_message,
                    self._ui_texts,
                    default_key="result.empty.live_waiting",
                    default_text="Waiting for live/paper candles...",
                ),
            )
            if state is not None:
                diag_parts: list[str] = []
                if state.candle_source:
                    diag_parts.append(f"source={state.candle_source}")
                diag_parts.append(f"candles={int(state.candles_count)}")
                if state.chart_state_reason:
                    diag_parts.append(f"reason={state.chart_state_reason}")
                diag_text = "  ".join(diag_parts)
                if diag_text:
                    diag_rect = QRectF(chart_rect.left(), chart_rect.center().y() + 18.0, chart_rect.width(), 18.0)
                    painter.drawText(diag_rect, Qt.AlignmentFlag.AlignCenter, diag_text)
            self._draw_candle_footer(painter, outer, state, candles)
            return

        current_price = float(state.current_price) if state is not None and state.current_price is not None else float(candles[-1].close)
        values = [float(item.high) for item in candles] + [float(item.low) for item in candles]
        values.append(float(current_price))
        if state is not None and state.position.is_open:
            for extra in (state.position.entry_price, state.position.stop_price, state.position.tp_price):
                if extra is not None:
                    values.append(float(extra))
        for marker in (state.entry_markers if state is not None else []):
            values.append(float(marker.price))
        for marker in (state.exit_markers if state is not None else []):
            values.append(float(marker.price))
        min_y = min(values)
        max_y = max(values)
        if abs(max_y - min_y) <= 1e-12:
            max_y += 1.0
            min_y -= 1.0
        span = max_y - min_y
        pad = span * max(0.03, float(self._view_state.y_padding_ratio))
        min_y -= pad
        max_y += pad

        ticks = self._build_y_axis_ticks(min_y, max_y, CHART_MODE_CANDLE)
        axis_width = self._axis_width(painter, ticks, chart_rect)
        plot_area_rect = chart_rect.adjusted(0.0, 0.0, 0.0, -_CANDLE_X_AXIS_HEIGHT)
        plot_rect = self._plot_rect(plot_area_rect, axis_width)
        if plot_rect.width() <= 10.0 or plot_rect.height() <= 10.0:
            return
        axis_rect = QRectF(chart_rect.left(), plot_rect.top(), max(0.0, plot_rect.left() - chart_rect.left() - _Y_AXIS_GUTTER), plot_rect.height())
        x_axis_rect = QRectF(plot_rect.left(), plot_rect.bottom() + 4.0, plot_rect.width(), max(12.0, chart_rect.bottom() - plot_rect.bottom() - 2.0))
        self._draw_y_axis(painter, axis_rect, plot_rect, ticks, min_y, max_y)
        painter.setPen(QPen(_COLOR_BORDER, 1.0))
        painter.drawRect(plot_rect)
        self._draw_candle_x_axis(painter, plot_rect, x_axis_rect, candles)

        self._draw_position_band(painter, plot_rect, candles, state, current_price, min_y, max_y)
        self._draw_candles(painter, plot_rect, candles, min_y, max_y)
        price_tag_requests = self._draw_position_lines(painter, plot_rect, candles, state, min_y, max_y)
        price_tag_requests.append(self._draw_current_price_line(painter, plot_rect, current_price, min_y, max_y))
        self._draw_entry_markers(painter, plot_rect, candles, state.entry_markers if state is not None else [], min_y, max_y)
        self._draw_exit_markers(painter, plot_rect, candles, state.exit_markers if state is not None else [], min_y, max_y)
        self._draw_price_tags(painter, plot_rect, price_tag_requests)
        self._draw_candle_overlay(painter, outer, state)
        self._draw_candle_footer(painter, outer, state, candles)

    def _draw_candles(self, painter: QPainter, plot_rect: QRectF, candles: list[ChartCandle], min_y: float, max_y: float) -> None:
        count = len(candles)
        if count <= 0:
            return
        slot = self._candle_slot_width(count, plot_rect)
        body_width = max(3.5, min(12.0, slot * 0.72))
        half_width = body_width / 2.0
        for index, candle in enumerate(candles):
            x = self._map_candle_index_to_x(index, count, plot_rect)
            y_open = self._map_y(float(candle.open), plot_rect, min_y, max_y)
            y_high = self._map_y(float(candle.high), plot_rect, min_y, max_y)
            y_low = self._map_y(float(candle.low), plot_rect, min_y, max_y)
            y_close = self._map_y(float(candle.close), plot_rect, min_y, max_y)
            up = float(candle.close) >= float(candle.open)
            color = _COLOR_CANDLE_UP if up else _COLOR_CANDLE_DOWN
            is_latest = index == (count - 1)
            painter.setPen(QPen(color, 1.45 if is_latest else 1.2))
            painter.drawLine(QPointF(x, y_high), QPointF(x, y_low))
            top = min(y_open, y_close)
            bottom = max(y_open, y_close)
            height = max(1.8, bottom - top)
            body_rect = QRectF(x - half_width, top, body_width, height)
            fill = QColor(color)
            fill.setAlpha(236 if is_latest else 210)
            painter.fillRect(body_rect, fill)
            painter.drawRect(body_rect)

    def _draw_position_band(
        self,
        painter: QPainter,
        plot_rect: QRectF,
        candles: list[ChartCandle],
        state: LiveChartState | None,
        current_price: float,
        min_y: float,
        max_y: float,
    ) -> None:
        if state is None or (not state.position.is_open) or state.position.entry_price is None:
            return
        entry_price = float(state.position.entry_price)
        y_entry = self._map_y(entry_price, plot_rect, min_y, max_y)
        y_current = self._map_y(float(current_price), plot_rect, min_y, max_y)
        x_start = plot_rect.left()
        if state.position.opened_ts_ms is not None:
            x_start = self._map_ts_to_x(int(state.position.opened_ts_ms), candles, plot_rect)
        band_rect = QRectF(x_start, min(y_entry, y_current), max(1.0, plot_rect.right() - x_start), abs(y_current - y_entry))
        color = QColor(_COLOR_POSITION_GAIN if current_price >= entry_price else _COLOR_POSITION_LOSS)
        color.setAlpha(36)
        painter.fillRect(band_rect, color)

    def _draw_position_lines(
        self,
        painter: QPainter,
        plot_rect: QRectF,
        candles: list[ChartCandle],
        state: LiveChartState | None,
        min_y: float,
        max_y: float,
    ) -> list[_PriceTagRequest]:
        tag_requests: list[_PriceTagRequest] = []
        if state is None or (not state.position.is_open):
            return tag_requests
        x_start = plot_rect.left()
        if state.position.opened_ts_ms is not None:
            x_start = self._map_ts_to_x(int(state.position.opened_ts_ms), candles, plot_rect)
        for short_label, value, color, pen_style, pen_width, priority, allow_hide in (
            ("ENTRY", state.position.entry_price, _COLOR_ENTRY, Qt.PenStyle.SolidLine, 1.45, 120, True),
            ("SL", state.position.stop_price, _COLOR_STOP, Qt.PenStyle.CustomDashLine, 1.35, 220, False),
            ("TP", state.position.tp_price, _COLOR_TP, Qt.PenStyle.CustomDashLine, 1.35, 220, False),
        ):
            if value is None:
                continue
            y = self._map_y(float(value), plot_rect, min_y, max_y)
            pen = QPen(color, pen_width, pen_style)
            if pen_style == Qt.PenStyle.CustomDashLine:
                pen.setDashPattern([6.0, 4.0])
            painter.setPen(pen)
            painter.drawLine(QPointF(x_start, y), QPointF(plot_rect.right(), y))
            tag_requests.append(
                _PriceTagRequest(
                    y=y,
                    text=f"{short_label} {_format_short_value(float(value), span=max_y - min_y)}",
                    color=color,
                    priority=int(priority),
                    allow_hide=bool(allow_hide),
                )
            )
        return tag_requests

    def _draw_current_price_line(
        self,
        painter: QPainter,
        plot_rect: QRectF,
        current_price: float,
        min_y: float,
        max_y: float,
    ) -> _PriceTagRequest:
        y = self._map_y(float(current_price), plot_rect, min_y, max_y)
        pen = QPen(_COLOR_PRICE_LINE, 1.35, Qt.PenStyle.CustomDashLine)
        pen.setDashPattern([7.0, 4.0])
        painter.setPen(pen)
        painter.drawLine(QPointF(plot_rect.left(), y), QPointF(plot_rect.right(), y))
        label = _format_short_value(float(current_price), span=max_y - min_y)
        return _PriceTagRequest(
            y=y,
            text=label,
            color=_COLOR_PRICE_LINE,
            priority=300,
            emphasized=True,
        )

    def _draw_candle_x_axis(
        self,
        painter: QPainter,
        plot_rect: QRectF,
        x_axis_rect: QRectF,
        candles: list[ChartCandle],
    ) -> None:
        if len(candles) < 2 or x_axis_rect.height() <= 8.0:
            return
        ticks = _build_time_ticks(int(candles[0].ts_ms), int(candles[-1].ts_ms), plot_rect.width())
        if not ticks:
            return
        metrics = QFontMetricsF(painter.font())
        edge_pad = 2.0
        min_gap = max(10.0, min(18.0, plot_rect.width() * 0.018))
        candidates: list[tuple[float, str, float]] = []
        for tick_ts, label in ticks:
            if not label:
                continue
            x = self._map_ts_to_x(int(tick_ts), candles, plot_rect)
            label_width = max(36.0, metrics.horizontalAdvance(label) + 12.0)
            candidates.append((x, label, label_width))
        while len(candidates) > 3:
            crowded = False
            last_right = x_axis_rect.left() - 9999.0
            for x, _label, label_width in candidates:
                left = max(
                    x_axis_rect.left() + edge_pad,
                    min(x - (label_width / 2.0), (x_axis_rect.right() - edge_pad) - label_width),
                )
                if left < (last_right + min_gap):
                    crowded = True
                    break
                last_right = left + label_width
            if not crowded:
                break
            target_count = max(3, len(candidates) - 1)
            if target_count >= len(candidates):
                break
            last_index = len(candidates) - 1
            keep_indices = sorted(
                {
                    int(round((float(index) * float(last_index)) / float(max(1, target_count - 1))))
                    for index in range(target_count)
                }
            )
            if len(keep_indices) >= len(candidates):
                break
            candidates = [candidates[index] for index in keep_indices]
        visible_ticks: list[tuple[float, str, float]] = []
        last_right = x_axis_rect.left() - 9999.0
        for x, label, label_width in candidates:
            left = max(
                x_axis_rect.left() + edge_pad,
                min(x - (label_width / 2.0), (x_axis_rect.right() - edge_pad) - label_width),
            )
            if visible_ticks and left < (last_right + min_gap):
                continue
            visible_ticks.append((x, label, label_width))
            last_right = left + label_width
        guide_color = QColor(_COLOR_GRID)
        guide_color.setAlpha(150)
        tick_text_color = QColor(_COLOR_MUTED)
        tick_text_color.setAlpha(230)
        for x, label, label_width in visible_ticks:
            painter.setPen(QPen(guide_color, 1.0))
            painter.drawLine(QPointF(x, plot_rect.top()), QPointF(x, plot_rect.bottom()))
            painter.setPen(QPen(_COLOR_BORDER, 1.0))
            painter.drawLine(QPointF(x, plot_rect.bottom()), QPointF(x, plot_rect.bottom() + 4.0))
            label_left = max(
                x_axis_rect.left() + edge_pad,
                min(x - (label_width / 2.0), (x_axis_rect.right() - edge_pad) - label_width),
            )
            label_rect = QRectF(label_left, x_axis_rect.top(), label_width, x_axis_rect.height())
            painter.setPen(tick_text_color)
            painter.drawText(label_rect, Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignTop, label)

    def _draw_entry_markers(
        self,
        painter: QPainter,
        plot_rect: QRectF,
        candles: list[ChartCandle],
        markers: list[ChartMarker],
        min_y: float,
        max_y: float,
    ) -> None:
        for index, marker in enumerate(markers):
            x = self._map_ts_to_x(int(marker.ts_ms), candles, plot_rect)
            y = self._map_y(float(marker.price), plot_rect, min_y, max_y)
            is_short = str(marker.side or "").strip().lower().startswith("short")
            self._draw_triangle_marker(painter, QPointF(x, y), 6.2, _COLOR_ENTRY, pointing_up=not is_short)
            self._draw_marker_tag(
                painter,
                plot_rect,
                x,
                y,
                str(marker.label or "E").strip()[:3] or "E",
                _COLOR_ENTRY,
                anchor_above=not is_short,
                variant=index,
            )

    def _draw_exit_markers(
        self,
        painter: QPainter,
        plot_rect: QRectF,
        candles: list[ChartCandle],
        markers: list[ChartMarker],
        min_y: float,
        max_y: float,
    ) -> None:
        for index, marker in enumerate(markers):
            x = self._map_ts_to_x(int(marker.ts_ms), candles, plot_rect)
            y = self._map_y(float(marker.price), plot_rect, min_y, max_y)
            self._draw_diamond_marker(painter, QPointF(x, y), 5.8, _COLOR_EXIT)
            self._draw_marker_tag(
                painter,
                plot_rect,
                x,
                y,
                str(marker.label or "X").strip()[:3] or "X",
                _COLOR_EXIT,
                anchor_above=bool(index % 2),
                variant=index,
            )

    def _draw_triangle_marker(self, painter: QPainter, center: QPointF, radius: float, color: QColor, *, pointing_up: bool) -> None:
        path = QPainterPath()
        if pointing_up:
            path.moveTo(center.x(), center.y() - radius)
            path.lineTo(center.x() - radius, center.y() + radius)
            path.lineTo(center.x() + radius, center.y() + radius)
        else:
            path.moveTo(center.x(), center.y() + radius)
            path.lineTo(center.x() - radius, center.y() - radius)
            path.lineTo(center.x() + radius, center.y() - radius)
        path.closeSubpath()
        fill = QColor(color)
        fill.setAlpha(235)
        painter.setPen(QPen(color, 1.2))
        painter.fillPath(path, fill)
        painter.drawPath(path)

    def _draw_diamond_marker(self, painter: QPainter, center: QPointF, radius: float, color: QColor) -> None:
        path = QPainterPath()
        path.moveTo(center.x(), center.y() - radius)
        path.lineTo(center.x() + radius, center.y())
        path.lineTo(center.x(), center.y() + radius)
        path.lineTo(center.x() - radius, center.y())
        path.closeSubpath()
        fill = QColor(color)
        fill.setAlpha(220)
        painter.setPen(QPen(color, 1.2))
        painter.fillPath(path, fill)
        painter.drawPath(path)
        cross_radius = radius * 0.55
        painter.drawLine(
            QPointF(center.x() - cross_radius, center.y() - cross_radius),
            QPointF(center.x() + cross_radius, center.y() + cross_radius),
        )
        painter.drawLine(
            QPointF(center.x() - cross_radius, center.y() + cross_radius),
            QPointF(center.x() + cross_radius, center.y() - cross_radius),
        )

    def _draw_marker_tag(
        self,
        painter: QPainter,
        plot_rect: QRectF,
        x: float,
        y: float,
        text: str,
        color: QColor,
        *,
        anchor_above: bool,
        variant: int = 0,
    ) -> None:
        label = str(text or "").strip()[:4]
        if not label:
            return
        metrics = QFontMetricsF(painter.font())
        label_width = max(16.0, metrics.horizontalAdvance(label) + 10.0)
        label_height = max(14.0, metrics.height() + 2.0)
        base_top = y - label_height - 14.0 if anchor_above else y + 12.0
        base_top += (-4.0 if anchor_above else 4.0) * float(variant % 2)
        label_rect = QRectF(x - (label_width / 2.0), base_top, label_width, label_height)
        label_rect = self._constrain_rect_to_bounds(label_rect, plot_rect.adjusted(2.0, 2.0, -2.0, -2.0))
        fill = QColor(_COLOR_PANEL)
        fill.setAlpha(236)
        painter.setPen(QPen(color, 1.0))
        painter.setBrush(fill)
        painter.drawRoundedRect(label_rect, 4.0, 4.0)
        painter.drawText(label_rect, Qt.AlignmentFlag.AlignCenter, label)

    def _draw_price_tag(
        self,
        painter: QPainter,
        plot_rect: QRectF,
        y: float,
        text: str,
        color: QColor,
        occupied_labels: list[QRectF] | None = None,
        *,
        emphasized: bool = False,
        allow_hide: bool = False,
    ) -> QRectF:
        metrics = QFontMetricsF(painter.font())
        label_width = max(44.0 if emphasized else 42.0, metrics.horizontalAdvance(text) + (18.0 if emphasized else 14.0))
        label_height = max(20.0 if emphasized else 18.0, metrics.height() + (6.0 if emphasized else 4.0))
        label_rect = QRectF(plot_rect.right() - label_width - 6.0, y - (label_height / 2.0), label_width, label_height)
        resolved_rect = self._resolve_label_overlap(
            label_rect,
            plot_rect.adjusted(2.0, 2.0, -2.0, -2.0),
            occupied_labels or [],
            allow_hide=allow_hide,
            min_gap=6.0 if emphasized else 5.0,
        )
        if resolved_rect is None:
            return QRectF()
        label_rect = resolved_rect
        fill = QColor(_COLOR_LABEL_BG)
        fill.setAlpha(246 if emphasized else 232)
        painter.setPen(QPen(color, 1.0))
        painter.setBrush(fill)
        painter.drawRoundedRect(label_rect, 4.0, 4.0)
        painter.drawText(label_rect, Qt.AlignmentFlag.AlignCenter, text)
        if occupied_labels is not None:
            occupied_labels.append(QRectF(label_rect))
        return label_rect

    def _resolve_label_overlap(
        self,
        label_rect: QRectF,
        bounds: QRectF,
        occupied: list[QRectF],
        *,
        allow_hide: bool = False,
        min_gap: float = 5.0,
    ) -> QRectF | None:
        if not occupied:
            return self._constrain_rect_to_bounds(label_rect, bounds)
        step = max(12.0, label_rect.height() + min_gap)
        best_candidate: QRectF | None = None
        best_score: float | None = None
        for offset in (0.0, -step, step, -(step * 2.0), step * 2.0, -(step * 3.0), step * 3.0, -(step * 4.0), step * 4.0):
            candidate = QRectF(label_rect)
            candidate.translate(0.0, offset)
            candidate = self._constrain_rect_to_bounds(candidate, bounds)
            probe = QRectF(candidate).adjusted(-2.0, -min_gap, 2.0, min_gap)
            overlap_count = sum(1 for item in occupied if probe.intersects(QRectF(item).adjusted(-2.0, -min_gap, 2.0, min_gap)))
            if overlap_count <= 0:
                return candidate
            score = (float(overlap_count) * 10_000.0) + abs(offset)
            if best_score is None or score < best_score:
                best_score = score
                best_candidate = candidate
        if allow_hide:
            return None
        return best_candidate or self._constrain_rect_to_bounds(label_rect, bounds)

    def _draw_price_tags(self, painter: QPainter, plot_rect: QRectF, requests: list[_PriceTagRequest]) -> None:
        occupied_labels: list[QRectF] = []
        for item in sorted(requests, key=lambda request: (-int(request.priority), float(request.y))):
            if not str(item.text or "").strip():
                continue
            self._draw_price_tag(
                painter,
                plot_rect,
                float(item.y),
                str(item.text),
                item.color,
                occupied_labels,
                emphasized=bool(item.emphasized),
                allow_hide=bool(item.allow_hide),
            )

    def _constrain_rect_to_bounds(self, rect: QRectF, bounds: QRectF) -> QRectF:
        out = QRectF(rect)
        if out.width() > bounds.width():
            out.setWidth(bounds.width())
        if out.height() > bounds.height():
            out.setHeight(bounds.height())
        if out.left() < bounds.left():
            out.moveLeft(bounds.left())
        if out.right() > bounds.right():
            out.moveLeft(max(bounds.left(), bounds.right() - out.width()))
        if out.top() < bounds.top():
            out.moveTop(bounds.top())
        if out.bottom() > bounds.bottom():
            out.moveTop(max(bounds.top(), bounds.bottom() - out.height()))
        return out

    def _draw_candle_overlay(self, painter: QPainter, rect: QRectF, state: LiveChartState | None) -> None:
        if state is None:
            return
        parts: list[str] = []
        if state.symbol:
            parts.append(state.symbol)
        if state.run_mode:
            parts.append(state.run_mode)
        if state.position.is_open and state.position.side:
            parts.append(state.position.side.upper())
        if state.position.is_open and state.position.unrealized_pnl is not None:
            pnl_text = _format_short_value(float(state.position.unrealized_pnl), span=abs(float(state.position.unrealized_pnl)))
            prefix = "+" if float(state.position.unrealized_pnl) > 0 else ""
            parts.append(f"UPNL {prefix}{pnl_text}")
        if not parts:
            return
        overlay_rect = QRectF(rect.left() + 14.0, rect.top() + 28.0, rect.width() - 28.0, 16.0)
        painter.setPen(_COLOR_MUTED)
        painter.drawText(overlay_rect, Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter, "  |  ".join(parts))

    def _draw_candle_footer(self, painter: QPainter, rect: QRectF, state: LiveChartState | None, candles: list[ChartCandle]) -> None:
        footer_rect = QRectF(rect.left() + 14.0, rect.bottom() - 24.0, rect.width() - 28.0, 16.0)
        painter.setPen(_COLOR_MUTED)
        left = _format_chart_time_label(candles[0].ts_ms) if candles else ""
        right = _format_chart_time_label(candles[-1].ts_ms) if candles else ""
        painter.drawText(footer_rect, Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter, left)
        painter.drawText(footer_rect, Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter, right)
        if state is not None and state.updated_at:
            center_rect = QRectF(footer_rect.left(), footer_rect.top(), footer_rect.width(), footer_rect.height())
            updated_label = _format_candle_updated_label(
                state.updated_at,
                candles[-1].ts_ms if candles else state.last_candle_ts_ms,
            )
            if updated_label:
                painter.drawText(center_rect, Qt.AlignmentFlag.AlignCenter, updated_label)

    def _candle_x_bounds(self, count: int, plot_rect: QRectF) -> tuple[float, float]:
        if count <= 0:
            return (plot_rect.left(), plot_rect.right())
        slot = plot_rect.width() / max(1.0, float(count))
        left_pad = max(4.0, min(plot_rect.width() * 0.04, slot * 0.45 + (plot_rect.width() * _CANDLE_LEFT_PAD_RATIO)))
        right_pad = max(18.0, min(plot_rect.width() * 0.14, max(slot * 0.9, plot_rect.width() * _CANDLE_RIGHT_PAD_RATIO)))
        left = plot_rect.left() + left_pad
        right = plot_rect.right() - right_pad
        if right <= left:
            return (plot_rect.left(), plot_rect.right())
        return (left, right)

    def _candle_slot_width(self, count: int, plot_rect: QRectF) -> float:
        if count <= 1:
            return max(8.0, min(16.0, plot_rect.width() * 0.4))
        left, right = self._candle_x_bounds(count, plot_rect)
        return max(1.0, (right - left) / float(count - 1))

    def _map_candle_index_to_x(self, index: int, count: int, plot_rect: QRectF) -> float:
        if count <= 1:
            left, right = self._candle_x_bounds(1, plot_rect)
            return (left + right) / 2.0
        left, right = self._candle_x_bounds(count, plot_rect)
        return left + ((right - left) * float(index) / float(count - 1))

    def _map_ts_to_x(self, ts_ms: int, candles: list[ChartCandle], plot_rect: QRectF) -> float:
        if not candles:
            return plot_rect.left()
        timestamps = [int(item.ts_ms) for item in candles]
        idx = bisect_left(timestamps, int(ts_ms))
        if idx <= 0:
            return self._map_candle_index_to_x(0, len(candles), plot_rect)
        if idx >= len(candles):
            return self._map_candle_index_to_x(len(candles) - 1, len(candles), plot_rect)
        prev_ts = timestamps[idx - 1]
        next_ts = timestamps[idx]
        prev_x = self._map_candle_index_to_x(idx - 1, len(candles), plot_rect)
        next_x = self._map_candle_index_to_x(idx, len(candles), plot_rect)
        if next_ts <= prev_ts:
            return prev_x
        ratio = max(0.0, min(1.0, (float(ts_ms) - float(prev_ts)) / float(next_ts - prev_ts)))
        return prev_x + ((next_x - prev_x) * ratio)


class ResultPanel(QWidget):
    refreshRequested = Signal()
    expandRequested = Signal()
    saveRequested = Signal()
    openFolderRequested = Signal()
    modeChanged = Signal(str)

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._data = ResultChartData()
        self._ui_texts: dict[str, str] = {}
        self.setObjectName("ResultPanel")
        self.setMinimumWidth(280)
        self.setStyleSheet(
            """
            QWidget#ResultPanel {
                background-color: #0a0c0f;
                border: 1px solid #1f242a;
                border-radius: 6px;
            }
            QLabel[resultKpi="true"] {
                background-color: #101419;
                border: 1px solid #20262d;
                border-radius: 4px;
                padding: 4px 8px;
            }
            QComboBox, QPushButton {
                border-radius: 4px;
            }
            """
        )

        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        self.setLayout(layout)

        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setFrameShape(QFrame.Shape.NoFrame)
        self.scroll_area.setStyleSheet("QScrollArea { border: none; background: transparent; }")
        layout.addWidget(self.scroll_area, 1)

        self.content = QWidget()
        self.content.setStyleSheet("background: transparent;")
        self.scroll_area.setWidget(self.content)

        content_layout = QVBoxLayout()
        content_layout.setContentsMargins(10, 10, 10, 10)
        content_layout.setSpacing(8)
        content_layout.setSizeConstraint(QLayout.SizeConstraint.SetMinimumSize)
        self.content.setLayout(content_layout)

        toolbar = QHBoxLayout()
        toolbar.setSpacing(6)
        self.mode_combo = QComboBox()
        self._rebuild_mode_combo()
        _apply_dark_combo_popup(self.mode_combo)
        self.btn_refresh = QPushButton("Refresh")
        self.btn_expand = QPushButton("Expand")
        self.btn_open_folder = QPushButton("Open Folder")
        toolbar.addWidget(self.mode_combo, 1)
        toolbar.addWidget(self.btn_refresh)
        toolbar.addWidget(self.btn_expand)
        content_layout.addLayout(toolbar)

        action_row = QHBoxLayout()
        action_row.setSpacing(6)
        self.btn_save = QPushButton("Save PNG")
        action_row.addStretch(1)
        action_row.addWidget(self.btn_open_folder)
        action_row.addWidget(self.btn_save)
        content_layout.addLayout(action_row)

        kpi_row = QHBoxLayout()
        kpi_row.setSpacing(6)
        self.kpi_net = self._make_kpi_label()
        self.kpi_max_dd = self._make_kpi_label()
        self.kpi_trades = self._make_kpi_label()
        kpi_row.addWidget(self.kpi_net)
        kpi_row.addWidget(self.kpi_max_dd)
        kpi_row.addWidget(self.kpi_trades)
        content_layout.addLayout(kpi_row)

        self.chart_widget = ResultChartWidget()
        content_layout.addWidget(self.chart_widget, 1)

        self.chart_widget.set_ui_texts(self._ui_texts)
        self.mode_combo.currentIndexChanged.connect(self._on_mode_changed)
        self.btn_refresh.clicked.connect(self.refreshRequested.emit)
        self.btn_expand.clicked.connect(self.expandRequested.emit)
        self.btn_open_folder.clicked.connect(self.openFolderRequested.emit)
        self.btn_save.clicked.connect(self.saveRequested.emit)

        self._apply_ui_texts()
        self._refresh_kpis()

    def _make_kpi_label(self) -> QLabel:
        label = QLabel()
        label.setProperty("resultKpi", True)
        label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        label.setMinimumHeight(54)
        label.setTextFormat(Qt.TextFormat.RichText)
        return label

    def _set_kpi(self, label: QLabel, *, title: str, value: str, color: QColor) -> None:
        label.setText(
            f"<div style='color:{_COLOR_MUTED.name()}; font-size:11px;'>{title}</div>"
            f"<div style='color:{color.name()}; font-size:16px; font-weight:700;'>{value}</div>"
        )

    def _text(self, key: str, default: str) -> str:
        return _lookup_ui_text(self._ui_texts, key, default)

    def _rebuild_mode_combo(self) -> None:
        current = normalize_chart_mode(self.mode_combo.currentData() or self.mode_combo.currentText() or CHART_MODE_EQUITY)
        self.mode_combo.blockSignals(True)
        self.mode_combo.clear()
        for mode in CHART_MODES:
            self.mode_combo.addItem(_display_chart_mode_text(mode, self._ui_texts), mode)
        idx = max(0, self.mode_combo.findData(current))
        self.mode_combo.setCurrentIndex(idx)
        self.mode_combo.blockSignals(False)

    def _apply_ui_texts(self) -> None:
        self._rebuild_mode_combo()
        self.btn_refresh.setText(self._text("action.refresh", "Refresh"))
        self.btn_expand.setText(self._text("action.expand", "Expand"))
        self.btn_open_folder.setText(self._text("action.open_folder", "Open Folder"))
        self.btn_save.setText(self._text("action.save_png", "Save PNG"))
        self.chart_widget.set_ui_texts(self._ui_texts)

    def set_ui_texts(self, texts: dict[str, str] | None) -> None:
        self._ui_texts = dict(texts or {})
        self._apply_ui_texts()
        self._refresh_kpis()
        self.update()

    def _refresh_kpis(self) -> None:
        self._set_kpi(
            self.kpi_net,
            title=self._text("result.kpi.net", "net"),
            value=_format_net_with_pct(self._data),
            color=_net_value_color(self._data.net_total),
        )
        self._set_kpi(
            self.kpi_max_dd,
            title=self._text("result.kpi.max_dd", "max_dd"),
            value=_format_drawdown_value(self._data.max_dd),
            color=_COLOR_DD,
        )
        self._set_kpi(
            self.kpi_trades,
            title=self._text("result.kpi.trades", "trades"),
            value=_format_trades_value(self._data.trades_count),
            color=_COLOR_TRADES,
        )

    def set_result_data(self, data: ResultChartData) -> None:
        self._data = data if isinstance(data, ResultChartData) else ResultChartData()
        self.chart_widget.set_result_data(self._data)
        self._refresh_kpis()

    def result_data(self) -> ResultChartData:
        return self._data

    def chart_mode(self) -> str:
        return normalize_chart_mode(self.mode_combo.currentData() or self.mode_combo.currentText())

    def set_chart_mode(self, mode: str) -> None:
        target = normalize_chart_mode(mode)
        idx = self.mode_combo.findData(target)
        self.mode_combo.setCurrentIndex(max(0, idx))

    def _on_mode_changed(self, _index: int) -> None:
        current = self.chart_mode()
        self.chart_widget.set_chart_mode(current)
        self.modeChanged.emit(current)
