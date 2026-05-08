# BUILD_ID: 2026-05-08_free_precomputed_signals_foundation_v1
from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

BUILD_ID = "2026-05-08_free_precomputed_signals_foundation_v1"
SCHEMA_VERSION = "lwf.precomputed.signal_tape.v1"
DD_SCHEMA_VERSION = "precomputed_signals_dd_v2"
DD_SCHEMA_VERSION_LEGACY_NORMALIZED = "precomputed_signals_dd_v2_legacy_normalized"
DD_SIGN_CONVENTION = "legacy_max_drawdown_signed_negative"
DD_DISPLAY_LABEL = "Max DD (abs, display)"
MAX_DRAWDOWN_LEGACY_NOTE = "max_drawdown is signed negative legacy compatibility field"
DEFAULT_PRODUCT = "free"

SAFETY_SCOPE = {
    "research_only": True,
    "paper_live_order_execution": False,
    "contains_api_key": False,
    "contains_secret": False,
    "contains_order_id": False,
}

TRADE_COLUMNS = [
    "trade_id",
    "symbol",
    "side",
    "entry_signal_ts_ms",
    "entry_bar_ts_ms",
    "entry_exec_ts_ms",
    "entry_reason",
    "entry_regime",
    "entry_direction",
    "entry_signal_path",
    "entry_raw",
    "entry_exec",
    "initial_stop",
    "initial_take_profit",
    "exit_signal_ts_ms",
    "exit_bar_ts_ms",
    "exit_exec_ts_ms",
    "exit_reason",
    "exit_raw",
    "exit_exec",
    "bars_held",
    "qty",
    "entry_fee_rate",
    "exit_fee_rate",
    "entry_notional",
    "exit_notional",
    "gross_pnl",
    "fee",
    "net",
    "source_equity_before",
    "source_equity_after",
    "source_peak",
    "source_dd",
]

MANIFEST_REQUIRED_FIELDS = [
    "schema_version",
    "product",
    "producer_script",
    "producer_build_id",
    "created_at_utc",
    "symbol",
    "symbol_normalized",
    "entry_tf",
    "filter_tf",
    "since_ms",
    "until_ms",
    "dataset_id",
    "dataset_files_hash",
    "strategy_file_hash",
    "strategy_build_id",
    "config_file_hash",
    "signal_config_hash",
    "accounting_config_hash",
    "signal_set_id",
    "fee_model",
    "slippage_model",
    "sizing_mode",
    "position_side",
    "market_type",
    "safety_scope",
    "files",
    "counts",
    "parity",
]

MANIFEST_REQUIRED_FILE_KEYS = ("trades_csv", "summary_json")

FORBIDDEN_TERMS = [
    "MEXC_API_KEY",
    "MEXC_API_SECRET",
    "apiKey",
    "apiSecret",
    "secret",
    "token",
    "authorization",
    "bearer",
    "order_id",
    "raw_order",
    "balance",
    "private_key",
    "raw_billing",
]

_ALLOWED_SAFETY_FLAGS = {
    ("safety_scope", "contains_api_key"),
    ("safety_scope", "contains_secret"),
    ("safety_scope", "contains_order_id"),
}


class SignalTapeError(ValueError):
    pass


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def resolve_precomputed_signals_root(env: Mapping[str, str] | None = None) -> Path:
    values = os.environ if env is None else env
    explicit = str(values.get("LWF_PRECOMPUTED_SIGNALS_ROOT", "") or "").strip()
    if explicit:
        return Path(os.path.expandvars(os.path.expanduser(explicit)))

    local_app_data = str(values.get("LOCALAPPDATA", "") or "").strip()
    if local_app_data:
        return Path(local_app_data) / "LoneWolfFang" / "data" / "precomputed_signals"

    return Path.home() / ".lonewolffang" / "data" / "precomputed_signals"


def normalize_symbol(symbol: str) -> str:
    normalized = re.sub(r"[/_\-\s]+", "", str(symbol or "")).upper()
    if not normalized or not re.fullmatch(r"[A-Z0-9]+", normalized):
        raise SignalTapeError(f"invalid symbol: {symbol!r}")
    return normalized


def _validate_component(name: str, value: str) -> str:
    text = str(value or "").strip()
    if not text or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", text):
        raise SignalTapeError(f"invalid {name}: {value!r}")
    return text


def signal_tape_dir(
    *,
    product: str = DEFAULT_PRODUCT,
    symbol: str,
    entry_tf: str,
    filter_tf: str,
    signal_set_id: str,
    root: str | Path | None = None,
) -> Path:
    base = Path(root) if root is not None else resolve_precomputed_signals_root()
    product_part = _validate_component("product", product)
    entry_part = _validate_component("entry_tf", entry_tf)
    filter_part = _validate_component("filter_tf", filter_tf)
    signal_part = _validate_component("signal_set_id", signal_set_id)
    return base / product_part / normalize_symbol(symbol) / f"{entry_part}_{filter_part}" / signal_part


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def stable_json_hash(payload: Any) -> str:
    return sha256_text(json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")))


def read_csv_rows(path: str | Path) -> tuple[list[str], list[dict[str, str]]]:
    with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = [dict(row) for row in reader]
        return list(reader.fieldnames or []), rows


def _lower_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key).lower(): value for key, value in row.items()}


def _first(row: Mapping[str, Any], *names: str, default: Any = "") -> Any:
    lowered = _lower_row(row)
    for name in names:
        value = lowered.get(name.lower())
        if value not in (None, ""):
            return value
    return default


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _finite_float(value: Any, default: float = 0.0) -> float:
    parsed = _to_float(value, default)
    return parsed if math.isfinite(parsed) else float(default)


def _ts_ms_to_iso_utc(ts_ms: int) -> str:
    if int(ts_ms) <= 0:
        return ""
    try:
        return datetime.fromtimestamp(int(ts_ms) / 1000.0, timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except (OverflowError, OSError, ValueError):
        return ""


def _equity_point(row: Any) -> tuple[int, float] | None:
    if isinstance(row, Mapping):
        equity_raw = _first(row, "equity", "mtm_equity", "source_equity_after", default=None)
        if equity_raw is None:
            return None
        return (
            _to_int(_first(row, "ts_ms", "ts", "exit_exec_ts_ms", "close_ts_ms", default=0)),
            _finite_float(equity_raw),
        )
    if isinstance(row, (list, tuple)) and len(row) >= 2:
        return (_to_int(row[0]), _finite_float(row[1]))
    return None


def compute_drawdown_metrics(equity_curve: Iterable[Any]) -> dict[str, Any]:
    peak_equity: float | None = None
    peak_at_max_dd = 0.0
    trough_at_max_dd = 0.0
    max_dd_ts_ms = 0
    max_dd_signed = 0.0
    saw_point = False

    for row in equity_curve:
        point = _equity_point(row)
        if point is None:
            continue
        ts_ms, equity = point
        if peak_equity is None:
            peak_equity = float(equity)
            peak_at_max_dd = float(equity)
            trough_at_max_dd = float(equity)
            max_dd_ts_ms = int(ts_ms)
            saw_point = True
        if float(equity) > float(peak_equity):
            peak_equity = float(equity)
        signed_dd = float(equity) - float(peak_equity)
        if signed_dd < max_dd_signed:
            max_dd_signed = float(signed_dd)
            peak_at_max_dd = float(peak_equity)
            trough_at_max_dd = float(equity)
            max_dd_ts_ms = int(ts_ms)

    if not saw_point:
        peak_at_max_dd = 0.0
        trough_at_max_dd = 0.0
        max_dd_ts_ms = 0

    max_dd_abs = abs(float(max_dd_signed))
    max_dd_pct = max_dd_abs / peak_at_max_dd if peak_at_max_dd > 0.0 else 0.0
    return {
        "dd_schema_version": DD_SCHEMA_VERSION,
        "dd_sign_convention": DD_SIGN_CONVENTION,
        "max_drawdown": float(max_dd_signed),
        "max_dd_signed": float(max_dd_signed),
        "max_dd_abs": float(max_dd_abs),
        "max_dd_pct": float(max_dd_pct),
        "max_dd_peak_equity": float(peak_at_max_dd),
        "max_dd_trough_equity": float(trough_at_max_dd),
        "max_dd_ts_ms": int(max_dd_ts_ms),
        "max_dd_ts_iso": _ts_ms_to_iso_utc(max_dd_ts_ms),
    }


def validate_drawdown_summary(summary: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(summary)
    signed = _finite_float(payload.get("max_dd_signed"))
    legacy = _finite_float(payload.get("max_drawdown"))
    abs_dd = _finite_float(payload.get("max_dd_abs"))
    pct = _finite_float(payload.get("max_dd_pct"))
    if signed > 0.0 or legacy > 0.0:
        raise SignalTapeError("drawdown summary must keep max_drawdown/max_dd_signed signed negative")
    if abs(legacy - signed) > 1e-9:
        raise SignalTapeError("drawdown summary max_drawdown must equal max_dd_signed")
    if abs_dd < 0.0:
        raise SignalTapeError("drawdown summary max_dd_abs must be non-negative")
    if pct < 0.0:
        raise SignalTapeError("drawdown summary max_dd_pct must be non-negative")
    if abs(abs_dd - abs(signed)) > 1e-9:
        raise SignalTapeError("drawdown summary max_dd_abs must equal abs(max_dd_signed)")
    if str(payload.get("dd_sign_convention") or "") != DD_SIGN_CONVENTION:
        raise SignalTapeError("drawdown summary dd_sign_convention invalid")
    if not str(payload.get("dd_schema_version") or "").startswith(DD_SCHEMA_VERSION):
        raise SignalTapeError("drawdown summary dd_schema_version invalid")
    return payload


def normalize_drawdown_summary(summary: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(summary)
    has_v2_fields = all(
        field in payload
        for field in (
            "dd_schema_version",
            "dd_sign_convention",
            "max_drawdown",
            "max_dd_signed",
            "max_dd_abs",
            "max_dd_pct",
            "max_dd_peak_equity",
            "max_dd_trough_equity",
            "max_dd_ts_ms",
            "max_dd_ts_iso",
        )
    )
    if has_v2_fields:
        return validate_drawdown_summary(payload)

    signed = _finite_float(payload.get("max_drawdown", payload.get("max_dd_signed", 0.0)))
    if signed > 0.0:
        raise SignalTapeError("legacy drawdown summary positive max_drawdown is unsafe")

    payload["dd_schema_version"] = DD_SCHEMA_VERSION_LEGACY_NORMALIZED
    payload["dd_sign_convention"] = DD_SIGN_CONVENTION
    payload["max_drawdown"] = float(signed)
    payload["max_dd_signed"] = float(signed)
    payload["max_dd_abs"] = abs(float(signed))
    payload["max_dd_pct"] = max(0.0, _finite_float(payload.get("max_dd_pct"), 0.0))
    payload["max_dd_peak_equity"] = _finite_float(payload.get("max_dd_peak_equity"), 0.0)
    payload["max_dd_trough_equity"] = _finite_float(payload.get("max_dd_trough_equity"), 0.0)
    payload["max_dd_ts_ms"] = _to_int(payload.get("max_dd_ts_ms"), 0)
    payload["max_dd_ts_iso"] = _to_text(payload.get("max_dd_ts_iso"))
    return validate_drawdown_summary(payload)


def attach_drawdown_display_fields(summary: Mapping[str, Any]) -> dict[str, Any]:
    payload = normalize_drawdown_summary(summary)
    payload["max_dd_display_abs"] = float(payload["max_dd_abs"])
    payload["max_dd_display_pct"] = float(payload["max_dd_pct"])
    payload["max_dd_display_label"] = DD_DISPLAY_LABEL
    payload["max_drawdown_legacy_note"] = MAX_DRAWDOWN_LEGACY_NOTE
    return validate_drawdown_display_summary(payload)


def validate_drawdown_display_summary(summary: Mapping[str, Any]) -> dict[str, Any]:
    payload = normalize_drawdown_summary(summary)
    missing = [
        field
        for field in (
            "max_dd_display_abs",
            "max_dd_display_pct",
            "max_dd_display_label",
            "max_drawdown_legacy_note",
        )
        if field not in payload
    ]
    if missing:
        raise SignalTapeError("drawdown display summary missing fields: " + ", ".join(missing))
    display_abs = _finite_float(payload.get("max_dd_display_abs"))
    display_pct = _finite_float(payload.get("max_dd_display_pct"))
    if display_abs < 0.0 or display_pct < 0.0:
        raise SignalTapeError("drawdown display fields must be non-negative")
    if abs(display_abs - _finite_float(payload.get("max_dd_abs"))) > 1e-9:
        raise SignalTapeError("drawdown display abs must equal max_dd_abs")
    if abs(display_pct - _finite_float(payload.get("max_dd_pct"))) > 1e-12:
        raise SignalTapeError("drawdown display pct must equal max_dd_pct")
    if str(payload.get("max_dd_display_label") or "") != DD_DISPLAY_LABEL:
        raise SignalTapeError("drawdown display label invalid")
    if str(payload.get("max_drawdown_legacy_note") or "") != MAX_DRAWDOWN_LEGACY_NOTE:
        raise SignalTapeError("drawdown display legacy note invalid")
    return payload


def build_drawdown_display_summary(summary: Mapping[str, Any]) -> dict[str, Any]:
    return attach_drawdown_display_fields(summary)


def drawdown_report_fields(summary: Mapping[str, Any]) -> dict[str, Any]:
    payload = build_drawdown_display_summary(summary)
    return {
        "max_drawdown": payload["max_drawdown"],
        "max_dd_signed": payload["max_dd_signed"],
        "max_dd_abs": payload["max_dd_abs"],
        "max_dd_pct": payload["max_dd_pct"],
        "max_dd_display_abs": payload["max_dd_display_abs"],
        "max_dd_display_pct": payload["max_dd_display_pct"],
        "max_dd_display_label": payload["max_dd_display_label"],
        "max_drawdown_legacy_note": payload["max_drawdown_legacy_note"],
    }


def canonicalize_trade_rows(
    source_rows: Iterable[Mapping[str, Any]],
    *,
    symbol: str,
    initial_equity: float = 300000.0,
    source_name: str = "synthetic.precomputed_signals_fixture",
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    running_equity = float(initial_equity)
    peak = float(initial_equity)

    for index, row in enumerate(source_rows, start=1):
        entry_ts = _to_int(_first(row, "entry_signal_ts_ms", "entry_ts_ms", "opened_ts", "entry_bar_ts_ms", "ts"))
        exit_ts = _to_int(_first(row, "exit_signal_ts_ms", "close_ts_ms", "exit_ts_ms", "ts"), entry_ts)
        entry_exec = _to_float(_first(row, "entry_exec", "entry_price", "open_price", "start_price"))
        exit_exec = _to_float(_first(row, "exit_exec", "exit_price", "close_price"))
        qty = _to_float(_first(row, "qty", "quantity", "qty_init"))
        side = _to_text(_first(row, "side", "direction", default="long")).lower() or "long"
        entry_notional = abs(entry_exec * qty)
        exit_notional = abs(exit_exec * qty)
        source_fee = _to_float(_first(row, "fee", "total_fee"))
        inferred_rate = source_fee / (entry_notional + exit_notional) if (entry_notional + exit_notional) > 0 else 0.0
        entry_fee_rate = _to_float(_first(row, "entry_fee_rate", "maker_fee_rate"), inferred_rate)
        exit_fee_rate = _to_float(_first(row, "exit_fee_rate", "taker_fee_rate"), inferred_rate)
        gross_pnl = _to_float(
            _first(row, "gross_pnl", "pnl"),
            (exit_exec - entry_exec) * qty if side == "long" else 0.0,
        )
        fee = _to_float(
            _first(row, "fee"),
            entry_notional * entry_fee_rate + exit_notional * exit_fee_rate,
        )
        net = _to_float(_first(row, "net"), gross_pnl - fee)
        source_before = running_equity
        source_after = _to_float(_first(row, "source_equity_after", "equity_after"), source_before + net)
        running_equity = source_after
        peak = max(peak, source_after)
        source_dd = source_after - peak

        normalized.append({
            "trade_id": _to_text(_first(row, "trade_id", default=index)) or str(index),
            "symbol": _to_text(_first(row, "symbol", default=symbol)) or symbol,
            "side": side,
            "entry_signal_ts_ms": entry_ts,
            "entry_bar_ts_ms": _to_int(_first(row, "entry_bar_ts_ms", "entry_ts_ms", "opened_ts"), entry_ts),
            "entry_exec_ts_ms": _to_int(_first(row, "entry_exec_ts_ms", "entry_ts_ms", "opened_ts"), entry_ts),
            "entry_reason": _to_text(_first(row, "entry_reason", "open_reason")),
            "entry_regime": _to_text(_first(row, "entry_regime", "regime")),
            "entry_direction": _to_text(_first(row, "entry_direction", "direction", default=side)) or side,
            "entry_signal_path": _to_text(_first(row, "entry_signal_path", default=source_name)),
            "entry_raw": _to_float(_first(row, "entry_raw", "start_price", "entry_exec"), entry_exec),
            "entry_exec": entry_exec,
            "initial_stop": _to_float(_first(row, "initial_stop", "init_stop", "stop")),
            "initial_take_profit": _to_float(_first(row, "initial_take_profit", "tp", "take_profit")),
            "exit_signal_ts_ms": exit_ts,
            "exit_bar_ts_ms": _to_int(_first(row, "exit_bar_ts_ms", "close_ts_ms", "ts"), exit_ts),
            "exit_exec_ts_ms": _to_int(_first(row, "exit_exec_ts_ms", "close_ts_ms", "ts"), exit_ts),
            "exit_reason": _to_text(_first(row, "exit_reason", "reason", "close_reason")),
            "exit_raw": _to_float(_first(row, "exit_raw", "exit_exec"), exit_exec),
            "exit_exec": exit_exec,
            "bars_held": _to_int(_first(row, "bars_held", "bars_to_exit")),
            "qty": qty,
            "entry_fee_rate": entry_fee_rate,
            "exit_fee_rate": exit_fee_rate,
            "entry_notional": entry_notional,
            "exit_notional": exit_notional,
            "gross_pnl": gross_pnl,
            "fee": fee,
            "net": net,
            "source_equity_before": source_before,
            "source_equity_after": source_after,
            "source_peak": peak,
            "source_dd": source_dd,
        })

    return normalized


def equity_reference_from_trades(trades: Iterable[Mapping[str, Any]], initial_equity: float) -> list[dict[str, Any]]:
    rows = [{
        "trade_id": "initial",
        "ts_ms": 0,
        "equity": float(initial_equity),
        "peak": float(initial_equity),
        "dd": 0.0,
        "net": 0.0,
    }]
    for trade in trades:
        rows.append({
            "trade_id": _to_text(trade.get("trade_id")),
            "ts_ms": _to_int(trade.get("exit_exec_ts_ms")),
            "equity": _to_float(trade.get("source_equity_after")),
            "peak": _to_float(trade.get("source_peak")),
            "dd": _to_float(trade.get("source_dd")),
            "net": _to_float(trade.get("net")),
        })
    return rows


def build_summary(trades: Iterable[Mapping[str, Any]], *, initial_equity: float) -> dict[str, Any]:
    trade_list = list(trades)
    net_total = sum(_to_float(row.get("net")) for row in trade_list)
    gross_total = sum(_to_float(row.get("gross_pnl")) for row in trade_list)
    fee_total = sum(_to_float(row.get("fee")) for row in trade_list)
    final_equity = float(initial_equity) + net_total
    dd_metrics = compute_drawdown_metrics(equity_reference_from_trades(trade_list, initial_equity))
    return attach_drawdown_display_fields({
        "schema_version": SCHEMA_VERSION,
        "trade_count": len(trade_list),
        "initial_equity": float(initial_equity),
        "final_equity": final_equity,
        "net_total": net_total,
        "gross_pnl_total": gross_total,
        "fee_total": fee_total,
        **dd_metrics,
        "win_count": sum(1 for row in trade_list if _to_float(row.get("net")) > 0),
        "loss_count": sum(1 for row in trade_list if _to_float(row.get("net")) < 0),
        "research_only": True,
        "paper_live_order_execution": False,
        "redaction": "safe-summary-v1",
    })


def build_manifest_template(
    *,
    product: str = DEFAULT_PRODUCT,
    producer_script: str,
    producer_build_id: str,
    symbol: str,
    entry_tf: str,
    filter_tf: str,
    since_ms: int,
    until_ms: int,
    dataset_id: str,
    dataset_files_hash: str,
    strategy_file_hash: str,
    strategy_build_id: str,
    config_file_hash: str,
    signal_config_hash: str,
    accounting_config_hash: str,
    signal_set_id: str,
    sizing_mode: str = "source_backtest",
    position_side: str = "long",
    market_type: str = "spot",
    fee_model: Mapping[str, Any] | None = None,
    slippage_model: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "product": product,
        "producer_script": producer_script,
        "producer_build_id": producer_build_id,
        "created_at_utc": utc_now_iso(),
        "symbol": symbol,
        "symbol_normalized": normalize_symbol(symbol),
        "entry_tf": entry_tf,
        "filter_tf": filter_tf,
        "since_ms": int(since_ms),
        "until_ms": int(until_ms),
        "dataset_id": dataset_id,
        "dataset_files_hash": dataset_files_hash,
        "strategy_file_hash": strategy_file_hash,
        "strategy_build_id": strategy_build_id,
        "config_file_hash": config_file_hash,
        "signal_config_hash": signal_config_hash,
        "accounting_config_hash": accounting_config_hash,
        "signal_set_id": signal_set_id,
        "fee_model": dict(fee_model or {"maker_fee_rate": 0.0001, "taker_fee_rate": 0.0002}),
        "slippage_model": dict(slippage_model or {"source": "deferred_producer", "fast_path_additional_slippage": 0.0}),
        "sizing_mode": sizing_mode,
        "position_side": position_side,
        "market_type": market_type,
        "safety_scope": dict(SAFETY_SCOPE),
        "files": {
            "trades_csv": {"name": "trades.csv"},
            "summary_json": {"name": "summary.json"},
        },
        "counts": {},
        "parity": {},
    }


def _is_benign_negative_text(text: str, lowered_term: str) -> bool:
    lowered = text.lower()
    if lowered_term not in {"secret", "token", "authorization", "bearer"}:
        return False
    benign_patterns = (
        rf"\bno\s+{re.escape(lowered_term)}\b",
        rf"\bwithout\s+{re.escape(lowered_term)}\b",
        rf"\bnot\s+containing\s+{re.escape(lowered_term)}\b",
        rf"\bdoes\s+not\s+contain\s+{re.escape(lowered_term)}\b",
    )
    return any(re.search(pattern, lowered) for pattern in benign_patterns)


def validate_no_secret_payload(payload: Any) -> None:
    violations: list[str] = []
    forbidden_lower = [(term, term.lower()) for term in FORBIDDEN_TERMS]

    def visit(value: Any, path: tuple[str, ...]) -> None:
        if isinstance(value, Mapping):
            for key, nested in value.items():
                key_text = str(key)
                key_path = path + (key_text,)
                allowed_safety_flag = key_path in _ALLOWED_SAFETY_FLAGS and nested is False
                if not allowed_safety_flag:
                    key_lower = key_text.lower()
                    for original, lowered in forbidden_lower:
                        if lowered in key_lower:
                            violations.append(".".join(key_path) + f" contains {original}")
                visit(nested, key_path)
            return
        if isinstance(value, (list, tuple)):
            for index, nested in enumerate(value):
                visit(nested, path + (str(index),))
            return
        if value is None:
            return
        text = str(value)
        text_lower = text.lower()
        for original, lowered in forbidden_lower:
            if lowered in text_lower and not _is_benign_negative_text(text, lowered):
                violations.append(".".join(path) + f" contains {original}")

    visit(payload, ())
    if violations:
        raise SignalTapeError("unsafe signal tape payload: " + "; ".join(violations[:10]))


def validate_manifest(
    manifest: Mapping[str, Any],
    *,
    expected_product: str | None = DEFAULT_PRODUCT,
    expected_symbol: str | None = None,
    expected_entry_tf: str | None = None,
    expected_filter_tf: str | None = None,
    expected_signal_set_id: str | None = None,
) -> dict[str, Any]:
    missing = [field for field in MANIFEST_REQUIRED_FIELDS if field not in manifest]
    if missing:
        raise SignalTapeError("manifest missing required fields: " + ", ".join(missing))
    validate_no_secret_payload(manifest)

    product = str(manifest["product"])
    symbol_normalized = str(manifest["symbol_normalized"])
    if expected_product is not None and product != expected_product:
        raise SignalTapeError(f"manifest product mismatch: {product!r} != {expected_product!r}")
    if expected_symbol is not None and symbol_normalized != normalize_symbol(expected_symbol):
        raise SignalTapeError("manifest symbol mismatch")
    if expected_entry_tf is not None and str(manifest["entry_tf"]) != expected_entry_tf:
        raise SignalTapeError("manifest entry_tf mismatch")
    if expected_filter_tf is not None and str(manifest["filter_tf"]) != expected_filter_tf:
        raise SignalTapeError("manifest filter_tf mismatch")
    if expected_signal_set_id is not None and str(manifest["signal_set_id"]) != expected_signal_set_id:
        raise SignalTapeError("manifest signal_set_id mismatch")
    if symbol_normalized != normalize_symbol(str(manifest["symbol"])):
        raise SignalTapeError("manifest symbol_normalized does not match symbol")
    if dict(manifest["safety_scope"]) != SAFETY_SCOPE:
        raise SignalTapeError("manifest safety_scope must be research-only and no-order")

    files = dict(manifest["files"])
    for file_key in MANIFEST_REQUIRED_FILE_KEYS:
        if file_key not in files:
            raise SignalTapeError(f"manifest files missing {file_key}")
    return dict(manifest)


def validate_summary(summary: Mapping[str, Any], *, require_safety_flags: bool = False) -> dict[str, Any]:
    validate_no_secret_payload(summary)
    payload = attach_drawdown_display_fields(summary)
    if require_safety_flags or "research_only" in payload:
        if payload.get("research_only") is not True:
            raise SignalTapeError("summary research_only must be true")
    if require_safety_flags or "paper_live_order_execution" in payload:
        if payload.get("paper_live_order_execution") is not False:
            raise SignalTapeError("summary paper_live_order_execution must be false")
    return payload


def load_manifest(signal_dir: str | Path, *, expected_product: str | None = DEFAULT_PRODUCT) -> dict[str, Any]:
    path = Path(signal_dir) / "manifest.json"
    return validate_manifest(json.loads(path.read_text(encoding="utf-8")), expected_product=expected_product)


def load_summary(signal_dir: str | Path, *, require_safety_flags: bool = True) -> dict[str, Any]:
    path = Path(signal_dir) / "summary.json"
    return validate_summary(json.loads(path.read_text(encoding="utf-8")), require_safety_flags=require_safety_flags)


def read_trades_csv(signal_dir_or_path: str | Path) -> list[dict[str, str]]:
    path = Path(signal_dir_or_path)
    if path.is_dir():
        path = path / "trades.csv"
    columns, rows = read_csv_rows(path)
    missing_columns = [column for column in TRADE_COLUMNS if column not in columns]
    if missing_columns:
        raise SignalTapeError("trades.csv missing required columns: " + ", ".join(missing_columns))
    validate_no_secret_payload(rows)
    return rows


def load_signal_tape(
    signal_dir: str | Path,
    *,
    expected_product: str | None = DEFAULT_PRODUCT,
    expected_symbol: str | None = None,
    expected_entry_tf: str | None = None,
    expected_filter_tf: str | None = None,
    expected_signal_set_id: str | None = None,
) -> dict[str, Any]:
    manifest = validate_manifest(
        json.loads((Path(signal_dir) / "manifest.json").read_text(encoding="utf-8")),
        expected_product=expected_product,
        expected_symbol=expected_symbol,
        expected_entry_tf=expected_entry_tf,
        expected_filter_tf=expected_filter_tf,
        expected_signal_set_id=expected_signal_set_id,
    )
    summary = load_summary(signal_dir)
    trades = read_trades_csv(signal_dir)
    return {"manifest": manifest, "summary": summary, "trades": trades}
