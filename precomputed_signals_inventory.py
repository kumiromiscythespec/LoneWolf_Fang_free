# BUILD_ID: 2026-05-08_free_precomputed_signal_inventory_v1
from __future__ import annotations

import argparse
import csv
import io
import json
import os
from pathlib import Path
from typing import Any, Mapping

import signal_tape as tape

BUILD_ID = "2026-05-08_free_precomputed_signal_inventory_v1"
INVENTORY_SCHEMA_VERSION = "lwf.precomputed.signal_tape.inventory.v1"
PRODUCT = tape.DEFAULT_PRODUCT
DEFAULT_LIMIT = 1000

VALID_ROW_FIELDS = [
    "product",
    "symbol",
    "symbol_normalized",
    "entry_tf",
    "filter_tf",
    "signal_set_id",
    "signal_dir",
    "created_at_utc",
    "dataset_id",
    "since_ms",
    "until_ms",
    "trade_count",
    "net_total",
    "final_equity",
    "max_drawdown",
    "max_dd_signed",
    "max_dd_abs",
    "max_dd_pct",
    "max_dd_display_abs",
    "max_dd_display_pct",
    "max_dd_display_label",
    "max_drawdown_legacy_note",
    "safety_research_only",
    "safety_paper_live_order_execution",
    "tape_files_present",
    "manifest_sha256",
    "summary_sha256",
    "trades_csv_sha256_from_manifest",
    "status",
    "status_reason",
]

INVALID_ROW_FIELDS = [
    "signal_dir",
    "status",
    "status_reason",
    "safe_error_code",
]

CSV_FIELDS = [*VALID_ROW_FIELDS, "safe_error_code"]


class InventoryError(ValueError):
    pass


def _fail(message: str) -> None:
    raise SystemExit(f"fail closed: {message}")


def resolve_inventory_root(root: str | Path | None = None, *, env: Mapping[str, str] | None = None) -> Path:
    text = str(root or "").strip()
    if text:
        return Path(os.path.expandvars(os.path.expanduser(text)))
    return tape.resolve_precomputed_signals_root(env=env)


def _safe_int(value: Any) -> int:
    try:
        if value in (None, ""):
            return 0
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _safe_float(value: Any) -> float:
    try:
        if value in (None, ""):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise InventoryError("invalid json") from exc
    except OSError as exc:
        raise InventoryError("read error") from exc


def _invalid_row(signal_dir: Path, reason: str, code: str) -> dict[str, Any]:
    return {
        "signal_dir": str(signal_dir),
        "status": "invalid",
        "status_reason": reason,
        "safe_error_code": code,
    }


def _error_code_and_reason(stage: str, exc: BaseException) -> tuple[str, str]:
    message = str(exc).lower()
    if isinstance(exc, InventoryError):
        if "invalid json" in message:
            return "INVALID_JSON", f"{stage} is not valid JSON"
        return "READ_ERROR", f"{stage} could not be read"
    if "unsafe signal tape payload" in message:
        return "FORBIDDEN_FIELD", f"{stage} contains forbidden private/runtime metadata"
    if "positive max_drawdown" in message or "signed negative" in message:
        return "POSITIVE_LEGACY_DD", f"{stage} contains invalid positive legacy drawdown"
    if "safety_scope" in message or "research_only" in message or "paper_live_order_execution" in message:
        return "UNSAFE_SAFETY_SCOPE", f"{stage} is outside the research-only no-order safety scope"
    if "product mismatch" in message:
        return "PRODUCT_MISMATCH", f"{stage} product is not supported"
    if "missing required fields" in message:
        return "MISSING_REQUIRED_FIELD", f"{stage} is missing required safe metadata"
    if "missing" in message:
        return "MISSING_REQUIRED_FILE", f"{stage} is missing a required file reference"
    return "VALIDATION_ERROR", f"{stage} failed safe validation"


def discover_signal_tape_dirs(root: str | Path, *, product: str = PRODUCT, limit: int | None = None) -> list[Path]:
    product_text = str(product or PRODUCT).strip()
    if product_text != PRODUCT:
        _fail("--product must be free")

    product_root = Path(root) / PRODUCT
    if not product_root.is_dir():
        return []

    candidates: set[Path] = set()
    for filename in ("manifest.json", "summary.json", "trades.csv"):
        for path in product_root.rglob(filename):
            if path.is_file():
                candidates.add(path.parent)
                if limit is not None and len(candidates) >= int(limit):
                    break
        if limit is not None and len(candidates) >= int(limit):
            break
    return sorted(candidates, key=lambda path: str(path).lower())


def _manifest_trades_sha(manifest: Mapping[str, Any]) -> str:
    files = dict(manifest.get("files") or {})
    trades = files.get("trades_csv") or {}
    if isinstance(trades, Mapping):
        return str(trades.get("sha256") or "")
    return ""


def _file_presence(signal_dir: Path) -> dict[str, bool]:
    return {
        "manifest_json": (signal_dir / "manifest.json").is_file(),
        "summary_json": (signal_dir / "summary.json").is_file(),
        "trades_csv": (signal_dir / "trades.csv").is_file(),
    }


def build_signal_tape_inventory_row(signal_dir: str | Path, *, product: str = PRODUCT) -> dict[str, Any]:
    signal_path = Path(signal_dir)
    manifest_path = signal_path / "manifest.json"
    summary_path = signal_path / "summary.json"
    trades_path = signal_path / "trades.csv"

    if not manifest_path.is_file():
        return _invalid_row(signal_path, "manifest.json is missing", "MISSING_MANIFEST")
    if not summary_path.is_file():
        return _invalid_row(signal_path, "summary.json is missing", "MISSING_SUMMARY")
    if not trades_path.is_file():
        return _invalid_row(signal_path, "trades.csv is missing", "MISSING_TRADES_CSV")

    try:
        manifest = tape.validate_manifest(_read_json(manifest_path), expected_product=product)
    except (InventoryError, tape.SignalTapeError) as exc:
        code, reason = _error_code_and_reason("manifest.json", exc)
        return _invalid_row(signal_path, reason, code)

    try:
        summary = tape.validate_summary(_read_json(summary_path), require_safety_flags=True)
    except (InventoryError, tape.SignalTapeError) as exc:
        code, reason = _error_code_and_reason("summary.json", exc)
        return _invalid_row(signal_path, reason, code)

    safety = dict(manifest.get("safety_scope") or {})
    return {
        "product": str(manifest.get("product") or ""),
        "symbol": str(manifest.get("symbol") or ""),
        "symbol_normalized": str(manifest.get("symbol_normalized") or ""),
        "entry_tf": str(manifest.get("entry_tf") or ""),
        "filter_tf": str(manifest.get("filter_tf") or ""),
        "signal_set_id": str(manifest.get("signal_set_id") or ""),
        "signal_dir": str(signal_path),
        "created_at_utc": str(manifest.get("created_at_utc") or ""),
        "dataset_id": str(manifest.get("dataset_id") or ""),
        "since_ms": _safe_int(manifest.get("since_ms")),
        "until_ms": _safe_int(manifest.get("until_ms")),
        "trade_count": _safe_int(summary.get("trade_count", dict(manifest.get("counts") or {}).get("trades"))),
        "net_total": _safe_float(summary.get("net_total")),
        "final_equity": _safe_float(summary.get("final_equity")),
        "max_drawdown": _safe_float(summary.get("max_drawdown")),
        "max_dd_signed": _safe_float(summary.get("max_dd_signed")),
        "max_dd_abs": _safe_float(summary.get("max_dd_abs")),
        "max_dd_pct": _safe_float(summary.get("max_dd_pct")),
        "max_dd_display_abs": _safe_float(summary.get("max_dd_display_abs")),
        "max_dd_display_pct": _safe_float(summary.get("max_dd_display_pct")),
        "max_dd_display_label": str(summary.get("max_dd_display_label") or ""),
        "max_drawdown_legacy_note": str(summary.get("max_drawdown_legacy_note") or ""),
        "safety_research_only": safety.get("research_only") is True,
        "safety_paper_live_order_execution": safety.get("paper_live_order_execution") is True,
        "tape_files_present": _file_presence(signal_path),
        "manifest_sha256": tape.sha256_file(manifest_path),
        "summary_sha256": tape.sha256_file(summary_path),
        "trades_csv_sha256_from_manifest": _manifest_trades_sha(manifest),
        "status": "valid",
        "status_reason": "ok",
    }


def _matches_filters(
    row: Mapping[str, Any],
    *,
    symbol: str | None = None,
    entry_tf: str | None = None,
    filter_tf: str | None = None,
    signal_set_id: str | None = None,
) -> bool:
    if row.get("status") != "valid":
        return True
    if symbol and str(row.get("symbol_normalized") or "") != tape.normalize_symbol(symbol):
        return False
    if entry_tf and str(row.get("entry_tf") or "") != str(entry_tf):
        return False
    if filter_tf and str(row.get("filter_tf") or "") != str(filter_tf):
        return False
    if signal_set_id and str(row.get("signal_set_id") or "") != str(signal_set_id):
        return False
    return True


def _sort_rows(rows: list[dict[str, Any]], sort_key: str) -> list[dict[str, Any]]:
    if sort_key not in {"created_at_utc", "symbol", "net_total", "final_equity"}:
        raise InventoryError("unsupported sort")

    def key(row: Mapping[str, Any]) -> tuple[int, Any, str]:
        invalid = 1 if row.get("status") != "valid" else 0
        if sort_key == "symbol":
            value: Any = str(row.get("symbol_normalized") or "")
        elif sort_key in {"net_total", "final_equity"}:
            value = _safe_float(row.get(sort_key))
        else:
            value = str(row.get("created_at_utc") or "")
        return (invalid, value, str(row.get("signal_dir") or ""))

    return sorted(rows, key=key)


def build_signal_tape_inventory(
    root: str | Path,
    *,
    product: str = PRODUCT,
    symbol: str | None = None,
    entry_tf: str | None = None,
    filter_tf: str | None = None,
    signal_set_id: str | None = None,
    include_invalid: bool = False,
    limit: int = DEFAULT_LIMIT,
    sort: str = "created_at_utc",
) -> dict[str, Any]:
    product_text = str(product or PRODUCT).strip()
    if product_text != PRODUCT:
        _fail("--product must be free")

    root_path = Path(root)
    rows: list[dict[str, Any]] = []
    valid_count = 0
    invalid_count = 0
    candidates = discover_signal_tape_dirs(root_path, product=product_text, limit=max(int(limit), 0) or DEFAULT_LIMIT)

    for signal_dir in candidates:
        row = build_signal_tape_inventory_row(signal_dir, product=product_text)
        if row.get("status") == "valid":
            valid_count += 1
            if _matches_filters(row, symbol=symbol, entry_tf=entry_tf, filter_tf=filter_tf, signal_set_id=signal_set_id):
                rows.append(row)
        else:
            invalid_count += 1
            if include_invalid:
                rows.append(row)
        if len(rows) >= int(limit):
            break

    return {
        "schema_version": INVENTORY_SCHEMA_VERSION,
        "build_id": BUILD_ID,
        "product": product_text,
        "root": str(root_path),
        "total": len(rows),
        "valid_count": sum(1 for row in rows if row.get("status") == "valid"),
        "invalid_count": sum(1 for row in rows if row.get("status") == "invalid"),
        "discovered_valid_count": valid_count,
        "discovered_invalid_count": invalid_count,
        "rows": _sort_rows(rows, sort),
    }


def write_signal_tape_inventory_json(path: str | Path, inventory: Mapping[str, Any]) -> Path:
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(inventory, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return out_path


def _csv_value(value: Any) -> Any:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return value


def write_signal_tape_inventory_csv(path: str | Path, rows: list[Mapping[str, Any]]) -> Path:
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: _csv_value(row.get(field, "")) for field in CSV_FIELDS})
    return out_path


def format_signal_tape_inventory_csv(rows: list[Mapping[str, Any]]) -> str:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=CSV_FIELDS, extrasaction="ignore", lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({field: _csv_value(row.get(field, "")) for field in CSV_FIELDS})
    return output.getvalue()


def _format_text(inventory: Mapping[str, Any], *, compact: bool = False) -> str:
    lines = [
        (
            f"precomputed signal inventory product={inventory.get('product')} "
            f"total={inventory.get('total')} valid={inventory.get('valid_count')} invalid={inventory.get('invalid_count')} "
            f"root={inventory.get('root')}"
        )
    ]
    if compact:
        return "\n".join(lines)
    for row in inventory.get("rows") or []:
        if not isinstance(row, Mapping):
            continue
        if row.get("status") == "valid":
            lines.append(
                (
                    f"valid signal_dir={row.get('signal_dir')} symbol={row.get('symbol')} "
                    f"entry_tf={row.get('entry_tf')} filter_tf={row.get('filter_tf')} "
                    f"signal_set_id={row.get('signal_set_id')} net_total={row.get('net_total')} "
                    f"final_equity={row.get('final_equity')} max_dd_abs={row.get('max_dd_abs')} "
                    f"max_dd_pct={row.get('max_dd_pct')}"
                )
            )
        else:
            lines.append(
                (
                    f"invalid signal_dir={row.get('signal_dir')} "
                    f"safe_error_code={row.get('safe_error_code')} status_reason={row.get('status_reason')}"
                )
            )
    return "\n".join(lines)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read-only inventory for Free precomputed signal tapes.")
    parser.add_argument("--product", default=PRODUCT)
    parser.add_argument("--root", default="")
    parser.add_argument("--symbol", default="")
    parser.add_argument("--entry-tf", default="")
    parser.add_argument("--filter-tf", default="")
    parser.add_argument("--signal-set-id", default="")
    parser.add_argument("--format", choices=("json", "csv", "text"), default="text")
    parser.add_argument("--out", default="")
    parser.add_argument("--include-invalid", action="store_true")
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--sort", choices=("created_at_utc", "symbol", "net_total", "final_equity"), default="created_at_utc")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    product = str(args.product or PRODUCT).strip()
    if product != PRODUCT:
        _fail("--product must be free")

    root = resolve_inventory_root(str(args.root or "").strip() or None)
    inventory = build_signal_tape_inventory(
        root,
        product=product,
        symbol=str(args.symbol or "").strip() or None,
        entry_tf=str(args.entry_tf or "").strip() or None,
        filter_tf=str(args.filter_tf or "").strip() or None,
        signal_set_id=str(args.signal_set_id or "").strip() or None,
        include_invalid=bool(args.include_invalid or args.strict),
        limit=max(int(args.limit), 1),
        sort=str(args.sort or "created_at_utc"),
    )

    out_path = str(args.out or "").strip()
    if out_path:
        if args.format == "csv":
            write_signal_tape_inventory_csv(out_path, list(inventory["rows"]))
        elif args.format == "text":
            text_path = Path(out_path)
            text_path.parent.mkdir(parents=True, exist_ok=True)
            text_path.write_text(_format_text(inventory) + "\n", encoding="utf-8")
        else:
            write_signal_tape_inventory_json(out_path, inventory)
        print(_format_text(inventory, compact=True))
    elif args.format == "json":
        print(json.dumps(inventory, ensure_ascii=True, sort_keys=True))
    elif args.format == "csv":
        print(format_signal_tape_inventory_csv(list(inventory["rows"])), end="")
    else:
        print(_format_text(inventory))

    if bool(args.strict) and int(inventory.get("discovered_invalid_count") or 0) > 0:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
