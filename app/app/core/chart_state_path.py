# BUILD_ID: 2026-04-18_free_chart_state_canonical_leaf_parity_v1
# BUILD_ID: 2026-03-29_free_from_standard_nonlive_build_v1
# BUILD_ID: 2026-03-27_chart_state_path_contract_v2_0_2
from __future__ import annotations

import os


BUILD_ID = "2026-04-18_free_chart_state_canonical_leaf_parity_v1"


def sanitize_symbol_for_chart_state(symbol: str) -> str:
    text = "".join(ch for ch in str(symbol or "").strip().lower() if ch.isalnum())
    return text or "btcjpy"


def build_chart_state_path(state_dir: str, exchange_id: str, run_mode: str, symbol: str) -> str:
    root = os.path.abspath(str(state_dir or "").strip())
    if not root:
        return ""
    exchange = "".join(ch for ch in str(exchange_id or "").strip().lower() if ch.isalnum()) or "exchange"
    mode = "".join(ch for ch in str(run_mode or "").strip().lower() if ch.isalnum()) or "paper"
    symbol_token = sanitize_symbol_for_chart_state(symbol)
    return os.path.abspath(os.path.join(root, f"chart_state_{exchange}_{mode}_{symbol_token}.json"))


def build_chart_state_read_candidates(
    chart_state_dir: str,
    legacy_state_dir: str,
    exchange_id: str,
    run_mode: str,
    symbol: str,
) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for source, root in (
        ("canonical", chart_state_dir),
        ("legacy_chart_state", legacy_state_dir),
    ):
        path = build_chart_state_path(root, exchange_id, run_mode, symbol)
        if not path:
            continue
        path_key = os.path.normcase(os.path.abspath(path))
        if path_key in seen:
            continue
        seen.add(path_key)
        out.append((source, path))
    return out


def resolve_chart_state_read_path(
    chart_state_dir: str,
    legacy_state_dir: str,
    exchange_id: str,
    run_mode: str,
    symbol: str,
) -> tuple[str, str]:
    candidates = build_chart_state_read_candidates(
        chart_state_dir,
        legacy_state_dir,
        exchange_id,
        run_mode,
        symbol,
    )
    canonical_path = candidates[0][1] if candidates else ""
    for source, path in candidates:
        if source == "canonical":
            canonical_path = path
            if os.path.isfile(path):
                return (path, source)
            continue
        if os.path.isfile(path):
            return (path, source)
    return (canonical_path, "canonical")
