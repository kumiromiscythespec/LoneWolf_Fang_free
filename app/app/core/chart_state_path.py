# BUILD_ID: 2026-03-29_free_from_standard_nonlive_build_v1
# BUILD_ID: 2026-03-27_chart_state_path_contract_v2_0_2
from __future__ import annotations

import os


BUILD_ID = "2026-03-27_chart_state_path_contract_v2_0_2"


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
