# BUILD_ID: 2026-03-04_coincheck_rules_fee_postonly_fallback_v1
from __future__ import annotations

import config as C


BUILD_ID = "2026-03-04_coincheck_rules_fee_postonly_fallback_v1"


def resolve_paper_fees(exchange_id: str) -> tuple[float, float]:
    ex = str(exchange_id or "").strip().lower()
    if ex == "binance":
        maker = float(getattr(C, "BINANCE_PAPER_FEE_RATE_MAKER", 0.001) or 0.001)
        taker = float(getattr(C, "BINANCE_PAPER_FEE_RATE_TAKER", maker) or maker)
        return (maker, taker)
    if ex == "coincheck":
        maker = float(getattr(C, "COINCHECK_FEE_RATE_MAKER", 0.0) or 0.0)
        taker = float(getattr(C, "COINCHECK_FEE_RATE_TAKER", maker) or maker)
        return (maker, taker)

    maker = float(getattr(C, "PAPER_FEE_RATE_MAKER", 0.0001) or 0.0001)
    taker = float(getattr(C, "PAPER_FEE_RATE_TAKER", 0.0002) or 0.0002)
    return (maker, taker)
