# BUILD_ID: 2026-03-25_free_tier_lock_v1
from __future__ import annotations

import os
from typing import Final


BUILD_ID = "2026-03-25_free_tier_lock_v1"
VALID_TIERS: Final[tuple[str, str, str]] = ("CORE", "RESEARCH", "EXECUTION")

# Embedded tier for distributables. build_zip.py rewrites this per output tier.
BUILD_TIER = "RESEARCH"


def _normalize_tier(raw: str | None) -> str:
    s = str(raw or "").strip().upper()
    return s if s in VALID_TIERS else ""


def get_build_tier() -> str:
    embedded = _normalize_tier(BUILD_TIER) or "EXECUTION"
    env_tier = _normalize_tier(os.getenv("LWF_TIER", ""))
    if embedded == "RESEARCH":
        return embedded
    if env_tier:
        return env_tier
    return embedded


def tier_rank(tier: str | None = None) -> int:
    t = _normalize_tier(tier) if tier is not None else get_build_tier()
    if t == "CORE":
        return 0
    if t == "RESEARCH":
        return 1
    return 2


def is_core() -> bool:
    return get_build_tier() == "CORE"


def is_research_or_higher() -> bool:
    return tier_rank() >= tier_rank("RESEARCH")


def is_execution() -> bool:
    return get_build_tier() == "EXECUTION"
