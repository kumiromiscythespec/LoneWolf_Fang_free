# BUILD_ID: 2026-04-18_free_no_activation_gate_v1
# BUILD_ID: 2026-03-22_standard_license_core_v1
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import os

from app.core.paths import get_paths
from app.core.tier import get_build_tier, tier_rank


BUILD_ID = "2026-04-18_free_no_activation_gate_v1"
CORE_MAX_REPLAY_DAYS = 31
CORE_MAX_BACKTEST_DAYS = 365


@dataclass
class LiveGateDecision:
    allowed: bool
    reason: str
    build_tier: str
    license_status: str = ""
    offline_grace_until: str = ""


def _ms_days(days: int) -> int:
    return int(days) * 86_400_000


def _range_ms(since_ms: int, until_ms: int) -> int:
    try:
        s = int(since_ms)
        u = int(until_ms)
    except Exception:
        return 0
    if u <= s:
        return 0
    return int(u - s)


def parse_iso_utc_maybe(raw: str) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except Exception:
        dt = None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                dt = datetime.strptime(text, fmt)
                break
            except Exception:
                continue
        if dt is None:
            return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def is_within_offline_grace(raw_offline_grace_until: str, now_utc: datetime | None = None) -> bool:
    grace_until = parse_iso_utc_maybe(raw_offline_grace_until)
    if grace_until is None:
        return False
    now = now_utc or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    else:
        now = now.astimezone(timezone.utc)
    return grace_until >= now


def evaluate_live_license_gate(now_utc: datetime | None = None) -> LiveGateDecision:
    build_tier = get_build_tier()
    return LiveGateDecision(
        allowed=False,
        reason="build_tier_disallows_live",
        build_tier=build_tier,
    )


def require_gui(feature_name: str = "GUI") -> None:
    if tier_rank() >= tier_rank("RESEARCH"):
        return
    tier = get_build_tier()
    raise SystemExit(
        f"{feature_name} は {tier} ビルドでは利用できません。"
        "RESEARCH または EXECUTION ビルドをご利用ください。"
    )


def require_live(feature_name: str = "LIVE execution") -> None:
    if tier_rank() >= tier_rank("EXECUTION"):
        return
    tier = get_build_tier()
    raise SystemExit(
        f"{feature_name} は {tier} ビルドでは利用できません。"
        "EXECUTION ビルドをご利用ください。"
    )


def require_live_activation(feature_name: str = "LIVE execution") -> None:
    require_live(feature_name=feature_name)
    decision = evaluate_live_license_gate()
    if decision.allowed:
        return
    raise SystemExit(f"{feature_name} requires desktop activation (reason={decision.reason}).")


def enforce_core_replay_window(since_ms: int, until_ms: int) -> None:
    if get_build_tier() != "CORE":
        return
    span = _range_ms(since_ms, until_ms)
    if span > _ms_days(CORE_MAX_REPLAY_DAYS):
        raise SystemExit(
            f"CORE 制限: replay期間は最大 {CORE_MAX_REPLAY_DAYS} 日です。"
            f"requested_ms={span}"
        )


def enforce_core_backtest_window(since_ms: int, until_ms: int) -> None:
    if get_build_tier() != "CORE":
        return
    span = _range_ms(since_ms, until_ms)
    if span > _ms_days(CORE_MAX_BACKTEST_DAYS):
        raise SystemExit(
            f"CORE 制限: backtest期間は最大 {CORE_MAX_BACKTEST_DAYS} 日です。"
            f"requested_ms={span}"
        )


def restrict_core_export_path(path_value: str) -> str:
    path = str(path_value or "").strip()
    if get_build_tier() != "CORE":
        return path
    p = get_paths()
    exports_root = os.path.abspath(os.path.join(str(p.repo_root), "exports"))
    if not path:
        return exports_root
    target = path if os.path.isabs(path) else os.path.join(str(p.repo_root), path)
    target_abs = os.path.abspath(target)
    try:
        common = os.path.commonpath([exports_root, target_abs])
    except Exception:
        common = ""
    if common != exports_root:
        raise SystemExit("CORE 制限: exports/ 配下のみ出力可能です。")
    return target_abs
