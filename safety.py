# safety.py
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional, Tuple

from state_store import StateStore


@dataclass(frozen=True)
class StopCheckResult:
    should_stop: bool
    stop_mode: str  # STOP / STOP_NEW_ONLY / STOP_CANCEL_OPEN / ""
    stop_reason: str  # STOP_FILE_DETECTED / CIRCUIT_BREAKER / ""
    stop_phase: str


STOP_FILES_PRIORITY = [
    ("STOP_CANCEL_OPEN", "STOP_CANCEL_OPEN"),
    ("STOP_NEW_ONLY", "STOP_NEW_ONLY"),
    ("STOP", "STOP"),
]


def detect_stop_file(base_dir: str) -> Tuple[bool, str]:
    """
    Detects stop mode by file existence in base_dir (fixed: 실행フォルダ直下).
    Priority: STOP_CANCEL_OPEN > STOP_NEW_ONLY > STOP
    """
    for fname, mode in STOP_FILES_PRIORITY:
        path = os.path.join(base_dir, fname)
        if os.path.exists(path):
            return True, mode
    return False, ""


def check_and_update_emergency_stop(
    store: StateStore,
    base_dir: str,
    phase: str,
) -> StopCheckResult:
    """
    - If a STOP* file exists -> set emergency stop in DB and request stop.
    - If no STOP* exists but DB is emergency_stop=1 -> clear (auto resume).
    """
    has_stop, mode = detect_stop_file(base_dir)

    if has_stop:
        store.set_stop(mode=mode, reason="STOP_FILE_DETECTED", phase=phase)
        return StopCheckResult(
            should_stop=True,
            stop_mode=mode,
            stop_reason="STOP_FILE_DETECTED",
            stop_phase=phase,
        )

    # No stop file -> if DB says stopped, clear it (auto-resume)
    st = store.get_stop_state()
    if st.emergency_stop == 1:
        store.clear_stop()

    return StopCheckResult(
        should_stop=False,
        stop_mode="",
        stop_reason="",
        stop_phase=phase,
    )
