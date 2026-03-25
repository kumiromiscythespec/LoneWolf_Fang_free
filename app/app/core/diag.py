# BUILD_ID: 2026-02-25_lwf_tiered_build_v1
from __future__ import annotations

import os
import re
from typing import Any

from app.core.paths import get_paths
from app.core.tier import get_build_tier


BUILD_ID = "2026-02-25_lwf_tiered_build_v1"


def _read_build_id(file_path: str) -> str | None:
    if not os.path.exists(file_path):
        return None
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            head = f.read(8192)
    except Exception:
        return None
    m1 = re.search(r"^BUILD_ID\s*=\s*[\"']([^\"']+)[\"']", head, flags=re.MULTILINE)
    if m1:
        return str(m1.group(1)).strip()
    m2 = re.search(r"^#\s*BUILD_ID:\s*(.+)$", head, flags=re.MULTILINE)
    if m2:
        return str(m2.group(1)).strip()
    return None


def collect_build_diag() -> dict[str, Any]:
    p = get_paths()
    repo_root = str(p.repo_root)
    gui_path = os.path.join(repo_root, "app", "gui", "main_window.py")
    runner_path = os.path.join(repo_root, "runner.py")
    backtest_path = os.path.join(repo_root, "backtest.py")
    return {
        "tier": get_build_tier(),
        "gui_build_id": _read_build_id(gui_path),
        "runner_build_id": _read_build_id(runner_path),
        "backtest_build_id": _read_build_id(backtest_path),
    }


def render_build_diag_lines() -> list[str]:
    d = collect_build_diag()
    return [
        f"tier={d.get('tier')}",
        f"gui_build_id={d.get('gui_build_id')}",
        f"runner_build_id={d.get('runner_build_id')}",
        f"backtest_build_id={d.get('backtest_build_id')}",
    ]
