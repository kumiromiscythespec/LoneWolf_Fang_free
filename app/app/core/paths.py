# BUILD_ID: 2026-03-31_free_user_scope_data_root_v1
# BUILD_ID: 2026-03-20_paths_market_data_define_fix_v1
# BUILD_ID: 2026-02-24_lonewolf_fang_gui_v1
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class AppPaths:
    repo_root: str
    runtime_dir: str
    logs_dir: str
    exports_dir: str
    state_dir: str
    configs_dir: str
    user_configs_dir: str
    market_data_dir: str
    precomputed_indicators_dir: str
    settings_path: str


def _find_repo_root() -> str:
    # Resolve repo root as the directory containing runner.py
    here = os.path.abspath(os.path.dirname(__file__))
    cur = here
    for _ in range(10):
        if os.path.exists(os.path.join(cur, "runner.py")):
            return cur
        nxt = os.path.dirname(cur)
        if nxt == cur:
            break
        cur = nxt
    # Fallback: current working directory
    return os.path.abspath(os.getcwd())


def _resolve_env_path(env_name: str, fallback: str) -> str:
    raw = str(os.environ.get(env_name, "") or "").strip()
    if raw:
        return os.path.abspath(raw)
    return os.path.abspath(fallback)


def get_paths() -> AppPaths:
    root = _resolve_env_path("LWF_HOME", _find_repo_root())
    runtime_dir = _resolve_env_path("LWF_RUNTIME_ROOT", os.path.join(root, "runtime"))
    logs_dir = os.path.join(runtime_dir, "logs")
    exports_dir = os.path.join(runtime_dir, "exports")
    state_dir = os.path.join(runtime_dir, "state")
    configs_dir = _resolve_env_path("LWF_CONFIGS_ROOT", os.path.join(root, "configs"))
    user_configs_dir = os.path.join(configs_dir, "user")
    market_data_dir = _resolve_env_path("LWF_MARKET_DATA_ROOT", os.path.join(root, "market_data"))
    precomputed_indicators_dir = os.path.join(market_data_dir, "precomputed_indicators")
    settings_path = os.path.join(runtime_dir, "settings.json")
    return AppPaths(
        repo_root=root,
        runtime_dir=runtime_dir,
        logs_dir=logs_dir,
        exports_dir=exports_dir,
        state_dir=state_dir,
        configs_dir=configs_dir,
        user_configs_dir=user_configs_dir,
        market_data_dir=market_data_dir,
        precomputed_indicators_dir=precomputed_indicators_dir,
        settings_path=settings_path,
    )


def ensure_runtime_dirs() -> AppPaths:
    p = get_paths()
    os.makedirs(p.runtime_dir, exist_ok=True)
    os.makedirs(p.logs_dir, exist_ok=True)
    os.makedirs(p.exports_dir, exist_ok=True)
    os.makedirs(p.state_dir, exist_ok=True)
    os.makedirs(p.configs_dir, exist_ok=True)
    os.makedirs(p.user_configs_dir, exist_ok=True)
    os.makedirs(p.market_data_dir, exist_ok=True)
    os.makedirs(p.precomputed_indicators_dir, exist_ok=True)
    return p
