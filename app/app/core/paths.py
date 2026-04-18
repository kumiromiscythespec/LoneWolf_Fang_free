# BUILD_ID: 2026-04-18_free_shared_market_data_root_v1
# BUILD_ID: 2026-03-31_free_user_scope_data_root_v1
# BUILD_ID: 2026-03-20_paths_market_data_define_fix_v1
# BUILD_ID: 2026-02-24_lonewolf_fang_gui_v1
from __future__ import annotations

import os
from dataclasses import dataclass


BUILD_ID = "2026-04-18_free_shared_market_data_root_v1"


@dataclass(frozen=True)
class AppPaths:
    repo_root: str
    shared_root: str
    shared_data_root: str
    product_data_root: str
    runtime_dir: str
    logs_dir: str
    exports_dir: str
    state_dir: str
    configs_dir: str
    user_configs_dir: str
    market_data_dir: str
    chart_cache_dir: str
    precomputed_indicators_dir: str
    settings_path: str
    market_data_candidates: tuple[tuple[str, str], ...]
    precomputed_candidates: tuple[tuple[str, str], ...]


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
    return os.path.abspath(os.getcwd())


def _get_env_path(env_name: str) -> str | None:
    raw = str(os.environ.get(env_name, "") or "").strip()
    if raw:
        return os.path.abspath(raw)
    return None


def _resolve_env_path(env_name: str, fallback: str) -> str:
    return _get_env_path(env_name) or os.path.abspath(fallback)


def _default_shared_root(repo_root: str) -> str:
    local_appdata = str(os.environ.get("LOCALAPPDATA", "") or "").strip()
    if local_appdata:
        return os.path.abspath(os.path.join(local_appdata, "LoneWolfFang"))
    return os.path.abspath(repo_root)


def _leaf_parent(path: str | None, leaf_name: str) -> str | None:
    if not path:
        return None
    candidate = os.path.abspath(path)
    if os.path.normcase(os.path.basename(candidate)) != os.path.normcase(leaf_name):
        return None
    return os.path.abspath(os.path.dirname(candidate))


def _same_path(left: str | None, right: str | None) -> bool:
    if not left or not right:
        return False
    return os.path.normcase(os.path.abspath(left)) == os.path.normcase(os.path.abspath(right))


def _append_candidate(candidates: list[tuple[str, str]], source: str, path_value: str | None) -> None:
    root = str(path_value or "").strip()
    if not root:
        return
    root_abs = os.path.abspath(root)
    root_key = os.path.normcase(root_abs)
    for _source, existing in candidates:
        if os.path.normcase(os.path.abspath(existing)) == root_key:
            return
    candidates.append((str(source or "").strip() or "candidate", root_abs))


def get_paths() -> AppPaths:
    root = _resolve_env_path("LWF_HOME", _find_repo_root())
    shared_root = _get_env_path("LWF_SHARED_ROOT") or _default_shared_root(root)
    shared_data_root = _get_env_path("LWF_SHARED_DATA_ROOT") or os.path.join(shared_root, "data")

    runtime_override = _get_env_path("LWF_RUNTIME_ROOT")
    configs_override = _get_env_path("LWF_CONFIGS_ROOT")
    market_data_override = _get_env_path("LWF_MARKET_DATA_ROOT")
    product_data_override = _get_env_path("LWF_PRODUCT_DATA_ROOT")

    compat_product_root = None
    runtime_parent = _leaf_parent(runtime_override, "runtime")
    configs_parent = _leaf_parent(configs_override, "configs")
    if runtime_parent and configs_parent and _same_path(runtime_parent, configs_parent):
        compat_product_root = runtime_parent

    product_data_root = product_data_override or compat_product_root or os.path.join(shared_data_root, "free")
    runtime_dir = runtime_override or os.path.join(product_data_root, "runtime")
    logs_dir = os.path.join(runtime_dir, "logs")
    exports_dir = os.path.join(runtime_dir, "exports")
    state_dir = os.path.join(runtime_dir, "state")
    configs_dir = configs_override or os.path.join(product_data_root, "configs")
    user_configs_dir = os.path.join(configs_dir, "user")
    market_data_dir = os.path.join(shared_data_root, "market_data")
    chart_cache_dir = os.path.join(market_data_dir, "chart_cache")
    precomputed_indicators_dir = os.path.join(market_data_dir, "precomputed_indicators")
    settings_path = os.path.join(runtime_dir, "settings.json")

    market_data_candidates: list[tuple[str, str]] = []
    _append_candidate(market_data_candidates, "canonical_shared_root", market_data_dir)
    if market_data_override and (not _same_path(market_data_override, market_data_dir)):
        _append_candidate(market_data_candidates, "legacy_env_root", market_data_override)
    legacy_product_market = os.path.join(product_data_root, "market_data")
    if not _same_path(legacy_product_market, market_data_dir):
        _append_candidate(market_data_candidates, "legacy_product_root", legacy_product_market)
    legacy_repo_market = os.path.join(root, "market_data")
    if not _same_path(legacy_repo_market, market_data_dir):
        _append_candidate(market_data_candidates, "legacy_repo_root", legacy_repo_market)

    precomputed_candidates: list[tuple[str, str]] = []
    _append_candidate(precomputed_candidates, "canonical_shared_root", precomputed_indicators_dir)
    for source, market_root in market_data_candidates[1:]:
        if source == "legacy_env_root":
            pre_source = "legacy_env_root"
        elif source == "legacy_product_root":
            pre_source = "legacy_product_root"
        elif source == "legacy_repo_root":
            pre_source = "legacy_repo_root"
        else:
            pre_source = source
        _append_candidate(precomputed_candidates, pre_source, os.path.join(market_root, "precomputed_indicators"))
    _append_candidate(precomputed_candidates, "legacy_exports_root", os.path.join(root, "exports", "precomputed_indicators"))

    return AppPaths(
        repo_root=root,
        shared_root=shared_root,
        shared_data_root=shared_data_root,
        product_data_root=product_data_root,
        runtime_dir=runtime_dir,
        logs_dir=logs_dir,
        exports_dir=exports_dir,
        state_dir=state_dir,
        configs_dir=configs_dir,
        user_configs_dir=user_configs_dir,
        market_data_dir=market_data_dir,
        chart_cache_dir=chart_cache_dir,
        precomputed_indicators_dir=precomputed_indicators_dir,
        settings_path=settings_path,
        market_data_candidates=tuple(market_data_candidates),
        precomputed_candidates=tuple(precomputed_candidates),
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
    os.makedirs(p.chart_cache_dir, exist_ok=True)
    os.makedirs(p.precomputed_indicators_dir, exist_ok=True)
    return p
