# BUILD_ID: 2026-03-05_context_state_store_refactor_v1
from __future__ import annotations

import json
import os
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


BUILD_ID = "2026-03-05_context_state_store_refactor_v1"


@dataclass(frozen=True)
class StateContext:
    exchange_id: str
    market_type: str
    run_mode: str
    symbol: str
    base_ccy: str
    quote_ccy: str
    account_ccy: str
    settlement_ccy: str
    profile_name: str = ""


@dataclass(frozen=True)
class StateContextPaths:
    state_root: str
    contexts_root: str
    context_root: str
    context_id: str
    db_path: str
    meta_path: str
    registry_path: str
    legacy_db_path: str
    legacy_paper_equity_path: str


def _safe_token(raw: str, *, fallback: str) -> str:
    txt = str(raw or "").strip()
    cleaned = "".join(ch if (ch.isalnum() or ch in ("-", "_")) else "_" for ch in txt)
    cleaned = cleaned.strip("_")
    return cleaned or str(fallback)


def normalize_symbol(raw_symbol: str) -> str:
    txt = str(raw_symbol or "").strip().upper()
    if not txt:
        return "SYMBOL"
    txt = txt.replace("/", "").replace("-", "").replace("_", "").replace(":", "")
    txt = "".join(ch for ch in txt if ch.isalnum())
    return txt or "SYMBOL"


def split_symbol_currencies(symbol: str) -> tuple[str, str]:
    s = str(symbol or "").strip().upper()
    if "/" in s:
        base, quote = s.split("/", 1)
        return (_safe_token(base, fallback="BASE").upper(), _safe_token(quote, fallback="QUOTE").upper())
    for sep in ("-", "_", ":"):
        if sep in s:
            base, quote = s.split(sep, 1)
            return (_safe_token(base, fallback="BASE").upper(), _safe_token(quote, fallback="QUOTE").upper())
    return (_safe_token(s, fallback="BASE").upper(), "QUOTE")


def build_state_context(
    *,
    exchange_id: str,
    market_type: str,
    run_mode: str,
    symbol: str,
    base_ccy: str | None = None,
    quote_ccy: str | None = None,
    account_ccy: str | None = None,
    settlement_ccy: str | None = None,
    profile_name: str = "",
) -> StateContext:
    symbol_txt = str(symbol or "").strip()
    symbol_norm = normalize_symbol(symbol_txt)
    sym_base, sym_quote = split_symbol_currencies(symbol_txt)
    base = _safe_token(str(base_ccy or sym_base), fallback=sym_base).upper()
    quote = _safe_token(str(quote_ccy or sym_quote), fallback=sym_quote).upper()
    acct = _safe_token(str(account_ccy or quote), fallback=quote).upper()
    settle = _safe_token(str(settlement_ccy or acct), fallback=acct).upper()
    return StateContext(
        exchange_id=_safe_token(exchange_id, fallback="exchange").lower(),
        market_type=_safe_token(market_type, fallback="spot").lower(),
        run_mode=_safe_token(run_mode, fallback="paper").upper(),
        symbol=symbol_norm.upper(),
        base_ccy=base,
        quote_ccy=quote,
        account_ccy=acct,
        settlement_ccy=settle,
        profile_name=_safe_token(profile_name, fallback=""),
    )


def context_id_for(ctx: StateContext) -> str:
    parts = [
        _safe_token(ctx.exchange_id, fallback="exchange").lower(),
        _safe_token(ctx.market_type, fallback="spot").lower(),
        _safe_token(ctx.run_mode, fallback="paper").lower(),
        _safe_token(ctx.symbol, fallback="symbol").upper(),
        f"base_{_safe_token(ctx.base_ccy, fallback='BASE').upper()}",
        f"quote_{_safe_token(ctx.quote_ccy, fallback='QUOTE').upper()}",
        f"acct_{_safe_token(ctx.account_ccy, fallback='ACCT').upper()}",
        f"settle_{_safe_token(ctx.settlement_ccy, fallback='SETTLE').upper()}",
    ]
    profile = _safe_token(ctx.profile_name, fallback="")
    if profile:
        parts.append(f"profile_{profile}")
    return "__".join(parts)


def resolve_state_context_paths(state_dir: str, ctx: StateContext) -> StateContextPaths:
    root = os.path.abspath(str(state_dir or "."))
    context_id = context_id_for(ctx)
    contexts_root = os.path.join(root, "contexts")
    context_root = os.path.join(contexts_root, context_id)
    return StateContextPaths(
        state_root=root,
        contexts_root=contexts_root,
        context_root=context_root,
        context_id=context_id,
        db_path=os.path.join(context_root, "state.db"),
        meta_path=os.path.join(context_root, "meta.json"),
        registry_path=os.path.join(root, "index.json"),
        legacy_db_path=os.path.join(root, "state.db"),
        legacy_paper_equity_path=os.path.join(root, "paper_equity.json"),
    )


def _atomic_write_json(path: str, payload: dict[str, Any]) -> None:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(out.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(str(tmp), str(out))


def ensure_context_layout(paths: StateContextPaths) -> None:
    Path(paths.context_root).mkdir(parents=True, exist_ok=True)


def write_context_meta(
    *,
    paths: StateContextPaths,
    ctx: StateContext,
    build_id: str,
    extra: dict[str, Any] | None = None,
) -> None:
    payload: dict[str, Any] = {
        "context_id": str(paths.context_id),
        "build_id": str(build_id),
        "updated_at": int(time.time()),
        "context": asdict(ctx),
    }
    if isinstance(extra, dict):
        payload.update(extra)
    _atomic_write_json(paths.meta_path, payload)


def load_context_meta(paths: StateContextPaths) -> dict[str, Any]:
    p = str(paths.meta_path)
    if not os.path.exists(p):
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            raw = json.load(f)
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}


def register_context(
    *,
    paths: StateContextPaths,
    ctx: StateContext,
    build_id: str,
) -> None:
    now_ts = int(time.time())
    current = {}
    if os.path.exists(paths.registry_path):
        try:
            with open(paths.registry_path, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                current = raw
        except Exception:
            current = {}
    contexts = current.get("contexts")
    if not isinstance(contexts, dict):
        contexts = {}
    contexts[str(paths.context_id)] = {
        "context_id": str(paths.context_id),
        "db_path": str(paths.db_path),
        "meta_path": str(paths.meta_path),
        "updated_at": int(now_ts),
        "build_id": str(build_id),
        "exchange_id": str(ctx.exchange_id),
        "run_mode": str(ctx.run_mode),
        "symbol": str(ctx.symbol),
        "account_ccy": str(ctx.account_ccy),
        "settlement_ccy": str(ctx.settlement_ccy),
    }
    payload = {
        "updated_at": int(now_ts),
        "contexts": contexts,
    }
    _atomic_write_json(paths.registry_path, payload)


def list_registered_contexts(state_dir: str) -> list[dict[str, Any]]:
    root = os.path.abspath(str(state_dir or "."))
    p = os.path.join(root, "index.json")
    if not os.path.exists(p):
        return []
    try:
        with open(p, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception:
        return []
    contexts = raw.get("contexts") if isinstance(raw, dict) else None
    if not isinstance(contexts, dict):
        return []
    out: list[dict[str, Any]] = []
    for _, item in contexts.items():
        if isinstance(item, dict):
            out.append(dict(item))
    out.sort(key=lambda x: int(x.get("updated_at") or 0), reverse=True)
    return out


def context_matches_meta(ctx: StateContext, meta: dict[str, Any]) -> bool:
    if not isinstance(meta, dict):
        return False
    expected_id = context_id_for(ctx)
    if str(meta.get("context_id") or "") == expected_id:
        return True
    ref = meta.get("context")
    if not isinstance(ref, dict):
        return False
    checks = {
        "exchange_id": str(ctx.exchange_id),
        "market_type": str(ctx.market_type),
        "run_mode": str(ctx.run_mode),
        "symbol": str(ctx.symbol),
        "base_ccy": str(ctx.base_ccy),
        "quote_ccy": str(ctx.quote_ccy),
        "account_ccy": str(ctx.account_ccy),
        "settlement_ccy": str(ctx.settlement_ccy),
    }
    for k, v in checks.items():
        if str(ref.get(k) or "") != str(v):
            return False
    return True


def format_context_brief(ctx: StateContext) -> str:
    return (
        f"{ctx.exchange_id}/{ctx.market_type}/{ctx.run_mode}/"
        f"{ctx.symbol}/{ctx.account_ccy}/{ctx.settlement_ccy}"
    )
