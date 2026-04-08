# BUILD_ID: 2026-04-08_free_bitbank_okx_spot_only_v1
# BUILD_ID: 2026-04-03_free_launch_salesafe_defaults_release_prep_v1
# BUILD_ID: 2026-03-29_free_gui_multiyear_backtest_fix_v1
# BUILD_ID: 2026-03-29_free_from_standard_nonlive_build_v1
# BUILD_ID: 2026-03-25_free_launch_boundary_v1
from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Optional, List, Dict

import config as C
from app.core.gating import evaluate_live_license_gate, is_within_offline_grace, require_live, require_live_activation
from app.core.paths import ensure_runtime_dirs
from app.core.tier import get_build_tier
from app.security.keyring_store import (
    LicenseState,
    get_or_create_license_device_id,
    load_license_seat_key,
    load_license_state,
    save_license_seat_key,
    save_license_state,
)
from app.security.license_client import (
    LicenseResponse,
    activate_license,
    compute_machine_hash,
    default_license_base_url,
    normalize_license_base_url,
    refresh_license,
)


BUILD_ID = "2026-04-08_free_bitbank_okx_spot_only_v1"

_RUNTIME_LOG_LEVELS = {"MINIMAL", "OPS", "DEBUG"}
_FREE_RUN_MODES = {"PAPER", "REPLAY", "BACKTEST"}
_FREE_BUILD_LIVE_MESSAGE = "FREE build does not support LIVE mode"
_DEFAULT_PRESET = str(getattr(C, "DEFAULT_PRESET", "SELL_SAFE") or "SELL_SAFE").strip().upper() or "SELL_SAFE"


def _is_free_build() -> bool:
    return get_build_tier() == "RESEARCH"


def _normalize_runtime_log_level(raw: str) -> str:
    if _is_free_build():
        return "MINIMAL"
    value = str(raw or "").strip().upper()
    if value in _RUNTIME_LOG_LEVELS:
        return value
    fallback = str(getattr(C, "RUNTIME_LOG_LEVEL", "OPS") or "OPS").strip().upper()
    return fallback if fallback in _RUNTIME_LOG_LEVELS else "OPS"


def _ensure_free_mode_supported(mode: str) -> str:
    value = str(mode or "").strip().upper()
    if not _is_free_build():
        return value
    if value == "LIVE":
        raise SystemExit(_FREE_BUILD_LIVE_MESSAGE)
    if value in _FREE_RUN_MODES:
        return value
    return "PAPER"


@dataclass
class LaunchSpec:
    preset: str  # OFF / SELL_SAFE
    api_key: str
    api_secret: str
    api_passphrase: str = ""
    exchange_id: str = "coincheck"
    symbol: str = "BTC/JPY"
    log_level: str = ""
    report_enabled: bool = False
    report_out: str = ""


@dataclass
class ReplaySpec:
    preset: str = _DEFAULT_PRESET
    replay_data_path: str = ""
    symbol: str = "BTC/JPY"
    timeframe: str = "5m"
    log_level: str = ""
    since_ms: int = 1735689600000
    until_ms: int = 1767225600000
    replay_engine: str = "live"
    replay_csv_dir_5m: str = ""
    replay_csv_dir_1h: str = ""
    replay_csv_glob_5m: str = ""
    replay_csv_glob_1h: str = ""
    replay_dataset_root: str = ""
    replay_dataset_prefix: str = ""
    replay_dataset_year: int = 0


@dataclass
class BacktestSpec:
    preset: str = _DEFAULT_PRESET
    symbol: str = "BTC/JPY"
    entry_timeframe: str = "5m"
    since_ymd: str = ""
    until_ymd: str = ""
    backtest_since_year: Optional[int] = None
    backtest_until_year: Optional[int] = None
    report_enabled: bool = False
    report_out: str = ""
    backtest_csv_dir_5m: str = ""
    backtest_csv_dir_1h: str = ""
    backtest_csv_glob_5m: str = ""
    backtest_csv_glob_1h: str = ""
    backtest_dataset_root: str = ""
    backtest_dataset_prefix: str = ""
    backtest_dataset_year: int = 0


class LicenseOperationError(RuntimeError):
    def __init__(self, error_code: str, message: str = "", response: LicenseResponse | None = None):
        self.error_code = str(error_code or "invalid_response").strip() or "invalid_response"
        self.detail = str(message or "").strip()
        self.response = response
        super().__init__(_license_error_text(self.error_code, self.detail))


def _app_build_id() -> str:
    raw = str(getattr(C, "BUILD_ID", "") or "").strip()
    return raw or str(BUILD_ID)


def _app_version() -> str:
    raw = str(getattr(C, "APP_VERSION", "") or getattr(C, "VERSION", "") or "").strip()
    return raw


def _ensure_live_license_base_url(raw: str = "") -> str:
    base_url = str(raw or "").strip() or default_license_base_url()
    return normalize_license_base_url(base_url)


def _license_error_text(error_code: str, message: str = "") -> str:
    code = str(error_code or "invalid_response").strip() or "invalid_response"
    mapping = {
        "invalid_seat_key": "invalid desktop activation seat key",
        "license_revoked": "desktop activation was revoked",
        "seat_already_bound": "seat is already bound to another device",
        "device_mismatch": "stored device does not match the activated device",
        "network_error": "license service is unavailable",
        "invalid_response": "license service returned an invalid response",
    }
    text = mapping.get(code, code)
    detail = str(message or "").strip()
    return f"{text}: {detail}" if detail else text


def _license_state_from_response(
    response: LicenseResponse,
    seat_key: str,
    device_id: str,
    *,
    previous: LicenseState | None = None,
) -> LicenseState:
    prev = previous or LicenseState()
    has_entitlements = isinstance(response.raw_body.get("entitlements"), dict)
    product_code = str(response.product_code or prev.product_code or "standard").strip() or "standard"
    license_status = str(response.device.status or response.error_code or response.outcome or prev.license_status or "").strip()
    last_verified_at = str(
        response.lease.verified_at or response.device.last_verified_at or prev.last_verified_at or ""
    ).strip()
    refresh_after = str(response.lease.refresh_after or (prev.refresh_after if response.ok else "") or "").strip()
    offline_grace_until = str(
        response.lease.offline_grace_until or (prev.offline_grace_until if response.ok else "") or ""
    ).strip()

    if has_entitlements:
        standard_enabled = bool(response.entitlements.standard_enabled)
        live_allowed = bool(response.entitlements.live_allowed)
        paper_allowed = bool(response.entitlements.paper_allowed)
        replay_allowed = bool(response.entitlements.replay_allowed)
        backtest_allowed = bool(response.entitlements.backtest_allowed)
        fallback_tier_on_failure = str(response.entitlements.fallback_tier_on_failure or "").strip()
    else:
        standard_enabled = bool(prev.standard_enabled if response.ok else False)
        live_allowed = bool(prev.live_allowed if response.ok else False)
        paper_allowed = bool(prev.paper_allowed if response.ok else False)
        replay_allowed = bool(prev.replay_allowed if response.ok else False)
        backtest_allowed = bool(prev.backtest_allowed if response.ok else False)
        fallback_tier_on_failure = str(prev.fallback_tier_on_failure or "")

    return LicenseState(
        product_code=product_code,
        seat_key=str(seat_key or "").strip(),
        device_id=str(device_id or "").strip(),
        machine_hash=compute_machine_hash(),
        license_status=license_status,
        seat_no=int(response.seat_no or prev.seat_no or 0),
        last_verified_at=last_verified_at,
        refresh_after=refresh_after,
        offline_grace_until=offline_grace_until,
        standard_enabled=standard_enabled,
        live_allowed=live_allowed,
        paper_allowed=paper_allowed,
        replay_allowed=replay_allowed,
        backtest_allowed=backtest_allowed,
        fallback_tier_on_failure=fallback_tier_on_failure,
    )


def activate_and_store_license(seat_key: str, *, base_url: str = "") -> LicenseState:
    clean_seat_key = str(seat_key or "").strip()
    if not clean_seat_key:
        raise LicenseOperationError("invalid_seat_key", "seat key is empty")
    normalized_base_url = _ensure_live_license_base_url(base_url)
    device_id = get_or_create_license_device_id()
    response = activate_license(
        clean_seat_key,
        device_id,
        base_url=normalized_base_url,
        app_version=_app_version(),
        build_id=_app_build_id(),
        device_name=str(os.getenv("COMPUTERNAME") or os.getenv("HOSTNAME") or "").strip(),
    )
    if not response.ok:
        raise LicenseOperationError(response.error_code, response.error_message, response)
    state = _license_state_from_response(response, clean_seat_key, device_id)
    save_license_seat_key(clean_seat_key)
    save_license_state(state)
    return state


def refresh_and_store_license(*, base_url: str = "") -> LicenseState:
    previous = load_license_state()
    seat_key = str(load_license_seat_key() or (previous.seat_key if previous else "") or "").strip()
    if not seat_key:
        raise LicenseOperationError("invalid_seat_key", "stored seat key is missing")
    device_id = get_or_create_license_device_id()
    response = refresh_license(
        seat_key,
        device_id,
        base_url=_ensure_live_license_base_url(base_url),
        app_version=_app_version(),
        build_id=_app_build_id(),
    )
    if response.error_code == "network_error":
        raise LicenseOperationError(response.error_code, response.error_message, response)
    state = _license_state_from_response(response, seat_key, device_id, previous=previous)
    save_license_seat_key(seat_key)
    save_license_state(state)
    if not response.ok:
        raise LicenseOperationError(response.error_code, response.error_message, response)
    return state


def ensure_live_license_or_raise(*, base_url: str = "", feature_name: str = "LIVE execution") -> None:
    if _is_free_build():
        raise SystemExit(_FREE_BUILD_LIVE_MESSAGE)
    require_live(feature_name=feature_name)
    local_state = load_license_state()
    if local_state is None:
        require_live_activation(feature_name=feature_name)
        return
    try:
        refresh_and_store_license(base_url=base_url)
    except LicenseOperationError as exc:
        if exc.error_code == "invalid_seat_key":
            raise SystemExit(
                f"{feature_name} blocked: {_license_error_text(exc.error_code, exc.detail)} (reason=missing_license_state)."
            )
        if exc.error_code == "network_error":
            if is_within_offline_grace(local_state.offline_grace_until):
                return
            raise SystemExit(
                f"{feature_name} blocked: {_license_error_text(exc.error_code, exc.detail)} (reason=offline_grace_expired)."
            )
        raise SystemExit(
            f"{feature_name} blocked: {_license_error_text(exc.error_code, exc.detail)} (reason={str(exc.error_code or 'license_refresh_failed')})."
        )
    require_live_activation(feature_name=feature_name)


def _build_env(spec: LaunchSpec) -> Dict[str, str]:
    p = ensure_runtime_dirs()
    env = dict(os.environ)

    # Preset precedence: ENV BOT_PRESET is respected by project policy.
    env["BOT_PRESET"] = str(spec.preset or _DEFAULT_PRESET)

    cred_envs = {
        "coincheck": ("COINCHECK_API_KEY", "COINCHECK_API_SECRET", ""),
        "mexc": ("MEXC_API_KEY", "MEXC_API_SECRET", ""),
        "binance": ("BINANCE_API_KEY", "BINANCE_API_SECRET", ""),
        "bitbank": ("BITBANK_API_KEY", "BITBANK_API_SECRET", ""),
        "okx": ("OKX_API_KEY", "OKX_API_SECRET", "OKX_API_PASSPHRASE"),
    }
    exchange_id = str(spec.exchange_id or "").strip().lower() or "coincheck"
    if exchange_id not in cred_envs:
        exchange_id = "coincheck"
    env["LWF_EXCHANGE_ID"] = str(exchange_id)

    # Pass creds via env only (never write to disk).
    for key_env, secret_env, passphrase_env in cred_envs.values():
        env.pop(key_env, None)
        env.pop(secret_env, None)
        if passphrase_env:
            env.pop(passphrase_env, None)
    env.pop("OKX_API_PASSWORD", None)
    key_env, secret_env, passphrase_env = cred_envs[exchange_id]
    env[key_env] = str(spec.api_key or "")
    env[secret_env] = str(spec.api_secret or "")
    if passphrase_env:
        passphrase = str(spec.api_passphrase or "").strip()
        if not passphrase:
            raise RuntimeError("OKX requires API passphrase.")
        env[passphrase_env] = passphrase
        env["OKX_API_PASSWORD"] = passphrase

    # Keep all runtime artifacts under runtime/
    # If runner/backtest already writes to exports/, this at least gives GUI a stable place to show.
    env.setdefault("EXPORTS_DIR", p.exports_dir)

    # Optional convenience: symbol (if your runner supports SYMBOLS via env/config)
    env.setdefault("BOT_SYMBOL", str(spec.symbol or "BTC/JPY"))

    # GUI LIVE must be long-running by default.
    env.setdefault("RUNNER_LOOP", "1")
    env.setdefault("RUNNER_LOOP_SLEEP_SEC", str(getattr(C, "RUNNER_LOOP_SLEEP_SEC", 10.0)))

    # Report is opt-in; runner may ignore this if not supported.
    if bool(spec.report_enabled):
        env["BOT_REPORT"] = "1"
        if spec.report_out:
            env["BOT_REPORT_OUT"] = str(spec.report_out)

    return env


def _build_replay_env(spec: ReplaySpec) -> Dict[str, str]:
    p = ensure_runtime_dirs()
    env = dict(os.environ)
    env["BOT_PRESET"] = str(spec.preset or _DEFAULT_PRESET)
    env.setdefault("EXPORTS_DIR", p.exports_dir)
    env.setdefault("BOT_SYMBOL", str(spec.symbol or "BTC/JPY"))
    exchange_id = (os.getenv("LWF_EXCHANGE_ID") or getattr(C, "EXCHANGE_ID", "coincheck")).strip().lower() or "coincheck"
    env["LWF_EXCHANGE_ID"] = str(exchange_id)
    # Replay is offline; do not require API credentials.
    env.pop("MEXC_API_KEY", None)
    env.pop("MEXC_API_SECRET", None)
    env.pop("BINANCE_API_KEY", None)
    env.pop("BINANCE_API_SECRET", None)
    env.pop("COINCHECK_API_KEY", None)
    env.pop("COINCHECK_API_SECRET", None)
    env.pop("BITBANK_API_KEY", None)
    env.pop("BITBANK_API_SECRET", None)
    env.pop("OKX_API_KEY", None)
    env.pop("OKX_API_SECRET", None)
    env.pop("OKX_API_PASSPHRASE", None)
    env.pop("OKX_API_PASSWORD", None)
    return env


def _build_backtest_env(spec: BacktestSpec) -> Dict[str, str]:
    env = _build_replay_env(
        ReplaySpec(
            preset=str(spec.preset or _DEFAULT_PRESET),
            symbol=str(spec.symbol or "BTC/JPY"),
            timeframe=str(spec.entry_timeframe or "5m"),
        )
    )
    use_continuous_years = (
        spec.backtest_since_year is not None
        and spec.backtest_until_year is not None
    )
    env["LWF_ENTRY_TF"] = str(spec.entry_timeframe or "")
    env["BACKTEST_DATASET_ROOT"] = str(spec.backtest_dataset_root or "")
    env["BACKTEST_DATASET_PREFIX"] = str(spec.backtest_dataset_prefix or "")
    env["BACKTEST_DATASET_YEAR"] = (
        ""
        if use_continuous_years
        else str(int(spec.backtest_dataset_year or 0) if int(spec.backtest_dataset_year or 0) > 0 else "")
    )
    env["BACKTEST_CSV_DIR_5M"] = "" if use_continuous_years else str(spec.backtest_csv_dir_5m or "")
    env["BACKTEST_CSV_DIR_1H"] = "" if use_continuous_years else str(spec.backtest_csv_dir_1h or "")
    env["BACKTEST_CSV_GLOB_5M"] = "" if use_continuous_years else str(spec.backtest_csv_glob_5m or "")
    env["BACKTEST_CSV_GLOB_1H"] = "" if use_continuous_years else str(spec.backtest_csv_glob_1h or "")
    if bool(spec.report_enabled):
        env["BOT_REPORT"] = "1"
        if str(spec.report_out or "").strip():
            env["BOT_REPORT_OUT"] = str(spec.report_out)
    return env


def _build_dataset_override_shim() -> str:
    return (
        "import os, sys, runpy\n"
        "script = sys.argv[1]\n"
        "sys.path.insert(0, os.path.dirname(script))\n"
        "import config\n"
        "sym = str(os.getenv('BOT_SYMBOL', '') or '').strip()\n"
        "if sym:\n"
        "    config.SYMBOLS = [sym]\n"
        "    config.BACKTEST_CSV_SYMBOL = sym\n"
        "entry_tf = str(os.getenv('LWF_ENTRY_TF', '') or '').strip()\n"
        "if entry_tf:\n"
        "    config.ENTRY_TF = entry_tf\n"
        "    config.TIMEFRAME_ENTRY = entry_tf\n"
        "config.BACKTEST_CSV_DIR_5M = str(os.getenv('BACKTEST_CSV_DIR_5M') or os.getenv('BOT_REPLAY_CSV_DIR_5M') or '.')\n"
        "config.BACKTEST_CSV_DIR_1H = str(os.getenv('BACKTEST_CSV_DIR_1H') or os.getenv('BOT_REPLAY_CSV_DIR_1H') or '.')\n"
        "config.BACKTEST_CSV_GLOB_5M = str(os.getenv('BACKTEST_CSV_GLOB_5M') or os.getenv('BOT_REPLAY_CSV_GLOB_5M') or config.BACKTEST_CSV_GLOB_5M)\n"
        "config.BACKTEST_CSV_GLOB_1H = str(os.getenv('BACKTEST_CSV_GLOB_1H') or os.getenv('BOT_REPLAY_CSV_GLOB_1H') or config.BACKTEST_CSV_GLOB_1H)\n"
        "sys.argv = [script] + sys.argv[2:]\n"
        "runpy.run_path(script, run_name='__main__')\n"
    )


def _resolve_runner_mode(env: Dict[str, str]) -> str:
    mode = str(env.get("LWF_MODE_OVERRIDE") or "").strip().upper()
    if _is_free_build():
        if mode:
            return _ensure_free_mode_supported(mode)
        config_mode = str(getattr(C, "RUN_MODE", getattr(C, "MODE", "PAPER")) or "PAPER").strip().upper()
        return _ensure_free_mode_supported(config_mode)
    if mode in ("LIVE", "PAPER", "REPLAY", "BACKTEST"):
        return mode
    return str(getattr(C, "RUN_MODE", getattr(C, "MODE", "LIVE")) or "LIVE").strip().upper()


def _resolve_serve_sleep_sec(env: Dict[str, str]) -> float:
    raw = str(env.get("RUNNER_LOOP_SLEEP_SEC") or getattr(C, "RUNNER_LOOP_SLEEP_SEC", 10.0)).strip()
    try:
        value = float(raw)
    except Exception:
        value = 10.0
    return max(0.0, value)


def _spawn(cmd: List[str], env: Dict[str, str], cwd: str) -> subprocess.Popen:
    creationflags = 0
    if os.name == "nt":
        creationflags = int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
    return subprocess.Popen(
        cmd,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        universal_newlines=True,
        creationflags=creationflags,
    )


def _derive_replay_csv_base(path: str) -> str:
    p = str(path or "").strip()
    if not p:
        return "."
    abs_p = os.path.abspath(p)
    base = abs_p if os.path.isdir(abs_p) else os.path.dirname(abs_p)
    return base or "."


def _derive_replay_filter_dir(base_dir: str) -> str:
    base = os.path.abspath(str(base_dir or "."))
    cand = base.replace("_5m_", "_1h_")
    if cand != base and os.path.isdir(cand):
        return cand
    cand = base.replace("-5m-", "-1h-")
    if cand != base and os.path.isdir(cand):
        return cand
    return base


def _symbol_to_prefix(symbol: str) -> str:
    s = str(symbol or "").strip().upper()
    if not s:
        return "BTCJPY"
    s = s.replace("/", "").replace("-", "").replace("_", "")
    s = "".join(ch for ch in s if ch.isalnum())
    return s or "BTCJPY"


def launch_runner(spec: LaunchSpec) -> subprocess.Popen:
    """
    Launch runner.py as a child process.
    - Uses the same Python interpreter to avoid venv mismatch.
    - Captures stdout/stderr for GUI.
    """
    p = ensure_runtime_dirs()
    env = _build_env(spec)
    run_mode = _resolve_runner_mode(env)
    env["LWF_MODE_OVERRIDE"] = str(run_mode)
    if run_mode == "LIVE":
        ensure_live_license_or_raise(feature_name="LIVE execution")

    # runner.py exists at repo root.
    runner_path = os.path.join(p.repo_root, "runner.py")
    if not os.path.exists(runner_path):
        raise FileNotFoundError(f"runner.py not found under repo root: {p.repo_root}")

    shim = (
        "import os, runpy, signal, sys\n"
        "def _on_break(sig, frame):\n"
        "    raise KeyboardInterrupt\n"
        "if hasattr(signal, 'SIGBREAK'):\n"
        "    signal.signal(signal.SIGBREAK, _on_break)\n"
        "script = sys.argv[1]\n"
        "sys.path.insert(0, os.path.dirname(script))\n"
        "sys.argv = [script] + sys.argv[2:]\n"
        "try:\n"
        "    runpy.run_path(script, run_name='__main__')\n"
        "except KeyboardInterrupt:\n"
        "    raise SystemExit(0)\n"
    )
    cmd: List[str] = [sys.executable, "-u", "-c", shim, runner_path]
    cmd.extend(["--log-level", _normalize_runtime_log_level(spec.log_level)])

    if run_mode in ("LIVE", "PAPER"):
        serve_sleep_sec = _resolve_serve_sleep_sec(env)
        cmd.extend(["--serve", "--serve-sleep-sec", str(serve_sleep_sec)])

    # Keep LIVE artifacts under runtime/ (exports/, state.db, logs via GUI stream).
    return _spawn(cmd=cmd, env=env, cwd=p.runtime_dir)


def launch_replay(spec: ReplaySpec) -> subprocess.Popen:
    p = ensure_runtime_dirs()
    env = _build_replay_env(spec)

    runner_path = os.path.join(p.repo_root, "runner.py")
    if not os.path.exists(runner_path):
        raise FileNotFoundError(f"runner.py not found under repo root: {p.repo_root}")

    replay_csv_base = _derive_replay_csv_base(spec.replay_data_path)
    prefix = _symbol_to_prefix(spec.symbol)
    replay_csv_dir_5m = str(spec.replay_csv_dir_5m or replay_csv_base)
    replay_csv_dir_1h = str(spec.replay_csv_dir_1h or _derive_replay_filter_dir(replay_csv_dir_5m))
    replay_csv_glob_5m = str(spec.replay_csv_glob_5m or f"{prefix}-5m-*.csv")
    replay_csv_glob_1h = str(spec.replay_csv_glob_1h or f"{prefix}-1h-*.csv")
    replay_dataset_root = str(spec.replay_dataset_root or replay_csv_base)
    replay_dataset_prefix = str(spec.replay_dataset_prefix or prefix)
    replay_dataset_year = int(spec.replay_dataset_year or 0)
    env["BOT_REPLAY_CSV_DIR_5M"] = str(replay_csv_dir_5m)
    env["BOT_REPLAY_CSV_DIR_1H"] = str(replay_csv_dir_1h)
    env["BOT_REPLAY_CSV_GLOB_5M"] = str(replay_csv_glob_5m)
    env["BOT_REPLAY_CSV_GLOB_1H"] = str(replay_csv_glob_1h)
    env["BOT_REPLAY_DATASET_ROOT"] = str(replay_dataset_root)
    env["BOT_REPLAY_DATASET_PREFIX"] = str(replay_dataset_prefix)
    env["BOT_REPLAY_DATASET_YEAR"] = str(replay_dataset_year if replay_dataset_year > 0 else "")
    env["BACKTEST_DATASET_ROOT"] = str(replay_dataset_root)
    env["BACKTEST_DATASET_PREFIX"] = str(replay_dataset_prefix)
    env["BACKTEST_DATASET_YEAR"] = str(replay_dataset_year if replay_dataset_year > 0 else "")
    env["BACKTEST_CSV_DIR_5M"] = str(replay_csv_dir_5m)
    env["BACKTEST_CSV_DIR_1H"] = str(replay_csv_dir_1h)
    env["BACKTEST_CSV_GLOB_5M"] = str(replay_csv_glob_5m)
    env["BACKTEST_CSV_GLOB_1H"] = str(replay_csv_glob_1h)
    env["LWF_ENTRY_TF"] = str(spec.timeframe or "")
    shim = _build_dataset_override_shim()
    cmd: List[str] = [
        sys.executable,
        "-u",
        "-c",
        shim,
        runner_path,
        "--replay",
        "--replay-engine",
        str(spec.replay_engine or "live"),
        "--since-ms",
        str(int(spec.since_ms)),
        "--until-ms",
        str(int(spec.until_ms)),
        "--replay-csv",
        str(replay_csv_base),
    ]
    cmd.extend(["--log-level", _normalize_runtime_log_level(spec.log_level)])
    proc: Optional[subprocess.Popen] = None
    try:
        proc = _spawn(cmd=cmd, env=env, cwd=p.runtime_dir)
        return proc
    except Exception:
        terminate_process(proc)
        raise


def launch_backtest(spec: BacktestSpec) -> subprocess.Popen:
    p = ensure_runtime_dirs()
    env = _build_backtest_env(spec)

    backtest_path = os.path.join(p.repo_root, "backtest.py")
    if not os.path.exists(backtest_path):
        raise FileNotFoundError(f"backtest.py not found under repo root: {p.repo_root}")

    report_out = str(spec.report_out or "").strip()
    if report_out and (not os.path.isabs(report_out)):
        report_out = os.path.abspath(os.path.join(p.repo_root, report_out))

    shim = _build_dataset_override_shim()
    cmd: List[str] = [
        sys.executable,
        "-u",
        "-c",
        shim,
        backtest_path,
        "--symbols",
        str(spec.symbol or "BTC/JPY"),
        "--since",
        str(spec.since_ymd or ""),
        "--until",
        str(spec.until_ymd or ""),
        "--entry-tf",
        str(spec.entry_timeframe or "5m"),
    ]
    if spec.backtest_since_year is not None and spec.backtest_until_year is not None:
        cmd.extend(
            [
                "--since-year",
                str(int(spec.backtest_since_year)),
                "--until-year",
                str(int(spec.backtest_until_year)),
            ]
        )
    if bool(spec.report_enabled):
        cmd.append("--report")
        if report_out:
            cmd.extend(["--report-out", str(report_out)])
    proc: Optional[subprocess.Popen] = None
    try:
        proc = _spawn(cmd=cmd, env=env, cwd=p.runtime_dir)
        return proc
    except Exception:
        terminate_process(proc)
        raise


def terminate_process(proc: Optional[subprocess.Popen]) -> bool:
    fallback_used = False
    if proc is None:
        return fallback_used
    try:
        if proc.poll() is not None:
            return fallback_used
        if os.name == "nt":
            try:
                proc.send_signal(signal.CTRL_BREAK_EVENT)
            except Exception:
                pass
            wait_sec = 15
            try:
                wait_env = int(str(os.getenv("LWF_GUI_STOP_WAIT_SEC", "15")).strip())
                wait_sec = max(1, min(60, int(wait_env)))
            except Exception:
                wait_sec = 15
            t0 = time.time()
            while (time.time() - t0) < float(wait_sec):
                if proc.poll() is not None:
                    return fallback_used
                time.sleep(0.2)
        if proc.poll() is None:
            fallback_used = True
            try:
                proc.terminate()
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=2)
    except Exception:
        pass
    return fallback_used
