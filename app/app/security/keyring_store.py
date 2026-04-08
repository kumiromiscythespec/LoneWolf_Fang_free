# BUILD_ID: 2026-04-08_free_okx_passphrase_coincheck_default_v1
# BUILD_ID: 2026-04-08_free_bitbank_okx_spot_only_v1
# BUILD_ID: 2026-03-29_free_from_standard_nonlive_build_v1
# BUILD_ID: 2026-03-27_keyring_activation_local_reset_v1
from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import logging
import sys
import uuid
from typing import Optional

import keyring


BUILD_ID = "2026-04-08_free_okx_passphrase_coincheck_default_v1"
logger = logging.getLogger(__name__)


SERVICE_NAME = "LoneWolf Fang"
ACCOUNT_MEXC_API_KEY = "lwf_mexc_api_key"
ACCOUNT_MEXC_API_SECRET = "lwf_mexc_api_secret"
ACCOUNT_COINCHECK_API_KEY = "lwf_coincheck_api_key"
ACCOUNT_COINCHECK_API_SECRET = "lwf_coincheck_api_secret"
ACCOUNT_BINANCE_API_KEY = "lwf_binance_api_key"
ACCOUNT_BINANCE_API_SECRET = "lwf_binance_api_secret"
ACCOUNT_BITBANK_API_KEY = "lwf_bitbank_api_key"
ACCOUNT_BITBANK_API_SECRET = "lwf_bitbank_api_secret"
ACCOUNT_OKX_API_KEY = "lwf_okx_api_key"
ACCOUNT_OKX_API_SECRET = "lwf_okx_api_secret"
ACCOUNT_OKX_API_PASSPHRASE = "lwf_okx_api_passphrase"
ACCOUNT_LICENSE_SEAT_KEY = "lwf_license_seat_key"
ACCOUNT_LICENSE_DEVICE_ID = "lwf_license_device_id"
ACCOUNT_LICENSE_STATE = "lwf_license_state"
_LEGACY_ACCOUNT_NAME = "mexc"
_ROUNDTRIP_SERVICE_NAME = f"{SERVICE_NAME} Audit"
_INSECURE_BACKEND_TOKENS = ("plaintext", "fail", "null", "file")


@dataclass
class ApiCreds:
    api_key: str
    api_secret: str
    api_passphrase: str = ""


@dataclass
class LicenseState:
    product_code: str = "standard"
    seat_key: str = ""
    device_id: str = ""
    machine_hash: str = ""
    license_status: str = ""
    seat_no: int = 0
    last_verified_at: str = ""
    refresh_after: str = ""
    offline_grace_until: str = ""
    standard_enabled: bool = False
    live_allowed: bool = False
    paper_allowed: bool = False
    replay_allowed: bool = False
    backtest_allowed: bool = False
    fallback_tier_on_failure: str = ""


def _normalize_exchange_id(exchange_id: str) -> str:
    x = str(exchange_id or "coincheck").strip().lower() or "coincheck"
    if x == "coincheck":
        return "coincheck"
    if x == "bitbank":
        return "bitbank"
    if x == "okx":
        return "okx"
    return "binance" if x == "binance" else "mexc"


def _account_pair(exchange_id: str) -> tuple[str, str]:
    ex = _normalize_exchange_id(exchange_id)
    if ex == "coincheck":
        return (ACCOUNT_COINCHECK_API_KEY, ACCOUNT_COINCHECK_API_SECRET)
    if ex == "binance":
        return (ACCOUNT_BINANCE_API_KEY, ACCOUNT_BINANCE_API_SECRET)
    if ex == "bitbank":
        return (ACCOUNT_BITBANK_API_KEY, ACCOUNT_BITBANK_API_SECRET)
    if ex == "okx":
        return (ACCOUNT_OKX_API_KEY, ACCOUNT_OKX_API_SECRET)
    return (ACCOUNT_MEXC_API_KEY, ACCOUNT_MEXC_API_SECRET)


def _account_passphrase(exchange_id: str) -> str:
    ex = _normalize_exchange_id(exchange_id)
    return ACCOUNT_OKX_API_PASSPHRASE if ex == "okx" else ""


def _requires_passphrase(exchange_id: str) -> bool:
    return _normalize_exchange_id(exchange_id) == "okx"


def _backend_obj():
    return keyring.get_keyring()


def _backend_name_parts(backend) -> tuple[str, str]:
    cls_name = str(getattr(getattr(backend, "__class__", object), "__name__", "") or "")
    module_name = str(getattr(getattr(backend, "__class__", object), "__module__", "") or "")
    return (cls_name, module_name)


def _describe_backend_obj(backend) -> dict[str, object]:
    cls_name, module_name = _backend_name_parts(backend)
    combined = f"{cls_name} {module_name}".lower()
    secure = True
    reason = "backend accepted"
    matched_token = next((token for token in _INSECURE_BACKEND_TOKENS if token in combined), "")
    if matched_token:
        secure = False
        reason = f"insecure backend token:{matched_token}"
    elif sys.platform == "win32":
        if "winvault" in combined:
            secure = True
            reason = "WinVaultKeyring on Windows"
        else:
            secure = False
            reason = "non-WinVault backend on Windows"
    return {
        "backend": str(cls_name or "unknown"),
        "module": str(module_name or "unknown"),
        "secure": bool(secure),
        "reason": str(reason),
    }


def describe_backend() -> dict[str, object]:
    return _describe_backend_obj(_backend_obj())


def _backend_get_password(backend, service_name: str, account_name: str) -> str | None:
    try:
        return backend.get_password(str(service_name), str(account_name))
    except Exception:
        return None


def _backend_set_password(backend, service_name: str, account_name: str, value: str) -> bool:
    try:
        backend.set_password(str(service_name), str(account_name), str(value or ""))
        return True
    except Exception:
        return False


def _backend_delete_password(backend, service_name: str, account_name: str) -> bool:
    try:
        backend.delete_password(str(service_name), str(account_name))
        return True
    except Exception:
        return False


def _migration_accounts() -> list[str]:
    accounts = [
        ACCOUNT_MEXC_API_KEY,
        ACCOUNT_MEXC_API_SECRET,
        ACCOUNT_COINCHECK_API_KEY,
        ACCOUNT_COINCHECK_API_SECRET,
        ACCOUNT_BINANCE_API_KEY,
        ACCOUNT_BINANCE_API_SECRET,
        ACCOUNT_BITBANK_API_KEY,
        ACCOUNT_BITBANK_API_SECRET,
        ACCOUNT_OKX_API_KEY,
        ACCOUNT_OKX_API_SECRET,
        ACCOUNT_OKX_API_PASSPHRASE,
        ACCOUNT_LICENSE_SEAT_KEY,
        ACCOUNT_LICENSE_DEVICE_ID,
        ACCOUNT_LICENSE_STATE,
    ]
    accounts.extend(
        [
            f"{_LEGACY_ACCOUNT_NAME}:api_key",
            f"{_LEGACY_ACCOUNT_NAME}:api_secret",
        ]
    )
    return accounts


def _migrate_backend_entries(old_backend, new_backend) -> None:
    if old_backend is None or new_backend is None or old_backend is new_backend:
        return
    moved = 0
    for account_name in _migration_accounts():
        value = _backend_get_password(old_backend, SERVICE_NAME, account_name)
        if not value:
            continue
        if not _backend_get_password(new_backend, SERVICE_NAME, account_name):
            if not _backend_set_password(new_backend, SERVICE_NAME, account_name, value):
                continue
        _backend_delete_password(old_backend, SERVICE_NAME, account_name)
        moved += 1
    if moved > 0:
        logger.debug("[keyring] migrated stored credential entries count=%s", int(moved))


def ensure_secure_backend() -> None:
    current_backend = _backend_obj()
    current_desc = _describe_backend_obj(current_backend)
    if bool(current_desc.get("secure")):
        return
    if sys.platform != "win32":
        logger.debug(
            "[keyring] insecure backend retained backend=%s module=%s reason=%s",
            current_desc.get("backend"),
            current_desc.get("module"),
            current_desc.get("reason"),
        )
        return
    try:
        from keyring.backends.Windows import WinVaultKeyring

        new_backend = WinVaultKeyring()
        current_cls, current_mod = _backend_name_parts(current_backend)
        new_cls, new_mod = _backend_name_parts(new_backend)
        if (current_cls, current_mod) != (new_cls, new_mod):
            keyring.set_keyring(new_backend)
            final_backend = _backend_obj()
            final_desc = _describe_backend_obj(final_backend)
            if bool(final_desc.get("secure")):
                _migrate_backend_entries(current_backend, final_backend)
    except Exception as exc:
        logger.debug(
            "[keyring] secure backend switch failed backend=%s module=%s error=%s",
            current_desc.get("backend"),
            current_desc.get("module"),
            exc.__class__.__name__,
        )


def _legacy_mexc_lookup() -> tuple[str | None, str | None]:
    backend = _backend_obj()
    legacy_key = _backend_get_password(backend, SERVICE_NAME, f"{_LEGACY_ACCOUNT_NAME}:api_key")
    legacy_secret = _backend_get_password(backend, SERVICE_NAME, f"{_LEGACY_ACCOUNT_NAME}:api_secret")
    return (legacy_key, legacy_secret)


def has_creds(exchange_id: str = "mexc") -> bool:
    ensure_secure_backend()
    try:
        k_acc, s_acc = _account_pair(exchange_id)
        backend = _backend_obj()
        k = _backend_get_password(backend, SERVICE_NAME, k_acc)
        s = _backend_get_password(backend, SERVICE_NAME, s_acc)
        if _normalize_exchange_id(exchange_id) == "mexc" and (not k or not s):
            legacy_k, legacy_s = _legacy_mexc_lookup()
            k = k or legacy_k
            s = s or legacy_s
        p_acc = _account_passphrase(exchange_id)
        p = _backend_get_password(backend, SERVICE_NAME, p_acc) if p_acc else ""
        if _requires_passphrase(exchange_id) and not p:
            return False
        return bool(k and s)
    except Exception as exc:
        logger.debug("[keyring] has_creds failed exchange=%s error=%s", _normalize_exchange_id(exchange_id), exc.__class__.__name__)
        return False


def test_roundtrip(exchange_id: str = "mexc") -> bool:
    ensure_secure_backend()
    desc = describe_backend()
    if not bool(desc.get("secure")):
        logger.debug(
            "[keyring] roundtrip skipped backend=%s module=%s reason=%s",
            desc.get("backend"),
            desc.get("module"),
            desc.get("reason"),
        )
        return False
    backend = _backend_obj()
    ex = _normalize_exchange_id(exchange_id)
    key_account = f"{ex}:audit:api_key"
    secret_account = f"{ex}:audit:api_secret"
    token_key = f"audit-{uuid.uuid4().hex}"
    token_secret = f"audit-{uuid.uuid4().hex}"
    try:
        if not _backend_set_password(backend, _ROUNDTRIP_SERVICE_NAME, key_account, token_key):
            return False
        if not _backend_set_password(backend, _ROUNDTRIP_SERVICE_NAME, secret_account, token_secret):
            return False
        saved_key = _backend_get_password(backend, _ROUNDTRIP_SERVICE_NAME, key_account)
        saved_secret = _backend_get_password(backend, _ROUNDTRIP_SERVICE_NAME, secret_account)
        if not (saved_key and saved_secret):
            return False
        _backend_delete_password(backend, _ROUNDTRIP_SERVICE_NAME, key_account)
        _backend_delete_password(backend, _ROUNDTRIP_SERVICE_NAME, secret_account)
        cleared_key = _backend_get_password(backend, _ROUNDTRIP_SERVICE_NAME, key_account)
        cleared_secret = _backend_get_password(backend, _ROUNDTRIP_SERVICE_NAME, secret_account)
        return not bool(cleared_key or cleared_secret)
    except Exception as exc:
        logger.debug("[keyring] roundtrip failed exchange=%s error=%s", ex, exc.__class__.__name__)
        return False
    finally:
        _backend_delete_password(backend, _ROUNDTRIP_SERVICE_NAME, key_account)
        _backend_delete_password(backend, _ROUNDTRIP_SERVICE_NAME, secret_account)


def load_creds(exchange_id: str = "mexc") -> Optional[ApiCreds]:
    ensure_secure_backend()
    try:
        k_acc, s_acc = _account_pair(exchange_id)
        backend = _backend_obj()
        k = _backend_get_password(backend, SERVICE_NAME, k_acc)
        s = _backend_get_password(backend, SERVICE_NAME, s_acc)
        if _normalize_exchange_id(exchange_id) == "mexc" and (not k or not s):
            legacy_k, legacy_s = _legacy_mexc_lookup()
            k = k or legacy_k
            s = s or legacy_s
        p_acc = _account_passphrase(exchange_id)
        p = _backend_get_password(backend, SERVICE_NAME, p_acc) if p_acc else ""
        if not k or not s:
            return None
        if _requires_passphrase(exchange_id) and not p:
            return None
        return ApiCreds(api_key=str(k), api_secret=str(s), api_passphrase=str(p or ""))
    except Exception as exc:
        desc = describe_backend()
        logger.debug(
            "[keyring] load_creds failed exchange=%s backend=%s module=%s secure=%s reason=%s error=%s",
            _normalize_exchange_id(exchange_id),
            desc.get("backend"),
            desc.get("module"),
            desc.get("secure"),
            desc.get("reason"),
            exc.__class__.__name__,
        )
        return None


def save_creds(api_key: str, api_secret: str, exchange_id: str = "mexc", api_passphrase: str = "") -> None:
    ensure_secure_backend()
    desc = describe_backend()
    if not bool(desc.get("secure")):
        raise RuntimeError(
            f"insecure keyring backend blocked: {desc.get('backend')} ({desc.get('reason')})"
        )
    k_acc, s_acc = _account_pair(exchange_id)
    p_acc = _account_passphrase(exchange_id)
    backend = _backend_obj()
    passphrase = str(api_passphrase or "")
    if _requires_passphrase(exchange_id) and not passphrase:
        raise RuntimeError("OKX API passphrase is required")
    if not _backend_set_password(backend, SERVICE_NAME, k_acc, str(api_key or "")):
        raise RuntimeError("failed to save API key to keyring")
    if not _backend_set_password(backend, SERVICE_NAME, s_acc, str(api_secret or "")):
        _backend_delete_password(backend, SERVICE_NAME, k_acc)
        raise RuntimeError("failed to save API secret to keyring")
    if p_acc:
        if not _backend_set_password(backend, SERVICE_NAME, p_acc, passphrase):
            _backend_delete_password(backend, SERVICE_NAME, k_acc)
            _backend_delete_password(backend, SERVICE_NAME, s_acc)
            raise RuntimeError("failed to save API passphrase to keyring")


def clear_creds(exchange_id: str = "mexc") -> None:
    ensure_secure_backend()
    k_acc, s_acc = _account_pair(exchange_id)
    p_acc = _account_passphrase(exchange_id)
    backend = _backend_obj()
    _backend_delete_password(backend, SERVICE_NAME, k_acc)
    _backend_delete_password(backend, SERVICE_NAME, s_acc)
    if p_acc:
        _backend_delete_password(backend, SERVICE_NAME, p_acc)
    if _normalize_exchange_id(exchange_id) == "mexc":
        _backend_delete_password(backend, SERVICE_NAME, f"{_LEGACY_ACCOUNT_NAME}:api_key")
        _backend_delete_password(backend, SERVICE_NAME, f"{_LEGACY_ACCOUNT_NAME}:api_secret")


def _require_secure_backend_for_license():
    ensure_secure_backend()
    desc = describe_backend()
    if not bool(desc.get("secure")):
        raise RuntimeError(
            f"insecure keyring backend blocked: {desc.get('backend')} ({desc.get('reason')})"
        )
    return _backend_obj()


def load_license_state() -> Optional[LicenseState]:
    ensure_secure_backend()
    try:
        backend = _backend_obj()
        raw = _backend_get_password(backend, SERVICE_NAME, ACCOUNT_LICENSE_STATE)
        if not raw:
            return None
        payload = json.loads(str(raw))
        if not isinstance(payload, dict):
            return None
        try:
            seat_no = int(payload.get("seat_no") or 0)
        except Exception:
            seat_no = 0
        return LicenseState(
            product_code=str(payload.get("product_code") or "standard"),
            seat_key=load_license_seat_key(),
            device_id=load_license_device_id(),
            machine_hash=str(payload.get("machine_hash") or ""),
            license_status=str(payload.get("license_status") or ""),
            seat_no=seat_no,
            last_verified_at=str(payload.get("last_verified_at") or ""),
            refresh_after=str(payload.get("refresh_after") or ""),
            offline_grace_until=str(payload.get("offline_grace_until") or ""),
            standard_enabled=bool(payload.get("standard_enabled")),
            live_allowed=bool(payload.get("live_allowed")),
            paper_allowed=bool(payload.get("paper_allowed")),
            replay_allowed=bool(payload.get("replay_allowed")),
            backtest_allowed=bool(payload.get("backtest_allowed")),
            fallback_tier_on_failure=str(payload.get("fallback_tier_on_failure") or ""),
        )
    except Exception as exc:
        logger.debug("[keyring] load_license_state failed error=%s", exc.__class__.__name__)
        return None


def save_license_state(state: LicenseState) -> None:
    backend = _require_secure_backend_for_license()
    payload = asdict(state)
    payload["seat_key"] = ""
    payload["device_id"] = ""
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    if not _backend_set_password(backend, SERVICE_NAME, ACCOUNT_LICENSE_STATE, raw):
        raise RuntimeError("failed to save license state to keyring")


def clear_license_state() -> None:
    ensure_secure_backend()
    _backend_delete_password(_backend_obj(), SERVICE_NAME, ACCOUNT_LICENSE_STATE)


def load_license_seat_key() -> str:
    ensure_secure_backend()
    try:
        return str(_backend_get_password(_backend_obj(), SERVICE_NAME, ACCOUNT_LICENSE_SEAT_KEY) or "")
    except Exception:
        return ""


def save_license_seat_key(seat_key: str) -> None:
    backend = _require_secure_backend_for_license()
    if not _backend_set_password(backend, SERVICE_NAME, ACCOUNT_LICENSE_SEAT_KEY, str(seat_key or "")):
        raise RuntimeError("failed to save license seat key to keyring")


def clear_license_seat_key() -> None:
    ensure_secure_backend()
    _backend_delete_password(_backend_obj(), SERVICE_NAME, ACCOUNT_LICENSE_SEAT_KEY)


def clear_license_device_id() -> None:
    ensure_secure_backend()
    _backend_delete_password(_backend_obj(), SERVICE_NAME, ACCOUNT_LICENSE_DEVICE_ID)


def clear_license_local_test_state() -> None:
    ensure_secure_backend()
    backend = _backend_obj()
    _backend_delete_password(backend, SERVICE_NAME, ACCOUNT_LICENSE_STATE)
    _backend_delete_password(backend, SERVICE_NAME, ACCOUNT_LICENSE_SEAT_KEY)
    _backend_delete_password(backend, SERVICE_NAME, ACCOUNT_LICENSE_DEVICE_ID)


def load_license_device_id() -> str:
    ensure_secure_backend()
    try:
        return str(_backend_get_password(_backend_obj(), SERVICE_NAME, ACCOUNT_LICENSE_DEVICE_ID) or "")
    except Exception:
        return ""


def save_license_device_id(device_id: str) -> None:
    backend = _require_secure_backend_for_license()
    if not _backend_set_password(backend, SERVICE_NAME, ACCOUNT_LICENSE_DEVICE_ID, str(device_id or "")):
        raise RuntimeError("failed to save license device id to keyring")


def get_or_create_license_device_id() -> str:
    device_id = load_license_device_id()
    if device_id:
        return device_id
    device_id = f"lwf-device-{uuid.uuid4().hex}"
    save_license_device_id(device_id)
    return device_id
