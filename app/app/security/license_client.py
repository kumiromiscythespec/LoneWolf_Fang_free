# BUILD_ID: 2026-03-29_free_from_standard_nonlive_build_v1
# BUILD_ID: 2026-03-26_standard_activate_refresh_device_id_restore_v1
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import os
import platform
import socket
import urllib.error
import urllib.request
from typing import Any


BUILD_ID = "2026-03-26_standard_activate_refresh_device_id_restore_v1"
_DEFAULT_PRODUCT_CODE = "standard"


@dataclass
class LicenseClientConfig:
    base_url: str
    timeout_sec: float = 10.0
    app_version: str = ""
    build_id: str = ""
    device_name: str = ""
    product_code: str = _DEFAULT_PRODUCT_CODE


@dataclass
class LicenseLease:
    verified_at: str = ""
    refresh_after: str = ""
    offline_grace_until: str = ""


@dataclass
class LicenseEntitlements:
    standard_enabled: bool = False
    live_allowed: bool = False
    paper_allowed: bool = False
    replay_allowed: bool = False
    backtest_allowed: bool = False
    fallback_tier_on_failure: str = ""


@dataclass
class LicenseDeviceInfo:
    device_id: str = ""
    status: str = ""
    activated_at: str = ""
    last_verified_at: str = ""


@dataclass
class LicenseResponse:
    ok: bool = False
    outcome: str = ""
    error_code: str = ""
    error_message: str = ""
    http_status: int = 0
    product_code: str = _DEFAULT_PRODUCT_CODE
    seat_no: int = 0
    device: LicenseDeviceInfo = field(default_factory=LicenseDeviceInfo)
    lease: LicenseLease = field(default_factory=LicenseLease)
    entitlements: LicenseEntitlements = field(default_factory=LicenseEntitlements)
    raw_body: dict[str, Any] = field(default_factory=dict)


def default_license_base_url() -> str:
    for env_name in ("LWF_LICENSE_BASE_URL", "LWF_SITE_BASE_URL"):
        raw = str(os.getenv(env_name) or "").strip()
        if raw:
            return normalize_license_base_url(raw)
    return "https://lonewolffang.com"


def normalize_license_base_url(raw: str) -> str:
    value = str(raw or "").strip()
    if not value:
        value = default_license_base_url()
    value = value.rstrip("/")
    if value.lower().endswith("/api"):
        value = value[:-4].rstrip("/")
    return value


def build_machine_summary() -> dict[str, str]:
    hostname = str(os.getenv("COMPUTERNAME") or socket.gethostname() or "").strip()
    arch = str(platform.machine() or os.getenv("PROCESSOR_ARCHITECTURE") or "").strip()
    return {
        "os": str(platform.system() or os.name or "").strip(),
        "hostname": hostname,
        "arch": arch,
    }


def compute_machine_hash(machine_summary: dict[str, str] | None = None) -> str:
    payload = machine_summary if isinstance(machine_summary, dict) else build_machine_summary()
    normalized = {
        "arch": str(payload.get("arch") or "").strip(),
        "hostname": str(payload.get("hostname") or "").strip(),
        "os": str(payload.get("os") or "").strip(),
    }
    body = json.dumps(normalized, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def post_json(url: str, payload: dict, timeout_sec: float = 10.0) -> tuple[int, dict]:
    try:
        request_body = json.dumps(payload or {}, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            str(url or ""),
            data=request_body,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
                "User-Agent": "LoneWolfFang-Standard-LicenseClient/1",
            },
            method="POST",
        )
    except Exception as exc:
        return (
            0,
            {
                "ok": False,
                "error_code": "invalid_response",
                "message": f"request build failed: {exc.__class__.__name__}",
            },
        )

    http_status = 0
    raw_body = b""
    try:
        with urllib.request.urlopen(req, timeout=float(timeout_sec)) as resp:
            http_status = int(getattr(resp, "status", 0) or resp.getcode() or 0)
            raw_body = resp.read() or b""
    except urllib.error.HTTPError as exc:
        http_status = int(getattr(exc, "code", 0) or 0)
        try:
            raw_body = exc.read() or b""
        except Exception:
            raw_body = b""
    except (urllib.error.URLError, OSError, ValueError) as exc:
        return (
            0,
            {
                "ok": False,
                "error_code": "network_error",
                "message": str(getattr(exc, "reason", "") or str(exc) or exc.__class__.__name__),
            },
        )
    except Exception as exc:
        return (
            0,
            {
                "ok": False,
                "error_code": "network_error",
                "message": exc.__class__.__name__,
            },
        )

    try:
        text = raw_body.decode("utf-8") if raw_body else ""
    except Exception:
        text = ""
    if not text.strip():
        return (http_status, {"ok": False, "error_code": "invalid_response", "message": "empty response body"})
    try:
        body = json.loads(text)
    except Exception:
        return (http_status, {"ok": False, "error_code": "invalid_response", "message": "response is not valid JSON"})
    if not isinstance(body, dict):
        return (http_status, {"ok": False, "error_code": "invalid_response", "message": "response JSON is not an object"})
    return (http_status, body)


def parse_license_response(http_status: int, body: dict) -> LicenseResponse:
    if not isinstance(body, dict):
        return LicenseResponse(
            ok=False,
            error_code="invalid_response",
            error_message="response body is not a JSON object",
            http_status=int(http_status or 0),
            raw_body={},
        )

    device_raw = body.get("device")
    lease_raw = body.get("lease")
    entitlements_raw = body.get("entitlements")
    device = device_raw if isinstance(device_raw, dict) else {}
    lease = lease_raw if isinstance(lease_raw, dict) else {}
    entitlements = entitlements_raw if isinstance(entitlements_raw, dict) else {}
    ok = bool(body.get("ok"))
    error_code = str(body.get("error_code") or body.get("errorCode") or body.get("code") or "").strip()
    if not ok and not error_code:
        error_code = "network_error" if int(http_status or 0) <= 0 else "invalid_response"
    error_message = str(body.get("message") or body.get("detail") or body.get("error") or "").strip()

    try:
        seat_no = int(body.get("seatNo") or body.get("seat_no") or 0)
    except Exception:
        seat_no = 0

    return LicenseResponse(
        ok=ok,
        outcome=str(body.get("outcome") or "").strip(),
        error_code=error_code,
        error_message=error_message,
        http_status=int(http_status or 0),
        product_code=str(body.get("productCode") or body.get("product_code") or _DEFAULT_PRODUCT_CODE).strip() or _DEFAULT_PRODUCT_CODE,
        seat_no=seat_no,
        device=LicenseDeviceInfo(
            device_id=str(device.get("deviceId") or device.get("device_id") or "").strip(),
            status=str(device.get("status") or "").strip(),
            activated_at=str(device.get("activatedAt") or device.get("activated_at") or "").strip(),
            last_verified_at=str(device.get("lastVerifiedAt") or device.get("last_verified_at") or "").strip(),
        ),
        lease=LicenseLease(
            verified_at=str(lease.get("verifiedAt") or lease.get("verified_at") or "").strip(),
            refresh_after=str(lease.get("refreshAfter") or lease.get("refresh_after") or "").strip(),
            offline_grace_until=str(lease.get("offlineGraceUntil") or lease.get("offline_grace_until") or "").strip(),
        ),
        entitlements=LicenseEntitlements(
            standard_enabled=bool(entitlements.get("standardEnabled")),
            live_allowed=bool(entitlements.get("liveAllowed")),
            paper_allowed=bool(entitlements.get("paperAllowed")),
            replay_allowed=bool(entitlements.get("replayAllowed")),
            backtest_allowed=bool(entitlements.get("backtestAllowed")),
            fallback_tier_on_failure=str(
                entitlements.get("fallbackTierOnFailure") or entitlements.get("fallback_tier_on_failure") or ""
            ).strip(),
        ),
        raw_body=dict(body),
    )


def activate_license(
    seat_key: str,
    device_id: str,
    *,
    base_url: str = "",
    app_version: str = "",
    build_id: str = "",
    device_name: str = "",
) -> LicenseResponse:
    config = LicenseClientConfig(
        base_url=normalize_license_base_url(base_url),
        app_version=str(app_version or "").strip(),
        build_id=str(build_id or "").strip(),
        device_name=str(device_name or "").strip(),
    )
    machine_summary = build_machine_summary()
    http_status, body = post_json(
        f"{config.base_url}/api/desktop/activate",
        {
            "productCode": config.product_code,
            "seatKey": str(seat_key or "").strip(),
            "device": {
                "deviceId": str(device_id or "").strip(),
                "machineHash": compute_machine_hash(machine_summary),
                "deviceName": config.device_name,
                "machineSummary": machine_summary,
            },
            "client": {
                "appVersion": config.app_version,
                "buildId": config.build_id,
            },
        },
        timeout_sec=config.timeout_sec,
    )
    return parse_license_response(http_status, body)


def deactivate_license(
    seat_key: str,
    device_id: str,
    *,
    base_url: str = "",
    app_version: str = "",
    build_id: str = "",
) -> LicenseResponse:
    config = LicenseClientConfig(
        base_url=normalize_license_base_url(base_url),
        app_version=str(app_version or "").strip(),
        build_id=str(build_id or "").strip(),
    )
    http_status, body = post_json(
        f"{config.base_url}/api/desktop/deactivate",
        {
            "productCode": config.product_code,
            "seatKey": str(seat_key or "").strip(),
            "device": {
                "deviceId": "",
                "machineHash": compute_machine_hash(),
            },
            "client": {
                "appVersion": config.app_version,
                "buildId": config.build_id,
            },
        },
        timeout_sec=config.timeout_sec,
    )
    return parse_license_response(http_status, body)


def refresh_license(
    seat_key: str,
    device_id: str,
    *,
    base_url: str = "",
    app_version: str = "",
    build_id: str = "",
) -> LicenseResponse:
    config = LicenseClientConfig(
        base_url=normalize_license_base_url(base_url),
        app_version=str(app_version or "").strip(),
        build_id=str(build_id or "").strip(),
    )
    http_status, body = post_json(
        f"{config.base_url}/api/desktop/refresh",
        {
            "productCode": config.product_code,
            "seatKey": str(seat_key or "").strip(),
            "device": {
                "deviceId": str(device_id or "").strip(),
                "machineHash": compute_machine_hash(),
            },
            "client": {
                "appVersion": config.app_version,
                "buildId": config.build_id,
            },
        },
        timeout_sec=config.timeout_sec,
    )
    return parse_license_response(http_status, body)
