# BUILD_ID: 2026-05-08_free_precomputed_signals_foundation_v1
from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import signal_tape as tape


def _sample_manifest(signal_set_id: str = "sig_unit") -> dict:
    return tape.build_manifest_template(
        product=tape.DEFAULT_PRODUCT,
        producer_script="deferred_phase2",
        producer_build_id=tape.BUILD_ID,
        symbol="BTC/USDT",
        entry_tf="5m",
        filter_tf="1h",
        since_ms=1000,
        until_ms=5000,
        dataset_id="synthetic_unit",
        dataset_files_hash=tape.stable_json_hash({"dataset": "synthetic_unit"}),
        strategy_file_hash=tape.stable_json_hash({"strategy": "read_only_reference"}),
        strategy_build_id="read_only_reference",
        config_file_hash=tape.stable_json_hash({"config": "read_only_reference"}),
        signal_config_hash=tape.stable_json_hash({"signal": "synthetic_unit"}),
        accounting_config_hash=tape.stable_json_hash({"accounting": "synthetic_unit"}),
        signal_set_id=signal_set_id,
    )


def test_root_resolution_prefers_env_then_localappdata(tmp_path) -> None:
    env_root = tmp_path / "explicit"
    env = {
        "LWF_PRECOMPUTED_SIGNALS_ROOT": str(env_root),
        "LOCALAPPDATA": str(tmp_path / "local"),
    }
    assert tape.resolve_precomputed_signals_root(env=env) == env_root

    env.pop("LWF_PRECOMPUTED_SIGNALS_ROOT")
    assert tape.resolve_precomputed_signals_root(env=env) == tmp_path / "local" / "LoneWolfFang" / "data" / "precomputed_signals"


def test_path_schema_uses_free_product_and_symbol_normalization(tmp_path) -> None:
    path = tape.signal_tape_dir(
        product=tape.DEFAULT_PRODUCT,
        symbol="BTC/USDT",
        entry_tf="5m",
        filter_tf="1h",
        signal_set_id="sig_xxx",
        root=tmp_path,
    )
    assert path == tmp_path / "free" / "BTCUSDT" / "5m_1h" / "sig_xxx"
    assert tape.normalize_symbol("BTC/USDT") == "BTCUSDT"
    assert tape.normalize_symbol("btc_usdt") == "BTCUSDT"
    assert tape.normalize_symbol("btc-usdt") == "BTCUSDT"


def test_manifest_safety_scope_safe_passes() -> None:
    manifest = _sample_manifest()
    loaded = tape.validate_manifest(manifest)
    assert loaded["product"] == "free"
    assert loaded["symbol_normalized"] == "BTCUSDT"
    assert loaded["safety_scope"] == tape.SAFETY_SCOPE


def test_manifest_safety_scope_unsafe_fails_closed() -> None:
    manifest = _sample_manifest()
    manifest["safety_scope"] = dict(tape.SAFETY_SCOPE)
    manifest["safety_scope"]["paper_live_order_execution"] = True
    with pytest.raises(tape.SignalTapeError):
        tape.validate_manifest(manifest)


def test_forbidden_field_fails_closed() -> None:
    manifest = _sample_manifest()
    manifest["apiKey"] = "unit-value"
    with pytest.raises(tape.SignalTapeError):
        tape.validate_manifest(manifest)


def test_summary_and_trades_forbidden_payloads_fail_closed() -> None:
    with pytest.raises(tape.SignalTapeError):
        tape.validate_summary({"max_drawdown": -1.0, "access_token": "unit-value"})

    with pytest.raises(tape.SignalTapeError):
        tape.validate_no_secret_payload([{"trade_id": "1", "raw_order": {"id": "unit"}}])

    with pytest.raises(tape.SignalTapeError):
        tape.validate_no_secret_payload({"summary": {"balance_snapshot": 1000}})


def test_negative_text_without_credential_value_is_allowed() -> None:
    tape.validate_no_secret_payload({"note": "no token"})
    tape.validate_no_secret_payload({"note": "without secret"})
