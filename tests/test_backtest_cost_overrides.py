# BUILD_ID: 2026-05-28_ethusdt_fee_spread_slippage_parameterization_tests_v1
from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _import_backtest_without_exchange() -> Any:
    for module_name in ("backtest", "runner", "exchange", "ccxt"):
        sys.modules.pop(module_name, None)
    module = importlib.import_module("backtest")
    assert "exchange" not in sys.modules
    assert "ccxt" not in sys.modules
    return module


def _clear_cost_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "BACKTEST_MAKER_FEE",
        "BACKTEST_TAKER_FEE",
        "BACKTEST_SPREAD_BPS",
        "BACKTEST_SLIPPAGE_BPS",
    ):
        monkeypatch.delenv(name, raising=False)


def _resolve_costs(backtest: Any, **kwargs: Any) -> Any:
    return backtest._resolve_effective_backtest_costs(
        default_maker_fee=0.0001,
        default_taker_fee=0.0002,
        default_spread_bps=2.0,
        default_slippage_bps=0.0,
        default_sources={
            "maker_fee_rate": "config",
            "taker_fee_rate": "config",
            "spread_bps": "config",
            "slippage_bps": "config",
        },
        **kwargs,
    )


def _minimal_result(kwargs: dict[str, Any]) -> dict[str, Any]:
    initial = float(kwargs.get("initial_equity", 1000.0) or 1000.0)
    return {
        "symbols": list(kwargs.get("symbols") or ["ETH/USDT"]),
        "since_ms": kwargs.get("since_ms"),
        "since_ms_effective": kwargs.get("since_ms"),
        "since_iso_effective": "",
        "entry_tf": str(kwargs.get("entry_tf") or "5m"),
        "filter_tf": str(kwargs.get("filter_tf") or "1h"),
        "warmup_bars": int(kwargs.get("warmup_bars") or 0),
        "spread_bps_est": 0.0,
        "recent_bars_entry": kwargs.get("recent_bars_entry"),
        "recent_bars_filter": kwargs.get("recent_bars_filter"),
        "trades": 0,
        "buy_signals": 0,
        "entries_opened": 0,
        "forced_closes": 0,
        "open_positions_end": 0,
        "win": 0,
        "lose": 0,
        "final_equity": initial,
        "max_dd": 0.0,
        "max_dd_mtm": 0.0,
        "net_total": 0.0,
        "net_avg": 0.0,
        "export_dir": "",
    }


def test_defaults_unchanged_without_override(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_cost_env(monkeypatch)
    backtest = _import_backtest_without_exchange()

    costs = _resolve_costs(backtest)

    assert costs.maker_fee == pytest.approx(0.0001)
    assert costs.taker_fee == pytest.approx(0.0002)
    assert costs.spread_bps == pytest.approx(2.0)
    assert costs.slippage_bps == pytest.approx(0.0)
    assert costs.sources == {
        "maker_fee_rate": "config",
        "taker_fee_rate": "config",
        "spread_bps": "config",
        "slippage_bps": "config",
    }
    assert costs.override_source == "default"


def test_cli_maker_taker_override_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_cost_env(monkeypatch)
    backtest = _import_backtest_without_exchange()

    costs = _resolve_costs(backtest, maker_fee_cli="0.0003", taker_fee_cli="0.0007")

    assert costs.maker_fee == pytest.approx(0.0003)
    assert costs.taker_fee == pytest.approx(0.0007)
    assert costs.sources["maker_fee_rate"] == "cli"
    assert costs.sources["taker_fee_rate"] == "cli"
    assert costs.override_source == "cli"


def test_env_maker_taker_override_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_cost_env(monkeypatch)
    monkeypatch.setenv("BACKTEST_MAKER_FEE", "0.0004")
    monkeypatch.setenv("BACKTEST_TAKER_FEE", "0.0008")
    backtest = _import_backtest_without_exchange()

    costs = _resolve_costs(backtest)

    assert costs.maker_fee == pytest.approx(0.0004)
    assert costs.taker_fee == pytest.approx(0.0008)
    assert costs.sources["maker_fee_rate"] == "env"
    assert costs.sources["taker_fee_rate"] == "env"
    assert costs.override_source == "env"


def test_cli_beats_env(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_cost_env(monkeypatch)
    monkeypatch.setenv("BACKTEST_TAKER_FEE", "0.0008")
    monkeypatch.setenv("BACKTEST_SPREAD_BPS", "10")
    backtest = _import_backtest_without_exchange()

    costs = _resolve_costs(backtest, taker_fee_cli="0.0005", spread_bps_cli="6")

    assert costs.taker_fee == pytest.approx(0.0005)
    assert costs.spread_bps == pytest.approx(6.0)
    assert costs.sources["taker_fee_rate"] == "cli"
    assert costs.sources["spread_bps"] == "cli"
    assert costs.override_source == "cli"


def test_spread_bps_is_total_spread_bps(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_cost_env(monkeypatch)
    backtest = _import_backtest_without_exchange()

    costs = _resolve_costs(backtest, spread_bps_cli="6")
    meta = backtest._build_effective_cost_assumptions(costs, mixed_exchange_basis_warning=True)

    assert meta["effective_spread_bps"] == pytest.approx(6.0)
    assert meta["effective_spread_bps_semantics"] == "total_spread_bps"
    assert meta["spread_bps_semantics"] == "total_bid_ask_scalar_estimate_not_fill_simulator"
    assert meta["spread_input_basis"] == "total_bps"


def test_backtest_spread_bps_env_still_works(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_cost_env(monkeypatch)
    monkeypatch.setenv("BACKTEST_SPREAD_BPS", "10")
    backtest = _import_backtest_without_exchange()

    spread_bps, env_override = backtest._resolve_backtest_spread_bps()
    costs = _resolve_costs(backtest)

    assert spread_bps == pytest.approx(10.0)
    assert env_override is True
    assert costs.spread_bps == pytest.approx(10.0)
    assert costs.sources["spread_bps"] == "env"


def test_slippage_bps_override_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_cost_env(monkeypatch)
    monkeypatch.setenv("BACKTEST_SLIPPAGE_BPS", "3")
    backtest = _import_backtest_without_exchange()

    costs = _resolve_costs(backtest)
    meta = backtest._build_effective_cost_assumptions(costs, mixed_exchange_basis_warning=True)

    assert costs.slippage_bps == pytest.approx(3.0)
    assert costs.sources["slippage_bps"] == "env"
    assert meta["effective_slippage_bps_semantics"] == "one_way_per_side_entry_plus_exit_minus"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"maker_fee_cli": "-0.1"},
        {"taker_fee_cli": "nan"},
        {"spread_bps_cli": "inf"},
        {"slippage_bps_cli": "not-a-number"},
    ],
)
def test_invalid_negative_or_nonfinite_cli_values_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
    kwargs: dict[str, str],
) -> None:
    _clear_cost_env(monkeypatch)
    backtest = _import_backtest_without_exchange()

    with pytest.raises(ValueError, match="finite number >= 0"):
        _resolve_costs(backtest, **kwargs)


def test_invalid_env_value_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_cost_env(monkeypatch)
    monkeypatch.setenv("BACKTEST_SLIPPAGE_BPS", "-1")
    backtest = _import_backtest_without_exchange()

    with pytest.raises(ValueError, match="BACKTEST_SLIPPAGE_BPS"):
        _resolve_costs(backtest)


def test_metadata_records_effective_values_and_research_safety_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_cost_env(monkeypatch)
    backtest = _import_backtest_without_exchange()

    costs = _resolve_costs(
        backtest,
        maker_fee_cli="0.00011",
        taker_fee_cli="0.00022",
        spread_bps_cli="2",
        slippage_bps_cli="1",
    )
    meta = backtest._build_effective_cost_assumptions(costs, mixed_exchange_basis_warning=True)

    assert meta["effective_maker_fee"] == pytest.approx(0.00011)
    assert meta["effective_taker_fee"] == pytest.approx(0.00022)
    assert meta["effective_spread_bps"] == pytest.approx(2.0)
    assert meta["effective_slippage_bps"] == pytest.approx(1.0)
    assert meta["override_source"] == "cli"
    assert meta["override_sources"]["taker_fee_rate"] == "cli"
    assert meta["research_only"] is True
    assert meta["mixed_exchange_basis_warning"] is True
    assert meta["final_validation"] is False
    assert meta["public_claim"] is False


def test_cli_cost_overrides_pass_to_run_backtest_without_running_real_backtest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_cost_env(monkeypatch)
    backtest = _import_backtest_without_exchange()
    called: dict[str, Any] = {}

    def fake_run_backtest(**kwargs: Any) -> dict[str, Any]:
        called.update(kwargs)
        return _minimal_result(kwargs)

    monkeypatch.setattr(backtest, "run_backtest", fake_run_backtest)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backtest.py",
            "--symbols",
            "ETH/USDT",
            "--entry-tf",
            "5m",
            "--filter-tf",
            "1h",
            "--initial",
            "1000",
            "--no-csv",
            "--maker-fee",
            "0.0003",
            "--taker-fee",
            "0.0005",
            "--spread-bps",
            "6",
            "--slippage-bps",
            "3",
        ],
    )

    assert backtest.main() == 0
    assert called["maker_fee_override"] == "0.0003"
    assert called["taker_fee_override"] == "0.0005"
    assert called["spread_bps_override"] == "6"
    assert called["slippage_bps_override"] == "3"
    assert "exchange" not in sys.modules
    assert "ccxt" not in sys.modules


def test_invalid_env_fails_before_run_backtest(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_cost_env(monkeypatch)
    monkeypatch.setenv("BACKTEST_TAKER_FEE", "nan")
    backtest = _import_backtest_without_exchange()
    monkeypatch.setattr(backtest, "run_backtest", lambda **_kwargs: pytest.fail("backtest must not run"))
    monkeypatch.setattr(sys, "argv", ["backtest.py", "--symbols", "ETH/USDT", "--no-csv"])

    assert backtest.main() == 2
    assert "exchange" not in sys.modules
    assert "ccxt" not in sys.modules
