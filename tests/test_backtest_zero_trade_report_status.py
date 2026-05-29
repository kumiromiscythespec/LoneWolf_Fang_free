# BUILD_ID: 2026-05-29_ethusdt_zero_trade_status_metadata_improvement_tests_v1
# BUILD_ID: 2026-05-29_ethusdt_zero_trade_report_behavior_tests_v1
from __future__ import annotations

import importlib
import json
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


def _minimal_result(
    kwargs: dict[str, Any],
    export_dir: Path,
    *,
    trades: int,
    run_id: str = "unit_zero_trade_report",
) -> dict[str, Any]:
    initial = float(kwargs.get("initial_equity", 1000.0) or 1000.0)
    net_total = 10.0 if trades else 0.0
    return {
        "symbols": list(kwargs.get("symbols") or ["ETH/USDT"]),
        "since_ms": kwargs.get("since_ms"),
        "since_ms_effective": kwargs.get("since_ms"),
        "since_iso_effective": "",
        "entry_tf": str(kwargs.get("entry_tf") or "5m"),
        "filter_tf": str(kwargs.get("filter_tf") or "1h"),
        "warmup_bars": int(kwargs.get("warmup_bars") or 0),
        "spread_bps_est": 2.0,
        "recent_bars_entry": kwargs.get("recent_bars_entry"),
        "recent_bars_filter": kwargs.get("recent_bars_filter"),
        "trades": int(trades),
        "buy_signals": 0,
        "entries_opened": int(trades),
        "forced_closes": 0,
        "open_positions_end": 0,
        "win": int(trades),
        "lose": 0,
        "final_equity": initial + net_total,
        "max_dd": 0.0,
        "max_dd_mtm": 0.0,
        "max_dd_worst_bar": 0.0,
        "net_total": net_total,
        "net_avg": net_total / max(1, int(trades)),
        "export_dir": str(export_dir),
        "run_id": str(run_id),
        "effective_maker_fee": 0.0001,
        "effective_taker_fee": 0.0002,
        "effective_spread_bps": 2.0,
        "effective_slippage_bps": 1.0,
        "effective_cost_assumptions": {
            "effective_maker_fee": 0.0001,
            "effective_taker_fee": 0.0002,
            "effective_spread_bps": 2.0,
            "effective_slippage_bps": 1.0,
            "mixed_exchange_basis_warning": True,
        },
        "mixed_exchange_basis_warning": True,
    }


def test_report_requested_zero_trade_writes_non_comparable_status(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    backtest = _import_backtest_without_exchange()
    export_dir = tmp_path / "exports"
    last_run: dict[str, Any] = {}

    def fake_run_backtest(**kwargs: Any) -> dict[str, Any]:
        backtest._CURRENT_EXPORT_DIR = str(export_dir)
        backtest._CURRENT_RUN_ID = "unit_zero_trade_report"
        backtest._CURRENT_EXPORT_SYMBOL = "ETH/USDT"
        return _minimal_result(kwargs, export_dir, trades=0)

    monkeypatch.setattr(backtest, "run_backtest", fake_run_backtest)
    monkeypatch.setattr(backtest.C, "apply_preset", lambda _name: None, raising=False)
    monkeypatch.setattr(backtest, "_write_last_run_reference", lambda **kwargs: last_run.update(kwargs) or "")
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
            "--report",
            "--report-out",
            str(export_dir / "report.json"),
            "--preset",
            "combined_heavy",
        ],
    )

    assert backtest.main() == 0

    status_json = export_dir / "scenario_status.json"
    status_md = export_dir / "scenario_status.md"
    assert status_json.is_file()
    assert status_md.is_file()
    assert not (export_dir / "report.json").exists()

    payload = json.loads(status_json.read_text(encoding="utf-8"))
    assert payload["artifact_type"] == "zero_trade_status"
    assert payload["status"] == "zero_trade"
    assert payload["classification"] == "kill_gate_zero_trade_non_comparable"
    assert payload["comparable_metrics_available"] is False
    assert payload["report_generated"] is False
    assert payload["trades_count"] == 0
    assert payload["scenario"] == "combined_heavy"
    assert payload["scenario_label"] == "combined_heavy"
    assert payload["scenario_source"] == "preset"
    assert payload["research_only"] is True
    assert payload["mixed_exchange_basis_warning"] is True
    assert payload["final_validation_approved"] is False
    assert payload["public_performance_claim_approved"] is False
    assert payload["paper_live_order_allowed"] is False
    assert payload["normal_report_path"] == str(export_dir / "report.json")
    assert payload["normal_report_path_is_relative"] is False
    assert payload["normal_report_path_base"] is None
    assert payload["normal_report_path_resolved"] == str(export_dir / "report.json")
    assert "trades.csv" in payload["missing_report_inputs"]
    assert "zero_trades" in payload["normal_report_skipped_reason"]

    md_text = status_md.read_text(encoding="utf-8")
    assert "Zero-trade / kill-gate occurred." in md_text
    assert "metrics-comparable" in md_text
    assert "PAPER/LIVE/order readiness: false" in md_text
    assert last_run["extra"]["report_written"] is False
    assert "exchange" not in sys.modules
    assert "ccxt" not in sys.modules


def test_zero_trade_without_preset_has_inferred_scenario_and_relative_report_path_metadata(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    backtest = _import_backtest_without_exchange()
    monkeypatch.delenv("BOT_PRESET", raising=False)
    monkeypatch.chdir(tmp_path)
    export_dir = tmp_path / "exports"
    last_run: dict[str, Any] = {}
    run_id = "ethsens_comboH_optionB_20260529_081114"

    def fake_run_backtest(**kwargs: Any) -> dict[str, Any]:
        backtest._CURRENT_EXPORT_DIR = str(export_dir)
        backtest._CURRENT_RUN_ID = run_id
        backtest._CURRENT_EXPORT_SYMBOL = "ETH/USDT"
        return _minimal_result(kwargs, export_dir, trades=0, run_id=run_id)

    monkeypatch.setattr(backtest, "run_backtest", fake_run_backtest)
    monkeypatch.setattr(backtest, "_write_last_run_reference", lambda **kwargs: last_run.update(kwargs) or "")
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
            "--report",
            "--report-out",
            "relative_report.json",
            "--run-id",
            run_id,
        ],
    )

    assert backtest.main() == 0

    status_json = export_dir / "scenario_status.json"
    assert status_json.is_file()
    assert not (tmp_path / "relative_report.json").exists()

    payload = json.loads(status_json.read_text(encoding="utf-8"))
    assert "scenario" in payload
    assert payload["scenario"] is None
    assert payload["scenario_label"] == "combined_heavy"
    assert payload["scenario_source"] == "run_id_inferred"
    assert payload["run_id"] == run_id
    assert payload["classification"] == "kill_gate_zero_trade_non_comparable"
    assert payload["comparable_metrics_available"] is False
    assert payload["report_generated"] is False
    assert payload["research_only"] is True
    assert payload["final_validation_approved"] is False
    assert payload["public_performance_claim_approved"] is False
    assert payload["normal_report_path"] == "relative_report.json"
    assert payload["normal_report_path_is_relative"] is True
    assert payload["normal_report_path_base"] == str(tmp_path.resolve())
    assert payload["normal_report_path_resolved"] == str((tmp_path / "relative_report.json").resolve())
    assert last_run["extra"]["report_written"] is False
    assert "exchange" not in sys.modules
    assert "ccxt" not in sys.modules


def test_trade_report_path_still_writes_normal_report_without_status(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    backtest = _import_backtest_without_exchange()
    export_dir = tmp_path / "exports"
    last_run: dict[str, Any] = {}

    def fake_run_backtest(**kwargs: Any) -> dict[str, Any]:
        export_dir.mkdir(parents=True, exist_ok=True)
        backtest._CURRENT_EXPORT_DIR = str(export_dir)
        backtest._CURRENT_RUN_ID = "unit_trade_report"
        backtest._CURRENT_EXPORT_SYMBOL = "ETH/USDT"
        (export_dir / "equity_curve.csv").write_text(
            "ts,ts_iso,equity,mtm_equity\n1000,,1000,1000\n2000,,1010,1010\n",
            encoding="utf-8",
        )
        (export_dir / "trades.csv").write_text(
            "ts,net\n2000,10\n",
            encoding="utf-8",
        )
        return _minimal_result(kwargs, export_dir, trades=1)

    monkeypatch.setattr(backtest, "run_backtest", fake_run_backtest)
    monkeypatch.setattr(backtest, "_write_last_run_reference", lambda **kwargs: last_run.update(kwargs) or "")
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
            "--report",
            "--report-out",
            str(export_dir / "report.json"),
        ],
    )

    assert backtest.main() == 0

    report_json = export_dir / "report.json"
    assert report_json.is_file()
    assert not (export_dir / "scenario_status.json").exists()
    assert not (export_dir / "scenario_status.md").exists()

    report = json.loads(report_json.read_text(encoding="utf-8"))
    assert report["overall"]["trades"] == 1
    assert report["overall"]["net_total"] == pytest.approx(10.0)
    assert last_run["extra"]["report_written"] is True
    assert "exchange" not in sys.modules
    assert "ccxt" not in sys.modules
