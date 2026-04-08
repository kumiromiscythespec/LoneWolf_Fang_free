# BUILD_ID: 2026-04-08_free_update_dialog_tr_texts_v1
# BUILD_ID: 2026-04-08_free_update_check_signal_dispatch_v1
# BUILD_ID: 2026-04-08_free_gui_title_version_1_1_1_v1
# BUILD_ID: 2026-04-08_free_update_check_manual_retry_v1
# BUILD_ID: 2026-04-08_free_update_check_notify_latch_v1
# BUILD_ID: 2026-04-08_free_manual_update_dialog_masked_keys_v1
# BUILD_ID: 2026-04-08_free_okx_passphrase_coincheck_default_v1
# BUILD_ID: 2026-04-08_free_bitbank_okx_spot_only_v1
# BUILD_ID: 2026-04-08_free_gui_update_check_top_button_v1
# BUILD_ID: 2026-04-08_free_gui_update_check_v1
# BUILD_ID: 2026-04-03_free_gui_report_path_readability_v1
# BUILD_ID: 2026-04-03_free_gui_settings_two_column_v1
# BUILD_ID: 2026-04-03_free_gui_version_title_release_prep_v1
# BUILD_ID: 2026-04-03_free_gui_release_prep_v1
# BUILD_ID: 2026-03-29_free_gui_responsiveness_fix_v1
# BUILD_ID: 2026-03-29_free_gui_multiyear_backtest_fix_v1
# BUILD_ID: 2026-03-29_free_ui_text_cleanup_v1
# BUILD_ID: 2026-03-29_free_final_polish_v1
# BUILD_ID: 2026-03-29_free_port_standard_gui_nonlive_improvements_v1
# BUILD_ID: 2026-03-29_free_from_standard_nonlive_build_v1
# BUILD_ID: 2026-03-27_main_window_activation_local_reset_v1
# BUILD_ID: 2026-03-27_main_window_collapsible_sections_v1
# BUILD_ID: 2026-03-27_main_window_snapshot_folder_align_v1
# BUILD_ID: 2026-03-27_main_window_chart_state_path_fix_v2_0_2
# BUILD_ID: 2026-03-27_main_window_chart_axis_layout_v1_2
# BUILD_ID: 2026-03-27_main_window_result_chart_panel_v1_1
# BUILD_ID: 2026-03-27_main_window_result_chart_panel_v1
# BUILD_ID: 2026-03-26_main_window_compact_controls_single_row_v1
# BUILD_ID: 2026-03-26_main_window_compact_activation_log_spacing_tune_v1
# BUILD_ID: 2026-03-26_main_window_compact_report_activation_semihorizontal_v1
# BUILD_ID: 2026-03-26_main_window_compact_responsive_layout_v1
# BUILD_ID: 2026-03-26_main_window_smaller_min_size_log_v1
# BUILD_ID: 2026-03-26_main_window_report_tools_default_open_v1
# BUILD_ID: 2026-03-26_main_window_report_tools_collapsible_v1
# BUILD_ID: 2026-03-26_main_window_runtime_section_autocollapse_v1
# BUILD_ID: 2026-03-26_main_window_initial_collapsible_sections_v1
# BUILD_ID: 2026-03-26_standard_gui_clear_requires_typed_seat_v4
# BUILD_ID: 2026-03-20_gui_runtime_exports_last_run_v1
# BUILD_ID: 2026-03-19_market_data_root_stage1_patchfix_v2
# BUILD_ID: 2026-03-12_gui_log_level_dropdown_v1
from __future__ import annotations

import csv
import glob
import io
import json
import logging
import os
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import zipfile
from collections import deque
from datetime import datetime, timezone
from typing import Optional

import config as C
from PySide6.QtCore import QSize, Qt, QTimer, QUrl, Signal
from PySide6.QtGui import QDesktopServices, QIcon, QTextCursor
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QGridLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QTextEdit,
    QComboBox,
    QCheckBox,
    QMessageBox,
    QFileDialog,
    QGroupBox,
    QSplitter,
    QStackedWidget,
)

from app.core.settings_store import AppSettings, get_gui_chart_state, load_settings, save_settings, set_gui_chart_state
from app.core.valuation_preview import (
    PREVIEW_ONLY_NOTE,
    SUPPORTED_ACCOUNT_CCYS,
    SUPPORTED_PREVIEW_MODES,
    ValuationPreviewRequest,
    calculate_valuation_preview,
    format_jpy_value,
)
from app.core.chart_state_path import build_chart_state_path
from app.security.keyring_store import (
    clear_creds,
    clear_license_local_test_state,
    load_creds,
    load_license_device_id,
    load_license_seat_key,
    load_license_state,
    save_creds,
)
from app.core.launch import (
    BacktestSpec,
    LaunchSpec,
    LicenseOperationError,
    ReplaySpec,
    activate_and_store_license,
    ensure_live_license_or_raise,
    launch_backtest,
    launch_runner,
    launch_replay,
    refresh_and_store_license,
    terminate_process,
)
from app.core.log_stream import start_log_threads
from app.core.paths import ensure_runtime_dirs, get_paths
from app.core.dataset import (
    DatasetResolutionError,
    build_missing_dataset_message,
    infer_prefix_year_from_source,
    resolve_dataset,
    resolve_dataset_layout,
    symbol_to_prefix,
)
from app.gui.exchange_registry import (
    EXCHANGES,
    ExchangeOption,
    get_exchange_option,
    normalize_exchange_id,
    normalize_symbol_for_exchange,
    symbols_for_exchange,
)
from app.gui.chart_dialog import ChartDialog
from app.gui.logo_loader import LogoAsset, load_logo_asset, render_logo_pixmap
from app.gui.result_chart import (
    CHART_MODE_CANDLE,
    LiveChartState,
    ResultChartData,
    ResultPanel,
    ResultChartWidget,
    infer_result_chart_snapshot_symbol,
    load_latest_result_chart_data,
    load_live_chart_state_for_runtime,
    with_live_chart_state,
)
from app.gui.win_titlebar import apply_dark_titlebar
from app.security.license_client import deactivate_license, default_license_base_url


BUILD_ID = "2026-04-08_free_update_dialog_tr_texts_v1"
logger = logging.getLogger(__name__)
APP_DISPLAY_NAME = str(getattr(C, "APP_DISPLAY_NAME", "") or "LoneWolf Fang Free").strip() or "LoneWolf Fang Free"
APP_VERSION = str(getattr(C, "APP_VERSION", "") or getattr(C, "VERSION", "") or "").strip()


def _make_app_window_title(display_name: str, version: str) -> str:
    name = str(display_name or "").strip()
    raw_version = str(version or "").strip()
    if not raw_version:
        return name
    if name and raw_version.casefold().startswith(name.casefold()):
        return raw_version
    title_version = raw_version if raw_version.lower().startswith("v") else f"v{raw_version}"
    return f"{name} {title_version}".strip()


APP_WINDOW_TITLE = _make_app_window_title(APP_DISPLAY_NAME, APP_VERSION)

_FREE_RUN_MODES = ("PAPER", "REPLAY", "BACKTEST")
_RUNTIME_LOG_LEVEL_VALUES = ("MINIMAL",)
FREE_BUILD_NOTE = "FREE build: PAPER / REPLAY / BACKTEST only"
COMPACT_WIDTH_THRESHOLD = 980
SETTINGS_TWO_COLUMN_WIDTH_THRESHOLD = 1180
DEFAULT_LOG_MIN_HEIGHT = 180
COMPACT_LOG_MIN_HEIGHT = 120
DEFAULT_SECTION_VERTICAL_SPACING = 8
COMPACT_SECTION_VERTICAL_SPACING = 6
DEFAULT_MAIN_WINDOW_WIDTH = 840
DEFAULT_MAIN_WINDOW_HEIGHT = 620
PROC_POLL_ACTIVE_INTERVAL_MS = 500
PROC_POLL_IDLE_INTERVAL_MS = 1500
LIVE_CHART_POLL_INTERVAL_MS = 1500
LIVE_CHART_STALE_SEC = 120.0
BRAND_LOGO_REFRESH_DEBOUNCE_MS = 90
BRAND_LOGO_SIZE_STEP_PX = 8
_UI_LANGUAGE_OPTIONS = (("ja", "日本語"), ("en", "English"))
_UI_TEXTS = {
    "ja": {
        "window.title": "LoneWolf Fang Free",
        "label.preset": "プリセット",
        "label.exchange": "取引所",
        "label.symbol": "通貨ペア",
        "label.run_mode": "実行モード",
        "label.log_level": "ログレベル",
        "label.language": "表示言語",
        "label.dataset_root": "データセットルート",
        "label.tf": "時間足",
        "label.since": "開始",
        "label.until": "終了",
        "label.enable_report": "レポートを有効化",
        "label.report_out": "出力先",
        "label.resolved": "解決後パス",
        "label.key": "キー",
        "label.secret": "シークレット",
        "label.passphrase": "Passphrase",
        "label.from": "開始",
        "label.to": "終了",
        "label.force_redownload": "再ダウンロードを強制",
        "label.preview_mode": "プレビュー方式",
        "label.account_currency": "口座通貨",
        "label.native_balance": "残高",
        "label.manual_jpy_balance": "手動 JPY 残高",
        "label.preview_jpy_valuation": "JPY 評価額プレビュー",
        "label.fx_source": "FX ソース",
        "label.estimated_band": "推定バンド",
        "label.preview_error": "プレビューエラー: {detail}",
        "group.replay": "Replay / Backtest / データセット",
        "group.report": "レポート",
        "group.valuation": "評価設定",
        "group.api_credentials": "API認証情報",
        "group.diagnostics_pipeline": "診断 / パイプライン",
        "note.free_build": "FREE build: PAPER / REPLAY / BACKTEST のみ",
        "note.preview_only": "プレビューのみです。課金には反映されません。",
        "placeholder.dataset_root": "<PREFIX>_5m と <PREFIX>_1h を含むデータセットルートを選択",
        "placeholder.api_key": "{exchange} キー",
        "placeholder.api_secret": "{exchange} シークレット",
        "placeholder.api_passphrase": "{exchange} passphrase",
        "placeholder.yyyy_mm": "YYYY-MM",
        "action.select_replay_data": "Replay Data を選択...",
        "action.select_dataset_root": "Dataset Root を選択...",
        "action.start_replay": "Replay 実行",
        "action.start_backtest": "Backtest 実行",
        "action.start_paper": "Paper 開始",
        "action.run_diagnostics": "診断実行",
        "action.create_support_bundle": "サポートバンドル作成",
        "action.download_precompute": "ダウンロード + 事前計算",
        "action.check_updates": "更新確認",
        "action.save_settings": "設定を保存",
        "action.clear_api_keys": "APIキーをクリア",
        "action.stop": "停止",
        "action.browse": "参照...",
        "action.open_folder": "フォルダを開く",
        "action.save_png": "PNG保存",
        "action.expand": "拡大",
        "action.fit_all": "全体表示",
        "action.reset_view": "表示をリセット",
        "action.close": "閉じる",
        "action.refresh": "更新",
        "status.preview_error": "プレビューエラー",
        "chart.mode.equity": "エクイティ",
        "chart.mode.net": "純損益",
        "chart.mode.max_dd": "最大DD",
        "chart.mode.trades": "取引数",
        "chart.mode.combined": "複合",
        "chart.mode.candle": "ローソク足",
        "chart.window_title": "結果チャート - {mode}",
        "result.kpi.net": "純損益",
        "result.kpi.max_dd": "最大DD",
        "result.kpi.trades": "取引数",
        "result.empty.no_result_data": "結果データがありません",
        "result.empty.no_result_data_yet": "結果データはまだありません",
        "result.empty.no_chart_ready_data": "チャート表示可能なデータがありません",
        "result.empty.live_parse_failed": "paper チャート状態の解析に失敗しました",
        "result.empty.live_waiting_ready": "paper チャート状態はありますが、ローソク足がまだ準備できていません",
        "result.empty.live_waiting": "paper のローソク足を待機中...",
        "dialog.missing_api_keys.title": "APIキー不足",
        "dialog.missing_api_keys.message": "{exchange} の API Key/Secret を入力して Save を押してください。",
        "dialog.missing_api_credentials.message": "Please enter {exchange} API credentials and click Save.",
        "dialog.replay_start_failed.title": "Replay起動失敗",
        "dialog.backtest_start_failed.title": "Backtest起動失敗",
        "dialog.start_failed.title": "起動失敗",
        "dialog.save_png.title": "PNG保存",
        "dialog.select_report_output.title": "レポート出力先を選択",
        "dialog.already_running.title": "実行中",
        "dialog.already_running.message": "Bot はすでに実行中です。",
        "dialog.invalid_period.title": "期間エラー",
        "dialog.invalid_period.message": "From/To は YYYY-MM で指定してください。",
        "dialog.period_yyyy_mm.message": "Since/Until は YYYY-MM で指定してください。\n{detail}",
        "dialog.data_empty.message": "解決後の {tag} データセットフォルダ配下に CSV ファイルが見つかりませんでした。",
        "dialog.until_month_invalid.message": "Until month は Since month 以上である必要があります。",
        "dialog.update_check.title": "更新確認",
        "dialog.update_check.up_to_date": "お使いのクライアントは最新バージョンです。",
        "dialog.update_check.failed": "更新確認に失敗しました。",
        "dialog.update_available.title": "更新があります",
        "dialog.update_available.message": "新しい LoneWolf Fang Free のリリースがあります。\n\n現在: {current}\n最新版: {latest}",
        "dialog.update_available.open_question": "GitHub Releases ページを開きますか?",
        "dialog.backtest_guidance.dataset_missing.title": "Backtest データ不足",
        "dialog.backtest_guidance.dataset_missing.message": "この Backtest に必要な 5m / 1h データセットフォルダが見つかりません。\n\nsymbol={symbol}\ndataset_root={dataset_root}\n\nDiagnostics / Pipeline を開いて Download + Precompute を実行してください。",
        "dialog.backtest_guidance.dataset_missing.action": "Diagnostics / Pipeline を開く",
        "dialog.backtest_guidance.symbol_missing.title": "Symbol 不足または未対応",
        "dialog.backtest_guidance.symbol_missing.message": "選択中の Symbol が現在のデータセットに見つかりません。\n\nsymbol={symbol}\ndataset_root={dataset_root}\n\nSymbol コンボから別の銘柄を選択してください。",
        "dialog.backtest_guidance.symbol_missing.action": "Symbol へ移動",
        "dialog.backtest_guidance.range_out_of_data.title": "期間がデータ範囲外",
        "dialog.backtest_guidance.range_out_of_data.message": "選択した Since / Until が利用可能なデータ範囲外です。\n\nsince={since}\nuntil={until}\ndataset_root={dataset_root}\n\n利用可能な月に合わせて Since / Until を調整してください。",
        "dialog.backtest_guidance.range_out_of_data.action": "Since / Until へ移動",
    },
    "en": {
        "window.title": "LoneWolf Fang Free",
        "label.preset": "Preset",
        "label.exchange": "Exchange",
        "label.symbol": "Symbol",
        "label.run_mode": "Run Mode",
        "label.log_level": "Log Level",
        "label.language": "Language",
        "label.dataset_root": "Dataset Root",
        "label.tf": "TF",
        "label.since": "Since",
        "label.until": "Until",
        "label.enable_report": "Enable report",
        "label.report_out": "Report Out",
        "label.resolved": "Resolved",
        "label.key": "Key",
        "label.secret": "Secret",
        "label.passphrase": "Passphrase",
        "label.from": "From",
        "label.to": "To",
        "label.force_redownload": "Force re-download",
        "label.preview_mode": "Preview Mode",
        "label.account_currency": "Account Currency",
        "label.native_balance": "Native Balance",
        "label.manual_jpy_balance": "Manual JPY Balance",
        "label.preview_jpy_valuation": "Preview JPY Valuation",
        "label.fx_source": "FX Source",
        "label.estimated_band": "Estimated Band",
        "label.preview_error": "Preview error: {detail}",
        "group.replay": "Replay / Backtest / Dataset",
        "group.report": "Report",
        "group.valuation": "Valuation Settings",
        "group.api_credentials": "API Credentials",
        "group.diagnostics_pipeline": "Diagnostics / Pipeline",
        "note.free_build": "FREE build: PAPER / REPLAY / BACKTEST only",
        "note.preview_only": "Preview only. Billing is not affected.",
        "placeholder.dataset_root": "Select dataset root containing <PREFIX>_5m and <PREFIX>_1h",
        "placeholder.api_key": "{exchange} key",
        "placeholder.api_secret": "{exchange} secret",
        "placeholder.api_passphrase": "{exchange} passphrase",
        "placeholder.yyyy_mm": "YYYY-MM",
        "action.select_replay_data": "Select Replay Data...",
        "action.select_dataset_root": "Select Dataset Root...",
        "action.start_replay": "Run Replay",
        "action.start_backtest": "Run Backtest",
        "action.start_paper": "Start Paper",
        "action.run_diagnostics": "Run Diagnostics",
        "action.create_support_bundle": "Create Support Bundle",
        "action.download_precompute": "Download + Precompute",
        "action.check_updates": "Check for Updates",
        "action.save_settings": "Save Settings",
        "action.clear_api_keys": "Clear API Keys",
        "action.stop": "Stop",
        "action.browse": "Browse...",
        "action.open_folder": "Open Folder",
        "action.save_png": "Save PNG",
        "action.expand": "Expand",
        "action.fit_all": "Fit All",
        "action.reset_view": "Reset View",
        "action.close": "Close",
        "action.refresh": "Refresh",
        "status.preview_error": "preview_error",
        "chart.mode.equity": "Equity",
        "chart.mode.net": "Net",
        "chart.mode.max_dd": "Max DD",
        "chart.mode.trades": "Trades",
        "chart.mode.combined": "Combined",
        "chart.mode.candle": "Candle",
        "chart.window_title": "Result Chart - {mode}",
        "result.kpi.net": "net",
        "result.kpi.max_dd": "max_dd",
        "result.kpi.trades": "trades",
        "result.empty.no_result_data": "No result data",
        "result.empty.no_result_data_yet": "No result data yet",
        "result.empty.no_chart_ready_data": "No chart-ready data",
        "result.empty.live_parse_failed": "paper chart state parse failed",
        "result.empty.live_waiting_ready": "paper chart state present but candles are not ready",
        "result.empty.live_waiting": "Waiting for paper candles...",
        "dialog.missing_api_keys.title": "Missing API Keys",
        "dialog.missing_api_keys.message": "Please enter {exchange} API Key/Secret and click Save.",
        "dialog.missing_api_credentials.message": "Please enter {exchange} API credentials and click Save.",
        "dialog.replay_start_failed.title": "Replay start failed",
        "dialog.backtest_start_failed.title": "Backtest start failed",
        "dialog.start_failed.title": "Start failed",
        "dialog.save_png.title": "Save PNG",
        "dialog.select_report_output.title": "Select Report Output",
        "dialog.already_running.title": "Already running",
        "dialog.already_running.message": "Bot is already running.",
        "dialog.invalid_period.title": "Invalid period",
        "dialog.invalid_period.message": "From/To must be YYYY-MM.",
        "dialog.period_yyyy_mm.message": "Since/Until must be YYYY-MM.\n{detail}",
        "dialog.data_empty.message": "No CSV files were found under the resolved {tag} dataset folders.",
        "dialog.until_month_invalid.message": "Until month must be greater than or equal to since month.",
        "dialog.update_check.title": "Update check",
        "dialog.update_check.up_to_date": "Your client is already up to date.",
        "dialog.update_check.failed": "Failed to check for updates.",
        "dialog.update_available.title": "Update available",
        "dialog.update_available.message": "A newer LoneWolf Fang Free release is available.\n\nCurrent: {current}\nLatest: {latest}",
        "dialog.update_available.open_question": "Open the GitHub Releases page?",
        "dialog.backtest_guidance.dataset_missing.title": "Backtest dataset missing",
        "dialog.backtest_guidance.dataset_missing.message": "5m / 1h dataset folders for this backtest were not found.\n\nsymbol={symbol}\ndataset_root={dataset_root}\n\nOpen Diagnostics / Pipeline and run Download + Precompute.",
        "dialog.backtest_guidance.dataset_missing.action": "Open Diagnostics / Pipeline",
        "dialog.backtest_guidance.symbol_missing.title": "Symbol missing or unavailable",
        "dialog.backtest_guidance.symbol_missing.message": "The selected symbol was not found in the current dataset.\n\nsymbol={symbol}\ndataset_root={dataset_root}\n\nChoose another symbol from the Symbol combo.",
        "dialog.backtest_guidance.symbol_missing.action": "Go to Symbol",
        "dialog.backtest_guidance.range_out_of_data.title": "Range out of data",
        "dialog.backtest_guidance.range_out_of_data.message": "The selected Since / Until range is outside the available dataset months.\n\nsince={since}\nuntil={until}\ndataset_root={dataset_root}\n\nAdjust Since / Until to match available months.",
        "dialog.backtest_guidance.range_out_of_data.action": "Go to Since / Until",
    },
}


def _mask_secret(s: str) -> str:
    text = str(s or "")
    if not text:
        return ""
    if len(text) <= 2:
        return "*" * len(text)
    if len(text) <= 6:
        return text[:1] + ("*" * (len(text) - 2)) + text[-1:]
    return text[:2] + ("*" * (len(text) - 4)) + text[-2:]


def _is_masked_secret_text(s: str) -> bool:
    return "*" in str(s or "")


def _is_plain_secret_text(s: str) -> bool:
    text = str(s or "").strip()
    return bool(text) and not _is_masked_secret_text(text)


def _normalize_exchange_id(raw: str) -> str:
    return normalize_exchange_id(str(raw or "coincheck"), default="coincheck")


def _normalize_run_mode(raw: str) -> str:
    x = str(raw or "PAPER").strip().upper()
    if x in _FREE_RUN_MODES:
        return x
    return "PAPER"


def _normalize_runtime_log_level(raw: str) -> str:
    _ = raw
    return "MINIMAL"


def _normalize_ui_language(raw: str) -> str:
    value = str(raw or "en").strip().lower()
    if value in {"ja", "en"}:
        return value
    return "en"


def _default_runtime_log_level() -> str:
    return "MINIMAL"


def _config_symbols() -> list[str]:
    try:
        import config as C  # local import to keep GUI module lightweight
        syms = getattr(C, "SYMBOLS", None) or ["BTC/JPY"]
        out = [str(x).strip() for x in syms if str(x).strip()]
        return out or ["BTC/JPY"]
    except Exception:
        return ["BTC/JPY"]


def _default_preset() -> str:
    try:
        import config as C  # local import to keep GUI module lightweight
        value = str(getattr(C, "DEFAULT_PRESET", "SELL_SAFE") or "SELL_SAFE").strip().upper()
        if value in {"OFF", "SELL_SAFE"}:
            return value
    except Exception:
        pass
    return "SELL_SAFE"


class MainWindow(QWidget):
    sig_log = Signal(str)
    sig_update_check_finished = Signal()
    sig_update_available = Signal(str, str, str)
    sig_update_up_to_date = Signal(str, str)
    sig_update_failed = Signal(str)

    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle(APP_WINDOW_TITLE)

        self._paths = ensure_runtime_dirs()

        self._proc: Optional[subprocess.Popen] = None
        self._settings: AppSettings = load_settings()
        self._ui_language: str = _normalize_ui_language(getattr(self._settings, "ui_language", "en"))
        self._chart_ui_state = get_gui_chart_state(self._settings)
        self._exchange_id: str = _normalize_exchange_id(getattr(self._settings, "exchange_id", "coincheck"))
        config_symbols = _config_symbols()
        saved_symbol = str(getattr(self._settings, "symbol", "") or "").strip()
        self._symbols: list[str] = symbols_for_exchange(self._exchange_id) or config_symbols
        self._default_symbol: str = normalize_symbol_for_exchange(
            self._exchange_id,
            saved_symbol or (self._symbols[0] if self._symbols else "BTC/JPY"),
        )
        self._run_mode: str = self._load_run_mode_from_settings()
        self._log_level: str = _normalize_runtime_log_level(getattr(self._settings, "log_level", "") or _default_runtime_log_level())
        self._ip_whitelist_alerted: bool = False
        self._default_dataset_root: str = os.path.abspath(str(getattr(self._paths, "market_data_dir", "") or self._paths.repo_root or os.getcwd()))
        self._replay_data_path: str = str(self._settings.dataset_root or self._default_dataset_root)
        self._replay_dataset_prefix: str = symbol_to_prefix(str(self._settings.dataset_prefix or self._default_symbol))
        self._replay_dataset_year: int = int(self._settings.dataset_year or 0)
        self._selected_replay_csv_source: str = ""
        self._last_auto_range_source: str = ""
        self._auto_range_enabled: bool = True
        self._proc_role: str = "runner"
        self._stderr_ring = deque(maxlen=2000)
        self._replay_log_fp = None
        self._replay_log_path: str = ""
        self._live_log_fp = None
        self._live_log_path: str = ""
        self._last_replay_report_path: str = ""
        self._last_replay_trade_log_path: str = ""
        self._manual_stop_requested: bool = False
        self._manual_stop_role: str = ""
        self._manual_stop_requested_at: float = 0.0
        self._stop_request_in_flight: bool = False
        self._credential_inputs: dict[str, dict[str, QLineEdit]] = {}
        self._credential_ui_refs: dict[str, dict[str, object]] = {}
        self._credential_page_index: dict[str, int] = {}
        self._titlebar_dark_applied = False
        self._chart_dialog: ChartDialog | None = None
        self._base_result_data = ResultChartData()
        self._live_chart_state: LiveChartState | None = None
        self._live_chart_available: bool = False
        self._chart_mode_before_live_candle: str = ""
        self._chart_mode_user_locked: bool = False
        self._chart_mode_auto_live_candle_applied: bool = False
        self._suppress_chart_mode_user_lock: bool = False
        self._update_check_started: bool = False
        self._update_check_running: bool = False
        self._update_check_notify: bool = False
        self._update_check_manual_retry: bool = False
        self._last_chart_state_diag_key: tuple[str, int, int, str] | None = None
        self._last_chart_state_reader_path: str = ""
        self._last_live_chart_poll_signature: tuple[str, str, str, bool, bool, int, int, bool] | None = None
        self._logo_asset: LogoAsset | None = load_logo_asset(self._paths.repo_root)
        if self._logo_asset is not None:
            self.setWindowIcon(QIcon(str(self._logo_asset.source_path)))
        self._last_logo_box: tuple[int, int] = (0, 0)
        self._last_logo_device_ratio_key: int = 0
        self._logo_label = QLabel()
        self._compact_mode: bool | None = None
        self._settings_two_column_mode: bool | None = None
        self._pending_brand_logo_force: bool = False
        self._gui_perf_counters: dict[str, int] = {
            "proc_poll_calls": 0,
            "proc_poll_idle_skips": 0,
            "live_chart_poll_calls": 0,
            "live_chart_poll_skips": 0,
            "result_refresh_calls": 0,
            "result_refresh_skips": 0,
            "brand_logo_refresh_calls": 0,
            "brand_logo_refresh_skips": 0,
        }
        self._logo_refresh_timer = QTimer(self)
        self._logo_refresh_timer.setSingleShot(True)
        self._logo_refresh_timer.setInterval(BRAND_LOGO_REFRESH_DEBOUNCE_MS)
        self._logo_refresh_timer.timeout.connect(self._flush_brand_logo_refresh)
        self.setMinimumSize(DEFAULT_MAIN_WINDOW_WIDTH, DEFAULT_MAIN_WINDOW_HEIGHT)
        initial_width = int(getattr(self._settings, "main_window_width", DEFAULT_MAIN_WINDOW_WIDTH) or DEFAULT_MAIN_WINDOW_WIDTH)
        initial_height = int(getattr(self._settings, "main_window_height", DEFAULT_MAIN_WINDOW_HEIGHT) or DEFAULT_MAIN_WINDOW_HEIGHT)
        self.resize(initial_width, initial_height)

        # --- UI ---
        root = QVBoxLayout()
        root.setContentsMargins(16, 16, 16, 16)
        root.setSpacing(12)
        self.setLayout(root)

        top_area = QHBoxLayout()
        top_area.setSpacing(16)
        top_form = QVBoxLayout()
        top_form.setSpacing(8)
        top_area.addLayout(top_form, stretch=1)
        self._logo_label.setAlignment(Qt.AlignRight | Qt.AlignTop)
        self._logo_label.setStyleSheet("background: transparent; border: none;")
        self._logo_label.setVisible(False)
        top_area.addWidget(self._logo_label, 0, Qt.AlignRight | Qt.AlignTop)
        root.addLayout(top_area)

        self.preset_label = QLabel("Preset")
        self.preset = QComboBox()
        self.preset.addItems(["OFF", "SELL_SAFE"])
        self.preset.setCurrentText(
            self._settings.preset if self._settings.preset in ("OFF", "SELL_SAFE") else _default_preset()
        )

        # Exchange / Symbol / Mode row
        self.controls_layout = QGridLayout()
        self.controls_layout.setContentsMargins(0, 0, 0, 0)
        self.controls_layout.setHorizontalSpacing(8)
        self.controls_layout.setVerticalSpacing(8)
        self.exchange_label = QLabel("Exchange")
        self.exchange = QComboBox()
        for item in EXCHANGES:
            self.exchange.addItem(str(item.label), str(item.id))
        idx_exchange = max(0, self.exchange.findData(self._exchange_id))
        self.exchange.setCurrentIndex(idx_exchange)
        self._exchange_id = _normalize_exchange_id(str(self.exchange.currentData() or self._exchange_id))
        self._symbols = symbols_for_exchange(self._exchange_id) or self._symbols
        self._default_symbol = normalize_symbol_for_exchange(self._exchange_id, self._default_symbol)

        self.symbol_label = QLabel("Symbol")
        self.symbol = QComboBox()

        self.run_mode_label = QLabel("Run Mode")
        self.run_mode = QComboBox()
        self.run_mode.addItems(list(_FREE_RUN_MODES))
        self.run_mode.setCurrentText(self._run_mode)
        self.log_level_label = QLabel("Log Level")
        self.log_level = QComboBox()
        self.log_level.addItems(list(_RUNTIME_LOG_LEVEL_VALUES))
        self.log_level.setCurrentText(self._log_level)
        self.language_label = QLabel("Language")
        self.language = QComboBox()
        for code, label in _UI_LANGUAGE_OPTIONS:
            self.language.addItem(label, code)
        idx_language = max(0, self.language.findData(self._ui_language))
        self.language.setCurrentIndex(idx_language)
        self.btn_check_updates = QPushButton("Check for Updates")
        top_form.addLayout(self.controls_layout)

        self.free_build_note = QLabel(FREE_BUILD_NOTE)
        self.free_build_note.setStyleSheet("color: #9aa4af;")
        top_form.addWidget(self.free_build_note)

        self.replay_group = QGroupBox("Replay / Backtest / Dataset")
        replay_layout = QVBoxLayout()
        replay_layout.setContentsMargins(12, 10, 12, 12)
        replay_layout.setSpacing(8)
        self.replay_group.setLayout(replay_layout)

        row_replay_root = QHBoxLayout()
        row_replay_root.setSpacing(8)
        self.replay_data_root_label = QLabel("Dataset Root")
        row_replay_root.addWidget(self.replay_data_root_label)
        self.replay_data = QLineEdit(self._replay_data_path)
        self.replay_data.setPlaceholderText("Select dataset root containing <PREFIX>_5m and <PREFIX>_1h")
        self.replay_data.setReadOnly(True)
        self.btn_select_replay = QPushButton("Select Replay Data...")
        self.btn_select_replay_dir = QPushButton("Select Dataset Root...")
        row_replay_root.addWidget(self.replay_data, stretch=1)
        row_replay_root.addWidget(self.btn_select_replay)
        row_replay_root.addWidget(self.btn_select_replay_dir)
        replay_layout.addLayout(row_replay_root)

        row_replay_controls = QHBoxLayout()
        row_replay_controls.setSpacing(8)
        self.replay_symbol = QComboBox()
        self.replay_tf = QComboBox()
        self.replay_tf.addItems(["5m", "1m", "15m", "1h"])
        self.replay_tf.setCurrentText("5m")
        self.replay_since_ym = QLineEdit("2025-01")
        self.replay_since_ym.setPlaceholderText("YYYY-MM")
        self.replay_until_ym = QLineEdit("2025-12")
        self.replay_until_ym.setPlaceholderText("YYYY-MM")
        self.btn_run_replay = QPushButton("Run Replay")
        self.replay_symbol_field_label = QLabel("Symbol")
        row_replay_controls.addWidget(self.replay_symbol_field_label)
        row_replay_controls.addWidget(self.replay_symbol)
        self.replay_tf_label = QLabel("TF")
        row_replay_controls.addWidget(self.replay_tf_label)
        row_replay_controls.addWidget(self.replay_tf)
        self.replay_since_label = QLabel("Since")
        row_replay_controls.addWidget(self.replay_since_label)
        row_replay_controls.addWidget(self.replay_since_ym)
        self.replay_until_label = QLabel("Until")
        row_replay_controls.addWidget(self.replay_until_label)
        row_replay_controls.addWidget(self.replay_until_ym)
        row_replay_controls.addStretch(1)
        row_replay_controls.addWidget(self.btn_run_replay)
        replay_layout.addLayout(row_replay_controls)
        self.btn_run_replay.setVisible(False)
        root.addWidget(self.replay_group)

        self.settings_sections = QWidget()
        self.settings_sections_layout = QHBoxLayout()
        self.settings_sections_layout.setContentsMargins(0, 0, 0, 0)
        self.settings_sections_layout.setSpacing(12)
        self.settings_sections.setLayout(self.settings_sections_layout)
        self.settings_left_column = QWidget()
        self.settings_left_column_layout = QVBoxLayout()
        self.settings_left_column_layout.setContentsMargins(0, 0, 0, 0)
        self.settings_left_column_layout.setSpacing(12)
        self.settings_left_column.setLayout(self.settings_left_column_layout)
        self.settings_right_column = QWidget()
        self.settings_right_column_layout = QVBoxLayout()
        self.settings_right_column_layout.setContentsMargins(0, 0, 0, 0)
        self.settings_right_column_layout.setSpacing(12)
        self.settings_right_column.setLayout(self.settings_right_column_layout)
        self.settings_sections_layout.addWidget(
            self.settings_left_column,
            stretch=1,
            alignment=Qt.AlignmentFlag.AlignTop,
        )
        self.settings_sections_layout.addWidget(
            self.settings_right_column,
            stretch=1,
            alignment=Qt.AlignmentFlag.AlignTop,
        )
        root.addWidget(self.settings_sections)

        self.report_group = QGroupBox("Report")
        self.report_group.setCheckable(True)
        report_layout = QVBoxLayout()
        report_layout.setContentsMargins(12, 10, 12, 12)
        report_layout.setSpacing(8)
        self.report_group.setLayout(report_layout)
        self.report_container = QWidget()
        self.report_container_layout = QGridLayout()
        self.report_container_layout.setContentsMargins(0, 0, 0, 0)
        self.report_container_layout.setHorizontalSpacing(8)
        self.report_container_layout.setVerticalSpacing(8)
        self.report_container.setLayout(self.report_container_layout)

        self.report_enabled = QCheckBox("Enable report")
        self.report_enabled.setChecked(bool(self._settings.report_enabled))
        self.report_out_label = QLabel("Report Out")
        self.report_out = QLineEdit(str(self._settings.report_out or self._default_report_out()))
        self.btn_report_out = QPushButton("Browse...")
        self.report_resolved_label = QLabel("Resolved")
        report_expanded = True
        self.report_group.setChecked(report_expanded)
        self.report_container.setVisible(report_expanded)
        self.report_preview = QLineEdit()
        self.report_preview.setReadOnly(True)
        report_layout.addWidget(self.report_container)

        self.valuation_group = QGroupBox("Valuation Settings")
        self.valuation_group.setCheckable(True)
        valuation_expanded = bool(getattr(self._settings, "gui_section_valuation_expanded", True))
        self.valuation_group.setChecked(valuation_expanded)
        valuation_layout = QVBoxLayout()
        valuation_layout.setContentsMargins(12, 10, 12, 12)
        valuation_layout.setSpacing(8)
        self.valuation_group.setLayout(valuation_layout)
        self.valuation_container = QWidget()
        self.valuation_container.setVisible(valuation_expanded)
        self.valuation_container_layout = QGridLayout()
        self.valuation_container_layout.setContentsMargins(0, 0, 0, 0)
        self.valuation_container_layout.setHorizontalSpacing(8)
        self.valuation_container_layout.setVerticalSpacing(8)
        self.valuation_container.setLayout(self.valuation_container_layout)

        self.valuation_mode_label = QLabel("Preview Mode")
        self.valuation_mode = QComboBox()
        self.valuation_mode.addItems(list(SUPPORTED_PREVIEW_MODES))
        self.valuation_mode.setCurrentText(str(getattr(self._settings, "preview_mode", "Manual JPY") or "Manual JPY"))
        self.valuation_account_ccy_label = QLabel("Account Currency")
        self.valuation_account_ccy = QComboBox()
        self.valuation_account_ccy.addItems(list(SUPPORTED_ACCOUNT_CCYS))
        valuation_account_ccy = str(getattr(self._settings, "account_ccy", "JPY") or "JPY")
        idx_valuation_ccy = max(0, self.valuation_account_ccy.findText(valuation_account_ccy))
        self.valuation_account_ccy.setCurrentIndex(idx_valuation_ccy)

        self.valuation_native_balance_label = QLabel("Native Balance")
        self.valuation_native_balance = QLineEdit(str(getattr(self._settings, "native_balance", "300000") or "300000"))
        self.valuation_native_balance.setClearButtonEnabled(True)
        self.valuation_manual_jpy_balance_label = QLabel("Manual JPY Balance")
        self.valuation_manual_jpy_balance = QLineEdit(
            str(getattr(self._settings, "manual_jpy_balance", "300000") or "300000")
        )
        self.valuation_manual_jpy_balance.setClearButtonEnabled(True)

        self.valuation_usdjpy_label = QLabel("USDJPY")
        self.valuation_usdjpy = QLineEdit(str(getattr(self._settings, "usdjpy", "") or ""))
        self.valuation_usdjpy.setClearButtonEnabled(True)
        self.valuation_usdtjpy_label = QLabel("USDTJPY")
        self.valuation_usdtjpy = QLineEdit(str(getattr(self._settings, "usdtjpy", "") or ""))
        self.valuation_usdtjpy.setClearButtonEnabled(True)
        self.valuation_usdcjpy_label = QLabel("USDCJPY")
        self.valuation_usdcjpy = QLineEdit(str(getattr(self._settings, "usdcjpy", "") or ""))
        self.valuation_usdcjpy.setClearButtonEnabled(True)

        self.valuation_preview_jpy_label = QLabel("Preview JPY Valuation")
        self.valuation_preview_jpy = QLineEdit()
        self.valuation_preview_jpy.setReadOnly(True)
        self.valuation_fx_source_label = QLabel("FX Source")
        self.valuation_fx_source = QLineEdit()
        self.valuation_fx_source.setReadOnly(True)
        self.valuation_estimated_band_label = QLabel("Estimated Band")
        self.valuation_estimated_band = QLineEdit()
        self.valuation_estimated_band.setReadOnly(True)
        self.valuation_note = QLabel(PREVIEW_ONLY_NOTE)
        self.valuation_note.setWordWrap(True)
        self.valuation_note.setStyleSheet("color: #9aa4af;")
        self.valuation_error = QLabel("")
        self.valuation_error.setWordWrap(True)
        self.valuation_error.setStyleSheet("color: #c0392b;")
        valuation_layout.addWidget(self.valuation_container)

        self.creds_group = QGroupBox("API Credentials")
        self.creds_group.setCheckable(True)
        creds_expanded = bool(getattr(self._settings, "gui_section_api_expanded", False))
        self.creds_group.setChecked(creds_expanded)
        creds_layout = QVBoxLayout()
        creds_layout.setContentsMargins(12, 10, 12, 12)
        creds_layout.setSpacing(8)
        self.creds_group.setLayout(creds_layout)
        self.creds_stack = QStackedWidget()
        self.creds_stack.setVisible(creds_expanded)
        creds_layout.addWidget(self.creds_stack)
        for item in EXCHANGES:
            page = QWidget()
            page_layout = QVBoxLayout()
            page_layout.setContentsMargins(0, 0, 0, 0)
            page_layout.setSpacing(8)
            page.setLayout(page_layout)

            key_row = QHBoxLayout()
            key_row.setSpacing(8)
            key_label = QLabel("Key")
            key_row.addWidget(key_label)
            key_edit = QLineEdit()
            key_edit.setEchoMode(QLineEdit.EchoMode.Normal)
            key_edit.setClearButtonEnabled(True)
            key_edit.setPlaceholderText(f"{item.label} key")
            key_row.addWidget(key_edit)
            page_layout.addLayout(key_row)

            secret_row = QHBoxLayout()
            secret_row.setSpacing(8)
            secret_label = QLabel("Secret")
            secret_row.addWidget(secret_label)
            secret_edit = QLineEdit()
            secret_edit.setEchoMode(QLineEdit.EchoMode.Normal)
            secret_edit.setClearButtonEnabled(True)
            secret_edit.setPlaceholderText(f"{item.label} secret")
            secret_row.addWidget(secret_edit)
            page_layout.addLayout(secret_row)

            passphrase_row_widget = QWidget()
            passphrase_row = QHBoxLayout()
            passphrase_row.setContentsMargins(0, 0, 0, 0)
            passphrase_row.setSpacing(8)
            passphrase_row_widget.setLayout(passphrase_row)
            passphrase_label = QLabel("Passphrase")
            passphrase_row.addWidget(passphrase_label)
            passphrase_edit = QLineEdit()
            passphrase_edit.setEchoMode(QLineEdit.EchoMode.Normal)
            passphrase_edit.setClearButtonEnabled(True)
            passphrase_edit.setPlaceholderText(f"{item.label} passphrase")
            passphrase_row.addWidget(passphrase_edit)
            passphrase_row_widget.setVisible(bool(item.passphrase_env))
            page_layout.addWidget(passphrase_row_widget)

            index = self.creds_stack.addWidget(page)
            self._credential_page_index[str(item.id)] = int(index)
            self._credential_inputs[str(item.id)] = {
                "key": key_edit,
                "secret": secret_edit,
                "passphrase": passphrase_edit,
            }
            self._credential_ui_refs[str(item.id)] = {
                "exchange_label": str(item.label),
                "key_label": key_label,
                "secret_label": secret_label,
                "passphrase_label": passphrase_label,
                "key_edit": key_edit,
                "secret_edit": secret_edit,
                "passphrase_edit": passphrase_edit,
                "passphrase_row": passphrase_row_widget,
            }

            creds = load_creds(str(item.id))
            if creds is not None:
                key_edit.setText(_mask_secret(creds.api_key))
                secret_edit.setText(_mask_secret(creds.api_secret))
                if str(getattr(creds, "api_passphrase", "") or ""):
                    passphrase_edit.setText(_mask_secret(str(creds.api_passphrase or "")))

        self.activation_group = QGroupBox("Desktop Activation")
        self.activation_group.setCheckable(True)
        activation_expanded = bool(getattr(self._settings, "gui_section_activation_expanded", False))
        self.activation_group.setChecked(activation_expanded)
        activation_layout = QVBoxLayout()
        activation_layout.setContentsMargins(12, 10, 12, 12)
        activation_layout.setSpacing(8)
        self.activation_group.setLayout(activation_layout)
        self.activation_container = QWidget()
        self.activation_container.setVisible(activation_expanded)
        self.activation_container_layout = QGridLayout()
        self.activation_container_layout.setContentsMargins(0, 0, 0, 0)
        self.activation_container_layout.setHorizontalSpacing(8)
        self.activation_container_layout.setVerticalSpacing(8)
        self.activation_container.setLayout(self.activation_container_layout)

        self.activation_seat_key_label = QLabel("Seat Key")
        self.license_seat_key = QLineEdit(_mask_secret(str(load_license_seat_key() or "")))
        self.license_seat_key.setClearButtonEnabled(True)
        self.license_seat_key.setPlaceholderText("Paste your Desktop Activation seat key")
        self.btn_license_activate = QPushButton("Activate")
        self.btn_license_refresh = QPushButton("Refresh")
        self.btn_license_clear = QPushButton("Clear")
        self.btn_license_reset_local = QPushButton("Reset Test State")
        self.btn_license_reset_local.setToolTip(
            "Clear only the saved local Desktop Activation state, seat key, and device id. "
            "Remote device binding is unchanged."
        )
        self.activation_status_label = QLabel("Status")
        self.license_status = QLineEdit()
        self.license_status.setReadOnly(True)
        self.activation_details_label = QLabel("Details")
        self.license_meta = QLineEdit()
        self.license_meta.setReadOnly(True)
        activation_layout.addWidget(self.activation_container)
        self.activation_group.setVisible(False)
        self.activation_group.setEnabled(False)

        self.tools_group = QGroupBox("Diagnostics / Pipeline")
        self.tools_group.setCheckable(True)
        tools_expanded = bool(getattr(self._settings, "gui_section_diagnostics_expanded", False))
        self.tools_group.setChecked(tools_expanded)
        tools_layout = QVBoxLayout()
        tools_layout.setContentsMargins(12, 10, 12, 12)
        tools_layout.setSpacing(8)
        self.tools_group.setLayout(tools_layout)
        self.tools_container = QWidget()
        self.tools_container.setVisible(tools_expanded)
        self.tools_container_layout = QGridLayout()
        self.tools_container_layout.setContentsMargins(0, 0, 0, 0)
        self.tools_container_layout.setHorizontalSpacing(8)
        self.tools_container_layout.setVerticalSpacing(8)
        self.tools_container.setLayout(self.tools_container_layout)

        self.btn_diag = QPushButton("Run Diagnostics")
        self.btn_bundle = QPushButton("Create Support Bundle")
        self.pipeline_from_label = QLabel("From")
        self.pipeline_from_ym = QLineEdit(datetime.now().strftime("%Y-%m"))
        self.pipeline_from_ym.setPlaceholderText("YYYY-MM")
        self.pipeline_to_label = QLabel("To")
        self.pipeline_to_ym = QLineEdit(datetime.now().strftime("%Y-%m"))
        self.pipeline_to_ym.setPlaceholderText("YYYY-MM")
        self.pipeline_force = QCheckBox("Force re-download")
        self.btn_pipeline = QPushButton("Download + Precompute")
        tools_layout.addWidget(self.tools_container)

        row_buttons = QHBoxLayout()
        row_buttons.setSpacing(8)
        self.btn_save = QPushButton("Save Settings")
        self.btn_clear = QPushButton("Clear API Keys")
        self.btn_start = QPushButton("Start")
        self.btn_start.setCheckable(True)
        self.btn_start.setStyleSheet(
            "QPushButton:checked, QPushButton:checked:disabled "
            "{ background-color: #111; color: #fff; font-weight: 700; border: 1px solid #111; }"
        )
        self.btn_stop = QPushButton("Stop")
        self.btn_stop.setEnabled(False)
        row_buttons.addWidget(self.btn_save)
        row_buttons.addWidget(self.btn_clear)
        row_buttons.addStretch(1)
        row_buttons.addWidget(self.btn_start)
        row_buttons.addWidget(self.btn_stop)
        root.addLayout(row_buttons)

        self._refresh_replay_symbol_options(preferred=self._default_symbol)
        first_inputs = self._credential_inputs.get(self._exchange_id, {})
        self.api_key = first_inputs.get("key")
        self.api_secret = first_inputs.get("secret")
        self.api_passphrase = first_inputs.get("passphrase")
        self._sync_exchange_fields()
        self._refresh_report_preview()
        self._refresh_valuation_preview()
        self._sync_mode_ui()
        self._apply_responsive_layout(force=True)

        # Log + result view
        self.bottom_splitter = QSplitter(Qt.Orientation.Horizontal)
        self.bottom_splitter.setChildrenCollapsible(False)
        self.bottom_splitter.setHandleWidth(6)
        root.addWidget(self.bottom_splitter, stretch=1)

        self.log = QTextEdit()
        self.log.setReadOnly(True)
        self.log.setLineWrapMode(QTextEdit.LineWrapMode.NoWrap)
        self.log.document().setMaximumBlockCount(3000)
        self.log.setMinimumHeight(DEFAULT_LOG_MIN_HEIGHT)
        self.bottom_splitter.addWidget(self.log)
        self.result_panel = ResultPanel()
        self.bottom_splitter.addWidget(self.result_panel)
        self.bottom_splitter.setStretchFactor(0, 3)
        self.bottom_splitter.setStretchFactor(1, 2)
        self.bottom_splitter.setSizes([560, 320])
        self._restore_chart_panel_state()
        self._set_log_minimum_height(bool(self._compact_mode))
        self._apply_ui_language()

        # Signals
        self.btn_save.clicked.connect(self.on_save)
        self.btn_clear.clicked.connect(self.on_clear_keys)
        self.btn_license_activate.clicked.connect(self.on_activate_license)
        self.btn_license_refresh.clicked.connect(self.on_refresh_license)
        self.btn_license_clear.clicked.connect(self.on_clear_activation)
        self.btn_license_reset_local.clicked.connect(self.on_reset_activation_test_state)
        self.btn_start.clicked.connect(self.on_start)
        self.btn_stop.clicked.connect(self.on_stop)
        self.btn_report_out.clicked.connect(self.on_select_report_out)
        self.btn_diag.clicked.connect(self.on_run_diagnostics)
        self.btn_bundle.clicked.connect(self.on_create_support_bundle)
        self.btn_pipeline.clicked.connect(self.on_run_pipeline)
        self.btn_check_updates.clicked.connect(self.on_check_updates)
        self.btn_select_replay.clicked.connect(self.on_select_replay_data)
        self.btn_select_replay_dir.clicked.connect(self.on_select_replay_folder)
        self.btn_run_replay.clicked.connect(self.on_run_replay)
        self.result_panel.refreshRequested.connect(self.on_refresh_result_panel)
        self.result_panel.expandRequested.connect(self.on_expand_result_chart)
        self.result_panel.saveRequested.connect(self.on_save_result_chart)
        self.result_panel.openFolderRequested.connect(lambda: self._open_result_export_folder(self.result_panel.result_data()))
        self.result_panel.modeChanged.connect(self._on_result_panel_mode_changed)
        self.bottom_splitter.splitterMoved.connect(self._on_bottom_splitter_moved)
        self.exchange.currentTextChanged.connect(self._on_exchange_changed)
        self.symbol.currentTextChanged.connect(self._on_symbol_changed)
        self.run_mode.currentTextChanged.connect(self._on_run_mode_changed)
        self.log_level.currentTextChanged.connect(self._on_log_level_changed)
        self.language.currentIndexChanged.connect(self._on_ui_language_changed)
        self.report_group.toggled.connect(self.report_container.setVisible)
        self.valuation_group.toggled.connect(self.valuation_container.setVisible)
        self.creds_group.toggled.connect(self.creds_stack.setVisible)
        self.activation_group.toggled.connect(self.activation_container.setVisible)
        self.tools_group.toggled.connect(self.tools_container.setVisible)
        self.valuation_group.toggled.connect(self._on_collapsible_sections_changed)
        self.creds_group.toggled.connect(self._on_collapsible_sections_changed)
        self.activation_group.toggled.connect(self._on_collapsible_sections_changed)
        self.tools_group.toggled.connect(self._on_collapsible_sections_changed)
        self.report_out.textChanged.connect(self._refresh_report_preview)
        self.report_enabled.toggled.connect(self._refresh_report_preview)
        self.valuation_mode.currentTextChanged.connect(self._on_valuation_input_changed)
        self.valuation_account_ccy.currentTextChanged.connect(self._on_valuation_input_changed)
        self.valuation_native_balance.textChanged.connect(self._on_valuation_input_changed)
        self.valuation_manual_jpy_balance.textChanged.connect(self._on_valuation_input_changed)
        self.valuation_usdjpy.textChanged.connect(self._on_valuation_input_changed)
        self.valuation_usdtjpy.textChanged.connect(self._on_valuation_input_changed)
        self.valuation_usdcjpy.textChanged.connect(self._on_valuation_input_changed)
        self.replay_since_ym.textEdited.connect(self._on_replay_range_edited)
        self.replay_until_ym.textEdited.connect(self._on_replay_range_edited)
        self.sig_log.connect(self._append)
        self.sig_update_available.connect(self._show_update_available)
        self.sig_update_up_to_date.connect(self._show_update_not_available)
        self.sig_update_failed.connect(self._show_update_check_failed)
        self.sig_update_check_finished.connect(self._on_update_check_finished)

        # Timer to poll process exit
        self._timer = QTimer(self)
        self._timer.setInterval(PROC_POLL_IDLE_INTERVAL_MS)
        self._timer.timeout.connect(self._poll_proc)
        self._timer.start()
        self._live_chart_timer = QTimer(self)
        self._live_chart_timer.setInterval(LIVE_CHART_POLL_INTERVAL_MS)
        self._live_chart_timer.timeout.connect(self._poll_live_chart_state)
        self._live_chart_timer.start()

        self._refresh_license_widgets()
        self._append(f"[ui] app_title={APP_WINDOW_TITLE}\n")
        self._append("[ui] Ready.\n")
        QTimer.singleShot(0, self._run_initial_gui_refresh)

    def showEvent(self, event) -> None:
        super().showEvent(event)
        if self._titlebar_dark_applied:
            self._refresh_brand_logo(force=True)
            return
        self._titlebar_dark_applied = True
        QTimer.singleShot(0, lambda: apply_dark_titlebar(self))
        QTimer.singleShot(0, lambda: self._refresh_brand_logo(force=True))

    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        self._apply_responsive_layout()
        self._schedule_brand_logo_refresh()

    def closeEvent(self, event) -> None:  # type: ignore[override]
        try:
            self._persist_chart_ui_state()
        except Exception:
            pass
        super().closeEvent(event)

    def _is_compact_width(self) -> bool:
        return int(self.width()) < COMPACT_WIDTH_THRESHOLD

    def _is_settings_two_column_width(self) -> bool:
        return int(self.width()) >= SETTINGS_TWO_COLUMN_WIDTH_THRESHOLD

    def _reset_box_layout(self, layout) -> None:
        while layout.count():
            layout.takeAt(0)

    def _apply_settings_sections_layout(self, two_column: bool) -> None:
        self._reset_box_layout(self.settings_left_column_layout)
        self._reset_box_layout(self.settings_right_column_layout)
        if two_column:
            self.settings_right_column.setVisible(True)
            for widget in (
                self.valuation_group,
                self.tools_group,
            ):
                self.settings_left_column_layout.addWidget(widget)
            self.settings_left_column_layout.addStretch(1)
            for widget in (
                self.report_group,
                self.creds_group,
                self.activation_group,
            ):
                self.settings_right_column_layout.addWidget(widget)
            self.settings_right_column_layout.addStretch(1)
            return
        self.settings_right_column.setVisible(False)
        for widget in (
            self.report_group,
            self.valuation_group,
            self.creds_group,
            self.tools_group,
            self.activation_group,
        ):
            self.settings_left_column_layout.addWidget(widget)
        self.settings_left_column_layout.addStretch(1)

    def _reset_grid_layout(self, layout: QGridLayout) -> None:
        while layout.count():
            layout.takeAt(0)
        for index in range(13):
            layout.setColumnStretch(index, 0)
            layout.setRowStretch(index, 0)

    def _apply_controls_layout(self, compact: bool) -> None:
        self._reset_grid_layout(self.controls_layout)
        self.controls_layout.setHorizontalSpacing(6 if compact else 8)
        self.controls_layout.setVerticalSpacing(8)
        if compact:
            self.controls_layout.addWidget(self.preset_label, 0, 0)
            self.controls_layout.addWidget(self.preset, 0, 1)
            self.controls_layout.addWidget(self.exchange_label, 0, 2)
            self.controls_layout.addWidget(self.exchange, 0, 3)
            self.controls_layout.addWidget(self.symbol_label, 0, 4)
            self.controls_layout.addWidget(self.symbol, 0, 5)
            self.controls_layout.addWidget(self.run_mode_label, 1, 0)
            self.controls_layout.addWidget(self.run_mode, 1, 1)
            self.controls_layout.addWidget(self.log_level_label, 1, 2)
            self.controls_layout.addWidget(self.log_level, 1, 3)
            self.controls_layout.addWidget(self.language_label, 1, 4)
            self.controls_layout.addWidget(self.language, 1, 5)
            self.controls_layout.addWidget(self.btn_check_updates, 2, 0, 1, 6, Qt.AlignmentFlag.AlignRight)
            self.controls_layout.setColumnStretch(1, 1)
            self.controls_layout.setColumnStretch(3, 1)
            self.controls_layout.setColumnStretch(5, 1)
            return
        self.controls_layout.addWidget(self.preset_label, 0, 0)
        self.controls_layout.addWidget(self.preset, 0, 1)
        self.controls_layout.addWidget(self.exchange_label, 0, 2)
        self.controls_layout.addWidget(self.exchange, 0, 3)
        self.controls_layout.addWidget(self.symbol_label, 0, 4)
        self.controls_layout.addWidget(self.symbol, 0, 5)
        self.controls_layout.addWidget(self.run_mode_label, 0, 6)
        self.controls_layout.addWidget(self.run_mode, 0, 7)
        self.controls_layout.addWidget(self.log_level_label, 0, 8)
        self.controls_layout.addWidget(self.log_level, 0, 9)
        self.controls_layout.addWidget(self.language_label, 0, 10)
        self.controls_layout.addWidget(self.language, 0, 11)
        self.controls_layout.addWidget(self.btn_check_updates, 1, 0, 1, 13, Qt.AlignmentFlag.AlignRight)
        self.controls_layout.setColumnStretch(1, 1)
        self.controls_layout.setColumnStretch(3, 1)
        self.controls_layout.setColumnStretch(5, 1)
        self.controls_layout.setColumnStretch(7, 1)
        self.controls_layout.setColumnStretch(9, 1)
        self.controls_layout.setColumnStretch(11, 1)

    def _sync_window_size_settings(self) -> None:
        if not isinstance(self._settings, AppSettings):
            self._settings = AppSettings()
        self._settings.main_window_width = max(DEFAULT_MAIN_WINDOW_WIDTH, int(self.width()))
        self._settings.main_window_height = max(DEFAULT_MAIN_WINDOW_HEIGHT, int(self.height()))

    def _section_ui_state(self) -> dict[str, bool]:
        return {
            "gui_section_valuation_expanded": (
                bool(self.valuation_group.isChecked()) if hasattr(self, "valuation_group") else True
            ),
            "gui_section_api_expanded": bool(self.creds_group.isChecked()) if hasattr(self, "creds_group") else False,
            "gui_section_activation_expanded": (
                bool(self.activation_group.isChecked()) if hasattr(self, "activation_group") else False
            ),
            "gui_section_diagnostics_expanded": bool(self.tools_group.isChecked()) if hasattr(self, "tools_group") else False,
        }

    def _sync_collapsible_section_settings(self) -> None:
        if not isinstance(self._settings, AppSettings):
            self._settings = AppSettings()
        section_state = self._section_ui_state()
        self._settings.gui_section_valuation_expanded = bool(section_state["gui_section_valuation_expanded"])
        self._settings.gui_section_api_expanded = bool(section_state["gui_section_api_expanded"])
        self._settings.gui_section_activation_expanded = bool(section_state["gui_section_activation_expanded"])
        self._settings.gui_section_diagnostics_expanded = bool(section_state["gui_section_diagnostics_expanded"])

    def _on_collapsible_sections_changed(self, _checked: bool) -> None:
        self._sync_collapsible_section_settings()

    def _sync_valuation_settings_from_inputs(self) -> None:
        if not isinstance(self._settings, AppSettings):
            self._settings = AppSettings()
        self._settings.preview_mode = str(self.valuation_mode.currentText() or "Manual JPY")
        self._settings.account_ccy = str(self.valuation_account_ccy.currentText() or "JPY")
        self._settings.native_balance = str(self.valuation_native_balance.text() or "").strip() or "300000"
        self._settings.manual_jpy_balance = str(self.valuation_manual_jpy_balance.text() or "").strip() or "300000"
        self._settings.usdjpy = str(self.valuation_usdjpy.text() or "").strip()
        self._settings.usdtjpy = str(self.valuation_usdtjpy.text() or "").strip()
        self._settings.usdcjpy = str(self.valuation_usdcjpy.text() or "").strip()

    def _valuation_preview_request(self) -> ValuationPreviewRequest:
        return ValuationPreviewRequest(
            preview_mode=str(self.valuation_mode.currentText() or "Manual JPY"),
            account_ccy=str(self.valuation_account_ccy.currentText() or "JPY"),
            native_balance=str(self.valuation_native_balance.text() or "").strip(),
            manual_jpy_balance=str(self.valuation_manual_jpy_balance.text() or "").strip(),
            usdjpy=str(self.valuation_usdjpy.text() or "").strip(),
            usdtjpy=str(self.valuation_usdtjpy.text() or "").strip(),
            usdcjpy=str(self.valuation_usdcjpy.text() or "").strip(),
            exchange_id=self._selected_exchange_id(),
            symbol=self._selected_symbol() or self._default_symbol,
            run_mode=self._selected_run_mode(),
        )

    def _refresh_valuation_preview(self) -> None:
        if not hasattr(self, "valuation_mode"):
            return
        try:
            result = calculate_valuation_preview(self._valuation_preview_request())
        except Exception as exc:
            self.valuation_preview_jpy.setText("")
            self.valuation_fx_source.setText(self.tr("status.preview_error"))
            self.valuation_fx_source.setToolTip("")
            self.valuation_estimated_band.setText("")
            self.valuation_note.setText(self.tr("note.preview_only"))
            self.valuation_error.setText(self.tr("label.preview_error", detail=str(exc) or "unknown error"))
            return

        self.valuation_preview_jpy.setText(format_jpy_value(result.raw_balance_jpy))
        self.valuation_preview_jpy.setToolTip(
            f"native_balance={result.native_balance:g} {result.account_ccy} fx_rate_to_jpy={result.fx_rate_to_jpy:g}"
        )
        self.valuation_fx_source.setText(str(result.fx_source or ""))
        tooltip_parts: list[str] = []
        if result.native_balance_source:
            tooltip_parts.append(f"native_balance_source={result.native_balance_source}")
        if result.state_db_path:
            tooltip_parts.append(f"state_db={result.state_db_path}")
        self.valuation_fx_source.setToolTip("\n".join(tooltip_parts))
        self.valuation_estimated_band.setText(str(result.estimated_band or ""))
        self.valuation_note.setText(self.tr("note.preview_only"))
        self.valuation_error.setText("")

    def _on_valuation_input_changed(self, *_args: object) -> None:
        self._refresh_valuation_preview()

    def _apply_report_layout(self, compact: bool) -> None:
        self._reset_grid_layout(self.report_container_layout)
        self.report_container_layout.setHorizontalSpacing(6 if compact else 8)
        self.report_container_layout.setVerticalSpacing(
            COMPACT_SECTION_VERTICAL_SPACING if compact else DEFAULT_SECTION_VERTICAL_SPACING
        )
        if compact:
            self.report_container_layout.addWidget(self.report_enabled, 0, 0, 1, 5)
            self.report_container_layout.addWidget(self.report_out_label, 1, 0)
            self.report_container_layout.addWidget(self.report_out, 1, 1, 1, 3)
            self.report_container_layout.addWidget(self.btn_report_out, 1, 4)
            self.report_container_layout.addWidget(self.report_resolved_label, 2, 0)
            self.report_container_layout.addWidget(self.report_preview, 2, 1, 1, 4)
            self.report_container_layout.setColumnStretch(1, 1)
            self.report_container_layout.setColumnStretch(2, 1)
            self.report_container_layout.setColumnStretch(3, 1)
            return
        self.report_container_layout.addWidget(self.report_enabled, 0, 0, 1, 5)
        self.report_container_layout.addWidget(self.report_out_label, 1, 0)
        self.report_container_layout.addWidget(self.report_out, 1, 1, 1, 3)
        self.report_container_layout.addWidget(self.btn_report_out, 1, 4)
        self.report_container_layout.addWidget(self.report_resolved_label, 2, 0)
        self.report_container_layout.addWidget(self.report_preview, 2, 1, 1, 4)
        self.report_container_layout.setColumnStretch(1, 1)
        self.report_container_layout.setColumnStretch(2, 1)
        self.report_container_layout.setColumnStretch(3, 1)

    def _apply_valuation_layout(self, compact: bool) -> None:
        self._reset_grid_layout(self.valuation_container_layout)
        self.valuation_container_layout.setVerticalSpacing(
            COMPACT_SECTION_VERTICAL_SPACING if compact else DEFAULT_SECTION_VERTICAL_SPACING
        )
        if compact:
            self.valuation_container_layout.addWidget(self.valuation_mode_label, 0, 0)
            self.valuation_container_layout.addWidget(self.valuation_mode, 0, 1)
            self.valuation_container_layout.addWidget(self.valuation_account_ccy_label, 0, 2)
            self.valuation_container_layout.addWidget(self.valuation_account_ccy, 0, 3)
            self.valuation_container_layout.addWidget(self.valuation_native_balance_label, 1, 0)
            self.valuation_container_layout.addWidget(self.valuation_native_balance, 1, 1)
            self.valuation_container_layout.addWidget(self.valuation_manual_jpy_balance_label, 1, 2)
            self.valuation_container_layout.addWidget(self.valuation_manual_jpy_balance, 1, 3)
            self.valuation_container_layout.addWidget(self.valuation_usdjpy_label, 2, 0)
            self.valuation_container_layout.addWidget(self.valuation_usdjpy, 2, 1)
            self.valuation_container_layout.addWidget(self.valuation_usdtjpy_label, 2, 2)
            self.valuation_container_layout.addWidget(self.valuation_usdtjpy, 2, 3)
            self.valuation_container_layout.addWidget(self.valuation_usdcjpy_label, 3, 0)
            self.valuation_container_layout.addWidget(self.valuation_usdcjpy, 3, 1, 1, 3)
            self.valuation_container_layout.addWidget(self.valuation_preview_jpy_label, 4, 0)
            self.valuation_container_layout.addWidget(self.valuation_preview_jpy, 4, 1, 1, 3)
            self.valuation_container_layout.addWidget(self.valuation_fx_source_label, 5, 0)
            self.valuation_container_layout.addWidget(self.valuation_fx_source, 5, 1, 1, 3)
            self.valuation_container_layout.addWidget(self.valuation_estimated_band_label, 6, 0)
            self.valuation_container_layout.addWidget(self.valuation_estimated_band, 6, 1, 1, 3)
            self.valuation_container_layout.addWidget(self.valuation_note, 7, 0, 1, 4)
            self.valuation_container_layout.addWidget(self.valuation_error, 8, 0, 1, 4)
            self.valuation_container_layout.setColumnStretch(1, 1)
            self.valuation_container_layout.setColumnStretch(3, 1)
            return
        self.valuation_container_layout.addWidget(self.valuation_mode_label, 0, 0)
        self.valuation_container_layout.addWidget(self.valuation_mode, 0, 1)
        self.valuation_container_layout.addWidget(self.valuation_account_ccy_label, 0, 2)
        self.valuation_container_layout.addWidget(self.valuation_account_ccy, 0, 3)
        self.valuation_container_layout.addWidget(self.valuation_native_balance_label, 1, 0)
        self.valuation_container_layout.addWidget(self.valuation_native_balance, 1, 1)
        self.valuation_container_layout.addWidget(self.valuation_manual_jpy_balance_label, 1, 2)
        self.valuation_container_layout.addWidget(self.valuation_manual_jpy_balance, 1, 3)
        self.valuation_container_layout.addWidget(self.valuation_usdjpy_label, 2, 0)
        self.valuation_container_layout.addWidget(self.valuation_usdjpy, 2, 1)
        self.valuation_container_layout.addWidget(self.valuation_usdtjpy_label, 2, 2)
        self.valuation_container_layout.addWidget(self.valuation_usdtjpy, 2, 3)
        self.valuation_container_layout.addWidget(self.valuation_usdcjpy_label, 3, 0)
        self.valuation_container_layout.addWidget(self.valuation_usdcjpy, 3, 1)
        self.valuation_container_layout.addWidget(self.valuation_preview_jpy_label, 3, 2)
        self.valuation_container_layout.addWidget(self.valuation_preview_jpy, 3, 3)
        self.valuation_container_layout.addWidget(self.valuation_fx_source_label, 4, 0)
        self.valuation_container_layout.addWidget(self.valuation_fx_source, 4, 1)
        self.valuation_container_layout.addWidget(self.valuation_estimated_band_label, 4, 2)
        self.valuation_container_layout.addWidget(self.valuation_estimated_band, 4, 3)
        self.valuation_container_layout.addWidget(self.valuation_note, 5, 0, 1, 4)
        self.valuation_container_layout.addWidget(self.valuation_error, 6, 0, 1, 4)
        self.valuation_container_layout.setColumnStretch(1, 1)
        self.valuation_container_layout.setColumnStretch(3, 1)

    def _apply_activation_layout(self, compact: bool) -> None:
        self._reset_grid_layout(self.activation_container_layout)
        self.activation_container_layout.setVerticalSpacing(
            COMPACT_SECTION_VERTICAL_SPACING if compact else DEFAULT_SECTION_VERTICAL_SPACING
        )
        if compact:
            self.activation_container_layout.addWidget(self.activation_seat_key_label, 0, 0)
            self.activation_container_layout.addWidget(self.license_seat_key, 0, 1, 1, 3)
            self.activation_container_layout.addWidget(self.btn_license_activate, 1, 0)
            self.activation_container_layout.addWidget(self.btn_license_refresh, 1, 1)
            self.activation_container_layout.addWidget(self.btn_license_clear, 1, 2)
            self.activation_container_layout.addWidget(self.btn_license_reset_local, 2, 0, 1, 4)
            self.activation_container_layout.addWidget(self.activation_status_label, 3, 0)
            self.activation_container_layout.addWidget(self.license_status, 3, 1, 1, 3)
            self.activation_container_layout.addWidget(self.activation_details_label, 4, 0)
            self.activation_container_layout.addWidget(self.license_meta, 4, 1, 1, 3)
            self.activation_container_layout.setColumnStretch(1, 1)
            self.activation_container_layout.setColumnStretch(2, 1)
            self.activation_container_layout.setColumnStretch(3, 1)
            return
        self.activation_container_layout.addWidget(self.activation_seat_key_label, 0, 0)
        self.activation_container_layout.addWidget(self.license_seat_key, 0, 1, 1, 3)
        self.activation_container_layout.addWidget(self.btn_license_activate, 1, 0)
        self.activation_container_layout.addWidget(self.btn_license_refresh, 1, 1)
        self.activation_container_layout.addWidget(self.btn_license_clear, 1, 2)
        self.activation_container_layout.addWidget(self.btn_license_reset_local, 2, 0, 1, 4)
        self.activation_container_layout.addWidget(self.activation_status_label, 3, 0)
        self.activation_container_layout.addWidget(self.license_status, 3, 1, 1, 3)
        self.activation_container_layout.addWidget(self.activation_details_label, 4, 0)
        self.activation_container_layout.addWidget(self.license_meta, 4, 1, 1, 3)
        self.activation_container_layout.setColumnStretch(1, 1)
        self.activation_container_layout.setColumnStretch(2, 1)
        self.activation_container_layout.setColumnStretch(3, 1)

    def _apply_tools_layout(self, compact: bool) -> None:
        self._reset_grid_layout(self.tools_container_layout)
        self.tools_container_layout.addWidget(self.btn_diag, 0, 0)
        self.tools_container_layout.addWidget(self.btn_bundle, 0, 1)
        if compact:
            self.tools_container_layout.addWidget(self.pipeline_from_label, 1, 0)
            self.tools_container_layout.addWidget(self.pipeline_from_ym, 1, 1)
            self.tools_container_layout.addWidget(self.pipeline_to_label, 1, 2)
            self.tools_container_layout.addWidget(self.pipeline_to_ym, 1, 3)
            self.tools_container_layout.addWidget(self.pipeline_force, 2, 0, 1, 2)
            self.tools_container_layout.addWidget(self.btn_pipeline, 2, 2, 1, 2)
            self.tools_container_layout.setColumnStretch(1, 1)
            self.tools_container_layout.setColumnStretch(3, 1)
            return
        self.tools_container_layout.addWidget(self.pipeline_from_label, 1, 0)
        self.tools_container_layout.addWidget(self.pipeline_from_ym, 1, 1)
        self.tools_container_layout.addWidget(self.pipeline_to_label, 1, 2)
        self.tools_container_layout.addWidget(self.pipeline_to_ym, 1, 3)
        self.tools_container_layout.addWidget(self.pipeline_force, 1, 4)
        self.tools_container_layout.addWidget(self.btn_pipeline, 1, 5)
        self.tools_container_layout.setColumnStretch(1, 1)
        self.tools_container_layout.setColumnStretch(3, 1)
        self.tools_container_layout.setColumnStretch(6, 1)

    def _set_log_minimum_height(self, compact: bool) -> None:
        if not hasattr(self, "log"):
            return
        self.log.setMinimumHeight(COMPACT_LOG_MIN_HEIGHT if compact else DEFAULT_LOG_MIN_HEIGHT)

    def _apply_responsive_layout(self, compact: bool | None = None, *, force: bool = False) -> None:
        mode = self._is_compact_width() if compact is None else bool(compact)
        settings_two_column = (not mode) and self._is_settings_two_column_width()
        section_compact = mode or settings_two_column
        if (not force) and (self._compact_mode == mode) and (self._settings_two_column_mode == settings_two_column):
            return
        self._compact_mode = mode
        self._settings_two_column_mode = settings_two_column
        self._apply_controls_layout(mode)
        self._apply_settings_sections_layout(settings_two_column)
        self._apply_report_layout(section_compact)
        self._apply_valuation_layout(section_compact)
        self._apply_activation_layout(section_compact)
        self._apply_tools_layout(section_compact)
        self._set_log_minimum_height(mode)
        if mode:
            self._clear_brand_logo()
            return
        self._refresh_brand_logo(force=True)

    def _ui_text_map(self) -> dict[str, str]:
        base = dict(_UI_TEXTS.get("en", {}))
        base.update(_UI_TEXTS.get(self._ui_language, {}))
        return base

    def tr(self, key: str, **kwargs) -> str:
        text = str(self._ui_text_map().get(key, key))
        if kwargs:
            try:
                return text.format(**kwargs)
            except Exception:
                return text
        return text

    def _selected_ui_language(self) -> str:
        try:
            return _normalize_ui_language(self.language.currentData() or self._ui_language)
        except Exception:
            return _normalize_ui_language(self._ui_language)

    def _apply_ui_language(self) -> None:
        self.setWindowTitle(APP_WINDOW_TITLE)
        self.preset_label.setText(self.tr("label.preset"))
        self.exchange_label.setText(self.tr("label.exchange"))
        self.symbol_label.setText(self.tr("label.symbol"))
        self.run_mode_label.setText(self.tr("label.run_mode"))
        self.log_level_label.setText(self.tr("label.log_level"))
        self.language_label.setText(self.tr("label.language"))
        self.free_build_note.setText(self.tr("note.free_build"))
        self.replay_group.setTitle(self.tr("group.replay"))
        self.replay_data_root_label.setText(self.tr("label.dataset_root"))
        self.replay_data.setPlaceholderText(self.tr("placeholder.dataset_root"))
        self.btn_select_replay.setText(self.tr("action.select_replay_data"))
        self.btn_select_replay_dir.setText(self.tr("action.select_dataset_root"))
        self.replay_symbol_field_label.setText(self.tr("label.symbol"))
        self.replay_tf_label.setText(self.tr("label.tf"))
        self.replay_since_label.setText(self.tr("label.since"))
        self.replay_until_label.setText(self.tr("label.until"))
        self.replay_since_ym.setPlaceholderText(self.tr("placeholder.yyyy_mm"))
        self.replay_until_ym.setPlaceholderText(self.tr("placeholder.yyyy_mm"))
        self.btn_run_replay.setText(self.tr("action.start_replay"))
        self.report_group.setTitle(self.tr("group.report"))
        self.report_enabled.setText(self.tr("label.enable_report"))
        self.report_out_label.setText(self.tr("label.report_out"))
        self.btn_report_out.setText(self.tr("action.browse"))
        self.report_resolved_label.setText(self.tr("label.resolved"))
        self.valuation_group.setTitle(self.tr("group.valuation"))
        self.valuation_mode_label.setText(self.tr("label.preview_mode"))
        self.valuation_account_ccy_label.setText(self.tr("label.account_currency"))
        self.valuation_native_balance_label.setText(self.tr("label.native_balance"))
        self.valuation_manual_jpy_balance_label.setText(self.tr("label.manual_jpy_balance"))
        self.valuation_usdjpy_label.setText("USDJPY")
        self.valuation_usdtjpy_label.setText("USDTJPY")
        self.valuation_usdcjpy_label.setText("USDCJPY")
        self.valuation_preview_jpy_label.setText(self.tr("label.preview_jpy_valuation"))
        self.valuation_fx_source_label.setText(self.tr("label.fx_source"))
        self.valuation_estimated_band_label.setText(self.tr("label.estimated_band"))
        self.valuation_note.setText(self.tr("note.preview_only"))
        self.creds_group.setTitle(self.tr("group.api_credentials"))
        for exchange_id, refs in self._credential_ui_refs.items():
            exchange_label = str(refs.get("exchange_label", "") or exchange_id)
            key_label = refs.get("key_label")
            secret_label = refs.get("secret_label")
            passphrase_label = refs.get("passphrase_label")
            key_edit = refs.get("key_edit")
            secret_edit = refs.get("secret_edit")
            passphrase_edit = refs.get("passphrase_edit")
            passphrase_row = refs.get("passphrase_row")
            option = get_exchange_option(exchange_id)
            if isinstance(key_label, QLabel):
                key_label.setText(self.tr("label.key"))
            if isinstance(secret_label, QLabel):
                secret_label.setText(self.tr("label.secret"))
            if isinstance(passphrase_label, QLabel):
                passphrase_label.setText(self.tr("label.passphrase"))
            if isinstance(key_edit, QLineEdit):
                key_edit.setPlaceholderText(self.tr("placeholder.api_key", exchange=exchange_label))
            if isinstance(secret_edit, QLineEdit):
                secret_edit.setPlaceholderText(self.tr("placeholder.api_secret", exchange=exchange_label))
            if isinstance(passphrase_edit, QLineEdit):
                passphrase_edit.setPlaceholderText(self.tr("placeholder.api_passphrase", exchange=exchange_label))
            if isinstance(passphrase_row, QWidget):
                passphrase_row.setVisible(bool(option.passphrase_env))
        self.tools_group.setTitle(self.tr("group.diagnostics_pipeline"))
        self.btn_diag.setText(self.tr("action.run_diagnostics"))
        self.btn_bundle.setText(self.tr("action.create_support_bundle"))
        self.pipeline_from_label.setText(self.tr("label.from"))
        self.pipeline_from_ym.setPlaceholderText(self.tr("placeholder.yyyy_mm"))
        self.pipeline_to_label.setText(self.tr("label.to"))
        self.pipeline_to_ym.setPlaceholderText(self.tr("placeholder.yyyy_mm"))
        self.pipeline_force.setText(self.tr("label.force_redownload"))
        self.btn_pipeline.setText(self.tr("action.download_precompute"))
        self.btn_check_updates.setText(self.tr("action.check_updates"))
        self.btn_check_updates.setMinimumWidth(self.btn_check_updates.sizeHint().width())
        self.btn_save.setText(self.tr("action.save_settings"))
        self.btn_clear.setText(self.tr("action.clear_api_keys"))
        self.btn_stop.setText(self.tr("action.stop"))
        self._sync_mode_ui()
        self.result_panel.set_ui_texts(self._ui_text_map())
        if self._chart_dialog is not None:
            self._chart_dialog.set_ui_texts(self._ui_text_map())

    def _on_ui_language_changed(self, _index: int) -> None:
        selected = self._selected_ui_language()
        if selected == self._ui_language:
            return
        self._ui_language = selected
        if not isinstance(self._settings, AppSettings):
            self._settings = AppSettings()
        self._settings.ui_language = self._ui_language
        self._apply_ui_language()
        self._update_chart_ui_state()
        save_settings(self._settings)
        self._save_run_mode_to_settings()

    def _license_base_url(self) -> str:
        try:
            return str(default_license_base_url() or "").strip()
        except Exception:
            return str(os.getenv("LWF_LICENSE_BASE_URL") or os.getenv("LWF_SITE_BASE_URL") or "").strip()

    def _license_status_text(self) -> str:
        state = load_license_state()
        if state is None:
            return "Not activated"
        status = str(state.license_status or "").strip() or "unknown"
        parts = [f"status={status}"]
        try:
            seat_no = int(state.seat_no or 0)
        except Exception:
            seat_no = 0
        if seat_no > 0:
            parts.append(f"seat={seat_no}")
        parts.append(f"live={'yes' if bool(state.live_allowed) else 'no'}")
        grace = str(state.offline_grace_until or "").strip()
        if grace:
            parts.append(f"offline_grace_until={grace}")
        return " | ".join(parts)

    def _plain_license_seat_key_input(self) -> str:
        text = str(self.license_seat_key.text() or "").strip()
        return text if _is_plain_secret_text(text) else ""

    def _license_seat_key_raw_value(self, *, prefer_typed: bool = False) -> str:
        typed_seat_key = self._plain_license_seat_key_input()
        stored_seat_key = str(load_license_seat_key() or "").strip()
        if prefer_typed and typed_seat_key:
            return typed_seat_key
        return stored_seat_key or typed_seat_key

    def _refresh_license_widgets(self) -> None:
        stored_seat_key = str(load_license_seat_key() or "").strip()
        current_text = str(self.license_seat_key.text() or "").strip()
        if stored_seat_key and ((not current_text) or _is_masked_secret_text(current_text)):
            self.license_seat_key.setText(_mask_secret(stored_seat_key))
        self.license_status.setText(self._license_status_text())
        device_id = str(load_license_device_id() or "").strip()
        base_url = self._license_base_url()
        detail_parts = [f"base_url={base_url or '(unset)'}"]
        detail_parts.append(f"device_id={device_id or '(not created yet)'}")
        self.license_meta.setText(" | ".join(detail_parts))

    def _clear_local_activation_state(self) -> None:
        clear_license_local_test_state()
        self.license_seat_key.clear()
        self._refresh_license_widgets()

    def on_reset_activation_test_state(self) -> None:
        response = QMessageBox.question(
            self,
            "Desktop Activation",
            "This clears only the local Desktop Activation test state.\n"
            "Remote device binding is NOT changed.\n"
            "Continue?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if response != QMessageBox.StandardButton.Yes:
            return
        try:
            self._clear_local_activation_state()
            self.activation_group.setChecked(True)
            self._append("[ui] Desktop activation local test state reset.\n")
        except Exception as exc:
            self._refresh_license_widgets()
            self.activation_group.setChecked(True)
            msg = str(exc) or exc.__class__.__name__
            self._append(f"[ui] Desktop activation local reset failed error={msg}\n")
            QMessageBox.warning(self, "Desktop Activation", msg)

    def on_activate_license(self) -> None:
        seat_key = self._license_seat_key_raw_value(prefer_typed=True)
        if not seat_key:
            self.activation_group.setChecked(True)
            QMessageBox.warning(self, "Desktop Activation", "Seat Key is required.")
            return
        try:
            state = activate_and_store_license(seat_key, base_url=self._license_base_url())
            self.license_seat_key.setText(_mask_secret(str(state.seat_key or seat_key)))
            self._refresh_license_widgets()
            self.activation_group.setChecked(False)
            self._append(f"[ui] Desktop activation succeeded status={str(state.license_status or '')} live_allowed={bool(state.live_allowed)}\n")
            QMessageBox.information(self, "Desktop Activation", "Activation completed successfully.")
        except LicenseOperationError as exc:
            self._refresh_license_widgets()
            self.activation_group.setChecked(True)
            msg = str(exc) or "Desktop activation failed."
            self._append(f"[ui] Desktop activation failed error={msg}\n")
            QMessageBox.warning(self, "Desktop Activation", msg)
        except Exception as exc:
            self._refresh_license_widgets()
            self.activation_group.setChecked(True)
            msg = str(exc) or exc.__class__.__name__
            self._append(f"[ui] Desktop activation failed error={msg}\n")
            QMessageBox.critical(self, "Desktop Activation", msg)

    def on_refresh_license(self) -> None:
        typed_seat_key = self._plain_license_seat_key_input()
        stored_seat_key = str(load_license_seat_key() or "").strip()
        if (not stored_seat_key) and typed_seat_key:
            self.on_activate_license()
            return
        if (not stored_seat_key) and (not typed_seat_key):
            self.activation_group.setChecked(True)
            QMessageBox.warning(self, "Desktop Activation", "No saved Seat Key was found. Activate first.")
            return
        try:
            state = refresh_and_store_license(base_url=self._license_base_url())
            self.license_seat_key.setText(_mask_secret(str(load_license_seat_key() or typed_seat_key or "")))
            self._refresh_license_widgets()
            self.activation_group.setChecked(False)
            self._append(f"[ui] Desktop activation refresh succeeded status={str(state.license_status or '')} live_allowed={bool(state.live_allowed)}\n")
            QMessageBox.information(self, "Desktop Activation", "Refresh completed successfully.")
        except LicenseOperationError as exc:
            self._refresh_license_widgets()
            self.activation_group.setChecked(True)
            msg = str(exc) or "Desktop activation refresh failed."
            self._append(f"[ui] Desktop activation refresh failed error={msg}\n")
            QMessageBox.warning(self, "Desktop Activation", msg)
        except Exception as exc:
            self._refresh_license_widgets()
            self.activation_group.setChecked(True)
            msg = str(exc) or exc.__class__.__name__
            self._append(f"[ui] Desktop activation refresh failed error={msg}\n")
            QMessageBox.critical(self, "Desktop Activation", msg)

    def on_clear_activation(self) -> None:
        seat_key = self._license_seat_key_raw_value()
        device_id = str(load_license_device_id() or "").strip()
        if not seat_key:
            self._refresh_license_widgets()
            self.activation_group.setChecked(True)
            msg = (
                "Seat Key is required to clear the remote device binding. "
                "Paste the Seat Key and press Clear again. Local state was not changed."
            )
            self._append(f"[ui] Desktop activation clear blocked error={msg}\n")
            QMessageBox.warning(self, "Desktop Activation", msg)
            return

        remote_outcome = ""
        try:
            response = deactivate_license(
                seat_key,
                device_id,
                base_url=self._license_base_url(),
            )
            if not response.ok:
                self._refresh_license_widgets()
                self.activation_group.setChecked(True)
                detail = str(response.error_message or response.error_code or "desktop activation clear failed").strip()
                msg = f"Could not clear the remote device binding. {detail}"
                self._append(f"[ui] Desktop activation clear failed error={msg}\n")
                QMessageBox.warning(self, "Desktop Activation", msg)
                return
            remote_outcome = str(response.outcome or "deactivated").strip() or "deactivated"
        except Exception as exc:
            self._refresh_license_widgets()
            self.activation_group.setChecked(True)
            detail = str(exc) or exc.__class__.__name__
            msg = f"Could not clear the remote device binding. {detail}"
            self._append(f"[ui] Desktop activation clear failed error={msg}\n")
            QMessageBox.warning(self, "Desktop Activation", msg)
            return

        self._clear_local_activation_state()
        self.activation_group.setChecked(True)
        self._append(f"[ui] Desktop activation cleared remote_binding={remote_outcome} and local state.\n")
        QMessageBox.information(
            self,
            "Desktop Activation",
            "Remote device binding was cleared and the saved local activation state was removed from keyring.",
        )

    def _last_run_json_path(self) -> str:
        return os.path.abspath(os.path.join(self._paths.exports_dir, "last_run.json"))

    def _load_last_run_payload(self) -> dict[str, object] | None:
        path = self._last_run_json_path()
        if not os.path.isfile(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                raw = json.load(f)
        except Exception:
            return None
        return raw if isinstance(raw, dict) else None

    def _last_run_export_dir(self) -> str:
        payload = self._load_last_run_payload()
        if not payload:
            return ""
        raw = str(payload.get("export_dir", "") or "").strip().strip("\"' ")
        if not raw:
            return ""
        if os.path.isabs(raw):
            cand = os.path.abspath(raw)
        else:
            cand = os.path.abspath(os.path.join(self._paths.repo_root, raw))
        return cand if os.path.isdir(cand) else ""

    def _glob_existing_files(self, *patterns: str) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for pat in patterns:
            if not str(pat or "").strip():
                continue
            for fp in glob.glob(pat):
                ab = os.path.abspath(fp)
                if ab in seen or not os.path.isfile(ab):
                    continue
                seen.add(ab)
                out.append(ab)
        return out

    def _extend_targets_with_fallback(
        self,
        targets: list[str],
        primary_patterns: list[str],
        fallback_patterns: list[str],
    ) -> None:
        for pat in primary_patterns:
            primary = self._glob_existing_files(pat)
            if primary:
                targets.extend(primary)
                return
        targets.extend(self._glob_existing_files(*fallback_patterns))

    def _format_diag_artifact_path(self, path: str, *, runtime_exports: str, repo_root: str) -> str:
        ab = os.path.abspath(path)
        runtime_abs = os.path.abspath(runtime_exports)
        repo_abs = os.path.abspath(repo_root)
        try:
            if os.path.commonpath([ab, runtime_abs]) == runtime_abs:
                return str(os.path.relpath(ab, runtime_abs))
        except Exception:
            pass
        try:
            return str(os.path.relpath(ab, repo_abs))
        except Exception:
            return str(os.path.basename(ab))

    def _default_report_out(self) -> str:
        export_dir = self._last_run_export_dir()
        if export_dir:
            cand = os.path.abspath(os.path.join(export_dir, "report.json"))
            if os.path.isfile(cand):
                return cand
        return os.path.abspath(os.path.join(self._paths.exports_dir, "report.json"))

    def _report_out_value(self) -> str:
        return str(self.report_out.text() or "").strip() or self._default_report_out()

    def _report_out_abs(self) -> str:
        value = self._report_out_value()
        if os.path.isabs(value):
            return os.path.abspath(value)
        return os.path.abspath(os.path.join(self._paths.repo_root, value))

    def _refresh_report_preview(self) -> None:
        report_out_path = self._report_out_abs()
        suffix = "" if self.report_enabled.isChecked() else " (disabled)"
        preview_text = f"{report_out_path}{suffix}"
        self.report_out.setToolTip(report_out_path)
        self.report_preview.setText(preview_text)
        self.report_preview.setToolTip(preview_text)
        if not self.report_out.hasFocus():
            self.report_out.setCursorPosition(0)
        self.report_preview.setCursorPosition(0)

    def _ensure_report_parent(self) -> None:
        if not self.report_enabled.isChecked():
            return
        out_path = self._report_out_abs()
        parent = os.path.dirname(out_path)
        if parent:
            os.makedirs(parent, exist_ok=True)

    def _logo_box_size(self) -> tuple[int, int]:
        max_width = max(44, min(120, int(round(self.width() * 0.10))))
        max_height = max(44, min(96, int(round(self.height() * 0.10))))
        step = max(1, int(BRAND_LOGO_SIZE_STEP_PX))
        box_width = max(44, min(120, int(round(max_width / step)) * step))
        box_height = max(44, min(96, int(round(max_height / step)) * step))
        return (box_width, box_height)

    def _clear_brand_logo(self) -> None:
        self._last_logo_box = (0, 0)
        self._last_logo_device_ratio_key = 0
        self._logo_label.clear()
        self._logo_label.setToolTip("")
        self._logo_label.setVisible(False)

    def _schedule_brand_logo_refresh(self, *, force: bool = False) -> None:
        if bool(self._compact_mode):
            self._logo_refresh_timer.stop()
            self._pending_brand_logo_force = False
            return
        self._pending_brand_logo_force = bool(self._pending_brand_logo_force or force)
        self._logo_refresh_timer.start()

    def _flush_brand_logo_refresh(self) -> None:
        force = bool(self._pending_brand_logo_force)
        self._pending_brand_logo_force = False
        self._refresh_brand_logo(force=force)

    def _refresh_brand_logo(self, *, force: bool = False) -> None:
        self._gui_perf_counters["brand_logo_refresh_calls"] += 1
        if bool(self._compact_mode):
            self._gui_perf_counters["brand_logo_refresh_skips"] += 1
            self._clear_brand_logo()
            return
        if self._logo_asset is None:
            self._gui_perf_counters["brand_logo_refresh_skips"] += 1
            self._clear_brand_logo()
            return
        box_width, box_height = self._logo_box_size()
        device_ratio_key = max(100, int(round(self.devicePixelRatioF() * 100.0)))
        if (not force) and (self._last_logo_box == (box_width, box_height)) and (self._last_logo_device_ratio_key == device_ratio_key):
            self._gui_perf_counters["brand_logo_refresh_skips"] += 1
            return
        pixmap = render_logo_pixmap(
            self._logo_asset,
            box_width=box_width,
            box_height=box_height,
            device_pixel_ratio=self.devicePixelRatioF(),
        )
        if pixmap is None:
            self._gui_perf_counters["brand_logo_refresh_skips"] += 1
            self._clear_brand_logo()
            return
        self._last_logo_box = (box_width, box_height)
        self._last_logo_device_ratio_key = device_ratio_key
        device_ratio = max(1.0, float(pixmap.devicePixelRatio()))
        logical_size = QSize(
            max(1, int(round(pixmap.width() / device_ratio))),
            max(1, int(round(pixmap.height() / device_ratio))),
        )
        self._logo_label.setPixmap(pixmap)
        self._logo_label.setFixedSize(logical_size)
        self._logo_label.setToolTip(str(self._logo_asset.source_path))
        self._logo_label.setVisible(True)

    def _append(self, s: str) -> None:
        text = str(s or "")
        if self.thread() != self.log.thread():
            self.sig_log.emit(text)
            return

        self._maybe_show_ip_whitelist_hint(text)
        self._capture_replay_artifact_paths(text)
        for ln in text.splitlines():
            if ln.startswith("[stderr] "):
                self._stderr_ring.append(ln)
        try:
            if str(self._proc_role) in {"replay", "backtest"} and self._replay_log_fp is not None:
                self._replay_log_fp.write(text)
                self._replay_log_fp.flush()
            if str(self._proc_role) == "runner" and self._live_log_fp is not None:
                self._live_log_fp.write(text)
                self._live_log_fp.flush()
        except Exception:
            pass

        self.log.moveCursor(QTextCursor.End)
        self.log.insertPlainText(text)
        self.log.moveCursor(QTextCursor.End)

    def _selected_exchange_id(self) -> str:
        try:
            return _normalize_exchange_id(str(self.exchange.currentData() or self.exchange.currentText()))
        except Exception:
            return _normalize_exchange_id(self._exchange_id)

    def _selected_exchange_option(self) -> ExchangeOption:
        return get_exchange_option(self._selected_exchange_id())

    def _selected_symbol(self) -> str:
        try:
            value = str(self.symbol.currentData() or self.symbol.currentText() or "").strip()
        except Exception:
            value = ""
        return normalize_symbol_for_exchange(self._selected_exchange_id(), value or self._default_symbol)

    def _available_replay_symbols(self) -> list[str]:
        out: list[str] = []
        candidate_syms = (
            list(symbols_for_exchange(self._selected_exchange_id()))
            + list(self._symbols or [])
            + [self._default_symbol]
        )
        for sym in candidate_syms:
            raw = str(sym or "").strip()
            if (not raw) or raw in out:
                continue
            out.append(raw)
        fallback_symbol = normalize_symbol_for_exchange(self._selected_exchange_id(), self._default_symbol)
        return out or [fallback_symbol]

    def _selected_replay_symbol(self) -> str:
        try:
            value = str(self.replay_symbol.currentData() or self.replay_symbol.currentText() or "").strip()
        except Exception:
            value = ""
        return str(value or self._selected_symbol() or self._default_symbol)

    def _refresh_replay_symbol_options(self, preferred: str | None = None) -> None:
        if not hasattr(self, "replay_symbol"):
            return
        target = str(preferred or self._selected_replay_symbol() or self._selected_symbol() or self._default_symbol).strip()
        symbols = self._available_replay_symbols()
        self.replay_symbol.blockSignals(True)
        self.replay_symbol.clear()
        for sym in symbols:
            self.replay_symbol.addItem(str(sym), str(sym))
        idx = self.replay_symbol.findData(target)
        if idx < 0:
            idx = self.replay_symbol.findData(self._selected_symbol())
        if idx < 0:
            idx = 0
        self.replay_symbol.setCurrentIndex(int(idx))
        self.replay_symbol.blockSignals(False)

    def _parse_yyyy_mm(self, raw: str) -> tuple[int, int]:
        text = str(raw or "").strip()
        if not re.fullmatch(r"\d{4}-\d{2}", text):
            raise ValueError(f"invalid YYYY-MM: {text}")
        year = int(text[:4])
        month = int(text[5:7])
        if month < 1 or month > 12:
            raise ValueError(f"invalid month: {text}")
        return (int(year), int(month))

    def _ym_start_ms_utc(self, year: int, month: int) -> int:
        dt0 = datetime(int(year), int(month), 1, tzinfo=timezone.utc)
        return int(dt0.timestamp() * 1000)

    def _replay_range_ms_from_inputs(self) -> tuple[int, int]:
        since_year, since_month = self._parse_yyyy_mm(self.replay_since_ym.text())
        until_year, until_month = self._parse_yyyy_mm(self.replay_until_ym.text())
        since_ms = self._ym_start_ms_utc(int(since_year), int(since_month))
        if int(until_month) == 12:
            until_year_ex = int(until_year) + 1
            until_month_ex = 1
        else:
            until_year_ex = int(until_year)
            until_month_ex = int(until_month) + 1
        until_ms = self._ym_start_ms_utc(int(until_year_ex), int(until_month_ex))
        return (int(since_ms), int(until_ms))

    def _month_range_cli_ymd_from_ms(self, since_ms: int, until_ms: int) -> tuple[str, str]:
        if int(until_ms) <= int(since_ms):
            raise ValueError("invalid month range")
        since_ymd = datetime.fromtimestamp(int(since_ms) / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")
        until_ymd = datetime.fromtimestamp((int(until_ms) - 1) / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")
        return (str(since_ymd), str(until_ymd))

    def _resolve_artifact_path(self, raw_path: str) -> str:
        text = str(raw_path or "").strip().strip("\"' ")
        if not text:
            return ""
        if os.path.isabs(text):
            return text
        p = get_paths()
        candidates = [
            os.path.abspath(os.path.join(str(p.runtime_dir), text)),
            os.path.abspath(os.path.join(str(p.repo_root), text)),
            os.path.abspath(text),
        ]
        for cand in candidates:
            if os.path.exists(cand):
                return str(cand)
        return str(candidates[0])

    def _capture_replay_artifact_paths(self, text: str) -> None:
        for ln in str(text or "").splitlines():
            m_report = re.search(r"\[REPLAY\]\[REPORT\]\s+path=(.+?)(?:\s+trades=|$)", ln)
            if m_report:
                self._last_replay_report_path = self._resolve_artifact_path(str(m_report.group(1)))
                continue
            m_trade = re.search(r"\[REPLAY\]\s+trade_log_path=(.+)$", ln)
            if m_trade:
                self._last_replay_trade_log_path = self._resolve_artifact_path(str(m_trade.group(1)))

    def _credential_widgets(self, exchange_id: str | None = None) -> dict[str, QLineEdit]:
        return self._credential_inputs.get(str(exchange_id or self._selected_exchange_id()), {})

    def _refresh_credential_widgets_from_keyring(self, exchange_id: str) -> None:
        creds = load_creds(str(exchange_id))
        if creds is None:
            return
        widgets = self._credential_widgets(exchange_id)
        key_edit = widgets.get("key")
        secret_edit = widgets.get("secret")
        passphrase_edit = widgets.get("passphrase")
        if key_edit is not None:
            key_edit.setText(_mask_secret(str(creds.api_key or "")))
        if secret_edit is not None:
            secret_edit.setText(_mask_secret(str(creds.api_secret or "")))
        if passphrase_edit is not None:
            passphrase = str(getattr(creds, "api_passphrase", "") or "")
            passphrase_edit.setText(_mask_secret(passphrase) if passphrase else "")

    def _has_saved_creds(self, exchange_id: str | None = None) -> bool:
        try:
            return load_creds(str(exchange_id or self._selected_exchange_id())) is not None
        except Exception:
            return False

    def _has_saved_activation(self) -> bool:
        try:
            stored_seat_key = str(load_license_seat_key() or "").strip()
            return bool(stored_seat_key) and (load_license_state() is not None)
        except Exception:
            return False

    def _resolve_runtime_creds(self, exchange_id: str | None = None) -> tuple[str, str, str]:
        ex = str(exchange_id or self._selected_exchange_id())
        option = get_exchange_option(ex)
        widgets = self._credential_widgets(ex)
        key_text = str((widgets.get("key").text() if widgets.get("key") is not None else "") or "").strip()
        secret_text = str((widgets.get("secret").text() if widgets.get("secret") is not None else "") or "").strip()
        passphrase_text = str((widgets.get("passphrase").text() if widgets.get("passphrase") is not None else "") or "").strip()
        key_plain = _is_plain_secret_text(key_text)
        secret_plain = _is_plain_secret_text(secret_text)
        passphrase_plain = _is_plain_secret_text(passphrase_text)
        needs_passphrase = bool(getattr(option, "passphrase_env", ""))
        if key_plain and secret_plain and ((not needs_passphrase) or passphrase_plain):
            return (key_text, secret_text, passphrase_text if passphrase_plain else "")
        creds = load_creds(ex)
        if creds is not None:
            return (
                str(creds.api_key or ""),
                str(creds.api_secret or ""),
                str(getattr(creds, "api_passphrase", "") or ""),
            )
        if key_plain or secret_plain or passphrase_plain:
            return (
                key_text if key_plain else "",
                secret_text if secret_plain else "",
                passphrase_text if passphrase_plain else "",
            )
        return ("", "", "")

    def _selected_run_mode(self) -> str:
        try:
            return _normalize_run_mode(self.run_mode.currentText())
        except Exception:
            return _normalize_run_mode(self._run_mode)

    def _selected_log_level(self) -> str:
        try:
            return _normalize_runtime_log_level(self.log_level.currentText())
        except Exception:
            return _normalize_runtime_log_level(self._log_level)

    def _load_run_mode_from_settings(self) -> str:
        mode = _normalize_run_mode(getattr(self._settings, "run_mode", "PAPER"))
        try:
            p = get_paths()
            if os.path.exists(p.settings_path):
                with open(p.settings_path, "r", encoding="utf-8") as f:
                    raw = json.load(f)
                if isinstance(raw, dict):
                    mode = _normalize_run_mode(raw.get("run_mode", mode))
        except Exception:
            pass
        return mode

    def _save_run_mode_to_settings(self) -> None:
        mode = self._selected_run_mode()
        self._run_mode = mode
        p = get_paths()
        data: dict[str, object] = {}
        try:
            if os.path.exists(p.settings_path):
                with open(p.settings_path, "r", encoding="utf-8") as f:
                    raw = json.load(f)
                if isinstance(raw, dict):
                    data = raw
        except Exception:
            data = {}
        data["run_mode"] = str(mode)
        with open(p.settings_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def _sync_exchange_fields(self) -> None:
        ex = self._selected_exchange_id()
        self._exchange_id = ex
        symbols = symbols_for_exchange(ex) or self._symbols or [self._default_symbol]
        current_symbol = self._selected_symbol() if self.symbol.count() > 0 else self._default_symbol
        target_symbol = normalize_symbol_for_exchange(ex, current_symbol)
        self.symbol.blockSignals(True)
        self.symbol.clear()
        for sym in symbols:
            self.symbol.addItem(str(sym), str(sym))
        idx_symbol = max(0, self.symbol.findData(target_symbol))
        self.symbol.setCurrentIndex(idx_symbol)
        self.symbol.blockSignals(False)
        self._symbols = list(symbols)
        self._default_symbol = target_symbol
        page_index = self._credential_page_index.get(ex, 0)
        self.creds_stack.setCurrentIndex(int(page_index))
        is_visible = bool(self.creds_group.isChecked())
        self.creds_stack.setVisible(is_visible)
        active_inputs = self._credential_widgets(ex)
        self.api_key = active_inputs.get("key")
        self.api_secret = active_inputs.get("secret")
        self.api_passphrase = active_inputs.get("passphrase")
        self._refresh_replay_symbol_options(preferred=target_symbol)

    def _on_exchange_changed(self, _value: str) -> None:
        self._sync_exchange_fields()
        self._refresh_valuation_preview()
        self._poll_live_chart_state()

    def _on_symbol_changed(self, _value: str) -> None:
        self._default_symbol = self._selected_symbol()
        self._refresh_replay_symbol_options(preferred=self._default_symbol)
        self._refresh_valuation_preview()
        self._poll_live_chart_state()

    def _sync_mode_ui(self) -> None:
        mode = self._selected_run_mode()
        self.replay_group.setVisible(mode in {"REPLAY", "BACKTEST"})
        self.creds_group.setVisible(mode == "PAPER")
        self.activation_group.setVisible(False)
        if mode == "REPLAY":
            self.btn_start.setText(self.tr("action.start_replay"))
        elif mode == "BACKTEST":
            self.btn_start.setText(self.tr("action.start_backtest"))
        else:
            self.btn_start.setText(self.tr("action.start_paper"))

    def _on_run_mode_changed(self, _value: str) -> None:
        self._run_mode = self._selected_run_mode()
        self._sync_mode_ui()
        self._refresh_valuation_preview()
        self._reset_live_chart_poll_tracking()
        if self._run_mode != "PAPER":
            self._restore_chart_mode_after_live_candle()
            self._set_live_chart_state(None, auto_prefer_candle=False)
            self._refresh_result_panel()
            return
        self._set_live_chart_state(None, auto_prefer_candle=False)
        self._refresh_result_panel()
        self._poll_live_chart_state()

    def _on_log_level_changed(self, _value: str) -> None:
        self._log_level = self._selected_log_level()

    def on_select_report_out(self) -> None:
        start_path = self._report_out_abs()
        path, _ = QFileDialog.getSaveFileName(
            self,
            self.tr("dialog.select_report_output.title"),
            start_path,
            "JSON files (*.json);;All files (*.*)",
        )
        if not path:
            return
        self.report_out.setText(os.path.abspath(path))
        self.report_out.setCursorPosition(0)

    def _open_replay_log_file(self, prefix: str = "replay") -> None:
        self._close_replay_log_file()
        self._stderr_ring.clear()
        try:
            p = get_paths()
            os.makedirs(p.logs_dir, exist_ok=True)
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            name = str(prefix or "replay").strip().lower() or "replay"
            self._replay_log_path = os.path.join(p.logs_dir, f"{name}_{stamp}.log")
            self._replay_log_fp = open(self._replay_log_path, "w", encoding="utf-8")
            self._append(f"[ui] {name}_log_file={self._replay_log_path}\n")
        except Exception as e:
            self._replay_log_fp = None
            self._replay_log_path = ""
            self._append(f"[ui] {str(prefix or 'replay').strip().lower() or 'replay'}_log_open_error={e}\n")

    def _close_replay_log_file(self) -> None:
        fp = self._replay_log_fp
        self._replay_log_fp = None
        if fp is None:
            return
        try:
            fp.close()
        except Exception:
            pass

    def _open_live_log_file(self) -> None:
        self._close_live_log_file()
        try:
            p = get_paths()
            os.makedirs(p.logs_dir, exist_ok=True)
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self._live_log_path = os.path.join(p.logs_dir, f"live_{stamp}.log")
            self._live_log_fp = open(self._live_log_path, "w", encoding="utf-8")
            self._append(f"[ui] live_log_file={self._live_log_path}\n")
        except Exception as e:
            self._live_log_fp = None
            self._live_log_path = ""
            self._append(f"[ui] live_log_open_error={e}\n")

    def _close_live_log_file(self) -> None:
        fp = self._live_log_fp
        self._live_log_fp = None
        if fp is None:
            return
        try:
            fp.close()
        except Exception:
            pass

    def _save_last_stderr_tail(self) -> str:
        p = get_paths()
        os.makedirs(p.logs_dir, exist_ok=True)
        out = os.path.join(p.logs_dir, "replay_last_stderr.txt")
        try:
            with open(out, "w", encoding="utf-8") as f:
                if self._stderr_ring:
                    f.write("\n".join(self._stderr_ring))
                    f.write("\n")
            return str(out)
        except Exception:
            return str(out)

    def _append_replay_results_summary(self) -> None:
        rp = self._resolve_artifact_path(str(self._last_replay_report_path or ""))
        if not rp:
            self._append("[results] summary unavailable (replay report path missing)\n")
            return
        if not os.path.exists(rp):
            self._append(f"[results] summary unavailable (replay report not found: {rp})\n")
            return
        try:
            with open(rp, "r", encoding="utf-8") as f:
                obj = json.load(f)
            meta = obj.get("meta") if isinstance(obj, dict) else None
            results = obj.get("results") if isinstance(obj, dict) else None
            if not isinstance(meta, dict) or not isinstance(results, dict):
                self._append(f"[results] summary unavailable (invalid replay report: {rp})\n")
                return
            self._append(f"[results] replay_report={rp}\n")
            if str(self._last_replay_trade_log_path or "").strip():
                self._append(f"[results] trade_log={self._last_replay_trade_log_path}\n")
            self._append(
                f"[results] bars={meta.get('bars')} "
                f"range={meta.get('since')}..{meta.get('until')} "
                f"first_ts_ms={meta.get('first_ts_ms')} last_ts_ms={meta.get('last_ts_ms')}\n"
            )
            self._append(
                f"[results] trades={results.get('trades')} "
                f"net_total={results.get('net_total')} "
                f"return_pct_of_init={results.get('return_pct_of_init')} "
                f"max_dd={results.get('max_dd')} "
                f"sharpe_like={results.get('sharpe_like')}\n"
            )
        except Exception as e:
            self._append(f"[results] summary unavailable (replay report read error: {e})\n")

    def _load_result_chart_data(self):
        run_mode = self._selected_run_mode()
        if run_mode == "PAPER":
            return self._build_runtime_result_panel_data(run_mode=run_mode), []
        return load_latest_result_chart_data(
            self._paths.exports_dir,
            repo_root=self._paths.repo_root,
            last_run_payload=self._load_last_run_payload(),
        )

    def _build_runtime_result_panel_data(self, *, run_mode: str = "") -> ResultChartData:
        mode = str(run_mode or self._selected_run_mode() or "PAPER").strip().upper() or "PAPER"
        symbol = str(self._selected_symbol() or self._default_symbol or "").strip()
        source_name = f"{symbol} / {mode}" if symbol else mode
        return ResultChartData(
            source_name=source_name,
            symbol=symbol,
            net_total=0.0,
            max_dd=0.0,
            trades_count=0,
            empty_message="Waiting for live/paper candles...",
        )

    def _clear_manual_stop_state(self) -> None:
        self._manual_stop_requested = False
        self._manual_stop_role = ""
        self._manual_stop_requested_at = 0.0
        self._stop_request_in_flight = False

    def _is_manual_stop_exit(self, role: str) -> bool:
        if not self._manual_stop_requested:
            return False
        expected_role = str(self._manual_stop_role or "").strip()
        if expected_role and str(role or "").strip() != expected_role:
            return False
        age_sec = max(0.0, time.time() - float(self._manual_stop_requested_at or 0.0))
        return age_sec <= 30.0

    def _apply_result_panel_data(self, data: ResultChartData) -> None:
        payload = data if isinstance(data, ResultChartData) else ResultChartData()
        self.result_panel.set_result_data(payload)
        if self._chart_dialog is not None:
            try:
                self._chart_dialog.set_result_data(payload)
            except Exception:
                pass

    def _set_chart_mode_programmatically(self, mode: str) -> None:
        self._suppress_chart_mode_user_lock = True
        try:
            self.result_panel.set_chart_mode(str(mode or ""))
            if self._chart_dialog is not None:
                try:
                    self._chart_dialog.set_chart_mode(str(mode or ""))
                except Exception:
                    pass
        finally:
            self._suppress_chart_mode_user_lock = False

    def _build_waiting_live_chart_state(self, *, reason: str, path: str = "", state_present: bool = False) -> LiveChartState:
        resolved_path = str(path or "").strip()
        if not resolved_path:
            resolved_path = build_chart_state_path(
                self._paths.state_dir,
                self._selected_exchange_id(),
                self._selected_run_mode(),
                self._selected_symbol(),
            )
        return LiveChartState(
            path=str(resolved_path or ""),
            exchange_id=self._selected_exchange_id(),
            run_mode=self._selected_run_mode(),
            symbol=self._selected_symbol(),
            candle_source="empty",
            chart_state_reason=str(reason or "").strip(),
            state_present=bool(state_present),
        )

    def _run_initial_gui_refresh(self) -> None:
        self._refresh_result_panel()
        if self._selected_run_mode() == "PAPER":
            self._poll_live_chart_state()
        self._check_latest_release_async(force=False)

    def _release_repo_full_name(self) -> str:
        return str(getattr(C, "FREE_RELEASE_REPO", "") or "kumiromiscythespec/LoneWolf_Fang_free").strip()

    def _release_latest_url(self) -> str:
        return str(
            getattr(C, "FREE_RELEASE_LATEST_URL", "")
            or f"https://github.com/{self._release_repo_full_name()}/releases/latest"
        ).strip()

    def _version_sort_key(self, raw: str) -> tuple[int, ...]:
        text = str(raw or "").strip().lower().lstrip("v")
        parts = [int(x) for x in re.findall(r"\d+", text)]
        return tuple(parts or [0])

    def _is_newer_version(self, latest: str, current: str) -> bool:
        latest_key = self._version_sort_key(latest)
        current_key = self._version_sort_key(current)
        width = max(len(latest_key), len(current_key))
        return latest_key + (0,) * (width - len(latest_key)) > current_key + (0,) * (width - len(current_key))

    def _check_latest_release_async(self, force: bool = False, notify: bool = False) -> None:
        if self._update_check_running:
            if notify:
                self._update_check_notify = True
            return
        if self._update_check_started and not force:
            return
        repo = self._release_repo_full_name()
        if not repo:
            if notify:
                self.sig_update_failed.emit("repo=<empty> error=missing_repo")
            return
        self._update_check_started = True
        self._update_check_running = True
        self._update_check_notify = bool(notify)
        if notify and hasattr(self, "btn_check_updates"):
            self.btn_check_updates.setEnabled(False)

        def worker() -> None:
            api_url = f"https://api.github.com/repos/{repo}/releases/latest"
            notify_result = bool(notify)
            try:
                req = urllib.request.Request(
                    api_url,
                    headers={
                        "Accept": "application/vnd.github+json",
                        "User-Agent": "LoneWolf-Fang-Free-GUI",
                    },
                )
                with urllib.request.urlopen(req, timeout=6.0) as resp:
                    status = str(getattr(resp, "status", "") or "unknown")
                    payload = json.loads((resp.read() or b"{}").decode("utf-8"))
                if not isinstance(payload, dict):
                    message = f"repo={repo} url={api_url} status={status} error=invalid_response"
                    logger.warning("[update] latest check skipped: %s", message)
                    if notify_result:
                        self.sig_update_failed.emit(message)
                    return
                latest = str(payload.get("tag_name") or payload.get("name") or "").strip()
                release_url = str(payload.get("html_url") or self._release_latest_url()).strip()
                current = str(APP_VERSION or "").strip()
                if not latest or not current:
                    message = f"repo={repo} url={api_url} status={status} error=missing_version latest={latest or '<empty>'} current={current or '<empty>'}"
                    logger.warning("[update] latest check skipped: %s", message)
                    if notify_result:
                        self.sig_update_failed.emit(message)
                    return
                response_log = f"[update] latest_response repo={repo} status={status} latest={latest} current={current} notify={notify_result}"
                logger.info(response_log)
                if notify_result:
                    self.sig_log.emit(f"{response_log}\n")
                if self._is_newer_version(latest, current):
                    if notify_result:
                        self.sig_update_available.emit(latest, current, release_url)
                    return
                if notify_result:
                    self.sig_update_up_to_date.emit(latest, current)
            except Exception as exc:
                message = f"repo={repo} url={api_url} error={exc.__class__.__name__}: {exc}"
                logger.warning("[update] latest check skipped: %s", message)
                if notify_result:
                    self.sig_update_failed.emit(message)
            finally:
                self.sig_update_check_finished.emit()

        threading.Thread(target=worker, name="lwf_free_update_check", daemon=True).start()

    def _on_update_check_finished(self) -> None:
        self._update_check_running = False
        if self._update_check_manual_retry:
            self._update_check_manual_retry = False
            self._update_check_notify = False
            self._check_latest_release_async(force=True, notify=True)
            return
        self._update_check_notify = False
        if hasattr(self, "btn_check_updates"):
            self.btn_check_updates.setEnabled(True)

    def on_check_updates(self) -> None:
        if self._update_check_running:
            self._update_check_notify = True
            self._update_check_manual_retry = True
            self.btn_check_updates.setEnabled(False)
            return
        self.btn_check_updates.setEnabled(False)
        self._append("[update] checking latest release...\n")
        self._check_latest_release_async(force=True, notify=True)
        if not self._update_check_running:
            self.btn_check_updates.setEnabled(True)

    def _show_update_not_available(self, latest: str, current: str) -> None:
        self._append(f"[update] up_to_date current={current} latest={latest}\n")
        QMessageBox.information(self, self.tr("dialog.update_check.title"), self.tr("dialog.update_check.up_to_date"))

    def _show_update_check_failed(self, message: str = "") -> None:
        detail = str(message or "").strip()
        if detail:
            self._append(f"[update] latest check skipped: {detail}\n")
        else:
            self._append("[update] latest_check_failed\n")
        QMessageBox.warning(self, self.tr("dialog.update_check.title"), self.tr("dialog.update_check.failed"))

    def _show_update_available(self, latest: str, current: str, release_url: str) -> None:
        self._append(f"[update] latest_available current={current} latest={latest} url={release_url}\n")
        msg = (
            f"{self.tr('dialog.update_available.message', current=current, latest=latest)}\n\n"
            f"{self.tr('dialog.update_available.open_question')}"
        )
        result = QMessageBox.question(self, self.tr("dialog.update_available.title"), msg)
        if result == QMessageBox.StandardButton.Yes:
            QDesktopServices.openUrl(QUrl(str(release_url or self._release_latest_url())))

    def _set_proc_poll_activity(self, active: bool) -> None:
        target = PROC_POLL_ACTIVE_INTERVAL_MS if active else PROC_POLL_IDLE_INTERVAL_MS
        if self._timer.interval() != target:
            self._timer.setInterval(target)

    def _reset_live_chart_poll_tracking(self) -> None:
        self._last_chart_state_diag_key = None
        self._last_chart_state_reader_path = ""
        self._last_live_chart_poll_signature = None

    def _log_live_chart_reader_path(self, state: LiveChartState | None) -> None:
        path_text = str(getattr(state, "path", "") or "").strip()
        if not path_text:
            path_text = build_chart_state_path(
                self._paths.state_dir,
                self._selected_exchange_id(),
                self._selected_run_mode(),
                self._selected_symbol(),
            )
        if not path_text or path_text == self._last_chart_state_reader_path:
            return
        self._last_chart_state_reader_path = path_text
        self._append(f"[chart_state] reader path={path_text}\n")

    def _log_live_chart_diag(self, state: LiveChartState | None) -> None:
        if not isinstance(state, LiveChartState):
            self._last_chart_state_diag_key = None
            return
        key = (
            str(state.candle_source or "").strip(),
            int(state.candles_count or 0),
            int(state.last_candle_ts_ms or 0),
            str(state.chart_state_reason or "").strip(),
        )
        if key == self._last_chart_state_diag_key:
            return
        self._last_chart_state_diag_key = key
        parts = ["[chart_state]"]
        if key[0]:
            parts.append(f"source={key[0]}")
        parts.append(f"candles={key[1]}")
        if key[2] > 0:
            parts.append(f"last_ts={key[2]}")
        if key[3]:
            parts.append(f"reason={key[3]}")
        self._append(" ".join(parts) + "\n")

    def _restore_chart_mode_after_live_candle(self) -> None:
        if not self._chart_mode_auto_live_candle_applied:
            return
        if self._chart_mode_user_locked:
            self._chart_mode_auto_live_candle_applied = False
            return
        if self.result_panel.chart_mode() != CHART_MODE_CANDLE:
            self._chart_mode_auto_live_candle_applied = False
            return
        restore_mode = str(self._chart_mode_before_live_candle or "Equity")
        self._set_chart_mode_programmatically(restore_mode)
        self._chart_mode_auto_live_candle_applied = False

    def _set_live_chart_state(self, state: LiveChartState | None, *, auto_prefer_candle: bool) -> None:
        self._live_chart_state = state if isinstance(state, LiveChartState) else None
        self._live_chart_available = bool(self._live_chart_state is not None and self._live_chart_state.has_data)
        merged = with_live_chart_state(self._base_result_data, self._live_chart_state)
        self._apply_result_panel_data(merged)
        self._log_live_chart_diag(self._live_chart_state)
        if (
            self._live_chart_available
            and auto_prefer_candle
            and (not self._chart_mode_user_locked)
            and (not self._chart_mode_auto_live_candle_applied)
        ):
            current_mode = self.result_panel.chart_mode()
            if current_mode != CHART_MODE_CANDLE:
                self._chart_mode_before_live_candle = str(current_mode or "Equity")
            self._set_chart_mode_programmatically(CHART_MODE_CANDLE)
            self._chart_mode_auto_live_candle_applied = True

    def _poll_live_chart_state(self) -> None:
        self._gui_perf_counters["live_chart_poll_calls"] += 1
        run_mode = self._selected_run_mode()
        if run_mode not in {"PAPER"}:
            if self._live_chart_state is not None or self._last_live_chart_poll_signature is not None:
                self._reset_live_chart_poll_tracking()
                self._set_live_chart_state(None, auto_prefer_candle=False)
            else:
                self._gui_perf_counters["live_chart_poll_skips"] += 1
            return
        exchange_id = self._selected_exchange_id()
        symbol = self._selected_symbol()
        chart_state_path = build_chart_state_path(self._paths.state_dir, exchange_id, run_mode, symbol)
        is_runner_active = self._proc is not None and self._proc.poll() is None and str(self._proc_role) == "runner"
        state_exists = os.path.isfile(chart_state_path)
        mtime_ns = 0
        size = 0
        stale = False
        if state_exists:
            try:
                stat_result = os.stat(chart_state_path)
                mtime_ns = int(getattr(stat_result, "st_mtime_ns", int(stat_result.st_mtime * 1_000_000_000)))
                size = int(stat_result.st_size)
                if not is_runner_active:
                    stale = max(0.0, time.time() - float(stat_result.st_mtime)) > LIVE_CHART_STALE_SEC
            except Exception:
                pass
        poll_signature = (exchange_id, run_mode, symbol, bool(is_runner_active), bool(state_exists), int(mtime_ns), int(size), bool(stale))
        if poll_signature == self._last_live_chart_poll_signature:
            self._gui_perf_counters["live_chart_poll_skips"] += 1
            return
        self._last_live_chart_poll_signature = poll_signature
        try:
            chart_state = load_live_chart_state_for_runtime(self._paths.state_dir, exchange_id, run_mode, symbol)
        except Exception:
            chart_state = self._build_waiting_live_chart_state(reason="state_load_failed", path=chart_state_path)
        try:
            if (not is_runner_active) and chart_state.path:
                age_sec = max(0.0, time.time() - os.path.getmtime(chart_state.path))
                if age_sec > LIVE_CHART_STALE_SEC:
                    chart_state = self._build_waiting_live_chart_state(
                        reason="state_stale",
                        path=str(chart_state.path or ""),
                        state_present=False,
                    )
        except Exception:
            pass
        self._log_live_chart_reader_path(chart_state)
        self._set_live_chart_state(chart_state, auto_prefer_candle=True)

    def _restore_chart_panel_state(self) -> None:
        state = dict(self._chart_ui_state or {})
        try:
            self._set_chart_mode_programmatically(str(state.get("last_chart_mode", "") or ""))
        except Exception:
            self._set_chart_mode_programmatically("")
        current_mode = self.result_panel.chart_mode()
        if current_mode != CHART_MODE_CANDLE:
            self._chart_mode_before_live_candle = str(current_mode or "Equity")
        sizes = state.get("chart_splitter_sizes")
        if isinstance(sizes, list) and len(sizes) == 2:
            try:
                self.bottom_splitter.setSizes([int(sizes[0]), int(sizes[1])])
            except Exception:
                pass

    def _update_chart_ui_state(
        self,
        *,
        chart_mode: str | None = None,
        chart_splitter_sizes: list[int] | None = None,
    ) -> None:
        if not isinstance(self._settings, AppSettings):
            self._settings = AppSettings()
        set_gui_chart_state(
            self._settings,
            last_chart_mode=chart_mode if chart_mode is not None else self.result_panel.chart_mode(),
            chart_splitter_sizes=chart_splitter_sizes if chart_splitter_sizes is not None else self.bottom_splitter.sizes(),
        )
        self._sync_window_size_settings()
        self._sync_collapsible_section_settings()
        self._chart_ui_state = get_gui_chart_state(self._settings)

    def _persist_chart_ui_state(
        self,
        *,
        chart_mode: str | None = None,
        chart_splitter_sizes: list[int] | None = None,
    ) -> None:
        previous = dict(self._chart_ui_state or {})
        previous_window = (
            int(getattr(self._settings, "main_window_width", DEFAULT_MAIN_WINDOW_WIDTH) or DEFAULT_MAIN_WINDOW_WIDTH),
            int(getattr(self._settings, "main_window_height", DEFAULT_MAIN_WINDOW_HEIGHT) or DEFAULT_MAIN_WINDOW_HEIGHT),
        )
        previous_sections = {
            "gui_section_api_expanded": bool(getattr(self._settings, "gui_section_api_expanded", False)),
            "gui_section_activation_expanded": bool(getattr(self._settings, "gui_section_activation_expanded", False)),
            "gui_section_diagnostics_expanded": bool(getattr(self._settings, "gui_section_diagnostics_expanded", False)),
        }
        try:
            self._update_chart_ui_state(chart_mode=chart_mode, chart_splitter_sizes=chart_splitter_sizes)
            current_window = (
                int(getattr(self._settings, "main_window_width", DEFAULT_MAIN_WINDOW_WIDTH) or DEFAULT_MAIN_WINDOW_WIDTH),
                int(getattr(self._settings, "main_window_height", DEFAULT_MAIN_WINDOW_HEIGHT) or DEFAULT_MAIN_WINDOW_HEIGHT),
            )
            current_sections = {
                "gui_section_api_expanded": bool(getattr(self._settings, "gui_section_api_expanded", False)),
                "gui_section_activation_expanded": bool(getattr(self._settings, "gui_section_activation_expanded", False)),
                "gui_section_diagnostics_expanded": bool(getattr(self._settings, "gui_section_diagnostics_expanded", False)),
            }
            if self._chart_ui_state == previous and current_window == previous_window and current_sections == previous_sections:
                return
            save_settings(self._settings)
        except Exception as exc:
            self._append(f"[ui] chart_state_save_failed error={exc}\n")

    def _on_result_panel_mode_changed(self, mode: str) -> None:
        if not self._suppress_chart_mode_user_lock:
            self._chart_mode_user_locked = True
        if str(mode or "") != CHART_MODE_CANDLE:
            self._chart_mode_before_live_candle = str(mode or "Equity")
            self._chart_mode_auto_live_candle_applied = False
        self._persist_chart_ui_state(chart_mode=str(mode or ""))

    def _on_bottom_splitter_moved(self, _pos: int, _index: int) -> None:
        self._persist_chart_ui_state(chart_splitter_sizes=self.bottom_splitter.sizes())

    def _snapshot_exports_dir(self) -> str:
        target = os.path.abspath(os.path.join(self._paths.exports_dir, "snapshots"))
        os.makedirs(target, exist_ok=True)
        return target

    def _result_export_folder_path(self, _data) -> str:
        return self._snapshot_exports_dir()

    def _open_folder_path(self, path: str) -> bool:
        target = os.path.abspath(str(path or "").strip() or self._snapshot_exports_dir())
        if not os.path.isdir(target):
            return False
        if sys.platform.startswith("win") and hasattr(os, "startfile"):
            try:
                os.startfile(target)
                return True
            except Exception:
                pass
        try:
            return bool(QDesktopServices.openUrl(QUrl.fromLocalFile(target)))
        except Exception:
            return False

    def _open_result_export_folder(self, data) -> None:
        folder = self._result_export_folder_path(data)
        try:
            ok = self._open_folder_path(folder)
        except Exception as exc:
            self._append(f"[result] open_folder failed error={exc}\n")
            return
        if ok:
            self._append(f"[result] open_folder path={folder}\n")
            return
        self._append(f"[result] open_folder failed path={folder}\n")

    def _refresh_result_panel(self, *, announce: bool = False) -> None:
        self._gui_perf_counters["result_refresh_calls"] += 1
        try:
            data, messages = self._load_result_chart_data()
        except Exception as exc:
            self._base_result_data = ResultChartData()
            self._set_live_chart_state(self._live_chart_state, auto_prefer_candle=False)
            self._append(f"[result] refresh failed error={exc}\n")
            return
        next_data = data if isinstance(data, ResultChartData) else ResultChartData()
        if (next_data == self._base_result_data) and (not messages):
            self._gui_perf_counters["result_refresh_skips"] += 1
            if announce:
                shown = self.result_panel.result_data()
                if shown.has_any_result:
                    location = shown.export_dir or self._last_run_export_dir() or self._result_export_folder_path(shown)
                    self._append(f"[result] refreshed export_dir={location}\n")
                else:
                    self._append("[result] No result data\n")
            return
        self._base_result_data = next_data
        self._set_live_chart_state(self._live_chart_state, auto_prefer_candle=False)
        for msg in messages:
            self._append(f"[result] {msg}\n")
        if announce:
            shown = self.result_panel.result_data()
            if shown.has_any_result:
                location = shown.export_dir or self._last_run_export_dir() or self._result_export_folder_path(shown)
                self._append(f"[result] refreshed export_dir={location}\n")
            else:
                self._append("[result] No result data\n")

    def on_refresh_result_panel(self) -> None:
        self._refresh_result_panel(announce=True)

    def _default_result_chart_png_path(self, mode: str, data: ResultChartData | None = None) -> str:
        safe_mode = re.sub(r"[^a-z0-9]+", "_", str(mode or "").strip().lower()).strip("_") or "equity"
        safe_symbol = infer_result_chart_snapshot_symbol(data if isinstance(data, ResultChartData) else ResultChartData())
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        folder = self._snapshot_exports_dir()
        return os.path.abspath(os.path.join(folder, f"chart_{safe_mode}_{safe_symbol}_{stamp}.png"))

    def _save_result_chart_widget(self, widget: ResultChartWidget, mode: str) -> None:
        path = self._default_result_chart_png_path(mode, widget.result_data())
        try:
            ok = bool(widget.save_png(path, QSize(1600, 900)))
        except Exception as exc:
            self._append(f"[result] save failed error={exc}\n")
            QMessageBox.warning(self, self.tr("dialog.save_png.title"), f"Could not save chart PNG.\n{exc}")
            return
        if not ok:
            msg = f"Could not save chart PNG.\n{path}"
            self._append(f"[result] save failed path={path}\n")
            QMessageBox.warning(self, self.tr("dialog.save_png.title"), msg)
            return
        self._append(f"[result] chart_saved path={path}\n")

    def on_save_result_chart(self) -> None:
        self._save_result_chart_widget(self.result_panel.chart_widget, self.result_panel.chart_mode())

    def on_expand_result_chart(self) -> None:
        if self._chart_dialog is None:
            dialog = ChartDialog(self)
            dialog.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, True)
            dialog.saveRequested.connect(lambda: self._save_result_chart_widget(dialog.chart_widget, dialog.chart_mode()))
            dialog.openFolderRequested.connect(lambda: self._open_result_export_folder(dialog.chart_widget.result_data()))

            def _clear_dialog_ref(_result: int) -> None:
                self._chart_dialog = None

            dialog.finished.connect(_clear_dialog_ref)
            self._chart_dialog = dialog
        self._chart_dialog.set_ui_texts(self._ui_text_map())
        self._chart_dialog.set_result_data(self.result_panel.result_data())
        self._chart_dialog.set_chart_mode(self.result_panel.chart_mode())
        self._chart_dialog.show()
        self._chart_dialog.raise_()
        self._chart_dialog.activateWindow()

    def _on_replay_range_edited(self, _text: str) -> None:
        if self._auto_range_enabled:
            self._auto_range_enabled = False
            self._append("[replay] auto range disabled by manual edit\n")

    def _maybe_show_ip_whitelist_hint(self, line: str) -> None:
        if self._ip_whitelist_alerted:
            return
        text = str(line or "")
        lower = text.lower()
        has_700006 = ('"code":700006' in lower) or ('code":700006' in lower)
        has_ip_token = ("ip [" in lower) or ("ip[" in lower)
        if not (has_700006 and has_ip_token):
            return
        m = re.search(r"ip\s*\[([^\]]+)\]", text, flags=re.IGNORECASE)
        ip = str(m.group(1)).strip() if m else "不明"
        if not ip:
            ip = "不明"
        self._ip_whitelist_alerted = True
        msg = (
            "IP制限がONのままです。リストを空にしてもOFFになりません。Bind IP のトグルをOFFにしてください\n"
            f"MEXCが見ているIP：{ip}\n"
            "MEXCのAPIキー編集画面でIP連携を無効化できない場合は、ホワイトリストにこのIPを追加してください"
        )
        QMessageBox.warning(self, "MEXC IP whitelist", msg)

    def _safe_import_build_id(self, module_name: str) -> str:
        try:
            mod = __import__(module_name)
            return str(getattr(mod, "BUILD_ID", None))
        except Exception:
            return "None"

    def _latest_diff_trace_path(self, repo_root: str, runtime_exports: str) -> str:
        last_run_export_dir = self._last_run_export_dir()
        cands = self._glob_existing_files(
            os.path.join(last_run_export_dir, "diff_trace_live_*.jsonl") if last_run_export_dir else "",
            os.path.join(runtime_exports, "runs", "*", "*", "diff_trace_live_*.jsonl"),
            os.path.join(runtime_exports, "diff_trace_live_*.jsonl"),
        )
        if not cands:
            cands = self._glob_existing_files(
                os.path.join(repo_root, "exports", "diff_trace_live_*.jsonl"),
            )
        if not cands:
            return ""
        try:
            return str(max(cands, key=lambda p: os.path.getmtime(p)))
        except Exception:
            return ""

    def _latest_diff_trace_file(self, repo_root: str, runtime_exports: str) -> str:
        latest = self._latest_diff_trace_path(repo_root, runtime_exports)
        if not latest:
            return "None"
        return self._format_diag_artifact_path(latest, runtime_exports=runtime_exports, repo_root=repo_root)

    def on_run_diagnostics(self) -> None:
        self._append("[diag] ===== Diagnostics =====\n")
        p = ensure_runtime_dirs()

        # 1) Version / build
        self._append(f"[diag] gui_build_id={BUILD_ID}\n")
        self._append(f"[diag] runner_build_id={self._safe_import_build_id('runner')}\n")
        self._append(f"[diag] backtest_build_id={self._safe_import_build_id('backtest')}\n")
        self._append(f"[diag] python_version={sys.version.splitlines()[0]}\n")
        self._append(f"[diag] python_executable={sys.executable}\n")

        # 2) Paths / artifacts
        self._append(f"[diag] repo_root={p.repo_root}\n")
        self._append(f"[diag] runtime_dir={p.runtime_dir}\n")
        self._append(f"[diag] logs_dir={p.logs_dir}\n")
        self._append(f"[diag] exports_dir={p.exports_dir}\n")
        self._append(f"[diag] state_dir={p.state_dir}\n")
        self._append(f"[diag] settings_path={p.settings_path} exists={os.path.exists(p.settings_path)}\n")
        self._append(f"[diag] latest_diff_trace_live={self._latest_diff_trace_file(p.repo_root, p.exports_dir)}\n")

        # 3) Keyring status (masked)
        try:
            for item in EXCHANGES:
                ex = str(item.id)
                creds = load_creds(ex)
                if creds is None:
                    self._append(f"[diag] keyring_{ex}_api_key=未保存\n")
                    self._append(f"[diag] keyring_{ex}_api_secret=未保存\n")
                else:
                    self._append(f"[diag] keyring_{ex}_api_key={_mask_secret(str(creds.api_key or ''))}\n")
                    self._append(f"[diag] keyring_{ex}_api_secret={_mask_secret(str(creds.api_secret or ''))}\n")
        except Exception as e:
            self._append(f"[diag] keyring_check_error={e}\n")

        # 4) Time health
        try:
            self._append(f"[diag] utc_now={datetime.now(timezone.utc).isoformat()}\n")
            self._append(f"[diag] local_now={datetime.now().astimezone().isoformat()}\n")
            self._append(f"[diag] time_time={time.time():.6f}\n")
        except Exception as e:
            self._append(f"[diag] time_check_error={e}\n")
        try:
            cp = subprocess.run(
                ["w32tm", "/query", "/status"],
                capture_output=True,
                text=True,
                timeout=4,
                check=False,
            )
            out = (cp.stdout or "").strip()
            if cp.returncode == 0 and out:
                lines = [ln.strip() for ln in out.splitlines() if ln.strip()]
                preview = " | ".join(lines[:6])
                self._append(f"[diag] w32tm_status={preview}\n")
            else:
                err = (cp.stderr or "").strip() or "取得失敗"
                self._append(f"[diag] w32tm_status=取得失敗 ({err})\n")
        except Exception as e:
            self._append(f"[diag] w32tm_status=取得失敗 ({e})\n")

        # 5) Public network reachability
        url = "https://api.mexc.com/api/v3/ping"
        t0 = time.perf_counter()
        try:
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=3.0) as resp:
                status = int(getattr(resp, "status", 0) or resp.getcode() or 0)
                _ = resp.read(128)
            dt_ms = (time.perf_counter() - t0) * 1000.0
            self._append(f"[diag] mexc_ping status={status} latency_ms={dt_ms:.1f}\n")
        except Exception as e:
            dt_ms = (time.perf_counter() - t0) * 1000.0
            self._append(f"[diag] mexc_ping error={e} latency_ms={dt_ms:.1f}\n")

    def on_create_support_bundle(self) -> None:
        p = ensure_runtime_dirs()
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_zip = os.path.join(p.exports_dir, f"support_bundle_{ts}.zip")
        os.makedirs(p.exports_dir, exist_ok=True)

        repo_exports = os.path.join(p.repo_root, "exports")
        last_run_export_dir = self._last_run_export_dir()
        targets = []
        targets.extend(self._glob_existing_files(self._last_run_json_path()))
        self._extend_targets_with_fallback(
            targets,
            [
                os.path.join(p.exports_dir, "daily_metrics_*.json"),
                os.path.join(last_run_export_dir, "daily_metrics_*.json") if last_run_export_dir else "",
            ],
            [os.path.join(repo_exports, "daily_metrics_*.json")],
        )
        latest_diff_trace = self._latest_diff_trace_path(p.repo_root, p.exports_dir)
        if latest_diff_trace:
            targets.append(latest_diff_trace)
        self._extend_targets_with_fallback(
            targets,
            [
                os.path.join(last_run_export_dir, "report.json") if last_run_export_dir else "",
                os.path.join(p.exports_dir, "report.json"),
            ],
            [os.path.join(repo_exports, "report.json")],
        )
        if os.path.exists(p.settings_path):
            targets.append(p.settings_path)
        if os.path.isdir(p.logs_dir):
            targets.extend(glob.glob(os.path.join(p.logs_dir, "*")))

        uniq = []
        seen = set()
        for fp in targets:
            ab = os.path.abspath(fp)
            if ab in seen:
                continue
            if not os.path.isfile(ab):
                continue
            seen.add(ab)
            uniq.append(ab)

        try:
            with zipfile.ZipFile(out_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                for fp in uniq:
                    try:
                        arc = os.path.relpath(fp, p.repo_root)
                    except Exception:
                        arc = os.path.basename(fp)
                    zf.write(fp, arcname=arc)
                zf.writestr("runtime/gui_log.txt", self.log.toPlainText())
            self._append(f"[diag] support_bundle_created={out_zip} files={len(uniq)+1}\n")
        except Exception as e:
            self._append(f"[diag] support_bundle_error={e}\n")

    def _tf_to_ms(self, tf: str) -> int:
        s = str(tf or "").strip().lower()
        try:
            if s.endswith("m"):
                return int(s[:-1]) * 60_000
            if s.endswith("h"):
                return int(s[:-1]) * 3_600_000
            if s.endswith("d"):
                return int(s[:-1]) * 86_400_000
        except Exception:
            return 300_000
        return 300_000

    def _to_ts_ms(self, raw: str) -> int:
        try:
            x = int(float(str(raw).strip()))
        except Exception:
            return 0
        if x <= 0:
            return 0
        if x < 1_000_000_000_000:
            x *= 1000
        if x >= 100_000_000_000_000:
            x //= 1000
        return int(x)

    def _csv_range_from_text_stream(self, stream: io.TextIOBase) -> tuple[int, int]:
        ts_cols = ("ts_ms", "timestamp", "time", "ts", "open_time")
        r = csv.reader(stream)
        first_row = next(r, None)
        if not first_row:
            return (0, 0)
        row0 = [str(x).strip() for x in first_row]
        row0_l = [x.lower() for x in row0]
        idx = 0
        has_header = False
        for k in ts_cols:
            if k in row0_l:
                idx = row0_l.index(k)
                has_header = True
                break
        first_ts = 0
        last_ts = 0

        def _row_ts(row: list[str]) -> int:
            if not row:
                return 0
            j = idx if idx < len(row) else 0
            return self._to_ts_ms(row[j] if j < len(row) else "")

        if not has_header:
            t0 = _row_ts(row0)
            if t0 > 0:
                first_ts = t0
                last_ts = t0

        for row in r:
            t = _row_ts([str(x).strip() for x in row])
            if t <= 0:
                continue
            if first_ts <= 0:
                first_ts = t
            last_ts = t
        return (int(first_ts), int(last_ts))

    def _auto_set_replay_range_from_path(self, path: str, source: str = "") -> bool:
        p = str(path or "").strip()
        if not p or not os.path.exists(p):
            return False
        first_ts = 0
        last_ts = 0
        used_source = str(source or p)
        try:
            ext = os.path.splitext(p)[1].lower()
            if ext == ".csv":
                with open(p, "r", encoding="utf-8-sig", newline="") as f:
                    first_ts, last_ts = self._csv_range_from_text_stream(f)
                used_source = str(p)
            elif os.path.isdir(p):
                files = sorted(glob.glob(os.path.join(p, "*.csv")))
                if files:
                    fp = str(files[-1])
                    with open(fp, "r", encoding="utf-8-sig", newline="") as f:
                        first_ts, last_ts = self._csv_range_from_text_stream(f)
                    used_source = str(fp)
            elif ext == ".zip":
                with zipfile.ZipFile(p, "r") as zf:
                    names = [n for n in zf.namelist() if str(n).lower().endswith(".csv")]
                    if names:
                        name = str(sorted(names)[-1])
                        with zf.open(name, "r") as bf:
                            tf = io.TextIOWrapper(bf, encoding="utf-8-sig", errors="ignore", newline="")
                            first_ts, last_ts = self._csv_range_from_text_stream(tf)
                        used_source = f"{p}:{name}"
            else:
                return False
        except Exception as e:
            self._append(f"[replay] auto range parse failed: {e}\n")
            return False

        if first_ts <= 0 or last_ts <= 0:
            return False
        tf_ms = int(self._tf_to_ms(self.replay_tf.currentText()))
        until_ms = int(last_ts) + int(tf_ms)
        since_ym = datetime.fromtimestamp(int(first_ts) / 1000.0, tz=timezone.utc).strftime("%Y-%m")
        until_ym = datetime.fromtimestamp(max(int(first_ts), int(until_ms) - 1) / 1000.0, tz=timezone.utc).strftime("%Y-%m")
        self._last_auto_range_source = str(used_source)
        self.replay_since_ym.setText(str(since_ym))
        self.replay_until_ym.setText(str(until_ym))
        self._append(
            f"[replay] auto range set since={since_ym} until={until_ym} "
            f"(since_ms={int(first_ts)} until_ms={int(until_ms)} tf_ms={int(tf_ms)}) source={used_source}\n"
        )
        return True

    def _resolve_replay_data_arg(self, selected_path: str) -> str:
        p = os.path.abspath(str(selected_path or "").strip())
        if not p:
            return str(self._default_dataset_root)
        if os.path.isfile(p):
            p = os.path.dirname(p)
        return str(p)

    def _symbol_to_prefix(self, symbol: str) -> str:
        return symbol_to_prefix(symbol)

    def _resolve_replay_dataset_paths(
        self,
        dataset_root: str,
        symbol: str,
        selected_csv_source: str = "",
        year_hint_override: int | None = None,
    ) -> tuple[str, str, str, str, str, str, str, int]:
        root = self._resolve_replay_data_arg(dataset_root)
        prefix = self._symbol_to_prefix(symbol)
        if not root:
            root = str(self._default_dataset_root)
        year_hint = int(self._replay_dataset_year or 0)
        reason = "from_default"

        src = str(selected_csv_source or self._selected_replay_csv_source or "").strip()
        if (not src) and self._auto_range_enabled:
            src = str(self._last_auto_range_source or "").strip()
        if src and ":" in src and (not os.path.exists(src)):
            src = src.split(":", 1)[0]
        if src:
            pfx_src, y_src = infer_prefix_year_from_source(src, fallback_prefix=prefix)
            if pfx_src:
                prefix = str(pfx_src)
            if y_src is not None:
                year_hint = int(y_src)
                reason = "from_selected_csv_year"

        pfx_dir, y_dir = infer_prefix_year_from_source(root, fallback_prefix=prefix)
        base = os.path.basename(os.path.abspath(root))
        if y_dir is not None and re.match(rf"^{re.escape(pfx_dir)}_(5m|1h)_{int(y_dir)}$", base, flags=re.IGNORECASE):
            prefix = str(pfx_dir)
            year_hint = int(y_dir)
            root = os.path.dirname(root) or root
            reason = "from_selected_year_dir"
        elif year_hint_override is not None and int(year_hint_override) > 0:
            year_hint = int(year_hint_override)
            reason = "from_range_year"

        layout = resolve_dataset_layout(
            dataset_root=root,
            prefix=prefix,
            year=(year_hint if int(year_hint) > 0 else None),
            tf_dirs=("5m", "1h"),
        )
        if reason == "from_default":
            reason = str(layout.reason)
        y_out = int(layout.year) if layout.year is not None else 0
        return (
            str(layout.prefix),
            str(layout.root),
            str(layout.dir_entry),
            str(layout.dir_filter),
            str(layout.glob_entry),
            str(layout.glob_filter),
            str(reason),
            int(y_out),
        )

    def _count_csv_matches(self, csv_dir: str) -> int:
        d = str(csv_dir or "").strip()
        if not d or (not os.path.isdir(d)):
            return 0
        try:
            return int(len(glob.glob(os.path.join(d, "*.csv"))))
        except Exception:
            return 0

    def _list_matching_csv_paths(self, csv_dir: str, csv_glob: str) -> list[str]:
        d = str(csv_dir or "").strip()
        pattern = str(csv_glob or "").strip()
        if not d or (not os.path.isdir(d)) or (not pattern):
            return []
        try:
            return sorted(str(path) for path in glob.glob(os.path.join(d, pattern)))
        except Exception:
            return []

    def _csv_path_month_key(self, csv_path: str) -> int:
        base = os.path.basename(str(csv_path or "").strip())
        m = re.search(r"-(\d{4})-(\d{2})\.csv$", base, flags=re.IGNORECASE)
        if not m:
            return 0
        try:
            year = int(m.group(1))
            month = int(m.group(2))
        except Exception:
            return 0
        if month < 1 or month > 12:
            return 0
        return int((year * 100) + month)

    def _filter_csv_paths_for_range(self, csv_paths: list[str], since_ms: int, until_ms: int) -> list[str]:
        try:
            since_key = int(datetime.fromtimestamp(int(since_ms) / 1000.0, tz=timezone.utc).strftime("%Y%m"))
            until_key = int(datetime.fromtimestamp((int(until_ms) - 1) / 1000.0, tz=timezone.utc).strftime("%Y%m"))
        except Exception:
            return []
        if since_key <= 0 or until_key < since_key:
            return []
        out: list[str] = []
        for path in csv_paths:
            month_key = self._csv_path_month_key(path)
            if month_key <= 0:
                continue
            if since_key <= month_key <= until_key:
                out.append(str(path))
        return out

    def _classify_backtest_guidance(self, reasons: list[str]) -> tuple[str, str]:
        reason_set = {str(reason or "").strip() for reason in reasons if str(reason or "").strip()}
        matched: list[str] = []
        if reason_set & {"dir_5m_missing", "dir_1h_missing"}:
            matched.append("DATASET_MISSING")
        if reason_set & {"csv_5m_missing", "csv_1h_missing"}:
            matched.append("SYMBOL_MISSING_OR_UNAVAILABLE")
        if reason_set & {"csv_5m_out_of_range", "csv_1h_out_of_range"}:
            matched.append("RANGE_OUT_OF_DATA")
        if not matched:
            return ("", "")
        primary = matched[0]
        overall = primary if len(matched) == 1 else "MIXED"
        return (overall, primary)

    def _focus_backtest_guidance_target(self, primary_category: str) -> None:
        if primary_category == "DATASET_MISSING":
            self.tools_group.setChecked(True)
            self.btn_pipeline.setFocus()
            return
        if primary_category == "SYMBOL_MISSING_OR_UNAVAILABLE":
            self.symbol.setFocus()
            try:
                self.symbol.showPopup()
            except Exception:
                pass
            return
        if primary_category == "RANGE_OUT_OF_DATA":
            self.replay_since_ym.setFocus()
            self.replay_since_ym.selectAll()

    def _show_backtest_guidance_dialog(
        self,
        *,
        primary_category: str,
        symbol: str,
        dataset_root: str,
        since_text: str,
        until_text: str,
    ) -> None:
        resolved_symbol = symbol or self._default_symbol
        resolved_dataset_root = dataset_root or self._default_dataset_root
        if primary_category == "DATASET_MISSING":
            guidance_key = "dataset_missing"
            message_kwargs = {
                "symbol": resolved_symbol,
                "dataset_root": resolved_dataset_root,
            }
        elif primary_category == "SYMBOL_MISSING_OR_UNAVAILABLE":
            guidance_key = "symbol_missing"
            message_kwargs = {
                "symbol": resolved_symbol,
                "dataset_root": resolved_dataset_root,
            }
        else:
            guidance_key = "range_out_of_data"
            message_kwargs = {
                "since": since_text,
                "until": until_text,
                "dataset_root": resolved_dataset_root,
            }

        title = self.tr(f"dialog.backtest_guidance.{guidance_key}.title")
        message = self.tr(f"dialog.backtest_guidance.{guidance_key}.message", **message_kwargs)
        action_text = self.tr(f"dialog.backtest_guidance.{guidance_key}.action")

        dialog = QMessageBox(self)
        dialog.setIcon(QMessageBox.Icon.Warning)
        dialog.setWindowTitle(title)
        dialog.setText(message)
        action_button = dialog.addButton(action_text, QMessageBox.ButtonRole.ActionRole)
        dialog.addButton(self.tr("action.close"), QMessageBox.ButtonRole.RejectRole)
        if isinstance(action_button, QPushButton):
            action_button.setDefault(True)
        dialog.exec()
        if dialog.clickedButton() is action_button:
            self._focus_backtest_guidance_target(primary_category)

    def _validate_backtest_launch_inputs(self, prepared: dict[str, object]) -> bool:
        symbol = str(prepared.get("symbol", "") or "").strip()
        dataset_root = str(prepared.get("data_arg", "") or "").strip()
        dir_5m = str(prepared.get("dir_5m", "") or "").strip()
        dir_1h = str(prepared.get("dir_1h", "") or "").strip()
        glob_5m = str(prepared.get("glob_5m", "") or "").strip()
        glob_1h = str(prepared.get("glob_1h", "") or "").strip()
        since_ms = int(prepared.get("since_ms", 0) or 0)
        until_ms = int(prepared.get("until_ms", 0) or 0)
        since_year = int(prepared.get("since_year", 0) or 0)
        until_year = int(prepared.get("until_year", 0) or 0)
        multi_year = since_year > 0 and until_year > since_year

        reasons: list[str] = []
        years_loaded: list[int] = []
        if multi_year:
            dataset_diag: dict[str, object] = {}
            csv_paths_5m: list[str] = []
            csv_paths_1h: list[str] = []
            try:
                dataset_spec = resolve_dataset(
                    dataset_root=dataset_root,
                    prefix=str(prepared.get("prefix", "") or symbol_to_prefix(symbol)),
                    year=None,
                    years=list(range(since_year, until_year + 1)),
                    tf_dirs=("5m", "1h"),
                    runtime_symbol=symbol,
                    context="BACKTEST",
                )
                dataset_diag = dict(dataset_spec.diagnostics or {})
                dir_5m = str(dataset_spec.dir_5m or dir_5m)
                dir_1h = str(dataset_spec.dir_1h or dir_1h)
                glob_5m = str(dataset_diag.get("glob_5m", glob_5m) or glob_5m)
                glob_1h = str(dataset_diag.get("glob_1h", glob_1h) or glob_1h)
                csv_paths_5m = [str(path) for path in list(dataset_spec.paths_5m)]
                csv_paths_1h = [str(path) for path in list(dataset_spec.paths_1h)]
            except DatasetResolutionError as e:
                dataset_diag = dict(e.diagnostics or {})
                searched_5m = [str(path) for path in list(dataset_diag.get("searched_paths_5m", []) or [])]
                searched_1h = [str(path) for path in list(dataset_diag.get("searched_paths_1h", []) or [])]
                dir_5m = str(dataset_diag.get("dir_5m", "") or (searched_5m[0] if searched_5m else dir_5m))
                dir_1h = str(dataset_diag.get("dir_1h", "") or (searched_1h[0] if searched_1h else dir_1h))
                glob_5m = str(dataset_diag.get("glob_5m", glob_5m) or glob_5m)
                glob_1h = str(dataset_diag.get("glob_1h", glob_1h) or glob_1h)
                csv_paths_5m = [str(path) for path in list(dataset_diag.get("paths_5m", []) or [])]
                csv_paths_1h = [str(path) for path in list(dataset_diag.get("paths_1h", []) or [])]
                mismatch_reasons = list(dataset_diag.get("mismatch_reasons", []) or [])
                if mismatch_reasons:
                    if not csv_paths_5m:
                        reasons.append("csv_5m_missing")
                    if not csv_paths_1h:
                        reasons.append("csv_1h_missing")
                else:
                    if (not csv_paths_5m) or list(dataset_diag.get("missing_years_5m", []) or []):
                        reasons.append("dir_5m_missing")
                    if (not csv_paths_1h) or list(dataset_diag.get("missing_years_1h", []) or []):
                        reasons.append("dir_1h_missing")

            prepared["dir_5m"] = str(dir_5m)
            prepared["dir_1h"] = str(dir_1h)
            prepared["glob_5m"] = str(glob_5m)
            prepared["glob_1h"] = str(glob_1h)
            years_loaded = [int(y) for y in list(dataset_diag.get("years_loaded", []) or []) if str(y).strip()]
            range_paths_5m = self._filter_csv_paths_for_range(csv_paths_5m, since_ms, until_ms)
            range_paths_1h = self._filter_csv_paths_for_range(csv_paths_1h, since_ms, until_ms)
            if csv_paths_5m and (not range_paths_5m):
                reasons.append("csv_5m_out_of_range")
            if csv_paths_1h and (not range_paths_1h):
                reasons.append("csv_1h_out_of_range")
        else:
            csv_paths_5m = self._list_matching_csv_paths(dir_5m, glob_5m)
            csv_paths_1h = self._list_matching_csv_paths(dir_1h, glob_1h)
            range_paths_5m = self._filter_csv_paths_for_range(csv_paths_5m, since_ms, until_ms)
            range_paths_1h = self._filter_csv_paths_for_range(csv_paths_1h, since_ms, until_ms)
            if not os.path.isdir(dir_5m):
                reasons.append("dir_5m_missing")
            if not os.path.isdir(dir_1h):
                reasons.append("dir_1h_missing")
            if not csv_paths_5m:
                reasons.append("csv_5m_missing")
            if not csv_paths_1h:
                reasons.append("csv_1h_missing")
            if csv_paths_5m and (not range_paths_5m):
                reasons.append("csv_5m_out_of_range")
            if csv_paths_1h and (not range_paths_1h):
                reasons.append("csv_1h_out_of_range")

        ok = not reasons
        since_text = str(self.replay_since_ym.text() or "").strip()
        until_text = str(self.replay_until_ym.text() or "").strip()
        if multi_year:
            self._append(
                f"[backtest] dataset validation symbol={symbol} dataset_root={dataset_root} "
                f"dir_5m={dir_5m} dir_1h={dir_1h} glob_5m={glob_5m} glob_1h={glob_1h} "
                f"years_requested={since_year}..{until_year} years_loaded={years_loaded} "
                f"count_5m={len(csv_paths_5m)} count_1h={len(csv_paths_1h)} "
                f"in_range_5m={len(range_paths_5m)} in_range_1h={len(range_paths_1h)} "
                f"since={since_text} until={until_text} result={'ok' if ok else 'ng'}"
                + (f" reasons={','.join(reasons)}" if reasons else "")
                + "\n"
            )
        else:
            self._append(
                f"[backtest] dataset validation symbol={symbol} dataset_root={dataset_root} "
                f"dir_5m={dir_5m} dir_1h={dir_1h} glob_5m={glob_5m} glob_1h={glob_1h} "
                f"count_5m={len(csv_paths_5m)} count_1h={len(csv_paths_1h)} "
                f"in_range_5m={len(range_paths_5m)} in_range_1h={len(range_paths_1h)} "
                f"since={since_text} until={until_text} result={'ok' if ok else 'ng'}"
                + (f" reasons={','.join(reasons)}" if reasons else "")
                + "\n"
            )
        if ok:
            return True

        category, primary_category = self._classify_backtest_guidance(reasons)
        self._show_backtest_guidance_dialog(
            primary_category=primary_category or "DATASET_MISSING",
            symbol=symbol,
            dataset_root=dataset_root,
            since_text=since_text,
            until_text=until_text,
        )
        self._append(
            f"[backtest] launch aborted due to missing data category={category or 'DATASET_MISSING'} "
            f"primary={primary_category or 'DATASET_MISSING'}\n"
        )
        return False

    def on_select_replay_data(self) -> None:
        p = get_paths()
        start_dir = p.repo_root
        cur = str(self.replay_data.text() or "").strip()
        if cur and os.path.exists(cur):
            start_dir = os.path.dirname(cur) if os.path.isfile(cur) else cur
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Select Replay Data",
            start_dir,
            "Replay data (*.zip *.csv *.json);;All files (*.*)",
        )
        if not path:
            return
        if os.path.isfile(path) and str(path).lower().endswith(".csv"):
            self._selected_replay_csv_source = os.path.abspath(str(path))
        else:
            self._selected_replay_csv_source = ""
        symbol = str(self._selected_replay_symbol() or self._default_symbol)
        prefix, root, dir_5m, _, _, _, _, y = self._resolve_replay_dataset_paths(
            str(path),
            symbol,
            selected_csv_source=self._selected_replay_csv_source,
        )
        self._replay_data_path = str(root or self._default_dataset_root)
        self._replay_dataset_prefix = str(prefix or self._replay_dataset_prefix)
        self._replay_dataset_year = int(y or 0)
        self.replay_data.setText(self._replay_data_path)
        self._append(f"[replay] data selected: {self._replay_data_path}\n")
        if not self._auto_range_enabled:
            self._append("[replay] auto range skipped (manual override)\n")
            return
        if os.path.isfile(path):
            if not self._auto_set_replay_range_from_path(path, source=str(path)):
                self._append("[replay] auto range not set (unsupported or parse failed)\n")
            return
        target = dir_5m if os.path.isdir(dir_5m) else self._replay_data_path
        if not self._auto_set_replay_range_from_path(target, source=str(target)):
            self._append("[replay] auto range not set (unsupported or parse failed)\n")

    def on_select_replay_folder(self) -> None:
        p = get_paths()
        start_dir = p.repo_root
        cur = str(self.replay_data.text() or "").strip()
        if cur and os.path.exists(cur):
            start_dir = cur if os.path.isdir(cur) else os.path.dirname(cur)
        path = QFileDialog.getExistingDirectory(self, "Select Dataset Root", start_dir)
        if not path:
            return
        self._selected_replay_csv_source = ""
        symbol = str(self._selected_replay_symbol() or self._default_symbol)
        prefix, root, dir_5m, _, _, _, _, y = self._resolve_replay_dataset_paths(str(path), symbol)
        self._replay_data_path = str(root or self._default_dataset_root)
        self._replay_dataset_prefix = str(prefix or self._replay_dataset_prefix)
        self._replay_dataset_year = int(y or 0)
        self.replay_data.setText(self._replay_data_path)
        self._append(f"[replay] dataset root selected: {self._replay_data_path}\n")
        _ = dir_5m
        self._append("[replay] auto range unchanged (dataset root selected)\n")

    def _parse_ms(self, raw: str, default: int) -> int:
        s = str(raw or "").strip()
        if not s:
            return int(default)
        try:
            v = int(float(s))
            if v > 0:
                return int(v)
        except Exception:
            pass
        return int(default)

    def _prepare_offline_run_inputs(self, mode_name: str) -> Optional[dict[str, object]]:
        title = str(mode_name or "Replay").strip() or "Replay"
        tag = title.lower()
        try:
            since_ms, until_ms = self._replay_range_ms_from_inputs()
            since_year, _ = self._parse_yyyy_mm(self.replay_since_ym.text())
            until_year, _ = self._parse_yyyy_mm(self.replay_until_ym.text())
            since_ymd, until_ymd = self._month_range_cli_ymd_from_ms(int(since_ms), int(until_ms))
        except Exception as e:
            QMessageBox.warning(self, f"{title} range invalid", self.tr("dialog.period_yyyy_mm.message", detail=str(e)))
            return None
        data_path = str(self.replay_data.text() or "").strip() or str(self._replay_data_path or self._default_dataset_root)
        symbol = str(self._selected_replay_symbol() or self._default_symbol)
        timeframe = str(self.replay_tf.currentText() or "5m")
        multi_year_backtest = tag == "backtest" and int(since_year or 0) > 0 and int(until_year or 0) > int(since_year or 0)
        if multi_year_backtest:
            prefix, data_arg, _, _, _, _, resolve_reason, _ = self._resolve_replay_dataset_paths(
                data_path,
                symbol,
                selected_csv_source=self._selected_replay_csv_source,
            )
            self._replay_data_path = str(data_arg)
            self._replay_dataset_prefix = str(prefix or self._replay_dataset_prefix)
            self._replay_dataset_year = 0
            self.replay_data.setText(self._replay_data_path)
            self._append(
                f"[{tag}] resolved continuous years: {int(since_year)}..{int(until_year)} "
                f"root={data_arg} prefix={prefix} (reason={resolve_reason})\n"
            )
            if int(until_ms) <= int(since_ms):
                QMessageBox.warning(self, f"{title} range invalid", self.tr("dialog.until_month_invalid.message"))
                return None
            return {
                "symbol": str(symbol),
                "timeframe": str(timeframe),
                "since_ms": int(since_ms),
                "until_ms": int(until_ms),
                "since_ymd": str(since_ymd),
                "until_ymd": str(until_ymd),
                "data_arg": str(data_arg),
                "prefix": str(prefix),
                "dir_5m": "",
                "dir_1h": "",
                "glob_5m": "",
                "glob_1h": "",
                "dataset_year": 0,
                "since_year": int(since_year),
                "until_year": int(until_year),
                "continuous_years": f"{int(since_year)}..{int(until_year)}",
            }
        prev_dataset_year = int(self._replay_dataset_year or 0)
        range_year_hint = int(since_year) if int(since_year) == int(until_year) else 0
        prefix, data_arg, dir_5m, dir_1h, glob_5m, glob_1h, resolve_reason, year = self._resolve_replay_dataset_paths(
            data_path,
            symbol,
            selected_csv_source=self._selected_replay_csv_source,
            year_hint_override=(int(range_year_hint) if int(range_year_hint) > 0 else None),
        )
        self._replay_data_path = str(data_arg)
        self._replay_dataset_prefix = str(prefix or self._replay_dataset_prefix)
        self._replay_dataset_year = int(year or 0)
        self.replay_data.setText(self._replay_data_path)
        dir_missing = (not os.path.isdir(dir_5m)) or (not os.path.isdir(dir_1h))
        if dir_missing and tag != "backtest":
            msg = build_missing_dataset_message(
                context=f"GUI_{title.upper()}",
                tf="5m/1h",
                searched_dir=f"{dir_5m} | {dir_1h}",
                searched_glob=f"{glob_5m} | {glob_1h}",
                dataset_root=data_arg,
                prefix=prefix,
                year=(int(year) if int(year) > 0 else None),
                tf_dirs=("5m", "1h"),
            )
            QMessageBox.warning(self, f"{title} folder missing", msg)
            return None

        if resolve_reason == "from_range_year" and int(prev_dataset_year) != int(year or 0):
            self._append(f"[{tag}] dataset year aligned to range: {prev_dataset_year} -> {int(year or 0)}\n")
        self._append(
            f"[{tag}] resolved dirs: 5m_dir={dir_5m} 1h_dir={dir_1h} "
            f"(reason={resolve_reason} year={int(year) if int(year) > 0 else 'auto'})\n"
        )
        cnt_5m = self._count_csv_matches(dir_5m)
        cnt_1h = self._count_csv_matches(dir_1h)
        cnt_total = int(cnt_5m) + int(cnt_1h)
        self._append(
            f"[{tag}] data check: 5m_dir={dir_5m} count={cnt_5m} "
            f"1h_dir={dir_1h} count={cnt_1h} total={cnt_total}\n"
        )
        if cnt_total <= 0 and not (tag == "backtest" and dir_missing):
            QMessageBox.warning(
                self,
                f"{title} data empty",
                self.tr("dialog.data_empty.message", tag=tag),
            )
            return None
        if int(until_ms) <= int(since_ms):
            QMessageBox.warning(self, f"{title} range invalid", self.tr("dialog.until_month_invalid.message"))
            return None
        return {
            "symbol": str(symbol),
            "timeframe": str(timeframe),
            "since_ms": int(since_ms),
            "until_ms": int(until_ms),
            "since_ymd": str(since_ymd),
            "until_ymd": str(until_ymd),
            "data_arg": str(data_arg),
            "prefix": str(prefix),
            "dir_5m": str(dir_5m),
            "dir_1h": str(dir_1h),
            "glob_5m": str(glob_5m),
            "glob_1h": str(glob_1h),
            "dataset_year": int(year or 0),
        }

    def on_run_pipeline(self) -> None:
        if self._proc is not None and self._proc.poll() is None:
            QMessageBox.information(self, "Already running", "Bot is already running.")
            return
        exchange_id = self._selected_exchange_id()
        symbol = self._selected_symbol()
        from_ym = str(self.pipeline_from_ym.text() or "").strip()
        to_ym = str(self.pipeline_to_ym.text() or "").strip()
        if not re.fullmatch(r"\d{4}-\d{2}", from_ym) or not re.fullmatch(r"\d{4}-\d{2}", to_ym):
            QMessageBox.warning(self, "Invalid period", "From/To must be YYYY-MM.")
            return
        try:
            p = get_paths()
            cmd = [
                sys.executable,
                "-u",
                "-m",
                "app.core.data_pipeline",
                "--symbol",
                str(symbol),
                "--from",
                str(from_ym),
                "--to",
                str(to_ym),
            ]
            env = dict(os.environ)
            env["LWF_EXCHANGE_ID"] = str(exchange_id)
            env["LWF_PIPELINE_FORCE"] = "1" if bool(self.pipeline_force.isChecked()) else "0"
            option = self._selected_exchange_option()
            api_key, api_secret, api_passphrase = self._resolve_runtime_creds(exchange_id)
            if option.key_env:
                env[str(option.key_env)] = str(api_key or "")
            if option.secret_env:
                env[str(option.secret_env)] = str(api_secret or "")
            if option.passphrase_env:
                env[str(option.passphrase_env)] = str(api_passphrase or "")
                env["OKX_API_PASSWORD"] = str(api_passphrase or "")
            creationflags = 0
            if os.name == "nt":
                creationflags = int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
            self._append(
                f"[UI] pipeline started exchange_id={exchange_id} symbol={symbol} "
                f"from={from_ym} to={to_ym} force={env['LWF_PIPELINE_FORCE']}\n"
            )
            self._clear_manual_stop_state()
            self._proc_role = "pipeline"
            self._close_replay_log_file()
            self._close_live_log_file()
            self._proc = subprocess.Popen(
                cmd,
                cwd=p.repo_root,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                universal_newlines=True,
                creationflags=creationflags,
            )
            self._set_proc_poll_activity(active=self._proc is not None)
            start_log_threads(
                stdout=self._proc.stdout,
                stderr=self._proc.stderr,
                emit=self.sig_log.emit,
            )
            self.btn_start.setEnabled(False)
            self.btn_start.setChecked(False)
            self.btn_run_replay.setEnabled(False)
            self.btn_stop.setEnabled(True)
            self.btn_pipeline.setEnabled(False)
        except Exception as e:
            QMessageBox.critical(self, "Pipeline start failed", str(e))
            self._proc = None
            self._proc_role = "runner"
            self._clear_manual_stop_state()
            self.btn_pipeline.setEnabled(True)

    def _on_run_replay_legacy(self) -> None:
        if self._proc is not None and self._proc.poll() is None:
            QMessageBox.information(self, "Already running", "Bot is already running.")
            return
        try:
            since_ms, until_ms = self._replay_range_ms_from_inputs()
            since_year, _ = self._parse_yyyy_mm(self.replay_since_ym.text())
            until_year, _ = self._parse_yyyy_mm(self.replay_until_ym.text())
        except Exception as e:
            QMessageBox.warning(self, "Replay range invalid", f"Since/Until must be YYYY-MM.\n{e}")
            return
        data_path = str(self.replay_data.text() or "").strip() or str(self._replay_data_path or self._default_dataset_root)
        symbol = str(self._selected_replay_symbol() or self._default_symbol)
        prev_dataset_year = int(self._replay_dataset_year or 0)
        range_year_hint = int(since_year) if int(since_year) == int(until_year) else 0
        prefix, data_arg, dir_5m, dir_1h, glob_5m, glob_1h, resolve_reason, y = self._resolve_replay_dataset_paths(
            data_path,
            symbol,
            selected_csv_source=self._selected_replay_csv_source,
            year_hint_override=(int(range_year_hint) if int(range_year_hint) > 0 else None),
        )
        self._replay_data_path = str(data_arg)
        self._replay_dataset_prefix = str(prefix or self._replay_dataset_prefix)
        self._replay_dataset_year = int(y or 0)
        self.replay_data.setText(self._replay_data_path)
        if (not os.path.isdir(dir_5m)) or (not os.path.isdir(dir_1h)):
            msg = build_missing_dataset_message(
                context="GUI_REPLAY",
                tf="5m/1h",
                searched_dir=f"{dir_5m} | {dir_1h}",
                searched_glob=f"{glob_5m} | {glob_1h}",
                dataset_root=data_arg,
                prefix=prefix,
                year=(int(y) if int(y) > 0 else None),
                tf_dirs=("5m", "1h"),
            )
            QMessageBox.warning(
                self,
                "Replay folder missing",
                msg,
            )
            return

        if resolve_reason == "from_range_year" and int(prev_dataset_year) != int(y or 0):
            self._append(f"[replay] dataset year aligned to range: {prev_dataset_year} -> {int(y or 0)}\n")
        self._append(
            f"[replay] resolved dirs: 5m_dir={dir_5m} 1h_dir={dir_1h} "
            f"(reason={resolve_reason} year={int(y) if int(y) > 0 else 'auto'})\n"
        )
        cnt_5m = self._count_csv_matches(dir_5m)
        cnt_1h = self._count_csv_matches(dir_1h)
        cnt_total = int(cnt_5m) + int(cnt_1h)
        self._append(
            f"[replay] data check: 5m_dir={dir_5m} count={cnt_5m} "
            f"1h_dir={dir_1h} count={cnt_1h} total={cnt_total}\n"
        )
        if cnt_total <= 0:
            QMessageBox.warning(
                self,
                "Replay data empty",
                "Replayデータが0件です。フォルダ指定にして、CSVが存在する場所を選んでください",
            )
            return

        spec = ReplaySpec(
            preset=str(self.preset.currentText() or _default_preset()),
            replay_data_path=data_arg,
            symbol=str(symbol),
            timeframe=str(self.replay_tf.currentText() or "5m"),
            log_level=self._selected_log_level(),
            since_ms=int(since_ms),
            until_ms=int(until_ms),
            replay_engine="live",
            replay_csv_dir_5m=str(dir_5m),
            replay_csv_dir_1h=str(dir_1h),
            replay_csv_glob_5m=str(glob_5m),
            replay_csv_glob_1h=str(glob_1h),
            replay_dataset_root=str(data_arg),
            replay_dataset_prefix=str(prefix),
            replay_dataset_year=int(y or 0),
        )
        if int(spec.until_ms) <= int(spec.since_ms):
            QMessageBox.warning(self, "Replay range invalid", "Until month must be greater than or equal to since month.")
            return

        try:
            self._last_replay_report_path = ""
            self._last_replay_trade_log_path = ""
            self._clear_manual_stop_state()
            self._settings.dataset_root = str(data_arg)
            self._settings.dataset_prefix = str(prefix)
            self._settings.dataset_year = int(y or 0)
            self._settings.log_level = self._selected_log_level()
            save_settings(self._settings)
            self._save_run_mode_to_settings()
            self._proc_role = "replay"
            self._close_live_log_file()
            self._open_replay_log_file()
            self._append(
                f"[ui] Starting replay preset={spec.preset} symbol={spec.symbol} tf={spec.timeframe} "
                f"log_level={spec.log_level} "
                f"since={self.replay_since_ym.text().strip()} until={self.replay_until_ym.text().strip()} "
                f"since_ms={spec.since_ms} until_ms={spec.until_ms} data={data_arg} "
                f"dir_5m={dir_5m} dir_1h={dir_1h} dataset_year={spec.replay_dataset_year} "
                "report=from_replay_output\n"
            )
            self._proc = launch_replay(spec)
            self._set_proc_poll_activity(active=self._proc is not None)
            start_log_threads(
                stdout=self._proc.stdout,
                stderr=self._proc.stderr,
                emit=self.sig_log.emit,
            )
            self.btn_start.setEnabled(False)
            self.btn_start.setChecked(True)
            self.btn_run_replay.setEnabled(False)
            self.btn_stop.setEnabled(True)
            self.btn_pipeline.setEnabled(False)
        except Exception as e:
            QMessageBox.critical(self, self.tr("dialog.replay_start_failed.title"), str(e))
            self._proc = None
            self._proc_role = "runner"
            self._clear_manual_stop_state()
            self._close_replay_log_file()
            self._close_live_log_file()
            self.btn_pipeline.setEnabled(True)

    def on_run_replay(self) -> None:
        if self._proc is not None and self._proc.poll() is None:
            QMessageBox.information(self, "Already running", "Bot is already running.")
            return
        prepared = self._prepare_offline_run_inputs("Replay")
        if prepared is None:
            return

        spec = ReplaySpec(
            preset=str(self.preset.currentText() or _default_preset()),
            replay_data_path=str(prepared["data_arg"]),
            symbol=str(prepared["symbol"]),
            timeframe=str(prepared["timeframe"]),
            log_level=self._selected_log_level(),
            since_ms=int(prepared["since_ms"]),
            until_ms=int(prepared["until_ms"]),
            replay_engine="live",
            replay_csv_dir_5m=str(prepared["dir_5m"]),
            replay_csv_dir_1h=str(prepared["dir_1h"]),
            replay_csv_glob_5m=str(prepared["glob_5m"]),
            replay_csv_glob_1h=str(prepared["glob_1h"]),
            replay_dataset_root=str(prepared["data_arg"]),
            replay_dataset_prefix=str(prepared["prefix"]),
            replay_dataset_year=int(prepared["dataset_year"]),
        )
        if int(spec.until_ms) <= int(spec.since_ms):
            QMessageBox.warning(self, "Replay range invalid", "Until month must be greater than or equal to since month.")
            return

        try:
            self._last_replay_report_path = ""
            self._last_replay_trade_log_path = ""
            self._clear_manual_stop_state()
            self._settings.dataset_root = str(prepared["data_arg"])
            self._settings.dataset_prefix = str(prepared["prefix"])
            self._settings.dataset_year = int(prepared["dataset_year"])
            self._settings.log_level = self._selected_log_level()
            save_settings(self._settings)
            self._save_run_mode_to_settings()
            self._proc_role = "replay"
            self._close_live_log_file()
            self._open_replay_log_file(prefix="replay")
            self._append(
                f"[ui] Starting replay preset={spec.preset} symbol={spec.symbol} tf={spec.timeframe} "
                f"log_level={spec.log_level} "
                f"since={self.replay_since_ym.text().strip()} until={self.replay_until_ym.text().strip()} "
                f"since_ms={spec.since_ms} until_ms={spec.until_ms} data={prepared['data_arg']} "
                f"dir_5m={prepared['dir_5m']} dir_1h={prepared['dir_1h']} dataset_year={spec.replay_dataset_year} "
                "report=from_replay_output\n"
            )
            self._proc = launch_replay(spec)
            self._set_proc_poll_activity(active=self._proc is not None)
            start_log_threads(
                stdout=self._proc.stdout,
                stderr=self._proc.stderr,
                emit=self.sig_log.emit,
            )
            self.btn_start.setEnabled(False)
            self.btn_start.setChecked(True)
            self.btn_run_replay.setEnabled(False)
            self.btn_stop.setEnabled(True)
            self.btn_pipeline.setEnabled(False)
        except Exception as e:
            QMessageBox.critical(self, self.tr("dialog.replay_start_failed.title"), str(e))
            self._proc = None
            self._proc_role = "runner"
            self._clear_manual_stop_state()
            self._close_replay_log_file()
            self._close_live_log_file()
            self.btn_pipeline.setEnabled(True)

    def on_run_backtest(self) -> None:
        if self._proc is not None and self._proc.poll() is None:
            QMessageBox.information(self, "Already running", "Bot is already running.")
            return
        prepared = self._prepare_offline_run_inputs("Backtest")
        if prepared is None:
            return
        if not self._validate_backtest_launch_inputs(prepared):
            return

        spec = BacktestSpec(
            preset=str(self.preset.currentText() or _default_preset()),
            symbol=str(prepared["symbol"]),
            entry_timeframe=str(prepared["timeframe"]),
            since_ymd=str(prepared["since_ymd"]),
            until_ymd=str(prepared["until_ymd"]),
            backtest_since_year=(int(prepared["since_year"]) if int(prepared.get("since_year", 0) or 0) > 0 else None),
            backtest_until_year=(int(prepared["until_year"]) if int(prepared.get("until_year", 0) or 0) > 0 else None),
            report_enabled=bool(self.report_enabled.isChecked()),
            report_out=self._report_out_value(),
            backtest_csv_dir_5m=str(prepared["dir_5m"]),
            backtest_csv_dir_1h=str(prepared["dir_1h"]),
            backtest_csv_glob_5m=str(prepared["glob_5m"]),
            backtest_csv_glob_1h=str(prepared["glob_1h"]),
            backtest_dataset_root=str(prepared["data_arg"]),
            backtest_dataset_prefix=str(prepared["prefix"]),
            backtest_dataset_year=int(prepared["dataset_year"]),
        )

        try:
            self._ensure_report_parent()
            self._clear_manual_stop_state()
            self._settings.dataset_root = str(prepared["data_arg"])
            self._settings.dataset_prefix = str(prepared["prefix"])
            self._settings.dataset_year = int(prepared["dataset_year"])
            self._settings.log_level = self._selected_log_level()
            save_settings(self._settings)
            self._save_run_mode_to_settings()
            self._proc_role = "backtest"
            self._close_live_log_file()
            self._open_replay_log_file(prefix="backtest")
            continuous_years = str(prepared.get("continuous_years", "") or "").strip()
            launch_dataset_text = (
                f"prefix={prepared['prefix']} continuous_years={continuous_years} "
                if continuous_years
                else f"dir_5m={prepared['dir_5m']} dir_1h={prepared['dir_1h']} dataset_year={spec.backtest_dataset_year} "
            )
            self._append(
                f"[ui] Starting backtest preset={spec.preset} symbol={spec.symbol} tf={spec.entry_timeframe} "
                f"since={self.replay_since_ym.text().strip()} until={self.replay_until_ym.text().strip()} "
                f"since_ymd={spec.since_ymd} until_ymd={spec.until_ymd} data={prepared['data_arg']} "
                + launch_dataset_text
                +
                f"report={spec.report_enabled}\n"
            )
            self._proc = launch_backtest(spec)
            self._set_proc_poll_activity(active=self._proc is not None)
            start_log_threads(
                stdout=self._proc.stdout,
                stderr=self._proc.stderr,
                emit=self.sig_log.emit,
            )
            self.btn_start.setEnabled(False)
            self.btn_start.setChecked(True)
            self.btn_run_replay.setEnabled(False)
            self.btn_stop.setEnabled(True)
            self.btn_pipeline.setEnabled(False)
        except Exception as e:
            QMessageBox.critical(self, self.tr("dialog.backtest_start_failed.title"), str(e))
            self._proc = None
            self._proc_role = "runner"
            self._clear_manual_stop_state()
            self._close_replay_log_file()
            self._close_live_log_file()
            self.btn_pipeline.setEnabled(True)

    def _save_typed_creds(self) -> bool:
        saved = False
        for item in EXCHANGES:
            widgets = self._credential_widgets(item.id)
            key_edit = widgets.get("key")
            secret_edit = widgets.get("secret")
            passphrase_edit = widgets.get("passphrase")
            key_text = str((key_edit.text() if key_edit is not None else "") or "").strip()
            secret_text = str((secret_edit.text() if secret_edit is not None else "") or "").strip()
            passphrase_text = str((passphrase_edit.text() if passphrase_edit is not None else "") or "").strip()
            key_plain = _is_plain_secret_text(key_text)
            secret_plain = _is_plain_secret_text(secret_text)
            passphrase_plain = _is_plain_secret_text(passphrase_text)
            passphrase_ok = (not item.passphrase_env) or passphrase_plain
            if key_plain and secret_plain and passphrase_ok:
                save_creds(key_text, secret_text, str(item.id), passphrase_text if item.passphrase_env else "")
                self._refresh_credential_widgets_from_keyring(str(item.id))
                saved = True
            elif (
                str(item.id).lower() == "okx"
                and bool(item.passphrase_env)
                and passphrase_plain
                and bool(key_text)
                and ("*" in key_text)
                and bool(secret_text)
                and ("*" in secret_text)
            ):
                creds = load_creds(str(item.id))
                if creds is not None and creds.api_key and creds.api_secret:
                    save_creds(str(creds.api_key or ""), str(creds.api_secret or ""), str(item.id), passphrase_text)
                    self._refresh_credential_widgets_from_keyring(str(item.id))
                    saved = True
        return saved

    def on_save(self) -> None:
        # Save non-secret settings
        self._settings.preset = self.preset.currentText()
        self._settings.symbol = self._selected_symbol() or "BTC/JPY"
        self._settings.report_enabled = bool(self.report_enabled.isChecked())
        self._settings.report_out = self._report_out_value()
        self._settings.exchange_id = self._selected_exchange_id()
        self._settings.dataset_root = str(self.replay_data.text() or "").strip() or self._default_dataset_root
        self._settings.dataset_prefix = str(self._replay_dataset_prefix or "")
        self._settings.dataset_year = int(self._replay_dataset_year or 0)
        self._settings.log_level = self._selected_log_level()
        self._settings.ui_language = self._ui_language
        self._sync_valuation_settings_from_inputs()
        self._update_chart_ui_state()
        save_settings(self._settings)
        self._save_run_mode_to_settings()

        if self._save_typed_creds():
            self._append("[ui] Saved settings + API keys (keyring).\n")
        else:
            self._append("[ui] Saved settings.\n")

    def on_clear_keys(self) -> None:
        ex = self._selected_exchange_id()
        clear_creds(ex)
        widgets = self._credential_widgets(ex)
        if widgets.get("key") is not None:
            widgets["key"].setText("")
        if widgets.get("secret") is not None:
            widgets["secret"].setText("")
        if widgets.get("passphrase") is not None:
            widgets["passphrase"].setText("")
        self._append(f"[ui] Cleared {ex} API keys from keyring.\n")

    def _get_spec(self) -> Optional[LaunchSpec]:
        preset = self.preset.currentText().strip() or _default_preset()
        symbol = self._selected_symbol() or "BTC/JPY"
        exchange_id = self._selected_exchange_id()
        option = self._selected_exchange_option()
        api_key, api_secret, api_passphrase = self._resolve_runtime_creds(exchange_id)
        if not api_key or not api_secret or (bool(option.passphrase_env) and not api_passphrase):
            QMessageBox.warning(
                self,
                self.tr("dialog.missing_api_keys.title"),
                self.tr("dialog.missing_api_credentials.message", exchange=option.label),
            )
            return None

        return LaunchSpec(
            preset=preset,
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
            exchange_id=exchange_id,
            symbol=symbol,
            log_level=self._selected_log_level(),
            report_enabled=bool(self.report_enabled.isChecked()),
            report_out=self._report_out_value(),
        )

    def on_start(self) -> None:
        if self._proc is not None and self._proc.poll() is None:
            QMessageBox.information(self, "Already running", "Bot is already running.")
            return

        run_mode = self._selected_run_mode()
        if run_mode == "REPLAY":
            self._append("[ui] Run Mode=REPLAY -> launch replay.\n")
            self.on_run_replay()
            return
        if run_mode == "BACKTEST":
            self._append("[ui] Run Mode=BACKTEST -> launch backtest.\n")
            self.on_run_backtest()
            return

        spec = self._get_spec()
        if spec is None:
            return

        try:
            self._ip_whitelist_alerted = False
            self._ensure_report_parent()
            self._clear_manual_stop_state()
            if str(run_mode).upper() == "LIVE":
                try:
                    ensure_live_license_or_raise(base_url=self._license_base_url(), feature_name="LIVE execution")
                except SystemExit as exc:
                    self._refresh_license_widgets()
                    msg = str(exc) or "LIVE execution requires desktop activation."
                    self._append(f"[ui] LIVE blocked: {msg}\n")
                    QMessageBox.warning(self, "LIVE blocked", msg)
                    self.btn_start.setChecked(False)
                    return
                except Exception as exc:
                    self._refresh_license_widgets()
                    msg = str(exc) or "LIVE execution requires desktop activation."
                    self._append(f"[ui] LIVE blocked: {msg}\n")
                    QMessageBox.warning(self, "LIVE blocked", msg)
                    self.btn_start.setChecked(False)
                    return
                self._refresh_license_widgets()
            os.environ["LWF_MODE_OVERRIDE"] = str(run_mode)
            self._append(
                f"[ui] Starting runner exchange_id={spec.exchange_id} "
                f"run_mode={run_mode} log_level={spec.log_level} "
                f"preset={spec.preset} symbol={spec.symbol} report={spec.report_enabled}\n"
            )
            self._chart_mode_auto_live_candle_applied = False
            self._reset_live_chart_poll_tracking()
            self._refresh_result_panel()
            self._poll_live_chart_state()
            self._proc_role = "runner"
            self._close_replay_log_file()
            self._open_live_log_file()
            self._proc = launch_runner(spec)
            self._set_proc_poll_activity(active=self._proc is not None)
            start_log_threads(
                stdout=self._proc.stdout,
                stderr=self._proc.stderr,
                emit=self.sig_log.emit,
            )
            self.btn_start.setEnabled(False)
            self.btn_start.setChecked(True)
            self.btn_run_replay.setEnabled(False)
            self.btn_stop.setEnabled(True)
            self.btn_pipeline.setEnabled(False)
        except Exception as e:
            QMessageBox.critical(self, self.tr("dialog.start_failed.title"), str(e))
            self._proc = None
            self._clear_manual_stop_state()
            self._close_live_log_file()
            self.btn_pipeline.setEnabled(True)

    def _stop_process_worker(self, proc: subprocess.Popen) -> None:
        try:
            fallback_used = bool(terminate_process(proc))
            if fallback_used:
                self.sig_log.emit("[UI] fallback terminate/kill\n")
        except Exception as e:
            self.sig_log.emit(f"[UI] stop request failed: {e}\n")
        finally:
            self._stop_request_in_flight = False

    def on_stop(self) -> None:
        if self._proc is None:
            return
        if self._stop_request_in_flight:
            self._append("[UI] stop already requested; waiting for process exit\n")
            return
        self._manual_stop_requested = True
        self._manual_stop_role = str(self._proc_role or "")
        self._manual_stop_requested_at = time.time()
        self._stop_request_in_flight = True
        self._append("[UI] graceful stop requested\n")
        if str(self._proc_role) == "pipeline":
            self._append("[ui] Stopping pipeline...\n")
        elif str(self._proc_role) == "replay":
            self._append("[ui] Stopping replay...\n")
        elif str(self._proc_role) == "backtest":
            self._append("[ui] Stopping backtest...\n")
        else:
            self._append("[ui] Stopping runner...\n")
        threading.Thread(target=self._stop_process_worker, args=(self._proc,), daemon=True).start()

    def _poll_proc(self) -> None:
        self._gui_perf_counters["proc_poll_calls"] += 1
        if self._proc is None:
            self._gui_perf_counters["proc_poll_idle_skips"] += 1
            self._set_proc_poll_activity(False)
            return
        self._set_proc_poll_activity(True)
        code = self._proc.poll()
        if code is None:
            return
        role = str(self._proc_role)
        stopped_by_user = self._is_manual_stop_exit(role)
        if role == "pipeline":
            if stopped_by_user:
                self._append("[UI] pipeline stopped by user\n")
            elif int(code) == 0:
                self._append("[UI] pipeline finished\n")
            else:
                self._append(f"[UI] pipeline error code={code}\n")
        elif role in {"replay", "backtest"}:
            self._close_replay_log_file()
            if stopped_by_user:
                self._append(f"[ui] {role.capitalize()} stopped by user\n")
            else:
                self._append(f"[ui] {role.capitalize()} finished code={code}\n")
            if (not stopped_by_user) and int(code) == 0 and role == "replay":
                self._append_replay_results_summary()
            else:
                if (not stopped_by_user) and int(code) != 0:
                    stderr_path = self._save_last_stderr_tail()
                    self._append(f"[ui] last_stderr_saved={stderr_path}\n")
            self._refresh_result_panel()
        else:
            self._close_live_log_file()
            if stopped_by_user:
                self._append("[ui] Runner stopped by user\n")
            else:
                self._append(f"[ui] Runner exited with code={code}\n")
        self._proc = None
        self._set_proc_poll_activity(False)
        self._proc_role = "runner"
        self._clear_manual_stop_state()
        self.btn_start.setEnabled(True)
        self.btn_start.setChecked(False)
        self.btn_run_replay.setEnabled(True)
        self.btn_stop.setEnabled(False)
        self.btn_pipeline.setEnabled(True)

    def gui_perf_counters(self) -> dict[str, object]:
        counters = dict(self._gui_perf_counters)
        counters["proc_poll_interval_ms"] = int(self._timer.interval()) if hasattr(self, "_timer") else 0
        counters["live_chart_poll_interval_ms"] = int(self._live_chart_timer.interval()) if hasattr(self, "_live_chart_timer") else 0
        counters["result_panel_chart"] = self.result_panel.chart_widget.diagnostic_counters() if hasattr(self, "result_panel") else {}
        return counters
