# BUILD_ID: 2026-03-29_free_from_standard_nonlive_build_v1
# BUILD_ID: 2026-03-27_chart_dialog_snapshot_folder_align_v1
# BUILD_ID: 2026-03-27_chart_dialog_candle_mode_v2_0
# BUILD_ID: 2026-03-27_chart_dialog_combo_popup_dark_v1_2_1
# BUILD_ID: 2026-03-27_chart_dialog_result_expand_v1_1
# BUILD_ID: 2026-03-27_chart_dialog_result_expand_v1
from __future__ import annotations

from PySide6.QtCore import Signal
from PySide6.QtWidgets import QComboBox, QDialog, QHBoxLayout, QPushButton, QVBoxLayout

from app.gui.result_chart import (
    CHART_MODE_EQUITY,
    CHART_MODES,
    ResultChartData,
    ResultChartWidget,
    _apply_dark_combo_popup,
    normalize_chart_mode,
)


BUILD_ID = "2026-03-27_chart_dialog_snapshot_folder_align_v1"


class ChartDialog(QDialog):
    saveRequested = Signal()
    openFolderRequested = Signal()

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.resize(1120, 760)
        self.setModal(False)

        layout = QVBoxLayout()
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(8)
        self.setLayout(layout)

        toolbar = QHBoxLayout()
        toolbar.setSpacing(6)
        self.mode_combo = QComboBox()
        self.mode_combo.addItems(list(CHART_MODES))
        self.mode_combo.setCurrentText(CHART_MODE_EQUITY)
        _apply_dark_combo_popup(self.mode_combo)
        self.btn_fit_all = QPushButton("Fit All")
        self.btn_reset_view = QPushButton("Reset View")
        self.btn_save = QPushButton("Save PNG")
        self.btn_open_folder = QPushButton("Open Folder")
        self.btn_close = QPushButton("Close")
        toolbar.addWidget(self.mode_combo, 1)
        toolbar.addWidget(self.btn_fit_all)
        toolbar.addWidget(self.btn_reset_view)
        toolbar.addWidget(self.btn_save)
        toolbar.addWidget(self.btn_open_folder)
        toolbar.addWidget(self.btn_close)
        layout.addLayout(toolbar)

        self.chart_widget = ResultChartWidget()
        self.chart_widget.setMinimumHeight(520)
        layout.addWidget(self.chart_widget, 1)

        self.mode_combo.currentTextChanged.connect(self._on_mode_changed)
        self.btn_fit_all.clicked.connect(self.chart_widget.fit_all)
        self.btn_reset_view.clicked.connect(self.chart_widget.reset_view)
        self.btn_save.clicked.connect(self.saveRequested.emit)
        self.btn_open_folder.clicked.connect(self.openFolderRequested.emit)
        self.btn_close.clicked.connect(self.close)
        self._update_window_title(self.mode_combo.currentText())

    def set_result_data(self, data: ResultChartData) -> None:
        self.chart_widget.set_result_data(data if isinstance(data, ResultChartData) else ResultChartData())

    def chart_mode(self) -> str:
        return normalize_chart_mode(self.mode_combo.currentText())

    def set_chart_mode(self, mode: str) -> None:
        target = normalize_chart_mode(mode)
        self.mode_combo.setCurrentText(target)
        self._update_window_title(target)

    def _on_mode_changed(self, mode: str) -> None:
        current = normalize_chart_mode(mode)
        self.chart_widget.set_chart_mode(current)
        self._update_window_title(current)

    def _update_window_title(self, mode: str) -> None:
        self.setWindowTitle(f"Result Chart - {normalize_chart_mode(mode)}")
