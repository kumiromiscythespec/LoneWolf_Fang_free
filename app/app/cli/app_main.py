# BUILD_ID: 2026-04-03_free_app_metadata_version_release_prep_v1
# BUILD_ID: 2026-03-25_free_app_metadata_v1
from __future__ import annotations

import argparse
import ctypes
import sys

import config as C
from app.core.gating import require_gui
from app.core.paths import get_paths


BUILD_ID = "2026-04-03_free_app_metadata_version_release_prep_v1"
APP_DISPLAY_NAME = str(getattr(C, "APP_DISPLAY_NAME", "") or "LoneWolf Fang Free").strip() or "LoneWolf Fang Free"
APP_USER_MODEL_ID = "LoneWolfFang.Free"


DARK_STYLESHEET = """
QWidget {
    background-color: #111111;
    color: #eeeeee;
    selection-background-color: #3a3a3a;
    selection-color: #ffffff;
}
QLineEdit, QTextEdit, QPlainTextEdit, QComboBox {
    background-color: #1c1c1c;
    color: #eeeeee;
    border: 1px solid #333333;
    border-radius: 4px;
    padding: 4px;
}
QPushButton {
    background-color: #2a2a2a;
    color: #eeeeee;
    border: 1px solid #3a3a3a;
    border-radius: 4px;
    padding: 6px 10px;
}
QPushButton:hover {
    background-color: #3a3a3a;
}
QPushButton:pressed {
    background-color: #444444;
}
QComboBox QAbstractItemView {
    background-color: #1c1c1c;
    color: #eeeeee;
    selection-background-color: #3a3a3a;
    selection-color: #ffffff;
}
"""


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(add_help=True)
    p.add_argument("--gui", action="store_true", help="Launch LoneWolf Fang Free GUI")
    return p.parse_args(argv)


def _set_windows_app_user_model_id() -> None:
    if sys.platform != "win32":
        return
    try:
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(APP_USER_MODEL_ID)
    except Exception:
        return


def main(argv: list[str] | None = None) -> int:
    _ = _parse_args(argv)
    try:
        require_gui(feature_name=APP_DISPLAY_NAME)
    except SystemExit as e:
        msg = str(e) or "GUI is not available in this build tier."
        sys.stderr.write(msg + "\n")
        return 2

    # Lazy import so that importing this module doesn't require Qt.
    from PySide6.QtWidgets import QApplication

    from app.gui.logo_loader import load_logo_icon
    from app.gui.main_window import MainWindow

    _set_windows_app_user_model_id()
    app = QApplication(sys.argv)
    app.setApplicationName(APP_DISPLAY_NAME)
    app.setApplicationDisplayName(APP_DISPLAY_NAME)
    icon = load_logo_icon(get_paths().repo_root)
    if icon is not None:
        app.setWindowIcon(icon)
    app.setStyleSheet(DARK_STYLESHEET)
    win = MainWindow()
    if icon is not None:
        win.setWindowIcon(icon)
    win.show()
    return int(app.exec())


if __name__ == "__main__":
    raise SystemExit(main())
