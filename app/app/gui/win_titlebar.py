# BUILD_ID: 2026-03-10_gui_layout_dark_titlebar_v1
from __future__ import annotations

import ctypes
import logging
import sys


BUILD_ID = "2026-03-10_gui_layout_dark_titlebar_v1"

logger = logging.getLogger(__name__)

DWMWA_BORDER_COLOR = 34
DWMWA_CAPTION_COLOR = 35
DWMWA_TEXT_COLOR = 36


def _set_window_attribute(dwmapi, hwnd: int, attr: int, value) -> bool:
    try:
        res = dwmapi.DwmSetWindowAttribute(
            ctypes.c_void_p(int(hwnd)),
            ctypes.c_uint(int(attr)),
            ctypes.byref(value),
            ctypes.sizeof(value),
        )
        return int(res) == 0
    except Exception:
        return False


def apply_dark_titlebar(widget_or_window) -> bool:
    if sys.platform != "win32":
        return False
    try:
        hwnd = int(widget_or_window.winId())
    except Exception:
        return False
    if hwnd <= 0:
        return False
    try:
        dwmapi = ctypes.windll.dwmapi
    except Exception:
        return False

    try:
        dark_value = ctypes.c_int(1)
        applied = False
        for attr in (20, 19):
            applied = _set_window_attribute(dwmapi, hwnd, attr, dark_value) or applied
        applied = _set_window_attribute(dwmapi, hwnd, DWMWA_CAPTION_COLOR, ctypes.c_uint(0x000000)) or applied
        applied = _set_window_attribute(dwmapi, hwnd, DWMWA_TEXT_COLOR, ctypes.c_uint(0xFFFFFF)) or applied
        applied = _set_window_attribute(dwmapi, hwnd, DWMWA_BORDER_COLOR, ctypes.c_uint(0x000000)) or applied
        return applied
    except Exception:
        logger.debug("Failed to apply dark titlebar", exc_info=True)
        return False
