# BUILD_ID: 2026-02-24_lonewolf_fang_gui_v1
from __future__ import annotations

import threading
from typing import Callable, Optional, TextIO


def _pump(stream: Optional[TextIO], emit: Callable[[str], None], prefix: str) -> None:
    if stream is None:
        return
    try:
        for line in stream:
            if not line:
                continue
            # Ensure each emitted line ends with newline for UI
            if not line.endswith("\n"):
                line = line + "\n"
            emit(f"{prefix}{line}")
    except Exception as e:
        emit(f"{prefix}[log_stream_error] {e}\n")


def start_log_threads(
    *,
    stdout: Optional[TextIO],
    stderr: Optional[TextIO],
    emit: Callable[[str], None],
) -> None:
    t1 = threading.Thread(target=_pump, args=(stdout, emit, ""), daemon=True)
    t2 = threading.Thread(target=_pump, args=(stderr, emit, "[stderr] "), daemon=True)
    t1.start()
    t2.start()
