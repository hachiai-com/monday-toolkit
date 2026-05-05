"""Append structured execution logs to logs.txt in this directory after each run."""
from __future__ import annotations

import io
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

_EASTERN_TZ = ZoneInfo("America/New_York")

_LOGS_PATH = Path(__file__).resolve().parent / "logs.txt"


class _TeeStream:
    """Write to the primary stream (console) and capture a copy."""

    __slots__ = ("_primary", "_capture")

    def __init__(self, primary, capture: io.StringIO) -> None:
        self._primary = primary
        self._capture = capture

    def write(self, s: str) -> int:
        if not s:
            return 0
        self._primary.write(s)
        self._capture.write(s)
        return len(s)

    def flush(self) -> None:
        self._primary.flush()
        self._capture.flush()

    def isatty(self) -> bool:
        return self._primary.isatty()

    @property
    def encoding(self) -> str:
        return getattr(self._primary, "encoding", "utf-8") or "utf-8"

    def fileno(self) -> int:
        return self._primary.fileno()


def _patch_root_stream_handlers(
    mapping: list[tuple[object, object]],
) -> list[tuple[logging.Handler, object]]:
    patched: list[tuple[logging.Handler, object]] = []
    for h in logging.root.handlers:
        if not isinstance(h, logging.StreamHandler):
            continue
        stream = getattr(h, "stream", None)
        for old_stream, new_stream in mapping:
            if stream is old_stream:
                h.stream = new_stream  # type: ignore[assignment]
                patched.append((h, old_stream))
                break
    return patched


def _append_log_block(date_s: str, time_est_s: str, exec_time_s: str, log_text: str) -> None:
    block = (
        f'date: "{date_s}"\n'
        f'time: "{time_est_s}"\n'
        f'execution_time: "{exec_time_s}"\n'
        f"logs:\n{json.dumps(log_text)}\n\n-----\n\n"
    )
    with open(_LOGS_PATH, "a", encoding="utf-8") as f:
        f.write(block)


def run_with_execution_log(main_callable) -> None:
    buf = io.StringIO()
    old_out, old_err = sys.stdout, sys.stderr
    tee_out = _TeeStream(old_out, buf)
    tee_err = _TeeStream(old_err, buf)
    patched = _patch_root_stream_handlers([(old_out, tee_out), (old_err, tee_err)])
    start = time.perf_counter()
    try:
        sys.stdout = tee_out
        sys.stderr = tee_err
        main_callable()
    finally:
        sys.stdout = old_out
        sys.stderr = old_err
        for h, orig in patched:
            h.stream = orig  # type: ignore[assignment]
        elapsed = time.perf_counter() - start
        ended = datetime.now()
        date_str = ended.strftime("%Y-%m-%d")
        ended_eastern = datetime.now(_EASTERN_TZ)
        tz_abbr = ended_eastern.tzname() or ""
        time_est_str = f"{ended_eastern.strftime('%H:%M:%S')} {tz_abbr}".strip()
        exec_time_str = f"{elapsed:.3f}s"
        try:
            _append_log_block(date_str, time_est_str, exec_time_str, buf.getvalue())
        except OSError:
            pass
