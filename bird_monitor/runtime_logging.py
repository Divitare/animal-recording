from __future__ import annotations

import logging
import threading
from collections import deque
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

from flask import Flask


class RecentLogBuffer:
    def __init__(self, max_entries: int = 300) -> None:
        self._entries: deque[dict[str, str]] = deque(maxlen=max_entries)
        self._lock = threading.Lock()

    def append(self, record: logging.LogRecord) -> None:
        if record.exc_info:
            formatter = logging.Formatter()
            exception_text = formatter.formatException(record.exc_info).splitlines()[-1]
            message = f"{record.getMessage()} | {exception_text}"
        else:
            message = record.getMessage()
        entry = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "thread": record.threadName,
            "message": message,
        }
        with self._lock:
            self._entries.append(entry)

    def items(self, limit: int = 80) -> list[dict[str, str]]:
        with self._lock:
            if limit <= 0:
                return []
            return list(self._entries)[-limit:]


class RecentLogBufferHandler(logging.Handler):
    def __init__(self, buffer: RecentLogBuffer) -> None:
        super().__init__(level=logging.INFO)
        self._buffer = buffer

    def emit(self, record: logging.LogRecord) -> None:
        self._buffer.append(record)


_birdnet_buffer = RecentLogBuffer()


def get_recent_birdnet_logs(limit: int = 80) -> list[dict[str, str]]:
    return _birdnet_buffer.items(limit=limit)


def get_birdnet_logger() -> logging.Logger:
    return logging.getLogger("bird_monitor.birdnet")


def configure_application_logging(app: Flask) -> None:
    if app.extensions.get("birdnet_logging_configured"):
        return

    log_dir = Path(app.config["LOG_DIR"])
    log_dir.mkdir(parents=True, exist_ok=True)
    birdnet_log_file = log_dir / "birdnet.log"
    application_log_file = log_dir / "bird-monitor.log"
    formatter = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")

    bird_monitor_logger = logging.getLogger("bird_monitor")
    bird_monitor_logger.setLevel(logging.INFO)

    if not any(isinstance(handler, RotatingFileHandler) and getattr(handler, "baseFilename", "") == str(application_log_file) for handler in bird_monitor_logger.handlers):
        app_file_handler = RotatingFileHandler(application_log_file, maxBytes=2_000_000, backupCount=3)
        app_file_handler.setFormatter(formatter)
        app_file_handler.setLevel(logging.INFO)
        bird_monitor_logger.addHandler(app_file_handler)

    birdnet_logger = get_birdnet_logger()
    birdnet_logger.setLevel(logging.INFO)

    if not any(isinstance(handler, RotatingFileHandler) and getattr(handler, "baseFilename", "") == str(birdnet_log_file) for handler in birdnet_logger.handlers):
        birdnet_file_handler = RotatingFileHandler(birdnet_log_file, maxBytes=2_000_000, backupCount=5)
        birdnet_file_handler.setFormatter(formatter)
        birdnet_file_handler.setLevel(logging.INFO)
        birdnet_logger.addHandler(birdnet_file_handler)

    if not any(isinstance(handler, RecentLogBufferHandler) for handler in birdnet_logger.handlers):
        birdnet_logger.addHandler(RecentLogBufferHandler(_birdnet_buffer))

    birdnet_logger.propagate = True
    app.logger.setLevel(logging.INFO)
    for handler in bird_monitor_logger.handlers:
        if handler not in app.logger.handlers:
            app.logger.addHandler(handler)
    app.extensions["birdnet_logging_configured"] = True
    app.config["BIRDNET_LOG_FILE"] = str(birdnet_log_file)
    app.config["APP_LOG_FILE"] = str(application_log_file)
