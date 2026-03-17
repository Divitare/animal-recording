from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


def get_application_logger() -> logging.Logger:
    return logging.getLogger("bird_node")


def get_birdnet_logger() -> logging.Logger:
    return logging.getLogger("bird_node.birdnet")


def configure_logging(log_dir: Path) -> dict[str, str]:
    log_dir.mkdir(parents=True, exist_ok=True)
    app_log_file = log_dir / "bird-node.log"
    birdnet_log_file = log_dir / "birdnet.log"
    formatter = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")

    app_logger = get_application_logger()
    app_logger.setLevel(logging.INFO)
    _ensure_stream_handler(app_logger, formatter)
    _ensure_rotating_file_handler(app_logger, app_log_file, formatter)

    birdnet_logger = get_birdnet_logger()
    birdnet_logger.setLevel(logging.INFO)
    _ensure_rotating_file_handler(birdnet_logger, birdnet_log_file, formatter)
    birdnet_logger.propagate = True

    return {
        "app_log_file": str(app_log_file),
        "birdnet_log_file": str(birdnet_log_file),
    }


def _ensure_stream_handler(logger: logging.Logger, formatter: logging.Formatter) -> None:
    if any(isinstance(handler, logging.StreamHandler) and not isinstance(handler, RotatingFileHandler) for handler in logger.handlers):
        return
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    handler.setLevel(logging.INFO)
    logger.addHandler(handler)


def _ensure_rotating_file_handler(logger: logging.Logger, log_file: Path, formatter: logging.Formatter) -> None:
    if any(isinstance(handler, RotatingFileHandler) and getattr(handler, "baseFilename", "") == str(log_file) for handler in logger.handlers):
        return
    handler = RotatingFileHandler(log_file, maxBytes=2_000_000, backupCount=5)
    handler.setFormatter(formatter)
    handler.setLevel(logging.INFO)
    logger.addHandler(handler)
