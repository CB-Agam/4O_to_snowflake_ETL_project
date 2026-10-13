from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_LOG_PATH = PROJECT_ROOT / "logs" / "etl_pipeline.log"
_HANDLER_MARKER = "_etl_pipeline_logging_handler"
_LOG_PATH_MARKER = "_etl_pipeline_log_path"


def configure_logging(log_path: Path = DEFAULT_LOG_PATH) -> None:
    resolved_log_path = log_path.resolve()
    resolved_log_path.parent.mkdir(parents=True, exist_ok=True)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    managed_handlers = [
        handler
        for handler in root_logger.handlers
        if getattr(handler, _HANDLER_MARKER, False)
    ]
    if managed_handlers and all(
        getattr(handler, _LOG_PATH_MARKER, None) == resolved_log_path
        for handler in managed_handlers
    ):
        return

    for handler in managed_handlers:
        root_logger.removeHandler(handler)
        handler.close()

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    file_handler = RotatingFileHandler(
        resolved_log_path,
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    console_handler = logging.StreamHandler()
    for handler in (file_handler, console_handler):
        handler.setFormatter(formatter)
        setattr(handler, _HANDLER_MARKER, True)
        setattr(handler, _LOG_PATH_MARKER, resolved_log_path)
        root_logger.addHandler(handler)
