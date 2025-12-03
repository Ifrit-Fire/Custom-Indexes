import logging
import sys
from logging.handlers import RotatingFileHandler

from src.consts import PATH_LOGS_ROOT
from src.logger.adapters import TreeLoggerAdapter
from src.logger.formatters import SafeFormatter, ColoredSafeFormatter

_LOG_FILE_PATH = PATH_LOGS_ROOT / "index-builder.log"


def plant(name: str = None) -> TreeLoggerAdapter:
    """
    Retrieve a cached logger adapter or create and cache a new one. On the first call, a logger name must be provided.
    That logger instance is cached for later calls, allowing the function to return the same adapter without
    requiring `name` again.

    Args:
        name (str, optional): Logger name. Required on the first call; ignored on later calls unless a new logger
            is being set.

    Returns:
        TreeLoggerAdapter: Logger adapter bound to the cached logger.
    """
    cached = getattr(plant, "_cached_logger", None)

    if name is None and cached is None:
        raise ValueError("There is no cached logger available, you must pass a name.")

    if name is not None:
        cached = logging.getLogger(name)
        setattr(plant, "_cached_logger", cached)

    return TreeLoggerAdapter(cached, {})


def till():
    """
    Configure the application logger with both console and file handlers.

    This setup establishes two logger outputs:
      - **Console handler**: logs messages at INFO level and above, formatted with color.
      - **File handler**: logs all messages at DEBUG level and above, written to a
        time-rotated log file. The file rotates at midnight and keeps up to 7 backups.
    """

    # Console handler — only INFO and up
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(ColoredSafeFormatter())

    # File handler — DEBUG and up
    file_handler = RotatingFileHandler(filename=_LOG_FILE_PATH, maxBytes=int(2.5 * 1024 * 1024), delay=True,
                                       encoding="utf-8", backupCount=7)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(SafeFormatter())

    # Root logger (collects everything)
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.handlers.clear()
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
