import logging
import sys
from logging.handlers import TimedRotatingFileHandler

from colorlog import ColoredFormatter

from src.consts import PATH_LOGS_ROOT, FORM_STRUCT, FORM_TEXT

_LOG_FILE_PATH = PATH_LOGS_ROOT / "index-builder.log"


def _setup_logging():
    """
    Configure application logging with both console and file handlers.

    This setup establishes two logging outputs:
      - **Console handler**: logs messages at INFO level and above, formatted with color.
      - **File handler**: logs all messages at DEBUG level and above, written to a
        time-rotated log file. The file rotates at midnight and keeps up to 7 backups.
    """

    # Console handler — only INFO and up
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(ColoredSafeFormatter())

    # File handler — DEBUG and up
    file_handler = TimedRotatingFileHandler(filename=_LOG_FILE_PATH, when="midnight", interval=1, backupCount=7)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(SafeFormatter())

    # Root logger (collects everything)
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.handlers.clear()
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)


class SafeFormatter(logging.Formatter):
    """
    This formatter outputs log records as JSON-like strings with fixed keys: `timestamp`, `source`, `level`, `event`,
    and `FORM_STRUCT`. If a record does not contain `FORM_STRUCT`, they are safely initialized as empty strings to
    prevent formatting errors. This ensures 3P libraries don't raise errors when utilizing logger.

    Example output:
        { "timestamp": "2025-08-20 00:11:00",
          "source": "top10-min10",
          "level": "INFO",
          "event": "Excluded assets",
          "reason": "stablecoin"
          "count": 23 }

    Notes:
        - Uses `str.format` style formatting (`style="{"`).
        - Intended to pair with a custom logger that injects extra fields.
    """

    def __init__(self):
        _fmt = ('{{ "timestamp": "{asctime}", "source": "{name}", '
                '"level": "{levelname}", "event": "{message}", '
                f'{{{FORM_STRUCT}}} }}}}')
        super().__init__(fmt=_fmt, datefmt="%Y-%m-%d %H:%M:%S", style="{")

    def format(self, record):
        if not hasattr(record, FORM_STRUCT):
            setattr(record, FORM_STRUCT, "")
        return super().format(record)


class ColoredSafeFormatter(ColoredFormatter):
    """
    This formatter outputs log records as JSON-like strings. If a record does not contain `FORM_TEXT`, they are safely
    initialized as empty strings to prevent formatting errors. This ensures 3P libraries don't raise errors when
    utilizing logger.

    Example output:
        { "timestamp": "2025-08-20 00:11:00",
          "source": "top10-min10",
          "level": "INFO",
          "event": "Excluded assets",
          "reason": "stablecoin"
          "count": 23 }

    Notes:
        - Uses `str.format` style formatting (`style="{"`).
        - Intended to pair with a custom logger that injects extra fields.
    """

    def __init__(self):
        _fmt = "{light_white}{asctime}{reset} " \
               "{bg_light_blue}{light_white}{name}{reset} " \
               "[{log_color}{levelname:<8}{reset}] " \
               "{bold_black}{message}{reset} " \
               f"{{cyan}}| {{{FORM_TEXT}}} {{reset}}"
        super().__init__(fmt=_fmt, datefmt="%Y-%m-%d %H:%M:%S", style="{",
                         log_colors={"DEBUG": "thin_green", "INFO": "green", "WARNING": "yellow", "ERROR": "red",
                                     "CRITICAL": "bold_red", })

    def format(self, record):
        if not hasattr(record, FORM_TEXT):
            setattr(record, FORM_TEXT, "")
        return super().format(record)


_setup_logging()
