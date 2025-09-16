import datetime
import json
import logging
import pathlib
from enum import Enum
from typing import Any

import numpy
from numpy import int64

from src.consts import FORM_TEXT, FORM_STRUCT


class TreeLoggerAdapter(logging.LoggerAdapter):
    """
    Logger adapter that expands keyword arguments into structured and text extras.

    This adapter lets you call logging methods with arbitrary keyword arguments, which are automatically converted
    into two forms for output:
      - `FORM_TEXT`: a plain "key=value" string for human-readable console logs.
      - `FORM_STRUCT`: a JSON-compatible string for structured file logs.

    Behavior:
      - Standard logging keywords (exc_info, stack_info, stacklevel, extra) are preserved and passed through unchanged.
      - Reserved keys (timestamp, logger, level, event, FORM_TEXT, FORM_STRUCT) are disallowed in kwargs. If used,
        they are dropped and an error is logged.
      - All other kwargs are JSON-sanitized to ensure safe serialization.
    """
    _STANDARD_KEYWORDS = {"exc_info", "stack_info", "stacklevel", "extra"}
    _RESERVED_KEYWORDS = {"timestamp", "logger", "level", "event"}

    def process(self, msg, kwargs) -> tuple[str, dict]:
        """
        Prepare a logging call by separating standard logging kwargs from structured extras.

        Args:
            msg (str): The log message.
            kwargs (dict): Arbitrary keyword arguments passed to the log call.

        Returns:
            tuple[str, dict]: The unchanged message and a modified kwargs dict containing `extra` with
            structured fields.
        """
        std_keys_present = set(kwargs) & TreeLoggerAdapter._STANDARD_KEYWORDS
        std_kwargs = {k: kwargs.pop(k) for k in std_keys_present}
        reserved = TreeLoggerAdapter._RESERVED_KEYWORDS | {FORM_TEXT, FORM_STRUCT}
        conflicts = reserved & kwargs.keys()
        if conflicts:
            self.logger.error("KeyError", reason=f"Used conflicting {sorted(conflicts)} keys in log", action="Ignored")
            for key in conflicts: kwargs.pop(key, None)

        extras_clean = {k: _to_json_safe(v) for k, v in kwargs.items()}
        extra = std_kwargs.pop("extra", {}) or {}
        extra.setdefault(FORM_TEXT, "")
        extra.setdefault(FORM_STRUCT, "")

        if kwargs:
            extra[FORM_TEXT] = ", ".join(f"{k}={v}" for k, v in kwargs.items())
            extra[FORM_STRUCT] = json.dumps(extras_clean)[1:-1]  # Ditch the outer {}.  Formatter will re-add them

        std_kwargs["extra"] = extra
        return msg, std_kwargs


def _to_json_safe(obj) -> Any:
    """
    Convert common non–JSON-serializable objects into safe representations. This helper ensures values can be safely
    encoded into JSON by normalizing certain Python or library-specific types.

    Args:
        obj (Any): Object to convert.

    Returns:
        Any: A JSON-serializable equivalent if recognized, otherwise the original object unchanged.
    """
    if isinstance(obj, datetime.date): return str(obj)
    if isinstance(obj, pathlib.Path): return str(obj)
    if isinstance(obj, int64): return int(obj)
    if isinstance(obj, Enum): return str(obj.value)
    if isinstance(obj, numpy.bool_): return bool(obj)
    return obj
