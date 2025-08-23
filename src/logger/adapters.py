import json
import logging

from src.consts import FORM_TEXT, FORM_STRUCT


class TreeLoggerAdapter(logging.LoggerAdapter):
    """
    Logger adapter that expands keyword arguments into `FORM_STRUCT` and `FORM_TEXT` extras.

    This adapter allows you to call logging methods with arbitrary keyword arguments, which are automatically
    formatted into:
      - `FORM_TEXT`: a human-readable string of "key=value" pairs for console output.
      - `FORM_STRUCT`: a JSON-like string of key-value pairs for structured file output.
    """
    _STANDARD_KEYWORDS = {"exc_info", "stack_info", "stacklevel", "extra"}
    _RESERVED_KEYWORDS = {"timestamp", "logger", "level", "event"}

    def process(self, msg, kwargs) -> tuple[str, dict]:
        """
        Process a logging call by splitting standard vs. extra keyword arguments.

        Args:
            msg (str): The log message.
            kwargs (dict): Arbitrary keyword arguments passed to the log call.

        Returns:
            tuple[str, dict]: Processed message and updated kwargs.
        """
        std_keys_present = set(kwargs) & TreeLoggerAdapter._STANDARD_KEYWORDS
        std_kwargs = {k: kwargs.pop(k) for k in std_keys_present}
        reserved = TreeLoggerAdapter._RESERVED_KEYWORDS | {FORM_TEXT, FORM_STRUCT}
        conflicts = reserved & kwargs.keys()
        if conflicts:
            self.logger.error("KeyError", reason=f"Used conflicting {sorted(conflicts)} keys in log", action="Ignored")
            for key in conflicts: kwargs.pop(key, None)

        extra = std_kwargs.pop("extra", {}) or {}
        extra.setdefault(FORM_TEXT, "")
        extra.setdefault(FORM_STRUCT, "")

        if kwargs:
            extra[FORM_TEXT] = ", ".join(f"{k}={v}" for k, v in kwargs.items())
            extra[FORM_STRUCT] = json.dumps(kwargs)[1:-1]  # Ditch the outer {}.  Formatter will re-add them

        std_kwargs["extra"] = extra
        return msg, std_kwargs
