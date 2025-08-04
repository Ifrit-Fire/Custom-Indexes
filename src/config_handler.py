from functools import cached_property
from typing import Any

import yaml

from src.consts import PATH_CONFIG

KEY_INDEX_SORTBY = "sort_by"
KEY_INDEX_TOP = "top"
KEY_INDEX_WEIGHT_MIN = "weight_min"

_KEY_BASE_DEFAULT = "default"
_KEY_INDEXES = "indexes"
_KEY_CONSOLIDATE = "symbol_consolidate"
_KEY_VOLUME = "volume"


class ConfigHandler:
    def __init__(self):
        self._raw = self._read_config()
        self._default = self._raw.get(_KEY_BASE_DEFAULT)
        self._indexes = self._raw.get(_KEY_INDEXES)

    @staticmethod
    def _read_config() -> Any:
        with open(PATH_CONFIG, "r") as f:
            return yaml.safe_load(f)

    @cached_property
    def symbol_merge(self) -> dict[str, str]:
        return {item["merge"]: item["into"] for item in self._default[_KEY_CONSOLIDATE]}

    def volume_limit_min(self) -> int:
        return self._default[_KEY_VOLUME]["limit_min"]

    def get_index_config(self, name: str) -> dict[str, Any]:
        return self._indexes[name]

    def get_all_indexes(self) -> list[dict[str, Any]]:
        return self._indexes


config = ConfigHandler()
