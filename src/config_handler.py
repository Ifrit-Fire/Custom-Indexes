from functools import cached_property
from typing import Any

import yaml
from dateutil.relativedelta import relativedelta

from src.consts import PATH_CONFIG

KEY_INDEX_SORTBY = "sort_by"
KEY_INDEX_TOP = "top"
KEY_INDEX_WEIGHT_MIN = "weight_min"

_KEY_BASE_DEFAULT = "default"
_KEY_CONSOLIDATE = "symbol_consolidate"
_KEY_INDEXES = "indexes"
_KEY_VOLUME = "volume"
_KEY_AGE_MIN = "age_min"
_KEY_AGE_MIN_STOCK = "stock"
_KEY_AGE_MIN_CRYPTO = "crypto"


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

    @property
    def volume_limit_min(self) -> int:
        return self._default[_KEY_VOLUME]["limit_min"]

    def get_all_indexes(self) -> dict[str, dict[str, Any]]:
        return self._indexes

    @cached_property
    def crypto_age_min(self) -> relativedelta:
        rule = self._default[_KEY_AGE_MIN].get(_KEY_AGE_MIN_CRYPTO, {})
        date = relativedelta(years=rule.get("years", 0), months=rule.get("months", 0), days=rule.get("days", 0))
        return date

    @cached_property
    def stock_age_min(self) -> relativedelta:
        rule = self._default[_KEY_AGE_MIN].get(_KEY_AGE_MIN_STOCK, {})
        date = relativedelta(years=rule.get("years", 0), months=rule.get("months", 0), days=rule.get("days", 0))
        return date


config = ConfigHandler()
