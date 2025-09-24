from abc import ABC, abstractmethod
from datetime import timedelta, datetime, timezone

import pandas as pd

from src.data.source import ProviderSource


class Provider(ABC):
    def __init__(self):
        self.cooldown_seconds = timedelta(seconds=60)
        self.cooldown_until = datetime.now(timezone.utc) - self.cooldown_seconds

    @property
    @abstractmethod
    def name(self) -> ProviderSource:
        pass

    @abstractmethod
    def fetch_stock_listing(self) -> pd.DataFrame:
        pass

    @abstractmethod
    def fetch_crypto_market(self) -> pd.DataFrame:
        pass

    @abstractmethod
    def fetch_symbol_data(self, symbol: str) -> pd.DataFrame:
        pass

    def is_available(self) -> bool:
        now = datetime.now(timezone.utc)
        return now >= self.cooldown_until

    def mark_unavailable(self) -> None:
        self.cooldown_until = datetime.now(timezone.utc) + self.cooldown_seconds
