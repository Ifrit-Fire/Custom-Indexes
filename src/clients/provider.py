from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import timedelta, datetime, timezone

import pandas as pd

from src.data.source import ProviderSource


@dataclass(slots=True)
class Provider(ABC):
    cooldown_seconds: timedelta = timedelta(seconds=60)
    cooldown_until: datetime | None = field(default=None)

    @property
    @abstractmethod
    def name(self) -> ProviderSource:
        pass

    @abstractmethod
    def fetch(self, symbol: str) -> pd.DataFrame:
        pass

    def is_available(self) -> bool:
        now = datetime.now(timezone.utc)
        return (self.cooldown_until is None) or (now >= self.cooldown_until)

    def mark_unavailable(self) -> None:
        self.cooldown_until = datetime.now(timezone.utc) + self.cooldown_seconds
