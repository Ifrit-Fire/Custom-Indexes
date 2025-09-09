from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Sequence, Iterator

import pandas as pd

from src.exceptions import APILimitReachedError, NoResultsFoundError
from src.logger import timber


@dataclass(slots=True)
class Provider(ABC):
    cooldown_seconds: timedelta = timedelta(seconds=60)
    cooldown_until: datetime | None = field(default=None)

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def fetch(self, symbol: str) -> pd.DataFrame:
        pass

    def is_available(self) -> bool:
        now = datetime.now(timezone.utc)
        return (self.cooldown_until is None) or (now >= self.cooldown_until)

    def mark_unavailable(self) -> None:
        self.cooldown_until = datetime.now(timezone.utc) + self.cooldown_seconds


class ProviderPool:
    """
    Manage multiple API providers with automatic failover and cooldowns.

    The pool cycles through a list of Provider objects in round-robin order, selecting the
    next available provider for each request. If a provider exceeds its rate limit, it is
    temporarily marked unavailable and skipped until its cooldown expires. If a provider
    returns no results, the pool tries the remaining providers; if all providers return
    no results, the pool stops and returns an empty DataFrame.

    Args:
        providers (Sequence[Provider]): A sequence of Provider objects.
    """

    def __init__(self, providers: Sequence[Provider]):
        self._providers = list(providers)
        self._index = 0

    def _next(self) -> Provider:
        """
        Select the next available provider in round-robin order. If no providers are currently available, will wait and
        retry until one becomes available.

        Returns:
            Provider: The next available provider.
        """
        while True:
            n = len(self._providers)
            for i in range(n):
                p = self._providers[(self._index + i) % n]
                if p.is_available():
                    self._index = (self._index + i + 1) % n
                    return p
            self._wait()

    def _wait(self):
        """
        Block until the earliest provider cooldown has expired.
        """
        log = timber.plant()
        now = datetime.now(timezone.utc)
        soonest = min(self._iter_cooldowns())
        sleep_for = max(0.0, (soonest - now).total_seconds() + 1)
        log.warning("Providers waiting", reason="limits reached, cooling off", duration=sleep_for, units="seconds")
        time.sleep(sleep_for)

    def _iter_cooldowns(self) -> Iterator[datetime]:
        """
        Yield all non-None provider cooldown expiry times.

        Yields:
            datetime: The `cooldown_until` values for providers currently
            marked unavailable.
        """
        for p in self._providers:
            if p.cooldown_until is not None:
                yield p.cooldown_until

    # noinspection PyTypeChecker
    def fetch_data(self, symbol: str) -> pd.DataFrame:
        log = timber.plant()
        no_results: set[str] = set()
        while True:
            p = self._next()
            if p.name in no_results:
                p.mark_unavailable()
                continue
            try:
                return p.fetch(symbol)
            except APILimitReachedError:
                log.debug("APILimitReachedError", response="switch providers")
                p.mark_unavailable()
                continue
            except NoResultsFoundError:
                # Just continue on, next provider may have the results
                log.debug("NoResultsFoundError", response="switch providers")
                no_results.add(p.name)
                if len(no_results) >= len(self._providers):
                    log.error("Providers Exhausted", reason="NoResultsFoundError", response="skipping", symbol=symbol)
                    return pd.DataFrame()
                continue
