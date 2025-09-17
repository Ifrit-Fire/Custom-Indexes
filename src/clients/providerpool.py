from __future__ import annotations

import sys
import time
from datetime import datetime, timezone
from typing import Sequence, Iterator, Tuple

import pandas as pd

from src.clients.provider import Provider
from src.data.source import ProviderSource
from src.exceptions import APILimitReachedError, NoResultsFoundError
from src.logger import timber


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

    def fetch_all_stock(self, except_from: list[ProviderSource] = None) -> dict[ProviderSource, pd.DataFrame]:
        """
        Fetches stock lists from all available providers in the pool, excluding any specified.

        Args:
            except_from: Optional. A list of providers to skip during fetch. Providers not in the pool are
                         ignored silently.

        Returns:
            A dictionary mapping each provider to its fetched stock list DataFrame.
            Providers returning empty data are excluded from the result.
            Returns an empty dictionary if nothing is found.
        """
        frames = {}
        providers = [val for val in self._providers if val.name not in except_from]
        for provider in providers:
            df = provider.fetch_all_stock()
            if df.empty: continue
            frames |= {provider.name: df}

        return frames

    def fetch_crypto_market(self) -> dict[ProviderSource, pd.DataFrame]:
        """
        Fetches crypto market data from all available providers in the pool.

        Returns:
            A dictionary mapping each provider to its fetched crypto list DataFrame.
            Providers returning empty data are excluded from the result.
            Returns an empty dictionary if nothing is found.
        """
        frames = {}
        providers = [val for val in self._providers]
        for provider in providers:
            df = provider.fetch_crypto_market()
            if df.empty: continue
            frames |= {provider.name: df}

        return frames

    def fetch_symbol_data(self, symbol: str) -> Tuple[pd.DataFrame, ProviderSource]:
        """
        Fetch detailed data for a symbol using available providers with round-robin selection. If all providers are
        exhausted with no results, an empty DataFrame is returned.

        Args:
            symbol: The ticker symbol to query.

        Returns:
            A normalized DataFrame with symbol details (empty if no results), and the provider that supplied the result.
        """
        log = timber.plant()
        no_results: set[ProviderSource] = set()
        while True:
            p = self._next()
            if p.name in no_results:
                p.mark_unavailable()
                continue
            try:
                return p.fetch_symbol_data(symbol), p.name
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
                    return pd.DataFrame(), p.name
                continue
        log.critical("Reached the unreachable", reason="perplexed", response="quitting")
        sys.exit()

    def _next(self) -> Provider:
        """
        Select the next available provider in round-robin order. If no providers are currently available, will wait and
        retry until one becomes available.

        Returns:
            The next available provider.
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
            The `cooldown_until` values for providers currently marked unavailable.
        """
        for p in self._providers:
            if p.cooldown_until is not None:
                yield p.cooldown_until
