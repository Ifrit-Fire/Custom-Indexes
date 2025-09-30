from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Sequence, Iterator, Tuple, Callable

import pandas as pd

from src.clients.provider import Provider
from src.data.reconciler import Reconciler
from src.data.source import ProviderSource
from src.exceptions import APILimitReachedError
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

    def fetch_ohlcv(self, date: pd.Timestamp) -> Tuple[pd.DataFrame, ProviderSource | None]:
        """
        Fetches OHLCV (Open, High, Low, Close, Volume) data for the specified date from available providers.  If all
        providers are exhausted with no results, an empty DataFrame is returned.


        Args:
            date: The specific date for which OHLCV data needs to be fetched.

        Returns:
            A tuple containing the fetched OHLCV data as a DataFrame and the source provider, or None if no data is available.
        """
        return self._fetch(func=lambda provider: provider.fetch_ohlcv(date))

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

    def fetch_stock_data(self, symbol: str) -> Tuple[pd.DataFrame, ProviderSource | None]:
        """
        Fetch detailed data for a stock symbol using available providers with round-robin selection. If all providers
        are exhausted with no results, an empty DataFrame is returned.

        Args:
            symbol: The stock symbol to query.

        Returns:
            A normalized DataFrame with stock details (empty if no results), and the provider that supplied the result.
        """
        return self._fetch(lambda provider: provider.fetch_stock_details(symbol))

    def fetch_stock_listings(self, except_from: list[ProviderSource] = None) -> dict[ProviderSource, pd.DataFrame]:
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
            df = provider.fetch_stock_listing()
            if df.empty: continue
            frames |= {provider.name: df}

        return frames

    def _fetch(self, func: Callable[[Provider], pd.DataFrame]) -> Tuple[pd.DataFrame, ProviderSource | None]:
        """
        Fetches data using the provided function by iterating through available providers.

        Attempts to retrieve data from a list of providers in sequence. Maintains a reconciler to combine and validate data
        from multiple sources. If an error occurs or a provider is unavailable, it switches to the next provider in the list.
        Returns the reconciled data and its source once the data is ready or returns an empty DataFrame with None if all
        providers are exhausted.

        Recommended and example invocation:
            `self._fetch(lambda provider: provider.fetch_*(my_arg))`

        Args:
            func: A callable that takes a Provider instance and returns a DataFrame.

        Returns:
            Tuple containing the reconciled DataFrame and the name of the provider source, or an empty DataFrame and None
            if all providers are exhausted.
        """
        log = timber.plant()
        reconciler = Reconciler()
        tried: set[ProviderSource] = set()

        while len(tried) < len(self._providers):
            provider = self._next()
            if provider.name in tried:
                provider.mark_unavailable()
                continue
            try:
                df = func(provider)
                tried.add(provider.name)
                reconciler.add(data=df, source=provider.name)
                if reconciler.is_ready:
                    return reconciler.data, reconciler.source
            except APILimitReachedError:
                log.debug("APILimitReachedError", response="switch providers")
                provider.mark_unavailable()
                continue
        # TODO: Add special case provider for checking? When partial data is available
        log.error("Providers Exhausted", reason="data retrieval unsuccessful", response="skipping")
        return pd.DataFrame(), None

    def _iter_cooldowns(self) -> Iterator[datetime]:
        """
        Yield all non-None provider cooldown expiry times.

        Yields:
            The `cooldown_until` values for providers currently marked unavailable.
        """
        for p in self._providers:
            if p.cooldown_until is not None:
                yield p.cooldown_until

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
