import json
import re
import time
from typing import Iterator

import pandas as pd
import requests
from polygon import RESTClient, BadResponse
from urllib3.exceptions import MaxRetryError

from src.clients.providerpool import Provider
from src.consts import API_POLY_TOKEN, COL_SYMBOL, COL_OUT_SHARES, COL_MIC, COL_CIK, COL_FIGI, COL_NAME, COL_TYPE, \
    MIC_CODES, COL_STATE, COL_POSTAL_CODE
from src.data import processing
from src.data.security_types import StockTypes
from src.data.source import ProviderSource
from src.exceptions import APILimitReachedError, NoResultsFoundError
from src.io import cache
from src.logger import timber

_BASE_ALL_TICKERS = "https://api.polygon.io/v3/reference/tickers"
_TYPE_TO_STANDARD = {"CS": StockTypes.COMMON_STOCK.value, "ADRC": StockTypes.ADR.value}


def _fix_dot_p(symbol: str) -> str:
    """
    Convert patterns like 'MS.PQ' to 'MSpQ'.

    Rules:
        - Remove the dot before 'P'
        - Lowercase the 'P'

    Args:
        symbol (str): Ticker symbol string to normalize.

    Returns:
        str: Normalized ticker symbol.
    """
    log = timber.plant()
    norm = re.sub(r'\.P', 'p', symbol)
    if norm != symbol: log.debug("Polygon normalization", symbol=symbol, normalized=norm)
    return norm


def _iter_all_stock(params: dict[str, str]) -> Iterator[dict[str, object]]:
    """
    Generator that streams all stock ticker data from Polygon's `list_tickers` API. If a 429 rate-limit error is
    encountered, waits 60 seconds before retrying.

    Args:
        params (dict[str, str]): Query parameters for the initial API request
            (e.g., market, exchange MIC, type, limit).

    Yields:
        dict: Raw JSON objects for each stock ticker as returned by the API.

    Notes:
        - Retries after hitting rate limits; may block until calls are allowed.
    """
    log = timber.plant()
    url = _BASE_ALL_TICKERS

    with requests.Session() as session:
        while url:
            final_param = {"apiKey": API_POLY_TOKEN}
            if url == _BASE_ALL_TICKERS:
                final_param |= {**params, "apiKey": API_POLY_TOKEN}

            response = session.get(url, params=final_param)
            if response.status_code == 429:
                log.warning("MaxRetryError", reason="exceeded API limit", response="waiting")
                time.sleep(60)
                continue
            response.raise_for_status()
            result = response.json()
            for row in result.get("results", []):
                yield row
            url = result.get("next_url")  # None when done


def get_all_stock() -> pd.DataFrame:
    """
    Retrieve the full list of active U.S. stocks from Polygon, with caching.

    Attempts to load from the local API cache if available and valid; otherwise queries the remote API.
    The results are normalized and cached for future use.

    Returns:
        pd.DataFrame: A DataFrame containing standardized stock data with columns:

            - `COL_CIK`: Central Index Key (CIK) if available.
            - `COL_FIGI`: Financial Instrument Global Identifier (FIGI).
            - `COL_NAME`: Company name.
            - `COL_MIC`: Market Identifier Code (exchange).
            - `COL_SYMBOL`: Standardized ticker symbol.
            - `COL_TYPE`: Security type.
    """
    log = timber.plant()
    log.info("Phase starts", fetch="stock list", endpoint="polygon")
    types = {"CS", "ADRC"}
    df = cache.load_stock_list(provider="polygon", filters=types | MIC_CODES)
    source = "cache"

    if df.empty:
        source = "API"
        tickers = []
        for mic in MIC_CODES:
            for tipe in types:
                params = {"market": "stocks", "active": "true", "exchange": mic, "type": tipe, "limit": 1000}
                tickers += _iter_all_stock(params)

        df = pd.json_normalize(tickers)
        df.rename(columns={"ticker": COL_SYMBOL, "primary_exchange": COL_MIC}, inplace=True)
        df[COL_SYMBOL] = processing.standardize_symbols(df[COL_SYMBOL])
        df[COL_TYPE] = df[COL_TYPE].replace(_TYPE_TO_STANDARD)
        cache.save_stock_list(df=df, provider="polygon", filters=types | MIC_CODES)

    log.info("Phase ends", fetch="stock list", endpoint="polygon", count=len(df), source=source)
    return df[[COL_CIK, COL_FIGI, COL_NAME, COL_MIC, COL_SYMBOL, COL_TYPE]]


class PolygonProvider(Provider):
    @property
    def name(self) -> ProviderSource:
        return ProviderSource.POLYGON

    def fetch(self, symbol: str) -> pd.DataFrame:
        return self._get_ticker_details(symbol)

    def _get_ticker_details(self, symbol: str) -> pd.DataFrame:
        """
        Retrieves and normalizes detailed ticker information for a given symbol.

        Args:
            symbol (str): The ticker symbol to query.

        Returns:
            pd.DataFrame: A single-row DataFrame with normalized ticker data, including standardized
            `COL_SYMBOL`, `COL_OUT_SHARES`, `COL_MIC`, `COL_STATE`, and `COL_POSTAL_CODE`.

        Raises:
            APILimitReachedError: When API rate limits are exceeded.
            NoResultsFoundError: When the symbol returns no results from provider.
        """
        log = timber.plant()
        norm_sym = _fix_dot_p(symbol)
        client = RESTClient(api_key=API_POLY_TOKEN)
        try:
            result = client.get_ticker_details(ticker=norm_sym, raw=True)
        except MaxRetryError:
            log.warning("MaxRetryError", reason="exceeded API limit", provider=self.name)
            raise APILimitReachedError()
        except BadResponse as e:
            log.error("BadResponse", reason="unknown ticker", symbol=symbol, normalized=norm_sym, provider=self.name)
            raise NoResultsFoundError()

        result = result.data.decode("utf-8")
        result = json.loads(result)["results"]
        df = pd.json_normalize(result)
        df.rename(columns={"ticker": COL_SYMBOL, "share_class_shares_outstanding": COL_OUT_SHARES,
                           "primary_exchange": COL_MIC, "address.state": COL_STATE,
                           "address.postal_code": COL_POSTAL_CODE}, inplace=True)
        df.loc[0, COL_SYMBOL] = symbol
        return df
