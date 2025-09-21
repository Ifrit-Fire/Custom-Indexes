import json
import re
import time
from typing import Iterator

import pandas as pd
import requests
from polygon import RESTClient, BadResponse
from urllib3.exceptions import MaxRetryError

from src.clients.providerpool import Provider
from src.consts import API_POLY_TOKEN, COL_SYMBOL, COL_OUT_SHARES, COL_MIC, COL_TYPE, MIC_CODES, COL_STATE, \
    COL_POSTAL_CODE
from src.data import processing
from src.data.security_types import StockTypes
from src.data.source import ProviderSource
from src.exceptions import APILimitReachedError
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
        symbol: Ticker symbol string to normalize.

    Returns:
        Normalized ticker symbol.
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
        params: Query parameters for the initial API request
            (e.g., market, exchange MIC, type, limit).

    Yields:
        Raw JSON objects for each stock ticker as returned by the API.

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


class PolygonProvider(Provider):
    @property
    def name(self) -> ProviderSource:
        return ProviderSource.POLYGON

    def fetch_stock_listing(self) -> pd.DataFrame:
        """
        Retrieves and normalizes all active stock listings. Focuses strictly on common stock, preferred stock, ADRs,
        REITs from the `XNYS`, `XNAS`, `XASE`, and `BATS` exchanges.

        Returns:
            A DataFrame containing active stock listings with standardized symbol, type, and MIC.
        """
        log = timber.plant()
        log.info("Phase starts", fetch="stock list", endpoint="polygon")

        types = {"CS", "ADRC"}
        tickers = []
        for mic in MIC_CODES:
            for ty in types:
                params = {"market": "stocks", "active": "true", "exchange": mic, "type": ty, "limit": 1000}
                tickers += _iter_all_stock(params)

        df = pd.json_normalize(tickers)
        df.rename(columns={"ticker": COL_SYMBOL, "primary_exchange": COL_MIC}, inplace=True)
        df[COL_SYMBOL] = processing.standardize_symbols(df[COL_SYMBOL])
        df[COL_TYPE] = df[COL_TYPE].replace(_TYPE_TO_STANDARD)

        log.info("Phase ends", fetch="stock list", endpoint="polygon", count=len(df), source="API")
        return df

    def fetch_symbol_data(self, symbol: str) -> pd.DataFrame:
        """
        Retrieves and normalizes detailed ticker information for a given symbol.

        Args:
            symbol: The ticker symbol to query.

        Returns:
            A single-row DataFrame containing standardized ticker information.

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
        except BadResponse:
            log.error("BadResponse", reason="unknown ticker", symbol=symbol, normalized=norm_sym, provider=self.name)
            return pd.DataFrame()

        result = result.data.decode("utf-8")
        result = json.loads(result)["results"]
        df = pd.json_normalize(result)
        df.rename(columns={"ticker": COL_SYMBOL, "share_class_shares_outstanding": COL_OUT_SHARES,
                           "primary_exchange": COL_MIC, "address.state": COL_STATE,
                           "address.postal_code": COL_POSTAL_CODE}, inplace=True)
        df.loc[0, COL_SYMBOL] = symbol
        return df

    def fetch_crypto_market(self) -> pd.DataFrame:
        return pd.DataFrame()
