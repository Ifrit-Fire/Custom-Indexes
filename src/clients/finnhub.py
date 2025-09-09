import sys
from pathlib import Path

import finnhub
import pandas as pd
from finnhub import FinnhubAPIException

from src import data_processing
from src.clients import cache
from src.clients.providerpool import Provider
from src.consts import API_FINN_TOKEN, COL_SYMBOL, STOCK_TYPES, COL_MC, COL_LIST_DATE, COL_OUT_SHARES, COL_FIGI, \
    COL_MIC, COL_TYPE, MIC_CODES, COL_COUNTRY, COL_NAME
from src.exceptions import APILimitReachedError, NoResultsFoundError
from src.logger import timber

_BASE_FILENAME = Path(__file__).name
_CLIENT = finnhub.Client(api_key=API_FINN_TOKEN)


def get_all_stock() -> pd.DataFrame:
    """
    Retrieve the full list of active U.S. stocks from Finnhub, with caching.

    Attempts to load from the local API cache if available and valid; otherwise queries the remote API.
    The results are normalized and cached for future use.

    Returns:
        pd.DataFrame: A DataFrame containing stock data with columns:

            - `COL_FIGI`: Financial Instrument Global Identifier (FIGI).
            - `COL_MIC`: Market Identifier Code (exchange).
            - `COL_SYMBOL`: Standardized ticker symbol.
            - `COL_TYPE`: Security type.
    """
    log = timber.plant()
    log.info("Phase starts", fetch="stock list", endpoint="finnhub")
    df = cache.load_api_cache(_BASE_FILENAME, {}, allow_stale=True)
    source = "cache"

    if df.empty:
        base_param = {"exchange": "US"}
        source = "API"

        frames = []
        for mic in MIC_CODES:
            param = base_param | {"mic": mic}
            result = _CLIENT.stock_symbol(**param)
            frames.append(pd.DataFrame(result))

        df = pd.concat(frames, ignore_index=True)
        df.rename(columns={"figi": COL_FIGI}, inplace=True)
        df[COL_SYMBOL] = data_processing.standardize_symbols(df[COL_SYMBOL])
        cache.save_api_cache(_BASE_FILENAME, {}, df)

    df = df[df["type"].isin(STOCK_TYPES)]
    log.info("Phase ends", fetch="stock list", endpoint="finnhub", count=len(df), source=source)
    return df[[COL_FIGI, COL_MIC, COL_SYMBOL, COL_TYPE]]


class FinnhubProvider(Provider):
    @property
    def name(self) -> str:
        return "finnhub"

    def fetch(self, symbol: str) -> pd.DataFrame:
        return self._get_company_profile2(symbol)

    def _get_company_profile2(self, symbol: str) -> pd.DataFrame:
        """
        Retrieve detailed company profile information for a given symbol, with caching.

        Attempts to load from the local API cache if available and valid; otherwise queries the remote API.
        The results are normalized and cached for future use.

        Args:
            symbol (str): The ticker symbol to query.

        Returns:
            pd.DataFrame: A single-row DataFrame with the following columns:

                - `COL_COUNTRY`: Country of the issuer.
                - `COL_MIC`: Market Identifier Code (exchange).
                - `COL_LIST_DATE`: IPO/listing date.
                - `COL_MC`: Market capitalization.
                - `COL_NAME`: Company name.
                - `COL_OUT_SHARES`: Shares outstanding.
                - `COL_SYMBOL`: Standardized ticker symbol.

        Raises:
            APILimitReachedError: When Finnhub API rate limits are exceeded.
            NoResultsFoundError: When the symbol returns no results from Finnhub.
        """
        log = timber.plant()
        filename = f"{_BASE_FILENAME}/{symbol[0]}"
        criteria = {"company": symbol}
        df = cache.load_api_cache(basename=filename, criteria=criteria, allow_stale=True)

        if df.empty:
            try:
                result = _CLIENT.company_profile2(**{"symbol": symbol})
            except FinnhubAPIException as e:
                if e.status_code == 429:
                    log.warning("FinnhubAPIException", reason="exceeded API limit", provider=self.name)
                    raise APILimitReachedError()
                else:
                    log.critical("FinnhubAPIException", reason=e.response.text, provider=self.name)
                    sys.exit(1)

            if not result:
                log.error("NoResultsFoundError", symbol=symbol, provider=self.name)
                raise NoResultsFoundError()

            df = pd.json_normalize(result)
            df.rename(columns={"marketCapitalization": COL_MC, "ticker": COL_SYMBOL, "ipo": COL_LIST_DATE,
                               "shareOutstanding": COL_OUT_SHARES}, inplace=True)
            df[COL_SYMBOL] = data_processing.standardize_symbols(df[COL_SYMBOL])
            cache.save_api_cache(basename=filename, criteria=criteria, df=df)
        else:
            log.debug("Fetch", target="CompanyProfile", source="disk", symbol=symbol, provider=self.name)

        df = df.drop(columns=["currency", "estimateCurrency", "exchange", "finnhubIndustry", "logo", "phone", "weburl"])
        return df[[COL_COUNTRY, COL_LIST_DATE, COL_MC, COL_NAME, COL_OUT_SHARES, COL_SYMBOL]]
