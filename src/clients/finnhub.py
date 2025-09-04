from pathlib import Path

import finnhub
import pandas as pd
from finnhub import FinnhubAPIException

from src import data_processing
from src.clients import cache
from src.clients.providerpool import Provider
from src.consts import API_FINN_TOKEN, API_FINN_CACHE_ONLY, COL_SYMBOL, STOCK_TYPES, COL_MC, COL_LIST_DATE, \
    COL_OUT_SHARES, COL_FIGI, COL_MIC, COL_TYPE, MIC_CODES
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
            - COL_FIGI: Financial Instrument Global Identifier (FIGI).
            - COL_MIC: Market Identifier Code (exchange).
            - COL_SYMBOL: Standardized ticker symbol.
            - COL_TYPE: Security type.
    """
    log = timber.plant()
    log.info("Phase starts", fetch="stock list", endpoint="finnhub")
    df = cache.load_api_cache(_BASE_FILENAME, {}, allow_stale=API_FINN_CACHE_ONLY)
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

    @staticmethod
    def _get_company_profile2(symbol: str) -> pd.DataFrame:
        log = timber.plant()
        df = cache.load_api_cache(basename=f"{_BASE_FILENAME}/{symbol[0]}", criteria={"company": symbol},
                                  allow_stale=API_FINN_CACHE_ONLY)

        if df.empty:
            try:
                result = _CLIENT.company_profile2(**{"symbol": symbol})
            except FinnhubAPIException as e:
                if e.status_code == 429:
                    log.warning("FinnhubAPIException", reason="exceeded API limit")
                    raise APILimitReachedError()
                else:
                    raise e

            if not result:
                log.error("NoResultsFoundError", reason=symbol)
                raise NoResultsFoundError()

            df = pd.json_normalize(result)
            df.rename(columns={"marketCapitalization": COL_MC, "ticker": COL_SYMBOL, "ipo": COL_LIST_DATE,
                               "shareOutstanding": COL_OUT_SHARES}, inplace=True)
            df[COL_SYMBOL] = data_processing.standardize_symbols(df[COL_SYMBOL])
            cache.save_api_cache(basename=f"{_BASE_FILENAME}/{symbol[0]}", criteria={"company": symbol}, df=df)
        else:
            log.debug("Fetch", target="CompanyProfile", source="disk", symbol=symbol)

        df = df.drop(columns=["currency", "estimateCurrency", "exchange", "phone", "weburl", "logo", "finnhubIndustry"])
        # Columns remaining:
        #   country, list_date, market_cap, name, outstanding_shares, symbol
        return df
