from pathlib import Path

import pandas as pd
import requests
from pandas import DataFrame

from src import data_processing
from src.clients import cache
from src.config_handler import KEY_INDEX_TOP
from src.consts import COL_NAME, COL_MC, COL_SYMBOL, MIN_MEGA_CAP, FMP_API_TOKEN, COL_PRICE, MIN_LARGE_CAP, MIN_MID_CAP, \
    MIN_SMALL_CAP, COL_VOLUME, MIN_ULTRA_CAP, _SYMBOL_NORMALIZE

# Financial Model Prep: https://intelligence.financialmodelingprep.com/developer/docs/stock-screener-api
_BASE_URL = "https://financialmodelingprep.com/api/v3/stock-screener"
_BASE_FILENAME = Path(__file__).name
_EXCHANGES = ["NYSE", "NASDAQ", "AMEX"]
_DEFAULT_PARAM = {"isEtf": False, "isFund": False, "isActivelyTrading": True, "apikey": FMP_API_TOKEN,
                  "exchange": _EXCHANGES}


def get_stock(criteria: dict) -> DataFrame:
    """
    API call which retrieves a DataFrame of stock specified by the criteria configurations. Automatically pulls from
    cache when available and cache data is recent.

    :param criteria: Configuration criteria for an index
    :return: Dataframe consisting of all needed columns with standardized column names.
    """

    df = cache.grab_api_cache(_BASE_FILENAME, criteria)
    source = "cache"

    if df.empty:
        source = "API"
        params = _DEFAULT_PARAM | {"marketCapMoreThan": _get_cap_restriction(criteria[KEY_INDEX_TOP])}
        response = requests.get(_BASE_URL, params=params)
        response.raise_for_status()

        df = pd.DataFrame(response.json())
        df.rename(columns={"companyName": COL_NAME, "marketCap": COL_MC}, inplace=True)
        df[COL_SYMBOL] = df[COL_SYMBOL].str.upper().replace(_SYMBOL_NORMALIZE)
        df = data_processing.prune_asset_type(df)
        cache.store_api_cache(_BASE_FILENAME, criteria, df)

    print(f"\tRetrieved {len(df)} stocks from {source}")
    return df[[COL_NAME, COL_SYMBOL, COL_MC, COL_PRICE, COL_VOLUME]]


def _get_cap_restriction(top: int):
    """
    Based on how many of the top stocks are desired, we return an estimate of which cap category that will entail.

    :param top: Top number of stocks desired
    :return: Estimated market cap that will ensure at least top number of stocks will be returned.
    """
    if top <= 5:
        return MIN_ULTRA_CAP
    if top <= 50:
        return MIN_MEGA_CAP
    if top <= 300:
        return MIN_LARGE_CAP
    if top <= 500:
        return MIN_MID_CAP
    else:
        return MIN_SMALL_CAP
