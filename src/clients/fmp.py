from pathlib import Path

import pandas as pd
import requests
from pandas import DataFrame

from src import data_processing
from src.clients import cache
from src.config_handler import KEY_INDEX_TOP, config
from src.consts import COL_NAME, COL_MC, COL_SYMBOL, MIN_MEGA_CAP, FMP_API_TOKEN, COL_PRICE, MIN_LARGE_CAP, MIN_MID_CAP, \
    MIN_SMALL_CAP, COL_VOLUME, MIN_ULTRA_CAP

# Financial Model Prep: https://intelligence.financialmodelingprep.com/developer/docs/stock-screener-api
_BASE_URL = "https://financialmodelingprep.com/api/v3/stock-screener"
_BASE_FILENAME = Path(__file__).name
_EXCHANGES = ["NYSE", "NASDAQ", "AMEX"]
_DEFAULT_PARAM = {"isEtf": False, "isFund": False, "isActivelyTrading": True, "apikey": FMP_API_TOKEN,
                  "exchange": _EXCHANGES}


def get_stock(criteria: dict) -> DataFrame:
    """
    Retrieve a DataFrame of stocks matching the given index criteria.

    Attempts to load from the local API cache if available and up-to-date; otherwise queries the remote API. The
    results are normalized, filtered to allowed asset types, and cached for future use.

    :param criteria: Dictionary of configuration values for the index, must include at least `KEY_INDEX_TOP`.
    :return: DataFrame containing standardized columns: COL_NAME, COL_SYMBOL, COL_MC, COL_PRICE, COL_VOLUME.
    """
    print(f"\tRetrieving stocks...")
    df = cache.grab_api_cache(_BASE_FILENAME, criteria)
    source = "cache"

    if df.empty:
        source = "API"
        params = _DEFAULT_PARAM | {"marketCapMoreThan": _get_cap_restriction(criteria[KEY_INDEX_TOP])}
        response = requests.get(_BASE_URL, params=params)
        response.raise_for_status()

        df = pd.DataFrame(response.json())
        df.rename(columns={"companyName": COL_NAME, "marketCap": COL_MC}, inplace=True)
        df[COL_SYMBOL] = data_processing.normalize_symbols(df[COL_SYMBOL])
        df = data_processing.prune_asset_type(df)
        cache.store_api_cache(_BASE_FILENAME, criteria, df)

    print(f"\t...Retrieved {len(df)} stocks from {source}")
    return df[[COL_NAME, COL_SYMBOL, COL_MC, COL_PRICE, COL_VOLUME]]


def _get_cap_restriction(top: int):
    """
    Determine the minimum market capitalization threshold based on the desired number of top-ranked stocks.

    This function maps a requested "top N" count to an estimated market cap category constant (e.g., MIN_ULTRA_CAP,
    MIN_MEGA_CAP, etc.) that should yield at least that many securities.

    :param top: Number of top stocks to include.
    :return: Market cap threshold constant for the appropriate category.
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
