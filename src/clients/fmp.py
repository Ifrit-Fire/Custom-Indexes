import pandas as pd
import requests
from pandas import DataFrame

from src.consts import COL_NAME, COL_MC, COL_SYMBOL, MIN_MEGA_CAP, FMP_API_TOKEN, PATH_DATA_ROOT, COL_PRICE, \
    MIN_LARGE_CAP, MIN_MID_CAP, MIN_SMALL_CAP, COL_VOLUME, MIN_ULTRA_CAP

# Financial Model Prep: https://intelligence.financialmodelingprep.com/developer/docs/stock-screener-api

DEV_PATH_API_MEGA_STOCK = PATH_DATA_ROOT / "api_mega_stock.pkl"

_BASE_URL = "https://financialmodelingprep.com/api/v3/stock-screener"
_EXCHANGES = ["NYSE", "NASDAQ", "AMEX"]
_DEFAULT_PARAM = {"isEtf": False, "isFund": False, "isActivelyTrading": True, "apikey": FMP_API_TOKEN,
                  "exchange": _EXCHANGES}


def get_stock(top: int, from_cache=False) -> DataFrame:
    """
    Stock-screener can not pre-sort the values. All queries limited to 1000 results.  Results are in no particular
    order. Need to use market cap limits to restrict return count.

    :param top: How many of the top stocks to return
    :param from_cache: Pull from cache instead of hitting API
    :return: DataFrame stripped of unused columns and standardized column names.
    """
    if from_cache and DEV_PATH_API_MEGA_STOCK.exists():
        df = pd.read_pickle(DEV_PATH_API_MEGA_STOCK)
    else:
        params = _DEFAULT_PARAM | {"marketCapMoreThan": _get_cap_restriction(top)}
        response = requests.get(_BASE_URL, params=params)
        response.raise_for_status()

        df = pd.DataFrame(response.json())
        df.rename(columns={"companyName": COL_NAME, "marketCap": COL_MC}, inplace=True)
        df.to_pickle(DEV_PATH_API_MEGA_STOCK)

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
