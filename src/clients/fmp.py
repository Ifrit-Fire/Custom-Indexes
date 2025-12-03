from pathlib import Path

import pandas as pd
import requests
from pandas import DataFrame, Series

from src.clients import polygon
from src.config_handler import KEY_INDEX_TOP
from src.consts import COL_NAME, COL_MC, COL_SYMBOL, MIN_MEGA_CAP, API_FMP_TOKEN, COL_C_PRICE, MIN_LARGE_CAP, \
    MIN_MID_CAP, MIN_SMALL_CAP, COL_VOLUME, MIN_ULTRA_CAP, COL_TYPE, COL_LIST_DATE, ASSET_TYPES, API_FMP_CACHE_ONLY
from src.io import cache
from src.logger import timber

# Financial Model Prep: https://intelligence.financialmodelingprep.com/developer/docs/stock-screener-api
_BASE_URL = "https://financialmodelingprep.com/api/v3/stock-screener"
_BASE_FILENAME = Path(__file__).name
_EXCHANGES = ["NYSE", "NASDAQ", "AMEX"]
_DEFAULT_PARAM = {"isEtf": False, "isFund": False, "isActivelyTrading": True, "apikey": API_FMP_TOKEN,
                  "exchange": _EXCHANGES}


def get_stock(criteria: dict) -> DataFrame:
    """
    Retrieve a DataFrame of stocks matching the given index criteria.

    Attempts to load from the local API cache if available and valid; otherwise queries the remote API. The
    results are normalized, filtered to allowed asset types, and cached for future use.

    Args:
        criteria (dict): Dictionary of configuration values for the index, must include at least `KEY_INDEX_TOP`.

    Returns:
        DataFrame: DataFrame containing standardized columns: `COL_NAME`, `COL_SYMBOL`, `COL_MC`, `COL_PRICE`,
        `COL_VOLUME`, `COL_TYPE`, `COL_LIST_DATE`.
    """
    log = timber.plant()
    log.info("Phase starts", fetch="stocks")
    df = cache.load_api_cache(_BASE_FILENAME, criteria, allow_stale=API_FMP_CACHE_ONLY)
    source = "cache"

    if df.empty:
        if API_FMP_CACHE_ONLY:
            log.critical("Missing", env="FMP_API_TOKEN", cache="Not found")
            raise RuntimeError("No FMP API token found.")

        source = "API"
        params = _DEFAULT_PARAM | {"marketCapMoreThan": _get_cap_restriction(criteria[KEY_INDEX_TOP])}
        response = requests.get(_BASE_URL, params=params)
        response.raise_for_status()

        df = pd.DataFrame(response.json())
        df.rename(columns={"companyName": COL_NAME, "marketCap": COL_MC}, inplace=True)
        df[COL_SYMBOL] = data_processing.standardize_symbols(df[COL_SYMBOL])
        df[[COL_TYPE, COL_LIST_DATE]] = df[COL_SYMBOL].apply(_get_type_date_info)
        cache.save_api_cache(_BASE_FILENAME, criteria, df)

    df = _exclude_asset_types(from_df=df, not_in=ASSET_TYPES)
    log.info("Phase ends", fetch="stocks", count=len(df), source=source)
    return df[[COL_NAME, COL_SYMBOL, COL_MC, COL_C_PRICE, COL_VOLUME, COL_TYPE, COL_LIST_DATE]]


def _get_type_date_info(sym: str) -> Series:
    """
    Retrieve the asset type and listing date for a given symbol.

    Args:
        sym (str): Ticker symbol to query.

    Returns:
        Series: A Series containing:
            - COL_TYPE: Asset type of the security.
            - COL_LIST_DATE: Listing date in YYYY-MM-DD format.

    Notes:
        Falls back to a Polygon API call if the symbol’s data is not found on disk.
    """
    ticker = polygon.get_stock(sym)
    return pd.Series({COL_TYPE: ticker.type, COL_LIST_DATE: ticker.list_date, })


def _get_cap_restriction(top: int):
    """
    Determine the minimum market capitalization threshold based on the desired number of top-ranked stocks.

    This function maps a requested "top N" count to an estimated market cap category constant (e.g., MIN_ULTRA_CAP,
    MIN_MEGA_CAP, etc.) that should yield at least that many securities.

    Args:
        top (int): Number of top stocks to include.

    Returns:
        int: Market cap threshold constant for the appropriate category.
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


def _exclude_asset_types(from_df: DataFrame, not_in: set[str]) -> DataFrame:
    """
    Exclude securities from the DataFrame whose asset type is not in a given set.

    Args:
        from_df (DataFrame): Input DataFrame containing at least the columns `COL_SYMBOL` and `COL_TYPE`.
        not_in (set[str]): Set of asset types to retain in the DataFrame. All others are excluded.

    Returns:
        DataFrame: Filtered DataFrame containing only rows with allowed asset types. The index is reset to start at 0.
    """
    log = timber.plant()
    mask = from_df[COL_TYPE].isin(not_in)
    excluded_df = from_df.loc[~mask, [COL_SYMBOL, COL_TYPE]]

    for _, row in excluded_df.iterrows():
        log.debug("Excluded", symbol=row[COL_SYMBOL], reason=row[COL_TYPE])
    log.info("Excluded", items="symbols", count=(~mask).sum(), reason="Asset Type")

    return from_df[mask].reset_index(drop=True)
