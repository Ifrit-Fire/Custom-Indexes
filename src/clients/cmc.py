from pathlib import Path

import pandas as pd
import requests
from pandas import DataFrame

from src.config_handler import KEY_INDEX_TOP
from src.consts import COL_NAME, COL_SYMBOL, COL_MC, API_CMC_TOKEN, COL_PRICE, COL_VOLUME, COL_TYPE, ASSET_CRYPTO, \
    COL_LIST_DATE, API_CMC_CACHE_ONLY
from src.io import cache
from src.logger import timber

# Coin Market Cap: https://coinmarketcap.com/api/

BASE_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
_BASE_FILENAME = Path(__file__).name


def get_crypto(criteria: dict) -> DataFrame:
    """
    Retrieve a DataFrame of cryptocurrencies matching the given index criteria.

    Attempts to load from the local API cache if available and valid; otherwise queries the remote API.
    The results are normalized and cached for future use.

    Args:
        criteria (dict): Dictionary of configuration values for the index, must include at least `KEY_INDEX_TOP`.

    Returns:
        DataFrame: DataFrame containing standardized columns:
            `COL_NAME`, `COL_SYMBOL`, `COL_MC`, `COL_PRICE`, `COL_VOLUME`, `COL_TYPE`, `COL_LIST_DATE`.
    """

    log = timber.plant()
    log.info("Phase starts", fetch="crypto")
    df = cache.load_api_cache(_BASE_FILENAME, criteria, allow_stale=API_CMC_CACHE_ONLY)
    source = "cache"

    if df.empty:
        source = "API"
        if API_CMC_CACHE_ONLY:
            log.critical("Missing", env="CMC_API_TOKEN", cache="Not found")
            raise RuntimeError("No CMC API token found.")

        # API defaults to sort "market_cap"; sort_dir defaults to "desc". Specifying anyway for clarity
        params = {"start": "1", "limit": criteria[KEY_INDEX_TOP], "convert": "USD", "sort": "market_cap",
                  "sort_dir": "desc"}
        headers = {"Accepts": "application/json", "X-CMC_PRO_API_KEY": API_CMC_TOKEN}
        response = requests.get(BASE_URL, headers=headers, params=params)
        response.raise_for_status()

        df = pd.json_normalize(response.json()["data"])
        df.rename(
            columns={"quote.USD.market_cap": COL_MC, "quote.USD.price": COL_PRICE, "quote.USD.volume_24h": COL_VOLUME,
                     "date_added": COL_LIST_DATE}, inplace=True)
        df[COL_SYMBOL] = data_processing.standardize_symbols(df[COL_SYMBOL])
        df[COL_TYPE] = ASSET_CRYPTO
        cache.save_api_cache(_BASE_FILENAME, criteria, df)

    df = _exclude_stablecoins(df)
    log.info("Phase ends", fetch="crypto", count=len(df), source=source)
    return df[[COL_NAME, COL_SYMBOL, COL_MC, COL_PRICE, COL_VOLUME, COL_TYPE, COL_LIST_DATE]]


def _exclude_stablecoins(df: DataFrame) -> DataFrame:
    """
    Remove stablecoins from the DataFrame based on the `tags` column.

    Args:
        df (pd.DataFrame): Input DataFrame containing at least `tags` and `COL_SYMBOL` columns.

    Returns:
        pd.DataFrame: A filtered DataFrame with all stablecoins removed.
    """
    log = timber.plant()
    mask = df["tags"].apply(lambda tags: "stablecoin" in tags)

    for symbol in df.loc[mask, COL_SYMBOL]:
        log.debug("Excluded", symbol=symbol, reason="stablecoin")
    log.info("Excluded", items="symbols", count=int((mask).sum()), reason="stablecoin")

    return df[~mask]
