from pathlib import Path

import pandas as pd
import requests
from pandas import DataFrame

from src import data_processing
from src.clients import cache
from src.config_handler import KEY_INDEX_TOP
from src.consts import COL_NAME, COL_SYMBOL, COL_MC, CMC_API_TOKEN, COL_PRICE, COL_VOLUME, COL_TYPE, ASSET_CRYPTO

# Coin Market Cap: https://coinmarketcap.com/api/

BASE_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
_BASE_FILENAME = Path(__file__).name


def get_crypto(criteria: dict) -> DataFrame:
    """
    Retrieve a DataFrame of cryptocurrencies matching the given index criteria.

    Attempts to load from the local API cache if available and up-to-date; otherwise queries the remote API. The 
    results are normalized and cached for future use.

    :param criteria: Dictionary of configuration values for the index, must include at least `KEY_INDEX_TOP`.
    :return: DataFrame containing standardized columns: `COL_NAME`, `COL_SYMBOL`, `COL_MC`, `COL_PRICE`, `COL_VOLUME`,
    `COL_TYPE`.
    """

    df = cache.grab_api_cache(_BASE_FILENAME, criteria)
    source = "cache"

    if df.empty:
        source = "API"
        # API defaults to sort "market_cap"; sort_dir defaults to "desc". Specifying anyway for clarity
        params = {"start": "1", "limit": criteria[KEY_INDEX_TOP], "convert": "USD", "sort": "market_cap",
                  "sort_dir": "desc"}
        headers = {"Accepts": "application/json", "X-CMC_PRO_API_KEY": CMC_API_TOKEN}
        response = requests.get(BASE_URL, headers=headers, params=params)
        response.raise_for_status()

        df = pd.json_normalize(response.json()["data"])
        df.rename(
            columns={"quote.USD.market_cap": COL_MC, "quote.USD.price": COL_PRICE, "quote.USD.volume_24h": COL_VOLUME},
            inplace=True)
        df[COL_SYMBOL] = data_processing.normalize_symbols(df[COL_SYMBOL])
        df[COL_TYPE] = ASSET_CRYPTO
        cache.store_api_cache(_BASE_FILENAME, criteria, df)

    print(f"\tRetrieved {len(df)} crypto from {source}")
    return df[[COL_NAME, COL_SYMBOL, COL_MC, COL_PRICE, COL_VOLUME, COL_TYPE]]
