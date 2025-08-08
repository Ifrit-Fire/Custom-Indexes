from pathlib import Path

import pandas as pd
import requests
from pandas import DataFrame

from src.clients import cache
from src.config_handler import KEY_INDEX_TOP
from src.consts import COL_NAME, COL_SYMBOL, COL_MC, CMC_API_TOKEN, COL_PRICE, COL_VOLUME

# Coin Market Cap: https://coinmarketcap.com/api/

BASE_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
_BASE_FILENAME = Path(__file__).name


def get_crypto(criteria: dict) -> DataFrame:
    """
    API call which retrieves a DataFrame of crypto specified by the criteria configurations.  Automatically pulls from
    cache when available and cache data is recent.

    :param criteria: Configuration criteria for an index
    :return: Dataframe consisting of all needed columns with standardized column names.
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
        cache.store_api_cache(_BASE_FILENAME, criteria, df)

    print(f"\tRetrieved {len(df)} crypto from {source}")
    return df[[COL_NAME, COL_SYMBOL, COL_MC, COL_PRICE, COL_VOLUME]]
