import pandas as pd
import requests
from pandas import DataFrame

from src.consts import COL_NAME, COL_SYMBOL, COL_MC, CMC_API_TOKEN, PATH_DATA_ROOT, COL_PRICE, COL_VOLUME

# Coin Market Cap: https://coinmarketcap.com/api/

BASE_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
DEV_PATH_API_MEGA_CRYPTO = PATH_DATA_ROOT / "api_mega_crypto.pkl"


def get_crypto(top: int, from_cache=False) -> DataFrame:
    if from_cache and DEV_PATH_API_MEGA_CRYPTO.exists():
        df = pd.read_pickle(DEV_PATH_API_MEGA_CRYPTO)
    else:
        # sort defaults to "market_cap"; sort_dir defaults to "desc". Specifying anyway for clarity
        params = {"start": "1", "limit": top, "convert": "USD", "sort": "market_cap", "sort_dir": "desc"}
        headers = {"Accepts": "application/json", "X-CMC_PRO_API_KEY": CMC_API_TOKEN}
        response = requests.get(BASE_URL, headers=headers, params=params)
        response.raise_for_status()

        df = pd.json_normalize(response.json()["data"])
        df.rename(
            columns={"quote.USD.market_cap": COL_MC, "quote.USD.price": COL_PRICE, "quote.USD.volume_24h": COL_VOLUME},
            inplace=True)
        df.to_pickle(DEV_PATH_API_MEGA_CRYPTO)

    return df[[COL_NAME, COL_SYMBOL, COL_MC, COL_PRICE, COL_VOLUME]]
