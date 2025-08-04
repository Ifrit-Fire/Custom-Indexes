import pandas as pd
import requests
from pandas import DataFrame

from src.consts import COL_NAME, COL_SYMBOL, COL_MC, MIN_MEGA_CAP, CMC_API_TOKEN, PATH_DATA_ROOT, COL_PRICE

# Coin Market Cap: https://coinmarketcap.com/api/

BASE_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
DEV_PATH_API_MEGA_CRYPTO = PATH_DATA_ROOT / "api_mega_crypto.pkl"


def get_mega_crypto(from_cache=False) -> DataFrame:
    if from_cache and DEV_PATH_API_MEGA_CRYPTO.exists():
        return pd.read_pickle(DEV_PATH_API_MEGA_CRYPTO)

    params = {"start": "1", "limit": "1000", "convert": "USD"}
    headers = {"Accepts": "application/json", "X-CMC_PRO_API_KEY": CMC_API_TOKEN}
    response = requests.get(BASE_URL, headers=headers, params=params)
    if response.status_code == 200:
        df = pd.json_normalize(response.json()["data"])
        df = _clean_mega(df)
        return df
    else:
        print(f"Error fetching data: {response.status_code}")
        return pd.DataFrame()


def _clean_mega(df: DataFrame) -> DataFrame:
    df.rename(columns={"quote.USD.market_cap": COL_MC, "quote.USD.price": COL_PRICE}, inplace=True)
    df = df[[COL_NAME, COL_SYMBOL, COL_MC, COL_PRICE]]
    df = df[df[COL_MC] > MIN_MEGA_CAP]
    df.to_pickle(DEV_PATH_API_MEGA_CRYPTO)
    return df
