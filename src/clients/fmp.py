import pandas as pd
import requests
from pandas import DataFrame

from src.consts import COL_NAME, COL_MC, COL_SYMBOL, MIN_MEGA_CAP, FMP_API_TOKEN, PATH_DATA

# Financial Model Prep: https://intelligence.financialmodelingprep.com/developer/docs/stock-screener-api

BASE_URL = "https://financialmodelingprep.com/api/v3/stock-screener"
DEV_PATH_API_MEGA_STOCK = PATH_DATA / "api_mega_stock.pkl"
EXCHANGES = ["NYSE", "NASDAQ", "AMEX"]


def get_mega_stock(from_cache=False) -> DataFrame:
    if from_cache and DEV_PATH_API_MEGA_STOCK.exists():
        return pd.read_pickle(DEV_PATH_API_MEGA_STOCK)

    params = {"marketCapMoreThan": MIN_MEGA_CAP, "isEtf": False, "isFund": False, "isActivelyTrading": True,
              "apikey": FMP_API_TOKEN, "volumeMoreThan": 10000, "exchange": EXCHANGES}
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        df = pd.DataFrame(response.json())
        df = _clean(df)
        return df
    else:
        print(f"Error fetching data: {response.status_code}")
        return pd.DataFrame()


def _clean(df: DataFrame) -> DataFrame:
    df.rename(columns={"companyName": COL_NAME, "marketCap": COL_MC}, inplace=True)
    df = df[[COL_NAME, COL_SYMBOL, COL_MC]]
    df = df[df[COL_MC].notnull()]
    df.to_pickle(DEV_PATH_API_MEGA_STOCK)
    return df
