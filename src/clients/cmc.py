from typing import Sequence

import pandas as pd
import requests

from src.clients.provider import Provider
from src.consts import COL_SYMBOL, COL_MC, API_CMC_TOKEN, COL_PRICE, COL_VOLUME, COL_TYPE, COL_LIST_DATE, COL_OUT_SHARES
from src.data import processing
from src.data.security_types import CryptoTypes
from src.data.source import ProviderSource

# Coin Market Cap: https://coinmarketcap.com/api/

BASE_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"


class CMCProvider(Provider):
    @property
    def name(self) -> ProviderSource:
        return ProviderSource.COIN_MC

    def fetch_crypto_market(self):
        """
        Retrieve all cryptocurrency market data. The results are normalized.

        Returns:
            A DataFrame containing active crypto listings with market data, standardized symbols and types.
        """
        # API defaults to sort "market_cap"; sort_dir defaults to "desc". Specifying anyway for clarity
        params = {"start": "1", "limit": "2000", "convert": "USD", "sort": "market_cap", "sort_dir": "desc"}
        headers = {"Accepts": "application/json", "X-CMC_PRO_API_KEY": API_CMC_TOKEN}
        response = requests.get(BASE_URL, headers=headers, params=params)
        response.raise_for_status()

        df = pd.json_normalize(response.json()["data"])
        col_rename = {"quote.USD.market_cap": COL_MC, "quote.USD.price": COL_PRICE, "quote.USD.volume_24h": COL_VOLUME,
                      "date_added": COL_LIST_DATE, "circulating_supply": COL_OUT_SHARES}
        df.rename(columns=col_rename, inplace=True)
        df[COL_SYMBOL] = processing.standardize_symbols(df[COL_SYMBOL])
        df[COL_TYPE] = df["tags"].apply(CMCProvider.tag_to_type)

        return df

    def fetch_ohlcv(self, date: pd.Timestamp) -> pd.DataFrame:
        return pd.DataFrame()

    def fetch_stock_details(self, symbol: str) -> pd.DataFrame:
        return pd.DataFrame()

    def fetch_stock_listing(self) -> pd.DataFrame:
        return pd.DataFrame()

    @staticmethod
    def tag_to_type(tags: Sequence[str]):
        return CryptoTypes.STABLECOIN.value if "stablecoin" in tags else CryptoTypes.CRYPTO.value
