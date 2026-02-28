from datetime import date

import pandas as pd
import requests

from clients.provider import BaseProvider, MixinForexRates
from consts import API_FRED_TOKEN
from data.source import ProviderSource
from src.logger import timber

_BASE_URL = "https://api.stlouisfed.org/fred/"

_ISO_SERIES_MAP = {"AED": "", "ARS": "", "AUD": "", "BRL": "", "CAD": "", "CHF": "", "CLP": "", "CNY": "", "COP": "",
                   "DKK": "", "EUR": "", "GBP": "", "HKD": "", "IDR": "", "ILS": "", "INR": "", "JPY": "DEXJPUS",
                   "KRW": "", "KZT": "", "MXN": "", "MYR": "", "PEN": "", "PHP": "", "SEK": "", "SGD": "", "TRY": "",
                   "TWD": "", "VND": "", "ZAR": ""}


class FredProvider(BaseProvider, MixinForexRates):
    @property
    def name(self) -> ProviderSource:
        return ProviderSource.FRED

    def fetch_forex_rates(self, iso_code: str, start: date = None, end: date = None) -> pd.DataFrame:
        log = timber.plant()
        series_id = _ISO_SERIES_MAP.get(iso_code)
        url = _BASE_URL + "series/observations"
        params = {"series_id": series_id, "api_key": API_FRED_TOKEN, "file_type": "json"}
        response = requests.get(url, params=params)
        response.raise_for_status()
        df = pd.DataFrame(response.json()["observations"])
        return df
