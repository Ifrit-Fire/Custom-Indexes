import sys

import finnhub
import pandas as pd
from finnhub import FinnhubAPIException

from clients.provider import MixinStockDetails, MixinStockListing
from src.clients.providerpool import BaseProvider
from src.consts import API_FINN_TOKEN, COL_SYMBOL, STOCK_TYPES, COL_MC, COL_LIST_DATE, COL_OUT_SHARES, COL_FIGI, \
    COL_TYPE, MIC_CODES
from src.data import processing
from src.data.source import ProviderSource
from src.exceptions import APILimitReachedError
from src.logger import timber

_CLIENT = finnhub.Client(api_key=API_FINN_TOKEN)


class FinnhubProvider(BaseProvider, MixinStockDetails, MixinStockListing):
    @property
    def name(self) -> ProviderSource:
        return ProviderSource.FINNHUB

    def fetch_stock_details(self, symbol: str) -> pd.DataFrame:
        """
        Retrieves and normalizes detailed ticker information for a given symbol.

        Args:
            symbol: The ticker symbol to query.

        Returns:
            A single-row DataFrame containing standardized ticker information.

        Raises:
            FinnhubAPIException: When API rate limits are exceeded.
            NoResultsFoundError: When the symbol returns no results from provider.
        """
        log = timber.plant()
        try:
            result = _CLIENT.company_profile2(**{"symbol": symbol})
        except FinnhubAPIException as e:
            if e.status_code == 429:
                log.warning("FinnhubAPIException", reason="exceeded API limit", provider=self.name)
                raise APILimitReachedError()
            else:
                log.critical("FinnhubAPIException", reason=e.response.text, provider=self.name)
                sys.exit(1)

        if not result:
            log.error("NoResultsFoundError", symbol=symbol, provider=self.name)
            return pd.DataFrame()

        df = pd.json_normalize(result)
        df.rename(columns={"marketCapitalization": COL_MC, "ticker": COL_SYMBOL, "ipo": COL_LIST_DATE,
                           "shareOutstanding": COL_OUT_SHARES}, inplace=True)
        ticker = df.loc[0, COL_SYMBOL]
        if ticker != symbol:
            # CompanyProfile2 API returns the primary listing symbol (e.g., international exchange), which does not
            # always align with the US exchange symbols we track. Overwrite it to preserve consistency.
            log.warning("Different symbol returned", queried=symbol, returned=ticker, action="ignoring returned",
                        provider=self.name)
            df.loc[0, COL_SYMBOL] = symbol
        df = processing.set_column_types(df)
        df[COL_MC] *= 1_000_000
        df[COL_OUT_SHARES] *= 1_000_000
        return df

    def fetch_stock_listing(self) -> pd.DataFrame:
        """
        Retrieves and normalizes all active stock listings. Focuses strictly on common stocks and ADRs from the
        `XNYS`, `XNAS`, `XASE`, and `BATS` exchanges.

        Returns:
            A DataFrame containing active stock listings with standardized symbol, type, and figi.
        """
        log = timber.plant()
        log.info("Phase starts", fetch="stock list", endpoint="finnhub")

        base_param = {"exchange": "US"}
        frames = []
        for mic in MIC_CODES:
            param = base_param | {"mic": mic}
            result = _CLIENT.stock_symbol(**param)
            frames.append(pd.DataFrame(result))

        df = pd.concat(frames, ignore_index=True)
        df.rename(columns={"figi": COL_FIGI}, inplace=True)
        df = df[df[COL_TYPE].isin(STOCK_TYPES)]
        df[COL_SYMBOL] = processing.standardize_symbols(df[COL_SYMBOL])
        df = processing.set_column_types(df)

        log.info("Phase ends", fetch="stock list", endpoint="finnhub", count=len(df), source="API")
        return df
