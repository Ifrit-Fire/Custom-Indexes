import json
import re
import sys

import pandas as pd
import requests
import yaml
from polygon import RESTClient, BadResponse
from polygon.rest.models import TickerDetails
from urllib3.exceptions import MaxRetryError

from src import data_processing
from src.clients import cache
from src.clients.providerpool import Provider
from src.consts import API_POLY_TOKEN, PATH_DATA_SYMBOLS_ROOT, API_POLY_CACHE_ONLY, COL_SYMBOL, COL_OUT_SHARES, COL_MIC, \
    COL_CIK, COL_FIGI, COL_NAME, COL_TYPE, MIC_CODES
from src.exceptions import APILimitReachedError, NoResultsFoundError
from src.logger import timber

# Codes used by polygon
_EXCHANGES = {"XNYS",  # NY stock exchange
              "XNAS",  # NASDAQ
              "XASE"}  # NYSE American (formerly AMEX)
_BASE_FILENAME = Path(__file__).name
_BASE_ALL_TICKERS = "https://api.polygon.io/v3/reference/tickers"


def _get_ticker_filename(symbol: str):
    """
    Construct the full file path for storing or retrieving ticker details.

    Args:
        symbol (str): Stock ticker symbol.

    Returns:
        Path: Path object pointing to the YAML file for the given symbol, located under `PATH_DATA_SYMBOLS_ROOT`.
    """
    return PATH_DATA_SYMBOLS_ROOT / f"{symbol}.yaml"


def _load_ticker_details(symbol: str) -> TickerDetails | None:
    """
    Load ticker details for a given symbol from local YAML storage.

    Args:
        symbol (str): Stock ticker symbol to load.

    Returns:
        TickerDetails | None: A `TickerDetails` object if the file exists, otherwise None.
    """
    filename = _get_ticker_filename(symbol)
    if filename.exists():
        with open(filename, "r") as file:
            data = yaml.safe_load(file)
            return TickerDetails.from_dict(data)
    else:
        return None


def _save_ticker_details(symbol: str, data: dict):
    """
    Save ticker details for a given symbol to local YAML storage.

    Args:
        symbol (str): Stock ticker symbol to save.
        data (dict): Dictionary of ticker details to write.
    """
    filename = _get_ticker_filename(symbol)
    with open(filename, "w") as file:
        yaml.safe_dump(data, file, indent=4)  # Save as yaml to be more human-readable.


def _fix_dot_p(symbol: str) -> str:
    """
    Convert patterns like 'MS.PQ' to 'MSpQ'.

    Rules:
        - Remove the dot before 'P'
        - Lowercase the 'P'

    Args:
        symbol (str): Ticker symbol string to normalize.

    Returns:
        str: Normalized ticker symbol.
    """
    log = timber.plant()
    norm = re.sub(r'\.P', 'p', symbol)
    if norm != symbol: log.debug("Polygon normalization", symbol=symbol, normalized=norm)
    return norm


def _iter_all_stock(params: dict[str, str]):
    log = timber.plant()
    url = _BASE_ALL_TICKERS

    with requests.Session() as session:
        while url:
            final_param = {"apiKey": API_POLY_TOKEN}
            if url == _BASE_ALL_TICKERS:
                final_param |= {**params, "apiKey": API_POLY_TOKEN}

            response = session.get(url, params=final_param)
            if response.status_code == 429:
                log.debug("MaxRetryError", reason="exceeded API limit", response="waiting")
                time.sleep(60)
                continue
            response.raise_for_status()
            data = response.json()
            for row in data.get("results", []):
                yield row
            url = data.get("next_url")  # None when done


def get_all_stock() -> pd.DataFrame:
    log = timber.plant()
    log.info("Phase starts", fetch="stock list", endpoint="polygon")
    df = cache.load_api_cache(_BASE_FILENAME, {}, allow_stale=API_POLY_CACHE_ONLY)
    source = "cache"

    if df.empty:
        source = "API"

        tickers = []
        types = ["CS", "ADRC"]
        for mic in MIC_CODES:
            for tipe in types:
                params = {"market": "stocks", "active": "true", "exchange": mic, "type": tipe, "limit": 1000}
                tickers += _iter_all_stock(params)

        df = pd.json_normalize(tickers)
        df.rename(columns={"ticker": COL_SYMBOL, "primary_exchange": COL_MIC}, inplace=True)
        df[COL_SYMBOL] = data_processing.standardize_symbols(df[COL_SYMBOL])
        cache.save_api_cache(_BASE_FILENAME, {}, df)

    log.info("Phase ends", fetch="stock list", endpoint="polygon", count=len(df), source=source)
    return df[[COL_CIK, COL_FIGI, COL_NAME, COL_MIC, COL_SYMBOL, COL_TYPE]]

    # Gotta pull down from the API
    attempt = raw = None  # Suppresses references before bound warning
    norm_sym = _fix_dot_p(symbol)
    client = RESTClient(api_key=API_POLY_TOKEN)
    for attempt in range(retries := 3):
        try:
            raw = client.get_ticker_details(ticker=norm_sym, raw=True)  # Grab raw so we can save json to disk
            if attempt > 0: log.info("Retry success", attempt=attempt + 1, retries=retries)
            log.debug("Fetch", target="TickerDetails", source="API", symbol=symbol)
            attempt = 0
            break
        except MaxRetryError:
            log.warning("MaxRetryError", reason="exceeded API limit", attempt=attempt + 1, retries=retries)
            io.console_countdown(msg="\tRetrying", seconds=60)
        except BadResponse as e:
            log.critical("BadResponse", reason="unknown ticker", symbol=symbol, normalized=norm_sym)
            print(f"\t{str(e)}")
            sys.exit(1)

    if attempt >= retries - 1:
        log.critical("Retry failed", reason="exceeded retries", symbol=symbol, attempt=attempt + 1, retries=retries)
        raise ConnectionError("Unknown issue with API end point.")

    # Save to disk
    raw = raw.data.decode("utf-8")
    raw = json.loads(raw)["results"]
    ticker = TickerDetails.from_dict(raw)
    ticker.ticker = symbol  # Ensure our symbol is used so lookups remain consistent
    _save_ticker_details(ticker.ticker, raw)

    return ticker
