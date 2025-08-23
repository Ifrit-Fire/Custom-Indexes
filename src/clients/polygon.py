import json
import re
import sys

import yaml
from polygon import RESTClient, BadResponse
from polygon.rest.models import TickerDetails
from urllib3.exceptions import MaxRetryError

from src import io
from src.consts import API_POLY_TOKEN, PATH_DATA_SYMBOLS_ROOT, API_POLY_CACHE_ONLY
from src.logging import timber

# Codes used by polygon
_EXCHANGES = {"XNYS",  # NY stock exchange
              "XNAS",  # NASDAQ
              "XASE"}  # NYSE American (formerly AMEX)


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


def get_stock(symbol: str) -> TickerDetails:
    """
    Retrieve ticker details for a given stock symbol, using cached data if available.

    This function first attempts to load ticker details from local storage. If not found,
    it queries the Polygon.io API (up to three retries on failure) and saves the retrieved
    data to disk for future use.

    Args:
        symbol (str): Stock ticker symbol to retrieve.

    Returns:
        TickerDetails: Object containing the symbol's details.

    Raises:
        ConnectionError: If the API call fails after the maximum number of retries.

    Notes:
        - On API failure due to rate limits or network issues, the function waits
          60 seconds between retries.
    """
    # Try loading from disk first
    log = timber.plant()
    ticker = _load_ticker_details(symbol)
    if ticker:
        log.debug("Fetch", target="TickerDetails", source="disk", symbol=symbol)
        return ticker

    if API_POLY_CACHE_ONLY:
        log.critical("Missing", env="POLY_API_TOKEN", cache="Not found")
        raise RuntimeError("No CMC API token found.")

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
