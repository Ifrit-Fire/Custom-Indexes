import json

import yaml
from polygon import RESTClient, BadResponse
from polygon.rest.models import TickerDetails
from urllib3.exceptions import MaxRetryError

from src import io
from src.consts import POLY_API_TOKEN, PATH_DATA_SYMBOLS_ROOT

# Codes used by polygon
_EXCHANGES = {"XNYS",  # NY stock exchange
              "XNAS",  # NASDAQ
              "XASE"}  # NYSE American (formerly AMEX)
# @formatter:off
_SYMBOL_MAPPINGS = {"MS.PQ": "MSpQ",
                    "KIM.PN": "KIMpN",
                    "RF.PF": "RFpF"}
# @formatter:on

def _get_ticker_filename(symbol: str):
    """
    Construct the full file path for storing or retrieving ticker details.

    :param symbol: Stock ticker symbol.
    :return: Path object pointing to the YAML file for the given symbol,
             located under `PATH_DATA_SYMBOLS_ROOT`.
    """
    return PATH_DATA_SYMBOLS_ROOT / f"{symbol}.yaml"


def _load_ticker_details(symbol: str) -> TickerDetails | None:
    """
    Load ticker details for a given symbol from local YAML storage.

    :param symbol: Stock ticker symbol to load.
    :return: A `TickerDetails` object if the file exists, otherwise None.
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

    :param symbol: Stock ticker symbol to save.
    :param data: Dictionary of ticker details to write.
    """
    filename = _get_ticker_filename(symbol)
    with open(filename, "w") as file:
        yaml.safe_dump(data, file, indent=4)  # Save as yaml to be more human-readable.


def get_stock(symbol: str) -> TickerDetails:
    """
    Retrieve ticker details for a given stock symbol, using cached data if available.

    This function first attempts to load ticker details from local storage. If not found,
    it queries the Polygon.io API (up to three retries on failure) and saves the retrieved
    data to disk for future use.

    :param symbol: Stock ticker symbol to retrieve.
    :return: A `TickerDetails` object containing the symbol's details.

    Raises:
        ConnectionError: If the API call fails after the maximum number of retries.

    Notes:
        - On API failure due to rate limits or network issues, the function waits 60 seconds
          between retries.
    """

    # Try loading from disk first
    ticker = _load_ticker_details(symbol)
    if ticker: return ticker

    # Gotta pull down from the API
    attempt = raw = norm_sym = None  # Suppresses references before bound warning
    client = RESTClient(api_key=POLY_API_TOKEN)
    for attempt in range(retries := 3):
        try:
            norm_sym = _SYMBOL_MAPPINGS.get(symbol, symbol)
            raw = client.get_ticker_details(ticker=norm_sym, raw=True)  # Grab raw so we can save json to disk
            if attempt > 0: print("\tSuccess!")
            attempt = 0
            break
        except MaxRetryError as e:
            print(f"\t{e.reason}")
            print(f"\tPossibly exceeded your accounts API limit. Attempt {attempt + 1} of {retries}")
            io.console_countdown("\tRetrying", 60)
        except BadResponse as e:
            print(f"\t{str(e)}")
            print(f"\tBadResponse for ticker {symbol} (normalized: {norm_sym})")

    if attempt >= retries - 1:
        raise ConnectionError("Unknown issue with API end point.")

    # Save to disk
    raw = raw.data.decode("utf-8")
    raw = json.loads(raw)["results"]
    ticker = TickerDetails.from_dict(raw)
    _save_ticker_details(ticker.ticker, raw)

    return ticker
