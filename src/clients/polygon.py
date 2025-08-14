import json

import yaml
from polygon import RESTClient
from polygon.rest.models import TickerDetails
from urllib3.exceptions import MaxRetryError

from src import io
from src.consts import POLY_API_TOKEN, PATH_DATA_SYMBOLS_ROOT

_EXCHANGES = {"XNYS",  # NY stock exchange
              "XNAS",  # NASDAQ
              "XASE"}  # NYSE American (formerly AMEX)


def _get_ticker_filename(symbol: str):
    return PATH_DATA_SYMBOLS_ROOT / f"{symbol}.yaml"


def _load_ticker_details(symbol: str) -> TickerDetails | None:
    filename = _get_ticker_filename(symbol)
    if filename.exists():
        with open(filename, "r") as file:
            data = yaml.safe_load(file)
            return TickerDetails.from_dict(data)
    else:
        return None


def _save_ticker_details(symbol: str, data: dict):
    filename = _get_ticker_filename(symbol)
    with open(filename, "w") as file:
        yaml.safe_dump(data, file, indent=4)  # Save as yaml to be more human-readable.


def get_stock(symbol: str) -> TickerDetails:
    # Try loading from disk first
    ticker = _load_ticker_details(symbol)
    if ticker: return ticker

    # Gotta pull down from the API
    attempt = raw = None  # Suppresses references before bound warning
    client = RESTClient(api_key=POLY_API_TOKEN)
    for attempt in range(retries := 3):
        try:
            raw = client.get_ticker_details(ticker=symbol, raw=True)  # Grab raw so we can save json to disk
            if attempt > 0: print("\tSuccess!")
            attempt = 0
            break
        except MaxRetryError as e:
            print(f"\t{e.reason}")
            print(f"\tPossibly exceeded your accounts API limit. Attempt {attempt + 1} of {retries}")
            io.console_countdown("\tRetrying", 60)

    if attempt >= retries - 1:
        raise ConnectionError("Unknown issue with API end point.")

    # Save to disk
    raw = raw.data.decode("utf-8")
    raw = json.loads(raw)["results"]
    ticker = TickerDetails.from_dict(raw)
    _save_ticker_details(ticker.ticker, raw)

    return ticker
