import exchange_calendars as xcals
import pandas as pd

from src.clients.cmc import CMCProvider
from src.clients.finnhub import FinnhubProvider
from src.clients.polygon import PolygonProvider
from src.clients.providerpool import ProviderPool
from src.consts import COL_TIMESTAMP
from src.data import projection, processing
from src.data.source import ProviderSource
from src.io import store, cache
from src.logger import timber

_POOL = ProviderPool(providers=[FinnhubProvider(), PolygonProvider(), CMCProvider()])


def get_crypto_market() -> pd.DataFrame:
    """
    Retrieves a list of crypto assets across the market.

    Loads cached crypto listings if available. Otherwise, fetches fresh data from the provider,
    saves it to cache, and projects it into the canonical schema.

    Returns:
        A DataFrame containing standardized crypto asset listings.
    """
    log = timber.plant()
    log.info("Phase starts", fetch="crypto")
    df = cache.load_crypto_lists()
    if df.empty:
        df = _POOL.fetch_crypto_market()[ProviderSource.COIN_MC]
        cache.save_crypto_list(df)
    df = processing.remove_stablecoin(df)
    df = projection.view_crypto_market(df)
    log.info("Phase ends", fetch="crypto", count=len(df))
    return df


def get_ohlcv() -> pd.DataFrame:
    """
    Retrieve OHLCV (Open, High, Low, Close, Volume) data for the last 30 trading days. Not including today.

    Checks the local store for stored data, determines which trading days are missing, and fetches those days from the
    provider pool.  Newly fetched data is appended to the store, ensuring the dataset remains complete.

    Returns:
        A DataFrame containing OHLCV data for the last 30 trading days.
    """
    log = timber.plant()
    log.info("Phase starts", fetch="ohlcv")
    exchange_dates = _get_last_trading(days=30)
    df = store.load_ohlcv(exchange_dates)

    if df.empty:
        missing = exchange_dates
    else:
        actual = pd.DatetimeIndex(df[COL_TIMESTAMP]).unique()
        missing = exchange_dates.difference(actual)

    if len(missing) > 0:
        frames = []
        for date in missing:
            df, provider = _POOL.fetch_ohlcv(date)
            frames.append(df)
        df = pd.concat(frames, ignore_index=True)
        store.save_ohlcv(df=df)

    df = projection.view_ohlcv(df)
    log.info("Phase ends", fetch="ohlcv", count=len(df))
    return df


def get_stock_listing() -> pd.DataFrame:
    """
    Retrieves a stock list derived from all known providers.

    Loads cached stock lists where available, fetches fresh data from remaining providers,
    and saves newly fetched results to cache. All data is projected into a canonical schema
    and combined using provider precedence.

    Returns:
        A single DataFrame containing the standardized stock list across all providers.
    """
    log = timber.plant()
    log.info("Phase starts", fetch="stock list")

    frames_cache = cache.load_stock_listings()
    frames_api = _POOL.fetch_stock_listings(except_from=list(frames_cache.keys()))
    if len(frames_api) > 0:
        for k, v in frames_api.items():
            cache.save_stock_list(df=v, provider=k)

    frames = frames_cache | frames_api
    for p in frames.keys():
        frames[p] = projection.view_stock_listing(frames[p])
    df = processing.merge_stock_listings(frames)

    log.info("Phase ends", fetch="stock list", count=len(df))
    return df


def get_stock_details(symbols: pd.Series) -> pd.DataFrame:
    """
    Retrieve all the detailed information for a list of stock symbols.

    For each symbol, attempts to load cached details. If unavailable, fetches from providers
    via the pool, caches the result, and then applies the symbol details projection. All results
    are combined into a single DataFrame.

    Args:
        symbols: Series of stock symbols to query.

    Returns:
        DataFrame containing standardized symbol details for all requested symbols.
    """
    log = timber.plant()
    log.info("Phase starts", fetch="stock details")

    details = []
    for symbol in symbols:
        df = cache.load_stock_details(symbol=symbol)
        if df.empty:
            df, provider = _POOL.fetch_stock_data(symbol)
            if df.empty:
                log.warning("No results from Pool", reason="unknown", symbol=symbol, action="manual investigation")
                continue  # rare but can happen if none of the providers support the given symbol
            cache.save_stock_details(df=df, provider=provider, symbol=symbol)
        df_proj = projection.view_stock_details(df)
        details.append(df_proj)

    df = pd.concat(details, ignore_index=True)
    log.info("Phase ends", fetch="stock details", count=len(df))
    return df


def _get_last_trading(days: int) -> pd.DatetimeIndex:
    """
    Gets the last trading days for the specified number of days. The return list of dates is ordered from the most
    recent backward.

    Args:
        days: The number of recent trading days to retrieve.

    Returns:
        A list of trading days as Pandas Timestamps, ordered from the most recent backward.
    """
    yesterday = pd.Timestamp.today().normalize() - pd.Timedelta(days=1)  # Free tier doesn't support today
    calendar = xcals.get_calendar("XNYS")  # Most exchanges in US follow same calendar
    lookback = yesterday - pd.Timedelta(days=days * 3)
    sessions = calendar.sessions_in_range(lookback, yesterday)
    sessions = sessions[-days:].to_list()
    sessions.reverse()
    return pd.DatetimeIndex(sessions)
