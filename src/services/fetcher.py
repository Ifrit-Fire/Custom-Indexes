import pandas as pd

from src.clients.finnhub import FinnhubProvider
from src.clients.polygon import PolygonProvider
from src.clients.providerpool import ProviderPool
from src.data import projection, processing
from src.io import cache
from src.logger import timber

_POOL = ProviderPool(providers=[FinnhubProvider(), PolygonProvider()])


def get_all_stock() -> pd.DataFrame:
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

    frames_cache = cache.load_stock_lists()
    frames_api = _POOL.fetch_all_stock(except_from=list(frames_cache.keys()))
    if len(frames_api) > 0:
        for k, v in frames_api.items():
            cache.save_stock_list(df=v, provider=k)

    frames = frames_cache | frames_api
    for p in frames.keys():
        frames[p] = projection.view_all_stock(frames[p])
    df = processing.merge_all_stock(frames)

    log.info("Phase ends", fetch="stock list", count=len(df))
    return df


def get_symbol_details(symbols: pd.Series) -> pd.DataFrame:
    """
    Retrieve all the detailed information for a list of symbols.

    For each symbol, attempts to load cached details. If unavailable, fetches from providers
    via the pool, caches the result, and then applies the symbol details projection. All results
    are combined into a single DataFrame.

    Args:
        symbols: Series of ticker symbols to query.

    Returns:
        DataFrame containing standardized symbol details for all requested symbols.
    """
    log = timber.plant()
    log.info("Phase starts", fetch="Symbol details")

    details = []
    for symbol in symbols:
        df = cache.load_symbol_details(symbol=symbol)
        if df.empty:
            df, provider = _POOL.fetch_symbol_data(symbol)
            if df.empty: continue  # rare but can happen if none of the providers support the given symbol
            cache.save_symbol_details(df=df, provider=provider, symbol=symbol)
        df_proj = projection.view_symbol_details(df)
        details.append(df_proj)

    log.info("Phase ends", fetch="Symbol details")
    df = pd.concat(details, ignore_index=True)
    return df
