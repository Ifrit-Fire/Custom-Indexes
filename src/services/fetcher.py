import pandas as pd

from src.clients.cmc import CMCProvider
from src.clients.finnhub import FinnhubProvider
from src.clients.polygon import PolygonProvider
from src.clients.providerpool import ProviderPool
from src.data import projection, processing
from src.data.source import ProviderSource
from src.io import cache
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
            if df.empty:
                log.warning("No results from Pool", reason="unknown", symbol=symbol, action="manual investigation")
                continue  # rare but can happen if none of the providers support the given symbol
            cache.save_symbol_details(df=df, provider=provider, symbol=symbol)
        df_proj = projection.view_symbol_details(df)
        details.append(df_proj)

    log.info("Phase ends", fetch="Symbol details")
    df = pd.concat(details, ignore_index=True)
    return df
