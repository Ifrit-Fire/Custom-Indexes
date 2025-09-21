from datetime import datetime, timezone, date
from pathlib import Path

import pandas as pd
from dateutil.relativedelta import relativedelta

from src.consts import PATH_DATA_CACHE_ROOT
from src.data.source import ProviderSource
from src.logger import timber

_ID_CRYPTO = "crypto"
_ID_STOCK = "stocks"
_KEY_DATA = "data"
_KEY_EXPIRES = "expires"
_NS_DEFAULT = "snapshot"
_NS_LIST = "lists"
_NS_SYMBOLS = "symbols"


def save_crypto_list(df: pd.DataFrame):
    """
    Save a snapshot of a crypto list to the cache with weekly expiration. The file is placed under the `lists`
    namespace and identified by a hardcoded provider name.

    Args:
        df: The crypto list DataFrame to persist.
    """
    save(data=df, namespace=_NS_LIST, name=ProviderSource.COIN_MC.value, identifier=_ID_CRYPTO, by_sharding=False,
         expires_on=datetime.now(timezone.utc) + relativedelta(weeks=1))


def save_stock_list(df: pd.DataFrame, provider: ProviderSource):
    """
    Save a snapshot of a stock list to the cache with a quarterly expiration. The file is placed under the `lists`
    namespace and identified by the provider name.

    Args:
        df: The stock list DataFrame to persist.
        provider: The provider that supplied the data.
    """
    save(data=df, namespace=_NS_LIST, name=provider.value, identifier=_ID_STOCK, by_sharding=False,
         expires_on=datetime.now(timezone.utc) + relativedelta(months=3))


def save_stock_details(df: pd.DataFrame, provider: ProviderSource, symbol: str):
    """
    Save stock details data to the cache with sharding enabled. The file is placed under the `symbols` namespace and
    stored in a sharded folder based upon the ticker symbol. No data expiration is set.

    Args:
        df: The stock detailed DataFrame to persist.
        provider: The provider that supplied the data.
        symbol: The stock symbol the data represents.
    """
    save(data=df, namespace=_NS_SYMBOLS, name=symbol, identifier=provider.value, by_sharding=True)


def save(data: pd.DataFrame, name: str, identifier: str, by_sharding: bool = False, expires_on: datetime = None,
         namespace: str = _NS_DEFAULT):
    """
    Save a DataFrame snapshot to a pickle file with optional sharding and expiration settings.

    Expiration dates indicate when the data should be refreshed. If no expiration is provided, the default is 30 years
    from the current time. Files are organized under the given `namespace` and optionally sharded by the first letter
    of `name`.

    Args:
        data: The DataFrame to persist.
        name: Main component of the filename.
        identifier: A unique identifier appended to the filename.
        by_sharding: Optional. If True, places the file under a folder with the first letter of `name`.
                     Defaults to False.
        expires_on: Optional. The expiration date for the saved data.
                    Defaults to 30 years if not provided.
        namespace: Optional. Parent folder for grouping saved data.
                    Defaults to `_NS_DEFAULT`.
    """
    log = timber.plant()
    filepath = _get_filepath(namespace=namespace, name=name, identifier=identifier, sharding=by_sharding)
    if not expires_on:
        expires_on = datetime.now(timezone.utc) + relativedelta(years=30)
    payload = {_KEY_EXPIRES: expires_on.date().isoformat(),  #
               _KEY_DATA: data}
    pd.to_pickle(payload, filepath)
    log.debug("Saved", file=filepath.name, type="pickle", count=len(data), path=filepath.parent)


def load_crypto_lists() -> pd.DataFrame:
    """
    Loads cached crypto list snapshot for a hardcoded provider.

    Returns:
        The cached DataFrame if available and valid, otherwise an empty DataFrame.
    """
    return load(namespace=_NS_LIST, name=ProviderSource.COIN_MC.value, identifier=_ID_CRYPTO, by_sharding=False,
                allow_stale=False)


def load_stock_listings(provider: ProviderSource = None) -> dict[ProviderSource, pd.DataFrame]:
    """
    Loads one or more cached stock list snapshots for the given provider(s).

    Args:
        provider: Optional. If specified, loads only that provider's stock list.
                  If None, attempts to load all known providers.

    Returns:
        A dictionary mapping each provider to its stock list DataFrame.
        Providers with no valid cached data are omitted.
        Returns an empty dictionary if nothing is found.
    """
    if provider:
        df = load(namespace=_NS_LIST, name=provider.value, identifier=_ID_STOCK, by_sharding=False, allow_stale=False)
        return {provider: df}

    frames = {}
    for provider in ProviderSource:
        df = load(namespace=_NS_LIST, name=provider.value, identifier=_ID_STOCK, by_sharding=False, allow_stale=False)
        if not df.empty: frames |= {provider: df}
    return frames


def load_stock_details(symbol: str, provider: ProviderSource = None) -> pd.DataFrame:
    """
    Load cached stock detailed data for a symbol. If `provider` is given, load from that source. If not, check
    providers in preferred order and return the first result found. Cached data is effectively non-expiring.

    Args:
        symbol: The stock symbol to load.
        provider: Optional. The provider to load from. If None, precedence rules apply.

    Returns:
        Cached stock details if available, otherwise an empty DataFrame.
    """
    if provider:
        return load(namespace=_NS_SYMBOLS, name=symbol, identifier=provider.value, by_sharding=True, allow_stale=True)

    for provider in [ProviderSource.POLYGON, ProviderSource.FINNHUB]:  # In preferred order
        df = load(namespace=_NS_SYMBOLS, name=symbol, identifier=provider.value, by_sharding=True, allow_stale=True)
        if not df.empty: return df
    return pd.DataFrame()


def load(name: str, identifier: str, by_sharding: bool = False, namespace: str = _NS_DEFAULT,
         allow_stale: bool = False) -> pd.DataFrame:
    """
    Load a cached DataFrame snapshot from a pickle file. If the cache file does not exist, an empty DataFrame is
    returned. Expired data is also ignored unless `allow_stale` is set to True.

    Args:
        name: Main component of the filename.
        identifier: Unique identifier appended to the filename.
        by_sharding: Optional. If True, load the file from a folder with the first letter of `name`.
                     Defaults to False.
        namespace: Optional. Parent folder grouping cached data.
                   Defaults to `_NS_DEFAULT`.
        allow_stale: Optional. If True, expired cache entries are returned instead of being ignored.
                     Defaults to False.

    Returns:
        The cached DataFrame if available and valid, otherwise an empty DataFrame.
    """
    log = timber.plant()
    filepath = _get_filepath(namespace=namespace, name=name, identifier=identifier, sharding=by_sharding)
    if not filepath.exists():
        return pd.DataFrame()

    payload = pd.read_pickle(filepath)
    expires = date.fromisoformat(payload[_KEY_EXPIRES])
    today = datetime.now(timezone.utc).date()
    if expires < today and not allow_stale:
        return pd.DataFrame()

    df = payload[_KEY_DATA]
    log.debug("Load", file=filepath.name, type="pickle", count=len(df), expires=expires, path=filepath.parent)
    return df


def _get_filepath(namespace: str, name: str, identifier: str, sharding: bool) -> Path:
    """
    Build the file path for cached data. The file is placed under the given `namespace`
    and optionally sharded by the first letter of `name`. The filename is constructed
    using `name` and `identifier` joined with `__` and saved as a pickle file.

    Args:
        namespace: Parent folder grouping the cached data.
        name: Main component of the filename.
        identifier: Unique identifier appended to the filename.
        sharding: If True, place the file under a folder with the first letter of `name`.

    Returns:
        The full file path for the cache entry.
    """
    shard = name[0] if sharding else ""
    filepath = PATH_DATA_CACHE_ROOT / namespace / shard / f"{name}__{identifier}.pkl"
    filepath.parent.mkdir(parents=True, exist_ok=True)
    return filepath
