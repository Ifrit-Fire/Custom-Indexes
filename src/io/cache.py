from datetime import datetime, timezone, date
from pathlib import Path

import pandas as pd
from dateutil.relativedelta import relativedelta

from src.consts import PATH_DATA_CACHE_ROOT
from src.logger import timber

_KEY_EXPIRES = "expires"
_KEY_DATA = "data"

def save_stock_list(df: pd.DataFrame, provider: str, filters: set):
    """
    Save a snapshot of a stock list to the cache with a quarterly expiration. The file is placed under the `lists`
    namespace and identified by the provider name combined with the given filters.

    Args:
        df (pd.DataFrame): The stock list DataFrame to persist.
        provider (str): The provider that supplied the data.
        filters (set): The set of filters used when pulling the data from the provider.
    """
    ids = "__".join(sorted(filters))
    save(data=df, namespace="lists", name=provider, identifier=ids, by_sharding=False,
         expires_on=datetime.now(timezone.utc) + relativedelta(months=3))


def save_symbol_details(df: pd.DataFrame, provider: str, symbol: str):
    """
    Save ticker detail data to the cache with sharding enabled. The file is placed under the `symbols` namespace and
    stored in a sharded folder based upon the ticker symbol. No data expiration is set.

    Args:
        df (pd.DataFrame): The ticker detail DataFrame to persist.
        provider (str): The provider that supplied the data.
        symbol (str): The ticker the data represents.
    """
    save(data=df, namespace="symbols", name=symbol, identifier=provider, by_sharding=True)


def save(data: pd.DataFrame, name: str, identifier: str, by_sharding: bool = False, expires_on: datetime = None,
         namespace: str = "snapshot"):
    """
    Save a DataFrame snapshot to a pickle file with optional sharding and expiration settings.

    Expiration dates indicate when the data should be refreshed. If no expiration is provided, the default is 30 years
    from the current time. Files are organized under the given `namespace` and optionally sharded by the first letter
    of `name`.

    Args:
        data (pd.DataFrame): The DataFrame to persist.
        name (str): Main component of the filename.
        identifier (str): A unique identifier appended to the filename.
        by_sharding (bool, optional): If True, places the file under a folder with the first letter of `name`.
            Defaults to False.
        expires_on (datetime, optional): The expiration date for the saved data.
            Defaults to 30 years if not provided.
        namespace (str, optional): Parent folder for grouping saved data.
            Defaults to "snapshot".
    """
    log = timber.plant()
    filepath = _get_filepath(namespace=namespace, name=name, identifier=identifier, sharding=by_sharding)
    if not expires_on:
        expires_on = datetime.now(timezone.utc) + relativedelta(years=30)
    payload = {_KEY_EXPIRES: expires_on.date().isoformat(),  #
               _KEY_DATA: data}
    pd.to_pickle(payload, filepath)
    log.debug("Saved", file=filepath.name, type="pickle", count=len(data), path=filepath.parent)


def load_stock_list(provider: str, filters: set) -> pd.DataFrame:
    """
    Load a snapshot of a stock list. The cache entry is identified by the provider name combined with the given
    filters. Expired data is ignored.

    Args:
        provider (str): The provider that supplied the data.
        filters (set): The filters used when pulling the data from the provider.

    Returns:
        pd.DataFrame: The cached stock list if available and valid, otherwise an empty DataFrame.
    """
    ids = "__".join(sorted(filters))
    return load(namespace="lists", name=provider, identifier=ids, by_sharding=False, allow_stale=False)


def load_symbol_details(provider: str, symbol: str) -> pd.DataFrame:
    """
    Load cached ticker detail data. This data never expire in practice.

    Args:
        provider (str): The provider that supplied the data.
        symbol (str): The ticker the data represents.

    Returns:
        pd.DataFrame: The cached ticker detail data if available, otherwise an empty DataFrame.
    """
    return load(namespace="symbols", name=symbol, identifier=provider, by_sharding=True, allow_stale=True)


def load(name: str, identifier: str, by_sharding: bool = False, namespace: str = "snapshot",
         allow_stale: bool = False) -> pd.DataFrame:
    """
    Load a cached DataFrame snapshot from a pickle file. If the cache file does not exist, an empty DataFrame is
    returned. Expired data is also ignored unless `allow_stale` is set to True.

    Args:
        name (str): Main component of the filename.
        identifier (str): Unique identifier appended to the filename.
        by_sharding (bool, optional): If True, load the file from a folder with the first letter of `name`.
            Defaults to False.
        namespace (str, optional): Parent folder grouping cached data.
            Defaults to "snapshot".
        allow_stale (bool, optional): If True, expired cache entries are returned instead of being ignored.
            Defaults to False.

    Returns:
        pd.DataFrame: The cached DataFrame if available and valid, otherwise an empty DataFrame.
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
        namespace (str): Parent folder grouping the cached data.
        name (str): Main component of the filename.
        identifier (str): Unique identifier appended to the filename.
        sharding (bool): If True, place the file under a folder with the first letter of `name`.

    Returns:
        Path: The full file path for the cache entry.
    """
    shard = name[0] if sharding else ""
    filepath = PATH_DATA_CACHE_ROOT / namespace / shard / f"{name}__{identifier}.pkl"
    filepath.parent.mkdir(parents=True, exist_ok=True)
    return filepath
