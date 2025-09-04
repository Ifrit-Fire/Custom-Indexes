from datetime import datetime, timezone, date
from pathlib import Path

import pandas as pd
from pandas import DataFrame

from src.consts import PATH_DATA_CACHE_ROOT
from src.logger import timber


def save_api_cache(basename: str, criteria: dict, df: DataFrame):
    """
    Save API query results to a local pickle file for caching. The cached file additionally stores some metadata to
    track creation date.

    Args:
        basename (str): Base filename for the cache file.
        criteria (dict): Dictionary of criteria used to build a unique cache filename.
        df (pd.DataFrame): DataFrame to be cached.
    """
    log = timber.plant()
    filepath = _get_file_name(basename, criteria)
    payload = {"created": datetime.now(timezone.utc).date().isoformat(),  #
               "data": df}
    pd.to_pickle(payload, filepath)
    log.debug("Saved", file=filepath.name, type="pickle", count=len(df), path=filepath.parent)


def load_api_cache(basename: str, criteria: dict, allow_stale=False) -> DataFrame:
    """
    Attempt to load API query results from cache.

    The cache file is identified by `basename` and `criteria`, and stores both the
    DataFrame (`data`) and its creation date (`created`). By default, only cache files
    created on the current UTC date are considered valid. If `allow_stale` is True,
    older cache files will also be accepted.

    Args:
        basename (str): Base filename for the cache file.
        criteria (dict): Dictionary of criteria used to locate the cache file.
        allow_stale (bool, optional): Flag for allowing the loading of older "stale" cache files. Defaults to False.

    Returns:
        pd.DataFrame: DataFrame loaded from cache if valid, otherwise an empty DataFrame.
    """
    log = timber.plant()
    filepath = _get_file_name(basename, criteria)
    if not filepath.exists():
        return DataFrame()

    payload = pd.read_pickle(filepath)
    created = date.fromisoformat(payload["created"])
    today = datetime.now(timezone.utc).date()
    if created < today and not allow_stale:
        return DataFrame()

    df = payload["data"]
    log.debug("Load", file=filepath.name, type="pickle", count=len(df), created=created, path=filepath.parent)
    return df


def _get_file_name(basename: str, criteria: dict) -> Path:
    """
    Build a unique cache file path based on the given criteria.

    Args:
        basename (str): Base name for the cache directory.
        criteria (dict): Dictionary of parameters used to generate a unique filename.
            Keys and values are concatenated and joined with '__'.

    Returns:
        Path: Path object pointing to the generated pickle file location.

    Note:
        The resulting path is structured as: `PATH_DATA_CACHE_ROOT / basename / "snapshot__<criteria>.pkl"`
    """
    param_str = "__".join(f"{k}={v}" for k, v in sorted(criteria.items()))
    filepath = PATH_DATA_CACHE_ROOT / basename / f"snapshot__{param_str}.pkl"
    filepath.parent.mkdir(parents=True, exist_ok=True)
    return filepath
