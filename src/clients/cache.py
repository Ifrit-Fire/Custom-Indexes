from datetime import date
from pathlib import Path

import pandas as pd
from pandas import DataFrame

from src import timber
from src.consts import PATH_DATA_CACHE_ROOT


def store_api_cache(basename: str, criteria: dict, df: DataFrame):
    """
    Save API query results to a local pickle file for caching.

    Args:
        basename (str): Base filename for the cache file.
        criteria (dict): Dictionary of criteria used to build a unique cache filename.
        df (DataFrame): DataFrame to be cached.
    """
    log = timber.plant()
    filepath = _get_file_name(basename, criteria)
    df.to_pickle(filepath)
    log.debug("Saved", file=filepath.name, type="pickle", count=len(df), path=str(filepath.parent))


def grab_api_cache(basename: str, criteria: dict) -> DataFrame:
    """
    Load API query results from a local pickle file if available.

    Args:
        basename (str): Base filename for the cache file.
        criteria (dict): Dictionary of criteria used to locate the cache file.

    Returns:
        DataFrame: Cached DataFrame if found, otherwise an empty DataFrame.
    """
    log = timber.plant()
    filepath = _get_file_name(basename, criteria)
    if filepath.exists():
        df = pd.read_pickle(filepath)
        log.debug("Load", file=filepath.name, type="pickle", count=len(df), path=str(filepath.parent))
    else:
        df = DataFrame()
    return df


def _get_file_name(basename: str, criteria: dict) -> Path:
    """
    Build a unique cache file path based on the current date and given criteria.

    Args:
        basename (str): Base name for the cache directory.
        criteria (dict): Dictionary of parameters used to generate a unique filename.
            Keys and values are concatenated and joined with '__'.

    Returns:
        Path: Path object pointing to the generated pickle file location.

    Note:
        The resulting path is structured as: `PATH_DATA_CACHE_ROOT / basename / "<YYYY-MM-DD>__<criteria>.pkl"`
    """
    today_str = date.today().isoformat()
    param_str = "__".join(f"{k}{v}" for k, v in sorted(criteria.items()))
    filepath = PATH_DATA_CACHE_ROOT / basename / f"{today_str}__{param_str}.pkl"
    filepath.parent.mkdir(parents=True, exist_ok=True)
    return filepath
