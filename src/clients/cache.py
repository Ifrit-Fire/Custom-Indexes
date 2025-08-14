from datetime import date
from pathlib import Path

import pandas as pd
from pandas import DataFrame

from src.consts import PATH_DATA_CACHE_ROOT


def store_api_cache(basename: str, criteria: dict, df: DataFrame):
    """
    Save API query results to a local pickle file for caching.

    :param basename: Base filename for the cache file.
    :param criteria: Dictionary of criteria used to build a unique cache filename.
    :param df: DataFrame to be cached.
    """
    filepath = _get_file_name(basename, criteria)
    df.to_pickle(filepath)


def grab_api_cache(basename: str, criteria: dict) -> DataFrame:
    """
    Load API query results from a local pickle file if available.

    :param basename: Base filename for the cache file.
    :param criteria: Dictionary of criteria used to locate the cache file.
    :return: Cached DataFrame if found, otherwise an empty DataFrame.
    """
    filepath = _get_file_name(basename, criteria)
    return pd.read_pickle(filepath) if filepath.exists() else DataFrame()


def _get_file_name(basename: str, criteria: dict) -> Path:
    """
    Build a unique cache file path based on the current date and given criteria.

    :param basename: Base name for the cache directory.
    :param criteria: Dictionary of parameters used to generate a unique filename. Keys and values are concatenated and
    joined with '__'.
    :return: Path object pointing to the generated pickle file location.

    Note:
        The resulting path is structured as:
        PATH_DATA_CACHE_ROOT / basename / "<YYYY-MM-DD>__<criteria>.pkl"
    """
    today_str = date.today().isoformat()
    param_str = "__".join(f"{k}{v}" for k, v in sorted(criteria.items()))
    filepath = PATH_DATA_CACHE_ROOT / basename / f"{today_str}__{param_str}.pkl"
    filepath.parent.mkdir(parents=True, exist_ok=True)
    return filepath
