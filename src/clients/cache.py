from datetime import date
from pathlib import Path

import pandas as pd
from pandas import DataFrame

from src.consts import PATH_DATA_CACHE_ROOT


def store_api_cache(basename: str, criteria: dict, df: DataFrame):
    filepath = _get_file_name(basename, criteria)
    df.to_pickle(filepath)


def grab_api_cache(basename: str, criteria: dict) -> DataFrame:
    filepath = _get_file_name(basename, criteria)
    return pd.read_pickle(filepath) if filepath.exists() else DataFrame()


def _get_file_name(basename: str, criteria: dict) -> Path:
    today_str = date.today().isoformat()
    param_str = "__".join(f"{k}{v}" for k, v in sorted(criteria.items()))
    filepath = PATH_DATA_CACHE_ROOT / basename / f"{today_str}__{param_str}.pkl"
    filepath.parent.mkdir(parents=True, exist_ok=True)
    return filepath
