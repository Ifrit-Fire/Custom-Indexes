from pathlib import Path

import pandas as pd

from consts import COL_TIMESTAMP, PATH_DATA_STORE_ROOT
from src.logger import timber

_COL_YEAR = "year"
_COL_MONTH = "month"
_COL_DAY = "day"
_NS_OHLCV = "ohlcv"


def load_ohlcv(date_range: pd.DatetimeIndex) -> pd.DataFrame:
    """
    Loads OHLCV data from a Parquet file based on the specified date range.  If the file is not found, an empty
     DataFrame is returned.

    Args:
        date_range: A range of dates to filter the OHLCV data.

    Returns:
        A DataFrame containing the OHLCV data filtered by the data_range
    """
    log = timber.plant()
    filters = [[(_COL_YEAR, "=", d.year), (_COL_MONTH, "=", d.month), (_COL_DAY, "=", d.day)] for d in date_range]
    filepath = _get_filepath(namespace=_NS_OHLCV)
    try:
        df = pd.read_parquet(filepath, filters=filters, engine="pyarrow")
    except FileNotFoundError:
        return pd.DataFrame()

    df[COL_TIMESTAMP] = pd.to_datetime(df[[_COL_YEAR, _COL_MONTH, _COL_DAY]])
    df = df.drop(columns=[_COL_YEAR, _COL_MONTH, _COL_DAY])
    log.debug("Load", file=filepath.name, type="parquet", count=len(df), path=filepath.parent)
    return df


def save_ohlcv(df: pd.DataFrame):
    """
    Saves Open, High, Low, Close, and Volume (OHLCV) data to a partitioned Parquet file.

    Args:
        DataFrame containing OHLCV data with a timestamp column.
    """
    log = timber.plant()
    df = df.copy()
    df[_COL_YEAR] = df[COL_TIMESTAMP].dt.year
    df[_COL_MONTH] = df[COL_TIMESTAMP].dt.month
    df[_COL_DAY] = df[COL_TIMESTAMP].dt.day
    df = df.drop(columns=[COL_TIMESTAMP])
    filepath = _get_filepath(namespace=_NS_OHLCV)

    df.to_parquet(filepath, engine="pyarrow", partition_cols=[_COL_YEAR, _COL_MONTH, _COL_DAY], index=False)
    log.debug("Saved", file=filepath.name, type="parquet", count=len(df), path=filepath.parent)


def _get_filepath(namespace: str) -> Path:
    """
    Generates a file path within the data store for the given namespace and ensures its parent directories exist.

    Args:
        namespace: A specific identifier used to create a corresponding file path in the data store.

    Returns:
        A path object representing the constructed file path within the data store.
    """
    filepath = PATH_DATA_STORE_ROOT / namespace
    filepath.parent.mkdir(parents=True, exist_ok=True)
    return filepath
