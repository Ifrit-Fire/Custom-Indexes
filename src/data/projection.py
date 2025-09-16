from typing import Sequence

import pandas as pd

from src.consts import COL_SYMBOL, COL_NAME, COL_MIC, COL_CIK, COL_FIGI, COL_MC, COL_LIST_DATE, COL_OUT_SHARES, \
    COL_STATE, COL_POSTAL_CODE, COL_COUNTRY, COL_TYPE


def view_all_stock(df: pd.DataFrame) -> pd.DataFrame:
    """
    Projects a DataFrame into the canonical stock list view. Ensures all expected columns exist, fills missing
    ones with NA, and reorders columns into the standard schema.

    Args:
        df: Input DataFrame with stock listings (Provider-agnostic).

    Returns:
        DataFrame with standardized stock listing columns.
    """
    columns = [COL_SYMBOL, COL_NAME, COL_TYPE, COL_MIC, COL_CIK, COL_FIGI]
    return _view(df, columns)


def view_symbol_details(df: pd.DataFrame) -> pd.DataFrame:
    """
    Projects a DataFrame into the canonical symbol details view. Ensures all expected columns exist, fills missing
    ones with NA, and reorders columns into the standard schema.

    Args:
        df: Input DataFrame with symbol details (Provider-agnostic).

    Returns:
        DataFrame with standardized symbol details columns.
    """
    columns = [COL_SYMBOL, COL_NAME, COL_MC, COL_MIC, COL_LIST_DATE, COL_CIK, COL_FIGI, COL_OUT_SHARES, COL_POSTAL_CODE,
               COL_STATE, COL_COUNTRY]
    return _view(df, columns)


def _view(df: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    """
    Returns a projected view of the DataFrame with specified columns. Missing columns are added and filled with pd.NA.

    Args:
        df: Source DataFrame.
        columns: Desired column names, in output order.

    Returns:
        A new copied DataFrame with all specified columns, in order.
    """
    for col in columns:
        if col not in df.columns:
            df[col] = pd.NA
    return df[columns].copy()
