import pandas as pd

from src.consts import COL_SYMBOL, COL_NAME, COL_MIC, COL_CIK, COL_FIGI, COL_MC, COL_LIST_DATE, COL_OUT_SHARES, \
    COL_STATE, COL_POSTAL_CODE, COL_COUNTRY


def view_symbol_details(df: pd.DataFrame) -> pd.DataFrame:
    """
    Project a DataFrame into the canonical symbol details view. Ensures all expected columns exist, filling missing
    ones with NA, and reorders columns into the standard schema.

    Args:
        df (pd.DataFrame): Input DataFrame with symbol details (Provider-agnostic).

    Returns:
        pd.DataFrame: DataFrame with standardized symbol details columns.
    """
    columns = [COL_SYMBOL, COL_NAME, COL_MC, COL_MIC, COL_LIST_DATE, COL_CIK, COL_FIGI, COL_OUT_SHARES, COL_POSTAL_CODE,
               COL_STATE, COL_COUNTRY]

    for col in columns:
        if col not in df.columns:
            df[col] = pd.NA
    return df[columns]
