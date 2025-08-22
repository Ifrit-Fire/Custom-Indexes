import pandas as pd
from pandas import DataFrame, Timestamp, Series

from src import transform, timber
from src.config_handler import KEY_INDEX_TOP, KEY_INDEX_SORTBY, config
from src.consts import COL_SYMBOL, COL_MC, COL_VOLUME, COL_TYPE, ASSET_TYPES, COL_LIST_DATE, ASSET_CRYPTO


def standardize_symbols(series: pd.Series) -> pd.Series:
    """
    Transform ticker symbols into a standard format.

    Args:
        series (pd.Series): Pandas Series of ticker symbols.

    Returns:
        pd.Series: Series with normalized ticker symbols.
    """
    return series.str.upper().str.replace("-", ".", regex=False)


def exclude_asset_types(from_df: DataFrame, not_in: set[str]) -> DataFrame:
    """
    Exclude securities from the DataFrame whose asset type is not in a given set.

    Args:
        from_df (DataFrame): Input DataFrame containing at least the columns `COL_SYMBOL` and `COL_TYPE`.
        not_in (set[str]): Set of asset types to retain in the DataFrame. All others are excluded.

    Returns:
        DataFrame: Filtered DataFrame containing only rows with allowed asset types. The index is reset to start at 0.
    """
    log = timber.plant()
    mask = from_df[COL_TYPE].isin(not_in)
    excluded_df = from_df.loc[~mask, [COL_SYMBOL, COL_TYPE]]

    for _, row in excluded_df.iterrows():
        log.debug("Excluded", symbol=row[COL_SYMBOL], reason=row[COL_TYPE])
    log.info("Excluded", items="symbols", count=int((~mask).sum()), reason="Asset Type")

    return from_df[mask].reset_index(drop=True)


def refine_data(using: dict, dfs: list[DataFrame]) -> DataFrame:
    """
    Merge, sort, prune, and filter multiple security DataFrames into a final refined list
    based on configuration settings.

    Args:
        using (dict): Configuration dictionary containing at least
            `KEY_INDEX_SORTBY` and `KEY_INDEX_TOP`.
        dfs (list[DataFrame]): List of Pandas DataFrames containing security data.

    Returns:
        DataFrame: A new DataFrame meeting all filter and sorting criteria.
    """
    print(f"\tRefining data based on configurations...")
    print(f"\t...merging all security DataFrames...")
    df = pd.concat(dfs, axis=0, ignore_index=True)
    df = _merge_symbols(df)

    print(f"\t...sorting by {using[KEY_INDEX_SORTBY]}")
    col = transform.sort_by_to_df_column(using[KEY_INDEX_SORTBY])
    df = df.sort_values(by=col, ascending=False)

    count = len(df)
    df = df[df[COL_MC] > 0]
    print(f"\t...removed {count - len(df)} securities for having no market cap.")

    count = len(df)
    df = df[df[COL_VOLUME] > config.volume_limit_min]
    print(f"\t...removed {count - len(df)} securities for volume limit restriction.")

    count = len(df)
    df = _prune_by_list_date(df)
    print(f"\t...removed {count - len(df)} securities for list-date restriction.")

    df = df.head(using[KEY_INDEX_TOP])
    print(f"\t...trimmed down to {len(df)} securities")
    return df


def _prune_by_list_date(df: DataFrame) -> DataFrame:
    """
    Prune securities based on their list date, applying different minimum age
    requirements for crypto and stocks.

    The function:
      1. Normalizes all list dates into timezone-aware midnight datetimes.
      2. Computes cutoffs for crypto and stocks based on configuration rules.
      3. Removes any securities that fail the minimum age requirements.

    Args:
        df (DataFrame): DataFrame containing security data with at least `COL_LIST_DATE` and `COL_TYPE`.

    Returns:
        DataFrame: Filtered DataFrame containing only securities that meet the minimum age restrictions.

    Raises:
        Exception: If `COL_LIST_DATE` cannot be fully converted to datetime with `pandas.to_datetime`.
    """
    df = df.copy()
    df[COL_LIST_DATE] = pd.to_datetime(df[COL_LIST_DATE], format="mixed", utc=True, errors="raise").dt.normalize()
    today_utc = pd.Timestamp.now(tz="UTC").normalize()
    crypto_mask = _prune_by_date_mask(df, {ASSET_CRYPTO}, today_utc - config.crypto_age_min)
    stock_mask = _prune_by_date_mask(df, ASSET_TYPES - {ASSET_CRYPTO}, today_utc - config.stock_age_min)
    df = df[crypto_mask | stock_mask].reset_index(drop=True)
    return df


def _prune_by_date_mask(df: DataFrame, asset_types: set[str], cutoff: Timestamp) -> Series:
    """
    Build a boolean mask for filtering securities of specific asset types that meet a minimum list-date cutoff.

    Args:
        df (DataFrame): DataFrame containing security data, including `COL_TYPE` and `COL_LIST_DATE`.
        asset_types (set[str]): Set of asset type identifiers to check.
        cutoff (pd.Timestamp): Datetime cutoff; only securities listed on or before this date are kept.

    Returns:
        pd.Series: Boolean mask aligned with `df` indicating which rows should be kept.
    """
    mask = df[COL_TYPE].isin(asset_types)
    filtered = mask & df[COL_LIST_DATE].le(cutoff)
    removed = mask.sum() - filtered.sum()
    print(f"\t...removed {removed} for type(s) {str(asset_types)} based on min list-date restriction {cutoff.date()}.")
    return filtered


def _merge_symbols(df: DataFrame) -> DataFrame:
    """
    Merge market capitalization values for equivalent or alternate ticker symbols
    defined in `config.symbol_merge`.

    Works on a copy of `df` and returns the merged result.

    Args:
        df (DataFrame): Pandas DataFrame containing at least the `COL_SYMBOL` and `COL_MC` columns.

    Returns:
        DataFrame: A new DataFrame with merged symbols and adjusted market cap values.
    """
    df = df.copy()
    for merge, into in config.symbol_merge.items():
        merge_row = df.loc[df[COL_SYMBOL] == merge, COL_MC]
        if merge_row.empty: continue
        df = df[df[COL_SYMBOL] != merge]
        print(f"\t...removed {merge} Symbol")

        mask = df[COL_SYMBOL] == into
        if mask.any():
            df.loc[mask, COL_MC] += merge_row.iat[0]
            print(f"\t...added {merge} market cap into {into}")
    return df
