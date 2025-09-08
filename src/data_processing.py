import sys

import datacompy
import pandas as pd
from pandas import DataFrame, Timestamp, Series

from src import transform
from src.config_handler import KEY_INDEX_TOP, KEY_INDEX_SORTBY, config
from src.consts import COL_SYMBOL, COL_MC, COL_VOLUME, COL_TYPE, ASSET_TYPES, COL_LIST_DATE, ASSET_CRYPTO, COL_FIGI
from src.logger import timber


def _fillna(found_in: DataFrame, with_df: DataFrame, on_column: str) -> DataFrame:
    """
    Fill NaN values in one DataFrame with values from another DataFrame. Both DataFrames need to be aligned on the
    global `COL_SYMBOL` index.

    Args:
        found_in (DataFrame): The DataFrame in which NaN values should be filled.
        with_df (DataFrame): The DataFrame providing replacement values.
        on_column (str): The column name in which to fill missing values.

    Returns:
        DataFrame: A copy of `found_in` with NaN values in `on_column` filled from `with_df`, and the index reset
        back to `COL_SYMBOL`.
    """
    found_in = found_in.set_index(COL_SYMBOL)
    with_df = with_df.set_index(COL_SYMBOL)
    found_in[on_column] = found_in[on_column].fillna(with_df[on_column])
    return found_in.reset_index()


def merge_all_stock(df_finn: DataFrame, df_poly: DataFrame) -> DataFrame:
    """
    Compares two stock datasets (`df_finn` and `df_poly`) on their shared `COL_SYMBOL`, cleans and aligns them to
    ensure one-for-one matching, and then merges the non-overlapping columns into a consolidated result. Unique rows to
    either DataFrame are dropped. Polygon is known to contain missing values for `COL_FIGI`.  Finnhub is default value
    used for discrepancies on datasets. In testing of

    Args:
        df_finn (DataFrame): Stock data from the "finnhub" client.
        df_poly (DataFrame): Stock data from the "polygon" client.

    Returns:
        DataFrame: A merged DataFrame containing all rows that overlap on `COL_SYMBOL` between `df_finn` and `df_poly`

    Raises:
        SystemExit: If, after reconciliation, the DataFrames still do not have all rows overlapping on `COL_SYMBOL`.

    Notes:
        - In hand testing of some of the data discrepancies, Finnhub was always correct. Polygon was always wrong.
    """
    log = timber.plant()
    log.info("Phase starts", merge="all stock")
    cmp = datacompy.Compare(df_finn, df_poly, COL_SYMBOL)
    if not cmp.all_rows_overlap():
        log.debug("All rows overlap", result=False)
        print(cmp.report())

    if len(cmp.df1_unq_rows) > 0 or len(cmp.df2_unq_rows) > 0:
        log.info("Unique Rows Detected", action="remove")
        common = pd.Index(df_finn[COL_SYMBOL]).intersection(df_poly[COL_SYMBOL])
        mask1 = df_finn[COL_SYMBOL].isin(common)
        mask2 = df_poly[COL_SYMBOL].isin(common)
        df_finn = df_finn.loc[mask1].copy()
        df_poly = df_poly.loc[mask2].copy()

    if df_poly[COL_FIGI].hasnans:
        log.info("Nan detected", action="fill in", using="Other dataframe")
        df_poly = _fillna(found_in=df_poly, with_df=df_finn, on_column=COL_FIGI)

    log.info("Mismatched Types", action="Pick one")
    df_poly[COL_TYPE] = df_poly[COL_SYMBOL].map(df_finn.set_index(COL_SYMBOL)[COL_TYPE])

    cmp = datacompy.Compare(df_finn, df_poly, COL_SYMBOL)
    if not cmp.all_rows_overlap():
        log.critical("All rows overlap", result="fail", reason="not expected")
        sys.exit()

    log.info("All rows overlap", result="success")
    df2_unique = cmp.df2[cmp.df2_unq_columns() | {COL_SYMBOL}]
    df_merge = cmp.df1.merge(df2_unique, on=COL_SYMBOL, how='inner')
    log.info("Phase ends", merge="all stock", count=len(df_merge))
    return df_merge


def standardize_symbols(series: pd.Series) -> pd.Series:
    """
    Transform ticker symbols into a standard format.

    Args:
        series (pd.Series): Pandas Series of ticker symbols.

    Returns:
        pd.Series: Series with normalized ticker symbols.
    """
    return series.str.upper().str.replace("-", ".", regex=False)


def refine_data(using: dict, dfs: list[DataFrame]) -> DataFrame:
    """
    Merge, sort, and filter multiple security DataFrames into a final refined list
    based on configuration settings.

    Args:
        using (dict): Configuration dictionary containing at least
            `KEY_INDEX_SORTBY` and `KEY_INDEX_TOP`.
        dfs (list[DataFrame]): List of Pandas DataFrames containing security data.

    Returns:
        DataFrame: A new DataFrame meeting all filter and sorting criteria.
    """
    log = timber.plant()
    log.info("Phase starts", data="refinement")

    log.debug("Merging Dataframes", count=len(dfs))
    df = pd.concat(dfs, axis=0, ignore_index=True)
    df = _merge_symbols(df)

    col = transform.sort_by_to_df_column(using[KEY_INDEX_SORTBY])
    df = df.sort_values(by=col, ascending=False)
    log.info("Sorted", using=using[KEY_INDEX_SORTBY], column=col)

    count = len(df)
    df = df[df[COL_MC] > 0]
    log.debug("Filtered", count=count - len(df), reason="no market cap")

    count = len(df)
    df = df[df[COL_VOLUME] > config.volume_limit_min]
    log.info("Filtered", count=count - len(df), reason="volume", limit=config.volume_limit_min)

    count = len(df)
    df = _filter_by_list_date(df)

    df = df.head(using[KEY_INDEX_TOP])
    count = len(df)
    if count < using[KEY_INDEX_TOP]:
        log.critical("ValueError", reason="Unable to satisfy criteria", top=using[KEY_INDEX_TOP])
        raise ValueError(f"Not enough remaining securities for top {using[KEY_INDEX_TOP]} constraint")
    log.info("Selected", count=count, reason="top")

    log.info("Phase ends", data="refinement")
    return df


def _filter_by_list_date(df: DataFrame) -> DataFrame:
    """
    Filter securities based on their list date, applying different minimum age requirements for crypto and stocks.

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
    log = timber.plant()
    count = len(df)
    df = df.copy()

    df[COL_LIST_DATE] = pd.to_datetime(df[COL_LIST_DATE], format="mixed", utc=True, errors="raise").dt.normalize()
    today_utc = pd.Timestamp.now(tz="UTC").normalize()
    crypto_mask = _filter_by_date_mask(df, {ASSET_CRYPTO}, today_utc - config.crypto_age_min)
    stock_mask = _filter_by_date_mask(df, ASSET_TYPES - {ASSET_CRYPTO}, today_utc - config.stock_age_min)
    df = df[crypto_mask | stock_mask].reset_index(drop=True)

    log.info("Filtered", count=count - len(df), reason="list-date")
    return df


def _filter_by_date_mask(df: DataFrame, asset_types: set[str], cutoff: Timestamp) -> Series:
    """
    Build a boolean mask for filtering securities of specific asset types that meet a minimum list-date cutoff.

    Args:
        df (DataFrame): DataFrame containing security data, including `COL_TYPE` and `COL_LIST_DATE`.
        asset_types (set[str]): Set of asset type identifiers to check.
        cutoff (pd.Timestamp): Datetime cutoff; only securities listed on or before this date are kept.

    Returns:
        pd.Series: Boolean mask aligned with `df` indicating which rows should be kept.
    """
    log = timber.plant()
    mask = df[COL_TYPE].isin(asset_types)
    filtered_mask = mask & df[COL_LIST_DATE].le(cutoff)
    removed = df.loc[mask & ~filtered_mask, [COL_SYMBOL, COL_TYPE, COL_LIST_DATE]]
    for _, row in removed.iterrows():
        log.info("Filtered out", symbol=row[COL_SYMBOL], reason="list-date", asset_type=row[COL_TYPE],
                 listdate=row[COL_LIST_DATE], cutoff=cutoff)

    return filtered_mask


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
    log = timber.plant()
    df = df.copy()
    for merge, into in config.symbol_merge.items():
        merge_mask = df[COL_SYMBOL].eq(merge)
        if not merge_mask.any():
            log.debug("Not Found", symbol=merge, action="merge", into=into)
            continue

        into_mask = df[COL_SYMBOL].eq(into)
        if into_mask.any():
            # We found both "merge" and "into" within the dataset, remove "merge" and add it's mc to "into"
            market_cap = df.loc[merge_mask, COL_MC].iat[0]
            df = df.loc[~merge_mask]
            df.loc[into_mask, COL_MC] += market_cap
        else:
            # We only found "merge". The "into" symbol isn't in dataset. Just replace "merge" symbol with "into"
            df.loc[merge_mask, COL_SYMBOL] = into
        log.info("Merged", symbol=merge, into=into, method="market_cap")

    return df
