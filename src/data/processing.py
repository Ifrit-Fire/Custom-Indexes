import sys
import unicodedata

import datacompy
import pandas as pd

from src import transform
from src.config_handler import KEY_INDEX_TOP, KEY_INDEX_SORTBY, config
from src.consts import COL_SYMBOL, COL_MC, COL_VOLUME, COL_TYPE, COL_LIST_DATE, COL_COUNTRY, COL_NAME, COL_OUT_SHARES, \
    COL_MIC, COL_CIK, COL_FIGI, COL_STATE, COL_POSTAL_CODE
from src.data.security_types import CryptoTypes
from src.data.source import ProviderSource
from src.logger import timber


def merge_stock(listing: pd.DataFrame, with_details: pd.DataFrame) -> pd.DataFrame:
    log = timber.plant()
    log.info("Phase starts", merge="listing with details")
    rcol_name = "_right"

    datacompy.Compare(df1=listing, df2=with_details, join_columns=COL_SYMBOL)
    df = pd.merge(left=listing, right=with_details, on=COL_SYMBOL, how="left", suffixes=("", rcol_name))
    df = df.sort_values(COL_SYMBOL)
    listing = listing.sort_values(COL_SYMBOL)
    with_details = with_details.sort_values(COL_SYMBOL)

    def normalize(sym):
        return unicodedata.normalize("NFKC", str(sym)).strip().upper()

    def clean_symbol(s: str) -> str:
        return unicodedata.normalize("NFKC", s).strip()

    # listing[COL_SYMBOL] = listing[COL_SYMBOL].astype("string")
    # with_details[COL_SYMBOL] = with_details[COL_SYMBOL].astype("string")
    # listing[COL_SYMBOL] = listing[COL_SYMBOL].str.strip()
    # with_details[COL_SYMBOL] = with_details[COL_SYMBOL].str.strip()
    # listing_syms = set(listing[COL_SYMBOL].astype(str).map(normalize))
    # details_syms = set(with_details[COL_SYMBOL].astype(str).map(normalize))
    # missing = listing_syms - details_syms
    # print(f"Missing after normalization: {len(missing)}")
    # print("set norm: ", len(listing_syms))
    # print("set norm: ",len(details_syms))

    print("norm", len(listing[COL_SYMBOL]))
    print("norm", len(with_details[COL_SYMBOL]))
    print("unique", len(listing[COL_SYMBOL].unique()))
    print("unique", len(with_details[COL_SYMBOL].unique()))
    print(len(set(listing[COL_SYMBOL]) - set(with_details[COL_SYMBOL])))
    print(with_details[with_details[COL_SYMBOL] == "RELX"])
    print(listing[COL_SYMBOL].dtype)
    print(with_details[COL_SYMBOL].dtype)
    bad_syms = [s for s in set(listing[COL_SYMBOL]) - set(with_details[COL_SYMBOL]) if "RELX" in s]
    print(bad_syms)
    bad_syms = sorted([s for s in set(listing[COL_SYMBOL]) - set(with_details[COL_SYMBOL])])
    # for s in bad_syms:
    #     print(repr(s))

    dupes = listing[listing.duplicated(subset=[COL_SYMBOL], keep=False)]
    print(f"Duplicate symbols: {dupes[COL_SYMBOL].nunique()} unique, {len(dupes)} total rows")
    print(dupes.sort_values(COL_SYMBOL).head(10))

    dupes = with_details[with_details.duplicated(subset=[COL_SYMBOL], keep=False)]
    print(f"Duplicate symbols: {dupes[COL_SYMBOL].nunique()} unique, {len(dupes)} total rows")
    # print(dupes.sort_values(COL_SYMBOL).head(10))

    matches = with_details[with_details[COL_SYMBOL].str.contains("RELX", regex=True)]
    print(matches[COL_SYMBOL].apply(lambda x: (x, list(map(ord, x)))))

    for sym in with_details[COL_SYMBOL].unique():
        if "RELX" in sym and sym != "RELX":
            print(f"Found weird RELX variant: {repr(sym)} → {[ord(c) for c in sym]}")

    log.info("Phase ends", merge="listing with details", count=len(df))
    return df


def merge_stock_listings(frames: dict[ProviderSource, pd.DataFrame]) -> pd.DataFrame:
    """
    Merges multiple provider-sourced DataFrames of all market stock into a single consolidated DataFrame.

    Starts with the highest-precedence provider, then merges in additional sources on `COL_SYMBOL`. For overlapping
    columns, values from the higher-precedence provider take priority. Columns missing
    from one source are forward-filled from the other where possible. Only symbols found in all sources are kept.

    Args:
        frames: A dictionary mapping providers to their projected stock DataFrames.

    Returns:
        A merged DataFrame containing the combined stock listings from all providers.

    Notes:
        - In hand-testing of discrepancies, Finnhub data was consistently correct — it's trusted more.
        - Provider precedence is hardcoded.
        - A post-merge warning logs any columns that still contain NaNs.
    """
    log = timber.plant()
    log.info("Phase starts", merge="stock listings")
    precedence = [ProviderSource.FINNHUB, ProviderSource.POLYGON]
    rcol_name = "_right"

    order = [p for p in precedence if p in frames]
    log.info("Merging in provider", provider=order[0])
    df_accum = frames[order[0]]
    cols = df_accum.columns
    for provider in order[1:]:
        df_curr = frames[provider]
        log.info("Merging in provider", provider=provider)
        datacompy.Compare(df1=df_accum, df2=df_curr, join_columns=COL_SYMBOL)

        df_accum = pd.merge(left=df_accum, right=df_curr, on=COL_SYMBOL, how="left", suffixes=("", rcol_name))
        for col in cols:
            if col == COL_SYMBOL: continue
            rcol = f"{col}{rcol_name}"
            df_accum.loc[:, col] = df_accum[col].combine_first(df_accum[rcol])  # Mismatch tends to happen with `type`.
            df_accum.drop(labels=[rcol], axis=1, inplace=True)

    col_nans = df_accum.columns[df_accum.isna().any()].tolist()
    log_call = log.warning if len(col_nans) > 0 else log.debug
    log_call("Post Merge NaN", hasnan=col_nans)
    log.info("Phase ends", merge="stock listings", count=len(df_accum))
    return df_accum


def remove_stablecoin(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes all stablecoin entries from the given DataFrame.

    Args:
        df: A DataFrame containing crypto asset listings with a `COL_TYPE` column.

    Returns:
        A new DataFrame excluding rows classified as stablecoins.
    """
    return df[df[COL_TYPE] != CryptoTypes.STABLECOIN.value].copy()


def set_column_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies standardized pandas extension dtypes to known columns in the given DataFrame.

    If any column remains with 'object' dtype after conversion, logs a critical error and exits the program.

    Args:
        df: Input DataFrame with potentially mixed or uncoerced column types.

    Returns:
        A new DataFrame with standardized column types applied where recognized.
    """
    log = timber.plant()
    schema = {"active": pd.StringDtype(), "address.address1": pd.StringDtype(), "address.address2": pd.StringDtype(),
              "address.city": pd.StringDtype(), "branding.icon_url": pd.StringDtype(),
              "branding.logo_url": pd.StringDtype(), COL_CIK: pd.StringDtype(), COL_FIGI: pd.StringDtype(),
              COL_COUNTRY: pd.StringDtype(), "currency": pd.StringDtype(), "currency_name": pd.StringDtype(),
              "description": pd.StringDtype(), "displaySymbol": pd.StringDtype(), "estimateCurrency": pd.StringDtype(),
              "exchange": pd.StringDtype(), "finnhubIndustry": pd.StringDtype(), "homepage_url": pd.StringDtype(),
              "last_updated_utc": pd.StringDtype(), "isin": pd.StringDtype(), "locale": pd.StringDtype(),
              "logo": pd.StringDtype(), "market": pd.StringDtype(), COL_MC: pd.Float64Dtype(),
              COL_MIC: pd.StringDtype(), COL_NAME: pd.StringDtype(), COL_OUT_SHARES: pd.Float64Dtype(),
              "phone": pd.StringDtype(), "phone_number": pd.StringDtype(), COL_POSTAL_CODE: pd.StringDtype(),
              "round_lot": pd.UInt16Dtype(), "share_class_figi": pd.StringDtype(), "shareClassFIGI": pd.StringDtype(),
              "sic_code": pd.StringDtype(), "sic_description": pd.StringDtype(), COL_STATE: pd.StringDtype(),
              COL_SYMBOL: pd.StringDtype(), "symbol2": pd.StringDtype(), "total_employees": pd.UInt64Dtype(),
              "ticker_root": pd.StringDtype(), "ticker_suffix": pd.StringDtype(), COL_TYPE: pd.StringDtype(),
              "weburl": pd.StringDtype(), "weighted_shares_outstanding": pd.Float64Dtype()}

    # Apply dtype conversions for existing columns
    found_types = {col: dtype for col, dtype in schema.items() if col in df.columns}
    df = df.astype(found_types)

    if COL_LIST_DATE in df.columns:
        df[COL_LIST_DATE] = pd.to_datetime(df[COL_LIST_DATE], errors="coerce")

    bad_cols = df.columns[df.dtypes == "object"]
    if not bad_cols.empty:
        log.critical("Detected object dtype", columns=df.columns[df.dtypes == "object"].tolist())
        sys.exit()

    return df


def standardize_symbols(series: pd.Series) -> pd.Series:
    """
    Transform ticker symbols into a standard format.

    Args:
        series: Pandas Series of ticker symbols.

    Returns:
        Series with normalized ticker symbols.
    """
    return series.str.upper().str.replace("-", ".", regex=False)


def refine_data(using: dict, dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Merge, sort, and filter multiple security DataFrames into a final refined list
    based on configuration settings.

    Args:
        using: Configuration dictionary containing at least `KEY_INDEX_SORTBY` and `KEY_INDEX_TOP`.
        dfs: List of Pandas DataFrames containing security data.

    Returns:
        A new DataFrame meeting all filter and sorting criteria.
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


def _filter_by_list_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter securities based on their list date, applying different minimum age requirements for crypto and stocks.

    The function:
      1. Normalizes all list dates into timezone-aware midnight datetimes.
      2. Computes cutoffs for crypto and stocks based on configuration rules.
      3. Removes any securities that fail the minimum age requirements.

    Args:
        df: DataFrame containing security data with at least `COL_LIST_DATE` and `COL_TYPE`.

    Returns:
        Filtered DataFrame containing only securities that meet the minimum age restrictions.

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


def _filter_by_date_mask(df: pd.DataFrame, asset_types: set[str], cutoff: pd.Timestamp) -> pd.Series:
    """
    Build a boolean mask for filtering securities of specific asset types that meet a minimum list-date cutoff.

    Args:
        df: DataFrame containing security data, including `COL_TYPE` and `COL_LIST_DATE`.
        asset_types: Set of asset type identifiers to check.
        cutoff: Datetime cutoff; only securities listed on or before this date are kept.

    Returns:
        Boolean mask aligned with `df` indicating which rows should be kept.
    """
    log = timber.plant()
    mask = df[COL_TYPE].isin(asset_types)
    filtered_mask = mask & df[COL_LIST_DATE].le(cutoff)
    removed = df.loc[mask & ~filtered_mask, [COL_SYMBOL, COL_TYPE, COL_LIST_DATE]]
    for _, row in removed.iterrows():
        log.info("Filtered out", symbol=row[COL_SYMBOL], reason="list-date", asset_type=row[COL_TYPE],
                 listdate=row[COL_LIST_DATE], cutoff=cutoff)

    return filtered_mask


def _merge_symbols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge market capitalization values for equivalent or alternate ticker symbols defined in `config.symbol_merge`.

    Works on a copy of `df` and returns the merged result.

    Args:
        df: Pandas DataFrame containing at least the `COL_SYMBOL` and `COL_MC` columns.

    Returns:
        A new DataFrame with merged symbols and adjusted market cap values.
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
