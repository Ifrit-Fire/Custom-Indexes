import pandas as pd
from pandas import DataFrame

from src import transform
from src.clients.polygon import get_stock
from src.config_handler import KEY_INDEX_TOP, KEY_INDEX_SORTBY, config
from src.consts import COL_SYMBOL, COL_MC, COL_VOLUME, COL_TYPE, ASSET_TYPES


def normalize_symbols(series: pd.Series) -> pd.Series:
    """
    Normalize ticker symbols to a standard format.

    Args:
        series (pd.Series): Pandas Series of ticker symbols.

    Returns:
        pd.Series: Series with normalized ticker symbols.
    """
    return series.str.upper().str.replace("-", ".", regex=False)


def tag_prune_stock_asset_type(df: DataFrame) -> DataFrame:
    """
    Tags each security in the DataFrame with its asset type, then filters to include only allowed stock asset types.

    ⚠️ This function will raise an error if a non-stock (e.g., crypto) symbol is provided.

    Args:
        df (pd.DataFrame): Pandas DataFrame containing at least the `COL_SYMBOL` column.

    Returns:
        pd.DataFrame: DataFrame filtered to only rows with allowed asset types, with `COL_TYPE`
        added and the index reset to start at 0.

    Notes:
        This function will make an API call once per symbol if it cannot find the stored
        information on disk.
    """
    df = df.copy()
    df[COL_TYPE] = df[COL_SYMBOL].map(lambda sym: get_stock(sym).type)
    mask = df[COL_TYPE].isin(ASSET_TYPES)
    pruned = df.loc[~mask, COL_SYMBOL].tolist()
    print(f"\t...pruned {(~mask).sum()} assets by type:")
    print("\n".join(f"\t\t{sym}" for sym in pruned))

    return df[mask].reset_index(drop=True)


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
    df = df[df[COL_VOLUME] > config.volume_limit_min()]
    print(f"\t...removed {count - len(df)} securities for volume limit restriction.")

    df = df.head(using[KEY_INDEX_TOP])
    print(f"\t...trimmed down to {len(df)} securities")
    return df


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
