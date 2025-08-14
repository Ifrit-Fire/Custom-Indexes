import pandas as pd
from pandas import DataFrame

from src import transform
from src.clients.polygon import get_stock
from src.config_handler import KEY_INDEX_TOP, KEY_INDEX_SORTBY, config
from src.consts import COL_SYMBOL, COL_MC, COL_VOLUME


def prune_asset_type(df: DataFrame) -> DataFrame:
    """
    Filter the DataFrame to include only the following allowed asset types.
    - "CS"   (Common Stock)
    - "ADRC" (American Depository Receipt - Common)
    - "OS"   (Ordinary Shares)

    :param df: Pandas DataFrame containing at least the `COL_SYMBOL` column.
    :return: A new DataFrame containing only rows with allowed asset types,
             reindexed from 0.

    Note:
        This function will make an API call once per symbol if it cannot find the stored information on disk.
    """

    def _is_allowed(symbol: str) -> bool:
        td = get_stock(symbol)
        return td.type in {"CS", "ADRC", "OS"}

    mask = df[COL_SYMBOL].map(_is_allowed)
    print(f"\tPruned {(~mask).sum()} assets by type")
    return df[mask].reset_index(drop=True)


def refine_data(using: dict, dfs: list[DataFrame]) -> DataFrame:
    """
    Merge, sort, prune, and filter multiple security DataFrames into a final refined list based on configuration
    settings.

    :param using: Configuration dictionary containing at least `KEY_INDEX_SORTBY` and `KEY_INDEX_TOP`.
    :param dfs: List of Pandas DataFrames containing security data.
    :return: A new DataFrame meeting all filter and sorting criteria.
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

    :param df: Pandas DataFrame containing at least the `COL_SYMBOL`, `COL_MC` columns
    :return: A new DataFrame with merged symbols and adjusted market cap values.
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
