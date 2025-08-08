import pandas as pd
from pandas import DataFrame

from src import transform
from src.config_handler import KEY_INDEX_TOP, KEY_INDEX_SORTBY, config
from src.consts import COL_SYMBOL, COL_MC, COL_VOLUME


def refine_data(using: dict, dfs: list[DataFrame]) -> DataFrame:
    print(f"\tRefining data based on configurations...")
    print(f"\t...merging all security DataFrames...")
    df = pd.concat(dfs, axis=0, ignore_index=True)
    df = merge_symbols(df)

    print(f"\t...sorting by {using[KEY_INDEX_SORTBY]}")
    col = transform.sort_by_to_df_column(using[KEY_INDEX_SORTBY])
    df = df.sort_values(by=col, ascending=False)

    count = len(df)
    df = df[df[COL_VOLUME] > config.volume_limit_min()]
    print(f"\t...removed {count - len(df)} securities for volume limit restriction.")

    df = df.head(using[KEY_INDEX_TOP])
    print(f"\t...trimmed down to {len(df)} securities")
    return df


def merge_symbols(df: DataFrame) -> DataFrame:
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
