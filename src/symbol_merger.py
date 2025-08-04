from pandas import DataFrame

from src.config_handler import config
from src.consts import COL_SYMBOL, COL_MC


def merge_symbols(df: DataFrame) -> DataFrame:
    for merge, into in config.symbol_merge.items():
        merge_row = df.loc[df[COL_SYMBOL] == merge, COL_MC]
        if merge_row.empty: continue
        df = df[df[COL_SYMBOL] != merge]
        print(f"\tRemoved {merge} Symbol")

        mask = df[COL_SYMBOL] == into
        if mask.any():
            df.loc[mask, COL_MC] += merge_row.iat[0]
            print(f"\tAdded {merge} market cap into {into}")
    return df
