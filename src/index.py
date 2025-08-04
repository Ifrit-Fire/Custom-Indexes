import numpy as np
from pandas import DataFrame

from src.consts import COL_WEIGHT, COL_MC, COL_SYMBOL, LIMIT_MIN_WEIGHT


def get_index(df: DataFrame, min_weight: int) -> DataFrame:
    # TODO: Figure out how to adjust index weighting based off min weight
    print("\tCreating weighted column...")
    df_index = df.copy()
    df_index[COL_WEIGHT] = round(df_index[COL_MC] / df_index[COL_MC].sum() * 100, 2)
    while df_index[COL_WEIGHT].min() < min_weight:
        symbol = df_index.iloc[-1][COL_SYMBOL]
        weight = df_index.iloc[-1][COL_WEIGHT]
        print(f"\t...dropping {symbol} with {weight:.2f}%")

        df_index = df_index.iloc[:-1]
        df_index[COL_WEIGHT] = round(df_index[COL_MC] / df_index[COL_MC].sum() * 100, 2)
    return _fix_rounding(df_index)


def _fix_rounding(df: DataFrame) -> DataFrame:
    # Let's fix rounding errors by using "Largest Remainder Method"
    df_exact = df[COL_MC] / df[COL_MC].sum() * 100
    floored = np.floor(df_exact * 10) / 10
    residual = round(100 - floored.sum(), 1)
    units = int(round(residual * 10))
    remainders = df_exact - floored

    # Now that we have a series representing all remainders we want to
    # 1) Sort by largest first
    # 2) Retrieve the first "Units" values from Series
    # 3) Bump those units by 0.1
    to_bump = remainders.sort_values(ascending=False).iloc[:units].index
    floored.loc[to_bump] += 0.1

    df[COL_WEIGHT] = floored
    return df
