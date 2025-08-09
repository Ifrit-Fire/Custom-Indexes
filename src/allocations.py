from decimal import Decimal

import numpy as np
from pandas import DataFrame

from src.config_handler import KEY_INDEX_WEIGHT_MIN
from src.consts import COL_WEIGHT, COL_MC, COL_SYMBOL


def add_weightings(df: DataFrame, criteria: dict) -> DataFrame:
    min_weight = criteria[KEY_INDEX_WEIGHT_MIN]
    precision = _decimal_places(min_weight) + 2

    print("\tCreating weighted column...")
    df_index = df.copy()
    df_index[COL_WEIGHT] = round(df_index[COL_MC] / df_index[COL_MC].sum() * 100, precision)
    while df_index[COL_WEIGHT].min() < min_weight:
        symbol = df_index.iloc[-1][COL_SYMBOL]
        weight = df_index.iloc[-1][COL_WEIGHT]
        print(f"\t...dropping {symbol} with {weight:.{precision}f}%")

        df_index = df_index.iloc[:-1]
        df_index[COL_WEIGHT] = round(df_index[COL_MC] / df_index[COL_MC].sum() * 100, precision)
    return _fix_rounding(df_index, precision)


def _fix_rounding(df: DataFrame, precision: int) -> DataFrame:
    """
    Fixes rounding errors by using "Largest Remainder Method". Further, to avoid floating point representation issues,
    all values are moved into integer space. Precision controls how much of a given number needs moving into an integer.

    :param df: DataFrame reference for marketcap
    :param precision: How many decimals to include, though calculated results will be precision - 1
    :return: DataFrame with new weight column added.
    """
    scale = 10 ** (precision - 1)
    s_exact_units = df[COL_MC] / df[COL_MC].sum() * 100 * scale  # Scale moves exact % to integer space
    s_base_units = np.floor(s_exact_units).astype(int)  # What we'll increase to fix rounding errors
    s_remainders = s_exact_units - s_base_units  # used for determining who gets the increase

    # how many 1-unit bumps we still need to reach exactly 100%
    units_to_add = int(round(100 * scale - s_base_units.sum()))

    if units_to_add > 0:
        # Now that we have a series representing all remainders we want to
        # 1) Sort by largest first
        # 2) Retrieve the first "Units" values from Series
        # 3) Bump those units by 1 unit
        bump_idx = s_remainders.sort_values(ascending=False).iloc[:units_to_add].index
        s_base_units[bump_idx] += 1

    df[COL_WEIGHT] = s_base_units / scale  # Move back to a decimal without any funky floating point inaccuracies.
    return df


def _decimal_places(num: float) -> int:
    d = Decimal(str(num)).normalize()
    if d == d.to_integral():
        return 0

    # noinspection PyTypeChecker
    return abs(d.as_tuple().exponent)
