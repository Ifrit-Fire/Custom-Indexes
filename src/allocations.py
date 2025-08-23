from decimal import Decimal

import numpy as np
from pandas import DataFrame

from src.config_handler import KEY_INDEX_WEIGHT_MIN
from src.consts import COL_WEIGHT, COL_MC, COL_SYMBOL, COL_TYPE
from src.logging import timber


def add_weightings(df: DataFrame, criteria: dict) -> DataFrame:
    """
    Calculate and add a weighting column to the DataFrame based on market capitalization, ensuring all weights meet a
    minimum threshold.

    The function:
      1. Calculates weights as a percentage of total market cap.
      2. Drops the smallest-weighted securities until the minimum weight requirement is met.
      3. Recalculates weights after each drop.
      4. Applies rounding adjustments using the Largest Remainder Method.

    Args:
        df (DataFrame): DataFrame containing security data, including `COL_MC` (market cap) and `COL_SYMBOL`.
        criteria (dict): Dictionary containing at least `KEY_INDEX_WEIGHT_MIN` (minimum allowed weight).

    Returns:
        DataFrame: A new DataFrame with the `COL_WEIGHT` column added, meeting the weight constraints.
    """
    log = timber.plant()
    log.info("Phase starts", create="weights")
    min_weight = criteria[KEY_INDEX_WEIGHT_MIN]
    precision = _decimal_places(min_weight) + 2
    log.debug("Calculate", precision=precision)

    df_index = df.copy()
    df_index[COL_WEIGHT] = round(df_index[COL_MC] / df_index[COL_MC].sum() * 100, precision)
    while df_index[COL_WEIGHT].min() < min_weight:
        symbol = df_index.iloc[-1][COL_SYMBOL]
        weight = df_index.iloc[-1][COL_WEIGHT]
        type = df_index.iloc[-1][COL_TYPE]
        log.info("Removed", symbol=symbol, weight=f"{weight:.{precision}f}", type=type)

        df_index = df_index.iloc[:-1]
        df_index[COL_WEIGHT] = round(df_index[COL_MC] / df_index[COL_MC].sum() * 100, precision)
    df_index = _fix_rounding(df_index, precision)

    final_count = len(df_index)
    removed_count = len(df) - final_count
    weight_sum = f"{df_index[COL_WEIGHT].sum():.2f}"
    log.info("Phase ends", create="weights", sum=weight_sum, dropped=removed_count, remaining=final_count)
    return df_index


def _fix_rounding(df: DataFrame, precision: int) -> DataFrame:
    """
    Correct percentage rounding errors using the Largest Remainder Method.

    This method:
      1. Converts percentages into integer units based on the given precision to avoid floating-point inaccuracies.
      2. Floors each value to a base integer unit.
      3. Distributes any remaining units to the rows with the largest remainders until the total equals exactly 100%.
      4. Converts back to a decimal percentage with minimal floating-point error.

    Args:
        df (DataFrame): DataFrame containing at least the `COL_MC` (market cap) and `COL_WEIGHT` column.
        precision (int): Number of decimal places to preserve; actual calculation uses `precision - 1` to determine scaling.

    Returns:
        DataFrame: `COL_WEIGHT` column with corrected weights.
    """
    log = timber.plant()
    scale = 10 ** (precision - 1)
    s_exact_units = df[COL_MC] / df[COL_MC].sum() * 100 * scale  # Scale moves exact % to integer space
    s_base_units = np.floor(s_exact_units).astype(int)  # What we'll increase to fix rounding errors
    s_remainders = s_exact_units - s_base_units  # used for determining who gets the increase

    # how many 1-unit bumps we still need to reach exactly 100%
    units_to_add = int(round(100 * scale - s_base_units.sum()))
    log.debug("Fix rounding", units=units_to_add)
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
    """
    Determine the number of decimal places in a given number.

    Uses Python's `Decimal` to avoid floating-point representation errors and
    correctly count decimal places, even for values like `1.2300`.

    Args:
        num (float): The number to evaluate.

    Returns:
        int: The count of decimal places (0 if the number is integral).
    """
    d = Decimal(str(num)).normalize()
    if d == d.to_integral():
        return 0

    # noinspection PyTypeChecker
    return abs(d.as_tuple().exponent)
