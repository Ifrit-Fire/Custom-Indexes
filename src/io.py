import sys
import time
from datetime import date

from pandas import DataFrame

from src.consts import COL_MC, COL_WEIGHT, PATH_INDEXES_ROOT


def save_index(name: str, df: DataFrame):
    df_save = df.copy()
    df_save[COL_MC] = df_save[COL_MC] / 1_000_000_000
    df_save[COL_MC] = df_save[COL_MC].apply(lambda x: f"{x:.3f}")
    df_save[COL_WEIGHT] = df_save[COL_WEIGHT].apply(lambda x: f"{x:.1f}")

    today_str = date.today().isoformat()
    filepath = PATH_INDEXES_ROOT / name / f"{today_str}.csv"
    filepath.parent.mkdir(parents=True, exist_ok=True)
    df_save.to_csv(filepath)
    print(f"\tSaved {filepath}")


def console_countdown(msg: str, seconds: int):
    """
    Display a countdown timer in the console with a custom message, overwriting the same line each second until it
    reaches zero.

    Args:
        msg (str): Message prefix displayed before the countdown.
        seconds (int): Number of seconds to count down from.

    Returns:
        None
    """
    for remaining in range(seconds, 0, -1):
        sys.stdout.write(f"\r{msg} in {remaining:02d} seconds...")
        sys.stdout.flush()
        time.sleep(1)
