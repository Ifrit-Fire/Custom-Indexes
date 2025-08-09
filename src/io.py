from datetime import date
from pathlib import Path

from pandas import DataFrame

from src.consts import COL_MC, COL_WEIGHT, PATH_INDEXES_ROOT


def save_index(name: str, df: DataFrame):
    df_save = df.copy()
    df_save[COL_MC] = df_save[COL_MC] / 1_000_000_000
    df_save[COL_MC] = df_save[COL_MC].apply(lambda x: f"{x:.3f}")
    df_save[COL_WEIGHT] = df_save[COL_WEIGHT].apply(lambda x: f"{x:.1f}")

    filepath = _get_file_name(name)
    df_save.to_csv(filepath)
    print(f"\tSaved {filepath}")


def _get_file_name(name: str) -> Path:
    today_str = date.today().isoformat()
    filepath = PATH_INDEXES_ROOT / name / f"{today_str}.csv"
    filepath.parent.mkdir(parents=True, exist_ok=True)
    return filepath
