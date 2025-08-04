from datetime import datetime

from pandas import DataFrame

from src.consts import COL_MC, COL_WEIGHT, PATH_INDEXES_ROOT

PATH_INDEX_T50M2 = PATH_INDEXES_ROOT / "top50min2"
PATH_INDEX_T50M2.mkdir(parents=True, exist_ok=True)


def save_index(df: DataFrame):
    df_save = df.copy()
    df_save[COL_MC] = df_save[COL_MC] / 1_000_000_000
    df_save[COL_MC] = df_save[COL_MC].apply(lambda x: f"{x:.3f}")
    df_save[COL_WEIGHT] = df_save[COL_WEIGHT].apply(lambda x: f"{x:.1f}")
    date_str = datetime.today().strftime("%y-%m-%d")
    filepath = PATH_INDEX_T50M2 / f"{date_str}.csv"
    print(f"Saved {filepath}")
    df_save.to_csv(filepath)
