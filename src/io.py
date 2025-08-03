from datetime import datetime

import yaml
from pandas import DataFrame

from src.consts import COL_MC, COL_WEIGHT, PATH_INDEXES, PATH_PROJECT_ROOT

PATH_INDEX_T50M2 = PATH_INDEXES / "top50min2"
PATH_CONFIG = PATH_PROJECT_ROOT / "config.yaml"

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


def load_config() -> dict[str, str]:
    with open(PATH_CONFIG, "r") as f:
        config = yaml.safe_load(f)
    symbol_consolidate = config.get("symbol_consolidate", [])
    symbol_merge = {item["merge"]: item["into"] for item in symbol_consolidate}
    return symbol_merge
