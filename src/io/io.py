from datetime import date

from pandas import DataFrame

from src.consts import COL_MC, COL_WEIGHT, PATH_INDEXES_ROOT
from src.logger import timber


def save_index(name: str, df: DataFrame):
    """
    Save an index DataFrame to both a dated snapshot file and an evergreen file.

    This function produces two CSV outputs:
      1. A **dated snapshot** at `indexes/{name}/{YYYY-MM-DD}.csv`
      2. An **evergreen file** at `indexes/{name}.csv`

    Additional formatting is applied before saving:
      - Market cap (`COL_MC`) scaled down to billions and rounded to 3 decimals.
      - Weights (`COL_WEIGHT`) rounded to 1 decimal.

    Args:
        name (str): Name of the index, used for file naming and directory structure.
        df (pd.DataFrame): The index DataFrame to save. Must include at least `COL_MC` and `COL_WEIGHT`.
    """
    log = timber.plant()
    df_save = df.copy()
    df_save[COL_MC] = df_save[COL_MC] / 1_000_000_000
    df_save[COL_MC] = df_save[COL_MC].apply(lambda x: f"{x:.3f}")
    df_save[COL_WEIGHT] = df_save[COL_WEIGHT].apply(lambda x: f"{x:.1f}")

    today_str = date.today().isoformat()
    filepath = PATH_INDEXES_ROOT / name / f"{today_str}.csv"
    filepath.parent.mkdir(parents=True, exist_ok=True)
    df_save.to_csv(filepath)
    log.debug("Saved", index=name, file=filepath.name, type="csv", count=len(df), path=filepath.parent)

    evergreen = PATH_INDEXES_ROOT / f"{name}.csv"
    df_save.index.name = f"built_{today_str}"  # Leverage an empty index header name to timestamp file creation
    df_save.to_csv(evergreen)
    log.debug("Saved", index=name, file=evergreen.name, type="csv", count=len(df), path=evergreen.parent)
