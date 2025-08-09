from src.consts import COL_MC


def sort_by_to_df_column(sort_by: str) -> str:
    if sort_by == "market_cap":
        return COL_MC
    else:
        raise ValueError(f"Undefined {sort_by} sort by encountered.")
