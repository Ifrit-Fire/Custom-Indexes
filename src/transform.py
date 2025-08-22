from src.consts import COL_MC


def sort_by_to_df_column(sort_by: str) -> str:
    """
    Map a sort criteria string to its corresponding DataFrame column constant. Currently, supports only `"market_cap"`.

    Args:
        sort_by (str): The sort criteria name.

    Returns:
        str: The corresponding DataFrame column constant (e.g., `COL_MC`).

    Raises:
        ValueError: If the provided `sort_by` value is not recognized.
    """
    if sort_by == "market_cap":
        return COL_MC
    else:
        raise ValueError(f"Undefined {sort_by} sort by encountered.")
