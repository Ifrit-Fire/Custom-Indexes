import src.data_processing as dp
from src import allocations
from src.clients import fmp, cmc
from src.config_handler import config, KEY_INDEX_WEIGHT_MIN
from src.consts import COL_WEIGHT
from src.io import save_index

PROD_API_CALL = False

for index, criteria in config.get_all_indexes().items():
    print(f"{index} - Creating Index")
    # TODO: Add volume limit logic
    # TODO: Test by adding another index: top100-min1, top20-min5, top10-min10
    # TODO: Test by adding top250-min0.4, top500-min0.25
    df_stock = fmp.get_stock(criteria)
    df_crypto = cmc.get_crypto(criteria)
    df_refined = dp.refine_data(using=criteria, dfs=[df_stock, df_crypto])
    df_weights = allocations.add_weightings(df_refined, criteria[KEY_INDEX_WEIGHT_MIN]).reset_index(drop=True)

    count = len(df_refined) - len(df_weights)
    print(f"\tDropped {count} symbols with {len(df_weights)} remaining.")
    print(f"\tIndex weighted results:")
    print(df_weights)
    print(f"\tFinal weighted sum: {df_weights[COL_WEIGHT].sum():.2f}%")
    print()

    if PROD_API_CALL:
        save_index(df_weights)
