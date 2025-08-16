import src.data_processing as dp
from src import allocations, io
from src.clients import fmp, cmc
from src.config_handler import config
from src.consts import COL_WEIGHT

# TODO: Add type (Stock, ADR, Crypto) to index DataFrame
# TODO: Switch all/most prints to logging
# TODO: Add ability to clean old cache
# TODO: Better way to execute running program....make?
# TODO: Setup GitHub actions to recurringly update indexes
# TODO: Unit tests?
# TODO: Make static page displaying performance and composition over time
# TODO: Update Indexes with historical data to run performance metrics on

for index, criteria in config.get_all_indexes().items():
    print(f"{index} - Creating Index")

    df_stock = fmp.get_stock(criteria)
    df_crypto = cmc.get_crypto(criteria)
    df_refined = dp.refine_data(using=criteria, dfs=[df_stock, df_crypto])
    df_weights = allocations.add_weightings(df_refined, criteria).reset_index(drop=True)

    count = len(df_refined) - len(df_weights)
    print(f"\tDropped {count} symbols with {len(df_weights)} remaining.")
    print(f"\tIndex weighted results:")
    print(df_weights)
    print(f"\tFinal weighted sum: {df_weights[COL_WEIGHT].sum():.2f}%")

    io.save_index(index, df_weights)
    print()
