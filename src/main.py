import src.data_processing as dp
from src import allocations, io, timber
from src.clients import fmp, cmc
from src.config_handler import config
from src.consts import COL_WEIGHT

# TODO: Test what happens if none, some, and all of the appropriate API keys are provided
# TODO: Indexes - should always have a latest named file to act as an evergreen link.
# TODO: Add ability to clean old cache
# TODO: Can we run main loop in parallel?
# TODO: Better way to execute running program....make?
# TODO: Research including ETVs holding physical commodities
# TODO: Setup GitHub actions to recurringly update indexes
# TODO: Unit tests?
# TODO: Make static page displaying performance and composition over time
# TODO: Update Indexes with historical data to run performance metrics on

for index, criteria in config.get_all_indexes().items():
    log = timber.plant(index)
    log.info("Phase starts", create=index)

    df_stock = fmp.get_stock(criteria)
    df_crypto = cmc.get_crypto(criteria)
    df_refined = dp.refine_data(using=criteria, dfs=[df_stock, df_crypto])
    df_weights = allocations.add_weightings(df_refined, criteria).reset_index(drop=True)

    io.save_index(index, df_weights)
    log.info("Phase ends", create=index, max_weight=df_weights[COL_WEIGHT].max(),
             min_weight=df_weights[COL_WEIGHT].min(), median_weight=df_weights[COL_WEIGHT].median(),
             count=len(df_weights))
