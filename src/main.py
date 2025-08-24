import src.data_processing as dp
from src import allocations, io
from src.clients import fmp, cmc
from src.config_handler import config
from src.consts import COL_WEIGHT
from src.logger import timber

timber.till()

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
