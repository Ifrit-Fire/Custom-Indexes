import sys

from src.clients import finnhub, polygon
from src.config_handler import config
from src.consts import COL_SYMBOL
from src.data import processing
from src.logger import timber
from src.services import fetcher

timber.till()
for index, criteria in config.get_all_indexes().items():
    log = timber.plant(index)
    log.info("Phase starts", create=index)
    df_all_finn = finnhub.get_all_stock()
    df_all_poly = polygon.get_all_stock()
    df_merge = processing.merge_all_stock(df_finn=df_all_finn, df_poly=df_all_poly)
    df_final = fetcher.get_symbol_details(df_merge[COL_SYMBOL])

    # df_stock = fmp.get_stock(criteria)
    sys.exit()
    df_crypto = cmc.get_crypto(criteria)
    df_refined = dp.refine_data(using=criteria, dfs=[df_stock, df_crypto])
    df_weights = allocations.add_weightings(df_refined, criteria).reset_index(drop=True)

    io.save_index(index, df_weights)
    log.info("Phase ends", create=index, max_weight=df_weights[COL_WEIGHT].max(),
             min_weight=df_weights[COL_WEIGHT].min(), median_weight=df_weights[COL_WEIGHT].median(),
             count=len(df_weights))
