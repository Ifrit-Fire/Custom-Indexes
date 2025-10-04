import sys

from src.config_handler import config
from src.consts import COL_SYMBOL, COL_VOLUME
from src.data import processing
from src.logger import timber
from src.services import fetcher

timber.till()
log = timber.plant("ETL")
log.info("Phase starts", perform="ETL")
df_listing = fetcher.get_stock_listing()
df_details = fetcher.get_stock_details(df_listing[COL_SYMBOL])
df_list_details = processing.merge_on_symbols(df_canonical=df_details, df_data=df_listing)
df_ohlcv = fetcher.get_ohlcv()
df_volume = df_ohlcv.groupby(COL_SYMBOL)[COL_VOLUME].mean().reset_index()
df_market = processing.merge_on_symbols(df_canonical=df_list_details, df_data=df_volume)
df_crypto = fetcher.get_crypto_market()
log.info("Phase ends", perform="ETL", df_stock=len(df_listing), df_crypto=len(df_crypto))

for index, criteria in config.get_all_indexes().items():
    log = timber.plant(index)
    log.info("Phase starts", create=index)

    sys.exit()

    df_refined = dp.refine_data(using=criteria, dfs=[df_listing, df_crypto])
    df_weights = allocations.add_weightings(df_refined, criteria).reset_index(drop=True)

    io.save_index(index, df_weights)
    log.info("Phase ends", create=index, max_weight=df_weights[COL_WEIGHT].max(),
             min_weight=df_weights[COL_WEIGHT].min(), median_weight=df_weights[COL_WEIGHT].median(),
             count=len(df_weights))
