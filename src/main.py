import pandas as pd

from src.clients.cmc import get_crypto
from src.clients.fmp import get_stock
from src.config_handler import config, KEY_INDEX_TOP, KEY_INDEX_WEIGHT_MIN
from src.consts import COL_MC, COL_WEIGHT
from src.index import get_index
from src.io import save_index
from src.symbol_merger import merge_symbols

PROD_API_CALL = False

for index, criteria in config.get_all_indexes().items():
    print(f"{index} - Creating Index")
    # TODO: Implement SORT_BY...right now it's all hard coded
    # TODO: Have caching be automatic.  Based on date and criteria
    # TODO: Add volume limit logic
    # TODO: Test by adding another index: top100-min1, top20-min5, top10-min10
    # TODO: Test by adding top250-min0.4, top500-min0.25
    df_stock = get_stock(top=criteria[KEY_INDEX_TOP], from_cache=True)
    df_crypto = get_crypto(top=criteria[KEY_INDEX_TOP], from_cache=True)
    print(f"\tRetrieved {len(df_stock)} stocks")
    print(f"\tRetrieved {len(df_crypto)} crypto")

    print(f"\tMerging all security types...")
    df_securities = pd.concat([df_stock, df_crypto], axis=0, ignore_index=True)
    df_securities = merge_symbols(df_securities)
    df_securities = df_securities.sort_values(by=COL_MC, ascending=False)
    df_securities = df_securities.head(criteria[KEY_INDEX_TOP])
    print(f"\t...trimmed down to {len(df_securities)} securities")

    df_index = get_index(df_securities, criteria[KEY_INDEX_WEIGHT_MIN]).reset_index(drop=True)
    count = len(df_securities) - len(df_index)
    print(f"\tDropped {count} symbols with {len(df_index)} remaining.")
    print(f"\tIndex weighted results:")
    print(df_index)
    print(f"\tFinal weighted sum: {df_index[COL_WEIGHT].sum():.2f}")
    print()

    if PROD_API_CALL:
        save_index(df_index)
