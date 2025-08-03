import pandas as pd

from src.clients.cmc import get_mega_crypto
from src.clients.fmp import get_mega_stock
from src.consts import COL_SYMBOL, COL_MC, COL_WEIGHT
from src.index import get_index
from src.io import save_index, load_config

PROD_API_CALL = True
LIMIT_FIDELITY = 50

# API Calls
df_stock = get_mega_stock(from_cache=not PROD_API_CALL)
df_crypto = get_mega_crypto(from_cache=not PROD_API_CALL)
print(f"Retrieved {len(df_stock)} megacap stocks")
print(f"Retrieved {len(df_crypto)} megacap crypto")

# Generate final Dataframe for processing
df_limit = pd.concat([df_stock, df_crypto], axis=0, ignore_index=True)

config = load_config()
for merge, into in config.items():
    merge_row = df_limit.loc[df_limit[COL_SYMBOL] == merge, COL_MC]
    if merge_row.empty: continue
    df_limit = df_limit[df_limit[COL_SYMBOL] != merge]
    print(f"Removed {merge} Symbol")

    mask = df_limit[COL_SYMBOL] == into
    if mask.any():
        df_limit.loc[mask, COL_MC] += merge_row.iat[0]
        print(f"Added {merge} market cap into {into}")

df_limit = df_limit.sort_values(by=COL_MC, ascending=False)
df_limit = df_limit.head(LIMIT_FIDELITY)
print("Combined megacap:")
print(df_limit)

# Index contains final weights
df_index = get_index(df_limit).reset_index(drop=True)

count = len(df_limit) - len(df_index)
print(f"Dropped {count} symbols")
print(f"Remaining {len(df_index)} symbols")
print(f"Index weighted results:")
print(df_index)
print(f"Final weighted sum: {df_index[COL_WEIGHT].sum():.2f}")

if PROD_API_CALL:
    save_index(df_index)
