from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from src.clients.cmc import get_mega_crypto
from src.clients.fmp import get_mega_stock
from src.consts import COL_SYMBOL, COL_MC, COL_WEIGHT
from src.index import get_index
from src.io import save_index

DOTENV_PATH = Path(__file__).resolve().parent.parent / ".env"
PROD_API_CALL = True
EXCLUDE_SYMBOLS = ["BRK.A", "BRK-A", "TCEHY"]
LIMIT_FIDELITY = 50

load_dotenv(DOTENV_PATH)

# Configure for displaying on console
pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.width", 0)  # auto-size to fit screen

# API Calls
df_stock = get_mega_stock(from_cache=not PROD_API_CALL)
df_crypto = get_mega_crypto(from_cache=not PROD_API_CALL)
print(f"Retrieved {len(df_stock)} megacap stocks")
print(f"Retrieved {len(df_crypto)} megacap crypto")

# Generate final Dataframe for processing
df_limit = pd.concat([df_stock, df_crypto], axis=0, ignore_index=True)
df_limit = df_limit[~df_limit[COL_SYMBOL].isin(EXCLUDE_SYMBOLS)]
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
