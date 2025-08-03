import os
from pathlib import Path

COL_NAME = "name"
COL_MC = "market_cap"
COL_SYMBOL = "symbol"
COL_WEIGHT = "weighted"
COL_PRICE = "price"

MIN_MEGA_CAP = 200_000_000_000

LIMIT_MIN_WEIGHT = 2

CMC_API_TOKEN = os.getenv("CMC_API_TOKEN")
FMP_API_TOKEN = os.getenv("FMP_API_TOKEN")

PATH_DATA = Path("../data/").resolve()
PATH_INDEXES = Path("../indexes/").resolve()
PATH_PROJECT_ROOT = Path(__file__).resolve().parents[1]

PATH_DATA.mkdir(parents=True, exist_ok=True)
PATH_INDEXES.mkdir(parents=True, exist_ok=True)
