import os
from pathlib import Path

COL_NAME = "name"
COL_MC = "market_cap"
COL_SYMBOL = "symbol"
COL_WEIGHT = "weighted"
COL_PRICE = "price"
COL_VOLUME = "volume"

MIN_ULTRA_CAP = 1_000_000_000_000
MIN_MEGA_CAP = 200_000_000_000
MIN_LARGE_CAP = 10_000_000_000
MIN_MID_CAP = 2_000_000_000
MIN_SMALL_CAP = 300_000_000

LIMIT_MIN_WEIGHT = 2

CMC_API_TOKEN = os.getenv("CMC_API_TOKEN")
FMP_API_TOKEN = os.getenv("FMP_API_TOKEN")

PATH_PROJECT_ROOT = Path(__file__).resolve().parents[1]
PATH_DATA_ROOT = PATH_PROJECT_ROOT / "data"
PATH_DATA_CACHE_ROOT = PATH_DATA_ROOT / "cache"
PATH_INDEXES_ROOT = PATH_PROJECT_ROOT / "indexes"
PATH_CONFIG = PATH_PROJECT_ROOT / "config.yaml"

PATH_DATA_ROOT.mkdir(parents=True, exist_ok=True)
PATH_INDEXES_ROOT.mkdir(parents=True, exist_ok=True)

_SYMBOL_NORMALIZE = {"BRK-A": "BRK.A", "BRK/A": "BRK.A", "BRK-B": "BRK.B", "BRK/B": "BRK.B"}
