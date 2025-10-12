import os
from pathlib import Path

from src.data.security_types import CryptoTypes, StockTypes, CommoditiesType

API_CMC_TOKEN = os.getenv("CMC_API_TOKEN")
API_FINN_TOKEN = os.getenv("FINN_API_TOKEN")
API_FMP_TOKEN = os.getenv("FMP_API_TOKEN")
API_FMP_CACHE_ONLY = API_FMP_TOKEN is None
API_POLY_TOKEN = os.getenv("POLY_API_TOKEN")

COL_CIK = "cik"
COL_C_PRICE = "closing_price"
COL_COUNTRY = "country"
COL_FIGI = "composite_figi"
COL_LIST_DATE = "list_date"
COL_MC = "market_cap"
COL_MIC = "mic"
COL_NAME = "name"
COL_OUT_SHARES = "outstanding_shares"
COL_POSTAL_CODE = "postal_code"
COL_STATE = "state"
COL_SYMBOL = "symbol"
COL_TIMESTAMP = "timestamp"
COL_TYPE = "type"
COL_VOLUME = "volume"
COL_WEIGHT = "weighted"

CRITICAL_COLUMNS = {COL_LIST_DATE, COL_MC, COL_SYMBOL, COL_TYPE, COL_VOLUME}

FORM_STRUCT = "structured"
FORM_TEXT = "text"

MIC_CODES = {"XNYS", "XNAS", "XASE", "BATS"}

MIN_ULTRA_CAP = 1_000_000_000_000
MIN_MEGA_CAP = 200_000_000_000
MIN_LARGE_CAP = 10_000_000_000
MIN_MID_CAP = 2_000_000_000
MIN_SMALL_CAP = 300_000_000

PATH_PROJECT_ROOT = Path(__file__).resolve().parents[1]
PATH_CONFIG = PATH_PROJECT_ROOT / "config.yaml"
PATH_DATA_ROOT = PATH_PROJECT_ROOT / "data"
PATH_DATA_CACHE_ROOT = PATH_DATA_ROOT / "cache"
PATH_DATA_STORE_ROOT = PATH_DATA_ROOT / "store"
PATH_INDEXES_ROOT = PATH_PROJECT_ROOT / "indexes"
PATH_LOGS_ROOT = PATH_PROJECT_ROOT / "logs"

PATH_DATA_ROOT.mkdir(parents=True, exist_ok=True)
PATH_DATA_CACHE_ROOT.mkdir(parents=True, exist_ok=True)
PATH_DATA_STORE_ROOT.mkdir(parents=True, exist_ok=True)
PATH_INDEXES_ROOT.mkdir(parents=True, exist_ok=True)
PATH_LOGS_ROOT.mkdir(parents=True, exist_ok=True)

SECURITY_TYPES = {t.value for t in (*CryptoTypes, *StockTypes, *CommoditiesType)}
STOCK_TYPES = {t.value for t in StockTypes}
CRYPTO_TYPES = {t.value for t in CryptoTypes}
