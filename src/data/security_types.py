from enum import Enum


class CryptoTypes(Enum):
    CRYPTO = "Crypto"
    STABLECOIN = "Stablecoin"


class StockTypes(Enum):
    COMMON_STOCK = "Common Stock"
    PREFERRED = "Preferred"
    ADR = "ADR"  # American Depositary Receipt
    REIT = "REIT"  # Real Estate Investment Trust


class CommoditiesType(Enum):
    ETF = "ETF"
