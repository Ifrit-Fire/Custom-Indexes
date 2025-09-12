from enum import Enum


class ProviderSource(Enum):
    COIN_MC = "coin-market-cap"
    FINN_MOD_PREP = "financial-model-prep"
    FINNHUB = "finnhub"
    POLYGON = "polygon"
