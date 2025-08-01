# Custom Indexes

Creates a custom index comprising the top securities by market cap, limited to 50 constituents, each with a minimum
weight of 2%.

## Background

This is a quick solution I built to manage a custom index
using [Fidelity Basket Portfolios](https://www.fidelity.com/etf/overview/basket-portfolios).  
It supports building an index using both stocks and cryptocurrencies — treating them equivalently during index
construction.

## Requirements

- Python 3.12 or greater
- macOS _(tested)_ or Linux _(untested)_
- API key from [CoinMarketCap](https://coinmarketcap.com/api/) _(free)_
- API key from [Financial Modeling Prep](https://site.financialmodelingprep.com) _(free)_

## Setup

1. Clone the repo
2. Create a `.env` file in the root directory:

```dotenv
CMC_API_TOKEN=your_coinmarketcap_api_key
FMP_API_TOKEN=your_financial_modeling_api_key
```

## Running

Run `main.py` to start.

- By default, each run fetches fresh data from the APIs and stores results in the `data/` directory.
- Results are printed to the console and saved locally in the `indexes/` directory.
- To avoid unnecessary API calls while developing, set `PROD_API_CALL = False` in `main.py`. This will use locally
  cached data instead.

## Future

- I'm open to adding additional features or improvements — feel free to open an issue.