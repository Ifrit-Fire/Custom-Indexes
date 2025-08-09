# Custom Indexes

Builds custom indexes of the top-ranked securities by market cap — across both stocks and cryptocurrencies — with
configurable size and minimum weighting rules.

## Overview

This project generates multiple index variants, each defined by:

- Number of constituents (_top_)
- Minimum weighting per constituent (_weight_min_)
- Sort criteria (_currently market_cap_)

All indexes are generated from combined stock + crypto market data and treat both asset classes equivalently during
index construction.

| Name          | Constituents | Min Weight (%) |
|---------------|--------------|----------------| 
| top10-min10   | 10           | 10%            |
| top20-min5    | 20           | 5%             |
| top50-min2    | 50           | 2%             |
| top100-min1   | 100          | 1%             |
| top250-min0.4 | 250          | 0.4%           |
| top500-min0.2 | 500          | 0.2%           |

The configuration also supports symbol consolidation (_e.g., merging BRK.A into BRK.B_) and a global minimum daily
volume threshold.

## Requirements

- Python 3.12+
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

3. Optionally modify `config.yaml` to customize existing or to create new indexes.

## Running

1. Run `build.sh` to setup the virtual environment.
2. Then run `main.py` to start.

- Each run fetches fresh data from the APIs if no cache is available.
- Cached data is stored in the `data/` directory and is refreshed daily.
- Results are printed to the console and written locally in the `indexes/` directory.

## Background

Originally built as a quick solution for managing a custom index
via [Fidelity Basket Portfolios](https://www.fidelity.com/direct-indexing/customized-investing/overview), the project
now supports multiple index definitions from a single YAML configuration.

## Future

- I'm open to adding additional features or improvements — feel free to open an issue.