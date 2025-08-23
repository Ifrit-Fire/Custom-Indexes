# Custom Indexes

Builds custom indexes of the top-ranked securities by market cap — across both stocks and cryptocurrencies — with
configurable size and minimum weighting rules.

## Overview

This project generates multiple index variants, each defined by:

- Number of constituents (_top_)
- Minimum weighting per constituent (_weight_min_)
- Sort criteria (_currently market_cap_)

All indexes can be composed of both stock + crypto. Both asset classes are treated equivalently during
index construction. Constituents are filtered by liquidity, listing age, and asset type restrictions, then ranked by
market capitalization.

For detailed construction rules, see the [Index Methodology](docs/methodology.md).

| Name          | Constituents | Min Weight (%) | Latest Build                                       |
|---------------|--------------|----------------|----------------------------------------------------|
| top10-min10   | 10           | 10%            | [2025-08-22](indexes/top10-min10/2025-08-22.csv)   |
| top20-min5    | 20           | 5%             | [2025-08-22](indexes/top20-min5/2025-08-22.csv)    |
| top50-min2    | 50           | 2%             | [2025-08-22](indexes/top50-min2/2025-08-22.csv)    |
| top100-min1   | 100          | 1%             | [2025-08-22](indexes/top100-min1/2025-08-22.csv)   |
| top250-min0.4 | 250          | 0.4%           | [2025-08-22](indexes/top250-min0.4/2025-08-22.csv) |
| top500-min0.2 | 500          | 0.2%           | [2025-08-22](indexes/top500-min0.2/2025-08-22.csv) |

## Requirements

- Python 3.12+
- macOS _(tested)_ or Linux _(untested)_

## Optional (API Keys)

You can run the project without API keys; it will fall back to bundled snapshot data so you can demo the tool and poke
around immediately. To fetch fresh market data, add API keys:

- API key from [CoinMarketCap](https://coinmarketcap.com/api/) _(free)_
- API key from [Financial Modeling Prep](https://site.financialmodelingprep.com) _(free)_
- API key from [Polygon.io](https://polygon.io/docs/rest/quickstart) _(free)_

## Setup

1. Clone the repo
2. Create a `.env` file in the root directory:

```dotenv
CMC_API_TOKEN=your_coinmarketcap_api_key
FMP_API_TOKEN=your_financial_modeling_api_key
POLY_API_TOKEN=your_polygon_api_key
```

3_(Optional)_ Modify `config.yaml` to customize existing and/or create new indexes.

## Running

1. Run `build.sh` to setup the virtual environment with installed dependencies.
2. Run `main.py` to build the indexes defined in `config.yaml`.
3. Results are written to `/indexes`

- Cached data is stored in the `data/` directory.
- If API tokens are available, the cache refreshes daily with market data.
- Without tokens, the build uses the included snapshot data.

## Background

Originally built as a quick solution for managing a custom index
via [Fidelity Basket Portfolios](https://www.fidelity.com/direct-indexing/customized-investing/overview), the project
now supports multiple index definitions from a single YAML configuration.

## Future

- I'm actively working this and have lots of features planned:
  - Building a static website to display the index results with fancy graphs
  - Research pulling in the index historical data to determine performance over time
  - Hooking up GitHub actions to have this run and update itself regularly
- I'm open to adding additional features or improvements — feel free to open an issue.