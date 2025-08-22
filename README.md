# Custom Indexes

Builds custom indexes of the top-ranked securities by market cap — across both stocks and cryptocurrencies — with
configurable size and minimum weighting rules.

## Overview

This project generates multiple index variants, each defined by:

- Number of constituents (_top_)
- Minimum weighting per constituent (_weight_min_)
- Sort criteria (_currently market_cap_)

All indexes are composed of both stock + crypto. Both asset classes are treated equivalently during
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
- API key from [CoinMarketCap](https://coinmarketcap.com/api/) _(free)_
- API key from [Financial Modeling Prep](https://site.financialmodelingprep.com) _(free)_
- API key from [Polygon.io](https://polygon.io/docs/rest/quickstart) _(free, optional)_

## Setup

1. Clone the repo
2. Create a `.env` file in the root directory:

```dotenv
CMC_API_TOKEN=your_coinmarketcap_api_key
FMP_API_TOKEN=your_financial_modeling_api_key
POLY_API_TOKEN=your_polygon_api_key
```

3. _(Optional)_ Add a Polygon API key to enable fetching detailed security information not already stored locally.
4. _(Optional)_ Modify `config.yaml` to customize existing and/or create new indexes.

## Running

1. Run `build.sh` to setup the virtual environment with installed dependencies.
2. Then run `main.py` to build the latest index outputs.
3. Built indexes are placed in `/indexes`

- Each run fetches fresh data from the APIs if no cache is available.
- Cached data is stored in the `data/` directory and is refreshed daily.
- Results are printed to the console and written locally in the `indexes/` directory.

## Background

Originally built as a quick solution for managing a custom index
via [Fidelity Basket Portfolios](https://www.fidelity.com/direct-indexing/customized-investing/overview), the project
now supports multiple index definitions from a single YAML configuration.

## Future

- I'm open to adding additional features or improvements — feel free to open an issue.