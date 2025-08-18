# Index Methodology

- [Index Overview](#index-overview)
- [Eligibility](#eligibility)
- [Listing Requirement](#listing-requirement)
  - [Rationale for the 3-year crypto horizon](#rationale-for-the-3-year-crypto-horizon)
- [Liquidity Requirement](#liquidity-requirement)
- [Symbol Consolidation](#symbol-consolidation)
- [Minimum Weight Threshold Rule](#minimum-weight-threshold-rule)
- [Weighting Methodology](#weighting-methodology)

## Index Overview

The following indexes are maintained under this methodology. Each index defines a maximum fixed number of constituents
ranked by market capitalization, with a proportional minimum weight rule applied to ensure meaningful representation.
The naming convention combines the constituent count and the corresponding minimum weight percentage.

| Name          | Constituents | Min Weight (%) |
|---------------|--------------|----------------|
| top10-min10   | 10           | 10%            |
| top20-min5    | 20           | 5%             |
| top50-min2    | 50           | 2%             |
| top100-min1   | 100          | 1%             |
| top250-min0.4 | 250          | 0.4%           |
| top500-min0.2 | 500          | 0.2%           |

## Eligibility

The index includes the following asset types:

- Common Stock
- ADR–Common
- Ordinary Stock
- Cryptocurrency

Exclusions apply to securities such as preferred shares, ETFs, funds, derivatives, stablecoins, and illiquid or
experimental tokens.

## Listing Requirement

- Stocks must have been listed for at least 1 year, which reflects a common industry practice and ensures a basic level
  of trading history.
- Cryptocurrencies must have been listed for at least 3 years _(measured from CoinMarketCap’s date_added)_. Three years
  should provide enough time for early hype cycles to settle and for projects to demonstrate sustained operations and
  user adoption.

### Rationale for the 3-year crypto horizon

The 3-year window balances coverage of established assets with risk control for new listings. New tokens often
experience large volatility and narrative-driven surges. A three-year lookback is meant to allow early hype to fade. The
early life of many tokens includes abandonment and stalled development. A multi-year horizon filters projects unable to
sustain operations or user traction.

A stricter 5-year rule was considered but found overly exclusionary for credible, widely adopted assets launched within
the last 3–5 years. Three years should provide a practical middle ground — new enough to capture relevance, but seasoned
enough to show durability.

## Liquidity Requirement

To ensure tradability, all securities must satisfy minimum liquidity standards.

- Securities with an average daily trading volume below 10,000 units are excluded.

## Symbol Consolidation

Certain tickers representing the same underlying company are consolidated for market capitalization purposes.

- BRK.A merges into BRK.B.

## Minimum Weight Threshold Rule

The indexes apply a proportional minimum weight threshold to maintain constituent significance and reduce the impact of
low-capitalization securities. For a target constituent count $N$, the minimum allowable weight, $w_{\min}$, is defined
as:

$w_{\min} = \frac{1}{N}$

where $w_{\min}$ is expressed as a percentage of the total index weight. This rule is similar to the equal-weight
allocation for $N$ securities but allows for weight adjustments based on market cap.

This approach ensures that:

1. **Proportional Scaling** – The minimum threshold dynamically adjusts with index breadth.
2. **Constituent Significance** – Every included security contributes a meaningful share of index exposure.
3. **Weight Threshold Enforcement** – Securities falling below the minimum weight threshold are systematically removed.

## Weighting Methodology

Constituents are weighted by market capitalization subject to the Minimum Weight Threshold Rule. The weighting process
is applied in the following steps:

1. **Initial Proportional Weights** - Each security’s weight is calculated as its market capitalization divided by the
   aggregate market capitalization of
   all eligible securities.

   $$
   w_i = \frac{MC_i}{\sum_{j=1}^{N} MC_j}
   $$

   where $MC_i$ is the market capitalization of security $i$, and $N$ is the number of constituents.
2. **Threshold Enforcement** - Securities whose calculated weight falls below $w_{\min}$ _(as defined in the Minimum
   Weight
   Threshold Rule)_ are excluded. Weights are then recalculated iteratively until all remaining securities meet or
   exceed the
   minimum threshold.
3. **Rounding Adjustment** _(Largest Remainder Method)_ - Final weights are scaled and rounded to the specified
   precision.
   Any rounding discrepancies are corrected using the Largest Remainder Method, which distributes residual units to
   securities with the largest fractional remainders. This ensures that the sum of all constituent weights equals
   exactly 100%.