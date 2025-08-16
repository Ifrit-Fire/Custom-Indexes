# Index Methodology

## Minimum Weight Threshold Rule

The indexes apply a proportional minimum weight threshold to maintain constituent significance and reduce the impact of
low-capitalization securities. For a target constituent count $N$, the minimum allowable weight, $w_{\min}$, is defined as:

$w_{\min} = \frac{1}{N}$

where $w_{\min}$ is expressed as a percentage of the total index weight. This rule is similar to the equal-weight
allocation for $N$ securities but allows for weight adjustments based on market cap.

This approach ensures that:

1. **Proportional Scaling** – The minimum threshold dynamically adjusts with index breadth.
2. **Constituent Significance** – Every included security contributes a meaningful share of index exposure.
3. **Weight Threshold Enforcement** – Securities falling below the minimum weight threshold are systematically removed.