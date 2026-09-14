# v4_volume_har_1: 5m buyer-volume HAR experiment

## Specification

- Training window: 2026-08-26 through 2026-09-13 UTC
- Rows: 120,350 total; 18,053 in the final chronological test split
- Baseline inputs: annualized realized volatility over 30s, 1m, and 5m
- Added inputs: `log(1 + buyer volume)` summed across venues over 30s, 5m, and 10m
- Estimator: non-negative linear regression
- Split: chronological 70% train / 15% validation / 15% test

The 10-minute window is intentional. The model was promoted by operator
approval after evaluation; live serving uses a dedicated v4 Redis contract and
does not depend on Feast.

## Final holdout

| Model | QLIKE | RMSE | MAE | R2 |
|---|---:|---:|---:|---:|
| Original HAR | -3.29046 | 0.06374 | 0.05017 | 0.20618 |
| Buyer-volume HAR | -3.31160 | 0.06089 | 0.04540 | 0.27574 |
| EWMA | -3.22908 | 0.05806 | 0.03967 | - |

The volume model improves QLIKE by 0.64% versus the same-row HAR baseline and
2.56% versus EWMA. RMSE improves 4.48% and MAE improves 9.51% versus the HAR
baseline, but both remain worse than EWMA.

## Coefficients

| Input | Coefficient |
|---|---:|
| realized_vol_30s | 0.03315 |
| realized_vol_1m | 0.07304 |
| realized_vol_5m | 0.50259 |
| log_buy_volume_30s | 0.01505 |
| log_buy_volume_5m | 0.00000 |
| log_buy_volume_10m | 0.01470 |

The 5-minute volume window adds no incremental value after the other variables.
The positive volume contribution comes from the 30-second and 10-minute
windows.

## Robustness

Across five expanding chronological test windows, buyer volume improved QLIKE
in three and worsened it in two. QLIKE changes versus the same-row baseline were
`+0.16%`, `-0.38%`, `-0.11%`, `+0.71%`, and `+0.43%` (positive means better).
The mixed rolling result remains a monitoring risk despite operator-approved
promotion. The v2 producer was retained during the rollout window; after v4
parity and production inference were established, the deployed v2 producer was
retired. The prior immutable model bundle remains available for rollback.

The live release validates all three buyer-volume fields and freshness before
the deployment passes. Consider replacing absolute cross-venue volume with a normalized
buyer share or venue-normalized volume to reduce sensitivity to Binance's much
larger reported volume.
