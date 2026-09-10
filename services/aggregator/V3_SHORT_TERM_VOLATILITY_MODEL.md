# V3 short-term volatility model

## Objective

Predict future annualized realized volatility from completed 10-second market bars. V3 retains the V2 10-second cadence while adding market-state features that can capture short-term volatility regime changes.

## Targets

Train one model per horizon. At feature timestamp `t`, each target is calculated exclusively from returns after `t`.

| Target | Future 10-second returns | Horizon |
| --- | ---: | ---: |
| `target_5m` | 30 | 5 minutes |
| `target_15m` | 90 | 15 minutes |
| `target_30m` | 180 | 30 minutes |
| `target_1h` | 360 | 1 hour |

Use the log of annualized future realized volatility as the training target and exponentiate predictions for serving.

## Feature groups

### Price and return dynamics

- Current 10-second log return, absolute return, and squared return.
- Rolling return, absolute-return, squared-return, and realized-volatility summaries over 1m, 5m, 15m, and 30m.
- Rolling high-low range, distance from rolling VWAP or moving average, jump flags, and time since the last jump.
- Signed-return imbalance and short-lag return autocorrelation.

### Cross-venue state

- Number of contributing venues and changes in venue count.
- Cross-venue mean, range, standard deviation, and return disagreement.
- Per-venue availability flags so temporary venue loss is explicit rather than silently imputed.

### Volatility regime

- Separate EWMA variance/volatility estimates driven by 10-second, 5-minute, 15-minute, and 30-minute returns.
- Relative-regime features such as `RV_1m / RV_30m`, EWMA ratios, and volatility-of-volatility.
- Recent maximum absolute return and counts of large-return events.

### Market context

- Cyclical minute-of-hour and hour-of-day fields, plus weekday/weekend flags.
- Kalshi time-to-expiry when the forecast supports Kalshi pricing.
- Kalshi implied volatility only after its historical availability and timestamp contract are verified.

## Data contract and leakage rules

- A V3 row is emitted only for a completed 10-second bucket.
- Every feature must use data with `available_at <= feature_timestamp`.
- The future target begins strictly after the feature timestamp; the current return cannot appear in both feature and target.
- Persist `event_timestamp`, `available_at`, source coverage, and feature-contract version in every row.

## Model and evaluation

Start with one XGBoost regressor per horizon. Keep matching-horizon EWMA as a production baseline and fallback.

- Split train, validation, and test chronologically; never randomly shuffle time series rows.
- Compare against EWMA using MAE/RMSE of log volatility, calibration by realized-volatility decile, and rising/falling-volatility directional accuracy.
- Apply explicit prediction floor and cap in serving.
- Shadow-publish `model_vol` and `ewma_vol` before using V3 for live contract pricing.

## Initial delivery scope

Begin with price dynamics, cross-venue dispersion, separate-horizon EWMA/RV, and calendar features. Add trade and order-book fields only after historical completeness and availability-time correctness are demonstrated.
