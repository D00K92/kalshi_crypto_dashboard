# Feature Engineering & Data Resampling Guide for Multi-Venue High-Frequency Volatility Prediction

> Research reference, not a runtime contract. The deployed contract is
> `market_features/v2_10s` with `log_return` and `venue_count`; the active
> forecast horizons are 5m, 15m, 30m, and 1h. Use `ARCHITECTURE.md` and the
> service READMEs for current implementation details.

A production-grade architectural guide and feature engineering blueprint for modeling multi-horizon (**5-minute, 10-minute, 30-minute, and 1-hour**) realized volatility from tick-level trade data and L2/L3 order book updates across 4 execution venues.

---

## 1. High-Frequency Resampling & Data Pipeline Strategy

When working with asynchronous, microsecond-level tick data and L2 limit order book updates across multiple execution venues, converting raw streams into a synchronized time series requires a strict division between **state variables** (snapshots of market conditions) and **flow/event variables** (cumulative actions over time).

### 1.1 Column Treatment Matrix

| Data Column Category | Raw Columns / Metrics | Resampling Operator | Post-Resampling Gap Fill | Financial Rationale |
| :--- | :--- | :--- | :--- | :--- |
| **Order Book Prices** | Best Bid, Best Ask, Level $N$ Bid/Ask Prices | `last` | **Forward Fill (`ffill`)** | Book prices reflect an ongoing state until explicitly replaced or canceled. |
| **Order Book Depths** | Bid Size, Ask Size, Level $N$ Quantities | `last` | **Forward Fill (`ffill`)** | Liquidity depth represents continuous state at top/deep levels. |
| **Trade Prices** | Last Traded Price (LTP) | `last` | **Forward Fill (`ffill`)** | The last executed price remains the market consensus until the next transaction. |
| **Intraday Price Bounds** | Intraday High / Low within $1s$ | `max` / `min` | **Forward Fill (`ffill`)** to LTP | Captures intra-second price dispersion and range expansion. |
| **Trade Volumes & Counts**| Volume Traded, Number of Trades | `sum` / `count` | **Zero Fill (`fillna(0)`)** | Volume is a flow variable; zero trades in a second means zero volume. |
| **Aggressor Trade Flow** | Buy Volume, Sell Volume (via Lee-Ready) | `sum` | **Zero Fill (`fillna(0)`)** | Directed flow updates only when trades actively execute. |
| **Order Flow Imbalance** | Net $1s$ Order Flow Imbalance (OFI) | `sum` | **Zero Fill (`fillna(0)`)** | Delta in order book quotes/sizes measures continuous order book pressure. |

---

### 1.2 Step-by-Step Resampling Pipeline

```
Raw Async Ticks (Venue A, B, C, D)
 └── 1. Intra-Second Aggregation (Resolve microsecond collisions)
      └── State: Keep LAST quote/price update
      └── Flow: Aggregate SUM / MAX / MIN / COUNT
 └── 2. Uniform 1-Second Reindexing (Full time grid creation)
      └── Forward-fill state columns (Quotes, Depths, LTP)
      └── Zero-fill flow columns (Volume, Order Flow)
 └── 3. Cross-Venue Grid Alignment
      └── Outer Join or Time-Sync Outer Merge on Uniform Timestamp
 └── 4. Target & Rolling Feature Computation
```

1. **Intra-Second Collision Resolution:** When multiple updates occur within a single $1s$ bucket, aggregate state variables using `last` and flow variables using `sum`, `max`, or `min`.
2. **Calendar Grid Reindexing:** Reindex the aggregated dataset onto a continuous $1$-second timestamp index ($\Delta t = 1s$) spanning market open to close.
3. **Forward & Zero Filling:**
   * **State Variables:** Apply Forward Fill (`ffill`). If no quotes arrive during a second, the previous second's order book remains active.
   * **Flow Variables:** Fill missing seconds with `0` (`fillna(0)`).
4. **Target Formulation:** Compute forward-looking target variables $y_{t, h}$ across 4 horizons ($h \in \{5m, 10m, 30m, 1h\}$).

---

## 2. Advanced Feature Engineering Taxonomy

Features are generated across multiple rolling lookback windows ($	au \in \{30s, 1m, 5m, 15m, 30m\}$) to capture cross-scale dynamics.

```
                   Feature Engineering Framework
                               │
   ┌───────────────────────────┼───────────────────────────┐
   ▼                           ▼                           ▼
1. Microstructure          2. Trade Flow              3. Volatility Proxies
   & Order Book Dynamics      & Liquidity                 & High-Frequency Risk
   ├── WAP & Microprice       ├── Order Flow Imbalance    ├── Realized Volatility
   ├── Order Book Imbalance   ├── Aggressor Imbalance     ├── Bipower Variation
   └── Spread & Book Slope    └── Liquidity Consumption   └── Range-based Estimators
                                                           │
                                                           ▼
                                                      4. Cross-Venue Dynamics
                                                         ├── Price Divergence
                                                         ├── Lead-Lag Correlations
                                                         └── Liquidity Fragmentation
```

### Category 1: Microstructure & Order Book Dynamics

* **Weighted Average Price (WAP) & Microprice:**
  Top-of-book WAP weights prices by inverse depth to reflect immediate market balance:
  $$\text{WAP}_1 = \frac{P_{\text{bid},1} \cdot Q_{\text{ask},1} + P_{\text{ask},1} \cdot Q_{\text{bid},1}}{Q_{\text{bid},1} + Q_{\text{ask},1}}$$

  Extend across $N$ depth levels to construct a Multi-Level Microprice:
  $$\text{Microprice}_N = \sum_{i=1}^{N} w_i \left( \frac{P_{\text{bid},i} \cdot Q_{\text{ask},i} + P_{\text{ask},i} \cdot Q_{\text{bid},i}}{Q_{\text{bid},i} + Q_{\text{ask},i}} \right), \quad \text{where } w_i = \frac{1/i}{\sum_{j=1}^N 1/j}$$

* **Order Book Imbalance (OBI):**
  Measures cumulative depth asymmetry at level $k$:
  $$\text{OBI}_k = \frac{\sum_{i=1}^k Q_{\text{bid},i} - \sum_{i=1}^k Q_{\text{ask},i}}{\sum_{i=1}^k Q_{\text{bid},i} + \sum_{i=1}^k Q_{\text{ask},i}}$$

* **Spread Dynamics:**
  Absolute spread, relative spread ($rac{P_{\text{ask}} - P_{\text{bid}}}{\text{WAP}}$), and depth-weighted spread cost to execute fixed sizes (e.g., $\$100k$ sweep cost).

* **Order Book Slope:**
  Measures how rapidly volume accumulates as price moves away from the mid-price:
  $$\text{Slope}_{\text{bid}} = \frac{\sum_{i=1}^N Q_{\text{bid},i}}{P_{\text{bid},1} - P_{\text{bid},N}}, \quad \text{Slope}_{\text{ask}} = \frac{\sum_{i=1}^N Q_{\text{ask},i}}{P_{\text{ask},N} - P_{\text{ask},1}}$$

---

### Category 2: Trade Flow & Liquidity Dynamics

* **Order Flow Imbalance (OFI):**
  Tracks changes in bid/ask quotes and quantities between consecutive updates $t-1$ and $t$:
  $$e_t = \begin{cases} 
  Q_{\text{bid},t} & \text{if } P_{\text{bid},t} > P_{\text{bid},t-1} \\
  Q_{\text{bid},t} - Q_{\text{bid},t-1} & \text{if } P_{\text{bid},t} = P_{\text{bid},t-1} \\
  0 & \text{if } P_{\text{bid},t} < P_{\text{bid},t-1}
  \end{cases}$$
  $$e'_t = \begin{cases} 
  0 & \text{if } P_{\text{ask},t} > P_{\text{ask},t-1} \\
  Q_{\text{ask},t} - Q_{\text{ask},t-1} & \text{if } P_{\text{ask},t} = P_{\text{ask},t-1} \\
  Q_{\text{ask},t} & \text{if } P_{\text{ask},t} < P_{\text{ask},t-1}
  \end{cases}$$
  $$\text{OFI}_t = e_t - e'_t$$

* **Aggressor Volume Ratio:**
  Determines buyer vs. seller aggressiveness (using the Lee-Ready algorithm or tick rule):
  $$\text{Aggressor Ratio}_{\tau} = \frac{V_{\text{buy},\tau} - V_{\text{sell},\tau}}{V_{\text{buy},\tau} + V_{\text{sell},\tau}}$$

* **Liquidity Consumption Rate:**
  Ratio of volume executed relative to depth available at top of book ($rac{V_{\text{traded},\tau}}{Q_{\text{bid},1} + Q_{\text{ask},1}}$). High ratios indicate imminent volatility spikes.

---

### Category 3: Realized Volatility & High-Frequency Risk Proxies

* **Realized Volatility ($RV$):**
  Target variable and primary predictor calculated over sliding lookback windows $\tau$:
  $$RV_{\tau} = \sqrt{\sum_{i=1}^{n} r_i^2}, \quad \text{where } r_i = \ln\left(\frac{\text{WAP}_i}{\text{WAP}_{i-1}}\right)$$

* **Bipower Variation ($BV$):**
  Robust to price jumps. Discrepancies between $RV$ and $BV$ isolate jump volatility from continuous diffusion:
  $$BV_{\tau} = \frac{\pi}{2} \sum_{i=2}^n |r_i| |r_{i-1}|$$
  $$\text{Jump Component} = \max\left(0, RV_{\tau}^2 - BV_{\tau}^2\right)$$

* **Parkinson Volatility (Extreme Value Volatility):**
  Leverages $1s$ High ($H$) and Low ($L$) price ranges to improve sample efficiency:
  $$\sigma_{\text{Parkinson}} = \sqrt{\frac{1}{4 \ln 2 \cdot n} \sum_{i=1}^n \left( \ln \frac{H_i}{L_i} \right)^2}$$

* **Garman-Klass Volatility:**
  Combines High, Low, Open ($O$), and Close ($C$) $1s$ bars for minimum-variance volatility estimation:
  $$\sigma_{\text{GK}} = \sqrt{\frac{1}{n} \sum_{i=1}^n \left[ 0.511 \left(\ln \frac{H_i}{L_i}\right)^2 - 0.019 \left( \ln \frac{C_i}{O_i} \cdot \ln \frac{H_i L_i}{O_i^2} - 2 \ln \frac{H_i}{O_i} \ln \frac{L_i}{O_i} \right) - 0.383 \left(\ln \frac{C_i}{O_i}\right)^2 \right]}$$

---

### Category 4: Cross-Venue / Arbitrage Features (4-Venue Dynamics)

* **Venue Price Divergence / Arbitrage Spread:**
  Difference between Venue $m$'s WAP and the Consolidated Best Bid/Offer (CBBO) Mid-price:
  $$\Delta P_{m} = \text{WAP}_{m} - \text{WAP}_{\text{CBBO}}$$

* **Venue Lead-Lag Correlation:**
  Cross-correlation between $1s$ returns on Venue $m$ and Venue $k$ at lag $\delta \in \{1s, 2s, 5s\}$:
  $$\rho_{m,k}(\delta) = \text{Corr}\left(r_{m,t}, r_{k,t-\delta}\right)$$

* **Cross-Venue Liquidity Fragmentation (HHI Index):**
  Herfindahl-Hirschman Index of liquidity distribution across all 4 venues:
  $$\text{HHI}_{t} = \sum_{m=1}^{4} \left( \frac{S_{m,t}}{\sum_{j=1}^4 S_{j,t}} \right)^2, \quad S_{m,t} = Q_{\text{bid},m,1} + Q_{\text{ask},m,1}$$
  *Low HHI indicates fragmented liquidity, which correlates with higher cross-venue latency arbitrage and volatility.*

---

## 3. Production Python & Dask Implementation

### 3.1 Efficient Resampling & Pipeline Implementation

```python
import dask.dataframe as dd
import numpy as np
import pandas as pd


def resample_single_venue(
    df: dd.DataFrame, venue_id: str, freq: str = "1s"
) -> dd.DataFrame:
    """Resamples asynchronous tick/orderbook data into a uniform 1-second grid."""
    # Ensure datetime index
    if "timestamp" in df.columns:
        df["timestamp"] = dd.to_datetime(df["timestamp"])
        df = df.set_index("timestamp")

    # Column Aggregation Rules
    agg_rules = {
        "bid_price_1": "last",
        "ask_price_1": "last",
        "bid_size_1": "last",
        "ask_size_1": "last",
        "trade_price": "last",
        "trade_volume": "sum",
        "buy_volume": "sum",
        "sell_volume": "sum",
        "trade_count": "count",
    }

    # Resample to uniform time buckets
    resampled = df.resample(freq).agg(agg_rules)

    # State variables: Forward fill last known quote/price
    state_cols = [
        "bid_price_1",
        "ask_price_1",
        "bid_size_1",
        "ask_size_1",
        "trade_price",
    ]
    resampled[state_cols] = resampled[state_cols].ffill()

    # Flow variables: Fill non-trading seconds with zero
    flow_cols = ["trade_volume", "buy_volume", "sell_volume", "trade_count"]
    resampled[flow_cols] = resampled[flow_cols].fillna(0)

    # Prefix columns with venue identifier
    resampled = resampled.rename(
        columns={c: f"{venue_id}_{c}" for c in resampled.columns}
    )

    return resampled


def build_consolidated_grid(venue_dfs: dict) -> dd.DataFrame:
    """Merges multiple resampled venue dataframes into a single aligned dataset."""
    resampled_list = [
        resample_single_venue(df, v_id) for v_id, df in venue_dfs.items()
    ]

    # Join on timestamp index
    consolidated = resampled_list[0]
    for other in resampled_list[1:]:
        consolidated = consolidated.join(other, how="outer")

    # Forward fill state variables across merged grid, zero fill flows
    for col in consolidated.columns:
        if "volume" in col or "count" in col:
            consolidated[col] = consolidated[col].fillna(0)
        else:
            consolidated[col] = consolidated[col].ffill()

    return consolidated
```

---

### 3.2 High-Performance Feature Calculation Routine

```python
def compute_microstructure_features(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates microstructure, volatility, and order flow features on resampled pandas partition."""
    out = pd.DataFrame(index=df.index)

    venues = list(
        set([c.split("_")[0] for c in df.columns if "_" in c])
    )

    for v in venues:
        bp = df[f"{v}_bid_price_1"]
        ap = df[f"{v}_ask_price_1"]
        bs = df[f"{v}_bid_size_1"]
        aps = df[f"{v}_ask_size_1"]

        # WAP & Mid Price
        wap = (bp * aps + ap * bs) / (bs + aps)
        out[f"{v}_wap"] = wap
        out[f"{v}_spread"] = ap - bp
        out[f"{v}_relative_spread"] = out[f"{v}_spread"] / wap

        # Order Book Imbalance (OBI)
        out[f"{v}_obi"] = (bs - aps) / (bs + aps)

        # Log Returns (1s)
        log_ret = np.log(wap / wap.shift(1)).fillna(0)
        out[f"{v}_log_ret"] = log_ret

        # Order Flow Imbalance (OFI)
        bid_delta = bp - bp.shift(1)
        ask_delta = ap - ap.shift(1)

        e_t = np.where(
            bid_delta > 0,
            bs,
            np.where(bid_delta == 0, bs - bs.shift(1), 0),
        )
        e_t_prime = np.where(
            ask_delta < 0,
            aps,
            np.where(ask_delta == 0, aps - aps.shift(1), 0),
        )
        out[f"{v}_ofi"] = e_t - e_t_prime

        # Multi-scale Realized Volatility & Bipower Variation
        for window in [30, 60, 300, 900]:  # 30s, 1m, 5m, 15m
            # Realized Volatility
            rv = np.sqrt(
                log_ret.pow(2).rolling(window, min_periods=5).sum()
            )
            out[f"{v}_rv_{window}s"] = rv

            # Bipower Variation
            bv = (
                (np.pi / 2)
                * (
                    log_ret.abs()
                    * log_ret.abs().shift(1)
                )
                .rolling(window, min_periods=5)
                .sum()
            )
            out[f"{v}_bv_{window}s"] = np.sqrt(bv)

            # Jump Component Ratio
            out[f"{v}_jump_ratio_{window}s"] = (
                np.maximum(0, rv**2 - bv) / (rv**2 + 1e-8)
            )

    # --- Cross-Venue Features ---
    if len(venues) > 1:
        waps = [out[f"{v}_wap"] for v in venues]
        cbbo_mid = pd.concat(waps, axis=1).mean(axis=1)

        # Cross-Venue Dispersion
        out["cross_venue_wap_std"] = pd.concat(waps, axis=1).std(
            axis=1
        )

        # HHI Volume Concentration
        total_depths = [
            df[f"{v}_bid_size_1"] + df[f"{v}_ask_size_1"]
            for v in venues
        ]
        sum_depth = pd.concat(total_depths, axis=1).sum(axis=1)
        hhi = sum(
            [(d / (sum_depth + 1e-8)) ** 2 for d in total_depths]
        )
        out["cross_venue_hhi"] = hhi

    return out
```

---

## 4. Modeling & Validation Architecture

To predict multi-horizon volatility ($5m, 10m, 30m, 1h$) effectively, model training must prevent temporal data leakage and use an appropriate loss function tailored to non-negative volatility distributions.

```
Time Domain ──>
[ Train Window (e.g. 5 Days) ] [ Purge Gap = 1 Hour ] [ Validation Window (1 Day) ]
                               ^^^^^^^^^^^^^^^^^^^^^^^
                               Prevents Target Leakage
```

### 4.1 Target Formulation
Define the multi-horizon target variable $y_{t, h}$ for horizon $h \in \{300s, 600s, 1800s, 3600s\}$ at time $t$ as:
$$y_{t, h} = \sqrt{\sum_{k=1}^{h} r_{t+k}^2}$$

### 4.2 Leakage-Free Validation Strategy: Purged Time-Series Split
* **The Overlap Problem:** Because the target $y_{t, 1h}$ looks forward 1 hour into the future, consecutive samples $t$ and $t+1s$ share $3,599$ seconds of identical forward price return information. Standard $K$-Fold cross-validation results in severe data leakage and inflated out-of-sample metrics.
* **Purge Gap Implementation:** Enforce a **Purge Gap equal to or greater than the maximum prediction horizon ($1$ hour)** between training and validation sets. Any training row within $1$ hour prior to the validation split start time must be discarded.

### 4.3 Objective Function: RMSPE
Because realized volatility is strictly positive and displays dynamic heteroskedasticity (high volatility regimes produce higher variance errors), standard Mean Squared Error (MSE) heavily overweights extreme volatility spikes. Optimize **Root Mean Squared Percentage Error (RMSPE)**:

$$\text{RMSPE} = \sqrt{\frac{1}{N} \sum_{i=1}^N \left( \frac{y_i - \hat{y}_i}{y_i} \right)^2}$$

In LightGBM, implement custom RMSPE gradients or fit on log-transformed targets $\tilde{y} = \ln(y)$ using standard RMSE loss:
$$\hat{y} = \exp\left( \widehat{\ln(y)} \right)$$

---

## 5. Execution Summary Plan

1. **ETL & Resampling Engine:** Execute Dask partition-wise processing on Hive storage to construct synchronized $1s$ bars across all 4 venues.
2. **Feature Extraction:** Construct rolling microstructure, order flow imbalance, realized jump components, and cross-venue dispersion features.
3. **Target Alignment:** Append $t+5m, t+10m, t+30m, t+1h$ realized volatility targets.
4. **Purged Training:** Fit LightGBM / XGBoost Regressors using Purged Group Time-Series CV and evaluate performance with RMSPE and SHAP importance rankings.
