### Category 1: Microstructure & Order Book Dynamics

* **Weighted Average Price (WAP) & Microprice:**
  Top-of-book WAP weights prices by inverse depth to reflect immediate market balance:
  $$\\text{WAP}_1 = \\frac{P_{\\text{bid},1} \\cdot Q_{\\text{ask},1} + P_{\\text{ask},1} \\cdot Q_{\\text{bid},1}}{Q_{\\text{bid},1} + Q_{\\text{ask},1}}$$

  Extend across $N$ depth levels to construct a Multi-Level Microprice:
  $$\\text{Microprice}_N = \\sum_{i=1}^{N} w_i \\left( \\frac{P_{\\text{bid},i} \\cdot Q_{\\text{ask},i} + P_{\\text{ask},i} \\cdot Q_{\\text{bid},i}}{Q_{\\text{bid},i} + Q_{\\text{ask},i}} \\right), \\quad \\text{where } w_i = \\frac{1/i}{\\sum_{j=1}^N 1/j}$$

* **Order Book Imbalance (OBI):**
  Measures cumulative depth asymmetry at level $k$:
  $$\\text{OBI}_k = \\frac{\\sum_{i=1}^k Q_{\\text{bid},i} - \\sum_{i=1}^k Q_{\\text{ask},i}}{\\sum_{i=1}^k Q_{\\text{bid},i} + \\sum_{i=1}^k Q_{\\text{ask},i}}$$

* **Spread Dynamics:**
  Absolute spread, relative spread ($\frac{P_{\\text{ask}} - P_{\\text{bid}}}{\\text{WAP}}$), and depth-weighted spread cost to execute fixed sizes (e.g., $\$100k$ sweep cost).

* **Order Book Slope:**
  Measures how rapidly volume accumulates as price moves away from the mid-price:
  $$\\text{Slope}_{\\text{bid}} = \\frac{\\sum_{i=1}^N Q_{\\text{bid},i}}{P_{\\text{bid},1} - P_{\\text{bid},N}}, \\quad \\text{Slope}_{\\text{ask}} = \\frac{\\sum_{i=1}^N Q_{\\text{ask},i}}{P_{\\text{ask},N} - P_{\\text{ask},1}}$$

---

### Category 2: Trade Flow & Liquidity Dynamics

* **Order Flow Imbalance (OFI):**
  Tracks changes in bid/ask quotes and quantities between consecutive updates $t-1$ and $t$:
  $$e_t = \\begin{cases}    Q_{\\text{bid},t} & \\text{if } P_{\\text{bid},t} > P_{\\text{bid},t-1} \\\\   Q_{\\text{bid},t} - Q_{\\text{bid},t-1} & \\text{if } P_{\\text{bid},t} = P_{\\text{bid},t-1} \\\\   0 & \\text{if } P_{\\text{bid},t} < P_{\\text{bid},t-1}   \\end{cases}$$
  $$e'_t = \\begin{cases}    0 & \\text{if } P_{\\text{ask},t} > P_{\\text{ask},t-1} \\\\   Q_{\\text{ask},t} - Q_{\\text{ask},t-1} & \\text{if } P_{\\text{ask},t} = P_{\\text{ask},t-1} \\\\   Q_{\\text{ask},t} & \\text{if } P_{\\text{ask},t} < P_{\\text{ask},t-1}   \\end{cases}$$
  $$\\text{OFI}_t = e_t - e'_t$$

* **Aggressor Volume Ratio:**
  Determines buyer vs. seller aggressiveness (using the Lee-Ready algorithm or tick rule):
  $$\\text{Aggressor Ratio}_{\\tau} = \\frac{V_{\\text{buy},\\tau} - V_{\\text{sell},\\tau}}{V_{\\text{buy},\\tau} + V_{\\text{sell},\\tau}}$$

* **Liquidity Consumption Rate:**
  Ratio of volume executed relative to depth available at top of book ($\frac{V_{\\text{traded},\\tau}}{Q_{\\text{bid},1} + Q_{\\text{ask},1}}$). High ratios indicate imminent volatility spikes.

---

### Category 3: Realized Volatility & High-Frequency Risk Proxies

* **Realized Volatility ($RV$):**
  Target variable and primary predictor calculated over sliding lookback windows $\\tau$:
  $$RV_{\\tau} = \\sqrt{\\sum_{i=1}^{n} r_i^2}, \\quad \\text{where } r_i = \\ln\\left(\\frac{\\text{WAP}_i}{\\text{WAP}_{i-1}}\\right)$$

* **Bipower Variation ($BV$):**
  Robust to price jumps. Discrepancies between $RV$ and $BV$ isolate jump volatility from continuous diffusion:
  $$BV_{\\tau} = \\frac{\\pi}{2} \\sum_{i=2}^n \vert{}r_i\vert{} \vert{}r_{i-1}\vert{}$$
  $$\\text{Jump Component} = \\max\\left(0, RV_{\\tau}^2 - BV_{\\tau}^2\\right)$$

* **Parkinson Volatility (Extreme Value Volatility):**
  Leverages $1s$ High ($H$) and Low ($L$) price ranges to improve sample efficiency:
  $$\\sigma_{\\text{Parkinson}} = \\sqrt{\\frac{1}{4 \\ln 2 \\cdot n} \\sum_{i=1}^n \\left( \\ln \\frac{H_i}{L_i} \\right)^2}$$

* **Garman-Klass Volatility:**
  Combines High, Low, Open ($O$), and Close ($C$) $1s$ bars for minimum-variance volatility estimation:
  $$\\sigma_{\\text{GK}} = \\sqrt{\\frac{1}{n} \\sum_{i=1}^n \\left[ 0.511 \\left(\\ln \\frac{H_i}{L_i}\\right)^2 - 0.019 \\left( \\ln \\frac{C_i}{O_i} \\cdot \\ln \\frac{H_i L_i}{O_i^2} - 2 \\ln \\frac{H_i}{O_i} \\ln \\frac{L_i}{O_i} \\right) - 0.383 \\left(\\ln \\frac{C_i}{O_i}\\right)^2 \\right]}$$

---

### Category 4: Cross-Venue / Arbitrage Features (4-Venue Dynamics)

* **Venue Price Divergence / Arbitrage Spread:**
  Difference between Venue $m$'s WAP and the Consolidated Best Bid/Offer (CBBO) Mid-price:
  $$\\Delta P_{m} = \\text{WAP}_{m} - \\text{WAP}_{\\text{CBBO}}$$

* **Venue Lead-Lag Correlation:**
  Cross-correlation between $1s$ returns on Venue $m$ and Venue $k$ at lag $\\delta \\in \\{1s, 2s, 5s\\}$:
  $$\\rho_{m,k}(\\delta) = \\text{Corr}\\left(r_{m,t}, r_{k,t-\\delta}\\right)$$

* **Cross-Venue Liquidity Fragmentation (HHI Index):**
  Herfindahl-Hirschman Index of liquidity distribution across all 4 venues:
  $$\\text{HHI}_{t} = \\sum_{m=1}^{4} \\left( \\frac{S_{m,t}}{\\sum_{j=1}^4 S_{j,t}} \\right)^2, \\quad S_{m,t} = Q_{\\text{bid},m,1} + Q_{\\text{ask},m,1}$$
  *Low HHI indicates fragmented liquidity, which correlates with higher cross-venue latency arbitrage and volatility.*