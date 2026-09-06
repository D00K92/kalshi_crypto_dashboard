# Aggregated Order Book Implementation Guide: Price Bucketing Method

## 1. Overview
When building a consolidated order book across multiple cryptocurrency exchanges (e.g., Binance, Bybit, Coinbase), aggregating raw price levels directly often leads to crossed order books (where the aggregated bid price exceeds the ask price). This occurs due to:
* **Venue Price Discrepancies:** Micro-dollar differences in the reference price across venues (e.g., 10–30 cents discrepancy in BTC pairs).
* **Network Latency & Clock Desynchronization:** Asynchronous WebSocket updates causing momentary misalignments in top-of-book prices.
* **Tick Density Variations:** Differing tick sizes and minimum order increments between venues.

The **Price Bucketing Method** resolves these issues by mapping each exchange's granular order book into fixed, discrete price intervals (buckets). This aggregates volume within predefined ranges, eliminating crossed-book visual anomalies while providing a clean, unified view of market depth.

---

## 2. Mathematical Formulation

### 2.1 Aggregated Mid Price Calculation
The global reference mid price $\text{Mid}_{\text{agg}}$ can be calculated as a volume-weighted average mid price across $N$ active venues:

$$\text{Mid}_{v} = \frac{\text{BestBid}_{v} + \text{BestAsk}_{v}}{2}$$

$$\text{Mid}_{\text{agg}} = \frac{\sum_{v=1}^{N} \left( \text{Mid}_{v} \cdot \text{Vol}_{v} \right)}{\sum_{v=1}^{N} \text{Vol}_{v}}$$

Where $\text{Vol}_{v}$ represents top-of-book or $k$-level depth volume for venue $v$. Alternatively, a simple unweighted average of valid venue mid prices can be used for computational efficiency:

$$\text{Mid}_{\text{agg}} = \frac{1}{N} \sum_{v=1}^{N} \text{Mid}_{v}$$

### 2.2 Quantization & Binning Functions
Given a user-defined bucket size $B$ (e.g., $B = 1.00$, $B = 5.00$, or $B = 10.00$ for BTC/USDT), raw order prices $P$ are mapped to bucket identifiers as follows:

For **Bids** (rounded down to the lower boundary):
$$\text{Bucket}_{\text{Bid}}(P, B) = \lfloor \frac{P}{B} \rfloor \times B$$

For **Asks** (rounded up to the upper boundary):
$$\text{Bucket}_{\text{Ask}}(P, B) = \lceil \frac{P}{B} \rceil \times B$$

### 2.3 Volume Aggregation & Filtering
For each price bucket index $k$, the total accumulated volume $V_k$ across all $N$ venues is defined by:

$$V_{k, \text{Bid}} = \sum_{v=1}^{N} \sum_{\{P_{i,v} \in \text{Bids}_v \mid \text{Bucket}_{\text{Bid}}(P_{i,v}, B) = k\}} Q_{i,v}$$

$$V_{k, \text{Ask}} = \sum_{v=1}^{N} \sum_{\{P_{j,v} \in \text{Asks}_v \mid \text{Bucket}_{\text{Ask}}(P_{j,v}, B) = k\}} Q_{j,v}$$

Where $Q_{i,v}$ is the order size at price level $P_{i,v}$ for venue $v$. 

To maintain strict non-crossing logic after binning:
1. Identify $\text{MaxBidBucket} = \max(\{k \mid V_{k, \text{Bid}} > 0\})$.
2. Identify $\text{MinAskBucket} = \min(\{k \mid V_{k, \text{Ask}} > 0\})$.
3. If $\text{MaxBidBucket} \ge \text{MinAskBucket}$, apply cross-book filtering by shifting overlapping bid volume to the highest non-overlapping bucket below $\text{MinAskBucket}$, or discarding transient crossing artifacts.

---

## 3. Production Python Implementation

Below is a complete, standalone Python module designed for execution in production pipeline agents or algorithmic trading engines.

```python
"""
Aggregated Order Book Engine (Price Bucketing Method)
-----------------------------------------------------
Aggregates live L2 order book updates from multiple crypto exchanges 
(Binance, Bybit, Coinbase) into standardized price buckets.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional
import time
import math


@dataclass
class OrderBookLevel:
    price: float
    quantity: float


@dataclass
class VenueOrderBook:
    venue_id: str
    symbol: str
    timestamp: float
    bids: List[OrderBookLevel]  # Expected sorted descending by price
    asks: List[OrderBookLevel]  # Expected sorted ascending by price

    @property
    def mid_price(self) -> Optional[float]:
        if self.bids and self.asks:
            return (self.bids[0].price + self.asks[0].price) / 2.0
        return None


@dataclass
class AggregatedLevel:
    price_bucket: float
    total_quantity: float
    venue_breakdown: Dict[str, float] = field(default_factory=dict)


@dataclass
class AggregatedOrderBook:
    symbol: str
    timestamp: float
    bucket_size: float
    aggregated_mid: float
    bids: List[AggregatedLevel]  # Sorted descending by price_bucket
    asks: List[AggregatedLevel]  # Sorted ascending by price_bucket


class OrderBookAggregator:
    """
    Maintains active local snapshots of multi-venue order books 
    and generates an aggregated, non-crossed price-bucketed order book.
    """

    def __init__(self, symbol: str, bucket_size: float = 1.0):
        """
        :param symbol: Trading pair identifier (e.g., 'BTC/USDT')
        :param bucket_size: Price aggregation interval (e.g., 1.0, 5.0, 10.0)
        """
        self.symbol = symbol
        self.bucket_size = float(bucket_size)
        self.venue_books: Dict[str, VenueOrderBook] = {}

    def update_venue_book(self, venue_id: str, snapshot: VenueOrderBook) -> None:
        """
        Ingest or update the local snapshot for a given venue.
        """
        self.venue_books[venue_id] = snapshot

    def _quantize_bid_price(self, price: float) -> float:
        """Floor bid price to the nearest bucket step."""
        return math.floor(price / self.bucket_size) * self.bucket_size

    def _quantize_ask_price(self, price: float) -> float:
        """Ceil ask price to the nearest bucket step."""
        return math.ceil(price / self.bucket_size) * self.bucket_size

    def compute_aggregated_mid(self) -> float:
        """
        Calculate volume-weighted or simple average mid price across active venues.
        """
        valid_mids = []
        for book in self.venue_books.values():
            mid = book.mid_price
            if mid is not None:
                valid_mids.append(mid)

        if not valid_mids:
            return 0.0

        return sum(valid_mids) / len(valid_mids)

    def aggregate(self, max_depth_levels: int = 20) -> AggregatedOrderBook:
        """
        Builds the aggregated bucketed order book across all registered venues.
        
        :param max_depth_levels: Maximum number of aggregated levels to return per side.
        :return: AggregatedOrderBook object containing standardized bids and asks.
        """
        bid_buckets: Dict[float, Dict[str, float]] = {}
        ask_buckets: Dict[float, Dict[str, float]] = {}

        current_time = time.time()

        # 1. Aggregate Bids across all venues
        for venue_id, book in self.venue_books.items():
            for level in book.bids:
                bucket = self._quantize_bid_price(level.price)
                if bucket not in bid_buckets:
                    bid_buckets[bucket] = {}
                bid_buckets[bucket][venue_id] = (
                    bid_buckets[bucket].get(venue_id, 0.0) + level.quantity
                )

        # 2. Aggregate Asks across all venues
        for venue_id, book in self.venue_books.items():
            for level in book.asks:
                bucket = self._quantize_ask_price(level.price)
                if bucket not in ask_buckets:
                    ask_buckets[bucket] = {}
                ask_buckets[bucket][venue_id] = (
                    ask_buckets[bucket].get(venue_id, 0.0) + level.quantity
                )

        # 3. Sort Buckets
        sorted_bid_keys = sorted(bid_buckets.keys(), reverse=True)
        sorted_ask_keys = sorted(ask_buckets.keys())

        # 4. Resolve Crossing Buckets if necessary
        if sorted_bid_keys and sorted_ask_keys:
            highest_bid_bucket = sorted_bid_keys[0]
            lowest_ask_bucket = sorted_ask_keys[0]

            if highest_bid_bucket >= lowest_ask_bucket:
                sorted_bid_keys = [k for k in sorted_bid_keys if k < lowest_ask_bucket]

        # 5. Format Output Levels
        aggregated_bids: List[AggregatedLevel] = []
        for key in sorted_bid_keys[:max_depth_levels]:
            breakdown = bid_buckets[key]
            total_qty = sum(breakdown.values())
            aggregated_bids.append(
                AggregatedLevel(
                    price_bucket=key,
                    total_quantity=round(total_qty, 6),
                    venue_breakdown={v: round(q, 6) for v, q in breakdown.items()}
                )
            )

        aggregated_asks: List[AggregatedLevel] = []
        for key in sorted_ask_keys[:max_depth_levels]:
            breakdown = ask_buckets[key]
            total_qty = sum(breakdown.values())
            aggregated_asks.append(
                AggregatedLevel(
                    price_bucket=key,
                    total_quantity=round(total_qty, 6),
                    venue_breakdown={v: round(q, 6) for v, q in breakdown.items()}
                )
            )

        agg_mid = self.compute_aggregated_mid()

        return AggregatedOrderBook(
            symbol=self.symbol,
            timestamp=current_time,
            bucket_size=self.bucket_size,
            aggregated_mid=round(agg_mid, 2),
            bids=aggregated_bids,
            asks=aggregated_asks
        )


# =====================================================================
# Example Usage & Verification Script
# =====================================================================
if __name__ == "__main__":
    # Create aggregator with a $5.00 price bucket step size
    aggregator = OrderBookAggregator(symbol="BTC/USDT", bucket_size=5.0)

    # Simulated Live L2 Feed Inputs from Binance, Bybit, and Coinbase
    binance_feed = VenueOrderBook(
        venue_id="binance",
        symbol="BTC/USDT",
        timestamp=time.time(),
        bids=[
            OrderBookLevel(price=65004.20, quantity=1.5),
            OrderBookLevel(price=65002.10, quantity=0.8),
            OrderBookLevel(price=64998.50, quantity=2.1),
        ],
        asks=[
            OrderBookLevel(price=65006.10, quantity=1.1),
            OrderBookLevel(price=65008.30, quantity=2.5),
            OrderBookLevel(price=65012.00, quantity=3.0),
        ]
    )

    bybit_feed = VenueOrderBook(
        venue_id="bybit",
        symbol="BTC/USDT",
        timestamp=time.time(),
        bids=[
            OrderBookLevel(price=65004.50, quantity=0.5),
            OrderBookLevel(price=65001.00, quantity=1.2),
            OrderBookLevel(price=64997.00, quantity=1.8),
        ],
        asks=[
            OrderBookLevel(price=65005.80, quantity=0.9),
            OrderBookLevel(price=65009.00, quantity=1.4),
            OrderBookLevel(price=65011.50, quantity=2.2),
        ]
    )

    coinbase_feed = VenueOrderBook(
        venue_id="coinbase",
        symbol="BTC/USDT",
        timestamp=time.time(),
        bids=[
            OrderBookLevel(price=65003.80, quantity=2.0),
            OrderBookLevel(price=65000.50, quantity=1.1),
            OrderBookLevel(price=64996.20, quantity=0.9),
        ],
        asks=[
            OrderBookLevel(price=65006.50, quantity=1.7),
            OrderBookLevel(price=65009.80, quantity=0.6),
            OrderBookLevel(price=65013.10, quantity=1.5),
        ]
    )

    # Register venue snapshots
    aggregator.update_venue_book("binance", binance_feed)
    aggregator.update_venue_book("bybit", bybit_feed)
    aggregator.update_venue_book("coinbase", coinbase_feed)

    # Produce aggregated orderbook
    agg_book = aggregator.aggregate(max_depth_levels=5)

    print(f"=== Aggregated OrderBook for {agg_book.symbol} ===")
    print(f"Aggregated Mid Price: ${agg_book.aggregated_mid}")
    print(f"Bucket Size: ${agg_book.bucket_size}\n")

    print("--- ASKS (Top Depth) ---")
    for ask in reversed(agg_book.asks):
        print(f"Bucket Price: ${ask.price_bucket:<8.2f} | Total Vol: {ask.total_quantity:<6.2f} | Venues: {ask.venue_breakdown}")

    print("\n--- BIDS (Top Depth) ---")
    for bid in agg_book.bids:
        print(f"Bucket Price: ${bid.price_bucket:<8.2f} | Total Vol: {bid.total_quantity:<6.2f} | Venues: {bid.venue_breakdown}")
```

---

## 4. Operational Considerations & Best Practices

1. **Bucket Size Selection:**
   * For **BTC/USDT**, bucket sizes of **$1.00, $5.00, or $10.00** work best depending on market volatility.
   * For **ETH/USDT**, bucket sizes of **$0.25, $0.50, or $1.00** are recommended.
   * Bucket size can be dynamically adjusted based on 1-hour realized volatility or ATR (Average True Range).

2. **Handling Transient Crosses:**
   * In high-frequency environments, network jitter may cause `Highest Bid Bucket >= Lowest Ask Bucket`.
   * Ensure your pipeline either filters out overlapping bid/ask buckets (as implemented above) or computes a synthetic cleared book via virtual execution matching.

3. **Performance Optimization:**
   * When handling high-throughput WebSockets (>1,000 updates/sec), replace dictionary allocations with static `numpy` arrays pre-allocated to fixed price ranges around the current mid price.