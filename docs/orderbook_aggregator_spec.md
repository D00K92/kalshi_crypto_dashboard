# System Specification: Multi-CEX Live Order Book Aggregator Engine

## 1. Overview & System Objectives
This document provides execution specifications for building a high-throughput, low-latency **Cross-Exchange Order Book Aggregator**. 

The engine ingests L2 order book data streams from multiple Centralized Exchanges (CEXs), normalizes price levels, applies fee adjustments, merges/bins liquidity levels, and dynamically uncrosses synthetic arbitrage levels before exposing a clean composite order book API/UI feed.

```
┌─────────────────┐
│ CEX WebSockets  │  (Binance, Coinbase, OKX, etc.)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 1. Ingestion    │  Stateful local book maintenance via sequence IDs
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 2. Fee Adjust   │  P_eff calculation (Taker fee integration)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 3. Sort & Bin   │  k-way merge heaps + fixed-precision tick buckets
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 4. Uncrossing   │  Synthetic volume netting loop (Best Bid < Best Ask)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 5. Export API   │  Clean composite depth feed (REST / WS / IPC)
└─────────────────┘
```

---

## 2. Detailed Technical Architecture & Pipeline

### Phase 1: Ingestion & Local Book Synchronization
- **Transport Layer:** Asynchronous WebSockets (`asyncio` / `tokio` / native event loop).
- **State Management:** Maintain a per-venue L2 order book structure in memory.
- **Resynchronization Logic:**
  - Ingest snapshot via REST on startup.
  - Buffer incoming WebSocket delta messages.
  - Validate continuity using `sequence_id` / `u` / `U` update IDs provided by the exchange.
  - Drop state and re-initialize if packet sequence loss or gap is detected.

### Phase 2: Fee-Adjusted Nominal Pricing (Cost Normalization)
Raw price levels across venues cannot be directly compared due to varying fee structures. Transform every price level $P$ into an **Effective Price ($P_{eff}$)** before sorting or binning.

$$	ext{Effective Ask Price } (P_{eff, ask}) = P_{raw, ask} 	imes (1 + 	ext{Fee}_{taker})$$

$$	ext{Effective Bid Price } (P_{eff, bid}) = P_{raw, bid} 	imes (1 - 	ext{Fee}_{taker})$$

*Where $	ext{Fee}_{taker}$ is the decimal taker fee rate for the venue (e.g., $0.0010$ for $0.10\%$).*

### Phase 3: Merging & Bucket Aggregation
1. **$k$-way Merge:**
   - Use a **Min-Heap** for Asks (lowest $P_{eff}$ first).
   - Use a **Max-Heap** for Bids (highest $P_{eff}$ first).
   - $k$ represents the number of active exchange feeds.
2. **Tick Size Binning (Bucketing):**
   - Standardize varying venue tick precisions into unified price buckets $\Delta_{tick}$.
   
$$	ext{Bucket Price}_{bid} = \lfloor rac{P_{eff, bid}}{\Delta_{tick}} floor 	imes \Delta_{tick}$$

$$	ext{Bucket Price}_{ask} = \lceil rac{P_{eff, ask}}{\Delta_{tick}} ceil 	imes \Delta_{tick}$$

   - Sum quantities across venues mapping to the same bucket price.

### Phase 4: Synthetic Uncrossing Loop (Resolution Strategy)
Because cross-venue latency and residual spreads can still cause synthetic crossed books ($	ext{Best Bid} \ge 	ext{Best Ask}$), run an uncrossing loop prior to publishing:

```python
def uncross_orderbook(bids, asks):
    """
    bids: Sorted list of [price, size] descending
    asks: Sorted list of [price, size] ascending
    """
    while bids and asks and bids[0].price >= asks[0].price:
        top_bid = bids[0]
        top_ask = asks[0]
        
        matched_qty = min(top_bid.size, top_ask.size)
        
        top_bid.size -= matched_qty
        top_ask.size -= matched_qty
        
        if top_bid.size == 0:
            bids.pop(0)
        if top_ask.size == 0:
            asks.pop(0)
            
    return bids, asks
```

### Phase 5: Export & Consumption Layer
Expose the composite L2 depth through high-speed internal structures or streaming endpoints for consumption by Smart Order Routers (SOR), execution algorithms, or trading UIs.

---

## 3. Reference Implementation (Python)

Below is a self-contained, production-grade template implementing the fee adjustment, binning, $k$-way merge, and uncrossing pipeline.

```python
import heapq
import math
from dataclasses import dataclass
from typing import Dict, List, Tuple

@dataclass
class OrderLevel:
    price: float
    size: float
    exchange: str

@dataclass
class AggregatedLevel:
    price: float
    size: float
    venues: Dict[str, float]

class OrderBookAggregator:
    def __init__(self, tick_size: float, taker_fees: Dict[str, float]):
        """
        :param tick_size: Target bucket size (e.g. 0.10, 1.00)
        :param taker_fees: Map of exchange name to decimal fee (e.g. {'binance': 0.001})
        """
        self.tick_size = tick_size
        self.taker_fees = taker_fees

    def _adjust_price(self, price: float, exchange: str, is_ask: bool) -> float:
        fee = self.taker_fees.get(exchange, 0.0)
        if is_ask:
            return price * (1.0 + fee)
        else:
            return price * (1.0 - fee)

    def _bucket_price(self, price: float, is_ask: bool) -> float:
        if is_ask:
            return math.ceil(price / self.tick_size) * self.tick_size
        else:
            return math.floor(price / self.tick_size) * self.tick_size

    def aggregate_and_uncross(
        self, 
        raw_bids: Dict[str, List[Tuple[float, float]]], 
        raw_asks: Dict[str, List[Tuple[float, float]]]
    ) -> Tuple[List[AggregatedLevel], List[AggregatedLevel]]:
        """
        Aggregates L2 order books across venues and uncrosses any residual overlaps.
        """
        # 1. Fee Adjust & Bucket Map
        bids_bucket_map: Dict[float, Dict[str, float]] = {}
        asks_bucket_map: Dict[float, Dict[str, float]] = {}

        # Process Bids
        for venue, levels in raw_bids.items():
            for raw_p, qty in levels:
                eff_p = self._adjust_price(raw_p, venue, is_ask=False)
                bucket_p = round(self._bucket_price(eff_p, is_ask=False), 8)
                
                if bucket_p not in bids_bucket_map:
                    bids_bucket_map[bucket_p] = {}
                bids_bucket_map[bucket_p][venue] = bids_bucket_map[bucket_p].get(venue, 0.0) + qty

        # Process Asks
        for venue, levels in raw_asks.items():
            for raw_p, qty in levels:
                eff_p = self._adjust_price(raw_p, venue, is_ask=True)
                bucket_p = round(self._bucket_price(eff_p, is_ask=True), 8)
                
                if bucket_p not in asks_bucket_map:
                    asks_bucket_map[bucket_p] = {}
                asks_bucket_map[bucket_p][venue] = asks_bucket_map[bucket_p].get(venue, 0.0) + qty

        # 2. k-way Merge / Heap Sorting
        # Max-Heap for Bids (Store negative price for max-heap in Python)
        bid_heap = [(-p, venues) for p, venues in bids_bucket_map.items()]
        heapq.heapify(bid_heap)
        sorted_bids: List[AggregatedLevel] = []
        while bid_heap:
            neg_p, venues = heapq.heappop(bid_heap)
            p = -neg_p
            total_qty = sum(venues.values())
            sorted_bids.append(AggregatedLevel(price=p, size=total_qty, venues=venues))

        # Min-Heap for Asks
        ask_heap = [(p, venues) for p, venues in asks_bucket_map.items()]
        heapq.heapify(ask_heap)
        sorted_asks: List[AggregatedLevel] = []
        while ask_heap:
            p, venues = heapq.heappop(ask_heap)
            total_qty = sum(venues.values())
            sorted_asks.append(AggregatedLevel(price=p, size=total_qty, venues=venues))

        # 3. Synthetic Uncrossing Loop
        i_bid = 0
        i_ask = 0
        while i_bid < len(sorted_bids) and i_ask < len(sorted_asks):
            top_bid = sorted_bids[i_bid]
            top_ask = sorted_asks[i_ask]

            if top_bid.price >= top_ask.price:
                matched_qty = min(top_bid.size, top_ask.size)
                
                top_bid.size -= matched_qty
                top_ask.size -= matched_qty

                if top_bid.size <= 1e-8:
                    i_bid += 1
                if top_ask.size <= 1e-8:
                    i_ask += 1
            else:
                break  # Order book is cleanly uncrossed

        clean_bids = [b for b in sorted_bids[i_bid:] if b.size > 1e-8]
        clean_asks = [a for a in sorted_asks[i_ask:] if a.size > 1e-8]

        return clean_bids, clean_asks


# Example Execution
if __name__ == "__main__":
    fees = {
        "binance": 0.0010,  # 0.10%
        "coinbase": 0.0020, # 0.20%
        "okx": 0.0008       # 0.08%
    }
    aggregator = OrderBookAggregator(tick_size=0.50, taker_fees=fees)

    # Sample Raw Inputs [price, size]
    sample_raw_bids = {
        "binance": [(50000.0, 1.5), (49999.0, 2.0)],
        "coinbase": [(50001.0, 0.5), (49998.0, 1.0)],
        "okx": [(50000.5, 3.0)]
    }
    sample_raw_asks = {
        "binance": [(50002.0, 1.0), (50003.0, 2.5)],
        "coinbase": [(50001.5, 0.8), (50004.0, 1.2)],
        "okx": [(50001.0, 2.0)]
    }

    clean_bids, clean_asks = aggregator.aggregate_and_uncross(sample_raw_bids, sample_raw_asks)

    print("=== UNCROSSED COMPOSITE BIDS ===")
    for b in clean_bids[:5]:
        print(f"Price: ${b.price:.2f} | Size: {b.size:.4f} | Breakdown: {b.venues}")

    print("\n=== UNCROSSED COMPOSITE ASKS ===")
    for a in clean_asks[:5]:
        print(f"Price: ${a.price:.2f} | Size: {a.size:.4f} | Breakdown: {a.venues}")
```

---

## 4. Operational Requirements & Key Edge Cases

### Edge Cases to Handle
1. **Exchange Disconnections:**
   - Implement a circuit breaker. If an exchange WS feed drops for $>500	ext{ms}$, immediately purge that exchange's liquidity pool from the aggregator to prevent routing orders to dead endpoints.
2. **Precision Loss & Floating Point Arithmetic:**
   - Round prices to $10^{-8}$ or use fixed-point decimal integer math (e.g., store prices as integer cents/satoshis) to prevent precision mismatch during bucket grouping.
3. **Latency-Induced Phantom Liquidity:**
   - Do not assume composite liquidity is $100\%$ fillable. Any routing algorithm downstream must treat the aggregated book as a probabilistic liquidity estimate rather than guaranteed execution depth.
