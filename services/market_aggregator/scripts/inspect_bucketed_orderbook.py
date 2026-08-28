"""Inspect a bucketed order book computed from the latest live venue snapshots.

This is deliberately read-only: it does not consume or acknowledge Redis
stream entries and does not modify any keys.
"""

from __future__ import annotations

import argparse
import os
import time
from decimal import Decimal

import orjson
import redis

from market_aggregator.aggregation import MarketAggregator


def redis_url() -> str:
    return os.getenv(
        "MARKET_AGGREGATOR_REDIS_URL",
        os.getenv("REDIS_URL", "redis://localhost:6379/0"),
    )


def endpoint(url: str) -> str:
    return url.rsplit("@", 1)[-1]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--redis-url", default=redis_url())
    parser.add_argument("--stream", default="stream:orderbook_snapshots")
    parser.add_argument("--bucket-size", default="1.00", help="Price bucket size in quote currency")
    parser.add_argument("--depth", type=int, default=10)
    parser.add_argument("--freshness-ms", type=int, default=10_000)
    parser.add_argument("--scan-count", type=int, default=200, help="Recent stream entries to scan")
    args = parser.parse_args()

    client = redis.Redis.from_url(args.redis_url, decode_responses=False, socket_connect_timeout=5)
    aggregator = MarketAggregator(args.bucket_size, args.depth, args.freshness_ms)
    snapshots: dict[str, dict] = {}

    try:
        client.ping()
        for _, fields in client.xrevrange(args.stream, count=args.scan_count):
            raw = fields.get(b"payload")
            if not raw:
                continue
            event = orjson.loads(raw)
            if event.get("event_type") != "book_snapshot":
                continue
            venue = str(event.get("venue", "")).lower()
            if venue and venue not in snapshots:
                snapshots[venue] = event
                aggregator.apply_book(event)

        snapshot = aggregator.book_snapshot(
            time.time_ns() // 1_000_000,
            "BTCUSDT",
        )
        print(f"Redis: {endpoint(args.redis_url)}")
        print(f"Raw snapshots: {', '.join(sorted(snapshots)) or 'none'}")
        print(
            f"Bucket method: {snapshot['bucket_method']} | "
            f"size: {snapshot['price_tick']} | "
            f"active: {snapshot['venues']} | stale: {snapshot['stale_venues']}"
        )
        print("\nASKS (nearest spread first)")
        for level in snapshot["asks"]:
            contributions = " ".join(f"{venue}={qty}" for venue, qty in level["venues"].items())
            print(f"  {level['price']:>14} total={level['total_quantity']:>14}  {contributions}")
        print("\nBIDS (nearest spread first)")
        for level in snapshot["bids"]:
            contributions = " ".join(f"{venue}={qty}" for venue, qty in level["venues"].items())
            print(f"  {level['price']:>14} total={level['total_quantity']:>14}  {contributions}")

        if snapshot["bids"] and snapshot["asks"]:
            best_bid = Decimal(snapshot["bids"][0]["price"])
            best_ask = Decimal(snapshot["asks"][0]["price"])
            print(f"\nBest bid: {best_bid} | best ask: {best_ask}")
            print(f"Non-crossed: {best_bid < best_ask}")
        else:
            print("\nNon-crossed: not applicable (one side is empty)")
    finally:
        client.close()


if __name__ == "__main__":
    main()
