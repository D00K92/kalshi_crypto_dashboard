"""Record raw production book updates and isolated local aggregate output."""

from __future__ import annotations

import argparse
import asyncio
import os
import time

import orjson
import redis.asyncio as redis


async def main(seconds: int, prefix: str) -> None:
    url = os.getenv("MARKET_AGGREGATOR_REDIS_URL", "redis://127.0.0.1:6380/0")
    client = redis.Redis.from_url(url, decode_responses=False, socket_connect_timeout=5, socket_timeout=5)
    await client.ping()
    deadline = time.monotonic() + seconds
    seen_raw: set[tuple[str, bytes]] = set()
    latest_raw: dict[str, bytes] = {}
    seen_agg: set[bytes] = set()
    raw_count = 0
    aggregate_count = 0
    try:
        print(f"Recording {seconds}s from {url.rsplit('@', 1)[-1]} with output prefix {prefix}")
        while time.monotonic() < deadline:
            printed_venues: set[str] = set()
            for redis_id, fields in await client.xrevrange("stream:orderbook_snapshots", count=200):
                payload = fields.get(b"payload")
                if not payload:
                    continue
                event = orjson.loads(payload)
                venue = str(event.get("venue", ""))
                key = (venue, redis_id)
                if venue and key not in seen_raw:
                    seen_raw.add(key)
                    raw_count += 1
                    bids, asks = event.get("bids", []), event.get("asks", [])
                    if venue not in printed_venues and latest_raw.get(venue) != redis_id:
                        latest_raw[venue] = redis_id
                        printed_venues.add(venue)
                        print(f"RAW {venue:10} id={redis_id.decode()} bid={bids[0]['price'] if bids else '-'} ask={asks[0]['price'] if asks else '-'}")

            payload = await client.get(f"{prefix}:book:BTCUSDT:latest")
            if payload and payload not in seen_agg:
                seen_agg.add(payload)
                aggregate_count += 1
                book = orjson.loads(payload)
                bids, asks = book.get("bids", []), book.get("asks", [])
                best_bid = bids[0]["price"] if bids else "-"
                best_ask = asks[0]["price"] if asks else "-"
                print(f"AGG id={book.get('generated_ts_ms')} bid={best_bid} ask={best_ask} bids={len(bids)} asks={len(asks)} venues={book.get('venues')} stale={book.get('stale_venues')}")
            await asyncio.sleep(0.25)
        print(f"SUMMARY raw_book_updates={raw_count} aggregate_states={aggregate_count}")
    finally:
        await client.aclose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=int, default=5)
    parser.add_argument("--prefix", default="local-live")
    args = parser.parse_args()
    asyncio.run(main(args.seconds, args.prefix))
