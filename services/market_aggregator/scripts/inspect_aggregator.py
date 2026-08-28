"""Read-only inspection tool for market-aggregator Redis outputs."""

from __future__ import annotations

import argparse
import asyncio
import os
import time

import orjson
import redis.asyncio as redis


def redis_url() -> str:
    explicit = os.getenv("MARKET_AGGREGATOR_REDIS_URL") or os.getenv("INGESTION_REDIS_URL")
    if explicit:
        return explicit
    host = os.getenv("REDIS_HOST", "localhost")
    port = os.getenv("REDIS_PORT", "6379")
    return f"redis://{host}:{port}/0"


def age_ms(payload: dict) -> int | None:
    generated = payload.get("generated_ts_ms")
    return int(time.time() * 1000 - generated) if generated else None


def print_book(book: dict) -> None:
    print(f"\nORDER BOOK {book.get('instrument')} depth={book.get('depth')} age_ms={age_ms(book)}")
    print("  ASKS")
    for level in reversed(book.get("asks", [])):
        venues = " ".join(f"{name}={qty}" for name, qty in level.get("venues", {}).items())
        print(f"    {level.get('price'):>14} total={level.get('total_quantity'):>12}  {venues}")
    print("  ---------------- spread ----------------")
    print("  BIDS")
    for level in book.get("bids", []):
        venues = " ".join(f"{name}={qty}" for name, qty in level.get("venues", {}).items())
        print(f"    {level.get('price'):>14} total={level.get('total_quantity'):>12}  {venues}")
    print(f"  active={book.get('venues', [])} stale={book.get('stale_venues', [])}")


def print_spot(spot: dict) -> None:
    print(f"\nSPOT {spot.get('instrument')} price={spot.get('price')} method={spot.get('method')} age_ms={age_ms(spot)}")
    print(f"  bucket={spot.get('bucket_start_ts_ms')}..{spot.get('bucket_end_ts_ms')} volume={spot.get('total_volume')}")
    for venue, data in spot.get("venues", {}).items():
        print(f"  {venue}: vwap={data.get('vwap')} volume={data.get('volume')} received={data.get('last_received_ts_ms')}")
    print(f"  used={spot.get('used_venues')} stale={spot.get('stale_venues', [])}")


def print_series(label: str, raw: bytes | None) -> None:
    if not raw:
        print(f"\n{label}: MISSING")
        return
    rows = orjson.loads(raw)
    print(f"\n{label}: {len(rows)} rows")
    for row in rows[-3:]:
        print(f"  {row}")


async def inspect(seconds: int) -> None:
    client = redis.Redis.from_url(redis_url(), decode_responses=False, socket_connect_timeout=5, socket_timeout=5)
    try:
        await client.ping()
        print(f"Redis connected: {redis_url().split('@')[-1]}")
        for key, printer in (("market:book:BTCUSDT:latest", print_book), ("market:spot:BTCUSDT:latest", print_spot)):
            raw = await client.get(key)
            if raw:
                printer(orjson.loads(raw))
            else:
                print(f"\n{key}: MISSING")
        print_series("CANDLES", await client.get("market:candles:BTCUSDT:5s"))
        print_series("CVD", await client.get("market:cvd:BTCUSDT:5s"))

        if seconds <= 0:
            return
        pubsub = client.pubsub()
        await pubsub.subscribe("pub:aggregated_orderbook", "pub:aggregated_spot", "pub:aggregated_candles", "pub:aggregated_cvd")
        print(f"\nWatching aggregated updates for {seconds}s...")
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if not message:
                continue
            channel = message["channel"].decode() if isinstance(message["channel"], bytes) else message["channel"]
            payload = orjson.loads(message["data"])
            if channel == "pub:aggregated_orderbook":
                print(f"update orderbook age_ms={age_ms(payload)} venues={payload.get('venues')} stale={payload.get('stale_venues')}")
            elif channel == "pub:aggregated_spot":
                print(f"update spot price={payload.get('price')} volume={payload.get('total_volume')} used={payload.get('used_venues')}")
            else:
                print(f"update {channel} rows={len(payload)}")
        await pubsub.close()
    finally:
        await client.aclose()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--watch", type=int, default=0, metavar="SECONDS", help="watch live aggregated Pub/Sub updates")
    args = parser.parse_args()
    asyncio.run(inspect(args.watch))


if __name__ == "__main__":
    main()
