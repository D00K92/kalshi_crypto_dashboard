"""Seed deterministic events and assert aggregator output in GKE."""

from __future__ import annotations

import argparse
import asyncio
import os
import time

import orjson
import redis.asyncio as redis


def event_trade(venue: str, price: str, quantity: str, side: str, event_id: str, ts: int) -> dict:
    return {"schema_version": 1, "event_type": "trade", "event_id": event_id, "venue": venue, "instrument": "BTCUSDT", "trade_id": event_id, "price": price, "quantity": quantity, "taker_side": side, "exchange_ts_ms": ts, "received_ts_ms": ts}


def event_book(venue: str, bid: str, ask: str, event_id: str, ts: int) -> dict:
    return {"schema_version": 1, "event_type": "book_snapshot", "event_id": event_id, "venue": venue, "instrument": "BTCUSDT", "sequence": 1, "bids": [{"price": bid, "quantity": "2"}], "asks": [{"price": ask, "quantity": "3"}], "exchange_ts_ms": ts, "received_ts_ms": ts, "depth": 1}


async def main(wait_seconds: int) -> None:
    url = os.getenv("MARKET_AGGREGATOR_REDIS_URL") or f"redis://{os.getenv('REDIS_HOST', 'localhost')}:{os.getenv('REDIS_PORT', '6379')}/0"
    prefix = os.getenv("AGGREGATOR_OUTPUT_PREFIX", "ci-market")
    book_stream = os.getenv("BOOK_STREAM", "ci:orderbooks")
    trade_stream = os.getenv("TRADE_STREAM", "ci:trades")
    client = redis.Redis.from_url(url, decode_responses=False)
    await client.ping()
    now = int(time.time() * 1000)
    books = [event_book("binance", "100", "101", "book-binance", now), event_book("coinbase", "100", "101", "book-coinbase", now), event_book("bybit", "100", "101", "book-bybit", now)]
    trades = [event_trade("binance", "100", "1", "buy", "trade-binance", now), event_trade("coinbase", "110", "3", "sell", "trade-coinbase", now)]
    for stream, events in ((book_stream, books), (trade_stream, trades)):
        for event in events:
            await client.xadd(stream, {"event_id": event["event_id"], "event_type": event["event_type"], "payload": orjson.dumps(event)})
    deadline = time.monotonic() + wait_seconds
    book = spot = None
    while time.monotonic() < deadline:
        book_raw = await client.get(f"{prefix}:book:BTCUSDT:latest")
        spot_raw = await client.get(f"{prefix}:spot:BTCUSDT:latest")
        if book_raw and spot_raw:
            book, spot = orjson.loads(book_raw), orjson.loads(spot_raw)
            break
        await asyncio.sleep(0.25)
    if not book or not spot:
        raise AssertionError("aggregator did not publish book and spot state")
    assert spot["price"] == "107.5", spot
    assert spot["total_volume"] == "4", spot
    assert book["bids"][0]["venues"] == {"binance": "2", "bybit": "2", "coinbase": "2"}, book
    assert len(book["bids"]) <= 10 and len(book["asks"]) <= 10
    assert await client.exists(f"{prefix}:candles:BTCUSDT:5s")
    assert await client.exists(f"{prefix}:cvd:BTCUSDT:5s")
    print("market-aggregator integration test passed")
    await client.aclose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--wait-seconds", type=int, default=20)
    args = parser.parse_args()
    asyncio.run(main(args.wait_seconds))
