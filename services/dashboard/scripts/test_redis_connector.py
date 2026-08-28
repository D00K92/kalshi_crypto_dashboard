"""Smoke-test the dashboard connector against a live aggregator Redis."""

from __future__ import annotations

import json

from dashboard.data import redis_client_from_env, RedisReader


def main() -> None:
    client = redis_client_from_env()
    client.ping()
    data = RedisReader(client).read()
    assert isinstance(data.book, dict)
    assert isinstance(data.spot, dict)
    assert isinstance(data.candles, list)
    assert isinstance(data.cvd, list)
    print(json.dumps({
        "redis": "ok",
        "book_levels": {"bids": len(data.book.get("bids", [])), "asks": len(data.book.get("asks", []))},
        "spot_price": data.spot.get("price"),
        "candle_points": len(data.candles),
        "cvd_points": len(data.cvd),
    }))


if __name__ == "__main__":
    main()
