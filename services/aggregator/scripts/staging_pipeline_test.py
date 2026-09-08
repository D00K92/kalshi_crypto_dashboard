"""Exercise the isolated staging aggregator -> live-feature Redis path."""

from __future__ import annotations

import argparse
import asyncio
import math
import os
import time
from typing import Any, Callable

import orjson
import redis.asyncio as redis


def required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"missing required staging parameter {name}")
    return value


class StagingProbe:
    def __init__(self) -> None:
        self.run_prefix = required("STAGING_RUN_PREFIX")
        self.book_stream = required("STAGING_BOOK_STREAM")
        self.trade_stream = required("STAGING_TRADE_STREAM")
        self.primitive_stream = required("STAGING_PRIMITIVE_STREAM")
        self.feature_stream = required("STAGING_FEATURE_STREAM")
        self.feature_key = required("STAGING_FEATURE_KEY")
        self.feature_group = required("STAGING_FEATURE_GROUP")
        self.output_prefix = required("STAGING_OUTPUT_PREFIX")
        self.control_key = f"{self.run_prefix}:probe:control"
        self.client = redis.Redis.from_url(required("STAGING_REDIS_URL"), decode_responses=False)

    async def close(self) -> None:
        await self._expire_run_keys()
        await self.client.aclose()

    async def baseline(self) -> None:
        if await self.client.xlen(self.primitive_stream):
            raise AssertionError("staging primitive stream is not isolated and empty")
        base = (time.time_ns() // 1_000_000 // 10_000) * 10_000
        await self.client.set(self.control_key, str(base))
        for venue, bid, ask in (("binance", "99", "101"), ("coinbase", "109", "111")):
            await self._append(
                self.book_stream,
                self._book(venue, bid, ask, f"baseline-book-{venue}", base + 1_000),
            )
        for event in (
            self._trade("binance", "100", "baseline-binance", base),
            self._trade("coinbase", "110", "baseline-coinbase", base),
            self._trade("binance", "100", "boundary-binance", base + 15_000),
            self._trade("coinbase", "110", "boundary-coinbase", base + 15_000),
            # The first bucket is finalized before this deliberately corrupting
            # late value arrives. It must never reach a primitive or feature.
            self._trade("binance", "999999", "late-corrupting-trade", base + 1_000),
            self._trade("binance", "100", "advance-watermark", base + 25_000),
        ):
            await self._append(self.trade_stream, event)

        feature = await self._wait_json(
            self.feature_key,
            lambda payload: payload.get("event_timestamp_ms") == base,
        )
        rows = await self._stream_payloads(self.primitive_stream)
        self._assert_primitives(rows, [base, base, base + 10_000, base + 10_000])
        self._assert_feature(feature, timestamp_ms=base, synthetic_price=105.0, log_return=None, venue_count=2)
        if feature.get("feature_set") != "market_features" or feature.get("feature_version") != "v2_10s":
            raise AssertionError(f"unexpected live feature contract: {feature}")
        progress = orjson.loads(
            await self.client.get(f"{self.output_prefix}:primitive_watermark:BTCUSDT:10s")
        )
        if progress.get("finalized_through_ms") != base + 10_000:
            raise AssertionError(f"unexpected event-time checkpoint: {progress}")
        print("staging baseline passed: monotonic primitives and uncorrupted feature")

    async def continuation(self) -> None:
        base = await self._base()
        before = await self._stream_payloads(self.primitive_stream)
        await self._append(
            self.trade_stream,
            self._trade("binance", "100", "post-restart-watermark", base + 35_000),
        )
        feature = await self._wait_json(
            self.feature_key,
            lambda payload: payload.get("event_timestamp_ms") == base + 10_000,
        )
        rows = await self._wait_stream_length(self.primitive_stream, len(before) + 1)
        self._assert_primitives(
            rows,
            [base, base, base + 10_000, base + 10_000, base + 20_000],
        )
        self._assert_feature(
            feature, timestamp_ms=base + 10_000, synthetic_price=105.0,
            log_return=0.0, venue_count=2,
        )
        print("staging restart passed: checkpoint continued without duplicate output")

    async def stage_pending(self) -> None:
        base = await self._base()
        for timestamp, venue, price in (
            (base + 30_000, "binance", "100"),
            (base + 30_000, "coinbase", "110"),
            (base + 40_000, "binance", "100"),
        ):
            await self._append(
                self.primitive_stream,
                self._primitive(timestamp, venue, price),
            )
        claimed = await self.client.xreadgroup(
            self.feature_group,
            "staging-crashed-consumer",
            {self.primitive_stream: ">"},
            count=3,
            block=1_000,
        )
        count = sum(len(entries) for _, entries in claimed)
        if count != 3:
            raise AssertionError(f"expected three abandoned entries, got {count}")
        if await self._pending_count() < 3:
            raise AssertionError("staging entries did not enter the pending list")
        print("staging recovery arranged: three entries abandoned by a crashed consumer")

    async def verify_recovery(self) -> None:
        base = await self._base()
        deadline = time.monotonic() + 30
        feature: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            raw = await self.client.get(self.feature_key)
            feature = orjson.loads(raw) if raw else None
            if (
                await self._pending_count() == 0
                and feature
                and feature.get("event_timestamp_ms") == base + 30_000
            ):
                break
            await asyncio.sleep(0.25)
        if await self._pending_count() != 0:
            raise AssertionError("live-feature XAUTOCLAIM did not drain pending entries")
        if not feature or feature.get("event_timestamp_ms") != base + 30_000:
            raise AssertionError(f"recovered feature did not advance: {feature}")
        self._assert_feature(
            feature, timestamp_ms=base + 30_000, synthetic_price=105.0,
            log_return=math.log(105.0 / 100.0), venue_count=2,
        )
        features = await self._stream_payloads(self.feature_stream)
        timestamps = [row["event_timestamp_ms"] for row in features]
        if timestamps != sorted(set(timestamps)):
            raise AssertionError(f"feature output is duplicated or out of order: {timestamps}")
        print("staging recovery passed: pending entries reclaimed exactly once")

    async def _base(self) -> int:
        raw = await self.client.get(self.control_key)
        if raw is None:
            raise AssertionError("missing baseline staging control state")
        return int(raw)

    async def _append(self, stream: str, payload: dict[str, Any]) -> None:
        await self.client.xadd(stream, {"payload": orjson.dumps(payload)})

    async def _stream_payloads(self, stream: str) -> list[dict[str, Any]]:
        return [
            orjson.loads(fields.get(b"payload") or fields["payload"])
            for _, fields in await self.client.xrange(stream)
        ]

    async def _wait_stream_length(self, stream: str, count: int) -> list[dict[str, Any]]:
        deadline = time.monotonic() + 30
        rows: list[dict[str, Any]] = []
        while time.monotonic() < deadline:
            rows = await self._stream_payloads(stream)
            if len(rows) >= count:
                return rows
            await asyncio.sleep(0.25)
        raise AssertionError(f"{stream} did not reach {count} rows; got {len(rows)}")

    async def _wait_json(
        self,
        key: str,
        predicate: Callable[[dict[str, Any]], bool],
    ) -> dict[str, Any]:
        deadline = time.monotonic() + 30
        last: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            raw = await self.client.get(key)
            last = orjson.loads(raw) if raw else None
            if last and predicate(last):
                return last
            await asyncio.sleep(0.25)
        raise AssertionError(f"timed out waiting for {key}; last={last}")

    async def _pending_count(self) -> int:
        summary = await self.client.xpending(self.primitive_stream, self.feature_group)
        value = summary.get("pending", summary.get(b"pending", 0))
        return int(value)

    async def _expire_run_keys(self) -> None:
        async for key in self.client.scan_iter(match=f"{self.run_prefix}:*"):
            await self.client.expire(key, 3_600)

    @staticmethod
    def _assert_primitives(rows: list[dict[str, Any]], expected: list[int]) -> None:
        timestamps = [int(row["bucket_start_ts_ms"]) for row in rows]
        identities = [(row["venue"], int(row["bucket_start_ts_ms"])) for row in rows]
        if timestamps != expected:
            raise AssertionError(f"primitive timestamps are not monotonic: {timestamps}")
        if len(identities) != len(set(identities)):
            raise AssertionError(f"duplicate primitive identities: {identities}")
        if any(row.get("p_trade_mean") == "999999" for row in rows):
            raise AssertionError("late corrupting trade reached the primitive stream")

    @staticmethod
    def _assert_feature(
        feature: dict[str, Any], *, timestamp_ms: int, synthetic_price: float,
        log_return: float | None, venue_count: int,
    ) -> None:
        values = feature.get("values", {})
        actual = (
            feature.get("event_timestamp_ms"), values.get("synthetic_price"),
            values.get("log_return"), values.get("venue_count"),
        )
        expected = (timestamp_ms, synthetic_price, log_return, venue_count)
        if actual != expected:
            raise AssertionError(f"offline-equivalent feature mismatch: expected={expected} actual={actual}")

    @staticmethod
    def _trade(venue: str, price: str, event_id: str, timestamp: int) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "event_type": "trade",
            "event_id": event_id,
            "venue": venue,
            "instrument": "BTCUSDT",
            "trade_id": event_id,
            "price": price,
            "quantity": "1",
            "taker_side": "buy",
            "exchange_ts_ms": timestamp,
            "received_ts_ms": timestamp,
        }

    @staticmethod
    def _book(
        venue: str, bid: str, ask: str, event_id: str, timestamp: int
    ) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "event_type": "book_snapshot",
            "event_id": event_id,
            "venue": venue,
            "instrument": "BTCUSDT",
            "sequence": 1,
            "bids": [{"price": bid, "quantity": "2"}],
            "asks": [{"price": ask, "quantity": "3"}],
            "exchange_ts_ms": timestamp,
            "received_ts_ms": timestamp,
            "depth": 1,
        }

    @staticmethod
    def _primitive(timestamp: int, venue: str, price: str) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "primitive_schema_version": 2,
            "event_type": "primitive_bar",
            "instrument": "BTCUSDT",
            "venue": venue,
            "frequency": "10s",
            "interval_ms": 10_000,
            "bucket_start_ts_ms": timestamp,
            "bucket_end_ts_ms": timestamp + 10_000,
            "p_trade_mean": price,
        }


async def main(phase: str) -> None:
    probe = StagingProbe()
    try:
        await probe.client.ping()
        await getattr(probe, phase.replace("-", "_"))()
    finally:
        await probe.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--phase",
        required=True,
        choices=("baseline", "continuation", "stage-pending", "verify-recovery"),
    )
    asyncio.run(main(parser.parse_args().phase))
