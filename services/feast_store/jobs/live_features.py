"""Bridge live aggregator features into the Feast Redis online store.

The market aggregator remains a lightweight Redis-stream producer. This
process owns the Feast SDK call and therefore can be deployed independently
without adding Feast's dependencies to the ingestion path.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from datetime import datetime, timezone

import pandas as pd
import redis.asyncio as redis
from redis.exceptions import ResponseError
from feast import FeatureStore
from feast.data_source import PushMode

from registry import resolve_feature_spec

LOGGER = logging.getLogger(__name__)


def _decode(value):
    return value.decode() if isinstance(value, bytes) else value


def payload_to_frame(payload: dict, *, spec=None) -> pd.DataFrame:
    """Convert a versioned feature envelope into a Feast push dataframe."""
    spec = spec or resolve_feature_spec(
        payload.get("feature_set", "market_features"),
        payload.get("feature_version", "v1"),
    )
    values = payload.get("values", payload)
    if not isinstance(values, dict):
        raise ValueError("feature payload values must be an object")
    spec.validate(values)
    event_timestamp = pd.to_datetime(payload["event_timestamp"], utc=True)
    created = pd.to_datetime(payload.get("created_timestamp", datetime.now(timezone.utc)), utc=True)
    row = {
        "asset": str(payload.get("asset", payload.get("entity", {}).get("asset"))),
        "event_timestamp": event_timestamp,
        "created_timestamp": created,
    }
    for name in spec.fields:
        value = values.get(name)
        row[name] = int(value) if name == "venue_count" else (None if value is None else float(value))
    return pd.DataFrame([row])


async def run_bridge(
    *, repo_path: str, redis_url: str, stream: str, group: str, consumer: str,
    batch_size: int = 100, block_ms: int = 1_000, pending_idle_ms: int = 120_000,
) -> None:
    """Consume live features and write them to Feast until cancelled."""
    client = redis.Redis.from_url(redis_url, decode_responses=False)
    store = FeatureStore(repo_path=repo_path)
    try:
        try:
            await client.xgroup_create(stream, group, id="$", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        while True:
            # Recover entries abandoned by a terminated pod.  The live stream
            # is deliberately bounded, so only recent pending entries matter.
            reclaimed = await client.xautoclaim(
                stream, group, consumer, min_idle_time=pending_idle_ms,
                start_id="0-0", count=batch_size,
            )
            if len(reclaimed) > 1 and reclaimed[1]:
                await _process_entries(client, store, stream, group, reclaimed[1])
            rows = await client.xreadgroup(
                group, consumer, {stream: ">"}, count=batch_size, block=block_ms
            )
            for _, entries in rows:
                await _process_entries(client, store, stream, group, entries)
    finally:
        await client.aclose()


async def _process_entries(client, store, stream: str, group: str, entries) -> None:
    """Push entries and acknowledge only success or permanent validation errors."""
    for entry_id, fields in entries:
        try:
            raw = fields.get(b"payload") or fields.get("payload")
            if raw is None:
                raise ValueError("missing payload")
            payload = json.loads(raw)
            spec = resolve_feature_spec(
                payload.get("feature_set", "market_features"),
                payload.get("feature_version", "v1"),
            )
            frame = payload_to_frame(payload, spec=spec)
            await asyncio.to_thread(
                store.push,
                push_source_name=spec.push_source,
                df=frame,
                allow_registry_cache=False,
                to=PushMode.ONLINE,
            )
            await client.xack(stream, group, entry_id)
        except (ValueError, TypeError, KeyError, OverflowError) as exc:
            LOGGER.warning("live_feature_rejected entry_id=%s error=%s", entry_id, exc)
            await client.xack(stream, group, entry_id)
        except Exception:
            LOGGER.exception("live_feature_write_failed entry_id=%s", entry_id)
            # Leave the entry pending for retry after a transient Feast/Redis
            # failure; XAUTOCLAIM recovers it after pod restarts.


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-path", default=os.getenv("FEAST_REPO_PATH", "/app/feast_store"))
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0"))
    parser.add_argument("--stream", default=os.getenv("FEATURE_STREAM", "stream:features:v1"))
    parser.add_argument("--group", default=os.getenv("FEAST_LIVE_GROUP", "feast-live-features"))
    parser.add_argument("--consumer", default=os.getenv("HOSTNAME", "feast-live-1"))
    parser.add_argument("--pending-idle-ms", type=int, default=int(os.getenv("FEAST_PENDING_IDLE_MS", "120000")))
    args = parser.parse_args()
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    asyncio.run(run_bridge(**vars(args)))


if __name__ == "__main__":
    main()
