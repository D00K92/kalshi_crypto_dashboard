"""Fail when recent Redis v2_10s features disagree with BigQuery offline features."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import re
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import redis.asyncio as redis
from feast import FeatureStore
from google.cloud import bigquery


DEFAULT_TABLE = "kalshi-crypto-506614.feature_store.realized_volatility_v2_10s"
TABLE_PATTERN = re.compile(r"^[A-Za-z0-9_-]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+$")


@dataclass(frozen=True)
class FeaturePoint:
    timestamp_ms: int
    synthetic_price: float
    log_return: float | None
    venue_count: int


@dataclass(frozen=True)
class ParityReport:
    status: str
    checked: int
    missing_online: int
    mismatched: int
    feast_mismatched: int
    latest_online_age_seconds: float
    latest_offline_age_seconds: float
    errors: tuple[dict[str, Any], ...]


def point_from_payload(payload: dict[str, Any]) -> FeaturePoint:
    if payload.get("feature_set") != "market_features" or payload.get("feature_version") != "v2_10s":
        raise ValueError("online payload is not market_features/v2_10s")
    values = payload.get("values")
    if not isinstance(values, dict):
        raise ValueError("online payload values must be an object")
    timestamp_ms = payload.get("event_timestamp_ms")
    if timestamp_ms is None:
        timestamp_ms = int(datetime.fromisoformat(payload["event_timestamp"]).timestamp() * 1_000)
    return FeaturePoint(
        timestamp_ms=int(timestamp_ms),
        synthetic_price=float(values["synthetic_price"]),
        log_return=None if values.get("log_return") is None else float(values["log_return"]),
        venue_count=int(values["venue_count"]),
    )


def compare_points(
    offline: list[FeaturePoint],
    online: dict[int, FeaturePoint],
    *,
    relative_tolerance: float,
    absolute_tolerance: float,
) -> tuple[int, int, list[dict[str, Any]]]:
    missing = 0
    mismatched = 0
    errors: list[dict[str, Any]] = []
    for expected in offline:
        actual = online.get(expected.timestamp_ms)
        if actual is None:
            missing += 1
            errors.append({"timestamp_ms": expected.timestamp_ms, "error": "missing_online"})
            continue
        differences: dict[str, Any] = {}
        if expected.venue_count != actual.venue_count:
            differences["venue_count"] = {"offline": expected.venue_count, "online": actual.venue_count}
        for field in ("synthetic_price", "log_return"):
            expected_value = getattr(expected, field)
            actual_value = getattr(actual, field)
            equal = (
                expected_value is None and actual_value is None
            ) or (
                expected_value is not None
                and actual_value is not None
                and math.isclose(
                    expected_value,
                    actual_value,
                    rel_tol=relative_tolerance,
                    abs_tol=absolute_tolerance,
                )
            )
            if not equal:
                differences[field] = {"offline": expected_value, "online": actual_value}
        if differences:
            mismatched += 1
            errors.append({"timestamp_ms": expected.timestamp_ms, "error": "value_mismatch", "fields": differences})
    return missing, mismatched, errors


def load_offline_points(
    *, client: bigquery.Client, table: str, cutoff: datetime, sample_size: int
) -> list[FeaturePoint]:
    if not TABLE_PATTERN.fullmatch(table):
        raise ValueError(f"invalid BigQuery table identifier: {table!r}")
    query = f"""
      SELECT event_timestamp, synthetic_price, log_return, venue_count
      FROM `{table}`
      WHERE asset = 'BTCUSD'
        AND feature_version = 'v2_10s'
        AND event_timestamp >= @cutoff
        AND synthetic_price IS NOT NULL
        AND venue_count > 0
      ORDER BY event_timestamp DESC
      LIMIT @sample_size
    """
    config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("cutoff", "TIMESTAMP", cutoff),
        bigquery.ScalarQueryParameter("sample_size", "INT64", sample_size),
    ])
    return [
        FeaturePoint(
            timestamp_ms=int(row.event_timestamp.timestamp() * 1_000),
            synthetic_price=float(row.synthetic_price),
            log_return=None if row.log_return is None else float(row.log_return),
            venue_count=int(row.venue_count),
        )
        for row in client.query(query, job_config=config).result()
    ]


def load_feast_online_point(*, repo_path: str) -> FeaturePoint:
    response = FeatureStore(repo_path=repo_path).get_online_features(
        features=[
            "v2_10s_market_features:synthetic_price",
            "v2_10s_market_features:log_return",
            "v2_10s_market_features:venue_count",
        ],
        entity_rows=[{"asset": "BTCUSD"}],
        full_feature_names=False,
    ).to_dict()

    def first(name: str) -> Any:
        values = response.get(name)
        if not values:
            raise RuntimeError(f"Feast online response missing {name}")
        return values[0]

    log_return = first("log_return")
    return FeaturePoint(
        # Feast's online API does not return the feature event timestamp. The
        # caller matches these values to a recent immutable stream point.
        timestamp_ms=0,
        synthetic_price=float(first("synthetic_price")),
        log_return=None if log_return is None else float(log_return),
        venue_count=int(first("venue_count")),
    )


def compare_feast_online(
    feast: FeaturePoint,
    online: dict[int, FeaturePoint],
    *,
    max_lag_ms: int,
    relative_tolerance: float,
    absolute_tolerance: float,
) -> tuple[int, list[dict[str, Any]]]:
    if not online:
        return 1, [{"error": "feast_online_mismatch", "detail": "online stream is empty"}]
    matching_timestamps = []
    for timestamp, point in online.items():
        candidate = FeaturePoint(timestamp, feast.synthetic_price, feast.log_return, feast.venue_count)
        _, mismatched, _ = compare_points(
            [point],
            {timestamp: candidate},
            relative_tolerance=relative_tolerance,
            absolute_tolerance=absolute_tolerance,
        )
        if not mismatched:
            matching_timestamps.append(timestamp)
    latest_timestamp = max(online)
    if matching_timestamps:
        matched_timestamp = max(matching_timestamps)
        lag_ms = latest_timestamp - matched_timestamp
        if lag_ms <= max_lag_ms:
            return 0, []
        return 1, [{
            "error": "feast_online_mismatch",
            "detail": "stale_feast_online",
            "lag_ms": lag_ms,
            "matched_timestamp_ms": matched_timestamp,
            "latest_timestamp_ms": latest_timestamp,
        }]

    latest = online[latest_timestamp]
    feast_at_latest = FeaturePoint(
        latest_timestamp, feast.synthetic_price, feast.log_return, feast.venue_count
    )
    _, _, errors = compare_points(
        [latest],
        {latest_timestamp: feast_at_latest},
        relative_tolerance=relative_tolerance,
        absolute_tolerance=absolute_tolerance,
    )
    for error in errors:
        error["error"] = "feast_online_mismatch"
    return 1, errors


async def load_online_points(client: Any, *, stream: str, search_count: int) -> dict[int, FeaturePoint]:
    points: dict[int, FeaturePoint] = {}
    for _, fields in await client.xrevrange(stream, max="+", min="-", count=search_count):
        raw = fields.get(b"payload") or fields.get("payload")
        if raw is None:
            continue
        try:
            point = point_from_payload(json.loads(raw))
        except (ValueError, TypeError, KeyError, json.JSONDecodeError):
            continue
        points.setdefault(point.timestamp_ms, point)
    return points


async def run_check(args: argparse.Namespace) -> ParityReport:
    now = datetime.now(timezone.utc)
    offline = await asyncio.to_thread(
        load_offline_points,
        client=bigquery.Client(project=args.project, location=args.location),
        table=args.offline_table,
        cutoff=now - timedelta(hours=args.lookback_hours),
        sample_size=args.sample_size,
    )
    feast_online = await asyncio.to_thread(
        load_feast_online_point,
        repo_path=args.repo_path,
    )
    redis_client = redis.Redis.from_url(args.redis_url, decode_responses=False)
    try:
        latest_raw = await redis_client.get(args.feature_key)
        if not latest_raw:
            raise RuntimeError(f"missing online feature key {args.feature_key}")
        latest_online = point_from_payload(json.loads(latest_raw))
        online = await load_online_points(redis_client, stream=args.feature_stream, search_count=args.search_count)
    finally:
        await redis_client.aclose()

    if len(offline) < args.minimum_matches:
        raise RuntimeError(f"only {len(offline)} eligible offline rows; need {args.minimum_matches}")
    latest_offline_ms = max(point.timestamp_ms for point in offline)
    latest_online_age = max(0.0, time.time() - latest_online.timestamp_ms / 1_000)
    latest_offline_age = max(0.0, time.time() - latest_offline_ms / 1_000)
    missing, mismatched, errors = compare_points(
        offline,
        online,
        relative_tolerance=args.relative_tolerance,
        absolute_tolerance=args.absolute_tolerance,
    )
    feast_mismatched, feast_errors = compare_feast_online(
        feast_online,
        online,
        max_lag_ms=args.max_feast_lag_seconds * 1_000,
        relative_tolerance=args.relative_tolerance,
        absolute_tolerance=args.absolute_tolerance,
    )
    errors.extend(feast_errors)
    if latest_online_age > args.max_online_age_seconds:
        errors.append({"error": "stale_online", "age_seconds": latest_online_age})
    if latest_offline_age > args.max_offline_age_seconds:
        errors.append({"error": "stale_offline", "age_seconds": latest_offline_age})
    if len(offline) - missing < args.minimum_matches:
        errors.append({"error": "insufficient_matches", "matched": len(offline) - missing})
    status = "pass" if not errors else "fail"
    return ParityReport(
        status=status,
        checked=len(offline),
        missing_online=missing,
        mismatched=mismatched,
        feast_mismatched=feast_mismatched,
        latest_online_age_seconds=round(latest_online_age, 3),
        latest_offline_age_seconds=round(latest_offline_age, 3),
        errors=tuple(errors[:20]),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default=os.getenv("GCP_PROJECT_ID", "kalshi-crypto-506614"))
    parser.add_argument("--location", default=os.getenv("GCP_REGION", "asia-northeast3"))
    parser.add_argument("--offline-table", default=os.getenv("PARITY_OFFLINE_TABLE", DEFAULT_TABLE))
    parser.add_argument("--repo-path", default=os.getenv("FEAST_REPO_PATH", "/app/feast_store"))
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0"))
    parser.add_argument("--feature-stream", default=os.getenv("FEATURE_STREAM", "stream:features:v2_10s"))
    parser.add_argument("--feature-key", default=os.getenv("FEATURE_KEY", "market:features:v2_10s:BTCUSD:latest"))
    parser.add_argument("--lookback-hours", type=int, default=6)
    parser.add_argument("--sample-size", type=int, default=30)
    parser.add_argument("--minimum-matches", type=int, default=12)
    parser.add_argument("--search-count", type=int, default=5_000)
    parser.add_argument("--max-online-age-seconds", type=int, default=180)
    parser.add_argument("--max-offline-age-seconds", type=int, default=10_800)
    parser.add_argument("--max-feast-lag-seconds", type=int, default=60)
    parser.add_argument("--relative-tolerance", type=float, default=1e-9)
    parser.add_argument("--absolute-tolerance", type=float, default=1e-10)
    args = parser.parse_args()
    if args.sample_size < args.minimum_matches or args.minimum_matches < 1:
        parser.error("sample size must be at least the positive minimum match count")
    if args.lookback_hours < 1 or args.search_count < args.sample_size:
        parser.error("lookback must be positive and search count must cover the sample")
    return args


def main() -> None:
    try:
        report = asyncio.run(run_check(parse_args()))
    except Exception as exc:
        print(json.dumps({"status": "error", "error": type(exc).__name__, "message": str(exc)}, sort_keys=True))
        raise SystemExit(2) from exc
    print(json.dumps(asdict(report), sort_keys=True))
    if report.status != "pass":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
