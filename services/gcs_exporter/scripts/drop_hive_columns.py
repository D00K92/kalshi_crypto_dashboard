"""Remove Hive partition columns from historical crypto Parquet objects.

This is a one-off migration for ``ticks/`` and ``books/``.  It intentionally
does not inspect or modify the separate ``kalshi/`` dataset.  Use ``--apply``
to replace objects; without it the script only reports the planned work.
"""

from __future__ import annotations

import argparse
import io
import logging
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date

import pyarrow.parquet as pq
from google.cloud import storage


LOGGER = logging.getLogger(__name__)
BUCKET_NAME = "kalshi-crypto-tick-data"
DATASETS = ("ticks", "books")
HIVE_DATE_RE = re.compile(r"(?:^|/)date=(\d{4}-\d{2}-\d{2})(?:/|$)")
LAST_DATE = date(2026, 8, 31)


@dataclass(frozen=True)
class RewriteResult:
    dataset: str
    name: str
    rows: int
    old_bytes: int
    new_bytes: int


def _object_date(name: str) -> date | None:
    match = HIVE_DATE_RE.search(name)
    return date.fromisoformat(match.group(1)) if match else None


def _is_in_scope(name: str, dataset: str, target_date: date | None = None) -> bool:
    object_date = _object_date(name)
    return (
        name.startswith(f"{dataset}/")
        and name.endswith(".parquet")
        and object_date is not None
        and (object_date == target_date if target_date is not None else object_date <= LAST_DATE)
    )


def _rewrite(data: bytes, name: str) -> tuple[RewriteResult, bytes] | None:
    table = pq.read_table(io.BytesIO(data))
    required = {"venue", "instrument"}
    missing = required.difference(table.column_names)
    if missing == required:
        return None
    if missing:
        raise ValueError(f"{name}: missing expected columns: {sorted(missing)}")

    rewritten = table.drop(sorted(required))
    output = io.BytesIO()
    pq.write_table(
        rewritten,
        output,
        compression="snappy",
        use_dictionary=["taker_side"] if name.startswith("ticks/") else False,
        write_statistics=True,
    )
    new_data = output.getvalue()

    check = pq.read_table(io.BytesIO(new_data))
    if check.num_rows != table.num_rows:
        raise ValueError(
            f"{name}: row count changed from {table.num_rows} to {check.num_rows}"
        )
    if required.intersection(check.column_names):
        raise ValueError(f"{name}: Hive columns remain after rewrite")

    dataset = name.split("/", 1)[0]
    return RewriteResult(dataset, name, table.num_rows, len(data), len(new_data)), new_data


_THREAD_STATE = threading.local()


def _process_blob(
    blob: storage.Blob, *, apply: bool, bucket_name: str
) -> RewriteResult | None:
    bucket = getattr(_THREAD_STATE, "bucket", None)
    if bucket is None:
        _THREAD_STATE.bucket = bucket = storage.Client().bucket(bucket_name)
    source = bucket.blob(blob.name, generation=blob.generation)
    source.content_type = blob.content_type
    original = source.download_as_bytes()
    rewritten = _rewrite(original, blob.name)
    if rewritten is None:
        return None
    result, new_data = rewritten
    if apply:
        bucket.blob(result.name).upload_from_string(
            new_data,
            content_type=source.content_type or "application/vnd.apache.parquet",
            if_generation_match=source.generation,
        )
    return result


def migrate(
    *,
    bucket_name: str,
    apply: bool,
    target_date: date | None = None,
    limit: int | None = None,
    workers: int = 16,
) -> tuple[int, int]:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    candidates = []

    for dataset in DATASETS:
        candidates.extend(
            blob
            for blob in client.list_blobs(bucket, prefix=f"{dataset}/")
            if _is_in_scope(blob.name, dataset, target_date)
        )
    if limit is not None:
        candidates = candidates[:limit]

    LOGGER.info("selected_objects=%d workers=%d apply=%s", len(candidates), workers, apply)
    processed = rewritten = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_process_blob, blob, apply=apply, bucket_name=bucket_name): blob
            for blob in candidates
        }
        for future in as_completed(futures):
            result = future.result()
            processed += 1
            if result is None:
                LOGGER.info("already_clean %s completed=%d/%d", futures[future].name, processed, len(candidates))
                continue
            rewritten += int(apply)
            LOGGER.info(
                "%s %s rows=%d bytes=%d->%d completed=%d/%d",
                "rewriting" if apply else "would_rewrite",
                result.name,
                result.rows,
                result.old_bytes,
                result.new_bytes,
                processed,
                len(candidates),
            )

    return len(candidates), rewritten


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", default=BUCKET_NAME)
    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        help="rewrite only this Hive date partition instead of the legacy through-date scope",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="replace in-scope objects after validating rewritten Parquet",
    )
    parser.add_argument("--limit", type=int, help="process at most N objects")
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be positive")
    if args.workers <= 0:
        parser.error("--workers must be positive")

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    planned, rewritten = migrate(
        bucket_name=args.bucket,
        apply=args.apply,
        target_date=args.date,
        limit=args.limit,
        workers=args.workers,
    )
    LOGGER.info(
        "migration_complete target_date=%s planned=%d rewritten=%d",
        args.date.isoformat() if args.date else "through-2026-08-31",
        planned,
        rewritten,
    )


if __name__ == "__main__":
    main()
