"""One-off backfill of Binance Spot BTCUSDT trades into the GCS dataset.

Binance's public archive contains exchange timestamps, but no local receive
timestamp or Redis stream ID.  This script therefore uses deterministic
archive IDs and sets ``received_ts_ms`` to the exchange timestamp.  Consumers
must use ``event_id`` when deduplicating live and historical records.
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import date, timedelta
import csv
import io
import logging
import urllib.error
import urllib.request
import zipfile

from gcs_exporter.gcs_uploader import GCSUploader
from gcs_exporter.models import TradeRow
from gcs_exporter.object_names import trade_object_name
from gcs_exporter.parquet_writer import write_trades


LOGGER = logging.getLogger("binance_backfill")
ARCHIVE_BASE = "https://data.binance.vision/data/spot/daily/trades"
CONTENT_TYPE = "application/vnd.apache.parquet"


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid date: {value!r}") from exc


def _timestamp_ms(raw: str) -> int:
    value = int(raw)
    # Spot archive timestamps switched from milliseconds to microseconds in 2025.
    return value // 1_000 if value >= 100_000_000_000_000 else value


def _iter_rows_from_csv(payload: bytes, symbol: str):
    """Yield canonical rows from one Binance archive CSV."""
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if len(csv_names) != 1:
            raise ValueError(f"expected one CSV in archive, found {csv_names!r}")
        text = io.TextIOWrapper(archive.open(csv_names[0]), encoding="utf-8", newline="")
        try:
            reader = csv.reader(text)
            for line_number, fields in enumerate(reader, start=1):
                if not fields or fields[0].lower() in {"id", "trade_id"}:
                    continue
                if len(fields) < 6:
                    raise ValueError(f"archive row {line_number} has {len(fields)} fields")
                trade_id, price, quantity, _quote_quantity, timestamp, is_buyer_maker = fields[:6]
                exchange_ts_ms = _timestamp_ms(timestamp)
                taker_side = "sell" if is_buyer_maker.strip().lower() == "true" else "buy"
                yield TradeRow(
                    redis_id=f"archive:binance:{symbol}:trade:{trade_id}",
                    event_id=f"binance:{symbol}:trade:{trade_id}",
                    venue="binance",
                    instrument=symbol,
                    trade_id=trade_id,
                    price=float(price),
                    quantity=float(quantity),
                    taker_side=taker_side,
                    exchange_ts_ms=exchange_ts_ms,
                    received_ts_ms=exchange_ts_ms,
                    schema_version=1,
                )
        finally:
            text.close()


def _rows_from_csv(payload: bytes, symbol: str) -> list[TradeRow]:
    """Convert one Binance archive CSV into canonical rows for tests."""
    return list(_iter_rows_from_csv(payload, symbol))


def _download(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "kalshi-crypto-backfill/1.0"})
    with urllib.request.urlopen(request, timeout=60) as response:
        return response.read()


def _archive_url(symbol: str, day: date) -> str:
    return f"{ARCHIVE_BASE}/{symbol}/{symbol}-trades-{day.isoformat()}.zip"


async def backfill(
    *,
    symbol: str,
    start_date: date,
    end_date: date,
    bucket: str,
    batch_rows: int,
    dry_run: bool,
) -> tuple[int, int, int]:
    if end_date < start_date:
        raise ValueError("end date must not precede start date")
    uploader = None if dry_run else GCSUploader(bucket)
    total_rows = total_objects = missing_days = 0
    day = start_date
    while day <= end_date:
        url = _archive_url(symbol, day)
        LOGGER.info("downloading", extra={"date": day.isoformat(), "url": url})
        try:
            payload = await asyncio.to_thread(_download, url)
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                missing_days += 1
                raise RuntimeError(f"Binance archive is missing for {day}: {url}") from exc
            raise

        day_rows = 0
        previous_ts = 0
        current_partition = None
        batch: list[TradeRow] = []

        async def flush() -> None:
            nonlocal total_objects, total_rows, batch
            if not batch:
                return
            object_name = trade_object_name(batch)
            if dry_run:
                LOGGER.debug("dry_run_object", extra={"object": object_name, "rows": len(batch)})
            else:
                assert uploader is not None
                await uploader.upload(object_name, write_trades(batch), CONTENT_TYPE)
            total_objects += 1
            total_rows += len(batch)
            batch = []

        for row in _iter_rows_from_csv(payload, symbol):
            if row.exchange_ts_ms < previous_ts:
                raise ValueError("Binance archive rows are not in chronological order")
            previous_ts = row.exchange_ts_ms
            day_rows += 1
            if current_partition is None:
                current_partition = row.partition
            elif row.partition != current_partition:
                await flush()
                current_partition = row.partition
            batch.append(row)
            if len(batch) >= batch_rows:
                await flush()
        await flush()
        LOGGER.info("day_complete", extra={"date": day.isoformat(), "rows": day_rows})
        day += timedelta(days=1)
    return total_rows, total_objects, missing_days


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", type=_parse_date, required=True, help="first UTC date, inclusive")
    parser.add_argument("--end-date", type=_parse_date, required=True, help="last UTC date, inclusive")
    parser.add_argument("--symbol", default="BTCUSDT", help="Binance Spot symbol")
    parser.add_argument("--bucket", default="kalshi-crypto-tick-data")
    parser.add_argument("--batch-rows", type=int, default=10_000)
    parser.add_argument("--dry-run", action="store_true", help="download and validate without uploading")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    if args.batch_rows <= 0:
        parser.error("--batch-rows must be positive")
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    rows, objects, _missing = asyncio.run(
        backfill(
            symbol=args.symbol,
            start_date=args.start_date,
            end_date=args.end_date,
            bucket=args.bucket,
            batch_rows=args.batch_rows,
            dry_run=args.dry_run,
        )
    )
    LOGGER.info("backfill_complete", extra={"rows": rows, "objects": objects})


if __name__ == "__main__":
    main()
