"""Order-book Redis stream exporter, sharing the trade exporter lifecycle."""

from __future__ import annotations

from collections import defaultdict
import time

from gcs_exporter.models import OrderBookRow, RawStreamEntry
from gcs_exporter.object_names import book_object_name
from gcs_exporter.parquet_writer import write_books
from gcs_exporter.service import GCSExporterService


class OrderBookExporterService(GCSExporterService):
    async def _ingest(self, entries: list[RawStreamEntry]) -> None:
        for entry in entries:
            try:
                row = OrderBookRow.from_entry(entry)
            except (UnicodeDecodeError, ValueError) as exc:
                await self._archive_malformed(entry, str(exc))
                continue
            if not self._buffer:
                self._buffer_started_at = time.monotonic()
            self._buffer.append(row)  # type: ignore[arg-type]
            if len(self._buffer) >= self._settings.flush_size:
                await self.flush()

    async def flush(self) -> None:
        if not self._buffer:
            return
        groups = defaultdict(list)
        for row in self._buffer:
            groups[row.partition].append(row)
        for rows in groups.values():
            data = write_books(rows)
            result = await self._uploader.upload(book_object_name(rows), data, "application/vnd.apache.parquet")
            ids = [row.redis_id for row in rows]
            await self._consumer.ack(ids)
            self._buffer = [row for row in self._buffer if row.redis_id not in set(ids)]
            self._last_reclaim_at = time.monotonic()
        self._buffer_started_at = time.monotonic() if self._buffer else None
