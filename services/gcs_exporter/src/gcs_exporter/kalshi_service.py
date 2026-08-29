"""Kalshi Redis stream exporters, sharing the base exporter lifecycle."""

from __future__ import annotations

from collections import defaultdict
import time
from typing import Callable, Generic, TypeVar

from gcs_exporter.models import (
    KalshiOrderBookRow,
    KalshiTickerRow,
    KalshiTradeRow,
    RawStreamEntry,
)
from gcs_exporter.object_names import (
    kalshi_orderbook_object_name,
    kalshi_ticker_object_name,
    kalshi_trade_object_name,
)
from gcs_exporter.parquet_writer import (
    write_kalshi_orderbooks,
    write_kalshi_tickers,
    write_kalshi_trades,
)
from gcs_exporter.service import GCSExporterService


RowT = TypeVar("RowT", KalshiTickerRow, KalshiTradeRow, KalshiOrderBookRow)


class KalshiExporterService(GCSExporterService, Generic[RowT]):
    row_factory: Callable[[RawStreamEntry], RowT]
    writer: Callable[[list[RowT]], bytes]
    object_namer: Callable[[list[RowT]], str]

    async def _ingest(self, entries: list[RawStreamEntry]) -> None:
        for entry in entries:
            try:
                row = self.row_factory(entry)
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
            data = self.writer(rows)
            result = await self._uploader.upload(
                self.object_namer(rows),
                data,
                "application/vnd.apache.parquet",
            )
            ids = [row.redis_id for row in rows]
            await self._consumer.ack(ids)
            committed = set(ids)
            self._buffer = [row for row in self._buffer if row.redis_id not in committed]
            self._last_reclaim_at = time.monotonic()
        self._buffer_started_at = time.monotonic() if self._buffer else None


class KalshiTickerExporterService(KalshiExporterService[KalshiTickerRow]):
    row_factory = KalshiTickerRow.from_entry
    writer = staticmethod(write_kalshi_tickers)
    object_namer = staticmethod(kalshi_ticker_object_name)


class KalshiTradeExporterService(KalshiExporterService[KalshiTradeRow]):
    row_factory = KalshiTradeRow.from_entry
    writer = staticmethod(write_kalshi_trades)
    object_namer = staticmethod(kalshi_trade_object_name)


class KalshiOrderBookExporterService(KalshiExporterService[KalshiOrderBookRow]):
    row_factory = KalshiOrderBookRow.from_entry
    writer = staticmethod(write_kalshi_orderbooks)
    object_namer = staticmethod(kalshi_orderbook_object_name)
