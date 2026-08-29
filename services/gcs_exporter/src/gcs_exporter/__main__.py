"""Command-line entry point for the GCS exporter."""

from __future__ import annotations

import asyncio
import signal
from dataclasses import replace

from gcs_exporter.config import Settings
from gcs_exporter.kalshi_service import (
    KalshiOrderBookExporterService,
    KalshiTickerExporterService,
    KalshiTradeExporterService,
)
from gcs_exporter.logging_config import configure_logging
from gcs_exporter.service import GCSExporterService
from gcs_exporter.book_service import OrderBookExporterService


async def _run() -> None:
    settings = Settings.from_env()
    configure_logging(settings.log_level)
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)
    trade_service = GCSExporterService(settings)
    book_settings = replace(
        settings,
        stream_name="stream:orderbook_snapshots",
        consumer_group="gcs_orderbook_archiver_group",
        consumer_name=f"{settings.consumer_name}-books",
        health_port=settings.health_port + 1,
    )
    book_service = OrderBookExporterService(book_settings)
    kalshi_ticker_settings = replace(
        settings,
        stream_name="stream:kalshi_tickers",
        consumer_group="gcs_kalshi_ticker_archiver_group",
        consumer_name=f"{settings.consumer_name}-kalshi-tickers",
        health_port=settings.health_port + 2,
    )
    kalshi_trade_settings = replace(
        settings,
        stream_name="stream:kalshi_trades",
        consumer_group="gcs_kalshi_trade_archiver_group",
        consumer_name=f"{settings.consumer_name}-kalshi-trades",
        health_port=settings.health_port + 3,
    )
    kalshi_orderbook_settings = replace(
        settings,
        stream_name="stream:kalshi_orderbook",
        consumer_group="gcs_kalshi_orderbook_archiver_group",
        consumer_name=f"{settings.consumer_name}-kalshi-orderbook",
        health_port=settings.health_port + 4,
    )
    kalshi_ticker_service = KalshiTickerExporterService(kalshi_ticker_settings)
    kalshi_trade_service = KalshiTradeExporterService(kalshi_trade_settings)
    kalshi_orderbook_service = KalshiOrderBookExporterService(kalshi_orderbook_settings)
    await asyncio.gather(
        trade_service.run(stop_event),
        book_service.run(stop_event),
        kalshi_ticker_service.run(stop_event),
        kalshi_trade_service.run(stop_event),
        kalshi_orderbook_service.run(stop_event),
    )


def main() -> None:
    try:
        try:
            import uvloop
        except ImportError:
            asyncio.run(_run())
        else:
            uvloop.run(_run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
