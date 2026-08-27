"""Command-line entry point for the ingestion service."""

from __future__ import annotations

import asyncio
import logging
import signal

from ingestion.config import Settings
from ingestion.service import IngestionService


async def _run() -> None:
    settings = Settings.from_env()
    logging.basicConfig(
        level=getattr(logging, settings.log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    await IngestionService(settings).run(stop_event)


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
