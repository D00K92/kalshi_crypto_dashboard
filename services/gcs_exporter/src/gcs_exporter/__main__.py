"""Command-line entry point for the GCS exporter."""

from __future__ import annotations

import asyncio
import signal

from gcs_exporter.config import Settings
from gcs_exporter.logging_config import configure_logging
from gcs_exporter.service import GCSExporterService


async def _run() -> None:
    settings = Settings.from_env()
    configure_logging(settings.log_level)
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)
    await GCSExporterService(settings).run(stop_event)


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
