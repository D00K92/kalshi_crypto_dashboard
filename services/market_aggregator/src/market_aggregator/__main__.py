from __future__ import annotations

import asyncio
import logging
import signal

try:
    import uvloop
    uvloop.install()
except ImportError:
    pass

from market_aggregator.config import Settings
from market_aggregator.redis_service import AggregatorService


def main() -> None:
    settings = Settings.from_env()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    stop = asyncio.Event()
    loop = asyncio.new_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    try:
        loop.run_until_complete(AggregatorService(settings).run(stop))
    finally:
        loop.close()


if __name__ == "__main__":
    main()
