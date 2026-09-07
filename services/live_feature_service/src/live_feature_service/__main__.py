from __future__ import annotations

import asyncio
import logging

from .service import run


if __name__ == "__main__":
    logging.basicConfig(level="INFO")
    asyncio.run(run())
