"""Print live Coinbase Advanced Trade Level 2 data for local diagnostics.

Credentials must be supplied through the environment:
COINBASE_API_KEY (CDP key name) and COINBASE_SECRET (ES256 PEM private key).
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from pathlib import Path

import jwt
import orjson
from websockets.asyncio.client import connect

from ingestion.adapters.coinbase import CoinbaseBook, CoinbaseMessageError
from ingestion.adapters.coinbase_auth import CoinbaseAuthError, build_coinbase_ws_jwt


URL = "wss://advanced-trade-ws.coinbase.com"


def _load_local_env() -> None:
    """Load simple KEY=VALUE entries from the service-local .env if present."""
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if not env_path.is_file():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        name, separator, value = line.partition("=")
        name = name.strip()
        if not separator or not name.isidentifier():
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        # Do not overwrite explicitly exported shell values.
        os.environ.setdefault(name, value)


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--product", default=os.getenv("COINBASE_PRODUCT_ID", "BTC-USD"))
    parser.add_argument("--seconds", type=float, default=30.0, help="stop after this many seconds (0 means forever)")
    return parser.parse_args()


async def main() -> None:
    _load_local_env()
    args = _args()
    try:
        api_key = os.getenv("COINBASE_API_KEY", "")
        secret = os.getenv("COINBASE_SECRET", "")
        print(f"Credentials loaded: api_key={'yes' if api_key else 'no'}, secret={'yes' if secret else 'no'}", flush=True)
        token = build_coinbase_ws_jwt(
            api_key,
            secret,
        )
    except CoinbaseAuthError as exc:
        print(f"Credential error: {exc}", file=sys.stderr)
        print("Set COINBASE_API_KEY and COINBASE_SECRET in the environment.", file=sys.stderr)
        raise SystemExit(2) from exc
    header = jwt.get_unverified_header(token)
    claims = jwt.decode(token, options={"verify_signature": False})
    print(
        "JWT generated: "
        f"alg={header.get('alg')}, kid={'present' if header.get('kid') else 'missing'}, "
        f"iss={claims.get('iss')}, ttl={int(claims.get('exp', 0) - claims.get('nbf', 0))}s",
        flush=True,
    )

    book = CoinbaseBook(args.product)
    started = time.monotonic()
    print(f"Connecting to Coinbase Advanced Trade ({args.product}) ...", flush=True)
    # Coinbase's initial level2 snapshot can exceed the websockets default
    # 1 MiB frame limit. The snapshot is validated and capped to top-15 levels
    # after receipt, so allow the complete control/data frame here.
    async with connect(
        URL,
        open_timeout=10,
        close_timeout=5,
        ping_interval=20,
        compression=None,
        max_size=None,
    ) as ws:
        print("Connected; subscribing to level2 ...", flush=True)
        await ws.send(orjson.dumps({
            "type": "subscribe",
            "channel": "level2",
            "product_ids": [args.product],
            "jwt": token,
        }))
        async for raw in ws:
            message = orjson.loads(raw)
            if not isinstance(message, dict):
                continue
            if message.get("type") == "error":
                print(f"Coinbase error: {message.get('message') or message.get('error_details')}", flush=True)
                continue
            if message.get("channel") != "l2_data":
                print(f"Control: channel={message.get('channel')} type={message.get('type')}", flush=True)
                continue
            for event in message.get("events", []):
                if not isinstance(event, dict):
                    continue
                try:
                    snapshot = book.apply_advanced(event, int(time.time() * 1000))
                except CoinbaseMessageError as exc:
                    print(f"Malformed book event: {exc}", file=sys.stderr, flush=True)
                    continue
                if snapshot is not None:
                    print(orjson.dumps({
                        "sequence": snapshot.sequence,
                        "best_bid": snapshot.bids[0].price,
                        "best_ask": snapshot.asks[0].price,
                        "bids": [{"price": x.price, "quantity": x.quantity} for x in snapshot.bids],
                        "asks": [{"price": x.price, "quantity": x.quantity} for x in snapshot.asks],
                    }).decode(), flush=True)
            if args.seconds > 0 and time.monotonic() - started >= args.seconds:
                return


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.", flush=True)
