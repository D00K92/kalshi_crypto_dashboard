"""Environment-backed service configuration."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path

from dotenv import load_dotenv


_LOCAL_ENV = Path(__file__).resolve().parents[2] / ".env"


def _positive_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{name} must be positive, got {value}")
    return value


def _redis_url() -> str:
    explicit_url = os.getenv("INGESTION_REDIS_URL")
    if explicit_url:
        return explicit_url

    host = os.getenv("REDIS_HOST", "localhost")
    port = _positive_int("REDIS_PORT", 6379)
    return f"redis://{host}:{port}/0"


@dataclass(frozen=True, slots=True)
class Settings:
    redis_url: str
    binance_ws_url: str
    gemini_ws_url: str
    gemini_symbol: str
    coinbase_ws_url: str
    coinbase_product_id: str
    coinbase_api_key: str
    coinbase_secret: str
    deribit_ws_url: str
    deribit_instrument: str
    bybit_ws_url: str
    bybit_symbol: str
    kraken_ws_url: str
    kraken_symbol: str
    kalshi_ws_url: str
    kalshi_rest_url: str
    kalshi_api_key: str
    kalshi_private_key: str
    kalshi_series_ticker: str
    queue_maxsize: int
    stream_maxlen: int
    shutdown_grace_seconds: int
    log_level: str

    @classmethod
    def from_env(cls) -> "Settings":
        # Local development convenience; Kubernetes injects environment variables
        # directly and therefore does not depend on this file.
        load_dotenv(_LOCAL_ENV, override=False)
        symbol = os.getenv("BINANCE_SYMBOL", "btcusdt").lower()
        default_streams = f"{symbol}@trade/{symbol}@depth20@100ms"
        default_ws_url = (
            "wss://stream.binance.com:9443/stream?streams=" + default_streams
        )
        rsa_path = os.getenv("KALSHI_RSA_PATH", "").strip()
        private_key = os.getenv("KALSHI_PRIVATE_KEY", "")
        if rsa_path:
            key_file = Path(rsa_path).expanduser()
            try:
                private_key = key_file.read_text(encoding="utf-8")
            except OSError as exc:
                raise ValueError(f"KALSHI_RSA_PATH is not readable: {key_file}") from exc

        return cls(
            redis_url=_redis_url(),
            binance_ws_url=os.getenv("BINANCE_WS_URL", default_ws_url),
            gemini_ws_url=os.getenv("GEMINI_WS_URL", "wss://ws.gemini.com"),
            gemini_symbol=os.getenv("GEMINI_SYMBOL", "btcusd"),
            coinbase_ws_url=os.getenv("COINBASE_WS_URL", "wss://advanced-trade-ws.coinbase.com"),
            coinbase_product_id=os.getenv("COINBASE_PRODUCT_ID", "BTC-USD"),
            coinbase_api_key=os.getenv("COINBASE_API_KEY", ""),
            coinbase_secret=os.getenv("COINBASE_SECRET", ""),
            deribit_ws_url=os.getenv("DERIBIT_WS_URL", "wss://www.deribit.com/ws/api/v2"),
            deribit_instrument=os.getenv("DERIBIT_INSTRUMENT", "BTC_USDT"),
            bybit_ws_url=os.getenv("BYBIT_WS_URL", "wss://stream.bybit.com/v5/public/spot"),
            bybit_symbol=os.getenv("BYBIT_SYMBOL", "BTCUSDT"),
            kraken_ws_url=os.getenv("KRAKEN_WS_URL", "wss://ws.kraken.com/v2"),
            kraken_symbol=os.getenv("KRAKEN_SYMBOL", "BTC/USD"),
            kalshi_ws_url=os.getenv(
                "KALSHI_WS_URL", "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
            ),
            kalshi_rest_url=os.getenv(
                "KALSHI_REST_URL", "https://external-api.kalshi.com"
            ),
            kalshi_api_key=os.getenv("KALSHI_API_KEY", ""),
            kalshi_private_key=private_key,
            kalshi_series_ticker=os.getenv("KALSHI_SERIES_TICKER", "KXBTCD"),
            queue_maxsize=_positive_int("INGESTION_QUEUE_MAXSIZE", 10_000),
            stream_maxlen=_positive_int("INGESTION_STREAM_MAXLEN", 1_000_000),
            shutdown_grace_seconds=_positive_int(
                "INGESTION_SHUTDOWN_GRACE_SECONDS", 10
            ),
            log_level=os.getenv("INGESTION_LOG_LEVEL", "INFO").upper(),
        )
