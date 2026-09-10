import time

import redis
from dashboard.data import RedisReader, decode


class FakeRedis:
    def mget(self, *keys):
        return [b'{"price":"100"}', b'{"price":"101","total_volume":"2"}', b'[]', b'[]']

    def xrevrange(self, stream, count):
        return []


def test_reader_supports_redis_url(monkeypatch):
    from dashboard import data
    monkeypatch.setenv("REDIS_URL", "redis://example.test:6380/2")
    client = data.redis_client_from_env()
    assert client.connection_pool.connection_kwargs["host"] == "example.test"
    assert client.connection_pool.connection_kwargs["port"] == 6380
    assert client.connection_pool.connection_kwargs["db"] == 2


def test_decode_invalid_uses_fallback():
    assert decode(b"not-json", {"ok": False}) == {"ok": False}


def test_reader_reads_aggregator_keys():
    data = RedisReader(FakeRedis()).read()
    assert data.spot["price"] == "101"
    assert data.candles == []


def test_market_reader_fetches_only_market_panel_keys():
    class SpyRedis(FakeRedis):
        def __init__(self):
            self.keys = None

        def mget(self, *keys):
            self.keys = keys
            return super().mget(*keys)

    client = SpyRedis()
    data = RedisReader(client).read_market_data()

    assert data["redis_ok"] is True
    assert client.keys == (
        "market:book:BTCUSDT:latest",
        "market:spot:BTCUSDT:latest",
        "market:candles:BTCUSDT:30s",
    )


def test_fast_market_reader_fetches_only_spot():
    class SpyRedis(FakeRedis):
        def __init__(self):
            self.keys = None

        def mget(self, *keys):
            self.keys = keys
            return super().mget(*keys)

    client = SpyRedis()
    data = RedisReader(client).read_fast_market_data()

    assert data["redis_ok"] is True
    assert client.keys == ("market:spot:BTCUSDT:latest",)


def test_reader_reads_kalshi_contract_streams():
    received_ts_ms = int(time.time() * 1000)

    class RedisWithKalshi(FakeRedis):
        def xrevrange(self, stream, count):
            if stream == "stream:kalshi_tickers":
                return [
                    ("2-0", {b"payload": (f'{{"event_ticker":"KXBTCD-TEST","market_ticker":"KXBTCD-TEST-T70199.99","yes_bid_dollars":"0.41","yes_ask_dollars":"0.42","last_price_dollars":"0.41","volume":"10","open_interest":"20","received_ts_ms":{received_ts_ms}}}').encode()}),
                    ("1-0", {b"payload": (f'{{"event_ticker":"KXBTCD-TEST","market_ticker":"KXBTCD-TEST-T70099.99","yes_bid_dollars":"0.91","yes_ask_dollars":"0.92","last_price_dollars":"0.91","volume":"30","open_interest":"40","received_ts_ms":{received_ts_ms}}}').encode()}),
                ]
            if stream == "stream:kalshi_trades":
                return [
                    ("3-0", {b"payload": (f'{{"event_ticker":"KXBTCD-TEST","market_ticker":"KXBTCD-TEST-T70199.99","yes_price_dollars":"0.42","count":"5","taker_side":"yes","received_ts_ms":{received_ts_ms}}}').encode()})
                ]
            return []

    data = RedisReader(RedisWithKalshi()).read()

    assert [row["market_ticker"] for row in data.kalshi_contracts] == [
        "KXBTCD-TEST-T70099.99",
        "KXBTCD-TEST-T70199.99",
    ]
    assert data.kalshi_contracts[1]["last_trade"] == "42¢"


def test_reader_returns_safe_state_when_redis_is_unavailable():
    class BrokenRedis:
        def mget(self, *keys):
            raise redis.ConnectionError("redis is down")

    data = RedisReader(BrokenRedis()).read()

    assert data.redis_ok is False
    assert data.redis_error == "ConnectionError"
    assert data.book["bids"] == []
    assert data.kalshi_contracts == []


def test_reader_joins_fresh_analytics_prices_read_only():
    now = int(time.time() * 1000)

    class AnalyticsRedis(FakeRedis):
        def xrevrange(self, stream, count):
            if stream == "stream:kalshi_tickers":
                return [("1-0", {"payload": ('{"event_ticker":"E","market_ticker":"E-T100",'
                                               '"yes_bid_dollars":"0.40","yes_ask_dollars":"0.50",'
                                               f'"received_ts_ms":{now}}}').encode()})]
            return []

        def mget(self, *keys):
            if keys == ("market:pricing:v1:E-T100",):
                return [(f'{{"status":"available","model_probability":0.6,"model_value_dollars":0.6,'
                         f'"model_value_cents":60,"annualized_volatility":0.215,'
                         f'"time_to_expiry_minutes":12.5,"edge_vs_mid_probability":0.15,'
                         f'"buy_yes_edge_probability":0.1,"sell_yes_edge_probability":-0.2,'
                         f'"generated_ts_ms":{now}}}').encode()]
            return super().mget(*keys)

    row = RedisReader(AnalyticsRedis()).read_kalshi_data(100)["contracts"][0]
    assert row["model_value"] == "60.0¢"
    assert row["model_vol"] == "21.5%"
    assert row["tau"] == "12.5m"
    assert row["edge_mid"] == "-15.0¢"


def test_reader_ignores_stale_analytics_price():
    now = int(time.time() * 1000)

    class StaleAnalyticsRedis(FakeRedis):
        def xrevrange(self, stream, count):
            if stream == "stream:kalshi_tickers":
                return [("1-0", {"payload": (f'{{"event_ticker":"E","market_ticker":"E-T100","received_ts_ms":{now}}}').encode()})]
            return []
        def mget(self, *keys):
            if len(keys) == 1 and keys[0].startswith("market:pricing"):
                return [(f'{{"status":"available","model_value_dollars":0.6,"generated_ts_ms":{now - 60_001}}}').encode()]
            return super().mget(*keys)

    row = RedisReader(StaleAnalyticsRedis()).read_kalshi_data(100)["contracts"][0]
    assert row["model_value"] == "-"
