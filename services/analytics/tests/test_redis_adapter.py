import json

from redis.exceptions import ResponseError

from kalshi_crypto_analytics.redis_adapter import RedisMarketData, RedisPricingPublisher


class Pipe:
    def __init__(self): self.calls = []
    def __getattr__(self, name):
        def call(*args, **kwargs): self.calls.append((name, args, kwargs)); return self
        return call
    async def execute(self): return self.calls


class Redis:
    def __init__(self): self.pipes = []; self.acked = []
    def pipeline(self, transaction=False): pipe = Pipe(); self.pipes.append(pipe); return pipe
    async def zrangebyscore(self, *args): return [b"OLD"]
    async def zrange(self, *args): return [b"OLD", b"NOT-ACTIVE"]
    async def ping(self): return True


async def test_unavailable_atomically_deletes_price_and_active_member():
    client = Redis(); publisher = RedisPricingPublisher(client)
    await publisher.unavailable("TICKER", "stale_spot", 100)
    calls = client.pipes[0].calls
    assert ("delete", ("market:pricing:v1:TICKER",), {}) in calls
    assert ("zrem", ("market:pricing:v1:active", "TICKER"), {}) in calls
    assert any(call[0] == "set" and call[1][0] == "market:pricing:v1:status:TICKER" for call in calls)


async def test_publication_cleanup_removes_expired_and_no_longer_active_keys():
    client = Redis(); publisher = RedisPricingPublisher(client)
    await publisher.expire_inactive({"CURRENT"}, 100_000)
    calls = client.pipes[0].calls
    assert any(call[:2] == ("delete", ("market:pricing:v1:OLD",)) for call in calls)
    assert any(call[:2] == ("delete", ("market:pricing:v1:NOT-ACTIVE",)) for call in calls)


class RecoveryRedis:
    def __init__(self): self.group_calls = 0
    async def xgroup_create(self, *args, **kwargs):
        self.group_calls += 1
        raise ResponseError("BUSYGROUP Consumer Group name already exists")
    async def xrevrange(self, *args, **kwargs):
        payload = {"event_type": "kalshi_ticker", "market_ticker": "T", "event_ticker": "E",
                   "series_ticker": "KXBTCD", "yes_bid_dollars": ".4", "yes_ask_dollars": ".5", "exchange_ts_ms": 10}
        return [(b"1-0", {b"payload": json.dumps(payload).encode()})]
    async def xautoclaim(self, *args, **kwargs): return [b"0-0", await self.xrevrange("")]
    async def xreadgroup(self, *args, **kwargs): return []


class FeatureRedis:
    def __init__(self, payload):
        self.payload = payload

    async def get(self, key):
        self.key = key
        return json.dumps(self.payload).encode()


async def test_read_features_uses_the_v2_10s_key_and_contract():
    source = RedisMarketData(FeatureRedis({
        "feature_set": "market_features",
        "feature_version": "v2_10s",
        "event_timestamp_ms": 100,
        "available_timestamp_ms": 110,
        "values": {"synthetic_price": 100, "log_return": 0, "venue_count": 2},
    }))

    observation = await source.read_features()

    assert source.client.key == "market:features:v2_10s:BTCUSD:latest"
    assert observation.feature_version == "v2_10s"


async def test_existing_group_bootstrap_and_pending_recovery():
    source = RedisMarketData(RecoveryRedis())
    await source.ensure_group()
    assert [item.market_ticker for item in await source.bootstrap_tickers()] == ["T"]
    recovered = await source.consume_tickers()
    assert recovered[0][0] == "1-0"
    assert recovered[0][1].market_ticker == "T"
