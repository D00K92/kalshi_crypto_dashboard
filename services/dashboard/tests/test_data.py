from dashboard.data import RedisReader, decode


class FakeRedis:
    def mget(self, *keys):
        return [b'{"price":"100"}', b'{"price":"101","total_volume":"2"}', b'[]', b'[]']


def test_reader_supports_redis_url(monkeypatch):
    import dashboard.data as data
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
