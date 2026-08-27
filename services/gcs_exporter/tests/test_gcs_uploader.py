from __future__ import annotations

from google.api_core.exceptions import PreconditionFailed

from gcs_exporter.gcs_uploader import GCSUploader


class FakeBlob:
    def __init__(self, fail_precondition: bool = False) -> None:
        self.fail_precondition = fail_precondition
        self.calls: list[tuple[bytes, dict[str, object]]] = []

    def upload_from_string(self, data: bytes, **kwargs: object) -> None:
        self.calls.append((data, kwargs))
        if self.fail_precondition:
            raise PreconditionFailed("already exists")


class FakeBucket:
    def __init__(self, blob: FakeBlob) -> None:
        self._blob = blob

    def blob(self, object_name: str) -> FakeBlob:
        return self._blob


class FakeClient:
    def __init__(self, blob: FakeBlob) -> None:
        self._bucket = FakeBucket(blob)

    def bucket(self, bucket_name: str) -> FakeBucket:
        return self._bucket


async def test_upload_uses_create_only_precondition_and_crc32c() -> None:
    blob = FakeBlob()
    uploader = GCSUploader("bucket", client=FakeClient(blob))  # type: ignore[arg-type]

    result = await uploader.upload("ticks/file.parquet", b"data", "type/parquet")

    assert result.already_existed is False
    assert blob.calls == [
        (
            b"data",
            {
                "content_type": "type/parquet",
                "if_generation_match": 0,
                "checksum": "crc32c",
                "timeout": 15,
                "retry": blob.calls[0][1]["retry"],
            },
        )
    ]
    assert blob.calls[0][1]["retry"]._deadline == 20.0


async def test_precondition_failure_is_an_idempotent_success() -> None:
    uploader = GCSUploader(
        "bucket", client=FakeClient(FakeBlob(fail_precondition=True))  # type: ignore[arg-type]
    )

    result = await uploader.upload("ticks/file.parquet", b"data", "type/parquet")

    assert result.already_existed is True
