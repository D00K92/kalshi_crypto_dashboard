"""Create-only, checksummed GCS object uploads."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import logging
from typing import Protocol

from google.api_core.exceptions import PreconditionFailed
from google.cloud import storage
from google.cloud.storage.retry import DEFAULT_RETRY


LOGGER = logging.getLogger(__name__)
UPLOAD_RETRY = DEFAULT_RETRY.with_deadline(20.0)


class BlobLike(Protocol):
    def upload_from_string(self, data: bytes, **kwargs: object) -> None: ...


@dataclass(frozen=True, slots=True)
class UploadResult:
    object_name: str
    already_existed: bool


class GCSUploader:
    def __init__(self, bucket_name: str, client: storage.Client | None = None) -> None:
        self._client = client or storage.Client()
        self._bucket = self._client.bucket(bucket_name)

    async def upload(
        self, object_name: str, data: bytes, content_type: str
    ) -> UploadResult:
        return await asyncio.to_thread(
            self._upload_sync, object_name, data, content_type
        )

    def _upload_sync(
        self, object_name: str, data: bytes, content_type: str
    ) -> UploadResult:
        blob: BlobLike = self._bucket.blob(object_name)
        try:
            blob.upload_from_string(
                data,
                content_type=content_type,
                if_generation_match=0,
                checksum="crc32c",
                timeout=15,
                retry=UPLOAD_RETRY,
            )
        except PreconditionFailed:
            LOGGER.info("gcs_object_already_exists", extra={"object": object_name})
            return UploadResult(object_name=object_name, already_existed=True)
        return UploadResult(object_name=object_name, already_existed=False)
