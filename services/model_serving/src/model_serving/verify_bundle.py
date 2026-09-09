from __future__ import annotations

import json

from .config import Settings
from .packaged_provider import PackagedHybridProvider


def main() -> None:
    settings = Settings.from_env()
    provider = PackagedHybridProvider.load(
        settings.model_bundle_manifest,
        model_version=settings.model_version,
        feature_version=settings.feature_version,
        decay=settings.ewma_decay,
    )
    print(json.dumps({"status": "ok", "resource_1h": provider.model_resource}, sort_keys=True))


if __name__ == "__main__":
    main()
