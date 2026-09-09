# Model-serving API

Internal ClusterIP service for validated four-horizon volatility forecasts.
The production image contains an immutable model bundle: the promoted 1h
XGBoost artifact plus EWMA configuration for 5m, 15m, and 30m. It does
not consume Redis, publish to Feast, or expose a public endpoint.

Release CI resolves the approved Vertex model, downloads its GCS artifact into
the Docker build context, checks the feature metadata and checksums, and builds
the bundle into the image. The running pod therefore needs no Vertex/GCS IAM.
The image build and runtime startup both load and smoke-test the bundle before
the service is considered ready.

Run locally:

```bash
uv run pytest
python scripts/prepare_bundle.py --artifact-dir /tmp/model \
  --output-dir model_bundle --resource-name projects/.../models/...@1 \
  --artifact-uri gs://bucket/path
MODEL_BUNDLE_MANIFEST=model_bundle/manifest.json \
  uv run uvicorn model_serving.api:app --host 127.0.0.1 --port 8080
```

A normal local Docker build validates the Dockerfile without requiring a model.
Production passes `--build-arg REQUIRE_MODEL_BUNDLE=true`; that build fails if
the approved bundle is absent, corrupt, or incompatible.
