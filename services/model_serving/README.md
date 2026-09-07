# Model-serving API

Internal ClusterIP service for validated five-horizon volatility forecasts.
The current implementation serves the deterministic EWMA champion. It does
not consume Redis, publish to Feast, or expose a public endpoint.

Run locally:

```bash
uv run pytest
uv run uvicorn model_serving.api:app --host 127.0.0.1 --port 8080
```
