# Portfolio PDF Plan — Data Scientist

## Title

**End-to-End Crypto Volatility Modelling and Prediction-Market Pricing Platform**

## Page 1 — Project objective and business problem

- Decision problem: estimate the probability that BTC settles above a Kalshi contract strike before expiration.
- Inputs: live multi-venue BTC market state plus Kalshi strike, expiry, and quotes.
- Outputs: five volatility forecasts (1m, 5m, 15m, 30m, 1h), probability, fair value, and quote-relative edge.
- Scope: decision-support/pricing analytics; no automated trade execution.
- Core promise: outputs are traceable to versioned models and withheld when dependencies are stale or invalid.

## Page 2 — Data architecture and feature-store design

- Ingestion normalizes crypto and Kalshi WebSocket messages into versioned Redis Stream events.
- Aggregator produces fresh-venue synthetic BTC state and live features.
- GCS exporter writes verified, partitioned Parquet archive data; BigQuery builds canonical bars, features, and labels.
- Feast supplies point-in-time historical feature retrieval and Redis online-feature materialization.
- Explain the v1 live-safe feature contract (`log_return`, `venue_count`) and train/serve consistency.

## Page 3 — ML pipeline, validation, and model lifecycle

- Future annualized realized-volatility labels for 1m, 5m, 15m, 30m, and 1h.
- Point-in-time Feast joins; chronological 70/15/15 split; one XGBoost regressor per horizon.
- QLIKE primary metric, EWMA baseline, RMSE/MAE/R² supporting metrics.
- Immutable model bundle: model, horizon, ordered features, version, and metrics; registered through Vertex AI.
- State the next step: promote richer microstructure features only after offline value, leakage, and serving-parity validation.

## Page 4 — Online inference, pricing, and MLOps

- Redis is the low-latency event/current-state layer; Feast owns feature contracts and offline/online consistency.
- Analytics reads the current Redis feature envelope and loads exactly five validated Vertex/GCS model bundles.
- Interpolate total variance to Kalshi expiry and derive an above-strike Gaussian probability, fair value, and bid/ask edges.
- Fail closed on stale/missing inputs, invalid metadata, incomplete models, or invalid quotes.
- CI: locked dependencies, tests, and Docker builds. CD: OIDC Workload Identity, commit-SHA images in Artifact Registry, GKE manifest application, rollout verification, and analytics smoke checks.

## Accuracy guardrails

- Do not claim latency or performance numbers unless measured.
- Do not claim automated trading, complete retraining automation, or CI coverage for every service.
- Do not use `k8s/PROJECT_DESCRIPTION.md` as a source for current implementation claims; it is historical and conflicts with the current architecture.
