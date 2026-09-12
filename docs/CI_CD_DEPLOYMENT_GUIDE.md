# CI/CD and deployment guide

This document describes the current path from a commit on `main` to the GKE
production workloads.

## Workflow chain

```text
push main
   |
   +-> CI
   |     tests + linux/amd64 build checks for all deployed services
   |
   +-> Staging Integration
   |     isolated Redis, aggregator, live-feature-service, restart/recovery probes
   |
   +-> Deploy Services
         production environment gate
         build/push commit-SHA images
         package approved 1h model
         apply GKE resources
         verify rollouts, feature parity, and live analytics outputs
```

`Deploy Services` runs automatically only when the matching Staging
Integration workflow succeeds on `main`. It can also be dispatched manually.
The production job has a concurrency lock and uses the GitHub `production`
environment for any configured approval protection.

A separate `ML Pipeline CI` workflow tests Feast and ML code. On a
main-branch change it publishes immutable `ml-load`, `ml-train`,
`ml-evaluate`, and `ml-register` task images and a compiled Vertex pipeline
template. It does not submit training or promote a model automatically.

## CI

Source: `.github/workflows/ci.yml`.

CI uses each service's committed `uv.lock` when present, runs its tests, and
performs a Linux Docker build for:

- ingestion
- gcs_exporter
- aggregator
- dashboard
- analytics
- batch_etl
- live_feature_service
- model_serving
- feast_store

The model-serving CI build is structural and may omit a production artifact.
The production build requires the approved bundle.

## Staging integration

Source: `.github/workflows/integration.yml`.

The staging job uses isolated resource names and deploys the real aggregator and
live-feature-service images. It verifies Redis input/output contracts,
feature generation, consumer-group recovery, and restart behavior. Nothing in
staging changes production consumer groups or latest-state keys.

A failure stops the workflow chain; production CD is not triggered.

## Production deployment

Source: `.github/workflows/cd.yml`.

CD checks out the exact successful commit and authenticates to Google Cloud via
OIDC Workload Identity. It builds and pushes commit-SHA images to
`asia-northeast3-docker.pkg.dev/kalshi-crypto-506614/quant-repo`.

Before the model-serving image is built, CD requires:

- `VOLATILITY_MODEL_1H`: exact immutable Vertex model resource ending in a
  version such as `@1`.
- `VOLATILITY_MODEL_1H_ARTIFACT_URI`: immutable GCS directory containing
  `model.joblib` and `metadata.json`.

CD downloads those two files, validates the v2_10s 1h contract, generates a
checksum manifest, and builds with `REQUIRE_MODEL_BUNDLE=true`. Model-serving
therefore needs no Vertex/GCS access at runtime.

The workflow then:

1. Resolves the Memorystore private endpoint.
2. Applies the Feast registry with a one-shot Job.
3. Renders and applies all Deployment, Service, CronJob, and Ingress manifests.
4. Waits for active Deployments to roll out and confirms the suspended
   `feast-server` compatibility Deployment is applied at zero replicas.
5. Confirms the batch and parity CronJobs exist.
6. Runs an immediate offline/online feature parity Job.
7. Checks analytics readiness and fresh live Redis outputs.
8. Verifies the exact approved 1h resource, EWMA short-horizon resources, and
   at least one available KXBTCD price.

## Required repository and cluster configuration

GitHub/GCP identity is configured in the workflows. The mutable deployment
prerequisites are:

| Item | Purpose |
|---|---|
| GitHub variable `VOLATILITY_MODEL_1H` | Approved immutable 1h Vertex version |
| GitHub variable `VOLATILITY_MODEL_1H_ARTIFACT_URI` | Matching immutable GCS artifact directory |
| GitHub variable `ANALYTICS_GCP_SERVICE_ACCOUNT` | Workload Identity annotation for analytics rollback capability |
| Secret `kalshi-credentials` | Kalshi API key and private key for ingestion/analytics |
| Secret `coinbase-credentials` | Optional authenticated Coinbase feed |
| Memorystore instance in `asia-northeast3` | Real-time state and Streams |
| GCS/BigQuery IAM for `gcs-exporter` and `batch-etl` service accounts | Archive and offline pipelines |
| Managed certificate, DNS, and static IP | Public dashboard ingress |

Secrets must never be committed or printed in logs.

## Operational checks

```bash
gh run list --limit 10
gh run view <run-id>

kubectl get deployments
kubectl get cronjobs
kubectl rollout status deployment/model-serving --timeout=5m
kubectl rollout status deployment/analytics --timeout=5m
kubectl logs deployment/analytics --tail=100
```

Application readiness:

- model-serving: `GET /readyz` only after the packaged provider loads.
- analytics: `GET /readyz` only after at least one fresh supported price.
- dashboard: `GET /readyz` only while Redis is reachable.
- aggregator/live-feature/exporter: pod-local readiness after Redis/state setup.

The authoritative interfaces and service ownership boundaries are documented in
`ARCHITECTURE.md` and each `services/<service>/README.md`.
