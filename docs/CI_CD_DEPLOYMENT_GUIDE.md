# CI/CD Deployment Guide

Automatic deployment means a developer's local machine stops being part of
the normal release process.

```text
Push code to GitHub
        ↓
GitHub Actions runs tests
        ↓
Builds linux/amd64 Docker image
        ↓
Pushes image tagged with commit SHA
        ↓
Updates GKE Deployment
        ↓
Waits for rollout and verifies Redis
```

This is CI/CD:

- CI: test and build every change.
- CD: deploy successful builds automatically.

## Current prerequisite

This directory currently has no Git repository or GitHub remote. Automation
needs:

1. A GitHub repository.
2. This project committed and pushed there.
3. GitHub authorized to access GCP.
4. A workflow in `.github/workflows/deploy-ingestion.yml`.

## Recommended release policy

For a production system:

- Pull request: run tests and build validation only.
- Merge to `main`: build and deploy ingestion.
- Tag every image with the Git commit SHA, not `v1` or `latest`.

An image would look like:

```text
asia-northeast3-docker.pkg.dev/kalshi-crypto-506614/quant-repo/ingestion:a8f41d...
```

That gives every deployment an immutable, traceable version.

## How GitHub accesses GCP

Use Workload Identity Federation.

GitHub presents a short-lived identity token to GCP. GCP verifies that the
request came from the authorized repository and grants temporary access. No
permanent GCP service-account key needs to be stored in GitHub.

Google recommends Workload Identity Federation over service-account JSON
keys. `google-github-actions/auth@v3` supports this directly:

- <https://github.com/google-github-actions/auth>

The deployment identity needs:

- Artifact Registry Writer to push images.
- GKE Cluster Viewer to obtain cluster credentials.
- Permission to update the Kubernetes Deployment.

The GKE node identity separately needs Artifact Registry Reader so nodes can
pull images.

## What the workflow will do

### 1. Trigger only for ingestion changes

The workflow should run when `main` changes in:

```text
services/ingestion/**
k8s/ingestion-deployment.yaml
.github/workflows/deploy-ingestion.yml
```

A dashboard-only change will not unnecessarily rebuild ingestion.

### 2. Run tests

Equivalent to:

```bash
uv run --directory services/ingestion --group dev pytest
```

A failing test stops deployment.

### 3. Build the image

GitHub's Linux runner builds `linux/amd64`. This avoids the Apple Silicon/GKE
architecture mismatch encountered when building locally.

### 4. Push with the commit SHA

Conceptually:

```bash
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/quant-repo/ingestion:${GITHUB_SHA}"
```

The workflow pushes that exact image to Artifact Registry.

### 5. Obtain temporary GKE credentials

Use:

```yaml
google-github-actions/get-gke-credentials@v3
```

It creates a temporary kubeconfig for the workflow:

- <https://github.com/google-github-actions/get-gke-credentials>

### 6. Deploy the exact image

The workflow updates only the ingestion container:

```bash
kubectl set image \
  deployment/ingestion-service \
  ingestion="$IMAGE"
```

Then waits for the rollout:

```bash
kubectl rollout status \
  deployment/ingestion-service \
  --timeout=5m
```

### 7. Verify behavior

Deployment success should require more than the process starting. The
workflow should verify:

- The pod is running.
- The container restart count remains zero.
- Logs contain `redis_ready`.
- Logs contain `venue_connected`.
- `stream:ticks` grows in Redis.

## Important ingestion-specific concern

A normal Kubernetes rolling update can briefly run the old and new ingestion
pods together. Both could consume Binance and duplicate events.

Initially, use this strategy:

```yaml
strategy:
  type: Recreate
```

That stops the old ingestion pod before starting the new one. It creates a
short data gap but prevents overlapping feeds. Later, leader election or
downstream deduplication can support zero-downtime rolling deployments.

## Rollback

Because every image has a unique commit tag:

```bash
kubectl rollout undo deployment/ingestion-service
```

Then wait for recovery:

```bash
kubectl rollout status deployment/ingestion-service
```

Kubernetes returns to the previous image.

## One-time work versus every deployment

One-time setup:

- Initialize Git.
- Create and connect a GitHub repository.
- Configure Workload Identity Federation.
- Create the deployment service account and IAM permissions.
- Add the GitHub Actions workflow.
- Add rollout-safe Kubernetes settings.

Afterward, the routine becomes:

```bash
git add .
git commit -m "Add Coinbase ingestion"
git push
```

GitHub handles testing, building, pushing, deploying, and rollout
verification. GitHub environments can also require approval before production
deployment:

- <https://docs.github.com/en/actions/reference/workflows-and-actions/deployments-and-environments>

Set this up before expanding ingestion substantially so subsequent venue
changes follow the same tested deployment path.
