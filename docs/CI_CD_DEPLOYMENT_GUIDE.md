# How This Project Deploys Software

This document explains how code moves from a developer's computer into the production system. It is written for someone who does not need to know CI/CD terminology already.

## The Short Version

When code is pushed to the `main` branch:

1. GitHub Actions checks the code and runs tests.
2. GitHub Actions builds Docker images for the services.
3. If the checks pass, another GitHub Actions workflow deploys the images to Google Cloud.
4. Kubernetes, running in Google Kubernetes Engine (GKE), replaces the old service containers with the new ones.
5. The deployment workflow waits until the new containers are running successfully.

The normal process is therefore:

```text
git push main
        |
        v
GitHub Actions: test and build
        |
        v
GitHub Actions: deploy
        |
        +--> Artifact Registry: store Docker images
        |
        +--> GKE/Kubernetes: run the new images
```

## What CI/CD Means

**CI** means Continuous Integration. It automatically checks new code when it is pushed. In this project, CI installs the locked dependencies, runs Python tests, and checks that Docker images can be built.

**CD** means Continuous Delivery or Continuous Deployment. It automatically delivers code to the production environment. In this project, a successful CI run starts the `Deploy Services` workflow.

GitHub Actions is the service that runs both CI and CD. Each workflow runs on a temporary GitHub-hosted VM. The VM is used for testing, building images, and running deployment commands. The application itself does not run permanently on that VM.

## What Happens During CI

The CI workflow is:

[`.github/workflows/ci.yml`](../.github/workflows/ci.yml)

For each service, CI:

1. Checks out the pushed commit.
2. Installs the Python version and dependencies from `uv.lock`.
3. Runs the service's test suite.
4. Builds the service's Linux Docker image.

The current services are:

- `ingestion`
- `gcs-exporter`
- `aggregator`
- `dashboard`

CI builds all service images because this is a monorepo and the deployment workflow deploys the service set together. A Docker build confirms that the image can be assembled; it is not the same as running a full production test.

## What Happens During Deploy Services

The deployment workflow is:

[`.github/workflows/cd.yml`](../.github/workflows/cd.yml)

It starts only after CI completes successfully on `main`.

The workflow then:

1. Checks out the exact commit that passed CI.
2. Authenticates to Google Cloud using GitHub's OIDC identity mechanism.
3. Builds and pushes versioned Docker images to Google Artifact Registry.
4. Resolves the production Redis endpoint.
5. Applies the Kubernetes deployment manifests to the GKE cluster.
6. Waits for every deployment to finish rolling out successfully.

The image tag is the Git commit SHA. This makes it possible to identify exactly which source code is running in production.

The workflow verifies these deployments:

```bash
kubectl rollout status deployment/gcs-exporter
kubectl rollout status deployment/ingestion-service
kubectl rollout status deployment/aggregator
kubectl rollout status deployment/dashboard
```

## What Kubernetes Does

Kubernetes is the system that keeps the application containers running in GKE.

When the deployment workflow applies an updated manifest, Kubernetes notices that a newer Docker image is required. It starts a new pod using that image, waits for it to become ready, and then removes the old pod. This is called a **rolling update** or **rollout**.

The `kubectl` commands are executed by the temporary GitHub Actions VM, but they control the real GKE cluster. The services run in GKE after the workflow finishes.

## Example: Ingestion Service

The ingestion service is defined in several places because each file has a different responsibility:

- Application source: [`services/ingestion/src`](../services/ingestion/src)
- Python dependencies: [`services/ingestion/pyproject.toml`](../services/ingestion/pyproject.toml)
- Docker image instructions: [`services/ingestion/Dockerfile`](../services/ingestion/Dockerfile)
- Production Kubernetes configuration: [`k8s/ingestion-deployment.yaml`](../k8s/ingestion-deployment.yaml)
- Deployment workflow step: [`.github/workflows/cd.yml`](../.github/workflows/cd.yml)

For ingestion, the deployment sequence is:

```text
Change ingestion source code
        |
        v
Push commit to main
        |
        v
Run ingestion tests
        |
        v
Build ingestion Docker image
        |
        v
Push image to Artifact Registry
        |
        v
Apply k8s/ingestion-deployment.yaml
        |
        v
Kubernetes replaces ingestion-service pod
```

The workflow uses the commit SHA as the image tag, so the Kubernetes deployment can run the exact image produced from the tested commit.

## Staging Integration Test

After successful CI, a separate workflow runs an integration test in GKE:

[`.github/workflows/integration.yml`](../.github/workflows/integration.yml)

This test builds a test version of the aggregator, starts an integration job in GKE, and checks behavior against the staging configuration. It is separate from the production rollout verification.

## How to Check a Deployment

In GitHub, open the repository's **Actions** tab. The relevant workflows are:

- **CI**: tests and Docker builds
- **Deploy Services**: production image deployment and Kubernetes rollout verification
- **Staging Integration**: integration test in GKE

A production deployment is complete when **Deploy Services** is successful. The staging integration result is useful additional evidence, but it is a separate workflow.

## Dashboard Access

The dashboard Kubernetes Service remains internal (`ClusterIP`). Its public,
read-only portfolio URL is served by the GKE external HTTPS load balancer:

```text
https://crypto-dashboard.kairos-trading.com
```

The ingress redirects HTTP to HTTPS and uses a Google-managed certificate. The
Cloudflare DNS record must point at the reserved global address named
`crypto-dashboard-public-ip`. Keep Cloudflare set to **DNS only** while the
Google certificate is provisioning. Once the certificate is `Active`, Cloudflare
may be proxied with SSL/TLS mode set to **Full (strict)**.

For temporary local access, use a port-forward:

```bash
kubectl port-forward service/dashboard 8052:8050
```

Then open:

```text
http://localhost:8052
```

This forwards a local computer port to the dashboard service inside GKE. It does not change the deployment or expose the dashboard publicly.

## Important Distinction

There are three different environments involved:

- **GitHub Actions VM**: temporary machine used to test, build, and issue deployment commands.
- **Artifact Registry**: Google Cloud storage for versioned Docker images.
- **GKE/Kubernetes**: the production environment where the services actually run.

The GitHub Actions VM disappears after the workflow. Artifact Registry keeps the images. GKE keeps the application services running.
