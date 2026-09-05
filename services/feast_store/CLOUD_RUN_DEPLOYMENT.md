# Feast on Cloud Run

The feature server and materialization job use the same image. Build from the
repository root:

```bash
PROJECT_ID=kalshi-crypto-506614
REGION=asia-northeast3
REPO=quant-repo
IMAGE="$REGION-docker.pkg.dev/$PROJECT_ID/$REPO/feast-store:v1"

gcloud builds submit services/feast_store \
  --project "$PROJECT_ID" \
  --tag "$IMAGE"
```

The Cloud Run service needs a Serverless VPC Access connector and a runtime
service account with GCS read access and private Redis access. Set
`REDIS_URL` in `feature_store.yaml` (normally to the Memorystore private IP);
the Kubernetes DNS name is not resolvable from Cloud Run.

```bash
CONNECTOR=projects/$PROJECT_ID/locations/$REGION/connectors/feast-vpc
SERVICE_ACCOUNT=feast-runtime@$PROJECT_ID.iam.gserviceaccount.com

gcloud run deploy feast-server \
  --project "$PROJECT_ID" --region "$REGION" --image "$IMAGE" \
  --port 6566 --vpc-connector "$CONNECTOR" \
  --service-account "$SERVICE_ACCOUNT" --no-allow-unauthenticated

gcloud run jobs create feast-materialize \
  --project "$PROJECT_ID" --region "$REGION" --image "$IMAGE" \
  --service-account "$SERVICE_ACCOUNT" \
  --command /app/feast_store/jobs/materialize.sh \
  --tasks 1
```

Run the job hourly with Cloud Scheduler. The Scheduler service account needs
`roles/run.invoker` on the job:

```bash
gcloud scheduler jobs create http feast-materialize-hourly \
  --project "$PROJECT_ID" --location "$REGION" --schedule="5 * * * *" \
  --uri="https://run.googleapis.com/apis/run.googleapis.com/v1/projects/$PROJECT_ID/locations/$REGION/jobs/feast-materialize:run" \
  --http-method=POST --oauth-service-account-email="$SERVICE_ACCOUNT" \
  --oauth-token-scope=https://www.googleapis.com/auth/cloud-platform
```

Before deployment, apply the repository once with `feast apply` and verify
`volatility_v1` appears in the registry. Use `get_historical_features` for
training and `get_online_features`/the feature server for inference.
