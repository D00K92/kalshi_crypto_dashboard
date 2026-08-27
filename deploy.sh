#!/usr/bin/env bash
set -e

# 서울 리전(asia-northeast3) 환경 변수 설정
export PROJECT_ID="kalshi-crypto-506614"
export REGION="asia-northeast3"
export CLUSTER_NAME="quant-cluster"
export REPO_NAME="quant-repo"
export REDIS_HOST=$(gcloud redis instances list --region=$REGION --format="value(host)" | head -n1)
export REDIS_PORT="6379"

echo "=== Deploying to GCP ($REGION) ==="
echo "Project:    $PROJECT_ID"
echo "Cluster:    $CLUSTER_NAME"
echo "Repo:       $REPO_NAME"
echo "Redis IP:   $REDIS_HOST"
echo "=================================="

# 1. 서울 리전 Artifact Registry 인증 설정
gcloud auth configure-docker ${REGION}-docker.pkg.dev --quiet

# 2. 이미지 빌드 및 푸시 (asia-northeast3-docker.pkg.dev)
docker build -t ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/ingestion:v1 services/ingestion/
docker push ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/ingestion:v1

# 3. GKE 자격 증명 획득
gcloud container clusters get-credentials ${CLUSTER_NAME} --region=${REGION}

# 4. 배포 적용 (k8s manifest 환경변수 치환)
envsubst < k8s/ingestion-deployment.yaml | kubectl apply -f -

echo "Deployment submitted! Checking rollout status..."
kubectl rollout status deployment/ingestion-service