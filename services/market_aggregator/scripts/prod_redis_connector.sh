#!/usr/bin/env bash
set -euo pipefail

# Bridge a private production Redis endpoint to localhost through a temporary
# Kubernetes pod. Keep this process running while local tools use localhost.
REMOTE_REDIS_HOST="${REMOTE_REDIS_HOST:?Set REMOTE_REDIS_HOST to the private Redis IP}"
REMOTE_REDIS_PORT="${REMOTE_REDIS_PORT:-6379}"
LOCAL_REDIS_PORT="${LOCAL_REDIS_PORT:-6380}"
BRIDGE_PORT="${BRIDGE_PORT:-6379}"
BRIDGE_POD="${BRIDGE_POD:-prod-redis-bridge-${USER:-local}}"

cleanup() {
  kubectl delete pod "${BRIDGE_POD}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

kubectl run "${BRIDGE_POD}" \
  --image=alpine/socat:latest \
  --restart=Never \
  --port="${BRIDGE_PORT}" \
  --command -- \
  socat "TCP-LISTEN:${BRIDGE_PORT},fork,reuseaddr" "TCP:${REMOTE_REDIS_HOST}:${REMOTE_REDIS_PORT}"

until [[ "$(kubectl get pod "${BRIDGE_POD}" -o jsonpath='{.status.phase}')" == "Running" ]]; do
  sleep 1
done

echo "Redis bridge ready: redis://127.0.0.1:${LOCAL_REDIS_PORT}/0"
echo "Press Ctrl-C to stop the bridge."
kubectl port-forward "pod/${BRIDGE_POD}" "${LOCAL_REDIS_PORT}:${BRIDGE_PORT}"
