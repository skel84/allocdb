#!/bin/sh
set -eu

workspace_root="${ALLOCDB_WORKSPACE_ROOT:-/var/lib/allocdb}"
statefulset_name="${ALLOCDB_STATEFULSET_NAME:-allocdb}"
headless_service="${ALLOCDB_HEADLESS_SERVICE:-allocdb-internal}"
namespace="${ALLOCDB_NAMESPACE:-default}"
cluster_domain="${ALLOCDB_CLUSTER_DOMAIN:-cluster.local}"
resolve_timeout_secs="${ALLOCDB_RESOLVE_TIMEOUT_SECS:-30}"
pod_name="${HOSTNAME:-$(hostname)}"
ordinal="${pod_name##*-}"

case "$ordinal" in
  ''|*[!0-9]*)
    echo "failed to derive StatefulSet ordinal from pod name: $pod_name" >&2
    exit 1
    ;;
esac

replica_id=$((ordinal + 1))
mkdir -p "$workspace_root" /run/allocdb

/usr/local/bin/allocdb-k8s-layout \
  --workspace-root "$workspace_root" \
  --statefulset-name "$statefulset_name" \
  --headless-service "$headless_service" \
  --namespace "$namespace" \
  --cluster-domain "$cluster_domain" \
  --resolve-timeout-secs "$resolve_timeout_secs"

exec /usr/local/bin/allocdb-local-cluster replica-daemon \
  --layout-file "$workspace_root/cluster-layout.txt" \
  --replica-id "$replica_id"
