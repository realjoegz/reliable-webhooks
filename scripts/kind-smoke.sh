#!/usr/bin/env bash
#
# The chart on a cluster, from nothing: a kind cluster, the image of this checkout, a PostgreSQL
# and the example subscriber from deploy/kind, and the chart with two replicas. Then the API in
# one run through the Service of the chart, which is scripts/walkthrough.sh, and last a replica
# is deleted while deliveries are pending.
#
# A run begins by deleting the cluster of the run before it, and leaves its own to be looked at:
#
#   kind export kubeconfig --name reliable-webhooks-smoke
#   kind delete cluster --name reliable-webhooks-smoke
#
# Needs docker, kind, kubectl, helm, curl 7.82 or later and jq.

set -euo pipefail
cd "$(dirname "$0")/.."

cluster=reliable-webhooks-smoke
# The image that deploy/kind/values.yaml names.
image=reliable-webhooks:smoke
# The name of the chart. A release of that name gives each of its resources the same name.
release=reliable-webhooks
replicas="app.kubernetes.io/instance=$release"

# A kubeconfig of this run, so that the one of the user is neither read nor changed.
scratch=$(mktemp -d)
export KUBECONFIG="$scratch/kubeconfig"

# What the cluster has to say about a run that failed, if the run got as far as a cluster.
report() {
  local status=$?
  if [ "$status" -ne 0 ] && [ -s "$KUBECONFIG" ]; then
    {
      kubectl get pods --output wide
      kubectl get events --sort-by .lastTimestamp
      kubectl logs --selector "$replicas" --prefix --tail 100
    } >&2 || true
  fi
  rm -rf "$scratch"
}
trap report EXIT

# One field of every replica that is running, in one line.
of_replicas() {
  kubectl get pods --selector "$replicas" --output jsonpath="{.items[*]$1}"
}

# Ends the script if a container of a replica that is running was restarted. A replica takes its
# count with it when it goes, so the count is read before a replica is deleted and at the end.
require_no_restart() {
  local restarts
  restarts=$(of_replicas '.status.containerStatuses[*].restartCount')
  echo "Restarts: $restarts"
  [ "$restarts" = "0 0" ]
}

# What Flyway said in each replica about the migrations when the replica started. A line of the
# output of a replica that is not a log line is passed over.
migration_log() {
  local pod
  for pod in $(kubectl get pods --selector "$replicas" --output name); do
    kubectl logs "$pod" \
      | jq --raw-input --raw-output --arg pod "$pod" \
        'fromjson? | objects | select(.logger_name // "" | endswith(".DbMigrate"))
         | "\($pod): \(.message)"'
  done
}

# Reads each of the five deliveries until it is in a state, for at most a number of seconds, and
# prints it with the statuses that its attempts were answered with.
read_all_until() {
  local delivery
  for delivery in "${deliveries[@]}"; do
    read_until "/v1/deliveries/$delivery" ".state == \"$1\"" "$2" \
      | jq --compact-output '{id, state, attempts: (.attempt_log | map(.http_status))}'
  done
}

echo "The image of this checkout, and a new cluster with that image in its node."
docker build --tag "$image" .
kind delete cluster --name "$cluster"
kind create cluster --name "$cluster" --config deploy/kind/cluster.yaml --wait 120s
kind load docker-image "$image" --name "$cluster"
kubectl config set-context --current --namespace reliable-webhooks >/dev/null

echo "PostgreSQL and the subscriber."
kubectl apply --filename deploy/kind/namespace.yaml --filename deploy/kind/credentials.yaml
kubectl create configmap subscriber-source --from-file examples/subscriber/Subscriber.java
kubectl apply --filename deploy/kind/postgres.yaml --filename deploy/kind/subscriber.yaml
# A replica finds the database by its name and ends when it cannot, so the names of the cluster
# have to resolve before the chart is installed.
kubectl rollout status deployment/coredns --namespace kube-system --timeout 180s
kubectl rollout status deployment/postgres deployment/subscriber --timeout 300s

echo "The chart, with two replicas."
helm install "$release" deploy/helm/reliable-webhooks --values deploy/kind/values.yaml
kubectl rollout status "deployment/$release" --timeout 300s

echo "The two replicas started together on an empty database. One of them applied the" \
  "migration, and the other found it applied."
migrations=$(migration_log)
echo "$migrations"
[ "$(grep --count 'Successfully applied 1 migration' <<<"$migrations")" = 1 ]
[ "$(grep --count 'No migration necessary' <<<"$migrations")" = 1 ]

echo "Each replica has a read-only root filesystem, and nothing is mounted in it but /tmp: it" \
  "has no token for the API of Kubernetes."
read_only=$(of_replicas '.spec.containers[*].securityContext.readOnlyRootFilesystem')
mounted=$(of_replicas '.spec.containers[*].volumeMounts[*].mountPath')
echo "Read-only: $read_only. Mounted: $mounted"
[ "$read_only" = "true true" ]
[ "$mounted" = "/tmp /tmp" ]

echo "The PodDisruptionBudget counts both replicas, so its selector finds them."
kubectl wait "poddisruptionbudget/$release" \
  --for jsonpath='{.status.currentHealthy}'=2 --timeout 60s

# Where the node publishes the Service of the chart and the subscriber on this machine. The
# Service hands a request to either replica, so both addresses of the walk are the Service.
node="$cluster-control-plane"
api_1="http://$(docker port "$node" 30080/tcp)"
api_2=$api_1
subscriber="http://$(docker port "$node" 30082/tcp)"

subscriber_log() {
  kubectl logs deployment/subscriber
}

# Steps 1 to 11. They leave the tenant, its endpoint and the functions that the steps below use.
source scripts/walkthrough.sh

echo "12. The subscriber is told to answer 503, and the tenant sends five events."
call --request PUT "$subscriber/status" --data 503
deliveries=()
for order in 2001 2002 2003 2004 2005; do
  event=$(send_event "order-$order-created" \
    "{\"type\": \"order.created\", \"data\": {\"order\": $order}}")
  deliveries+=("$(delivery_of "$(jq --raw-output .id <<<"$event")")")
done

echo "13. Five failed attempts in a row open the circuit of the endpoint. The five deliveries" \
  "are pending, and nothing is sent for them until a probe is due, 30 s later."
read_until "/v1/endpoints/$(jq --raw-output .id <<<"$endpoint")" '.circuit == "open"' 60
read_all_until pending 0

echo "14. Neither of the two replicas has been restarted since they started together."
require_no_restart

echo "15. The schema history is given the row of a migration that the image does not have, as" \
  "a later release leaves it. Then one of the two replicas is deleted."
kubectl exec deployment/postgres -- psql --username webhooks --dbname webhooks --command "
  INSERT INTO flyway_schema_history
    (installed_rank, version, description, type, script, checksum, installed_by, execution_time,
     success)
  VALUES (2, '2', 'of a later release', 'SQL', 'V2__of_a_later_release.sql', 0, 'webhooks', 0,
          true)"
kubectl delete pod "$(kubectl get pods --selector "$replicas" \
  --output jsonpath='{.items[0].metadata.name}')"

echo "16. The replica is gone, and the five deliveries are still pending. The subscriber answers" \
  "200 again, a probe closes the circuit, and every delivery succeeds."
read_all_until pending 0
call --request PUT "$subscriber/status" --data 200
read_all_until succeeded 180

echo "17. The Deployment has started a replica in place of the deleted one. It is Ready on a" \
  "schema that is newer than its image, as a replica is after a rollback of the image."
kubectl rollout status "deployment/$release" --timeout 300s
migrations=$(migration_log)
echo "$migrations"
[ "$(grep --count 'newer than the latest available migration' <<<"$migrations")" = 1 ]

echo "18. Neither the replica that stayed nor the one that took the place of the deleted one has" \
  "been restarted."
require_no_restart
