#!/usr/bin/env bash
#
# The API in one run, against the stack of compose.yaml: an operator creates a tenant, the tenant
# registers the example subscriber as an endpoint and sends an event, and the subscriber gets
# the delivery and verifies its signature. Then the subscriber refuses a forged request, and it
# fails one attempt of a second delivery, which succeeds with its retry.
#
# Start the stack first: docker compose up --build --detach --wait
# Needs curl 7.82 or later and jq.

set -euo pipefail
cd "$(dirname "$0")/.."

# Where compose.yaml publishes the two replicas and the subscriber on this machine.
replica_1="http://$(docker compose port replica-1 8080)"
replica_2="http://$(docker compose port replica-2 8080)"
subscriber="http://$(docker compose port subscriber 8082)"

# The admin token that compose.yaml gives the replicas. A development value.
admin_token="admin-token-for-local-development-only"

# One request. An answer that is an error is printed and fails the call.
call() {
  local answer
  if ! answer=$(curl --silent --show-error --fail-with-body "$@"); then
    printf '%s\n' "$answer" >&2
    return 1
  fi
  printf '%s' "$answer"
}

# The tenant sends an event under an idempotency key, which the route requires.
send_event() {
  call "$replica_1/v1/events" \
    --header "Authorization: Bearer $api_key" \
    --header "Idempotency-Key: $1" \
    --json "$2"
}

# The id of the one delivery of an event, read from the other replica.
delivery_of() {
  call "$replica_2/v1/events/$1" --header "Authorization: Bearer $api_key" \
    | jq --raw-output '.deliveries[0].id'
}

# Reads a delivery from the other replica once a second until the jq condition holds for it,
# and prints it. The script ends when the condition does not hold after 30 s.
await_delivery() {
  local delivery deadline=$((SECONDS + 30))
  until
    delivery=$(call "$replica_2/v1/deliveries/$1" --header "Authorization: Bearer $api_key") \
      || exit 1
    jq --exit-status "$2" <<<"$delivery" >/dev/null
  do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "After 30 s this does not hold for the delivery: $2" >&2
      jq . <<<"$delivery" >&2
      exit 1
    fi
    sleep 1
  done
  jq . <<<"$delivery"
}

# A replica takes requests once its readiness probe answers. "docker compose up --wait" does not
# wait for that, because the image of a replica has no shell for a health check.
deadline=$((SECONDS + 60))
until curl --silent --fail --output /dev/null "$replica_1/actuator/health/readiness" \
  && curl --silent --fail --output /dev/null "$replica_2/actuator/health/readiness"; do
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "The replicas are not ready after 60 s." >&2
    exit 1
  fi
  sleep 1
done

echo "1. The operator creates a tenant. Its API key is shown this once."
tenant=$(call "$replica_1/admin/tenants" \
  --header "Authorization: Bearer $admin_token" \
  --json '{"name": "demo"}')
jq '.api_key = "(hidden)"' <<<"$tenant"
api_key=$(jq --raw-output .api_key <<<"$tenant")

echo "2. The tenant registers the subscriber as an endpoint. Its signing secret is shown this once."
endpoint=$(call "$replica_1/v1/endpoints" \
  --header "Authorization: Bearer $api_key" \
  --json '{"url": "http://subscriber:8082/webhooks", "event_types": ["order.created"]}')
jq '.secret = "(hidden)"' <<<"$endpoint"

echo "3. The subscriber is given the secret that it verifies with."
call --request PUT "$subscriber/secret" --data "$(jq --raw-output .secret <<<"$endpoint")"

echo "4. The tenant sends an event."
event=$(send_event order-1001-created \
  '{"type": "order.created", "data": {"order": 1001, "total": "49.90"}}')
jq . <<<"$event"

echo "5. The other replica is asked for the delivery until it has succeeded."
delivery_id=$(delivery_of "$(jq --raw-output .id <<<"$event")")
await_delivery "$delivery_id" '.state == "succeeded"'

echo "6. The subscriber has printed the request. Its webhook-id is the id of the delivery."
docker compose logs --no-log-prefix subscriber | grep "^signature=verified webhook-id=$delivery_id "

echo "7. The subscriber is sent a forged request: that webhook-id, and a signature of no secret."
forged_status=$(curl --silent --show-error --output /dev/null --write-out '%{http_code}' \
  "$subscriber/webhooks" \
  --header "webhook-id: $delivery_id" \
  --header "webhook-timestamp: $(date +%s)" \
  --header "webhook-signature: v1,Zm9yZ2Vk" \
  --json '{"type": "order.created", "data": {"order": 1001, "total": "0.00"}}')
echo "The subscriber answered $forged_status."
[ "$forged_status" = 401 ]

echo "8. The subscriber is told to answer 503, and the tenant sends a second event."
call --request PUT "$subscriber/status" --data 503
event=$(send_event order-1002-created \
  '{"type": "order.created", "data": {"order": 1002, "total": "12.00"}}')
jq . <<<"$event"

echo "9. The first attempt of its delivery fails, and the delivery waits for its retry."
delivery_id=$(delivery_of "$(jq --raw-output .id <<<"$event")")
await_delivery "$delivery_id" '.attempt_log[0].http_status == 503'

echo "10. The subscriber answers 200 again, and the retry succeeds 5 to 10 s after the failure."
call --request PUT "$subscriber/status" --data 200
await_delivery "$delivery_id" \
  '.state == "succeeded" and (.attempt_log | map(.http_status) == [503, 200])'

echo "11. The subscriber has printed both requests of that delivery, with the same webhook-id."
requests=$(docker compose logs --no-log-prefix subscriber \
  | grep "^signature=verified webhook-id=$delivery_id ")
echo "$requests"
[ "$(grep --count . <<<"$requests")" = 2 ]
