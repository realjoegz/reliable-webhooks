# The API in one run: an operator creates a tenant, the tenant registers the example subscriber as
# an endpoint and sends an event, and the subscriber gets the delivery and verifies its
# signature. Then the subscriber refuses a forged request, and it fails one attempt of a second
# delivery, which succeeds with its retry.
#
# This file is not run. scripts/demo.sh sources it for the stack of compose.yaml and
# scripts/kind-smoke.sh for a cluster, each after it has said where its stack is:
#
#   api_1, api_2     Two addresses of the API. Requests go to the first, and what they made is
#                    read from the second. Where the two are replicas, a delivery is therefore
#                    read from the replica that did not accept its event.
#   subscriber       The address of the example subscriber.
#   subscriber_log   A function that prints what the subscriber has written.
#
# It leaves its caller the functions call, send_event, delivery_of and read_until, the API key of
# the tenant as api_key and the endpoint as endpoint. scripts/kind-smoke.sh goes on with them,
# and numbers its own steps from 12, after the 11 of this file.
#
# In both stacks the replicas reach the subscriber as subscriber:8082.
# Needs curl 7.82 or later and jq.

# The admin token that compose.yaml and deploy/kind/credentials.yaml give the replicas. A
# development value.
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
  call "$api_1/v1/events" \
    --header "Authorization: Bearer $api_key" \
    --header "Idempotency-Key: $1" \
    --json "$2"
}

# The id of the one delivery of an event.
delivery_of() {
  call "$api_2/v1/events/$1" --header "Authorization: Bearer $api_key" \
    | jq --raw-output '.deliveries[0].id'
}

# Reads what the tenant has at a path once a second until the jq condition holds for it, and
# prints it. The script ends when the condition does not hold after 30 s, or after the number of
# seconds that a third argument gives.
read_until() {
  local path=$1 condition=$2 seconds=${3:-30} answer deadline
  deadline=$((SECONDS + seconds))
  until
    answer=$(call "$api_2$path" --header "Authorization: Bearer $api_key") || exit 1
    jq --exit-status "$condition" <<<"$answer" >/dev/null
  do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "After $seconds s this does not hold for $path: $condition" >&2
      jq . <<<"$answer" >&2
      exit 1
    fi
    sleep 1
  done
  jq . <<<"$answer"
}

# A replica takes requests once its readiness probe answers. On the stack of compose.yaml nothing
# has waited for that: "docker compose up --wait" does not, because the image of a replica has
# no shell for a health check.
deadline=$((SECONDS + 60))
until curl --silent --fail --output /dev/null "$api_1/actuator/health/readiness" \
  && curl --silent --fail --output /dev/null "$api_2/actuator/health/readiness"; do
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "The replicas are not ready after 60 s." >&2
    exit 1
  fi
  sleep 1
done

echo "1. The operator creates a tenant. Its API key is shown this once."
tenant=$(call "$api_1/admin/tenants" \
  --header "Authorization: Bearer $admin_token" \
  --json '{"name": "demo"}')
jq '.api_key = "(hidden)"' <<<"$tenant"
api_key=$(jq --raw-output .api_key <<<"$tenant")

echo "2. The tenant registers the subscriber as an endpoint. Its signing secret is shown this once."
endpoint=$(call "$api_1/v1/endpoints" \
  --header "Authorization: Bearer $api_key" \
  --json '{"url": "http://subscriber:8082/webhooks", "event_types": ["order.created"]}')
jq '.secret = "(hidden)"' <<<"$endpoint"

echo "3. The subscriber is given the secret that it verifies with."
call --request PUT "$subscriber/secret" --data "$(jq --raw-output .secret <<<"$endpoint")"

echo "4. The tenant sends an event."
event=$(send_event order-1001-created \
  '{"type": "order.created", "data": {"order": 1001, "total": "49.90"}}')
jq . <<<"$event"

echo "5. The delivery is read until it has succeeded."
delivery_id=$(delivery_of "$(jq --raw-output .id <<<"$event")")
read_until "/v1/deliveries/$delivery_id" '.state == "succeeded"'

echo "6. The subscriber has printed the request. Its webhook-id is the id of the delivery."
subscriber_log | grep "^signature=verified webhook-id=$delivery_id "

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
read_until "/v1/deliveries/$delivery_id" '.attempt_log[0].http_status == 503'

echo "10. The subscriber answers 200 again, and the retry succeeds 5 to 10 s after the failure."
call --request PUT "$subscriber/status" --data 200
read_until "/v1/deliveries/$delivery_id" \
  '.state == "succeeded" and (.attempt_log | map(.http_status) == [503, 200])'

echo "11. The subscriber has printed both requests of that delivery, with the same webhook-id."
requests=$(subscriber_log | grep "^signature=verified webhook-id=$delivery_id ")
echo "$requests"
[ "$(grep --count . <<<"$requests")" = 2 ]
