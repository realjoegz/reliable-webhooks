#!/usr/bin/env bash
#
# What the README says can be opened after scripts/demo.sh, asked of the stack of compose.yaml:
# Grafana has the dashboard and data for every query of it, and Jaeger has the trace of a
# delivery. CI runs this after the demo.
#
# Needs curl 7.82 or later and jq.

set -euo pipefail

# Where compose.yaml publishes Grafana and Jaeger on this machine.
grafana="http://127.0.0.1:3000"
jaeger="http://127.0.0.1:16686"

# Repeats a check once a second until it holds. Prometheus scrapes every 5 s and a replica hands
# its spans on in batches, so neither has the demo at once. After 60 s the script ends with what
# the check last said.
await() {
  local said deadline=$((SECONDS + 60))
  until said=$("$@" 2>&1); do
    if [ "$SECONDS" -ge "$deadline" ]; then
      printf '%s\n' "$said" >&2
      exit 1
    fi
    sleep 1
  done
  printf '%s\n' "$said"
}

# Every query of the dashboard returns a number. The queries are the ones that Grafana holds,
# and Grafana runs each through the data source of its panel over the time range of the
# dashboard, as it does for the page. A quantile of the deliveries is a number only once
# Prometheus has seen a delivery succeed.
dashboard_has_data() {
  local dashboard queries answer without_data
  dashboard=$(curl --silent --show-error --fail-with-body \
    "$grafana/api/dashboards/uid/reliable-webhooks") || { echo "$dashboard"; return 1; }
  queries=$(jq '.dashboard | {
      from: .time.from,
      to: .time.to,
      queries: [.panels[] | .title as $panel | .datasource as $datasource | .targets[]
                | {refId: "\($panel): \(.expr)", datasource: $datasource, expr}]
    }' <<<"$dashboard") || return 1
  answer=$(curl --silent --show-error --fail-with-body "$grafana/api/ds/query" --json "$queries") \
    || { echo "$answer"; return 1; }
  without_data=$(jq --raw-output '.results | to_entries[]
      | select(any(.value.frames[]?; any(.data.values[1][]?; type == "number")) | not)
      | .key' <<<"$answer") || return 1
  if [ -n "$without_data" ]; then
    printf 'Grafana has no data for these queries of the dashboard:\n%s\n' "$without_data"
    return 1
  fi
  jq --raw-output '.results | keys_unsorted[]' <<<"$answer"
}

# Jaeger has a trace of the last hour in which the service made an attempt that succeeded. The
# spans of each such trace are printed: the request that accepted the event, and the attempts.
trace_of_a_delivery() {
  local traces
  traces=$(curl --silent --show-error --fail-with-body --get "$jaeger/api/v3/traces" \
    --data-urlencode "query.service_name=reliable-webhooks" \
    --data-urlencode 'query.attributes={"webhooks.outcome":"success"}' \
    --data-urlencode "query.start_time_min=$(jq --null-input --raw-output 'now - 3600 | todate')" \
    --data-urlencode "query.start_time_max=$(jq --null-input --raw-output 'now | todate')") \
    || { echo "$traces"; return 1; }
  jq --raw-output '.result.resourceSpans[].scopeSpans[].spans[] | "\(.traceId) \(.name)"' \
    <<<"$traces"
}

echo "Grafana has data for every query of the dashboard:"
await dashboard_has_data

echo "Jaeger has the trace of a delivery:"
await trace_of_a_delivery
