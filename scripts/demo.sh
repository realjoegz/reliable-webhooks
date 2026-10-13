#!/usr/bin/env bash
#
# The API in one run, against the stack of compose.yaml. scripts/walkthrough.sh is the run, and
# says what it shows.
#
# Start the stack first: docker compose up --build --detach --wait
# Needs curl 7.82 or later and jq.

set -euo pipefail
cd "$(dirname "$0")/.."

# Where compose.yaml publishes the two replicas and the subscriber on this machine. An event is
# sent to the first replica, and its delivery is read from the second.
api_1="http://$(docker compose port replica-1 8080)"
api_2="http://$(docker compose port replica-2 8080)"
subscriber="http://$(docker compose port subscriber 8082)"

subscriber_log() {
  docker compose logs --no-log-prefix subscriber
}

source scripts/walkthrough.sh
