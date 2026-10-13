#!/usr/bin/env bash
#
# One run of a scenario of the load tests, on a stack that the run starts from nothing and
# removes when it ends: PostgreSQL, two replicas, the sink and the load generator, all in Docker
# on one machine.
#
#   loadtest/run.sh ingest | end-to-end | endpoint-ceiling | kill
#
# A run starts the stack, warms it up, runs the scenario with k6, waits until every delivery is
# made and writes loadtest/results/<scenario>-<time>.txt: when, of which commit and on which
# machine it ran, the summary of k6, the queries of loadtest/queries with their answers, what
# the log of the database says about its checkpoints and vacuums, what the meters of the
# replicas counted during the scenario and what the sink received. docs/performance.md
# describes the scenarios and how to read a result.
#
# Needs Docker with Compose 2.24 or later, curl and jq. The stack of compose.yaml has to be down,
# because a run gives its network the address range of that stack, and Docker has one network
# for a range.

set -euo pipefail
cd "$(dirname "$0")/.."

scenario=${1:-}
case "$scenario" in
  ingest | end-to-end | endpoint-ceiling | kill) ;;
  *)
    echo "usage: loadtest/run.sh ingest|end-to-end|endpoint-ceiling|kill" >&2
    exit 2
    ;;
esac

# A project of its own, so that a run does not remove the stack of scripts/demo.sh.
compose() {
  docker compose --project-name reliable-webhooks-load \
    --file compose.yaml --file loadtest/compose.yaml "$@"
}

sql() {
  compose exec -T postgres psql --username webhooks --dbname webhooks --no-psqlrc --quiet "$@"
}

# One run of k6 with a script of this directory. Its summary is what it writes to stdout. It
# ends with 99 when a threshold of the script was crossed, which the summary shows.
run_k6() {
  local status=0
  compose run --rm -T k6 run --quiet --no-color "/loadtest/$1.js" || status=$?
  [ "$status" -eq 0 ] || [ "$status" -eq 99 ]
}

# Repeats a check once a second until it holds, and ends the run when it does not hold after a
# number of seconds.
await() {
  local seconds=$1 deadline
  shift
  deadline=$((SECONDS + seconds))
  until "$@"; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "After $seconds s this does not hold: $*" >&2
      exit 1
    fi
    sleep 1
  done
}

ready() {
  curl --silent --fail --output /dev/null "$1/actuator/health/readiness"
}

no_delivery_is_pending() {
  [ "$(sql --tuples-only --no-align \
    --command "SELECT count(*) FROM delivery WHERE state = 'pending'")" -eq 0 ]
}

events_of_the_scenario_are_at_least() {
  [ "$(sql --tuples-only --no-align --set started="$started" \
    <<<"SELECT count(*) FROM event WHERE created_at >= :'started'")" -ge "$1" ]
}

a_delivery_of_the_scenario_has_succeeded() {
  [ "$(sql --tuples-only --no-align --set started="$started" <<<"SELECT count(*) FROM delivery
    WHERE created_at >= :'started' AND state = 'succeeded'")" -gt 0 ]
}

# Kills the second replica once a third of the events of loadtest/kill.js are in, which sends
# 36,000. The time of the kill on the clock of the database is kept for the queries.
kill_second_replica() {
  await 120 events_of_the_scenario_are_at_least 12000
  compose kill --signal SIGKILL replica-2
  sql --tuples-only --no-align --command "SELECT now()" >"$scratch/killed"
}

machine() {
  case "$(uname -s)" in
    Darwin)
      echo "$(sysctl -n machdep.cpu.brand_string), $(sysctl -n hw.ncpu) cores," \
        "$(($(sysctl -n hw.memsize) / 1073741824)) GiB of memory, macOS $(sw_vers -productVersion)"
      ;;
    *)
      echo "$(lscpu | sed -n 's/^Model name: *//p' | head -n 1), $(nproc) cores," \
        "$(awk '/^MemTotal/ { printf "%.0f", $2 / 1048576 }' /proc/meminfo) GiB of memory," \
        "$(uname -sr)"
      ;;
  esac
}

# The machine that Docker runs the containers on. With Colima that is its virtual machine, with
# the CPUs and the memory that Colima was given.
docker_host() {
  docker info --format '{{json .}}' | jq --raw-output '
    "\(.ServerVersion) on \(.Name): \(.NCPU) CPUs,"
    + " \(.MemTotal / 1073741824 * 10 | round / 10) GiB of memory,"
    + " \(.OperatingSystem), kernel \(.KernelVersion)"'
}

scratch=$(mktemp -d)
killer=

finish() {
  local status=$?
  if [ -n "$killer" ]; then
    kill "$killer" 2>/dev/null || true
  fi
  if [ "$status" -ne 0 ]; then
    compose logs --no-color --tail 40 >&2 || true
  fi
  compose down --volumes --remove-orphans || true
  rm -rf "$scratch"
}
trap finish EXIT

# The image of the replicas is built from these paths. A change to one of them that is not
# committed is not in the commit that the result names.
commit=$(git rev-parse HEAD)
if [ -n "$(git status --porcelain -- Dockerfile pom.xml mvnw .mvn src/main compose.yaml)" ]; then
  commit="$commit, with changes to the service that are not committed"
fi

echo "The stack, from nothing, with the image of this checkout."
compose down --volumes --remove-orphans
compose up --build --detach --wait postgres replica-1 replica-2 sink
api_1="http://$(compose port replica-1 8080)"
api_2="http://$(compose port replica-2 8080)"
sink="http://$(compose port sink 8082)"
await 60 ready "$api_1"
await 60 ready "$api_2"

date=$(date -u +%Y-%m-%dT%H:%M:%SZ)
result="$scratch/result"
{
  echo "Scenario  $scenario"
  echo "Date      $date"
  echo "Commit    $commit"
  echo "Command   loadtest/run.sh $scenario"
  echo "Machine   $(machine)"
  echo "Docker    $(docker_host)"
  echo
  echo "--- PostgreSQL ---"
  echo
  sql --echo-all <loadtest/queries/settings.sql
  echo "-- What a flush of the WAL costs on the volume of the database, by pg_test_fsync."
  compose exec -T postgres pg_test_fsync --secs-per-test 1 \
    --filename /var/lib/postgresql/pg_test_fsync.out | sed -n '/one 8kB write/,/^$/p'
} >"$result"

echo "The warm-up. Then every dead row is vacuumed, and a checkpoint is made."
run_k6 warm-up >/dev/null
sql --command "VACUUM (ANALYZE)" --command "CHECKPOINT"
curl --silent --show-error --fail --request DELETE "$sink/stats"

echo "The scenario."
started=$(sql --tuples-only --no-align --command "SELECT now()")
variables=(--set "started=$started")
wal_segments=$(sql --tuples-only --no-align \
  --command "SELECT count(*) FROM pg_ls_waldir() WHERE name ~ '^[0-9A-F]{24}\$'")
curl --silent --show-error --fail "$api_1/actuator/prometheus" >"$scratch/before-1"
curl --silent --show-error --fail "$api_2/actuator/prometheus" >"$scratch/before-2"
if [ "$scenario" = kill ]; then
  kill_second_replica &
  killer=$!
fi
run_k6 "$scenario" >"$scratch/k6"
if [ "$scenario" = kill ]; then
  wait "$killer"
  killer=
  variables+=(--set "killed=$(cat "$scratch/killed")")
fi

# The backlog of loadtest/endpoint-ceiling.js is drained from the probe on that closes the
# circuit. What a claim costs is read once that drain has begun, and again when it is over.
if [ "$scenario" = endpoint-ceiling ]; then
  await 150 a_delivery_of_the_scenario_has_succeeded
  sql <loadtest/queries/claim-plan.sql >"$scratch/claim-in-the-drain"
fi

echo "Every delivery is made."
await 300 no_delivery_is_pending

{
  echo "--- k6 ---"
  cat "$scratch/k6"
  echo "--- Database ---"
  echo
  if [ "$scenario" = kill ]; then
    container=$(compose ps --all --quiet replica-2)
    echo "replica-2 was killed with SIGKILL at $(cat "$scratch/killed") on the clock of the" \
      "database, and exited with $(docker inspect --format '{{.State.ExitCode}}' "$container")."
    echo
  fi
  cat loadtest/queries/deliveries.sql "loadtest/queries/$scenario.sql" \
    | sql --echo-all "${variables[@]}"
  if [ "$scenario" = endpoint-ceiling ]; then
    echo "--- Claim ---"
    echo
    echo "What loadtest/queries/claim-plan.sql answered once the drain of the backlog had begun:"
    echo
    cat "$scratch/claim-in-the-drain"
    echo "And once the backlog was drained:"
    echo
    sql <loadtest/queries/claim-plan.sql
  fi
  echo "--- PostgreSQL log ---"
  echo
  echo "WAL segments that the database had when the scenario began: $wal_segments"
  echo "Its checkpoints and its vacuums of the delivery table from then on:"
  # The log is in UTC, as the time that the scenario began is.
  compose logs --no-log-prefix postgres \
    | awk -v began="${started:0:23}" 'substr($0, 1, 23) >= began' \
    | grep -E 'checkpoint (starting|complete)|automatic vacuum of table "[a-z.]+\.delivery"' \
    | sed -E 's/; write=.*//' || true
  echo
  # The meters of a replica end with it, so a run that kills one reads none.
  if [ "$scenario" != kill ]; then
    echo "--- Meters ---"
    echo
    curl --silent --show-error --fail "$api_1/actuator/prometheus" >"$scratch/after-1"
    curl --silent --show-error --fail "$api_2/actuator/prometheus" >"$scratch/after-2"
    awk -f loadtest/meters.awk \
      "$scratch/before-1" "$scratch/after-1" "$scratch/before-2" "$scratch/after-2"
    echo
  fi
  echo "--- Sink ---"
  echo
  echo "What the sink received during the scenario, and for how many webhook-ids more than once:"
  curl --silent --show-error --fail "$sink/stats"
} >>"$result"

mkdir -p loadtest/results
kept="loadtest/results/$scenario-${date//[-:]/}.txt"
mv "$result" "$kept"
echo "Written: $kept"
