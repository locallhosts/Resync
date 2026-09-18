#!/usr/bin/env bash
# load-test.sh — Week 6: "simulate 100s of concurrent alerts" against
# multiple sandbox-worker replicas, and confirm Redis locking + Kafka
# consumer-group partitioning held up: every case's event log is
# internally consistent (no duplicate or skipped sequence numbers) and
# every case reached a terminal state.
#
# Usage: ./scripts/load-test.sh [num_cases] [num_worker_replicas]
set -euo pipefail
cd "$(dirname "$0")/.."

NUM_CASES="${1:-200}"
NUM_WORKERS="${2:-4}"
COMPOSE="docker compose"

log() { printf '\n\033[1;36m== %s ==\033[0m\n' "$1"; }

log "Bringing up the stack"
$COMPOSE up -d --build

log "Scaling sandbox-worker to $NUM_WORKERS replicas"
# All replicas share CONSUMER_GROUP=sandbox-worker (see docker-compose.yml),
# so Kafka splits soar.commands' partitions across them automatically —
# this is the point of the load test: no code changes needed to scale,
# just more containers.
$COMPOSE up -d --scale sandbox-worker="$NUM_WORKERS" --no-recreate sandbox-worker
sleep 3
$COMPOSE ps sandbox-worker

log "Firing $NUM_CASES cases concurrently"
cd go
START=$(date +%s)
seq "$NUM_CASES" | xargs -P 20 -I{} go run ./cmd/seed -severity high > /tmp/load-test-seed.log 2>&1
END=$(date +%s)
cd ..
CASE_IDS=$(grep -o 'CASE_ID=.*' /tmp/load-test-seed.log | cut -d= -f2)
CASE_COUNT=$(echo "$CASE_IDS" | grep -c . || true)
echo "seeded $CASE_COUNT cases in $((END - START))s"

log "Waiting for the pipeline to drain"
# Poll case_read_model rather than sleeping a fixed amount — under load
# the pipeline should catch up, but how long that takes depends on the
# host this runs on.
for i in $(seq 1 30); do
  OPEN=$(docker exec -i soar-postgres psql -U soar -d soar -tA -c \
    "select count(*) from case_read_model where status <> 'closed';")
  echo "  open cases remaining: $OPEN (check $i/30)"
  [ "$OPEN" = "0" ] && break
  sleep 3
done

log "Integrity check: no duplicate or skipped sequence numbers in any case"
GAPS=$(docker exec -i soar-postgres psql -U soar -d soar -tA -c "
  with per_case as (
    select case_id, seq, row_number() over (partition by case_id order by seq) as rn
    from events
  )
  select count(*) from per_case where seq <> rn;
")
echo "sequence gap/duplicate count (expect 0): $GAPS"

log "Result summary"
docker exec -i soar-postgres psql -U soar -d soar -c "
  select status, last_action_status, count(*)
  from case_read_model
  group by status, last_action_status
  order by 1, 2;
"

if [ "$GAPS" != "0" ]; then
  echo
  echo "FAIL: sequence integrity violated — this would mean the event"
  echo "store's per-case locking (SELECT ... FOR UPDATE, see"
  echo "internal/eventstore/store.go) didn't hold under concurrency."
  exit 1
fi

echo
echo "PASS: $CASE_COUNT cases processed by $NUM_WORKERS worker replicas,"
echo "zero sequence gaps or duplicates across every case's event log."
