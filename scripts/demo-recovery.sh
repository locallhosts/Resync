#!/usr/bin/env bash
# demo-recovery.sh — the headline feature of this project, automated.
#
# Simulates a sandbox worker crashing the instant it picks up a command
# (Kafka has already committed the read, so the message is never
# redelivered — see cmd/sandbox-worker/main.go's simulateCrashIfConfigured
# and internal/kafkabus/consumer.go's doc comment on why that's fine),
# then shows the workflow engine's RecoveryRunner noticing on its next
# startup and re-publishing the exact same command, which then actually
# completes.
#
# Requires: docker compose, and this repo's stack not already using the
# IP this script picks (203.0.113.222 — not in sandbox-worker's default
# BLOCKIP_FAIL_FOR list, so once recovery re-delivers the command it
# succeeds, showing "crash -> recover -> succeed" rather than
# "crash -> recover -> fail again").
set -euo pipefail
cd "$(dirname "$0")/.."

CRASH_IP="203.0.113.222"
COMPOSE="docker compose"

log() { printf '\n\033[1;36m== %s ==\033[0m\n' "$1"; }

log "Step 1/6: bring up the full stack"
$COMPOSE up -d --build
echo "waiting for core services to report healthy..."
for svc in soar-kafka soar-postgres soar-redis; do
  for _ in $(seq 1 30); do
    status=$(docker inspect --format='{{.State.Health.Status}}' "$svc" 2>/dev/null || echo "")
    [ "$status" = "healthy" ] && break
    sleep 2
  done
done
$COMPOSE ps

log "Step 2/6: arm sandbox-worker to crash the instant it sees a command for $CRASH_IP"
# --force-recreate gives it a fresh filesystem, so its crash-once
# marker file (/tmp/.crash-sim-fired) is cleared and it WILL crash on
# the next matching command, exactly once.
CRASH_SIM_VALUE="$CRASH_IP" $COMPOSE up -d --force-recreate --no-deps sandbox-worker
sleep 2

log "Step 3/6: seed a case for $CRASH_IP and let the engine decide"
cd go
SEED_OUTPUT=$(go run ./cmd/seed -ip "$CRASH_IP" -severity high)
cd ..
echo "$SEED_OUTPUT"
CASE_ID=$(echo "$SEED_OUTPUT" | grep -o 'CASE_ID=.*' | cut -d= -f2)
if [ -z "$CASE_ID" ]; then
  echo "could not determine case ID from seed output, aborting" >&2
  exit 1
fi
echo "case id: $CASE_ID"

log "Step 4/6: give the engine time to command BlockIP and the worker time to crash on it"
sleep 6
echo "sandbox-worker logs (expect a crash-sim line, then the container exiting):"
$COMPOSE logs --tail 20 sandbox-worker

log "Step 5/6: restart the workflow engine to trigger RecoveryRunner"
$COMPOSE restart workflow-engine
sleep 5
echo "workflow-engine logs (expect 'recovery: found 1 in-flight case(s) to resume'):"
$COMPOSE logs --tail 30 workflow-engine | grep -i recovery || true

log "Step 6/6: confirm the event log — one ActionCommanded, then a real outcome, no gaps or dupes"
sleep 5
docker exec -i soar-postgres psql -U soar -d soar -c \
  "select seq, type, occurred_at from events where case_id = '$CASE_ID' order by seq;"

echo
echo "Expect: AlertReceived(1), ActionCommanded(2), then ActionSucceeded(3) —"
echo "exactly one ActionCommanded despite the crash, because RecoveryRunner"
echo "re-published the same in-flight command instead of the engine"
echo "re-deciding from scratch."
