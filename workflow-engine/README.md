# Workflow Engine (Java) — Week 2 + Week 4

The event-sourced state machine from the architecture doc: consumes
`soar.events`, folds each case's history, decides what should happen
next, and publishes `ActionCommanded` for the Go sandbox worker to act
on. Also owns the recovery/replay feature (Week 4) — see
`RecoveryRunner`.

## Package layout

```
com.example.soar
├── Json.java                    dependency-free JSON (see its javadoc for why)
├── WorkflowEngine.java           main(): recovery, then the consume loop
├── events/
│   ├── EventType.java            mirrors internal/events.Type (Go)
│   └── Envelope.java             mirrors internal/events.Envelope (Go)
├── eventstore/
│   └── EventStore.java           JDBC append/load/loadFrom + findInFlightCases
├── kafka/
│   └── EventBus.java             thin KafkaProducer/KafkaConsumer wrapper
└── engine/
    ├── CaseState.java            what a case's history folds down to
    ├── Decision.java             {none, command, close}
    ├── CaseStateMachine.java     fold() + decide() — the actual playbook logic
    └── RecoveryRunner.java       startup: resume in-flight cases
```

## The playbook, in one paragraph

A `high` severity `AlertReceived` with an IP address in its raw text
triggers a `BlockIP` command. If the sandbox worker reports
`ActionFailed`, the engine retries the *same* action with the *same*
params up to twice more; after that it closes the case with reason
`action_failed_max_retries`. An `ActionSucceeded` closes the case as
`resolved`. That's intentionally the whole playbook — the interesting
part of this project is the event sourcing and recovery around it, not
a rules engine, so `CaseStateMachine.decide` stays small on purpose.

## How this was verified

This module was written in a network-sandboxed environment that
couldn't reach Maven Central, so it couldn't be `mvn package`-built
there. What *was* verified, and you can re-verify identically once you
have normal internet access:

- **`CaseStateMachine` (the actual decision logic)** — compiled with
  plain `javac` (zero dependencies needed) and run against an 18-check
  test harness covering: correct IP extraction and auto-block on high
  severity, no action on low severity, retry-with-same-params on
  failure, giving up after max retries, closing as resolved on success,
  and — the case that matters most — **not re-deciding while a command
  is still outstanding**, which is what keeps recovery from double
  actioning. All 18 checks passed. Re-run it yourself:
  ```bash
  mvn compile
  java -cp target/classes com.example.soar.engine.CaseStateMachineManualTest
  ```
- **`Json`** — compiled and round-trip tested (parse → write → parse,
  compared for equality) against nested objects, arrays, numbers,
  booleans, and null.
- **`EventStore`** — compiled successfully against the real
  `postgresql-42.7.3.jar` (fetched directly from pgjdbc's GitHub
  releases, since Maven Central itself was blocked).
- **`EventBus` and `WorkflowEngine`** — compiled successfully against a
  hand-written stub of the exact `kafka-clients` API surface used here
  (`KafkaProducer`, `KafkaConsumer`, `ProducerRecord`,
  `ConsumerRecords`, the config classes), built from the real
  `org.apache.kafka` public API signatures. This catches real Java
  errors — wrong types, bad imports, typos — but obviously isn't a
  substitute for `mvn package` against the genuine artifact. Do that
  once you have normal internet access:
  ```bash
  mvn -B package
  ```
- **Cross-language wire format** — this compile-only approach also
  caught a real bug, not just syntax errors: `Envelope.toJson()`
  originally serialized `seq` as a Java `double`, producing `"seq":3.0`.
  Go's `encoding/json` refuses to unmarshal a JSON float literal into an
  `int64` field (`json: cannot unmarshal number 3.0 into ... int64`),
  which would have silently broken every message this engine published
  to Kafka the moment the Go sandbox worker tried to read it. Fixed by
  keeping `seq` as a boxed `Long` rather than casting to `double`; the
  fix is verified with a small round-trip test asserting the field never
  contains a decimal point.

## Running it

This service is one part of the full stack. From the repo root:

```bash
docker compose up --build
```

To run it standalone against Kafka/Postgres you already have up:

```bash
mvn -B package
java -jar target/workflow-engine.jar
```

Environment variables (all optional, shown with defaults):

| Variable | Default |
|---|---|
| `KAFKA_BROKERS` | `localhost:9092` |
| `EVENT_TOPIC` | `soar.events` |
| `COMMAND_TOPIC` | `soar.commands` |
| `CONSUMER_GROUP` | `workflow-engine` |
| `POSTGRES_DSN` | `postgres://soar:soar@localhost:5432/soar?sslmode=disable` |

## The recovery/replay demo (Week 4)

This is the headline feature to show in an interview. There's a scripted
version of this at `../scripts/demo-recovery.sh` that automates the
steps below against the docker-compose stack — read it alongside this
section, since the script is literally these steps translated into
`docker compose` commands.

1. Start the full stack: `docker compose up --build` from the repo root.
2. Seed a case: `go run ./go/cmd/seed -fail` — this publishes just an
   `AlertReceived` event and lets the engine decide (see `cmd/seed`'s
   README section in `go/README.md`). The engine will command `BlockIP`
   on its own.
3. The `sandbox-worker` container is configured with `CRASH_SIM_VALUE`
   unset by default (normal operation). To see recovery, restart it
   with a value matching the IP you seeded:
   `docker compose up -d --build -e CRASH_SIM_VALUE=203.0.113.77 sandbox-worker`
   (or set it in `docker-compose.yml` and `docker compose up -d sandbox-worker`).
   The worker will exit the instant it picks up the matching command,
   *before* running the action or appending any event.
4. Restart the workflow engine to trigger `RecoveryRunner`:
   `docker compose restart workflow-engine`. Watch its logs for:
   ```
   recovery: found 1 in-flight case(s) to resume
   recovery: case <id> resuming action=BlockIP (last known seq=N, 0 prior failure(s))
   ```
5. The re-published command lands back on `soar.commands`; the
   (now-restarted, by `restart: unless-stopped`) sandbox worker picks it
   up and this time actually runs it — its crash-once marker file means
   it won't crash again on the same value.
6. Confirm in Postgres that the event log has no gap and no duplicate
   `ActionCommanded` — only one exists at seq N; recovery re-published
   it to Kafka, it did not re-decide or re-append:
   ```bash
   docker exec -it soar-postgres psql -U soar -d soar \
     -c "select seq, type from events where case_id = '<id>' order by seq;"
   ```

## Design note: why Java here and Go for the sandbox worker

This split is deliberate, and worth stating explicitly in a portfolio
write-up: the workflow engine is a long-running, stateful service where
the JVM's maturity (GC tuning, observability tooling, a huge ecosystem
if you ever do want a real rules engine) is a genuine advantage. The
sandbox worker is the opposite shape — short-lived, cold-starts
constantly in a container, wants the smallest possible attack surface —
which is exactly Go's strength. Being able to justify *why* a given
language fits a given component, rather than using one language
everywhere, is itself a signal worth calling out to an interviewer.
