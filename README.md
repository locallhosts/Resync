# Resync

**Resync is a distributed Security Orchestration, Automation and Response (SOAR) platform built around event sourcing, CQRS, Kafka, PostgreSQL, Redis, Go, Java, and React.**

The project is designed to model a security-response workflow from **alert ingestion → decision → action command → isolated execution → durable event → read-model projection → analyst UI**, with recovery and concurrency behavior built into the architecture.

This repository is intentionally focused on the engineering behind a resilient SOAR workflow rather than on integrating a large number of third-party security products.

---

## What Resync Does

A security alert enters the system as an immutable event.

For a high-severity alert containing an IPv4 address, the workflow engine can:

1. Record the alert in the PostgreSQL event store.
2. Publish the alert to Kafka.
3. Reconstruct the case state from its event history.
4. Apply the workflow decision logic.
5. Create an `ActionCommanded` event for `BlockIP`.
6. Publish the command to Kafka.
7. Let a Go sandbox worker acquire a Redis lock for the case.
8. Execute the configured action.
9. Append either `ActionSucceeded` or `ActionFailed` to PostgreSQL.
10. Publish the result back to Kafka.
11. Project the event into the CQRS read model.
12. Display the case and complete event timeline in the React console.

The important design property is that **the event log is the durable source of truth**. The UI read model is derived from it, and workflow state can be reconstructed by replaying the event history.

---

## Architecture

At a high level:

```text
                         SECURITY ALERT
                               |
                               v
                       +----------------+
                       |    Seed /      |
                       | Alert Ingest   |
                       +-------+--------+
                               |
                               | AlertReceived
                               v
                    +----------------------+
                    |        Kafka         |
                    |     soar.events      |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |   Java Workflow      |
                    |       Engine         |
                    |                      |
                    | - Event replay       |
                    | - State machine      |
                    | - Decision logic     |
                    | - Recovery           |
                    +----------+-----------+
                               |
                               | ActionCommanded
                               v
                    +----------------------+
                    |        Kafka         |
                    |    soar.commands     |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |   Go Sandbox Worker  |
                    |                      |
                    | - Kafka consumer     |
                    | - Redis case lock    |
                    | - Action registry   |
                    | - Action execution   |
                    +----------+-----------+
                               |
                               | ActionSucceeded /
                               | ActionFailed
                               v
                    +----------------------+
                    |     PostgreSQL       |
                    |   Event Store        |
                    +----------+-----------+
                               |
                               | events
                               v
                    +----------------------+
                    |   Go Projector       |
                    |      CQRS Read       |
                    |       Model          |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |      Go API          |
                    |    Read-only HTTP    |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |    React / TS UI     |
                    |   Case Queue +       |
                    |   Event Timeline     |
                    +----------------------+
```

Supporting the workflow are:

- **Redis** for per-case distributed locking.
- **Docker Compose** for the complete local environment.
- **RecoveryRunner** in the Java workflow engine for unresolved in-flight commands.
- **Demo scripts** for crash/recovery and concurrent-load testing.

---

# Core Design

## 1. Event Sourcing

Resync does not maintain a mutable case object as the primary source of truth.

Instead, state changes are represented as immutable events:

- `AlertReceived`
- `EnrichmentDone`
- `ActionCommanded`
- `ActionSucceeded`
- `ActionFailed`
- `CaseClosed`

Each event contains:

- event ID
- case ID
- per-case sequence number
- event type
- JSON payload
- timestamp

Example conceptual history:

```text
seq 1  AlertReceived
seq 2  ActionCommanded
seq 3  ActionSucceeded
seq 4  CaseClosed
```

The current case state is reconstructed by folding these events through the Java `CaseStateMachine`.

This makes the event history useful for:

- recovery
- auditing
- debugging
- replay
- timeline visualization
- rebuilding read models

### Per-case ordering

The event store assigns sequence numbers transactionally.

For a case, the next sequence number is calculated inside a PostgreSQL transaction using row locking:

```text
SELECT COALESCE(MAX(seq), 0) + 1
FROM events
WHERE case_id = ?
FOR UPDATE
```

The database also enforces uniqueness on the case/sequence pair.

Different cases can be processed concurrently while events belonging to the same case remain ordered.

---

## 2. CQRS

The project separates the write/event side from the read side.

### Write side

The durable event history is stored in PostgreSQL.

### Read side

The Go projector consumes Kafka events and maintains:

```text
case_read_model
```

The React console queries this read model through the Go API rather than replaying every case whenever the queue is displayed.

This gives the system two useful properties:

- the event log remains the source of truth;
- the read model can be rebuilt if necessary.

The projector is intentionally disposable. If the read model is lost, it can be reconstructed from the event stream.

---

# Workflow Engine

The workflow engine is implemented in **Java** using a deliberately small dependency set rather than a large application framework.

Important classes include:

```text
workflow-engine/
└── src/main/java/com/example/soar/
    ├── WorkflowEngine.java
    ├── Json.java
    ├── engine/
    │   ├── CaseState.java
    │   ├── CaseStateMachine.java
    │   ├── Decision.java
    │   └── RecoveryRunner.java
    ├── eventstore/
    │   └── EventStore.java
    ├── events/
    │   ├── Envelope.java
    │   └── EventType.java
    └── kafka/
        └── EventBus.java
```

## State machine

`CaseStateMachine.fold()` deterministically reconstructs a case from its events.

The decision function then evaluates the resulting state.

The current portfolio playbook is intentionally small:

- only **high-severity** alerts are automatically acted upon;
- an IPv4 address is extracted from the alert text;
- the workflow issues a `BlockIP` command;
- successful actions close the case;
- failed actions can be retried;
- after the retry limit is exceeded, the case is closed as unresolved.

The workflow is intentionally simple so that the distributed-system behavior remains easy to inspect.

---

# Recovery and Replay

Recovery is one of the main features of Resync.

A difficult failure case for an event-driven security platform is:

```text
Workflow engine
      |
      | ActionCommanded
      v
Kafka
      |
      v
Sandbox worker
      |
      X  PROCESS CRASH
```

If the worker consumes and commits the Kafka message before completing the action, Kafka may not deliver that message again.

Resync therefore does not depend on Kafka redelivery to recover the workflow.

Instead, the Java `RecoveryRunner` checks PostgreSQL for cases where the **latest durable event is an `ActionCommanded` with no recorded outcome**.

It then:

1. Loads the complete event history.
2. Replays the history through the state machine.
3. Identifies the command that was in flight.
4. Republishes the existing command.
5. Does **not** append a second `ActionCommanded` event.

Conceptually:

```text
Before crash:

1 AlertReceived
2 ActionCommanded
3 <missing outcome>

Restart

RecoveryRunner
      |
      +--> replay case history
      |
      +--> find unresolved command
      |
      +--> republish existing command
      |
      v
Sandbox worker
      |
      v
3 ActionSucceeded
```

The important distinction is that recovery **redelivers the existing command** instead of making the workflow start over from the original alert.

Run the automated recovery demonstration with:

```bash
./scripts/demo-recovery.sh
```

The script deliberately crashes a worker for a specific test IP, restarts the workflow engine, and checks the resulting event sequence.

Expected sequence:

```text
1 AlertReceived
2 ActionCommanded
3 ActionSucceeded
```

---

# Sandbox Worker

The execution layer is written in **Go**.

The worker:

1. consumes `ActionCommanded` events from Kafka;
2. obtains a Redis lock for the case;
3. looks up the requested action in an action registry;
4. executes the action with a timeout;
5. appends the result to PostgreSQL;
6. publishes the durable result event to Kafka.

The action abstraction is:

```go
type Action interface {
    Name() string
    Run(ctx context.Context, params json.RawMessage) (json.RawMessage, error)
}
```

Current demo integrations include:

- `BlockIP`
- `DisableAccount`

They are deterministic mock integrations intended for local demonstrations and testing.

The architecture is designed so additional integrations can be added as separate actions rather than expanding one large worker switch statement.

---

# Redis Distributed Locking

Multiple sandbox workers can run simultaneously.

A per-case Redis lock prevents two workers from executing actions for the same case at the same time.

The lock uses:

```text
SET NX PX
```

with a random ownership token.

Release uses a Redis Lua script that deletes the key only when the token still belongs to the releasing worker.

This protects against a worker releasing a lock after its TTL has expired and another worker has acquired the same case lock.

The current implementation intentionally uses a single Redis instance and a minimal locking design. It is not presented as a multi-node Redlock implementation.

---

# Kafka

Kafka provides asynchronous communication between the services.

Two main topics are used:

```text
soar.events
soar.commands
```

Messages are keyed by the case ID.

That means events for the same case are routed consistently to the same Kafka partition, preserving per-case ordering within the stream.

The Go side uses `segmentio/kafka-go`.

The Java side uses the official Kafka client.

The project deliberately keeps Kafka access behind small wrapper classes so the rest of the application is not tightly coupled to client-library types.

---

# PostgreSQL Event Store

PostgreSQL stores the append-only event history.

The schema is initialized from:

```text
go/db/migrations/0001_init_events.sql
```

The Go event store and Java event store use the same database table and sequencing model.

This is important because the system has two language boundaries but only one durable event history.

The Go event store provides:

- append
- full replay
- replay from a sequence number

The Java event store additionally queries unresolved in-flight cases for recovery.

---

# Go Read API

The API is intentionally read-only.

Endpoints:

```text
GET /healthz
GET /api/cases
GET /api/cases/{case-id}/events
```

### `GET /api/cases`

Reads from `case_read_model`.

It returns fields such as:

- case ID
- status
- severity
- last event sequence
- last action
- last action status
- update time

### `GET /api/cases/{case-id}/events`

Loads the complete event history for the selected case.

This endpoint is used by the UI to display the event-sourced timeline.

---

# React Security Console

The frontend is a small **React + TypeScript** application.

It provides two primary views:

### Case queue

Displays current cases from the CQRS read model.

### Case timeline

Displays the selected case's event history in sequence order.

The UI polls the case queue every three seconds so changes to the read model appear without a page reload.

Individual event payloads can be expanded to inspect the JSON associated with an event.

The frontend is intentionally lightweight:

```text
ui/src/
├── App.tsx
├── api.ts
├── types.ts
└── components/
    ├── CaseQueue.tsx
    └── CaseTimeline.tsx
```

---

# Repository Structure

```text
Resync/
├── go/
│   ├── cmd/
│   │   ├── api/
│   │   ├── projector/
│   │   ├── sandbox-worker/
│   │   └── seed/
│   ├── db/
│   │   └── migrations/
│   ├── internal/
│   │   ├── actions/
│   │   ├── events/
│   │   ├── eventstore/
│   │   ├── kafkabus/
│   │   └── lock/
│   ├── go.mod
│   └── go.sum
│
├── workflow-engine/
│   ├── src/
│   │   ├── main/
│   │   └── test/
│   ├── pom.xml
│   ├── Dockerfile
│   └── README.md
│
├── ui/
│   ├── src/
│   ├── public/
│   ├── package.json
│   ├── Dockerfile
│   └── vite.config.ts
│
├── scripts/
│   ├── demo-recovery.sh
│   └── load-test.sh
│
├── architecture.md
├── docker-compose.yml
├── LICENSE
└── README.md
```

---

# Technology Stack

| Area | Technology |
|---|---|
| Workflow engine | Java |
| Execution workers | Go |
| Read API | Go |
| CQRS projector | Go |
| Frontend | React + TypeScript |
| Event transport | Apache Kafka |
| Event store | PostgreSQL |
| Distributed lock | Redis |
| Local orchestration | Docker Compose |
| Frontend tooling | Vite, TypeScript, oxlint |
| Java build | Maven |

---

# Running Locally

## Requirements

Install:

- Docker
- Docker Compose
- Go
- Java/JDK
- Maven
- Node.js and npm

The easiest way to run the complete stack is Docker Compose.

## Start the platform

From the repository root:

```bash
docker compose up --build
```

The compose stack starts:

```text
Kafka
PostgreSQL
Redis
Java workflow engine
Go sandbox worker
Go projector
Go API
React UI
```

## Open the console

The React console is exposed on:

```text
http://localhost:5173
```

The read API is exposed on:

```text
http://localhost:8080
```

---

# Creating a Test Case

The seed command creates a case and sends an alert through the normal event-driven path.

From the `go` directory:

```bash
cd go
go run ./cmd/seed
```

The default alert is high severity and contains an IP address, so the workflow engine should decide to issue `BlockIP`.

For a low-severity alert:

```bash
go run ./cmd/seed -severity low
```

The current playbook does not automatically execute the action for low severity.

To force the deterministic `BlockIP` failure case:

```bash
go run ./cmd/seed -fail
```

The default failing test IP is:

```text
203.0.113.77
```

To provide your own test IP:

```bash
go run ./cmd/seed -ip 203.0.113.10
```

There is also a direct mode that bypasses the Java workflow engine and sends a `BlockIP` command directly to the sandbox worker:

```bash
go run ./cmd/seed -direct
```

This is useful when testing the Go execution side independently.

---

# Recovery Demonstration

The recovery demo is the main end-to-end failure-injection demonstration.

From the repository root:

```bash
./scripts/demo-recovery.sh
```

The script:

1. Builds and starts the complete stack.
2. Arms the sandbox worker with a crash trigger.
3. Creates a high-severity case.
4. Lets the workflow engine issue `BlockIP`.
5. Crashes the worker before the action result is recorded.
6. Restarts the workflow engine.
7. Lets `RecoveryRunner` find the unresolved command.
8. Republishes the original command.
9. Verifies the resulting PostgreSQL event sequence.

This demonstrates that the recovery path is based on the durable event history rather than relying on Kafka to redeliver the consumed command.

---

# Load Test

The repository includes a concurrent-load script:

```bash
./scripts/load-test.sh
```

Defaults:

```text
200 cases
4 sandbox-worker replicas
```

You can specify both values:

```bash
./scripts/load-test.sh 500 8
```

The test:

1. Starts the stack.
2. Scales the sandbox worker.
3. Creates many cases concurrently.
4. Waits for the CQRS read model to drain.
5. Checks event sequence integrity.
6. Prints a result summary.

The integrity query verifies that every case has contiguous sequence numbers:

```text
1, 2, 3, ...
```

This exercises several pieces of the design simultaneously:

- Kafka consumer groups
- multiple worker replicas
- Redis case locks
- PostgreSQL event sequencing
- CQRS projection
- concurrent alert processing

---

# Development

## Go

Build or run an individual service from `go/`:

```bash
cd go
go run ./cmd/api
```

Other commands follow the same pattern:

```bash
go run ./cmd/projector
go run ./cmd/sandbox-worker
go run ./cmd/seed
```

## Java

From `workflow-engine/`:

```bash
mvn test
```

The project also contains a manual state-machine test under:

```text
src/test/java/com/example/soar/engine/
```

## Frontend

From `ui/`:

```bash
npm install
npm run dev
```

Build:

```bash
npm run build
```

Lint:

```bash
npm run lint
```

---

# Failure Injection

The platform includes deterministic failure controls so important behavior can be reproduced without depending on a real firewall or identity provider.

The sandbox worker supports environment variables such as:

```text
BLOCKIP_FAIL_FOR
DISABLEACCOUNT_FAIL_FOR
CRASH_SIM_VALUE
```

For example, `BLOCKIP_FAIL_FOR` can make the mock firewall action fail for a specific IP.

`CRASH_SIM_VALUE` is used by the recovery demonstration to deliberately terminate the worker when a matching command is received.

These controls exist for testing and demonstration rather than as production integrations.

---

# Security Engineering Concepts Demonstrated

Resync brings several security-engineering and distributed-systems concepts together:

- SOAR workflow orchestration
- event sourcing
- CQRS
- immutable audit history
- asynchronous event processing
- Kafka consumer groups
- per-case message ordering
- distributed locking
- idempotent action design
- failure injection
- crash recovery
- workflow replay
- isolated execution workers
- containerized services
- read-only API boundaries
- materialized read models
- concurrent processing
- deterministic testing

The execution sandbox is also intentionally kept small. The worker is built as a static Go binary and its Docker image is designed around a minimal runtime rather than a general-purpose application environment.

---

# Design Trade-offs

This repository is a portfolio-scale implementation, not a claim that every component is production hardened.

Some deliberate simplifications include:

- Kafka is configured as a single local broker in Docker Compose.
- Redis is a single instance.
- Mock actions stand in for real security-product integrations.
- The workflow playbook currently contains one primary automated response path.
- The UI uses polling rather than a websocket/event-stream connection.
- Authentication and authorization are outside the current local demonstration.
- Observability is primarily service logging rather than a full metrics/tracing stack.

These choices keep the core architecture visible and reproducible while leaving clear extension points for a larger deployment.

---

# Architecture Documentation

The deeper architecture discussion is available in:

```text
architecture.md
```

That document focuses on the reasoning behind the event-sourcing model, CQRS boundary, recovery design, isolated execution model, distributed locking, and language choices.

---

# License

Licensed under the Eclipse Public License 2.0.
