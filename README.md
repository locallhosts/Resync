# Resync

### Distributed Security Orchestration, Automation and Response

Resync is a distributed SOAR platform built to demonstrate how security alerts can move through an event-driven system, trigger automated response workflows, execute actions through isolated workers, and become queryable through a live security operations console.

The project combines **event sourcing, CQRS, Apache Kafka, PostgreSQL, Redis, Go, Java, React, and TypeScript** into one reproducible local environment.

[![Resync live demo](docs/videos/live-demo.gif)](https://github.com/locallhosts/Resync/blob/main/docs/videos/live-demo.gif)

> End-to-end demo: alert ingestion → workflow decision → action execution → projected case state.

---

## What Resync Does

For a high-severity alert/ containing an IP address, the current demo workflow can issue a `BlockIP` action.

The important design principle is that the **event log is the durable source of truth**. The dashboard is a read model derived from that event history.

---

## Live Demo

The included demo assets show the platform processing real local cases.

### Live platform

[![Resync live demo](docs/videos/live-demo.gif)](https://github.com/locallhosts/Resync/blob/main/docs/videos/live-demo.gif)

### Case view

![Resync case view](docs/screenshots/demo-case.png)

### Alert timeline

![Resync alert timeline](docs/screenshots/case-alert.png)

---

## Architecture

[![Resync Architecture](docs/screenshots/architecture.jpeg)](https://github.com/locallhosts/Resync/blob/main/docs/screenshots/architecture.jpeg)

Resync uses an event-driven architecture in which alerts are persisted as events, processed through Kafka and the Java workflow engine, executed by Go workers, and projected into a CQRS read model for the React console.

Supporting services:

- **PostgreSQL** — durable event store and CQRS read model
- **Kafka** — asynchronous event and command transport
- **Redis** — per-case worker coordination
- **Docker Compose** — local multi-service environment

---

## Event Model

Cases are represented by an ordered event history rather than a single mutable record.

Current event types include:

| Event | Purpose |
|---|---|
| `AlertReceived` | A security alert enters the system |
| `EnrichmentDone` | Alert enrichment completes |
| `ActionCommanded` | A workflow requests an action |
| `ActionSucceeded` | The action completes successfully |
| `ActionFailed` | The action fails |
| `CaseClosed` | The workflow reaches a terminal state |

A successful high-severity case looks like:

```text
seq=1  AlertReceived
seq=2  ActionCommanded
seq=3  ActionSucceeded
```

The event history can then be replayed to reconstruct the case state.

---

## Event Sourcing and Ordering

PostgreSQL stores the append-only event history.

Each event contains a case ID, sequence number, event type, payload, and timestamp.

Per-case sequencing is protected transactionally using PostgreSQL advisory locks, allowing concurrent cases while preserving ordering inside an individual case.

The key invariant is:

```text
Case A:  1 → 2 → 3
Case B:  1 → 2 → 3 → 4
Case C:  1 → 2
```

Different cases can progress concurrently while each case maintains its own ordered history.

A uniqueness constraint on `(case_id, seq)` provides an additional database-level invariant.

---

## CQRS

Resync separates the event/write side from the read side.

### Write side

```text
Alert
  ↓
Event
  ↓
PostgreSQL
```

### Read side

```text
Kafka
  ↓
Go Projector
  ↓
case_read_model
  ↓
Go API
  ↓
React UI
```

The read model is derived from the event stream, keeping dashboard queries simple while preserving the complete event history.

---

## Workflow Engine

The workflow engine is implemented in Java.

It:

- consumes alert events from Kafka
- loads and replays case history
- evaluates workflow decisions
- records `ActionCommanded`
- publishes commands
- detects unresolved in-flight actions
- supports recovery after restart

The current demonstration playbook intentionally remains small so the distributed workflow behavior is easy to inspect.

For a high-severity alert containing an IPv4 address, the workflow can issue:

```text
BlockIP
```

Low-severity alerts do not trigger the same automated response path.

---

## Recovery

Resync includes a recovery path for interrupted action execution.

A worker can be deliberately crashed after consuming a command. When the workflow engine restarts, its recovery runner checks the durable event history for cases whose latest state contains an unresolved `ActionCommanded`.

The recovery flow is:

```text
ActionCommanded
      ↓
Worker crashes
      ↓
Workflow Engine restarts
      ↓
RecoveryRunner finds in-flight case
      ↓
Existing command is republished
      ↓
Worker executes action
      ↓
ActionSucceeded
```

The recovery mechanism is based on durable state rather than relying exclusively on Kafka redelivery.

Run the demonstration with:

```bash
./scripts/demo-recovery.sh
```

Expected successful sequence:

```text
AlertReceived       seq=1
ActionCommanded     seq=2
ActionSucceeded     seq=3
```

---

## Sandbox Worker

The execution layer is written in Go.

The worker:

1. consumes commands from Kafka
2. acquires a per-case Redis lock
3. resolves the requested action
4. executes the action
5. records the result in PostgreSQL
6. publishes the result event

The action interface is intentionally small so additional integrations can be added without turning the worker into one large conditional block.

The current project uses deterministic mock security actions for local testing rather than connecting to a real firewall or identity provider.

---

## Redis Coordination

Multiple sandbox workers can run simultaneously.

Resync uses a per-case Redis lock based on:

```text
SET NX PX
```

with a random ownership token.

The release operation verifies the token before deleting the lock.

This prevents one worker from releasing a lock belonging to another worker after a lock expiration/reacquisition.

The current implementation uses a single Redis instance for the local deployment; it is intentionally not presented as a multi-node Redlock implementation.

---

## Kafka

Kafka provides asynchronous communication between the major services.

Primary topics:

```text
soar.events
soar.commands
```

Messages are keyed by `case_id`, allowing events for the same case to maintain partition-local ordering.

Docker services use:

```text
kafka:19092
```

Host-side clients use:

```text
localhost:9092
```

---

## REST API

The Go API exposes the read side of the system.

### Health

```http
GET /healthz
```

### Cases

```http
GET /api/cases
```

### Case events

```http
GET /api/cases/{caseId}/events
```

Example case state:

```json
{
  "case_id": "55f87ee1-f81e-4e4e-948a-8e283ad9e942",
  "status": "open",
  "severity": "high",
  "last_event_seq": 3,
  "last_action": "BlockIP",
  "last_action_status": "succeeded"
}
```

---

## React Security Console

The frontend is built with React, TypeScript, and Vite.

The console provides:

- live case queue
- severity and status visibility
- selected-case details
- event timeline
- event payload inspection
- automatic case refresh

The UI polls the case queue so new events projected into the read model appear without a page reload.

---

## Running Locally

### Requirements

- Docker
- Docker Compose
- Go
- Java / JDK
- Maven
- Node.js and npm

### Start the stack

From the repository root:

```bash
docker compose up --build
```

Check services:

```bash
docker compose ps
```

The main services are:

```text
Kafka
PostgreSQL
Redis
Workflow Engine
Sandbox Worker
Projector
API
React UI
```

Open the console:

```text
http://localhost:5173
```

API:

```text
http://localhost:8080
```

Health check:

```bash
curl http://localhost:8080/healthz
```

---

## Generating Test Alerts

The repository includes a Go seed command.

For the Docker environment, run it from the repository root with:

```bash
docker run --rm \
  --network resync_default \
  -v "$PWD/go:/app" \
  -w /app \
  golang:1.23 \
  go run ./cmd/seed \
  -postgres-dsn "postgres://soar:soar@postgres:5432/soar?sslmode=disable" \
  -brokers "kafka:19092"
```

A successful run prints a case ID:

```text
CASE_ID=<uuid>
```

### High severity

```bash
go run ./cmd/seed -severity high
```

High-severity alerts containing an IP can trigger the `BlockIP` workflow.

### Low severity

```bash
go run ./cmd/seed -severity low
```

This exercises the no-automatic-action path.

### Simulated failure

```bash
go run ./cmd/seed -fail
```

This exercises the deterministic action-failure path.

### Custom IP

```bash
go run ./cmd/seed -ip 203.0.113.10
```

---

## Testing

### Recovery test

```bash
./scripts/demo-recovery.sh
```

### Concurrent load test

```bash
./scripts/load-test.sh
```

The load test defaults to multiple cases and worker replicas and checks that event sequence integrity is maintained while processing cases concurrently.

---

## Technology Stack/

| Component | Technology |
|---|---|
| Workflow engine | Java |
| API | Go |
| Projector | Go |
| Sandbox worker | Go |
| Frontend | React + TypeScript + Vite |
| Messaging | Apache Kafka |
| Event store | PostgreSQL |
| Coordination | Redis |
| Infrastructure | Docker Compose |

---

## Design Trade-offs

Resync is an engineering-focused local SOAR implementation rather than a claim of production hardening.

Current deliberate simplifications include:

- single Kafka broker in the local Compose environment
- single Redis instance
- deterministic mock security actions
- a small workflow playbook
- polling in the React console instead of WebSockets
- no authentication layer in the local demonstration
- service logging instead of a full metrics/tracing stack

These choices keep the core distributed workflow visible and reproducible while leaving clear extension points for a larger deployment.

---

## What This Project Demonstrates

- Event-driven security automation
- SOAR workflow orchestration
- Event sourcing
- CQRS
- Kafka-based asynchronous processing
- Transactional PostgreSQL event storage
- Per-case event ordering
- Redis-based distributed coordination
- Isolated action workers
- Failure injection
- Workflow recovery
- Durable audit history
- Read-model projection
- REST API design
- React security operations UI
- Dockerized multi-service deployment
- Concurrent worker processing

---

## Status

The core workflow has been tested end-to-end in the local Docker environment.

A real test case was observed progressing through:

```text
AlertReceived
      ↓
ActionCommanded
      ↓
ActionSucceeded
```

with the final read-model state reporting:

```text
last_event_seq: 3
last_action: BlockIP
last_action_status: succeeded
```

---

## License

Licensed under the Eclipse Public License 2.0.
