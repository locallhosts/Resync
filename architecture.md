# Architecture — Distributed SOAR Engine with Event Sourcing

This is the design this repo was built from. See the root `README.md`
for what's actually implemented and how to run it; this document is the
"why," not the "how to run it."

## Diagram

```
[Alert Ingest] --> [Kafka: soar.events topic]
                          │
                          v
              ┌───────────────────────┐
              │ Workflow Engine        │
              │ (Java, plain JDK +      │
              │  kafka-clients +         │
              │  postgresql-jdbc)          │
              │ - Event-sourced state       │
              │   machine per case            │
              │ - Emits Commands                │
              └──────────┬─────────────────────┘
                          │ commands (e.g. "BlockIP")
                          v
              ┌───────────────────────┐
              │ Kafka: soar.commands   │
              └──────────┬─────────────┘
                          v
              ┌───────────────────────┐
              │ Go Execution            │
              │ Sandbox (isolated,       │
              │ distroless containers)    │
              │ - Executes action           │
              │ - Emits Event (success/       │
              │   failure) back to Kafka        │
              └──────────┬─────────────────────┘
                          v
              ┌───────────────────────┐
              │ Event Store (Postgres  │
              │ append-only event log) │
              │ + Redis (distributed   │
              │ locking, dedup)         │
              └──────────┬─────────────┘
                          v
              ┌───────────────────────┐
              │ CQRS Read Model         │
              │ (materialized view for  │
              │ fast case queries)       │
              └──────────┬─────────────┘
                          v
              ┌───────────────────────┐
              │ TypeScript React UI     │
              │ - case timeline (event   │
              │   replay visualization)  │
              └───────────────────────┘
```

## Components

- **Event sourcing core**: every state change to a case (alert
  received, action commanded, action succeeded/failed, case closed) is
  an immutable event appended to a Postgres event log. Current state is
  derived by replaying events, not stored directly. Implemented
  identically on both sides of the language boundary — `go/internal/eventstore`
  and `workflow-engine/.../eventstore/EventStore.java` — against the
  same table, same guarantees (per-case gapless sequencing via
  `SELECT ... FOR UPDATE`).
- **CQRS split**: writes go through the event-sourced workflow engine
  and the sandbox worker; reads go through `case_read_model`, a
  materialized table kept in sync by `go/cmd/projector`, updated
  asynchronously as events are appended. The UI never replays a case's
  full history just to render a list.
- **Recovery/replay**: if a playbook fails halfway (blocked IP, failed
  to disable account), the system resumes from the last successful
  step on the workflow engine's next startup — `RecoveryRunner` finds
  every case whose most recent event is an unresolved `ActionCommanded`
  and re-publishes exactly that command. This is the single most
  important thing to demo — see `scripts/demo-recovery.sh`.
- **Isolated execution sandbox**: the Go workers that actually perform
  actions run in short-lived, distroless containers (`gcr.io/distroless/static-debian12:nonroot`)
  with no shell, no interpreter, and access to nothing except the
  specific API they need. This shows you thought about the security of
  your security tool.
- **Distributed locking**: Redis locks (`go/internal/lock`) prevent two
  workers from acting on the same case simultaneously.

## Why three languages

- **Go** for the sandbox worker: single static binary, minimal
  container image, fast cold starts, goroutines make concurrent actions
  trivial. Exactly the shape you want for something that starts and
  stops constantly and should have the smallest possible attack
  surface.
- **Java** for the workflow engine: long-running, stateful process
  where JVM maturity (GC tuning, observability tooling, a huge
  ecosystem if this ever grew into a real rules engine) is a genuine
  advantage. The opposite shape from the sandbox worker — which is the
  point of using a different language rather than "Go everywhere."
- **TypeScript/React** for the console: the one place a UI framework
  earns its keep.

Being able to justify *why* a given language fits a given component,
rather than picking one and using it everywhere, is itself a signal
worth calling out in an interview.

## Prerequisites this project assumes

- Event sourcing and CQRS patterns (Martin Fowler's writeups are a good
  starting point).
- Kafka producer/consumer semantics, consumer groups, at-least-once
  delivery and why idempotent handling downstream matters more than
  exactly-once transport guarantees.
- Basic Go and basic modern Java (records, switch expressions, text
  blocks — see `CaseStateMachine.java` and `Json.java`).

## Portfolio framing notes

- Lead a README or demo video with the **replay/recovery demo** — it's
  the single thing that separates "I built a CRUD app" from "I built a
  resilient distributed system," and it's the part hiring managers
  remember.
- Be ready to explain the language choices above — it's a stronger
  signal of engineering maturity than picking one stack and using it
  everywhere.
