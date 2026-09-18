# SOAR Engine — Go services

The Go half of the platform: the isolated execution sandbox, the CQRS
projector, and the read API. See the repo root README for how this
fits with the Java workflow engine and the React UI, and for full-stack
run instructions (`docker compose up --build` from the repo root).

## What's here

- **`internal/eventstore`** — append-only Postgres log with per-case
  gapless sequencing, plus `Load`/`LoadFrom` for full or partial
  replay. (Week 1.)
- **`internal/events`** — the event & command schema: `AlertReceived`,
  `ActionCommanded`, `ActionSucceeded`, `ActionFailed`, `CaseClosed`.
  Mirrored field-for-field by the Java workflow engine's `events`
  package so both languages read/write the same `events` table.
- **`cmd/sandbox-worker`** — consumes commands from Kafka, takes a
  per-case Redis lock, runs the action, appends the outcome event,
  republishes it. (Week 3, plus the Redis-locking half of Week 6.) Also
  has a `CRASH_SIM_VALUE` hook for the Week 4 recovery demo — see the
  repo root's `scripts/demo-recovery.sh`.
- **`internal/actions`** — mock `BlockIP` and `DisableAccount`, each
  with a configurable "always fail for this value" hook.
- **`cmd/projector`** + **`db/migrations`** — the CQRS read side:
  consumes the event topic, upserts a flat `case_read_model` table.
  (Week 5 backend half.)
- **`cmd/api`** — read-only HTTP API (`GET /api/cases`,
  `GET /api/cases/{id}/events`) feeding the React UI in `../ui`.
- **`cmd/seed`** — creates a case and either lets the real workflow
  engine decide what to do (default) or bypasses it and commands
  `BlockIP` directly (`-direct`), for testing this half of the stack in
  isolation. See its file header for full usage.

## Why Go for the sandbox worker, not Python

See `cmd/sandbox-worker/Dockerfile`: the final image is
`gcr.io/distroless/static-debian12:nonroot` plus one static binary —
no shell, no interpreter, no package manager inside the container. If
an attacker ever got code execution inside a running action container,
there's nothing there to pivot with.

## Running just this half

```bash
go build ./...
go vet ./...
gofmt -l .   # should print nothing
```

Requires Kafka, Postgres, and Redis reachable — easiest via
`docker compose up kafka postgres redis` from the repo root, then run
any of the `cmd/*` binaries with `POSTGRES_DSN`, `KAFKA_BROKERS`, and
(for sandbox-worker) `REDIS_ADDR` pointed at `localhost`'s mapped ports
(the defaults already assume `localhost`).

## A note on `go.mod`

This module was built inside a network-sandboxed dev environment that
could only reach `github.com`, not `golang.org` or `gopkg.in` directly
(most real machines don't have that restriction). The `replace`
directives near the bottom of `go.mod` route around that by pointing at
the official GitHub mirrors of the same modules — they're harmless to
keep, or delete the block and run `go mod tidy` on a normal connection
to resolve everything against the canonical `golang.org/x/*` paths
instead. Either way you get the same code.
