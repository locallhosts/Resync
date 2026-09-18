# SOAR Console (React + TypeScript)

Week 5's deliverable: a case queue and per-case event-replay timeline,
reading from the Go `cmd/api` read API. See the repo root README for
how this fits into the full stack.

## Design

Dark operational console, built for scanning a queue of active cases
quickly rather than for marketing appeal — severity and outcome color
(red/amber/teal) are the only decoration, and they carry real meaning
(failed action, pending, resolved). IBM Plex Sans for UI text, IBM Plex
Mono for case IDs, timestamps, and event types — anything an analyst
might copy-paste or grep for.

## Running it

```bash
npm install
cp .env.example .env   # point at your API, if not localhost:8080
npm run dev
```

Requires the `api` service (see repo root `docker-compose.yml`, or
`go run ./go/cmd/api`) to be running, and at least one case seeded via
`go run ./go/cmd/seed` from the repo root.

## Structure

- `src/api.ts` — fetch wrapper around `GET /api/cases` and
  `GET /api/cases/{id}/events`
- `src/components/CaseQueue.tsx` — left-pane list, polled every 3s
- `src/components/CaseTimeline.tsx` — right-pane event replay for the
  selected case, expandable payloads
- `src/App.tsx` — polling/selection state, ties the two panes together

## Build

```bash
npm run build   # outputs to dist/, verified locally: tsc -b && vite build
```
