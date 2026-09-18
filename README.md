# Resync

A distributed event-driven platform designed for reliable synchronization, recovery workflows, and scalable backend processing.

## Overview

Resync is built around a service-oriented architecture using Go services, event processing, persistence, and a web-based interface. The project focuses on handling state synchronization, recovery flows, and resilient communication between components.

## Architecture

High-level components:

- **Go Backend Services** - Core APIs and processing services.
- **Event System** - Event-driven communication and asynchronous workflows.
- **Projector Service** - Builds derived state from events.
- **Sandbox Worker** - Executes isolated background tasks.
- **Database Layer** - Stores application state and event data.
- **Web UI** - Frontend interface for interacting with the platform.

## Repository Structure

```
.
├── go/                 # Backend services written in Go
├── ui/                 # Frontend application
├── scripts/            # Development and testing scripts
├── docker-compose.yml  # Local service orchestration
├── architecture.md     # Detailed architecture notes
└── README.md
```

## Technology Stack

- Go
- JavaScript / TypeScript frontend
- Docker Compose
- PostgreSQL
- Event-driven architecture
- Distributed service patterns

## Running Locally

### Requirements

- Docker
- Docker Compose
- Go toolchain
- Node.js (for UI development)

### Start Services

```bash
docker compose up --build
```

## Development

Backend:

```bash
cd go
go run ./cmd/api
```

Frontend:

```bash
cd ui
npm install
npm run dev
```

## Goals

Resync explores reliable distributed synchronization patterns including:

- Event sourcing concepts
- Recovery workflows
- Service communication
- Scalable background processing
- Observability and testing

## License

Licensed under the Eclipse Public License 2.0.
