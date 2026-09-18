// Package eventstore is the only code in the system allowed to write to
// the events table. It guarantees two things every event-sourced system
// needs: (1) events for a given case are appended in a strict, gapless
// sequence even under concurrent writers, and (2) a case's full history
// (or the tail of it, for recovery) can be replayed back out in order.
package eventstore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/example/soar-engine/internal/events"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

type Store struct {
	db *sql.DB
}

func Open(dsn string) (*Store, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// Append assigns the next sequence number for env.CaseID and writes the
// event inside a transaction. The SELECT ... FOR UPDATE row lock on the
// case's existing events serializes concurrent appenders for the SAME
// case (different cases append fully in parallel), and the UNIQUE
// (case_id, seq) constraint is a second line of defense: if two workers
// somehow race past the lock, one of them gets a constraint violation
// instead of silently overwriting history.
func (s *Store) Append(ctx context.Context, env events.Envelope) (events.Envelope, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return env, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op if Commit succeeds

	var nextSeq int64
	err = tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(seq), 0) + 1 FROM events WHERE case_id = $1 FOR UPDATE`,
		env.CaseID,
	).Scan(&nextSeq)
	if err != nil {
		return env, fmt.Errorf("select next seq: %w", err)
	}
	env.Seq = nextSeq

	_, err = tx.ExecContext(ctx,
		`INSERT INTO events (event_id, case_id, seq, type, payload, occurred_at)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		env.EventID, env.CaseID, env.Seq, env.Type, env.Payload, env.OccurredAt,
	)
	if err != nil {
		return env, fmt.Errorf("insert event: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return env, fmt.Errorf("commit: %w", err)
	}
	return env, nil
}

// Load replays a case's full history in order. This is what the
// workflow engine calls on startup (or after a crash) to reconstruct
// current state — there is no separate "state" table for cases; state is
// always a fold over this slice.
func (s *Store) Load(ctx context.Context, caseID uuid.UUID) ([]events.Envelope, error) {
	return s.LoadFrom(ctx, caseID, 0)
}

// LoadFrom replays events with seq > fromSeq. This is the primitive the
// recovery/replay demo is built on: if a playbook dies after step 3 of 5,
// resuming means loading events from seq=3 onward, deriving "what still
// needs to happen", and re-emitting only the remaining commands — not
// re-running the whole case from scratch and not re-doing already
// completed actions.
func (s *Store) LoadFrom(ctx context.Context, caseID uuid.UUID, fromSeq int64) ([]events.Envelope, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT event_id, case_id, seq, type, payload, occurred_at
		 FROM events WHERE case_id = $1 AND seq > $2 ORDER BY seq ASC`,
		caseID, fromSeq,
	)
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}
	defer rows.Close()

	var out []events.Envelope
	for rows.Next() {
		var e events.Envelope
		if err := rows.Scan(&e.EventID, &e.CaseID, &e.Seq, &e.Type, &e.Payload, &e.OccurredAt); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
