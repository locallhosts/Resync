// Command projector is the read side of CQRS: it consumes the same
// event topic the workflow engine publishes to, and upserts
// case_read_model so the UI can query fast, flat, current-state rows
// instead of replaying a case's whole event history on every page load.
//
// This process is disposable by design — case_read_model can be
// TRUNCATEd and this binary restarted with StartOffset: FirstOffset (see
// internal/kafkabus.NewConsumer) to rebuild the entire read model from
// scratch. That's the whole point of CQRS: the read model is a cache of
// the truth, never the truth itself.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/example/soar-engine/internal/events"
	"github.com/example/soar-engine/internal/kafkabus"
	_ "github.com/lib/pq"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	brokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")
	eventTopic := getEnv("EVENT_TOPIC", "soar.events")
	groupID := getEnv("CONSUMER_GROUP", "projector")
	pgDSN := getEnv("POSTGRES_DSN", "postgres://soar:soar@localhost:5432/soar?sslmode=disable")

	db, err := sql.Open("postgres", pgDSN)
	if err != nil {
		log.Fatalf("open postgres: %v", err)
	}
	defer db.Close()

	consumer := kafkabus.NewConsumer(brokers, eventTopic, groupID)
	defer consumer.Close()

	log.Printf("projector started (group=%s, event_topic=%s)", groupID, eventTopic)

	for {
		select {
		case <-ctx.Done():
			log.Println("shutting down")
			return
		default:
		}

		msg, err := consumer.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("fetch error: %v", err)
			continue
		}

		var env events.Envelope
		if err := json.Unmarshal(msg.Value, &env); err != nil {
			log.Printf("bad envelope, skipping: %v", err)
			continue
		}

		if err := apply(ctx, db, env); err != nil {
			log.Printf("case %s: apply projection: %v", env.CaseID, err)
		}
	}
}

// apply is intentionally a plain switch, not a generic "merge JSON into
// the row" trick — the read model's columns are a deliberate, reviewable
// projection of the event stream, not a mirror of it.
func apply(ctx context.Context, db *sql.DB, env events.Envelope) error {
	switch env.Type {
	case events.TypeAlertReceived:
		var p events.AlertReceived
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return err
		}
		_, err := db.ExecContext(ctx, `
			INSERT INTO case_read_model (case_id, status, severity, last_event_seq, updated_at)
			VALUES ($1, 'open', $2, $3, now())
			ON CONFLICT (case_id) DO UPDATE SET
				severity = EXCLUDED.severity,
				last_event_seq = EXCLUDED.last_event_seq,
				updated_at = now()`,
			env.CaseID, p.Severity, env.Seq)
		return err

	case events.TypeActionCommanded:
		var p events.ActionCommanded
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return err
		}
		_, err := db.ExecContext(ctx, `
			UPDATE case_read_model SET
				status = 'actioning',
				last_action = $2,
				last_action_status = 'pending',
				last_event_seq = $3,
				updated_at = now()
			WHERE case_id = $1`,
			env.CaseID, p.Action, env.Seq)
		return err

	case events.TypeActionSucceeded:
		var p events.ActionSucceeded
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return err
		}
		_, err := db.ExecContext(ctx, `
			UPDATE case_read_model SET
				last_action = $2,
				last_action_status = 'succeeded',
				last_event_seq = $3,
				updated_at = now()
			WHERE case_id = $1`,
			env.CaseID, p.Action, env.Seq)
		return err

	case events.TypeActionFailed:
		var p events.ActionFailed
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return err
		}
		_, err := db.ExecContext(ctx, `
			UPDATE case_read_model SET
				last_action = $2,
				last_action_status = 'failed',
				last_event_seq = $3,
				updated_at = now()
			WHERE case_id = $1`,
			env.CaseID, p.Action, env.Seq)
		return err

	case events.TypeCaseClosed:
		_, err := db.ExecContext(ctx, `
			UPDATE case_read_model SET
				status = 'closed',
				last_event_seq = $2,
				updated_at = now()
			WHERE case_id = $1`,
			env.CaseID, env.Seq)
		return err

	default:
		// EnrichmentDone and any future event types that don't change
		// the read model's shape are fine to no-op on.
		return nil
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
