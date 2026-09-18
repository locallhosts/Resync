// Command sandbox-worker is the "isolated execution sandbox" from the
// architecture doc. Each replica:
//  1. consumes one ActionCommanded message from the command topic
//  2. takes a per-case Redis lock (skips if another worker already has it)
//  3. runs the named action against its mock/real integration
//  4. appends an ActionSucceeded or ActionFailed event to the Postgres
//     event log (this is the durable record; Kafka is just transport)
//  5. publishes that event onto the event topic for the workflow engine
//     and the CQRS projector to pick up
//
// It's intentionally a single small binary with no framework — that's
// the point of using Go here: the container image is FROM scratch plus
// this one binary, nothing else runs inside it.
package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/example/soar-engine/internal/actions"
	"github.com/example/soar-engine/internal/events"
	"github.com/example/soar-engine/internal/eventstore"
	"github.com/example/soar-engine/internal/kafkabus"
	"github.com/example/soar-engine/internal/lock"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	brokers := splitEnv("KAFKA_BROKERS", "localhost:9092")
	commandTopic := getEnv("COMMAND_TOPIC", "soar.commands")
	eventTopic := getEnv("EVENT_TOPIC", "soar.events")
	groupID := getEnv("CONSUMER_GROUP", "sandbox-worker")
	pgDSN := getEnv("POSTGRES_DSN", "postgres://soar:soar@localhost:5432/soar?sslmode=disable")
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")

	store, err := eventstore.Open(pgDSN)
	if err != nil {
		log.Fatalf("event store: %v", err)
	}
	defer store.Close()

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	caseLock := lock.New(rdb, 30*time.Second)

	consumer := kafkabus.NewConsumer(brokers, commandTopic, groupID)
	defer consumer.Close()
	producer := kafkabus.NewProducer(brokers, eventTopic)
	defer producer.Close()

	// Registry of actions this worker replica knows how to run. In a
	// real deployment you'd likely split this into per-action-type
	// worker pools so one slow integration can't starve the others;
	// for the portfolio version a single registry is enough to
	// demonstrate the pattern.
	registry := actions.NewRegistry(
		actions.BlockIP{FailFor: toSet(getEnv("BLOCKIP_FAIL_FOR", "203.0.113.77"))},
		actions.DisableAccount{FailFor: toSet(getEnv("DISABLEACCOUNT_FAIL_FOR", ""))},
	)

	log.Printf("sandbox-worker started (group=%s, command_topic=%s)", groupID, commandTopic)

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
			log.Printf("bad command envelope, skipping: %v", err)
			continue
		}
		var cmd events.ActionCommanded
		if err := json.Unmarshal(env.Payload, &cmd); err != nil {
			log.Printf("bad ActionCommanded payload, skipping: %v", err)
			continue
		}

		simulateCrashIfConfigured(cmd)
		handle(ctx, store, producer, caseLock, registry, env, cmd)
	}
}

func handle(
	ctx context.Context,
	store *eventstore.Store,
	producer *kafkabus.Producer,
	caseLock *lock.CaseLock,
	registry actions.Registry,
	env events.Envelope,
	cmd events.ActionCommanded,
) {
	caseID := env.CaseID.String()

	token, ok, err := caseLock.Acquire(ctx, caseID)
	if err != nil {
		log.Printf("case %s: lock error: %v", caseID, err)
		return
	}
	if !ok {
		// Another replica already owns this case's lock. Do not retry
		// in a loop here — the command stays uncommitted-in-spirit
		// only in the sense that this replica walks away; whichever
		// replica holds the lock is responsible for it. If that
		// replica dies, the lock's TTL expires and the case becomes
		// available again.
		log.Printf("case %s: already locked, skipping", caseID)
		return
	}
	defer func() {
		if err := caseLock.Release(ctx, caseID, token); err != nil {
			log.Printf("case %s: lock release: %v", caseID, err)
		}
	}()

	action, ok := registry[cmd.Action]
	if !ok {
		log.Printf("case %s: unknown action %q", caseID, cmd.Action)
		return
	}

	runCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	result, runErr := action.Run(runCtx, cmd.Params)

	var outEnv events.Envelope
	if runErr != nil {
		outEnv, err = events.NewEnvelope(env.CaseID, events.TypeActionFailed, events.ActionFailed{
			CommandID: cmd.CommandID,
			Action:    cmd.Action,
			Error:     runErr.Error(),
			Retryable: true,
		})
	} else {
		outEnv, err = events.NewEnvelope(env.CaseID, events.TypeActionSucceeded, events.ActionSucceeded{
			CommandID: cmd.CommandID,
			Action:    cmd.Action,
			Result:    result,
		})
	}
	if err != nil {
		log.Printf("case %s: build result envelope: %v", caseID, err)
		return
	}

	stored, err := store.Append(ctx, outEnv)
	if err != nil {
		// The event never made it into the durable log. We deliberately
		// do NOT publish to Kafka in this case: the log is the source
		// of truth, and a case that's stuck here is exactly what Week 4's
		// replay/recovery demo is for — on restart, LoadFrom picks up
		// wherever the log actually got to.
		log.Printf("case %s: append event: %v", caseID, err)
		return
	}

	if err := producer.Publish(ctx, caseID, stored); err != nil {
		// The event IS durable at this point (it's in Postgres); this
		// is "only" a delivery failure to the workflow engine, which
		// will notice via its own polling/replay rather than losing
		// the outcome entirely.
		log.Printf("case %s: publish event: %v", caseID, err)
	}

	log.Printf("case %s: action=%s outcome=%s seq=%d", caseID, cmd.Action, stored.Type, stored.Seq)
}

// simulateCrashIfConfigured is a demo-only hook for the Week 4 recovery
// story: if CRASH_SIM_VALUE is set and appears anywhere in the
// command's params (e.g. an IP or user ID), the process exits
// immediately — AFTER Kafka has already committed the read (see
// kafkabus.Consumer.Fetch's doc comment) but BEFORE the action runs or
// any event is appended.
//
// That's precisely the failure mode com.example.soar.engine.RecoveryRunner
// in the Java workflow engine exists to recover from: Kafka will never
// redeliver this message, so the ONLY way the case makes progress again
// is RecoveryRunner noticing, on the workflow engine's NEXT startup,
// that this case's most recent event is an ActionCommanded with no
// outcome, and re-publishing it. With docker-compose's
// `restart: unless-stopped`, both this container and the workflow
// engine restart automatically — watch the workflow-engine logs after
// the crash for the "recovery: found N in-flight case(s)" line.
//
// This only fires once per matching value per process lifetime (a real
// crash doesn't repeat forever) — set CRASH_SIM_VALUE to a value you'll
// only ever use in one demo command.
func simulateCrashIfConfigured(cmd events.ActionCommanded) {
	target := os.Getenv("CRASH_SIM_VALUE")
	if target == "" {
		return
	}
	// Marker file so that when docker-compose's `restart: unless-stopped`
	// brings this same container back up, it doesn't see the same
	// CRASH_SIM_VALUE in the reconciler's re-published command and
	// crash-loop forever — the whole point is to crash exactly ONCE.
	const marker = "/tmp/.crash-sim-fired"
	if _, err := os.Stat(marker); err == nil {
		return
	}
	if strings.Contains(string(cmd.Params), target) {
		log.Printf("CRASH_SIM_VALUE=%q matched command %s params — simulating a hard crash now", target, cmd.CommandID)
		_ = os.WriteFile(marker, []byte("fired"), 0o644)
		os.Exit(1)
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func splitEnv(key, def string) []string {
	v := getEnv(key, def)
	return strings.Split(v, ",")
}

// toSet turns "a,b,c" into a lookup set, skipping empty entries so an
// unset/empty env var produces an empty (never-fails) set rather than
// a set containing "".
func toSet(csv string) map[string]bool {
	out := map[string]bool{}
	for _, s := range strings.Split(csv, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out[s] = true
		}
	}
	return out
}
