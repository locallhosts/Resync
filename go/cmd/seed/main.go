// Command seed creates a new case and gets it moving through the
// pipeline. By default it publishes just an AlertReceived event onto
// soar.events and lets the Java workflow engine decide what to do —
// this is the realistic path and the one the Week 4 recovery demo
// uses. Pass -direct to skip the engine entirely and command BlockIP
// straight at the sandbox worker, which is useful for testing the Go
// side in isolation without the engine running.
//
// Usage:
//
//	go run ./cmd/seed                    # engine decides -> BlockIP, should succeed
//	go run ./cmd/seed -fail              # engine decides -> BlockIP against an IP
//	                                      # the mock action is configured to fail on
//	go run ./cmd/seed -severity low      # engine decides -> no auto action
//	go run ./cmd/seed -direct            # bypass the engine, command the
//	                                      # sandbox worker directly
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/example/soar-engine/internal/actions"
	"github.com/example/soar-engine/internal/events"
	"github.com/example/soar-engine/internal/eventstore"
	"github.com/example/soar-engine/internal/kafkabus"
	"github.com/google/uuid"
)

func main() {
	brokers := flag.String("brokers", "localhost:9092", "comma-separated Kafka brokers")
	pgDSN := flag.String("postgres-dsn", "postgres://soar:soar@localhost:5432/soar?sslmode=disable", "Postgres DSN")
	commandTopic := flag.String("command-topic", "soar.commands", "Kafka command topic")
	eventTopic := flag.String("event-topic", "soar.events", "Kafka event topic")
	severity := flag.String("severity", "high", "alert severity (only \"high\" triggers an auto action)")
	fail := flag.Bool("fail", false, "use an IP the mock BlockIP action is configured to fail on")
	ipFlag := flag.String("ip", "", "explicit source IP to put in the alert (overrides -fail's default)")
	direct := flag.Bool("direct", false, "bypass the workflow engine: command BlockIP directly against the sandbox worker")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := eventstore.Open(*pgDSN)
	if err != nil {
		log.Fatalf("event store: %v", err)
	}
	defer store.Close()

	caseID := uuid.New()
	ip := "203.0.113.7"
	if *fail {
		ip = "203.0.113.77" // must match sandbox-worker's BLOCKIP_FAIL_FOR
	}
	if *ipFlag != "" {
		ip = *ipFlag
	}

	alertEnv, err := events.NewEnvelope(caseID, events.TypeAlertReceived, events.AlertReceived{
		Source:   "seed-cli",
		AlertRaw: fmt.Sprintf("suspicious traffic from %s", ip),
		Severity: *severity,
	})
	must(err)
	alertStored, err := store.Append(ctx, alertEnv)
	must(err)
	log.Printf("case %s: appended %s (seq=%d)", caseID, alertStored.Type, alertStored.Seq)
	// Printed in a stable, greppable format so scripts (see
	// ../scripts/demo-recovery.sh) can capture the case ID without
	// parsing the human-readable log lines around it.
	fmt.Printf("CASE_ID=%s\n", caseID)

	if *direct {
		producer := kafkabus.NewProducer(strings.Split(*brokers, ","), *commandTopic)
		defer producer.Close()

		commandEnv, err := events.NewEnvelope(caseID, events.TypeActionCommanded, events.ActionCommanded{
			CommandID: uuid.New(),
			Action:    actions.BlockIP{}.Name(),
			Params:    mustJSON(actions.BlockIPParams{IP: ip, Reason: "seed-cli -direct demo"}),
		})
		must(err)
		must(producer.Publish(ctx, caseID.String(), commandEnv))
		log.Printf("case %s: published ActionCommanded action=%s ip=%s directly (engine bypassed)",
			caseID, actions.BlockIP{}.Name(), ip)
		return
	}

	// Realistic path: publish the durable AlertReceived envelope onto
	// soar.events, same as the sandbox worker publishes its outcome
	// events. The workflow engine is subscribed to this topic and will
	// fold this case's history, decide, and command BlockIP itself.
	eventsProducer := kafkabus.NewProducer(strings.Split(*brokers, ","), *eventTopic)
	defer eventsProducer.Close()
	must(eventsProducer.Publish(ctx, caseID.String(), alertStored))
	log.Printf("case %s: published AlertReceived (severity=%s) — watch the workflow-engine logs", caseID, *severity)
	log.Println("watch the sandbox-worker logs next, then re-run with -fail to see the retry/recovery path")
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	must(err)
	return b
}
