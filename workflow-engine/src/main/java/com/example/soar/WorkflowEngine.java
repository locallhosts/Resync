package com.example.soar;

import com.example.soar.engine.CaseState;
import com.example.soar.engine.CaseStateMachine;
import com.example.soar.engine.Decision;
import com.example.soar.engine.RecoveryRunner;
import com.example.soar.eventstore.EventStore;
import com.example.soar.events.Envelope;
import com.example.soar.events.EventType;
import com.example.soar.kafka.EventBus;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Entry point for Week 2's deliverable: the event-sourced state machine
 * per case, emitting commands. Consumes {@code soar.events} (everything
 * the sandbox worker and this engine itself produce), folds each
 * touched case's full history, decides what — if anything — should
 * happen next, and publishes {@code ActionCommanded} onto
 * {@code soar.commands} for the sandbox worker to pick up.
 *
 * <p>Every decision the engine makes is durable BEFORE it's published:
 * an {@code ActionCommanded} is appended to the event store first, and
 * only the stored (seq-assigned) envelope is what gets sent to Kafka.
 * That ordering is what makes {@link RecoveryRunner} trustworthy — the
 * event log is always the first thing to know about a decision, Kafka
 * delivery is secondary and can be safely retried.
 */
public final class WorkflowEngine {

    public static void main(String[] args) throws Exception {
        List<String> brokers = Arrays.asList(getEnv("KAFKA_BROKERS", "localhost:9092").split(","));
        String eventTopic = getEnv("EVENT_TOPIC", "soar.events");
        String commandTopic = getEnv("COMMAND_TOPIC", "soar.commands");
        String groupId = getEnv("CONSUMER_GROUP", "workflow-engine");
        String pgDsn = toJdbcUrl(getEnv("POSTGRES_DSN",
                "postgres://soar:soar@localhost:5432/soar?sslmode=disable"));

        EventStore store = EventStore.open(pgDsn);
        EventBus bus = new EventBus(brokers, commandTopic, eventTopic, groupId);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("workflow-engine: shutting down");
            bus.close();
        }));

        // The headline feature: resume anything left mid-action by a
        // previous instance of this process before doing anything else.
        System.out.println("workflow-engine: running startup recovery");
        new RecoveryRunner(store, bus).run();

        System.out.println("workflow-engine: started (group=" + groupId + ", event_topic=" + eventTopic + ")");

        while (true) {
            bus.poll(Duration.ofSeconds(1), envelope -> handle(store, bus, envelope));
        }
    }

    private static void handle(EventStore store, EventBus bus, Envelope envelope) {
        // Commands this engine itself just published loop back around
        // on soar.events only once the sandbox worker acts on them and
        // appends an outcome — ActionCommanded events on this topic are
        // therefore always ones we already know about; skip them so we
        // don't re-decide on our own not-yet-actioned command.
        if (envelope.type == EventType.ActionCommanded) {
            return;
        }

        UUID caseId = envelope.caseId;
        try {
            List<Envelope> history = store.load(caseId);
            CaseState state = CaseStateMachine.fold(caseId, history);
            Decision decision = CaseStateMachine.decide(state);

            switch (decision.kind) {
                case COMMAND -> {
                    Envelope commandEnv = Envelope.of(caseId, EventType.ActionCommanded, Map.of(
                            "command_id", UUID.randomUUID().toString(),
                            "action", decision.action,
                            "params", decision.params
                    ));
                    Envelope stored = store.append(commandEnv);
                    bus.publish(caseId.toString(), stored);
                    System.out.printf("case %s: commanded action=%s (seq=%d)%n",
                            caseId, decision.action, stored.seq);
                }
                case CLOSE -> {
                    Envelope closeEnv = Envelope.of(caseId, EventType.CaseClosed, Map.of(
                            "reason", decision.closeReason
                    ));
                    Envelope stored = store.append(closeEnv);
                    bus.publish(caseId.toString(), stored);
                    System.out.printf("case %s: closed reason=%s (seq=%d)%n",
                            caseId, decision.closeReason, stored.seq);
                }
                case NONE -> {
                    // Nothing to do yet — e.g. waiting on an outstanding
                    // command's outcome, or a low-severity alert this
                    // portfolio-scope playbook doesn't auto-act on.
                }
            }
        } catch (SQLException e) {
            // Deliberately do not crash the whole engine over one case's
            // DB hiccup; log and let the next relevant event (or the
            // next startup's RecoveryRunner pass, if this was actually
            // fatal) pick it back up.
            System.err.printf("case %s: error handling event: %s%n", caseId, e.getMessage());
        }
    }

    /**
     * The rest of this project uses the Go-style {@code postgres://} DSN
     * everywhere (docker-compose env vars, the Go services); JDBC wants
     * {@code jdbc:postgresql://...}, so this is the one place that
     * translates between the two rather than maintaining two separate
     * env var conventions across languages.
     */
    static String toJdbcUrl(String dsn) {
        if (dsn.startsWith("jdbc:")) {
            return dsn;
        }
        return "jdbc:" + dsn.replaceFirst("^postgres://", "postgresql://");
    }

    private static String getEnv(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isEmpty()) ? def : v;
    }
}
