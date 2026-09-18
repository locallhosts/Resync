package com.example.soar.engine;

import com.example.soar.eventstore.EventStore;
import com.example.soar.events.Envelope;
import com.example.soar.kafka.EventBus;

import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

/**
 * This class is the point of the whole project: "if a playbook fails
 * halfway ... the system can replay the event log for that case and
 * resume from the last successful step."
 *
 * <p>On startup (and this is the important part — ONLY on startup, not
 * as some separate periodic job) it asks the event store for every case
 * whose most recent event is an {@code ActionCommanded} with no
 * recorded outcome, folds each one's full history through
 * {@link CaseStateMachine}, and re-publishes the same command with the
 * same command ID. It does not:
 *
 * <ul>
 *   <li>re-run the alert-received decision (the case doesn't go back
 *       to square one),</li>
 *   <li>lose the failure count if this is actually the Nth attempt
 *       (fold() already replayed every prior ActionFailed), or</li>
 *   <li>need to know WHY the engine died — crash, redeploy, OOM kill,
 *       it's all the same code path.</li>
 * </ul>
 *
 * <p>To see this in action: start the stack, seed a case, kill
 * {@code -9} the workflow engine process after it commands BlockIP but
 * before the sandbox worker's result event lands, then start the engine
 * back up. The log line below fires and the same command gets
 * re-published — watch the event log's seq numbers to confirm nothing
 * was skipped or duplicated.
 */
public final class RecoveryRunner {
    private final EventStore store;
    private final EventBus bus;

    public RecoveryRunner(EventStore store, EventBus bus) {
        this.store = store;
        this.bus = bus;
    }

    public void run() throws SQLException {
        List<UUID> inFlight = store.findInFlightCases();
        if (inFlight.isEmpty()) {
            System.out.println("recovery: no in-flight cases, nothing to resume");
            return;
        }
        System.out.println("recovery: found " + inFlight.size() + " in-flight case(s) to resume");

        for (UUID caseId : inFlight) {
            List<Envelope> history = store.load(caseId);
            CaseState state = CaseStateMachine.fold(caseId, history);

            if (state.status != CaseState.Status.COMMANDED) {
                // Shouldn't happen — findInFlightCases only returns
                // cases whose LAST event is ActionCommanded — but fold()
                // is the single source of truth, so trust it over the
                // SQL query if they ever disagree.
                continue;
            }

            Envelope lastCommand = history.get(history.size() - 1);
            System.out.printf(
                    "recovery: case %s resuming action=%s (last known seq=%d, %d prior failure(s))%n",
                    caseId, state.lastAction, state.lastSeq, state.failureCount);

            // Re-publish the exact command that was in flight. We do NOT
            // append a new ActionCommanded event — the one already in
            // the log (at lastCommand.seq) is still the correct record
            // of "we decided to do this"; we're only re-delivering it to
            // Kafka in case the sandbox worker never received or
            // finished it the first time.
            bus.publish(caseId.toString(), lastCommand);
        }
    }
}
