package com.example.soar.engine;

import java.util.Map;
import java.util.UUID;

/**
 * The state a case's history folds down to. Nothing in the system
 * stores this directly — it is always recomputed by
 * {@link CaseStateMachine#fold} from the event log. Compare to the Go
 * side, which doesn't materialize this at all (the read model plays
 * that role for the UI); the workflow engine needs it in-process to
 * decide what to do next.
 */
public final class CaseState {
    public enum Status { NEW, ALERTED, COMMANDED, SUCCEEDED, FAILED, CLOSED }

    public final UUID caseId;
    public final Status status;
    public final long lastSeq;
    public final String severity;
    public final String lastAlertRaw;

    /** Name of the most recently commanded action, if any — needed to retry with the same action. */
    public final String lastAction;
    /** Params the most recent command was issued with — needed to retry with the same params. */
    public final Map<String, Object> lastActionParams;
    public final int failureCount;

    public CaseState(UUID caseId, Status status, long lastSeq, String severity, String lastAlertRaw,
                      String lastAction, Map<String, Object> lastActionParams, int failureCount) {
        this.caseId = caseId;
        this.status = status;
        this.lastSeq = lastSeq;
        this.severity = severity;
        this.lastAlertRaw = lastAlertRaw;
        this.lastAction = lastAction;
        this.lastActionParams = lastActionParams;
        this.failureCount = failureCount;
    }

    public static CaseState empty(UUID caseId) {
        return new CaseState(caseId, Status.NEW, 0, null, null, null, null, 0);
    }

    @Override
    public String toString() {
        return "CaseState{case=" + caseId + " status=" + status + " lastSeq=" + lastSeq
                + " lastAction=" + lastAction + " failures=" + failureCount + "}";
    }
}
