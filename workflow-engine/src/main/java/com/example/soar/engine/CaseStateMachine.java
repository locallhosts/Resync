package com.example.soar.engine;

import com.example.soar.events.Envelope;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The state machine described in the architecture doc: "event-sourced
 * state machine per case, emits Commands." {@link #fold} is pure and
 * total — given the same event list it always produces the same state,
 * which is what makes replay (and therefore recovery) trustworthy.
 * {@link #decide} is the only place business logic like "retry twice
 * then give up" lives.
 */
public final class CaseStateMachine {

    /** Max times a failed action is retried before the case is closed unresolved. */
    private static final int MAX_RETRIES = 2;

    /** Matches an IPv4 address anywhere in the alert's free text. */
    private static final Pattern IP_PATTERN = Pattern.compile("\\b(\\d{1,3}(?:\\.\\d{1,3}){3})\\b");

    private CaseStateMachine() {}

    public static CaseState fold(java.util.UUID caseId, List<Envelope> events) {
        CaseState.Status status = CaseState.Status.NEW;
        long lastSeq = 0;
        String severity = null;
        String lastAlertRaw = null;
        String lastAction = null;
        Map<String, Object> lastActionParams = null;
        int failureCount = 0;

        for (Envelope e : events) {
            lastSeq = e.seq;
            switch (e.type) {
                case AlertReceived -> {
                    status = CaseState.Status.ALERTED;
                    severity = str(e.payload, "severity");
                    lastAlertRaw = str(e.payload, "alert_raw");
                }
                case ActionCommanded -> {
                    status = CaseState.Status.COMMANDED;
                    lastAction = str(e.payload, "action");
                    Object params = e.payload.get("params");
                    lastActionParams = params instanceof Map ? castParams(params) : Map.of();
                }
                case ActionSucceeded -> {
                    status = CaseState.Status.SUCCEEDED;
                }
                case ActionFailed -> {
                    status = CaseState.Status.FAILED;
                    failureCount++;
                }
                case CaseClosed -> {
                    status = CaseState.Status.CLOSED;
                }
                case EnrichmentDone -> {
                    // doesn't change the decision-relevant state in this
                    // portfolio version; a real system would fold
                    // enrichment results in here for the decide() step
                    // to use (e.g. "is this IP on a known-bad list").
                }
            }
        }

        return new CaseState(caseId, status, lastSeq, severity, lastAlertRaw,
                lastAction, lastActionParams, failureCount);
    }

    /**
     * The actual playbook logic. This is deliberately simple (one alert
     * type, one action) so the interesting part of the project — event
     * sourcing, replay, recovery — isn't buried under a rules engine.
     * A real system would dispatch on alert type / enrichment results
     * here instead of a single hardcoded rule.
     */
    public static Decision decide(CaseState state) {
        return switch (state.status) {
            case ALERTED -> decideFromAlert(state);
            case FAILED -> decideFromFailure(state);
            case SUCCEEDED -> Decision.close("resolved");
            case NEW, COMMANDED, CLOSED -> Decision.none();
        };
    }

    private static Decision decideFromAlert(CaseState state) {
        if (!"high".equalsIgnoreCase(state.severity)) {
            // Portfolio-scope playbook only auto-acts on high severity;
            // anything else waits for a human, which this version
            // models simply as "no decision" rather than a real queue.
            return Decision.none();
        }
        Matcher m = IP_PATTERN.matcher(state.lastAlertRaw == null ? "" : state.lastAlertRaw);
        if (!m.find()) {
            return Decision.none();
        }
        return Decision.command("BlockIP", Map.of(
                "ip", m.group(1),
                "reason", "auto-blocked: high severity alert"
        ));
    }

    private static Decision decideFromFailure(CaseState state) {
        if (state.failureCount > MAX_RETRIES) {
            return Decision.close("action_failed_max_retries");
        }
        if (state.lastAction == null) {
            return Decision.close("action_failed_no_retry_target");
        }
        // Retry the SAME action with the SAME params — this is the
        // resumability property: retrying isn't "start the case over",
        // it's "re-issue exactly the step that didn't finish."
        return Decision.command(state.lastAction, state.lastActionParams);
    }

    private static String str(Map<String, Object> payload, String key) {
        Object v = payload.get(key);
        return v == null ? null : v.toString();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> castParams(Object o) {
        return (Map<String, Object>) o;
    }
}
