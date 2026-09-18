package com.example.soar.events;

/**
 * Mirrors internal/events.Type on the Go side exactly — the sandbox
 * worker and the workflow engine must agree on these strings since
 * they're what's stored in the {@code type} column and read back on
 * replay. Keep this list append-only, same rule as the Go side: never
 * rename or remove a value once events using it exist in the log.
 */
public enum EventType {
    AlertReceived,
    EnrichmentDone,
    ActionCommanded,
    ActionSucceeded,
    ActionFailed,
    CaseClosed,
}
