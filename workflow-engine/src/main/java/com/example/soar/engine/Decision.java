package com.example.soar.engine;

import java.util.Map;

/**
 * What {@link CaseStateMachine#decide} tells the engine to do next.
 * A sealed-style hierarchy would be cleaner in newer Java, but three
 * small static factories keep this readable without needing pattern
 * matching on sealed interfaces at every call site.
 */
public final class Decision {
    public enum Kind { NONE, COMMAND, CLOSE }

    public final Kind kind;
    public final String action;         // set iff kind == COMMAND
    public final Map<String, Object> params; // set iff kind == COMMAND
    public final String closeReason;    // set iff kind == CLOSE

    private Decision(Kind kind, String action, Map<String, Object> params, String closeReason) {
        this.kind = kind;
        this.action = action;
        this.params = params;
        this.closeReason = closeReason;
    }

    public static Decision none() {
        return new Decision(Kind.NONE, null, null, null);
    }

    public static Decision command(String action, Map<String, Object> params) {
        return new Decision(Kind.COMMAND, action, params, null);
    }

    public static Decision close(String reason) {
        return new Decision(Kind.CLOSE, null, null, reason);
    }

    @Override
    public String toString() {
        return switch (kind) {
            case NONE -> "Decision{none}";
            case COMMAND -> "Decision{command=" + action + " params=" + params + "}";
            case CLOSE -> "Decision{close=" + closeReason + "}";
        };
    }
}
