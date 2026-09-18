package com.example.soar.eventstore;

import com.example.soar.events.Envelope;
import com.example.soar.events.EventType;
import com.example.soar.Json;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The JVM-side counterpart to internal/eventstore.Store on the Go side.
 * Same table, same guarantees: per-case sequence numbers are assigned
 * inside a transaction holding a {@code SELECT ... FOR UPDATE} lock on
 * the case's existing rows, so two concurrent appenders for the same
 * case can never collide.
 */
public final class EventStore implements AutoCloseable {
    private final String dsn;

    private EventStore(String dsn) {
        this.dsn = dsn;
    }

    public static EventStore open(String dsn) throws SQLException {
        // Fail fast if the DSN is bad, same as the Go store's Open+Ping.
        try (Connection c = DriverManager.getConnection(dsn)) {
            // connection validated, closed immediately; callers get a
            // fresh connection per operation below rather than holding
            // one open for the store's whole lifetime.
        }
        return new EventStore(dsn);
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(dsn);
    }

    public Envelope append(Envelope env) throws SQLException {
        try (Connection c = connect()) {
            c.setAutoCommit(false);
            long nextSeq;
            try (PreparedStatement sel = c.prepareStatement(
                    "SELECT COALESCE(MAX(seq), 0) + 1 FROM events WHERE case_id = ? FOR UPDATE")) {
                sel.setObject(1, env.caseId);
                try (ResultSet rs = sel.executeQuery()) {
                    rs.next();
                    nextSeq = rs.getLong(1);
                }
            }

            Envelope stored = env.withSeq(nextSeq);

            try (PreparedStatement ins = c.prepareStatement(
                    "INSERT INTO events (event_id, case_id, seq, type, payload, occurred_at) " +
                            "VALUES (?, ?, ?, ?, ?::jsonb, ?)")) {
                ins.setObject(1, stored.eventId);
                ins.setObject(2, stored.caseId);
                ins.setLong(3, stored.seq);
                ins.setString(4, stored.type.name());
                ins.setString(5, stored.payloadJson());
                ins.setTimestamp(6, Timestamp.from(stored.occurredAt));
                ins.executeUpdate();
            }

            c.commit();
            return stored;
        }
    }

    public List<Envelope> load(UUID caseId) throws SQLException {
        return loadFrom(caseId, 0);
    }

    /**
     * Replays events with {@code seq > fromSeq}. This is what
     * {@link com.example.soar.engine.RecoveryRunner} calls on startup:
     * for a case whose last event is an unresolved ActionCommanded,
     * folding the case's full history (see CaseStateMachine) is how the
     * engine figures out "what still needs to happen" without
     * re-running already-completed steps.
     */
    public List<Envelope> loadFrom(UUID caseId, long fromSeq) throws SQLException {
        List<Envelope> out = new ArrayList<>();
        try (Connection c = connect();
             PreparedStatement sel = c.prepareStatement(
                     "SELECT event_id, case_id, seq, type, payload, occurred_at " +
                             "FROM events WHERE case_id = ? AND seq > ? ORDER BY seq ASC")) {
            sel.setObject(1, caseId);
            sel.setLong(2, fromSeq);
            try (ResultSet rs = sel.executeQuery()) {
                while (rs.next()) {
                    out.add(rowToEnvelope(rs));
                }
            }
        }
        return out;
    }

    /**
     * Returns the case IDs whose most recent event is ActionCommanded —
     * i.e. a command was issued but no ActionSucceeded/ActionFailed has
     * been recorded yet. On a clean shutdown this set is empty; after a
     * crash mid-action, these are exactly the cases
     * {@link com.example.soar.engine.RecoveryRunner} needs to resume.
     */
    public List<UUID> findInFlightCases() throws SQLException {
        List<UUID> out = new ArrayList<>();
        String sql = """
                SELECT e.case_id FROM events e
                INNER JOIN (
                    SELECT case_id, MAX(seq) AS max_seq FROM events GROUP BY case_id
                ) latest ON e.case_id = latest.case_id AND e.seq = latest.max_seq
                WHERE e.type = 'ActionCommanded'
                """;
        try (Connection c = connect();
             PreparedStatement sel = c.prepareStatement(sql);
             ResultSet rs = sel.executeQuery()) {
            while (rs.next()) {
                out.add((UUID) rs.getObject(1));
            }
        }
        return out;
    }

    private Envelope rowToEnvelope(ResultSet rs) throws SQLException {
        UUID eventId = (UUID) rs.getObject("event_id");
        UUID caseId = (UUID) rs.getObject("case_id");
        long seq = rs.getLong("seq");
        EventType type = EventType.valueOf(rs.getString("type"));
        Map<String, Object> payload = Json.parseObject(rs.getString("payload"));
        Instant occurredAt = rs.getTimestamp("occurred_at").toInstant();
        return new Envelope(eventId, caseId, seq, type, payload, occurredAt);
    }

    @Override
    public void close() {
        // No pooled connection held between calls (see connect()), so
        // there is nothing to release here. Kept as a no-op close()
        // so callers can still use try-with-resources / AutoCloseable
        // uniformly with the Go side's Store.Close().
    }
}
