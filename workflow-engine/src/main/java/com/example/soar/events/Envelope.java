package com.example.soar.events;

import com.example.soar.Json;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * The polymorphic envelope every event is wrapped in, matching
 * internal/events.Envelope on the Go side field-for-field so both
 * languages can read the same {@code events} table. {@code payload} is
 * kept as a parsed {@link Map} (via {@link Json}) rather than a typed
 * object, for the same reason the Go side uses {@code json.RawMessage}:
 * new event types never require touching this class.
 */
public final class Envelope {
    public final UUID eventId;
    public final UUID caseId;
    public final long seq; // 0 until EventStore.append assigns it
    public final EventType type;
    public final Map<String, Object> payload;
    public final Instant occurredAt;

    public Envelope(UUID eventId, UUID caseId, long seq, EventType type,
                     Map<String, Object> payload, Instant occurredAt) {
        this.eventId = eventId;
        this.caseId = caseId;
        this.seq = seq;
        this.type = type;
        this.payload = payload;
        this.occurredAt = occurredAt;
    }

    /** Creates a new envelope ready to append; seq is assigned by the store. */
    public static Envelope of(UUID caseId, EventType type, Map<String, Object> payload) {
        return new Envelope(UUID.randomUUID(), caseId, 0, type, payload, Instant.now());
    }

    /** Returns a copy of this envelope with seq set — EventStore uses this after INSERT. */
    public Envelope withSeq(long newSeq) {
        return new Envelope(eventId, caseId, newSeq, type, payload, occurredAt);
    }

    public String payloadJson() {
        return Json.write(payload);
    }

    /** Serializes the whole envelope (used when publishing to Kafka). */
    public String toJson() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("event_id", eventId.toString());
        m.put("case_id", caseId.toString());
        m.put("seq", seq);
        m.put("type", type.name());
        m.put("payload", payload);
        m.put("occurred_at", occurredAt.toString());
        return Json.write(m);
    }

    @SuppressWarnings("unchecked")
    public static Envelope fromJson(String json) {
        Map<String, Object> m = Json.parseObject(json);
        Object payloadObj = m.get("payload");
        Map<String, Object> payload = payloadObj instanceof Map
                ? (Map<String, Object>) payloadObj
                : Map.of();
        return new Envelope(
                UUID.fromString((String) m.get("event_id")),
                UUID.fromString((String) m.get("case_id")),
                ((Number) m.get("seq")).longValue(),
                EventType.valueOf((String) m.get("type")),
                payload,
                Instant.parse((String) m.get("occurred_at")));
    }

    @Override
    public String toString() {
        return "Envelope{case=" + caseId + " seq=" + seq + " type=" + type + "}";
    }
}
