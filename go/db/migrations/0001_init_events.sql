-- 0001_init_events.sql
-- Append-only event log (the "write model") plus a materialized read
-- model table for the UI (the "read model"). This is the CQRS split:
-- nothing ever UPDATEs the events table; case_read_model is rebuilt by
-- replaying events and is safe to TRUNCATE + rebuild at any time.

CREATE TABLE IF NOT EXISTS events (
    event_id     UUID PRIMARY KEY,
    case_id      UUID NOT NULL,
    seq          BIGINT NOT NULL,
    type         TEXT NOT NULL,
    payload      JSONB NOT NULL,
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (case_id, seq) -- guarantees no two events can claim the same
                          -- position in a case's history, even under
                          -- concurrent writers
);

CREATE INDEX IF NOT EXISTS idx_events_case_id ON events (case_id, seq);
CREATE INDEX IF NOT EXISTS idx_events_type ON events (type);

-- Read model: current derived state per case, kept in sync by a
-- projector that consumes the event log and upserts here. The UI reads
-- ONLY from this table, never from events directly, so read latency
-- stays flat no matter how long a case's history gets.
CREATE TABLE IF NOT EXISTS case_read_model (
    case_id       UUID PRIMARY KEY,
    status        TEXT NOT NULL,        -- e.g. open / actioning / closed
    severity      TEXT,
    last_event_seq BIGINT NOT NULL DEFAULT 0,
    last_action   TEXT,
    last_action_status TEXT,            -- pending / succeeded / failed
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
