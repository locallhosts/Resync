// Package events defines the immutable event types that make up a case's
// history. Nothing in the system ever mutates case state directly — every
// change is recorded as one of these events and appended to the event log.
// Current state is always derived by replaying a case's events in order.
package events

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Type is the discriminator for the polymorphic event payload stored in
// the append-only log. Keep this list append-only too: never rename or
// remove a value once events using it exist in the log, or replay breaks.
type Type string

const (
	TypeAlertReceived   Type = "AlertReceived"
	TypeEnrichmentDone  Type = "EnrichmentDone"
	TypeActionCommanded Type = "ActionCommanded"
	TypeActionSucceeded Type = "ActionSucceeded"
	TypeActionFailed    Type = "ActionFailed"
	TypeCaseClosed      Type = "CaseClosed"
)

// Envelope is the row shape stored in the Postgres event log. Payload is
// kept as raw JSON so new event types never require a schema migration —
// only a new Go struct and a case in the replay switch.
type Envelope struct {
	EventID    uuid.UUID       `json:"event_id"`
	CaseID     uuid.UUID       `json:"case_id"`
	Seq        int64           `json:"seq"` // per-case monotonically increasing sequence number
	Type       Type            `json:"type"`
	Payload    json.RawMessage `json:"payload"`
	OccurredAt time.Time       `json:"occurred_at"`
}

// --- Payload types, one per event Type above ---

type AlertReceived struct {
	Source   string `json:"source"`
	AlertRaw string `json:"alert_raw"`
	Severity string `json:"severity"`
}

type EnrichmentDone struct {
	Enricher string          `json:"enricher"`
	Result   json.RawMessage `json:"result"`
}

type ActionCommanded struct {
	CommandID uuid.UUID       `json:"command_id"`
	Action    string          `json:"action"` // e.g. "BlockIP", "DisableAccount"
	Params    json.RawMessage `json:"params"`
}

type ActionSucceeded struct {
	CommandID uuid.UUID       `json:"command_id"`
	Action    string          `json:"action"`
	Result    json.RawMessage `json:"result"`
}

type ActionFailed struct {
	CommandID uuid.UUID `json:"command_id"`
	Action    string    `json:"action"`
	Error     string    `json:"error"`
	Retryable bool      `json:"retryable"`
}

type CaseClosed struct {
	Reason string `json:"reason"`
}

// NewEnvelope marshals a typed payload into an Envelope ready to append.
// Seq is left at 0 here — the event store assigns it atomically at
// append time so two concurrent writers can never collide.
func NewEnvelope(caseID uuid.UUID, t Type, payload any) (Envelope, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, err
	}
	return Envelope{
		EventID:    uuid.New(),
		CaseID:     caseID,
		Type:       t,
		Payload:    raw,
		OccurredAt: time.Now().UTC(),
	}, nil
}
