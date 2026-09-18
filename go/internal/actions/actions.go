// Package actions defines the pluggable interface the execution sandbox
// runs. Each Action talks to exactly one external system and nothing
// else — that's what keeps the "isolated execution sandbox" story
// honest: a container running the BlockIP action has network access to
// the firewall API and nowhere else.
package actions

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Action is implemented once per real-world capability (block an IP,
// disable an account, quarantine a host, ...). Run should be idempotent
// where the underlying API allows it, since a resumed case may re-issue
// a command whose success event was lost before the workflow engine saw
// it — better to safely no-op than to double-fire.
type Action interface {
	Name() string
	Run(ctx context.Context, params json.RawMessage) (result json.RawMessage, err error)
}

// Registry maps action names (as they appear in an ActionCommanded
// event) to their implementation, so the worker's dispatch loop doesn't
// need a type switch that grows every time a new action is added.
type Registry map[string]Action

func NewRegistry(as ...Action) Registry {
	r := make(Registry, len(as))
	for _, a := range as {
		r[a.Name()] = a
	}
	return r
}

// --- Mock actions ---
//
// These simulate real integrations (a firewall API, an IdP API) with a
// short sleep and a deterministic failure mode, which is exactly what
// you want for the Week 4 failure-injection/replay demo: you can force
// a specific action to fail on command instead of hoping a real
// third-party API cooperates during a live demo.

type BlockIPParams struct {
	IP     string `json:"ip"`
	Reason string `json:"reason"`
}

type BlockIP struct {
	// FailFor, if set, makes Run fail whenever the IP matches — used to
	// deterministically trigger the recovery/replay path in demos and
	// tests without needing a real flaky dependency.
	FailFor map[string]bool
}

func (a BlockIP) Name() string { return "BlockIP" }

func (a BlockIP) Run(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
	var p BlockIPParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("unmarshal BlockIP params: %w", err)
	}

	select {
	case <-time.After(300 * time.Millisecond): // simulate network call
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	if a.FailFor[p.IP] {
		return nil, fmt.Errorf("firewall API: rule push timed out for %s", p.IP)
	}

	result, _ := json.Marshal(map[string]string{
		"ip":     p.IP,
		"status": "blocked",
	})
	return result, nil
}

type DisableAccountParams struct {
	UserID string `json:"user_id"`
}

type DisableAccount struct {
	FailFor map[string]bool
}

func (a DisableAccount) Name() string { return "DisableAccount" }

func (a DisableAccount) Run(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
	var p DisableAccountParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("unmarshal DisableAccount params: %w", err)
	}

	select {
	case <-time.After(300 * time.Millisecond):
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	if a.FailFor[p.UserID] {
		return nil, fmt.Errorf("IdP API: 503 while disabling %s", p.UserID)
	}

	result, _ := json.Marshal(map[string]string{
		"user_id": p.UserID,
		"status":  "disabled",
	})
	return result, nil
}
