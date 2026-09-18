// Command api is a small read-only HTTP layer over the CQRS read model
// and the raw event log. It exists purely to feed the React UI — it
// never writes anything, which keeps the "reads never touch the
// write path" CQRS boundary honest even at the HTTP layer.
package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/example/soar-engine/internal/eventstore"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

type caseRow struct {
	CaseID           string `json:"case_id"`
	Status           string `json:"status"`
	Severity         string `json:"severity"`
	LastEventSeq     int64  `json:"last_event_seq"`
	LastAction       string `json:"last_action"`
	LastActionStatus string `json:"last_action_status"`
	UpdatedAt        string `json:"updated_at"`
}

func main() {
	pgDSN := getEnv("POSTGRES_DSN", "postgres://soar:soar@localhost:5432/soar?sslmode=disable")
	addr := getEnv("API_ADDR", ":8080")

	db, err := sql.Open("postgres", pgDSN)
	if err != nil {
		log.Fatalf("open postgres: %v", err)
	}
	defer db.Close()

	store, err := eventstore.Open(pgDSN)
	if err != nil {
		log.Fatalf("open event store: %v", err)
	}
	defer store.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/cases", withCORS(listCases(db)))
	mux.HandleFunc("/api/cases/", withCORS(caseTimeline(store)))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	log.Printf("api listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

// listCases serves GET /api/cases from the read model — this is the
// whole point of the projector: the UI never has to replay every case's
// full history just to render a list.
func listCases(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		rows, err := db.QueryContext(r.Context(), `
			SELECT case_id, status, COALESCE(severity, ''), last_event_seq,
			       COALESCE(last_action, ''), COALESCE(last_action_status, ''),
			       updated_at
			FROM case_read_model ORDER BY updated_at DESC LIMIT 200`)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		out := []caseRow{}
		for rows.Next() {
			var c caseRow
			if err := rows.Scan(&c.CaseID, &c.Status, &c.Severity, &c.LastEventSeq,
				&c.LastAction, &c.LastActionStatus, &c.UpdatedAt); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			out = append(out, c)
		}
		writeJSON(w, out)
	}
}

// caseTimeline serves GET /api/cases/{id}/events by replaying the case's
// full event log — this is literally Store.Load, the same primitive the
// workflow engine's recovery path uses, just rendered for a human
// instead of driving a state machine.
func caseTimeline(store *eventstore.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		path := strings.TrimPrefix(r.URL.Path, "/api/cases/")
		id := strings.TrimSuffix(path, "/events")
		caseID, err := uuid.Parse(id)
		if err != nil {
			http.Error(w, "invalid case id", http.StatusBadRequest)
			return
		}
		envs, err := store.Load(r.Context(), caseID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, envs)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func withCORS(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next(w, r)
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
