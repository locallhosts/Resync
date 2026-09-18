import type { CaseSummary } from "../types";

interface Props {
  cases: CaseSummary[];
  selectedId: string | null;
  onSelect: (id: string) => void;
}

function statusGlyph(c: CaseSummary): { glyph: string; className: string } {
  if (c.last_action_status === "failed") return { glyph: "✕", className: "glyph-failed" };
  if (c.status === "closed") return { glyph: "●", className: "glyph-closed" };
  if (c.last_action_status === "pending") return { glyph: "⟳", className: "glyph-pending" };
  return { glyph: "○", className: "glyph-open" };
}

export default function CaseQueue({ cases, selectedId, onSelect }: Props) {
  if (cases.length === 0) {
    return (
      <div className="queue-empty">
        <p>No cases yet.</p>
        <p className="queue-empty-sub">
          Run <code>go run ./cmd/seed</code> against the engine to create one.
        </p>
      </div>
    );
  }

  return (
    <ul className="case-queue" role="list">
      {cases.map((c) => {
        const { glyph, className } = statusGlyph(c);
        return (
          <li key={c.case_id}>
            <button
              className={`case-row ${c.case_id === selectedId ? "case-row-selected" : ""}`}
              onClick={() => onSelect(c.case_id)}
            >
              <span className={`case-glyph ${className}`}>{glyph}</span>
              <span className="case-row-main">
                <span className="case-id">{c.case_id.slice(0, 8)}</span>
                <span className={`case-severity severity-${c.severity || "unknown"}`}>
                  {c.severity || "—"}
                </span>
              </span>
              <span className="case-row-sub">
                {c.last_action ? `${c.last_action} · ${c.last_action_status || c.status}` : c.status}
              </span>
            </button>
          </li>
        );
      })}
    </ul>
  );
}
