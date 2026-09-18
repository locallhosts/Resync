import { useState } from "react";
import type { CaseEvent } from "../types";

interface Props {
  caseId: string | null;
  events: CaseEvent[];
  loading: boolean;
}

const TYPE_LABEL: Record<CaseEvent["type"], string> = {
  AlertReceived: "Alert received",
  EnrichmentDone: "Enrichment done",
  ActionCommanded: "Action commanded",
  ActionSucceeded: "Action succeeded",
  ActionFailed: "Action failed",
  CaseClosed: "Case closed",
};

function formatTime(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleTimeString(undefined, { hour12: false }) + "." + String(d.getMilliseconds()).padStart(3, "0");
}

function EventRow({ event }: { event: CaseEvent }) {
  const [open, setOpen] = useState(false);
  const hasPayload = event.payload && Object.keys(event.payload).length > 0;

  return (
    <li className={`timeline-row type-${event.type}`}>
      <div className="timeline-seq">{event.seq}</div>
      <div className="timeline-rail">
        <div className="timeline-dot" />
        <div className="timeline-line" />
      </div>
      <div className="timeline-body">
        <button
          className="timeline-header"
          onClick={() => hasPayload && setOpen((v) => !v)}
          aria-expanded={open}
        >
          <span className="timeline-type">{TYPE_LABEL[event.type] ?? event.type}</span>
          <span className="timeline-time">{formatTime(event.occurred_at)}</span>
        </button>
        {open && hasPayload && (
          <pre className="timeline-payload">{JSON.stringify(event.payload, null, 2)}</pre>
        )}
      </div>
    </li>
  );
}

export default function CaseTimeline({ caseId, events, loading }: Props) {
  if (!caseId) {
    return (
      <div className="timeline-empty">
        <p>Select a case to replay its history.</p>
      </div>
    );
  }

  return (
    <div className="timeline-pane">
      <div className="timeline-pane-header">
        <h2>{caseId}</h2>
        {loading && <span className="timeline-loading">refreshing…</span>}
      </div>
      {events.length === 0 && !loading ? (
        <p className="timeline-empty-sub">No events recorded for this case yet.</p>
      ) : (
        <ul className="timeline">
          {events.map((e) => (
            <EventRow key={e.event_id} event={e} />
          ))}
        </ul>
      )}
    </div>
  );
}
