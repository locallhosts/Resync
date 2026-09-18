import { useEffect, useRef, useState } from "react";
import "./App.css";
import CaseQueue from "./components/CaseQueue";
import CaseTimeline from "./components/CaseTimeline";
import { fetchCases, fetchCaseTimeline } from "./api";
import type { CaseEvent, CaseSummary } from "./types";

const POLL_MS = 3000;

export default function App() {
  const [cases, setCases] = useState<CaseSummary[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [events, setEvents] = useState<CaseEvent[]>([]);
  const [timelineLoading, setTimelineLoading] = useState(false);
  const [connectionError, setConnectionError] = useState<string | null>(null);
  const selectedRef = useRef<string | null>(null);
  useEffect(() => {
    selectedRef.current = selectedId;
  }, [selectedId]);

  // Poll the case queue. This mirrors how a real SOC console works: the
  // list is a live view over the read model, not a one-shot fetch.
  useEffect(() => {
    let cancelled = false;

    async function poll() {
      try {
        const list = await fetchCases();
        if (cancelled) return;
        setCases(list);
        setConnectionError(null);
        if (!selectedRef.current && list.length > 0) {
          setSelectedId(list[0].case_id);
        }
      } catch (err) {
        if (!cancelled) {
          setConnectionError(
            err instanceof Error ? err.message : "could not reach the API"
          );
        }
      }
    }

    poll();
    const id = setInterval(poll, POLL_MS);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  // Reload the selected case's timeline whenever selection changes or
  // the queue refreshes (an in-flight case's timeline grows over time).
  useEffect(() => {
    if (!selectedId) return;
    let cancelled = false;
    const timer = setTimeout(() => {
      if (!cancelled) setTimelineLoading(true);
    }, 0);
    fetchCaseTimeline(selectedId)
      .then((evts) => {
        if (!cancelled) setEvents(evts);
      })
      .catch(() => {
        if (!cancelled) setEvents([]);
      })
      .finally(() => {
        if (!cancelled) setTimelineLoading(false);
      });
    return () => {
      cancelled = true;
      clearTimeout(timer);
    };
  }, [selectedId, cases]);

  const openCount = cases.filter((c) => c.status !== "closed").length;

  return (
    <div className="console">
      <header className="console-header">
        <span className="console-title">SOAR console</span>
        <span className="console-meta">
          {connectionError ? (
            <span className="console-error">{connectionError}</span>
          ) : (
            `${openCount} open case${openCount === 1 ? "" : "s"}`
          )}
        </span>
      </header>
      <div className="console-body">
        <aside className="console-queue">
          <h1 className="pane-title">Case queue</h1>
          <CaseQueue cases={cases} selectedId={selectedId} onSelect={setSelectedId} />
        </aside>
        <main className="console-timeline">
          <CaseTimeline caseId={selectedId} events={events} loading={timelineLoading} />
        </main>
      </div>
    </div>
  );
}
