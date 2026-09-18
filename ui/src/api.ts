import type { CaseEvent, CaseSummary } from "./types";

const BASE_URL = import.meta.env.VITE_API_URL ?? "http://localhost:8080";

async function getJSON<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`);
  if (!res.ok) {
    throw new Error(`${path} returned ${res.status}`);
  }
  return res.json() as Promise<T>;
}

export function fetchCases(): Promise<CaseSummary[]> {
  return getJSON<CaseSummary[]>("/api/cases");
}

export function fetchCaseTimeline(caseId: string): Promise<CaseEvent[]> {
  return getJSON<CaseEvent[]>(`/api/cases/${caseId}/events`);
}
