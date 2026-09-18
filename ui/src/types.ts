export type CaseStatus = "open" | "actioning" | "closed";
export type ActionStatus = "pending" | "succeeded" | "failed" | "";

export interface CaseSummary {
  case_id: string;
  status: CaseStatus;
  severity: string;
  last_event_seq: number;
  last_action: string;
  last_action_status: ActionStatus;
  updated_at: string;
}

export type EventType =
  | "AlertReceived"
  | "EnrichmentDone"
  | "ActionCommanded"
  | "ActionSucceeded"
  | "ActionFailed"
  | "CaseClosed";

export interface CaseEvent {
  event_id: string;
  case_id: string;
  seq: number;
  type: EventType;
  payload: Record<string, unknown>;
  occurred_at: string;
}
