export type OperatorOverview = {
  admissions_stopped: boolean;
  active_runs: number;
  unknown_effects: number;
  uncertain_allocations: number;
  pending_inbox_deliveries: number;
  oldest_pending_inbox_at: string | null;
  artifact_charged_bytes: number;
  artifact_capacity_limit_bytes: number;
  suspended_accounts: number;
};

export type OperatorUnknownEffect = {
  effect_id: string;
  owner_id: string;
  run_id: string;
  status: string;
  created_at: string;
  updated_at: string;
};

export type OperatorUncertainAllocation = {
  allocation_id: string;
  owner_id: string;
  run_id: string;
  status: string;
  provider_name: string;
  created_at: string;
  last_observed_at: string | null;
};

export type OperatorInboxDelivery = {
  delivery_id: string;
  event_type: string;
  received_at: string;
};

export type OperatorSuspendedAccount = {
  user_id: string;
  github_user_id: number;
  created_at: string;
};

export type OperatorAuditEvent = {
  audit_id: string;
  actor_github_user_id: number;
  action: string;
  target_user_id: string | null;
  reason_code: string | null;
  created_at: string;
};

async function get<T>(path: string): Promise<T> {
  let response: Response;
  try {
    response = await fetch(path, {
      method: "GET",
      credentials: "same-origin",
      headers: { Accept: "application/json" },
    });
  } catch {
    throw new Error("operator_connection_failed");
  }
  if (!response.ok) {
    throw new OperatorApiError(response.status);
  }
  try {
    return await response.json() as T;
  } catch {
    throw new Error("operator_response_invalid");
  }
}

export class OperatorApiError extends Error {
  constructor(readonly status: number) {
    super("operator_request_failed");
  }
}

export const getOperatorOverview = () => get<OperatorOverview>("/api/v1/operator/overview");
export const getUnknownEffects = () => get<OperatorUnknownEffect[]>("/api/v1/operator/unknown-effects");
export const getUncertainAllocations = () => get<OperatorUncertainAllocation[]>("/api/v1/operator/uncertain-allocations");
export const getPendingInbox = () => get<OperatorInboxDelivery[]>("/api/v1/operator/inbox-lag");
export const getSuspendedAccounts = () => get<OperatorSuspendedAccount[]>("/api/v1/operator/suspended-accounts");
export const getOperatorAudit = () => get<OperatorAuditEvent[]>("/api/v1/operator/audit");
