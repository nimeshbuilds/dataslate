import { request } from './api';

export interface WorkflowBinding {
  id: string; workspaceId: string; name: string; policyVersion: string;
  dagId: string; dagVersion: number; bundleVersion: string; airflowVersion: '3.4.0';
  maxRunSeconds: number; scheduler: 'airflow'; retryOwner: 'airflow';
}
export interface WorkflowDefinition {
  bindingId: string; workspaceId: string; policyVersion: string; dagId: string; dagVersion: number; bundleVersion: string;
  observedAt: string; paused: boolean; stale: boolean; hasImportErrors: boolean; scheduleSummary: string; timezone: string;
  nextRunAt?: string; versionMatches: boolean; canTrigger: boolean; reasonCode: string;
  tasks: { id: string; downstreamTaskIds: string[] }[];
}
export interface WorkflowTask { id: string; mapIndex: number; tryNumber: number; state: string; startedAt?: string; endedAt?: string }
export interface WorkflowRun {
  id: string; workspaceId: string; createdAt: string; updatedAt: string; revision: number; bindingId: string; policyVersion: string;
  dagId: string; dagVersion: number; bundleVersion: string; backendRunId: string; submittedBy: { issuer: string; subject: string };
  state: 'queued' | 'checking' | 'submitting' | 'running' | 'succeeded' | 'failed' | 'unknown'; dispatchStarted: boolean; reconciliationComplete: boolean;
  message: string; observedAt?: string; startedAt?: string; endedAt?: string; tasks: WorkflowTask[]; tasksTruncated: boolean;
}
export interface WorkflowBackendRun { dagId: string; backendRunId: string; state: string; runType: string; dagVersion: number; bundleVersion: string; logicalDate?: string; startedAt?: string; endedAt?: string }
export interface WorkflowHistory { items: WorkflowBackendRun[]; observedAt: string; truncated: boolean }
export interface WorkflowIntent { bindingId: string; idempotencyKey: string; policyVersion: string; dagId: string; dagVersion: number; bundleVersion: string }
const base = (ws: string) => `/workspaces/${encodeURIComponent(ws)}`;
export const workflows = {
  bindings: (ws: string, signal?: AbortSignal) => request<{ items: WorkflowBinding[] }>(`${base(ws)}/workflow-bindings`, { signal }),
  definition: (ws: string, id: string, signal?: AbortSignal) => request<WorkflowDefinition>(`${base(ws)}/workflow-bindings/${encodeURIComponent(id)}/definition`, { signal }),
  history: (ws: string, id: string, signal?: AbortSignal) => request<WorkflowHistory>(`${base(ws)}/workflow-bindings/${encodeURIComponent(id)}/history`, { signal }),
  list: (ws: string, signal?: AbortSignal) => request<{ items: WorkflowRun[] }>(`${base(ws)}/workflow-runs`, { signal }),
  get: (ws: string, id: string, signal?: AbortSignal) => request<WorkflowRun>(`${base(ws)}/workflow-runs/${encodeURIComponent(id)}`, { signal }),
  submit: (ws: string, intent: WorkflowIntent, signal?: AbortSignal) => request<{ run: WorkflowRun; replayed: boolean }>(`${base(ws)}/workflow-runs`, { method: 'POST', body: JSON.stringify(intent), signal }),
};
const object = (v: unknown): v is Record<string, unknown> => !!v && typeof v === 'object' && !Array.isArray(v);
const keys = (v: Record<string, unknown>, allowed: string[]) => Object.keys(v).every((k) => allowed.includes(k));
const text = (v: unknown, max = 2048): v is string => typeof v === 'string' && !!v.trim() && !v.includes('\0') && new TextEncoder().encode(v).length <= max;
const id = (v: unknown): v is string => typeof v === 'string' && /^[a-z0-9][a-z0-9-]{0,62}$/.test(v);
const positive = (v: unknown) => Number.isSafeInteger(v) && Number(v) > 0;
export function validWorkflowBinding(v: unknown, ws: string): v is WorkflowBinding {
  return object(v) && keys(v, ['id', 'workspaceId', 'name', 'policyVersion', 'dagId', 'dagVersion', 'bundleVersion', 'airflowVersion', 'maxRunSeconds', 'scheduler', 'retryOwner']) && id(v.id) && v.workspaceId === ws && text(v.name, 200) && text(v.policyVersion, 256) && text(v.dagId, 250) && positive(v.dagVersion) && text(v.bundleVersion, 256) && v.airflowVersion === '3.4.0' && positive(v.maxRunSeconds) && v.scheduler === 'airflow' && v.retryOwner === 'airflow';
}
export function validWorkflowIntent(v: unknown): v is WorkflowIntent { return object(v) && keys(v, ['bindingId', 'idempotencyKey', 'policyVersion', 'dagId', 'dagVersion', 'bundleVersion']) && id(v.bindingId) && typeof v.idempotencyKey === 'string' && /^[A-Za-z0-9._:-]{8,128}$/.test(v.idempotencyKey) && text(v.policyVersion, 256) && text(v.dagId, 250) && positive(v.dagVersion) && text(v.bundleVersion, 256); }
const sameIntentProfile = (v: Record<string, unknown> | WorkflowIntent, expected: WorkflowIntent) => v.bindingId === expected.bindingId && v.policyVersion === expected.policyVersion && v.dagId === expected.dagId && v.dagVersion === expected.dagVersion && v.bundleVersion === expected.bundleVersion;
export const workflowStorageKey = (ws: string, issuer: string, subject: string) => `dataslate.workflow.intent.${JSON.stringify([issuer, subject, ws])}`;
export function restoreWorkflow(key: string): { intent?: WorkflowIntent; error?: string } {
  try { const raw = sessionStorage.getItem(key); if (!raw) return {}; const value: unknown = JSON.parse(raw); if (!validWorkflowIntent(value)) throw new Error('Invalid record'); return { intent: value }; }
  catch { return { error: 'Workflow recovery is unavailable or invalid. Inspect your runs before repairing browser storage.' }; }
}
export function forgetWorkflow(key: string, expected: WorkflowIntent) {
  const raw = sessionStorage.getItem(key); if (!raw) return; const value: unknown = JSON.parse(raw);
  if (!validWorkflowIntent(value)) throw new Error('Workflow recovery record is invalid.');
  if (sameIntentProfile(value, expected) && value.idempotencyKey === expected.idempotencyKey) sessionStorage.removeItem(key);
}

const date = (v: unknown) => typeof v === 'string' && Number.isFinite(Date.parse(v));
const optionalDates = (v: Record<string, unknown>, fields: string[]) => fields.every((k) => v[k] === undefined || date(v[k]));
const sameProfile = (v: Record<string, unknown>, b: WorkflowBinding) => v.bindingId === b.id && v.workspaceId === b.workspaceId && v.policyVersion === b.policyVersion && v.dagId === b.dagId && v.dagVersion === b.dagVersion && v.bundleVersion === b.bundleVersion;
export const sameWorkflowProfile = (a: WorkflowBinding, b: WorkflowBinding) => a.id === b.id && a.workspaceId === b.workspaceId && a.policyVersion === b.policyVersion && a.dagId === b.dagId && a.dagVersion === b.dagVersion && a.bundleVersion === b.bundleVersion && a.maxRunSeconds === b.maxRunSeconds;
export const workflowTerminal = (run: WorkflowRun) => run.reconciliationComplete;
export function validWorkflowDefinition(v: unknown, binding: WorkflowBinding): v is WorkflowDefinition {
  if (!object(v) || !keys(v, ['bindingId', 'workspaceId', 'policyVersion', 'dagId', 'dagVersion', 'bundleVersion', 'observedAt', 'paused', 'stale', 'hasImportErrors', 'scheduleSummary', 'timezone', 'nextRunAt', 'versionMatches', 'canTrigger', 'reasonCode', 'tasks']) || !sameProfile(v, binding) || !date(v.observedAt) || !optionalDates(v, ['nextRunAt']) || !['paused', 'stale', 'hasImportErrors', 'versionMatches', 'canTrigger'].every((k) => typeof v[k] === 'boolean') || !text(v.scheduleSummary, 2048) || !text(v.timezone, 128) || !['ready', 'version_mismatch', 'paused', 'stale', 'import_errors'].includes(String(v.reasonCode)) || !Array.isArray(v.tasks) || v.tasks.length > 200) return false;
  if (v.canTrigger !== (v.versionMatches && !v.paused && !v.stale && !v.hasImportErrors) || v.canTrigger !== (v.reasonCode === 'ready')) return false;
  if (!v.tasks.every((task) => object(task) && keys(task, ['id', 'downstreamTaskIds']) && text(task.id, 250) && Array.isArray(task.downstreamTaskIds) && task.downstreamTaskIds.length <= 200 && task.downstreamTaskIds.every((t) => text(t, 250)) && new Set(task.downstreamTaskIds).size === task.downstreamTaskIds.length)) return false;
  const ids = new Set(v.tasks.map((t) => t.id)); return ids.size === v.tasks.length && v.tasks.every((t) => t.downstreamTaskIds.every((next: string) => ids.has(next)));
}
export function validWorkflowRun(v: unknown, ws: string, issuer: string, subject: string, expected?: { id?: string; intent?: WorkflowIntent; binding?: WorkflowBinding }): v is WorkflowRun {
  if (!object(v) || !keys(v, ['id', 'workspaceId', 'createdAt', 'updatedAt', 'revision', 'bindingId', 'policyVersion', 'dagId', 'dagVersion', 'bundleVersion', 'backendRunId', 'submittedBy', 'state', 'dispatchStarted', 'reconciliationComplete', 'message', 'observedAt', 'startedAt', 'endedAt', 'tasks', 'tasksTruncated']) || !id(v.id) || v.workspaceId !== ws || !date(v.createdAt) || !date(v.updatedAt) || !positive(v.revision) || !id(v.bindingId) || !text(v.policyVersion, 256) || !text(v.dagId, 250) || !positive(v.dagVersion) || !text(v.bundleVersion, 256) || !text(v.backendRunId, 250) || !object(v.submittedBy) || !keys(v.submittedBy, ['issuer', 'subject']) || v.submittedBy.issuer !== issuer || v.submittedBy.subject !== subject || !['queued', 'checking', 'submitting', 'running', 'succeeded', 'failed', 'unknown'].includes(String(v.state)) || typeof v.dispatchStarted !== 'boolean' || typeof v.reconciliationComplete !== 'boolean' || typeof v.message !== 'string' || new TextEncoder().encode(v.message).length > 4096 || !optionalDates(v, ['observedAt', 'startedAt', 'endedAt']) || typeof v.tasksTruncated !== 'boolean' || !Array.isArray(v.tasks) || v.tasks.length > 200) return false;
  if ((['succeeded', 'failed'].includes(String(v.state)) && !v.reconciliationComplete) || (v.reconciliationComplete && !['succeeded', 'failed', 'unknown'].includes(String(v.state))) || (v.state === 'succeeded' && !v.dispatchStarted)) return false;
  if (!v.tasks.every((t) => object(t) && keys(t, ['id', 'mapIndex', 'tryNumber', 'state', 'startedAt', 'endedAt']) && text(t.id, 250) && Number.isSafeInteger(t.mapIndex) && Number(t.mapIndex) >= -1 && Number.isSafeInteger(t.tryNumber) && Number(t.tryNumber) >= 0 && Number(t.tryNumber) <= 10000 && ['pending', 'removed', 'scheduled', 'queued', 'running', 'success', 'restarting', 'failed', 'up_for_retry', 'up_for_reschedule', 'upstream_failed', 'skipped', 'deferred'].includes(String(t.state)) && optionalDates(t, ['startedAt', 'endedAt'])) || new Set(v.tasks.map((t) => JSON.stringify([t.id, t.mapIndex]))).size !== v.tasks.length) return false;
  return (!expected?.id || v.id === expected.id) && (!expected?.intent || sameIntentProfile(v, expected.intent)) && (!expected?.binding || sameProfile(v, expected.binding));
}
export function validWorkflowHistory(v: unknown, binding: WorkflowBinding): v is WorkflowHistory {
  return object(v) && keys(v, ['items', 'observedAt', 'truncated']) && date(v.observedAt) && typeof v.truncated === 'boolean' && Array.isArray(v.items) && v.items.length <= 50 && v.items.every((r) => object(r) && keys(r, ['dagId', 'backendRunId', 'state', 'runType', 'dagVersion', 'bundleVersion', 'logicalDate', 'startedAt', 'endedAt']) && r.dagId === binding.dagId && text(r.backendRunId, 250) && ['queued', 'running', 'success', 'failed'].includes(String(r.state)) && ['manual', 'scheduled', 'backfill', 'asset_triggered'].includes(String(r.runType)) && positive(r.dagVersion) && text(r.bundleVersion, 256) && optionalDates(r, ['logicalDate', 'startedAt', 'endedAt'])) && new Set(v.items.map((r) => r.backendRunId)).size === v.items.length;
}
