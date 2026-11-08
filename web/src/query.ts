import { request } from './api';
import type { RunState } from './api';

export interface SqlContext { policyVersion: string; engine: 'trino'; catalog: string; schema: string; maxRunSeconds: number }
export interface SqlBinding extends SqlContext { id: string; workspaceId: string; name: string }
export interface QuerySelection { id: string; revision: number; cellId: string }
export interface QueryIntent { bindingId: string; idempotencyKey: string; notebook: QuerySelection }
export interface QuerySnapshot extends QuerySelection { sha256: string; bytes: number }
export interface QueryResultMetadata { reason?: string; rowCount: number; bytes: number; truncated: boolean; complete: boolean }
export interface SqlQuery {
  id: string; workspaceId: string; bindingId: string; submittedBy: { issuer: string; subject: string };
  revision: number; createdAt: string; updatedAt: string; state: RunState; submissionStarted: boolean; cancelRequested: boolean; cancelAccepted: boolean;
  engineQueryId?: string; message: string; notebook: QuerySnapshot; context: SqlContext; result: QueryResultMetadata;
}
export interface QueryResult extends QueryResultMetadata { queryId: string; workspaceId: string; columns: { name: string; type: string }[]; rows: (string | null)[][] }
const base = (workspaceId: string) => `/workspaces/${encodeURIComponent(workspaceId)}`;
export const queries = {
  bindings: (workspaceId: string, signal?: AbortSignal) => request<{ items: SqlBinding[] }>(`${base(workspaceId)}/sql-bindings`, { signal }),
  list: (workspaceId: string, signal?: AbortSignal) => request<{ items: SqlQuery[] }>(`${base(workspaceId)}/queries`, { signal }),
  get: (workspaceId: string, id: string, signal?: AbortSignal) => request<SqlQuery>(`${base(workspaceId)}/queries/${encodeURIComponent(id)}`, { signal }),
  submit: (workspaceId: string, input: QueryIntent, signal?: AbortSignal) => request<SqlQuery>(`${base(workspaceId)}/queries`, { method: 'POST', body: JSON.stringify(input), signal }),
  cancel: (workspaceId: string, id: string, signal?: AbortSignal) => request<SqlQuery>(`${base(workspaceId)}/queries/${encodeURIComponent(id)}/cancel`, { method: 'POST', body: '{}', signal }),
  result: (workspaceId: string, id: string, signal?: AbortSignal) => request<QueryResult>(`${base(workspaceId)}/queries/${encodeURIComponent(id)}/result`, { signal }),
};
const record = (value: unknown): value is Record<string, unknown> => Boolean(value && typeof value === 'object' && !Array.isArray(value));
const id = (value: unknown) => typeof value === 'string' && /^[a-z0-9][a-z0-9-]{0,62}$/.test(value);
const integer = (value: unknown, min = 0) => Number.isSafeInteger(value) && Number(value) >= min;
const selection = (value: unknown): value is QuerySelection => record(value) && id(value.id) && integer(value.revision, 1) && typeof value.cellId === 'string' && value.cellId.length > 0 && value.cellId.length <= 128;
const metadata = (value: unknown): value is QueryResultMetadata => record(value) && integer(value.rowCount) && integer(value.bytes) && typeof value.truncated === 'boolean' && typeof value.complete === 'boolean' && !(value.complete && value.truncated);
export const queryTerminal = (state: SqlQuery['state']) => ['succeeded', 'failed', 'cancelled'].includes(state);
export function validQuery(value: unknown, workspaceId: string, issuer: string, subject: string, expected?: { id?: string; intent?: QueryIntent }): value is SqlQuery {
  if (!record(value) || !id(value.id) || value.workspaceId !== workspaceId || !id(value.bindingId) || (expected?.id && value.id !== expected.id)) return false;
  if (!record(value.submittedBy) || value.submittedBy.issuer !== issuer || value.submittedBy.subject !== subject) return false;
  if (!integer(value.revision, 1) || !['createdAt', 'updatedAt'].every((key) => typeof value[key] === 'string' && Number.isFinite(Date.parse(String(value[key]))))) return false;
  if (!['queued', 'submitting', 'running', 'succeeded', 'failed', 'cancel_requested', 'cancelled', 'unknown'].includes(String(value.state)) || !['submissionStarted', 'cancelRequested', 'cancelAccepted'].every((key) => typeof value[key] === 'boolean') || typeof value.message !== 'string' || (value.engineQueryId !== undefined && typeof value.engineQueryId !== 'string')) return false;
  if (!selection(value.notebook) || !record(value.notebook) || typeof value.notebook.sha256 !== 'string' || !/^[a-f0-9]{64}$/.test(value.notebook.sha256) || !integer(value.notebook.bytes, 1) || !metadata(value.result)) return false;
  if (!record(value.context) || typeof value.context.policyVersion !== 'string' || !value.context.policyVersion || value.context.engine !== 'trino' || typeof value.context.catalog !== 'string' || typeof value.context.schema !== 'string' || !integer(value.context.maxRunSeconds, 1)) return false;
  const intent = expected?.intent;
  return !intent || (value.bindingId === intent.bindingId && value.notebook.id === intent.notebook.id && value.notebook.revision === intent.notebook.revision && value.notebook.cellId === intent.notebook.cellId);
}
export function validQueryResult(value: unknown, workspaceId: string, queryId: string): value is QueryResult {
  return record(value) && value.workspaceId === workspaceId && value.queryId === queryId && metadata(value) && Array.isArray(value.columns) && value.columns.length <= 1000 && value.columns.every((column) => record(column) && typeof column.name === 'string' && typeof column.type === 'string') && Array.isArray(value.rows) && value.rows.length === value.rowCount && value.rows.length <= 10000 && value.rows.every((row) => Array.isArray(row) && row.length === (value.columns as unknown[]).length && row.every((cell) => cell === null || typeof cell === 'string'));
}
export const queryIntentKey = (issuer: string, subject: string, workspaceId: string) => 'dataslate.query.intent.' + JSON.stringify([issuer, subject, workspaceId]);
export function restoreQueryIntent(key: string): { intent?: QueryIntent; error?: string } {
  try {
    const raw = window.sessionStorage.getItem(key); if (!raw) return {};
    const value = JSON.parse(raw);
    if (!record(value) || !id(value.bindingId) || typeof value.idempotencyKey !== 'string' || !/^[A-Za-z0-9._:-]{8,128}$/.test(value.idempotencyKey) || !selection(value.notebook)) throw new Error('Invalid query intent');
    return { intent: { bindingId: value.bindingId as string, idempotencyKey: value.idempotencyKey, notebook: { id: value.notebook.id, revision: value.notebook.revision, cellId: value.notebook.cellId } } };
  } catch { return { error: 'Query recovery storage is unavailable or invalid. Inspect your queries before repairing browser storage and submitting another request.' }; }
}
export function forgetQueryIntent(key: string, request: QueryIntent) {
  try {
    const raw = window.sessionStorage.getItem(key); if (!raw) return;
    const saved = JSON.parse(raw);
    if (saved?.bindingId === request.bindingId && saved?.idempotencyKey === request.idempotencyKey && saved?.notebook?.id === request.notebook.id && saved?.notebook?.revision === request.notebook.revision && saved?.notebook?.cellId === request.notebook.cellId) window.sessionStorage.removeItem(key);
  } catch { /* Preserve uncertain recovery rather than erasing another request. */ }
}

// Charts may approximate screen positions, but never silently change a stored numeric value.
// Compare normalized decimal spelling before using a JavaScript number; large integers,
// decimals that would round, infinities and underflow are rejected. Raw table cells stay strings.
function decimalForm(value: string): string | undefined {
  if (value.length > 1000) return undefined;
  const match = /^([+-]?)(\d+)(?:\.(\d*))?(?:[eE]([+-]?\d+))?$/.exec(value);
  if (!match) return undefined;
  let digits = (match[2] + (match[3] ?? '')).replace(/^0+/, '');
  if (!digits) return '0';
  let exponent = Number(match[4] ?? '0') - (match[3]?.length ?? 0);
  if (!Number.isSafeInteger(exponent) || Math.abs(exponent) > 10000) return undefined;
  while (digits.endsWith('0')) { digits = digits.slice(0, -1); exponent += 1; }
  return `${match[1] === '-' ? '-' : ''}${digits}e${exponent}`;
}
export const numericColumn = (type: string) => /^(?:tinyint|smallint|integer|int|bigint|real|float|double|decimal(?:\(\s*\d+\s*,\s*\d+\s*\))?|numeric(?:\(\s*\d+\s*,\s*\d+\s*\))?)$/i.test(type);
export function chartNumber(value: string): number | undefined {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || Math.abs(parsed) > Number.MAX_SAFE_INTEGER || decimalForm(value) === undefined || decimalForm(value) !== decimalForm(parsed.toString())) return undefined;
  return parsed;
}
