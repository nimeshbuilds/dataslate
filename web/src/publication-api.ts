import { request } from './api';
import type { ExecutionRun } from './api';

export interface PublicationTarget { catalog: string; schema: string; table: string }
export interface PublicationBinding extends PublicationTarget { id: string; workspaceId: string; name: string; policyVersion: string; pipelineId: string; executionBindingId: string; maxRunSeconds: number }
export interface IngestionReceipt { version: 2; pipelineId: string; runId: string; pipelineRevision: number; receiptBindingSha256: string; state: 'succeeded'; runtime: { spark: string; delta: string }; contractSha256: string; queryId: string; completedBatches: string; processedFiles: string; processedRows: string; lastBatchId: string | null; tableExists: boolean; table: null | { id: string; version: string; schemaSha256: string; minReaderVersion: number; minWriterVersion: number; readerFeatures: string[]; writerFeatures: string[] } }
export interface IngestionOutcome { runId: string; workspaceId: string; state: 'pending' | 'verified' | 'unavailable'; reasonCode?: string; recordedAt?: string; receipt?: IngestionReceipt; evidence?: { applicationUid: string; driverUid: string; driverName: string; sourceSha256: string; receiptSha256: string } }
export interface PublicationIntent { runId: string; bindingId: string; idempotencyKey: string }
export interface TablePublication { id: string; workspaceId: string; createdAt: string; updatedAt: string; revision: number; bindingId: string; policyVersion: string; runId: string; submittedBy: { issuer: string; subject: string }; pipeline: { id: string; revision: number }; target: PublicationTarget; deltaVersion: string; state: 'queued' | 'checking' | 'registering' | 'verifying' | 'published' | 'failed' | 'unknown'; registrationStarted: boolean; reconciliationComplete: boolean; message: string; publishedTableId?: string; verifiedAt?: string }
export interface PublishedTable { id: string; workspaceId: string; name: string; bindingId: string; policyVersion: string; target: PublicationTarget; format: 'delta'; fields: { name: string; type: string; nullable: true }[]; verifiedDeltaVersion: string; verifiedAt: string; queryMode: 'latest'; sql: { bindingId: string; policyVersion: string; draft: string } }
export interface PublishedSqlSeed { tableId: string; workspaceId: string; nonce: string }
const base = (ws: string) => `/workspaces/${encodeURIComponent(ws)}`;
export const publications = {
  outcome: (ws: string, runId: string, signal?: AbortSignal) => request<IngestionOutcome>(`${base(ws)}/runs/${encodeURIComponent(runId)}/ingestion-outcome`, { signal }),
  bindings: (ws: string, signal?: AbortSignal) => request<{ items: PublicationBinding[] }>(`${base(ws)}/publication-bindings`, { signal }),
  list: (ws: string, signal?: AbortSignal) => request<{ items: TablePublication[] }>(`${base(ws)}/table-publications`, { signal }),
  get: (ws: string, id: string, signal?: AbortSignal) => request<TablePublication>(`${base(ws)}/table-publications/${encodeURIComponent(id)}`, { signal }),
  submit: (ws: string, input: PublicationIntent, signal?: AbortSignal) => request<{ publication: TablePublication; replayed: boolean }>(`${base(ws)}/table-publications`, { method: 'POST', body: JSON.stringify(input), signal }),
  tables: (ws: string, signal?: AbortSignal) => request<{ items: PublishedTable[] }>(`${base(ws)}/published-tables`, { signal }),
  table: (ws: string, id: string, signal?: AbortSignal) => request<PublishedTable>(`${base(ws)}/published-tables/${encodeURIComponent(id)}`, { signal }),
};
const object = (v: unknown): v is Record<string, unknown> => !!v && typeof v === 'object' && !Array.isArray(v);
const keys = (v: Record<string, unknown>, allowed: string[]) => Object.keys(v).every((k) => allowed.includes(k));
const text = (v: unknown, max = 2048): v is string => typeof v === 'string' && !!v.trim() && !v.includes('\0') && new TextEncoder().encode(v).length <= max;
const id = (v: unknown): v is string => typeof v === 'string' && /^[a-z0-9][a-z0-9-]{0,62}$/.test(v);
const positive = (v: unknown) => Number.isSafeInteger(v) && Number(v) > 0;
const date = (v: unknown) => typeof v === 'string' && Number.isFinite(Date.parse(v));
const hash = (v: unknown) => typeof v === 'string' && /^[a-f0-9]{64}$/.test(v);
const decimal = (v: unknown) => typeof v === 'string' && /^(0|[1-9][0-9]{0,39})$/.test(v);
const target = (v: unknown): v is PublicationTarget => object(v) && keys(v, ['catalog', 'schema', 'table']) && ['catalog', 'schema', 'table'].every((k) => text(v[k], 256));
const strings = (v: unknown) => Array.isArray(v) && v.length <= 100 && v.every((s) => text(s, 128));
export const sameTarget = (a: PublicationTarget, b: PublicationTarget) => a.catalog === b.catalog && a.schema === b.schema && a.table === b.table;
export const targetName = (t: PublicationTarget) => [t.catalog, t.schema, t.table].join(' / ');
export const publicationTerminal = (p: TablePublication) => p.reconciliationComplete && ['published', 'failed', 'unknown'].includes(p.state);
export function validPublicationBinding(v: unknown, ws: string): v is PublicationBinding { return object(v) && keys(v, ['id', 'workspaceId', 'name', 'policyVersion', 'pipelineId', 'executionBindingId', 'catalog', 'schema', 'table', 'maxRunSeconds']) && id(v.id) && v.workspaceId === ws && text(v.name, 200) && text(v.policyVersion, 256) && id(v.pipelineId) && id(v.executionBindingId) && ['catalog', 'schema', 'table'].every((k) => text(v[k], 256)) && positive(v.maxRunSeconds); }
export function validOutcome(v: unknown, ws: string, run: ExecutionRun): v is IngestionOutcome {
  if (!object(v) || v.runId !== run.id || v.workspaceId !== ws || !['pending', 'verified', 'unavailable'].includes(String(v.state)) || (v.reasonCode !== undefined && !text(v.reasonCode)) || (v.recordedAt !== undefined && !date(v.recordedAt))) return false;
  if (v.state !== 'verified') return v.receipt === undefined && v.evidence === undefined;
  const r = v.receipt, e = v.evidence;
  if (!run.ingestion || !object(r) || !object(e) || !date(v.recordedAt) || r.version !== 2 || r.state !== 'succeeded' || r.runId !== run.id || r.pipelineId !== run.ingestion.id || r.pipelineRevision !== run.ingestion.revision || !hash(r.receiptBindingSha256) || !hash(r.contractSha256) || !object(r.runtime) || !text(r.runtime.spark, 64) || !text(r.runtime.delta, 64) || !text(r.queryId, 128) || !['completedBatches', 'processedFiles', 'processedRows'].every((k) => decimal(r[k])) || !(r.lastBatchId === null || decimal(r.lastBatchId)) || typeof r.tableExists !== 'boolean') return false;
  if (!['applicationUid', 'driverUid', 'driverName'].every((k) => text(e[k], 256)) || e.applicationUid !== run.kubernetesUid || e.sourceSha256 !== run.ingestion.sha256 || !hash(e.receiptSha256)) return false;
  const t = r.table;
  return r.tableExists ? object(t) && text(t.id, 128) && decimal(t.version) && hash(t.schemaSha256) && positive(t.minReaderVersion) && positive(t.minWriterVersion) && strings(t.readerFeatures) && strings(t.writerFeatures) : t === null;
}
export function validPublication(v: unknown, ws: string, issuer: string, subject: string, expected?: { id?: string; intent?: PublicationIntent }): v is TablePublication {
  if (!object(v) || !id(v.id) || v.workspaceId !== ws || !date(v.createdAt) || !date(v.updatedAt) || !positive(v.revision) || !id(v.bindingId) || !text(v.policyVersion, 256) || !id(v.runId) || !object(v.submittedBy) || v.submittedBy.issuer !== issuer || v.submittedBy.subject !== subject || !object(v.pipeline) || !id(v.pipeline.id) || !positive(v.pipeline.revision) || !target(v.target) || !decimal(v.deltaVersion) || !['queued', 'checking', 'registering', 'verifying', 'published', 'failed', 'unknown'].includes(String(v.state)) || typeof v.registrationStarted !== 'boolean' || typeof v.reconciliationComplete !== 'boolean' || typeof v.message !== 'string') return false;
  if ((['published', 'failed'].includes(String(v.state)) && !v.reconciliationComplete) || (v.reconciliationComplete && !['published', 'failed', 'unknown'].includes(String(v.state)))) return false;
  if ((v.publishedTableId !== undefined && v.publishedTableId !== v.bindingId) || (v.verifiedAt !== undefined && !date(v.verifiedAt)) || (v.state === 'published' && (v.publishedTableId !== v.bindingId || !date(v.verifiedAt)))) return false;
  return (!expected?.id || v.id === expected.id) && (!expected?.intent || v.runId === expected.intent.runId && v.bindingId === expected.intent.bindingId);
}
export function validPublishedTable(v: unknown, ws: string, expectedId?: string): v is PublishedTable {
  // Reader responses must never carry publisher identities or physical lake locations.
  return object(v) && keys(v, ['id', 'workspaceId', 'name', 'bindingId', 'policyVersion', 'target', 'format', 'fields', 'verifiedDeltaVersion', 'verifiedAt', 'queryMode', 'sql']) && id(v.id) && (!expectedId || v.id === expectedId) && v.bindingId === v.id && v.workspaceId === ws && text(v.name, 200) && text(v.policyVersion, 256) && target(v.target) && v.format === 'delta' && Array.isArray(v.fields) && v.fields.length > 0 && v.fields.length <= 200 && v.fields.every((f) => object(f) && keys(f, ['name', 'type', 'nullable']) && text(f.name, 128) && ['string', 'long', 'double', 'boolean', 'date'].includes(String(f.type)) && f.nullable === true) && new Set(v.fields.map((f) => f.name)).size === v.fields.length && decimal(v.verifiedDeltaVersion) && date(v.verifiedAt) && v.queryMode === 'latest' && object(v.sql) && keys(v.sql, ['bindingId', 'policyVersion', 'draft']) && id(v.sql.bindingId) && text(v.sql.policyVersion, 256) && text(v.sql.draft, 65536);
}
export function validPublicationIntent(v: unknown): v is PublicationIntent { return object(v) && keys(v, ['runId', 'bindingId', 'idempotencyKey']) && id(v.runId) && id(v.bindingId) && typeof v.idempotencyKey === 'string' && /^[A-Za-z0-9._:-]{8,128}$/.test(v.idempotencyKey); }
export const publicationStorageKey = (ws: string, issuer: string, subject: string) => `dataslate.publication.intent.${JSON.stringify([issuer, subject, ws])}`;
export function restorePublication(key: string): { intent?: PublicationIntent; error?: string } { try { const raw = sessionStorage.getItem(key); if (!raw) return {}; const value = JSON.parse(raw); if (!validPublicationIntent(value)) throw new Error('Invalid record'); return { intent: value }; } catch { return { error: 'Publication recovery is unavailable or invalid. Inspect your operations before repairing browser storage.' }; } }
export function forgetPublication(key: string, expected: PublicationIntent) { const raw = sessionStorage.getItem(key); if (!raw) return; const v = JSON.parse(raw); if (validPublicationIntent(v) && v.runId === expected.runId && v.bindingId === expected.bindingId && v.idempotencyKey === expected.idempotencyKey) sessionStorage.removeItem(key); }
