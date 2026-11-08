import { request } from './api';
import { sameNamespace, validNamespace } from './catalog-api';

export interface SearchProfile { id: string; workspaceId: string; name: string; mode: 'lexical' | 'hybrid'; policyVersion: string; includeCellSource: boolean; dataTransferAccepted: boolean; embeddingModel?: string; dimensions?: number; vectorAvailable: boolean }
export interface SearchNotebook { id: string; revision: number }
export interface SearchCatalog { bindingId: string; namespace: string[]; name: string }
export interface SearchInput { bindingId: string; query: string; mode: 'lexical' | 'hybrid'; limit: number }
export interface SearchHit { documentId: string; kind: 'notebook' | 'catalog'; title: string; snippet: string; score: number; notebook?: SearchNotebook; catalog?: SearchCatalog & { policyVersion: string }; indexedAt: string; expiresAt?: string }
export interface SearchResult { bindingId: string; workspaceId: string; mode: 'lexical' | 'hybrid'; query: string; items: SearchHit[]; complete: true }
export interface IndexIntent { bindingId: string; idempotencyKey: string; notebook?: SearchNotebook; catalog?: SearchCatalog }
export interface IndexJob { id: string; workspaceId: string; bindingId: string; createdAt: string; updatedAt: string; revision: number; submittedBy: { issuer: string; subject: string }; policyVersion: string; notebook?: SearchNotebook; catalog?: SearchCatalog; state: 'queued' | 'indexing' | 'succeeded' | 'failed' | 'unknown' | 'superseded'; message: string; embeddingStarted: boolean; documentId?: string }
const base = (workspace: string) => `/workspaces/${encodeURIComponent(workspace)}/search`;
export const searchAPI = {
  profiles: (workspace: string, signal?: AbortSignal) => request<{ items: SearchProfile[] }>(`${base(workspace)}/status`, { signal }),
  search: (workspace: string, input: SearchInput, signal?: AbortSignal) => request<SearchResult>(base(workspace), { method: 'POST', body: JSON.stringify(input), signal }),
  index: (workspace: string, input: IndexIntent, signal?: AbortSignal) => request<IndexJob>(`${base(workspace)}/index`, { method: 'POST', body: JSON.stringify(input), signal }),
  job: (workspace: string, id: string, signal?: AbortSignal) => request<IndexJob>(`${base(workspace)}/index-jobs/${encodeURIComponent(id)}`, { signal }),
};
const record = (value: unknown): value is Record<string, unknown> => Boolean(value && typeof value === 'object' && !Array.isArray(value));
const id = (value: unknown): value is string => typeof value === 'string' && /^[a-z0-9][a-z0-9-]{0,62}$/.test(value);
const date = (value: unknown): value is string => typeof value === 'string' && Number.isFinite(Date.parse(value));
const notebook = (value: unknown): value is SearchNotebook => record(value) && id(value.id) && Number.isSafeInteger(value.revision) && Number(value.revision) >= 1;
const catalog = (value: unknown): value is SearchCatalog => record(value) && id(value.bindingId) && validNamespace(value.namespace) && typeof value.name === 'string' && value.name.length > 0;
export const indexTerminal = (state: IndexJob['state']) => ['succeeded', 'failed', 'unknown', 'superseded'].includes(state);
export function validSearchProfiles(value: unknown, workspace: string): value is { items: SearchProfile[] } {
  return record(value) && Array.isArray(value.items) && value.items.every((p) => record(p) && id(p.id) && p.workspaceId === workspace && typeof p.name === 'string' && typeof p.policyVersion === 'string' && Boolean(p.policyVersion) && ['lexical', 'hybrid'].includes(String(p.mode)) && ['includeCellSource', 'dataTransferAccepted', 'vectorAvailable'].every((key) => typeof p[key] === 'boolean') && (p.mode !== 'hybrid' || typeof p.embeddingModel === 'string' && Number.isSafeInteger(p.dimensions) && Number(p.dimensions) > 0));
}
export function validSearchResult(value: unknown, workspace: string, input: SearchInput): value is SearchResult {
  return record(value) && value.workspaceId === workspace && value.bindingId === input.bindingId && value.mode === input.mode && value.query === input.query && value.complete === true && Array.isArray(value.items) && value.items.length <= input.limit && value.items.every((h) => record(h) && id(h.documentId) && typeof h.title === 'string' && typeof h.snippet === 'string' && typeof h.score === 'number' && Number.isFinite(h.score) && date(h.indexedAt) && (h.expiresAt === undefined || date(h.expiresAt)) && (h.kind === 'notebook' && notebook(h.notebook) && h.catalog === undefined || h.kind === 'catalog' && catalog(h.catalog) && record(h.catalog) && typeof h.catalog.policyVersion === 'string' && h.notebook === undefined && date(h.expiresAt)));
}
export function validIndexIntent(value: unknown): value is IndexIntent {
  return record(value) && id(value.bindingId) && typeof value.idempotencyKey === 'string' && /^[A-Za-z0-9._:-]{8,128}$/.test(value.idempotencyKey) && (notebook(value.notebook) && value.catalog === undefined || catalog(value.catalog) && value.notebook === undefined);
}
export function validIndexJob(value: unknown, workspace: string, issuer: string, subject: string, expected?: { id?: string; intent?: IndexIntent }): value is IndexJob {
  if (!record(value) || !id(value.id) || value.workspaceId !== workspace || !id(value.bindingId) || expected?.id && value.id !== expected.id || !record(value.submittedBy) || value.submittedBy.issuer !== issuer || value.submittedBy.subject !== subject || !date(value.createdAt) || !date(value.updatedAt) || !Number.isSafeInteger(value.revision) || Number(value.revision) < 1 || typeof value.policyVersion !== 'string' || !value.policyVersion || !['queued', 'indexing', 'succeeded', 'failed', 'unknown', 'superseded'].includes(String(value.state)) || typeof value.message !== 'string' || typeof value.embeddingStarted !== 'boolean' || !(notebook(value.notebook) && value.catalog === undefined || catalog(value.catalog) && value.notebook === undefined)) return false;
  const intent = expected?.intent;
  return !intent || value.bindingId === intent.bindingId && (intent.notebook ? notebook(value.notebook) && value.notebook.id === intent.notebook.id && value.notebook.revision === intent.notebook.revision : catalog(value.catalog) && Boolean(intent.catalog) && value.catalog.bindingId === intent.catalog!.bindingId && value.catalog.name === intent.catalog!.name && sameNamespace(value.catalog.namespace, intent.catalog!.namespace));
}
export const indexStorageKey = (issuer: string, subject: string, workspace: string) => `dataslate.search.index.${JSON.stringify([issuer, subject, workspace])}`;
export function restoreIndex(key: string): { intent?: IndexIntent; jobId?: string; error?: string } {
  try {
    const raw = window.sessionStorage.getItem(key); if (!raw) return {};
    const value = JSON.parse(raw);
    if (!record(value) || value.intent !== undefined && !validIndexIntent(value.intent) || value.jobId !== undefined && !id(value.jobId) || !value.intent && !value.jobId) throw new Error('Invalid recovery entry');
    return { intent: value.intent as IndexIntent | undefined, jobId: value.jobId as string | undefined };
  } catch { return { error: 'Index recovery storage is unavailable or invalid. Repair browser storage before submitting another indexing request.' }; }
}
