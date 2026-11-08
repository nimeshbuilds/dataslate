import { request } from './api';

export interface CatalogBinding { id: string; workspaceId: string; name: string; kind: 'polaris'; catalog: string; policyVersion: string; discoveryVerified: boolean; checkedAt?: string }
export interface TableIdentifier { namespace: string[]; name: string }
export interface CatalogCheck { bindingId: string; workspaceId: string; verified: true; checkedAt: string; capabilities: string[] }
export interface CatalogTable {
  bindingId: string; workspaceId: string; policyVersion: string; identifier: TableIdentifier;
  formatVersion: 1 | 2 | 3; uuid: string; lastUpdatedMs: number; currentSnapshotId?: string;
  currentSchema: { id: number; fields: { id: number; name: string; required: boolean; type: string | Record<string, unknown> }[] };
}
const base = (workspace: string, binding?: string) => `/workspaces/${encodeURIComponent(workspace)}/catalog-bindings${binding ? `/${encodeURIComponent(binding)}` : ''}`;
export const catalogs = {
  bindings: (workspace: string, signal?: AbortSignal) => request<{ items: CatalogBinding[] }>(base(workspace), { signal }),
  verify: (workspace: string, binding: string, signal?: AbortSignal) => request<CatalogCheck>(`${base(workspace, binding)}/verify`, { method: 'POST', body: '{}', signal }),
  namespaces: (workspace: string, binding: string, signal?: AbortSignal) => request<{ items: { namespace: string[] }[]; source: 'authorized-grants' }>(`${base(workspace, binding)}/namespaces`, { signal }),
  tables: (workspace: string, binding: string, namespace: string[], signal?: AbortSignal) => request<{ items: TableIdentifier[]; source: 'authorized-grants' }>(`${base(workspace, binding)}/tables?namespace=${encodeURIComponent(JSON.stringify(namespace))}`, { signal }),
  table: (workspace: string, binding: string, identifier: TableIdentifier, signal?: AbortSignal) => request<CatalogTable>(`${base(workspace, binding)}/table`, { method: 'POST', body: JSON.stringify(identifier), signal }),
};
const record = (value: unknown): value is Record<string, unknown> => Boolean(value && typeof value === 'object' && !Array.isArray(value));
const text = (value: unknown): value is string => typeof value === 'string' && value.length > 0;
const integer = (value: unknown) => Number.isSafeInteger(value) && Number(value) >= 0;
export const validNamespace = (value: unknown): value is string[] => Array.isArray(value) && value.length >= 1 && value.length <= 16 && value.every(text);
export const sameNamespace = (left: string[], right: string[]) => JSON.stringify(left) === JSON.stringify(right);
export function validBindings(value: unknown, workspace: string): value is { items: CatalogBinding[] } {
  return record(value) && Array.isArray(value.items) && value.items.every((b) => record(b) && text(b.id) && b.workspaceId === workspace && text(b.name) && b.kind === 'polaris' && text(b.catalog) && text(b.policyVersion) && typeof b.discoveryVerified === 'boolean' && (b.checkedAt === undefined || typeof b.checkedAt === 'string' && Number.isFinite(Date.parse(b.checkedAt))));
}
export function validCatalogTable(value: unknown, binding: CatalogBinding, identifier: TableIdentifier): value is CatalogTable {
  return record(value) && value.workspaceId === binding.workspaceId && value.bindingId === binding.id && value.policyVersion === binding.policyVersion && record(value.identifier) && validNamespace(value.identifier.namespace) && sameNamespace(value.identifier.namespace, identifier.namespace) && value.identifier.name === identifier.name && typeof value.formatVersion === 'number' && [1, 2, 3].includes(value.formatVersion) && text(value.uuid) && integer(value.lastUpdatedMs) && (value.currentSnapshotId === undefined || typeof value.currentSnapshotId === 'string') && record(value.currentSchema) && integer(value.currentSchema.id) && Array.isArray(value.currentSchema.fields) && value.currentSchema.fields.every((f) => record(f) && integer(f.id) && text(f.name) && typeof f.required === 'boolean' && (text(f.type) || record(f.type)));
}
