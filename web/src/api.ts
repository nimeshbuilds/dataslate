export interface Workspace { id: string; name: string; slug: string; revision: number; createdAt: string; updatedAt: string }
export interface Cell { id: string; language: 'python' | 'sql'; source: string }
export interface Notebook { id: string; workspaceId: string; name: string; kind: 'notebook' | 'sql'; cells: Cell[]; revision: number; createdAt: string; updatedAt: string }
export interface Module { id: string; name: string; category: string; implementation: 'foundation' | 'planned'; description: string; dependencies: string[]; modes: ('managed' | 'external')[]; enabledByDefault: boolean }
export interface PlatformStatus { product: string; assistant: string; phase: string; authMode: string; persistence: string; executionEnabled: boolean }
export interface DeploymentPlan { cloud: string; namespace: string; actions: { moduleId: string; mode: string; action: string; verification: string }[]; warnings: string[]; executable: false }
export interface ManifestPreview { manifest: object; warnings: string[]; submitted: false }
export interface AuthConfig { mode: 'development' | 'oidc'; issuer?: string; clientId?: string; audience?: string; scopes?: string[] | string }

export class ApiError extends Error {
  constructor(public code: string, message: string, public status: number) { super(message); this.name = 'ApiError'; }
}

let getAccessToken: () => Promise<string | undefined> = async () => undefined;
export function configureAccessToken(provider: typeof getAccessToken) { getAccessToken = provider; }

export async function request<T>(path: string, init?: RequestInit, publicRequest = false): Promise<T> {
  const headers = new Headers(init?.headers);
  headers.set('Accept', 'application/json');
  if (init?.body) headers.set('Content-Type', 'application/json');
  if (!publicRequest) {
    const token = await getAccessToken();
    if (token) headers.set('Authorization', `Bearer ${token}`);
  }
  let response: Response;
  try { response = await fetch(`/api/v1${path}`, { ...init, headers, credentials: 'same-origin', signal: init?.signal ?? AbortSignal.timeout(20000) }); }
  catch { throw new ApiError('connection_unavailable', 'DataSlate could not reach the API. Your unsaved edits remain in this tab. Check the connection and try again.', 0); }
  const body = await response.json().catch(() => undefined);
  if (!response.ok) {
    if (response.status === 401 && !publicRequest) window.dispatchEvent(new Event('dataslate:unauthorized'));
    throw new ApiError(body?.error?.code ?? 'request_failed', body?.error?.message ?? `The API returned an unexpected response (${response.status}).`, response.status);
  }
  if (body === undefined) throw new ApiError('invalid_response', 'The API did not return a valid JSON response.', response.status);
  return body as T;
}

export const api = {
  authConfig: () => request<AuthConfig>('/auth/config', undefined, true),
  status: () => request<PlatformStatus>('/status'),
  workspaces: () => request<{ items: Workspace[] }>('/workspaces'),
  createWorkspace: (name: string, slug: string) => request<Workspace>('/workspaces', { method: 'POST', body: JSON.stringify({ name, slug }) }),
  notebooks: (workspaceId: string) => request<{ items: Notebook[] }>(`/workspaces/${encodeURIComponent(workspaceId)}/notebooks`),
  notebook: (workspaceId: string, id: string) => request<Notebook>(`/workspaces/${encodeURIComponent(workspaceId)}/notebooks/${encodeURIComponent(id)}`),
  saveNotebook: (workspaceId: string, draft: Pick<Notebook, 'name' | 'kind' | 'cells'>, id?: string, revision?: number) => request<Notebook>(`/workspaces/${encodeURIComponent(workspaceId)}/notebooks${id ? `/${encodeURIComponent(id)}` : ''}`, { method: id ? 'PUT' : 'POST', body: JSON.stringify({ ...draft, ...(id ? { revision } : {}) }) }),
  modules: () => request<{ items: Module[] }>('/modules'),
  plan: (namespace: string, modules: { id: string; mode: 'managed' | 'external'; endpoint?: string }[]) => request<DeploymentPlan>('/deployment-plans', { method: 'POST', body: JSON.stringify({ namespace, cloud: 'aws', modules }) }),
  preview: (workspaceId: string, body: { name: string; operator: string; image: string; mainApplicationFile: string; serviceAccount: string; namespace: string; executorInstances: number }) => request<ManifestPreview>(`/workspaces/${encodeURIComponent(workspaceId)}/spark-applications/preview`, { method: 'POST', body: JSON.stringify(body) }),
};

export function message(error: unknown): string { return error instanceof Error ? error.message : 'Something went wrong. Please try again.'; }
