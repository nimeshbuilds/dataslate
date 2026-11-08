export interface Workspace { id: string; name: string; slug: string; revision: number; createdAt: string; updatedAt: string }
export interface Cell { id: string; language: 'python' | 'sql'; source: string }
export interface Notebook { id: string; workspaceId: string; name: string; kind: 'notebook' | 'sql'; cells: Cell[]; revision: number; createdAt: string; updatedAt: string }
export interface Module { id: string; name: string; category: string; implementation: 'foundation' | 'planned'; description: string; dependencies: string[]; modes: ('managed' | 'external')[]; enabledByDefault: boolean }
export interface PlatformStatus { product: string; assistant: string; phase: string; authMode: string; persistence: string; executionEnabled: boolean; notebookExecutionEnabled?: boolean; ingestionExecutionEnabled?: boolean; sqlExecutionEnabled?: boolean }
export interface DeploymentPlan { cloud: string; namespace: string; actions: { moduleId: string; mode: string; action: string; verification: string }[]; warnings: string[]; executable: false }
export interface ManifestPreview { manifest: object; warnings: string[]; submitted: false }
export interface AuthConfig { mode: 'development' | 'oidc'; issuer?: string; clientId?: string; audience?: string; scopes?: string[] | string }
export interface ExecutionProfile { operator: 'apache' | 'kubeflow'; image: string; mainApplicationFile?: string; namespace: string; serviceAccount: string; executorInstances: number; maxRunSeconds?: number; executionKind?: 'approved-app' | 'notebook-python-batch' | 'file-ingestion-batch'; ingestionStorage?: { readPrefixes: string[]; writePrefixes: string[] } }
export interface ExecutionBinding extends ExecutionProfile { id: string; workspaceId: string; name: string }
export type RunState = 'queued' | 'submitting' | 'running' | 'succeeded' | 'failed' | 'cancel_requested' | 'cancelled' | 'unknown';
export interface RunEvent { at: string; state: RunState; message: string }
export interface NotebookSelection { id: string; revision: number }
export interface PipelineSelection { id: string; revision: number }
export interface IngestionSnapshot extends PipelineSelection { sha256: string; bytes: number; sourceUri: string; destinationUri: string; checkpointUri: string }
export interface IngestionField { name: string; type: 'string' | 'long' | 'double' | 'boolean' | 'timestamp' | 'date'; nullable: boolean }
export interface IngestionDraft { name: string; source: { format: 'json' | 'parquet'; uri: string; schema: IngestionField[] }; destination: { uri: string }; checkpointUri: string; maxFilesPerTrigger: number }
export interface IngestionPipeline extends IngestionDraft { id: string; workspaceId: string; revision: number; createdAt: string; updatedAt: string }
export interface NotebookSnapshot extends NotebookSelection { sha256: string; cellIds: string[]; bytes: number }
export interface ExecutionRun {
  id: string; workspaceId: string; bindingId: string; revision: number; createdAt: string; updatedAt: string; observedAt?: string;
  submittedBy: { issuer: string; subject: string }; profile: ExecutionProfile; state: RunState; cancelRequested: boolean;
  kubernetesName: string; kubernetesUid?: string; message: string; events: RunEvent[]; eventsTruncated: boolean;
  notebook?: NotebookSnapshot; ingestion?: IngestionSnapshot; writerReleased?: boolean; sourceArtifact?: { name: string; uid?: string; state: 'pending' | 'ready' | 'cleanup_pending' | 'deleted' };
}
export interface RunIntent { bindingId: string; idempotencyKey: string; notebook?: NotebookSelection; pipeline?: PipelineSelection }
export interface RunLogs { text: string; truncated: boolean }

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
  if (response.status === 204 && response.ok && init?.method === 'DELETE') return undefined as T;
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
  notebooks: (workspaceId: string, signal?: AbortSignal) => request<{ items: Notebook[] }>(`/workspaces/${encodeURIComponent(workspaceId)}/notebooks`, { signal }),
  notebook: (workspaceId: string, id: string, signal?: AbortSignal) => request<Notebook>(`/workspaces/${encodeURIComponent(workspaceId)}/notebooks/${encodeURIComponent(id)}`, { signal }),
  saveNotebook: (workspaceId: string, draft: Pick<Notebook, 'name' | 'kind' | 'cells'>, id?: string, revision?: number) => request<Notebook>(`/workspaces/${encodeURIComponent(workspaceId)}/notebooks${id ? `/${encodeURIComponent(id)}` : ''}`, { method: id ? 'PUT' : 'POST', body: JSON.stringify({ ...draft, ...(id ? { revision } : {}) }) }),
  ingestionPipelines: (workspaceId: string, signal?: AbortSignal) => request<{ items: IngestionPipeline[] }>(`/workspaces/${encodeURIComponent(workspaceId)}/ingestion-pipelines`, { signal }),
  ingestionPipeline: (workspaceId: string, id: string, signal?: AbortSignal) => request<IngestionPipeline>(`/workspaces/${encodeURIComponent(workspaceId)}/ingestion-pipelines/${encodeURIComponent(id)}`, { signal }),
  saveIngestionPipeline: (workspaceId: string, draft: IngestionDraft, id?: string, revision?: number, signal?: AbortSignal) => request<IngestionPipeline>(`/workspaces/${encodeURIComponent(workspaceId)}/ingestion-pipelines${id ? `/${encodeURIComponent(id)}` : ''}`, { method: id ? 'PUT' : 'POST', body: JSON.stringify({ ...draft, ...(id ? { revision } : {}) }), signal }),
  deleteIngestionPipeline: (workspaceId: string, id: string, revision: number, signal?: AbortSignal) => request<void>(`/workspaces/${encodeURIComponent(workspaceId)}/ingestion-pipelines/${encodeURIComponent(id)}?revision=${encodeURIComponent(revision)}`, { method: 'DELETE', signal }),
  modules: () => request<{ items: Module[] }>('/modules'),
  plan: (namespace: string, modules: { id: string; mode: 'managed' | 'external'; endpoint?: string }[]) => request<DeploymentPlan>('/deployment-plans', { method: 'POST', body: JSON.stringify({ namespace, cloud: 'aws', modules }) }),
  preview: (workspaceId: string, body: { name: string; operator: string; image: string; mainApplicationFile: string; serviceAccount: string; namespace: string; executorInstances: number; maxRunSeconds?: number }) => request<ManifestPreview>(`/workspaces/${encodeURIComponent(workspaceId)}/spark-applications/preview`, { method: 'POST', body: JSON.stringify(body) }),
  executionBindings: (workspaceId: string, signal?: AbortSignal) => request<{ items: ExecutionBinding[] }>(`/workspaces/${encodeURIComponent(workspaceId)}/execution-bindings`, { signal }),
  runs: (workspaceId: string, signal?: AbortSignal) => request<{ items: ExecutionRun[] }>(`/workspaces/${encodeURIComponent(workspaceId)}/runs`, { signal }),
  run: (workspaceId: string, id: string, signal?: AbortSignal) => request<ExecutionRun>(`/workspaces/${encodeURIComponent(workspaceId)}/runs/${encodeURIComponent(id)}`, { signal }),
  submitRun: (workspaceId: string, intent: RunIntent, signal?: AbortSignal) => request<ExecutionRun>(`/workspaces/${encodeURIComponent(workspaceId)}/runs`, { method: 'POST', body: JSON.stringify(intent), signal }),
  cancelRun: (workspaceId: string, id: string, signal?: AbortSignal) => request<ExecutionRun>(`/workspaces/${encodeURIComponent(workspaceId)}/runs/${encodeURIComponent(id)}/cancel`, { method: 'POST', body: '{}', signal }),
  runLogs: (workspaceId: string, id: string, signal?: AbortSignal) => request<RunLogs>(`/workspaces/${encodeURIComponent(workspaceId)}/runs/${encodeURIComponent(id)}/logs`, { signal }),
};

export function message(error: unknown): string { return error instanceof Error ? error.message : 'Something went wrong. Please try again.'; }
