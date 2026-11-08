import type { ExecutionRun, NotebookSelection, PipelineSelection, RunIntent } from './api';

export function validRunReceipt(value: unknown, workspaceId: string, bindingId: string, issuer: string, subject: string, id?: string, notebook?: NotebookSelection, pipeline?: PipelineSelection): value is ExecutionRun {
  if (!value || typeof value !== 'object') return false;
  const run = value as Record<string, unknown>;
  const record = (item: unknown): item is Record<string, unknown> => Boolean(item && typeof item === 'object' && !Array.isArray(item));
  const timestamp = (item: unknown) => typeof item === 'string' && Number.isFinite(Date.parse(item));
  const state = (item: unknown) => typeof item === 'string' && ['queued', 'submitting', 'running', 'succeeded', 'failed', 'cancel_requested', 'cancelled', 'unknown'].includes(item);
  const positiveInteger = (item: unknown) => typeof item === 'number' && Number.isSafeInteger(item) && item > 0;
  if (typeof run.id !== 'string' || !/^[a-z0-9][a-z0-9-]{0,62}$/.test(run.id) || (id !== undefined && run.id !== id) || run.workspaceId !== workspaceId || run.bindingId !== bindingId) return false;
  if (!record(run.submittedBy) || run.submittedBy.issuer !== issuer || run.submittedBy.subject !== subject) return false;
  if (!positiveInteger(run.revision) || !timestamp(run.createdAt) || !timestamp(run.updatedAt) || (run.observedAt !== undefined && !timestamp(run.observedAt))) return false;
  if (!state(run.state) || typeof run.cancelRequested !== 'boolean' || typeof run.message !== 'string' || typeof run.kubernetesName !== 'string' || (run.kubernetesUid !== undefined && typeof run.kubernetesUid !== 'string')) return false;
  if (!record(run.profile) || typeof run.profile.operator !== 'string' || !['apache', 'kubeflow'].includes(run.profile.operator) || !positiveInteger(run.profile.executorInstances) || (run.profile.maxRunSeconds !== undefined && !positiveInteger(run.profile.maxRunSeconds))) return false;
  if (!['image', 'namespace', 'serviceAccount'].every((key) => typeof (run.profile as Record<string, unknown>)[key] === 'string')) return false;
  if (run.profile.executionKind !== undefined && run.profile.executionKind !== 'approved-app' && run.profile.executionKind !== 'notebook-python-batch' && run.profile.executionKind !== 'file-ingestion-batch') return false;
  if (run.profile.executionKind === 'notebook-python-batch') {
    if (pipeline || run.ingestion !== undefined) return false;
    if (!record(run.notebook) || typeof run.notebook.id !== 'string' || !positiveInteger(run.notebook.revision) || typeof run.notebook.sha256 !== 'string' || !/^[a-f0-9]{64}$/.test(run.notebook.sha256) || !Array.isArray(run.notebook.cellIds) || !run.notebook.cellIds.every((cell) => typeof cell === 'string') || !positiveInteger(run.notebook.bytes)) return false;
    if (notebook && (run.notebook.id !== notebook.id || run.notebook.revision !== notebook.revision)) return false;
  } else if (run.profile.executionKind === 'file-ingestion-batch') {
    if (!record(run.profile.ingestionStorage) || !['readPrefixes', 'writePrefixes'].every((key) => { const prefixes = (run.profile as Record<string, unknown>).ingestionStorage as Record<string, unknown>; return Array.isArray(prefixes[key]) && (prefixes[key] as unknown[]).length > 0 && (prefixes[key] as unknown[]).every((prefix) => typeof prefix === 'string'); })) return false;
    if (notebook || run.notebook !== undefined || typeof run.writerReleased !== 'boolean' || !record(run.ingestion) || typeof run.ingestion.id !== 'string' || !positiveInteger(run.ingestion.revision) || typeof run.ingestion.sha256 !== 'string' || !/^[a-f0-9]{64}$/.test(run.ingestion.sha256) || !positiveInteger(run.ingestion.bytes) || !['sourceUri', 'destinationUri', 'checkpointUri'].every((key) => typeof (run.ingestion as Record<string, unknown>)[key] === 'string')) return false;
    if (pipeline && (run.ingestion.id !== pipeline.id || run.ingestion.revision !== pipeline.revision)) return false;
  } else if (typeof run.profile.mainApplicationFile !== 'string' || notebook || pipeline || run.notebook !== undefined || run.ingestion !== undefined) return false;
  if (run.ingestionOutcome !== undefined && (!run.ingestion || !record(run.ingestionOutcome) || !['pending', 'verified', 'unavailable'].includes(String(run.ingestionOutcome.state)) || (run.ingestionOutcome.reasonCode !== undefined && typeof run.ingestionOutcome.reasonCode !== 'string'))) return false;
  if (run.sourceArtifact !== undefined && (!record(run.sourceArtifact) || typeof run.sourceArtifact.name !== 'string' || !['pending', 'ready', 'cleanup_pending', 'deleted'].includes(String(run.sourceArtifact.state)) || (run.sourceArtifact.uid !== undefined && typeof run.sourceArtifact.uid !== 'string'))) return false;
  return typeof run.eventsTruncated === 'boolean' && Array.isArray(run.events) && run.events.every((event) => record(event) && timestamp(event.at) && state(event.state) && typeof event.message === 'string');
}

export function forgetSubmission(key: string, request: RunIntent) {
  try {
    const raw = window.sessionStorage.getItem(key);
    if (!raw) return;
    const saved = JSON.parse(raw);
    // An older response must never erase a newer request created under the same identity scope.
    if (saved?.bindingId === request.bindingId && saved?.idempotencyKey === request.idempotencyKey && saved?.notebook?.id === request.notebook?.id && saved?.notebook?.revision === request.notebook?.revision && saved?.pipeline?.id === request.pipeline?.id && saved?.pipeline?.revision === request.pipeline?.revision) window.sessionStorage.removeItem(key);
  } catch { /* Retaining a scoped same-key retry is safer than losing recovery information. */ }
}

export function restoredSubmission(key: string): { intent?: RunIntent; error?: string } {
  try {
    const raw = window.sessionStorage.getItem(key);
    if (!raw) return {};
    const value = JSON.parse(raw);
    if (!value || typeof value.bindingId !== 'string' || !/^[a-z0-9][a-z0-9-]{0,62}$/.test(value.bindingId) || typeof value.idempotencyKey !== 'string' || !/^[A-Za-z0-9._:-]{8,128}$/.test(value.idempotencyKey)) throw new Error('invalid intent');
    if (value.notebook !== undefined && (!value.notebook || typeof value.notebook.id !== 'string' || !/^[a-z0-9][a-z0-9-]{0,62}$/.test(value.notebook.id) || !Number.isSafeInteger(value.notebook.revision) || value.notebook.revision < 1)) throw new Error('invalid notebook selection');
    if (value.pipeline !== undefined && (!value.pipeline || typeof value.pipeline.id !== 'string' || !/^[a-z0-9][a-z0-9-]{0,62}$/.test(value.pipeline.id) || !Number.isSafeInteger(value.pipeline.revision) || value.pipeline.revision < 1 || value.notebook !== undefined)) throw new Error('invalid pipeline selection');
    return { intent: { bindingId: value.bindingId, idempotencyKey: value.idempotencyKey, ...(value.notebook ? { notebook: { id: value.notebook.id, revision: value.notebook.revision } } : {}), ...(value.pipeline ? { pipeline: { id: value.pipeline.id, revision: value.pipeline.revision } } : {}) } };
  } catch { return { error: 'Submission recovery storage is unavailable or invalid. Check your existing runs before restoring browser storage and submitting again.' }; }
}

export function runIntentKey(issuer: string, subject: string, workspaceId: string, notebookId?: string) {
  return 'dataslate.run.intent.' + JSON.stringify([issuer, subject, workspaceId, ...(notebookId ? ['notebook', notebookId] : [])]);
}

export function ingestionIntentKey(issuer: string, subject: string, workspaceId: string) {
  return 'dataslate.run.intent.' + JSON.stringify([issuer, subject, workspaceId, 'ingestion']);
}
