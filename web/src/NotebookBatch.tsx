import { useEffect, useRef, useState } from 'react';
import { ArrowRight, Play, RefreshCw, ShieldCheck } from 'lucide-react';
import { api, ApiError, message } from './api';
import type { Cell, ExecutionBinding, ExecutionRun, RunIntent } from './api';
import { useAuth } from './auth';
import { forgetSubmission, restoredSubmission, runIntentKey, validRunReceipt } from './execution';
import { ExecutionProfileView } from './Runs';
import { ErrorNotice, Modal, Pill, Spinner } from './ui';

interface BatchDocument { id?: string; revision?: number; name: string; kind: 'notebook' | 'sql'; cells: Cell[] }
interface BatchProps {
  workspaceId: string; document: BatchDocument; blockedReason: string;
  onUnresolved: (unresolved: boolean) => void; onViewRun: (id: string) => void; onRevisionConflict: () => void;
}

export function NotebookBatch(props: BatchProps) {
  const auth = useAuth();
  if (!auth.authenticated || !props.document.id || !props.document.revision) return <section className="notebook-batch" aria-label="Notebook batch execution"><div><span className="eyebrow">PYTHON BATCH</span><strong>Run a saved revision.</strong><p>{!auth.authenticated ? 'Sign in again to inspect bindings or submit a batch.' : 'Save a Python notebook before choosing an engineering binding.'}</p></div><button className="button secondary compact" disabled><Play size={13} />Run saved revision as batch</button></section>;
  return <SavedNotebookBatch key={JSON.stringify([auth.config?.issuer, auth.user?.profile.sub, props.workspaceId, props.document.id])} {...props} document={{ ...props.document, id: props.document.id, revision: props.document.revision }} />;
}

function SavedNotebookBatch({ workspaceId, document, blockedReason, onUnresolved, onViewRun, onRevisionConflict }: BatchProps & { document: BatchDocument & { id: string; revision: number } }) {
  const auth = useAuth();
  const issuer = auth.config?.issuer ?? 'development';
  const subject = auth.user?.profile.sub ?? 'development-admin';
  const storageKey = runIntentKey(issuer, subject, workspaceId, document.id);
  const [restored] = useState(() => {
    const recovery = restoredSubmission(storageKey);
    if (recovery.intent && recovery.intent.notebook?.id !== document.id) return { error: 'The saved recovery request does not match this notebook. Check your runs before repairing browser recovery storage.' };
    return recovery;
  });
  const [intent, setIntent] = useState<RunIntent | undefined>(restored.intent);
  const [bindings, setBindings] = useState<ExecutionBinding[]>([]);
  const [bindingId, setBindingId] = useState('');
  const [bindingError, setBindingError] = useState('');
  const [submissionError, setSubmissionError] = useState(restored.error ?? '');
  const [loading, setLoading] = useState(true);
  const [refresh, setRefresh] = useState(0);
  const [dialog, setDialog] = useState(false);
  const [pending, setPending] = useState(false);
  const [accepted, setAccepted] = useState<ExecutionRun>();
  const lifecycle = useRef<AbortController | undefined>(undefined);
  const inFlight = useRef(false);
  const accessEpoch = useRef(0);
  const effectiveBindingId = intent?.bindingId ?? bindingId;
  const binding = bindings.find((item) => item.id === effectiveBindingId);
  const pythonOnly = document.kind === 'notebook' && document.cells.length > 0 && document.cells.every((cell) => cell.language === 'python');
  const blocked = blockedReason || (!pythonOnly ? 'Batch execution supports Python-only notebooks. SQL and mixed-language cells require another execution engine.' : '');
  const selectedRevision = intent?.notebook?.revision ?? document.revision;

  useEffect(() => {
    const controller = new AbortController(); lifecycle.current = controller;
    return () => controller.abort();
  }, []);
  useEffect(() => { onUnresolved(Boolean(intent)); return () => onUnresolved(false); }, [intent, onUnresolved]);
  useEffect(() => {
    const controller = new AbortController(); setLoading(true); setBindingError('');
    api.executionBindings(workspaceId, AbortSignal.any([controller.signal, AbortSignal.timeout(20000)])).then((result) => {
      if (controller.signal.aborted) return;
      const next = result.items.filter((item) => item.executionKind === 'notebook-python-batch');
      setBindings(next); setBindingId((current) => next.some((item) => item.id === current) ? current : next[0]?.id ?? '');
    }).catch((error) => { if (!controller.signal.aborted) { setBindings([]); setBindingError(message(error)); setAccepted(undefined); if (error instanceof ApiError && [401, 403, 404].includes(error.status)) { accessEpoch.current += 1; setDialog(false); } } })
      .finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, [workspaceId, refresh]);

  async function submit() {
    if (inFlight.current || (!intent && (!binding || blocked)) || restored.error) return;
    const request: RunIntent = intent ?? { bindingId: binding!.id, idempotencyKey: crypto.randomUUID(), notebook: { id: document.id, revision: document.revision } };
    try { window.sessionStorage.setItem(storageKey, JSON.stringify(request)); }
    catch { setSubmissionError('Browser recovery storage is unavailable. No request was sent. Restore session storage before submitting.'); return; }
    const recovering = Boolean(intent); const controller = lifecycle.current!;
    const epoch = accessEpoch.current;
    setIntent(request); setPending(true); inFlight.current = true; setSubmissionError(''); setAccepted(undefined);
    try {
      const run = await api.submitRun(workspaceId, request, AbortSignal.any([controller.signal, AbortSignal.timeout(20000)]));
      if (!validRunReceipt(run, workspaceId, request.bindingId, issuer, subject, undefined, request.notebook)) throw new ApiError('invalid_response', 'The API returned an invalid notebook run confirmation. Recover using the same saved revision and request key.', 200);
      forgetSubmission(storageKey, request);
      if (controller.signal.aborted || epoch !== accessEpoch.current) return;
      setIntent(undefined); setAccepted(run); setDialog(false);
    } catch (error) {
      if (controller.signal.aborted || epoch !== accessEpoch.current) return;
      setSubmissionError(message(error));
      if (!recovering && error instanceof ApiError && ([400, 401, 403, 404, 422, 429, 501].includes(error.status) || error.code === 'notebook_revision_conflict')) {
        forgetSubmission(storageKey, request); setIntent(undefined);
        if (error.code === 'notebook_revision_conflict') onRevisionConflict();
      }
    } finally { inFlight.current = false; if (!controller.signal.aborted) setPending(false); }
  }

  return <section className="notebook-batch" aria-label="Notebook batch execution"><div className="notebook-batch-intro"><span className="eyebrow">PYTHON BATCH</span><strong>{intent ? `Recover saved revision ${selectedRevision}` : `Saved revision ${document.revision}`}</strong><p>{loading ? 'Checking your engineering bindings…' : intent ? 'An earlier submission is unresolved. Recover its exact saved revision before starting another batch of this notebook.' : blocked || (!bindings.length ? 'No Python engineering binding is granted to your identity in this workspace.' : 'A fresh Spark process runs this saved revision. Interactive cell state and rich outputs are not available.')}</p></div><button className="button secondary compact" disabled={loading || pending || Boolean(restored.error) || Boolean(bindingError) || (!intent && (Boolean(blocked) || !binding))} onClick={() => setDialog(true)}>{intent ? <><RefreshCw size={13} />Recover batch submission</> : <><Play size={13} />Run saved revision as batch</>}</button>
    {bindingError && <div className="notebook-batch-wide"><ErrorNotice retry={() => setRefresh((value) => value + 1)}>{bindingError}</ErrorNotice></div>}
    {submissionError && !dialog && <div className="notebook-batch-wide"><ErrorNotice>{submissionError}</ErrorNotice></div>}
    {intent && !pending && <div className="notebook-batch-wide notice warning" role="status"><div><strong>Batch submission outcome is unresolved.</strong><p>Notebook {intent.notebook?.id} · saved revision {selectedRevision}. Recovery never substitutes your current edits.</p><code className="run-key">{intent.idempotencyKey}</code></div></div>}
    {accepted && <div className="notebook-batch-wide batch-accepted" role="status"><div><Pill tone="green">Run accepted</Pill><p>Saved revision {accepted.notebook?.revision} · <code>{accepted.id}</code></p><small>Acceptance is recorded. Follow the run for observed execution status and driver logs.</small></div><button className="button secondary compact" onClick={() => onViewRun(accepted.id)}>View batch run <ArrowRight size={13} /></button></div>}
    {dialog && <Modal title={intent ? 'Recover notebook batch' : 'Review saved Python batch'} onClose={() => { if (!pending) setDialog(false); }}><div className="modal-body"><Pill>Saved revision {selectedRevision}</Pill><p><strong>{document.name}</strong><br /><span className="muted">Notebook {document.id}</span></p>{intent && selectedRevision !== document.revision && <div className="notice warning"><p>Recovery refers to revision {selectedRevision}. The current saved revision {document.revision} and any unsaved edits are not part of this request.</p></div>}
      <label className="field">Python engineering binding<select value={effectiveBindingId} disabled={Boolean(intent) || pending} onChange={(event) => setBindingId(event.target.value)}>{intent && !binding && <option value={intent.bindingId}>{intent.bindingId} (unresolved request)</option>}{bindings.map((item) => <option value={item.id} key={item.id}>{item.name}</option>)}</select></label>
      {binding && <ExecutionProfileView profile={binding} />}
      <div className="notice warning"><ShieldCheck size={17} /><p>This binding runs arbitrary Python using its displayed workload identity and storage permissions. It is an engineering runtime. A batch uses a fresh process; it does not provide interactive cell results or enforce row-level policies on raw storage access.</p></div>
      {intent && <p className="run-launch-note">Recovery reuses request key <code>{intent.idempotencyKey}</code>. If already accepted, the existing run keeps its original runtime and saved source snapshot.</p>}
      {blocked && !intent && <ErrorNotice>{blocked}</ErrorNotice>}{submissionError && <ErrorNotice>{submissionError}</ErrorNotice>}
      <div className="form-actions"><button className="button secondary" disabled={pending} onClick={() => setDialog(false)}>Close</button><button className="button primary" disabled={pending || Boolean(restored.error) || (!intent && (!binding || Boolean(blocked)))} onClick={() => void submit()}>{pending ? <Spinner label="Confirming batch…" /> : intent ? 'Check batch submission' : 'Submit saved revision'}</button></div>
    </div></Modal>}
  </section>;
}
