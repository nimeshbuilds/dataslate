import { useEffect, useRef, useState } from 'react';
import { Activity, Boxes, Clock3, FileCode2, History, Play, RefreshCw, ShieldCheck, Square, Terminal } from 'lucide-react';
import { api, ApiError, message } from './api';
import type { ExecutionBinding, ExecutionProfile, ExecutionRun, RunIntent, RunLogs, RunState, Workspace } from './api';
import { SparkPreview } from './Platform';
import { useAuth } from './auth';
import { forgetSubmission, restoredSubmission, runIntentKey, validRunReceipt } from './execution';
import { EmptyState, ErrorNotice, Modal, Pill, Spinner } from './ui';

const terminal = (state: RunState) => ['succeeded', 'failed', 'cancelled'].includes(state);
const needsObservation = (run: ExecutionRun) => !terminal(run.state) || Boolean(run.sourceArtifact && run.sourceArtifact.state !== 'deleted') || Boolean(run.ingestion && !run.writerReleased);
const stateLabel = (state: RunState) => state.replaceAll('_', ' ');
function date(value?: string) { return value ? new Date(value).toLocaleString() : 'Not observed yet'; }

export function ComputeWorkspace({ workspace, onDirty, initialRunId }: { workspace?: Workspace; onDirty: (dirty: boolean) => void; initialRunId?: string }) {
  const auth = useAuth();
  const [tab, setTab] = useState<'runs' | 'preview'>('runs');
  return <div className="compute-page"><div className="page-tabs" role="tablist" aria-label="Compute">
    <button id="runs-tab" role="tab" aria-controls="runs-panel" aria-selected={tab === 'runs'} onClick={() => setTab('runs')}>Application runs</button>
    <button id="preview-tab" role="tab" aria-controls="preview-panel" aria-selected={tab === 'preview'} onClick={() => setTab('preview')}>Manifest preview</button>
  </div><section id="runs-panel" role="tabpanel" aria-labelledby="runs-tab" hidden={tab !== 'runs'}>
    {workspace ? <Runs key={JSON.stringify([auth.config?.issuer, auth.user?.profile.sub, workspace.id])} workspace={workspace} active={tab === 'runs'} onDirty={onDirty} initialRunId={initialRunId} /> : <EmptyState icon={<Boxes size={27} />} title="Select a workspace first" description="Your application runs and approved execution bindings belong to a workspace." />}
  </section><section id="preview-panel" role="tabpanel" aria-labelledby="preview-tab" hidden={tab !== 'preview'}>{tab === 'preview' && <SparkPreview key={workspace?.id} workspace={workspace} />}</section></div>;
}

function Runs({ workspace, active, onDirty, initialRunId }: { workspace: Workspace; active: boolean; onDirty: (dirty: boolean) => void; initialRunId?: string }) {
  const auth = useAuth();
  const memberships = auth.user?.profile.dataslate_workspaces;
  const role = memberships && typeof memberships === 'object' ? (memberships as Record<string, unknown>)[workspace.id] : undefined;
  const canCancel = auth.config?.mode === 'development' || role === 'editor' || role === 'admin';
  const [bindings, setBindings] = useState<ExecutionBinding[]>([]);
  const [bindingId, setBindingId] = useState('');
  const [runs, setRuns] = useState<ExecutionRun[]>([]);
  const [selectedId, setSelectedId] = useState(initialRunId ?? '');
  const [selected, setSelected] = useState<ExecutionRun>();
  const [loading, setLoading] = useState(true);
  const [refresh, setRefresh] = useState(0);
  const [error, setError] = useState('');
  const issuer = auth.config?.issuer ?? 'development';
  const subject = auth.user?.profile.sub ?? 'development-admin';
  const storageKey = runIntentKey(issuer, subject, workspace.id);
  const [restored] = useState(() => restoredSubmission(storageKey));
  const [submissionError, setSubmissionError] = useState(restored.error ?? '');
  const [pending, setPending] = useState(false);
  const [intent, setIntent] = useState<RunIntent | undefined>(restored.intent);
  const [confirmation, setConfirmation] = useState<ExecutionRun>();
  const [cancelling, setCancelling] = useState(false);
  const [cancelError, setCancelError] = useState('');
  const [logs, setLogs] = useState<RunLogs>();
  const [logError, setLogError] = useState('');
  const [logsPending, setLogsPending] = useState(false);
  const lifecycle = useRef<AbortController | undefined>(undefined);
  const inFlight = useRef(false);
  const logRequest = useRef<AbortController | undefined>(undefined);
  const accessEpoch = useRef(0);
  const details = useRef<HTMLElement>(null);
  const effectiveBindingId = intent?.bindingId ?? bindingId;
  const binding = bindings.find((item) => item.id === effectiveBindingId);

  useEffect(() => {
    const controller = new AbortController(); lifecycle.current = controller;
    return () => { controller.abort(); logRequest.current?.abort(); };
  }, []);
  useEffect(() => { onDirty(Boolean(intent)); return () => onDirty(false); }, [intent, onDirty]);
  useEffect(() => {
    if (initialRunId && selected?.id === initialRunId) { details.current?.focus({ preventScroll: true }); details.current?.scrollIntoView({ block: 'start' }); }
  }, [initialRunId, selected?.id]);
  useEffect(() => {
    setLogs(undefined); setLogError(''); setLogsPending(false); logRequest.current?.abort();
  }, [selectedId]);

  useEffect(() => {
    if (!active) return;
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    async function load() {
      try {
        // Polling reads server state only. Submission and cancellation always require an explicit action.
        const [nextBindings, nextRuns, nextSelected] = await Promise.all([
          api.executionBindings(workspace.id, AbortSignal.any([controller.signal, AbortSignal.timeout(20000)])),
          api.runs(workspace.id, AbortSignal.any([controller.signal, AbortSignal.timeout(20000)])),
          selectedId ? api.run(workspace.id, selectedId, AbortSignal.any([controller.signal, AbortSignal.timeout(20000)])) : Promise.resolve(undefined),
        ]);
        if (controller.signal.aborted) return;
        const approvedBindings = nextBindings.items.filter((item) => !item.executionKind || item.executionKind === 'approved-app');
        setBindings(approvedBindings); setRuns(nextRuns.items); setSelected(nextSelected); setError('');
        setBindingId((current) => approvedBindings.some((item) => item.id === current) ? current : approvedBindings[0]?.id ?? '');
        if (nextRuns.items.some(needsObservation) || (nextSelected && needsObservation(nextSelected))) timer = setTimeout(() => void load(), 5000);
      } catch (err) {
        if (controller.signal.aborted) return;
        setError(message(err));
        if (err instanceof ApiError && [401, 403, 404].includes(err.status)) {
          accessEpoch.current += 1;
          logRequest.current?.abort();
          setBindings([]); setRuns([]); setSelected(undefined); setSelectedId(''); setLogs(undefined); setLogsPending(false); setConfirmation(undefined);
        }
        // A failed observation is not a new run state. Leave the saved timestamp visible.
        timer = setTimeout(() => void load(), 10000);
      } finally { if (!controller.signal.aborted) setLoading(false); }
    }
    void load();
    return () => { controller.abort(); clearTimeout(timer); };
  }, [workspace.id, selectedId, refresh, active]);

  async function submit() {
    if (inFlight.current || (!intent && !binding)) return;
    // Retain precisely this request across timeout, disconnect and ambiguous server errors.
    // A retry asks the API to reconcile the same operation, never to launch another job.
    const request = intent ?? { bindingId: binding!.id, idempotencyKey: crypto.randomUUID() };
    // Store only the request identity, never tokens, source, logs or server-owned resources.
    // Refuse a new POST if the recovery key cannot survive a reload in this tab.
    try { window.sessionStorage.setItem(storageKey, JSON.stringify(request)); }
    catch { setSubmissionError('Browser recovery storage is unavailable. No request was sent. Restore session storage before submitting.'); return; }
    const recovering = Boolean(intent);
    setIntent(request); inFlight.current = true; setPending(true); setSubmissionError('');
    const controller = lifecycle.current!;
    const epoch = accessEpoch.current;
    try {
      const run = await api.submitRun(workspace.id, request, AbortSignal.any([controller.signal, AbortSignal.timeout(20000)]));
      if (!validRunReceipt(run, workspace.id, request.bindingId, issuer, subject)) throw new ApiError('invalid_response', 'The API returned an invalid run confirmation. Recover using the same request key.', 200);
      // A confirmed receipt may safely clear this scope even after navigation or revocation.
      forgetSubmission(storageKey, request);
      if (controller.signal.aborted || epoch !== accessEpoch.current) return;
      setIntent(undefined); setSelectedId(run.id); setSelected(run); setRuns((items) => [run, ...items.filter((item) => item.id !== run.id)]); setRefresh((value) => value + 1);
    } catch (err) {
      if (controller.signal.aborted || epoch !== accessEpoch.current) return;
      setSubmissionError(message(err));
      if (!recovering && err instanceof ApiError && [400, 401, 403, 404, 422, 429, 501].includes(err.status)) {
        forgetSubmission(storageKey, request);
        setIntent(undefined);
      }
    } finally { inFlight.current = false; if (!controller.signal.aborted) setPending(false); }
  }

  async function cancel() {
    if (!confirmation || cancelling) return;
    const run = confirmation; const controller = lifecycle.current!;
    const epoch = accessEpoch.current;
    setCancelling(true); setCancelError('');
    try {
      const updated = await api.cancelRun(workspace.id, run.id, AbortSignal.any([controller.signal, AbortSignal.timeout(20000)]));
      if (!validRunReceipt(updated, workspace.id, run.bindingId, issuer, subject, run.id)) throw new ApiError('invalid_response', 'The API returned an invalid cancellation confirmation.', 200);
      if (controller.signal.aborted || epoch !== accessEpoch.current) return;
      setSelected(updated); setRuns((items) => items.map((item) => item.id === updated.id ? updated : item)); setConfirmation(undefined); setRefresh((value) => value + 1);
    } catch (err) {
      if (!controller.signal.aborted) { setCancelError(`${message(err)} Cancellation is not confirmed. Refresh the run before trying again.`); setRefresh((value) => value + 1); }
    } finally { if (!controller.signal.aborted) setCancelling(false); }
  }

  async function loadLogs() {
    if (!selected) return;
    logRequest.current?.abort(); const controller = new AbortController(); logRequest.current = controller;
    setLogs(undefined); setLogError(''); setLogsPending(true);
    try {
      const result = await api.runLogs(workspace.id, selected.id, AbortSignal.any([controller.signal, lifecycle.current!.signal, AbortSignal.timeout(20000)]));
      if (!controller.signal.aborted && !lifecycle.current?.signal.aborted) setLogs(result);
    } catch (err) { if (!controller.signal.aborted && !lifecycle.current?.signal.aborted) setLogError(message(err)); }
    finally { if (!controller.signal.aborted && !lifecycle.current?.signal.aborted) setLogsPending(false); }
  }

  return <div className="page-wrap runs-page"><div className="page-heading"><div><span className="eyebrow">COMPUTE / APPLICATION RUNS</span><h1>Spark application runs.</h1><p>Launch an administrator-approved Spark application. Follow its recorded lifecycle, inspect the immutable runtime and reconcile interrupted requests in one place.</p></div><button className="button secondary" onClick={() => setRefresh((value) => value + 1)} disabled={loading}><RefreshCw size={15} />Refresh runs</button></div>
    {error && <ErrorNotice retry={() => setRefresh((value) => value + 1)}>{error} Last recorded state may be stale.</ErrorNotice>}
    <div className="run-layout"><section className="run-launch" aria-label="Launch an approved application"><div className="output-heading"><ShieldCheck size={17} /><h2>Approved applications</h2></div>
      {loading ? <div className="run-loading"><Spinner label="Loading execution access…" /></div> : <div className="run-launch-body">
        {bindings.length ? <><label className="field">Execution binding<select value={effectiveBindingId} onChange={(event) => setBindingId(event.target.value)} disabled={Boolean(intent)}>{intent && !bindings.some((item) => item.id === intent.bindingId) && <option value={intent.bindingId}>{intent.bindingId} (unresolved request)</option>}{bindings.map((item) => <option key={item.id} value={item.id}>{item.name}</option>)}</select><small>Available bindings are restricted to your identity and workspace. Runtime settings are controlled by your administrator.</small></label>{binding && <ExecutionProfileView profile={binding} />}</> : <div className="run-unavailable"><Boxes size={27} /><h3>No approved execution bindings</h3><p>Ask an administrator to configure a binding for your identity in this workspace, with an immutable image, packaged application and workload identity.</p></div>}
        {submissionError && <ErrorNotice>{submissionError}</ErrorNotice>}
        {intent && !pending && <div className="notice warning" role="status"><div><strong>Submission outcome is unresolved.</strong><p>The API may have accepted this request. Check it using the same request key before starting another application. The request key survives reloads in this tab; it is not an authentication token.</p><code className="run-key">{intent.idempotencyKey}</code></div></div>}
        <button className="button primary full-width" disabled={pending || (!binding && !intent) || Boolean(error) || Boolean(restored.error)} onClick={() => void submit()}>{pending ? <Spinner label="Confirming submission…" /> : intent ? <><RefreshCw size={15} />Check submission</> : <><Play size={15} />Submit application</>}</button>
        <p className="run-launch-note">Submitting may consume cluster resources. Notebook cells and arbitrary code are not submitted by this form.</p>
      </div>}
    </section><section className="run-history" aria-label="Your application runs"><div className="output-heading"><History size={17} /><h2>Your runs</h2><span className="run-count">{runs.length}</span></div>
      {loading ? <div className="run-loading"><Spinner label="Loading runs…" /></div> : runs.length ? <div className="run-list">{runs.map((run) => <button key={run.id} className={`run-row ${selectedId === run.id ? 'selected' : ''}`} aria-pressed={selectedId === run.id} onClick={() => { setSelectedId(run.id); setSelected(run); }}><span className="run-row-icon"><Activity size={17} /></span><span className="run-row-name"><strong>{bindings.find((item) => item.id === run.bindingId)?.name ?? run.bindingId}</strong><small>{run.id}</small>{run.notebook && <small>Notebook {run.notebook.id} · revision {run.notebook.revision}</small>}{run.ingestion && <small>Ingestion {run.ingestion.id} · revision {run.ingestion.revision}</small>}<span>{date(run.createdAt)}</span></span><RunStatus state={run.state} /></button>)}</div> : <EmptyState icon={<History size={25} />} title="No application runs yet" description="Runs accepted by the API will appear here. Your current identity can see its own runs within this workspace." />}
    </section></div>
    {selected && <section ref={details} tabIndex={-1} className="run-detail" aria-label="Run details"><div className="run-detail-heading"><div><span className="eyebrow">RECORDED EXECUTION</span><h2>{selected.id}</h2></div><RunStatus state={selected.state} />{canCancel && !terminal(selected.state) && <button className="button secondary compact" disabled={selected.cancelRequested || cancelling} onClick={() => { setCancelError(''); setConfirmation(selected); }}><Square size={13} />{selected.cancelRequested ? 'Cancellation requested' : 'Cancel run'}</button>}</div>
      <div className={`notice ${selected.state === 'unknown' || (selected.cancelRequested && !terminal(selected.state)) ? 'warning' : ''}`}><Activity size={17} /><div><p>{selected.message || 'Waiting for the execution controller to observe the application.'}</p>{selected.state === 'unknown' && <p>The backend outcome is unknown. Reconciliation must determine what happened; do not launch a replacement to recover this run.</p>}{selected.cancelRequested && !terminal(selected.state) && <p>Cancellation has been requested. The workload is not confirmed stopped until the controller observes cancellation.</p>}</div></div>
      <div className="run-detail-grid"><div>{selected.notebook && <><h3><FileCode2 size={16} />Saved notebook snapshot</h3><dl className="run-profile"><dt>Notebook</dt><dd>{selected.notebook.id}</dd><dt>Saved revision</dt><dd>{selected.notebook.revision}</dd><dt>Source SHA-256</dt><dd><code>{selected.notebook.sha256}</code></dd><dt>Snapshot</dt><dd>{selected.notebook.cellIds.length} Python cells · {selected.notebook.bytes.toLocaleString()} bytes</dd><dt>Source artifact</dt><dd>{selected.sourceArtifact?.state.replaceAll('_', ' ') ?? 'Not recorded'}</dd></dl><p className="run-launch-note">This run uses the saved snapshot recorded at acceptance. Later notebook edits do not change its source. Cell outputs are not available for batch runs.</p></>}{selected.ingestion && <><h3><FileCode2 size={16} />Saved ingestion snapshot</h3><dl className="run-profile"><dt>Pipeline</dt><dd>{selected.ingestion.id}</dd><dt>Saved revision</dt><dd>{selected.ingestion.revision}</dd><dt>Source SHA-256</dt><dd><code>{selected.ingestion.sha256}</code></dd><dt>Generated source</dt><dd>{selected.ingestion.bytes.toLocaleString()} bytes</dd><dt>Source location</dt><dd>{selected.ingestion.sourceUri}</dd><dt>Delta destination</dt><dd>{selected.ingestion.destinationUri}</dd><dt>Checkpoint</dt><dd>{selected.ingestion.checkpointUri}</dd><dt>Source artifact</dt><dd>{selected.sourceArtifact?.state.replaceAll('_', ' ') ?? 'Not recorded'}</dd><dt>Writer reservation</dt><dd>{selected.writerReleased ? 'Released' : 'Held until workload cleanup is observed'}</dd></dl><p className="run-launch-note">This run uses the source and locations captured at acceptance. Later definition changes do not alter it. A terminal run may retain its writer reservation while Kubernetes cleanup is reconciled.</p></>}<h3><FileCode2 size={16} />Immutable runtime</h3><ExecutionProfileView profile={selected.profile} /><dl className="run-profile"><dt>Kubernetes application</dt><dd>{selected.kubernetesName || 'Not assigned'}</dd><dt>Resource UID</dt><dd>{selected.kubernetesUid || 'Not observed yet'}</dd></dl></div><div><h3><Clock3 size={16} />Lifecycle</h3><dl className="run-profile"><dt>Created</dt><dd><time dateTime={selected.createdAt}>{date(selected.createdAt)}</time></dd><dt>Record updated</dt><dd><time dateTime={selected.updatedAt}>{date(selected.updatedAt)}</time></dd><dt>Backend observed</dt><dd>{selected.observedAt ? <time dateTime={selected.observedAt}>{date(selected.observedAt)}</time> : 'Not observed yet'}</dd><dt>Revision</dt><dd>{selected.revision}</dd></dl><ol className="run-events">{selected.events?.map((event, index) => <li key={`${event.at}-${index}`}><span className="run-event-marker" /><div><strong>{stateLabel(event.state)}</strong><time dateTime={event.at}>{date(event.at)}</time><p>{event.message}</p></div></li>)}</ol>{selected.eventsTruncated && <p className="run-launch-note">Earlier events were omitted. This is the most recent recorded lifecycle history.</p>}{!selected.events?.length && <p className="muted run-launch-note">No lifecycle events have been recorded.</p>}</div></div>
      <div className="run-log-heading"><h3><Terminal size={16} />Driver logs</h3><button className="button secondary compact" onClick={() => void loadLogs()} disabled={logsPending}>{logsPending ? <Spinner label="Reading logs…" /> : logs ? 'Refresh logs' : 'Load logs'}</button></div>{logError && <ErrorNotice>{logError}</ErrorNotice>}{logs ? <><pre className="run-logs">{logs.text || 'The backend returned no log content.'}</pre>{logs.truncated && <p className="run-launch-note">This response is truncated. It does not contain the complete driver log.</p>}</> : !logError && <p className="run-launch-note">Logs are fetched separately under your run permissions. Availability depends on the configured backend adapter.</p>}
      <div className="notice run-results-note"><ShieldCheck size={16} /><p>A successful Spark lifecycle does not validate the data it wrote. Governed result delivery, interactive notebook sessions, rich cell outputs and data-quality checks require their own integrations.</p></div>
    </section>}
    {confirmation && <Modal title="Cancel this application?" onClose={() => { if (!cancelling) setConfirmation(undefined); }}><div className="modal-body"><p>Request cancellation of run <code>{confirmation.id}</code>. The controller will reconcile the workload before reporting a terminal state. Already committed data is not rolled back.</p>{cancelError && <ErrorNotice>{cancelError}</ErrorNotice>}<div className="form-actions"><button className="button secondary" disabled={cancelling} onClick={() => setConfirmation(undefined)}>Keep running</button><button className="button primary" disabled={cancelling} onClick={() => void cancel()}>{cancelling ? <Spinner label="Requesting…" /> : 'Request cancellation'}</button></div></div></Modal>}
  </div>;
}

function RunStatus({ state }: { state: RunState }) { return <span className={`run-state run-state-${state}`}><Pill tone={state === 'succeeded' ? 'green' : ['unknown', 'cancel_requested'].includes(state) ? 'amber' : 'neutral'}>{stateLabel(state)}</Pill></span>; }
export function ExecutionProfileView({ profile }: { profile: ExecutionProfile }) {
  return <dl className="run-profile"><dt>Execution kind</dt><dd>{profile.executionKind === 'notebook-python-batch' ? 'Python notebook batch' : profile.executionKind === 'file-ingestion-batch' ? 'File ingestion batch' : 'Approved application'}</dd><dt>Operator</dt><dd>{profile.operator === 'apache' ? 'Apache Spark Operator' : 'Kubeflow Spark Operator'}</dd><dt>Image</dt><dd><code>{profile.image}</code></dd><dt>Application</dt><dd><code>{profile.executionKind === 'notebook-python-batch' ? 'Immutable saved Python snapshot' : profile.executionKind === 'file-ingestion-batch' ? 'Immutable generated ingestion application' : profile.mainApplicationFile}</code></dd><dt>Namespace</dt><dd>{profile.namespace}</dd><dt>Service account</dt><dd>{profile.serviceAccount}</dd><dt>Executors</dt><dd>{profile.executorInstances}</dd>{profile.ingestionStorage && <><dt>Allowed reads</dt><dd>{profile.ingestionStorage.readPrefixes.map((prefix) => <code className="storage-prefix" key={prefix}>{prefix}</code>)}</dd><dt>Allowed writes</dt><dd>{profile.ingestionStorage.writePrefixes.map((prefix) => <code className="storage-prefix" key={prefix}>{prefix}</code>)}</dd></>}<dt>Cancellation deadline</dt><dd>{(profile.maxRunSeconds || 3600) / 60} minutes after acceptance, including queue time</dd></dl>;
}
