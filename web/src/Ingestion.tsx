import { useEffect, useRef, useState } from 'react';
import { ArrowRight, Copy, Database, Download, FileInput, GitBranch, Play, Plus, RefreshCw, Save, ShieldCheck, Trash2 } from 'lucide-react';
import { api, ApiError, message } from './api';
import type { ExecutionBinding, ExecutionRun, IngestionDraft, IngestionField, IngestionPipeline, RunIntent, Workspace } from './api';
import { useAuth } from './auth';
import { forgetSubmission, ingestionIntentKey, restoredSubmission, validRunReceipt } from './execution';
import { ExecutionProfileView } from './Runs';
import { download, EmptyState, ErrorNotice, Modal, Pill, Spinner } from './ui';

const types: IngestionField['type'][] = ['string', 'long', 'double', 'boolean', 'timestamp', 'date'];
const newDraft = (): IngestionDraft => ({ name: 'New ingestion pipeline', source: { format: 'json', uri: '', schema: [{ name: '', type: 'string', nullable: true }] }, destination: { uri: '' }, checkpointUri: '', maxFilesPerTrigger: 1000 });
const asDraft = (pipeline: IngestionPipeline): IngestionDraft => ({ name: pipeline.name, source: structuredClone(pipeline.source), destination: { ...pipeline.destination }, checkpointUri: pipeline.checkpointUri, maxFilesPerTrigger: pipeline.maxFilesPerTrigger });
const signal = (controller: AbortController) => AbortSignal.any([controller.signal, AbortSignal.timeout(20000)]);

export function IngestionWorkspace({ workspace, onDirty, openRun }: { workspace?: Workspace; onDirty: (dirty: boolean) => void; openRun: (id: string) => void }) {
  const auth = useAuth();
  if (!auth.authenticated) return <EmptyState icon={<ShieldCheck size={27} />} title="Sign in to view pipelines" description="Pipeline definitions and execution details are cleared when your session expires." />;
  if (!workspace) return <EmptyState icon={<GitBranch size={27} />} title="Select a workspace first" description="Ingestion pipelines and their execution bindings belong to a workspace." />;
  return <Pipelines key={JSON.stringify([auth.config?.issuer, auth.user?.profile.sub, workspace.id])} workspace={workspace} onDirty={onDirty} openRun={openRun} />;
}

function Pipelines({ workspace, onDirty, openRun }: { workspace: Workspace; onDirty: (dirty: boolean) => void; openRun: (id: string) => void }) {
  const auth = useAuth();
  const memberships = auth.user?.profile.dataslate_workspaces;
  const role = memberships && typeof memberships === 'object' ? (memberships as Record<string, unknown>)[workspace.id] : undefined;
  const canEdit = auth.config?.mode === 'development' || role === 'editor' || role === 'admin';
  const [pipelines, setPipelines] = useState<IngestionPipeline[]>([]);
  const [saved, setSaved] = useState<IngestionPipeline>();
  const [draft, setDraft] = useState<IngestionDraft>();
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [refresh, setRefresh] = useState(0);
  const [listError, setListError] = useState('');
  const [error, setError] = useState('');
  const [conflict, setConflict] = useState(false);
  const [deleteDialog, setDeleteDialog] = useState(false);
  const [unresolved, setUnresolved] = useState(false);
  const lifecycle = useRef<AbortController | undefined>(undefined);
  const inFlight = useRef(false);
  const dirty = Boolean(draft && (!saved || JSON.stringify(draft) !== JSON.stringify(asDraft(saved))));

  useEffect(() => { const controller = new AbortController(); lifecycle.current = controller; return () => controller.abort(); }, []);
  useEffect(() => { onDirty(dirty || unresolved); return () => onDirty(false); }, [dirty, unresolved, onDirty]);
  useEffect(() => {
    const controller = new AbortController(); setLoading(true); setListError('');
    api.ingestionPipelines(workspace.id, signal(controller)).then((result) => { if (!controller.signal.aborted) setPipelines(result.items); })
      .catch((err) => { if (!controller.signal.aborted) { setListError(message(err)); if (err instanceof ApiError && [401, 403, 404].includes(err.status)) { setPipelines([]); setSaved(undefined); setDraft(undefined); } } })
      .finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, [workspace.id, refresh]);

  function discardAllowed() { return !dirty || window.confirm('Discard unsaved pipeline edits? You can download your edits first.'); }
  function create(copy?: IngestionPipeline) {
    if (busy || !discardAllowed()) return;
    setSaved(undefined); setError(''); setConflict(false);
    setDraft(copy ? { ...asDraft(copy), name: `${copy.name} copy`.slice(0, 200), destination: { uri: '' }, checkpointUri: '' } : newDraft());
  }
  async function open(pipeline: IngestionPipeline, force = false) {
    if (inFlight.current || (!force && !discardAllowed())) return;
    const controller = lifecycle.current!; inFlight.current = true; setBusy(true); setError('');
    try { const next = await api.ingestionPipeline(workspace.id, pipeline.id, signal(controller)); if (!controller.signal.aborted) { setSaved(next); setDraft(asDraft(next)); setPipelines((items) => items.map((item) => item.id === next.id ? next : item)); setConflict(false); } }
    catch (err) { if (!controller.signal.aborted) setError(message(err)); }
    finally { inFlight.current = false; if (!controller.signal.aborted) setBusy(false); }
  }
  async function save() {
    if (!draft || inFlight.current || !canEdit || conflict) return;
    const controller = lifecycle.current!; const submitted = structuredClone(draft); inFlight.current = true; setBusy(true); setError('');
    try {
      const next = await api.saveIngestionPipeline(workspace.id, submitted, saved?.id, saved?.revision, signal(controller));
      if (controller.signal.aborted) return;
      setSaved(next); setDraft(asDraft(next)); setPipelines((items) => [next, ...items.filter((item) => item.id !== next.id)]); setConflict(false);
    } catch (err) {
      if (controller.signal.aborted) return;
      setError(message(err));
      if (err instanceof ApiError && ['conflict', 'revision_conflict', 'ingestion_revision_conflict', 'ingestion_config_immutable'].includes(err.code)) setConflict(true);
    } finally { inFlight.current = false; if (!controller.signal.aborted) setBusy(false); }
  }
  async function remove() {
    if (!saved || inFlight.current) return;
    const controller = lifecycle.current!; inFlight.current = true; setBusy(true); setError('');
    try {
      await api.deleteIngestionPipeline(workspace.id, saved.id, saved.revision, signal(controller));
      if (controller.signal.aborted) return;
      setPipelines((items) => items.filter((item) => item.id !== saved.id)); setSaved(undefined); setDraft(undefined); setDeleteDialog(false);
    } catch (err) { if (!controller.signal.aborted) { setError(message(err)); setDeleteDialog(false); if (err instanceof ApiError && ['conflict', 'revision_conflict', 'ingestion_revision_conflict'].includes(err.code)) setConflict(true); } }
    finally { inFlight.current = false; if (!controller.signal.aborted) setBusy(false); }
  }
  function changeSchema(index: number, patch: Partial<IngestionField>) { setDraft((current) => current && ({ ...current, source: { ...current.source, schema: current.source.schema.map((field, i) => i === index ? { ...field, ...patch } : field) } })); }

  return <div className="page-wrap ingestion-page"><div className="page-heading"><div><span className="eyebrow">PIPELINES / FILE INGESTION</span><h1>From files to your lake.</h1><p>Capture available JSON or Parquet files in a checkpointed Spark batch and append them to a Snappy-compressed Delta table.</p></div>{canEdit && <button className="button primary" disabled={busy} onClick={() => create()}><Plus size={15} />New pipeline</button>}</div>
    <IngestionBatch workspaceId={workspace.id} pipeline={saved} blocked={dirty ? 'Save or discard your edits before running the saved pipeline.' : conflict ? 'Reload the current revision before submitting.' : !canEdit ? 'Workspace editor access is required to submit an ingestion batch.' : ''} onUnresolved={setUnresolved} onRevisionConflict={() => setConflict(true)} openRun={openRun} />
    {error && !draft && <ErrorNotice>{error}</ErrorNotice>}<div className="ingestion-layout"><aside className="pipeline-library" aria-label="Saved ingestion pipelines"><div className="output-heading"><GitBranch size={16} /><h2>Saved pipelines</h2><button className="icon-button" aria-label="Refresh pipelines" disabled={loading} onClick={() => setRefresh((value) => value + 1)}><RefreshCw size={15} /></button></div>{listError && <ErrorNotice retry={() => setRefresh((value) => value + 1)}>{listError}</ErrorNotice>}{loading ? <div className="run-loading"><Spinner label="Loading pipelines…" /></div> : pipelines.length ? <div className="pipeline-list">{pipelines.map((pipeline) => <button key={pipeline.id} className={`pipeline-item ${saved?.id === pipeline.id ? 'selected' : ''}`} disabled={busy} onClick={() => void open(pipeline)} aria-pressed={saved?.id === pipeline.id}><FileInput size={16} /><span><strong>{pipeline.name}</strong><small>{pipeline.source.format.toUpperCase()} → Delta · revision {pipeline.revision}</small></span></button>)}</div> : !listError && <p className="pipeline-list-empty">Your saved ingestion pipelines will appear here.</p>}<p className="pipeline-foot">Definitions are shared within this workspace. Run details and logs are scoped to the identity that submitted them.</p></aside>
    <section className="pipeline-editor" aria-label="Pipeline editor">{draft ? <form onSubmit={(event) => { event.preventDefault(); void save(); }}><div className="pipeline-editor-heading"><div><Pill>{saved ? `Saved revision ${saved.revision}` : 'New pipeline'}</Pill><span className="pipeline-save-state" role="status">{dirty ? 'Unsaved changes' : 'Saved'}</span></div><div className="pipeline-editor-actions"><button type="button" className="icon-button" aria-label="Download pipeline edits" onClick={() => download(`${draft.name || 'pipeline'}.json`, JSON.stringify(draft, null, 2))}><Download size={16} /></button>{saved && canEdit && <><button type="button" className="icon-button" aria-label="Duplicate pipeline" disabled={busy} onClick={() => create(saved)}><Copy size={16} /></button><button type="button" className="icon-button" aria-label="Delete pipeline" disabled={busy} onClick={() => setDeleteDialog(true)}><Trash2 size={16} /></button></>}<button className="button primary compact" disabled={busy || !canEdit || !dirty || conflict}>{busy ? <Spinner label="Saving…" /> : <><Save size={14} />Save pipeline</>}</button></div></div>
      <div className="pipeline-form-body">{error && <ErrorNotice>{error}</ErrorNotice>}{conflict && <div className="notice warning"><div><strong>A newer revision or configuration conflict needs review.</strong><p>Your edits remain in this tab. Download them before reloading.</p>{saved && <button type="button" className="text-button" onClick={() => { if (discardAllowed()) void open(saved, true); }}>Reload pipeline revision <RefreshCw size={13} /></button>}</div></div>}
      <label className="field">Pipeline name<input required maxLength={200} value={draft.name} disabled={busy || !canEdit} onChange={(event) => setDraft({ ...draft, name: event.target.value })} /></label>
      {saved && <div className="notice pipeline-config-note"><ShieldCheck size={16} /><p>Source, schema and storage locations are fixed after creation to preserve checkpoint continuity. You can rename this pipeline or duplicate it with new destination and checkpoint locations.</p></div>}
      <fieldset disabled={Boolean(saved) || busy || !canEdit} className="pipeline-config"><legend className="sr-only">Ingestion configuration</legend>
      <section className="pipeline-section"><div className="form-section-title"><span>01</span><h2>Source files</h2></div><div className="field-grid"><label className="field">Source format<select value={draft.source.format} onChange={(event) => setDraft({ ...draft, source: { ...draft.source, format: event.target.value as 'json' | 'parquet' } })}><option value="json">JSON (one object per line)</option><option value="parquet">Parquet</option></select></label><label className="field">Maximum files per micro-batch<input type="number" min={1} max={10000} step={1} required value={draft.maxFilesPerTrigger} onChange={(event) => setDraft({ ...draft, maxFilesPerTrigger: Number(event.target.value) })} /></label></div><label className="field">Source location<input required maxLength={2048} aria-label="Source location" aria-describedby="pipeline-source-help" value={draft.source.uri} placeholder="s3://your-lake/incoming/orders/" onChange={(event) => setDraft({ ...draft, source: { ...draft.source, uri: event.target.value } })} /><small id="pipeline-source-help">Use a canonical s3://bucket/prefix/ location with a trailing slash. Credentials and signed URLs do not belong in pipeline definitions.</small></label>
      <div className="pipeline-schema-heading"><div><h3>Explicit source schema</h3><p>Declare the columns Spark should read. Schema inference and automatic evolution are disabled.</p></div><button type="button" className="button secondary compact" disabled={draft.source.schema.length >= 200} onClick={() => setDraft({ ...draft, source: { ...draft.source, schema: [...draft.source.schema, { name: '', type: 'string', nullable: true }] } })}><Plus size={13} />Add column</button></div>
      <div className="pipeline-schema">{draft.source.schema.map((field, index) => <div className="pipeline-schema-row" key={index}><label className="field">Column {index + 1} name<input required value={field.name} maxLength={128} pattern="[A-Za-z_][A-Za-z0-9_]*" onChange={(event) => changeSchema(index, { name: event.target.value })} /></label><label className="field">Column {index + 1} type<select value={field.type} onChange={(event) => changeSchema(index, { type: event.target.value as IngestionField['type'] })}>{types.map((type) => <option key={type} value={type}>{type}</option>)}</select></label><label className="schema-nullable"><input type="checkbox" checked={field.nullable} aria-label={`Column ${index + 1} nullable`} onChange={(event) => changeSchema(index, { nullable: event.target.checked })} />Nullable</label><button type="button" className="icon-button" aria-label={`Remove column ${index + 1}`} disabled={draft.source.schema.length === 1} onClick={() => setDraft({ ...draft, source: { ...draft.source, schema: draft.source.schema.filter((_, i) => i !== index) } })}><Trash2 size={14} /></button></div>)}</div></section>
      <section className="pipeline-section"><div className="form-section-title"><span>02</span><h2>Delta destination</h2></div><label className="field">Delta table location<input required maxLength={2048} aria-label="Delta table location" aria-describedby="pipeline-destination-help" value={draft.destination.uri} placeholder="s3://your-lake/bronze/orders/" onChange={(event) => setDraft({ ...draft, destination: { uri: event.target.value } })} /><small id="pipeline-destination-help">Append-only Delta Lake output with Snappy compression. Use a dedicated location for this pipeline.</small></label><label className="field">Checkpoint location<input required maxLength={2048} aria-label="Checkpoint location" aria-describedby="pipeline-checkpoint-help" value={draft.checkpointUri} placeholder="s3://your-lake/checkpoints/orders/" onChange={(event) => setDraft({ ...draft, checkpointUri: event.target.value })} /><small id="pipeline-checkpoint-help">Keep this location for later batches. It must be separate from the source and destination. Deleting checkpoints can cause repeated ingestion.</small></label></section>
      </fieldset><div className="notice pipeline-scope"><Database size={17} /><p>Each run processes the available files and then stops. Verified output can be published separately to an approved Delta catalog target from its run details. Kafka, CDC, schedules and storage credential setup require their own integrations. Saving a definition does not verify the storage or start a job.</p></div></div>
    </form> : <EmptyState icon={<FileInput size={28} />} eyebrow="A CLEAR PATH INTO YOUR LAKE" title="Create your first ingestion flow" description="Choose a source, declare its schema, and save a dedicated Delta destination and checkpoint. Run a saved revision using an approved ingestion binding." action={canEdit ? <button className="button primary" onClick={() => create()}><Plus size={15} />Create ingestion pipeline</button> : <p className="muted">Workspace editor access is required to create pipelines.</p>} />}</section></div>
    {deleteDialog && saved && <Modal title="Delete this pipeline?" onClose={() => { if (!busy) setDeleteDialog(false); }}><div className="modal-body"><p>Delete <strong>{saved.name}</strong> at revision {saved.revision}. This removes its workspace definition. It does not cancel runs or delete source files, Delta data or checkpoints.</p>{dirty && <p className="muted">Unsaved edits in this tab will also be discarded.</p>}<div className="form-actions"><button className="button secondary" disabled={busy} onClick={() => setDeleteDialog(false)}>Keep pipeline</button><button className="button primary" disabled={busy} onClick={() => void remove()}>{busy ? <Spinner label="Deleting…" /> : 'Delete saved pipeline'}</button></div></div></Modal>}
  </div>;
}

function IngestionBatch({ workspaceId, pipeline, blocked, onUnresolved, onRevisionConflict, openRun }: { workspaceId: string; pipeline?: IngestionPipeline; blocked: string; onUnresolved: (value: boolean) => void; onRevisionConflict: () => void; openRun: (id: string) => void }) {
  const auth = useAuth(); const issuer = auth.config?.issuer ?? 'development'; const subject = auth.user?.profile.sub ?? 'development-admin';
  const storageKey = ingestionIntentKey(issuer, subject, workspaceId);
  const [restored] = useState(() => { const recovery = restoredSubmission(storageKey); return recovery.intent && !recovery.intent.pipeline ? { error: 'The saved ingestion request is invalid. Check your existing runs before repairing browser recovery storage.' } : recovery; });
  const [intent, setIntent] = useState<RunIntent | undefined>(restored.intent);
  const [bindings, setBindings] = useState<ExecutionBinding[]>([]); const [bindingId, setBindingId] = useState('');
  const [loading, setLoading] = useState(true); const [refresh, setRefresh] = useState(0); const [bindingError, setBindingError] = useState('');
  const [error, setError] = useState(restored.error ?? ''); const [pending, setPending] = useState(false); const [dialog, setDialog] = useState(false);
  const [accepted, setAccepted] = useState<ExecutionRun>(); const lifecycle = useRef<AbortController | undefined>(undefined); const inFlight = useRef(false); const accessEpoch = useRef(0);
  const binding = bindings.find((item) => item.id === (intent?.bindingId ?? bindingId));
  const selection = intent?.pipeline ?? (pipeline ? { id: pipeline.id, revision: pipeline.revision } : undefined);
  const displayedPipeline = pipeline?.id === selection?.id ? pipeline : undefined;
  useEffect(() => { const controller = new AbortController(); lifecycle.current = controller; return () => controller.abort(); }, []);
  useEffect(() => { onUnresolved(Boolean(intent)); return () => onUnresolved(false); }, [intent, onUnresolved]);
  useEffect(() => {
    const controller = new AbortController(); setLoading(true); setBindingError('');
    api.executionBindings(workspaceId, signal(controller)).then((result) => { if (!controller.signal.aborted) { const next = result.items.filter((item) => item.executionKind === 'file-ingestion-batch'); setBindings(next); setBindingId((current) => next.some((item) => item.id === current) ? current : next[0]?.id ?? ''); } })
      .catch((err) => { if (!controller.signal.aborted) { setBindings([]); setBindingError(message(err)); setAccepted(undefined); if (err instanceof ApiError && [401, 403, 404].includes(err.status)) { accessEpoch.current += 1; setDialog(false); } } })
      .finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, [workspaceId, refresh]);
  async function submit() {
    if (inFlight.current || restored.error || (!intent && (!pipeline || blocked || !binding))) return;
    const request: RunIntent = intent ?? { bindingId: binding!.id, idempotencyKey: crypto.randomUUID(), pipeline: { id: pipeline!.id, revision: pipeline!.revision } };
    try { window.sessionStorage.setItem(storageKey, JSON.stringify(request)); }
    catch { setError('Browser recovery storage is unavailable. No request was sent. Restore session storage before submitting.'); return; }
    const controller = lifecycle.current!; const epoch = accessEpoch.current; const recovering = Boolean(intent);
    inFlight.current = true; setPending(true); setIntent(request); setAccepted(undefined); setError('');
    try {
      const run = await api.submitRun(workspaceId, request, signal(controller));
      if (!validRunReceipt(run, workspaceId, request.bindingId, issuer, subject, undefined, undefined, request.pipeline)) throw new ApiError('invalid_response', 'The API returned an invalid ingestion run confirmation. Recover with the original pipeline revision and request key.', 200);
      forgetSubmission(storageKey, request);
      if (controller.signal.aborted || epoch !== accessEpoch.current) return;
      setIntent(undefined); setAccepted(run); setDialog(false);
    } catch (err) {
      if (controller.signal.aborted || epoch !== accessEpoch.current) return;
      setError(message(err));
      if (!recovering && err instanceof ApiError && ([400, 401, 403, 404, 422, 429, 501].includes(err.status) || ['ingestion_revision_conflict', 'ingestion_writer_busy'].includes(err.code))) {
        forgetSubmission(storageKey, request); setIntent(undefined); if (err.code === 'ingestion_revision_conflict') onRevisionConflict();
      }
    } finally { inFlight.current = false; if (!controller.signal.aborted) setPending(false); }
  }
  return <section className="pipeline-batch" aria-label="Ingestion batch execution"><div className="pipeline-batch-intro"><span className="eyebrow">AVAILABLE FILES → DELTA</span><strong>{intent ? `Recover pipeline revision ${intent.pipeline?.revision}` : pipeline ? `Run saved revision ${pipeline.revision}` : 'Save a pipeline to start an ingestion batch.'}</strong><p>{intent ? 'An earlier submission is unresolved. Recover it using its original request key, even if its definition has since changed or been deleted.' : loading ? 'Checking your approved ingestion bindings…' : blocked || (!bindings.length ? 'No file ingestion binding is granted to your identity in this workspace.' : 'A checkpointed batch processes available files and exits. Later runs continue from the same checkpoint.')}</p></div><button className="button secondary compact" disabled={loading || pending || Boolean(restored.error) || Boolean(bindingError) || (!intent && (!pipeline || Boolean(blocked) || !binding))} onClick={() => setDialog(true)}>{intent ? <><RefreshCw size={14} />Recover ingestion submission</> : <><Play size={14} />Run ingestion batch</>}</button>
    {bindingError && <div className="pipeline-batch-wide"><ErrorNotice retry={() => setRefresh((value) => value + 1)}>{bindingError}</ErrorNotice></div>}{error && !dialog && <div className="pipeline-batch-wide"><ErrorNotice>{error}</ErrorNotice></div>}
    {intent && <div className="pipeline-batch-wide notice warning" role="status"><div><strong>Ingestion submission outcome is unresolved.</strong><p>Pipeline {intent.pipeline?.id} · revision {intent.pipeline?.revision}</p><code className="run-key">{intent.idempotencyKey}</code></div></div>}
    {accepted && <div className="pipeline-batch-wide batch-accepted" role="status"><div><Pill tone="green">Ingestion run accepted</Pill><p>Pipeline {accepted.ingestion?.id} · revision {accepted.ingestion?.revision}</p><small>Follow the run to observe execution status. Acceptance does not confirm a Delta commit.</small></div><button className="button secondary compact" onClick={() => openRun(accepted.id)}>View ingestion run <ArrowRight size={13} /></button></div>}
    {dialog && <Modal title={intent ? 'Recover ingestion batch' : 'Review ingestion batch'} onClose={() => { if (!pending) setDialog(false); }}><div className="modal-body"><Pill>Saved revision {selection?.revision}</Pill><p><strong>{displayedPipeline?.name ?? 'Saved ingestion pipeline'}</strong><br /><span className="muted">Pipeline {selection?.id}</span></p>{displayedPipeline && <dl className="run-profile"><dt>Source</dt><dd>{displayedPipeline.source.uri}</dd><dt>Input schema</dt><dd>{displayedPipeline.source.format.toUpperCase()} · {displayedPipeline.source.schema.length} declared columns</dd><dt>Micro-batch limit</dt><dd>{displayedPipeline.maxFilesPerTrigger.toLocaleString()} files</dd><dt>Delta destination</dt><dd>{displayedPipeline.destination.uri}</dd><dt>Checkpoint</dt><dd>{displayedPipeline.checkpointUri}</dd><dt>Write behavior</dt><dd>Append · Snappy Parquet · Delta transaction log</dd></dl>}
      {intent && <div className="notice warning"><p>Recovery uses saved revision {selection?.revision} and the original request key. It does not substitute current edits, a renamed pipeline or another runtime. If accepted earlier, the existing run retains its original snapshot.</p></div>}
      <label className="field">Ingestion execution binding<select value={intent?.bindingId ?? bindingId} disabled={Boolean(intent) || pending} onChange={(event) => setBindingId(event.target.value)}>{intent && !binding && <option value={intent.bindingId}>{intent.bindingId} (unresolved request)</option>}{bindings.map((item) => <option value={item.id} key={item.id}>{item.name}</option>)}</select></label>{binding && <ExecutionProfileView profile={binding} />}
      <div className="notice warning"><ShieldCheck size={17} /><p>This batch reads and writes storage using the displayed workload identity. Its administrator-approved prefixes must include the source, destination and checkpoint. Cloud permissions and storage connectivity must also be configured.</p></div>{error && <ErrorNotice>{error}</ErrorNotice>}{!intent && blocked && <ErrorNotice>{blocked}</ErrorNotice>}<div className="form-actions"><button className="button secondary" disabled={pending} onClick={() => setDialog(false)}>Close</button><button className="button primary" disabled={pending || Boolean(restored.error) || (!intent && (!pipeline || Boolean(blocked) || !binding))} onClick={() => void submit()}>{pending ? <Spinner label="Confirming ingestion…" /> : intent ? 'Check ingestion submission' : 'Submit ingestion revision'}</button></div></div></Modal>}
  </section>;
}
