import { useEffect, useRef, useState } from 'react';
import { FileSearch, RefreshCw, ShieldCheck } from 'lucide-react';
import { api, ApiError, message } from './api';
import type { Notebook } from './api';
import { useAuth } from './auth';
import { catalogs, sameNamespace, validBindings, validNamespace } from './catalog-api';
import type { CatalogBinding, TableIdentifier } from './catalog-api';
import { indexStorageKey, indexTerminal, restoreIndex, searchAPI, validIndexJob } from './search-api';
import type { IndexIntent, IndexJob, SearchProfile } from './search-api';
import { ErrorNotice, Modal, Pill, Spinner } from './ui';

type Source = Pick<IndexIntent, 'notebook' | 'catalog'>;
const signal = (controller: AbortController) => AbortSignal.any([controller.signal, AbortSignal.timeout(20000)]);
export function SearchIndex({ workspaceId, profile, profiles, onDirty }: { workspaceId: string; profile?: SearchProfile; profiles: SearchProfile[]; onDirty: (value: boolean) => void }) {
  const auth = useAuth(); const issuer = auth.config?.issuer ?? 'development'; const subject = auth.user?.profile.sub ?? 'development-admin';
  const roles = auth.user?.profile.dataslate_workspaces; const role = roles && typeof roles === 'object' ? (roles as Record<string, unknown>)[workspaceId] : undefined;
  const canIndexNotebook = auth.config?.mode === 'development' || role === 'editor' || role === 'admin';
  const key = indexStorageKey(issuer, subject, workspaceId); const [restored] = useState(() => restoreIndex(key));
  const [intent, setIntent] = useState(restored.intent); const [jobId, setJobId] = useState(restored.jobId ?? ''); const [job, setJob] = useState<IndexJob>();
  const [source, setSource] = useState<Source>(); const [error, setError] = useState(restored.error ?? ''); const [jobError, setJobError] = useState('');
  const [pending, setPending] = useState(false); const [dialog, setDialog] = useState(false); const [discard, setDiscard] = useState(false); const [refresh, setRefresh] = useState(0); const [sourceRefresh, setSourceRefresh] = useState(0);
  const life = useRef<AbortController | undefined>(undefined); const inFlight = useRef(false);
  const selectedProfile = intent ? profiles.find((p) => p.id === intent.bindingId) : profile;
  const selectedSource = intent ?? source;
  useEffect(() => { const c = new AbortController(); life.current = c; return () => c.abort(); }, []);
  useEffect(() => { onDirty(Boolean(intent)); return () => onDirty(false); }, [intent, onDirty]);
  useEffect(() => {
    const c = new AbortController(); let timer: ReturnType<typeof setTimeout> | undefined;
    setJob(undefined); setJobError(''); if (!jobId) return;
    async function observe() {
      try {
        const next = await searchAPI.job(workspaceId, jobId, signal(c));
        if (!validIndexJob(next, workspaceId, issuer, subject, { id: jobId })) throw new Error('The indexing record did not match your identity or workspace.');
        if (c.signal.aborted) return; setJob(next); setJobError('');
        if (!indexTerminal(next.state)) timer = setTimeout(() => void observe(), 2000);
      } catch (err) { if (!c.signal.aborted) { setJob(undefined); setJobError(message(err)); if (!(err instanceof ApiError && [401, 403, 404].includes(err.status))) timer = setTimeout(() => void observe(), 5000); } }
    }
    void observe(); return () => { c.abort(); clearTimeout(timer); };
  }, [workspaceId, jobId, refresh, issuer, subject]);

  async function submit() {
    if (inFlight.current || restored.error || !intent && (!profile || !source)) return;
    const input: IndexIntent = intent ?? { bindingId: profile!.id, idempotencyKey: crypto.randomUUID(), ...source! };
    try { window.sessionStorage.setItem(key, JSON.stringify({ intent: input, ...(jobId ? { jobId } : {}) })); }
    catch { setError('Browser recovery storage is unavailable. No indexing request was sent.'); return; }
    const c = life.current!; const recovering = Boolean(intent); setIntent(input); setError(''); setPending(true); inFlight.current = true;
    try {
      const next = await searchAPI.index(workspaceId, input, signal(c));
      if (!validIndexJob(next, workspaceId, issuer, subject, { intent: input })) throw new Error('The indexing confirmation did not match your identity or source. Recover with the same request key.');
      // Persist the accepted ID before forgetting the ambiguous submission key.
      window.sessionStorage.setItem(key, JSON.stringify({ jobId: next.id }));
      if (c.signal.aborted) return; setIntent(undefined); setJob(next); setJobId(next.id); setDialog(false); setRefresh((v) => v + 1);
    } catch (err) {
      if (c.signal.aborted) return;
      setError(message(err));
      if (!recovering && err instanceof ApiError && ([400, 401, 403, 404, 422, 429, 501].includes(err.status) || err.code === 'search_revision_conflict')) {
        try { if (jobId) window.sessionStorage.setItem(key, JSON.stringify({ jobId })); else window.sessionStorage.removeItem(key); setIntent(undefined); } catch { /* Keep the exact recovery request if storage cannot be repaired. */ }
        if (err.code === 'search_revision_conflict') { setSource(undefined); setSourceRefresh((v) => v + 1); }
      }
    } finally { inFlight.current = false; if (!c.signal.aborted) setPending(false); }
  }
  function forget() {
    try { window.sessionStorage.removeItem(key); setIntent(undefined); setJobId(''); setJob(undefined); setError(''); setJobError(''); setDiscard(false); setDialog(false); }
    catch { setError('Browser recovery storage could not be cleared. The saved request remains available.'); }
  }
  return <section className="search-index-panel" aria-label="Search indexing"><div className="section-toolbar"><div><span className="eyebrow">INDEX SOURCES</span><h2>Keep discovery current.</h2></div>{jobId && <button className="icon-button" aria-label="Refresh indexing job" onClick={() => setRefresh((v) => v + 1)}><RefreshCw size={16} /></button>}</div><p className="muted">Index a saved document or refresh an authorized table's schema. Catalog metadata expires after 15 minutes; indexing never loads data rows or private query outputs.</p>
    {intent ? <div className="notice warning"><div><strong>Index submission needs recovery</strong><p>Check the saved request before creating another job. Recovery keeps its original source and request key.</p><code className="run-key">{intent.idempotencyKey}</code><div className="form-actions"><button className="button secondary compact" onClick={() => { setError(''); setDialog(true); }}>Recover indexing submission</button><button className="text-button" onClick={() => setDiscard(true)}>Discard recovery record</button></div></div></div> : <><IndexSourcePicker key={sourceRefresh} workspaceId={workspaceId} canIndexNotebook={canIndexNotebook} onChange={setSource} /><button className="button secondary" disabled={!profile || !source || Boolean(restored.error) || profile.mode === 'hybrid' && !profile.vectorAvailable} onClick={() => { setError(''); setDialog(true); }}><FileSearch size={16} />Review indexing request</button></>}
    {error && !dialog && <ErrorNotice>{error}</ErrorNotice>}{jobError && <ErrorNotice>{jobError}<p>Stored indexing jobs may disappear when their source notebook is deleted. Forgetting this browser record does not cancel engine work.</p><button className="text-button" onClick={() => setDiscard(true)}>Forget browser job record</button></ErrorNotice>}
    {job && <section className="search-job" aria-label="Indexing job"><div className="section-toolbar"><strong>{job.id}</strong><Pill tone={job.state === 'succeeded' ? 'green' : job.state === 'failed' || job.state === 'unknown' ? 'amber' : 'neutral'}>{job.state}</Pill></div><p>{job.message}</p><dl className="details-list"><dt>Search profile</dt><dd>{job.bindingId}</dd><dt>Policy version</dt><dd>{job.policyVersion}</dd><dt>Source</dt><dd>{job.notebook ? `${job.notebook.id} · revision ${job.notebook.revision}` : `${job.catalog!.bindingId} / ${job.catalog!.namespace.join(' / ')} / ${job.catalog!.name}`}</dd><dt>Embedding started</dt><dd>{job.embeddingStarted ? 'Yes' : 'No'}</dd><dt>Updated</dt><dd>{new Date(job.updatedAt).toLocaleString()}</dd></dl>{job.state === 'unknown' && <div className="notice warning"><p>The embedding outcome is unknown. Ask an administrator to inspect this job before submitting a replacement; the provider may already have processed it.</p></div>}{job.state === 'superseded' && <p className="field-help">A newer source or policy superseded this job. Reload the saved selection before requesting a fresh index.</p>}</section>}
    {dialog && <Modal title={intent ? 'Recover indexing submission' : 'Review indexing request'} onClose={() => { if (!pending) setDialog(false); }}><div className="modal-body"><p>Index the selected saved source under the administrator's search policy.</p><dl className="details-list"><dt>Search profile</dt><dd>{selectedProfile?.name ?? intent?.bindingId}</dd><dt>Policy version</dt><dd>{selectedProfile?.policyVersion ?? 'Current policy must be checked by the API'}</dd><dt>Source</dt><dd>{selectedSource?.notebook ? `${selectedSource.notebook.id} · saved revision ${selectedSource.notebook.revision}` : selectedSource?.catalog ? `${selectedSource.catalog.bindingId} / ${selectedSource.catalog.namespace.join(' / ')} / ${selectedSource.catalog.name}` : 'Unavailable'}</dd></dl>
      <div className={`notice ${selectedProfile?.mode === 'hybrid' ? 'warning' : ''}`}><ShieldCheck size={17} /><p>{selectedProfile?.mode === 'hybrid' ? `This profile sends approved source text to the configured embedding service (${selectedProfile.embeddingModel}). ${selectedProfile.includeCellSource ? 'Notebook cell source is included by administrator policy.' : 'Notebook cell source is excluded; notebook metadata and explicitly approved catalog schema may be sent.'}` : selectedProfile ? `Lexical indexing uses local storage. ${selectedProfile.includeCellSource ? 'Notebook cell source is included by administrator policy.' : 'Notebook cell source is excluded.'}` : 'The prior request may have reached an embedding service. Recover the same request to inspect its actual outcome.'}</p></div>{intent && <p className="field-help">Request key: <code>{intent.idempotencyKey}</code></p>}{error && <ErrorNotice>{error}</ErrorNotice>}<div className="form-actions"><button className="button secondary" disabled={pending} onClick={() => setDialog(false)}>Close</button><button className="button primary" disabled={pending || Boolean(restored.error) || !intent && (!profile || !source)} onClick={() => void submit()}>{pending ? <Spinner label="Checking indexing request…" /> : intent ? 'Check indexing submission' : 'Submit indexing request'}</button></div></div></Modal>}
    {discard && <Modal title="Forget this indexing record?" onClose={() => setDiscard(false)}><div className="modal-body"><p>This removes the browser's recovery key and remembered job ID. It does not cancel indexing, undo external embedding requests or delete indexed data. Inspect an uncertain job before submitting a replacement.</p><div className="form-actions"><button className="button secondary" onClick={() => setDiscard(false)}>Keep record</button><button className="button primary" onClick={forget}>Forget browser record</button></div></div></Modal>}
  </section>;
}

function IndexSourcePicker({ workspaceId, canIndexNotebook, onChange }: { workspaceId: string; canIndexNotebook: boolean; onChange: (value: Source | undefined) => void }) {
  const [kind, setKind] = useState<'notebook' | 'catalog'>(canIndexNotebook ? 'notebook' : 'catalog'); const [notebooks, setNotebooks] = useState<Notebook[]>([]); const [notebookId, setNotebookId] = useState('');
  const [bindings, setBindings] = useState<CatalogBinding[]>([]); const [bindingId, setBindingId] = useState(''); const [namespaces, setNamespaces] = useState<string[][]>([]); const [namespace, setNamespace] = useState<string[]>(); const [tables, setTables] = useState<TableIdentifier[]>([]); const [tableName, setTableName] = useState('');
  const [error, setError] = useState(''); const [loading, setLoading] = useState(false); const [attempt, setAttempt] = useState(0);
  useEffect(() => {
    const c = new AbortController(); setError(''); setLoading(true); setNotebooks([]); setBindings([]); setNotebookId(''); setBindingId(''); onChange(undefined);
    (kind === 'notebook' ? api.notebooks(workspaceId, signal(c)).then((result) => { if (!c.signal.aborted) setNotebooks(result.items); }) : catalogs.bindings(workspaceId, signal(c)).then((result) => { if (c.signal.aborted) return; if (!validBindings(result, workspaceId)) throw new Error('Invalid catalog access response.'); setBindings(result.items); }))
      .catch((err) => { if (!c.signal.aborted) setError(message(err)); }).finally(() => { if (!c.signal.aborted) setLoading(false); });
    return () => c.abort();
  }, [workspaceId, kind, attempt, onChange]);
  useEffect(() => {
    const c = new AbortController(); setNamespaces([]); setNamespace(undefined); setTables([]); setTableName('');
    if (!bindingId) return;
    catalogs.namespaces(workspaceId, bindingId, signal(c)).then((result) => { if (c.signal.aborted) return; if (result.source !== 'authorized-grants' || !result.items.every((v) => validNamespace(v.namespace))) throw new Error('Invalid namespace grants.'); setNamespaces(result.items.map((v) => v.namespace)); }).catch((err) => { if (!c.signal.aborted) setError(message(err)); });
    return () => c.abort();
  }, [workspaceId, bindingId]);
  useEffect(() => {
    const c = new AbortController(); setTables([]); setTableName(''); if (!namespace || !bindingId) return;
    catalogs.tables(workspaceId, bindingId, namespace, signal(c)).then((result) => { if (c.signal.aborted) return; if (result.source !== 'authorized-grants' || !result.items.every((v) => validNamespace(v.namespace) && sameNamespace(v.namespace, namespace))) throw new Error('Invalid table grants.'); setTables(result.items); }).catch((err) => { if (!c.signal.aborted) setError(message(err)); });
    return () => c.abort();
  }, [workspaceId, bindingId, namespace]);
  return <div className="index-source-picker"><label className="field">Source type<select aria-label="Index source type" value={kind} onChange={(e) => { onChange(undefined); setKind(e.target.value as typeof kind); }}><option value="notebook" disabled={!canIndexNotebook}>Saved notebook or SQL document</option><option value="catalog">Catalog table schema</option></select></label>{loading ? <Spinner label="Loading indexable sources…" /> : error ? <ErrorNotice retry={() => setAttempt((v) => v + 1)}>{error}</ErrorNotice> : kind === 'notebook' ? <label className="field">Saved document<select aria-label="Document to index" value={notebookId} onChange={(e) => { const n = notebooks.find((v) => v.id === e.target.value); setNotebookId(e.target.value); onChange(n ? { notebook: { id: n.id, revision: n.revision } } : undefined); }}><option value="">Select a saved document</option>{notebooks.map((n) => <option key={n.id} value={n.id}>{n.name} · revision {n.revision}</option>)}</select></label> : <><label className="field">Catalog connection<select aria-label="Catalog to index" value={bindingId} onChange={(e) => { onChange(undefined); setBindingId(e.target.value); }}><option value="">Select an authorized catalog</option>{bindings.map((b) => <option key={b.id} value={b.id}>{b.name}</option>)}</select></label>{bindingId && <label className="field">Namespace<select aria-label="Namespace to index" value={namespace ? JSON.stringify(namespace) : ''} onChange={(e) => { onChange(undefined); setNamespace(e.target.value ? JSON.parse(e.target.value) : undefined); }}><option value="">Select a namespace</option>{namespaces.map((n) => <option key={JSON.stringify(n)} value={JSON.stringify(n)}>{n.join(' / ')}</option>)}</select></label>}{namespace && <label className="field">Table<select aria-label="Table to index" value={tableName} onChange={(e) => { setTableName(e.target.value); onChange(e.target.value ? { catalog: { bindingId, namespace, name: e.target.value } } : undefined); }}><option value="">Select an authorized table</option>{tables.map((t) => <option key={t.name} value={t.name}>{t.name}</option>)}</select></label>}</>}<p className="field-help">The selected source must also be enabled in the search profile. The API rechecks the saved revision, table grants and current policy before indexing.</p></div>;
}
