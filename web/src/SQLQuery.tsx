import { useEffect, useRef, useState } from 'react';
import { History, Play, RefreshCw, ShieldCheck, Square, Terminal } from 'lucide-react';
import { ApiError, message } from './api';
import type { Cell } from './api';
import { useAuth } from './auth';
import { forgetQueryIntent, queries, queryIntentKey, queryTerminal, restoreQueryIntent, validQuery, validQueryResult } from './query';
import type { QueryIntent, QueryResult, SqlBinding, SqlQuery } from './query';
import { ResultPanel } from './Results';
import { ErrorNotice, Modal, Pill, Spinner } from './ui';

interface SqlDocument { id?: string; revision?: number; name: string; cells: Cell[] }
interface SqlProps { workspaceId: string; document: SqlDocument; blockedReason: string; onUnresolved: (value: boolean) => void; onRevisionConflict: () => void }
const signal = (controller: AbortController) => AbortSignal.any([controller.signal, AbortSignal.timeout(20000)]);
export function SQLQuery(props: SqlProps) {
  const auth = useAuth();
  if (!auth.authenticated) return <ResultPanel />;
  return <AuthorizedSQL key={JSON.stringify([auth.config?.issuer, auth.user?.profile.sub, props.workspaceId, props.document.id ?? props.document.cells[0]?.id])} {...props} />;
}
function AuthorizedSQL({ workspaceId, document, blockedReason, onUnresolved, onRevisionConflict }: SqlProps) {
  const auth = useAuth(); const issuer = auth.config?.issuer ?? 'development'; const subject = auth.user?.profile.sub ?? 'development-admin';
  const memberships = auth.user?.profile.dataslate_workspaces;
  const role = memberships && typeof memberships === 'object' ? (memberships as Record<string, unknown>)[workspaceId] : undefined;
  const canSubmit = auth.config?.mode === 'development' || role === 'editor' || role === 'admin';
  const storageKey = queryIntentKey(issuer, subject, workspaceId); const [restored] = useState(() => restoreQueryIntent(storageKey));
  const [intent, setIntent] = useState<QueryIntent | undefined>(restored.intent); const [error, setError] = useState(restored.error ?? '');
  const [bindings, setBindings] = useState<SqlBinding[]>([]); const [bindingId, setBindingId] = useState(''); const [items, setItems] = useState<SqlQuery[]>([]);
  const [cellId, setCellId] = useState(document.cells.find((cell) => cell.language === 'sql')?.id ?? '');
  const [query, setQuery] = useState<SqlQuery>(); const [queryId, setQueryId] = useState(''); const [result, setResult] = useState<QueryResult>();
  const [loading, setLoading] = useState(true); const [resultsLoading, setResultsLoading] = useState(false); const [accessError, setAccessError] = useState(''); const [resultError, setResultError] = useState('');
  const [refresh, setRefresh] = useState(0); const [pending, setPending] = useState(false); const [dialog, setDialog] = useState(false); const [cancelDialog, setCancelDialog] = useState(false); const [cancelling, setCancelling] = useState(false);
  const lifecycle = useRef<AbortController | undefined>(undefined); const inFlight = useRef(false); const epoch = useRef(0); const selection = useRef(queryId); selection.current = queryId;
  const sqlCells = document.cells.map((cell, index) => ({ ...cell, index })).filter((cell) => cell.language === 'sql');
  const selectedCell = sqlCells.find((cell) => cell.id === cellId) ?? sqlCells[0];
  const binding = bindings.find((item) => item.id === (intent?.bindingId ?? bindingId));
  const blocked = blockedReason || (!canSubmit ? 'Workspace editor access is required to submit SQL.' : !document.id || !document.revision ? 'Save this document before executing a SQL cell.' : !selectedCell ? 'Add a SQL cell before submitting a query.' : !selectedCell.source.trim() ? 'The selected SQL cell is empty.' : '');
  const visible = sqlCells.length > 0 || Boolean(intent) || items.length > 0;
  useEffect(() => { const controller = new AbortController(); lifecycle.current = controller; return () => controller.abort(); }, []);
  useEffect(() => { setCancelDialog(false); }, [queryId]);
  useEffect(() => { onUnresolved(Boolean(intent)); return () => onUnresolved(false); }, [intent, onUnresolved]);
  useEffect(() => {
    const controller = new AbortController(); setLoading(true); setAccessError('');
    Promise.all([queries.bindings(workspaceId, signal(controller)), queries.list(workspaceId, signal(controller))]).then(([grants, history]) => {
      if (controller.signal.aborted) return;
      const own = history.items.filter((item) => item.notebook?.id === document.id);
      if (!own.every((item) => validQuery(item, workspaceId, issuer, subject))) throw new ApiError('invalid_response', 'The API returned an invalid SQL query history.', 200);
      setBindings(grants.items); setBindingId((current) => grants.items.some((item) => item.id === current) ? current : grants.items[0]?.id ?? ''); setItems(own);
    }).catch((err) => { if (!controller.signal.aborted) { setAccessError(message(err)); if (err instanceof ApiError && [401, 403, 404].includes(err.status)) { epoch.current += 1; setBindings([]); setItems([]); setQuery(undefined); setQueryId(''); setResult(undefined); setDialog(false); setCancelDialog(false); } } })
      .finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, [workspaceId, document.id, refresh, issuer, subject]);
  useEffect(() => {
    if (!queryId) { setQuery(undefined); setResult(undefined); setResultError(''); return; }
    const controller = new AbortController(); let timer: ReturnType<typeof setTimeout> | undefined;
    setResult(undefined); setResultError(''); setResultsLoading(true);
    async function observe() {
      try {
        const next = await queries.get(workspaceId, queryId, signal(controller));
        if (!validQuery(next, workspaceId, issuer, subject, { id: queryId })) throw new ApiError('invalid_response', 'The API returned an invalid query record.', 200);
        const rows = await queries.result(workspaceId, queryId, signal(controller));
        if (!validQueryResult(rows, workspaceId, queryId)) throw new ApiError('invalid_response', 'The API returned an invalid or mismatched result. No result rows were displayed.', 200);
        if (controller.signal.aborted) return;
        setQuery(next); setResult(rows); setResultError(''); setItems((current) => [next, ...current.filter((item) => item.id !== next.id)]);
        if (!queryTerminal(next.state)) timer = setTimeout(() => void observe(), 2000);
      } catch (err) {
        if (controller.signal.aborted) return;
        setResultError(message(err)); setResult(undefined);
        if (err instanceof ApiError && [401, 403, 404].includes(err.status)) { epoch.current += 1; setQuery(undefined); setItems([]); setCancelDialog(false); }
        else timer = setTimeout(() => void observe(), 5000);
      } finally { if (!controller.signal.aborted) setResultsLoading(false); }
    }
    void observe(); return () => { controller.abort(); clearTimeout(timer); };
  }, [workspaceId, queryId, refresh, issuer, subject]);

  async function submit() {
    if (inFlight.current || restored.error || (!intent && (!binding || blocked || !selectedCell))) return;
    const request = intent ?? { bindingId: binding!.id, idempotencyKey: crypto.randomUUID(), notebook: { id: document.id!, revision: document.revision!, cellId: selectedCell!.id } };
    try { window.sessionStorage.setItem(storageKey, JSON.stringify(request)); }
    catch { setError('Browser recovery storage is unavailable. No SQL request was sent. Restore session storage before submitting.'); return; }
    const controller = lifecycle.current!; const requestEpoch = epoch.current; const recovering = Boolean(intent);
    inFlight.current = true; setPending(true); setIntent(request); setError('');
    try {
      const next = await queries.submit(workspaceId, request, signal(controller));
      if (!validQuery(next, workspaceId, issuer, subject, { intent: request })) throw new ApiError('invalid_response', 'The SQL confirmation did not match your identity or saved cell selection. Recover using the same request key.', 200);
      forgetQueryIntent(storageKey, request);
      if (controller.signal.aborted || requestEpoch !== epoch.current) return;
      setIntent(undefined); setQuery(next); setQueryId(next.id); setResult(undefined); setDialog(false); setItems((current) => [next, ...current.filter((item) => item.id !== next.id)]);
    } catch (err) {
      if (controller.signal.aborted || requestEpoch !== epoch.current) return;
      setError(message(err));
      if (!recovering && err instanceof ApiError && ([400, 401, 403, 404, 422, 429, 501].includes(err.status) || err.code === 'sql_revision_conflict')) { forgetQueryIntent(storageKey, request); setIntent(undefined); if (err.code === 'sql_revision_conflict') onRevisionConflict(); }
    } finally { inFlight.current = false; if (!controller.signal.aborted) setPending(false); }
  }
  async function cancel() {
    if (!query || cancelling) return; const controller = lifecycle.current!; const requestEpoch = epoch.current; setCancelling(true); setError('');
    try {
      const next = await queries.cancel(workspaceId, query.id, signal(controller));
      if (!validQuery(next, workspaceId, issuer, subject, { id: query.id })) throw new ApiError('invalid_response', 'The API returned an invalid cancellation confirmation.', 200);
      if (controller.signal.aborted || requestEpoch !== epoch.current || selection.current !== next.id) return;
      setQuery(next); setCancelDialog(false); setRefresh((value) => value + 1);
    } catch (err) { if (!controller.signal.aborted) setError(`${message(err)} Cancellation is not confirmed; refresh the query before trying again.`); }
    finally { if (!controller.signal.aborted) setCancelling(false); }
  }
  return <>{visible && <section className="sql-workbench" aria-label="SQL execution"><div className="sql-workbench-heading"><div><span className="eyebrow">GOVERNED SQL</span><h2><Terminal size={17} />Run a saved SQL cell.</h2><p>The SQL engine applies the configured user identity and data policies. Results stay separate from shared notebook content.</p></div><button className="icon-button" aria-label="Refresh SQL queries" disabled={loading} onClick={() => setRefresh((value) => value + 1)}><RefreshCw size={15} /></button></div>
    <div className="sql-controls"><label className="field">SQL cell<select value={intent?.notebook.cellId ?? selectedCell?.id ?? ''} disabled={pending || Boolean(intent) || !sqlCells.length} onChange={(event) => setCellId(event.target.value)}>{intent && !sqlCells.some((cell) => cell.id === intent.notebook.cellId) && <option value={intent.notebook.cellId}>Saved cell {intent.notebook.cellId}</option>}{sqlCells.map((cell) => <option key={cell.id} value={cell.id}>Cell {cell.index + 1}</option>)}</select></label><label className="field">SQL execution binding<select value={intent?.bindingId ?? bindingId} disabled={loading || pending || Boolean(intent)} onChange={(event) => setBindingId(event.target.value)}>{intent && !binding && <option value={intent.bindingId}>{intent.bindingId} (unresolved)</option>}{!bindings.length && !intent && <option value="">No SQL binding granted</option>}{bindings.map((item) => <option key={item.id} value={item.id}>{item.name}</option>)}</select></label><button className="button primary" disabled={loading || pending || Boolean(restored.error) || Boolean(accessError) || (!intent && (!binding || Boolean(blocked)))} onClick={() => setDialog(true)}>{intent ? <><RefreshCw size={14} />Recover SQL submission</> : <><Play size={14} />Run saved SQL cell</>}</button></div>
    {!intent && <p className="query-control-note">{loading ? 'Checking SQL access…' : blocked || (!bindings.length ? 'Ask an administrator to grant a qualified SQL binding to your identity.' : `Saved revision ${document.revision}. Query submission may consume compute resources.`)}</p>}
    {accessError && <ErrorNotice>{accessError}</ErrorNotice>}{error && !dialog && !cancelDialog && <ErrorNotice>{error}</ErrorNotice>}
    {intent && <div className="notice warning query-intent" role="status"><div><strong>SQL submission outcome is unresolved.</strong><p>Notebook {intent.notebook.id} · revision {intent.notebook.revision} · cell {intent.notebook.cellId}. Recovery retains the original selection after edits or deletion.</p><code className="run-key">{intent.idempotencyKey}</code></div></div>}
    {items.length > 0 && <label className="field query-history"><span><History size={14} />Your queries for this document</span><select value={queryId} onChange={(event) => { setQuery(undefined); setResult(undefined); setQueryId(event.target.value); }}><option value="">Select a query to inspect</option>{items.map((item) => <option key={item.id} value={item.id}>{item.id} · revision {item.notebook.revision} · {item.state.replaceAll('_', ' ')}</option>)}</select></label>}
    {query && <div className="query-state" role="status"><div><Pill tone={query.state === 'succeeded' ? 'green' : ['unknown', 'cancel_requested'].includes(query.state) ? 'amber' : 'neutral'}>{query.state.replaceAll('_', ' ')}</Pill><code>{query.id}</code><p>{query.message}</p>{query.state === 'unknown' && <p>The engine outcome is unresolved. Inspect this query rather than submitting a replacement.</p>}{query.cancelRequested && !queryTerminal(query.state) && <p>Cancellation is requested. The query is not confirmed stopped until the engine reaches a terminal state.</p>}</div>{canSubmit && !queryTerminal(query.state) && <button className="button secondary compact" disabled={query.cancelRequested || cancelling} onClick={() => { setError(''); setCancelDialog(true); }}><Square size={13} />Cancel SQL query</button>}</div>}
  </section>}
    <ResultPanel key={queryId || 'empty'} query={query} result={result} error={resultError} loading={resultsLoading} retry={() => setRefresh((value) => value + 1)} />
    {dialog && <Modal title={intent ? 'Recover saved SQL query' : 'Review saved SQL query'} onClose={() => { if (!pending) setDialog(false); }}><div className="modal-body"><Pill>Saved revision {intent?.notebook.revision ?? document.revision}</Pill><p>Notebook {intent?.notebook.id ?? document.id}<br />Cell {intent?.notebook.cellId ?? selectedCell?.id}</p>{binding && <dl className="run-profile"><dt>SQL engine</dt><dd>{binding.engine}</dd><dt>Policy version</dt><dd>{binding.policyVersion}</dd><dt>Catalog</dt><dd>{binding.catalog}</dd><dt>Schema</dt><dd>{binding.schema}</dd><dt>Deadline</dt><dd>{binding.maxRunSeconds} seconds after acceptance</dd></dl>}<div className="notice"><ShieldCheck size={16} /><p>{intent ? 'Recovery checks the same request key and exact saved cell. A previously accepted query retains its original SQL and execution context.' : 'The API reads this exact saved SQL cell. Unsaved edits are not executed. Access depends on your current workspace membership, binding grant and SQL engine policies.'}</p></div>{intent && <code className="run-key">{intent.idempotencyKey}</code>}{error && <ErrorNotice>{error}</ErrorNotice>}{blocked && !intent && <ErrorNotice>{blocked}</ErrorNotice>}<div className="form-actions"><button className="button secondary" disabled={pending} onClick={() => setDialog(false)}>Close</button><button className="button primary" disabled={pending || Boolean(restored.error) || (!intent && (!binding || Boolean(blocked)))} onClick={() => void submit()}>{pending ? <Spinner label="Confirming SQL…" /> : intent ? 'Check SQL submission' : 'Submit saved SQL cell'}</button></div></div></Modal>}
    {cancelDialog && query && <Modal title="Cancel this SQL query?" onClose={() => { if (!cancelling) setCancelDialog(false); }}><div className="modal-body"><p>Request cancellation of <code>{query.id}</code>. An accepted request does not prove the engine has stopped. Already committed changes are not rolled back.</p>{error && <ErrorNotice>{error}</ErrorNotice>}<div className="form-actions"><button className="button secondary" disabled={cancelling} onClick={() => setCancelDialog(false)}>Keep query running</button><button className="button primary" disabled={cancelling} onClick={() => void cancel()}>{cancelling ? <Spinner label="Requesting cancellation…" /> : 'Request SQL cancellation'}</button></div></div></Modal>}
  </>;
}
