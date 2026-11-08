import { useEffect, useId, useRef, useState } from 'react';
import { CalendarClock, GitBranch, Play, RefreshCw, ShieldCheck } from 'lucide-react';
import { ApiError, message } from './api';
import type { Workspace } from './api';
import { useAuth } from './auth';
import { EmptyState, ErrorNotice, Modal, Pill, Spinner } from './ui';
import { forgetWorkflow, restoreWorkflow, sameWorkflowProfile, validWorkflowBinding, validWorkflowDefinition, validWorkflowHistory, validWorkflowRun, workflows, workflowStorageKey, workflowTerminal } from './workflow-api';
import type { WorkflowBinding, WorkflowDefinition, WorkflowHistory, WorkflowIntent, WorkflowRun } from './workflow-api';

const bounded = (c: AbortController) => AbortSignal.any([c.signal, AbortSignal.timeout(20000)]);
const when = (value?: string) => value ? new Date(value).toLocaleString(undefined, { timeZoneName: 'short' }) : 'Not recorded';
const tone = (state: string) => ['succeeded', 'success'].includes(state) ? 'green' as const : ['failed', 'unknown', 'upstream_failed'].includes(state) ? 'amber' as const : 'neutral' as const;
const reasons: Record<string, string> = { paused: 'This DAG is paused in Airflow.', stale: 'Airflow reports a stale DAG definition.', import_errors: 'Airflow reports DAG import errors.', version_mismatch: 'The deployed DAG differs from the approved version.' };

export function WorkflowWorkspace({ workspace, onDirty }: { workspace?: Workspace; onDirty: (dirty: boolean) => void }) {
  const auth = useAuth();
  if (!workspace) return <EmptyState icon={<GitBranch size={27} />} title="Select a workspace" description="Workflow access requires a workspace and an explicit Airflow DAG grant." />;
  return <Workflows key={JSON.stringify([auth.config?.issuer, auth.user?.profile.sub, workspace.id])} workspaceId={workspace.id} onDirty={onDirty} />;
}
function Workflows({ workspaceId, onDirty }: { workspaceId: string; onDirty: (dirty: boolean) => void }) {
  const auth = useAuth(); const issuer = auth.config?.issuer ?? 'development'; const subject = auth.user?.profile.sub ?? 'development-admin';
  const role = (auth.user?.profile.dataslate_workspaces as Record<string, unknown> | undefined)?.[workspaceId];
  const canEdit = auth.config?.mode === 'development' || role === 'editor' || role === 'admin';
  const storageKey = workflowStorageKey(workspaceId, issuer, subject); const [restored] = useState(() => restoreWorkflow(storageKey));
  const [intent, setIntent] = useState(restored.intent); const [bindings, setBindings] = useState<WorkflowBinding[]>([]); const [bindingId, setBindingId] = useState('');
  const [definition, setDefinition] = useState<WorkflowDefinition>(); const [history, setHistory] = useState<WorkflowHistory>();
  const [items, setItems] = useState<WorkflowRun[]>([]); const [selectedId, setSelectedId] = useState(''); const [selected, setSelected] = useState<WorkflowRun>();
  const [error, setError] = useState(restored.error ?? ''); const [accessError, setAccessError] = useState(''); const [definitionError, setDefinitionError] = useState(''); const [historyError, setHistoryError] = useState(''); const [runError, setRunError] = useState('');
  const [loading, setLoading] = useState(true); const [reading, setReading] = useState(false); const [busy, setBusy] = useState(false); const [refresh, setRefresh] = useState(0); const [filter, setFilter] = useState('');
  const [review, setReview] = useState<{ binding?: WorkflowBinding; definition?: WorkflowDefinition }>(); const [discard, setDiscard] = useState(false);
  const life = useRef<AbortController | undefined>(undefined); const inFlight = useRef(false); const currentBindings = useRef(bindings); currentBindings.current = bindings;
  const binding = bindings.find((b) => b.id === bindingId); const profileKey = binding ? JSON.stringify(binding) : '';
  useEffect(() => { const c = new AbortController(); life.current = c; return () => c.abort(); }, []);
  useEffect(() => { onDirty(Boolean(intent)); return () => onDirty(false); }, [intent, onDirty]);
  useEffect(() => {
    const c = new AbortController(); let timer: ReturnType<typeof setTimeout> | undefined; let checking = false; setLoading(true); setAccessError('');
    async function observe() {
      if (checking || c.signal.aborted) return; checking = true; clearTimeout(timer);
      try {
        const [grants, runs] = await Promise.all([workflows.bindings(workspaceId, bounded(c)), workflows.list(workspaceId, bounded(c))]);
        if (!Array.isArray(grants.items) || grants.items.length > 1000 || !grants.items.every((b) => validWorkflowBinding(b, workspaceId)) || new Set(grants.items.map((b) => b.id)).size !== grants.items.length || !Array.isArray(runs.items) || runs.items.length > 200 || !runs.items.every((r) => { const grant = grants.items.find((b) => b.id === r.bindingId); return !!grant && validWorkflowRun(r, workspaceId, issuer, subject, { binding: grant }); }) || new Set(runs.items.map((r) => r.id)).size !== runs.items.length) throw new Error('Invalid workflow grants or owner-scoped run history.');
        if (c.signal.aborted) return; setBindings(grants.items); setItems(runs.items); setAccessError(''); setBindingId((value) => grants.items.some((b) => b.id === value) ? value : grants.items[0]?.id ?? ''); setSelectedId((value) => runs.items.some((r) => r.id === value) ? value : ''); timer = setTimeout(() => void observe(), 10000);
      } catch (e) { if (!c.signal.aborted) { setBindings([]); setItems([]); setDefinition(undefined); setHistory(undefined); setSelected(undefined); setReview(undefined); setAccessError(message(e)); } }
      finally { checking = false; if (!c.signal.aborted) setLoading(false); }
    }
    const visible = () => { if (document.visibilityState === 'visible') void observe(); }; void observe(); document.addEventListener('visibilitychange', visible);
    return () => { c.abort(); clearTimeout(timer); document.removeEventListener('visibilitychange', visible); };
  }, [workspaceId, issuer, subject, refresh]);
  useEffect(() => {
    setDefinition(undefined); setHistory(undefined); setDefinitionError(''); setHistoryError(''); if (!binding) return;
    const currentBinding = binding; const c = new AbortController(); let timer: ReturnType<typeof setTimeout> | undefined; let checking = false; setReading(true);
    async function observe() {
      if (checking || c.signal.aborted) return; checking = true; clearTimeout(timer);
      const results = await Promise.allSettled([workflows.definition(workspaceId, currentBinding.id, bounded(c)), workflows.history(workspaceId, currentBinding.id, bounded(c))]);
      if (!c.signal.aborted) {
        const [dag, runs] = results;
        if (dag.status === 'fulfilled' && validWorkflowDefinition(dag.value, currentBinding)) { setDefinition(dag.value); setDefinitionError(''); }
        else { setDefinition(undefined); setDefinitionError(dag.status === 'rejected' ? message(dag.reason) : 'The workflow definition did not match its approved profile.'); }
        if (runs.status === 'fulfilled' && validWorkflowHistory(runs.value, currentBinding)) { setHistory(runs.value); setHistoryError(''); }
        else { setHistory(undefined); setHistoryError(runs.status === 'rejected' ? message(runs.reason) : 'Invalid Airflow history. Protected configuration or output is not allowed here.'); }
        setReading(false); if (dag.status === 'fulfilled' && runs.status === 'fulfilled') timer = setTimeout(() => void observe(), 10000);
      }
      checking = false;
    }
    const visible = () => { if (document.visibilityState === 'visible') void observe(); }; void observe(); document.addEventListener('visibilitychange', visible);
    return () => { c.abort(); clearTimeout(timer); document.removeEventListener('visibilitychange', visible); };
  }, [workspaceId, profileKey, refresh]);
  useEffect(() => {
    setSelected(undefined); setRunError(''); if (!selectedId) return;
    const c = new AbortController(); let timer: ReturnType<typeof setTimeout> | undefined; let checking = false; let revision = 0;
    async function observe() {
      if (checking || c.signal.aborted) return; checking = true; clearTimeout(timer);
      try {
        const run = await workflows.get(workspaceId, selectedId, bounded(c));
        const grant = currentBindings.current.find((b) => b.id === run.bindingId);
        if (!grant || !validWorkflowRun(run, workspaceId, issuer, subject, { id: selectedId, binding: grant }) || run.revision < revision) throw new Error('The workflow run does not match its owner, identity or latest observed revision.');
        if (c.signal.aborted) return; revision = run.revision; setSelected(run); setRunError(''); setItems((v) => [run, ...v.filter((r) => r.id !== run.id)]); if (!workflowTerminal(run)) timer = setTimeout(() => void observe(), 2000);
      } catch (e) { if (!c.signal.aborted) { setSelected(undefined); setItems((v) => v.filter((r) => r.id !== selectedId)); setRunError(message(e)); } }
      finally { checking = false; }
    }
    const visible = () => { if (document.visibilityState === 'visible') void observe(); }; void observe(); document.addEventListener('visibilitychange', visible);
    return () => { c.abort(); clearTimeout(timer); document.removeEventListener('visibilitychange', visible); };
  }, [workspaceId, issuer, subject, selectedId, refresh]);
  const reviewEligible = !!review?.binding && !!binding && sameWorkflowProfile(review.binding, binding) && definition?.canTrigger && canEdit;
  async function openReview() {
    if (intent) { setError(''); setReview({}); return; }
    if (!binding || !canEdit || !definition?.canTrigger || inFlight.current) return;
    const expected = binding; const c = life.current!; setBusy(true); setError(''); inFlight.current = true;
    try { const current = await workflows.definition(workspaceId, expected.id, bounded(c)); if (!validWorkflowDefinition(current, expected)) throw new Error('The workflow definition changed. Refresh its current approved profile.'); if (!c.signal.aborted) { setDefinition(current); if (!current.canTrigger) throw new Error('This workflow is not currently eligible for a manual run.'); setReview({ binding: expected, definition: current }); } }
    catch (e) { if (!c.signal.aborted) setError(message(e)); } finally { inFlight.current = false; if (!c.signal.aborted) setBusy(false); }
  }
  async function submit() {
    if (inFlight.current || restored.error || (!intent && !reviewEligible)) return;
    const recovering = !!intent; const approved = review?.binding; const input: WorkflowIntent = intent ?? { bindingId: approved!.id, idempotencyKey: crypto.randomUUID(), policyVersion: approved!.policyVersion, dagId: approved!.dagId, dagVersion: approved!.dagVersion, bundleVersion: approved!.bundleVersion };
    try { sessionStorage.setItem(storageKey, JSON.stringify(input)); } catch { setError('Browser recovery storage is unavailable. No workflow request was sent.'); return; }
    setIntent(input); setBusy(true); setError(''); inFlight.current = true; const c = life.current!;
    try {
      const reply = await workflows.submit(workspaceId, input, bounded(c));
      if (typeof reply.replayed !== 'boolean' || !validWorkflowRun(reply.run, workspaceId, issuer, subject, { intent: input, ...(!recovering && approved ? { binding: approved } : {}) })) throw new Error('The workflow receipt does not match the original request and owner. Recover with the same key.');
      forgetWorkflow(storageKey, input); if (c.signal.aborted) return; setIntent(undefined); setReview(undefined); setSelectedId(reply.run.id); setItems((v) => [reply.run, ...v.filter((r) => r.id !== reply.run.id)]);
    } catch (e) {
      if (c.signal.aborted) return; setError(message(e));
      if (!recovering && e instanceof ApiError && [400, 401, 403, 404, 422, 429, 501].includes(e.status)) { try { forgetWorkflow(storageKey, input); setIntent(undefined); } catch { /* Keep exact recovery if storage becomes unavailable. */ } }
    } finally { inFlight.current = false; if (!c.signal.aborted) setBusy(false); }
  }
  const visibleBindings = bindings.filter((b) => `${b.name} ${b.dagId}`.toLocaleLowerCase().includes(filter.toLocaleLowerCase()));
  return <div className="page-wrap workflow-page"><div className="page-heading"><div><span className="eyebrow">WORKFLOWS / APACHE AIRFLOW</span><h1>Orchestrate with a clear view.</h1><p>Inspect approved workflows, their schedules and observed task progress. Airflow owns scheduling and retries; your manual runs keep a durable DataSlate receipt.</p></div><button className="button secondary" disabled={loading || busy} onClick={() => setRefresh((v) => v + 1)}><RefreshCw size={15} />Refresh workflows</button></div>
    {loading && <Spinner label="Checking workflow access…" />}{accessError && <ErrorNotice>{accessError}</ErrorNotice>}{error && !review && <ErrorNotice>{error}</ErrorNotice>}
    {intent && <div className="notice warning workflow-recovery"><div><strong>Workflow submission is unresolved.</strong><p>Binding {intent.bindingId}. Check the original key before starting another run. Recovery returns the same DataSlate request; it never asks Airflow to trigger it twice.</p><code className="run-key">{intent.idempotencyKey}</code><div className="workflow-recovery-actions"><button className="text-button" disabled={busy || !!restored.error} onClick={() => void openReview()}>Recover workflow submission</button><button className="text-button" disabled={busy} onClick={() => setDiscard(true)}>Discard workflow recovery</button></div></div></div>}
    {!loading && !bindings.length && !accessError ? <EmptyState icon={<GitBranch size={28} />} title="No workflows granted" description="An administrator can connect an existing Airflow 3.4.0 DAG with an approved immutable bundle and grant your identity access. No workflow has been installed or triggered." /> : !!bindings.length && <div className="workflow-layout"><aside className="workflow-bindings" aria-label="Approved workflows"><h2>Approved workflows</h2><p>Current grants · {bindings.length}</p><label className="field">Filter workflows<input value={filter} onChange={(e) => setFilter(e.target.value)} /></label>{visibleBindings.map((b) => <button className="workflow-choice" aria-pressed={b.id === bindingId} key={b.id} onClick={() => { setReview(undefined); setBindingId(b.id); }}><GitBranch size={17} /><span><strong>{b.name}</strong><small>{b.dagId} · version {b.dagVersion}</small></span></button>)}{!visibleBindings.length && <p>No matching workflows.</p>}</aside><section className="workflow-detail" aria-label="Workflow definition">{reading && !definition ? <Spinner label="Reading deployed workflow…" /> : <>{definitionError && <ErrorNotice>{definitionError}</ErrorNotice>}{binding && <><span className="eyebrow">APPROVED DAG</span><h2>{binding.name}</h2><p className="muted">{binding.dagId}</p></>}{definition && binding && <><Pill tone={definition.canTrigger ? 'green' : 'amber'}>{definition.canTrigger ? 'Approved version available' : 'Manual run unavailable'}</Pill><div className="workflow-summary"><div><span>Schedule</span><strong>{definition.scheduleSummary}</strong></div><div><span>Scheduler status</span><strong>{definition.paused ? 'Paused' : 'Unpaused'}</strong></div><div><span>Time zone</span><strong>{definition.timezone}</strong></div></div><dl className="run-profile"><dt>Approved DAG version</dt><dd>{binding.dagVersion}</dd><dt>Immutable bundle</dt><dd><code>{binding.bundleVersion}</code></dd><dt>Policy</dt><dd>{binding.policyVersion}</dd><dt>Next scheduled run</dt><dd>{definition.nextRunAt ? when(definition.nextRunAt) : 'Not reported by Airflow'}</dd><dt>Observed</dt><dd>{when(definition.observedAt)}</dd><dt>Observation limit</dt><dd>{binding.maxRunSeconds} seconds</dd></dl>{!definition.canTrigger && <div className="notice warning"><p>{reasons[definition.reasonCode] ?? 'The deployed workflow does not currently meet its approved trigger policy.'} {definition.reasonCode && <code>{definition.reasonCode}</code>} An administrator must resolve the condition in Airflow or update the approved binding.</p></div>}<button className="button primary" disabled={busy || !!intent || !!restored.error || !canEdit || !definition.canTrigger} onClick={() => void openReview()}><Play size={15} />Review manual run</button>{!canEdit && <p className="field-help">Manual triggers require an editor or administrator workspace role.</p>}<p className="field-help">A manual run is separate from the schedule. Reaching the observation limit does not stop Airflow tasks.</p><h3>Deployed task graph</h3><WorkflowGraph tasks={definition.tasks} /><div className="query-table-scroll workflow-tasks" role="region" aria-label="Deployed task graph" tabIndex={0}><table className="query-table"><thead><tr><th scope="col">Task</th><th scope="col">Downstream tasks</th></tr></thead><tbody>{definition.tasks.map((task) => <tr key={task.id}><th scope="row"><code>{task.id}</code></th><td>{task.downstreamTaskIds.length ? task.downstreamTaskIds.join(', ') : 'End of branch'}</td></tr>)}</tbody></table></div>{!definition.tasks.length && <p className="muted">No deployed tasks reported.</p>}<p className="field-help">This graph and schedule describe the currently deployed DAG. Editing source, schedules and backfills requires the Airflow deployment workflow.</p></>}</>}</section></div>}
    {binding && <section className="workflow-history" aria-label="Airflow scheduled and manual history"><div className="section-toolbar"><div><span className="eyebrow">SCHEDULER OBSERVATIONS</span><h2><CalendarClock size={17} /> Recent Airflow runs</h2></div>{history && <Pill>{history.truncated ? 'Latest 50 · partial history' : `${history.items.length} observed runs`}</Pill>}</div><p>Your explicit DAG grant includes sanitized scheduled and manual run metadata. These runs are not assumed to belong to your DataSlate identity.</p>{historyError && <ErrorNotice>{historyError}</ErrorNotice>}{history && <>{history.items.length ? <div className="query-table-scroll" role="region" aria-label="Recent Airflow runs" tabIndex={0}><table className="query-table"><thead><tr><th scope="col">Airflow run</th><th scope="col">Type / state</th><th scope="col">Version</th><th scope="col">Logical date</th><th scope="col">Started / ended</th></tr></thead><tbody>{history.items.map((r) => <tr key={r.backendRunId}><th scope="row"><code>{r.backendRunId}</code></th><td>{r.runType}<br /><Pill tone={tone(r.state)}>{r.state}</Pill></td><td>{r.dagVersion}<br /><code>{r.bundleVersion}</code></td><td>{when(r.logicalDate)}</td><td>{when(r.startedAt)}<br />{when(r.endedAt)}</td></tr>)}</tbody></table></div> : <p>No upstream runs were returned.</p>}<p className="field-help">Observed {when(history.observedAt)}. Logs, configuration and output values are excluded.</p></>}</section>}
    <section className="workflow-history" aria-label="Your workflow requests"><span className="eyebrow">PRIVATE MANUAL REQUESTS</span><h2>Your DataSlate runs</h2><p>Only your requests under current binding grants appear here. Acceptance records an intent; Airflow observation confirms execution.</p>{runError && <ErrorNotice>{runError}</ErrorNotice>}{!loading && !items.length && <p className="muted">No authorized manual workflow requests.</p>}{!!items.length && <label className="field">Your workflow run<select value={selectedId} onChange={(e) => setSelectedId(e.target.value)}><option value="">Select a request</option>{items.map((r) => <option key={r.id} value={r.id}>{r.dagId} · {r.state} · {r.id}</option>)}</select></label>}{selected && <WorkflowRunDetail run={selected} />}</section>
    {review && <Modal title={intent ? 'Recover workflow submission' : 'Review manual workflow run'} onClose={() => { if (!busy) setReview(undefined); }}><div className="modal-body"><p>{intent ? 'Check the original approval and key. Recovery cannot substitute a newer DAG or policy. A lost acknowledgment is not evidence that a run failed to start.' : 'Trigger the approved immutable DAG bundle once. Airflow schedules tasks and owns their retry policy.'}</p><dl className="run-profile"><dt>Workflow binding</dt><dd>{intent?.bindingId ?? review.binding?.id}</dd>{(intent || review.binding) && <><dt>DAG / version</dt><dd>{intent?.dagId ?? review.binding?.dagId} / {intent?.dagVersion ?? review.binding?.dagVersion}</dd><dt>Bundle</dt><dd><code>{intent?.bundleVersion ?? review.binding?.bundleVersion}</code></dd><dt>Policy</dt><dd>{intent?.policyVersion ?? review.binding?.policyVersion}</dd>{!intent && <><dt>Schedule</dt><dd>{review.definition?.scheduleSummary}</dd></>}</>}</dl>{intent && <code className="run-key">{intent.idempotencyKey}</code>}<div className="notice workflow-reviewed"><ShieldCheck size={16} /><p>This action does not edit the schedule or supply arbitrary parameters. No automatic rerun or cancellation is sent. An uncertain upstream outcome is reconciled by reading the same Airflow run.</p></div>{!intent && !reviewEligible && <ErrorNotice>Current workflow access or eligibility changed. Close this review and refresh before submitting.</ErrorNotice>}{error && <ErrorNotice>{error}</ErrorNotice>}<div className="form-actions"><button className="button secondary" disabled={busy} onClick={() => setReview(undefined)}>Close</button><button className="button primary" disabled={busy || !!restored.error || (!intent && !reviewEligible)} onClick={() => void submit()}>{busy ? <Spinner label="Confirming workflow request…" /> : intent ? 'Check workflow submission' : 'Submit reviewed workflow'}</button></div></div></Modal>}
    {discard && <Modal title="Discard workflow recovery?" onClose={() => setDiscard(false)}><div className="modal-body"><p>This removes this tab’s request key. It does not stop or remove a DataSlate request or Airflow run. Inspect existing runs before starting a replacement.</p><button className="button primary" onClick={() => { if (!intent) return; try { forgetWorkflow(storageKey, intent); setIntent(undefined); setDiscard(false); setReview(undefined); setRefresh((v) => v + 1); } catch { setError('Workflow recovery could not be cleared.'); } }}>Discard saved workflow request</button></div></Modal>}
  </div>;
}
function WorkflowRunDetail({ run }: { run: WorkflowRun }) {
  return <div className="workflow-run" role="region" aria-label="Workflow run details"><div className="section-toolbar"><Pill tone={tone(run.state)}>{run.state}</Pill><code>{run.id}</code></div><p>{run.message}</p><dl className="run-profile"><dt>Airflow run</dt><dd><code>{run.backendRunId}</code></dd><dt>DAG / version</dt><dd>{run.dagId} / {run.dagVersion}</dd><dt>Bundle / policy</dt><dd><code>{run.bundleVersion}</code> / {run.policyVersion}</dd><dt>Dispatch intent</dt><dd>{run.dispatchStarted ? 'Recorded; acknowledgment alone is not completion' : 'Not dispatched'}</dd><dt>Last observation</dt><dd>{when(run.observedAt)}</dd><dt>Started / ended</dt><dd>{when(run.startedAt)} / {when(run.endedAt)}</dd><dt>Reconciliation</dt><dd>{run.reconciliationComplete ? 'Finished' : 'In progress'}</dd></dl>{run.state === 'unknown' && <div className="notice warning"><p>{run.reconciliationComplete ? 'Automatic reconciliation has ended. The Airflow run may still be active. An administrator must inspect it; no new trigger or cancellation is sent.' : 'The upstream outcome is uncertain. DataSlate is reading the same Airflow run within its observation window; no new trigger is sent.'}</p></div>}{run.state === 'failed' && !run.dispatchStarted && <p>The request failed before dispatch. This is not a reported Airflow task failure.</p>}<h3>Observed tasks</h3>{run.tasksTruncated && <div className="notice warning"><p>Task observations are incomplete. The DAG run state determines success or failure; this list cannot establish completion.</p></div>}{run.tasks.length ? <div className="query-table-scroll workflow-tasks" role="region" aria-label="Observed workflow tasks" tabIndex={0}><table className="query-table"><thead><tr><th scope="col">Task</th><th scope="col">Map index</th><th scope="col">Try</th><th scope="col">State</th><th scope="col">Started / ended</th></tr></thead><tbody>{run.tasks.map((t) => <tr key={JSON.stringify([t.id, t.mapIndex])}><th scope="row"><code>{t.id}</code></th><td>{t.mapIndex === -1 ? 'Unmapped' : t.mapIndex}</td><td>{t.tryNumber}</td><td><Pill tone={tone(t.state)}>{t.state}</Pill></td><td>{when(t.startedAt)}<br />{when(t.endedAt)}</td></tr>)}</tbody></table></div> : <p className="muted">No task observations yet. An empty list does not confirm completion.</p>}</div>;
}

function WorkflowGraph({ tasks }: { tasks: WorkflowDefinition['tasks'] }) {
  const arrow = useId();
  if (!tasks.length || tasks.length > 24) return null;
  const incoming = new Map(tasks.map((t) => [t.id, 0]));
  for (const task of tasks) for (const next of task.downstreamTaskIds) incoming.set(next, (incoming.get(next) ?? 0) + 1);
  const levels: string[][] = []; const placed = new Set<string>();
  while (placed.size < tasks.length) {
    const level = tasks.filter((t) => !placed.has(t.id) && incoming.get(t.id) === 0).map((t) => t.id);
    if (!level.length) return null;
    levels.push(level); for (const id of level) placed.add(id);
    for (const id of level) for (const next of tasks.find((t) => t.id === id)!.downstreamTaskIds) incoming.set(next, incoming.get(next)! - 1);
  }
  const positions = new Map<string, { x: number; y: number }>();
  levels.forEach((level, x) => level.forEach((id, y) => positions.set(id, { x: x * 190 + 14, y: y * 76 + 14 })));
  const width = levels.length * 190 + 4; const height = Math.max(...levels.map((l) => l.length)) * 76 + 4;
  return <div className="workflow-graph" role="region" aria-label="Workflow dependency diagram" tabIndex={0}><svg width={width} height={height} role="img" aria-label={`${tasks.length} tasks and their deployed dependencies`}><defs><marker id={arrow} markerWidth="7" markerHeight="7" refX="6" refY="3.5" orient="auto"><path d="M0 0L7 3.5L0 7" fill="#8aa99a" /></marker></defs>{tasks.flatMap((task) => task.downstreamTaskIds.map((next) => { const a = positions.get(task.id)!, b = positions.get(next)!; const from = a.x + 150, to = b.x - 5; return <path key={`${task.id}:${next}`} d={`M${from} ${a.y + 25} C${(from + to) / 2} ${a.y + 25} ${(from + to) / 2} ${b.y + 25} ${to} ${b.y + 25}`} stroke="#8aa99a" strokeWidth="1.5" fill="none" markerEnd={`url(#${arrow})`} />; }))}{tasks.map((task) => { const pos = positions.get(task.id)!; return <g key={task.id} transform={`translate(${pos.x},${pos.y})`}><title>{task.id}</title><rect width="150" height="50" rx="7" fill="#f0f6f0" stroke="#d2e1d5" /><circle cx="14" cy="25" r="4" fill="#56836d" /><text x="27" y="29" fill="#244d40" fontSize="11" fontFamily="DM Sans, sans-serif">{task.id.length > 18 ? task.id.slice(0, 16) + '…' : task.id}</text></g>; })}</svg></div>;
}
