import { useEffect, useRef, useState } from 'react';
import { Bookmark, RefreshCw } from 'lucide-react';
import { ApiError, message } from './api';
import { sylonAPI, validSylonJob } from './sylon-api';
import type { SylonBinding, SylonJob } from './sylon-api';
import { memoryAPI, memoryBytes, memoryStorageKey, restoreMemory, validMemory, validMemoryFields, validMemoryMutation, validMemorySummary } from './sylon-memory-api';
import type { MemoryFields, MemoryIntent, MemoryReceiptReference, MemoryRef, MemorySummary, PrivateMemory } from './sylon-memory-api';
import { EmptyState, ErrorNotice, Modal, Pill, Spinner } from './ui';

const bounded = (c: AbortController) => AbortSignal.any([c.signal, AbortSignal.timeout(20000)]);
const expiryLabel = (value: string) => new Date(value).toLocaleString();
function localDate(value: string) { const d = new Date(value); return new Date(d.getTime() - d.getTimezoneOffset() * 60000).toISOString().slice(0, 16); }
function utcDate(value: string) { const d = new Date(value); return Number.isFinite(d.getTime()) ? d.toISOString() : ''; }
interface Identity { workspaceId: string; issuer: string; subject: string }
interface Editor { originalExpiresAt?: string; operation: 'create' | 'correct'; bindingId: string; fields: MemoryFields; source?: { jobId: string; revision: number }; id?: string; revision?: number }
interface Props extends Identity { active: boolean; bindings: SylonBinding[]; saveJob?: SylonJob; onSaveClosed: () => void; onDirty: (dirty: boolean) => void; onChanged: (id: string) => void }

export function SylonMemories({ active, bindings, saveJob, onSaveClosed, onDirty, onChanged, workspaceId, issuer, subject }: Props) {
  const key = memoryStorageKey(workspaceId, issuer, subject);
  const [restored] = useState(() => restoreMemory(key)); const [pending, setPending] = useState(restored.intent); const pendingRef = useRef(restored.intent);
  const [reference, setReference] = useState(restored.reference);
  const [items, setItems] = useState<MemorySummary[]>([]); const [loading, setLoading] = useState(false); const [busy, setBusy] = useState(false); const [error, setError] = useState(restored.error ?? ''); const [notice, setNotice] = useState(''); const [refresh, setRefresh] = useState(0);
  const [view, setView] = useState<PrivateMemory>(); const [editor, setEditor] = useState<Editor>(); const [remove, setRemove] = useState<MemorySummary>(); const [recover, setRecover] = useState(false); const [discard, setDiscard] = useState(false); const mutation = useRef(false); const life = useRef<AbortController | undefined>(undefined);
  const policy = bindings.find((b) => b.id === editor?.bindingId)?.memory;
  useEffect(() => { const c = new AbortController(); life.current = c; return () => c.abort(); }, []);
  useEffect(() => { onDirty(Boolean(editor || pending || reference)); return () => onDirty(false); }, [editor, pending, reference, onDirty]);
  useEffect(() => {
    if (!saveJob) return;
    if (pendingRef.current || reference) { setNotice('Resolve the pending memory change before saving another answer.'); onSaveClosed(); return; }
    const profile = bindings.find((b) => b.id === saveJob.bindingId);
    if (!profile?.memory || saveJob.state !== 'succeeded' || !saveJob.result) { onSaveClosed(); return; }
    setError(''); setView(undefined); setEditor({ operation: 'create', bindingId: profile.id, source: { jobId: saveJob.id, revision: saveJob.revision }, fields: { title: 'Saved Sylon answer', content: memoryBytes(saveJob.result.answer) <= 4096 ? saveJob.result.answer : '', expiresAt: new Date(Date.now() + profile.memory.defaultTTLDays * 86400000).toISOString() } });
    onSaveClosed();
  }, [saveJob, bindings, onSaveClosed, reference]);
  useEffect(() => {
    if (!active) return; const c = new AbortController(); let timer: ReturnType<typeof setTimeout> | undefined;
    setLoading(true); setError(restored.error ?? ''); setItems([]); setView(undefined);
    async function observe() {
      try { const r = await memoryAPI.list(workspaceId, bounded(c)); if (c.signal.aborted) return; if (!Array.isArray(r.items) || r.items.length > 100 || !r.items.every((m) => validMemorySummary(m, workspaceId))) throw new Error('Invalid private memory management response.'); setItems(r.items); const expiries = r.items.filter((m) => m.state === 'available').map((m) => Date.parse(m.expiresAt) - Date.now()); const delay = r.items.some((m) => m.purgePending) ? 2000 : expiries.length ? Math.max(1000, Math.min(...expiries, 2147483647)) : undefined; if (delay !== undefined) timer = setTimeout(() => void observe(), delay); }
      catch (e) { if (!c.signal.aborted) { setItems([]); setError(message(e)); } }
      finally { if (!c.signal.aborted) setLoading(false); }
    }
    void observe(); return () => { c.abort(); clearTimeout(timer); };
  }, [active, workspaceId, refresh, restored.error]);
  function persist(intent?: MemoryIntent) { if (intent) sessionStorage.setItem(key, JSON.stringify(intent)); else sessionStorage.removeItem(key); pendingRef.current = intent; setPending(intent); setReference(undefined); }
  // Recheck visible/temporarily recovered source material; server authorization remains authoritative.
  const guardMemoryId = view?.id ?? (editor?.operation === 'correct' ? editor.id : undefined) ?? (pending?.operation === 'correct' ? pending.id : undefined);
  const guardMemoryRevision = view?.revision ?? (editor?.operation === 'correct' ? editor.revision : undefined) ?? (pending?.operation === 'correct' ? pending.input.revision : undefined);
  const guardJobId = editor?.source?.jobId ?? (pending?.operation === 'create' ? pending.input.source.jobId : undefined);
  const guardExpiresAt = view?.expiresAt ?? editor?.originalExpiresAt ?? (pending && pending.operation !== 'delete' ? pending.input.expiresAt : undefined);
  useEffect(() => {
    if (!guardMemoryId && !guardJobId) return;
    const c = new AbortController(); let timer: ReturnType<typeof setTimeout> | undefined; let checking = false;
    function clearProtected(reason: string) {
      if (c.signal.aborted) return;
      setView(undefined); setEditor(undefined); setItems([]); setError(reason);
      const intent = pendingRef.current;
      if (intent?.operation === 'correct') {
        const safe: MemoryReceiptReference = { kind: 'unresolved-receipt', operation: 'correct', id: intent.id, revision: intent.input.revision, idempotencyKey: intent.input.idempotencyKey };
        pendingRef.current = undefined; setPending(undefined); setReference(safe);
        try { sessionStorage.setItem(key, JSON.stringify(safe)); } catch { try { sessionStorage.removeItem(key); } catch { /* The warning tells the user to close the tab if storage cannot be cleared. */ } setError(`${reason} Browser recovery could not be replaced safely; close this tab to discard its temporary data.`); }
      } else if (intent && intent.operation !== 'delete') { try { persist(); } catch { pendingRef.current = undefined; setPending(undefined); setError(`${reason} Browser recovery could not be cleared; close this tab to discard its temporary data.`); } }
      setRecover(false); setNotice('Protected memory content was cleared after an access, revision or expiry check. An earlier mutation may already have completed; inspect the current memory before making a replacement.');
    }
    async function check() {
      if (checking || c.signal.aborted) return; checking = true;
      try {
        if (guardExpiresAt && Date.parse(guardExpiresAt) <= Date.now()) { clearProtected('This memory expired. Its content is no longer available.'); return; }
        if (guardMemoryId) { const m = await memoryAPI.get(workspaceId, guardMemoryId, bounded(c)); if (!validMemory(m, workspaceId, issuer, subject) || m.id !== guardMemoryId) { clearProtected('Private memory access could not be verified.'); return; } if (m.revision !== guardMemoryRevision || m.purgePending) { clearProtected('This memory revision changed. Its earlier content is no longer available.'); return; } if (Date.parse(m.expiresAt) <= Date.now()) { clearProtected('This memory expired. Its content is no longer available.'); return; } }
        else if (guardJobId) { const j = await sylonAPI.job(workspaceId, guardJobId, bounded(c)); if (!validSylonJob(j, workspaceId, issuer, subject, { id: guardJobId }) || j.state !== 'succeeded' || !j.result) { clearProtected('Source answer access could not be verified.'); return; } }
      } catch (e) { if (e instanceof ApiError && [401, 403, 404].includes(e.status)) clearProtected(message(e)); }
      finally { checking = false; if (!c.signal.aborted) timer = setTimeout(() => void check(), Math.max(1, Math.min(10000, guardExpiresAt ? Date.parse(guardExpiresAt) - Date.now() : 10000))); }
    }
    const visible = () => { if (document.visibilityState === 'visible') void check(); };
    timer = setTimeout(() => void check(), Math.max(1, Math.min(10000, guardExpiresAt ? Date.parse(guardExpiresAt) - Date.now() : 10000)));
    document.addEventListener('visibilitychange', visible);
    return () => { c.abort(); clearTimeout(timer); document.removeEventListener('visibilitychange', visible); };
  }, [workspaceId, issuer, subject, guardMemoryId, guardMemoryRevision, guardJobId, guardExpiresAt]);
  async function read(item: MemorySummary, correct = false) {
    if (mutation.current || pending || correct && reference) return; setBusy(true); setView(undefined); setEditor(undefined); setError(''); const c = life.current!;
    try { const next = await memoryAPI.get(workspaceId, item.id, bounded(c)); if (!validMemory(next, workspaceId, issuer, subject) || next.id !== item.id || next.revision !== item.revision || Date.parse(next.expiresAt) <= Date.now() || next.purgePending) throw new Error('This memory changed. Refresh the list before opening its current revision.'); if (c.signal.aborted) return; if (correct) setEditor({ operation: 'correct', originalExpiresAt: next.expiresAt, bindingId: next.bindingId, id: next.id, revision: next.revision, fields: { title: next.title, content: next.content, expiresAt: next.expiresAt } }); else setView(next); }
    catch (e) { if (!c.signal.aborted) setError(message(e)); } finally { if (!c.signal.aborted) setBusy(false); }
  }
  async function inspectReference() {
    if (!reference || mutation.current) return; setBusy(true); setView(undefined); setError(''); const c = life.current!;
    try { const next = await memoryAPI.get(workspaceId, reference.id, bounded(c)); if (!validMemory(next, workspaceId, issuer, subject) || next.id !== reference.id || next.purgePending || Date.parse(next.expiresAt) <= Date.now()) throw new Error('The current memory is not available for inspection.'); if (!c.signal.aborted) setView(next); }
    catch (e) { if (!c.signal.aborted) setError(message(e)); } finally { if (!c.signal.aborted) setBusy(false); }
  }
  async function mutate(intent: MemoryIntent, recovering = false) {
    if (mutation.current || restored.error || reference) return;
    try { persist(intent); } catch { setError('Browser recovery storage is unavailable. No memory change was sent.'); return; }
    mutation.current = true; setBusy(true); setError(''); const c = life.current!;
    try { const response = await memoryAPI.mutate(workspaceId, intent, bounded(c)); if (!validMemoryMutation(response, workspaceId, intent)) throw new Error('The memory receipt did not match the original request. Recover with the same key.'); persist(); if (c.signal.aborted) return; setEditor(undefined); setRemove(undefined); setView(undefined); setRecover(false); setRefresh((v) => v + 1); onChanged(response.receipt.memoryId); setNotice(`${intent.operation === 'delete' ? 'Memory deleted' : intent.operation === 'correct' ? 'Memory corrected' : 'Private memory saved'} · revision ${response.receipt.revision}. ${intent.operation === 'create' ? 'Select it explicitly when you want to use it.' : 'Previous selections and dependent conversation context are no longer available.'}`); }
    catch (e) { if (c.signal.aborted) return; setError(message(e)); if (!recovering && e instanceof ApiError && [400, 401, 403, 404, 409, 422, 429, 501].includes(e.status)) { try { persist(); } catch { /* Preserve the pending intent if browser storage fails. */ } } }
    finally { mutation.current = false; if (!c.signal.aborted) setBusy(false); }
  }
  function save() { if (!editor || !validMemoryFields(editor.fields, policy)) return; const idempotencyKey = crypto.randomUUID(); void mutate(editor.operation === 'create' ? { operation: 'create', input: { idempotencyKey, source: editor.source!, ...editor.fields } } : { operation: 'correct', id: editor.id!, input: { idempotencyKey, revision: editor.revision!, ...editor.fields } }); }
  const modalOpen = Boolean(editor || remove || recover || discard);
  return <>
    <section hidden={!active} className="sylon-memory" aria-label="Private memory library"><div className="section-toolbar"><div><h2>Private memory</h2><p>Private to you in this workspace.</p></div><button className="icon-button" aria-label="Refresh private memories" disabled={busy} onClick={() => setRefresh((v) => v + 1)}><RefreshCw size={16} /></button></div><p className="field-help">Save a successful Sylon answer after reviewing its summary. All original source permissions still apply. Deleting its source conversation deletes the memory.</p>
      {loading && <Spinner label="Loading private memories…" />}
      {!loading && !items.length && !error && <EmptyState icon={<Bookmark size={24} />} title="Nothing remembered yet" description="Use Save to private memory on an authorized answer. Memory is stored by DataSlate and never attached to a request automatically." />}
      <ul className="memory-list">{items.map((item) => <li key={item.id}><div className="section-toolbar"><strong>{item.title ?? `Private memory · ${item.id}`}</strong><Pill tone={item.state === 'available' ? 'green' : 'amber'}>{item.state}</Pill></div><p>Revision {item.revision} · expires {expiryLabel(item.expiresAt)}</p>{item.purgePending && <p>Content cleanup pending.</p>}{item.state === 'restricted' && <p>Content and source names are hidden because current access is unavailable.</p>}<div className="memory-actions">{item.state === 'available' && <><button className="button secondary compact" disabled={busy || !!pending} onClick={() => void read(item)}>View memory</button><button className="button secondary compact" disabled={busy || !!pending || !!reference} onClick={() => void read(item, true)}>Correct or change expiry</button></>}<button className="button ghost compact" disabled={busy || !!pending || !!reference} onClick={() => { setError(''); setRemove(item); }}>Delete memory</button></div></li>)}</ul>
    </section>
    {error && !modalOpen && <ErrorNotice>{error}</ErrorNotice>}{notice && <div className="notice"><p>{notice}</p></div>}
    {pending && <div className="notice warning memory-recovery"><div><strong>Memory change needs recovery</strong><p>The exact request is saved temporarily in this tab. Check it before starting another memory change.</p><button className="text-button" onClick={() => { setEditor(undefined); setRemove(undefined); setError(''); setRecover(true); }}>Recover memory change</button><button className="text-button" onClick={() => setDiscard(true)}>Discard memory recovery</button></div></div>}
    {reference && <div className="notice warning memory-recovery"><div><strong>Memory change outcome is unresolved</strong><p>The old request contents were cleared after a privacy check. Only memory {reference.id}, original revision {reference.revision} and the request key remain. This cannot be replayed. A newer revision does not confirm which request created it.</p><code className="run-key">{reference.idempotencyKey}</code><button className="text-button" disabled={busy} onClick={() => void inspectReference()}>Inspect current memory</button><button className="text-button" disabled={busy} onClick={() => setDiscard(true)}>Discard memory reference</button></div></div>}
    {view && <Modal title="Private memory" onClose={() => setView(undefined)}><div className="modal-body"><h3>{view.title}</h3><pre className="sylon-prompt-preview">{view.content}</pre><MemoryProvenance memory={view} /><button className="button secondary" onClick={() => setView(undefined)}>Close</button></div></Modal>}
    {editor && <Modal title={editor.operation === 'create' ? 'Save private memory' : 'Correct private memory'} onClose={() => { if (!busy) setEditor(undefined); }}><div className="modal-body"><p>{editor.operation === 'create' ? 'Review the summary to retain. Saving makes no model request. The original answer and all of its source dependencies remain attached to this memory.' : 'A correction or expiry change creates a new revision and closes conversation history that used the previous revision.'}</p><label className="field">Memory title<input aria-label="Memory title" value={editor.fields.title} disabled={busy || !!pending} onChange={(e) => setEditor({ ...editor, fields: { ...editor.fields, title: e.target.value } })} /><small>{memoryBytes(editor.fields.title)} / 200 UTF-8 bytes</small></label><label className="field">Memory summary<textarea aria-label="Memory summary" rows={6} value={editor.fields.content} disabled={busy || !!pending} onChange={(e) => setEditor({ ...editor, fields: { ...editor.fields, content: e.target.value } })} /><small>{memoryBytes(editor.fields.content)} / 4,096 UTF-8 bytes</small></label><label className="field">Expires at · your local time<input type="datetime-local" aria-label="Memory expiry" value={editor.fields.expiresAt ? localDate(editor.fields.expiresAt) : ''} disabled={busy || !!pending} onChange={(e) => setEditor({ ...editor, fields: { ...editor.fields, expiresAt: utcDate(e.target.value) } })} /><small>{policy ? `Maximum ${policy.maxTTLDays} days from now. Expiry prevents recall and closes dependent history.` : 'This profile no longer permits memory.'}</small></label><p className="field-help">Private to you in this workspace. Source conversation deletion removes this memory. The exact summary is stored temporarily in this tab only while a save receipt is unresolved.</p>{error && <ErrorNotice>{error}</ErrorNotice>}<div className="form-actions"><button className="button secondary" disabled={busy} onClick={() => setEditor(undefined)}>Close</button><button className="button primary" disabled={busy || !!pending || !validMemoryFields(editor.fields, policy)} onClick={save}>Save reviewed memory</button></div></div></Modal>}
    {remove && <Modal title="Delete private memory?" onClose={() => { if (!busy) setRemove(undefined); }}><div className="modal-body"><p>This removes memory {remove.id} and prevents recall. Conversation history that used this memory becomes unavailable. This does not undo notebook edits or reverse provider processing.</p>{error && <ErrorNotice>{error}</ErrorNotice>}<div className="form-actions"><button className="button secondary" disabled={busy} onClick={() => setRemove(undefined)}>Keep memory</button><button className="button primary" disabled={busy || !!pending} onClick={() => void mutate({ operation: 'delete', id: remove.id, input: { idempotencyKey: crypto.randomUUID(), revision: remove.revision } })}>Delete reviewed memory</button></div></div></Modal>}
    {recover && pending && <Modal title="Recover memory change" onClose={() => { if (!busy) setRecover(false); }}><div className="modal-body"><p>Check the original {pending.operation} request using the same key. A confirmed earlier change will not be applied again.</p><code className="run-key">{pending.input.idempotencyKey}</code><p>{pending.operation === 'create' ? `Source job ${pending.input.source.jobId} · revision ${pending.input.source.revision}` : `Memory ${pending.id} · original revision ${pending.input.revision}`}</p>{error && <ErrorNotice>{error}</ErrorNotice>}<div className="form-actions"><button className="button secondary" disabled={busy} onClick={() => setRecover(false)}>Close</button><button className="button primary" disabled={busy} onClick={() => void mutate(pending, true)}>Check memory change</button></div></div></Modal>}
    {discard && <Modal title="Discard memory recovery?" onClose={() => setDiscard(false)}><div className="modal-body"><p>This removes the temporary browser request, not its server-side effects. Refresh the memory library before making a replacement change.</p><button className="button primary" onClick={() => { try { persist(); setDiscard(false); setRecover(false); setEditor(undefined); setRemove(undefined); setError(''); setRefresh((v) => v + 1); } catch { setError('Browser recovery could not be cleared.'); } }}>Discard pending memory request</button></div></Modal>}
  </>;
}

function MemoryProvenance({ memory }: { memory: PrivateMemory }) { return <dl className="details-list"><dt>Saved revision</dt><dd>{memory.revision}</dd><dt>Source answer</dt><dd>{memory.source.jobId} · revision {memory.source.revision}</dd><dt>Assistant policy</dt><dd>{memory.bindingId} · {memory.policyVersion}</dd><dt>Expires</dt><dd>{expiryLabel(memory.expiresAt)}</dd></dl>; }

interface RecallProps extends Identity { binding: SylonBinding; selected: MemoryRef[]; disabled: boolean; onChange: (refs: MemoryRef[]) => void }
export function SylonMemoryRecall({ binding, selected, disabled, onChange, workspaceId, issuer, subject }: RecallProps) {
  const [query, setQuery] = useState(''); const [items, setItems] = useState<PrivateMemory[]>([]); const [busy, setBusy] = useState(false); const [error, setError] = useState(''); const controller = useRef<AbortController | undefined>(undefined);
  useEffect(() => () => controller.current?.abort(), []);
  useEffect(() => {
    if (!items.length) return; const c = new AbortController(); let timer: ReturnType<typeof setTimeout> | undefined; let checking = false;
    function schedule() { timer = setTimeout(() => void check(), Math.max(1, Math.min(10000, ...items.map((m) => Date.parse(m.expiresAt) - Date.now())))); }
    async function check() {
      if (checking || c.signal.aborted) return; checking = true; clearTimeout(timer);
      try {
        if (items.some((m) => Date.parse(m.expiresAt) <= Date.now())) throw new Error('Recalled memory expired. Find current authorized memory before selecting it again.');
        const current = await Promise.all(items.map((m) => memoryAPI.get(workspaceId, m.id, bounded(c))));
        if (!current.every((m, i) => validMemory(m, workspaceId, issuer, subject) && m.id === items[i].id && m.revision === items[i].revision && m.bindingId === binding.id && !m.purgePending && Date.parse(m.expiresAt) > Date.now())) throw new Error('Recalled memory changed or is no longer available. Find current authorized memory again.');
      } catch (e) { if (!c.signal.aborted) { setItems([]); setError(message(e)); } }
      finally { checking = false; if (!c.signal.aborted) schedule(); }
    }
    const visible = () => { if (document.visibilityState === 'visible') void check(); }; schedule(); document.addEventListener('visibilitychange', visible);
    return () => { c.abort(); clearTimeout(timer); document.removeEventListener('visibilitychange', visible); };
  }, [items, workspaceId, issuer, subject, binding.id]);
  async function recall() { controller.current?.abort(); const c = new AbortController(); controller.current = c; setBusy(true); setError(''); setItems([]); try { const r = await memoryAPI.recall(workspaceId, binding.id, query, bounded(c)); if (r.workspaceId !== workspaceId || r.bindingId !== binding.id || r.query !== query || r.mode !== 'lexical' || r.complete !== true || !Array.isArray(r.items) || r.items.length > 4 || !r.items.every((i) => validMemory(i.memory, workspaceId, issuer, subject) && i.memory.bindingId === binding.id && Date.parse(i.memory.expiresAt) > Date.now() && !i.memory.purgePending && Number.isFinite(i.score) && i.score > 0) || new Set(r.items.map((i) => i.memory.id)).size !== r.items.length) throw new Error('Invalid authorized memory recall response.'); if (!c.signal.aborted) setItems(r.items.map((i) => i.memory)); } catch (e) { if (!c.signal.aborted) setError(message(e)); } finally { if (!c.signal.aborted) setBusy(false); } }
  return <div className="memory-recall"><h3>Recall private memory</h3><p className="field-help">Local lexical matching. No embedding or model request. Select up to four exact revisions; nothing is attached automatically.</p><label className="field">Find remembered context<input aria-label="Memory recall query" value={query} disabled={disabled || busy} onChange={(e) => setQuery(e.target.value)} /></label><button className="button secondary compact" disabled={disabled || busy || !query.trim() || memoryBytes(query) > 4096} onClick={() => void recall()}>{busy ? 'Finding memories…' : 'Find private memories'}</button>{error && <ErrorNotice>{error}</ErrorNotice>}<ul className="memory-list">{items.map((m) => <li key={m.id}><strong>{m.title}</strong><p className="memory-content">{m.content}</p><small>Revision {m.revision} · expires {expiryLabel(m.expiresAt)}</small><button className="text-button" disabled={disabled || selected.length >= 4 || selected.some((r) => r.id === m.id) || Date.parse(m.expiresAt) <= Date.now()} onClick={() => onChange([...selected, { id: m.id, revision: m.revision }])}>Select memory</button></li>)}</ul>{selected.map((m) => <div className="sylon-context-chip" key={m.id}><span>{m.id} · revision {m.revision}</span><button className="text-button" disabled={disabled} aria-label={`Remove memory ${m.id}`} onClick={() => onChange(selected.filter((r) => r.id !== m.id))}>Remove</button></div>)}</div>;
}

interface ReviewProps extends Identity { memories: MemoryRef[]; bindingId?: string; onReady: (ready: boolean) => void }
export function SylonMemoryReview({ memories, bindingId, onReady, workspaceId, issuer, subject }: ReviewProps) {
  const [items, setItems] = useState<PrivateMemory[]>([]); const [error, setError] = useState(''); const selection = JSON.stringify(memories);
  useEffect(() => {
    const c = new AbortController(); let timer: ReturnType<typeof setTimeout> | undefined; let expiryTimer: ReturnType<typeof setTimeout> | undefined; let checking = false;
    onReady(false); setItems([]); setError('');
    async function check() {
      if (checking || c.signal.aborted) return; checking = true; onReady(false);
      try {
        const values = await Promise.all(memories.map((m) => memoryAPI.get(workspaceId, m.id, bounded(c))));
        if (c.signal.aborted) return;
        if (!values.every((m, index) => validMemory(m, workspaceId, issuer, subject) && m.id === memories[index].id && m.revision === memories[index].revision && m.bindingId === bindingId && !m.purgePending && Date.parse(m.expiresAt) > Date.now())) throw new Error('Selected memory changed, expired or no longer belongs to this profile. Close review and select its current authorized revision.');
        setItems(values); setError(''); onReady(true); clearTimeout(expiryTimer);
        if (values.length) { const expire = () => { const delay = Math.min(...values.map((m) => Date.parse(m.expiresAt) - Date.now())); if (delay <= 0) { setItems([]); onReady(false); setError('Selected memory expired. Close review and remove it before sending.'); } else expiryTimer = setTimeout(expire, Math.min(delay, 2147483647)); }; expire(); }
      } catch (e) { if (!c.signal.aborted) { setItems([]); setError(message(e)); onReady(false); } }
      finally { checking = false; if (!c.signal.aborted) timer = setTimeout(() => void check(), 10000); }
    }
    const visible = () => { if (document.visibilityState === 'visible') void check(); }; void check(); document.addEventListener('visibilitychange', visible);
    return () => { c.abort(); clearTimeout(timer); clearTimeout(expiryTimer); document.removeEventListener('visibilitychange', visible); onReady(false); };
  }, [workspaceId, issuer, subject, selection, bindingId, onReady]);
  return <section aria-label="Reviewed private memories"><h3>Private memory · {memories.length}</h3>{error && <ErrorNotice>{error}</ErrorNotice>}{!error && !items.length && memories.length > 0 && <Spinner label="Checking memory revisions and access…" />}{items.map((m) => <div className="memory-review" key={m.id}><strong>{m.title}</strong><pre className="sylon-prompt-preview">{m.content}</pre><MemoryProvenance memory={m} /></div>)}</section>;
}
