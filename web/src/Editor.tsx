import { useCallback, useEffect, useRef, useState } from 'react';
import CodeMirror from '@uiw/react-codemirror';
import { python } from '@codemirror/lang-python';
import { sql } from '@codemirror/lang-sql';
import { BarChart3, BookOpen, Check, ChevronRight, Code2, Download, FileCode2, ListTree, Play, Plus, Save, Search, Terminal, Trash2 } from 'lucide-react';
import { api, ApiError, message } from './api';
import type { Cell, Notebook, Workspace } from './api';
import { NotebookBatch } from './NotebookBatch';
import type { SylonApplyReceipt, SylonSeed } from './sylon-api';
import { SQLQuery } from './SQLQuery';
import { download, ErrorNotice, Pill, Spinner, SylonMark } from './ui';

type Draft = Pick<Notebook, 'name' | 'kind' | 'cells'> & { id?: string; revision?: number; updatedAt?: string };
const serialize = (draft: Draft | undefined) => draft ? JSON.stringify({ name: draft.name, kind: draft.kind, cells: draft.cells }) : '';

export function Editor({ workspace, initialNotebookId, onDirty, openCompute, openRun, openSylon, appliedReceipt }: { workspace: Workspace; initialNotebookId?: string; onDirty: (dirty: boolean) => void; openCompute: () => void; openRun: (id: string) => void; openSylon: (seed?: SylonSeed) => void; appliedReceipt?: SylonApplyReceipt }) {
  const [notebooks, setNotebooks] = useState<Notebook[]>([]);
  const [draft, setDraft] = useState<Draft>();
  const [baseline, setBaseline] = useState('');
  const [filter, setFilter] = useState('');
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [opening, setOpening] = useState(false);
  const [error, setError] = useState('');
  const [conflict, setConflict] = useState(false);
  const [batchUnresolved, setBatchUnresolved] = useState(false);
  const [queryUnresolved, setQueryUnresolved] = useState(false);
  const [listError, setListError] = useState('');
  const dirty = Boolean(draft && serialize(draft) !== baseline);
  const draftRef = useRef(draft); draftRef.current = draft;
  useEffect(() => {
    if (!appliedReceipt || appliedReceipt.notebookId !== draftRef.current?.id) return;
    if (dirty) { setError('Sylon saved a newer notebook revision. Your local edits were preserved; download them before reloading.'); setConflict(true); return; }
    const c = new AbortController(); const prior = serialize(draftRef.current);
    api.notebook(workspace.id, appliedReceipt.notebookId, c.signal).then((next) => {
      if (c.signal.aborted || draftRef.current?.id !== appliedReceipt.notebookId) return;
      if (serialize(draftRef.current) !== prior) { setError('Sylon saved a newer revision while you edited. Your local edits were preserved.'); setConflict(true); return; }
      if (next.id !== appliedReceipt.notebookId || next.workspaceId !== workspace.id || next.revision < appliedReceipt.revision) throw new Error('The saved assistant revision could not be verified. Reload the document before running it.');
      setDraft(next); setBaseline(serialize(next)); setNotebooks((items) => [next, ...items.filter((n) => n.id !== next.id)]);
    }).catch((e) => { if (!c.signal.aborted) setError(message(e)); });
    return () => c.abort();
  }, [appliedReceipt, workspace.id]);

  const refresh = useCallback(async () => {
    setLoading(true); setListError('');
    try { setNotebooks((await api.notebooks(workspace.id)).items); }
    catch (err) { setListError(message(err)); }
    finally { setLoading(false); }
  }, [workspace.id]);
  useEffect(() => { void refresh(); }, [refresh]);
  useEffect(() => {
    if (!initialNotebookId) return;
    let active = true; setOpening(true); setError('');
    api.notebook(workspace.id, initialNotebookId).then((notebook) => {
      if (!active) return;
      if (notebook.id !== initialNotebookId || notebook.workspaceId !== workspace.id) throw new Error('The saved document did not match this workspace.');
      setDraft(notebook); setBaseline(serialize(notebook));
    }).catch((err) => { if (active) setError(message(err)); }).finally(() => { if (active) setOpening(false); });
    return () => { active = false; };
  }, [workspace.id, initialNotebookId]);
  useEffect(() => { onDirty(dirty || batchUnresolved || queryUnresolved); }, [dirty, batchUnresolved, queryUnresolved, onDirty]);
  useEffect(() => () => onDirty(false), [onDirty]);

  function confirmDiscard() { return !dirty || window.confirm('Discard your unsaved edits and open another document?'); }
  async function open(id: string, discard = false) {
    if (!discard && !confirmDiscard()) return;
    setOpening(true); setError(''); setConflict(false);
    try { const notebook = await api.notebook(workspace.id, id); setDraft(notebook); setBaseline(serialize(notebook)); }
    catch (err) { setError(message(err)); }
    finally { setOpening(false); }
  }
  function create(kind: 'notebook' | 'sql') {
    if (opening || saving) return;
    if (!confirmDiscard()) return;
    setError(''); setConflict(false); setBaseline('');
    setDraft({ name: kind === 'sql' ? 'Untitled query' : 'Untitled notebook', kind, cells: [{ id: crypto.randomUUID(), language: kind === 'sql' ? 'sql' : 'python', source: kind === 'sql' ? '-- Write your SQL here.\n' : '# Start your analysis here.\n' }] });
  }
  const save = useCallback(async () => {
    if (!draft || saving || !draft.name.trim()) return;
    setSaving(true); setError(''); setConflict(false);
    try {
      const saved = await api.saveNotebook(workspace.id, { name: draft.name.trim(), kind: draft.kind, cells: draft.cells }, draft.id, draft.revision);
      setDraft(saved); setBaseline(serialize(saved));
      setNotebooks((items) => [saved, ...items.filter((item) => item.id !== saved.id)]);
    } catch (err) { setError(message(err)); setConflict(err instanceof ApiError && err.status === 409); }
    finally { setSaving(false); }
  }, [draft, saving, workspace.id]);
  useEffect(() => {
    const keydown = (event: KeyboardEvent) => { if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 's') { event.preventDefault(); if (dirty) void save(); } };
    window.addEventListener('keydown', keydown); return () => window.removeEventListener('keydown', keydown);
  }, [dirty, save]);

  function editCell(id: string, updates: Partial<Cell>) { setDraft((previous) => previous && ({ ...previous, cells: previous.cells.map((cell) => cell.id === id ? { ...cell, ...updates } : cell) })); }
  const filtered = notebooks.filter((notebook) => notebook.name.toLowerCase().includes(filter.toLowerCase()));

  return <div className="develop-layout"><aside className="document-sidebar"><div className="document-heading"><strong>Workspace</strong><span className="document-count">{notebooks.length}</span></div><div className="document-actions"><button className="button secondary small-button" onClick={() => create('notebook')}><Plus size={14} />Notebook</button><button className="button secondary small-button" onClick={() => create('sql')}><Plus size={14} />SQL</button></div><label className="search-field"><Search size={14} /><input aria-label="Search documents" value={filter} onChange={(e) => setFilter(e.target.value)} placeholder="Find a document" /></label><div className="document-list">{loading ? <Spinner label="Loading documents…" /> : listError ? <ErrorNotice retry={() => void refresh()}>{listError}</ErrorNotice> : filtered.length ? filtered.map((notebook) => <button key={notebook.id} className={`document-item ${draft?.id === notebook.id ? 'selected' : ''}`} onClick={() => void open(notebook.id)} disabled={opening || saving}>{notebook.kind === 'sql' ? <FileCode2 size={17} /> : <BookOpen size={17} />}<span><strong>{notebook.name}</strong><small>{notebook.kind === 'sql' ? 'SQL query' : 'Notebook'} · revision {notebook.revision}</small></span></button>) : <div className="document-empty">{filter ? 'No matching documents.' : 'Your saved notebooks and queries will appear here.'}</div>}</div><div className="document-foot"><ShieldNote />Workspace-scoped access</div></aside>
    <section className="editor-workspace" aria-label="Document editor">{!draft ? <div className="editor-welcome"><div className="welcome-top"><span className="eyebrow">DEVELOP / {workspace.name}</span><Pill tone="neutral">Start with a notebook</Pill></div><div className="welcome-content"><span className="welcome-icon"><Code2 size={34} strokeWidth={1.4} /></span><h1>Great data work<br />starts on a clean slate.</h1><p>Bring your questions, code and ideas together.<br />Create a notebook or a SQL query to get started.</p><div className="welcome-actions"><button className="button primary" onClick={() => create('notebook')}><Plus size={16} />Create notebook</button><button className="button secondary" onClick={() => create('sql')}><Terminal size={16} />Write SQL</button></div><div className="welcome-capabilities"><div><BookOpen size={18} /><strong>One place to develop</strong><span>Python and SQL cells, saved with revision checks.</span></div><div><BarChart3 size={18} /><strong>A shared results experience</strong><span>Table, visualization and execution details.</span></div><div><SylonMark size={18} /><strong>Sylon, alongside your work</strong><span>AI assistance when your provider is connected.</span></div></div></div><div className="foundation-footnote"><span className="status-dot warning" />Authoring is available. Python batch execution requires an approved engineering binding; interactive sessions need additional integration. Sylon is available when an approved assistant profile is configured.</div>{error && <ErrorNotice>{error}</ErrorNotice>}</div> : <>
      <div className="document-breadcrumb"><span>Workspace</span><ChevronRight size={13} /><span>{draft.kind === 'sql' ? 'SQL query' : 'Notebook'}</span>{opening && <Spinner label="Opening…" />}</div>
      <div className="editor-titlebar"><div className="editor-title"><span className="document-type-icon">{draft.kind === 'sql' ? <FileCode2 size={22} /> : <BookOpen size={22} />}</span><div><input aria-label="Document name" maxLength={120} value={draft.name} onChange={(e) => setDraft({ ...draft, name: e.target.value })} disabled={saving || opening} /><div className="save-status" aria-live="polite">{saving ? <Spinner label="Saving…" /> : dirty ? <><span className="unsaved-dot" />Unsaved changes</> : <><Check size={13} />Saved · revision {draft.revision}</>}</div></div></div><div className="editor-actions"><button className="button secondary" onClick={() => void save()} disabled={!dirty || saving || opening || !draft.name.trim()}><Save size={15} />Save</button><button className="icon-button" aria-label="Download document" onClick={() => download(`${draft.name || 'notebook'}.json`, JSON.stringify({ name: draft.name, kind: draft.kind, cells: draft.cells }, null, 2))}><Download size={17} /></button></div></div>
      <div className="editor-toolbar"><button className="compute-selector" onClick={openCompute}><span className="status-dot warning" />Spark application runs<ChevronRight size={14} /></button><div><button className="button compact secondary" disabled title="Interactive cell execution is unavailable. Saved Python notebooks can use an approved batch binding."><Play size={13} />Run all</button><button className="button compact ghost" onClick={() => openSylon()}><SylonMark size={17} />Sylon</button></div></div>
      {draft.kind === 'notebook' && <NotebookBatch workspaceId={workspace.id} document={draft} blockedReason={conflict ? 'Resolve the newer saved revision before submitting a batch.' : saving || opening ? 'Wait for the document operation to finish.' : dirty ? 'Save your changes before submitting a new batch.' : ''} onUnresolved={setBatchUnresolved} onViewRun={openRun} onRevisionConflict={() => { setConflict(true); setError('The saved notebook changed before this batch was accepted. Reload the saved revision and review it before submitting again.'); }} />}
      {error && <div className="editor-notice"><ErrorNotice>{error}{conflict && <div className="conflict-actions"><p>A newer revision exists. Download your edits before reloading; DataSlate will not overwrite the saved version.</p><button className="button secondary compact" onClick={() => download(`${draft.name}-unsaved.json`, JSON.stringify(draft, null, 2))}>Download my edits</button><button className="button secondary compact" onClick={() => { if (draft.id && window.confirm('Reload the latest saved version? Your unsaved edits will be discarded.')) void open(draft.id, true); }}>Reload saved revision</button></div>}</ErrorNotice></div>}
      <div className="cells-scroll">{draft.cells.map((cell, index) => <article className="notebook-cell" key={cell.id} aria-label={`Cell ${index + 1}`}><div className="cell-heading"><div><span className="cell-number">{String(index + 1).padStart(2, '0')}</span><select aria-label={`Cell ${index + 1} language`} value={cell.language} onChange={(e) => editCell(cell.id, { language: e.target.value as Cell['language'] })} disabled={saving}><option value="python">Python</option><option value="sql">SQL</option></select></div><div><span className="cell-hint">Cell execution not tracked</span><button className="icon-button" aria-label={`Remove cell ${index + 1}`} disabled={draft.cells.length < 2 || saving} onClick={() => setDraft({ ...draft, cells: draft.cells.filter((item) => item.id !== cell.id) })}><Trash2 size={14} /></button></div></div><CodeMirror aria-label={`Cell ${index + 1} code`} value={cell.source} extensions={[cell.language === 'sql' ? sql() : python()]} onChange={(source) => editCell(cell.id, { source })} minHeight="142px" editable={!saving && !opening} basicSetup={{ lineNumbers: true, foldGutter: true, highlightActiveLine: true, autocompletion: true }} /><div className="cell-assistant-action"><button className="button ghost compact" disabled={dirty || saving || opening || conflict || !draft.id} aria-label={`Ask Sylon about cell ${index + 1}`} title={dirty ? 'Save this document before selecting assistant context.' : 'Review an assistant request with this exact saved cell.'} onClick={() => { if (draft.id && draft.revision) openSylon({ notebook: { id: draft.id, revision: draft.revision, cellId: cell.id }, prompt: 'Explain this cell and suggest improvements.', nonce: crypto.randomUUID() }); }}><SylonMark size={14} />Ask Sylon</button></div><div className="cell-footer"><Play size={11} /><span>{cell.language === 'sql' ? 'Run this saved SQL cell below. Results remain private to your identity.' : 'Cell outputs are unavailable. Batch execution runs a saved notebook revision.'}</span></div></article>)}<div className="add-cell-row"><span /><button className="button secondary compact" onClick={() => setDraft({ ...draft, cells: [...draft.cells, { id: crypto.randomUUID(), language: draft.kind === 'sql' ? 'sql' : 'python', source: '' }] })} disabled={saving}><Plus size={13} />Add cell</button><span /></div><SQLQuery workspaceId={workspace.id} document={draft} blockedReason={conflict ? 'Resolve the newer saved revision before submitting SQL.' : saving || opening ? 'Wait for the document operation to finish.' : dirty ? 'Save your changes before submitting a new query.' : ''} onUnresolved={setQueryUnresolved} onRevisionConflict={() => { setConflict(true); setError('The saved SQL cell changed before this query was accepted. Reload and review the saved revision before submitting again.'); }} /></div>
      <div className="editor-statusbar"><span><Code2 size={13} />{draft.cells.length} {draft.cells.length === 1 ? 'cell' : 'cells'}</span><span>{draft.id ? 'Saved to workspace API' : 'New document'}<span className="status-divider">/</span>Results stay separate from this document</span></div>
    </>}</section>
  </div>;
}

function ShieldNote() { return <ListTree size={13} />; }
