import { useCallback, useEffect, useState } from 'react';
import CodeMirror from '@uiw/react-codemirror';
import { python } from '@codemirror/lang-python';
import { sql } from '@codemirror/lang-sql';
import { BarChart3, BookOpen, Check, ChevronRight, Clock3, Code2, Download, FileCode2, FileText, ListTree, Play, Plus, Save, Search, Table2, Terminal, Trash2 } from 'lucide-react';
import { api, ApiError, message } from './api';
import type { Cell, Notebook, Workspace } from './api';
import { download, EmptyState, ErrorNotice, Pill, Spinner, SylonMark } from './ui';

type Draft = Pick<Notebook, 'name' | 'kind' | 'cells'> & { id?: string; revision?: number; updatedAt?: string };
const serialize = (draft: Draft | undefined) => draft ? JSON.stringify({ name: draft.name, kind: draft.kind, cells: draft.cells }) : '';

export function Editor({ workspace, onDirty, openCompute, openSylon }: { workspace: Workspace; onDirty: (dirty: boolean) => void; openCompute: () => void; openSylon: () => void }) {
  const [notebooks, setNotebooks] = useState<Notebook[]>([]);
  const [draft, setDraft] = useState<Draft>();
  const [baseline, setBaseline] = useState('');
  const [filter, setFilter] = useState('');
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [opening, setOpening] = useState(false);
  const [error, setError] = useState('');
  const [conflict, setConflict] = useState(false);
  const [listError, setListError] = useState('');
  const dirty = Boolean(draft && serialize(draft) !== baseline);

  const refresh = useCallback(async () => {
    setLoading(true); setListError('');
    try { setNotebooks((await api.notebooks(workspace.id)).items); }
    catch (err) { setListError(message(err)); }
    finally { setLoading(false); }
  }, [workspace.id]);
  useEffect(() => { void refresh(); }, [refresh]);
  useEffect(() => { onDirty(dirty); }, [dirty, onDirty]);
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
    <section className="editor-workspace" aria-label="Document editor">{!draft ? <div className="editor-welcome"><div className="welcome-top"><span className="eyebrow">DEVELOP / {workspace.name}</span><Pill tone="neutral">Start with a notebook</Pill></div><div className="welcome-content"><span className="welcome-icon"><Code2 size={34} strokeWidth={1.4} /></span><h1>Great data work<br />starts on a clean slate.</h1><p>Bring your questions, code and ideas together.<br />Create a notebook or a SQL query to get started.</p><div className="welcome-actions"><button className="button primary" onClick={() => create('notebook')}><Plus size={16} />Create notebook</button><button className="button secondary" onClick={() => create('sql')}><Terminal size={16} />Write SQL</button></div><div className="welcome-capabilities"><div><BookOpen size={18} /><strong>One place to develop</strong><span>Python and SQL cells, saved with revision checks.</span></div><div><BarChart3 size={18} /><strong>A shared results experience</strong><span>Table, visualization and execution details.</span></div><div><SylonMark size={18} /><strong>Sylon, alongside your work</strong><span>AI assistance when your provider is connected.</span></div></div></div><div className="foundation-footnote"><span className="status-dot warning" />Authoring is available. Compute execution and AI integration are under development.</div>{error && <ErrorNotice>{error}</ErrorNotice>}</div> : <>
      <div className="document-breadcrumb"><span>Workspace</span><ChevronRight size={13} /><span>{draft.kind === 'sql' ? 'SQL query' : 'Notebook'}</span>{opening && <Spinner label="Opening…" />}</div>
      <div className="editor-titlebar"><div className="editor-title"><span className="document-type-icon">{draft.kind === 'sql' ? <FileCode2 size={22} /> : <BookOpen size={22} />}</span><div><input aria-label="Document name" maxLength={120} value={draft.name} onChange={(e) => setDraft({ ...draft, name: e.target.value })} disabled={saving || opening} /><div className="save-status" aria-live="polite">{saving ? <Spinner label="Saving…" /> : dirty ? <><span className="unsaved-dot" />Unsaved changes</> : <><Check size={13} />Saved · revision {draft.revision}</>}</div></div></div><div className="editor-actions"><button className="button secondary" onClick={() => void save()} disabled={!dirty || saving || opening || !draft.name.trim()}><Save size={15} />Save</button><button className="icon-button" aria-label="Download document" onClick={() => download(`${draft.name || 'notebook'}.json`, JSON.stringify({ name: draft.name, kind: draft.kind, cells: draft.cells }, null, 2))}><Download size={17} /></button></div></div>
      <div className="editor-toolbar"><button className="compute-selector" onClick={openCompute}><span className="status-dot warning" />Compute not connected<ChevronRight size={14} /></button><div><button className="button compact secondary" disabled title="Query execution is not implemented in this build"><Play size={13} />Run all</button><button className="button compact ghost" onClick={openSylon}><SylonMark size={17} />Sylon</button></div></div>
      {error && <div className="editor-notice"><ErrorNotice>{error}{conflict && <div className="conflict-actions"><p>A newer revision exists. Download your edits before reloading; DataSlate will not overwrite the saved version.</p><button className="button secondary compact" onClick={() => download(`${draft.name}-unsaved.json`, JSON.stringify(draft, null, 2))}>Download my edits</button><button className="button secondary compact" onClick={() => { if (draft.id && window.confirm('Reload the latest saved version? Your unsaved edits will be discarded.')) void open(draft.id, true); }}>Reload saved revision</button></div>}</ErrorNotice></div>}
      <div className="cells-scroll">{draft.cells.map((cell, index) => <article className="notebook-cell" key={cell.id} aria-label={`Cell ${index + 1}`}><div className="cell-heading"><div><span className="cell-number">{String(index + 1).padStart(2, '0')}</span><select aria-label={`Cell ${index + 1} language`} value={cell.language} onChange={(e) => editCell(cell.id, { language: e.target.value as Cell['language'] })} disabled={saving}><option value="python">Python</option><option value="sql">SQL</option></select></div><div><span className="cell-hint">Not executed</span><button className="icon-button" aria-label={`Remove cell ${index + 1}`} disabled={draft.cells.length < 2 || saving} onClick={() => setDraft({ ...draft, cells: draft.cells.filter((item) => item.id !== cell.id) })}><Trash2 size={14} /></button></div></div><CodeMirror aria-label={`Cell ${index + 1} code`} value={cell.source} extensions={[cell.language === 'sql' ? sql() : python()]} onChange={(source) => editCell(cell.id, { source })} minHeight="142px" editable={!saving && !opening} basicSetup={{ lineNumbers: true, foldGutter: true, highlightActiveLine: true, autocompletion: true }} /><div className="cell-footer"><Play size={11} /><span>Save and edit code. Execution is not yet connected.</span></div></article>)}<div className="add-cell-row"><span /><button className="button secondary compact" onClick={() => setDraft({ ...draft, cells: [...draft.cells, { id: crypto.randomUUID(), language: draft.kind === 'sql' ? 'sql' : 'python', source: '' }] })} disabled={saving}><Plus size={13} />Add cell</button><span /></div><ResultPanel /></div>
      <div className="editor-statusbar"><span><Code2 size={13} />{draft.cells.length} {draft.cells.length === 1 ? 'cell' : 'cells'}</span><span>{draft.id ? 'Saved to workspace API' : 'New document'}<span className="status-divider">/</span>No execution outputs stored</span></div>
    </>}</section>
  </div>;
}

function ShieldNote() { return <ListTree size={13} />; }
function ResultPanel() {
  const [tab, setTab] = useState('table');
  const tabs = [{ id: 'table', label: 'Table', icon: Table2 }, { id: 'visualize', label: 'Visualize', icon: BarChart3 }, { id: 'profile', label: 'Profile', icon: ListTree }, { id: 'execution', label: 'Execution', icon: Clock3 }];
  const copy: Record<string, { title: string; description: string }> = {
    table: { title: 'Your results will appear here', description: 'Connect a qualified execution service to run code and return authorized, paginated results. No query has been executed.' },
    visualize: { title: 'From results to a clearer picture', description: 'The chart builder requires the Result Service integration. Charts will use governed backend aggregations; any preview sample must be labeled.' },
    profile: { title: 'Understand the shape of your data', description: 'Schema, distributions and null counts require a real result set. Profiling is not available in this build.' },
    execution: { title: 'Every run, traceable', description: 'Status, attempts, logs and query plans will appear when the execution adapter is connected. No run has been submitted.' },
  };
  return <section className="result-panel" aria-label="Results"><div className="result-tabs" role="tablist" aria-label="Result views">{tabs.map(({ id, label, icon: Icon }) => <button id={`result-tab-${id}`} aria-controls={`result-content-${id}`} role="tab" aria-selected={tab === id} key={id} onClick={() => setTab(id)}><Icon size={14} />{label}</button>)}<span className="result-count">No result set</span></div><div role="tabpanel" id={`result-content-${tab}`} aria-labelledby={`result-tab-${tab}`}><EmptyState icon={tab === 'visualize' ? <BarChart3 size={22} /> : <FileText size={22} />} title={copy[tab].title} description={copy[tab].description} /></div></section>;
}
