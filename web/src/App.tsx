import { lazy, Suspense, useEffect, useState } from 'react';
import { ArrowRight, Blocks, BookOpen, ChevronDown, CircleHelp, Cloud, Code2, Database, GitBranch, LogOut, Plus, Settings2, ShieldCheck, Sparkles, Terminal, X } from 'lucide-react';
import { api, message } from './api';
import type { Module, PlatformStatus, Workspace } from './api';
import { useAuth } from './auth';
import { ModuleExplorer, PlanBuilder, SparkPreview } from './Platform';
import { EmptyState, ErrorNotice, Logo, Modal, Pill, Spinner, SylonMark } from './ui';

const Editor = lazy(() => import('./Editor').then((module) => ({ default: module.Editor })));

type View = 'develop' | 'data' | 'pipelines' | 'compute' | 'ai' | 'administration';
const navigation = [
  { id: 'develop' as const, label: 'Develop', icon: Code2 },
  { id: 'data' as const, label: 'Data', icon: Database },
  { id: 'pipelines' as const, label: 'Pipelines', icon: GitBranch },
  { id: 'compute' as const, label: 'Compute', icon: Blocks },
  { id: 'ai' as const, label: 'AI & models', icon: Sparkles },
  { id: 'administration' as const, label: 'Administration', icon: Settings2 },
];
const collections: Record<string, string[]> = {
  data: ['unity-catalog', 'delta', 'iceberg', 'ingestion', 'federation', 'search', 'sharing', 'lineage', 'dashboards'],
  pipelines: ['airflow', 'pipelines', 'ingestion', 'maintenance', 'kafka', 'cdc', 'lineage'],
  ai: ['sylon', 'agents', 'memory', 'headroom', 'mlflow', 'model-serving', 'feature-store', 'search', 'apps'],
};

export function App() {
  const auth = useAuth();
  // On session expiry, keep the editor accessible so drafts can be exported before redirecting.
  return <>
    {(auth.authenticated || auth.user) && <WorkspaceApp key={auth.user?.profile.sub ?? 'development'} active={auth.authenticated} />}
    {(!auth.ready || (!auth.authenticated && !auth.user)) && <div className="auth-screen"><div className="auth-card"><Logo /><div className="auth-illustration"><Database size={36} /><span /><Sparkles size={27} /></div><span className="eyebrow">YOUR LAKE. YOUR WORKSPACE.</span><h1>Build something<br />with your data.</h1><p>A connected home for data engineering, analytics and AI. Running on your Kubernetes infrastructure.</p>
      {!auth.ready ? <Spinner label="Connecting to DataSlate…" /> : <>{auth.error && <ErrorNotice>{auth.error}</ErrorNotice>}{auth.config?.mode === 'oidc' ? <button className="button primary full-width" onClick={() => void auth.login()}>Continue with your organization <ArrowRight size={17} /></button> : <button className="button primary full-width" onClick={auth.retry}>Retry connection</button>}</>}
      <div className="auth-foot"><ShieldCheck size={14} />Your organization controls access to this workspace.</div>
    </div><span className="auth-caption">DataSlate / Kubernetes lakehouse platform</span></div>}
  </>;
}

function WorkspaceApp({ active }: { active: boolean }) {
  const auth = useAuth();
  const [status, setStatus] = useState<PlatformStatus>();
  const [workspaces, setWorkspaces] = useState<Workspace[]>([]);
  const [workspaceId, setWorkspaceId] = useState('');
  const [modules, setModules] = useState<Module[]>([]);
  const [view, setView] = useState<View>('develop');
  const [booting, setBooting] = useState(true);
  const [error, setError] = useState('');
  const [bootAttempt, setBootAttempt] = useState(0);
  const [dirty, setDirty] = useState(false);
  const [createWorkspace, setCreateWorkspace] = useState(false);
  const [sylon, setSylon] = useState(false);
  const [about, setAbout] = useState(false);
  const [adminTab, setAdminTab] = useState<'modules' | 'plan'>('modules');
  const workspace = workspaces.find((w) => w.id === workspaceId);
  const platformAdmin = auth.config?.mode === 'development' || (Array.isArray(auth.user?.profile.dataslate_roles) && auth.user.profile.dataslate_roles.includes('platform-admin'));

  useEffect(() => {
    if (!active) return;
    let current = true;
    setBooting(true); setError('');
    Promise.all([api.status(), api.workspaces(), api.modules()]).then(([nextStatus, nextWorkspaces, nextModules]) => {
      if (!current) return;
      setStatus(nextStatus); setWorkspaces(nextWorkspaces.items); setModules(nextModules.items);
      setWorkspaceId((previous) => nextWorkspaces.items.some((w) => w.id === previous) ? previous : nextWorkspaces.items[0]?.id ?? '');
    }).catch((err) => { if (current) setError(message(err)); }).finally(() => { if (current) setBooting(false); });
    return () => { current = false; };
  }, [active, bootAttempt]);

  useEffect(() => {
    const unload = (event: BeforeUnloadEvent) => { if (dirty) { event.preventDefault(); event.returnValue = ''; } };
    window.addEventListener('beforeunload', unload);
    return () => window.removeEventListener('beforeunload', unload);
  }, [dirty]);

  function leaveEditor(action: () => void) { if (!dirty || window.confirm('Leave this notebook and discard unsaved edits?')) { setDirty(false); action(); } }
  function go(next: View) { if (next !== view) leaveEditor(() => setView(next)); }
  if (!active && !status) return null;

  return <div className="app-shell"><a href="#main-content" className="skip-link">Skip to workspace</a>
    <aside className="sidebar"><div className="brand-wrap"><Logo /></div><div className="sidebar-group-label">WORKSPACE</div><nav aria-label="Main navigation">{navigation.map(({ id, label, icon: Icon }) => <button key={id} className={`nav-item ${view === id ? 'active' : ''}`} onClick={() => go(id)} aria-current={view === id ? 'page' : undefined}><Icon size={18} /><span>{label}</span>{id === 'ai' && <span className="nav-dot" />}</button>)}</nav>
      <div className="sidebar-bottom"><button className={`sylon-launch ${sylon ? 'selected' : ''}`} onClick={() => setSylon(!sylon)}><span className="sylon-symbol"><SylonMark size={23} /></span><span><strong>Sylon</strong><small>Your AI companion</small></span><ArrowRight size={15} /></button><button className="nav-item subdued" onClick={() => setAbout(true)}><CircleHelp size={17} /><span>About this build</span></button><div className="installation"><span className={`status-dot ${error ? 'warning' : ''}`} /><span>{error ? 'API unavailable' : status ? 'Connected to API' : 'Connecting…'}</span><span className="mono">K8s</span></div></div>
    </aside>
    <div className="workspace-shell"><header className="topbar"><div className="workspace-switch"><Cloud size={17} /><select aria-label="Workspace" value={workspaceId} onChange={(e) => { const id = e.target.value; leaveEditor(() => setWorkspaceId(id)); }} disabled={!workspaces.length}>{!workspaces.length && <option value="">No workspace</option>}{workspaces.map((w) => <option key={w.id} value={w.id}>{w.name}</option>)}</select><ChevronDown size={13} />{platformAdmin && <button className="icon-button" aria-label="Create workspace" onClick={() => setCreateWorkspace(true)}><Plus size={16} /></button>}</div><div className="topbar-right"><Pill>Foundation build</Pill><span className="topbar-divider" /><span className="avatar" title={auth.user?.profile.name ?? 'Development administrator'}>{String(auth.user?.profile.name ?? 'Dev').slice(0, 2).toUpperCase()}</span>{auth.config?.mode !== 'development' && <button className="icon-button" aria-label="Sign out of DataSlate" onClick={() => leaveEditor(() => void auth.logout())}><LogOut size={16} /></button>}</div></header>
      {auth.config?.mode === 'development' && <div className="development-banner"><ShieldCheck size={14} /><span>Development identity · loopback API only{status?.persistence === 'memory' ? ' · In-memory storage resets when the API restarts' : ''}</span></div>}
      {!active && auth.user && <div className="session-banner" role="alert"><span>Your session has expired. Download any unsaved documents before signing in again.</span><button className="button secondary compact" onClick={() => void auth.login()}>Sign in again</button></div>}
      <main id="main-content" className={`main-content ${view === 'develop' ? 'editor-main' : ''}`}>
        {error ? <div className="page-wrap"><ErrorNotice retry={() => setBootAttempt((n) => n + 1)}>{error}</ErrorNotice></div> : booting && !status ? <div className="page-loading"><Spinner label="Opening your workspace…" /></div> : !workspace && view === 'develop' ? <EmptyState icon={<BookOpen size={28} />} eyebrow="A FRESH START" title="Create your first workspace" description="A workspace keeps your team's notebooks, SQL and data projects together. Membership is checked by the API for every request." action={platformAdmin ? <button className="button primary" onClick={() => setCreateWorkspace(true)}><Plus size={16} />Create workspace</button> : <p className="muted">Ask your administrator to grant you workspace membership.</p>} /> : <>
          {view === 'develop' && workspace && <Suspense fallback={<div className="page-loading"><Spinner label="Loading editor…" /></div>}><Editor key={workspace.id} workspace={workspace} onDirty={setDirty} openCompute={() => go('compute')} openSylon={() => setSylon(true)} /></Suspense>}
          {view === 'data' && <ModuleExplorer eyebrow="DATA" title="Your lake, connected." description="Discover, govern and share data where it already lives. These integration definitions describe the platform's scope; they do not indicate installed services." modules={modules.filter((m) => collections.data.includes(m.id))} plan={() => { setAdminTab('plan'); go('administration'); }} />}
          {view === 'pipelines' && <ModuleExplorer eyebrow="PIPELINES" title="From source to trusted data." description="Batch ingestion, streaming, orchestration and maintenance belong in one execution experience. Backend execution adapters are still being implemented." modules={modules.filter((m) => collections.pipelines.includes(m.id))} plan={() => { setAdminTab('plan'); go('administration'); }} />}
          {view === 'compute' && <SparkPreview workspace={workspace} />}
          {view === 'ai' && <ModuleExplorer eyebrow="SYLON & AI" title="Intelligence with context." description="Sylon, agents, experiments and model serving share governed data access. No models or agent runtimes are connected in this foundation build." modules={modules.filter((m) => collections.ai.includes(m.id))} plan={() => { setAdminTab('plan'); go('administration'); }} />}
          {view === 'administration' && <div className="admin-page"><div className="page-tabs" role="tablist" aria-label="Administration"><button role="tab" aria-selected={adminTab === 'modules'} onClick={() => setAdminTab('modules')}>Platform modules</button><button role="tab" aria-selected={adminTab === 'plan'} onClick={() => setAdminTab('plan')}>Deployment planner</button></div>{adminTab === 'modules' ? <ModuleExplorer eyebrow="ADMINISTRATION" title="One platform. Your infrastructure." description="Keep existing services where integration is supported, or declare DataSlate-managed components. Every backend must pass compatibility and security qualification before it can run here." modules={modules} plan={() => setAdminTab('plan')} /> : <PlanBuilder modules={modules} />}</div>}
        </>}
      </main>
    </div>
    {sylon && <aside className="sylon-panel" aria-label="Sylon assistant"><div className="sylon-heading"><span className="sylon-symbol"><SylonMark size={25} /></span><div><strong>Sylon</strong><small>Workspace assistant</small></div><button className="icon-button" aria-label="Close Sylon" onClick={() => setSylon(false)}><X size={18} /></button></div><div className="sylon-content"><Pill tone="amber">Provider not connected</Pill><h2>A little context.<br />A lot of possibility.</h2><p>Sylon will help explore your data, write code and investigate errors within your permissions.</p><div className="assistant-capability"><Terminal size={17} /><div><strong>Work with your code</strong><span>Review suggestions before applying edits.</span></div></div><div className="assistant-capability"><Database size={17} /><div><strong>Ask your data</strong><span>Use governed queries and traceable sources.</span></div></div><div className="assistant-capability"><GitBranch size={17} /><div><strong>Build in-house agents</strong><span>Define tools, memory and explicit action policies.</span></div></div><div className="notice"><ShieldCheck size={17} /><p>Chat is unavailable until a provider adapter, tool policies and execution service are configured.</p></div><button className="button secondary full-width" onClick={() => { go('ai'); setSylon(false); }}>Explore AI components <ArrowRight size={15} /></button></div></aside>}
    {createWorkspace && <CreateWorkspace onClose={() => setCreateWorkspace(false)} onCreated={(created) => { setWorkspaces((items) => [...items, created]); leaveEditor(() => { setWorkspaceId(created.id); setView('develop'); }); setCreateWorkspace(false); }} />}
    {about && <Modal title="About this build" onClose={() => setAbout(false)}><div className="modal-body"><Pill>DataSlate · foundation</Pill><p>This is working source code for the workspace, document API, identity boundary and configuration previews. It is not a running lakehouse yet.</p><dl className="details-list"><dt>Document storage</dt><dd>{status?.persistence ?? 'Unavailable'}</dd><dt>Authentication</dt><dd>{status?.authMode ?? 'Unavailable'}</dd><dt>Query / Spark execution</dt><dd>Not implemented</dd><dt>Initial cloud target</dt><dd>AWS EKS + S3</dd></dl><p className="muted">Module definitions and previews are planning metadata. No preview installs services, verifies connections, submits jobs or generates AI answers.</p></div></Modal>}
  </div>;
}

function CreateWorkspace({ onClose, onCreated }: { onClose: () => void; onCreated: (workspace: Workspace) => void }) {
  const [name, setName] = useState(''); const [slug, setSlug] = useState(''); const [pending, setPending] = useState(false); const [error, setError] = useState('');
  return <Modal title="Create a workspace" onClose={onClose}><form className="modal-body" onSubmit={async (event) => { event.preventDefault(); setPending(true); setError(''); try { onCreated(await api.createWorkspace(name, slug)); } catch (err) { setError(message(err)); } finally { setPending(false); } }}><p className="muted">Give your team a shared place for notebooks and SQL.</p><label className="field">Workspace name<input autoFocus required maxLength={100} value={name} placeholder="e.g. Data engineering" onChange={(event) => { setName(event.target.value); setSlug(event.target.value.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '')); }} /></label><label className="field">Workspace slug<input required pattern="[a-z0-9]+(?:-[a-z0-9]+)*" maxLength={63} value={slug} onChange={(event) => setSlug(event.target.value)} /><small>Lowercase letters, numbers and hyphens.</small></label>{error && <ErrorNotice>{error}</ErrorNotice>}<div className="form-actions"><button type="button" className="button secondary" onClick={onClose}>Cancel</button><button className="button primary" disabled={pending || !name.trim() || !slug}>{pending ? <Spinner label="Creating…" /> : 'Create workspace'}</button></div></form></Modal>;
}
