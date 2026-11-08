import { useEffect, useRef, useState } from 'react';
import { ArrowUpRight, BookOpen, Database, RefreshCw, Search, ShieldCheck } from 'lucide-react';
import { message } from './api';
import type { Workspace } from './api';
import { useAuth } from './auth';
import { searchAPI, validSearchProfiles, validSearchResult } from './search-api';
import type { SearchCatalog, SearchHit, SearchInput, SearchProfile, SearchResult } from './search-api';
import { SearchIndex } from './SearchIndex';
import { EmptyState, ErrorNotice, Pill, Spinner } from './ui';

export interface SearchNavigation { openNotebook: (id: string) => void; openCatalog: (source: SearchCatalog) => void }
export function SearchWorkspace({ workspace, onDirty, ...navigation }: { workspace?: Workspace; onDirty: (value: boolean) => void } & SearchNavigation) {
  const auth = useAuth();
  if (!workspace) return <EmptyState icon={<Search size={26} />} title="Choose a workspace" description="Search indexes and source access belong to a workspace." />;
  if (!auth.authenticated) return <EmptyState icon={<ShieldCheck size={26} />} title="Sign in to search" description="Search queries and results are cleared when your session expires." />;
  return <AuthorizedSearch key={JSON.stringify([workspace.id, auth.config?.issuer, auth.user?.profile.sub])} workspace={workspace} onDirty={onDirty} {...navigation} />;
}
function AuthorizedSearch({ workspace, onDirty, ...navigation }: { workspace: Workspace; onDirty: (value: boolean) => void } & SearchNavigation) {
  const [profiles, setProfiles] = useState<SearchProfile[]>([]); const [profileId, setProfileId] = useState(''); const [mode, setMode] = useState<SearchInput['mode']>('lexical');
  const [query, setQuery] = useState(''); const [limit, setLimit] = useState(10); const [result, setResult] = useState<SearchResult>();
  const [loading, setLoading] = useState(true); const [pending, setPending] = useState(false); const [accessError, setAccessError] = useState(''); const [error, setError] = useState(''); const [attempt, setAttempt] = useState(0);
  const queryRequest = useRef<AbortController | undefined>(undefined); const epoch = useRef(0); const inFlight = useRef(false);
  const profile = profiles.find((p) => p.id === profileId); const bytes = new TextEncoder().encode(query).length;
  function clearQuery() { inFlight.current = false; epoch.current += 1; queryRequest.current?.abort(); setResult(undefined); setError(''); setPending(false); }
  useEffect(() => {
    const c = new AbortController(); inFlight.current = false; epoch.current += 1; queryRequest.current?.abort(); setResult(undefined); setPending(false); setError(''); setAccessError(''); setProfiles([]); setLoading(true);
    searchAPI.profiles(workspace.id, c.signal).then((data) => { if (c.signal.aborted) return; if (!validSearchProfiles(data, workspace.id)) throw new Error('The API returned invalid search profiles.'); setProfiles(data.items); setProfileId((id) => data.items.some((p) => p.id === id) ? id : data.items[0]?.id ?? ''); setMode('lexical'); })
      .catch((err) => { if (!c.signal.aborted) setAccessError(message(err)); }).finally(() => { if (!c.signal.aborted) setLoading(false); });
    return () => { c.abort(); queryRequest.current?.abort(); };
  }, [workspace.id, attempt]);
  async function search() {
    if (inFlight.current || !profile || !query.trim() || bytes > 4096 || query.includes('\0') || mode === 'hybrid' && (!profile.vectorAvailable || !profile.dataTransferAccepted)) return;
    queryRequest.current?.abort(); const c = new AbortController(); queryRequest.current = c; const revision = ++epoch.current;
    inFlight.current = true; const input: SearchInput = { bindingId: profile.id, query, mode, limit }; setPending(true); setError(''); setResult(undefined);
    try {
      const data = await searchAPI.search(workspace.id, input, AbortSignal.any([c.signal, AbortSignal.timeout(20000)]));
      if (!validSearchResult(data, workspace.id, input)) throw new Error('The API returned results for an invalid query or workspace. No matches were displayed.');
      if (!c.signal.aborted && epoch.current === revision) setResult(data);
    } catch (err) { if (!c.signal.aborted && epoch.current === revision) { setError(message(err)); setResult(undefined); } }
    finally { if (queryRequest.current === c) inFlight.current = false; if (!c.signal.aborted && epoch.current === revision) setPending(false); }
  }
  return <div className="page-wrap search-workspace"><div className="page-heading"><div><span className="eyebrow">WORKSPACE SEARCH</span><h1>Find the next connection.</h1><p>Discover saved documents and approved catalog schemas through your workspace's search index.</p></div><button className="button secondary" aria-label="Refresh search access" disabled={loading} onClick={() => setAttempt((v) => v + 1)}><RefreshCw size={16} />Refresh access</button></div>
    {loading ? <Spinner label="Loading search profiles…" /> : accessError ? <ErrorNotice retry={() => setAttempt((v) => v + 1)}>{accessError}</ErrorNotice> : <><div className="search-main-grid"><section className="search-query-panel" aria-label="Workspace search">{!profiles.length ? <EmptyState icon={<Search size={26} />} title="Search is not configured" description="An administrator can enable lexical search, or an approved embedding profile for hybrid search. Nothing is sent to a model from this screen until a profile is configured and you submit a request." /> : <><form onSubmit={(e) => { e.preventDefault(); void search(); }}><div className="search-profile-fields"><label className="field">Search profile<select aria-label="Search profile" value={profileId} onChange={(e) => { clearQuery(); setProfileId(e.target.value); setMode('lexical'); }}>{profiles.map((p) => <option key={p.id} value={p.id}>{p.name}</option>)}</select></label><label className="field">Search mode<select aria-label="Search mode" value={mode} onChange={(e) => { clearQuery(); setMode(e.target.value as typeof mode); }}><option value="lexical">Lexical · local matching</option><option value="hybrid" disabled={profile?.mode !== 'hybrid' || !profile.vectorAvailable || !profile.dataTransferAccepted}>Hybrid · embeddings + lexical</option></select></label></div>
      {profile && <div className={`notice ${mode === 'hybrid' ? 'warning' : ''}`}><ShieldCheck size={17} /><div><p>{mode === 'hybrid' ? `Your query is sent to the configured embedding service (${profile.embeddingModel}, ${profile.dimensions} dimensions). This transfer is enabled by your administrator.` : 'Lexical queries use local matching and do not call the embedding service.'}</p><p>Policy {profile.policyVersion}. {profile.includeCellSource ? 'Notebook cell source is enabled in this index.' : 'Notebook cell source is excluded from this index.'}</p>{profile.mode === 'hybrid' && !profile.vectorAvailable && <p>Hybrid search is unavailable until the database vector capability is ready. Select lexical search to query any existing local index.</p>}</div></div>}
      <label className="field search-query-input">Search query<textarea aria-label="Search query" rows={3} value={query} placeholder="Find notebooks or table schemas about customer orders" onChange={(e) => { clearQuery(); setQuery(e.target.value); }} /><small>{bytes.toLocaleString()} / 4,096 UTF-8 bytes</small></label><div className="search-query-actions"><label className="field">Maximum matches<select aria-label="Maximum search matches" value={limit} onChange={(e) => { clearQuery(); setLimit(Number(e.target.value)); }}><option value={5}>5</option><option value={10}>10</option><option value={20}>20</option></select></label><button className="button primary" disabled={pending || !query.trim() || bytes > 4096 || query.includes('\0')}><Search size={16} />{pending ? 'Searching…' : 'Search workspace'}</button></div></form>{error && <ErrorNotice>{error}</ErrorNotice>}{result && <SearchResults result={result} {...navigation} />}</>}
    </section><SearchIndex workspaceId={workspace.id} profile={profile} profiles={profiles} onDirty={onDirty} /></div></>}
  </div>;
}
function SearchResults({ result, ...navigation }: { result: SearchResult } & SearchNavigation) {
  return <section className="search-results" aria-label="Search results"><div className="section-toolbar"><h2>{result.items.length} {result.items.length === 1 ? 'match' : 'matches'}</h2><Pill>{result.mode === 'hybrid' ? 'Hybrid ranking' : 'Lexical ranking'}</Pill></div><p className="field-help">Matches come from the current authorized index. Scores rank this response; they are not confidence probabilities or proof of complete source coverage.</p>{result.items.length ? <ol className="search-hit-list">{result.items.map((hit) => <SearchResultCard key={hit.documentId} hit={hit} {...navigation} />)}</ol> : <EmptyState icon={<Search size={24} />} title="No indexed matches" description="Try another query or explicitly index an approved source. An empty result does not mean the source data is absent." />}</section>;
}
function SearchResultCard({ hit, openNotebook, openCatalog }: { hit: SearchHit } & SearchNavigation) {
  const expired = Boolean(hit.expiresAt && Date.parse(hit.expiresAt) <= Date.now());
  return <li className="search-hit"><div className="search-hit-heading"><span>{hit.kind === 'notebook' ? <BookOpen size={18} /> : <Database size={18} />}</span><div><h3>{hit.title}</h3><p>{hit.notebook ? `Saved revision ${hit.notebook.revision}` : `${hit.catalog!.bindingId} / ${hit.catalog!.namespace.join(' / ')} / ${hit.catalog!.name}`}</p></div><span className="search-rank">Rank score {hit.score.toPrecision(4)}</span></div><p className="search-snippet">{hit.snippet}</p><div className="search-hit-footer"><span>Indexed {new Date(hit.indexedAt).toLocaleString()}{hit.expiresAt && <> · expires {new Date(hit.expiresAt).toLocaleString()}</>}</span><button className="text-button" disabled={expired} onClick={() => hit.notebook ? openNotebook(hit.notebook.id) : openCatalog(hit.catalog!)}>{expired ? 'Refresh this index' : hit.notebook ? 'Open saved document' : 'Load current table metadata'}<ArrowUpRight size={14} /></button></div>{hit.notebook && <p className="field-help">Opening loads the current saved document and rechecks access.</p>}</li>;
}
