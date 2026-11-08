import { useEffect, useRef, useState } from 'react';
import { ChevronLeft, ChevronRight, Database, Folder, RefreshCw, Search, ShieldCheck, Table2 } from 'lucide-react';
import { message } from './api';
import type { Workspace } from './api';
import { catalogs, sameNamespace, validBindings, validCatalogTable, validNamespace } from './catalog-api';
import type { CatalogBinding, CatalogCheck, CatalogTable, TableIdentifier } from './catalog-api';
import { EmptyState, ErrorNotice, Pill, Spinner } from './ui';

const displayNamespace = (namespace: string[]) => namespace.length ? namespace.join(' / ') : '(root namespace)';

export function CatalogWorkspace({ workspace, initialSelection }: { workspace?: Workspace; initialSelection?: TableIdentifier & { bindingId: string } }) {
  const [bindings, setBindings] = useState<CatalogBinding[]>([]);
  const [selected, setSelected] = useState('');
  const [attempt, setAttempt] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  useEffect(() => {
    const controller = new AbortController(); setBindings([]); setSelected(''); setError(''); setLoading(true);
    if (!workspace) { setLoading(false); return; }
    catalogs.bindings(workspace.id, controller.signal).then((result) => {
      if (controller.signal.aborted) return;
      if (!validBindings(result, workspace.id)) throw new Error('The API returned catalog bindings for an invalid workspace or policy.');
      if (initialSelection && !result.items.some((b) => b.id === initialSelection.bindingId)) throw new Error('The requested catalog connection is no longer available to your identity.');
      setBindings(result.items); setSelected(initialSelection?.bindingId ?? result.items[0]?.id ?? '');
    }).catch((err) => { if (!controller.signal.aborted) setError(message(err)); }).finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, [workspace?.id, attempt, initialSelection]);
  const binding = bindings.find((b) => b.id === selected);
  return <div className="page-wrap catalog-page"><div className="page-heading"><div><span className="eyebrow">DATA CATALOG</span><h1>Your lake, connected.</h1><p>Explore the tables your identity can access, then inspect schema and snapshot metadata from your catalog.</p></div><button className="button secondary" aria-label="Refresh catalog access" disabled={loading || !workspace} onClick={() => setAttempt((v) => v + 1)}><RefreshCw size={16} />Refresh access</button></div>
    {!workspace ? <EmptyState icon={<Database size={26} />} title="Choose a workspace" description="Catalog connections are granted within a workspace." /> : loading ? <Spinner label="Loading catalog access…" /> : error ? <ErrorNotice retry={() => setAttempt((v) => v + 1)}>{error}</ErrorNotice> : !bindings.length ? <EmptyState icon={<Database size={28} />} title="No catalog access configured" description="Your administrator can connect Polaris and grant your identity access to specific Iceberg tables. Only your approved connections appear here." /> : <div className="catalog-layout"><aside className="catalog-connections" aria-label="Catalog connections"><h2>Connections <span className="muted">{bindings.length}</span></h2>{bindings.map((item) => <button key={item.id} className={`catalog-connection ${item.id === selected ? 'selected' : ''}`} aria-pressed={item.id === selected} onClick={() => setSelected(item.id)}><Database size={17} /><span><strong>{item.name}</strong><small>{item.catalog} · Polaris</small></span><ChevronRight size={15} /></button>)}</aside>{binding && <CatalogBrowser key={`${binding.workspaceId}/${binding.id}/${binding.policyVersion}/${attempt}`} binding={binding} initialSelection={initialSelection?.bindingId === binding.id ? initialSelection : undefined} />}</div>}
  </div>;
}

function CatalogBrowser({ binding, initialSelection }: { binding: CatalogBinding; initialSelection?: TableIdentifier }) {
  const initialApplied = useRef(false);
  const [namespaces, setNamespaces] = useState<string[][]>([]);
  const [namespace, setNamespace] = useState<string[]>();
  const [tables, setTables] = useState<TableIdentifier[]>([]);
  const [selected, setSelected] = useState<TableIdentifier>();
  const [table, setTable] = useState<CatalogTable>();
  const [filter, setFilter] = useState('');
  const [indexLoading, setIndexLoading] = useState(true);
  const [tablesLoading, setTablesLoading] = useState(false);
  const [metadataLoading, setMetadataLoading] = useState(false);
  const [indexError, setIndexError] = useState('');
  const [tableError, setTableError] = useState('');
  const [indexAttempt, setIndexAttempt] = useState(0);
  const [tableAttempt, setTableAttempt] = useState(0);
  const [checkAttempt, setCheckAttempt] = useState(0);
  const [check, setCheck] = useState<CatalogCheck>();
  const [checking, setChecking] = useState(false);
  const [checkError, setCheckError] = useState('');
  const [tablePage, setTablePage] = useState(0);

  useEffect(() => {
    const controller = new AbortController(); setNamespaces([]); setNamespace(undefined); setTables([]); setSelected(undefined); setTable(undefined); setIndexError(''); setIndexLoading(true);
    catalogs.namespaces(binding.workspaceId, binding.id, controller.signal).then((result) => {
      if (controller.signal.aborted) return;
      if (result.source !== 'authorized-grants' || !Array.isArray(result.items) || !result.items.every((item) => validNamespace(item.namespace))) throw new Error('The API returned an invalid catalog grant index.');
      if (initialSelection && !result.items.some((item) => sameNamespace(item.namespace, initialSelection.namespace))) throw new Error('The requested table namespace is no longer authorized.');
      setNamespaces(result.items.map((item) => item.namespace)); setNamespace(initialSelection?.namespace ?? result.items[0]?.namespace);
    }).catch((err) => { if (!controller.signal.aborted) setIndexError(message(err)); }).finally(() => { if (!controller.signal.aborted) setIndexLoading(false); });
    return () => controller.abort();
  }, [binding.id, binding.workspaceId, indexAttempt, initialSelection]);

  useEffect(() => {
    const controller = new AbortController(); setTables([]); setSelected(undefined); setTable(undefined); setTableError(''); setFilter(''); setTablePage(0); setTablesLoading(Boolean(namespace));
    if (!namespace) return;
    catalogs.tables(binding.workspaceId, binding.id, namespace, controller.signal).then((result) => {
      if (controller.signal.aborted) return;
      if (result.source !== 'authorized-grants' || !Array.isArray(result.items) || !result.items.every((item) => validNamespace(item.namespace) && sameNamespace(item.namespace, namespace) && typeof item.name === 'string' && item.name)) throw new Error('The API returned an invalid table grant index for this namespace.');
      setTables(result.items);
      if (initialSelection && !initialApplied.current && sameNamespace(namespace, initialSelection.namespace)) {
        const target = result.items.find((item) => item.name === initialSelection.name);
        if (!target) throw new Error('The requested table grant is no longer available.');
        initialApplied.current = true; setSelected(target);
      }
    }).catch((err) => { if (!controller.signal.aborted) setIndexError(message(err)); }).finally(() => { if (!controller.signal.aborted) setTablesLoading(false); });
    return () => controller.abort();
  }, [binding.id, binding.workspaceId, namespace, initialSelection]);

  useEffect(() => {
    const controller = new AbortController(); setTable(undefined); setTableError(''); setMetadataLoading(Boolean(selected));
    if (!selected) return;
    catalogs.table(binding.workspaceId, binding.id, selected, controller.signal).then((result) => {
      if (controller.signal.aborted) return;
      if (!validCatalogTable(result, binding, selected)) throw new Error('The API returned metadata for a different table or policy. Refresh catalog access before trying again.');
      setTable(result);
    }).catch((err) => { if (!controller.signal.aborted) setTableError(message(err)); }).finally(() => { if (!controller.signal.aborted) setMetadataLoading(false); });
    return () => controller.abort();
  }, [binding, selected, tableAttempt]);

  useEffect(() => {
    const controller = new AbortController();
    if (!checkAttempt) return;
    setCheck(undefined); setCheckError(''); setChecking(true);
    catalogs.verify(binding.workspaceId, binding.id, controller.signal).then((result) => {
      if (controller.signal.aborted) return;
      if (result.bindingId !== binding.id || result.workspaceId !== binding.workspaceId || result.verified !== true || !Number.isFinite(Date.parse(result.checkedAt)) || !Array.isArray(result.capabilities) || !result.capabilities.every((value) => typeof value === 'string')) throw new Error('The API returned an invalid discovery confirmation.');
      setCheck(result);
    }).catch((err) => { if (!controller.signal.aborted) setCheckError(message(err)); }).finally(() => { if (!controller.signal.aborted) setChecking(false); });
    return () => controller.abort();
  }, [binding, checkAttempt]);

  const visible = tables.filter((item) => item.name.toLocaleLowerCase().includes(filter.toLocaleLowerCase()));
  const checkedAt = check?.checkedAt ?? (checkAttempt === 0 ? binding.checkedAt : undefined);
  const discoveryVerified = !checking && !checkError && (check?.verified || checkAttempt === 0 && binding.discoveryVerified);
  return <section className="catalog-browser" aria-label={binding.name}><div className="catalog-browser-heading"><div><span className="eyebrow">POLARIS / ICEBERG</span><h2>{binding.name}</h2><p className="muted">{binding.catalog} · policy {binding.policyVersion}</p></div><button className="button secondary compact" disabled={checking} onClick={() => { setSelected(undefined); setTable(undefined); setCheckAttempt((v) => v + 1); }}><ShieldCheck size={15} />{checking ? 'Checking…' : 'Check catalog'}</button></div>
    <div className="catalog-discovery"><Pill tone={discoveryVerified ? 'green' : 'neutral'}>{discoveryVerified ? 'Discovery checked' : 'Discovery not confirmed'}</Pill>{checkedAt && <span>Checked {new Date(checkedAt).toLocaleString()}</span>}<p>Discovery checks the catalog configuration. Loading a table separately confirms that its metadata is readable.</p>{check && <p>{check.capabilities.length ? `Advertised read capabilities: ${check.capabilities.join(', ')}` : 'No table-read capability was advertised. Table metadata may be unavailable.'}</p>}</div>
    {checkError && <ErrorNotice>{checkError}</ErrorNotice>}
    <div className="notice"><ShieldCheck size={17} /><p>The browser lists your configured table grants. Names do not confirm that a table exists upstream; select one to load its metadata.</p></div>
    {indexLoading ? <Spinner label="Loading authorized namespaces…" /> : indexError ? <ErrorNotice retry={() => setIndexAttempt((v) => v + 1)}>{indexError}</ErrorNotice> : !namespaces.length ? <EmptyState icon={<Folder size={24} />} title="No table grants" description="This connection has no tables granted to your identity." /> : <><div className="catalog-tools"><label className="field">Namespace<select aria-label="Catalog namespace" value={JSON.stringify(namespace ?? [])} onChange={(event) => { setSelected(undefined); setTable(undefined); setNamespace(JSON.parse(event.target.value)); }}>{namespaces.map((item) => <option key={JSON.stringify(item)} value={JSON.stringify(item)}>{displayNamespace(item)}</option>)}</select></label><label className="search-field"><Search size={15} /><input aria-label="Filter authorized tables" placeholder="Filter authorized tables" value={filter} onChange={(event) => { setFilter(event.target.value); setTablePage(0); }} /></label></div>
      {tablesLoading ? <Spinner label="Loading authorized tables…" /> : <div className="catalog-table-layout"><div className="catalog-table-index" aria-label="Authorized tables"><h3>Authorized tables <span className="muted">{visible.length}</span></h3>{visible.slice(tablePage * 50, tablePage * 50 + 50).map((item) => <button key={JSON.stringify(item)} className={`catalog-table-link ${selected?.name === item.name ? 'selected' : ''}`} aria-pressed={selected?.name === item.name} onClick={() => { setTable(undefined); setTableError(''); setSelected(item); }}><Table2 size={16} /><span>{item.name}</span><ChevronRight size={14} /></button>)}{!visible.length && <p className="muted">{tables.length ? 'No matching table grants.' : 'No tables granted in this namespace.'}</p>}{visible.length > 50 && <div className="catalog-pagination"><button className="icon-button" aria-label="Previous table grants" disabled={!tablePage} onClick={() => setTablePage((v) => v - 1)}><ChevronLeft size={16} /></button><span>{tablePage * 50 + 1}–{Math.min(visible.length, tablePage * 50 + 50)} of {visible.length}</span><button className="icon-button" aria-label="Next table grants" disabled={(tablePage + 1) * 50 >= visible.length} onClick={() => setTablePage((v) => v + 1)}><ChevronRight size={16} /></button></div>}</div>
        <section className="catalog-metadata" aria-label="Table metadata">{metadataLoading ? <Spinner label="Loading metadata from Polaris…" /> : tableError ? <ErrorNotice retry={() => setTableAttempt((v) => v + 1)}>{tableError}</ErrorNotice> : table ? <TableMetadata key={`${table.uuid}/${table.lastUpdatedMs}`} table={table} refresh={() => { setTable(undefined); setTableAttempt((v) => v + 1); }} /> : <EmptyState icon={<Table2 size={26} />} title="Inspect an authorized table" description="Select a table to read its current Iceberg schema and snapshot metadata from Polaris. Data rows and storage credentials are not loaded here." />}</section>
      </div>}
    </>}
  </section>;
}

function TableMetadata({ table, refresh }: { table: CatalogTable; refresh: () => void }) {
  const [page, setPage] = useState(0); const fields = table.currentSchema.fields;
  return <><div className="catalog-metadata-heading"><div><span className="eyebrow">LIVE METADATA RESPONSE</span><h3>{table.identifier.name}</h3><p className="muted">{displayNamespace(table.identifier.namespace)}</p></div><button className="icon-button" aria-label="Refresh table metadata" onClick={refresh}><RefreshCw size={16} /></button></div><dl className="details-list"><dt>Format</dt><dd>Iceberg v{table.formatVersion}</dd><dt>Table UUID</dt><dd className="mono">{table.uuid}</dd><dt>Current snapshot</dt><dd className="mono">{table.currentSnapshotId ?? 'No current snapshot'}</dd><dt>Metadata updated</dt><dd>{new Date(table.lastUpdatedMs).toLocaleString()}</dd><dt>Policy version</dt><dd>{table.policyVersion}</dd></dl><div className="section-toolbar"><h3>Schema {table.currentSchema.id}</h3><span className="muted">{fields.length} fields</span></div><div className="catalog-schema-scroll"><table className="catalog-schema"><caption className="sr-only">Current Iceberg schema for {table.identifier.name}</caption><thead><tr><th scope="col">Field</th><th scope="col">Type</th><th scope="col">Required</th><th scope="col">ID</th></tr></thead><tbody>{fields.slice(page * 50, page * 50 + 50).map((field) => <tr key={field.id}><th scope="row">{field.name}</th><td className="mono">{typeof field.type === 'string' ? field.type : JSON.stringify(field.type)}</td><td>{field.required ? 'Yes' : 'No'}</td><td>{field.id}</td></tr>)}</tbody></table></div>{!fields.length && <p className="muted">The returned schema contains no fields.</p>}{fields.length > 50 && <div className="catalog-pagination"><button className="icon-button" aria-label="Previous schema fields" disabled={!page} onClick={() => setPage((v) => v - 1)}><ChevronLeft size={16} /></button><span>Fields {page * 50 + 1}–{Math.min(fields.length, page * 50 + 50)} of {fields.length}</span><button className="icon-button" aria-label="Next schema fields" disabled={(page + 1) * 50 >= fields.length} onClick={() => setPage((v) => v + 1)}><ChevronRight size={16} /></button></div>}<p className="field-help">Schema and snapshot metadata only. Query data through a separately granted SQL binding. Metadata does not prove your engine's row or column policies.</p></>;
}
