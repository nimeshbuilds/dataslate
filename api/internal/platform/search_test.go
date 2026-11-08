package platform

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"dataslate.local/api/internal/embeddings"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func searchBinding() SearchBinding {
	return SearchBinding{ID: "workspace-search", WorkspaceID: "default", Name: "Workspace search", PolicyVersion: "v1", Mode: "lexical", Sources: SearchSources{Notebooks: true, Catalogs: []SearchCatalogGrant{}}, MaxDocuments: 100}
}
func hybridSearchBinding() SearchBinding {
	b := searchBinding()
	b.Mode = "hybrid"
	b.DataTransferAccepted = true
	b.Embedding = &embeddings.Config{Endpoint: "https://embeddings.example.com/v1/embeddings", Model: "fixture-model", Dimensions: 2, Auth: embeddings.Auth{BearerTokenFile: "/run/secrets/embeddings/token"}}
	return b
}

type fakeSearchEmbedder struct {
	calls  int
	inputs [][]string
	err    error
	hook   func()
}

func (f *fakeSearchEmbedder) Embed(_ context.Context, _ embeddings.Config, input []string) ([][]float32, error) {
	f.calls++
	f.inputs = append(f.inputs, append([]string{}, input...))
	if f.hook != nil {
		f.hook()
	}
	out := make([][]float32, len(input))
	for i, text := range input {
		out[i] = []float32{1, 0}
		if strings.Contains(strings.ToLower(text), "warehouse") {
			out[i] = []float32{0, 1}
		}
	}
	return out, f.err
}
func searchHandler(store Store, p Principal, b []SearchBinding, catalogs []CatalogBinding, embed SearchEmbedder) http.Handler {
	return (&Server{Config: Config{AuthMode: "oidc", SearchBindings: b, CatalogBindings: catalogs}, Store: store, Auth: testAuth{p: p}, SearchEmbedder: embed}).Handler()
}
func searchIndex(t *testing.T, s Store, b SearchBinding, owner ExecutionSubject, in SearchIndexInput) SearchJob {
	t.Helper()
	if in.BindingID == "" {
		in.BindingID = b.ID
	}
	if in.IdempotencyKey == "" {
		in.IdempotencyKey = newResource("intent-").ID
	}
	j, _, err := s.CreateSearchJob(context.Background(), newSearchJob(b, owner, in), b.Hash(), in.IdempotencyKey, searchFingerprint(in), b.MaxDocuments)
	if err != nil {
		t.Fatal(err)
	}
	return j
}
func searchHits(t *testing.T, h http.Handler, ws, binding, query, mode string) []SearchHit {
	t.Helper()
	data, _ := json.Marshal(SearchRequest{BindingID: binding, Query: query, Mode: mode, Limit: 20})
	r := request(h, "POST", "/api/v1/workspaces/"+ws+"/search", string(data))
	if r.Code != 200 {
		t.Fatal(r.Code, r.Body)
	}
	var out struct{ Items []SearchHit }
	if json.Unmarshal(r.Body.Bytes(), &out) != nil {
		t.Fatal("invalid search receipt")
	}
	return out.Items
}
func searchTestStore(t *testing.T) (*PostgresStore, string) {
	t.Helper()
	dsn := os.Getenv("DATASLATE_TEST_DATABASE_URL")
	if dsn == "" {
		if os.Getenv("DATASLATE_TEST_REQUIRE_VECTOR") == "1" {
			t.Fatal("required vector qualification has no PostgreSQL URL")
		}
		t.Skip("isolated PostgreSQL required")
	}
	admin, err := OpenPostgres(context.Background(), dsn, false)
	if err != nil {
		t.Fatal(err)
	}
	schema := strings.ReplaceAll(newResource("test_").ID, "-", "_")
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.pool.Exec(context.Background(), "CREATE SCHEMA "+quoted); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	// A separate schema prevents a global job lease from claiming another test's
	// unfinished work. Vector types/operators remain explicitly public-qualified.
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	s := &PostgresStore{pool: pool}
	t.Cleanup(func() {
		s.Close()
		_, _ = admin.pool.Exec(context.Background(), "DROP SCHEMA "+quoted+" CASCADE")
		admin.Close()
	})
	if err = s.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	ws, err := s.CreateWorkspace(context.Background(), "Search tests", newResource("test-").ID)
	if err != nil {
		t.Fatal(err)
	}
	return s, ws.ID
}
func requireSearchVector(t *testing.T, s Store) {
	t.Helper()
	available, err := s.SearchVectorAvailable(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !available {
		if os.Getenv("DATASLATE_TEST_REQUIRE_VECTOR") == "1" {
			t.Fatal("required pgvector extension is not installed in public")
		}
		t.Skip("operator-installed public pgvector required")
	}
}
func exerciseLexicalSearch(t *testing.T, s Store, ws string) {
	ctx := context.Background()
	b := searchBinding()
	b.WorkspaceID = ws
	p := executionPrincipal()
	p.Workspaces = map[string]string{ws: "editor"}
	embed := &fakeSearchEmbedder{}
	h := searchHandler(s, p, []SearchBinding{b}, nil, embed)
	input := NotebookInput{Name: "Revenue forecasting", Kind: "notebook", Cells: []Cell{{ID: "c1", Language: "python", Source: "SUPERSECRETLITERAL = 'not-for-index'"}}}
	n, err := s.SaveNotebook(ctx, ws, "", input)
	if err != nil {
		t.Fatal(err)
	}
	controller := &SearchController{Config: Config{SearchBindings: []SearchBinding{b}}, Store: s, Embedder: embed, CatalogBackend: &fakeCatalogBackend{}}
	if err = controller.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = controller.ReconcileOnce(ctx, "worker"); err != nil {
		t.Fatal(err)
	}
	hits := searchHits(t, h, ws, b.ID, "Revenue", "lexical")
	if len(hits) != 1 || hits[0].Notebook == nil || hits[0].Notebook.Revision != 1 || strings.Contains(hits[0].Snippet, "SUPERSECRET") {
		t.Fatal("metadata-only indexing failed")
	}
	if len(searchHits(t, h, ws, b.ID, "SUPERSECRETLITERAL", "lexical")) != 0 || embed.calls != 0 {
		t.Fatal("cell code left metadata boundary")
	}
	input.Name = "Inventory planning"
	input.Revision = 1
	n, err = s.SaveNotebook(ctx, ws, n.ID, input)
	if err != nil {
		t.Fatal(err)
	}
	if len(searchHits(t, h, ws, b.ID, "Revenue", "lexical")) != 0 {
		t.Fatal("stale source remained searchable after edit")
	}
	if err = controller.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = controller.ReconcileOnce(ctx, "worker"); err != nil {
		t.Fatal(err)
	}
	if len(searchHits(t, h, ws, b.ID, "Inventory", "lexical")) != 1 {
		t.Fatal("new revision missing")
	}
	if err = s.DeleteNotebook(ctx, ws, n.ID, 2); err != nil {
		t.Fatal(err)
	}
	if len(searchHits(t, h, ws, b.ID, "Inventory", "lexical")) != 0 {
		t.Fatal("deleted notebook retained searchable contents")
	}
}
func TestSearchLexicalStores(t *testing.T) {
	t.Run("memory", func(t *testing.T) { exerciseLexicalSearch(t, NewMemoryStore(), "default") })
	t.Run("postgres", func(t *testing.T) { s, ws := searchTestStore(t); exerciseLexicalSearch(t, s, ws) })
}
func TestSearchIndexIntentScopeAndCapacity(t *testing.T) {
	s := NewMemoryStore()
	b := searchBinding()
	b.MaxDocuments = 1
	p := executionPrincipal()
	h := searchHandler(s, p, []SearchBinding{b}, nil, &fakeSearchEmbedder{})
	n, _ := s.SaveNotebook(context.Background(), "default", "", input())
	in := SearchIndexInput{BindingID: b.ID, IdempotencyKey: "one-intent", Notebook: &SearchNotebookSelection{n.ID, 1}}
	body, _ := json.Marshal(in)
	path := "/api/v1/workspaces/default/search/index"
	r := request(h, "POST", path, string(body))
	if r.Code != 202 {
		t.Fatal(r.Code, r.Body)
	}
	var job SearchJob
	json.Unmarshal(r.Body.Bytes(), &job)
	r = request(h, "POST", path, string(body))
	if r.Code != 200 {
		t.Fatal("exact intent replay failed")
	}
	in.Notebook.Revision = 2
	body, _ = json.Marshal(in)
	r = request(h, "POST", path, string(body))
	if r.Code != 409 {
		t.Fatal("changed intent replayed")
	}
	n2, _ := s.SaveNotebook(context.Background(), "default", "", input())
	in.IdempotencyKey = "second-intent"
	in.Notebook = &SearchNotebookSelection{n2.ID, 1}
	body, _ = json.Marshal(in)
	r = request(h, "POST", path, string(body))
	if r.Code != 429 {
		t.Fatal("document budget not reserved before provider request", r.Code)
	}
	other := p
	other.Subject = "other"
	h = searchHandler(s, other, []SearchBinding{b}, nil, &fakeSearchEmbedder{})
	r = request(h, "GET", path+"-jobs/"+job.ID, "")
	if r.Code != 404 {
		t.Fatal("index job owner scope leaked", r.Code)
	}
	p.Workspaces["default"] = "viewer"
	h = searchHandler(s, p, []SearchBinding{b}, nil, &fakeSearchEmbedder{})
	r = request(h, "POST", path, string(body))
	if r.Code != 403 {
		t.Fatal("viewer indexed shared notebook")
	}
}
func TestSearchMetadataAndUnicodeBounds(t *testing.T) {
	text := strings.Repeat("猫", 20000)
	chunks, err := chunkSearchText(text)
	if err != nil {
		t.Fatal(err)
	}
	joined := ""
	for _, chunk := range chunks {
		if len(chunk.Text) > 2000 || !utf8.ValidString(chunk.Text) {
			t.Fatal("split UTF-8 rune")
		}
		joined += chunk.Text
	}
	if joined != text {
		t.Fatal("source silently truncated")
	}
	if _, err = chunkSearchText(strings.Repeat("x", MaxSearchSourceBytes+1)); err == nil {
		t.Fatal("oversized source accepted")
	}
	b := hybridSearchBinding()
	b.DataTransferAccepted = false
	if b.Validate() == nil {
		t.Fatal("unapproved external transfer accepted")
	}
	b = searchBinding()
	b.Sources.Notebooks = false
	b.Sources.IncludeCellSource = true
	if b.Validate() == nil {
		t.Fatal("source transfer without notebook indexing accepted")
	}
	s := NewMemoryStore()
	b = searchBinding()
	b.Sources.IncludeCellSource = true
	n, _ := s.SaveNotebook(context.Background(), "default", "", NotebookInput{Name: "Oversized", Kind: "notebook", Cells: []Cell{{ID: "c1", Language: "python", Source: strings.Repeat("x", MaxSearchSourceBytes)}}})
	embed := &fakeSearchEmbedder{}
	job := searchIndex(t, s, b, subjectFor(executionPrincipal()), SearchIndexInput{Notebook: &SearchNotebookSelection{n.ID, 1}})
	controller := &SearchController{Store: s, Config: Config{SearchBindings: []SearchBinding{b}}, Embedder: embed}
	controller.ReconcileOnce(context.Background(), "worker")
	job, _ = s.SearchJob(context.Background(), "default", job.ID, job.SubmittedBy)
	if job.State != "failed" || embed.calls != 0 {
		t.Fatal("oversize source was indexed or transferred")
	}
}
func TestSearchCatalogPolicyAndTTL(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	catalog := catalogBinding()
	b := searchBinding()
	b.Sources.Notebooks = false
	b.Sources.Catalogs = []SearchCatalogGrant{{BindingID: catalog.ID, Tables: catalog.AllowedSubjects[0].Tables}}
	owner := subjectFor(executionPrincipal())
	selection := SearchCatalogSelection{BindingID: catalog.ID, Identifier: catalog.AllowedSubjects[0].Tables[0]}
	job := searchIndex(t, s, b, owner, SearchIndexInput{Catalog: &selection})
	backend := &fakeCatalogBackend{}
	controller := &SearchController{Store: s, Config: Config{SearchBindings: []SearchBinding{b}, CatalogBindings: []CatalogBinding{catalog}}, CatalogBackend: backend, Embedder: &fakeSearchEmbedder{}}
	if _, err := controller.ReconcileOnce(ctx, "worker"); err != nil {
		t.Fatal(err)
	}
	job, _ = s.SearchJob(ctx, "default", job.ID, owner)
	if job.State != "succeeded" {
		t.Fatal(job.State, job.Message)
	}
	h := searchHandler(s, executionPrincipal(), []SearchBinding{b}, []CatalogBinding{catalog}, &fakeSearchEmbedder{})
	hits := searchHits(t, h, "default", b.ID, "orders", "lexical")
	if len(hits) != 1 || hits[0].Catalog == nil || strings.Contains(hits[0].Snippet, "s3://") || hits[0].ExpiresAt == nil {
		t.Fatal("safe catalog snapshot missing")
	}
	other := executionPrincipal()
	other.Subject = "other"
	h = searchHandler(s, other, []SearchBinding{b}, []CatalogBinding{catalog}, &fakeSearchEmbedder{})
	if len(searchHits(t, h, "default", b.ID, "orders", "lexical")) != 0 {
		t.Fatal("catalog grants widened by shared index")
	}
	catalog.PolicyVersion = "v2"
	h = searchHandler(s, executionPrincipal(), []SearchBinding{b}, []CatalogBinding{catalog}, &fakeSearchEmbedder{})
	if len(searchHits(t, h, "default", b.ID, "orders", "lexical")) != 0 {
		t.Fatal("stale authority hash exposed catalog content")
	}
	s.mu.Lock()
	for key, doc := range s.searchDocuments {
		expired := time.Now().Add(-time.Second)
		doc.ExpiresAt = &expired
		s.searchDocuments[key] = doc
	}
	s.mu.Unlock()
	catalog.PolicyVersion = "v1"
	h = searchHandler(s, executionPrincipal(), []SearchBinding{b}, []CatalogBinding{catalog}, &fakeSearchEmbedder{})
	if len(searchHits(t, h, "default", b.ID, "orders", "lexical")) != 0 {
		t.Fatal("expired catalog metadata remained searchable")
	}
}
func TestSearchPostgresHybridAndUnknownAttempt(t *testing.T) {
	s, ws := searchTestStore(t)
	requireSearchVector(t, s)
	ctx := context.Background()
	b := hybridSearchBinding()
	b.WorkspaceID = ws
	p := executionPrincipal()
	p.Workspaces = map[string]string{ws: "editor"}
	embed := &fakeSearchEmbedder{}
	controller := &SearchController{Store: s, Config: Config{SearchBindings: []SearchBinding{b}}, Embedder: embed}
	for _, name := range []string{"Feline finance", "Warehouse analytics"} {
		n, err := s.SaveNotebook(ctx, ws, "", NotebookInput{Name: name, Kind: "notebook", Cells: []Cell{}})
		if err != nil {
			t.Fatal(err)
		}
		searchIndex(t, s, b, subjectFor(p), SearchIndexInput{Notebook: &SearchNotebookSelection{n.ID, 1}})
		if _, err = controller.ReconcileOnce(ctx, "worker"); err != nil {
			t.Fatal(err)
		}
	}
	h := searchHandler(s, p, []SearchBinding{b}, nil, embed)
	hits := searchHits(t, h, ws, b.ID, "cat", "hybrid")
	if len(hits) != 2 || hits[0].Title != "Feline finance" {
		t.Fatal("pgvector ranking failed", len(hits))
	}
	if len(searchHits(t, h, ws, b.ID, "cat", "lexical")) != 0 {
		t.Fatal("semantic hit fabricated as lexical")
	}
	n, _ := s.SaveNotebook(ctx, ws, "", NotebookInput{Name: "Uncertain request", Kind: "notebook", Cells: []Cell{}})
	job := searchIndex(t, s, b, subjectFor(p), SearchIndexInput{Notebook: &SearchNotebookSelection{n.ID, 1}})
	embed.err = errors.New("lost response")
	before := embed.calls
	if _, err := controller.ReconcileOnce(ctx, "worker"); err != nil {
		t.Fatal(err)
	}
	job, _ = s.SearchJob(ctx, ws, job.ID, subjectFor(p))
	if job.State != "unknown" || !job.EmbeddingStarted {
		t.Fatal("ambiguous transfer not durable", job)
	}
	controller.ReconcileOnce(ctx, "restarted-worker")
	if embed.calls != before+1 {
		t.Fatal("provider POST automatically repeated")
	}
}
func TestSearchPostgresLeaseAndEditDuringEmbedding(t *testing.T) {
	s, ws := searchTestStore(t)
	requireSearchVector(t, s)
	ctx := context.Background()
	b := hybridSearchBinding()
	b.WorkspaceID = ws
	p := executionPrincipal()
	p.Workspaces = map[string]string{ws: "editor"}
	n, _ := s.SaveNotebook(ctx, ws, "", NotebookInput{Name: "Old revision", Kind: "notebook", Cells: []Cell{}})
	job := searchIndex(t, s, b, subjectFor(p), SearchIndexInput{Notebook: &SearchNotebookSelection{n.ID, 1}})
	_, lease, err := s.AcquireSearchJob(ctx, "lost-worker", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.MarkSearchEmbedding(ctx, lease); err != nil {
		t.Fatal(err)
	}
	s.pool.Exec(ctx, "UPDATE dataslate_search_jobs SET lease_until=now()-interval '1 second' WHERE id=$1", job.ID)
	embed := &fakeSearchEmbedder{}
	controller := &SearchController{Store: s, Config: Config{SearchBindings: []SearchBinding{b}}, Embedder: embed}
	if _, err = controller.ReconcileOnce(ctx, "recovered-worker"); err != nil {
		t.Fatal(err)
	}
	if embed.calls != 0 {
		t.Fatal("restart repeated possible provider POST")
	}
	if err = s.FinishSearchJob(ctx, lease, "failed", "stale", nil, 0); !errors.Is(err, ErrSearchLease) {
		t.Fatal("stale worker bypassed fencing")
	}
	job = searchIndex(t, s, b, subjectFor(p), SearchIndexInput{Notebook: &SearchNotebookSelection{n.ID, 1}})
	embed.hook = func() {
		_, err := s.SaveNotebook(ctx, ws, n.ID, NotebookInput{Name: "New revision", Kind: "notebook", Cells: []Cell{}, Revision: 1})
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err = controller.ReconcileOnce(ctx, "worker"); err != nil {
		t.Fatal(err)
	}
	job, _ = s.SearchJob(ctx, ws, job.ID, subjectFor(p))
	if job.State != "superseded" {
		t.Fatal("in-flight stale source published", job.State)
	}
	h := searchHandler(s, p, []SearchBinding{b}, nil, embed)
	if len(searchHits(t, h, ws, b.ID, "Old", "lexical")) != 0 {
		t.Fatal("superseded embedding searchable")
	}
}
func TestSearchRealTLSEmbeddingsAndPolicyRevocation(t *testing.T) {
	s, ws := searchTestStore(t)
	requireSearchVector(t, s)
	ctx := context.Background()
	posts := 0
	var onPost func()
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posts++
		if r.Method != "POST" || r.URL.Path != "/v1/embeddings" || r.Header.Get("Authorization") != "Bearer embedding-token" {
			t.Error("wrong provider request")
		}
		var in struct {
			Input      []string
			Model      string
			Dimensions int
		}
		json.NewDecoder(r.Body).Decode(&in)
		if in.Model != "fixture-model" || in.Dimensions != 2 {
			t.Error("wrong model profile")
		}
		data := []map[string]any{}
		for i, input := range in.Input {
			if strings.Contains(input, "SUPERSECRETLITERAL") {
				t.Error("default metadata index sent cell source")
			}
			data = append(data, map[string]any{"object": "embedding", "index": i, "embedding": []float32{1, 0}})
		}
		if onPost != nil {
			onPost()
		}
		json.NewEncoder(w).Encode(map[string]any{"object": "list", "model": "fixture-model", "data": data})
	}))
	defer upstream.Close()
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.pem")
	token := filepath.Join(dir, "token")
	os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: upstream.Certificate().Raw}), 0600)
	os.WriteFile(token, []byte("embedding-token"), 0600)
	b := hybridSearchBinding()
	b.WorkspaceID = ws
	b.Embedding.Endpoint = upstream.URL + "/v1/embeddings"
	b.Embedding.CAFile = ca
	b.Embedding.Auth.BearerTokenFile = token
	file := filepath.Join(dir, "search.json")
	write := func() { data, _ := json.Marshal([]SearchBinding{b}); os.WriteFile(file, data, 0600) }
	write()
	n, _ := s.SaveNotebook(ctx, ws, "", NotebookInput{Name: "Revenue", Kind: "notebook", Cells: []Cell{{ID: "c1", Language: "python", Source: "SUPERSECRETLITERAL"}}})
	p := executionPrincipal()
	p.Workspaces = map[string]string{ws: "editor"}
	searchIndex(t, s, b, subjectFor(p), SearchIndexInput{Notebook: &SearchNotebookSelection{n.ID, 1}})
	embed := &GatewaySearchEmbedder{}
	controller := &SearchController{Store: s, Config: Config{SearchBindingsFile: file}, Embedder: embed}
	if _, err := controller.ReconcileOnce(ctx, "worker"); err != nil {
		t.Fatal(err)
	}
	server := &Server{Config: Config{AuthMode: "oidc", SearchBindingsFile: file}, Store: s, Auth: testAuth{p: p}, SearchEmbedder: embed}
	h := server.Handler()
	if len(searchHits(t, h, ws, b.ID, "cashflow", "hybrid")) != 1 {
		t.Fatal("real TLS embedding search failed")
	}
	onPost = func() { b.PolicyVersion = "v2"; write() }
	body, _ := json.Marshal(SearchRequest{BindingID: b.ID, Query: "Revenue", Mode: "hybrid", Limit: 20})
	r := request(h, "POST", "/api/v1/workspaces/"+ws+"/search", string(body))
	if r.Code != 403 || strings.Contains(r.Body.String(), "snippet") {
		t.Fatal("in-flight policy change leaked result", r.Code, r.Body)
	}
	if posts != 3 {
		t.Fatal("unexpected provider retry", posts)
	}
}

type searchRetrievalHookStore struct {
	Store
	after func()
}

func (s searchRetrievalHookStore) Search(ctx context.Context, scope SearchScope, in SearchRequest, vector []float32) ([]SearchHit, error) {
	hits, err := s.Store.Search(ctx, scope, in, vector)
	if s.after != nil {
		s.after()
	}
	return hits, err
}
func exerciseSearchRetrievalRace(t *testing.T, s Store, ws string, remove bool) {
	ctx := context.Background()
	b := searchBinding()
	b.WorkspaceID = ws
	p := executionPrincipal()
	p.Workspaces = map[string]string{ws: "editor"}
	n, err := s.SaveNotebook(ctx, ws, "", NotebookInput{Name: "Revenue private revision", Kind: "notebook", Cells: []Cell{}})
	if err != nil {
		t.Fatal(err)
	}
	searchIndex(t, s, b, subjectFor(p), SearchIndexInput{Notebook: &SearchNotebookSelection{n.ID, 1}})
	controller := &SearchController{Store: s, Config: Config{SearchBindings: []SearchBinding{b}}, Embedder: &fakeSearchEmbedder{}}
	if _, err = controller.ReconcileOnce(ctx, "worker"); err != nil {
		t.Fatal(err)
	}
	wrapped := searchRetrievalHookStore{Store: s, after: func() {
		if remove {
			err = s.DeleteNotebook(ctx, ws, n.ID, 1)
		} else {
			_, err = s.SaveNotebook(ctx, ws, n.ID, NotebookInput{Name: "Changed", Kind: "notebook", Cells: []Cell{}, Revision: 1})
		}
		if err != nil {
			t.Fatal(err)
		}
	}}
	h := searchHandler(wrapped, p, []SearchBinding{b}, nil, &fakeSearchEmbedder{})
	if hits := searchHits(t, h, ws, b.ID, "Revenue", "lexical"); len(hits) != 0 {
		t.Fatal("revision changed after ranking but stale content returned")
	}
}
func TestSearchRetrievalRechecksCurrentSource(t *testing.T) {
	for _, remove := range []bool{false, true} {
		name := "edit"
		if remove {
			name = "delete"
		}
		t.Run("memory/"+name, func(t *testing.T) { exerciseSearchRetrievalRace(t, NewMemoryStore(), "default", remove) })
		t.Run("postgres/"+name, func(t *testing.T) { s, ws := searchTestStore(t); exerciseSearchRetrievalRace(t, s, ws, remove) })
	}
}
func TestSearchJobReceiptRequiresCurrentCatalogGrant(t *testing.T) {
	s := NewMemoryStore()
	b := searchBinding()
	catalog := catalogBinding()
	b.Sources.Catalogs = []SearchCatalogGrant{{BindingID: catalog.ID, Tables: catalog.AllowedSubjects[0].Tables}}
	selection := SearchCatalogSelection{BindingID: catalog.ID, Identifier: catalog.AllowedSubjects[0].Tables[0]}
	job := searchIndex(t, s, b, subjectFor(executionPrincipal()), SearchIndexInput{Catalog: &selection})
	catalog.AllowedSubjects = []CatalogSubject{}
	h := searchHandler(s, executionPrincipal(), []SearchBinding{b}, []CatalogBinding{catalog}, &fakeSearchEmbedder{})
	r := request(h, "GET", "/api/v1/workspaces/default/search/index-jobs/"+job.ID, "")
	if r.Code != 403 || strings.Contains(r.Body.String(), "orders") {
		t.Fatal("revoked catalog identifier leaked via receipt", r.Code, r.Body)
	}
}
func TestSearchPostgresLeaseExpiresWhileWorkspaceLocked(t *testing.T) {
	s, ws := searchTestStore(t)
	ctx := context.Background()
	b := searchBinding()
	b.WorkspaceID = ws
	n, _ := s.SaveNotebook(ctx, ws, "", input())
	job := searchIndex(t, s, b, subjectFor(executionPrincipal()), SearchIndexInput{Notebook: &SearchNotebookSelection{n.ID, 1}})
	work, lease, err := s.AcquireSearchJob(ctx, "expiring-worker", 200*time.Millisecond)
	if err != nil || work.Job.ID != job.ID {
		t.Fatal("cannot acquire fixture job", err)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SELECT id FROM dataslate_workspaces WHERE id=$1 FOR UPDATE", ws); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.FinishSearchJob(ctx, lease, "failed", "expired worker", nil, 0) }()
	time.Sleep(350 * time.Millisecond)
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-done; !errors.Is(err, ErrSearchLease) {
		t.Fatal("transaction-start now allowed expired lease after lock wait", err)
	}
	_, next, err := s.AcquireSearchJob(ctx, "replacement", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.FinishSearchJob(ctx, next, "failed", "fixture cleaned", nil, 0); err != nil {
		t.Fatal(err)
	}
}
func exerciseSearchAdmittedSourcePriority(t *testing.T, s Store, ws string) {
	ctx := context.Background()
	b := searchBinding()
	b.WorkspaceID = ws
	b.MaxDocuments = 1
	owner := subjectFor(executionPrincipal())
	a, err := s.SaveNotebook(ctx, ws, "", input())
	if err != nil {
		t.Fatal(err)
	}
	z, err := s.SaveNotebook(ctx, ws, "", input())
	if err != nil {
		t.Fatal(err)
	}
	if a.ID > z.ID {
		a, z = z, a
	}
	searchIndex(t, s, b, owner, SearchIndexInput{Notebook: &SearchNotebookSelection{z.ID, 1}})
	controller := &SearchController{Store: s, Config: Config{SearchBindings: []SearchBinding{b}}, Embedder: &fakeSearchEmbedder{}}
	if _, err = controller.ReconcileOnce(ctx, "worker"); err != nil {
		t.Fatal(err)
	}
	_, err = s.SaveNotebook(ctx, ws, z.ID, NotebookInput{Name: "Admitted updated source", Kind: "notebook", Cells: []Cell{}, Revision: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err = controller.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = controller.ReconcileOnce(ctx, "worker"); err != nil {
		t.Fatal(err)
	}
	scope := SearchScope{WorkspaceID: ws, BindingID: b.ID, PolicyHash: b.Hash(), Notebooks: true, Catalogs: map[string]string{}}
	hits, err := s.Search(ctx, scope, SearchRequest{BindingID: b.ID, Query: "Admitted", Mode: "lexical", Limit: 20}, nil)
	if err != nil || len(hits) != 1 || hits[0].Notebook.ID != z.ID || hits[0].Notebook.Revision != 2 {
		t.Fatal("lower new ID starved admitted source update", err, len(hits))
	}
}
func TestSearchAdmittedSourcePriority(t *testing.T) {
	t.Run("memory", func(t *testing.T) { exerciseSearchAdmittedSourcePriority(t, NewMemoryStore(), "default") })
	t.Run("postgres", func(t *testing.T) { s, ws := searchTestStore(t); exerciseSearchAdmittedSourcePriority(t, s, ws) })
}
