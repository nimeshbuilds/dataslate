package platform

import (
	"context"
	"dataslate.local/api/internal/trino"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func sqlBinding() SQLBinding {
	return SQLBinding{ID: "governed-sql", WorkspaceID: "default", Name: "Governed SQL", SQLProfile: SQLProfile{SQLContext: SQLContext{Engine: "trino", PolicyVersion: "v1", Catalog: "lake", Schema: "analytics", MaxRunSeconds: 300}, Endpoint: "https://trino.example.com", TokenFile: "/var/run/secrets/trino/token", ReadOnlyPolicyAccepted: true}, AllowedSubjects: []SQLSubject{{ExecutionSubject: subjectFor(executionPrincipal()), EngineUser: "engine-alice"}}}
}
func sqlNotebook() NotebookInput {
	return NotebookInput{Name: "Orders SQL", Kind: "sql", Cells: []Cell{{ID: "query-cell", Language: "sql", Source: "SELECT id, amount FROM orders"}}}
}
func createTestQuery(t *testing.T, s Store, ws string) (Query, SQLPolicy) {
	t.Helper()
	b := sqlBinding()
	b.WorkspaceID = ws
	n, e := s.SaveNotebook(context.Background(), ws, "", sqlNotebook())
	if e != nil {
		t.Fatal(e)
	}
	policy, _ := b.Policy(b.AllowedSubjects[0].ExecutionSubject)
	q, _, e := s.CreateQuery(context.Background(), newQuery(ws, b, b.AllowedSubjects[0].ExecutionSubject, SQLSelection{ID: n.ID, Revision: n.Revision, CellID: n.Cells[0].ID}), policy, newResource("key-").ID, "fixture")
	if e != nil {
		t.Fatal(e)
	}
	return q, policy
}
func queryHandler(store Store, p Principal, bindings []SQLBinding) http.Handler {
	return (&Server{Config: Config{AuthMode: "oidc", SQLBindings: bindings}, Store: store, Auth: testAuth{p: p}}).Handler()
}
func queryDue(s *MemoryStore, id string) {
	s.mu.Lock()
	s.queries[id].Next = time.Time{}
	s.mu.Unlock()
}
func exerciseQuerySnapshot(t *testing.T, s Store, ws string) {
	ctx := context.Background()
	b := sqlBinding()
	b.WorkspaceID = ws
	n, e := s.SaveNotebook(ctx, ws, "", sqlNotebook())
	if e != nil {
		t.Fatal(e)
	}
	owner := b.AllowedSubjects[0].ExecutionSubject
	policy, _ := b.Policy(owner)
	in := QueryInput{BindingID: b.ID, IdempotencyKey: "exact-query-key", Notebook: SQLSelection{ID: n.ID, Revision: 1, CellID: "query-cell"}}
	var wg sync.WaitGroup
	errorsOut := make(chan error, 8)
	queries := make(chan Query, 8)
	for i := 0; i < 8; i++ {
		wg.Go(func() {
			q, _, e := s.CreateQuery(ctx, newQuery(ws, b, owner, in.Notebook), policy, in.IdempotencyKey, queryFingerprint(in))
			errorsOut <- e
			queries <- q
		})
	}
	wg.Wait()
	close(errorsOut)
	close(queries)
	for e := range errorsOut {
		if e != nil {
			t.Fatal(e)
		}
	}
	var q Query
	for next := range queries {
		if q.ID != "" && q.ID != next.ID {
			t.Fatal("idempotency race")
		}
		q = next
	}
	source, e := s.QuerySource(ctx, ws, q.ID, owner)
	if e != nil || source.Source != n.Cells[0].Source || source.Notebook.SHA256 != q.Notebook.SHA256 {
		t.Fatal("SQL snapshot", e)
	}
	other := owner
	other.Issuer = "https://other.example.com"
	if _, e = s.QueryResult(ctx, ws, q.ID, other); !errors.Is(e, ErrNotFound) {
		t.Fatal("cross issuer result leak", e)
	}
	if _, e = s.QuerySource(ctx, "different", q.ID, owner); !errors.Is(e, ErrNotFound) {
		t.Fatal("cross workspace source leak")
	}
	edit := sqlNotebook()
	edit.Revision = 1
	edit.Cells[0].Source = "SELECT secret FROM changed"
	if _, e = s.SaveNotebook(ctx, ws, n.ID, edit); e != nil {
		t.Fatal(e)
	}
	if _, _, e = s.CreateQuery(ctx, newQuery(ws, b, owner, in.Notebook), policy, "new-stale-query-key", "new"); !errors.Is(e, ErrSQLRevision) {
		t.Fatal("stale SQL accepted", e)
	}
	if e = s.DeleteNotebook(ctx, ws, n.ID, 2); e != nil {
		t.Fatal(e)
	}
	replay, replayed, e := s.CreateQuery(ctx, newQuery(ws, b, owner, in.Notebook), policy, in.IdempotencyKey, queryFingerprint(in))
	if e != nil || !replayed || replay.ID != q.ID {
		t.Fatal("replay after source deletion", e)
	}
	if _, _, e = s.CreateQuery(ctx, newQuery(ws, b, owner, in.Notebook), policy, in.IdempotencyKey, "different"); !errors.Is(e, ErrConflict) {
		t.Fatal("key conflict accepted")
	}
	source, e = s.QuerySource(ctx, ws, q.ID, owner)
	if e != nil || source.Source != n.Cells[0].Source {
		t.Fatal("source mutated", e)
	}
}
func TestMemorySQLSnapshotAndReplay(t *testing.T) {
	exerciseQuerySnapshot(t, NewMemoryStore(), "default")
}
func TestPostgresSQLSnapshotAndReplay(t *testing.T) {
	dsn := os.Getenv("DATASLATE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set isolated DATASLATE_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	s, e := OpenPostgres(ctx, dsn, false)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if e = s.Migrate(ctx); e != nil {
		t.Fatal(e)
	}
	ws, e := s.CreateWorkspace(ctx, "SQL tests", newResource("test-").ID)
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		s.pool.Exec(ctx, `DELETE FROM dataslate_queries WHERE workspace_id=$1`, ws.ID)
		s.pool.Exec(ctx, `DELETE FROM dataslate_notebooks WHERE workspace_id=$1`, ws.ID)
		s.pool.Exec(ctx, `DELETE FROM dataslate_workspaces WHERE id=$1`, ws.ID)
	}()
	exerciseQuerySnapshot(t, s, ws.ID)
	q, _ := createTestQuery(t, s, ws.ID)
	work, lease, e := s.AcquireQuery(ctx, "first", 30*time.Second)
	if e != nil {
		t.Fatal(e)
	}
	result := emptyQueryResult(work.Query)
	result, err := appendQueryPage(result, trino.Page{Columns: []trino.Column{{Name: "large", Type: "bigint"}}, Rows: [][]json.RawMessage{{json.RawMessage("9223372036854775807")}}})
	if err != nil {
		t.Fatal(err)
	}
	cursor := trino.Cursor{ID: "engine-query", NextURI: "https://trino.example.com/v1/statement/engine-query/1"}
	_, e = s.AdvanceQuery(ctx, lease, QueryUpdate{State: "running", Message: "first page", EngineQueryID: cursor.ID, SubmissionStarted: true, Cursor: &cursor, Processed: []string{"pagehash"}, Result: &result})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.AdvanceQuery(ctx, lease, QueryUpdate{State: "failed", Message: "late worker"}); !errors.Is(e, ErrLeaseLost) {
		t.Fatal("lease reuse", e)
	}
	read, e := s.QueryResult(ctx, ws.ID, work.Query.ID, work.Query.SubmittedBy)
	if e != nil || *read.Rows[0][0] != "9223372036854775807" {
		t.Fatal("durable exact result", e)
	}
	_ = q
}

type fakeSQLBackend struct {
	allowed                           bool
	authError                         error
	submitError                       error
	nextError                         error
	cancelError                       error
	submitted, nextCalls, cancelCalls int
	pages                             []trino.Page
	nextCursors                       []string
}

func (b *fakeSQLBackend) Authorize(context.Context, Query, SQLPolicy) (bool, error) {
	return b.allowed, b.authError
}
func (b *fakeSQLBackend) Submit(context.Context, QueryWork) (trino.Page, error) {
	b.submitted++
	if b.submitError != nil {
		return trino.Page{}, b.submitError
	}
	p := b.pages[0]
	b.pages = b.pages[1:]
	return p, nil
}
func (b *fakeSQLBackend) Next(_ context.Context, w QueryWork) (trino.Page, error) {
	b.nextCalls++
	b.nextCursors = append(b.nextCursors, w.Cursor.NextURI)
	if b.nextError != nil {
		return trino.Page{}, b.nextError
	}
	p := b.pages[0]
	b.pages = b.pages[1:]
	return p, nil
}
func (b *fakeSQLBackend) Cancel(context.Context, QueryWork) error {
	b.cancelCalls++
	return b.cancelError
}
func queryPage(next string, rows ...string) trino.Page {
	out := trino.Page{Cursor: trino.Cursor{ID: "engine-query", NextURI: next}, Columns: []trino.Column{{Name: "id", Type: "bigint"}}}
	for _, row := range rows {
		out.Rows = append(out.Rows, []json.RawMessage{json.RawMessage(row)})
	}
	return out
}
func TestQueryReconcilesPagesExactlyAndKeepsLossUnknown(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	q, _ := createTestQuery(t, s, "default")
	uri := "https://trino.example.com/v1/statement/engine-query/1"
	b := &fakeSQLBackend{allowed: true, pages: []trino.Page{queryPage(uri, "9223372036854775807"), queryPage("", "2")}}
	if _, e := ReconcileQueryOnce(ctx, s, b, "worker"); e != nil {
		t.Fatal(e)
	}
	queryDue(s, q.ID)
	b.nextError = errors.New("lost page")
	if _, e := ReconcileQueryOnce(ctx, s, b, "worker"); e != nil {
		t.Fatal(e)
	}
	partial, _ := s.QueryResult(ctx, "default", q.ID, q.SubmittedBy)
	if partial.RowCount != 1 || partial.Complete {
		t.Fatal("lost page fabricated completion")
	}
	queryDue(s, q.ID)
	b.nextError = nil
	if _, e := ReconcileQueryOnce(ctx, s, b, "worker"); e != nil {
		t.Fatal(e)
	}
	done, _ := s.Query(ctx, "default", q.ID, q.SubmittedBy)
	result, _ := s.QueryResult(ctx, "default", q.ID, q.SubmittedBy)
	if done.State != "succeeded" || !result.Complete || result.RowCount != 2 || *result.Rows[0][0] != "9223372036854775807" || b.submitted != 1 || len(b.nextCursors) != 2 || b.nextCursors[0] != b.nextCursors[1] {
		t.Fatal("incorrect page recovery", done, result)
	}
	s = NewMemoryStore()
	q, _ = createTestQuery(t, s, "default")
	b = &fakeSQLBackend{allowed: true, submitError: errors.New("lost POST")}
	for i := 0; i < 3; i++ {
		queryDue(s, q.ID)
		if _, e := ReconcileQueryOnce(ctx, s, b, "worker"); e != nil {
			t.Fatal(e)
		}
	}
	q, _ = s.Query(ctx, "default", q.ID, q.SubmittedBy)
	if q.State != "unknown" || b.submitted != 1 {
		t.Fatal("blind resubmit", q, b.submitted)
	}
}
func TestQueryCancellationAcceptedIsNotCompletion(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	q, _ := createTestQuery(t, s, "default")
	uri := "https://trino.example.com/v1/statement/engine-query/1"
	b := &fakeSQLBackend{allowed: true, pages: []trino.Page{queryPage(uri)}}
	ReconcileQueryOnce(ctx, s, b, "worker")
	s.CancelQuery(ctx, "default", q.ID, q.SubmittedBy)
	b.nextError = errors.New("410")
	if _, e := ReconcileQueryOnce(ctx, s, b, "worker"); e != nil {
		t.Fatal(e)
	}
	q, _ = s.Query(ctx, "default", q.ID, q.SubmittedBy)
	if q.State != "cancel_requested" || !q.CancelAccepted || q.Terminal() {
		t.Fatal("DELETE acceptance fabricated terminal", q)
	}
	queryDue(s, q.ID)
	b.nextError = nil
	b.pages = []trino.Page{{Cursor: trino.Cursor{ID: "engine-query"}, Error: &trino.QueryError{Name: "USER_CANCELED"}}}
	if _, e := ReconcileQueryOnce(ctx, s, b, "worker"); e != nil {
		t.Fatal(e)
	}
	q, _ = s.Query(ctx, "default", q.ID, q.SubmittedBy)
	if q.State != "cancelled" || b.cancelCalls != 1 {
		t.Fatal("terminal cancellation", q)
	}
}
func TestQueryLimitsAndTypedScalars(t *testing.T) {
	q := Query{Resource: Resource{ID: "qry-test"}, WorkspaceID: "default"}
	for _, raw := range []string{"9223372036854775807", "0.1234567890123456789012345", "true", "\"2026-11-07\"", "null"} {
		v, e := scalarCell(json.RawMessage(raw))
		if e != nil {
			t.Fatal(e)
		}
		if raw != "null" && v == nil {
			t.Fatal("scalar missing")
		}
	}
	for _, raw := range []string{"{}", "[]", "NaN"} {
		if _, e := scalarCell(json.RawMessage(raw)); e == nil {
			t.Fatal("complex coerced", raw)
		}
	}
	page := queryPage("")
	for i := 0; i < MaxQueryRows+1; i++ {
		page.Rows = append(page.Rows, []json.RawMessage{json.RawMessage("1")})
	}
	result, e := appendQueryPage(emptyQueryResult(q), page)
	if e != nil || !result.Truncated || result.RowCount != MaxQueryRows || result.Reason != "row_limit" {
		t.Fatal("row bound", e)
	}
	large, _ := json.Marshal(strings.Repeat("a", MaxQueryBytes))
	page = trino.Page{Columns: []trino.Column{{Name: "value", Type: "varchar"}}, Rows: [][]json.RawMessage{{large}}}
	result, e = appendQueryPage(emptyQueryResult(q), page)
	if e != nil || !result.Truncated || result.RowCount != 0 || result.Bytes > MaxQueryBytes {
		t.Fatal("byte bound", e)
	}
	page = trino.Page{Columns: []trino.Column{{Name: "array", Type: "array(bigint)"}}}
	if _, e = appendQueryPage(emptyQueryResult(q), page); e == nil {
		t.Fatal("complex type accepted")
	}
}
func TestQueryOutputRequiresCurrentPolicyAndOpenAPI(t *testing.T) {
	s := NewMemoryStore()
	b := sqlBinding()
	q, _ := createTestQuery(t, s, "default")
	h := queryHandler(s, executionPrincipal(), []SQLBinding{b})
	raw, _ := os.ReadFile("../../openapi.yaml")
	var doc any
	json.Unmarshal(raw, &doc)
	compiler := jsonschema.NewCompiler()
	compiler.AddResource("contract.json", doc)
	for _, tc := range []struct{ suffix, schema string }{{"", "Query"}, {"/source", "QuerySource"}, {"/result", "QueryResult"}} {
		response := request(h, "GET", "/api/v1/workspaces/default/queries/"+q.ID+tc.suffix, "")
		if response.Code != 200 {
			t.Fatal(response.Code, response.Body)
		}
		schema, e := compiler.Compile("contract.json#/components/schemas/" + tc.schema)
		if e != nil {
			t.Fatal(e)
		}
		var body any
		json.Unmarshal(response.Body.Bytes(), &body)
		if e = schema.Validate(body); e != nil {
			t.Fatal(e)
		}
		if strings.Contains(response.Body.String(), b.Endpoint) || strings.Contains(response.Body.String(), b.TokenFile) || strings.Contains(response.Body.String(), b.AllowedSubjects[0].EngineUser) {
			t.Fatal("private policy leaked")
		}
	}
	b.AllowedSubjects = []SQLSubject{}
	revoked := queryHandler(s, executionPrincipal(), []SQLBinding{b})
	for _, suffix := range []string{"/source", "/result"} {
		if response := request(revoked, "GET", "/api/v1/workspaces/default/queries/"+q.ID+suffix, ""); response.Code != 403 {
			t.Fatal("revoked output access", response.Code)
		}
	}
	other := executionPrincipal()
	other.Subject = "other"
	if response := request(queryHandler(s, other, []SQLBinding{sqlBinding()}), "GET", "/api/v1/workspaces/default/queries/"+q.ID+"/result", ""); response.Code != 404 {
		t.Fatal("owner bypass")
	}
	file := filepath.Join(t.TempDir(), "bindings.json")
	bindings, _ := json.Marshal([]SQLBinding{sqlBinding()})
	os.WriteFile(file, bindings, 0600)
	server := &Server{Config: Config{AuthMode: "oidc", SQLBindingsFile: file}, Store: s, Auth: testAuth{p: executionPrincipal()}}
	if got := request(server.Handler(), "GET", "/api/v1/workspaces/default/queries/"+q.ID+"/result", ""); got.Code != 200 {
		t.Fatal(got.Body)
	}
	os.WriteFile(file, []byte("invalid"), 0600)
	if got := request(server.Handler(), "GET", "/api/v1/workspaces/default/queries/"+q.ID+"/result", ""); got.Code != 503 {
		t.Fatal("malformed currentpolicy didnotfailclosed")
	}
}
func TestSQLHTTPExactSelectionAndRoles(t *testing.T) {
	s := NewMemoryStore()
	n, _ := s.SaveNotebook(context.Background(), "default", "", sqlNotebook())
	body := fmt.Sprintf(`{"bindingId":"governed-sql","idempotencyKey":"http-sql-query-key","notebook":{"id":%q,"revision":1,"cellId":"query-cell"}}`, n.ID)
	p := executionPrincipal()
	viewer := p
	viewer.Workspaces = map[string]string{"default": "viewer"}
	for _, tc := range []struct {
		p        Principal
		bindings []SQLBinding
		want     int
	}{{p, []SQLBinding{sqlBinding()}, 202}, {viewer, []SQLBinding{sqlBinding()}, 403}, {p, nil, 501}} {
		response := request(queryHandler(s, tc.p, tc.bindings), "POST", "/api/v1/workspaces/default/queries", body)
		if response.Code != tc.want {
			t.Fatal(response.Code, response.Body)
		}
	}
}

type fakeSQLStatusBackend struct {
	fakeSQLBackend
	status      trino.QueryStatus
	statusError error
}

func (b *fakeSQLStatusBackend) ObserveQuery(context.Context, QueryWork) (trino.QueryStatus, error) {
	return b.status, b.statusError
}
func TestQueryRetiredCursorUsesVerifiedTerminalState(t *testing.T) {
	for _, tc := range []struct {
		name, state, errorName, want string
		cancel                       bool
	}{{"cancelled", "FAILED", "USER_CANCELED", "cancelled", true}, {"finished without pages", "FINISHED", "", "succeeded", false}, {"running remains unknown", "RUNNING", "", "unknown", false}, {"engine failure", "FAILED", "PERMISSION_DENIED", "failed", false}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			s := NewMemoryStore()
			q, _ := createTestQuery(t, s, "default")
			b := &fakeSQLStatusBackend{fakeSQLBackend: fakeSQLBackend{allowed: true, pages: []trino.Page{queryPage("https://trino.example.com/v1/statement/engine-query/1", "1")}}, status: trino.QueryStatus{ID: "engine-query", State: tc.state, ErrorName: tc.errorName}}
			if _, e := ReconcileQueryOnce(ctx, s, b, "worker"); e != nil {
				t.Fatal(e)
			}
			if tc.cancel {
				s.CancelQuery(ctx, "default", q.ID, q.SubmittedBy)
			}
			queryDue(s, q.ID)
			b.nextError = errors.New("cursor retired")
			if _, e := ReconcileQueryOnce(ctx, s, b, "worker"); e != nil {
				t.Fatal(e)
			}
			q, _ = s.Query(ctx, "default", q.ID, q.SubmittedBy)
			result, _ := s.QueryResult(ctx, "default", q.ID, q.SubmittedBy)
			if q.State != tc.want || result.Complete {
				t.Fatal("invented completion or lost terminal evidence", q, result)
			}
			if tc.state == "FINISHED" && (!result.Truncated || result.Reason != "result_unavailable") {
				t.Fatal("lost pages presented as complete")
			}
		})
	}
}
func TestQueryDataCursorCycleNeverDuplicatesRows(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	q, _ := createTestQuery(t, s, "default")
	a := "https://trino.example.com/v1/statement/engine-query/1"
	buri := "https://trino.example.com/v1/statement/engine-query/2"
	b := &fakeSQLBackend{allowed: true, pages: []trino.Page{queryPage(a, "1"), queryPage(buri, "2"), queryPage(a, "3")}}
	for i := 0; i < 3; i++ {
		queryDue(s, q.ID)
		if _, e := ReconcileQueryOnce(ctx, s, b, "worker"); e != nil {
			t.Fatal(e)
		}
	}
	q, _ = s.Query(ctx, "default", q.ID, q.SubmittedBy)
	result, _ := s.QueryResult(ctx, "default", q.ID, q.SubmittedBy)
	if q.State != "cancel_requested" || result.RowCount != 2 || result.Complete {
		t.Fatal("cycle duplicated data", q, result)
	}
}
func TestQueryRevocationAndDeadlinePreventNewPost(t *testing.T) {
	for _, reason := range []string{"revoked", "deadline", "corrupt source"} {
		t.Run(reason, func(t *testing.T) {
			s := NewMemoryStore()
			q, _ := createTestQuery(t, s, "default")
			b := &fakeSQLBackend{allowed: reason != "revoked"}
			s.mu.Lock()
			if reason == "deadline" {
				s.queries[q.ID].Work.Query.CreatedAt = time.Now().Add(-time.Hour)
			}
			if reason == "corrupt source" {
				s.queries[q.ID].Work.Source.Source = "SELECT wrong"
			}
			s.mu.Unlock()
			if _, e := ReconcileQueryOnce(context.Background(), s, b, "worker"); e != nil {
				t.Fatal(e)
			}
			q, _ = s.Query(context.Background(), "default", q.ID, q.SubmittedBy)
			if b.submitted != 0 || reason != "corrupt source" && q.State != "cancelled" || reason == "corrupt source" && q.State != "unknown" {
				t.Fatal("unsafe POST", q, b.submitted)
			}
		})
	}
}

func TestSQLControllerUsesApprovedEngineIdentityAndImmutableSource(t *testing.T) {
	ctx := context.Background()
	var endpoint string
	posts, gets := 0, 0
	expectedSQL := "SELECT id, amount FROM orders"
	engine := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer engine-token" || r.Header.Get("X-Trino-User") != "engine-alice" {
			t.Error("wrong engineidentity/credential")
		}
		w.Header().Set("Content-Type", "application/json")
		if r.Method == "POST" {
			posts++
			body, _ := io.ReadAll(r.Body)
			if string(body) != expectedSQL || r.Header.Get("X-Trino-Catalog") != "lake" || r.Header.Get("X-Trino-Schema") != "analytics" {
				t.Error("unapproved statementcontext")
			}
			json.NewEncoder(w).Encode(queryPage(endpoint+"/v1/statement/engine-query/1", "9223372036854775807"))
			return
		}
		if r.Method == "GET" {
			gets++
			json.NewEncoder(w).Encode(queryPage("", "2"))
			return
		}
		w.WriteHeader(500)
	}))
	defer engine.Close()
	endpoint = engine.URL
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.pem")
	token := filepath.Join(dir, "token")
	os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: engine.Certificate().Raw}), 0600)
	os.WriteFile(token, []byte("engine-token"), 0600)
	b := sqlBinding()
	b.Endpoint = endpoint
	b.CAFile = ca
	b.TokenFile = token
	s := NewMemoryStore()
	n, e := s.SaveNotebook(ctx, "default", "", sqlNotebook())
	if e != nil {
		t.Fatal(e)
	}
	policy, _ := b.Policy(b.AllowedSubjects[0].ExecutionSubject)
	q, _, e := s.CreateQuery(ctx, newQuery("default", b, b.AllowedSubjects[0].ExecutionSubject, SQLSelection{ID: n.ID, Revision: 1, CellID: "query-cell"}), policy, "tls-engine-intent", "tls")
	if e != nil {
		t.Fatal(e)
	}
	if e = s.DeleteNotebook(ctx, "default", n.ID, 1); e != nil {
		t.Fatal(e)
	}
	backend := &FileSQLBackend{Bindings: []SQLBinding{b}}
	for i := 0; i < 2; i++ {
		queryDue(s, q.ID)
		if _, e = ReconcileQueryOnce(ctx, s, backend, "worker"); e != nil {
			t.Fatal(e)
		}
	}
	result, e := s.QueryResult(ctx, "default", q.ID, q.SubmittedBy)
	if e != nil || !result.Complete || result.RowCount != 2 || posts != 1 || gets != 1 {
		t.Fatal("integrated query lifecycle", e, result, posts, gets)
	}
	b.PolicyVersion = "v2"
	backend.Bindings = []SQLBinding{b}
	if allowed, e := backend.Authorize(ctx, q, policy); e != nil || allowed {
		t.Fatal("oldresult policyversion remainsauthorized", e)
	}
}
