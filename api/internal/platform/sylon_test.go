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
	"sync"
	"testing"
	"time"

	"dataslate.local/api/internal/llm"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

type fakeSylonGenerator struct {
	calls  int
	hook   func()
	result llm.Result
	err    error
}

func (f *fakeSylonGenerator) Generate(_ context.Context, _ llm.Config, _ llm.Request) (llm.Result, error) {
	f.calls++
	if f.hook != nil {
		f.hook()
	}
	return f.result, f.err
}
func sylonFixture(t *testing.T, s Store, ws string) (SylonBinding, SylonConversation, Notebook) {
	t.Helper()
	b := contextSylonBinding()
	b.WorkspaceID = ws
	n, e := s.SaveNotebook(context.Background(), ws, "", input())
	if e != nil {
		t.Fatal(e)
	}
	c, e := s.CreateSylonConversation(context.Background(), SylonConversation{Resource: newResource("chat-"), WorkspaceID: ws, BindingID: b.ID, Owner: b.AllowedSubjects[0], Title: "Private assistant"})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = s.DeleteSylonConversation(context.Background(), ws, c.ID, c.Owner) })
	return b, c, n
}
func sylonEnqueue(t *testing.T, s Store, b SylonBinding, c SylonConversation, selection *SQLSelection) SylonJob {
	t.Helper()
	in := SylonMessageInput{IdempotencyKey: newResource("intent-").ID, Prompt: "Explain and improve this code", Notebook: selection, Catalogs: []SearchCatalogSelection{}}
	j, _, e := s.CreateSylonJob(context.Background(), SylonWork{Job: newSylonJob(c, b, in), PolicyHash: b.Hash()}, in.IdempotencyKey, sylonFingerprint(in))
	if e != nil {
		t.Fatal(e)
	}
	return j
}
func sylonSelection(n Notebook) *SQLSelection {
	return &SQLSelection{ID: n.ID, Revision: n.Revision, CellID: n.Cells[0].ID}
}
func sylonRead(t *testing.T, s Store, j SylonJob) SylonWork {
	t.Helper()
	w, e := s.SylonWork(context.Background(), j.WorkspaceID, j.ID, j.SubmittedBy)
	if e != nil {
		t.Fatal(e)
	}
	return w
}
func forSylonStores(t *testing.T, fn func(*testing.T, Store, string)) {
	t.Helper()
	t.Run("memory", func(t *testing.T) { fn(t, NewMemoryStore(), "default") })
	t.Run("postgres", func(t *testing.T) { s, ws := searchTestStore(t); fn(t, s, ws) })
}
func TestSylonDurableIdempotencyOwnershipAndApply(t *testing.T) {
	forSylonStores(t, func(t *testing.T, s Store, ws string) {
		ctx := context.Background()
		b, c, n := sylonFixture(t, s, ws)
		in := SylonMessageInput{IdempotencyKey: "exact-assistant-key", Prompt: "Improve this saved cell", Notebook: sylonSelection(n), Catalogs: []SearchCatalogSelection{}}
		var wg sync.WaitGroup
		jobs := make(chan SylonJob, 8)
		errs := make(chan error, 8)
		for i := 0; i < 8; i++ {
			wg.Go(func() {
				j, _, e := s.CreateSylonJob(ctx, SylonWork{Job: newSylonJob(c, b, in), PolicyHash: b.Hash()}, in.IdempotencyKey, sylonFingerprint(in))
				jobs <- j
				errs <- e
			})
		}
		wg.Wait()
		close(jobs)
		close(errs)
		for e := range errs {
			if e != nil {
				t.Fatal(e)
			}
		}
		var j SylonJob
		for next := range jobs {
			if j.ID != "" && next.ID != j.ID {
				t.Fatal("duplicate job")
			}
			j = next
		}
		if _, _, e := s.CreateSylonJob(ctx, SylonWork{Job: newSylonJob(c, b, in), PolicyHash: b.Hash()}, in.IdempotencyKey, "changed"); !errors.Is(e, ErrConflict) {
			t.Fatal("key conflict", e)
		}
		if _, _, e := s.CreateSylonJob(ctx, SylonWork{Job: newSylonJob(c, b, in), PolicyHash: b.Hash()}, "another-key", "changed"); !errors.Is(e, ErrSylonCapacity) {
			t.Fatal("concurrent conversation turn", e)
		}
		foreign := j.SubmittedBy
		foreign.Issuer = "https://foreign.example.com"
		if _, e := s.SylonWork(ctx, ws, j.ID, foreign); !errors.Is(e, ErrNotFound) {
			t.Fatal("cross issuer job", e)
		}
		if _, e := s.SylonHistory(ctx, "foreign", c.ID, c.Owner); !errors.Is(e, ErrNotFound) {
			t.Fatal("cross workspace history", e)
		}
		gen := &fakeSylonGenerator{result: llm.Result{Answer: "Proposal", Citations: []string{}, ProposedCode: &llm.Code{Language: "python", Source: "print('improved')"}}}
		controller := SylonController{Config: Config{SylonBindings: []SylonBinding{b}}, Store: s, Generator: gen}
		if _, e := controller.ReconcileOnce(ctx, "worker"); e != nil {
			t.Fatal(e)
		}
		work := sylonRead(t, s, j)
		if work.Job.State != "succeeded" || !work.Job.DispatchStarted || len(work.Context.Dependencies) != 1 || gen.calls != 1 {
			t.Fatalf("wrong result %+v", work.Job)
		}
		before, _ := s.Notebook(ctx, ws, n.ID)
		if before.Revision != 1 || before.Cells[0].Source != n.Cells[0].Source {
			t.Fatal("generation wrote notebook")
		}
		apply := SylonApplyInput{NotebookID: n.ID, Revision: 1, CellID: n.Cells[0].ID}
		var receipt SylonApplyReceipt
		receipts := make(chan SylonApplyReceipt, 8)
		errs = make(chan error, 8)
		for i := 0; i < 8; i++ {
			wg.Go(func() { r, e := s.ApplySylonCode(ctx, ws, j.ID, c.Owner, apply); receipts <- r; errs <- e })
		}
		wg.Wait()
		close(receipts)
		close(errs)
		for e := range errs {
			if e != nil {
				t.Fatal(e)
			}
		}
		for r := range receipts {
			if receipt.JobID != "" && receipt != r {
				t.Fatal("non-idempotent apply")
			}
			receipt = r
		}
		after, _ := s.Notebook(ctx, ws, n.ID)
		if after.Revision != 2 || after.Cells[0].Source != "print('improved')" || receipt.PreviousRevision != 1 || receipt.Revision != 2 {
			t.Fatal("atomic apply failed")
		}
		edit := NotebookInput{Name: after.Name, Kind: after.Kind, Cells: after.Cells, Revision: 2}
		edit.Cells[0].Source = "new user work"
		if _, e := s.SaveNotebook(ctx, ws, n.ID, edit); e != nil {
			t.Fatal(e)
		}
		replay, e := s.ApplySylonCode(ctx, ws, j.ID, c.Owner, apply)
		if e != nil || replay != receipt {
			t.Fatal("exact apply recovery failed", e)
		}
		apply.Revision = 3
		if _, e = s.ApplySylonCode(ctx, ws, j.ID, c.Owner, apply); !errors.Is(e, ErrConflict) {
			t.Fatal("different apply accepted", e)
		}
		old, replayed, e := s.CreateSylonJob(ctx, SylonWork{Job: newSylonJob(c, b, in), PolicyHash: b.Hash()}, in.IdempotencyKey, sylonFingerprint(in))
		if e != nil || !replayed || old.ID != j.ID {
			t.Fatal("original message recovery", e)
		}
		history, e := s.Revisions(ctx, ws, n.ID)
		if e != nil || len(history) != 3 {
			t.Fatal("apply history", e)
		}
	})
}
func TestSylonCancelUnknownDeleteAndPublicationRace(t *testing.T) {
	for _, scenario := range []string{"queued-cancel", "dispatch-cancel", "provider-unknown", "deleted-conversation", "edited-source", "publication-race", "policy-revoked"} {
		t.Run(scenario, func(t *testing.T) {
			forSylonStores(t, func(t *testing.T, s Store, ws string) {
				ctx := context.Background()
				b, c, n := sylonFixture(t, s, ws)
				j := sylonEnqueue(t, s, b, c, sylonSelection(n))
				gen := &fakeSylonGenerator{result: llm.Result{Answer: "Must not leak", Citations: []string{}}}
				controller := SylonController{Config: Config{SylonBindings: []SylonBinding{b}}, Store: s, Generator: gen}
				edit := func() {
					in := input()
					in.Revision = 1
					in.Cells[0].Source = "new work"
					if _, e := s.SaveNotebook(ctx, ws, n.ID, in); e != nil {
						t.Fatal(e)
					}
				}
				switch scenario {
				case "queued-cancel":
					if _, e := s.CancelSylonJob(ctx, ws, j.ID, c.Owner); e != nil {
						t.Fatal(e)
					}
				case "dispatch-cancel":
					gen.hook = func() {
						if _, e := s.CancelSylonJob(ctx, ws, j.ID, c.Owner); e != nil {
							t.Fatal(e)
						}
					}
				case "provider-unknown":
					gen.err = errors.New("ambiguous provider POST")
				case "deleted-conversation":
					gen.hook = func() {
						if e := s.DeleteSylonConversation(ctx, ws, c.ID, c.Owner); e != nil {
							t.Fatal(e)
						}
					}
				case "edited-source":
					gen.hook = edit
				case "publication-race":
					controller.Store = &sylonFinishHookStore{Store: s, hook: edit}
				case "policy-revoked":
					gen.hook = func() { controller.Config.SylonBindings[0].AllowedSubjects = []ExecutionSubject{} }
				}
				_, e := controller.ReconcileOnce(ctx, "worker")
				if scenario == "deleted-conversation" {
					if !errors.Is(e, ErrNotFound) {
						t.Fatal(e)
					}
					if _, e = s.SylonWork(ctx, ws, j.ID, c.Owner); !errors.Is(e, ErrNotFound) {
						t.Fatal("late publication")
					}
					return
				}
				if e != nil {
					t.Fatal(e)
				}
				work := sylonRead(t, s, j)
				want := "failed"
				if strings.Contains(scenario, "cancel") {
					want = "cancelled"
				}
				if scenario == "provider-unknown" {
					want = "unknown"
				}
				if work.Job.State != want || work.Job.Result != nil {
					t.Fatalf("%s got %+v", scenario, work.Job)
				}
				if scenario == "queued-cancel" && gen.calls != 0 {
					t.Fatal("cancelled job dispatched")
				}
				calls := gen.calls
				if processed, e := controller.ReconcileOnce(ctx, "restart"); e != nil || processed || gen.calls != calls {
					t.Fatal("terminal job retried", e)
				}
			})
		})
	}
}

type sylonFinishHookStore struct {
	Store
	hook func()
}

func (s *sylonFinishHookStore) FinishSylonJob(ctx context.Context, l SylonLease, state, message string, p *SylonPreparedContext, j *SylonJob) error {
	if s.hook != nil {
		s.hook()
		s.hook = nil
	}
	return s.Store.FinishSylonJob(ctx, l, state, message, p, j)
}
func TestSylonExpiredDispatchNeverReplays(t *testing.T) {
	forSylonStores(t, func(t *testing.T, s Store, ws string) {
		ctx := context.Background()
		b, c, _ := sylonFixture(t, s, ws)
		j := sylonEnqueue(t, s, b, c, nil)
		_, lease, e := s.AcquireSylonJob(ctx, "old", 30*time.Millisecond)
		if e != nil {
			t.Fatal(e)
		}
		if e = s.MarkSylonDispatch(ctx, lease, "generating"); e != nil {
			t.Fatal(e)
		}
		time.Sleep(40 * time.Millisecond)
		gen := &fakeSylonGenerator{}
		controller := SylonController{Config: Config{SylonBindings: []SylonBinding{b}}, Store: s, Generator: gen}
		if _, e = controller.ReconcileOnce(ctx, "new"); e != nil {
			t.Fatal(e)
		}
		if w := sylonRead(t, s, j); w.Job.State != "unknown" || gen.calls != 0 {
			t.Fatal("ambiguous dispatch replayed")
		}
		if e = s.FinishSylonJob(ctx, lease, "succeeded", "stale", nil, &SylonJob{Result: &llm.Result{Answer: "stale"}}); !errors.Is(e, ErrSylonLease) {
			t.Fatal("stale lease published", e)
		}
	})
}
func TestSylonHistoryPhysicalMessageCap(t *testing.T) {
	forSylonStores(t, func(t *testing.T, s Store, ws string) {
		ctx := context.Background()
		b, c, _ := sylonFixture(t, s, ws)
		gen := &fakeSylonGenerator{result: llm.Result{Answer: "answer", Citations: []string{}}}
		controller := SylonController{Config: Config{SylonBindings: []SylonBinding{b}}, Store: s, Generator: gen}
		for i := 0; i < 10; i++ {
			sylonEnqueue(t, s, b, c, nil)
			if _, e := controller.ReconcileOnce(ctx, "worker"); e != nil {
				t.Fatal(e)
			}
		}
		in := SylonMessageInput{IdempotencyKey: "over-history", Prompt: "One more", Catalogs: []SearchCatalogSelection{}}
		if _, _, e := s.CreateSylonJob(ctx, SylonWork{Job: newSylonJob(c, b, in), PolicyHash: b.Hash()}, in.IdempotencyKey, sylonFingerprint(in)); !errors.Is(e, ErrSylonCapacity) {
			t.Fatal("history unbounded", e)
		}
	})
}
func TestSylonHTTPAuthorizationRevocationAndErasure(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	b, c, n := sylonFixture(t, s, "default")
	p := executionPrincipal()
	server := &Server{Config: Config{AuthMode: "oidc", SylonBindings: []SylonBinding{b}}, Store: s, Auth: testAuth{p: p}}
	h := server.Handler()
	base := "/api/v1/workspaces/default/sylon"
	r := request(h, "GET", base+"/bindings", "")
	if r.Code != 200 || strings.Contains(r.Body.String(), "api.example.com") || strings.Contains(r.Body.String(), "/run/secrets") {
		t.Fatal("private binding leak", r.Body)
	}
	viewer := p
	viewer.Workspaces = map[string]string{"default": "viewer"}
	server.Auth = testAuth{p: viewer}
	in := SylonMessageInput{IdempotencyKey: "viewer-key", Prompt: "Explain", Notebook: sylonSelection(n), Catalogs: []SearchCatalogSelection{}}
	data, _ := json.Marshal(in)
	if r = request(h, "POST", base+"/conversations/"+c.ID+"/messages", string(data)); r.Code != 403 {
		t.Fatal("viewer cell source", r.Code)
	}
	in.Notebook = nil
	data, _ = json.Marshal(in)
	if r = request(h, "POST", base+"/conversations/"+c.ID+"/messages", string(data)); r.Code != 202 {
		t.Fatal("granted generic viewer", r.Code, r.Body)
	}
	var j SylonJob
	json.Unmarshal(r.Body.Bytes(), &j)
	server.Auth = testAuth{p: p}
	server.Config.SylonBindings[0].AllowedSubjects = []ExecutionSubject{}
	if r = request(h, "GET", base+"/jobs/"+j.ID, ""); r.Code != 403 {
		t.Fatal("revoked job readable", r.Code)
	}
	if r = request(h, "POST", base+"/jobs/"+j.ID+"/cancel", "{}"); r.Code != 202 || strings.Contains(r.Body.String(), "prompt") || strings.Contains(r.Body.String(), "catalogs") {
		t.Fatal("cancel projection", r.Code, r.Body)
	}
	if r = request(h, "DELETE", base+"/conversations/"+c.ID, ""); r.Code != 204 {
		t.Fatal("owner cannot erase after revoke", r.Code)
	}
	if _, e := s.SylonWork(ctx, "default", j.ID, c.Owner); !errors.Is(e, ErrNotFound) {
		t.Fatal("erasure retained private job")
	}
}
func TestSylonRealTLSProviderPostgresLifecycle(t *testing.T) {
	s, ws := searchTestStore(t)
	ctx := context.Background()
	b, c, n := sylonFixture(t, s, ws)
	calls := 0
	tlsServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != "POST" || r.URL.Path != "/v1/responses" || r.Header.Get("Authorization") != "Bearer fixture-only" {
			t.Error("wrong provider request")
		}
		var body map[string]any
		if json.NewDecoder(r.Body).Decode(&body) != nil || body["store"] != false || body["model"] != "fixture-model" {
			t.Error("unsafe provider request")
		}
		answer, _ := json.Marshal(llm.Result{Answer: "Verified TLS response", Citations: []string{}, ProposedCode: &llm.Code{Language: "python", Source: "print('tls-proposal')"}})
		json.NewEncoder(w).Encode(map[string]any{"id": "resp-fixture", "object": "response", "model": "fixture-model", "status": "completed", "output": []any{map[string]any{"type": "message", "role": "assistant", "status": "completed", "content": []any{map[string]any{"type": "output_text", "text": string(answer)}}}}})
	}))
	defer tlsServer.Close()
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.pem")
	token := filepath.Join(dir, "token")
	os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: tlsServer.Certificate().Raw}), 0600)
	os.WriteFile(token, []byte("fixture-only"), 0600)
	b.Provider.Endpoint = tlsServer.URL + "/v1/responses"
	b.Provider.CAFile = ca
	b.Provider.Auth.BearerTokenFile = token
	j := sylonEnqueue(t, s, b, c, sylonSelection(n))
	controller := SylonController{Config: Config{SylonBindings: []SylonBinding{b}}, Store: s, Generator: &ProviderSylonGenerator{}}
	if _, e := controller.ReconcileOnce(ctx, "tls-worker"); e != nil {
		t.Fatal(e)
	}
	work := sylonRead(t, s, j)
	if work.Job.State != "succeeded" || work.Job.Result == nil || calls != 1 {
		t.Fatalf("real provider failed: %+v calls=%d", work.Job, calls)
	}
	if _, e := s.ApplySylonCode(ctx, ws, j.ID, c.Owner, SylonApplyInput{NotebookID: n.ID, Revision: 1, CellID: n.Cells[0].ID}); e != nil {
		t.Fatal(e)
	}
}
func TestSylonConfigurationFailsClosed(t *testing.T) {
	good := contextSylonBinding()
	if e := good.Validate(); e != nil {
		t.Fatal(e)
	}
	for name, mutate := range map[string]func(*SylonBinding){"transfer": func(b *SylonBinding) { b.DataTransferAccepted = false }, "context": func(b *SylonBinding) { b.MaxContextBytes = 98305 }, "relative": func(b *SylonBinding) { b.Provider.Auth.BearerTokenFile = "relative" }, "http": func(b *SylonBinding) { b.Provider.Endpoint = "http://example.com" }, "duplicate": func(b *SylonBinding) { b.AllowedSubjects = append(b.AllowedSubjects, b.AllowedSubjects[0]) }} {
		t.Run(name, func(t *testing.T) {
			b := good
			mutate(&b)
			if b.Validate() == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}

func TestSylonAndSearchLeaseCheckedAfterBlockedRowLock(t *testing.T) {
	for _, kind := range []string{"sylon", "search"} {
		t.Run(kind, func(t *testing.T) {
			s, ws := searchTestStore(t)
			ctx := context.Background()
			var id string
			var finish func() error
			if kind == "sylon" {
				b, c, _ := sylonFixture(t, s, ws)
				j := sylonEnqueue(t, s, b, c, nil)
				_, l, e := s.AcquireSylonJob(ctx, "blocked-worker", 200*time.Millisecond)
				if e != nil {
					t.Fatal(e)
				}
				id = j.ID
				finish = func() error { return s.MarkSylonDispatch(ctx, l, "generating") }
			} else {
				b := searchBinding()
				b.WorkspaceID = ws
				n, e := s.SaveNotebook(ctx, ws, "", input())
				if e != nil {
					t.Fatal(e)
				}
				j := searchIndex(t, s, b, subjectFor(executionPrincipal()), SearchIndexInput{Notebook: &SearchNotebookSelection{ID: n.ID, Revision: n.Revision}})
				_, l, e := s.AcquireSearchJob(ctx, "blocked-worker", 200*time.Millisecond)
				if e != nil {
					t.Fatal(e)
				}
				id = j.ID
				finish = func() error { return s.MarkSearchEmbedding(ctx, l) }
			}
			tx, e := s.pool.Begin(ctx)
			if e != nil {
				t.Fatal(e)
			}
			defer tx.Rollback(ctx)
			table := "dataslate_sylon_jobs"
			if kind == "search" {
				table = "dataslate_search_jobs"
			}
			t.Cleanup(func() { _, _ = s.pool.Exec(context.Background(), "DELETE FROM "+table+" WHERE id=$1", id) })
			if _, e = tx.Exec(ctx, "SELECT id FROM "+table+" WHERE id=$1 FOR UPDATE", id); e != nil {
				t.Fatal(e)
			}
			done := make(chan error, 1)
			go func() { done <- finish() }()
			select {
			case e := <-done:
				t.Fatal("worker failed to wait for row lock", e)
			case <-time.After(250 * time.Millisecond):
			}
			if e = tx.Commit(ctx); e != nil {
				t.Fatal(e)
			}
			e = <-done
			if kind == "sylon" && !errors.Is(e, ErrSylonLease) {
				t.Fatal("expired Sylon lease dispatched", e)
			}
			if kind == "search" && !errors.Is(e, ErrSearchLease) {
				t.Fatal("expired search lease dispatched", e)
			}
		})
	}
}
func TestSylonUncitedDependenciesRemainProtected(t *testing.T) {
	forSylonStores(t, func(t *testing.T, s Store, ws string) {
		ctx := context.Background()
		b, c, n := sylonFixture(t, s, ws)
		j := sylonEnqueue(t, s, b, c, sylonSelection(n))
		gen := &fakeSylonGenerator{result: llm.Result{Answer: "Derived answer without citations", Citations: []string{}}}
		controller := SylonController{Config: Config{SylonBindings: []SylonBinding{b}}, Store: s, Generator: gen}
		if _, e := controller.ReconcileOnce(ctx, "worker"); e != nil {
			t.Fatal(e)
		}
		p := executionPrincipal()
		p.Workspaces = map[string]string{ws: "editor"}
		server := &Server{Config: controller.Config, Store: s, Auth: testAuth{p: p}}
		server.Config.AuthMode = "oidc"
		h := server.Handler()
		url := "/api/v1/workspaces/" + ws + "/sylon/jobs/" + j.ID
		if r := request(h, "GET", url, ""); r.Code != 200 {
			t.Fatal("authorized result missing", r.Code, r.Body)
		}
		in := input()
		in.Revision = 1
		in.Cells[0].Source = "changed"
		if _, e := s.SaveNotebook(ctx, ws, n.ID, in); e != nil {
			t.Fatal(e)
		}
		if r := request(h, "GET", url, ""); r.Code != 200 || !strings.Contains(r.Body.String(), `"historical":true`) {
			t.Fatal("historical revision not labeled", r.Code, r.Body)
		}
		if e := s.DeleteNotebook(ctx, ws, n.ID, 2); e != nil {
			t.Fatal(e)
		}
		if r := request(h, "GET", url, ""); r.Code != 403 || strings.Contains(r.Body.String(), "Derived answer") {
			t.Fatal("uncited source deletion leaked answer", r.Code, r.Body)
		}
	})
}

func TestSylonPublicReceiptsMatchOpenAPI(t *testing.T) {
	s := NewMemoryStore()
	b, c, n := sylonFixture(t, s, "default")
	b.Context.CatalogBindingIDs = nil
	b.Context.SearchBindingIDs = nil
	j := sylonEnqueue(t, s, b, c, sylonSelection(n))
	controller := SylonController{Config: Config{SylonBindings: []SylonBinding{b}}, Store: s, Generator: &fakeSylonGenerator{result: llm.Result{Answer: "Answer", Citations: []string{}, ProposedCode: &llm.Code{Language: "python", Source: "print(42)"}}}}
	if _, err := controller.ReconcileOnce(context.Background(), "worker"); err != nil {
		t.Fatal(err)
	}
	server := &Server{Config: controller.Config, Store: s, Auth: testAuth{p: executionPrincipal()}}
	server.Config.AuthMode = "oidc"
	h := server.Handler()
	base := "/api/v1/workspaces/default/sylon"
	raw, err := os.ReadFile("../../openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var doc any
	if err = json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	compiler := jsonschema.NewCompiler()
	if err = compiler.AddResource("contract.json", doc); err != nil {
		t.Fatal(err)
	}
	apply, _ := json.Marshal(SylonApplyInput{NotebookID: n.ID, Revision: 1, CellID: n.Cells[0].ID})
	cases := []struct {
		method, path, body, schema string
		code                       int
	}{{"GET", base + "/jobs/" + j.ID, "", "SylonJob", 200}, {"GET", base + "/conversations/" + c.ID, "", "SylonConversationHistory", 200}, {"POST", base + "/jobs/" + j.ID + "/apply", string(apply), "SylonApplyReceipt", 200}, {"POST", base + "/jobs/" + j.ID + "/cancel", "{}", "SylonCancelReceipt", 202}}
	for _, tc := range cases {
		r := request(h, tc.method, tc.path, tc.body)
		if r.Code != tc.code {
			t.Fatal(tc.path, r.Code, r.Body)
		}
		schema, e := compiler.Compile("contract.json#/components/schemas/" + tc.schema)
		if e != nil {
			t.Fatal(e)
		}
		var body any
		json.Unmarshal(r.Body.Bytes(), &body)
		if e = schema.Validate(body); e != nil {
			t.Fatal(tc.schema, e)
		}
	}
	r := request(h, "GET", base+"/bindings", "")
	var list struct{ Items []any }
	if r.Code != 200 || json.Unmarshal(r.Body.Bytes(), &list) != nil || len(list.Items) != 1 {
		t.Fatal(r.Code, r.Body)
	}
	schema, e := compiler.Compile("contract.json#/components/schemas/SylonBinding")
	if e != nil {
		t.Fatal(e)
	}
	if e = schema.Validate(list.Items[0]); e != nil {
		t.Fatal(e)
	}
}
func TestSylonStaleUnappliedProposalCannotOverwrite(t *testing.T) {
	forSylonStores(t, func(t *testing.T, s Store, ws string) {
		ctx := context.Background()
		b, c, n := sylonFixture(t, s, ws)
		j := sylonEnqueue(t, s, b, c, sylonSelection(n))
		controller := SylonController{Config: Config{SylonBindings: []SylonBinding{b}}, Store: s, Generator: &fakeSylonGenerator{result: llm.Result{Answer: "Suggestion", Citations: []string{}, ProposedCode: &llm.Code{Language: "python", Source: "unapplied proposal"}}}}
		if _, e := controller.ReconcileOnce(ctx, "worker"); e != nil {
			t.Fatal(e)
		}
		in := input()
		in.Revision = 1
		in.Cells[0].Source = "newer user work"
		if _, e := s.SaveNotebook(ctx, ws, n.ID, in); e != nil {
			t.Fatal(e)
		}
		if _, e := s.ApplySylonCode(ctx, ws, j.ID, c.Owner, SylonApplyInput{NotebookID: n.ID, Revision: 1, CellID: n.Cells[0].ID}); !errors.Is(e, ErrSylonRevision) {
			t.Fatal("stale proposal applied", e)
		}
		actual, e := s.Notebook(ctx, ws, n.ID)
		if e != nil || actual.Revision != 2 || actual.Cells[0].Source != "newer user work" {
			t.Fatal("user edit lost", e)
		}
	})
}

func TestSylonDatabaseRejectsCrossWorkspaceConversationLink(t *testing.T) {
	s, ws := searchTestStore(t)
	b, c, _ := sylonFixture(t, s, ws)
	j := sylonEnqueue(t, s, b, c, nil)
	other, e := s.CreateWorkspace(context.Background(), "Other tenant", newResource("test-").ID)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.pool.Exec(context.Background(), "UPDATE dataslate_sylon_jobs SET workspace_id=$2 WHERE id=$1", j.ID, other.ID); e == nil {
		t.Fatal("database accepted cross-workspace conversation link")
	}
}
