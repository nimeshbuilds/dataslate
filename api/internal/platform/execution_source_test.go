package platform

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

func notebookBinding() ExecutionBinding {
	b := executionTestBinding()
	b.ID = "python-batch"
	b.ExecutionKind = NotebookPythonBatch
	b.MainApplicationFile = ""
	return b
}
func pythonNotebook() NotebookInput {
	return NotebookInput{Name: "Python batch", Kind: "notebook", Cells: []Cell{{ID: "first", Language: "python", Source: "x = 7"}, {ID: "second", Language: "python", Source: "print(x * 6)"}}}
}
func notebookRun(r Run, n Notebook) Run {
	r.Notebook = &NotebookProvenance{NotebookSelection: NotebookSelection{ID: n.ID, Revision: n.Revision}}
	return r
}
func createNotebookRun(t *testing.T, s Store, ws string) Run {
	t.Helper()
	b := notebookBinding()
	b.WorkspaceID = ws
	n, e := s.SaveNotebook(context.Background(), ws, "", pythonNotebook())
	if e != nil {
		t.Fatal(e)
	}
	r, _, e := s.CreateRun(context.Background(), notebookRun(newRun(ws, b, b.AllowedSubjects[0]), n), newResource("key-").ID, "snapshot-request")
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func TestNotebookBindingIsExplicitCapability(t *testing.T) {
	b := notebookBinding()
	if e := b.Validate(); e != nil {
		t.Fatal(e)
	}
	b.MainApplicationFile = NotebookApplicationFile
	if b.Validate() == nil {
		t.Fatal("notebook binding accepted caller application URI")
	}
	b = notebookBinding()
	b.ExecutionKind = "interactive-python"
	if b.Validate() == nil {
		t.Fatal("unimplemented capability accepted")
	}
	b = executionTestBinding()
	if b.Kind() != ApprovedApplication {
		t.Fatal("legacy default changed")
	}
}
func TestMemoryNotebookAtomicSnapshot(t *testing.T) {
	exerciseNotebookSnapshot(t, NewMemoryStore(), "default")
}
func exerciseNotebookSnapshot(t *testing.T, s Store, ws string) {
	t.Helper()
	ctx := context.Background()
	b := notebookBinding()
	b.WorkspaceID = ws
	p := b.AllowedSubjects[0]
	n, e := s.SaveNotebook(ctx, ws, "", pythonNotebook())
	if e != nil {
		t.Fatal(e)
	}
	input := RunInput{BindingID: b.ID, IdempotencyKey: newResource("key-").ID, Notebook: &NotebookSelection{ID: n.ID, Revision: 1}}
	makeRun := func() Run { return notebookRun(newRun(ws, b, p), n) }
	var wg sync.WaitGroup
	results := make(chan Run, 8)
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Go(func() {
			r, _, err := s.CreateRun(ctx, makeRun(), input.IdempotencyKey, runFingerprint(input))
			results <- r
			errs <- err
		})
	}
	wg.Wait()
	close(results)
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	var original Run
	for r := range results {
		if original.ID != "" && original.ID != r.ID {
			t.Fatal("multiple snapshots for same request")
		}
		original = r
	}
	source, e := s.RunSource(ctx, ws, original.ID, p)
	if e != nil {
		t.Fatal(e)
	}
	sum := sha256.Sum256([]byte(source.Source))
	if source.Notebook.SHA256 != hex.EncodeToString(sum[:]) || source.Notebook.Bytes != len(source.Source) || strings.Join(source.Notebook.CellIDs, ",") != "first,second" {
		t.Fatal("incorrect independent provenance")
	}
	changed := pythonNotebook()
	changed.Revision = 1
	changed.Cells[0].Source = "x = 99"
	updated, e := s.SaveNotebook(ctx, ws, n.ID, changed)
	if e != nil {
		t.Fatal(e)
	}
	if _, _, e = s.CreateRun(ctx, makeRun(), newResource("key-").ID, "stale"); !errors.Is(e, ErrNotebookRevision) {
		t.Fatal("stale revision was accepted", e)
	}
	if e = s.DeleteNotebook(ctx, ws, n.ID, updated.Revision); e != nil {
		t.Fatal(e)
	}
	replay, yes, e := s.CreateRun(ctx, makeRun(), input.IdempotencyKey, runFingerprint(input))
	if e != nil || !yes || replay.ID != original.ID {
		t.Fatal("deleted notebook broke exact idempotent replay", e)
	}
	retained, e := s.RunSource(ctx, ws, original.ID, p)
	if e != nil || retained.Source != source.Source || retained.Notebook.SHA256 != source.Notebook.SHA256 {
		t.Fatal("notebook deletion removed run source", e)
	}
	altered := input
	selection := *input.Notebook
	selection.Revision = 2
	altered.Notebook = &selection
	if _, _, e = s.CreateRun(ctx, makeRun(), input.IdempotencyKey, runFingerprint(altered)); !errors.Is(e, ErrConflict) {
		t.Fatal("changed selection reused key", e)
	}
	for _, tc := range []struct {
		ws string
		p  ExecutionSubject
	}{{"wrong-workspace", p}, {ws, ExecutionSubject{Issuer: p.Issuer, Subject: "bob"}}, {ws, ExecutionSubject{Issuer: "https://other.example.test", Subject: p.Subject}}} {
		if _, e = s.RunSource(ctx, tc.ws, original.ID, tc.p); !errors.Is(e, ErrNotFound) {
			t.Fatal("source authorization bypass", e)
		}
	}
	list, e := s.Runs(ctx, ws, p)
	if e != nil {
		t.Fatal(e)
	}
	encoded, _ := json.Marshal(list)
	if strings.Contains(string(encoded), "__dataslate_batch") {
		t.Fatal("source leaked into run list")
	}
	leased, lease, e := s.AcquireRun(ctx, "snapshot-test", time.Second)
	if e != nil {
		t.Fatal(e)
	}
	if leased.ID != original.ID || leased.ValidateSource() != nil {
		t.Fatal("controller did not load independent source", e)
	}
	if _, e = s.AdvanceRun(ctx, lease, RunUpdate{State: "cancelled", Message: "Never submitted"}); e != nil {
		t.Fatal(e)
	}
	terminal, lease, e := s.AcquireRun(ctx, "cleanup-test", time.Second)
	if e != nil || terminal.State != "cancelled" || terminal.SourceArtifact.State != "cleanup_pending" || terminal.ValidateSource() != nil {
		t.Fatal("terminal source cleanup not durably leased", e)
	}
	a := *terminal.SourceArtifact
	a.State = "deleted"
	if _, e = s.AdvanceRun(ctx, lease, RunUpdate{State: "cancelled", Message: terminal.Message, SourceArtifact: &a}); e != nil {
		t.Fatal(e)
	}
	if _, _, e = s.AcquireRun(ctx, "after-cleanup", time.Second); !errors.Is(e, ErrNotFound) {
		t.Fatal("completed source cleanup still leased", e)
	}
}
func TestPostgresNotebookAtomicSnapshot(t *testing.T) {
	dsn := os.Getenv("DATASLATE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires isolated PostgreSQL")
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
	if e = s.Ready(ctx); e != nil {
		t.Fatal(e)
	}
	ws, e := s.CreateWorkspace(ctx, "Notebook execution", newResource("nbtest-").ID)
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		s.pool.Exec(ctx, "DELETE FROM dataslate_runs WHERE workspace_id=$1", ws.ID)
		s.pool.Exec(ctx, "DELETE FROM dataslate_notebooks WHERE workspace_id=$1", ws.ID)
		s.pool.Exec(ctx, "DELETE FROM dataslate_workspaces WHERE id=$1", ws.ID)
	}()
	exerciseNotebookSnapshot(t, s, ws.ID)
}
func TestNotebookAPIBoundaries(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	b := notebookBinding()
	h := executionHandler(s, executionPrincipal(), []ExecutionBinding{b, executionTestBinding()}, nil)
	n, _ := s.SaveNotebook(ctx, "default", "", pythonNotebook())
	submit := func(binding, id string, rev int, key string) string {
		v, _ := json.Marshal(RunInput{BindingID: binding, IdempotencyKey: key, Notebook: &NotebookSelection{ID: id, Revision: rev}})
		return string(v)
	}
	for _, tc := range []struct {
		body   string
		status int
		code   string
	}{
		{submit(b.ID, n.ID, 2, "stale-revision"), 409, "notebook_revision_conflict"},
		{submit("approved-app", n.ID, 1, "wrong-capability"), 422, "notebook_not_executable"},
		{`{"bindingId":"python-batch","idempotencyKey":"missing-selection"}`, 422, "notebook_not_executable"},
		{`{"bindingId":"python-batch","idempotencyKey":"inline-source","notebook":{"id":"x","revision":1,"source":"print(1)"}}`, 400, "invalid_json"},
	} {
		w := request(h, "POST", "/api/v1/workspaces/default/runs", tc.body)
		if w.Code != tc.status || !strings.Contains(w.Body.String(), tc.code) {
			t.Fatalf("%d %s", w.Code, w.Body)
		}
	}
	for _, change := range []func(*NotebookInput){func(n *NotebookInput) { n.Kind = "sql"; n.Cells[0].Language = "sql" }, func(n *NotebookInput) { n.Cells[1].Language = "sql" }, func(n *NotebookInput) { n.Cells = []Cell{} }, func(n *NotebookInput) { n.Cells[0].Source = strings.Repeat("a", MaxNotebookSourceBytes) }} {
		v := pythonNotebook()
		change(&v)
		bad, _ := s.SaveNotebook(ctx, "default", "", v)
		w := request(h, "POST", "/api/v1/workspaces/default/runs", submit(b.ID, bad.ID, 1, newResource("key-").ID))
		if w.Code != 422 || !strings.Contains(w.Body.String(), "notebook_not_executable") {
			t.Fatal("invalid notebook accepted", w.Code, w.Body)
		}
	}
	w := request(h, "POST", "/api/v1/workspaces/default/runs", submit(b.ID, n.ID, 1, "accepted-notebook"))
	if w.Code != 202 {
		t.Fatal(w.Code, w.Body)
	}
	var r Run
	json.Unmarshal(w.Body.Bytes(), &r)
	if strings.Contains(w.Body.String(), "__dataslate_batch") || r.Notebook == nil {
		t.Fatal("wrong public source projection")
	}
	source := request(h, "GET", "/api/v1/workspaces/default/runs/"+r.ID+"/source", "")
	if source.Code != 200 || !strings.Contains(source.Body.String(), "__dataslate_batch") {
		t.Fatal("owned snapshot unavailable", source.Code)
	}
	for _, p := range []Principal{{Issuer: b.AllowedSubjects[0].Issuer, Subject: "bob", Workspaces: map[string]string{"default": "admin"}}, {Issuer: b.AllowedSubjects[0].Issuer, Subject: "alice", Workspaces: map[string]string{}}, {Issuer: "https://other.example.test", Subject: "alice", Workspaces: map[string]string{"default": "editor"}}} {
		denied := executionHandler(s, p, []ExecutionBinding{b}, nil)
		resp := request(denied, "GET", "/api/v1/workspaces/default/runs/"+r.ID+"/source", "")
		if resp.Code != 403 && resp.Code != 404 {
			t.Fatal("source leaked", resp.Code)
		}
	}
	status := request(h, "GET", "/api/v1/status", "")
	if !strings.Contains(status.Body.String(), `"notebookExecutionEnabled":true`) {
		t.Fatal("notebook capability unadvertised")
	}
	if binding := request(h, "GET", "/api/v1/workspaces/default/execution-bindings", ""); !strings.Contains(binding.Body.String(), NotebookPythonBatch) {
		t.Fatal("notebook binding undiscoverable")
	}
}
func TestGeneratedPythonCellBoundariesAndUnicode(t *testing.T) {
	python, e := exec.LookPath("python3")
	if e != nil {
		t.Skip("Python interpreter unavailable")
	}
	b := notebookBinding()
	n := Notebook{Resource: Resource{ID: "nb-source", Revision: 3}, WorkspaceID: "default", Kind: "notebook", Cells: []Cell{{ID: "one", Language: "python", Source: "x = 'quote\\\" \\n snow: 雪 🦊'\ncompile = 1\nglobals = 2\nexec = 3"}, {ID: "two", Language: "python", Source: "assert x.endswith('雪 🦊')\nprint(6 * 7)"}}}
	r, e := snapshotNotebook(notebookRun(newRun("default", b, b.AllowedSubjects[0]), n), n)
	if e != nil {
		t.Fatal(e)
	}
	cmd := exec.Command(python, "-")
	cmd.Stdin = strings.NewReader(r.Source.Source)
	out, e := cmd.CombinedOutput()
	if e != nil || strings.TrimSpace(string(out)) != "42" {
		t.Fatalf("generated Python failed: %s %v", out, e)
	}
	n.Cells[0].Source = "x = '''"
	n.Cells[1].Source = "'''\nprint('accidentally combined')"
	r, e = snapshotNotebook(notebookRun(newRun("default", b, b.AllowedSubjects[0]), n), n)
	if e != nil {
		t.Fatal(e)
	}
	cmd = exec.Command(python, "-")
	cmd.Stdin = strings.NewReader(r.Source.Source)
	out, e = cmd.CombinedOutput()
	if e == nil || !strings.Contains(string(out), "dataslate://nb-source/3/one.py") {
		t.Fatal("cell boundaries or traceback identity lost", string(out))
	}
}

type sourceTestBackend struct {
	expireDuringPrepare bool
	expireDuringObserve bool
	fakeExecutionBackend
	allowed                   bool
	policyError               error
	artifact                  SourceObservation
	prepareError, deleteError error
	prepares, binds, deletes  int
	cancelDuringPrepare       bool
}

func (b *sourceTestBackend) Observe(ctx context.Context, r Run) (BackendObservation, error) {
	if b.expireDuringObserve {
		<-ctx.Done()
	}
	return b.fakeExecutionBackend.Observe(ctx, r)
}
func (b *sourceTestBackend) Authorize(context.Context, Run) (bool, error) {
	return b.allowed, b.policyError
}
func (b *sourceTestBackend) ObserveSource(context.Context, Run) (SourceObservation, error) {
	return b.artifact, nil
}
func (b *sourceTestBackend) PrepareSource(ctx context.Context, r Run) (SourceArtifact, error) {
	b.prepares++
	if b.expireDuringPrepare {
		<-ctx.Done()
	}
	stored, _ := b.store.Run(ctx, r.WorkspaceID, r.ID, r.SubmittedBy)
	if !stored.SourceArtifact.CreationStarted || r.ValidateSource() != nil || stored.SubmissionStarted {
		panic("source effect without valid durable preparation intent")
	}
	if b.cancelDuringPrepare {
		b.store.CancelRun(ctx, r.WorkspaceID, r.ID, r.SubmittedBy)
	}
	if b.prepareError != nil {
		return SourceArtifact{}, b.prepareError
	}
	b.artifact = SourceObservation{Exists: true, UID: "source-uid"}
	return SourceArtifact{Name: r.SourceArtifact.Name, UID: b.artifact.UID, State: "ready", CreationStarted: true}, nil
}
func (b *sourceTestBackend) BindSource(context.Context, Run) error { b.binds++; return nil }
func (b *sourceTestBackend) DeleteSource(ctx context.Context, r Run) error {
	b.deletes++
	stored, _ := b.store.Run(ctx, r.WorkspaceID, r.ID, r.SubmittedBy)
	if !stored.Terminal() || stored.SourceArtifact.UID == "" {
		panic("cleanup without durable terminal/source identity")
	}
	if b.deleteError != nil {
		return b.deleteError
	}
	b.artifact = SourceObservation{}
	return nil
}
func TestNotebookReconciliationAndCleanup(t *testing.T) {
	for _, mode := range []string{"success", "cancel-before-prepare", "cancel-during-prepare", "revoked", "uncertain-prepare", "lost-delete", "policy-outage-cleanup"} {
		t.Run(mode, func(t *testing.T) {
			s := NewMemoryStore()
			r := createNotebookRun(t, s, "default")
			ctx := context.Background()
			backend := &sourceTestBackend{fakeExecutionBackend: fakeExecutionBackend{store: s}, allowed: true}
			if mode == "cancel-before-prepare" {
				s.CancelRun(ctx, "default", r.ID, r.SubmittedBy)
			}
			if mode == "revoked" {
				backend.allowed = false
			}
			if mode == "cancel-during-prepare" {
				backend.cancelDuringPrepare = true
			}
			if mode == "uncertain-prepare" {
				backend.prepareError = errors.New("unknown create")
			}
			step := func() {
				t.Helper()
				dueRun(s, r.ID)
				if worked, e := ReconcileOnce(ctx, s, backend, "controller"); e != nil || !worked {
					t.Fatal(worked, e)
				}
			}
			step()
			got, _ := s.Run(ctx, "default", r.ID, r.SubmittedBy)
			switch mode {
			case "cancel-before-prepare", "revoked":
				if backend.prepares != 0 || backend.submits != 0 || got.State != "cancelled" {
					t.Fatal("cancel/revocation performed an effect")
				}
			case "cancel-during-prepare":
				if backend.prepares != 1 || backend.submits != 0 || got.State != "cancelled" {
					t.Fatal("cancelled source preparation submitted Spark")
				}
			case "uncertain-prepare":
				if got.State != "unknown" || got.SubmissionStarted || backend.submits != 0 {
					t.Fatal("unknown source creation submitted Spark")
				}
				s.CancelRun(ctx, "default", r.ID, r.SubmittedBy)
				step()
				step()
				got, _ = s.Run(ctx, "default", r.ID, r.SubmittedBy)
				if got.SourceArtifact.State != "cleanup_pending" || backend.deletes != 0 {
					t.Fatal("unknown artifact absence claimed cleanup")
				}
				backend.artifact = SourceObservation{Exists: true, UID: "source-late-uid"}
			default:
				if got.SourceArtifact.State != "ready" || got.SourceArtifact.UID != "source-uid" || backend.submits != 1 {
					t.Fatalf("notebook not submitted: %+v", got)
				}
				step()
				if backend.binds != 1 {
					t.Fatal("source not bound to durable application UID")
				}
				backend.observation.State = "succeeded"
				step()
			}
			if mode == "lost-delete" {
				backend.deleteError = errors.New("lost response")
				step()
				got, _ = s.Run(ctx, "default", r.ID, r.SubmittedBy)
				if got.State != "succeeded" || got.SourceArtifact.State != "cleanup_pending" {
					t.Fatal("cleanup overwrote real execution outcome")
				}
				backend.artifact = SourceObservation{}
				backend.deleteError = nil
			}
			if mode == "policy-outage-cleanup" {
				backend.policyError = errors.New("bindings file lost")
			}
			step()
			got, _ = s.Run(ctx, "default", r.ID, r.SubmittedBy)
			if !got.Terminal() || got.SourceArtifact.State != "deleted" {
				t.Fatalf("source cleanup incomplete %+v", got.SourceArtifact)
			}
			if worked, e := ReconcileOnce(ctx, s, backend, "done"); e != nil || worked {
				t.Fatal("cleaned terminal run remained work", e)
			}
		})
	}
}
func TestPendingSourceCleanupConsumesCapacity(t *testing.T) {
	s := NewMemoryStore()
	b := notebookBinding()
	ctx := context.Background()
	for i := 0; i < MaxActiveWorkspaceRuns; i++ {
		r := newRun("default", b, b.AllowedSubjects[0])
		r.State = "cancelled"
		r.SourceArtifact = &SourceArtifact{Name: r.KubernetesName + "-source", State: "cleanup_pending", CreationStarted: true}
		s.executions[r.ID] = &memoryExecution{Run: r}
	}
	if _, _, e := s.CreateRun(ctx, newRun("default", executionTestBinding(), b.AllowedSubjects[0]), "cannot-bypass-cap", fmt.Sprint(1)); !errors.Is(e, ErrRunCapacity) {
		t.Fatal("cleanup bypassed workspace capacity", e)
	}
}

func exerciseNotebookEditRace(t *testing.T, s Store, ws string) {
	t.Helper()
	ctx := context.Background()
	b := notebookBinding()
	b.WorkspaceID = ws
	for i := 0; i < 12; i++ {
		n, e := s.SaveNotebook(ctx, ws, "", pythonNotebook())
		if e != nil {
			t.Fatal(e)
		}
		start := make(chan struct{})
		saved := make(chan error, 1)
		type acceptance struct {
			run Run
			err error
		}
		accepted := make(chan acceptance, 1)
		go func() {
			<-start
			updated := pythonNotebook()
			updated.Revision = 1
			updated.Cells[0].Source = "x = 999"
			_, err := s.SaveNotebook(ctx, ws, n.ID, updated)
			saved <- err
		}()
		go func() {
			<-start
			r, _, err := s.CreateRun(ctx, notebookRun(newRun(ws, b, b.AllowedSubjects[0]), n), newResource("key-").ID, "edit-race")
			accepted <- acceptance{r, err}
		}()
		close(start)
		if e = <-saved; e != nil {
			t.Fatal(e)
		}
		a := <-accepted
		if a.err != nil {
			if !errors.Is(a.err, ErrNotebookRevision) {
				t.Fatal("unexpected race error", a.err)
			}
		} else {
			source, err := s.RunSource(ctx, ws, a.run.ID, b.AllowedSubjects[0])
			if err != nil || strings.Contains(source.Source, "999") || source.Notebook.Revision != 1 {
				t.Fatal("accepted snapshot mixed with concurrent edit", err)
			}
			_, lease, err := s.AcquireRun(ctx, "edit-race-cleanup", time.Second)
			if err != nil {
				t.Fatal(err)
			}
			artifact := *a.run.SourceArtifact
			artifact.State = "deleted"
			if _, err = s.AdvanceRun(ctx, lease, RunUpdate{State: "cancelled", Message: "No backend effect in snapshot race test", SourceArtifact: &artifact}); err != nil {
				t.Fatal(err)
			}
		}
		if e = s.DeleteNotebook(ctx, ws, n.ID, 2); e != nil {
			t.Fatal(e)
		}
	}
}
func TestMemoryNotebookConcurrentEditSnapshot(t *testing.T) {
	exerciseNotebookEditRace(t, NewMemoryStore(), "default")
}
func TestPostgresNotebookConcurrentEditSnapshot(t *testing.T) {
	dsn := os.Getenv("DATASLATE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires isolated PostgreSQL")
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
	ws, e := s.CreateWorkspace(ctx, "Notebook race", newResource("nb-race-").ID)
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		s.pool.Exec(ctx, "DELETE FROM dataslate_runs WHERE workspace_id=$1", ws.ID)
		s.pool.Exec(ctx, "DELETE FROM dataslate_notebooks WHERE workspace_id=$1", ws.ID)
		s.pool.Exec(ctx, "DELETE FROM dataslate_workspaces WHERE id=$1", ws.ID)
	}()
	exerciseNotebookEditRace(t, s, ws.ID)
}

func TestNotebookOperationDeadlineBeforeSubmitPreservesSafeRetry(t *testing.T) {
	s := NewMemoryStore()
	r := createNotebookRun(t, s, "default")
	ctx := context.Background()
	backend := &sourceTestBackend{fakeExecutionBackend: fakeExecutionBackend{store: s}, allowed: true, expireDuringPrepare: true}
	if worked, e := reconcileOnceWithTimeout(ctx, s, backend, "expired-operation", time.Now, 5*time.Millisecond); e != nil || !worked {
		t.Fatal(worked, e)
	}
	got, _ := s.Run(ctx, "default", r.ID, r.SubmittedBy)
	if got.SubmissionStarted || backend.submits != 0 || got.State != "unknown" || got.SourceArtifact.State != "ready" {
		t.Fatal("expired operation recorded an ambiguous Spark submission")
	}
	backend.expireDuringPrepare = false
	dueRun(s, r.ID)
	if _, e := ReconcileOnce(ctx, s, backend, "fresh-operation"); e != nil {
		t.Fatal(e)
	}
	got, _ = s.Run(ctx, "default", r.ID, r.SubmittedBy)
	if !got.SubmissionStarted || backend.submits != 1 || got.State != "running" {
		t.Fatal("safe retry did not resume prepared source")
	}
}

func TestNotebookOperationDeadlineBeforePreparationDoesNotRecordCreate(t *testing.T) {
	s := NewMemoryStore()
	r := createNotebookRun(t, s, "default")
	backend := &sourceTestBackend{fakeExecutionBackend: fakeExecutionBackend{store: s}, allowed: true, expireDuringObserve: true}
	if _, e := reconcileOnceWithTimeout(context.Background(), s, backend, "expired-observation", time.Now, 5*time.Millisecond); e != nil {
		t.Fatal(e)
	}
	got, _ := s.Run(context.Background(), "default", r.ID, r.SubmittedBy)
	if got.SourceArtifact.CreationStarted || got.SubmissionStarted || backend.prepares != 0 || backend.submits != 0 || got.State != "unknown" {
		t.Fatal("expired observation recorded an external create intent")
	}
}
