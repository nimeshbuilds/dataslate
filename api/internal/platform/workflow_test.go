package platform

import (
	"context"
	"dataslate.local/api/internal/airflow"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func workflowTestBinding(ws string) WorkflowBinding {
	return WorkflowBinding{WorkflowBindingSummary: WorkflowBindingSummary{ID: "daily", WorkspaceID: ws, Name: "Daily ingestion", PolicyVersion: "policy-1", DAGID: "daily_ingestion", DAGVersion: 1, BundleVersion: "commit-abc", AirflowVersion: "3.4.0", MaxRunSeconds: 300}, Endpoint: "https://airflow.example.test", TokenFile: "/private/broker-token", MetadataAccessAccepted: true, AllowedSubjects: []ExecutionSubject{{Issuer: "https://identity.example.test", Subject: "alice"}}}
}

type fakeWorkflowBackend struct {
	taskReads                     int
	binding                       WorkflowBinding
	posts                         int
	runErr                        error
	state                         string
	wrongReceipt, paused, revoked bool
	onDefinition                  func()
	onRead                        func()
}

func (f *fakeWorkflowBackend) Authorize(_ context.Context, w WorkflowWork) error {
	if f.revoked || f.binding.Hash() != w.PolicyHash || !f.binding.Allows(w.Run.SubmittedBy) {
		return ErrWorkflowAccess
	}
	return nil
}
func (f *fakeWorkflowBackend) Open(context.Context, WorkflowBinding) (WorkflowEngine, error) {
	return f, nil
}
func (f *fakeWorkflowBackend) Close() {}
func (f *fakeWorkflowBackend) Definition(_ context.Context, t airflow.Target) (airflow.Definition, error) {
	if f.onDefinition != nil {
		f.onDefinition()
	}
	return airflow.Definition{DAGID: t.DAGID, Version: airflow.Version{ID: "10000000-0000-4000-8000-000000000001", DAGID: t.DAGID, Number: t.DAGVersion, BundleVersion: t.BundleVersion}, Paused: f.paused, Timezone: "UTC", ScheduleSummary: "0 0 * * *", Tasks: []airflow.Task{{ID: "ingest", DownstreamTaskIDs: []string{}}}}, nil
}
func (f *fakeWorkflowBackend) Trigger(_ context.Context, t airflow.Target, id string, receipt airflow.Receipt) (airflow.Run, error) {
	f.posts++
	return airflow.Run{}, airflow.ErrUnavailable
}
func (f *fakeWorkflowBackend) Run(_ context.Context, t airflow.Target, id string, receipt airflow.Receipt) (airflow.Run, error) {
	if f.onRead != nil {
		f.onRead()
	}
	if f.runErr != nil {
		return airflow.Run{}, f.runErr
	}
	if f.wrongReceipt {
		receipt.RunID = "foreign"
	}
	conf, _ := json.Marshal(airflow.Conf{DataSlate: receipt})
	state := f.state
	if state == "" {
		state = "running"
	}
	return airflow.Run{DAGID: t.DAGID, RunID: id, State: state, RunType: "manual", Versions: []airflow.Version{{ID: "10000000-0000-4000-8000-000000000001", DAGID: t.DAGID, Number: t.DAGVersion, BundleVersion: t.BundleVersion}}, BundleVersion: t.BundleVersion, Conf: conf}, nil
}
func (f *fakeWorkflowBackend) Tasks(context.Context, airflow.Target, string) ([]airflow.TaskInstance, bool, error) {
	f.taskReads++
	return []airflow.TaskInstance{{ID: "ingest", MapIndex: -1, TryNumber: 2, State: "up_for_retry"}}, false, nil
}
func (f *fakeWorkflowBackend) History(_ context.Context, t airflow.Target) ([]airflow.Run, bool, error) {
	r, _ := f.Run(context.Background(), t, "scheduled__2026-11-08T00:00:00+00:00", airflow.Receipt{})
	r.RunType = "scheduled"
	r.Conf = json.RawMessage(`{"sensitive":"must not expose"}`)
	return []airflow.Run{r}, true, nil
}
func workflowDue(t *testing.T, s Store, id string) {
	t.Helper()
	switch db := s.(type) {
	case *MemoryStore:
		db.mu.Lock()
		db.workflows[id].Next = time.Time{}
		db.mu.Unlock()
	case *PostgresStore:
		if _, e := db.pool.Exec(context.Background(), "UPDATE dataslate_workflows SET next_reconcile_at=clock_timestamp()-interval '1 second' WHERE id=$1", id); e != nil {
			t.Fatal(e)
		}
	}
}
func workflowExpire(t *testing.T, s Store, id string) {
	t.Helper()
	switch db := s.(type) {
	case *MemoryStore:
		db.mu.Lock()
		db.workflows[id].Until = time.Now().Add(-time.Second)
		db.mu.Unlock()
	case *PostgresStore:
		if _, e := db.pool.Exec(context.Background(), "UPDATE dataslate_workflows SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", id); e != nil {
			t.Fatal(e)
		}
	}
}
func mustWorkflow(t *testing.T, s Store, ws string) WorkflowWork {
	t.Helper()
	b := workflowTestBinding(ws)
	w, _, e := s.CreateWorkflow(context.Background(), newWorkflow(b, b.AllowedSubjects[0]), "original-key", digestJSON([]string{b.ID}))
	if e != nil {
		t.Fatal(e)
	}
	return w
}
func readWorkflow(t *testing.T, s Store, w WorkflowWork) WorkflowWork {
	t.Helper()
	got, e := s.Workflow(context.Background(), w.Run.WorkspaceID, w.Run.ID, w.Run.SubmittedBy)
	if e != nil {
		t.Fatal(e)
	}
	return got
}
func TestWorkflowSingleTriggerUnknownRecoveryAndAirflowRetryOwner(t *testing.T) {
	forSylonStores(t, func(t *testing.T, s Store, ws string) {
		ctx := context.Background()
		w := mustWorkflow(t, s, ws)
		backend := &fakeWorkflowBackend{binding: w.Binding, runErr: airflow.ErrNotFound}
		if _, e := ReconcileWorkflowOnce(ctx, s, backend, "first"); e != nil {
			t.Fatal(e)
		}
		got := readWorkflow(t, s, w)
		if backend.posts != 1 || got.Run.State != "unknown" || !got.Run.DispatchStarted || got.Run.ReconciliationComplete {
			t.Fatal(got.Run, backend.posts)
		}
		backend.runErr = nil
		workflowDue(t, s, w.Run.ID)
		if _, e := ReconcileWorkflowOnce(ctx, s, backend, "restarted"); e != nil {
			t.Fatal(e)
		}
		got = readWorkflow(t, s, w)
		if got.Run.State != "running" || len(got.Run.Tasks) != 1 || got.Run.Tasks[0].State != "up_for_retry" || backend.posts != 1 {
			t.Fatal("Airflow retry must not trigger a second workflow", got.Run, backend.posts)
		}
		backend.state = "success"
		workflowDue(t, s, w.Run.ID)
		if _, e := ReconcileWorkflowOnce(ctx, s, backend, "restarted"); e != nil {
			t.Fatal(e)
		}
		got = readWorkflow(t, s, w)
		if got.Run.State != "succeeded" || !got.Run.ReconciliationComplete || backend.posts != 1 {
			t.Fatal(got.Run)
		}
		if _, _, e := s.AcquireWorkflow(ctx, "another", time.Minute); !errors.Is(e, ErrNotFound) {
			t.Fatal("terminal acquired", e)
		}
	})
}
func TestWorkflowIdempotencyAndOwnerIsolation(t *testing.T) {
	forSylonStores(t, func(t *testing.T, s Store, ws string) {
		ctx := context.Background()
		b := workflowTestBinding(ws)
		var wg sync.WaitGroup
		results := make(chan WorkflowWork, 8)
		errs := make(chan error, 8)
		for i := 0; i < 8; i++ {
			wg.Go(func() {
				w, _, e := s.CreateWorkflow(ctx, newWorkflow(b, b.AllowedSubjects[0]), "concurrent-key", "fingerprint")
				results <- w
				errs <- e
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
		id := ""
		var w WorkflowWork
		for result := range results {
			if id != "" && id != result.Run.ID {
				t.Fatal("duplicate workflow")
			}
			id = result.Run.ID
			w = result
		}
		if _, _, e := s.CreateWorkflow(ctx, newWorkflow(b, b.AllowedSubjects[0]), "concurrent-key", "different"); !errors.Is(e, ErrConflict) {
			t.Fatal("different replay accepted", e)
		}
		for _, owner := range []ExecutionSubject{{Issuer: b.AllowedSubjects[0].Issuer, Subject: "bob"}, {Issuer: "https://foreign.test", Subject: "alice"}} {
			if _, e := s.Workflow(ctx, ws, id, owner); !errors.Is(e, ErrNotFound) {
				t.Fatal("cross identity leak", e)
			}
		}
		if _, e := s.Workflow(ctx, "other", id, w.Run.SubmittedBy); !errors.Is(e, ErrNotFound) {
			t.Fatal("cross workspace leak", e)
		}
	})
}
func TestWorkflowCrashBeforePostFencingAndNoResubmit(t *testing.T) {
	forSylonStores(t, func(t *testing.T, s Store, ws string) {
		ctx := context.Background()
		w := mustWorkflow(t, s, ws)
		_, lease, e := s.AcquireWorkflow(ctx, "crashed", time.Minute)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = s.AdvanceWorkflow(ctx, lease, WorkflowUpdate{State: "submitting", DispatchStarted: true, RetainLease: true}); e != nil {
			t.Fatal(e)
		}
		workflowExpire(t, s, w.Run.ID)
		backend := &fakeWorkflowBackend{binding: w.Binding, runErr: airflow.ErrNotFound}
		if _, e = ReconcileWorkflowOnce(ctx, s, backend, "replacement"); e != nil {
			t.Fatal(e)
		}
		if backend.posts != 0 {
			t.Fatal("reserved dispatch resent")
		}
		if _, e = s.AdvanceWorkflow(ctx, lease, WorkflowUpdate{State: "unknown"}); !errors.Is(e, ErrLeaseLost) {
			t.Fatal("stale lease persisted", e)
		}
	})
}
func TestWorkflowAdmissionAndRevocation(t *testing.T) {
	for _, mode := range []string{"paused", "before-dispatch", "after-read"} {
		t.Run(mode, func(t *testing.T) {
			s := NewMemoryStore()
			w := mustWorkflow(t, s, "default")
			backend := &fakeWorkflowBackend{binding: w.Binding}
			switch mode {
			case "paused":
				backend.paused = true
			case "before-dispatch":
				backend.onDefinition = func() { backend.revoked = true }
			case "after-read":
				backend.onRead = func() { backend.revoked = true }
			}
			if _, e := ReconcileWorkflowOnce(context.Background(), s, backend, "worker"); e != nil {
				t.Fatal(e)
			}
			got := readWorkflow(t, s, w)
			if !got.Run.ReconciliationComplete {
				t.Fatal("revocation not final")
			}
			if mode == "after-read" {
				if got.Run.State != "unknown" || backend.posts != 1 || got.Run.ObservedAt != nil || backend.taskReads != 0 {
					t.Fatal("revoked result exposed", got.Run)
				}
			} else if got.Run.State != "failed" || backend.posts != 0 {
				t.Fatal("inadmissible trigger", got.Run, backend.posts)
			}
		})
	}
}
func TestWorkflowRejectsWrongReceiptAndExpiredDispatch(t *testing.T) {
	forSylonStores(t, func(t *testing.T, s Store, ws string) {
		ctx := context.Background()
		w := mustWorkflow(t, s, ws)
		_, lease, e := s.AcquireWorkflow(ctx, "worker", time.Minute)
		if e != nil {
			t.Fatal(e)
		}
		w, e = s.AdvanceWorkflow(ctx, lease, WorkflowUpdate{State: "submitting", DispatchStarted: true, RetainLease: true})
		if e != nil {
			t.Fatal(e)
		}
		backend := &fakeWorkflowBackend{binding: w.Binding, wrongReceipt: true}
		observed, _ := backend.Run(ctx, w.Binding.Target(), w.Run.BackendRunID, w.Receipt())
		if _, e = s.AdvanceWorkflow(ctx, lease, WorkflowUpdate{State: "succeeded", Final: true, Observation: &observed}); !errors.Is(e, ErrConflict) {
			t.Fatal("wrong receipt finalized", e)
		}
		if _, e = s.AdvanceWorkflow(ctx, lease, WorkflowUpdate{State: "submitting", DispatchStarted: true, RetainLease: true}); !errors.Is(e, ErrConflict) {
			t.Fatal("second dispatch intent allowed", e)
		}
		old := w
		old.Run.DispatchStarted = false
		old.Run.CreatedAt = time.Now().Add(-time.Hour)
		if _, e = updateWorkflow(old, WorkflowUpdate{State: "submitting", DispatchStarted: true, RetainLease: true}, time.Now()); !errors.Is(e, ErrConflict) {
			t.Fatal("expired trigger allowed", e)
		}
	})
}
func TestWorkflowHTTPGrantRecoveryAndMetadataProjection(t *testing.T) {
	s := NewMemoryStore()
	b := workflowTestBinding("default")
	backend := &fakeWorkflowBackend{binding: b}
	server := &Server{Store: s, Auth: testAuth{p: executionPrincipal()}, Config: Config{AuthMode: "oidc", WorkflowBindings: []WorkflowBinding{b}}, WorkflowBackend: backend}
	h := server.Handler()
	base := "/api/v1/workspaces/default/"
	for _, path := range []string{"workflow-bindings", "workflow-bindings/daily/definition", "workflow-bindings/daily/history"} {
		response := request(h, "GET", base+path, "")
		if response.Code != 200 {
			t.Fatal(path, response.Code, response.Body)
		}
		for _, secret := range []string{"broker-token", "airflow.example", "must not expose", "allowedSubjects", "conf", "endpoint"} {
			if strings.Contains(response.Body.String(), secret) {
				t.Fatal("metadata leaked", path, secret)
			}
		}
	}
	response := request(h, "POST", base+"workflow-runs", `{"bindingId":"daily","idempotencyKey":"http-original-key","policyVersion":"policy-1","dagId":"daily_ingestion","dagVersion":1,"bundleVersion":"commit-abc"}`)
	if response.Code != 202 {
		t.Fatal(response.Code, response.Body)
	}
	var receipt struct {
		Run      WorkflowRun
		Replayed bool
	}
	json.Unmarshal(response.Body.Bytes(), &receipt)
	response = request(h, "POST", base+"workflow-runs", `{"bindingId":"daily","idempotencyKey":"http-original-key","policyVersion":"policy-1","dagId":"daily_ingestion","dagVersion":1,"bundleVersion":"commit-abc"}`)
	var replay struct {
		Run      WorkflowRun
		Replayed bool
	}
	json.Unmarshal(response.Body.Bytes(), &replay)
	if response.Code != 202 || !replay.Replayed || replay.Run.ID != receipt.Run.ID {
		t.Fatal("lost response recovery", response.Body)
	}
	for _, body := range []string{`{"bindingId":"daily","idempotencyKey":"http-original-key","policyVersion":"policy-1","dagId":"daily_ingestion","dagVersion":1,"bundleVersion":"commit-abc","conf":{}}`, `{"bindingId":"daily","idempotencyKey":"short"}`} {
		if r := request(h, "POST", base+"workflow-runs", body); r.Code != 400 {
			t.Fatal(r.Code, r.Body)
		}
	}
	server.Config.WorkflowBindings[0].PolicyVersion = "new-policy"
	if r := request(h, "GET", base+"workflow-runs/"+receipt.Run.ID, ""); r.Code != 403 {
		t.Fatal("changed policy leaked record", r.Code)
	}
	if r := request(h, "POST", base+"workflow-runs", `{"bindingId":"daily","idempotencyKey":"http-original-key","policyVersion":"policy-1","dagId":"daily_ingestion","dagVersion":1,"bundleVersion":"commit-abc"}`); r.Code != 409 {
		t.Fatal("revoked replay leaked record", r.Code)
	}
	server.Config.WorkflowBindings[0] = b
	other := executionPrincipal()
	other.Subject = "bob"
	server.Auth = testAuth{p: other}
	if r := request(server.Handler(), "GET", base+"workflow-runs/"+receipt.Run.ID, ""); r.Code != 404 {
		t.Fatal("owner leak", r.Code)
	}
	viewer := executionPrincipal()
	viewer.Workspaces["default"] = "viewer"
	server.Auth = testAuth{p: viewer}
	if r := request(server.Handler(), "POST", base+"workflow-runs", `{"bindingId":"daily","idempotencyKey":"viewer-original"}`); r.Code != 403 {
		t.Fatal("viewer trigger", r.Code)
	}
}
func TestWorkflowBindingValidationAndStrictLoad(t *testing.T) {
	good := workflowTestBinding("default")
	if e := good.Validate(); e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"http", "path", "version", "dag", "bundle", "secret", "metadata", "subjects", "deadline"} {
		t.Run(name, func(t *testing.T) {
			b := good
			switch name {
			case "http":
				b.Endpoint = "http://airflow.test"
			case "path":
				b.Endpoint += "/api"
			case "version":
				b.AirflowVersion = "3.3.2"
			case "dag":
				b.DAGID = "../../admin"
			case "bundle":
				b.BundleVersion = ""
			case "secret":
				b.TokenFile = "relative"
			case "metadata":
				b.MetadataAccessAccepted = false
			case "subjects":
				b.AllowedSubjects = nil
			case "deadline":
				b.MaxRunSeconds = 86401
			}
			if b.Validate() == nil {
				t.Fatal("unsafe binding accepted")
			}
		})
	}
	path := filepath.Join(t.TempDir(), "bindings.json")
	data, _ := json.Marshal([]WorkflowBinding{good})
	os.WriteFile(path, data, 0600)
	if _, e := LoadWorkflowBindings(path); e != nil {
		t.Fatal(e)
	}
	for _, data := range []string{"null", `[{"token":"secret"}]`, fmt.Sprintf("[%s,%s]", mustJSON(good), mustJSON(good))} {
		os.WriteFile(path, []byte(data), 0600)
		if _, e := LoadWorkflowBindings(path); e == nil {
			t.Fatal("invalid file accepted")
		}
	}
}
func mustJSON(v any) string { b, _ := json.Marshal(v); return string(b) }

func TestWorkflowLeaseExpiresWhileWaitingForRowLock(t *testing.T) {
	s, ws := searchTestStore(t)
	ctx := context.Background()
	w := mustWorkflow(t, s, ws)
	_, lease, e := s.AcquireWorkflow(ctx, "held-worker", 100*time.Millisecond)
	if e != nil {
		t.Fatal(e)
	}
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, "SELECT id FROM dataslate_workflows WHERE id=$1 FOR UPDATE", w.Run.ID); e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() {
		_, e := s.AdvanceWorkflow(ctx, lease, WorkflowUpdate{State: "submitting", DispatchStarted: true, RetainLease: true})
		done <- e
	}()
	time.Sleep(150 * time.Millisecond)
	if e = tx.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	if e = <-done; !errors.Is(e, ErrLeaseLost) {
		t.Fatal("expired lease sent dispatch after row-lock wait", e)
	}
	got := readWorkflow(t, s, w)
	if got.Run.DispatchStarted {
		t.Fatal("expired lease changed durable intent")
	}
}
func TestWorkflowLateObservationCannotFinalizeAndDeadlineDoesNotCancel(t *testing.T) {
	forSylonStores(t, func(t *testing.T, s Store, ws string) {
		ctx := context.Background()
		b := workflowTestBinding(ws)
		w := newWorkflow(b, b.AllowedSubjects[0])
		w.Run.CreatedAt = time.Now().Add(-299 * time.Second)
		w, _, e := s.CreateWorkflow(ctx, w, "late-observation", "fingerprint")
		if e != nil {
			t.Fatal(e)
		}
		_, lease, e := s.AcquireWorkflow(ctx, "worker", time.Minute)
		if e != nil {
			t.Fatal(e)
		}
		w, e = s.AdvanceWorkflow(ctx, lease, WorkflowUpdate{State: "submitting", DispatchStarted: true, RetainLease: true})
		if e != nil {
			t.Fatal(e)
		}
		// Advance authoritative resource time in the test store without touching the lease.
		switch db := s.(type) {
		case *MemoryStore:
			db.mu.Lock()
			db.workflows[w.Run.ID].Work.Run.CreatedAt = time.Now().Add(-301 * time.Second)
			db.mu.Unlock()
		case *PostgresStore:
			if _, e = db.pool.Exec(ctx, `UPDATE dataslate_workflows SET document=jsonb_set(document,'{Run,createdAt}',to_jsonb(clock_timestamp()-interval '301 seconds')) WHERE id=$1`, w.Run.ID); e != nil {
				t.Fatal(e)
			}
		}
		backend := &fakeWorkflowBackend{binding: b, state: "success"}
		observed, _ := backend.Run(ctx, b.Target(), w.Run.BackendRunID, w.Receipt())
		if _, e = s.AdvanceWorkflow(ctx, lease, WorkflowUpdate{State: "succeeded", Final: true, Observation: &observed}); !errors.Is(e, ErrConflict) {
			t.Fatal("late observation finalized", e)
		}
		workflowExpire(t, s, w.Run.ID)
		if _, e = ReconcileWorkflowOnce(ctx, s, backend, "after-deadline"); e != nil {
			t.Fatal(e)
		}
		got := readWorkflow(t, s, w)
		if got.Run.State != "unknown" || !got.Run.ReconciliationComplete || backend.posts != 0 {
			t.Fatal("deadline represented as cancellation or retried", got.Run)
		}
	})
}

func workflowReviewedInput(b WorkflowBinding, key string) WorkflowInput {
	return WorkflowInput{BindingID: b.ID, IdempotencyKey: key, PolicyVersion: b.PolicyVersion, DAGID: b.DAGID, DAGVersion: b.DAGVersion, BundleVersion: b.BundleVersion}
}
func workflowInputJSON(t *testing.T, in WorkflowInput) string {
	t.Helper()
	data, e := json.Marshal(in)
	if e != nil {
		t.Fatal(e)
	}
	return string(data)
}
func TestWorkflowHTTPReviewedIntentBeforePersistence(t *testing.T) {
	for _, field := range []string{"policyVersion", "dagId", "dagVersion", "bundleVersion"} {
		t.Run(field, func(t *testing.T) {
			forSylonStores(t, func(t *testing.T, store Store, ws string) {
				b := workflowTestBinding(ws)
				p := executionPrincipal()
				p.Workspaces[ws] = "editor"
				backend := &fakeWorkflowBackend{binding: b}
				server := &Server{Store: store, Auth: testAuth{p: p}, Config: Config{AuthMode: "oidc", WorkflowBindings: []WorkflowBinding{b}}, WorkflowBackend: backend}
				h := server.Handler()
				path := "/api/v1/workspaces/" + ws + "/workflow-runs"
				in := workflowReviewedInput(b, "reviewed-original")
				body := workflowInputJSON(t, in)
				// An administrator changes one reviewed field after review but before POST.
				changed := b
				switch field {
				case "policyVersion":
					changed.PolicyVersion = "policy-2"
				case "dagId":
					changed.DAGID = "replacement_dag"
				case "dagVersion":
					changed.DAGVersion = 2
				case "bundleVersion":
					changed.BundleVersion = "commit-def"
				}
				server.Config.WorkflowBindings[0] = changed
				response := request(h, "POST", path, body)
				if response.Code != 409 || !strings.Contains(response.Body.String(), "workflow_review_changed") {
					t.Fatal("stale approval accepted", response.Code, response.Body)
				}
				works, e := store.Workflows(context.Background(), ws, b.AllowedSubjects[0])
				if e != nil || len(works) != 0 {
					t.Fatal("stale approval persisted a run", len(works), e)
				}
				if _, _, e = store.AcquireWorkflow(context.Background(), "unreviewed-worker", time.Minute); !errors.Is(e, ErrNotFound) {
					t.Fatal("stale approval is dispatchable", e)
				}
				if backend.posts != 0 || backend.taskReads != 0 {
					t.Fatal("stale review contacted backend")
				}
				// Persist the original, then change the current binding. A replay of
				// the old input must fail review; a new tuple with the same key must
				// conflict with the original fingerprint, never create a replacement.
				server.Config.WorkflowBindings[0] = b
				response = request(h, "POST", path, body)
				if response.Code != 202 {
					t.Fatal(response.Code, response.Body)
				}
				var first struct{ Run WorkflowRun }
				if json.Unmarshal(response.Body.Bytes(), &first) != nil {
					t.Fatal("invalid receipt")
				}
				server.Config.WorkflowBindings[0] = changed
				if response = request(h, "POST", path, body); response.Code != 409 {
					t.Fatal("old approval replay accepted", response.Code)
				}
				if response = request(h, "POST", path, workflowInputJSON(t, workflowReviewedInput(changed, in.IdempotencyKey))); response.Code != 409 {
					t.Fatal("changed tuple reused old key", response.Code, response.Body)
				}
				works, e = store.Workflows(context.Background(), ws, b.AllowedSubjects[0])
				if e != nil || len(works) != 1 || works[0].Run.ID != first.Run.ID {
					t.Fatal("replacement workflow persisted", len(works), e)
				}
				server.Config.WorkflowBindings[0] = b
				response = request(h, "POST", path, body)
				if response.Code != 202 || !strings.Contains(response.Body.String(), `"replayed":true`) {
					t.Fatal("exact original recovery failed", response.Code, response.Body)
				}
			})
		})
	}
}
func TestWorkflowHTTPRequiresCompleteTypedReviewedIntent(t *testing.T) {
	b := workflowTestBinding("default")
	s := NewMemoryStore()
	h := (&Server{Store: s, Auth: testAuth{p: executionPrincipal()}, Config: Config{AuthMode: "oidc", WorkflowBindings: []WorkflowBinding{b}}}).Handler()
	path := "/api/v1/workspaces/default/workflow-runs"
	in := workflowReviewedInput(b, "reviewed-original")
	for _, field := range []string{"policyVersion", "dagId", "dagVersion", "bundleVersion"} {
		var fields map[string]any
		json.Unmarshal([]byte(workflowInputJSON(t, in)), &fields)
		delete(fields, field)
		body, _ := json.Marshal(fields)
		if r := request(h, "POST", path, string(body)); r.Code != 400 {
			t.Fatal("missing reviewed field accepted", field, r.Code)
		}
	}
	for _, value := range []any{0, -1, 1000001, true, "1", 1.5, nil} {
		var fields map[string]any
		json.Unmarshal([]byte(workflowInputJSON(t, in)), &fields)
		fields["dagVersion"] = value
		body, _ := json.Marshal(fields)
		if r := request(h, "POST", path, string(body)); r.Code != 400 {
			t.Fatal("invalid DAG version accepted", value, r.Code)
		}
	}
	for _, field := range []string{"policyVersion", "dagId", "bundleVersion"} {
		var fields map[string]any
		json.Unmarshal([]byte(workflowInputJSON(t, in)), &fields)
		fields[field] = strings.Repeat("a", 300)
		body, _ := json.Marshal(fields)
		if r := request(h, "POST", path, string(body)); r.Code != 400 {
			t.Fatal("unbounded reviewed field accepted", field, r.Code)
		}
	}
	works, e := s.Workflows(context.Background(), b.WorkspaceID, b.AllowedSubjects[0])
	if e != nil || len(works) != 0 {
		t.Fatal("invalid input persisted", e)
	}
}
