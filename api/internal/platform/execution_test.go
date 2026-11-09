package platform

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func executionTestBinding() ExecutionBinding {
	return ExecutionBinding{ID: "approved-app", WorkspaceID: "default", Name: "Approved app", ExecutionProfile: ExecutionProfile{Operator: "apache", Image: "example.test/data/app@sha256:" + strings.Repeat("a", 64), MainApplicationFile: "local:///opt/spark/app.py", Namespace: "analytics", ServiceAccount: "spark-approved", ExecutorInstances: 1}, AllowedSubjects: []ExecutionSubject{{Issuer: "https://identity.example.test", Subject: "alice"}}}
}
func executionPrincipal() Principal {
	return Principal{Issuer: "https://identity.example.test", Subject: "alice", Workspaces: map[string]string{"default": "editor"}}
}
func executionHandler(store Store, p Principal, b []ExecutionBinding, backend ExecutionBackend) http.Handler {
	return (&Server{Config: Config{AuthMode: "oidc", ExecutionBindings: b}, Store: store, Auth: testAuth{p: p}, Backend: backend}).Handler()
}
func mustRun(t *testing.T, h http.Handler, key string) Run {
	t.Helper()
	response := request(h, "POST", "/api/v1/workspaces/default/runs", `{"bindingId":"approved-app","idempotencyKey":"`+key+`"}`)
	if response.Code != 202 {
		t.Fatalf("create: %d %s", response.Code, response.Body)
	}
	var r Run
	if err := json.Unmarshal(response.Body.Bytes(), &r); err != nil {
		t.Fatal(err)
	}
	return r
}
func TestExecutionBindingSecurity(t *testing.T) {
	valid := executionTestBinding()
	denied := valid
	denied.AllowedSubjects = []ExecutionSubject{}
	if err := denied.Validate(); err != nil {
		t.Fatal("explicit deny-all binding rejected", err)
	}
	if denied.Allows(valid.AllowedSubjects[0]) {
		t.Fatal("deny-all binding granted execution")
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*ExecutionBinding){func(b *ExecutionBinding) { b.Image = "example.test/app:1.0" }, func(b *ExecutionBinding) { b.MainApplicationFile = "s3://lake/job.py" }, func(b *ExecutionBinding) { b.Operator = "kubeflow" }, func(b *ExecutionBinding) { b.AllowedSubjects = nil }, func(b *ExecutionBinding) { b.Namespace = "../default" }, func(b *ExecutionBinding) { b.AllowedSubjects = []ExecutionSubject{{Issuer: "", Subject: "alice"}} }, func(b *ExecutionBinding) { b.ExecutorInstances = 101 }} {
		b := valid
		change(&b)
		if b.Validate() == nil {
			t.Fatal("unsafe execution binding accepted")
		}
	}
	path := filepath.Join(t.TempDir(), "bindings.json")
	raw, _ := json.Marshal([]ExecutionBinding{valid})
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	bindings, err := LoadExecutionBindings(path)
	if err != nil || len(bindings) != 1 {
		t.Fatalf("bindings: %v", err)
	}
	for _, raw := range []string{`[] {}`, `[{"id":"extra","unknown":true}]`, `null {}`, `{}`} {
		if err = os.WriteFile(path, []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err = LoadExecutionBindings(path); err == nil {
			t.Fatal("invalid config accepted")
		}
	}
}
func TestExecutionAuthorizationAndIdempotency(t *testing.T) {
	s := NewMemoryStore()
	b := executionTestBinding()
	h := executionHandler(s, executionPrincipal(), []ExecutionBinding{b}, nil)
	response := request(h, "GET", "/api/v1/workspaces/default/execution-bindings", "")
	if response.Code != 200 || strings.Contains(response.Body.String(), "allowedSubjects") {
		t.Fatalf("binding discovery leaks grants: %s", response.Body)
	}
	r := mustRun(t, h, "idem-key-0001")
	if r.State != "queued" || r.SubmissionStarted || r.KubernetesUID != "" {
		t.Fatal("unexecuted work reported as started")
	}
	response = request(h, "POST", "/api/v1/workspaces/default/runs", `{"bindingId":"approved-app","idempotencyKey":"idem-key-0001"}`)
	if response.Code != 200 || !strings.Contains(response.Body.String(), r.ID) {
		t.Fatal("idempotent request did not reopen original run")
	}
	b2 := b
	b2.ID = "other-approved"
	h = executionHandler(s, executionPrincipal(), []ExecutionBinding{b, b2}, nil)
	response = request(h, "POST", "/api/v1/workspaces/default/runs", `{"bindingId":"other-approved","idempotencyKey":"idem-key-0001"}`)
	if response.Code != 409 {
		t.Fatalf("conflicting retry: %d", response.Code)
	}
	for _, tc := range []struct {
		name   string
		p      Principal
		create int
		read   int
	}{{"viewer", Principal{Issuer: b.AllowedSubjects[0].Issuer, Subject: "alice", Workspaces: map[string]string{"default": "viewer"}}, 403, 200}, {"other-owner", Principal{Issuer: b.AllowedSubjects[0].Issuer, Subject: "bob", Workspaces: map[string]string{"default": "admin"}}, 403, 404}, {"other-issuer", Principal{Issuer: "https://other.example.test", Subject: "alice", Workspaces: map[string]string{"default": "admin"}}, 403, 404}, {"platform-admin", Principal{Issuer: b.AllowedSubjects[0].Issuer, Subject: "bob", PlatformAdmin: true}, 403, 403}} {
		t.Run(tc.name, func(t *testing.T) {
			h := executionHandler(s, tc.p, []ExecutionBinding{b}, nil)
			for _, suffix := range []string{"", "/events", "/logs"} {
				want := tc.read
				if suffix == "/logs" && want == 200 {
					want = 501
				}
				w := request(h, "GET", "/api/v1/workspaces/default/runs/"+r.ID+suffix, "")
				if w.Code != want {
					t.Fatalf("read %s: %d want%d", suffix, w.Code, want)
				}
			}
			w := request(h, "POST", "/api/v1/workspaces/default/runs", `{"bindingId":"approved-app","idempotencyKey":"owner-other-key"}`)
			if w.Code != tc.create {
				t.Fatalf("create: %d want%d", w.Code, tc.create)
			}
			w = request(h, "POST", "/api/v1/workspaces/default/runs/"+r.ID+"/cancel", `{}`)
			want := tc.read
			if want == 200 {
				want = 403
			}
			if w.Code != want {
				t.Fatalf("cancel: %d want%d", w.Code, want)
			}
		})
	}
	for _, body := range []string{`{"bindingId":"approved-app","idempotencyKey":"id-key-010","image":"attacker:latest"}`, `{"bindingId":"approved-app","idempotencyKey":"id-key-010","parameters":["--steal"]}`} {
		if w := request(h, "POST", "/api/v1/workspaces/default/runs", body); w.Code != 400 {
			t.Fatal("client execution override accepted")
		}
	}
	response = request(h, "POST", "/api/v1/workspaces/default/runs/"+r.ID+"/cancel", `{}`)
	if response.Code != 202 || !strings.Contains(response.Body.String(), `"state":"cancel_requested"`) {
		t.Fatalf("cancel response: %s", response.Body)
	}
}
func TestMemoryExecutionConcurrencyAndFencing(t *testing.T) {
	s := NewMemoryStore()
	exerciseExecutionStore(t, s, "default")
}

// Keep fresh-lease assertions independent of network/scheduler latency, then
// expire only the expected lease without changing its owner or fencing token.
func expireExecutionLease(t *testing.T, s Store, lease RunLease) {
	t.Helper()
	switch db := s.(type) {
	case *MemoryStore:
		db.mu.Lock()
		defer db.mu.Unlock()
		entry := db.executions[lease.RunID]
		if entry == nil || entry.Owner != lease.Owner || entry.Fence != lease.Fence || !entry.Until.After(time.Now()) {
			t.Fatal("expected a fresh execution lease before forced expiry")
		}
		entry.Until = time.Now().Add(-time.Second)
	case *PostgresStore:
		tag, err := db.pool.Exec(context.Background(), `UPDATE dataslate_runs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1 AND lease_owner=$2 AND lease_fence=$3 AND lease_until>clock_timestamp()`, lease.RunID, lease.Owner, lease.Fence)
		if err != nil {
			t.Fatal(err)
		}
		if tag.RowsAffected() != 1 {
			t.Fatal("expected exactly one fresh execution lease before forced expiry")
		}
	default:
		t.Fatalf("unsupported execution lease fixture %T", s)
	}
}

func exerciseExecutionStore(t *testing.T, s Store, ws string) {
	t.Helper()
	ctx := context.Background()
	b := executionTestBinding()
	b.WorkspaceID = ws
	p := b.AllowedSubjects[0]
	key := newResource("key-").ID
	var wg sync.WaitGroup
	results := make(chan Run, 12)
	errs := make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Go(func() {
			r, _, err := s.CreateRun(ctx, newRun(ws, b, p), key, "same-request")
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
	id := ""
	for r := range results {
		if id != "" && r.ID != id {
			t.Fatal("concurrent idempotency created multiple runs")
		}
		id = r.ID
	}
	if _, _, err := s.CreateRun(ctx, newRun(ws, b, p), key, "different-request"); !errors.Is(err, ErrConflict) {
		t.Fatal("key reuse accepted different request")
	}
	r, first, err := s.AcquireRun(ctx, "first-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if r.ID != id {
		t.Fatalf("unexpected leased run %s != %s", r.ID, id)
	}
	if _, _, err = s.AcquireRun(ctx, "second-worker", time.Minute); !errors.Is(err, ErrNotFound) {
		t.Fatal("simultaneous live leases")
	}
	expireExecutionLease(t, s, first)
	if _, err = s.AdvanceRun(ctx, first, RunUpdate{State: "running", Message: "expired worker"}); !errors.Is(err, ErrLeaseLost) {
		t.Fatal("expired worker wrote state before lease takeover")
	}
	_, second, err := s.AcquireRun(ctx, "second-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if second.RunID != first.RunID || second.Fence <= first.Fence {
		t.Fatal("lease takeover did not advance the same run's fence")
	}
	if _, err = s.AdvanceRun(ctx, first, RunUpdate{State: "running", Message: "stale worker"}); !errors.Is(err, ErrLeaseLost) {
		t.Fatal("expired worker wrote state")
	}
	staleFence := second
	staleFence.Fence = first.Fence
	if _, err = s.AdvanceRun(ctx, staleFence, RunUpdate{State: "running", Message: "stale fence"}); !errors.Is(err, ErrLeaseLost) {
		t.Fatal("current owner wrote state with a stale fencing token")
	}
	if _, err = s.CancelRun(ctx, ws, id, p); err != nil {
		t.Fatal(err)
	}
	r, err = s.AdvanceRun(ctx, second, RunUpdate{State: "running", UID: "uid-1", Message: "Running", Observed: true})
	if err != nil {
		t.Fatal(err)
	}
	if !r.CancelRequested || r.State != "cancel_requested" || r.ObservedAt == nil {
		t.Fatal("controller overwrote concurrent cancellation")
	}
	if _, err = s.Run(ctx, ws, id, ExecutionSubject{Issuer: p.Issuer, Subject: "other"}); !errors.Is(err, ErrNotFound) {
		t.Fatal("cross-owner read")
	}
	if _, err = s.Run(ctx, "other-workspace", id, p); !errors.Is(err, ErrNotFound) {
		t.Fatal("cross-workspace read")
	}
	_, third, err := s.AcquireRun(ctx, "third-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.AdvanceRun(ctx, third, RunUpdate{State: "cancelled", UID: "uid-1", Message: "Deleted", Observed: true}); err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.AcquireRun(ctx, "fourth-worker", time.Minute); !errors.Is(err, ErrNotFound) {
		t.Fatal("terminal run was leased again")
	}
}

type fakeExecutionBackend struct {
	observation                      BackendObservation
	observeErr, submitErr, cancelErr error
	submits, cancels                 int
	store                            *MemoryStore
	submitAfterCancel                bool
}

func (b *fakeExecutionBackend) Observe(context.Context, Run) (BackendObservation, error) {
	return b.observation, b.observeErr
}
func (b *fakeExecutionBackend) Submit(_ context.Context, r Run) (BackendObservation, error) {
	b.submits++
	if !r.SubmissionStarted {
		panic("backend effect before durable checkpoint")
	}
	if b.store != nil {
		stored, _ := b.store.Run(context.Background(), r.WorkspaceID, r.ID, r.SubmittedBy)
		if !stored.SubmissionStarted {
			panic("checkpoint not durable")
		}
	}
	if b.submitErr != nil {
		return BackendObservation{}, b.submitErr
	}
	b.observation = BackendObservation{Exists: true, UID: "test-uid", State: "running", Message: "Application is running."}
	return b.observation, nil
}
func (b *fakeExecutionBackend) Cancel(context.Context, Run) (BackendObservation, error) {
	b.cancels++
	if b.cancelErr != nil {
		return BackendObservation{}, b.cancelErr
	}
	b.observation = BackendObservation{}
	return b.observation, nil
}
func (b *fakeExecutionBackend) Logs(context.Context, Run) (RunLogs, error) {
	return RunLogs{Text: "driver output", Truncated: false}, nil
}
func dueRun(s *MemoryStore, id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.executions[id].Next = time.Time{}
}
func TestReconcileSubmissionRecoveryAndCancellation(t *testing.T) {
	for _, mode := range []string{"complete", "cancel-before-submit", "cancel-running", "lost-response", "disappeared-resource", "backend-offline", "foreign-uid"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			s := NewMemoryStore()
			b := executionTestBinding()
			r, _, _ := s.CreateRun(ctx, newRun("default", b, b.AllowedSubjects[0]), "test-idempotency", "fingerprint")
			backend := &fakeExecutionBackend{store: s}
			if mode == "cancel-before-submit" {
				s.CancelRun(ctx, "default", r.ID, r.SubmittedBy)
			}
			if mode == "lost-response" {
				backend.submitErr = errors.New("network timeout")
			}
			if mode == "backend-offline" {
				backend.observeErr = errors.New("network unavailable")
			}
			if worked, err := ReconcileOnce(ctx, s, backend, "controller"); err != nil || !worked {
				t.Fatalf("reconcile: %v", err)
			}
			r, _ = s.Run(ctx, "default", r.ID, r.SubmittedBy)
			switch mode {
			case "cancel-before-submit":
				if backend.submits != 0 || r.State != "cancelled" {
					t.Fatal("cancelled queued job submitted")
				}
				return
			case "backend-offline":
				if backend.submits != 0 || r.State != "unknown" || r.ObservedAt != nil {
					t.Fatal("offline backend claimed observation")
				}
				return
			case "lost-response":
				if r.State != "unknown" || !r.SubmissionStarted {
					t.Fatal("ambiguous outcome not recorded")
				}
			case "cancel-running":
				s.CancelRun(ctx, "default", r.ID, r.SubmittedBy)
			case "disappeared-resource":
				backend.observation = BackendObservation{}
			case "foreign-uid":
				backend.observation.UID = "replacement-uid"
			case "complete":
				backend.observation.State = "succeeded"
				backend.observation.Message = "Application completed successfully."
			}
			dueRun(s, r.ID)
			if _, err := ReconcileOnce(ctx, s, backend, "controller-2"); err != nil {
				t.Fatal(err)
			}
			r, _ = s.Run(ctx, "default", r.ID, r.SubmittedBy)
			if backend.submits != 1 {
				t.Fatal("reconciliation duplicated submission")
			}
			switch mode {
			case "complete":
				if r.State != "succeeded" {
					t.Fatal(r.State)
				}
			case "cancel-running":
				if r.State != "cancelled" || backend.cancels != 1 {
					t.Fatal("cancellation did not confirm deletion")
				}
			default:
				if r.State != "unknown" {
					t.Fatal("ambiguous state was misrepresented")
				}
			}
		})
	}
}
func TestRunLogsRequireOwnerAndMembership(t *testing.T) {
	s := NewMemoryStore()
	b := executionTestBinding()
	backend := &fakeExecutionBackend{}
	h := executionHandler(s, executionPrincipal(), []ExecutionBinding{b}, backend)
	r := mustRun(t, h, "logs-key-0001")
	if w := request(h, "GET", "/api/v1/workspaces/default/runs/"+r.ID+"/logs", ""); w.Code != 200 || !strings.Contains(w.Body.String(), "driver output") {
		t.Fatal(w.Body)
	}
	p := executionPrincipal()
	p.Workspaces = nil
	h = executionHandler(s, p, []ExecutionBinding{b}, backend)
	if w := request(h, "GET", "/api/v1/workspaces/default/runs/"+r.ID+"/logs", ""); w.Code != 403 {
		t.Fatal("revoked membership retained log access")
	}
}

func exerciseExecutionCapacity(t *testing.T, s Store, ws string) {
	t.Helper()
	ctx := context.Background()
	b := executionTestBinding()
	b.WorkspaceID = ws
	p := b.AllowedSubjects[0]
	for i := 0; i < MaxActiveWorkspaceRuns-5; i++ {
		if _, _, err := s.CreateRun(ctx, newRun(ws, b, p), newResource("key-").ID, "same"); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Go(func() { _, _, e := s.CreateRun(ctx, newRun(ws, b, p), newResource("key-").ID, "same"); errs <- e })
	}
	wg.Wait()
	close(errs)
	accepted, denied := 0, 0
	for err := range errs {
		switch {
		case err == nil:
			accepted++
		case errors.Is(err, ErrRunCapacity):
			denied++
		default:
			t.Fatal(err)
		}
	}
	if accepted != 5 || denied != 15 {
		t.Fatalf("capacity race accepted=%d denied=%d", accepted, denied)
	}
	r, lease, err := s.AcquireRun(ctx, "capacity-worker", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.AdvanceRun(ctx, lease, RunUpdate{State: "succeeded", Message: "Complete", Observed: true}); err != nil {
		t.Fatal(err)
	}
	key := newResource("replacement-").ID
	next, _, err := s.CreateRun(ctx, newRun(ws, b, p), key, "same")
	if err != nil {
		t.Fatal("terminal work did not free capacity", err)
	}
	replay, isReplay, err := s.CreateRun(ctx, newRun(ws, b, p), key, "same")
	if err != nil || !isReplay || replay.ID != next.ID {
		t.Fatal("idempotent replay rejected at capacity")
	}
	other := p
	other.Subject = "another-submitter"
	if _, _, err = s.CreateRun(ctx, newRun(ws, b, other), newResource("key-").ID, "same"); !errors.Is(err, ErrRunCapacity) {
		t.Fatal("capacity was incorrectly isolated per subject")
	}
	if r.ID == next.ID {
		t.Fatal("new run reused a terminal identity")
	}
}
func TestMemoryExecutionCapacity(t *testing.T) {
	exerciseExecutionCapacity(t, NewMemoryStore(), "default")
}
func TestBoundedRunEvents(t *testing.T) {
	b := executionTestBinding()
	r := newRun("default", b, b.AllowedSubjects[0])
	for i := 0; i < 3*MaxRunEvents; i++ {
		state := "running"
		if i%2 == 0 {
			state = "unknown"
		}
		var err error
		r, err = applyRunUpdate(r, RunUpdate{State: state, Message: state}, time.Now())
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(r.Events) != MaxRunEvents || !r.EventsTruncated || r.Events[len(r.Events)-1].State != r.State {
		t.Fatal("event history is not bounded or latest event lost")
	}
}
func TestExecutionCapacityHTTP(t *testing.T) {
	s := NewMemoryStore()
	b := executionTestBinding()
	for i := 0; i < MaxActiveWorkspaceRuns; i++ {
		s.CreateRun(context.Background(), newRun("default", b, b.AllowedSubjects[0]), newResource("key-").ID, "same")
	}
	h := executionHandler(s, executionPrincipal(), []ExecutionBinding{b}, nil)
	w := request(h, "POST", "/api/v1/workspaces/default/runs", `{"bindingId":"approved-app","idempotencyKey":"capacity-blocked"}`)
	if w.Code != 429 || !strings.Contains(w.Body.String(), "run_capacity_exceeded") {
		t.Fatalf("capacity HTTP: %d %s", w.Code, w.Body)
	}
}

type policyExecutionBackend struct {
	fakeExecutionBackend
	allowed   bool
	policyErr error
	observes  int
}

func (b *policyExecutionBackend) Authorize(context.Context, Run) (bool, error) {
	return b.allowed, b.policyErr
}
func (b *policyExecutionBackend) Observe(ctx context.Context, r Run) (BackendObservation, error) {
	b.observes++
	return b.fakeExecutionBackend.Observe(ctx, r)
}
func TestExecutionRevocationAndUnavailablePolicy(t *testing.T) {
	for _, mode := range []string{"revoked-queued", "revoked-running", "policy-unavailable"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			s := NewMemoryStore()
			b := executionTestBinding()
			r, _, _ := s.CreateRun(ctx, newRun("default", b, b.AllowedSubjects[0]), "policy-test-key", "same")
			backend := &policyExecutionBackend{fakeExecutionBackend: fakeExecutionBackend{store: s}}
			if mode == "revoked-running" {
				backend.allowed = true
				if _, err := ReconcileOnce(ctx, s, backend, "first"); err != nil {
					t.Fatal(err)
				}
				dueRun(s, r.ID)
				backend.allowed = false
			}
			if mode == "policy-unavailable" {
				backend.policyErr = errors.New("invalid mounted policy")
			}
			beforeObserves := backend.observes
			if _, err := ReconcileOnce(ctx, s, backend, "second"); err != nil {
				t.Fatal(err)
			}
			r, _ = s.Run(ctx, "default", r.ID, r.SubmittedBy)
			if mode == "policy-unavailable" {
				if backend.observes != beforeObserves || backend.submits != 0 || r.State != "unknown" {
					t.Fatal("unavailable policy permitted an external operation")
				}
				return
			}
			if r.State != "cancelled" || !r.CancelRequested {
				t.Fatal("revoked work not cancelled")
			}
			if mode == "revoked-queued" && backend.submits != 0 {
				t.Fatal("revoked queued job submitted")
			}
			if mode == "revoked-running" && backend.cancels != 1 {
				t.Fatal("revoked running job not deleted")
			}
		})
	}
}
func TestCancellationLostDeleteResponseRetainsUID(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	b := executionTestBinding()
	r, _, _ := s.CreateRun(ctx, newRun("default", b, b.AllowedSubjects[0]), "delete-response-key", "same")
	_, lease, _ := s.AcquireRun(ctx, "pre-submit", time.Second)
	s.AdvanceRun(ctx, lease, RunUpdate{State: "unknown", SubmissionStarted: true, Message: "Lost POST response"})
	s.CancelRun(ctx, "default", r.ID, r.SubmittedBy)
	backend := &fakeExecutionBackend{observation: BackendObservation{Exists: true, UID: "confirmed-before-delete", State: "running", Message: "Running"}, cancelErr: errors.New("lost DELETE response")}
	if _, err := ReconcileOnce(ctx, s, backend, "cancel-worker"); err != nil {
		t.Fatal(err)
	}
	r, _ = s.Run(ctx, "default", r.ID, r.SubmittedBy)
	if r.KubernetesUID != "confirmed-before-delete" {
		t.Fatal("observed UID not persisted before deletion")
	}
	backend.observation = BackendObservation{}
	dueRun(s, r.ID)
	if _, err := ReconcileOnce(ctx, s, backend, "recover-worker"); err != nil {
		t.Fatal(err)
	}
	r, _ = s.Run(ctx, "default", r.ID, r.SubmittedBy)
	if r.State != "cancelled" {
		t.Fatal("lost delete response could not be reconciled")
	}
}

func TestPolicyUnavailableDoesNotBlockRequestedOrExpiredRunCleanup(t *testing.T) {
	for _, mode := range []string{"user-cancellation", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			s := NewMemoryStore()
			binding := executionTestBinding()
			binding.MaxRunSeconds = 60
			r, _, err := s.CreateRun(ctx, newRun("default", binding, binding.AllowedSubjects[0]), "cleanup-policy-key", "same")
			if err != nil {
				t.Fatal(err)
			}
			backend := &policyExecutionBackend{fakeExecutionBackend: fakeExecutionBackend{store: s}, allowed: true}
			if _, err = ReconcileOnce(ctx, s, backend, "submit-worker"); err != nil {
				t.Fatal(err)
			}
			stored, err := s.Run(ctx, "default", r.ID, r.SubmittedBy)
			if err != nil || stored.KubernetesUID == "" {
				t.Fatal("test did not persist a real observed resource identity", err)
			}
			backend.policyErr = errors.New("mounted policy file is unavailable")
			if mode == "user-cancellation" {
				if _, err = s.CancelRun(ctx, "default", r.ID, r.SubmittedBy); err != nil {
					t.Fatal(err)
				}
			}
			dueRun(s, r.ID)
			clock := time.Now
			if mode == "deadline" {
				clock = func() time.Time { return stored.ExecutionDeadline().Add(time.Second) }
			}
			if _, err = reconcileOnce(ctx, s, backend, "cleanup-worker", clock); err != nil {
				t.Fatal(err)
			}
			after, err := s.Run(ctx, "default", r.ID, r.SubmittedBy)
			if err != nil {
				t.Fatal(err)
			}
			if backend.submits != 1 || backend.cancels != 1 || after.State != "cancelled" || !after.CancelRequested {
				t.Fatalf("policy outage blocked cleanup or permitted execution: submits=%d cancels=%d state=%s", backend.submits, backend.cancels, after.State)
			}
		})
	}
}

func exerciseRunListSummaries(t *testing.T, s Store, ws string) {
	t.Helper()
	ctx := context.Background()
	binding := executionTestBinding()
	binding.WorkspaceID = ws
	r := newRun(ws, binding, binding.AllowedSubjects[0])
	observed := time.Now().UTC()
	r.ObservedAt = &observed
	for i := 0; i < MaxRunEvents; i++ {
		r = appendRunEvent(r, RunEvent{At: observed, State: "running", Message: strings.Repeat("event-details-", 120)})
	}
	saved, _, err := s.CreateRun(ctx, r, "bounded-list-key", "same")
	if err != nil {
		t.Fatal(err)
	}
	listed, err := s.Runs(ctx, ws, r.SubmittedBy)
	if err != nil || len(listed) != 1 {
		t.Fatalf("list: %v %+v", err, listed)
	}
	summary := listed[0]
	if summary.ID != saved.ID || len(summary.Events) != 0 || !summary.EventsTruncated || summary.Profile != saved.Profile || summary.State != saved.State || summary.ObservedAt == nil || !summary.ObservedAt.Equal(observed) {
		t.Fatal("list projection removed resource fields or retained event history")
	}
	encoded, err := json.Marshal(listed)
	if err != nil || len(encoded) > 8192 {
		t.Fatalf("run summary response includes unexpectedly large history: %d %v", len(encoded), err)
	}
	detail, err := s.Run(ctx, ws, saved.ID, saved.SubmittedBy)
	if err != nil || len(detail.Events) != MaxRunEvents || detail.Events[0].Message == "" {
		t.Fatal("list projection modified persisted detail history", err)
	}
	summary.ObservedAt = nil
	listed[0].Profile.Image = "changed-client-copy"
	again, err := s.Run(ctx, ws, saved.ID, saved.SubmittedBy)
	if err != nil || again.Profile.Image != saved.Profile.Image || again.ObservedAt == nil {
		t.Fatal("summary mutation corrupted stored resource")
	}
}
func TestMemoryRunListOmitsHistory(t *testing.T) {
	exerciseRunListSummaries(t, NewMemoryStore(), "default")
}
