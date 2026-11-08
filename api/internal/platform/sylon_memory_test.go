package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"dataslate.local/api/internal/llm"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

func memoryBody(t *testing.T, v any) string {
	t.Helper()
	data, e := json.Marshal(v)
	if e != nil {
		t.Fatal(e)
	}
	return string(data)
}
func TestSylonMemoryMutationConcurrencyProvenanceAndReplay(t *testing.T) {
	forSylonStores(t, func(t *testing.T, s Store, ws string) {
		ctx := context.Background()
		_, _, _, m := persistentMemoryContextFixture(t, s, ws)
		foreign := m.Memory.Owner
		foreign.Issuer = "https://foreign.example.test"
		if _, e := s.SylonMemoryWork(ctx, ws, m.Memory.ID, foreign); !errors.Is(e, ErrNotFound) {
			t.Fatal("cross issuer", e)
		}
		if _, e := s.SylonMemoryWork(ctx, "other", m.Memory.ID, m.Memory.Owner); !errors.Is(e, ErrNotFound) {
			t.Fatal("cross workspace", e)
		}
		in := SylonMemoryInput{IdempotencyKey: "correct-once", Revision: 1, Title: "Corrected title", Content: "Corrected content", ExpiresAt: time.Now().Add(2 * time.Hour)}
		fp := memoryFingerprint("correct", m.Memory.ID, in)
		var wg sync.WaitGroup
		receipts := make(chan SylonMemoryReceipt, 8)
		errs := make(chan error, 8)
		for range 8 {
			wg.Go(func() {
				r, _, e := s.CorrectSylonMemory(ctx, ws, m.Memory.ID, m.Memory.Owner, in, fp)
				receipts <- r
				errs <- e
			})
		}
		wg.Wait()
		close(receipts)
		close(errs)
		for e := range errs {
			if e != nil {
				t.Fatal(e)
			}
		}
		var original SylonMemoryReceipt
		for r := range receipts {
			if r.PreviousRevision != 1 || r.Revision != 2 || r.Operation != "correct" {
				t.Fatal("bad receipt", r)
			}
			if original.MemoryID != "" && original != r {
				t.Fatal("non-identical replay")
			}
			original = r
		}
		current, e := s.SylonMemoryWork(ctx, ws, m.Memory.ID, m.Memory.Owner)
		if e != nil {
			t.Fatal(e)
		}
		if current.Memory.Source != m.Memory.Source || current.PolicyHash != m.PolicyHash || current.OutputHash != m.OutputHash || memoryBody(t, current.Dependencies) != memoryBody(t, m.Dependencies) {
			t.Fatal("correction changed provenance")
		}
		if !current.Memory.PurgePending || current.Memory.Content != in.Content {
			t.Fatal("missing purge obligation")
		}
		if _, _, e = s.CorrectSylonMemory(ctx, ws, m.Memory.ID, m.Memory.Owner, in, "different-input"); !errors.Is(e, ErrConflict) {
			t.Fatal("reused key", e)
		}
		stale := in
		stale.IdempotencyKey = "stale-revision"
		if _, _, e = s.CorrectSylonMemory(ctx, ws, m.Memory.ID, m.Memory.Owner, stale, "stale"); !errors.Is(e, ErrMemoryRevision) {
			t.Fatal("stale correction", e)
		}
		del := SylonMemoryDeleteInput{IdempotencyKey: "forget-once", Revision: 2}
		dfp := memoryFingerprint("delete", m.Memory.ID, del)
		receipt, replay, e := s.DeleteSylonMemory(ctx, ws, m.Memory.ID, m.Memory.Owner, del, dfp)
		if e != nil || replay || receipt.Revision != 3 {
			t.Fatal("delete", e)
		}
		exact, replay, e := s.DeleteSylonMemory(ctx, ws, m.Memory.ID, m.Memory.Owner, del, dfp)
		if e != nil || !replay || exact != receipt {
			t.Fatal("delete replay", e)
		}
		exact, replay, e = s.CorrectSylonMemory(ctx, ws, m.Memory.ID, m.Memory.Owner, in, fp)
		if e != nil || !replay || exact != original {
			t.Fatal("old correction receipt recovery", e)
		}
		current, e = s.SylonMemoryWork(ctx, ws, m.Memory.ID, m.Memory.Owner)
		if e != nil || current.State != "deleted" || current.Memory.Content != "" || current.Memory.Title != "" {
			t.Fatal("deleted head retained body", e)
		}
	})
}
func TestSylonMemoryCorrectionInvalidatesAndPurgesTransitiveHistory(t *testing.T) {
	forSylonStores(t, func(t *testing.T, s Store, ws string) {
		ctx := context.Background()
		b, _, _, m := persistentMemoryContextFixture(t, s, ws)
		c, e := s.CreateSylonConversation(ctx, SylonConversation{Resource: newResource("chat-"), WorkspaceID: ws, BindingID: b.ID, Owner: m.Memory.Owner, Title: "Derived private conversation"})
		if e != nil {
			t.Fatal(e)
		}
		j := enqueueMemoryContextJob(t, s, b, c, m, nil)
		ctl := SylonController{Config: Config{SylonBindings: []SylonBinding{b}}, Store: s, Generator: &fakeSylonGenerator{result: llm.Result{Answer: "DERIVED_PRIVATE_COPY", Citations: []string{}}}}
		if _, e = ctl.ReconcileOnce(ctx, "derive"); e != nil {
			t.Fatal(e)
		}
		origin := sylonRead(t, s, j)
		if origin.Job.State != "succeeded" || !strings.Contains(origin.Context.Input, m.Memory.Content) {
			t.Fatal("derived fixture missing memory")
		}
		child := cloneMemory(m)
		child.Memory.Resource = newResource("mem-")
		child.Memory.Source = SylonMemorySource{JobID: j.ID, Revision: origin.Job.Revision}
		child.Memory.Content = "DESCENDANT_COPY"
		child.Dependencies = origin.Context.Dependencies
		child.OutputHash = SylonOutputHash(origin.Job.Result)
		if _, _, e = s.CreateSylonMemory(ctx, child, "save-child", "save-child-fp"); e != nil {
			t.Fatal(e)
		}
		in := SylonMemoryInput{IdempotencyKey: "correct-root", Revision: 1, Title: "Fixed", Content: "Replacement", ExpiresAt: time.Now().Add(time.Hour)}
		if _, _, e = s.CorrectSylonMemory(ctx, ws, m.Memory.ID, m.Memory.Owner, in, "correct-root-fp"); e != nil {
			t.Fatal(e)
		}
		server := contextServer(b, s)
		if e = server.authorizeSylonWork(ctx, origin, false); !errors.Is(e, ErrSylonAccess) {
			t.Fatal("derived history survived correction", e)
		}
		if _, e = server.AuthorizeSylonMemory(ctx, b, m.Memory.Owner, memoryContextSelection(child)); !errors.Is(e, ErrSylonAccess) {
			t.Fatal("descendant survived correction", e)
		}
		for i := 0; i < 20; i++ {
			more, e := s.PurgeSylonMemories(ctx, 1)
			if e != nil {
				t.Fatal(e)
			}
			if !more {
				break
			}
			if i == 19 {
				t.Fatal("purge did not drain")
			}
		}
		redacted := sylonRead(t, s, j)
		if !redacted.Job.MemoryInvalidated || redacted.Job.Result != nil || redacted.Context.Input != "" || len(redacted.Context.Dependencies) != 0 {
			t.Fatal("derived stored content retained")
		}
		child, e = s.SylonMemoryWork(ctx, ws, child.Memory.ID, m.Memory.Owner)
		if e != nil || child.State != "deleted" || child.Memory.Content != "" || child.Memory.PurgePending {
			t.Fatal("descendant purge not completed", e)
		}
		root, e := s.SylonMemoryWork(ctx, ws, m.Memory.ID, m.Memory.Owner)
		if e != nil || root.Memory.PurgePending || root.Memory.Content != "Replacement" {
			t.Fatal("correction lost replacement or purge pending", e)
		}
		if _, e = server.AuthorizeSylonMemory(ctx, b, m.Memory.Owner, memoryContextSelection(root)); e != nil {
			t.Fatal("replacement cannot be recalled", e)
		}
	})
}
func TestSylonMemoryActiveCapacityAndDeleteReclaimsSlot(t *testing.T) {
	forSylonStores(t, func(t *testing.T, s Store, ws string) {
		ctx := context.Background()
		_, _, _, m := persistentMemoryContextFixture(t, s, ws)
		for i := 1; i < MaxSylonMemories; i++ {
			next := cloneMemory(m)
			next.Memory.Resource = newResource("mem-")
			if _, _, e := s.CreateSylonMemory(ctx, next, fmt.Sprintf("capacity-%d", i), fmt.Sprintf("capacity-fp-%d", i)); e != nil {
				t.Fatal(i, e)
			}
		}
		overflow := cloneMemory(m)
		overflow.Memory.Resource = newResource("mem-")
		if _, _, e := s.CreateSylonMemory(ctx, overflow, "capacity-overflow", "capacity-overflow-fp"); !errors.Is(e, ErrMemoryCapacity) {
			t.Fatal("capacity escaped", e)
		}
		items, e := s.SylonMemories(ctx, ws, m.Memory.Owner, "", true)
		if e != nil || len(items) != 100 {
			t.Fatal("unbounded corpus", len(items), e)
		}
		if _, _, e = s.DeleteSylonMemory(ctx, ws, m.Memory.ID, m.Memory.Owner, SylonMemoryDeleteInput{IdempotencyKey: "free-slot", Revision: 1}, "free-slot-fp"); e != nil {
			t.Fatal(e)
		}
		if _, _, e = s.CreateSylonMemory(ctx, overflow, "capacity-overflow", "capacity-overflow-fp"); e != nil {
			t.Fatal("deleted slot not reclaimed", e)
		}
	})
}
func TestSylonMemoryHTTPReviewedSaveRecallRevocationAndRoles(t *testing.T) {
	s := NewMemoryStore()
	b, c, _, seed := persistentMemoryContextFixture(t, s, "default")
	p := executionPrincipal()
	server := &Server{Config: Config{AuthMode: "oidc", SylonBindings: []SylonBinding{b}}, Store: s, Auth: testAuth{p: p}}
	h := server.Handler()
	base := "/api/v1/workspaces/default/sylon/memories"
	in := SylonMemoryInput{IdempotencyKey: "reviewed-save", Source: &seed.Memory.Source, Title: "Lake tuning", Content: "Use parquet compression for lake tuning", ExpiresAt: time.Now().Add(24 * time.Hour)}
	r := request(h, "POST", base, memoryBody(t, in))
	if r.Code != 201 {
		t.Fatal(r.Code, r.Body)
	}
	var saved SylonMemoryMutation
	if e := json.Unmarshal(r.Body.Bytes(), &saved); e != nil {
		t.Fatal(e)
	}
	id := saved.Receipt.MemoryID
	if saved.Memory.State != "available" || saved.Replayed || saved.Receipt.PreviousRevision != 0 || saved.Receipt.Revision != 1 {
		t.Fatal("save receipt", r.Body)
	}
	if r = request(h, "GET", base+"/"+id, ""); r.Code != 200 || !strings.Contains(r.Body.String(), in.Content) {
		t.Fatal("read", r.Code, r.Body)
	}
	recall := SylonMemoryRecallInput{BindingID: b.ID, Query: "LAKE tuning", Limit: 4}
	if r = request(h, "POST", base+"/recall", memoryBody(t, recall)); r.Code != 200 || !strings.Contains(r.Body.String(), id) || strings.Contains(r.Body.String(), seed.Memory.ID) {
		t.Fatal("lexical recall", r.Code, r.Body)
	}
	viewer := p
	viewer.Workspaces = map[string]string{"default": "viewer"}
	server.Auth = testAuth{p: viewer}
	if r = request(h, "GET", base+"/"+id, ""); r.Code != 403 {
		t.Fatal("viewer derived source", r.Code)
	}
	if r = request(h, "GET", base, ""); r.Code != 200 || strings.Contains(r.Body.String(), in.Title) || !strings.Contains(r.Body.String(), "restricted") {
		t.Fatal("viewer summary leak", r.Body)
	}
	if r = request(h, "POST", base, memoryBody(t, in)); r.Code != 200 || strings.Contains(r.Body.String(), in.Title) {
		t.Fatal("viewer replay leaked title", r.Body)
	}
	if r = request(h, "POST", base+"/recall", memoryBody(t, recall)); r.Code != 200 || strings.Contains(r.Body.String(), in.Content) {
		t.Fatal("viewer recall leak", r.Body)
	}
	msg := SylonMessageInput{IdempotencyKey: "viewer-memory-send", Prompt: "Use memory", Memories: []SylonMemorySelection{{ID: id, Revision: 1}}}
	if r = request(h, "POST", "/api/v1/workspaces/default/sylon/conversations/"+c.ID+"/messages", memoryBody(t, msg)); r.Code != 403 {
		t.Fatal("viewer transferred memory", r.Code, r.Body)
	}
	server.Auth = testAuth{p: p}
	server.Config.SylonBindings[0].AllowedSubjects = []ExecutionSubject{}
	if r = request(h, "GET", base+"/"+id, ""); r.Code != 403 {
		t.Fatal("revoked detail", r.Code)
	}
	if r = request(h, "POST", base, memoryBody(t, in)); r.Code != 200 || strings.Contains(r.Body.String(), in.Title) || strings.Contains(r.Body.String(), seed.Memory.Source.JobID) {
		t.Fatal("revoked replay leak", r.Body)
	}
	del := SylonMemoryDeleteInput{IdempotencyKey: "revoked-delete", Revision: 1}
	if r = request(h, "DELETE", base+"/"+id, memoryBody(t, del)); r.Code != 200 || !strings.Contains(r.Body.String(), `"state":"deleted"`) {
		t.Fatal("revoked owner deletion", r.Code, r.Body)
	}
	if r = request(h, "DELETE", base+"/"+id, memoryBody(t, del)); r.Code != 200 || !strings.Contains(r.Body.String(), `"replayed":true`) {
		t.Fatal("delete replay", r.Body)
	}
}
func TestSylonMemoryValidationAndPublicSchema(t *testing.T) {
	s := NewMemoryStore()
	b, _, _, m := persistentMemoryContextFixture(t, s, "default")
	server := &Server{Config: Config{AuthMode: "oidc", SylonBindings: []SylonBinding{b}}, Store: s, Auth: testAuth{p: executionPrincipal()}}
	h := server.Handler()
	base := "/api/v1/workspaces/default/sylon/memories"
	valid := SylonMemoryInput{IdempotencyKey: "schema-save", Source: &m.Memory.Source, Title: "Memory", Content: "Reviewed content", ExpiresAt: time.Now().Add(time.Hour)}
	for name, mutate := range map[string]func(*SylonMemoryInput){"unbound": func(v *SylonMemoryInput) { v.Source = nil }, "utf8-bytes": func(v *SylonMemoryInput) { v.Title = strings.Repeat("é", 101) }, "body": func(v *SylonMemoryInput) { v.Content = strings.Repeat("x", 4097) }, "expired": func(v *SylonMemoryInput) { v.ExpiresAt = time.Now().Add(-time.Second) }, "max-ttl": func(v *SylonMemoryInput) { v.ExpiresAt = time.Now().Add(31 * 24 * time.Hour) }} {
		t.Run(name, func(t *testing.T) {
			v := valid
			mutate(&v)
			if r := request(h, "POST", base, memoryBody(t, v)); r.Code != 422 {
				t.Fatal(r.Code, r.Body)
			}
		})
	}
	raw, e := os.ReadFile("../../openapi.yaml")
	if e != nil {
		t.Fatal(e)
	}
	var doc any
	if e = json.Unmarshal(raw, &doc); e != nil {
		t.Fatal(e)
	}
	compiler := jsonschema.NewCompiler()
	if e = compiler.AddResource("memory-contract.json", doc); e != nil {
		t.Fatal(e)
	}
	tests := []struct {
		method, path, body, schema string
		code                       int
	}{{http.MethodGet, base + "/" + m.Memory.ID, "", "SylonMemory", 200}, {http.MethodPost, base, memoryBody(t, valid), "SylonMemoryMutation", 201}, {http.MethodPost, base + "/recall", memoryBody(t, SylonMemoryRecallInput{BindingID: b.ID, Query: "content", Limit: 4}), "SylonMemoryRecall", 200}}
	for _, tc := range tests {
		r := request(h, tc.method, tc.path, tc.body)
		if r.Code != tc.code {
			t.Fatal(tc.path, r.Code, r.Body)
		}
		schema, e := compiler.Compile("memory-contract.json#/components/schemas/" + tc.schema)
		if e != nil {
			t.Fatal(e)
		}
		var payload any
		if e = json.Unmarshal(r.Body.Bytes(), &payload); e != nil {
			t.Fatal(e)
		}
		if e = schema.Validate(payload); e != nil {
			t.Fatal(tc.schema, e)
		}
	}
}

func TestSylonMemoryRejectsExhaustedProvenanceCapacity(t *testing.T) {
	forSylonStores(t, func(t *testing.T, s Store, ws string) {
		_, _, _, m := persistentMemoryContextFixture(t, s, ws)
		m.Memory.Resource = newResource("mem-")
		for len(m.Dependencies) < MaxSylonDependencies {
			m.Dependencies = append(m.Dependencies, SylonDependency{Kind: "notebook-metadata", NotebookID: fmt.Sprintf("nb-%d", len(m.Dependencies))})
		}
		if _, _, e := s.CreateSylonMemory(context.Background(), m, "too-many-deps", "too-many-deps-fp"); !errors.Is(e, ErrMemoryCapacity) {
			t.Fatal("unusable memory persisted", e)
		}
		if _, e := s.SylonMemoryWork(context.Background(), ws, m.Memory.ID, m.Memory.Owner); !errors.Is(e, ErrNotFound) {
			t.Fatal("rejected memory exists", e)
		}
	})
}
func TestSylonMemoryRoutesRequireAuthentication(t *testing.T) {
	server := &Server{Config: Config{AuthMode: "oidc"}, Store: NewMemoryStore(), Auth: testAuth{err: errors.New("denied")}}
	for _, tc := range []struct{ method, path string }{{"GET", ""}, {"GET", "/mem-one"}, {"POST", ""}, {"PUT", "/mem-one"}, {"DELETE", "/mem-one"}, {"POST", "/recall"}} {
		r := request(server.Handler(), tc.method, "/api/v1/workspaces/default/sylon/memories"+tc.path, "{}")
		if r.Code != 401 {
			t.Fatal(tc, r.Code, r.Body)
		}
	}
}

func TestSylonMemoryLeaseExpiresWhileDependencyLocked(t *testing.T) {
	store, ws := searchTestStore(t)
	ctx := context.Background()
	b, c, _, m := persistentMemoryContextFixture(t, store, ws)
	job := enqueueMemoryContextJob(t, store, b, c, m, nil)
	work, lease, e := store.AcquireSylonJob(ctx, "memory-lease-test", time.Second)
	if e != nil || work.Job.ID != job.ID {
		t.Fatal("lease fixture", e)
	}
	prepared, e := contextServer(b, store).PrepareSylonContext(ctx, b, work, nil, nil)
	if e != nil {
		t.Fatal(e)
	}
	tx, e := store.pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, "SELECT id FROM dataslate_sylon_memories WHERE id=$1 FOR UPDATE", m.Memory.ID); e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { done <- store.SaveSylonContext(ctx, lease, prepared) }()
	blocked := false
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if e = store.pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1::integer=ANY(pg_blocking_pids(pid)))", int32(tx.Conn().PgConn().PID())).Scan(&blocked); e != nil {
			t.Fatal(e)
		}
		if blocked {
			break
		}
		select {
		case e := <-done:
			t.Fatal("save did not block", e)
		case <-time.After(10 * time.Millisecond):
		}
	}
	if !blocked {
		t.Fatal("save never reached dependency lock")
	}
	// The lock holder remains unchanged; a WHERE-time check before the wait would
	// therefore be insufficient even though the job row itself was already held.
	time.Sleep(time.Second)
	if e = tx.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	if e = <-done; !errors.Is(e, ErrSylonLease) {
		t.Fatal("expired worker saved context after dependency wait", e)
	}
	current := sylonRead(t, store, job)
	if current.Context.Input != "" || len(current.Job.Citations) > 0 {
		t.Fatal("expired worker published context")
	}
}
