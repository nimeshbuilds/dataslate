package platform

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

func TestPostgresExecutionDurabilityLeasesAndIdempotency(t *testing.T) {
	dsn := os.Getenv("DATASLATE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set DATASLATE_TEST_DATABASE_URL for isolated PostgreSQL execution integration")
	}
	ctx := context.Background()
	s, err := OpenPostgres(ctx, dsn, false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err = s.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	ws, err := s.CreateWorkspace(ctx, "Execution integration", newResource("execution-").ID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		s.pool.Exec(ctx, "DELETE FROM dataslate_runs WHERE workspace_id=$1", ws.ID)
		s.pool.Exec(ctx, "DELETE FROM dataslate_workspaces WHERE id=$1", ws.ID)
	}()
	exerciseExecutionStore(t, s, ws.ID)
	// A new connection sees an immutable runtime snapshot, a persisted checkpoint and the same idempotent run.
	b := executionTestBinding()
	b.WorkspaceID = ws.ID
	r, _, err := s.CreateRun(ctx, newRun(ws.ID, b, b.AllowedSubjects[0]), "restart-idempotency-key", "fingerprint")
	if err != nil {
		t.Fatal(err)
	}
	_, lease, err := s.AcquireRun(ctx, "old-controller", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	r, err = s.AdvanceRun(ctx, lease, RunUpdate{State: "submitting", Message: "Checkpoint", SubmissionStarted: true, RetainLease: true})
	if err != nil {
		t.Fatal(err)
	}
	second, err := OpenPostgres(ctx, dsn, false)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	read, err := second.Run(ctx, ws.ID, r.ID, r.SubmittedBy)
	if err != nil || !read.SubmissionStarted || read.Profile != b.ExecutionProfile {
		t.Fatalf("durable restart snapshot: %#v %v", read, err)
	}
	expireExecutionLease(t, s, lease)
	backend := &fakeExecutionBackend{}
	if _, err = ReconcileOnce(ctx, second, backend, "new-controller"); err != nil {
		t.Fatal(err)
	}
	if backend.submits != 0 {
		t.Fatal("restart blindly resubmitted ambiguous application")
	}
	read, err = second.Run(ctx, ws.ID, r.ID, r.SubmittedBy)
	if err != nil || read.State != "unknown" {
		t.Fatal("restart did not retain uncertainty")
	}
	if _, err = s.AdvanceRun(ctx, lease, RunUpdate{State: "succeeded", Message: "Stale process"}); !errors.Is(err, ErrLeaseLost) {
		t.Fatal("old process changed state after restart")
	}
}
func TestPostgresConcurrentRunClaims(t *testing.T) {
	dsn := os.Getenv("DATASLATE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set DATASLATE_TEST_DATABASE_URL for isolated PostgreSQL execution integration")
	}
	ctx := context.Background()
	s, err := OpenPostgres(ctx, dsn, false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	ws, err := s.CreateWorkspace(ctx, "Claim integration", newResource("claim-").ID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		s.pool.Exec(ctx, "DELETE FROM dataslate_runs WHERE workspace_id=$1", ws.ID)
		s.pool.Exec(ctx, "DELETE FROM dataslate_workspaces WHERE id=$1", ws.ID)
	}()
	b := executionTestBinding()
	b.WorkspaceID = ws.ID
	for i := 0; i < 8; i++ {
		if _, _, err = s.CreateRun(ctx, newRun(ws.ID, b, b.AllowedSubjects[0]), newResource("key-").ID, "fingerprint"); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	leases := make(chan RunLease, 8)
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Go(func() {
			_, lease, e := s.AcquireRun(ctx, newResource("worker-").ID, time.Minute)
			leases <- lease
			errs <- e
		})
	}
	wg.Wait()
	close(leases)
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	seen := map[string]bool{}
	for lease := range leases {
		if seen[lease.RunID] {
			t.Fatal("two workers leased the same run")
		}
		seen[lease.RunID] = true
	}
	if len(seen) != 8 {
		t.Fatal("pending work skipped")
	}
}
func TestPostgresExecutionCapacity(t *testing.T) {
	dsn := os.Getenv("DATASLATE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set DATASLATE_TEST_DATABASE_URL for isolated PostgreSQL execution integration")
	}
	ctx := context.Background()
	s, err := OpenPostgres(ctx, dsn, false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	ws, err := s.CreateWorkspace(ctx, "Capacity integration", newResource("capacity-").ID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		s.pool.Exec(ctx, "DELETE FROM dataslate_runs WHERE workspace_id=$1", ws.ID)
		s.pool.Exec(ctx, "DELETE FROM dataslate_workspaces WHERE id=$1", ws.ID)
	}()
	exerciseExecutionCapacity(t, s, ws.ID)
}
func TestPostgresRunListOmitsHistory(t *testing.T) {
	dsn := os.Getenv("DATASLATE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set DATASLATE_TEST_DATABASE_URL for isolated PostgreSQL execution integration")
	}
	ctx := context.Background()
	s, err := OpenPostgres(ctx, dsn, false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	ws, err := s.CreateWorkspace(ctx, "Summary integration", newResource("summary-").ID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		s.pool.Exec(ctx, "DELETE FROM dataslate_runs WHERE workspace_id=$1", ws.ID)
		s.pool.Exec(ctx, "DELETE FROM dataslate_workspaces WHERE id=$1", ws.ID)
	}()
	exerciseRunListSummaries(t, s, ws.ID)
}
