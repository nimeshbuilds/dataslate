package platform

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
)

// This test only touches unique test-owned resources in an explicitly provided test database.
func TestPostgresPersistenceAndConcurrentRevision(t *testing.T) {
	dsn := os.Getenv("DATASLATE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set DATASLATE_TEST_DATABASE_URL for isolated PostgreSQL integration test")
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
		t.Fatalf("migration not idempotent: %v", err)
	}
	if err = s.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	ws, err := s.CreateWorkspace(ctx, "Integration test", newResource("test-").ID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		s.pool.Exec(ctx, "DELETE FROM dataslate_notebooks WHERE workspace_id=$1", ws.ID)
		s.pool.Exec(ctx, "DELETE FROM dataslate_workspaces WHERE id=$1", ws.ID)
	}()
	n, err := s.SaveNotebook(ctx, ws.ID, "", input())
	if err != nil {
		t.Fatal(err)
	}
	second, err := OpenPostgres(ctx, dsn, false)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	read, err := second.Notebook(ctx, ws.ID, n.ID)
	if err != nil || read.Cells[0].Source != n.Cells[0].Source {
		t.Fatalf("independent connection cannot reopen: %v", err)
	}
	if _, err = second.Notebook(ctx, "wrong-workspace", n.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("cross-workspace access leaked")
	}
	v := input()
	v.Revision = n.Revision
	v.Name = "Updated"
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, db := range []*PostgresStore{s, second} {
		wg.Go(func() { _, e := db.SaveNotebook(ctx, ws.ID, n.ID, v); results <- e })
	}
	wg.Wait()
	close(results)
	success, conflicts := 0, 0
	for e := range results {
		if e == nil {
			success++
		} else if errors.Is(e, ErrConflict) {
			conflicts++
		} else {
			t.Fatal(e)
		}
	}
	if success != 1 || conflicts != 1 {
		t.Fatal("concurrent update lost revision fence")
	}
	history, err := s.Revisions(ctx, ws.ID, n.ID)
	if err != nil || len(history) != 2 || history[0].Name == history[1].Name {
		t.Fatalf("revision transaction incorrect: %v %v", history, err)
	}
	if err = s.DeleteNotebook(ctx, ws.ID, n.ID, 1); !errors.Is(err, ErrConflict) {
		t.Fatal("stale delete accepted")
	}
	if err = s.DeleteNotebook(ctx, ws.ID, n.ID, 2); err != nil {
		t.Fatal(err)
	}
}
