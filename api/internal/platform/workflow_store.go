package platform

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"sort"
	"time"
)

type memoryWorkflow struct {
	Work               WorkflowWork
	Fingerprint, Owner string
	Fence              int64
	Until, Next        time.Time
}

func (s *MemoryStore) CreateWorkflow(_ context.Context, w WorkflowWork, key, fp string) (WorkflowWork, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.workflows == nil {
		s.workflows = map[string]*memoryWorkflow{}
		s.workflowKeys = map[string]string{}
	}
	k := runKey(w.Run.WorkspaceID, w.Run.SubmittedBy, key)
	if id, ok := s.workflowKeys[k]; ok {
		entry := s.workflows[id]
		if entry.Fingerprint != fp {
			return WorkflowWork{}, false, ErrConflict
		}
		return cloneWorkflow(entry.Work), true, nil
	}
	if _, ok := s.workspaces[w.Run.WorkspaceID]; !ok {
		return WorkflowWork{}, false, ErrNotFound
	}
	count := 0
	for _, entry := range s.workflows {
		if entry.Work.Run.WorkspaceID == w.Run.WorkspaceID && !entry.Work.Run.ReconciliationComplete {
			count++
		}
	}
	if count >= 100 {
		return WorkflowWork{}, false, ErrRunCapacity
	}
	s.workflows[w.Run.ID] = &memoryWorkflow{Work: cloneWorkflow(w), Fingerprint: fp}
	s.workflowKeys[k] = w.Run.ID
	return cloneWorkflow(w), false, nil
}
func (s *MemoryStore) Workflows(_ context.Context, ws string, owner ExecutionSubject) ([]WorkflowWork, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []WorkflowWork{}
	for _, entry := range s.workflows {
		if entry.Work.Run.WorkspaceID == ws && entry.Work.Run.SubmittedBy == owner {
			out = append(out, cloneWorkflow(entry.Work))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Run.CreatedAt.After(out[j].Run.CreatedAt) })
	if len(out) > 200 {
		out = out[:200]
	}
	return out, nil
}
func (s *MemoryStore) Workflow(_ context.Context, ws, id string, owner ExecutionSubject) (WorkflowWork, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.workflows[id]
	if !ok || entry.Work.Run.WorkspaceID != ws || entry.Work.Run.SubmittedBy != owner {
		return WorkflowWork{}, ErrNotFound
	}
	return cloneWorkflow(entry.Work), nil
}
func (s *MemoryStore) AcquireWorkflow(_ context.Context, owner string, ttl time.Duration) (WorkflowWork, WorkflowLease, error) {
	if !validPublicationLease(owner, ttl) {
		return WorkflowWork{}, WorkflowLease{}, ErrConflict
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	var chosen *memoryWorkflow
	for _, entry := range s.workflows {
		if entry.Work.Run.ReconciliationComplete || entry.Until.After(now) || entry.Next.After(now) {
			continue
		}
		if chosen == nil || entry.Next.Before(chosen.Next) {
			chosen = entry
		}
	}
	if chosen == nil {
		return WorkflowWork{}, WorkflowLease{}, ErrNotFound
	}
	chosen.Owner = owner
	chosen.Fence++
	chosen.Until = now.Add(ttl)
	return cloneWorkflow(chosen.Work), WorkflowLease{chosen.Work.Run.ID, owner, chosen.Fence}, nil
}
func (s *MemoryStore) AdvanceWorkflow(_ context.Context, l WorkflowLease, u WorkflowUpdate) (WorkflowWork, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	entry, ok := s.workflows[l.ID]
	if !ok || entry.Owner != l.Owner || entry.Fence != l.Fence || !entry.Until.After(now) {
		return WorkflowWork{}, ErrLeaseLost
	}
	w, e := updateWorkflow(entry.Work, u, now)
	if e != nil {
		return WorkflowWork{}, e
	}
	entry.Work = w
	if !u.RetainLease {
		entry.Owner = ""
		entry.Until = time.Time{}
		entry.Next = now.Add(u.RetryAfter)
	}
	return cloneWorkflow(w), nil
}
func (s *PostgresStore) CreateWorkflow(ctx context.Context, w WorkflowWork, key, fp string) (WorkflowWork, bool, error) {
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return WorkflowWork{}, false, e
	}
	defer tx.Rollback(ctx)
	// Serialize capacity and replay admission for this workspace.
	var ws string
	e = tx.QueryRow(ctx, "SELECT id FROM dataslate_workspaces WHERE id=$1 FOR UPDATE", w.Run.WorkspaceID).Scan(&ws)
	if errors.Is(e, pgx.ErrNoRows) {
		return WorkflowWork{}, false, ErrNotFound
	}
	if e != nil {
		return WorkflowWork{}, false, e
	}
	var old WorkflowWork
	var oldFP string
	e = tx.QueryRow(ctx, `SELECT document,fingerprint FROM dataslate_workflows WHERE workspace_id=$1 AND issuer=$2 AND subject=$3 AND idempotency_key=$4`, ws, w.Run.SubmittedBy.Issuer, w.Run.SubmittedBy.Subject, key).Scan(&old, &oldFP)
	if e == nil {
		if fp != oldFP {
			return WorkflowWork{}, false, ErrConflict
		}
		return old, true, tx.Commit(ctx)
	}
	if !errors.Is(e, pgx.ErrNoRows) {
		return WorkflowWork{}, false, e
	}
	var count int
	if e = tx.QueryRow(ctx, "SELECT count(*) FROM dataslate_workflows WHERE workspace_id=$1 AND NOT final", ws).Scan(&count); e != nil {
		return WorkflowWork{}, false, e
	}
	if count >= 100 {
		return WorkflowWork{}, false, ErrRunCapacity
	}
	_, e = tx.Exec(ctx, `INSERT INTO dataslate_workflows(id,workspace_id,issuer,subject,idempotency_key,fingerprint,document,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, w.Run.ID, ws, w.Run.SubmittedBy.Issuer, w.Run.SubmittedBy.Subject, key, fp, w, w.Run.CreatedAt)
	if e != nil {
		return WorkflowWork{}, false, e
	}
	return w, false, tx.Commit(ctx)
}
func (s *PostgresStore) Workflows(ctx context.Context, ws string, owner ExecutionSubject) ([]WorkflowWork, error) {
	rows, e := s.pool.Query(ctx, `SELECT document FROM dataslate_workflows WHERE workspace_id=$1 AND issuer=$2 AND subject=$3 ORDER BY created_at DESC,id LIMIT 200`, ws, owner.Issuer, owner.Subject)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []WorkflowWork{}
	for rows.Next() {
		var w WorkflowWork
		if e = rows.Scan(&w); e != nil {
			return nil, e
		}
		out = append(out, w)
	}
	return out, rows.Err()
}
func (s *PostgresStore) Workflow(ctx context.Context, ws, id string, owner ExecutionSubject) (WorkflowWork, error) {
	var w WorkflowWork
	e := s.pool.QueryRow(ctx, `SELECT document FROM dataslate_workflows WHERE workspace_id=$1 AND id=$2 AND issuer=$3 AND subject=$4`, ws, id, owner.Issuer, owner.Subject).Scan(&w)
	if errors.Is(e, pgx.ErrNoRows) {
		e = ErrNotFound
	}
	return w, e
}
func (s *PostgresStore) AcquireWorkflow(ctx context.Context, owner string, ttl time.Duration) (WorkflowWork, WorkflowLease, error) {
	if !validPublicationLease(owner, ttl) {
		return WorkflowWork{}, WorkflowLease{}, ErrConflict
	}
	var w WorkflowWork
	var fence int64
	e := s.pool.QueryRow(ctx, `WITH candidate AS (SELECT id FROM dataslate_workflows WHERE NOT final AND next_reconcile_at<=clock_timestamp() AND (lease_until IS NULL OR lease_until<=clock_timestamp()) ORDER BY next_reconcile_at,id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE dataslate_workflows q SET lease_owner=$1,lease_fence=lease_fence+1,lease_until=clock_timestamp()+($2*interval '1 millisecond') FROM candidate WHERE q.id=candidate.id RETURNING q.document,q.lease_fence`, owner, ttl.Milliseconds()).Scan(&w, &fence)
	if errors.Is(e, pgx.ErrNoRows) {
		e = ErrNotFound
	}
	return w, WorkflowLease{w.Run.ID, owner, fence}, e
}
func (s *PostgresStore) AdvanceWorkflow(ctx context.Context, l WorkflowLease, u WorkflowUpdate) (WorkflowWork, error) {
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return WorkflowWork{}, e
	}
	defer tx.Rollback(ctx)
	var w WorkflowWork
	var owner string
	var fence int64
	var until time.Time
	e = tx.QueryRow(ctx, `SELECT document,COALESCE(lease_owner,''),lease_fence,COALESCE(lease_until,'epoch') FROM dataslate_workflows WHERE id=$1 FOR UPDATE`, l.ID).Scan(&w, &owner, &fence, &until)
	if errors.Is(e, pgx.ErrNoRows) {
		return WorkflowWork{}, ErrLeaseLost
	}
	if e != nil {
		return WorkflowWork{}, e
	}
	var now time.Time
	if e = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); e != nil {
		return WorkflowWork{}, e
	}
	if owner != l.Owner || fence != l.Fence || !until.After(now) {
		return WorkflowWork{}, ErrLeaseLost
	}
	w, e = updateWorkflow(w, u, now)
	if e != nil {
		return WorkflowWork{}, e
	}
	tag, e := tx.Exec(ctx, `UPDATE dataslate_workflows SET document=$2,final=$3,lease_owner=CASE WHEN $4 THEN lease_owner ELSE NULL END,lease_until=CASE WHEN $4 THEN lease_until ELSE NULL END,next_reconcile_at=clock_timestamp()+($5*interval '1 millisecond') WHERE id=$1 AND lease_owner=$6 AND lease_fence=$7 AND lease_until>clock_timestamp() AND (NOT $8 OR $9::timestamptz>clock_timestamp())`, l.ID, w, w.Run.ReconciliationComplete, u.RetainLease, u.RetryAfter.Milliseconds(), l.Owner, l.Fence, u.DispatchStarted || u.Observation != nil, w.Run.CreatedAt.Add(time.Duration(w.Binding.MaxRunSeconds)*time.Second))
	if e != nil {
		return WorkflowWork{}, e
	}
	if tag.RowsAffected() != 1 {
		return WorkflowWork{}, ErrLeaseLost
	}
	return w, tx.Commit(ctx)
}
