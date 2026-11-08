package platform

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"sort"
	"time"
)

type memoryQuery struct {
	Work               QueryWork
	Fingerprint, Owner string
	Fence              int64
	Until, Next        time.Time
}

func (s *MemoryStore) CreateQuery(_ context.Context, q Query, policy SQLPolicy, key, fingerprint string) (Query, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.workspaces[q.WorkspaceID]; !ok {
		return Query{}, false, ErrNotFound
	}
	k := runKey(q.WorkspaceID, q.SubmittedBy, key)
	if id, ok := s.queryKeys[k]; ok {
		e := s.queries[id]
		if e.Fingerprint != fingerprint {
			return Query{}, false, ErrConflict
		}
		return e.Work.Query, true, nil
	}
	active := 0
	for _, e := range s.queries {
		if e.Work.Query.WorkspaceID == q.WorkspaceID && !e.Work.Query.Terminal() {
			active++
		}
	}
	if active >= MaxWorkspaceQueries {
		return Query{}, false, ErrQueryCapacity
	}
	n, ok := s.notebooks[q.Notebook.ID]
	if !ok || n.WorkspaceID != q.WorkspaceID {
		return Query{}, false, ErrNotFound
	}
	source, err := snapshotSQL(q, n)
	if err != nil {
		return Query{}, false, err
	}
	q.Notebook = source.Notebook
	s.queries[q.ID] = &memoryQuery{Work: QueryWork{Query: q, Policy: policy, Source: source, Result: emptyQueryResult(q)}, Fingerprint: fingerprint}
	s.queryKeys[k] = q.ID
	return q, false, nil
}
func (s *MemoryStore) Queries(_ context.Context, ws string, owner ExecutionSubject) ([]Query, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []Query{}
	for _, e := range s.queries {
		q := e.Work.Query
		if q.WorkspaceID == ws && q.SubmittedBy == owner {
			out = append(out, q)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	if len(out) > 200 {
		out = out[:200]
	}
	return out, nil
}
func (s *MemoryStore) queryOwned(ws, id string, owner ExecutionSubject) (*memoryQuery, error) {
	e, ok := s.queries[id]
	if !ok || e.Work.Query.WorkspaceID != ws || e.Work.Query.SubmittedBy != owner {
		return nil, ErrNotFound
	}
	return e, nil
}
func (s *MemoryStore) Query(_ context.Context, ws, id string, owner ExecutionSubject) (Query, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, err := s.queryOwned(ws, id, owner)
	if err != nil {
		return Query{}, err
	}
	return e.Work.Query, nil
}
func (s *MemoryStore) QuerySource(_ context.Context, ws, id string, owner ExecutionSubject) (QuerySource, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, err := s.queryOwned(ws, id, owner)
	if err != nil {
		return QuerySource{}, err
	}
	return e.Work.Source, nil
}
func (s *MemoryStore) QueryResult(_ context.Context, ws, id string, owner ExecutionSubject) (QueryResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, err := s.queryOwned(ws, id, owner)
	if err != nil {
		return QueryResult{}, err
	}
	return cloneQueryResult(e.Work.Result), nil
}
func (s *MemoryStore) QueryPolicy(_ context.Context, ws, id string, owner ExecutionSubject) (SQLPolicy, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, err := s.queryOwned(ws, id, owner)
	if err != nil {
		return SQLPolicy{}, err
	}
	return e.Work.Policy, nil
}
func requestQueryCancel(q Query) Query {
	if q.Terminal() || q.CancelRequested {
		return q
	}
	q.CancelRequested = true
	q.State = "cancel_requested"
	q.Message = "Cancellation requested; engine confirmation is pending."
	q.Revision++
	q.UpdatedAt = time.Now().UTC()
	return q
}
func (s *MemoryStore) CancelQuery(_ context.Context, ws, id string, owner ExecutionSubject) (Query, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, err := s.queryOwned(ws, id, owner)
	if err != nil {
		return Query{}, err
	}
	e.Work.Query = requestQueryCancel(e.Work.Query)
	e.Next = time.Time{}
	return e.Work.Query, nil
}
func (s *MemoryStore) AcquireQuery(_ context.Context, owner string, ttl time.Duration) (QueryWork, QueryLease, error) {
	if owner == "" || len(owner) > 200 || ttl <= 0 || ttl > time.Minute {
		return QueryWork{}, QueryLease{}, errors.New("invalid query lease")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	var selected *memoryQuery
	for _, e := range s.queries {
		if e.Work.Query.Terminal() || e.Until.After(now) || e.Next.After(now) {
			continue
		}
		if selected == nil || e.Next.Before(selected.Next) {
			selected = e
		}
	}
	if selected == nil {
		return QueryWork{}, QueryLease{}, ErrNotFound
	}
	selected.Owner = owner
	selected.Fence++
	selected.Until = now.Add(ttl)
	out := selected.Work
	out.Result = cloneQueryResult(out.Result)
	out.Processed = append([]string{}, out.Processed...)
	return out, QueryLease{QueryID: out.Query.ID, Owner: owner, Fence: selected.Fence}, nil
}
func (s *MemoryStore) AdvanceQuery(_ context.Context, lease QueryLease, u QueryUpdate) (Query, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.queries[lease.QueryID]
	now := time.Now().UTC()
	if !ok || e.Owner != lease.Owner || e.Fence != lease.Fence || !e.Until.After(now) {
		return Query{}, ErrLeaseLost
	}
	q, err := applyQueryUpdate(e.Work.Query, u, now)
	if err != nil {
		return Query{}, err
	}
	e.Work.Query = q
	if u.Processed != nil {
		e.Work.Processed = append([]string{}, u.Processed...)
	}
	if u.Cursor != nil {
		e.Work.Cursor = *u.Cursor
	}
	if u.Result != nil {
		e.Work.Result = cloneQueryResult(*u.Result)
	}
	if !u.RetainLease {
		e.Owner = ""
		e.Until = time.Time{}
		e.Next = now.Add(u.RetryAfter)
	}
	return q, nil
}

func (s *PostgresStore) CreateQuery(ctx context.Context, q Query, policy SQLPolicy, key, fingerprint string) (Query, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Query{}, false, err
	}
	defer tx.Rollback(ctx)
	var ws string
	if err = tx.QueryRow(ctx, `SELECT id FROM dataslate_workspaces WHERE id=$1 FOR UPDATE`, q.WorkspaceID).Scan(&ws); err != nil {
		return Query{}, false, storeError(err)
	}
	var existing Query
	var fp string
	err = tx.QueryRow(ctx, `SELECT document,request_fingerprint FROM dataslate_queries WHERE workspace_id=$1 AND issuer=$2 AND subject=$3 AND idempotency_key=$4`, ws, q.SubmittedBy.Issuer, q.SubmittedBy.Subject, key).Scan(&existing, &fp)
	if err == nil {
		if fp != fingerprint {
			return Query{}, false, ErrConflict
		}
		return existing, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Query{}, false, err
	}
	var active int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM dataslate_queries WHERE workspace_id=$1 AND state NOT IN ('succeeded','failed','cancelled')`, ws).Scan(&active); err != nil {
		return Query{}, false, err
	}
	if active >= MaxWorkspaceQueries {
		return Query{}, false, ErrQueryCapacity
	}
	var n Notebook
	err = tx.QueryRow(ctx, `SELECT id,workspace_id,name,kind,cells,created_at,updated_at,revision FROM dataslate_notebooks WHERE workspace_id=$1 AND id=$2 FOR SHARE`, ws, q.Notebook.ID).Scan(&n.ID, &n.WorkspaceID, &n.Name, &n.Kind, &n.Cells, &n.CreatedAt, &n.UpdatedAt, &n.Revision)
	if err != nil {
		return Query{}, false, storeError(err)
	}
	source, err := snapshotSQL(q, n)
	if err != nil {
		return Query{}, false, err
	}
	q.Notebook = source.Notebook
	if _, err = tx.Exec(ctx, `INSERT INTO dataslate_queries(id,workspace_id,issuer,subject,idempotency_key,request_fingerprint,document,policy,cursor,state,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,'{}',$9,$10)`, q.ID, ws, q.SubmittedBy.Issuer, q.SubmittedBy.Subject, key, fingerprint, q, policy, q.State, q.CreatedAt); err != nil {
		return Query{}, false, storeError(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO dataslate_query_outputs(query_id,source,result) VALUES($1,$2,$3)`, q.ID, source, emptyQueryResult(q)); err != nil {
		return Query{}, false, err
	}
	return q, false, tx.Commit(ctx)
}
func (s *PostgresStore) Queries(ctx context.Context, ws string, owner ExecutionSubject) ([]Query, error) {
	rows, err := s.pool.Query(ctx, `SELECT document FROM dataslate_queries WHERE workspace_id=$1 AND issuer=$2 AND subject=$3 ORDER BY created_at DESC LIMIT 200`, ws, owner.Issuer, owner.Subject)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Query{}
	for rows.Next() {
		var q Query
		if err = rows.Scan(&q); err != nil {
			return nil, err
		}
		out = append(out, q)
	}
	return out, rows.Err()
}
func (s *PostgresStore) Query(ctx context.Context, ws, id string, owner ExecutionSubject) (Query, error) {
	var q Query
	err := s.pool.QueryRow(ctx, `SELECT document FROM dataslate_queries WHERE workspace_id=$1 AND id=$2 AND issuer=$3 AND subject=$4`, ws, id, owner.Issuer, owner.Subject).Scan(&q)
	return q, storeError(err)
}
func (s *PostgresStore) QuerySource(ctx context.Context, ws, id string, owner ExecutionSubject) (QuerySource, error) {
	var source QuerySource
	err := s.pool.QueryRow(ctx, `SELECT o.source FROM dataslate_query_outputs o JOIN dataslate_queries q ON q.id=o.query_id WHERE q.workspace_id=$1 AND q.id=$2 AND q.issuer=$3 AND q.subject=$4`, ws, id, owner.Issuer, owner.Subject).Scan(&source)
	return source, storeError(err)
}
func (s *PostgresStore) QueryResult(ctx context.Context, ws, id string, owner ExecutionSubject) (QueryResult, error) {
	var result QueryResult
	err := s.pool.QueryRow(ctx, `SELECT o.result FROM dataslate_query_outputs o JOIN dataslate_queries q ON q.id=o.query_id WHERE q.workspace_id=$1 AND q.id=$2 AND q.issuer=$3 AND q.subject=$4`, ws, id, owner.Issuer, owner.Subject).Scan(&result)
	return result, storeError(err)
}
func (s *PostgresStore) QueryPolicy(ctx context.Context, ws, id string, owner ExecutionSubject) (SQLPolicy, error) {
	var policy SQLPolicy
	err := s.pool.QueryRow(ctx, `SELECT policy FROM dataslate_queries WHERE workspace_id=$1 AND id=$2 AND issuer=$3 AND subject=$4`, ws, id, owner.Issuer, owner.Subject).Scan(&policy)
	return policy, storeError(err)
}
func (s *PostgresStore) CancelQuery(ctx context.Context, ws, id string, owner ExecutionSubject) (Query, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Query{}, err
	}
	defer tx.Rollback(ctx)
	var q Query
	if err = tx.QueryRow(ctx, `SELECT document FROM dataslate_queries WHERE workspace_id=$1 AND id=$2 AND issuer=$3 AND subject=$4 FOR UPDATE`, ws, id, owner.Issuer, owner.Subject).Scan(&q); err != nil {
		return Query{}, storeError(err)
	}
	q = requestQueryCancel(q)
	if _, err = tx.Exec(ctx, `UPDATE dataslate_queries SET document=$2,state=$3,next_reconcile_at=clock_timestamp() WHERE id=$1`, id, q, q.State); err != nil {
		return Query{}, err
	}
	return q, tx.Commit(ctx)
}
func (s *PostgresStore) AcquireQuery(ctx context.Context, owner string, ttl time.Duration) (QueryWork, QueryLease, error) {
	if owner == "" || len(owner) > 200 || ttl <= 0 || ttl > time.Minute {
		return QueryWork{}, QueryLease{}, errors.New("invalid query lease")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return QueryWork{}, QueryLease{}, err
	}
	defer tx.Rollback(ctx)
	var work QueryWork
	lease := QueryLease{Owner: owner}
	err = tx.QueryRow(ctx, `WITH candidate AS (SELECT id FROM dataslate_queries WHERE state NOT IN ('succeeded','failed','cancelled') AND next_reconcile_at<=clock_timestamp() AND (lease_until IS NULL OR lease_until<=clock_timestamp()) ORDER BY next_reconcile_at,id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE dataslate_queries q SET lease_owner=$1,lease_fence=q.lease_fence+1,lease_until=clock_timestamp()+($2*interval '1 millisecond') FROM candidate WHERE q.id=candidate.id RETURNING q.id,q.document,q.policy,q.cursor,q.processed,q.lease_fence`, owner, ttl.Milliseconds()).Scan(&lease.QueryID, &work.Query, &work.Policy, &work.Cursor, &work.Processed, &lease.Fence)
	if err != nil {
		return QueryWork{}, QueryLease{}, storeError(err)
	}
	if err = tx.QueryRow(ctx, `SELECT source,result FROM dataslate_query_outputs WHERE query_id=$1`, lease.QueryID).Scan(&work.Source, &work.Result); err != nil {
		return QueryWork{}, QueryLease{}, err
	}
	return work, lease, tx.Commit(ctx)
}
func (s *PostgresStore) AdvanceQuery(ctx context.Context, lease QueryLease, u QueryUpdate) (Query, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Query{}, err
	}
	defer tx.Rollback(ctx)
	var q Query
	var now time.Time
	err = tx.QueryRow(ctx, `SELECT document,clock_timestamp() FROM dataslate_queries WHERE id=$1 AND lease_owner=$2 AND lease_fence=$3 AND lease_until>clock_timestamp() FOR UPDATE`, lease.QueryID, lease.Owner, lease.Fence).Scan(&q, &now)
	if errors.Is(err, pgx.ErrNoRows) {
		return Query{}, ErrLeaseLost
	}
	if err != nil {
		return Query{}, err
	}
	q, err = applyQueryUpdate(q, u, now)
	if err != nil {
		return Query{}, err
	}
	if u.Result != nil {
		if _, err = tx.Exec(ctx, `UPDATE dataslate_query_outputs SET result=$2 WHERE query_id=$1`, q.ID, u.Result); err != nil {
			return Query{}, err
		}
	}
	if u.Processed != nil {
		if _, err = tx.Exec(ctx, `UPDATE dataslate_queries SET processed=$2 WHERE id=$1`, q.ID, u.Processed); err != nil {
			return Query{}, err
		}
	}
	if u.Cursor != nil {
		if _, err = tx.Exec(ctx, `UPDATE dataslate_queries SET cursor=$2 WHERE id=$1`, q.ID, u.Cursor); err != nil {
			return Query{}, err
		}
	}
	var affected int64
	if u.RetainLease {
		tag, e := tx.Exec(ctx, `UPDATE dataslate_queries SET document=$2,state=$3 WHERE id=$1 AND lease_owner=$4 AND lease_fence=$5 AND lease_until>clock_timestamp()`, q.ID, q, q.State, lease.Owner, lease.Fence)
		err = e
		affected = tag.RowsAffected()
	} else {
		tag, e := tx.Exec(ctx, `UPDATE dataslate_queries SET document=$2,state=$3,lease_owner=NULL,lease_until=NULL,next_reconcile_at=clock_timestamp()+($4*interval '1 millisecond') WHERE id=$1 AND lease_owner=$5 AND lease_fence=$6 AND lease_until>clock_timestamp()`, q.ID, q, q.State, u.RetryAfter.Milliseconds(), lease.Owner, lease.Fence)
		err = e
		affected = tag.RowsAffected()
	}
	if err != nil {
		return Query{}, err
	}
	if affected != 1 {
		return Query{}, ErrLeaseLost
	}
	return q, tx.Commit(ctx)
}
