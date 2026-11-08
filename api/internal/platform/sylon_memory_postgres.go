package platform

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"sort"
	"time"
)

func scanMemory(row pgx.Row) (SylonMemoryWork, error) {
	var data []byte
	if e := row.Scan(&data); e != nil {
		return SylonMemoryWork{}, storeError(e)
	}
	var w SylonMemoryWork
	e := json.Unmarshal(data, &w)
	return w, e
}
func (s *PostgresStore) SylonMemoryWork(ctx context.Context, ws, id string, o ExecutionSubject) (SylonMemoryWork, error) {
	return scanMemory(s.pool.QueryRow(ctx, "SELECT document FROM dataslate_sylon_memories WHERE workspace_id=$1 AND id=$2 AND issuer=$3 AND subject=$4", ws, id, o.Issuer, o.Subject))
}
func (s *PostgresStore) SylonMemories(ctx context.Context, ws string, o ExecutionSubject, binding string, recall bool) ([]SylonMemoryWork, error) {
	rows, e := s.pool.Query(ctx, `SELECT document FROM dataslate_sylon_memories WHERE workspace_id=$1 AND issuer=$2 AND subject=$3 AND ($4='' OR binding_id=$4) AND state<>'deleted' AND (state<>'expired' OR (document->'Memory'->>'purgePending')::boolean) AND (NOT $5 OR(state='active' AND expires_at>clock_timestamp())) ORDER BY updated_at DESC,id LIMIT 100`, ws, o.Issuer, o.Subject, binding, recall)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []SylonMemoryWork{}
	for rows.Next() {
		w, e := scanMemory(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, w)
	}
	return out, rows.Err()
}
func readMemoryMutation(ctx context.Context, tx interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, ws string, o ExecutionSubject, key, fp string) (SylonMemoryReceipt, bool, error) {
	var old string
	var data []byte
	e := tx.QueryRow(ctx, "SELECT fingerprint,receipt FROM dataslate_sylon_memory_mutations WHERE workspace_id=$1 AND issuer=$2 AND subject=$3 AND idempotency_key=$4", ws, o.Issuer, o.Subject, key).Scan(&old, &data)
	if errors.Is(e, pgx.ErrNoRows) {
		return SylonMemoryReceipt{}, false, nil
	}
	if e != nil {
		return SylonMemoryReceipt{}, false, e
	}
	if old != fp {
		return SylonMemoryReceipt{}, false, ErrConflict
	}
	var r SylonMemoryReceipt
	e = json.Unmarshal(data, &r)
	return r, true, e
}
func (s *PostgresStore) SylonMemoryMutation(ctx context.Context, ws string, o ExecutionSubject, key, fp string) (SylonMemoryReceipt, bool, error) {
	return readMemoryMutation(ctx, s.pool, ws, o, key, fp)
}
func lockMemoryWorkspace(ctx context.Context, tx pgx.Tx, ws string) error {
	var id string
	return storeError(tx.QueryRow(ctx, "SELECT id FROM dataslate_workspaces WHERE id=$1 FOR UPDATE", ws).Scan(&id))
}
func writeMemory(ctx context.Context, tx pgx.Tx, w SylonMemoryWork) error {
	data, e := json.Marshal(w)
	if e != nil {
		return e
	}
	_, e = tx.Exec(ctx, `UPDATE dataslate_sylon_memories SET revision=$2,state=$3,expires_at=$4,updated_at=$5,document=$6 WHERE id=$1`, w.Memory.ID, w.Memory.Revision, w.State, w.Memory.ExpiresAt, w.Memory.UpdatedAt, data)
	return e
}
func writeMemoryReceipt(ctx context.Context, tx pgx.Tx, o ExecutionSubject, key, fp string, r SylonMemoryReceipt) error {
	data, _ := json.Marshal(r)
	_, e := tx.Exec(ctx, "INSERT INTO dataslate_sylon_memory_mutations(workspace_id,issuer,subject,idempotency_key,fingerprint,receipt) VALUES($1,$2,$3,$4,$5,$6)", r.WorkspaceID, o.Issuer, o.Subject, key, fp, data)
	return e
}
func queueMemoryPurge(ctx context.Context, tx pgx.Tx, w SylonMemoryWork, through int) error {
	_, e := tx.Exec(ctx, "INSERT INTO dataslate_sylon_memory_purges(workspace_id,memory_id,through_revision) VALUES($1,$2,$3) ON CONFLICT DO NOTHING", w.Memory.WorkspaceID, w.Memory.ID, through)
	return e
}

// All callers acquire the workspace row first. Lock every dependency before
// evaluating deadlines using the database clock; a later lock must not make an
// earlier expiry check stale.
func memoryDepsCurrentPG(ctx context.Context, tx pgx.Tx, ws string, o ExecutionSubject, deps []SylonDependency, recordJob string) error {
	ordered := append([]SylonDependency{}, deps...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].MemoryID < ordered[j].MemoryID })
	locked := []SylonMemoryWork{}
	for _, d := range ordered {
		if d.Kind != "memory" {
			continue
		}
		w, e := scanMemory(tx.QueryRow(ctx, "SELECT document FROM dataslate_sylon_memories WHERE workspace_id=$1 AND id=$2 AND issuer=$3 AND subject=$4 FOR SHARE", ws, d.MemoryID, o.Issuer, o.Subject))
		if e != nil || w.State != "active" || w.Memory.Revision != d.MemoryRevision {
			return ErrMemoryAccess
		}
		origin, e := scanSylonWork(tx.QueryRow(ctx, "SELECT document,policy_hash,context FROM dataslate_sylon_jobs WHERE workspace_id=$1 AND id=$2 AND issuer=$3 AND subject=$4", ws, w.Memory.Source.JobID, o.Issuer, o.Subject))
		if e != nil || origin.Job.MemoryInvalidated || origin.Job.State != "succeeded" || origin.Job.Result == nil || SylonOutputHash(origin.Job.Result) != w.OutputHash {
			return ErrMemoryAccess
		}
		locked = append(locked, w)
	}
	var now time.Time
	if err := tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return err
	}
	for _, w := range locked {
		if !w.Memory.ExpiresAt.After(now) {
			return ErrMemoryAccess
		}
	}
	if recordJob != "" {
		for _, w := range locked {
			if _, e := tx.Exec(ctx, "INSERT INTO dataslate_sylon_memory_uses(workspace_id,job_id,memory_id,memory_revision) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING", ws, recordJob, w.Memory.ID, w.Memory.Revision); e != nil {
				return e
			}
		}
	}
	return nil
}
func lockSylonMemoryWorkspace(ctx context.Context, tx pgx.Tx, l SylonLease) error {
	var ws string
	if e := tx.QueryRow(ctx, "SELECT workspace_id FROM dataslate_sylon_jobs WHERE id=$1", l.ID).Scan(&ws); e != nil {
		return storeError(e)
	}
	return lockMemoryWorkspace(ctx, tx, ws)
}
func memoryFuturePG(ctx context.Context, tx pgx.Tx, deadlines ...time.Time) error {
	var now time.Time
	if e := tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); e != nil {
		return e
	}
	for _, d := range deadlines {
		if !d.After(now) {
			return ErrMemoryAccess
		}
	}
	return nil
}
func (s *PostgresStore) CreateSylonMemory(ctx context.Context, w SylonMemoryWork, key, fp string) (SylonMemoryReceipt, bool, error) {
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return SylonMemoryReceipt{}, false, e
	}
	defer tx.Rollback(ctx)
	m := w.Memory
	if e = lockMemoryWorkspace(ctx, tx, m.WorkspaceID); e != nil {
		return SylonMemoryReceipt{}, false, e
	}
	if r, ok, e := readMemoryMutation(ctx, tx, m.WorkspaceID, m.Owner, key, fp); ok || e != nil {
		return r, ok, e
	}
	origin, e := scanSylonWork(tx.QueryRow(ctx, "SELECT document,policy_hash,context FROM dataslate_sylon_jobs WHERE workspace_id=$1 AND id=$2 AND issuer=$3 AND subject=$4 FOR SHARE", m.WorkspaceID, m.Source.JobID, m.Owner.Issuer, m.Owner.Subject))
	if e != nil {
		return SylonMemoryReceipt{}, false, e
	}
	if origin.Job.State != "succeeded" || origin.Job.Revision != m.Source.Revision || origin.Job.Result == nil || SylonOutputHash(origin.Job.Result) != w.OutputHash || origin.PolicyHash != w.PolicyHash {
		return SylonMemoryReceipt{}, false, ErrMemoryRevision
	}
	if e = memoryDepsCurrentPG(ctx, tx, m.WorkspaceID, m.Owner, w.Dependencies, ""); e != nil {
		return SylonMemoryReceipt{}, false, e
	}
	var count int
	if e = tx.QueryRow(ctx, "SELECT count(*) FROM dataslate_sylon_memories WHERE workspace_id=$1 AND issuer=$2 AND subject=$3 AND state='active'", m.WorkspaceID, m.Owner.Issuer, m.Owner.Subject).Scan(&count); e != nil {
		return SylonMemoryReceipt{}, false, e
	}
	if count >= 100 || len(w.Dependencies) >= MaxSylonDependencies {
		return SylonMemoryReceipt{}, false, ErrMemoryCapacity
	}
	if e = memoryFuturePG(ctx, tx, m.ExpiresAt); e != nil {
		return SylonMemoryReceipt{}, false, e
	}
	data, _ := json.Marshal(w)
	_, e = tx.Exec(ctx, `INSERT INTO dataslate_sylon_memories(id,workspace_id,issuer,subject,binding_id,policy_hash,origin_job_id,origin_conversation_id,revision,state,expires_at,updated_at,document) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, m.ID, m.WorkspaceID, m.Owner.Issuer, m.Owner.Subject, m.BindingID, w.PolicyHash, m.Source.JobID, origin.Job.ConversationID, m.Revision, w.State, m.ExpiresAt, m.UpdatedAt, data)
	if e != nil {
		return SylonMemoryReceipt{}, false, e
	}
	r := memoryReceipt(w, "create", 0)
	if e = writeMemoryReceipt(ctx, tx, m.Owner, key, fp, r); e != nil {
		return SylonMemoryReceipt{}, false, e
	}
	return r, false, tx.Commit(ctx)
}
func (s *PostgresStore) CorrectSylonMemory(ctx context.Context, ws, id string, o ExecutionSubject, in SylonMemoryInput, fp string) (SylonMemoryReceipt, bool, error) {
	return s.mutateMemory(ctx, ws, id, o, in, SylonMemoryDeleteInput{}, fp, false)
}
func (s *PostgresStore) DeleteSylonMemory(ctx context.Context, ws, id string, o ExecutionSubject, in SylonMemoryDeleteInput, fp string) (SylonMemoryReceipt, bool, error) {
	return s.mutateMemory(ctx, ws, id, o, SylonMemoryInput{}, in, fp, true)
}
func (s *PostgresStore) mutateMemory(ctx context.Context, ws, id string, o ExecutionSubject, in SylonMemoryInput, del SylonMemoryDeleteInput, fp string, remove bool) (SylonMemoryReceipt, bool, error) {
	key, expected, op := in.IdempotencyKey, in.Revision, "correct"
	if remove {
		key, expected, op = del.IdempotencyKey, del.Revision, "delete"
	}
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return SylonMemoryReceipt{}, false, e
	}
	defer tx.Rollback(ctx)
	if e = lockMemoryWorkspace(ctx, tx, ws); e != nil {
		return SylonMemoryReceipt{}, false, e
	}
	if r, ok, e := readMemoryMutation(ctx, tx, ws, o, key, fp); ok || e != nil {
		return r, ok, e
	}
	w, e := scanMemory(tx.QueryRow(ctx, "SELECT document FROM dataslate_sylon_memories WHERE workspace_id=$1 AND id=$2 AND issuer=$3 AND subject=$4 FOR UPDATE", ws, id, o.Issuer, o.Subject))
	if e != nil {
		return SylonMemoryReceipt{}, false, e
	}
	if w.Memory.Revision != expected || w.State == "deleted" || (!remove && (w.State != "active" || !w.Memory.ExpiresAt.After(time.Now()))) {
		return SylonMemoryReceipt{}, false, ErrMemoryRevision
	}
	if !remove {
		if e = memoryDepsCurrentPG(ctx, tx, ws, o, w.Dependencies, ""); e != nil {
			return SylonMemoryReceipt{}, false, e
		}
	}
	if !remove {
		if e = memoryFuturePG(ctx, tx, w.Memory.ExpiresAt, in.ExpiresAt); e != nil {
			return SylonMemoryReceipt{}, false, e
		}
	}
	previous := w.Memory.Revision
	w.Memory.Revision++
	w.Memory.UpdatedAt = time.Now().UTC()
	w.Memory.PurgePending = true
	if remove {
		w.State = "deleted"
		w.Memory.Title = ""
		w.Memory.Content = ""
	} else {
		w.Memory.Title = in.Title
		w.Memory.Content = in.Content
		w.Memory.ExpiresAt = in.ExpiresAt
	}
	if e = writeMemory(ctx, tx, w); e != nil {
		return SylonMemoryReceipt{}, false, e
	}
	if e = queueMemoryPurge(ctx, tx, w, previous); e != nil {
		return SylonMemoryReceipt{}, false, e
	}
	r := memoryReceipt(w, op, previous)
	if e = writeMemoryReceipt(ctx, tx, o, key, fp, r); e != nil {
		return SylonMemoryReceipt{}, false, e
	}
	return r, false, tx.Commit(ctx)
}
func invalidateConversationMemoriesPG(ctx context.Context, tx pgx.Tx, ws, conversation string) error {
	rows, e := tx.Query(ctx, "SELECT document FROM dataslate_sylon_memories WHERE workspace_id=$1 AND origin_conversation_id=$2 AND state='active' ORDER BY id FOR UPDATE", ws, conversation)
	if e != nil {
		return e
	}
	items := []SylonMemoryWork{}
	for rows.Next() {
		w, e := scanMemory(rows)
		if e != nil {
			rows.Close()
			return e
		}
		items = append(items, w)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for _, w := range items {
		previous := w.Memory.Revision
		w.State = "deleted"
		w.Memory.Revision++
		w.Memory.Title = ""
		w.Memory.Content = ""
		w.Memory.PurgePending = true
		w.Memory.UpdatedAt = time.Now().UTC()
		if e = writeMemory(ctx, tx, w); e != nil {
			return e
		}
		if e = queueMemoryPurge(ctx, tx, w, previous); e != nil {
			return e
		}
	}
	return nil
}
func (s *PostgresStore) expireMemories(ctx context.Context) error {
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	var ws string
	e = tx.QueryRow(ctx, "SELECT workspace_id FROM dataslate_sylon_memories WHERE state='active' AND expires_at<=clock_timestamp() ORDER BY expires_at,id LIMIT 1").Scan(&ws)
	if errors.Is(e, pgx.ErrNoRows) {
		return nil
	}
	if e != nil {
		return e
	}
	if e = lockMemoryWorkspace(ctx, tx, ws); e != nil {
		return e
	}
	rows, e := tx.Query(ctx, "SELECT document FROM dataslate_sylon_memories WHERE workspace_id=$1 AND state='active' AND expires_at<=clock_timestamp() ORDER BY expires_at,id LIMIT 100 FOR UPDATE", ws)
	if e != nil {
		return e
	}
	items := []SylonMemoryWork{}
	for rows.Next() {
		w, e := scanMemory(rows)
		if e != nil {
			rows.Close()
			return e
		}
		items = append(items, w)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for _, w := range items {
		w.State = "expired"
		w.Memory.Title = ""
		w.Memory.Content = ""
		w.Memory.PurgePending = true
		if e = writeMemory(ctx, tx, w); e != nil {
			return e
		}
		if e = queueMemoryPurge(ctx, tx, w, w.Memory.Revision); e != nil {
			return e
		}
	}
	return tx.Commit(ctx)
}
func (s *PostgresStore) PurgeSylonMemories(ctx context.Context, limit int) (bool, error) {
	if e := s.expireMemories(ctx); e != nil {
		return false, e
	}
	if limit < 1 || limit > 100 {
		limit = 100
	}
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return false, e
	}
	defer tx.Rollback(ctx)
	var queue int64
	var ws, id string
	var through int
	e = tx.QueryRow(ctx, "SELECT id,workspace_id FROM dataslate_sylon_memory_purges ORDER BY id LIMIT 1").Scan(&queue, &ws)
	if errors.Is(e, pgx.ErrNoRows) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	if e = lockMemoryWorkspace(ctx, tx, ws); e != nil {
		return false, e
	}
	e = tx.QueryRow(ctx, "SELECT memory_id,through_revision FROM dataslate_sylon_memory_purges WHERE id=$1 FOR UPDATE SKIP LOCKED", queue).Scan(&id, &through)
	if errors.Is(e, pgx.ErrNoRows) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	parent, e := scanMemory(tx.QueryRow(ctx, "SELECT document FROM dataslate_sylon_memories WHERE workspace_id=$1 AND id=$2", ws, id))
	if e != nil {
		return false, e
	}
	rows, e := tx.Query(ctx, `SELECT j.document,j.policy_hash,j.context FROM dataslate_sylon_jobs j WHERE j.workspace_id=$1 AND EXISTS(SELECT 1 FROM dataslate_sylon_memory_uses u WHERE u.job_id=j.id AND u.memory_id=$2 AND u.memory_revision<=$3) ORDER BY j.id LIMIT $4 FOR UPDATE`, ws, id, through, limit)
	if e != nil {
		return false, e
	}
	jobs := []SylonWork{}
	for rows.Next() {
		w, e := scanSylonWork(rows)
		if e != nil {
			rows.Close()
			return false, e
		}
		jobs = append(jobs, w)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return false, e
	}
	for _, w := range jobs {
		redactMemoryJob(&w)
		doc, _ := json.Marshal(w.Job)
		if _, e = tx.Exec(ctx, "UPDATE dataslate_sylon_jobs SET document=$2,context='{}',state='failed',lease_until=NULL,lease_fence=lease_fence+1 WHERE id=$1", w.Job.ID, doc); e != nil {
			return false, e
		}
		if _, e = tx.Exec(ctx, "DELETE FROM dataslate_sylon_memory_uses WHERE job_id=$1", w.Job.ID); e != nil {
			return false, e
		}
	}
	if len(jobs) == limit {
		return true, tx.Commit(ctx)
	}
	rows, e = tx.Query(ctx, `SELECT document FROM dataslate_sylon_memories WHERE workspace_id=$1 AND issuer=$5 AND subject=$6 AND state='active' AND EXISTS(SELECT 1 FROM jsonb_array_elements(document->'Dependencies') d WHERE d->>'kind'='memory' AND d->>'memoryId'=$2 AND (d->>'memoryRevision')::integer<=$3) ORDER BY id LIMIT $4 FOR UPDATE`, ws, id, through, limit, parent.Memory.Owner.Issuer, parent.Memory.Owner.Subject)
	if e != nil {
		return false, e
	}
	desc := []SylonMemoryWork{}
	for rows.Next() {
		w, e := scanMemory(rows)
		if e != nil {
			rows.Close()
			return false, e
		}
		desc = append(desc, w)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return false, e
	}
	for _, w := range desc {
		old := w.Memory.Revision
		w.State = "deleted"
		w.Memory.Revision++
		w.Memory.Title = ""
		w.Memory.Content = ""
		w.Memory.PurgePending = true
		w.Memory.UpdatedAt = time.Now().UTC()
		if e = writeMemory(ctx, tx, w); e != nil {
			return false, e
		}
		if e = queueMemoryPurge(ctx, tx, w, old); e != nil {
			return false, e
		}
	}
	if len(desc) == limit {
		return true, tx.Commit(ctx)
	}
	if _, e = tx.Exec(ctx, "DELETE FROM dataslate_sylon_memory_purges WHERE id=$1", queue); e != nil {
		return false, e
	}
	w, e := scanMemory(tx.QueryRow(ctx, "SELECT document FROM dataslate_sylon_memories WHERE id=$1 FOR UPDATE", id))
	if e != nil {
		return false, e
	}
	if e = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM dataslate_sylon_memory_purges WHERE memory_id=$1)", id).Scan(&w.Memory.PurgePending); e != nil {
		return false, e
	}
	if e = writeMemory(ctx, tx, w); e != nil {
		return false, e
	}
	return true, tx.Commit(ctx)
}
