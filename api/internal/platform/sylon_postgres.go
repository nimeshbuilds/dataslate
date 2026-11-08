package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

func (s *PostgresStore) CreateSylonConversation(ctx context.Context, c SylonConversation) (SylonConversation, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return SylonConversation{}, err
	}
	defer tx.Rollback(ctx)
	var ws string
	if err = tx.QueryRow(ctx, "SELECT id FROM dataslate_workspaces WHERE id=$1 FOR UPDATE", c.WorkspaceID).Scan(&ws); err != nil {
		return SylonConversation{}, storeError(err)
	}
	var count int
	if err = tx.QueryRow(ctx, "SELECT count(*) FROM dataslate_sylon_conversations WHERE workspace_id=$1 AND issuer=$2 AND subject=$3", ws, c.Owner.Issuer, c.Owner.Subject).Scan(&count); err != nil {
		return SylonConversation{}, err
	}
	if count >= 100 {
		return SylonConversation{}, ErrSylonCapacity
	}
	data, _ := json.Marshal(c)
	if _, err = tx.Exec(ctx, "INSERT INTO dataslate_sylon_conversations(id,workspace_id,binding_id,issuer,subject,document,created_at) VALUES($1,$2,$3,$4,$5,$6,$7)", c.ID, ws, c.BindingID, c.Owner.Issuer, c.Owner.Subject, data, c.CreatedAt); err != nil {
		return SylonConversation{}, err
	}
	return c, tx.Commit(ctx)
}
func (s *PostgresStore) SylonConversations(ctx context.Context, ws string, owner ExecutionSubject) ([]SylonConversation, error) {
	rows, err := s.pool.Query(ctx, "SELECT document FROM dataslate_sylon_conversations WHERE workspace_id=$1 AND issuer=$2 AND subject=$3 ORDER BY created_at DESC", ws, owner.Issuer, owner.Subject)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []SylonConversation{}
	for rows.Next() {
		var data []byte
		if err = rows.Scan(&data); err != nil {
			return nil, err
		}
		var c SylonConversation
		if err = json.Unmarshal(data, &c); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
func (s *PostgresStore) SylonConversation(ctx context.Context, ws, id string, owner ExecutionSubject) (SylonConversation, error) {
	var data []byte
	err := s.pool.QueryRow(ctx, "SELECT document FROM dataslate_sylon_conversations WHERE workspace_id=$1 AND id=$2 AND issuer=$3 AND subject=$4", ws, id, owner.Issuer, owner.Subject).Scan(&data)
	if err != nil {
		return SylonConversation{}, storeError(err)
	}
	var c SylonConversation
	err = json.Unmarshal(data, &c)
	return c, err
}
func (s *PostgresStore) DeleteSylonConversation(ctx context.Context, ws, id string, owner ExecutionSubject) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = lockMemoryWorkspace(ctx, tx, ws); err != nil {
		return err
	}
	var found string
	if err = tx.QueryRow(ctx, "SELECT id FROM dataslate_sylon_conversations WHERE workspace_id=$1 AND id=$2 AND issuer=$3 AND subject=$4 FOR UPDATE", ws, id, owner.Issuer, owner.Subject).Scan(&found); err != nil {
		return storeError(err)
	}
	if _, err = tx.Exec(ctx, "SELECT id FROM dataslate_sylon_jobs WHERE conversation_id=$1 ORDER BY id FOR UPDATE", id); err != nil {
		return err
	}
	if err = invalidateConversationMemoriesPG(ctx, tx, ws, id); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "DELETE FROM dataslate_sylon_conversations WHERE id=$1", id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func scanSylonWork(row pgx.Row) (SylonWork, error) {
	var data, prepared []byte
	var hash string
	err := row.Scan(&data, &hash, &prepared)
	if err != nil {
		return SylonWork{}, storeError(err)
	}
	var work SylonWork
	work.PolicyHash = hash
	if err = json.Unmarshal(data, &work.Job); err != nil {
		return SylonWork{}, err
	}
	err = json.Unmarshal(prepared, &work.Context)
	return work, err
}
func (s *PostgresStore) SylonHistory(ctx context.Context, ws, id string, owner ExecutionSubject) ([]SylonWork, error) {
	if _, err := s.SylonConversation(ctx, ws, id, owner); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, "SELECT document,policy_hash,context FROM dataslate_sylon_jobs WHERE workspace_id=$1 AND conversation_id=$2 AND issuer=$3 AND subject=$4 ORDER BY created_at,id", ws, id, owner.Issuer, owner.Subject)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []SylonWork{}
	for rows.Next() {
		work, err := scanSylonWork(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, work)
	}
	return out, rows.Err()
}
func (s *PostgresStore) CreateSylonJob(ctx context.Context, work SylonWork, key, fingerprint string) (SylonJob, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return SylonJob{}, false, err
	}
	defer tx.Rollback(ctx)
	j := work.Job
	var ws string
	if err = tx.QueryRow(ctx, "SELECT id FROM dataslate_workspaces WHERE id=$1 FOR UPDATE", j.WorkspaceID).Scan(&ws); err != nil {
		return SylonJob{}, false, storeError(err)
	}
	var cData []byte
	if err = tx.QueryRow(ctx, "SELECT document FROM dataslate_sylon_conversations WHERE workspace_id=$1 AND id=$2 AND issuer=$3 AND subject=$4 FOR UPDATE", ws, j.ConversationID, j.SubmittedBy.Issuer, j.SubmittedBy.Subject).Scan(&cData); err != nil {
		return SylonJob{}, false, storeError(err)
	}
	var conversation SylonConversation
	if err = json.Unmarshal(cData, &conversation); err != nil {
		return SylonJob{}, false, err
	}
	if conversation.BindingID != j.BindingID {
		return SylonJob{}, false, ErrConflict
	}
	var old []byte
	var previous string
	err = tx.QueryRow(ctx, "SELECT document,fingerprint FROM dataslate_sylon_jobs WHERE conversation_id=$1 AND idempotency_key=$2", j.ConversationID, key).Scan(&old, &previous)
	if err == nil {
		if previous != fingerprint {
			return SylonJob{}, false, ErrConflict
		}
		var out SylonJob
		err = json.Unmarshal(old, &out)
		return out, true, err
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return SylonJob{}, false, err
	}
	var messages, active int
	if err = tx.QueryRow(ctx, `SELECT COALESCE(sum(1+CASE WHEN document->'result' IS NOT NULL AND document->'result'<>'null'::jsonb THEN 1 ELSE 0 END),0),count(*) FILTER(WHERE state IN ('queued','preparing','generating','cancel_requested')) FROM dataslate_sylon_jobs WHERE conversation_id=$1`, j.ConversationID).Scan(&messages, &active); err != nil {
		return SylonJob{}, false, err
	}
	if messages+2 > MaxSylonHistoryMessages || active > 0 {
		return SylonJob{}, false, ErrSylonCapacity
	}
	if err = tx.QueryRow(ctx, "SELECT count(*) FROM dataslate_sylon_jobs WHERE workspace_id=$1 AND state IN ('queued','preparing','generating','cancel_requested')", ws).Scan(&active); err != nil {
		return SylonJob{}, false, err
	}
	if active >= MaxSylonWorkspaceJobs {
		return SylonJob{}, false, ErrSylonCapacity
	}
	if j.Notebook != nil {
		n, err := scanNotebook(tx.QueryRow(ctx, "SELECT "+notebookColumns+" FROM dataslate_notebooks WHERE workspace_id=$1 AND id=$2 FOR SHARE", ws, j.Notebook.ID))
		if err != nil {
			return SylonJob{}, false, err
		}
		if n.Revision != j.Notebook.Revision {
			return SylonJob{}, false, ErrSylonRevision
		}
		found := false
		for _, cell := range n.Cells {
			if cell.ID == j.Notebook.CellID && (cell.Language == "python" || cell.Language == "sql") {
				found = true
			}
		}
		if !found {
			return SylonJob{}, false, ErrSylonRevision
		}
	}
	if err = memoryDepsCurrentPG(ctx, tx, ws, j.SubmittedBy, memorySelectionDependencies(j.Memories), ""); err != nil {
		return SylonJob{}, false, err
	}
	data, _ := json.Marshal(j)
	prepared, _ := json.Marshal(work.Context)
	if _, err = tx.Exec(ctx, `INSERT INTO dataslate_sylon_jobs(id,workspace_id,conversation_id,issuer,subject,idempotency_key,fingerprint,policy_hash,document,context,state,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, j.ID, ws, j.ConversationID, j.SubmittedBy.Issuer, j.SubmittedBy.Subject, key, fingerprint, work.PolicyHash, data, prepared, j.State, j.CreatedAt); err != nil {
		return SylonJob{}, false, err
	}
	conversation.Revision++
	conversation.UpdatedAt = time.Now().UTC()
	cData, _ = json.Marshal(conversation)
	if _, err = tx.Exec(ctx, "UPDATE dataslate_sylon_conversations SET document=$2 WHERE id=$1", conversation.ID, cData); err != nil {
		return SylonJob{}, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return SylonJob{}, false, err
	}
	return j, false, nil
}
func (s *PostgresStore) SylonWork(ctx context.Context, ws, id string, owner ExecutionSubject) (SylonWork, error) {
	return scanSylonWork(s.pool.QueryRow(ctx, "SELECT document,policy_hash,context FROM dataslate_sylon_jobs WHERE workspace_id=$1 AND id=$2 AND issuer=$3 AND subject=$4", ws, id, owner.Issuer, owner.Subject))
}
func (s *PostgresStore) CancelSylonJob(ctx context.Context, ws, id string, owner ExecutionSubject) (SylonJob, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return SylonJob{}, err
	}
	defer tx.Rollback(ctx)
	work, err := scanSylonWork(tx.QueryRow(ctx, "SELECT document,policy_hash,context FROM dataslate_sylon_jobs WHERE workspace_id=$1 AND id=$2 AND issuer=$3 AND subject=$4 FOR UPDATE", ws, id, owner.Issuer, owner.Subject))
	if err != nil {
		return SylonJob{}, err
	}
	cancelSylon(&work.Job)
	data, _ := json.Marshal(work.Job)
	if _, err = tx.Exec(ctx, "UPDATE dataslate_sylon_jobs SET document=$2,state=$3 WHERE id=$1", id, data, work.Job.State); err != nil {
		return SylonJob{}, err
	}
	return work.Job, tx.Commit(ctx)
}
func (s *PostgresStore) AcquireSylonJob(ctx context.Context, owner string, ttl time.Duration) (SylonWork, SylonLease, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return SylonWork{}, SylonLease{}, err
	}
	defer tx.Rollback(ctx)
	work, err := scanSylonWork(tx.QueryRow(ctx, "SELECT document,policy_hash,context FROM dataslate_sylon_jobs WHERE state IN ('queued','preparing','generating','cancel_requested') AND (lease_until IS NULL OR lease_until<clock_timestamp()) ORDER BY created_at,id FOR UPDATE SKIP LOCKED LIMIT 1"))
	if err != nil {
		return SylonWork{}, SylonLease{}, err
	}
	if work.Job.State == "queued" {
		work.Job.State = "preparing"
	}
	work.Job.Revision++
	work.Job.UpdatedAt = time.Now().UTC()
	data, _ := json.Marshal(work.Job)
	var fence int64
	err = tx.QueryRow(ctx, "UPDATE dataslate_sylon_jobs SET document=$2,state=$3,lease_owner=$4,lease_fence=lease_fence+1,lease_until=clock_timestamp()+$5::interval WHERE id=$1 RETURNING lease_fence", work.Job.ID, data, work.Job.State, owner, fmt.Sprintf("%f seconds", ttl.Seconds())).Scan(&fence)
	if err != nil {
		return SylonWork{}, SylonLease{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return SylonWork{}, SylonLease{}, err
	}
	return work, SylonLease{work.Job.ID, owner, fence}, nil
}
func sylonLeaseRow(ctx context.Context, tx pgx.Tx, l SylonLease) (SylonWork, error) {
	// Lock first, then evaluate the deadline using a fresh statement. The WHERE
	// clause of a blocked SELECT FOR UPDATE may run before the lock is obtained.
	var id string
	if err := tx.QueryRow(ctx, "SELECT id FROM dataslate_sylon_jobs WHERE id=$1 FOR UPDATE", l.ID).Scan(&id); err != nil {
		return SylonWork{}, storeError(err)
	}
	work, err := scanSylonWork(tx.QueryRow(ctx, "SELECT document,policy_hash,context FROM dataslate_sylon_jobs WHERE id=$1 AND lease_owner=$2 AND lease_fence=$3 AND lease_until>clock_timestamp()", l.ID, l.Owner, l.Fence))
	if errors.Is(err, ErrNotFound) {
		return SylonWork{}, ErrSylonLease
	}
	return work, err
}
func (s *PostgresStore) SaveSylonContext(ctx context.Context, l SylonLease, prepared SylonPreparedContext) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = lockSylonMemoryWorkspace(ctx, tx, l); err != nil {
		return err
	}
	work, err := sylonLeaseRow(ctx, tx, l)
	if err != nil {
		return err
	}
	if work.Job.CancelRequested || work.Job.Terminal() {
		return ErrSylonCancelled
	}
	if err = memoryDepsCurrentPG(ctx, tx, work.Job.WorkspaceID, work.Job.SubmittedBy, prepared.Dependencies, l.ID); err != nil {
		return err
	}
	if _, err = sylonLeaseRow(ctx, tx, l); err != nil {
		return err
	}
	work.Job.Citations = prepared.Citations
	data, _ := json.Marshal(work.Job)
	contextData, _ := json.Marshal(prepared)
	if _, err = tx.Exec(ctx, "UPDATE dataslate_sylon_jobs SET document=$2,context=$3 WHERE id=$1", l.ID, data, contextData); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *PostgresStore) MarkSylonDispatch(ctx context.Context, l SylonLease, stage string) error {
	if stage != "preparing" && stage != "generating" {
		return errors.New("invalid provider stage")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = lockSylonMemoryWorkspace(ctx, tx, l); err != nil {
		return err
	}
	work, err := sylonLeaseRow(ctx, tx, l)
	if err != nil {
		return err
	}
	if work.Job.CancelRequested || work.Job.Terminal() {
		return ErrSylonCancelled
	}
	if err = memoryDepsCurrentPG(ctx, tx, work.Job.WorkspaceID, work.Job.SubmittedBy, work.Context.Dependencies, ""); err != nil {
		return err
	}
	if _, err = sylonLeaseRow(ctx, tx, l); err != nil {
		return err
	}
	work.Job.DispatchStarted = true
	work.Job.State = stage
	work.Job.Revision++
	work.Job.UpdatedAt = time.Now().UTC()
	data, _ := json.Marshal(work.Job)
	if _, err = tx.Exec(ctx, "UPDATE dataslate_sylon_jobs SET document=$2,state=$3 WHERE id=$1", l.ID, data, stage); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *PostgresStore) FinishSylonJob(ctx context.Context, l SylonLease, state, message string, prepared *SylonPreparedContext, resultJob *SylonJob) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = lockSylonMemoryWorkspace(ctx, tx, l); err != nil {
		return err
	}
	work, err := sylonLeaseRow(ctx, tx, l)
	if err != nil {
		return err
	}
	deps := append([]SylonDependency{}, work.Context.Dependencies...)
	if prepared != nil {
		deps = append(deps, prepared.Dependencies...)
	}
	if state == "succeeded" && !work.Job.CancelRequested {
		if selected := work.Job.Notebook; selected != nil {
			var revision int
			err = tx.QueryRow(ctx, "SELECT revision FROM dataslate_notebooks WHERE workspace_id=$1 AND id=$2 FOR SHARE", work.Job.WorkspaceID, selected.ID).Scan(&revision)
			if errors.Is(err, pgx.ErrNoRows) || (err == nil && revision != selected.Revision) {
				state, message, resultJob = "failed", "The selected saved notebook changed before publication; output was discarded.", nil
			} else if err != nil {
				return err
			}
			// A notebook writer may have held the lock past this worker's lease.
			if _, err = sylonLeaseRow(ctx, tx, l); err != nil {
				return err
			}
		}
	}
	// Notebook and all memory locks are now held. Revalidate time/fence last.
	if err = memoryDepsCurrentPG(ctx, tx, work.Job.WorkspaceID, work.Job.SubmittedBy, deps, l.ID); err != nil {
		if !errors.Is(err, ErrMemoryAccess) {
			return err
		}
		redactMemoryJob(&work)
		state = "failed"
		message = work.Job.Message
		prepared = nil
		resultJob = nil
	}
	if _, err = sylonLeaseRow(ctx, tx, l); err != nil {
		return err
	}
	if err = finishSylon(&work, state, message, prepared, resultJob); err != nil {
		return err
	}
	data, _ := json.Marshal(work.Job)
	contextData, _ := json.Marshal(work.Context)
	if _, err = tx.Exec(ctx, "UPDATE dataslate_sylon_jobs SET document=$2,context=$3,state=$4,lease_until=NULL WHERE id=$1", l.ID, data, contextData, work.Job.State); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *PostgresStore) ApplySylonCode(ctx context.Context, ws, id string, owner ExecutionSubject, in SylonApplyInput) (SylonApplyReceipt, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return SylonApplyReceipt{}, err
	}
	defer tx.Rollback(ctx)
	if err = lockMemoryWorkspace(ctx, tx, ws); err != nil {
		return SylonApplyReceipt{}, err
	}
	work, err := scanSylonWork(tx.QueryRow(ctx, "SELECT document,policy_hash,context FROM dataslate_sylon_jobs WHERE workspace_id=$1 AND id=$2 AND issuer=$3 AND subject=$4 FOR UPDATE", ws, id, owner.Issuer, owner.Subject))
	if err != nil {
		return SylonApplyReceipt{}, err
	}
	if work.Job.MemoryInvalidated {
		return SylonApplyReceipt{}, ErrMemoryAccess
	}
	if err = memoryDepsCurrentPG(ctx, tx, ws, owner, work.Context.Dependencies, ""); err != nil {
		return SylonApplyReceipt{}, err
	}
	if old := work.Job.Applied; old != nil {
		if old.NotebookID != in.NotebookID || old.CellID != in.CellID || old.PreviousRevision != in.Revision {
			return SylonApplyReceipt{}, ErrConflict
		}
		return *old, nil
	}
	n, err := scanNotebook(tx.QueryRow(ctx, "SELECT "+notebookColumns+" FROM dataslate_notebooks WHERE workspace_id=$1 AND id=$2 FOR UPDATE", ws, in.NotebookID))
	if err != nil {
		return SylonApplyReceipt{}, err
	}
	if err = memoryDepsCurrentPG(ctx, tx, ws, owner, work.Context.Dependencies, ""); err != nil {
		return SylonApplyReceipt{}, err
	}
	next, err := sylonApplyNotebook(work.Job, n, in)
	if err != nil {
		return SylonApplyReceipt{}, err
	}
	if _, err = tx.Exec(ctx, "UPDATE dataslate_notebooks SET cells=$3,revision=$4,updated_at=$5 WHERE workspace_id=$1 AND id=$2", ws, n.ID, next.Cells, next.Revision, next.UpdatedAt); err != nil {
		return SylonApplyReceipt{}, err
	}
	document, _ := json.Marshal(next)
	if _, err = tx.Exec(ctx, "INSERT INTO dataslate_notebook_revisions(workspace_id,notebook_id,revision,document) VALUES($1,$2,$3,$4)", ws, n.ID, next.Revision, document); err != nil {
		return SylonApplyReceipt{}, err
	}
	if _, err = tx.Exec(ctx, "DELETE FROM dataslate_search_documents WHERE workspace_id=$1 AND notebook_id=$2", ws, n.ID); err != nil {
		return SylonApplyReceipt{}, err
	}
	if _, err = tx.Exec(ctx, "INSERT INTO dataslate_search_changes(notebook_id,workspace_id,revision) VALUES($1,$2,$3) ON CONFLICT(notebook_id) DO UPDATE SET revision=EXCLUDED.revision", n.ID, ws, next.Revision); err != nil {
		return SylonApplyReceipt{}, err
	}
	receipt := SylonApplyReceipt{JobID: id, WorkspaceID: ws, NotebookID: n.ID, CellID: in.CellID, PreviousRevision: in.Revision, Revision: next.Revision, AppliedAt: next.UpdatedAt}
	work.Job.Applied = &receipt
	work.Job.Revision++
	work.Job.UpdatedAt = next.UpdatedAt
	data, _ := json.Marshal(work.Job)
	if _, err = tx.Exec(ctx, "UPDATE dataslate_sylon_jobs SET document=$2 WHERE id=$1", id, data); err != nil {
		return SylonApplyReceipt{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return SylonApplyReceipt{}, err
	}
	return receipt, nil
}
