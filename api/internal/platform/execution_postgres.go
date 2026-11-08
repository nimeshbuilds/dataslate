package platform

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func (s *PostgresStore) CreateRun(ctx context.Context, r Run, key, fingerprint string) (Run, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Run{}, false, err
	}
	defer tx.Rollback(ctx)
	// Serialize permanent location reservations across all workspaces before their row locks.
	if r.Profile.Kind() == FileIngestionBatch {
		if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(72610524002)`); err != nil {
			return Run{}, false, err
		}
	}
	// Serializing on the workspace row makes capacity enforcement atomic across API replicas and owners.
	var ws string
	if err = tx.QueryRow(ctx, `SELECT id FROM dataslate_workspaces WHERE id=$1 FOR UPDATE`, r.WorkspaceID).Scan(&ws); err != nil {
		return Run{}, false, storeError(err)
	}
	var existing Run
	var actual string
	err = tx.QueryRow(ctx, `SELECT document,request_fingerprint FROM dataslate_runs WHERE workspace_id=$1 AND issuer=$2 AND subject=$3 AND idempotency_key=$4`, r.WorkspaceID, r.SubmittedBy.Issuer, r.SubmittedBy.Subject, key).Scan(&existing, &actual)
	if err == nil {
		if actual != fingerprint {
			return Run{}, false, ErrConflict
		}
		return existing, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Run{}, false, err
	}
	var active int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM dataslate_runs WHERE workspace_id=$1 AND (state NOT IN ('succeeded','failed','cancelled') OR document->'sourceArtifact'->>'state' IN ('pending','ready','cleanup_pending') OR (document ? 'ingestion' AND COALESCE((document->>'writerReleased')::boolean,false)=false))`, ws).Scan(&active); err != nil {
		return Run{}, false, err
	}
	if active >= MaxActiveWorkspaceRuns {
		return Run{}, false, ErrRunCapacity
	}
	if r.Profile.Kind() == NotebookPythonBatch {
		if r.Notebook == nil {
			return Run{}, false, ErrNotebookNotExecutable
		}
		var n Notebook
		// Hold the current notebook row against concurrent edit/delete until source+run commit.
		err = tx.QueryRow(ctx, `SELECT id,workspace_id,name,kind,cells,created_at,updated_at,revision FROM dataslate_notebooks WHERE workspace_id=$1 AND id=$2 FOR SHARE`, r.WorkspaceID, r.Notebook.ID).Scan(&n.ID, &n.WorkspaceID, &n.Name, &n.Kind, &n.Cells, &n.CreatedAt, &n.UpdatedAt, &n.Revision)
		if err != nil {
			return Run{}, false, storeError(err)
		}
		r, err = snapshotNotebook(r, n)
		if err != nil {
			return Run{}, false, err
		}
	} else if r.Profile.Kind() == FileIngestionBatch {
		r, err = snapshotIngestionTransaction(ctx, tx, r)
		if err != nil {
			return Run{}, false, err
		}
	} else if r.Ingestion != nil {
		return Run{}, false, ErrIngestionNotExecutable
	} else if r.Notebook != nil {
		return Run{}, false, ErrNotebookNotExecutable
	}
	_, err = tx.Exec(ctx, `INSERT INTO dataslate_runs(id,workspace_id,issuer,subject,idempotency_key,request_fingerprint,document,state,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, r.ID, r.WorkspaceID, r.SubmittedBy.Issuer, r.SubmittedBy.Subject, key, fingerprint, r, r.State, r.CreatedAt)
	if err != nil {
		return Run{}, false, storeError(err)
	}
	if r.Source != nil {
		if _, err = tx.Exec(ctx, `INSERT INTO dataslate_run_sources(run_id,document) VALUES($1,$2)`, r.ID, r.Source); err != nil {
			return Run{}, false, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return Run{}, false, err
	}
	return r, false, nil
}
func (s *PostgresStore) Runs(ctx context.Context, ws string, subject ExecutionSubject) ([]Run, error) {
	rows, err := s.pool.Query(ctx, `SELECT document || jsonb_build_object('events','[]'::jsonb,'eventsTruncated',COALESCE(jsonb_array_length(document->'events'),0)>0 OR COALESCE((document->>'eventsTruncated')::boolean,false)) FROM dataslate_runs WHERE workspace_id=$1 AND issuer=$2 AND subject=$3 ORDER BY created_at DESC LIMIT 200`, ws, subject.Issuer, subject.Subject)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Run{}
	for rows.Next() {
		var r Run
		if err = rows.Scan(&r); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
func (s *PostgresStore) Run(ctx context.Context, ws, id string, subject ExecutionSubject) (Run, error) {
	var r Run
	err := s.pool.QueryRow(ctx, `SELECT document FROM dataslate_runs WHERE workspace_id=$1 AND id=$2 AND issuer=$3 AND subject=$4`, ws, id, subject.Issuer, subject.Subject).Scan(&r)
	return r, storeError(err)
}
func (s *PostgresStore) CancelRun(ctx context.Context, ws, id string, subject ExecutionSubject) (Run, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Run{}, err
	}
	defer tx.Rollback(ctx)
	var r Run
	var now time.Time
	err = tx.QueryRow(ctx, `SELECT document,clock_timestamp() FROM dataslate_runs WHERE workspace_id=$1 AND id=$2 AND issuer=$3 AND subject=$4 FOR UPDATE`, ws, id, subject.Issuer, subject.Subject).Scan(&r, &now)
	if err != nil {
		return Run{}, storeError(err)
	}
	if r.Terminal() || r.CancelRequested {
		return r, nil
	}
	r.CancelRequested = true
	r.State = "cancel_requested"
	r.Message = "Cancellation requested; backend confirmation is pending."
	r.Revision++
	r.UpdatedAt = now
	r = appendRunEvent(r, RunEvent{At: now, State: r.State, Message: r.Message})
	if _, err = tx.Exec(ctx, `UPDATE dataslate_runs SET document=$2,state=$3,next_reconcile_at=clock_timestamp() WHERE id=$1`, id, r, r.State); err != nil {
		return Run{}, err
	}
	return r, tx.Commit(ctx)
}
func (s *PostgresStore) AcquireRun(ctx context.Context, owner string, ttl time.Duration) (Run, RunLease, error) {
	if owner == "" || len(owner) > 200 || ttl <= 0 || ttl > time.Minute {
		return Run{}, RunLease{}, errors.New("invalid execution lease request")
	}
	var r Run
	lease := RunLease{Owner: owner}
	var source *RunSource
	err := s.pool.QueryRow(ctx, `WITH candidate AS (SELECT id FROM dataslate_runs WHERE (state NOT IN ('succeeded','failed','cancelled') OR document->'sourceArtifact'->>'state' IN ('pending','ready','cleanup_pending') OR (document ? 'ingestion' AND COALESCE((document->>'writerReleased')::boolean,false)=false)) AND next_reconcile_at<=clock_timestamp() AND (lease_until IS NULL OR lease_until<=clock_timestamp()) ORDER BY next_reconcile_at,id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE dataslate_runs r SET lease_owner=$1,lease_fence=r.lease_fence+1,lease_until=clock_timestamp()+($2 * interval '1 millisecond') FROM candidate WHERE r.id=candidate.id RETURNING r.id,r.document,r.lease_fence,(SELECT document FROM dataslate_run_sources WHERE run_id=r.id)`, owner, ttl.Milliseconds()).Scan(&lease.RunID, &r, &lease.Fence, &source)
	r.Source = source
	return r, lease, storeError(err)
}
func (s *PostgresStore) AdvanceRun(ctx context.Context, lease RunLease, u RunUpdate) (Run, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Run{}, err
	}
	defer tx.Rollback(ctx)
	var r Run
	var now time.Time
	err = tx.QueryRow(ctx, `SELECT document,clock_timestamp() FROM dataslate_runs WHERE id=$1 AND lease_owner=$2 AND lease_fence=$3 AND lease_until>clock_timestamp() FOR UPDATE`, lease.RunID, lease.Owner, lease.Fence).Scan(&r, &now)
	if errors.Is(err, pgx.ErrNoRows) {
		return Run{}, ErrLeaseLost
	}
	if err != nil {
		return Run{}, err
	}
	r, err = applyRunUpdate(r, u, now)
	if err != nil {
		return Run{}, err
	}
	var tag pgconn.CommandTag
	if u.RetainLease {
		tag, err = tx.Exec(ctx, `UPDATE dataslate_runs SET document=$2,state=$3 WHERE id=$1 AND lease_owner=$4 AND lease_fence=$5 AND lease_until>clock_timestamp()`, r.ID, r, r.State, lease.Owner, lease.Fence)
	} else {
		tag, err = tx.Exec(ctx, `UPDATE dataslate_runs SET document=$2,state=$3,lease_owner=NULL,lease_until=NULL,next_reconcile_at=clock_timestamp()+($4 * interval '1 millisecond') WHERE id=$1 AND lease_owner=$5 AND lease_fence=$6 AND lease_until>clock_timestamp()`, r.ID, r, r.State, u.RetryAfter.Milliseconds(), lease.Owner, lease.Fence)
	}
	if err != nil {
		return Run{}, err
	}
	if tag.RowsAffected() != 1 {
		return Run{}, ErrLeaseLost
	}
	return r, tx.Commit(ctx)
}

func (s *PostgresStore) RunSource(ctx context.Context, ws, id string, subject ExecutionSubject) (RunSource, error) {
	var source RunSource
	err := s.pool.QueryRow(ctx, `SELECT s.document FROM dataslate_run_sources s JOIN dataslate_runs r ON r.id=s.run_id WHERE r.workspace_id=$1 AND r.id=$2 AND r.issuer=$3 AND r.subject=$4`, ws, id, subject.Issuer, subject.Subject).Scan(&source)
	return source, storeError(err)
}
