package platform

import (
	"context"
	"reflect"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
)

func (s *MemoryStore) IngestionPipelines(_ context.Context, ws string) ([]IngestionPipeline, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []IngestionPipeline{}
	for _, p := range s.ingestion {
		if p.WorkspaceID == ws {
			out = append(out, cloneIngestion(p))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}
func (s *MemoryStore) IngestionPipeline(_ context.Context, ws, id string) (IngestionPipeline, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.ingestion[id]
	if !ok || p.WorkspaceID != ws {
		return IngestionPipeline{}, ErrNotFound
	}
	return cloneIngestion(p), nil
}
func updateIngestion(old IngestionPipeline, in IngestionPipelineInput) (IngestionPipeline, error) {
	if old.Revision != in.Revision {
		return IngestionPipeline{}, ErrConflict
	}
	if !reflect.DeepEqual(old.IngestionSpec, in.IngestionSpec) {
		return IngestionPipeline{}, ErrIngestionConfigImmutable
	}
	old.Name = in.Name
	old.Revision++
	old.UpdatedAt = time.Now().UTC()
	return cloneIngestion(old), nil
}
func (s *MemoryStore) SaveIngestionPipeline(_ context.Context, ws, id string, in IngestionPipelineInput) (IngestionPipeline, error) {
	if err := in.Validate(id != ""); err != nil {
		return IngestionPipeline{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.workspaces[ws]; !ok {
		return IngestionPipeline{}, ErrNotFound
	}
	p := IngestionPipeline{Resource: newResource("pipe-"), WorkspaceID: ws, Name: in.Name, IngestionSpec: in.IngestionSpec}
	if id != "" {
		old, ok := s.ingestion[id]
		if !ok || old.WorkspaceID != ws {
			return IngestionPipeline{}, ErrNotFound
		}
		var err error
		p, err = updateIngestion(old, in)
		if err != nil {
			return IngestionPipeline{}, err
		}
	}
	s.ingestion[p.ID] = cloneIngestion(p)
	return cloneIngestion(p), nil
}
func (s *MemoryStore) DeleteIngestionPipeline(_ context.Context, ws, id string, revision int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.ingestion[id]
	if !ok || p.WorkspaceID != ws {
		return ErrNotFound
	}
	if p.Revision != revision {
		return ErrConflict
	}
	delete(s.ingestion, id)
	return nil
}
func (s *MemoryStore) snapshotIngestionLocked(r Run) (Run, error) {
	if r.Ingestion == nil {
		return Run{}, ErrIngestionNotExecutable
	}
	p, ok := s.ingestion[r.Ingestion.ID]
	if !ok || p.WorkspaceID != r.WorkspaceID {
		return Run{}, ErrNotFound
	}
	r, err := snapshotIngestion(r, p)
	if err != nil {
		return Run{}, err
	}
	for _, entry := range s.executions {
		if entry.Run.Ingestion != nil && entry.Run.Ingestion.ID == p.ID && entry.Run.NeedsReconciliation() {
			return Run{}, ErrIngestionWriterBusy
		}
	}
	for id, reservation := range s.ingestionReservations {
		if id != p.ID && reservation.conflicts(p.IngestionSpec) {
			return Run{}, ErrIngestionLocationConflict
		}
	}
	s.ingestionReservations[p.ID] = ingestionReservation{p.ID, p.WorkspaceID, p.Destination.URI, p.CheckpointURI}
	return r, nil
}
func (s *PostgresStore) IngestionPipelines(ctx context.Context, ws string) ([]IngestionPipeline, error) {
	rows, err := s.pool.Query(ctx, `SELECT document FROM dataslate_ingestion_pipelines WHERE workspace_id=$1 ORDER BY id`, ws)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []IngestionPipeline{}
	for rows.Next() {
		var p IngestionPipeline
		if err = rows.Scan(&p); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
func (s *PostgresStore) IngestionPipeline(ctx context.Context, ws, id string) (IngestionPipeline, error) {
	var p IngestionPipeline
	err := s.pool.QueryRow(ctx, `SELECT document FROM dataslate_ingestion_pipelines WHERE workspace_id=$1 AND id=$2`, ws, id).Scan(&p)
	return p, storeError(err)
}
func (s *PostgresStore) SaveIngestionPipeline(ctx context.Context, ws, id string, in IngestionPipelineInput) (IngestionPipeline, error) {
	if err := in.Validate(id != ""); err != nil {
		return IngestionPipeline{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return IngestionPipeline{}, err
	}
	defer tx.Rollback(ctx)
	var workspace string
	if err = tx.QueryRow(ctx, `SELECT id FROM dataslate_workspaces WHERE id=$1`, ws).Scan(&workspace); err != nil {
		return IngestionPipeline{}, storeError(err)
	}
	p := IngestionPipeline{Resource: newResource("pipe-"), WorkspaceID: ws, Name: in.Name, IngestionSpec: in.IngestionSpec}
	if id != "" {
		var old IngestionPipeline
		if err = tx.QueryRow(ctx, `SELECT document FROM dataslate_ingestion_pipelines WHERE workspace_id=$1 AND id=$2 FOR UPDATE`, ws, id).Scan(&old); err != nil {
			return IngestionPipeline{}, storeError(err)
		}
		p, err = updateIngestion(old, in)
		if err != nil {
			return IngestionPipeline{}, err
		}
		_, err = tx.Exec(ctx, `UPDATE dataslate_ingestion_pipelines SET document=$2,revision=$3 WHERE id=$1`, id, p, p.Revision)
	} else {
		_, err = tx.Exec(ctx, `INSERT INTO dataslate_ingestion_pipelines(id,workspace_id,document,revision) VALUES($1,$2,$3,$4)`, p.ID, ws, p, p.Revision)
	}
	if err != nil {
		return IngestionPipeline{}, storeError(err)
	}
	return p, tx.Commit(ctx)
}
func (s *PostgresStore) DeleteIngestionPipeline(ctx context.Context, ws, id string, revision int) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var actual int
	if err = tx.QueryRow(ctx, `SELECT revision FROM dataslate_ingestion_pipelines WHERE workspace_id=$1 AND id=$2 FOR UPDATE`, ws, id).Scan(&actual); err != nil {
		return storeError(err)
	}
	if actual != revision {
		return ErrConflict
	}
	if _, err = tx.Exec(ctx, `DELETE FROM dataslate_ingestion_pipelines WHERE id=$1`, id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func snapshotIngestionTransaction(ctx context.Context, tx pgx.Tx, r Run) (Run, error) {
	if r.Ingestion == nil {
		return Run{}, ErrIngestionNotExecutable
	}
	var p IngestionPipeline
	if err := tx.QueryRow(ctx, `SELECT document FROM dataslate_ingestion_pipelines WHERE workspace_id=$1 AND id=$2 FOR SHARE`, r.WorkspaceID, r.Ingestion.ID).Scan(&p); err != nil {
		return Run{}, storeError(err)
	}
	r, err := snapshotIngestion(r, p)
	if err != nil {
		return Run{}, err
	}
	var busy bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM dataslate_runs WHERE document->'ingestion'->>'id'=$1 AND (state NOT IN ('succeeded','failed','cancelled') OR COALESCE((document->>'writerReleased')::boolean,false)=false OR document->'sourceArtifact'->>'state' IN ('pending','ready','cleanup_pending')))`, p.ID).Scan(&busy); err != nil {
		return Run{}, err
	}
	if busy {
		return Run{}, ErrIngestionWriterBusy
	}
	rows, err := tx.Query(ctx, `SELECT pipeline_id,workspace_id,destination_uri,checkpoint_uri FROM dataslate_ingestion_reservations WHERE pipeline_id<>$1`, p.ID)
	if err != nil {
		return Run{}, err
	}
	conflict := false
	for rows.Next() {
		var reservation ingestionReservation
		if err = rows.Scan(&reservation.PipelineID, &reservation.WorkspaceID, &reservation.DestinationURI, &reservation.CheckpointURI); err != nil {
			break
		}
		if reservation.conflicts(p.IngestionSpec) {
			conflict = true
			break
		}
	}
	rows.Close()
	if err != nil {
		return Run{}, err
	}
	if rows.Err() != nil {
		return Run{}, rows.Err()
	}
	if conflict {
		return Run{}, ErrIngestionLocationConflict
	}
	if _, err = tx.Exec(ctx, `INSERT INTO dataslate_ingestion_reservations(pipeline_id,workspace_id,destination_uri,checkpoint_uri) VALUES($1,$2,$3,$4) ON CONFLICT(pipeline_id) DO NOTHING`, p.ID, p.WorkspaceID, p.Destination.URI, p.CheckpointURI); err != nil {
		return Run{}, err
	}
	return r, nil
}
