package platform

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
)

type CatalogStore interface {
	CatalogObservation(context.Context, string, string) (CatalogObservation, error)
	SaveCatalogObservation(context.Context, CatalogObservation) error
}

func (s *MemoryStore) CatalogObservation(_ context.Context, ws, id string) (CatalogObservation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	o, ok := s.catalogObservations[ws+"/"+id]
	if !ok {
		return CatalogObservation{}, ErrNotFound
	}
	o.Capabilities = append([]string{}, o.Capabilities...)
	return o, nil
}
func (s *MemoryStore) SaveCatalogObservation(_ context.Context, o CatalogObservation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.workspaces[o.WorkspaceID]; !ok {
		return ErrNotFound
	}
	key := o.WorkspaceID + "/" + o.BindingID
	if old, ok := s.catalogObservations[key]; ok && !old.CheckedAt.Before(o.CheckedAt) {
		return nil
	}
	o.Capabilities = append([]string{}, o.Capabilities...)
	s.catalogObservations[key] = o
	return nil
}
func (s *PostgresStore) CatalogObservation(ctx context.Context, ws, id string) (CatalogObservation, error) {
	var data []byte
	err := s.pool.QueryRow(ctx, "SELECT document FROM dataslate_catalog_observations WHERE workspace_id=$1 AND binding_id=$2", ws, id).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return CatalogObservation{}, ErrNotFound
	}
	if err != nil {
		return CatalogObservation{}, err
	}
	var out CatalogObservation
	err = json.Unmarshal(data, &out)
	return out, err
}
func (s *PostgresStore) SaveCatalogObservation(ctx context.Context, o CatalogObservation) error {
	data, err := json.Marshal(o)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `INSERT INTO dataslate_catalog_observations(workspace_id,binding_id,checked_at,document) VALUES($1,$2,$3,$4) ON CONFLICT(workspace_id,binding_id) DO UPDATE SET checked_at=EXCLUDED.checked_at,document=EXCLUDED.document WHERE dataslate_catalog_observations.checked_at<EXCLUDED.checked_at`, o.WorkspaceID, o.BindingID, o.CheckedAt, data)
	return err
}
