package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"dataslate.local/api/migrations"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresStore struct{ pool *pgxpool.Pool }

// requireTLS is true in every production code path. False is only for isolated integration tests.
func OpenPostgres(ctx context.Context, dsn string, requireTLS bool) (*PostgresStore, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("invalid database configuration")
	}
	if requireTLS {
		if cfg.ConnConfig.TLSConfig == nil || cfg.ConnConfig.TLSConfig.InsecureSkipVerify {
			return nil, errors.New("production PostgreSQL requires TLS sslmode=verify-full")
		}
		for _, f := range cfg.ConnConfig.Fallbacks {
			if f.TLSConfig == nil || f.TLSConfig.InsecureSkipVerify {
				return nil, errors.New("production PostgreSQL cannot use insecure TLS fallback")
			}
		}
	}
	cfg.MaxConns = 10
	cfg.MinConns = 1
	cfg.ConnConfig.ConnectTimeout = 10 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &PostgresStore{pool: pool}, nil
}
func (s *PostgresStore) Close()     { s.pool.Close() }
func (*PostgresStore) Mode() string { return "postgres" }
func (s *PostgresStore) Ready(ctx context.Context) error {
	var count, latest int
	if err := s.pool.QueryRow(ctx, "SELECT count(*),COALESCE(max(version),0) FROM dataslate_schema_migrations").Scan(&count, &latest); err != nil {
		return err
	}
	entries, err := migrations.Files.ReadDir(".")
	if err != nil {
		return err
	}
	expected := 0
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".sql") {
			expected++
		}
	}
	if count != expected || latest != expected {
		return errors.New("database schema version does not match this binary")
	}
	return nil
}
func (s *PostgresStore) Migrate(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(72610524001)"); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "CREATE TABLE IF NOT EXISTS dataslate_schema_migrations(version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"); err != nil {
		return err
	}
	entries, err := migrations.Files.ReadDir(".")
	if err != nil {
		return err
	}
	expected := 0
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		expected++
		version, e := strconv.Atoi(strings.SplitN(entry.Name(), "_", 2)[0])
		if e != nil || version != expected {
			return errors.New("migration filenames must be contiguous and ordered")
		}
		var exists bool
		if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM dataslate_schema_migrations WHERE version=$1)", version).Scan(&exists); err != nil {
			return err
		}
		if exists {
			continue
		}
		sql, e := migrations.Files.ReadFile(entry.Name())
		if e != nil {
			return e
		}
		if _, err = tx.Exec(ctx, string(sql)); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, "INSERT INTO dataslate_schema_migrations(version) VALUES($1)", version); err != nil {
			return err
		}
	}
	var latest int
	if err = tx.QueryRow(ctx, "SELECT COALESCE(max(version),0) FROM dataslate_schema_migrations").Scan(&latest); err != nil {
		return err
	}
	if latest > expected {
		return errors.New("database schema is newer than this binary")
	}
	return tx.Commit(ctx)
}
func storeError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	var e *pgconn.PgError
	if errors.As(err, &e) && e.Code == "23505" {
		return ErrConflict
	}
	return err
}
func scanWorkspace(row pgx.Row) (Workspace, error) {
	var w Workspace
	err := row.Scan(&w.ID, &w.Name, &w.Slug, &w.CreatedAt, &w.UpdatedAt, &w.Revision)
	return w, storeError(err)
}
func (s *PostgresStore) Workspace(ctx context.Context, id string) (Workspace, error) {
	return scanWorkspace(s.pool.QueryRow(ctx, "SELECT id,name,slug,created_at,updated_at,revision FROM dataslate_workspaces WHERE id=$1", id))
}
func (s *PostgresStore) Workspaces(ctx context.Context) ([]Workspace, error) {
	rows, err := s.pool.Query(ctx, "SELECT id,name,slug,created_at,updated_at,revision FROM dataslate_workspaces ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Workspace{}
	for rows.Next() {
		w, err := scanWorkspace(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}
func (s *PostgresStore) CreateWorkspace(ctx context.Context, name, slug string) (Workspace, error) {
	w := Workspace{Resource: newResource("ws-"), Name: name, Slug: slug}
	_, err := s.pool.Exec(ctx, "INSERT INTO dataslate_workspaces(id,name,slug,created_at,updated_at,revision) VALUES($1,$2,$3,$4,$5,$6)", w.ID, w.Name, w.Slug, w.CreatedAt, w.UpdatedAt, w.Revision)
	return w, storeError(err)
}

const notebookColumns = "id,workspace_id,name,kind,cells,created_at,updated_at,revision"

func scanNotebook(row pgx.Row) (Notebook, error) {
	var n Notebook
	err := row.Scan(&n.ID, &n.WorkspaceID, &n.Name, &n.Kind, &n.Cells, &n.CreatedAt, &n.UpdatedAt, &n.Revision)
	return n, storeError(err)
}
func (s *PostgresStore) Notebook(ctx context.Context, ws, id string) (Notebook, error) {
	return scanNotebook(s.pool.QueryRow(ctx, "SELECT "+notebookColumns+" FROM dataslate_notebooks WHERE workspace_id=$1 AND id=$2", ws, id))
}
func (s *PostgresStore) Notebooks(ctx context.Context, ws string) ([]Notebook, error) {
	rows, err := s.pool.Query(ctx, "SELECT "+notebookColumns+" FROM dataslate_notebooks WHERE workspace_id=$1 ORDER BY id", ws)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Notebook{}
	for rows.Next() {
		n, err := scanNotebook(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}
func (s *PostgresStore) SaveNotebook(ctx context.Context, ws, id string, in NotebookInput) (Notebook, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Notebook{}, err
	}
	defer tx.Rollback(ctx)
	n := Notebook{Resource: newResource("nb-"), WorkspaceID: ws, Name: in.Name, Kind: in.Kind, Cells: in.Cells}
	if id == "" {
		var exists bool
		if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM dataslate_workspaces WHERE id=$1)", ws).Scan(&exists); err != nil {
			return Notebook{}, err
		}
		if !exists {
			return Notebook{}, ErrNotFound
		}
		_, err = tx.Exec(ctx, "INSERT INTO dataslate_notebooks("+notebookColumns+") VALUES($1,$2,$3,$4,$5,$6,$7,$8)", n.ID, ws, n.Name, n.Kind, n.Cells, n.CreatedAt, n.UpdatedAt, n.Revision)
	} else {
		n, err = scanNotebook(tx.QueryRow(ctx, "UPDATE dataslate_notebooks SET name=$3,kind=$4,cells=$5,revision=revision+1,updated_at=$6 WHERE workspace_id=$1 AND id=$2 AND revision=$7 RETURNING "+notebookColumns, ws, id, in.Name, in.Kind, in.Cells, time.Now().UTC(), in.Revision))
		if errors.Is(err, ErrNotFound) {
			var exists bool
			if e := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM dataslate_notebooks WHERE workspace_id=$1 AND id=$2)", ws, id).Scan(&exists); e != nil {
				return Notebook{}, e
			}
			if exists {
				return Notebook{}, ErrConflict
			}
		}
	}
	if err != nil {
		return Notebook{}, storeError(err)
	}
	if _, err = tx.Exec(ctx, "DELETE FROM dataslate_search_documents WHERE workspace_id=$1 AND notebook_id=$2", ws, n.ID); err != nil {
		return Notebook{}, err
	}
	if _, err = tx.Exec(ctx, "INSERT INTO dataslate_search_changes(notebook_id,workspace_id,revision) VALUES($1,$2,$3) ON CONFLICT(notebook_id) DO UPDATE SET revision=EXCLUDED.revision", n.ID, ws, n.Revision); err != nil {
		return Notebook{}, err
	}
	doc, err := json.Marshal(n)
	if err != nil {
		return Notebook{}, err
	}
	if _, err = tx.Exec(ctx, "INSERT INTO dataslate_notebook_revisions(workspace_id,notebook_id,revision,document) VALUES($1,$2,$3,$4)", ws, n.ID, n.Revision, doc); err != nil {
		return Notebook{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Notebook{}, err
	}
	return n, nil
}
func (s *PostgresStore) DeleteNotebook(ctx context.Context, ws, id string, revision int) error {
	tag, err := s.pool.Exec(ctx, "DELETE FROM dataslate_notebooks WHERE workspace_id=$1 AND id=$2 AND revision=$3", ws, id, revision)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	_, err = s.Notebook(ctx, ws, id)
	if err != nil {
		return err
	}
	return ErrConflict
}
func (s *PostgresStore) Revisions(ctx context.Context, ws, id string) ([]Notebook, error) {
	if _, err := s.Notebook(ctx, ws, id); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, "SELECT document FROM dataslate_notebook_revisions WHERE workspace_id=$1 AND notebook_id=$2 ORDER BY revision", ws, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Notebook{}
	for rows.Next() {
		var n Notebook
		if err = rows.Scan(&n); err != nil {
			return nil, fmt.Errorf("decode revision: %w", err)
		}
		out = append(out, n)
	}
	return out, rows.Err()
}
