package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func (s *PostgresStore) SearchChanges(ctx context.Context, b SearchBinding) ([]SearchNotebookSelection, error) {
	rows, err := s.pool.Query(ctx, `SELECT c.notebook_id,c.revision FROM dataslate_search_changes c WHERE c.workspace_id=$1 AND NOT EXISTS(SELECT 1 FROM dataslate_search_jobs j WHERE j.workspace_id=$1 AND j.binding_id=$2 AND j.policy_hash=$3 AND j.notebook_id=c.notebook_id AND (j.document#>>'{notebook,revision}')::int=c.revision)
 ORDER BY EXISTS(SELECT 1 FROM dataslate_search_jobs j WHERE j.workspace_id=$1 AND j.binding_id=$2 AND j.policy_hash=$3 AND j.notebook_id=c.notebook_id) DESC,c.notebook_id LIMIT 1000`, b.WorkspaceID, b.ID, b.Hash())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []SearchNotebookSelection{}
	for rows.Next() {
		var n SearchNotebookSelection
		if err = rows.Scan(&n.ID, &n.Revision); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

func (s *PostgresStore) CreateSearchJob(ctx context.Context, j SearchJob, hash, key, fingerprint string, budget int) (SearchJob, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return SearchJob{}, false, err
	}
	defer tx.Rollback(ctx)
	var ws string
	if err = tx.QueryRow(ctx, "SELECT id FROM dataslate_workspaces WHERE id=$1 FOR UPDATE", j.WorkspaceID).Scan(&ws); err != nil {
		return SearchJob{}, false, storeError(err)
	}
	var old []byte
	var previous string
	err = tx.QueryRow(ctx, "SELECT document,fingerprint FROM dataslate_search_jobs WHERE workspace_id=$1 AND issuer=$2 AND subject=$3 AND idempotency_key=$4", j.WorkspaceID, j.SubmittedBy.Issuer, j.SubmittedBy.Subject, key).Scan(&old, &previous)
	if err == nil {
		if previous != fingerprint {
			return SearchJob{}, false, ErrConflict
		}
		var out SearchJob
		if err = json.Unmarshal(old, &out); err != nil {
			return SearchJob{}, false, err
		}
		return out, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return SearchJob{}, false, err
	}
	var notebookID any
	if j.Notebook != nil {
		var rev int
		err = tx.QueryRow(ctx, "SELECT revision FROM dataslate_notebooks WHERE workspace_id=$1 AND id=$2 FOR SHARE", j.WorkspaceID, j.Notebook.ID).Scan(&rev)
		if err != nil {
			return SearchJob{}, false, storeError(err)
		}
		if rev != j.Notebook.Revision {
			return SearchJob{}, false, ErrSearchRevision
		}
		notebookID = j.Notebook.ID
	}
	var reserved int
	err = tx.QueryRow(ctx, `SELECT count(*) FROM(SELECT id FROM dataslate_search_documents WHERE workspace_id=$1 AND binding_id=$2 AND policy_hash=$3 AND (expires_at IS NULL OR expires_at>now()) UNION SELECT document_key FROM dataslate_search_jobs WHERE workspace_id=$1 AND binding_id=$2 AND policy_hash=$3 AND state IN ('queued','indexing') UNION SELECT $4::text) sources`, j.WorkspaceID, j.BindingID, hash, searchDocumentID(j)).Scan(&reserved)
	if err != nil {
		return SearchJob{}, false, err
	}
	if budget < 1 || budget > 1000 || reserved > budget {
		return SearchJob{}, false, ErrSearchCapacity
	}
	var active int
	if err = tx.QueryRow(ctx, "SELECT count(*) FROM dataslate_search_jobs WHERE workspace_id=$1 AND state IN ('queued','indexing')", j.WorkspaceID).Scan(&active); err != nil {
		return SearchJob{}, false, err
	}
	if active >= MaxSearchJobs {
		return SearchJob{}, false, ErrSearchCapacity
	}
	data, _ := json.Marshal(j)
	_, err = tx.Exec(ctx, `INSERT INTO dataslate_search_jobs(id,workspace_id,binding_id,issuer,subject,idempotency_key,fingerprint,policy_hash,document,state,notebook_id,created_at,document_key) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, j.ID, j.WorkspaceID, j.BindingID, j.SubmittedBy.Issuer, j.SubmittedBy.Subject, key, fingerprint, hash, data, j.State, notebookID, j.CreatedAt, searchDocumentID(j))
	if err != nil {
		return SearchJob{}, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return SearchJob{}, false, err
	}
	return j, false, nil
}
func (s *PostgresStore) SearchJob(ctx context.Context, ws, id string, owner ExecutionSubject) (SearchJob, error) {
	var data []byte
	err := s.pool.QueryRow(ctx, "SELECT document FROM dataslate_search_jobs WHERE workspace_id=$1 AND id=$2 AND issuer=$3 AND subject=$4", ws, id, owner.Issuer, owner.Subject).Scan(&data)
	if err != nil {
		return SearchJob{}, storeError(err)
	}
	var out SearchJob
	err = json.Unmarshal(data, &out)
	return out, err
}
func (s *PostgresStore) AcquireSearchJob(ctx context.Context, owner string, ttl time.Duration) (SearchWork, SearchLease, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return SearchWork{}, SearchLease{}, err
	}
	defer tx.Rollback(ctx)
	var data []byte
	var hash string
	var fence int64
	err = tx.QueryRow(ctx, `SELECT document,policy_hash,lease_fence FROM dataslate_search_jobs WHERE state IN ('queued','indexing') AND (lease_until IS NULL OR lease_until<clock_timestamp()) ORDER BY created_at,id FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(&data, &hash, &fence)
	if err != nil {
		return SearchWork{}, SearchLease{}, storeError(err)
	}
	var j SearchJob
	if err = json.Unmarshal(data, &j); err != nil {
		return SearchWork{}, SearchLease{}, err
	}
	j.State = "indexing"
	j.UpdatedAt = time.Now().UTC()
	j.Revision++
	data, _ = json.Marshal(j)
	fence++
	_, err = tx.Exec(ctx, "UPDATE dataslate_search_jobs SET document=$2,state='indexing',lease_owner=$3,lease_fence=$4,lease_until=clock_timestamp()+$5::interval WHERE id=$1", j.ID, data, owner, fence, fmt.Sprintf("%f seconds", ttl.Seconds()))
	if err != nil {
		return SearchWork{}, SearchLease{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return SearchWork{}, SearchLease{}, err
	}
	return SearchWork{j, hash}, SearchLease{j.ID, owner, fence}, nil
}
func searchLeaseRow(ctx context.Context, tx pgx.Tx, l SearchLease) (SearchWork, error) {
	var data []byte
	var hash string
	var id string
	if err := tx.QueryRow(ctx, "SELECT id FROM dataslate_search_jobs WHERE id=$1 FOR UPDATE", l.ID).Scan(&id); err != nil {
		return SearchWork{}, storeError(err)
	}
	err := tx.QueryRow(ctx, "SELECT document,policy_hash FROM dataslate_search_jobs WHERE id=$1 AND lease_owner=$2 AND lease_fence=$3 AND lease_until>clock_timestamp()", l.ID, l.Owner, l.Fence).Scan(&data, &hash)
	if errors.Is(err, pgx.ErrNoRows) {
		return SearchWork{}, ErrSearchLease
	}
	if err != nil {
		return SearchWork{}, err
	}
	var j SearchJob
	err = json.Unmarshal(data, &j)
	return SearchWork{j, hash}, err
}
func (s *PostgresStore) MarkSearchEmbedding(ctx context.Context, l SearchLease) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	work, err := searchLeaseRow(ctx, tx, l)
	if err != nil {
		return err
	}
	if work.Job.EmbeddingStarted {
		return ErrConflict
	}
	work.Job.EmbeddingStarted = true
	work.Job.Revision++
	work.Job.UpdatedAt = time.Now().UTC()
	data, _ := json.Marshal(work.Job)
	if _, err = tx.Exec(ctx, "UPDATE dataslate_search_jobs SET document=$2 WHERE id=$1", l.ID, data); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *PostgresStore) FinishSearchJob(ctx context.Context, l SearchLease, state, message string, doc *SearchDocument, maxDocuments int) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	// Workspace lock serializes document budget checks and publication, before job lock.
	var ws string
	if err = tx.QueryRow(ctx, "SELECT workspace_id FROM dataslate_search_jobs WHERE id=$1", l.ID).Scan(&ws); err != nil {
		return storeError(err)
	}
	if _, err = tx.Exec(ctx, "SELECT id FROM dataslate_workspaces WHERE id=$1 FOR UPDATE", ws); err != nil {
		return err
	}
	work, err := searchLeaseRow(ctx, tx, l)
	if err != nil {
		return err
	}
	if !(SearchJob{State: state}).Terminal() {
		return errors.New("invalid final index state")
	}
	if doc != nil {
		if state != "succeeded" || !validSearchDocument(*doc) || doc.WorkspaceID != work.Job.WorkspaceID || doc.BindingID != work.Job.BindingID || doc.PolicyHash != work.PolicyHash {
			return errors.New("invalid index publication")
		}
		if doc.Notebook != nil {
			var revision int
			err = tx.QueryRow(ctx, "SELECT revision FROM dataslate_notebooks WHERE workspace_id=$1 AND id=$2 FOR SHARE", ws, doc.Notebook.ID).Scan(&revision)
			if err != nil || revision != doc.Notebook.Revision {
				if err != nil && !errors.Is(err, pgx.ErrNoRows) {
					return err
				}
				state = "superseded"
				message = "Source changed before index publication."
				doc = nil
			}
		}
	}
	if doc != nil {
		if _, err = tx.Exec(ctx, "DELETE FROM dataslate_search_documents WHERE workspace_id=$1 AND binding_id=$2 AND (policy_hash<>$3 OR expires_at<=now())", ws, doc.BindingID, doc.PolicyHash); err != nil {
			return err
		}
		var count int
		if err = tx.QueryRow(ctx, "SELECT count(*) FROM dataslate_search_documents WHERE workspace_id=$1 AND binding_id=$2 AND id<>$3", ws, doc.BindingID, doc.ID).Scan(&count); err != nil {
			return err
		}
		var oldTime time.Time
		oldErr := tx.QueryRow(ctx, "SELECT indexed_at FROM dataslate_search_documents WHERE workspace_id=$1 AND binding_id=$2 AND id=$3", ws, doc.BindingID, doc.ID).Scan(&oldTime)
		if oldErr != nil && !errors.Is(oldErr, pgx.ErrNoRows) {
			return oldErr
		}
		if count >= maxDocuments {
			state = "failed"
			message = "Index document budget reached; no document was published."
			doc = nil
		} else if oldErr == nil && oldTime.After(doc.IndexedAt) {
			state = "superseded"
			message = "A newer index snapshot already exists."
			doc = nil
		}
	}
	if doc != nil {
		var notebookID, notebookRevision any
		catalogKey := ""
		if doc.Notebook != nil {
			notebookID = doc.Notebook.ID
			notebookRevision = doc.Notebook.Revision
		} else {
			catalogKey = searchCatalogKey(doc.Catalog.SearchCatalogSelection)
		}
		metadata := *doc
		metadata.Chunks = nil
		data, _ := json.Marshal(metadata)
		if _, err = tx.Exec(ctx, "DELETE FROM dataslate_search_documents WHERE workspace_id=$1 AND binding_id=$2 AND id=$3", ws, doc.BindingID, doc.ID); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO dataslate_search_documents(workspace_id,binding_id,id,policy_hash,authority_hash,catalog_key,notebook_id,notebook_revision,indexed_at,expires_at,document) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, ws, doc.BindingID, doc.ID, doc.PolicyHash, doc.AuthorityHash, catalogKey, notebookID, notebookRevision, doc.IndexedAt, doc.ExpiresAt, data)
		if err != nil {
			return err
		}
		for i, c := range doc.Chunks {
			var vector any
			if len(c.Vector) > 0 {
				vector = c.Vector
			}
			if _, err = tx.Exec(ctx, "INSERT INTO dataslate_search_chunks(workspace_id,binding_id,document_id,ordinal,content,embedding) VALUES($1,$2,$3,$4,$5,$6)", ws, doc.BindingID, doc.ID, i, c.Text, vector); err != nil {
				return err
			}
		}
		work.Job.DocumentID = doc.ID
	}
	work.Job.State = state
	work.Job.Message = message
	work.Job.UpdatedAt = time.Now().UTC()
	work.Job.Revision++
	data, _ := json.Marshal(work.Job)
	if _, err = tx.Exec(ctx, "UPDATE dataslate_search_jobs SET document=$2,state=$3,lease_until=NULL WHERE id=$1", l.ID, data, state); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *PostgresStore) SearchVectorAvailable(ctx context.Context) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_extension e JOIN pg_namespace n ON n.oid=e.extnamespace WHERE e.extname='vector' AND n.nspname='public')").Scan(&exists)
	return exists, err
}
func (s *PostgresStore) Search(ctx context.Context, scope SearchScope, in SearchRequest, vector []float32) ([]SearchHit, error) {
	if in.Mode == "hybrid" && !searchVectorValid(vector, len(vector)) {
		return nil, errors.New("invalid query vector")
	}
	grants, _ := json.Marshal(scope.Catalogs)
	// Authorization is applied inside the materialized CTE before ranking and before any content leaves PostgreSQL.
	query := `WITH authorized AS MATERIALIZED (
 SELECT d.document,c.content,c.document_id,c.ordinal,c.lexical,c.embedding
 FROM dataslate_search_documents d JOIN dataslate_search_chunks c ON(c.workspace_id=d.workspace_id AND c.binding_id=d.binding_id AND c.document_id=d.id)
 LEFT JOIN dataslate_notebooks n ON(n.id=d.notebook_id AND n.workspace_id=d.workspace_id)
 WHERE d.workspace_id=$1 AND d.binding_id=$2 AND d.policy_hash=$3 AND
 (($4 AND d.notebook_id IS NOT NULL AND n.revision=d.notebook_revision) OR
 (d.notebook_id IS NULL AND d.expires_at>now() AND d.authority_hash<>'' AND ($5::jsonb->>d.catalog_key)=d.authority_hash))
 ), lexical AS (SELECT *,row_number() OVER(ORDER BY ts_rank_cd(lexical,websearch_to_tsquery('simple',$6)) DESC,document_id,ordinal) AS rank
 FROM authorized WHERE lexical@@websearch_to_tsquery('simple',$6) ORDER BY ts_rank_cd(lexical,websearch_to_tsquery('simple',$6)) DESC,document_id,ordinal LIMIT 100)`
	args := []any{scope.WorkspaceID, scope.BindingID, scope.PolicyHash, scope.Notebooks, grants, in.Query, in.Limit}
	if in.Mode == "hybrid" {
		values := make([]string, len(vector))
		for i, v := range vector {
			values[i] = strconv.FormatFloat(float64(v), 'g', -1, 32)
		}
		literal := "[" + strings.Join(values, ",") + "]"
		args = append(args, literal, len(vector))
		query += `, semantic AS (SELECT *,row_number() OVER(ORDER BY embedding::public.vector OPERATOR(public.<=>) $8::public.vector,document_id,ordinal) AS rank
 FROM authorized WHERE cardinality(embedding)=$9 ORDER BY embedding::public.vector OPERATOR(public.<=>) $8::public.vector,document_id,ordinal LIMIT 100), scores AS (
 SELECT document_id,ordinal,sum(score) AS score FROM(SELECT document_id,ordinal,1.0/(60+rank) AS score FROM lexical UNION ALL SELECT document_id,ordinal,1.0/(60+rank) AS score FROM semantic) ranked GROUP BY document_id,ordinal), picked AS (
 SELECT DISTINCT ON(a.document_id) a.document,a.content,s.score,a.document_id FROM authorized a JOIN scores s USING(document_id,ordinal) ORDER BY a.document_id,s.score DESC,a.ordinal)
 SELECT document,content,score::float8 FROM picked ORDER BY score DESC,document_id LIMIT $7`
	} else {
		query += `, picked AS (SELECT DISTINCT ON(document_id) document,content,document_id,1.0/(60+rank) AS score FROM lexical ORDER BY document_id,rank)
 SELECT document,content,score::float8 FROM picked ORDER BY score DESC,document_id LIMIT $7`
	}
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []SearchHit{}
	for rows.Next() {
		var data []byte
		var text string
		var score float64
		if err = rows.Scan(&data, &text, &score); err != nil {
			return nil, err
		}
		var doc SearchDocument
		if json.Unmarshal(data, &doc) != nil || !scope.Allows(doc) {
			return nil, errors.New("search document authority mismatch")
		}
		out = append(out, searchHit(doc, text, score))
	}
	return out, rows.Err()
}

func (s *PostgresStore) FilterCurrentSearchHits(ctx context.Context, ws string, hits []SearchHit) ([]SearchHit, error) {
	ids := []string{}
	for _, hit := range hits {
		if hit.Notebook != nil {
			ids = append(ids, hit.Notebook.ID)
		}
	}
	current := map[string]int{}
	if len(ids) > 0 {
		rows, err := s.pool.Query(ctx, "SELECT id,revision FROM dataslate_notebooks WHERE workspace_id=$1 AND id=ANY($2::text[])", ws, ids)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			var rev int
			if err = rows.Scan(&id, &rev); err != nil {
				return nil, err
			}
			current[id] = rev
		}
		if err = rows.Err(); err != nil {
			return nil, err
		}
	}
	out := []SearchHit{}
	for _, hit := range hits {
		if hit.Notebook != nil {
			if current[hit.Notebook.ID] != hit.Notebook.Revision {
				continue
			}
		} else if hit.ExpiresAt == nil || !hit.ExpiresAt.After(time.Now()) {
			continue
		}
		out = append(out, hit)
	}
	return out, nil
}
