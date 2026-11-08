package platform

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"
)

type SearchController struct {
	Config         Config
	Store          Store
	Embedder       SearchEmbedder
	CatalogBackend CatalogBackend
}

func (c *SearchController) server() *Server {
	return &Server{Config: c.Config, Store: c.Store, CatalogBackend: c.CatalogBackend, SearchEmbedder: c.Embedder}
}
func (c *SearchController) binding(work SearchWork) (SearchBinding, bool, error) {
	bindings, err := c.server().currentSearchBindings()
	if err != nil {
		return SearchBinding{}, false, err
	}
	for _, b := range bindings {
		if b.ID == work.Job.BindingID && b.WorkspaceID == work.Job.WorkspaceID {
			return b, b.Hash() == work.PolicyHash, nil
		}
	}
	return SearchBinding{}, false, nil
}
func (c *SearchController) Sync(ctx context.Context) error {
	bindings, err := c.server().currentSearchBindings()
	if err != nil {
		return err
	}
	for _, b := range bindings {
		if !b.Sources.Notebooks {
			continue
		}
		changes, err := c.Store.SearchChanges(ctx, b)
		if err != nil {
			return err
		}
		for _, n := range changes {
			in := SearchIndexInput{BindingID: b.ID, Notebook: &SearchNotebookSelection{n.ID, n.Revision}}
			sum := sha256.Sum256([]byte(b.Hash() + searchFingerprint(in)))
			in.IdempotencyKey = "auto-" + hex.EncodeToString(sum[:])
			_, _, err = c.Store.CreateSearchJob(ctx, newSearchJob(b, ExecutionSubject{Issuer: "system", Subject: "notebook-indexer"}, in), b.Hash(), in.IdempotencyKey, searchFingerprint(in), b.MaxDocuments)
			if errors.Is(err, ErrSearchCapacity) {
				continue
			}
			if err != nil && !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrSearchRevision) {
				return err
			}
		}
	}
	return nil
}
func (c *SearchController) catalogAuthority(work SearchWork, b SearchBinding) (CatalogBinding, bool, error) {
	if work.Job.Catalog == nil || !b.AllowsCatalog(*work.Job.Catalog) {
		return CatalogBinding{}, false, nil
	}
	bindings, err := c.server().currentCatalogBindings()
	if err != nil {
		return CatalogBinding{}, false, err
	}
	for _, binding := range bindings {
		if binding.ID == work.Job.Catalog.BindingID && binding.WorkspaceID == work.Job.WorkspaceID {
			return binding, binding.Allows(work.Job.SubmittedBy, work.Job.Catalog.Identifier), nil
		}
	}
	return CatalogBinding{}, false, nil
}
func (c *SearchController) ReconcileOnce(ctx context.Context, owner string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 35*time.Second)
	defer cancel()
	work, lease, err := c.Store.AcquireSearchJob(ctx, owner, 45*time.Second)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	finish := func(state, message string, doc *SearchDocument, budget int) (bool, error) {
		finishCtx, done := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer done()
		return true, c.Store.FinishSearchJob(finishCtx, lease, state, message, doc, budget)
	}
	if work.Job.EmbeddingStarted {
		return finish("unknown", "A prior embedding attempt has an uncertain outcome. Explicitly create a new indexing intent to retry.", nil, 0)
	}
	b, allowed, err := c.binding(work)
	if err != nil {
		return finish("failed", "Current search policy is unavailable; no embedding request was made.", nil, 0)
	}
	if !allowed {
		return finish("superseded", "Search policy changed before indexing.", nil, 0)
	}
	job := work.Job
	doc := SearchDocument{ID: searchDocumentID(job), WorkspaceID: job.WorkspaceID, BindingID: job.BindingID, PolicyHash: b.Hash(), IndexedAt: time.Now().UTC()}
	var text string
	var catalog CatalogBinding
	if job.Notebook != nil {
		if !b.Sources.Notebooks {
			return finish("superseded", "Notebook indexing is disabled.", nil, 0)
		}
		notebook, err := c.Store.Notebook(ctx, job.WorkspaceID, job.Notebook.ID)
		if errors.Is(err, ErrNotFound) || err == nil && notebook.Revision != job.Notebook.Revision {
			return finish("superseded", "Notebook was changed or deleted before indexing.", nil, 0)
		}
		if err != nil {
			return finish("failed", "Source could not be read; no embedding request was made.", nil, 0)
		}
		doc.Kind = "notebook"
		doc.Title = notebook.Name
		doc.Notebook = job.Notebook
		text = searchNotebookText(notebook, b.Sources.IncludeCellSource)
	} else {
		catalog, allowed, err = c.catalogAuthority(work, b)
		if err != nil || !allowed {
			return finish("superseded", "Current exact catalog grants are unavailable or revoked.", nil, 0)
		}
		caps, err := c.CatalogBackend.Discover(ctx, catalog)
		if err != nil || caps.Catalog != catalog.Connection.Warehouse || !slices.Contains(caps.Endpoints, catalogLoadEndpoint) {
			return finish("failed", "Polaris metadata discovery could not be verified.", nil, 0)
		}
		table, err := c.CatalogBackend.LoadTable(ctx, catalog, job.Catalog.Identifier)
		if err != nil {
			return finish("failed", "Authorized Polaris table metadata could not be read.", nil, 0)
		}
		if identifierKey(table.Identifier) != identifierKey(job.Catalog.Identifier) {
			return finish("failed", "Polaris returned inconsistent table identity.", nil, 0)
		}
		safe, _ := json.Marshal(struct {
			Identifier any `json:"identifier"`
			Schema     any `json:"schema"`
		}{table.Identifier, table.CurrentSchema})
		text = string(safe)
		doc.Kind = "catalog"
		doc.Title = strings.Join(table.Identifier.Namespace, ".") + "." + table.Identifier.Name
		doc.Catalog = &SearchCatalogProvenance{SearchCatalogSelection: *job.Catalog, PolicyVersion: catalog.PolicyVersion}
		doc.AuthorityHash = catalog.Hash()
		expiry := doc.IndexedAt.Add(SearchCatalogTTL)
		doc.ExpiresAt = &expiry
	}
	chunks, err := chunkSearchText(text)
	if err != nil {
		return finish("failed", err.Error(), nil, 0)
	}
	doc.Chunks = chunks
	current := func() bool {
		next, ok, err := c.binding(work)
		if err != nil || !ok || next.Hash() != b.Hash() {
			return false
		}
		if job.Notebook != nil {
			n, err := c.Store.Notebook(ctx, job.WorkspaceID, job.Notebook.ID)
			return err == nil && n.Revision == job.Notebook.Revision
		}
		authority, ok, err := c.catalogAuthority(work, b)
		return err == nil && ok && authority.Hash() == catalog.Hash()
	}
	if !current() {
		return finish("superseded", "Source or permissions changed before indexing.", nil, 0)
	}
	if b.Mode == "hybrid" {
		available, err := c.Store.SearchVectorAvailable(ctx)
		if err != nil || !available {
			return finish("failed", "Hybrid search requires operator-installed pgvector; no embedding request was made.", nil, 0)
		}
		if err = c.Store.MarkSearchEmbedding(ctx, lease); err != nil {
			return true, err
		}
		if !current() {
			return finish("superseded", "Source or permissions changed before the embedding request; no index was published.", nil, 0)
		}
		input := make([]string, len(doc.Chunks))
		for i, chunk := range doc.Chunks {
			input[i] = chunk.Text
		}
		vectors, err := c.Embedder.Embed(ctx, *b.Embedding, input)
		if err != nil {
			return finish("unknown", "Embedding request failed or its outcome is uncertain. No automatic retry was made; create a new intent to retry.", nil, 0)
		}
		if len(vectors) != len(doc.Chunks) {
			return finish("failed", "Embedding response did not match the source chunks.", nil, 0)
		}
		for i, v := range vectors {
			if !searchVectorValid(v, b.Embedding.Dimensions) {
				return finish("failed", "Embedding response contained an invalid vector.", nil, 0)
			}
			doc.Chunks[i].Vector = v
		}
	}
	if !current() {
		return finish("superseded", "Source or permissions changed during indexing; no index was published.", nil, 0)
	}
	return finish("succeeded", "The complete bounded source was indexed under the current policy.", &doc, b.MaxDocuments)
}
