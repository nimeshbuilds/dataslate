package platform

import (
	"errors"
	"net/http"
	"reflect"
)

func (s *Server) currentSearchBindings() ([]SearchBinding, error) {
	bindings := s.Config.SearchBindings
	var err error
	if s.Config.SearchBindingsFile != "" {
		bindings, err = LoadSearchBindings(s.Config.SearchBindingsFile)
		if err != nil {
			return nil, err
		}
	}
	for _, b := range bindings {
		if err = b.Validate(); err != nil {
			return nil, err
		}
	}
	return bindings, nil
}
func (s *Server) searchBinding(w http.ResponseWriter, r *http.Request, id string) (SearchBinding, bool) {
	bindings, err := s.currentSearchBindings()
	if err != nil {
		fail(w, 503, "search_policy_unavailable", "Search policy is unavailable.")
		return SearchBinding{}, false
	}
	for _, b := range bindings {
		if b.WorkspaceID == r.PathValue("workspaceId") && b.ID == id {
			return b, true
		}
	}
	fail(w, 403, "search_access_denied", "This workspace search profile is not enabled.")
	return SearchBinding{}, false
}
func (s *Server) searchScope(b SearchBinding, owner ExecutionSubject) (SearchScope, error) {
	scope := SearchScope{WorkspaceID: b.WorkspaceID, BindingID: b.ID, PolicyHash: b.Hash(), Notebooks: b.Sources.Notebooks, Catalogs: map[string]string{}}
	if len(b.Sources.Catalogs) == 0 {
		return scope, nil
	}
	catalogs, err := s.currentCatalogBindings()
	if err != nil {
		return SearchScope{}, err
	}
	for _, c := range catalogs {
		if c.WorkspaceID != b.WorkspaceID {
			continue
		}
		tables, _ := c.Tables(owner)
		for _, table := range tables {
			id := SearchCatalogSelection{BindingID: c.ID, Identifier: table}
			if b.AllowsCatalog(id) {
				scope.Catalogs[searchCatalogKey(id)] = c.Hash()
			}
		}
	}
	return scope, nil
}
func (s *Server) searchScopeCurrent(w http.ResponseWriter, r *http.Request, b SearchBinding, old SearchScope) bool {
	current, ok := s.searchBinding(w, r, b.ID)
	if !ok {
		return false
	}
	scope, err := s.searchScope(current, subjectFor(principal(r)))
	if err != nil {
		fail(w, 503, "search_policy_unavailable", "Current catalog grants are unavailable.")
		return false
	}
	if current.Hash() != b.Hash() || !reflect.DeepEqual(scope, old) {
		fail(w, 403, "search_access_denied", "Search authority changed during the request. Reload before retrying.")
		return false
	}
	return true
}
func (s *Server) searchStatus(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	bindings, err := s.currentSearchBindings()
	if err != nil {
		fail(w, 503, "search_policy_unavailable", "Search policy is unavailable.")
		return
	}
	available, err := s.Store.SearchVectorAvailable(r.Context())
	if err != nil {
		storeFailure(w, err)
		return
	}
	items := []map[string]any{}
	for _, b := range bindings {
		if b.WorkspaceID != r.PathValue("workspaceId") {
			continue
		}
		item := map[string]any{"id": b.ID, "workspaceId": b.WorkspaceID, "name": b.Name, "mode": b.Mode, "policyVersion": b.PolicyVersion, "includeCellSource": b.Sources.IncludeCellSource, "dataTransferAccepted": b.DataTransferAccepted, "vectorAvailable": available}
		if b.Embedding != nil {
			item["embeddingModel"] = b.Embedding.Model
			item["dimensions"] = b.Embedding.Dimensions
		}
		items = append(items, item)
	}
	writeJSON(w, 200, map[string]any{"items": items})
}
func (s *Server) search(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	var in SearchRequest
	if !decode(w, r, &in) {
		return
	}
	if err := in.Validate(); err != nil {
		fail(w, 422, "invalid_search", err.Error())
		return
	}
	b, ok := s.searchBinding(w, r, in.BindingID)
	if !ok {
		return
	}
	scope, err := s.searchScope(b, subjectFor(principal(r)))
	if err != nil {
		fail(w, 503, "search_policy_unavailable", "Current catalog grants are unavailable.")
		return
	}
	var vector []float32
	if in.Mode == "hybrid" {
		if b.Mode != "hybrid" || b.Embedding == nil || !b.DataTransferAccepted {
			fail(w, 501, "search_capability_unavailable", "Hybrid search requires an approved embedding transfer profile.")
			return
		}
		available, err := s.Store.SearchVectorAvailable(r.Context())
		if err != nil {
			storeFailure(w, err)
			return
		}
		if !available {
			fail(w, 501, "search_capability_unavailable", "Hybrid search requires operator-installed pgvector in PostgreSQL.")
			return
		}
		if !s.searchScopeCurrent(w, r, b, scope) {
			return
		}
		vectors, err := s.SearchEmbedder.Embed(r.Context(), *b.Embedding, []string{in.Query})
		if err != nil || len(vectors) != 1 || !searchVectorValid(vectors[0], b.Embedding.Dimensions) {
			fail(w, 503, "search_backend_unavailable", "Embedding request failed or its outcome is uncertain. No automatic retry was made.")
			return
		}
		vector = vectors[0]
		if !s.searchScopeCurrent(w, r, b, scope) {
			return
		}
	}
	items, err := s.Store.Search(r.Context(), scope, in, vector)
	if err != nil {
		fail(w, 503, "search_backend_unavailable", "Authorized search results are unavailable.")
		return
	}
	items, err = s.Store.FilterCurrentSearchHits(r.Context(), b.WorkspaceID, items)
	if err != nil {
		fail(w, 503, "search_backend_unavailable", "Current source revisions could not be verified.")
		return
	}
	if !s.searchScopeCurrent(w, r, b, scope) {
		return
	}
	writeJSON(w, 200, map[string]any{"bindingId": b.ID, "workspaceId": b.WorkspaceID, "mode": in.Mode, "query": in.Query, "items": items, "complete": true})
}
func (s *Server) createSearchIndex(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	var in SearchIndexInput
	if !decode(w, r, &in) {
		return
	}
	if err := in.Validate(); err != nil {
		fail(w, 422, "invalid_search_index", err.Error())
		return
	}
	b, ok := s.searchBinding(w, r, in.BindingID)
	if !ok {
		return
	}
	if in.Notebook != nil && (!b.Sources.Notebooks || !principal(r).Can(b.WorkspaceID, true)) {
		fail(w, 403, "search_access_denied", "Notebook indexing requires an enabled source and workspace editor role.")
		return
	}
	scope, err := s.searchScope(b, subjectFor(principal(r)))
	if err != nil {
		fail(w, 503, "search_policy_unavailable", "Current catalog grants are unavailable.")
		return
	}
	if in.Catalog != nil && scope.Catalogs[searchCatalogKey(*in.Catalog)] == "" {
		fail(w, 403, "search_access_denied", "Catalog indexing requires exact search and metadata table grants.")
		return
	}
	if !s.searchScopeCurrent(w, r, b, scope) {
		return
	}
	job, replay, err := s.Store.CreateSearchJob(r.Context(), newSearchJob(b, subjectFor(principal(r)), in), b.Hash(), in.IdempotencyKey, searchFingerprint(in), b.MaxDocuments)
	if errors.Is(err, ErrSearchCapacity) {
		fail(w, 429, "search_capacity", "Workspace indexing capacity is exhausted.")
		return
	}
	if errors.Is(err, ErrSearchRevision) {
		fail(w, 409, "search_revision_conflict", "Reload the current saved notebook revision before indexing.")
		return
	}
	if err != nil {
		storeFailure(w, err)
		return
	}
	code := 202
	if replay {
		code = 200
	}
	writeJSON(w, code, job)
}
func (s *Server) searchIndexJob(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	job, err := s.Store.SearchJob(r.Context(), r.PathValue("workspaceId"), r.PathValue("id"), subjectFor(principal(r)))
	if err != nil {
		storeFailure(w, err)
		return
	}
	b, ok := s.searchBinding(w, r, job.BindingID)
	if !ok {
		return
	}
	scope, err := s.searchScope(b, subjectFor(principal(r)))
	if err != nil {
		fail(w, 503, "search_policy_unavailable", "Current source grants are unavailable.")
		return
	}
	if job.Notebook != nil && !scope.Notebooks || job.Catalog != nil && scope.Catalogs[searchCatalogKey(*job.Catalog)] == "" {
		fail(w, 403, "search_access_denied", "Current source grants are required to read this index receipt.")
		return
	}
	if !s.searchScopeCurrent(w, r, b, scope) {
		return
	}
	writeJSON(w, 200, job)
}
