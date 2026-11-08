package platform

import (
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"time"

	"dataslate.local/api/internal/catalogs/polaris"
)

func (s *Server) currentCatalogBindings() ([]CatalogBinding, error) {
	bindings := s.Config.CatalogBindings
	var err error
	if s.Config.CatalogBindingsFile != "" {
		bindings, err = LoadCatalogBindings(s.Config.CatalogBindingsFile)
		if err != nil {
			return nil, err
		}
	}
	issuer := s.Config.Issuer
	if s.Config.AuthMode == "development" {
		issuer = "development"
	}
	for _, b := range bindings {
		if err = b.Validate(); err != nil {
			return nil, err
		}
		for _, subject := range b.AllowedSubjects {
			if issuer != "" && subject.Issuer != issuer {
				return nil, errors.New("catalog subject issuer mismatch")
			}
		}
	}
	return bindings, nil
}
func (s *Server) authorizedCatalog(w http.ResponseWriter, r *http.Request) (CatalogBinding, bool) {
	if !s.workspace(w, r, false) {
		return CatalogBinding{}, false
	}
	bindings, err := s.currentCatalogBindings()
	if err != nil {
		fail(w, 503, "catalog_policy_unavailable", "Catalog policy is unavailable.")
		return CatalogBinding{}, false
	}
	for _, b := range bindings {
		if b.WorkspaceID == r.PathValue("workspaceId") && b.ID == r.PathValue("id") {
			if _, ok := b.Tables(subjectFor(principal(r))); ok {
				return b, true
			}
		}
	}
	fail(w, 403, "catalog_access_denied", "An explicit catalog table grant is required.")
	return CatalogBinding{}, false
}
func (s *Server) catalogStillAuthorized(w http.ResponseWriter, r *http.Request, b CatalogBinding) bool {
	current, ok := s.authorizedCatalog(w, r)
	if !ok {
		return false
	}
	if !sameCatalogBinding(current, b) {
		fail(w, 403, "catalog_access_denied", "Catalog policy changed during the request. Reload before retrying.")
		return false
	}
	return true
}
func (s *Server) catalogBindings(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	bindings, err := s.currentCatalogBindings()
	if err != nil {
		fail(w, 503, "catalog_policy_unavailable", "Catalog policy is unavailable.")
		return
	}
	out := []map[string]any{}
	for _, b := range bindings {
		if b.WorkspaceID != r.PathValue("workspaceId") {
			continue
		}
		if _, ok := b.Tables(subjectFor(principal(r))); !ok {
			continue
		}
		observation, err := s.Store.CatalogObservation(r.Context(), b.WorkspaceID, b.ID)
		if err != nil && !errors.Is(err, ErrNotFound) {
			storeFailure(w, err)
			return
		}
		item := map[string]any{"id": b.ID, "workspaceId": b.WorkspaceID, "name": b.Name, "kind": b.Kind, "catalog": b.Connection.Warehouse, "policyVersion": b.PolicyVersion, "discoveryVerified": observation.Fresh(b, time.Now())}
		if observation.ConfigHash == b.Hash() && !observation.CheckedAt.IsZero() {
			item["checkedAt"] = observation.CheckedAt
		}
		out = append(out, item)
	}
	// Re-read after persistence access; an updated administrator policy must not leak the old grant index.
	current, err := s.currentCatalogBindings()
	if err != nil {
		fail(w, 503, "catalog_policy_unavailable", "Catalog policy is unavailable.")
		return
	}
	if !sameCatalogBindings(bindings, current) {
		fail(w, 403, "catalog_access_denied", "Catalog policy changed during the request. Reload before retrying.")
		return
	}
	writeJSON(w, 200, map[string]any{"items": out})
}
func sameCatalogBindings(a, b []CatalogBinding) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !sameCatalogBinding(a[i], b[i]) {
			return false
		}
	}
	return true
}
func (s *Server) discoverCatalog(w http.ResponseWriter, r *http.Request, b CatalogBinding, force bool) (CatalogObservation, bool) {
	if !force {
		observation, err := s.Store.CatalogObservation(r.Context(), b.WorkspaceID, b.ID)
		if err != nil && !errors.Is(err, ErrNotFound) {
			storeFailure(w, err)
			return CatalogObservation{}, false
		}
		if observation.Fresh(b, time.Now()) {
			return observation, true
		}
	}
	observation := CatalogObservation{WorkspaceID: b.WorkspaceID, BindingID: b.ID, ConfigHash: b.Hash(), CheckedAt: time.Now().UTC(), Capabilities: []string{}}
	capabilities, err := s.CatalogBackend.Discover(r.Context(), b)
	if err == nil && (capabilities.Catalog != b.Connection.Warehouse || capabilities.Prefix != b.Connection.Warehouse) {
		err = errors.New("discovery identity mismatch")
	}
	if err == nil {
		observation.Verified = true
		for _, endpoint := range capabilities.Endpoints {
			if slices.Contains([]string{"GET /v1/{prefix}/namespaces", "GET /v1/{prefix}/namespaces/{namespace}/tables", catalogLoadEndpoint}, endpoint) && !slices.Contains(observation.Capabilities, endpoint) {
				observation.Capabilities = append(observation.Capabilities, endpoint)
			}
		}
	}
	if !s.catalogStillAuthorized(w, r, b) {
		return CatalogObservation{}, false
	}
	if saveErr := s.Store.SaveCatalogObservation(r.Context(), observation); saveErr != nil {
		storeFailure(w, saveErr)
		return CatalogObservation{}, false
	}
	if err != nil {
		fail(w, 503, "catalog_backend_unavailable", "Polaris discovery could not be verified.")
		return CatalogObservation{}, false
	}
	return observation, true
}
func (s *Server) verifyCatalog(w http.ResponseWriter, r *http.Request) {
	b, ok := s.authorizedCatalog(w, r)
	if !ok {
		return
	}
	var in struct{}
	if !decode(w, r, &in) {
		return
	}
	observation, ok := s.discoverCatalog(w, r, b, true)
	if !ok {
		return
	}
	if !s.catalogStillAuthorized(w, r, b) {
		return
	}
	writeJSON(w, 200, map[string]any{"bindingId": b.ID, "workspaceId": b.WorkspaceID, "verified": true, "checkedAt": observation.CheckedAt, "capabilities": observation.Capabilities})
}
func (s *Server) catalogNamespaces(w http.ResponseWriter, r *http.Request) {
	b, ok := s.authorizedCatalog(w, r)
	if !ok {
		return
	}
	tables, _ := b.Tables(subjectFor(principal(r)))
	seen := map[string][]string{}
	for _, t := range tables {
		data, _ := json.Marshal(t.Namespace)
		seen[string(data)] = t.Namespace
	}
	keys := []string{}
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	items := []map[string]any{}
	for _, k := range keys {
		items = append(items, map[string]any{"namespace": seen[k]})
	}
	if !s.catalogStillAuthorized(w, r, b) {
		return
	}
	writeJSON(w, 200, map[string]any{"items": items, "source": "authorized-grants"})
}
func (s *Server) catalogTables(w http.ResponseWriter, r *http.Request) {
	b, ok := s.authorizedCatalog(w, r)
	if !ok {
		return
	}
	var ns []string
	query := r.URL.Query()
	if len(query) != 1 || len(query["namespace"]) != 1 || len(query.Get("namespace")) > 8192 || json.Unmarshal([]byte(query.Get("namespace")), &ns) != nil || !catalogNamespace(ns) {
		fail(w, 422, "invalid_catalog_identifier", "A namespace JSON array of 1–16 valid components is required.")
		return
	}
	tables, _ := b.Tables(subjectFor(principal(r)))
	items := []polaris.Identifier{}
	for _, t := range tables {
		if slices.Equal(t.Namespace, ns) {
			items = append(items, t)
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	if !s.catalogStillAuthorized(w, r, b) {
		return
	}
	writeJSON(w, 200, map[string]any{"items": items, "source": "authorized-grants"})
}
func (s *Server) catalogTable(w http.ResponseWriter, r *http.Request) {
	b, ok := s.authorizedCatalog(w, r)
	if !ok {
		return
	}
	var id polaris.Identifier
	if !decode(w, r, &id) {
		return
	}
	if !catalogIdentifier(id) {
		fail(w, 422, "invalid_catalog_identifier", "A valid namespace and table name are required.")
		return
	}
	if !b.Allows(subjectFor(principal(r)), id) {
		fail(w, 403, "catalog_access_denied", "An exact table grant is required.")
		return
	}
	observation, ok := s.discoverCatalog(w, r, b, false)
	if !ok {
		return
	}
	if !slices.Contains(observation.Capabilities, catalogLoadEndpoint) {
		fail(w, 501, "catalog_capability_unavailable", "Polaris did not advertise table metadata reads.")
		return
	}
	// Recheck after discovery/cache access before using the shared service credential.
	if !s.catalogStillAuthorized(w, r, b) {
		return
	}
	table, err := s.CatalogBackend.LoadTable(r.Context(), b, id)
	if !s.catalogStillAuthorized(w, r, b) {
		return
	}
	if err != nil {
		var remote *polaris.Error
		if errors.As(err, &remote) && remote.StatusCode == 404 {
			fail(w, 404, "table_not_found", "The granted table was not found in Polaris.")
		} else {
			fail(w, 503, "catalog_backend_unavailable", "Polaris table metadata is unavailable.")
		}
		return
	}
	if table.Identifier.Name != id.Name || !slices.Equal(table.Identifier.Namespace, id.Namespace) {
		fail(w, 503, "catalog_backend_unavailable", "Polaris returned inconsistent table identity.")
		return
	}
	out := map[string]any{"bindingId": b.ID, "workspaceId": b.WorkspaceID, "policyVersion": b.PolicyVersion, "identifier": table.Identifier, "formatVersion": table.FormatVersion, "uuid": table.UUID, "lastUpdatedMs": table.LastUpdatedMS, "currentSchema": table.CurrentSchema}
	if table.CurrentSnapshotID != nil {
		out["currentSnapshotId"] = strconv.FormatInt(*table.CurrentSnapshotID, 10)
	}
	writeJSON(w, 200, out)
}
