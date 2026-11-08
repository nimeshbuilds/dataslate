package platform

import (
	"errors"
	"net/http"
)

func (s *Server) currentSQLBindings() ([]SQLBinding, error) {
	bindings := s.Config.SQLBindings
	var err error
	if s.Config.SQLBindingsFile != "" {
		bindings, err = LoadSQLBindings(s.Config.SQLBindingsFile)
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
				return nil, errors.New("SQL subject issuer mismatches authentication issuer")
			}
		}
	}
	return bindings, nil
}
func (s *Server) sqlBindings(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	bindings, err := s.currentSQLBindings()
	if err != nil {
		fail(w, 503, "sql_policy_unavailable", "SQL policy is unavailable.")
		return
	}
	out := []map[string]any{}
	if principal(r).Can(r.PathValue("workspaceId"), true) {
		for _, b := range bindings {
			if b.WorkspaceID == r.PathValue("workspaceId") {
				if _, allowed := b.Policy(subjectFor(principal(r))); allowed {
					out = append(out, map[string]any{"id": b.ID, "workspaceId": b.WorkspaceID, "name": b.Name, "engine": b.Engine, "policyVersion": b.PolicyVersion, "catalog": b.Catalog, "schema": b.Schema, "maxRunSeconds": b.MaxRunSeconds})
				}
			}
		}
	}
	writeJSON(w, 200, map[string]any{"items": out})
}
func (s *Server) createQuery(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, true) {
		return
	}
	bindings, err := s.currentSQLBindings()
	if err != nil {
		fail(w, 503, "sql_policy_unavailable", "SQL policy is unavailable.")
		return
	}
	if len(bindings) == 0 {
		fail(w, 501, "capability_unavailable", "Governed SQL is not configured. No engine request was made.")
		return
	}
	var in QueryInput
	if !decode(w, r, &in) {
		return
	}
	if err = in.Validate(); err != nil {
		fail(w, 422, "invalid_query", err.Error())
		return
	}
	subject := subjectFor(principal(r))
	var selected *SQLBinding
	var policy SQLPolicy
	for _, b := range bindings {
		if b.ID == in.BindingID && b.WorkspaceID == r.PathValue("workspaceId") {
			if p, allowed := b.Policy(subject); allowed {
				selected = &b
				policy = p
				break
			}
		}
	}
	if selected == nil {
		fail(w, 403, "sql_execution_denied", "An explicit SQL execution grant and approved engine identity are required.")
		return
	}
	q := newQuery(r.PathValue("workspaceId"), *selected, subject, in.Notebook)
	out, replay, err := s.Store.CreateQuery(r.Context(), q, policy, in.IdempotencyKey, queryFingerprint(in))
	if errors.Is(err, ErrSQLRevision) {
		fail(w, 409, "sql_revision_conflict", "The saved SQL revision changed. Reload before a new query intent.")
		return
	}
	if errors.Is(err, ErrSQLNotExecutable) {
		fail(w, 422, "sql_not_executable", "Select a nonempty saved SQL cell no larger than 64 KiB.")
		return
	}
	if errors.Is(err, ErrQueryCapacity) {
		fail(w, 429, "query_capacity_exceeded", "This workspace has 100 active or unresolved queries.")
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
	writeJSON(w, code, out)
}
func (s *Server) listQueries(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	out, err := s.Store.Queries(r.Context(), r.PathValue("workspaceId"), subjectFor(principal(r)))
	if err != nil {
		storeFailure(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"items": out})
}
func (s *Server) getQuery(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	out, err := s.Store.Query(r.Context(), r.PathValue("workspaceId"), r.PathValue("id"), subjectFor(principal(r)))
	if err != nil {
		storeFailure(w, err)
		return
	}
	writeJSON(w, 200, out)
}
func (s *Server) cancelQuery(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, true) {
		return
	}
	var input struct{}
	if !decode(w, r, &input) {
		return
	}
	out, err := s.Store.CancelQuery(r.Context(), r.PathValue("workspaceId"), r.PathValue("id"), subjectFor(principal(r)))
	if err != nil {
		storeFailure(w, err)
		return
	}
	writeJSON(w, 202, out)
}
func (s *Server) authorizeQueryOutput(w http.ResponseWriter, r *http.Request) bool {
	q, err := s.Store.Query(r.Context(), r.PathValue("workspaceId"), r.PathValue("id"), subjectFor(principal(r)))
	if err != nil {
		storeFailure(w, err)
		return false
	}
	policy, err := s.Store.QueryPolicy(r.Context(), q.WorkspaceID, q.ID, q.SubmittedBy)
	if err != nil {
		storeFailure(w, err)
		return false
	}
	bindings, err := s.currentSQLBindings()
	if err != nil {
		fail(w, 503, "sql_policy_unavailable", "Current SQL policy is unavailable; source and results remain closed.")
		return false
	}
	for _, b := range bindings {
		if b.ID == q.BindingID && b.WorkspaceID == q.WorkspaceID {
			current, allowed := b.Policy(q.SubmittedBy)
			if allowed && current == policy {
				return true
			}
		}
	}
	fail(w, 403, "sql_execution_denied", "Current SQL execution identity and policy are required to access this source or result.")
	return false
}
func (s *Server) querySource(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) || !s.authorizeQueryOutput(w, r) {
		return
	}
	out, err := s.Store.QuerySource(r.Context(), r.PathValue("workspaceId"), r.PathValue("id"), subjectFor(principal(r)))
	if err != nil {
		storeFailure(w, err)
		return
	}
	if !s.authorizeQueryOutput(w, r) {
		return
	}
	writeJSON(w, 200, out)
}
func (s *Server) queryResult(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) || !s.authorizeQueryOutput(w, r) {
		return
	}
	out, err := s.Store.QueryResult(r.Context(), r.PathValue("workspaceId"), r.PathValue("id"), subjectFor(principal(r)))
	if err != nil {
		storeFailure(w, err)
		return
	}
	if !s.authorizeQueryOutput(w, r) {
		return
	}
	writeJSON(w, 200, out)
}
