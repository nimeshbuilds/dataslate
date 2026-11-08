package platform

import (
	"context"
	"errors"
	"net/http"
)

func (s *Server) currentExecutionBindings() ([]ExecutionBinding, error) {
	bindings := s.Config.ExecutionBindings
	var e error
	if s.Config.ExecutionBindingsFile != "" {
		bindings, e = LoadExecutionBindings(s.Config.ExecutionBindingsFile)
		if e != nil {
			return nil, e
		}
	}
	issuer := s.Config.Issuer
	if s.Config.AuthMode == "development" {
		issuer = "development"
	}
	for _, b := range bindings {
		if e = b.Validate(); e != nil {
			return nil, e
		}
		for _, o := range b.AllowedSubjects {
			if issuer != "" && o.Issuer != issuer {
				return nil, ErrPublicationAccess
			}
		}
	}
	return bindings, nil
}
func (s *Server) authorizeOutcomeRun(r Run) error {
	bindings, e := s.currentExecutionBindings()
	if e != nil {
		return e
	}
	for _, b := range bindings {
		if b.WorkspaceID == r.WorkspaceID && b.ID == r.BindingID && b.Allows(r.SubmittedBy) && sameExecutionProfile(b.ExecutionProfile, r.Profile) {
			return nil
		}
	}
	return ErrPublicationAccess
}
func (s *Server) currentPublicationBindings() ([]PublicationBinding, error) {
	bindings := s.Config.PublicationBindings
	var e error
	if s.Config.PublicationBindingsFile != "" {
		bindings, e = LoadPublicationBindings(s.Config.PublicationBindingsFile)
		if e != nil {
			return nil, e
		}
	}
	issuer := s.Config.Issuer
	if s.Config.AuthMode == "development" {
		issuer = "development"
	}
	for _, b := range bindings {
		if e = b.Validate(); e != nil {
			return nil, e
		}
		for _, o := range b.AllowedPublishers {
			if issuer != "" && o.Issuer != issuer {
				return nil, ErrPublicationAccess
			}
		}
		for _, o := range b.Readers {
			if issuer != "" && o.Issuer != issuer {
				return nil, ErrPublicationAccess
			}
		}
	}
	sql, e := s.currentSQLBindings()
	if e != nil {
		return nil, e
	}
	execution, e := s.currentExecutionBindings()
	if e != nil {
		return nil, e
	}
	if e = ValidatePublicationConfiguration(bindings, execution, sql); e != nil {
		return nil, e
	}
	return bindings, nil
}
func (s *Server) authorizePublication(w PublicationWork) error {
	bindings, e := s.currentPublicationBindings()
	if e != nil {
		return e
	}
	for _, b := range bindings {
		if b.WorkspaceID == w.Publication.WorkspaceID && b.ID == w.Publication.BindingID && b.Hash() == w.PolicyHash && b.Allows(w.Publication.SubmittedBy) {
			return s.authorizeOutcomeRun(w.Run)
		}
	}
	return ErrPublicationAccess
}
func (s *Server) ingestionOutcome(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	run, e := s.Store.Run(r.Context(), r.PathValue("workspaceId"), r.PathValue("id"), subjectFor(principal(r)))
	if e != nil {
		storeFailure(w, e)
		return
	}
	if run.Ingestion == nil {
		storeFailure(w, ErrNotFound)
		return
	}
	if s.authorizeOutcomeRun(run) != nil {
		fail(w, 403, "forbidden", "Current ingestion execution access is required.")
		return
	}
	outcome, e := s.Store.IngestionOutcome(r.Context(), run.WorkspaceID, run.ID, run.SubmittedBy)
	if e != nil {
		storeFailure(w, e)
		return
	}
	if s.authorizeOutcomeRun(run) != nil {
		fail(w, 403, "forbidden", "Current ingestion execution access is required.")
		return
	}
	writeJSON(w, 200, outcome)
}
func (s *Server) publicationBindings(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	bindings, e := s.currentPublicationBindings()
	if e != nil {
		fail(w, 503, "publication_policy_unavailable", "Publication policy is unavailable.")
		return
	}
	out := []PublicationBindingSummary{}
	if principal(r).Can(r.PathValue("workspaceId"), true) {
		for _, b := range bindings {
			if b.WorkspaceID == r.PathValue("workspaceId") && b.Allows(subjectFor(principal(r))) {
				out = append(out, b.PublicationBindingSummary)
			}
		}
	}
	writeJSON(w, 200, map[string]any{"items": out})
}
func publicationFail(w http.ResponseWriter, e error) {
	switch {
	case errors.Is(e, ErrPublicationAccess):
		fail(w, 403, "forbidden", "Current publication and ingestion grants are required.")
	case errors.Is(e, ErrIngestionOutcome):
		fail(w, 409, "ingestion_outcome_unavailable", "A succeeded, drained ingestion run with verified nonempty Delta evidence is required.")
	case errors.Is(e, ErrPublicationSchema):
		fail(w, 422, "publication_schema_unsupported", "This reader profile cannot preserve the ingestion schema. Timestamp publication is not supported.")
	case errors.Is(e, ErrPublicationBusy):
		fail(w, 409, "publication_target_busy", "This target has unresolved publication work. Recover its original request.")
	case errors.Is(e, ErrPublicationTarget):
		fail(w, 409, "publication_target_conflict", "The target is permanently bound to another mapping.")
	case errors.Is(e, ErrRunCapacity):
		fail(w, 429, "publication_capacity", "The workspace has reached its publication capacity.")
	default:
		storeFailure(w, e)
	}
}
func (s *Server) createPublication(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, true) {
		return
	}
	var in PublicationInput
	if !decode(w, r, &in) {
		return
	}
	if e := in.Validate(); e != nil {
		fail(w, 422, "invalid_publication", e.Error())
		return
	}
	bindings, e := s.currentPublicationBindings()
	if e != nil {
		fail(w, 503, "publication_policy_unavailable", "Publication policy is unavailable.")
		return
	}
	if len(bindings) == 0 {
		fail(w, 501, "capability_unavailable", "Delta publication is not configured.")
		return
	}
	var binding *PublicationBinding
	owner := subjectFor(principal(r))
	for _, b := range bindings {
		if b.ID == in.BindingID && b.WorkspaceID == r.PathValue("workspaceId") && b.Allows(owner) {
			binding = &b
			break
		}
	}
	if binding == nil {
		publicationFail(w, ErrPublicationAccess)
		return
	}
	run, e := s.Store.Run(r.Context(), binding.WorkspaceID, in.RunID, owner)
	if e != nil {
		storeFailure(w, e)
		return
	}
	if e = s.authorizeOutcomeRun(run); e != nil {
		publicationFail(w, ErrPublicationAccess)
		return
	}
	if !run.WriterReleased {
		fail(w, 409, "ingestion_writer_busy", "Ingestion compute must be confirmed drained before publication.")
		return
	}
	source, e := s.Store.RunSource(r.Context(), run.WorkspaceID, run.ID, owner)
	if e != nil {
		storeFailure(w, e)
		return
	}
	outcome, e := s.Store.IngestionOutcome(r.Context(), run.WorkspaceID, run.ID, owner)
	if e != nil {
		storeFailure(w, e)
		return
	}
	work, e := newPublication(*binding, run, source, outcome)
	if e != nil {
		publicationFail(w, e)
		return
	}
	out, replay, e := s.Store.CreatePublication(r.Context(), work, in.IdempotencyKey, digestJSON([]string{in.RunID, in.BindingID}))
	if e != nil {
		publicationFail(w, e)
		return
	}
	if s.authorizePublication(out) != nil {
		publicationFail(w, ErrPublicationAccess)
		return
	}
	writeJSON(w, 202, map[string]any{"publication": out.Publication, "replayed": replay})
}
func (s *Server) listPublications(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	works, e := s.Store.Publications(r.Context(), r.PathValue("workspaceId"), subjectFor(principal(r)))
	if e != nil {
		storeFailure(w, e)
		return
	}
	out := []TablePublication{}
	for _, work := range works {
		if s.authorizePublication(work) == nil {
			out = append(out, work.Publication)
		}
	}
	writeJSON(w, 200, map[string]any{"items": out})
}
func (s *Server) getPublication(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	work, e := s.Store.Publication(r.Context(), r.PathValue("workspaceId"), r.PathValue("id"), subjectFor(principal(r)))
	if e != nil {
		storeFailure(w, e)
		return
	}
	if s.authorizePublication(work) != nil {
		publicationFail(w, ErrPublicationAccess)
		return
	}
	writeJSON(w, 200, work.Publication)
}
func (s *Server) authorizedPublishedTable(ctx context.Context, ws, id string, owner ExecutionSubject) (PublishedTable, error) {
	record, e := s.Store.PublishedTable(ctx, ws, id)
	if e != nil {
		return PublishedTable{}, e
	}
	bindings, e := s.currentPublicationBindings()
	if e != nil {
		return PublishedTable{}, ErrPublicationAccess
	}
	sql, e := s.currentSQLBindings()
	if e != nil {
		return PublishedTable{}, ErrPublicationAccess
	}
	for _, b := range bindings {
		if b.WorkspaceID != ws || b.ID != id || b.Hash() != record.PolicyHash || b.mappingKey() != record.MappingKey {
			continue
		}
		for _, grant := range b.Readers {
			if grant.ExecutionSubject != owner {
				continue
			}
			for _, engine := range sql {
				if engine.WorkspaceID == ws && engine.ID == grant.SQLBindingID && canonicalPublicationEndpoint(engine.Endpoint) == canonicalPublicationEndpoint(b.Endpoint) && engine.Catalog == b.Catalog && engine.Schema == b.Schema {
					if _, ok := engine.Policy(owner); ok {
						table := record.Table
						table.SQL = PublishedTableSQL{BindingID: engine.ID, PolicyVersion: engine.PolicyVersion, Draft: `SELECT * FROM "` + b.Catalog + `"."` + b.Schema + `"."` + b.Table + `" LIMIT 1000`}
						return table, nil
					}
				}
			}
		}
	}
	return PublishedTable{}, ErrPublicationAccess
}
func (s *Server) listPublishedTables(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	bindings, e := s.currentPublicationBindings()
	if e != nil {
		fail(w, 503, "publication_policy_unavailable", "Publication policy is unavailable.")
		return
	}
	out := []PublishedTable{}
	for _, b := range bindings {
		if b.WorkspaceID == r.PathValue("workspaceId") {
			table, e := s.authorizedPublishedTable(r.Context(), b.WorkspaceID, b.ID, subjectFor(principal(r)))
			if e == nil {
				out = append(out, table)
			}
		}
	}
	writeJSON(w, 200, map[string]any{"items": out})
}
func (s *Server) getPublishedTable(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	table, e := s.authorizedPublishedTable(r.Context(), r.PathValue("workspaceId"), r.PathValue("id"), subjectFor(principal(r)))
	if e != nil {
		publicationFail(w, e)
		return
	}
	writeJSON(w, 200, table)
}
