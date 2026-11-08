package platform

import (
	"context"
	"errors"
	"net/http"
	"slices"
)

func sylonFailure(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrSylonAccess), errors.Is(err, ErrMemoryAccess):
		fail(w, 403, "sylon_access_denied", "Current assistant and source grants are required.")
	case errors.Is(err, ErrSylonRevision):
		fail(w, 409, "sylon_revision_conflict", "The saved notebook revision or cell changed. Reload before continuing.")
	case errors.Is(err, ErrSylonCapacity):
		fail(w, 429, "sylon_capacity", "Conversation history or active assistant capacity is exhausted. Finish the current turn or start a new conversation.")
	default:
		storeFailure(w, err)
	}
}
func (s *Server) sylonCurrent(w http.ResponseWriter, r *http.Request, id string) (SylonBinding, bool) {
	b, err := s.authorizedSylonBinding(r.PathValue("workspaceId"), id, subjectFor(principal(r)))
	if err != nil {
		if errors.Is(err, ErrSylonAccess) {
			sylonFailure(w, err)
		} else {
			fail(w, 503, "sylon_policy_unavailable", "Current assistant policy is unavailable.")
		}
		return SylonBinding{}, false
	}
	return b, true
}
func (s *Server) authorizeSylonWork(ctx context.Context, work SylonWork, currentRevision bool) error {
	j := work.Job
	if j.MemoryInvalidated {
		return ErrMemoryAccess
	}
	b, err := s.authorizedSylonBinding(j.WorkspaceID, j.BindingID, j.SubmittedBy)
	if err != nil {
		return err
	}
	if b.Hash() != work.PolicyHash {
		return ErrSylonAccess
	}
	for _, m := range j.Memories {
		if _, err := s.AuthorizeSylonMemory(ctx, b, j.SubmittedBy, m); err != nil {
			return err
		}
	}
	if j.Notebook != nil {
		if !b.Context.NotebookCellSource {
			return ErrSylonAccess
		}
		n, err := s.Store.Notebook(ctx, j.WorkspaceID, j.Notebook.ID)
		if err != nil {
			return ErrSylonAccess
		}
		if currentRevision {
			if n.Revision != j.Notebook.Revision {
				return ErrSylonRevision
			}
			found := false
			for _, cell := range n.Cells {
				if cell.ID == j.Notebook.CellID && (cell.Language == "python" || cell.Language == "sql") {
					found = true
				}
			}
			if !found {
				return ErrSylonRevision
			}
		}
	}
	if len(j.Catalogs) > 0 {
		catalogs, err := s.currentCatalogBindings()
		if err != nil {
			return err
		}
		for _, selection := range j.Catalogs {
			if !slices.Contains(b.Context.CatalogBindingIDs, selection.BindingID) {
				return ErrSylonAccess
			}
			allowed := false
			for _, catalog := range catalogs {
				if catalog.WorkspaceID == j.WorkspaceID && catalog.ID == selection.BindingID && catalog.Allows(j.SubmittedBy, selection.Identifier) {
					allowed = true
				}
			}
			if !allowed {
				return ErrSylonAccess
			}
		}
	}
	if j.Search != nil {
		if !slices.Contains(b.Context.SearchBindingIDs, j.Search.BindingID) {
			return ErrSylonAccess
		}
		bindings, err := s.currentSearchBindings()
		if err != nil {
			return err
		}
		allowed := false
		for _, search := range bindings {
			if search.WorkspaceID == j.WorkspaceID && search.ID == j.Search.BindingID {
				if search.Sources.IncludeCellSource && !b.Context.NotebookCellSource {
					return ErrSylonAccess
				}
				allowed = true
			}
		}
		if !allowed {
			return ErrSylonAccess
		}
	}
	return s.AuthorizeSylonDependencies(ctx, b, j.SubmittedBy, work.Context.Dependencies)
}
func (s *Server) sylonPublicJob(ctx context.Context, work SylonWork) (SylonJob, error) {
	if err := s.authorizeSylonWork(ctx, work, false); err != nil {
		return SylonJob{}, err
	}
	job := cloneSylonWork(work).Job
	for i, citation := range job.Citations {
		if citation.Notebook != nil {
			n, err := s.Store.Notebook(ctx, job.WorkspaceID, citation.Notebook.ID)
			if err != nil {
				return SylonJob{}, ErrSylonAccess
			}
			job.Citations[i].Historical = n.Revision != citation.Notebook.Revision
		}
	}
	return job, nil
}
func (s *Server) sylonBindings(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	bindings, err := s.currentSylonBindings()
	if err != nil {
		fail(w, 503, "sylon_policy_unavailable", "Current assistant policy is unavailable.")
		return
	}
	items := []map[string]any{}
	for _, b := range bindings {
		if b.WorkspaceID == r.PathValue("workspaceId") && b.Allows(subjectFor(principal(r))) {
			publicContext := b.Context
			publicContext.CatalogBindingIDs = append([]string{}, b.Context.CatalogBindingIDs...)
			publicContext.SearchBindingIDs = append([]string{}, b.Context.SearchBindingIDs...)
			item := map[string]any{"id": b.ID, "workspaceId": b.WorkspaceID, "name": b.Name, "policyVersion": b.PolicyVersion, "model": b.Provider.Model, "protocol": b.Provider.Protocol, "maxOutputTokens": b.Provider.MaxOutputTokens, "maxContextBytes": b.MaxContextBytes, "dataTransferAccepted": b.DataTransferAccepted, "context": publicContext}
			if b.Memory != nil {
				item["memory"] = b.Memory
			}
			items = append(items, item)
		}
	}
	writeJSON(w, 200, map[string]any{"items": items})
}
func (s *Server) listSylonConversations(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	bindings, err := s.currentSylonBindings()
	if err != nil {
		fail(w, 503, "sylon_policy_unavailable", "Current assistant policy is unavailable.")
		return
	}
	items, err := s.Store.SylonConversations(r.Context(), r.PathValue("workspaceId"), subjectFor(principal(r)))
	if err != nil {
		sylonFailure(w, err)
		return
	}
	out := []SylonConversation{}
	for _, c := range items {
		for _, b := range bindings {
			if b.WorkspaceID == c.WorkspaceID && b.ID == c.BindingID && b.Allows(c.Owner) {
				out = append(out, c)
				break
			}
		}
	}
	writeJSON(w, 200, map[string]any{"items": out})
}
func (s *Server) createSylonConversation(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	var in SylonConversationInput
	if !decode(w, r, &in) {
		return
	}
	if err := in.Validate(); err != nil {
		fail(w, 422, "invalid_sylon_conversation", err.Error())
		return
	}
	b, ok := s.sylonCurrent(w, r, in.BindingID)
	if !ok {
		return
	}
	out, err := s.Store.CreateSylonConversation(r.Context(), SylonConversation{Resource: newResource("chat-"), WorkspaceID: b.WorkspaceID, BindingID: b.ID, Owner: subjectFor(principal(r)), Title: in.Title})
	if err != nil {
		sylonFailure(w, err)
		return
	}
	writeJSON(w, 201, out)
}
func (s *Server) getSylonConversation(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	ws, id, owner := r.PathValue("workspaceId"), r.PathValue("id"), subjectFor(principal(r))
	conversation, err := s.Store.SylonConversation(r.Context(), ws, id, owner)
	if err != nil {
		sylonFailure(w, err)
		return
	}
	if _, ok := s.sylonCurrent(w, r, conversation.BindingID); !ok {
		return
	}
	history, err := s.Store.SylonHistory(r.Context(), ws, id, owner)
	if err != nil {
		sylonFailure(w, err)
		return
	}
	messages := []SylonJob{}
	for _, work := range history {
		if !s.sylonRequestRoleAllowed(r, work) {
			sylonFailure(w, ErrSylonAccess)
			return
		}
		job, err := s.sylonPublicJob(r.Context(), work)
		if err != nil {
			sylonFailure(w, err)
			return
		}
		messages = append(messages, job)
	}
	if _, err = s.Store.SylonConversation(r.Context(), ws, id, owner); err != nil {
		sylonFailure(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"conversation": conversation, "messages": messages})
}
func (s *Server) deleteSylonConversation(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	if err := s.Store.DeleteSylonConversation(r.Context(), r.PathValue("workspaceId"), r.PathValue("id"), subjectFor(principal(r))); err != nil {
		sylonFailure(w, err)
		return
	}
	w.WriteHeader(204)
}
func (s *Server) createSylonMessage(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	var in SylonMessageInput
	if !decode(w, r, &in) {
		return
	}
	if err := in.Validate(); err != nil {
		fail(w, 422, "invalid_sylon_message", err.Error())
		return
	}
	ws, owner := r.PathValue("workspaceId"), subjectFor(principal(r))
	conversation, err := s.Store.SylonConversation(r.Context(), ws, r.PathValue("id"), owner)
	if err != nil {
		sylonFailure(w, err)
		return
	}
	b, ok := s.sylonCurrent(w, r, conversation.BindingID)
	if !ok {
		return
	}
	if in.Notebook != nil && !principal(r).Can(ws, true) {
		sylonFailure(w, ErrSylonAccess)
		return
	}
	history, err := s.Store.SylonHistory(r.Context(), ws, conversation.ID, owner)
	if err != nil {
		sylonFailure(w, err)
		return
	}
	for _, work := range history {
		if !s.sylonRequestRoleAllowed(r, work) {
			sylonFailure(w, ErrSylonAccess)
			return
		}
		if err = s.authorizeSylonWork(r.Context(), work, false); err != nil {
			sylonFailure(w, err)
			return
		}
	}
	work := SylonWork{Job: newSylonJob(conversation, b, in), PolicyHash: b.Hash()}
	if !s.sylonRequestRoleAllowed(r, work) {
		sylonFailure(w, ErrSylonAccess)
		return
	}
	if err = s.authorizeSylonWork(r.Context(), work, false); err != nil {
		sylonFailure(w, err)
		return
	}
	job, replay, err := s.Store.CreateSylonJob(r.Context(), work, in.IdempotencyKey, sylonFingerprint(in))
	if err != nil {
		sylonFailure(w, err)
		return
	}
	accepted, err := s.Store.SylonWork(r.Context(), ws, job.ID, owner)
	if err != nil {
		sylonFailure(w, err)
		return
	}
	if !s.sylonRequestRoleAllowed(r, accepted) {
		sylonFailure(w, ErrSylonAccess)
		return
	}
	job, err = s.sylonPublicJob(r.Context(), accepted)
	if err != nil {
		sylonFailure(w, err)
		return
	}
	code := 202
	if replay {
		code = 200
	}
	writeJSON(w, code, job)
}
func (s *Server) getSylonJob(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	work, err := s.Store.SylonWork(r.Context(), r.PathValue("workspaceId"), r.PathValue("id"), subjectFor(principal(r)))
	if err != nil {
		sylonFailure(w, err)
		return
	}
	if !s.sylonRequestRoleAllowed(r, work) {
		sylonFailure(w, ErrSylonAccess)
		return
	}
	out, err := s.sylonPublicJob(r.Context(), work)
	if err != nil {
		sylonFailure(w, err)
		return
	}
	writeJSON(w, 200, out)
}
func (s *Server) cancelSylonJob(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	var in struct{}
	if !decode(w, r, &in) {
		return
	}
	out, err := s.Store.CancelSylonJob(r.Context(), r.PathValue("workspaceId"), r.PathValue("id"), subjectFor(principal(r)))
	if err != nil {
		sylonFailure(w, err)
		return
	}
	writeJSON(w, 202, map[string]any{"jobId": out.ID, "workspaceId": out.WorkspaceID, "state": out.State, "cancelRequested": out.CancelRequested})
}
func (s *Server) applySylonCode(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, true) {
		return
	}
	var in SylonApplyInput
	if !decode(w, r, &in) {
		return
	}
	if !identifier.MatchString(in.NotebookID) || !identifier.MatchString(in.CellID) || in.Revision < 1 {
		fail(w, 422, "invalid_sylon_apply", "An exact target notebook, revision and cell are required.")
		return
	}
	ws, id, owner := r.PathValue("workspaceId"), r.PathValue("id"), subjectFor(principal(r))
	work, err := s.Store.SylonWork(r.Context(), ws, id, owner)
	if err != nil {
		sylonFailure(w, err)
		return
	}
	if err = s.authorizeSylonWork(r.Context(), work, false); err != nil {
		sylonFailure(w, err)
		return
	}
	out, err := s.Store.ApplySylonCode(r.Context(), ws, id, owner, in)
	if err != nil {
		sylonFailure(w, err)
		return
	}
	writeJSON(w, 200, out)
}

func (s *Server) sylonRequestRoleAllowed(r *http.Request, work SylonWork) bool {
	p := principal(r)
	if p.Can(work.Job.WorkspaceID, true) {
		return true
	}
	if work.Job.Notebook != nil {
		return false
	}
	for _, d := range work.Context.Dependencies {
		if d.Kind == "notebook-cell" {
			return false
		}
	}
	for _, sel := range work.Job.Memories {
		m, e := s.Store.SylonMemoryWork(r.Context(), work.Job.WorkspaceID, sel.ID, work.Job.SubmittedBy)
		if e != nil || !memoryRoleAllowed(p, m) {
			return false
		}
	}
	return p.Can(work.Job.WorkspaceID, false)
}
