package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"
)

func (s *Server) currentWorkflowBindings() ([]WorkflowBinding, error) {
	bindings := s.Config.WorkflowBindings
	var e error
	if s.Config.WorkflowBindingsFile != "" {
		bindings, e = LoadWorkflowBindings(s.Config.WorkflowBindingsFile)
		if e != nil {
			return nil, e
		}
	}
	issuer := s.Config.Issuer
	if s.Config.AuthMode == "development" {
		issuer = "development"
	}
	for _, b := range bindings {
		if b.Validate() != nil {
			return nil, ErrWorkflowAccess
		}
		for _, owner := range b.AllowedSubjects {
			if issuer != "" && owner.Issuer != issuer {
				return nil, ErrWorkflowAccess
			}
		}
	}
	return bindings, nil
}
func (s *Server) workflowBinding(ws, id string, owner ExecutionSubject) (WorkflowBinding, error) {
	bindings, e := s.currentWorkflowBindings()
	if e != nil {
		return WorkflowBinding{}, e
	}
	for _, b := range bindings {
		if b.WorkspaceID == ws && b.ID == id && b.Allows(owner) {
			return b, nil
		}
	}
	return WorkflowBinding{}, ErrWorkflowAccess
}
func (s *Server) authorizeWorkflow(w WorkflowWork) error {
	b, e := s.workflowBinding(w.Run.WorkspaceID, w.Run.BindingID, w.Run.SubmittedBy)
	if e != nil || b.Hash() != w.PolicyHash {
		return ErrWorkflowAccess
	}
	return nil
}
func (s *Server) workflowEngine(ctx context.Context, b WorkflowBinding) (WorkflowEngine, error) {
	if s.WorkflowBackend != nil {
		return s.WorkflowBackend.Open(ctx, b)
	}
	return (&FileWorkflowBackend{}).Open(ctx, b)
}
func workflowFailure(w http.ResponseWriter, e error) {
	switch {
	case errors.Is(e, ErrWorkflowAccess):
		fail(w, 403, "forbidden", "A current exact workflow binding grant is required.")
	case errors.Is(e, ErrRunCapacity):
		fail(w, 429, "workflow_capacity", "The workspace has reached its active workflow limit.")
	default:
		storeFailure(w, e)
	}
}
func (s *Server) workflowBindings(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	bindings, e := s.currentWorkflowBindings()
	if e != nil {
		fail(w, 503, "workflow_policy_unavailable", "Workflow policy is unavailable.")
		return
	}
	out := []any{}
	for _, b := range bindings {
		if b.WorkspaceID == r.PathValue("workspaceId") && b.Allows(subjectFor(principal(r))) {
			out = append(out, struct {
				WorkflowBindingSummary
				Scheduler  string `json:"scheduler"`
				RetryOwner string `json:"retryOwner"`
			}{b.WorkflowBindingSummary, "airflow", "airflow"})
		}
	}
	writeJSON(w, 200, map[string]any{"items": out})
}
func (s *Server) workflowDefinition(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	b, e := s.workflowBinding(r.PathValue("workspaceId"), r.PathValue("id"), subjectFor(principal(r)))
	if e != nil {
		workflowFailure(w, ErrWorkflowAccess)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
	defer cancel()
	engine, e := s.workflowEngine(ctx, b)
	if e != nil {
		fail(w, 502, "workflow_backend_unavailable", "Airflow definition is unavailable.")
		return
	}
	defer engine.Close()
	d, e := engine.Definition(ctx, b.Target())
	if e != nil {
		fail(w, 502, "workflow_backend_unavailable", "The approved Airflow definition could not be verified.")
		return
	}
	current, e := s.workflowBinding(b.WorkspaceID, b.ID, subjectFor(principal(r)))
	if e != nil || current.Hash() != b.Hash() {
		workflowFailure(w, ErrWorkflowAccess)
		return
	}
	writeJSON(w, 200, workflowDefinition(b, d))
}
func (s *Server) workflowHistory(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	b, e := s.workflowBinding(r.PathValue("workspaceId"), r.PathValue("id"), subjectFor(principal(r)))
	if e != nil {
		workflowFailure(w, ErrWorkflowAccess)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	engine, e := s.workflowEngine(ctx, b)
	if e != nil {
		fail(w, 502, "workflow_backend_unavailable", "Airflow history is unavailable.")
		return
	}
	defer engine.Close()
	runs, truncated, e := engine.History(ctx, b.Target())
	if e != nil {
		fail(w, 502, "workflow_backend_unavailable", "Airflow history could not be verified for this versioned-bundle profile.")
		return
	}
	current, e := s.workflowBinding(b.WorkspaceID, b.ID, subjectFor(principal(r)))
	if e != nil || current.Hash() != b.Hash() {
		workflowFailure(w, ErrWorkflowAccess)
		return
	}
	out := []WorkflowHistoryRun{}
	for _, run := range runs {
		out = append(out, WorkflowHistoryRun{run.DAGID, run.RunID, run.State, run.RunType, run.Versions[0].Number, run.BundleVersion, run.LogicalDate, run.StartedAt, run.EndedAt})
	}
	writeJSON(w, 200, map[string]any{"items": out, "observedAt": time.Now().UTC(), "truncated": truncated})
}
func (s *Server) createWorkflow(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, true) {
		return
	}
	var raw json.RawMessage
	if !decode(w, r, &raw) {
		return
	}
	var in WorkflowInput
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if len(raw) > 4096 || validateUniqueJSON(raw) != nil || d.Decode(&in) != nil {
		fail(w, 400, "invalid_json", "A bounded workflow request with unique known fields is required.")
		return
	}
	if e := in.Validate(); e != nil {
		fail(w, 400, "invalid_request", e.Error())
		return
	}
	bindings, e := s.currentWorkflowBindings()
	if e != nil {
		fail(w, 503, "workflow_policy_unavailable", "Workflow policy is unavailable.")
		return
	}
	if len(bindings) == 0 {
		fail(w, 501, "capability_unavailable", "Airflow workflows are not configured.")
		return
	}
	owner := subjectFor(principal(r))
	b, e := s.workflowBinding(r.PathValue("workspaceId"), in.BindingID, owner)
	if e != nil {
		workflowFailure(w, ErrWorkflowAccess)
		return
	}
	if in.PolicyVersion != b.PolicyVersion || in.DAGID != b.DAGID || in.DAGVersion != b.DAGVersion || in.BundleVersion != b.BundleVersion {
		fail(w, 409, "workflow_review_changed", "The workflow binding changed. Review its current exact policy and DAG version before creating a new request.")
		return
	}
	work := newWorkflow(b, owner)
	out, replayed, e := s.Store.CreateWorkflow(r.Context(), work, in.IdempotencyKey, digestJSON(in))
	if e != nil {
		workflowFailure(w, e)
		return
	}
	if s.authorizeWorkflow(out) != nil {
		workflowFailure(w, ErrWorkflowAccess)
		return
	}
	writeJSON(w, 202, map[string]any{"run": out.Run, "replayed": replayed})
}
func (s *Server) listWorkflows(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	works, e := s.Store.Workflows(r.Context(), r.PathValue("workspaceId"), subjectFor(principal(r)))
	if e != nil {
		storeFailure(w, e)
		return
	}
	if _, e = s.currentWorkflowBindings(); e != nil {
		fail(w, 503, "workflow_policy_unavailable", "Workflow policy is unavailable.")
		return
	}
	out := []WorkflowRun{}
	for _, work := range works {
		if s.authorizeWorkflow(work) == nil {
			out = append(out, work.Run)
		}
	}
	writeJSON(w, 200, map[string]any{"items": out})
}
func (s *Server) getWorkflow(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	work, e := s.Store.Workflow(r.Context(), r.PathValue("workspaceId"), r.PathValue("id"), subjectFor(principal(r)))
	if e != nil {
		storeFailure(w, e)
		return
	}
	if s.authorizeWorkflow(work) != nil {
		workflowFailure(w, ErrWorkflowAccess)
		return
	}
	writeJSON(w, 200, work.Run)
}
