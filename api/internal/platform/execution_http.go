package platform

import (
	"errors"
	"net/http"
)

func (s *Server) executionBindings(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	out := []map[string]any{}
	p := principal(r)
	subject := subjectFor(p)
	if p.Can(r.PathValue("workspaceId"), true) {
		for _, b := range s.Config.ExecutionBindings {
			if b.WorkspaceID == r.PathValue("workspaceId") && b.Allows(subject) {
				item := map[string]any{"id": b.ID, "workspaceId": b.WorkspaceID, "name": b.Name, "operator": b.Operator, "image": b.Image, "mainApplicationFile": b.MainApplicationFile, "executionKind": b.Kind(), "namespace": b.Namespace, "serviceAccount": b.ServiceAccount, "executorInstances": b.ExecutorInstances, "maxRunSeconds": b.RunLimitSeconds(), "ingestionStorage": b.IngestionStorage}
				if b.IngestionStorage == nil {
					delete(item, "ingestionStorage")
				}
				out = append(out, item)
			}
		}
	}
	writeJSON(w, 200, map[string]any{"items": out})
}
func (s *Server) createRun(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, true) {
		return
	}
	if len(s.Config.ExecutionBindings) == 0 {
		fail(w, 501, "capability_unavailable", "Approved batch execution is not configured. No engine request was made.")
		return
	}
	var in RunInput
	if !decode(w, r, &in) {
		return
	}
	if err := in.Validate(); err != nil {
		fail(w, 422, "invalid_run", err.Error())
		return
	}
	subject := subjectFor(principal(r))
	var binding *ExecutionBinding
	for _, b := range s.Config.ExecutionBindings {
		if b.ID == in.BindingID && b.WorkspaceID == r.PathValue("workspaceId") && b.Allows(subject) {
			binding = &b
			break
		}
	}
	if binding == nil {
		fail(w, 403, "execution_denied", "An approved execution binding and explicit execution grant are required.")
		return
	}
	if err := binding.Validate(); err != nil {
		fail(w, 503, "execution_configuration_invalid", "Execution binding configuration is invalid.")
		return
	}
	if (binding.Kind() == FileIngestionBatch) != (in.Pipeline != nil) {
		fail(w, 422, "ingestion_not_executable", "Ingestion bindings require a saved pipeline selection; other bindings do not accept pipelines.")
		return
	}
	if (binding.Kind() == NotebookPythonBatch) != (in.Notebook != nil) {
		fail(w, 422, "notebook_not_executable", "Notebook bindings require a saved Python notebook selection; approved application bindings do not accept notebooks.")
		return
	}
	run := newRun(r.PathValue("workspaceId"), *binding, subject)
	if in.Pipeline != nil {
		run.Ingestion = &IngestionProvenance{PipelineSelection: *in.Pipeline}
	}
	if in.Notebook != nil {
		run.Notebook = &NotebookProvenance{NotebookSelection: *in.Notebook}
	}
	out, replay, err := s.Store.CreateRun(r.Context(), run, in.IdempotencyKey, runFingerprint(in))
	if ingestionFailure(w, err) {
		return
	}
	if errors.Is(err, ErrNotebookRevision) {
		fail(w, 409, "notebook_revision_conflict", "The saved notebook revision changed. Save or reload and create a new submission intent.")
		return
	}
	if errors.Is(err, ErrNotebookNotExecutable) {
		fail(w, 422, "notebook_not_executable", "Batch execution requires a nonempty all-Python notebook with generated UTF-8 source no larger than 256 KiB.")
		return
	}
	if errors.Is(err, ErrRunCapacity) {
		fail(w, 429, "run_capacity_exceeded", "This workspace has 100 active or unresolved runs. Wait for completion or investigate unresolved runs before submitting new work.")
		return
	}
	if err != nil {
		storeFailure(w, err)
		return
	}
	status := 202
	if replay {
		status = 200
	}
	writeJSON(w, status, out)
}
func (s *Server) listRuns(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	out, err := s.Store.Runs(r.Context(), r.PathValue("workspaceId"), subjectFor(principal(r)))
	if err != nil {
		storeFailure(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"items": out})
}
func (s *Server) getRun(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	out, err := s.Store.Run(r.Context(), r.PathValue("workspaceId"), r.PathValue("id"), subjectFor(principal(r)))
	if err != nil {
		storeFailure(w, err)
		return
	}
	writeJSON(w, 200, out)
}
func (s *Server) cancelRun(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, true) {
		return
	}
	var in struct{}
	if !decode(w, r, &in) {
		return
	}
	out, err := s.Store.CancelRun(r.Context(), r.PathValue("workspaceId"), r.PathValue("id"), subjectFor(principal(r)))
	if err != nil {
		storeFailure(w, err)
		return
	}
	writeJSON(w, 202, out)
}
func (s *Server) runEvents(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	out, err := s.Store.Run(r.Context(), r.PathValue("workspaceId"), r.PathValue("id"), subjectFor(principal(r)))
	if err != nil {
		storeFailure(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"items": out.Events, "truncated": out.EventsTruncated})
}
func (s *Server) runLogs(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	out, err := s.Store.Run(r.Context(), r.PathValue("workspaceId"), r.PathValue("id"), subjectFor(principal(r)))
	if err != nil {
		storeFailure(w, err)
		return
	}
	if s.Backend == nil {
		fail(w, 501, "capability_unavailable", "Backend logs are not configured.")
		return
	}
	logs, err := s.Backend.Logs(r.Context(), out)
	if errors.Is(err, ErrCapabilityUnavailable) {
		fail(w, 501, "capability_unavailable", "Driver logs are not currently available.")
		return
	}
	if err != nil {
		fail(w, 503, "backend_unavailable", "Backend logs could not be verified or retrieved.")
		return
	}
	if len(logs.Text) > 1024*1024 {
		logs.Text = logs.Text[len(logs.Text)-1024*1024:]
		logs.Truncated = true
	}
	writeJSON(w, 200, logs)
}

func (s *Server) runSource(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	out, err := s.Store.RunSource(r.Context(), r.PathValue("workspaceId"), r.PathValue("id"), subjectFor(principal(r)))
	if err != nil {
		storeFailure(w, err)
		return
	}
	writeJSON(w, 200, out)
}
func notebookExecutionEnabled(c Config) bool {
	for _, b := range c.ExecutionBindings {
		if b.Kind() == NotebookPythonBatch {
			return true
		}
	}
	return false
}
