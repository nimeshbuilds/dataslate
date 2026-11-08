package platform

import (
	"errors"
	"net/http"
	"strconv"
)

func ingestionFailure(w http.ResponseWriter, err error) bool {
	switch {
	case errors.Is(err, ErrIngestionRevision):
		fail(w, 409, "ingestion_revision_conflict", "The saved pipeline revision changed. Reload before creating a new submission intent.")
	case errors.Is(err, ErrIngestionConfigImmutable):
		fail(w, 409, "ingestion_config_immutable", "Pipeline configuration is immutable. Create another pipeline with new destination and checkpoint locations.")
	case errors.Is(err, ErrIngestionLocationConflict):
		fail(w, 409, "ingestion_location_conflict", "A write location overlaps a permanent pipeline reservation. Choose new destination and checkpoint locations.")
	case errors.Is(err, ErrIngestionWriterBusy):
		fail(w, 409, "ingestion_writer_busy", "A pipeline run is active or its writer/source cleanup is unresolved. Reconcile that run before a new submission.")
	case errors.Is(err, ErrIngestionStorageDenied):
		fail(w, 403, "ingestion_storage_denied", "The pipeline locations are outside the binding's administrator-approved storage prefixes.")
	case errors.Is(err, ErrIngestionNotExecutable):
		fail(w, 422, "ingestion_not_executable", "A valid saved ingestion pipeline and matching execution profile are required.")
	default:
		return false
	}
	return true
}
func (s *Server) listIngestionPipelines(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	out, err := s.Store.IngestionPipelines(r.Context(), r.PathValue("workspaceId"))
	if err != nil {
		storeFailure(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"items": out})
}
func (s *Server) getIngestionPipeline(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	out, err := s.Store.IngestionPipeline(r.Context(), r.PathValue("workspaceId"), r.PathValue("id"))
	if err != nil {
		storeFailure(w, err)
		return
	}
	writeJSON(w, 200, out)
}
func (s *Server) saveIngestionPipeline(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, true) {
		return
	}
	var in IngestionPipelineInput
	if !decode(w, r, &in) {
		return
	}
	update := r.Method == "PUT"
	if err := in.Validate(update); err != nil {
		fail(w, 422, "invalid_ingestion_pipeline", err.Error())
		return
	}
	out, err := s.Store.SaveIngestionPipeline(r.Context(), r.PathValue("workspaceId"), r.PathValue("id"), in)
	if ingestionFailure(w, err) {
		return
	}
	if err != nil {
		storeFailure(w, err)
		return
	}
	code := 201
	if update {
		code = 200
	}
	writeJSON(w, code, out)
}
func (s *Server) deleteIngestionPipeline(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, true) {
		return
	}
	revision, err := strconv.Atoi(r.URL.Query().Get("revision"))
	if err != nil || revision < 1 {
		fail(w, 422, "invalid_revision", "A positive revision query parameter is required.")
		return
	}
	if err = s.Store.DeleteIngestionPipeline(r.Context(), r.PathValue("workspaceId"), r.PathValue("id"), revision); err != nil {
		storeFailure(w, err)
		return
	}
	w.WriteHeader(204)
}
func ingestionExecutionEnabled(c Config) bool {
	for _, b := range c.ExecutionBindings {
		if b.Kind() == FileIngestionBatch {
			return true
		}
	}
	return false
}
