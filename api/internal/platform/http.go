package platform

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Server struct {
	SearchEmbedder SearchEmbedder
	CatalogBackend CatalogBackend
	Config         Config
	Store          Store
	Auth           Authenticator
	Backend        ExecutionBackend
}

func (s *Server) Handler() http.Handler {
	if s.SearchEmbedder == nil {
		s.SearchEmbedder = &GatewaySearchEmbedder{}
	}
	if s.CatalogBackend == nil {
		s.CatalogBackend = &PolarisCatalogBackend{}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if s.Store.Ready(ctx) != nil {
			fail(w, 503, "storage_unavailable", "Storage or required schema migration is unavailable.")
			return
		}
		writeJSON(w, 200, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("GET /api/v1/auth/config", func(w http.ResponseWriter, r *http.Request) {
		v := map[string]any{"mode": s.Config.AuthMode}
		if s.Config.AuthMode == "oidc" {
			v["issuer"] = s.Config.Issuer
			v["audience"] = s.Config.Audience
			v["clientId"] = s.Config.ClientID
			v["scopes"] = s.Config.Scopes
		}
		writeJSON(w, 200, v)
	})
	api := http.NewServeMux()
	api.HandleFunc("GET /api/v1/status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"product": "DataSlate", "assistant": "Sylon", "phase": "foundation", "authMode": s.Config.AuthMode, "persistence": s.Store.Mode(), "executionEnabled": len(s.Config.ExecutionBindings) > 0, "notebookExecutionEnabled": notebookExecutionEnabled(s.Config), "ingestionExecutionEnabled": ingestionExecutionEnabled(s.Config), "sqlExecutionEnabled": len(s.Config.SQLBindings) > 0, "executionMode": executionMode(s.Config)})
	})
	api.HandleFunc("GET /api/v1/modules", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]any{"items": Modules()}) })
	api.HandleFunc("POST /api/v1/deployment-plans", func(w http.ResponseWriter, r *http.Request) {
		var in PlanInput
		if !decode(w, r, &in) {
			return
		}
		out, err := CompilePlan(in)
		if err != nil {
			fail(w, 422, "invalid_plan", err.Error())
			return
		}
		writeJSON(w, 200, out)
	})
	api.HandleFunc("GET /api/v1/workspaces", s.listWorkspaces)
	api.HandleFunc("POST /api/v1/workspaces", s.createWorkspace)
	api.HandleFunc("GET /api/v1/workspaces/{workspaceId}/notebooks", s.listNotebooks)
	api.HandleFunc("POST /api/v1/workspaces/{workspaceId}/notebooks", s.saveNotebook)
	api.HandleFunc("GET /api/v1/workspaces/{workspaceId}/notebooks/{id}", s.getNotebook)
	api.HandleFunc("PUT /api/v1/workspaces/{workspaceId}/notebooks/{id}", s.saveNotebook)
	api.HandleFunc("DELETE /api/v1/workspaces/{workspaceId}/notebooks/{id}", s.deleteNotebook)
	api.HandleFunc("GET /api/v1/workspaces/{workspaceId}/notebooks/{id}/revisions", s.revisions)
	api.HandleFunc("GET /api/v1/workspaces/{workspaceId}/ingestion-pipelines", s.listIngestionPipelines)
	api.HandleFunc("POST /api/v1/workspaces/{workspaceId}/ingestion-pipelines", s.saveIngestionPipeline)
	api.HandleFunc("GET /api/v1/workspaces/{workspaceId}/ingestion-pipelines/{id}", s.getIngestionPipeline)
	api.HandleFunc("PUT /api/v1/workspaces/{workspaceId}/ingestion-pipelines/{id}", s.saveIngestionPipeline)
	api.HandleFunc("DELETE /api/v1/workspaces/{workspaceId}/ingestion-pipelines/{id}", s.deleteIngestionPipeline)
	api.HandleFunc("POST /api/v1/workspaces/{workspaceId}/spark-applications/preview", s.sparkPreview)
	api.HandleFunc("GET /api/v1/workspaces/{workspaceId}/execution-bindings", s.executionBindings)
	api.HandleFunc("POST /api/v1/workspaces/{workspaceId}/runs", s.createRun)
	api.HandleFunc("GET /api/v1/workspaces/{workspaceId}/runs", s.listRuns)
	api.HandleFunc("GET /api/v1/workspaces/{workspaceId}/runs/{id}", s.getRun)
	api.HandleFunc("POST /api/v1/workspaces/{workspaceId}/runs/{id}/cancel", s.cancelRun)
	api.HandleFunc("GET /api/v1/workspaces/{workspaceId}/runs/{id}/events", s.runEvents)
	api.HandleFunc("GET /api/v1/workspaces/{workspaceId}/runs/{id}/logs", s.runLogs)
	api.HandleFunc("GET /api/v1/workspaces/{workspaceId}/runs/{id}/source", s.runSource)

	api.HandleFunc("GET /api/v1/workspaces/{workspaceId}/search/status", s.searchStatus)
	api.HandleFunc("POST /api/v1/workspaces/{workspaceId}/search", s.search)
	api.HandleFunc("POST /api/v1/workspaces/{workspaceId}/search/index", s.createSearchIndex)
	api.HandleFunc("GET /api/v1/workspaces/{workspaceId}/search/index-jobs/{id}", s.searchIndexJob)
	api.HandleFunc("GET /api/v1/workspaces/{workspaceId}/catalog-bindings", s.catalogBindings)
	api.HandleFunc("POST /api/v1/workspaces/{workspaceId}/catalog-bindings/{id}/verify", s.verifyCatalog)
	api.HandleFunc("GET /api/v1/workspaces/{workspaceId}/catalog-bindings/{id}/namespaces", s.catalogNamespaces)
	api.HandleFunc("GET /api/v1/workspaces/{workspaceId}/catalog-bindings/{id}/tables", s.catalogTables)
	api.HandleFunc("POST /api/v1/workspaces/{workspaceId}/catalog-bindings/{id}/table", s.catalogTable)
	api.HandleFunc("GET /api/v1/workspaces/{workspaceId}/sql-bindings", s.sqlBindings)
	api.HandleFunc("GET /api/v1/workspaces/{workspaceId}/queries", s.listQueries)
	api.HandleFunc("POST /api/v1/workspaces/{workspaceId}/queries", s.createQuery)
	api.HandleFunc("GET /api/v1/workspaces/{workspaceId}/queries/{id}", s.getQuery)
	api.HandleFunc("POST /api/v1/workspaces/{workspaceId}/queries/{id}/cancel", s.cancelQuery)
	api.HandleFunc("GET /api/v1/workspaces/{workspaceId}/queries/{id}/source", s.querySource)
	api.HandleFunc("GET /api/v1/workspaces/{workspaceId}/queries/{id}/result", s.queryResult)
	api.HandleFunc("GET /api/v1/workspaces/{workspaceId}/sylon/bindings", s.sylonBindings)
	api.HandleFunc("GET /api/v1/workspaces/{workspaceId}/sylon/conversations", s.listSylonConversations)
	api.HandleFunc("POST /api/v1/workspaces/{workspaceId}/sylon/conversations", s.createSylonConversation)
	api.HandleFunc("GET /api/v1/workspaces/{workspaceId}/sylon/conversations/{id}", s.getSylonConversation)
	api.HandleFunc("DELETE /api/v1/workspaces/{workspaceId}/sylon/conversations/{id}", s.deleteSylonConversation)
	api.HandleFunc("POST /api/v1/workspaces/{workspaceId}/sylon/conversations/{id}/messages", s.createSylonMessage)
	api.HandleFunc("GET /api/v1/workspaces/{workspaceId}/sylon/jobs/{id}", s.getSylonJob)
	api.HandleFunc("POST /api/v1/workspaces/{workspaceId}/sylon/jobs/{id}/cancel", s.cancelSylonJob)
	api.HandleFunc("POST /api/v1/workspaces/{workspaceId}/sylon/jobs/{id}/apply", s.applySylonCode)
	api.HandleFunc("POST /api/v1/workspaces/{workspaceId}/sylon/messages", s.unavailable)
	api.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		fail(w, 404, "not_found", "API route is not implemented.")
	})
	mux.Handle("/api/", s.authenticate(api))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { fail(w, 404, "not_found", "Route not found.") })
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.Config.AuthMode == "development" && !loopbackHost(r.Host) {
			fail(w, 403, "development_host_denied", "Development mode only accepts loopback Host values.")
			return
		}
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
		if r.Method != "GET" && r.Method != "HEAD" {
			// Bearer auth is not cookie-based. These guards additionally protect explicit development mode against cross-origin browser writes.
			if r.Header.Get("Sec-Fetch-Site") == "cross-site" {
				fail(w, 403, "cross_origin_denied", "Cross-origin mutations are not supported.")
				return
			}
			if raw := r.Header.Get("Origin"); raw != "" {
				origin, err := url.Parse(raw)
				allowedHost := err == nil && origin.Host == r.Host
				if s.Config.AuthMode == "development" && err == nil {
					allowedHost = loopbackHost(origin.Host)
				}
				if err != nil || !allowedHost || (origin.Scheme != "http" && origin.Scheme != "https") {
					fail(w, 403, "cross_origin_denied", "Cross-origin mutations are not supported.")
					return
				}
			}
		}
		ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		defer cancel()
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}

type principalKey struct{}

func principal(r *http.Request) Principal { return r.Context().Value(principalKey{}).(Principal) }
func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, err := s.Auth.Authenticate(r.Context(), r.Header.Get("Authorization"))
		if err != nil {
			w.Header().Set("WWW-Authenticate", `Bearer realm="dataslate"`)
			fail(w, 401, "unauthorized", "A valid API access token is required.")
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), principalKey{}, p)))
	})
}
func (s *Server) workspace(w http.ResponseWriter, r *http.Request, write bool) bool {
	ws := r.PathValue("workspaceId")
	if !identifier.MatchString(ws) {
		fail(w, 400, "invalid_id", "Invalid workspace identifier.")
		return false
	}
	if id := r.PathValue("id"); id != "" && !identifier.MatchString(id) {
		fail(w, 400, "invalid_id", "Invalid notebook identifier.")
		return false
	}
	if !principal(r).Can(ws, write) {
		fail(w, 403, "forbidden", "Workspace membership with sufficient permission is required.")
		return false
	}
	if _, err := s.Store.Workspace(r.Context(), ws); err != nil {
		storeFailure(w, err)
		return false
	}
	return true
}
func (s *Server) listWorkspaces(w http.ResponseWriter, r *http.Request) {
	all, err := s.Store.Workspaces(r.Context())
	if err != nil {
		storeFailure(w, err)
		return
	}
	out := []Workspace{}
	p := principal(r)
	for _, v := range all {
		if p.Can(v.ID, false) {
			out = append(out, v)
		}
	}
	writeJSON(w, 200, map[string]any{"items": out})
}
func (s *Server) createWorkspace(w http.ResponseWriter, r *http.Request) {
	if !principal(r).PlatformAdmin {
		fail(w, 403, "forbidden", "Platform administrator role is required.")
		return
	}
	var in struct {
		Name string `json:"name"`
		Slug string `json:"slug"`
	}
	if !decode(w, r, &in) {
		return
	}
	if strings.TrimSpace(in.Name) == "" || len(in.Name) > 200 || !dnsLabel.MatchString(in.Slug) {
		fail(w, 422, "invalid_workspace", "Provide a name of 1–200 bytes and a DNS-label slug.")
		return
	}
	out, err := s.Store.CreateWorkspace(r.Context(), in.Name, in.Slug)
	if err != nil {
		storeFailure(w, err)
		return
	}
	writeJSON(w, 201, out)
}
func (s *Server) listNotebooks(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	out, err := s.Store.Notebooks(r.Context(), r.PathValue("workspaceId"))
	if err != nil {
		storeFailure(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"items": out})
}
func (s *Server) getNotebook(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	out, err := s.Store.Notebook(r.Context(), r.PathValue("workspaceId"), r.PathValue("id"))
	if err != nil {
		storeFailure(w, err)
		return
	}
	writeJSON(w, 200, out)
}
func (s *Server) saveNotebook(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, true) {
		return
	}
	var in NotebookInput
	if !decode(w, r, &in) {
		return
	}
	update := r.Method == "PUT"
	if err := in.Validate(update); err != nil {
		fail(w, 422, "invalid_notebook", err.Error())
		return
	}
	out, err := s.Store.SaveNotebook(r.Context(), r.PathValue("workspaceId"), r.PathValue("id"), in)
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
func (s *Server) deleteNotebook(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, true) {
		return
	}
	rev, err := strconv.Atoi(r.URL.Query().Get("revision"))
	if err != nil || rev < 1 {
		fail(w, 422, "invalid_revision", "A positive revision query parameter is required.")
		return
	}
	if err = s.Store.DeleteNotebook(r.Context(), r.PathValue("workspaceId"), r.PathValue("id"), rev); err != nil {
		storeFailure(w, err)
		return
	}
	w.WriteHeader(204)
}
func (s *Server) revisions(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	out, err := s.Store.Revisions(r.Context(), r.PathValue("workspaceId"), r.PathValue("id"))
	if err != nil {
		storeFailure(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"items": out})
}
func (s *Server) sparkPreview(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, true) {
		return
	}
	var in SparkInput
	if !decode(w, r, &in) {
		return
	}
	out, err := CompileSpark(in, r.PathValue("workspaceId"))
	if err != nil {
		fail(w, 422, "invalid_spark_application", err.Error())
		return
	}
	writeJSON(w, 200, out)
}
func (s *Server) unavailable(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, true) {
		return
	}
	fail(w, 501, "capability_unavailable", "Execution is not enabled in this foundation. No engine or model request was made.")
}
func decode(w http.ResponseWriter, r *http.Request, out any) bool {
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		fail(w, 415, "unsupported_media_type", "Content-Type must be application/json.")
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1024*1024)
	defer r.Body.Close()
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err = d.Decode(out); err != nil {
		var max *http.MaxBytesError
		if errors.As(err, &max) {
			fail(w, 413, "body_too_large", "Request exceeds 1 MiB.")
		} else {
			fail(w, 400, "invalid_json", "Invalid JSON body or unsupported field.")
		}
		return false
	}
	if err = d.Decode(&struct{}{}); err != io.EOF {
		fail(w, 400, "invalid_json", "Only one JSON document is accepted.")
		return false
	}
	return true
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func fail(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}
func storeFailure(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrNotFound):
		fail(w, 404, "not_found", "Resource not found in this workspace.")
	case errors.Is(err, ErrConflict):
		fail(w, 409, "conflict", "Resource changed or the unique name is already in use. Reload before retrying.")
	default:
		fail(w, 503, "storage_unavailable", "Storage operation is unavailable. Retry after checking service health.")
	}
}

func loopbackHost(authority string) bool {
	host := authority
	if h, _, err := net.SplitHostPort(authority); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func executionMode(c Config) string {
	if len(c.ExecutionBindings) > 0 {
		return "approved-batch"
	}
	return "disabled"
}
