package platform

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Opt-in: only use a task-owned Airflow fixture with a harmless immutable DAG.
func TestWorkflowNativeAirflow(t *testing.T) {
	path := os.Getenv("DATASLATE_TEST_AIRFLOW_FIXTURE")
	if path == "" {
		t.Skip("task-owned Airflow 3.4 versioned-bundle fixture required")
	}
	data, e := os.ReadFile(path)
	if e != nil {
		t.Fatal("fixture unavailable")
	}
	var f struct{ Endpoint, CAFile, TokenFile, DAGID, BundleVersion string }
	if json.Unmarshal(data, &f) != nil {
		t.Fatal("invalid fixture")
	}
	b := workflowTestBinding("default")
	b.Endpoint = f.Endpoint
	b.CAFile = f.CAFile
	b.TokenFile = f.TokenFile
	b.DAGID = f.DAGID
	b.BundleVersion = f.BundleVersion
	b.MaxRunSeconds = 120
	config := filepath.Join(t.TempDir(), "bindings.json")
	data, _ = json.Marshal([]WorkflowBinding{b})
	if e = os.WriteFile(config, data, 0600); e != nil {
		t.Fatal(e)
	}
	backend := &FileWorkflowBackend{BindingsFile: config}
	engine, e := backend.Open(context.Background(), b)
	if e != nil {
		t.Fatal(e)
	}
	defer engine.Close()
	definition, e := engine.Definition(context.Background(), b.Target())
	if e != nil {
		t.Fatal("native definition", e)
	}
	if !workflowDefinition(b, definition).CanTrigger || len(definition.Tasks) != 3 {
		t.Fatal("native DAG admission/graph did not match")
	}
	history, _, e := engine.History(context.Background(), b.Target())
	if e != nil {
		t.Fatal("native history", e)
	}
	scheduled := false
	for _, run := range history {
		scheduled = scheduled || run.RunType == "scheduled"
	}
	if !scheduled {
		t.Fatal("native scheduled history missing")
	}
	store := NewMemoryStore()
	server := &Server{Store: store, Auth: testAuth{p: executionPrincipal()}, Config: Config{AuthMode: "oidc", WorkflowBindingsFile: config}, WorkflowBackend: backend}
	response := request(server.Handler(), "POST", "/api/v1/workspaces/default/workflow-runs", workflowInputJSON(t, workflowReviewedInput(b, "native-airflow-original")))
	if response.Code != 202 {
		t.Fatal("native reviewed submission", response.Code, response.Body)
	}
	var accepted struct{ Run WorkflowRun }
	if json.Unmarshal(response.Body.Bytes(), &accepted) != nil {
		t.Fatal("native receipt unavailable")
	}
	work, e := store.Workflow(context.Background(), b.WorkspaceID, accepted.Run.ID, b.AllowedSubjects[0])
	if e != nil {
		t.Fatal(e)
	}
	deadline := time.Now().Add(100 * time.Second)
	for time.Now().Before(deadline) {
		workflowDue(t, store, work.Run.ID)
		if _, e = ReconcileWorkflowOnce(context.Background(), store, backend, "native-worker"); e != nil {
			t.Fatal(e)
		}
		work = readWorkflow(t, store, work)
		if work.Run.ReconciliationComplete {
			break
		}
		time.Sleep(time.Second)
	}
	if work.Run.State != "succeeded" || !work.Run.ReconciliationComplete || len(work.Run.Tasks) != 3 || work.Run.TasksTruncated {
		t.Fatalf("native run did not reach verified success: state=%s final=%v tasks=%d", work.Run.State, work.Run.ReconciliationComplete, len(work.Run.Tasks))
	}
	for _, task := range work.Run.Tasks {
		if task.State != "success" {
			t.Fatal("native task incomplete")
		}
	}
	t.Log("Real Airflow 3.4.0: TLS/bearer, pinned versioned Git bundle, schedule/graph/history, durable single trigger, receipt readback and three completed tasks verified.")
}

// Exposes real handlers and PostgreSQL for an opt-in browser qualification. The
// only test substitute is loopback development authentication; engine work is real.
func TestWorkflowNativeBrowserHarness(t *testing.T) {
	path := os.Getenv("DATASLATE_TEST_WORKFLOW_BROWSER_CONFIG")
	if path == "" {
		t.Skip("explicit owned browser fixture required")
	}
	data, e := os.ReadFile(path)
	if e != nil {
		t.Fatal("browser fixture unavailable")
	}
	var f struct{ RuntimeURL, BindingsFile, StopFile string }
	if json.Unmarshal(data, &f) != nil {
		t.Fatal("invalid browser fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	db, e := OpenPostgres(ctx, f.RuntimeURL, true)
	if e != nil {
		t.Fatal("browser database connection failed")
	}
	defer db.Close()
	if db.Ready(ctx) != nil {
		t.Fatal("browser schema unavailable")
	}
	bindings, e := LoadWorkflowBindings(f.BindingsFile)
	if e != nil {
		t.Fatal(e)
	}
	api := &Server{Store: db, Auth: DevelopmentAuth{}, Config: Config{AuthMode: "development", WorkflowBindingsFile: f.BindingsFile, WorkflowBindings: bindings}}
	listener, e := net.Listen("tcp", "127.0.0.1:8080")
	if e != nil {
		t.Fatal("loopback API port unavailable")
	}
	server := &http.Server{Handler: api.Handler(), ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 30 * time.Second}
	defer server.Close()
	go server.Serve(listener)
	t.Log("Owned loopback API handlers ready on port8080 with TLS PostgreSQL and native Airflow.")
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			t.Fatal("browser harness timed out")
		case <-ticker.C:
			if _, e = os.Stat(f.StopFile); e == nil {
				return
			}
		}
	}
}
