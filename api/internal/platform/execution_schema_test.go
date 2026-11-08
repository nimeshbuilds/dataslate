package platform

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

func TestExecutionResponsesMatchOpenAPI(t *testing.T) {
	raw, err := os.ReadFile("../../openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err = json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	compiler := jsonschema.NewCompiler()
	if err = compiler.AddResource("contract.json", doc); err != nil {
		t.Fatal(err)
	}
	s := NewMemoryStore()
	h := executionHandler(s, executionPrincipal(), []ExecutionBinding{executionTestBinding()}, &fakeExecutionBackend{})
	r := mustRun(t, h, "schema-test-key")
	for _, tc := range []struct{ path, schema string }{{"/api/v1/status", "Status"}, {"/api/v1/workspaces/default/runs/" + r.ID, "Run"}, {"/api/v1/workspaces/default/runs/" + r.ID + "/logs", "RunLogs"}} {
		t.Run(tc.schema, func(t *testing.T) {
			schema, err := compiler.Compile("contract.json#/components/schemas/" + tc.schema)
			if err != nil {
				t.Fatal(err)
			}
			response := request(h, "GET", tc.path, "")
			if response.Code != 200 {
				t.Fatal(response.Body)
			}
			var body any
			if err = json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if err = schema.Validate(body); err != nil {
				t.Fatal(err)
			}
		})
	}
	response := request(h, "GET", "/api/v1/workspaces/default/execution-bindings", "")
	var listed struct {
		Items []json.RawMessage `json:"items"`
	}
	if err = json.Unmarshal(response.Body.Bytes(), &listed); err != nil || len(listed.Items) != 1 {
		t.Fatal("binding list shape")
	}
	schema, err := compiler.Compile("contract.json#/components/schemas/ExecutionBinding")
	if err != nil {
		t.Fatal(err)
	}
	var body any
	json.Unmarshal(listed.Items[0], &body)
	if err = schema.Validate(body); err != nil {
		t.Fatal(err)
	}
}

func TestNotebookExecutionResponsesMatchOpenAPI(t *testing.T) {
	raw, e := os.ReadFile("../../openapi.yaml")
	if e != nil {
		t.Fatal(e)
	}
	var doc map[string]any
	if e = json.Unmarshal(raw, &doc); e != nil {
		t.Fatal(e)
	}
	compiler := jsonschema.NewCompiler()
	if e = compiler.AddResource("contract.json", doc); e != nil {
		t.Fatal(e)
	}
	store := NewMemoryStore()
	run := createNotebookRun(t, store, "default")
	h := executionHandler(store, executionPrincipal(), []ExecutionBinding{notebookBinding()}, nil)
	for _, tc := range []struct{ path, schema string }{{"/api/v1/status", "Status"}, {"/api/v1/workspaces/default/runs/" + run.ID, "Run"}, {"/api/v1/workspaces/default/runs/" + run.ID + "/source", "RunSource"}} {
		schema, e := compiler.Compile("contract.json#/components/schemas/" + tc.schema)
		if e != nil {
			t.Fatal(e)
		}
		response := request(h, "GET", tc.path, "")
		if response.Code != 200 {
			t.Fatal(response.Code, response.Body)
		}
		var body any
		json.Unmarshal(response.Body.Bytes(), &body)
		if e = schema.Validate(body); e != nil {
			t.Fatal(e)
		}
	}
	source, e := store.RunSource(context.Background(), "default", run.ID, run.SubmittedBy)
	if e != nil || source.Notebook.SHA256 != run.Notebook.SHA256 {
		t.Fatal("schema response source not same immutable snapshot", e)
	}
}
