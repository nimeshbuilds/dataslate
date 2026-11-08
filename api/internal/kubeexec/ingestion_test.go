package kubeexec

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"

	"dataslate.local/api/internal/platform"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

func ingestionExampleRun() platform.Run {
	r := notebookExampleRun()
	r.Profile.ExecutionKind = platform.FileIngestionBatch
	r.Profile.IngestionStorage = &platform.IngestionStorage{ReadPrefixes: []string{"s3://customer-lake/raw/"}, WritePrefixes: []string{"s3://customer-lake/delta/", "s3://customer-lake/checkpoints/"}}
	p := platform.IngestionProvenance{PipelineSelection: platform.PipelineSelection{ID: "pipe-example", Revision: 1}, SHA256: r.Notebook.SHA256, Bytes: r.Notebook.Bytes, SourceURI: "s3://customer-lake/raw/orders/", DestinationURI: "s3://customer-lake/delta/orders/", CheckpointURI: "s3://customer-lake/checkpoints/orders/"}
	r.Notebook = nil
	r.Ingestion = &p
	r.Source.Notebook = platform.NotebookProvenance{}
	r.Source.Ingestion = &p
	return r
}
func TestIngestionManifestSourceAndStablePodLabels(t *testing.T) {
	r := ingestionExampleRun()
	c := &Client{namespaces: map[string]bool{"execution-a": true}}
	app, e := c.manifest(r)
	if e != nil {
		t.Fatal(e)
	}
	raw, e := os.ReadFile("../platform/testdata/apache-sparkapplication-schema.json")
	if e != nil {
		t.Fatal(e)
	}
	var doc any
	json.Unmarshal(raw, &doc)
	compiler := jsonschema.NewCompiler()
	compiler.AddResource("apache.json", doc)
	schema, e := compiler.Compile("apache.json")
	if e != nil {
		t.Fatal(e)
	}
	data, _ := json.Marshal(app)
	var value any
	json.Unmarshal(data, &value)
	if e = schema.Validate(value); e != nil {
		t.Fatal(e)
	}
	conf := app.Spec["sparkConf"].(map[string]any)
	for _, role := range []string{"driver", "executor"} {
		if conf["spark.kubernetes."+role+".label.dataslate.io/run"] != r.ID {
			t.Fatal("run identity absent in Spark labels")
		}
		template := app.Spec[role+"Spec"].(map[string]any)["podTemplateSpec"].(map[string]any)
		if template["metadata"].(map[string]any)["labels"].(map[string]any)["dataslate.io/run"] != r.ID {
			t.Fatal("run identity absent in pod template")
		}
	}
	cm, e := c.sourceManifest(r)
	if e != nil || cm.Metadata.Annotations["dataslate.io/source-sha256"] != r.Ingestion.SHA256 || cm.Data["source.py"] != r.Source.Source {
		t.Fatal("ingestion source failed generic lifecycle", e)
	}
	bindings := []platform.ExecutionBinding{{ID: r.BindingID, WorkspaceID: r.WorkspaceID, Name: "Ingestion", ExecutionProfile: r.Profile, AllowedSubjects: []platform.ExecutionSubject{r.SubmittedBy}}}
	if e = c.ValidateBindings(bindings); e != nil {
		t.Fatal(e)
	}
}
func TestIngestionDrainRequiresUIDDeletionAndAllPodAbsence(t *testing.T) {
	for _, tc := range []struct {
		name                                                      string
		known, submitted, app, pods, paged, replacement, failList bool
		wantReleased                                              bool
		wantDelete                                                int
	}{
		{name: "never submitted", wantReleased: true},
		{name: "uncertain POST", submitted: true},
		{name: "terminal app", known: true, submitted: true, app: true, wantReleased: true, wantDelete: 1},
		{name: "remaining executor", known: true, submitted: true, app: true, pods: true, wantDelete: 1},
		{name: "truncated list", known: true, submitted: true, paged: true},
		{name: "replacement UID", known: true, submitted: true, app: true, replacement: true},
		{name: "list unavailable", known: true, submitted: true, failList: true},
		{name: "unexpected app", app: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := ingestionExampleRun()
			r.State = "succeeded"
			r.SubmissionStarted = tc.submitted
			if tc.known {
				r.KubernetesUID = "original-uid"
			}
			compiler := &Client{namespaces: map[string]bool{"execution-a": true}}
			app, e := compiler.manifest(r)
			if e != nil {
				t.Fatal(e)
			}
			app.Metadata.UID = "original-uid"
			if tc.replacement {
				app.Metadata.UID = "replacement-uid"
			}
			exists := tc.app
			deletes := 0
			c, _ := testClient(t, func(w http.ResponseWriter, request *http.Request) {
				if strings.HasSuffix(request.URL.Path, "/pods") {
					if request.Method != "GET" || request.URL.Query().Get("labelSelector") != "dataslate.io/run="+r.ID {
						t.Error("unsafe pod selection")
					}
					if tc.failList {
						w.WriteHeader(503)
						return
					}
					items := []any{}
					if tc.pods {
						items = append(items, map[string]any{"metadata": map[string]any{"name": "foreign-or-owned-pod"}})
					}
					next := ""
					if tc.paged {
						next = "opaque-next-page"
					}
					writeJSON(t, w, 200, map[string]any{"items": items, "metadata": map[string]string{"continue": next}})
					return
				}
				if request.Method == "DELETE" {
					deletes++
					var input struct {
						PropagationPolicy string            `json:"propagationPolicy"`
						Preconditions     map[string]string `json:"preconditions"`
					}
					json.NewDecoder(request.Body).Decode(&input)
					if input.PropagationPolicy != "Foreground" || input.Preconditions["uid"] != r.KubernetesUID {
						t.Error("unsafe app deletion")
					}
					exists = false
					w.WriteHeader(200)
					return
				}
				if !exists {
					w.WriteHeader(404)
					return
				}
				writeJSON(t, w, 200, app)
			})
			released, _ := c.DrainIngestion(context.Background(), r)
			if released != tc.wantReleased || deletes != tc.wantDelete {
				t.Fatal("incorrect drain", released, deletes)
			}
		})
	}
}
func TestIngestionDrainRechecksApplicationAfterPodAbsence(t *testing.T) {
	r := ingestionExampleRun()
	r.State = "succeeded"
	r.KubernetesUID = "original-uid"
	r.SubmissionStarted = true
	reads := 0
	c, _ := testClient(t, func(w http.ResponseWriter, request *http.Request) {
		if strings.HasSuffix(request.URL.Path, "/pods") {
			writeJSON(t, w, 200, map[string]any{"items": []any{}})
			return
		}
		reads++
		if reads == 1 {
			w.WriteHeader(404)
		} else {
			w.WriteHeader(503)
		}
	})
	if released, e := c.DrainIngestion(context.Background(), r); e == nil || released {
		t.Fatal("failed app recheck released writer")
	}
}
