package kubeexec

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"dataslate.local/api/internal/platform"
)

func notebookExampleRun() platform.Run {
	r := exampleRun()
	r.Profile.ExecutionKind = platform.NotebookPythonBatch
	r.Profile.MainApplicationFile = ""
	r.SubmissionStarted = false
	code := "print('immutable source')\n"
	hash := sha256.Sum256([]byte(code))
	p := platform.NotebookProvenance{NotebookSelection: platform.NotebookSelection{ID: "nb-snapshot", Revision: 3}, SHA256: hex.EncodeToString(hash[:]), CellIDs: []string{"cell-one"}, Bytes: len(code)}
	r.Notebook = &p
	r.Source = &platform.RunSource{Source: code, Notebook: p}
	r.SourceArtifact = &platform.SourceArtifact{Name: r.KubernetesName + "-source", State: "pending", CreationStarted: true}
	return r
}

type sourceAPI struct {
	mu                                                    sync.Mutex
	cm                                                    *sourceConfigMap
	app                                                   *application
	sourcePosts, sourceDeletes, sourcePatches, sparkPosts int
	lostCreate, lostDelete, lostPatch                     bool
	rejectCreate                                          bool
}

func (a *sourceAPI) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		t.Helper()
		a.mu.Lock()
		defer a.mu.Unlock()
		if strings.Contains(r.URL.Path, "/configmaps") {
			switch r.Method {
			case http.MethodGet:
				if a.cm == nil {
					w.WriteHeader(404)
					return
				}
				writeJSON(t, w, 200, a.cm)
			case http.MethodPost:
				a.sourcePosts++
				if a.rejectCreate {
					w.WriteHeader(503)
					return
				}
				if a.cm != nil {
					w.WriteHeader(409)
					return
				}
				var cm sourceConfigMap
				if e := json.NewDecoder(r.Body).Decode(&cm); e != nil {
					t.Error(e)
				}
				cm.Metadata.UID = "immutable-source-uid"
				cm.Metadata.ResourceVersion = "11"
				a.cm = &cm
				if a.lostCreate {
					w.WriteHeader(503)
					return
				}
				writeJSON(t, w, 201, a.cm)
			case http.MethodPatch:
				a.sourcePatches++
				if r.Header.Get("Content-Type") != "application/json-patch+json" {
					t.Error("incorrect patch media type")
				}
				if a.cm == nil {
					w.WriteHeader(404)
					return
				}
				var patch []struct {
					Op, Path string
					Value    json.RawMessage
				}
				json.NewDecoder(r.Body).Decode(&patch)
				if len(patch) != 3 || patch[0].Op != "test" || patch[0].Path != "/metadata/uid" || patch[1].Op != "test" || patch[1].Path != "/metadata/resourceVersion" || patch[2].Op != "add" || patch[2].Path != "/metadata/ownerReferences" {
					t.Error("missing identity/resource version patch preconditions")
				}
				var uid, rv string
				json.Unmarshal(patch[0].Value, &uid)
				json.Unmarshal(patch[1].Value, &rv)
				if uid != a.cm.Metadata.UID || rv != a.cm.Metadata.ResourceVersion {
					w.WriteHeader(409)
					return
				}
				json.Unmarshal(patch[2].Value, &a.cm.Metadata.OwnerReferences)
				a.cm.Metadata.ResourceVersion = "12"
				if a.lostPatch {
					w.WriteHeader(503)
					return
				}
				writeJSON(t, w, 200, a.cm)
			case http.MethodDelete:
				a.sourceDeletes++
				if a.cm == nil {
					w.WriteHeader(404)
					return
				}
				var input struct{ Preconditions map[string]string }
				json.NewDecoder(r.Body).Decode(&input)
				if input.Preconditions["uid"] != a.cm.Metadata.UID || input.Preconditions["resourceVersion"] != a.cm.Metadata.ResourceVersion {
					t.Error("unsafe source deletion")
					w.WriteHeader(409)
					return
				}
				a.cm = nil
				if a.lostDelete {
					w.WriteHeader(503)
					return
				}
				writeJSON(t, w, 200, map[string]any{"status": "Success"})
			default:
				t.Error("unexpected source operation", r.Method)
				w.WriteHeader(405)
			}
			return
		}
		if strings.Contains(r.URL.Path, "/sparkapplications") {
			switch r.Method {
			case http.MethodGet:
				if a.app == nil {
					w.WriteHeader(404)
					return
				}
				writeJSON(t, w, 200, a.app)
			case http.MethodPost:
				a.sparkPosts++
				var app application
				json.NewDecoder(r.Body).Decode(&app)
				app.Metadata.UID = "application-uid"
				app.Status.Current.Summary = "RunningHealthy"
				a.app = &app
				writeJSON(t, w, 201, a.app)
			default:
				t.Error("unexpected Spark operation", r.Method)
				w.WriteHeader(405)
			}
			return
		}
		t.Error("unexpected path", r.URL.Path)
		w.WriteHeader(404)
	}
}
func TestNotebookSourceImmutableLifecycleAndUnknownResponses(t *testing.T) {
	for _, mode := range []string{"ordinary", "lost-create", "lost-owner-patch", "lost-delete"} {
		t.Run(mode, func(t *testing.T) {
			api := &sourceAPI{lostCreate: mode == "lost-create", lostPatch: mode == "lost-owner-patch", lostDelete: mode == "lost-delete"}
			c, _ := testClient(t, api.handler(t))
			ctx := context.Background()
			r := notebookExampleRun()
			a, e := c.PrepareSource(ctx, r)
			if e != nil || a.UID == "" || api.sourcePosts != 1 {
				t.Fatal("source creation not reconciled", a, e)
			}
			r.SourceArtifact = &a
			again, e := c.PrepareSource(ctx, r)
			if e != nil || again.UID != a.UID || api.sourcePosts != 1 {
				t.Fatal("prepared source was recreated", e)
			}
			r.SubmissionStarted = true
			o, e := c.Submit(ctx, r)
			if e != nil || !o.Exists || api.sparkPosts != 1 {
				t.Fatal("notebook Spark submission failed", e)
			}
			r.KubernetesUID = o.UID
			if e = c.BindSource(ctx, r); e != nil {
				t.Fatal("owner binding failed", e)
			}
			if e = c.BindSource(ctx, r); e != nil || api.sourcePatches != 1 {
				t.Fatal("owner binding not idempotent", e)
			}
			if api.cm.Metadata.OwnerReferences[0].BlockOwnerDeletion {
				t.Fatal("source cleanup blocks foreground Spark cancellation")
			}
			if e = c.DeleteSource(ctx, r); e == nil {
				t.Fatal("nonterminal source deletion accepted")
			}
			r.State = "succeeded"
			r.SourceArtifact.State = "cleanup_pending"
			e = c.DeleteSource(ctx, r)
			if mode == "lost-delete" {
				if e == nil {
					t.Fatal("lost deletion response claimed completion")
				}
				sourceObserved, observeErr := c.ObserveSource(ctx, r)
				if observeErr != nil || sourceObserved.Exists {
					t.Fatal("known UID absence not recoverable", observeErr)
				}
				e = c.DeleteSource(ctx, r)
			}
			if e != nil || api.cm != nil || api.sourceDeletes != 1 {
				t.Fatal("source cleanup failed", e)
			}
		})
	}
}
func TestNotebookSourceUnknownCreateNeverSubmitsSpark(t *testing.T) {
	api := &sourceAPI{rejectCreate: true}
	c, _ := testClient(t, api.handler(t))
	r := notebookExampleRun()
	ctx := context.Background()
	if _, e := c.PrepareSource(ctx, r); e == nil {
		t.Fatal("unknown create accepted")
	}
	if api.sourcePosts != 1 || api.sparkPosts != 0 {
		t.Fatal("blind create retry")
	}
	r.SubmissionStarted = true
	if _, e := c.Submit(ctx, r); e == nil || api.sparkPosts != 0 {
		t.Fatal("Spark submitted without prepared source")
	}
	r.SubmissionStarted = false
	api.rejectCreate = false
	a, e := c.PrepareSource(ctx, r)
	if e != nil || api.sourcePosts != 2 || a.UID == "" {
		t.Fatal("safe source-only retry failed", e)
	}
}
func TestNotebookSourceRejectsMutationAndReplacement(t *testing.T) {
	mutations := map[string]func(*sourceConfigMap){
		"uid":         func(c *sourceConfigMap) { c.Metadata.UID = "replacement" },
		"data":        func(c *sourceConfigMap) { c.Data["source.py"] = "print('attacker')" },
		"extra-data":  func(c *sourceConfigMap) { c.Data["unexpected"] = "x" },
		"binary-data": func(c *sourceConfigMap) { c.BinaryData = map[string]string{"hidden": "eA=="} },
		"mutable":     func(c *sourceConfigMap) { c.Immutable = false },
		"hash": func(c *sourceConfigMap) {
			c.Metadata.Annotations["dataslate.io/source-sha256"] = strings.Repeat("0", 64)
		},
		"owner-label": func(c *sourceConfigMap) { c.Metadata.Labels["dataslate.io/run"] = "another" },
		"owner-ref": func(c *sourceConfigMap) {
			c.Metadata.OwnerReferences = []ownerReference{{Kind: "SparkApplication", UID: "foreign"}}
		},
		"deleting":  func(c *sourceConfigMap) { c.Metadata.DeletionTimestamp = "2026-01-01T00:00:00Z" },
		"namespace": func(c *sourceConfigMap) { c.Metadata.Namespace = "foreign" },
	}
	for name, change := range mutations {
		t.Run(name, func(t *testing.T) {
			api := &sourceAPI{}
			c, _ := testClient(t, api.handler(t))
			ctx := context.Background()
			r := notebookExampleRun()
			a, e := c.PrepareSource(ctx, r)
			if e != nil {
				t.Fatal(e)
			}
			r.SourceArtifact = &a
			change(api.cm)
			if _, e = c.PrepareSource(ctx, r); e == nil {
				t.Fatal("mutated source adopted")
			}
			r.SubmissionStarted = true
			if _, e = c.Submit(ctx, r); e == nil || api.sparkPosts != 0 {
				t.Fatal("mutated source submitted")
			}
			r.State = "cancelled"
			if e = c.DeleteSource(ctx, r); e == nil || api.sourceDeletes != 0 {
				t.Fatal("foreign/changed source deleted")
			}
		})
	}
}
func TestNotebookSourceGoneCannotBeReplacedAfterUIDPersisted(t *testing.T) {
	api := &sourceAPI{}
	c, _ := testClient(t, api.handler(t))
	r := notebookExampleRun()
	ctx := context.Background()
	a, e := c.PrepareSource(ctx, r)
	if e != nil {
		t.Fatal(e)
	}
	r.SourceArtifact = &a
	api.cm = nil
	if _, e = c.PrepareSource(ctx, r); e == nil || api.sourcePosts != 1 {
		t.Fatal("missing immutable source recreated")
	}
}
func TestNotebookSourcePreparationPolicyRevocationAndCorruption(t *testing.T) {
	api := &sourceAPI{}
	c, _ := testClient(t, api.handler(t))
	r := notebookExampleRun()
	b := platform.ExecutionBinding{ID: r.BindingID, WorkspaceID: r.WorkspaceID, Name: "Notebook Python", ExecutionProfile: r.Profile, AllowedSubjects: []platform.ExecutionSubject{r.SubmittedBy}}
	file := filepath.Join(t.TempDir(), "bindings.json")
	raw, _ := json.Marshal([]platform.ExecutionBinding{b})
	os.WriteFile(file, raw, 0600)
	g, e := NewGuarded(c, file)
	if e != nil {
		t.Fatal(e)
	}
	os.WriteFile(file, []byte("[]"), 0600)
	if _, e = g.PrepareSource(context.Background(), r); e == nil || api.sourcePosts != 0 {
		t.Fatal("revoked grant created source")
	}
	os.WriteFile(file, raw, 0600)
	r.Source.Source += "tampered"
	if _, e = g.PrepareSource(context.Background(), r); e == nil || api.sourcePosts != 0 {
		t.Fatal("corrupted stored source created artifact")
	}
}
func TestNotebookManifestUsesOnlyDriverSourceAndPythonRunner(t *testing.T) {
	c := &Client{namespaces: map[string]bool{"execution-a": true}}
	r := notebookExampleRun()
	a, e := c.manifest(r)
	if e != nil {
		t.Fatal(e)
	}
	if a.Spec["mainClass"] != "org.apache.spark.deploy.PythonRunner" || a.Spec["pyFiles"] != platform.NotebookApplicationFile {
		t.Fatal("wrong Python entrypoint")
	}
	d := a.Spec["driverSpec"].(map[string]any)["podTemplateSpec"].(map[string]any)["spec"].(map[string]any)
	volume := d["volumes"].([]any)[0].(map[string]any)["configMap"].(map[string]any)
	if volume["name"] != r.SourceArtifact.Name {
		t.Fatal("source volume does not refer to deterministic artifact")
	}
	eSpec := a.Spec["executorSpec"].(map[string]any)["podTemplateSpec"].(map[string]any)["spec"].(map[string]any)
	if _, ok := eSpec["volumes"]; ok {
		t.Fatal("source mounted in executors")
	}
	encoded, _ := json.Marshal(a)
	if strings.Contains(string(encoded), r.Source.Source) || strings.Contains(string(encoded), "spark.submit.pyFiles") {
		t.Fatal("source copied into CR or supplemental worker dependency")
	}
}
