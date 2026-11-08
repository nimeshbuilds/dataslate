package kubeexec

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"dataslate.local/api/internal/platform"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

func exampleRun() platform.Run {
	return platform.Run{Resource: platform.Resource{ID: "run-0123456789abcdef0123456789abcdef", CreatedAt: time.Now().UTC()}, WorkspaceID: "workspace-a", BindingID: "approved-job", SubmittedBy: platform.ExecutionSubject{Issuer: "https://identity.example.com", Subject: "user-a"}, Profile: platform.ExecutionProfile{Operator: "apache", Image: "registry.example.com/etl@sha256:" + strings.Repeat("a", 64), MainApplicationFile: "local:///opt/jobs/etl.py", Namespace: "execution-a", ServiceAccount: "etl-job", ExecutorInstances: 2}, State: "submitting", SubmissionStarted: true, KubernetesName: "ds-0123456789abcdef0123456789abcdef"}
}

func TestExpiredRunCannotReachKubernetesCreate(t *testing.T) {
	requests := 0
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) { requests++; w.WriteHeader(404) })
	r := exampleRun()
	r.CreatedAt = time.Now().Add(-2 * time.Hour)
	if _, err := c.Submit(context.Background(), r); err == nil {
		t.Fatal("expired run was allowed to submit")
	}
	if requests != 0 {
		t.Fatal("expired run reached Kubernetes")
	}
}

func testClient(t *testing.T, handler http.HandlerFunc) (*Client, Config) {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.crt")
	token := filepath.Join(dir, "token")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(token, []byte("token-one"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := Config{ServerURL: server.URL, CAFile: ca, TokenFile: token, AllowedNamespaces: []string{"execution-a"}}
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c, cfg
}

func writeJSON(t *testing.T, w http.ResponseWriter, status int, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Error(err)
	}
}

func TestManifestMatchesPinnedApacheSchemaAndLimitsInputs(t *testing.T) {
	c := &Client{namespaces: map[string]bool{"execution-a": true}}
	r := exampleRun()
	a, err := c.manifest(r)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile("../platform/testdata/apache-sparkapplication-schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc any
	if err = json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	compiler := jsonschema.NewCompiler()
	if err = compiler.AddResource("apache.json", doc); err != nil {
		t.Fatal(err)
	}
	schema, err := compiler.Compile("apache.json")
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(a)
	var value any
	_ = json.Unmarshal(data, &value)
	if err = schema.Validate(value); err != nil {
		t.Fatal(err)
	}
	tolerations := a.Spec["applicationTolerations"].(map[string]any)
	if tolerations["restartConfig"].(map[string]any)["restartPolicy"] != "Never" {
		t.Fatal("whole-job retry must be disabled")
	}
	conf := a.Spec["sparkConf"].(map[string]any)
	if conf["spark.driver.port"] != "7078" || conf["spark.blockManager.port"] != "7079" || conf["spark.kubernetes.authenticate.executor.serviceAccountName"] != r.Profile.ServiceAccount {
		t.Fatal("approved networking or executor identity was not fixed")
	}
	for name, wantsToken := range map[string]bool{"driverSpec": true, "executorSpec": false} {
		pod := a.Spec[name].(map[string]any)["podTemplateSpec"].(map[string]any)["spec"].(map[string]any)
		if pod["automountServiceAccountToken"] != wantsToken {
			t.Fatal("unexpected executor/driver API credential mount")
		}
		security := pod["securityContext"].(map[string]any)
		if security["runAsUser"] != float64(185) || security["runAsGroup"] != float64(185) || security["runAsNonRoot"] != true || security["seccompProfile"].(map[string]any)["type"] != "RuntimeDefault" {
			t.Fatal("pod security defaults missing")
		}
		container := pod["containers"].([]any)[0].(map[string]any)["securityContext"].(map[string]any)
		if container["allowPrivilegeEscalation"] != false || container["capabilities"].(map[string]any)["drop"].([]any)[0] != "ALL" {
			t.Fatal("container privilege restrictions missing")
		}
	}
	for name, change := range map[string]func(*platform.Run){
		"mutable image":        func(r *platform.Run) { r.Profile.Image = "registry.example.com/etl:1" },
		"mutable source":       func(r *platform.Run) { r.Profile.MainApplicationFile = "s3a://lake/etl.py" },
		"foreign namespace":    func(r *platform.Run) { r.Profile.Namespace = "kube-system" },
		"path escape":          func(r *platform.Run) { r.Profile.MainApplicationFile = "local:///opt/../secret.py" },
		"encoded path":         func(r *platform.Run) { r.Profile.MainApplicationFile = "local:///opt/%65tl.py" },
		"unsupported operator": func(r *platform.Run) { r.Profile.Operator = "kubeflow" },
		"forged name":          func(r *platform.Run) { r.KubernetesName = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			bad := r
			change(&bad)
			if _, err := c.manifest(bad); err == nil {
				t.Fatal("unsafe profile accepted")
			}
		})
	}
}

func TestSubmissionReconcilesConflictAndLostResponseWithoutSecondCreate(t *testing.T) {
	for _, mode := range []string{"created", "conflict", "lost response"} {
		t.Run(mode, func(t *testing.T) {
			var mu sync.Mutex
			var stored *application
			creates := 0
			reads := 0
			c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				switch r.Method {
				case http.MethodGet:
					reads++
					if stored == nil {
						w.WriteHeader(404)
						return
					}
					writeJSON(t, w, 200, stored)
				case http.MethodPost:
					creates++
					var app application
					if err := json.NewDecoder(r.Body).Decode(&app); err != nil {
						t.Error(err)
					}
					app.Metadata.UID = "application-uid"
					app.Status.Current.Summary = "Submitted"
					stored = &app
					if mode == "conflict" {
						w.WriteHeader(409)
						return
					}
					if mode == "lost response" {
						conn, _, err := w.(http.Hijacker).Hijack()
						if err != nil {
							t.Error(err)
						} else {
							_ = conn.Close()
						}
						return
					}
					writeJSON(t, w, 201, stored)
				default:
					t.Errorf("unexpected method %s", r.Method)
				}
			})
			r := exampleRun()
			o, err := c.Submit(context.Background(), r)
			if err != nil || !o.Exists || o.UID != "application-uid" || o.State != "submitting" {
				t.Fatalf("submit: %+v %v", o, err)
			}
			r.KubernetesUID = o.UID
			if _, err = c.Submit(context.Background(), r); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			if creates != 1 || reads < 2 {
				t.Fatalf("creates=%d reads=%d", creates, reads)
			}
		})
	}
}

func TestSubmissionDoesNotRetryUnknownPost(t *testing.T) {
	creates := 0
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			creates++
			w.WriteHeader(504)
			return
		}
		w.WriteHeader(404)
	})
	if _, err := c.Submit(context.Background(), exampleRun()); err == nil {
		t.Fatal("uncertain submission reported success")
	}
	if creates != 1 {
		t.Fatalf("repeated POST %d", creates)
	}
}

func TestForeignOrMutatedApplicationCannotBeAdoptedCancelledOrRead(t *testing.T) {
	for _, field := range []string{"uid", "label", "hash", "configuration", "extra field"} {
		t.Run(field, func(t *testing.T) {
			var app application
			mutations := 0
			c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					mutations++
				}
				writeJSON(t, w, 200, app)
			})
			r := exampleRun()
			r.KubernetesUID = "original-uid"
			app, _ = c.manifest(r)
			app.Metadata.UID = "original-uid"
			switch field {
			case "uid":
				app.Metadata.UID = "replacement-uid"
			case "label":
				app.Metadata.Labels["dataslate.io/run"] = "someone-else"
			case "hash":
				app.Metadata.Annotations["dataslate.io/spec-sha256"] = "wrong"
			case "configuration":
				app.Spec["sparkConf"].(map[string]any)["spark.kubernetes.authenticate.driver.serviceAccountName"] = "admin"
			case "extra field":
				app.Spec["driverSpec"] = map[string]any{"hostNetwork": true}
			}
			if _, err := c.Observe(context.Background(), r); err == nil {
				t.Fatal("observe accepted foreign object")
			}
			if _, err := c.Cancel(context.Background(), r); err == nil {
				t.Fatal("cancel accepted foreign object")
			}
			if _, err := c.Submit(context.Background(), r); err == nil {
				t.Fatal("submit adopted foreign object")
			}
			if _, err := c.Logs(context.Background(), r); err == nil {
				t.Fatal("logs accepted foreign object")
			}
			if mutations != 0 {
				t.Fatal("foreign resource mutated")
			}
		})
	}
}

func TestCancellationRequiresUIDAndForegroundDeletionConfirmation(t *testing.T) {
	r := exampleRun()
	r.KubernetesUID = "app-uid"
	r.CancelRequested = true
	var app application
	deleted := false
	gone := false
	c, _ := testClient(t, func(w http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodDelete {
			var v struct {
				Propagation   string `json:"propagationPolicy"`
				Preconditions struct {
					UID string `json:"uid"`
				} `json:"preconditions"`
			}
			if err := json.NewDecoder(request.Body).Decode(&v); err != nil {
				t.Error(err)
			}
			if v.Propagation != "Foreground" || v.Preconditions.UID != "app-uid" {
				t.Errorf("unsafe delete %+v", v)
			}
			deleted = true
			app.Metadata.DeletionTimestamp = "2026-11-07T00:00:00Z"
			writeJSON(t, w, 202, map[string]string{"kind": "Status", "status": "Success"})
			return
		}
		if gone {
			w.WriteHeader(404)
			return
		}
		writeJSON(t, w, 200, app)
	})
	app, _ = c.manifest(r)
	app.Metadata.UID = "app-uid"
	app.Status.Current.Summary = "RunningHealthy"
	o, err := c.Cancel(context.Background(), r)
	if err != nil || !deleted || !o.Exists || o.State != "cancel_requested" {
		t.Fatalf("premature cancellation %+v %v", o, err)
	}
	gone = true
	o, err = c.Cancel(context.Background(), r)
	if err != nil || o.Exists || o.State != "cancelled" {
		t.Fatalf("deletion confirmation %+v %v", o, err)
	}
}

func TestStateNormalizationDoesNotConfuseCleanupWithSuccess(t *testing.T) {
	for _, tc := range []struct{ current, prior, want string }{{"RunningHealthy", "", "running"}, {"DriverStarted", "", "running"}, {"Submitted", "", "submitting"}, {"Succeeded", "", "succeeded"}, {"Failed", "", "failed"}, {"DriverEvicted", "", "failed"}, {"ScheduledToRestart", "", "unknown"}, {"FutureState", "", "unknown"}, {"ResourceReleased", "", "unknown"}, {"ResourceReleased", "Succeeded", "succeeded"}, {"TerminatedWithoutReleaseResources", "Failed", "failed"}} {
		t.Run(fmt.Sprint(tc), func(t *testing.T) {
			a := application{}
			a.Metadata.UID = "uid"
			a.Status.Current.Summary = tc.current
			if tc.prior != "" {
				a.Status.History = map[string]appState{"9": {Summary: tc.prior}, "10": {Summary: tc.current}}
			}
			if got := observe(a).State; got != tc.want {
				t.Fatalf("got %s want %s", got, tc.want)
			}
		})
	}
}
