package kubeexec_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"dataslate.local/api/internal/kubeexec"
	"dataslate.local/api/internal/platform"
	"github.com/jackc/pgx/v5"
)

// This test uses real HTTP handlers, real PostgreSQL and the actual Kubernetes
// adapter over a TLS API fixture. It does not run Kubernetes or Spark. Its
// private schema prevents cross-package controller tests from leasing its runs.
func TestHTTPPostgresControllerAndKubernetesAdapter(t *testing.T) {
	dsn := os.Getenv("DATASLATE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set DATASLATE_TEST_DATABASE_URL for isolated PostgreSQL integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal("cannot connect to explicitly selected test database")
	}
	defer admin.Close(context.Background())
	suffix := make([]byte, 8)
	if _, err = rand.Read(suffix); err != nil {
		t.Fatal(err)
	}
	schema := "kubeexec_integration_" + hex.EncodeToString(suffix)
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := admin.Exec(context.Background(), "DROP SCHEMA "+quoted+" CASCADE"); err != nil {
			t.Error("test schema cleanup failed")
		}
	}()
	isolatedDSN := dsn + " search_path=" + schema
	if u, e := url.Parse(dsn); e == nil && (u.Scheme == "postgres" || u.Scheme == "postgresql") {
		query := u.Query()
		query.Set("search_path", schema)
		u.RawQuery = query.Encode()
		isolatedDSN = u.String()
	}
	first, err := platform.OpenPostgres(ctx, isolatedDSN, false)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if err = first.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	ws, err := first.CreateWorkspace(ctx, "Execution integration", "integration")
	if err != nil {
		t.Fatal(err)
	}
	binding := platform.ExecutionBinding{ID: "approved-batch", WorkspaceID: ws.ID, Name: "Test-owned immutable job", ExecutionProfile: platform.ExecutionProfile{Operator: "apache", Image: "example.test/etl@sha256:" + strings.Repeat("b", 64), MainApplicationFile: "local:///opt/jobs/test.py", Namespace: "integration-jobs", ServiceAccount: "approved-job", ExecutorInstances: 1}, AllowedSubjects: []platform.ExecutionSubject{{Issuer: "https://identity.example.test", Subject: "alice"}}}
	var mu sync.Mutex
	var application map[string]any
	creates, logReads := 0, 0
	kube := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("Authorization") != "Bearer integration-kube-token" {
			t.Error("missing Kubernetes credential")
			w.WriteHeader(401)
			return
		}
		respond := func(v any) {
			if err := json.NewEncoder(w).Encode(v); err != nil {
				t.Error(err)
			}
		}
		switch {
		case r.URL.Path == "/apis/spark.apache.org/v1":
			respond(map[string]any{"groupVersion": "spark.apache.org/v1", "resources": []any{map[string]any{"name": "sparkapplications", "kind": "SparkApplication", "namespaced": true, "verbs": []string{"get", "create", "delete"}}}})
		case strings.Contains(r.URL.Path, "/sparkapplications"):
			if r.Method == http.MethodPost {
				creates++
				if err := json.NewDecoder(r.Body).Decode(&application); err != nil {
					t.Error(err)
				}
				application["metadata"].(map[string]any)["uid"] = "integration-app-uid"
				application["status"] = map[string]any{"currentState": map[string]string{"currentStateSummary": "RunningHealthy"}}
				w.WriteHeader(201)
				respond(application)
				return
			}
			if application == nil {
				w.WriteHeader(404)
				return
			}
			respond(application)
		case strings.HasSuffix(r.URL.Path, "/log"):
			logReads++
			_, _ = w.Write([]byte("2026-11-07T00:00:00Z actual fixture driver output\n"))
		case strings.Contains(r.URL.Path, "/pods"):
			if application == nil {
				w.WriteHeader(404)
				return
			}
			name := application["metadata"].(map[string]any)["name"].(string)
			pod := map[string]any{"metadata": map[string]any{"name": "integration-driver", "namespace": binding.Namespace, "uid": "integration-driver-uid", "labels": map[string]string{"spark-role": "driver", "spark.operator/spark-app-name": name}, "ownerReferences": []any{map[string]string{"apiVersion": "spark.apache.org/v1", "kind": "SparkApplication", "name": name, "uid": "integration-app-uid"}}}, "spec": map[string]any{"containers": []any{map[string]string{"name": "spark-kubernetes-driver"}}}}
			if strings.HasSuffix(r.URL.Path, "/pods") {
				respond(map[string]any{"items": []any{pod}})
			} else {
				respond(pod)
			}
		default:
			t.Errorf("unexpected Kubernetes fixture request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(500)
		}
	}))
	defer kube.Close()
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.crt")
	tokenPath := filepath.Join(dir, "token")
	policyPath := filepath.Join(dir, "bindings.json")
	policy, _ := json.Marshal([]platform.ExecutionBinding{binding})
	for path, data := range map[string][]byte{caPath: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: kube.Certificate().Raw}), tokenPath: []byte("integration-kube-token"), policyPath: policy} {
		if err = os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	client, err := kubeexec.New(kubeexec.Config{ServerURL: kube.URL, CAFile: caPath, TokenFile: tokenPath, AllowedNamespaces: []string{binding.Namespace}})
	if err != nil {
		t.Fatal(err)
	}
	if err = client.Discover(ctx); err != nil {
		t.Fatal(err)
	}
	backend, err := kubeexec.NewGuarded(client, policyPath)
	if err != nil {
		t.Fatal(err)
	}
	startAPI := func(store platform.Store) *httptest.Server {
		return httptest.NewServer((&platform.Server{Config: platform.Config{AuthMode: "oidc", ExecutionBindings: []platform.ExecutionBinding{binding}}, Store: store, Auth: integrationAuth{workspace: ws.ID}, Backend: client}).Handler())
	}
	request := func(base, method, path, body, user string) (int, []byte) {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, method, base+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+user)
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, data
	}
	firstAPI := startAPI(first)
	path := "/api/v1/workspaces/" + ws.ID + "/runs"
	status, raw := request(firstAPI.URL, "POST", path, `{"bindingId":"approved-batch","idempotencyKey":"integration-idempotency-0001"}`, "alice")
	if status != 202 {
		t.Fatalf("create: %d %s", status, raw)
	}
	var run platform.Run
	if err = json.Unmarshal(raw, &run); err != nil {
		t.Fatal(err)
	}
	if run.State != "queued" || run.KubernetesUID != "" {
		t.Fatal("acceptance claimed engine execution")
	}
	firstAPI.Close()
	first.Close()
	// A fresh connection and API process reopen the durable queue after restart.
	second, err := platform.OpenPostgres(ctx, isolatedDSN, false)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	secondAPI := startAPI(second)
	defer secondAPI.Close()
	if worked, err := platform.ReconcileOnce(ctx, second, backend, "controller-after-restart"); err != nil || !worked {
		t.Fatalf("reconcile: %v", err)
	}
	status, raw = request(secondAPI.URL, "GET", path+"/"+run.ID, "", "alice")
	if status != 200 {
		t.Fatalf("read: %d %s", status, raw)
	}
	if err = json.Unmarshal(raw, &run); err != nil {
		t.Fatal(err)
	}
	if run.State != "running" || run.KubernetesUID != "integration-app-uid" || !run.SubmissionStarted {
		t.Fatalf("durable observation %+v", run)
	}
	status, raw = request(secondAPI.URL, "GET", path+"/"+run.ID+"/logs", "", "bob")
	if status != 404 {
		t.Fatalf("foreign owner received logs: %d %s", status, raw)
	}
	mu.Lock()
	beforeLogs := logReads
	mu.Unlock()
	if beforeLogs != 0 {
		t.Fatal("denied log read reached Kubernetes")
	}
	status, raw = request(secondAPI.URL, "GET", path+"/"+run.ID+"/logs", "", "alice")
	if status != 200 || !strings.Contains(string(raw), "fixture driver output") {
		t.Fatalf("owner logs: %d %s", status, raw)
	}
	mu.Lock()
	application["status"] = map[string]any{"currentState": map[string]string{"currentStateSummary": "ResourceReleased"}, "stateTransitionHistory": map[string]any{"8": map[string]string{"currentStateSummary": "Succeeded"}, "9": map[string]string{"currentStateSummary": "ResourceReleased"}}}
	mu.Unlock()
	if _, err = admin.Exec(ctx, "UPDATE "+quoted+".dataslate_runs SET next_reconcile_at=clock_timestamp() WHERE id=$1", run.ID); err != nil {
		t.Fatal(err)
	}
	third, err := platform.OpenPostgres(ctx, isolatedDSN, false)
	if err != nil {
		t.Fatal(err)
	}
	defer third.Close()
	if worked, err := platform.ReconcileOnce(ctx, third, backend, "replacement-controller"); err != nil || !worked {
		t.Fatalf("replacement reconciliation: %v", err)
	}
	status, raw = request(secondAPI.URL, "GET", path+"/"+run.ID, "", "alice")
	if status != 200 {
		t.Fatalf("terminal read: %d", status)
	}
	if err = json.Unmarshal(raw, &run); err != nil {
		t.Fatal(err)
	}
	if run.State != "succeeded" || run.ObservedAt == nil {
		t.Fatal("terminal outcome not persisted")
	}
	status, raw = request(secondAPI.URL, "POST", path, `{"bindingId":"approved-batch","idempotencyKey":"integration-idempotency-0001"}`, "alice")
	if status != 200 || !strings.Contains(string(raw), run.ID) {
		t.Fatal("HTTP retry did not return original durable run")
	}
	mu.Lock()
	defer mu.Unlock()
	if creates != 1 {
		t.Fatalf("duplicate Kubernetes creations: %d", creates)
	}
}

// Authentication is the sole API-side fixture; signed JWT verification is
// independently exercised by platform/auth_test.go. Both users have membership.
type integrationAuth struct{ workspace string }

func (a integrationAuth) Authenticate(_ context.Context, header string) (platform.Principal, error) {
	subject := strings.TrimPrefix(header, "Bearer ")
	if subject != "alice" && subject != "bob" {
		return platform.Principal{}, errors.New("invalid test principal")
	}
	return platform.Principal{Issuer: "https://identity.example.test", Subject: subject, Workspaces: map[string]string{a.workspace: "editor"}}, nil
}
