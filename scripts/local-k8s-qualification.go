// This harness is copied beneath api/.qa before execution so it can import the
// real API internals. It uses in-memory persistence and a real, isolated local
// Kubernetes API. It does not qualify OIDC, PostgreSQL, EKS, S3, or data ingestion.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"time"

	"dataslate.local/api/internal/kubeexec"
	"dataslate.local/api/internal/platform"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	bindings, err := platform.LoadExecutionBindings(os.Getenv("DATASLATE_EXECUTION_BINDINGS_FILE"))
	must(err)
	client, err := kubeexec.New(kubeexec.ConfigFromEnv())
	must(err)
	backend, err := kubeexec.NewGuarded(client, os.Getenv("DATASLATE_EXECUTION_BINDINGS_FILE"))
	must(err)
	must(backend.Discover(ctx))
	readConfig := kubeexec.ConfigFromEnv()
	readConfig.TokenFile = os.Getenv("DATASLATE_KUBERNETES_API_TOKEN_FILE")
	readClient, err := kubeexec.New(readConfig)
	must(err)
	must(readClient.Discover(ctx))
	store := platform.NewMemoryStore()
	server := &platform.Server{Config: platform.Config{AuthMode: "development", ExecutionBindings: bindings}, Store: store, Auth: platform.DevelopmentAuth{}, Backend: readClient}
	handler := server.Handler()
	request := func(method, path, body string, expected int) []byte {
		r := httptest.NewRequest(method, "/api/v1/workspaces/default"+path, strings.NewReader(body))
		r.Host = "127.0.0.1"
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != expected {
			panic(fmt.Sprintf("%s %s: status=%d expected=%d body=%s", method, path, w.Code, expected, w.Body.String()))
		}
		return w.Body.Bytes()
	}
	save := func(name string, data any) {
		b, e := json.MarshalIndent(data, "", "  ")
		must(e)
		must(os.WriteFile(filepath.Join(os.Getenv("QUALIFICATION_OUTPUT_DIR"), name), append(b, '\n'), 0600))
	}
	subject := platform.ExecutionSubject{Issuer: "development", Subject: "development-admin"}
	create := func(binding, key string) platform.Run {
		body := fmt.Sprintf(`{"bindingId":%q,"idempotencyKey":%q}`, binding, key)
		var r platform.Run
		must(json.Unmarshal(request(http.MethodPost, "/runs", body, 202), &r))
		var replay platform.Run
		must(json.Unmarshal(request(http.MethodPost, "/runs", body, 200), &replay))
		if replay.ID != r.ID {
			panic("idempotent replay created another run")
		}
		return r
	}
	wait := func(r platform.Run, want string, cancelAfterSubmit bool) platform.Run {
		lastState := ""
		cancelled := false
		gotLogs := false
		deadline := time.Now().Add(5 * time.Minute)
		for time.Now().Before(deadline) {
			_, e := platform.ReconcileOnce(ctx, store, backend, "local-k8s-qualification")
			must(e)
			r, e = store.Run(ctx, r.WorkspaceID, r.ID, subject)
			must(e)
			if r.State != lastState {
				fmt.Printf("%s %s uid=%s\n", r.BindingID, r.State, r.KubernetesUID)
				lastState = r.State
			}
			if r.State == "unknown" {
				o, e := backend.Observe(ctx, r)
				fmt.Printf("observe diagnostic: %+v error=%v\n", o, e)
			}
			if r.KubernetesUID != "" {
				logs, e := readClient.Logs(ctx, r)
				if e == nil && logs.Text != "" {
					save(r.BindingID+"-logs.json", logs)
					gotLogs = true
				}
			}
			if cancelAfterSubmit && r.KubernetesUID != "" && !cancelled {
				request(http.MethodPost, "/runs/"+r.ID+"/cancel", "{}", 202)
				cancelled = true
			}
			if r.Terminal() {
				save(r.BindingID+"-run.json", r)
				if want == "succeeded" && !gotLogs {
					panic("successful fixture never returned owner-verified driver logs")
				}
				if r.State != want {
					panic(fmt.Sprintf("%s: expected %s, got %s", r.BindingID, want, r.State))
				}
				if r.SubmissionStarted && r.KubernetesUID == "" {
					panic("terminal submitted run lacks verified UID")
				}
				return r
			}
			select {
			case <-ctx.Done():
				panic(ctx.Err())
			case <-time.After(time.Second):
			}
		}
		save(r.BindingID+"-run.json", r)
		panic("local Kubernetes run timed out")
	}
	request(http.MethodGet, "/execution-bindings", "", 200)
	success := wait(create("local-pi-fixture", "local-k8s-success"), "succeeded", false)
	logs, e := readClient.Logs(ctx, success)
	if e == nil {
		save("local-pi-fixture-final-logs.json", logs)
	} else if !errors.Is(e, platform.ErrCapabilityUnavailable) {
		panic(e)
	}
	wait(create("local-failure-fixture", "local-k8s-failure"), "failed", false)
	wait(create("local-cancel-fixture", "local-k8s-cancel"), "cancelled", true)
	// A revoked grant must cancel before submission without a Kubernetes UID.
	revoked := create("local-revoke-fixture", "local-k8s-revocation")
	filtered := []platform.ExecutionBinding{}
	for _, b := range bindings {
		if b.ID != "local-revoke-fixture" {
			filtered = append(filtered, b)
		}
	}
	b, e := json.Marshal(filtered)
	must(e)
	must(os.WriteFile(os.Getenv("DATASLATE_EXECUTION_BINDINGS_FILE"), b, 0600))
	revoked = wait(revoked, "cancelled", false)
	if revoked.KubernetesUID != "" || revoked.SubmissionStarted {
		panic("revoked queued run reached Kubernetes")
	}
	// Read actual event history through HTTP and preserve the evidence.
	var events any
	must(json.Unmarshal(request(http.MethodGet, "/runs/"+success.ID+"/events", "", 200), &events))
	save("success-events.json", events)
	summary := map[string]any{
		"passed":      true,
		"persistence": "memory",
		"versions":    map[string]string{"kubernetes": "v1.35.0+k3s1", "spark": "4.1.3", "operator": "1.0.0", "chart": "1.8.0"},
		"cluster":     "isolated local k3s",
		"checks": []string{
			"real CRD discovery and admission",
			"digest-pinned Apache Spark 4.1.3 Python example succeeds",
			"actual operator failure observed",
			"UID-precondition cancellation confirmed",
			"owner-verified driver log retrieval through separate read-only API identity",
			"idempotent HTTP replay",
			"guard policy revocation before submission",
		},
		"excluded": []string{"PostgreSQL durability", "OIDC", "EKS", "S3", "real data ingestion", "production resource sizing"},
	}
	save("summary.json", summary)
	out := new(bytes.Buffer)
	must(json.NewEncoder(out).Encode(summary))
	fmt.Print(out.String())
}
