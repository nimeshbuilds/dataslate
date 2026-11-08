// Saved-notebook qualification against an isolated real Kubernetes API. Copy this
// file beneath api/.qa before running. Persistence and authentication are explicit
// in-memory development fixtures; source data never leaves this local cluster.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
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
func encoded(v any) []byte { b, e := json.Marshal(v); must(e); return b }

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	bindings, err := platform.LoadExecutionBindings(os.Getenv("DATASLATE_EXECUTION_BINDINGS_FILE"))
	must(err)
	cfg := kubeexec.ConfigFromEnv()
	client, err := kubeexec.New(cfg)
	must(err)
	backend, err := kubeexec.NewGuarded(client, os.Getenv("DATASLATE_EXECUTION_BINDINGS_FILE"))
	must(err)
	must(backend.Discover(ctx))
	readCfg := cfg
	readCfg.TokenFile = os.Getenv("DATASLATE_KUBERNETES_API_TOKEN_FILE")
	reader, err := kubeexec.New(readCfg)
	must(err)
	store := platform.NewMemoryStore()
	server := &platform.Server{Config: platform.Config{AuthMode: "development", ExecutionBindings: bindings}, Store: store, Auth: platform.DevelopmentAuth{}, Backend: reader}
	handler := server.Handler()
	request := func(method, path string, body any, status int) map[string]any {
		var input io.Reader
		if body != nil {
			input = bytes.NewReader(encoded(body))
		}
		r := httptest.NewRequest(method, "/api/v1/workspaces/default"+path, input)
		r.Host = "127.0.0.1"
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != status {
			panic(fmt.Sprintf("%s %s: status %d expected %d: %s", method, path, w.Code, status, w.Body.String()))
		}
		if w.Body.Len() == 0 {
			return map[string]any{}
		}
		var result map[string]any
		must(json.Unmarshal(w.Body.Bytes(), &result))
		return result
	}
	save := func(name string, v any) {
		b, e := json.MarshalIndent(v, "", "  ")
		must(e)
		must(os.WriteFile(filepath.Join(os.Getenv("QUALIFICATION_OUTPUT_DIR"), name), append(b, '\n'), 0600))
	}
	// This direct API helper inspects only the exact source artifact created for
	// the current fixture, using the controller's explicitly scoped credentials.
	ca, e := os.ReadFile(cfg.CAFile)
	must(e)
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		panic("invalid fixture CA")
	}
	kubeHTTP := &http.Client{Timeout: 8 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}}
	kubeAs := func(method, path string, body any, tokenFile string) (int, map[string]any) {
		var input io.Reader
		if body != nil {
			input = bytes.NewReader(encoded(body))
		}
		r, e := http.NewRequestWithContext(ctx, method, cfg.ServerURL+path, input)
		must(e)
		token, e := os.ReadFile(tokenFile)
		must(e)
		r.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
		r.Header.Set("Content-Type", "application/merge-patch+json")
		response, e := kubeHTTP.Do(r)
		must(e)
		defer response.Body.Close()
		b, e := io.ReadAll(io.LimitReader(response.Body, 2<<20))
		must(e)
		var result map[string]any
		must(json.Unmarshal(b, &result))
		return response.StatusCode, result
	}
	kube := func(method, path string, body any) (int, map[string]any) {
		return kubeAs(method, path, body, cfg.TokenFile)
	}

	sourceBody := func(source string, revision int) map[string]any {
		m := map[string]any{"name": "Local immutable notebook fixture", "kind": "notebook", "cells": []map[string]string{{"id": "fixture-cell", "language": "python", "source": source}}}
		if revision > 0 {
			m["revision"] = revision
		}
		return m
	}
	examplePath := os.Getenv("QUALIFICATION_NOTEBOOK_FILE")
	if examplePath == "" {
		examplePath = "../examples/python-batch-notebook.json"
	}
	exampleBytes, e := os.ReadFile(examplePath)
	must(e)
	var example map[string]any
	must(json.Unmarshal(exampleBytes, &example))
	expectedSources := map[string]string{}

	subject := platform.ExecutionSubject{Issuer: "development", Subject: "development-admin"}
	newRun := func(binding, key string, source any) (platform.Run, map[string]any, map[string]any) {
		notebookBody := source
		if code, ok := source.(string); ok {
			notebookBody = sourceBody(code, 0)
		}
		notebook := request("POST", "/notebooks", notebookBody, 201)
		body := map[string]any{"bindingId": binding, "idempotencyKey": key, "notebook": map[string]any{"id": notebook["id"], "revision": notebook["revision"]}}
		response := request("POST", "/runs", body, 202)
		var r platform.Run
		must(json.Unmarshal(encoded(response), &r))
		snapshot := request("GET", "/runs/"+r.ID+"/source", nil, 200)
		expectedSources[r.ID] = snapshot["source"].(string)
		return r, notebook, body
	}
	wait := func(r platform.Run, label, want, marker string, cancelAfterSubmit bool) platform.Run {
		deadline := time.Now().Add(6 * time.Minute)
		last := ""
		cancelled := false
		artifactSeen := false
		immutableTested := false
		matchedLog := false
		matchedCellTraceback := false
		artifactPath := ""
		for time.Now().Before(deadline) {
			_, e := platform.ReconcileOnce(ctx, store, backend, "local-notebook-qualification")
			must(e)
			r, e = store.Run(ctx, r.WorkspaceID, r.ID, subject)
			must(e)
			doc := request("GET", "/runs/"+r.ID, nil, 200)
			if r.State != last {
				fmt.Printf("%s state=%s uid=%s\n", label, r.State, r.KubernetesUID)
				last = r.State
			}
			if artifact, ok := doc["sourceArtifact"].(map[string]any); ok {
				if name, ok := artifact["name"].(string); ok && name != "" {
					artifactPath = "/api/v1/namespaces/" + r.Profile.Namespace + "/configmaps/" + name
					status, cm := kube("GET", artifactPath, nil)
					if status == 200 {
						artifactSeen = true
						data, ok := cm["data"].(map[string]any)
						if !ok || data["source.py"] != expectedSources[r.ID] {
							panic("source ConfigMap bytes differ from persisted snapshot")
						}
						save(label+"-source-configmap.json", cm)
						if cm["immutable"] != true {
							panic("notebook source ConfigMap is mutable")
						}
						if !immutableTested {
							status, _ := kube("PATCH", artifactPath, map[string]any{"data": map[string]string{"source.py": "raise RuntimeError('mutated source must not run')\n"}})
							if status != 422 {
								panic(fmt.Sprintf("immutable ConfigMap mutation returned %d, expected 422", status))
							}
							status, denied := kubeAs("DELETE", artifactPath+"?dryRun=All", nil, os.Getenv("QUALIFICATION_WORKLOAD_TOKEN_FILE"))
							if status != 403 || !strings.Contains(fmt.Sprint(denied["message"]), "ValidatingAdmissionPolicy") {
								panic(fmt.Sprintf("workload source deletion was not denied by admission: %d %v", status, denied["message"]))
							}
							status, _ = kubeAs("GET", artifactPath, nil, readCfg.TokenFile)
							if status != 403 {
								panic("API identity unexpectedly reads source ConfigMaps directly")
							}
							save(label+"-artifact-policy.json", map[string]any{"immutablePatchRejected": true, "workloadDeleteRejectedByAdmission": true, "apiDirectReadDenied": true})
							immutableTested = true
						}
					} else if status != 404 {
						panic(fmt.Sprintf("source inspection returned %d", status))
					}
				}
			}
			if r.KubernetesUID != "" {
				logs, e := reader.Logs(ctx, r)
				if e == nil && logs.Text != "" {
					save(label+"-logs.json", logs)
					if strings.Contains(logs.Text, marker) {
						matchedLog = true
					}
					if strings.Contains(logs.Text, "/1/fixture-cell.py") && strings.Contains(logs.Text, "Traceback") {
						matchedCellTraceback = true
					}
				}
			}
			if cancelAfterSubmit && r.KubernetesUID != "" && !cancelled {
				request("POST", "/runs/"+r.ID+"/cancel", map[string]any{}, 202)
				cancelled = true
			}
			if r.Terminal() {
				save(label+"-run.json", doc)
				if r.State != want {
					panic(fmt.Sprintf("%s wanted %s got %s", label, want, r.State))
				}
				if !artifactSeen || !immutableTested {
					panic("source artifact was never verified before termination")
				}
				if marker != "" && !matchedLog {
					panic("expected fixture output not observed in owner-verified logs")
				}
				if label == "notebook-failure" && !matchedCellTraceback {
					panic("failure traceback did not identify the saved notebook revision and cell")
				}
				// Source cleanup is a separate persisted operation after terminal execution.
				for cleanupDeadline := time.Now().Add(45 * time.Second); time.Now().Before(cleanupDeadline); {
					_, e = platform.ReconcileOnce(ctx, store, backend, "local-notebook-qualification")
					must(e)
					status, _ := kube("GET", artifactPath, nil)
					doc = request("GET", "/runs/"+r.ID, nil, 200)
					save(label+"-run.json", doc)
					if status == 404 {
						save(label+"-cleanup.json", map[string]any{"sourceConfigMapAbsent": true, "runId": r.ID})
						return r
					}
					time.Sleep(time.Second)
				}
				panic("terminal notebook source ConfigMap was not cleaned")
			}
			if r.State == "unknown" {
				o, e := backend.Observe(ctx, r)
				fmt.Printf("observation diagnostic=%+v error=%v\n", o, e)
			}
			select {
			case <-ctx.Done():
				panic(ctx.Err())
			case <-time.After(time.Second):
			}
		}
		panic("notebook qualification run timed out")
	}
	run, notebook, body := newRun("local-notebook-fixture", "notebook-success-revision-one", example)
	source := request("GET", "/runs/"+run.ID+"/source", nil, 200)
	save("notebook-source-before-edit.json", source)
	sourceText, ok := source["source"].(string)
	if !ok {
		panic("source endpoint returned no source")
	}
	digest := sha256.Sum256([]byte(sourceText))
	meta, ok := source["notebook"].(map[string]any)
	if !ok || meta["sha256"] != hex.EncodeToString(digest[:]) {
		panic("persisted source digest does not match source bytes")
	}
	edited := request("PUT", "/notebooks/"+notebook["id"].(string), sourceBody("raise RuntimeError('REVISION_TWO_MUST_NOT_RUN')\n", int(notebook["revision"].(float64))), 200)
	request("DELETE", fmt.Sprintf("/notebooks/%s?revision=%d", notebook["id"], int(edited["revision"].(float64))), nil, 204)
	replay := request("POST", "/runs", body, 200)
	if replay["id"] != run.ID {
		panic("idempotent replay after notebook edit changed run")
	}
	after := request("GET", "/runs/"+run.ID+"/source", nil, 200)
	if after["source"] != sourceText {
		panic("saved notebook edit changed immutable run source")
	}
	wait(run, "notebook-success", "succeeded", "DATASLATE_EXAMPLE_RESULT {\"rows\": 100, \"sumSquares\": 328350}", false)
	after = request("GET", "/runs/"+run.ID+"/source", nil, 200)
	if after["source"] != sourceText {
		panic("artifact cleanup removed persisted source snapshot")
	}
	save("notebook-source-after-cleanup.json", after)
	failed, _, _ := newRun("local-notebook-fixture", "notebook-deliberate-failure", "raise RuntimeError('DATASLATE_NOTEBOOK_EXPECTED_FAILURE')\n")
	wait(failed, "notebook-failure", "failed", "DATASLATE_NOTEBOOK_EXPECTED_FAILURE", false)
	cancelled, _, _ := newRun("local-notebook-fixture", "notebook-cancellation", "import time\ntime.sleep(180)\n")
	wait(cancelled, "notebook-cancel", "cancelled", "", true)
	timed, _, _ := newRun("local-notebook-timeout", "notebook-runtime-deadline", "import time\nprint('DATASLATE_NOTEBOOK_TIMEOUT_STARTED', flush=True)\ntime.sleep(180)\n")
	wait(timed, "notebook-timeout", "cancelled", "DATASLATE_NOTEBOOK_TIMEOUT_STARTED", false)
	save("summary.json", map[string]any{"passed": true, "persistence": "memory", "scope": "isolated local Spark notebook execution", "checks": []string{"immutable saved revision executes real Spark sum of squares 328350", "editing and deleting notebook preserves queued run source", "idempotent replay after notebook edit", "source SHA256 matches persisted bytes", "immutable ConfigMap rejects mutation", "workload source deletion blocked by admission", "API identity cannot directly read source ConfigMaps", "source remains owner-readable after artifact cleanup", "success/failure/cancellation/deadline observed", "terminal source ConfigMaps deleted"}, "excluded": []string{"EKS", "S3", "production OIDC", "PostgreSQL durability", "governed SQL", "untrusted tenant isolation"}})
	fmt.Println("saved notebook local Kubernetes qualification passed")
}
