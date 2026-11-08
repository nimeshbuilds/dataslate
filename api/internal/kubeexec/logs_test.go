package kubeexec

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func TestLogsRequireOwnedDriverAndDiscardReplacementRace(t *testing.T) {
	for _, mode := range []string{"owned", "foreign", "changed after read", "multiple"} {
		t.Run(mode, func(t *testing.T) {
			r := exampleRun()
			r.KubernetesUID = "app-uid"
			var app application
			logReads := 0
			podReads := 0
			podDoc := map[string]any{"metadata": map[string]any{"name": "run-driver", "namespace": r.Profile.Namespace, "uid": "pod-uid", "labels": map[string]string{"spark-role": "driver", "spark.operator/spark-app-name": r.KubernetesName}, "ownerReferences": []any{map[string]any{"apiVersion": "spark.apache.org/v1", "kind": "SparkApplication", "name": r.KubernetesName, "uid": "app-uid", "blockOwnerDeletion": true}}}, "spec": map[string]any{"containers": []any{map[string]string{"name": "spark-kubernetes-driver"}}}}
			if mode == "foreign" {
				podDoc["metadata"].(map[string]any)["ownerReferences"] = []any{map[string]string{"apiVersion": "spark.apache.org/v1", "kind": "SparkApplication", "name": r.KubernetesName, "uid": "foreign-uid"}}
			}
			c, _ := testClient(t, func(w http.ResponseWriter, request *http.Request) {
				switch {
				case strings.Contains(request.URL.Path, "sparkapplications"):
					writeJSON(t, w, 200, app)
				case strings.HasSuffix(request.URL.Path, "/log"):
					logReads++
					if request.URL.Query().Get("container") != "spark-kubernetes-driver" || request.URL.Query().Get("limitBytes") != "65536" || request.URL.Query().Get("tailLines") != "500" {
						t.Error("unbounded or incorrect log request")
					}
					_, _ = w.Write([]byte("2026-11-07T00:00:00Z job output\n"))
				case strings.HasSuffix(request.URL.Path, "/pods"):
					if !strings.Contains(request.URL.Query().Get("labelSelector"), "spark-role=driver") {
						t.Error("missing driver filter")
					}
					items := []any{podDoc}
					if mode == "multiple" {
						items = append(items, podDoc)
					}
					writeJSON(t, w, 200, map[string]any{"items": items})
				case strings.HasSuffix(request.URL.Path, "/pods/run-driver"):
					podReads++
					if mode == "changed after read" && podReads == 2 {
						raw, _ := json.Marshal(podDoc)
						var changed map[string]any
						_ = json.Unmarshal(raw, &changed)
						changed["metadata"].(map[string]any)["uid"] = "replacement-uid"
						writeJSON(t, w, 200, changed)
						return
					}
					writeJSON(t, w, 200, podDoc)
				default:
					t.Errorf("unexpected request %s", request.URL.Path)
					w.WriteHeader(500)
				}
			})
			app, _ = c.manifest(r)
			app.Metadata.UID = "app-uid"
			logs, err := c.Logs(context.Background(), r)
			if mode == "owned" {
				if err != nil || !strings.Contains(logs.Text, "job output") || !logs.Truncated {
					t.Fatalf("logs %+v %v", logs, err)
				}
			} else {
				if err == nil || logs.Text != "" {
					t.Fatalf("unsafe logs returned %+v %v", logs, err)
				}
				if (mode == "foreign" || mode == "multiple") && logReads != 0 {
					t.Fatal("unauthorized log read")
				}
			}
		})
	}
}
