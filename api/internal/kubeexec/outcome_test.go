package kubeexec

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"dataslate.local/api/internal/platform"
)

func TestIngestionReceiptRequiresLiveSuccessfulOwnedDriver(t *testing.T) {
	for _, mode := range []string{"valid", "operator running", "operator changed", "foreign", "pod replacement", "restart", "failed exit", "missing status", "oversize", "multiple", "wrong image", "wrong termination"} {
		t.Run(mode, func(t *testing.T) {
			r := ingestionExampleRun()
			r.IngestionOutcome = &platform.IngestionOutcomeSummary{State: "pending"}
			r.State = "succeeded"
			r.KubernetesUID = "app-uid"
			var app application
			applicationReads := 0
			terminated := map[string]any{"exitCode": 0, "message": "complete-receipt"}
			if mode == "failed exit" {
				terminated["exitCode"] = 1
			}
			if mode == "oversize" {
				terminated["message"] = strings.Repeat("x", 4097)
			}
			status := map[string]any{"name": "spark-kubernetes-driver", "restartCount": 0, "state": map[string]any{"terminated": terminated}}
			if mode == "restart" {
				status["restartCount"] = 1
			}
			if mode == "missing status" {
				status["state"] = map[string]any{}
			}
			pod := map[string]any{"metadata": map[string]any{"name": "owned-driver", "namespace": r.Profile.Namespace, "uid": "driver-uid", "labels": map[string]string{"spark-role": "driver", "spark.operator/spark-app-name": r.KubernetesName, "dataslate.io/run": r.ID}, "ownerReferences": []any{map[string]any{"apiVersion": "spark.apache.org/v1", "kind": "SparkApplication", "name": r.KubernetesName, "uid": r.KubernetesUID}}}, "spec": map[string]any{"containers": []any{map[string]any{"name": "spark-kubernetes-driver", "image": r.Profile.Image, "terminationMessagePath": "/dev/termination-log", "terminationMessagePolicy": "File"}}}, "status": map[string]any{"containerStatuses": []any{status}}}
			container := pod["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)
			if mode == "wrong image" {
				container["image"] = "unapproved@sha256:bad"
			}
			if mode == "wrong termination" {
				container["terminationMessagePolicy"] = "FallbackToLogsOnError"
			}
			if mode == "foreign" {
				pod["metadata"].(map[string]any)["ownerReferences"] = []any{}
			}
			c, _ := testClient(t, func(w http.ResponseWriter, req *http.Request) {
				switch {
				case strings.Contains(req.URL.Path, "sparkapplications"):
					applicationReads++
					a := app
					if mode == "operator running" || mode == "operator changed" && applicationReads > 1 {
						a.Status.Current.Summary = "Running"
					}
					writeJSON(t, w, 200, a)
				case strings.HasSuffix(req.URL.Path, "/pods"):
					items := []any{pod}
					if mode == "multiple" {
						items = append(items, pod)
					}
					writeJSON(t, w, 200, map[string]any{"items": items})
				case strings.HasSuffix(req.URL.Path, "/owned-driver"):
					if mode == "pod replacement" {
						pod["metadata"].(map[string]any)["uid"] = "replacement"
					}
					writeJSON(t, w, 200, pod)
				default:
					t.Errorf("unexpected endpoint %s", req.URL.Path)
					w.WriteHeader(500)
				}
			})
			app, _ = c.manifest(r)
			app.Metadata.UID = r.KubernetesUID
			app.Status.Current.Summary = "Succeeded"
			result, e := c.CaptureIngestionOutcome(context.Background(), r)
			if mode == "valid" || mode == "failed exit" || mode == "missing status" {
				if e != nil {
					t.Fatal(e)
				}
				if mode == "valid" && (!result.Final || result.Message != "complete-receipt" || result.DriverUID != "driver-uid") {
					t.Fatal(result)
				}
				if mode == "failed exit" && result.ExitCode != 1 {
					t.Fatal("failed exit lost")
				}
				if mode == "missing status" && result.Final {
					t.Fatal("unconfirmed termination accepted")
				}
			} else if e == nil {
				t.Fatal("untrusted receipt returned", result)
			}
			tolerance := app.Spec["applicationTolerations"].(map[string]any)
			if tolerance["resourceRetainPolicy"] != "Always" {
				t.Fatal("successful driver not retained")
			}
		})
	}
}
