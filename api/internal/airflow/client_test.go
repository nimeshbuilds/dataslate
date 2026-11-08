package airflow

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
	"sync/atomic"
	"testing"
)

func clientFixture(t *testing.T, handler http.HandlerFunc) (*Client, string) {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.pem")
	token := filepath.Join(dir, "token")
	os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600)
	os.WriteFile(token, []byte("test-token"), 0600)
	c, e := New(Config{server.URL, ca, token})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(c.Close)
	return c, token
}
func targetFixture() Target { return Target{"approved_dag", 1, "bundle-sha256"} }
func versionFixture() Version {
	return Version{"10000000-0000-4000-8000-000000000001", 1, "approved_dag", "bundle-sha256"}
}
func runFixture() Run {
	conf, _ := json.Marshal(Conf{Receipt{"wf-one", "default", "daily", "v1"}})
	return Run{DAGID: "approved_dag", RunID: "dataslate-wf-one", State: "running", RunType: "manual", Versions: []Version{versionFixture()}, BundleVersion: "bundle-sha256", Conf: conf}
}
func TestAirflowExactTriggerReadbackAndTokenRotation(t *testing.T) {
	var posts atomic.Int32
	tokenValue := "test-token"
	run := runFixture()
	c, path := clientFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+tokenValue {
			t.Error("credential was not refreshed")
		}
		w.Header().Set("Content-Type", "application/json")
		if r.Method == "POST" {
			posts.Add(1)
			var body map[string]json.RawMessage
			json.NewDecoder(r.Body).Decode(&body)
			if len(body) != 4 || string(body["logical_date"]) != "null" || string(body["bundle_version"]) != `"bundle-sha256"` || string(body["dag_run_id"]) != `"dataslate-wf-one"` {
				t.Error("trigger contract mismatch")
			}
		}
		json.NewEncoder(w).Encode(run)
	})
	receipt := Receipt{"wf-one", "default", "daily", "v1"}
	got, e := c.Trigger(context.Background(), targetFixture(), run.RunID, receipt)
	if e != nil || got.State != "running" {
		t.Fatal(got, e)
	}
	tokenValue = "rotated-token"
	os.WriteFile(path, []byte(tokenValue), 0600)
	if _, e = c.Run(context.Background(), targetFixture(), run.RunID, receipt); e != nil {
		t.Fatal(e)
	}
	if posts.Load() != 1 {
		t.Fatal("POST replay")
	}
}
func TestAirflowRejectsWrongOwnershipVersionRedirectAndMalformedResponse(t *testing.T) {
	for _, tc := range []string{"owner", "version", "dag", "run", "duplicate", "redirect", "unauthorized", "oversized", "html", "multiple_versions", "null_state"} {
		t.Run(tc, func(t *testing.T) {
			var calls atomic.Int32
			c, _ := clientFixture(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				run := runFixture()
				w.Header().Set("Content-Type", "application/json")
				switch tc {
				case "owner":
					run.Conf = json.RawMessage(`{"dataslate":{"runId":"wf-foreign","workspaceId":"default","bindingId":"daily","policyVersion":"v1"}}`)
				case "version":
					run.Versions[0].Number = 2
				case "dag":
					run.DAGID = "foreign"
				case "run":
					run.RunID = "foreign"
				case "duplicate":
					fmt.Fprint(w, `{"dag_id":"approved_dag","dag_id":"foreign"}`)
					return
				case "redirect":
					w.Header().Set("Location", "https://foreign.example/token")
					w.WriteHeader(302)
					return
				case "unauthorized":
					w.WriteHeader(403)
					fmt.Fprint(w, "secret upstream message")
					return
				case "oversized":
					fmt.Fprint(w, strings.Repeat(" ", 2*1024*1024+1))
					return
				case "html":
					w.Header().Set("Content-Type", "text/html")
				case "multiple_versions":
					run.Versions = append(run.Versions, versionFixture())
				case "null_state":
					run.State = ""
				}
				json.NewEncoder(w).Encode(run)
			})
			_, e := c.Trigger(context.Background(), targetFixture(), "dataslate-wf-one", Receipt{"wf-one", "default", "daily", "v1"})
			if e == nil {
				t.Fatal("unsafe response accepted")
			}
			if strings.Contains(e.Error(), "secret") {
				t.Fatal("backend content leaked")
			}
			if calls.Load() != 1 {
				t.Fatal("request was retried or redirected")
			}
		})
	}
}
func TestAirflowDefinitionGraphAndVersionRace(t *testing.T) {
	for _, race := range []bool{false, true} {
		t.Run(fmt.Sprint(race), func(t *testing.T) {
			reads := 0
			c, _ := clientFixture(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/api/v2/version":
					fmt.Fprint(w, `{"version":"3.4.0"}`)
				case "/api/v2/dags/approved_dag/details":
					reads++
					v := versionFixture()
					if race && reads > 1 {
						v.Number = 2
					}
					json.NewEncoder(w).Encode(map[string]any{"dag_id": "approved_dag", "is_paused": false, "is_stale": false, "has_import_errors": false, "timetable_summary": "0 0 * * *", "timezone": "UTC", "latest_dag_version": v, "params": map[string]string{"sensitive": "not public"}})
				case "/api/v2/dags/approved_dag/tasks":
					fmt.Fprint(w, `{"tasks":[{"task_id":"ingest","downstream_task_ids":["sql"],"params":{"secret":"notpublic"}},{"task_id":"sql","downstream_task_ids":[]}],"total_entries":2}`)
				default:
					t.Error(r.URL.Path)
				}
			})
			d, e := c.Definition(context.Background(), targetFixture())
			if race {
				if e == nil {
					t.Fatal("version changed mid graph read")
				}
			} else if e != nil || !d.Matches(targetFixture()) || len(d.Tasks) != 2 {
				t.Fatal(d, e)
			}
		})
	}
}
func TestAirflowHistoryScheduledIdentifiersAndNoRawResult(t *testing.T) {
	c, _ := clientFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("limit") != "50" || r.URL.Query().Get("order_by") != "-run_after" {
			t.Error("unbounded history")
		}
		w.Header().Set("Content-Type", "application/json")
		run := runFixture()
		run.RunID = "scheduled__2026-11-08T00:00:00+00:00"
		run.RunType = "scheduled"
		run.Conf = json.RawMessage(`{"anything":"private"}`)
		json.NewEncoder(w).Encode(map[string]any{"dag_runs": []Run{run}, "total_entries": 51})
	})
	runs, truncated, e := c.History(context.Background(), targetFixture())
	if e != nil || len(runs) != 1 || !truncated {
		t.Fatal(runs, truncated, e)
	}
}
func TestAirflowTasksIdentityAndBounds(t *testing.T) {
	for _, foreign := range []bool{false, true} {
		t.Run(fmt.Sprint(foreign), func(t *testing.T) {
			c, _ := clientFixture(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				dag := "approved_dag"
				if foreign {
					dag = "foreign"
				}
				json.NewEncoder(w).Encode(map[string]any{"task_instances": []any{map[string]any{"dag_id": dag, "dag_run_id": "dataslate-wf-one", "task_id": "ingest", "map_index": -1, "try_number": 0, "state": nil}}, "total_entries": 201})
			})
			tasks, truncated, e := c.Tasks(context.Background(), targetFixture(), "dataslate-wf-one")
			if foreign {
				if e == nil {
					t.Fatal("foreign tasks accepted")
				}
			} else if e != nil || len(tasks) != 1 || tasks[0].State != "pending" || !truncated {
				t.Fatal(tasks, truncated, e)
			}
		})
	}
}

func TestAirflowDefinitionUsesFinalAdmissionFlags(t *testing.T) {
	for _, flag := range []string{"is_paused", "is_stale", "has_import_errors"} {
		t.Run(flag, func(t *testing.T) {
			reads := 0
			c, _ := clientFixture(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/api/v2/version":
					fmt.Fprint(w, `{"version":"3.4.0"}`)
				case "/api/v2/dags/approved_dag/details":
					reads++
					body := map[string]any{"dag_id": "approved_dag", "is_paused": false, "is_stale": false, "has_import_errors": false, "latest_dag_version": versionFixture()}
					if reads > 1 {
						body[flag] = true
					}
					json.NewEncoder(w).Encode(body)
				case "/api/v2/dags/approved_dag/tasks":
					fmt.Fprint(w, `{"tasks":[],"total_entries":0}`)
				}
			})
			d, e := c.Definition(context.Background(), targetFixture())
			if e != nil {
				t.Fatal(e)
			}
			if !d.Paused && !d.Stale && !d.HasImportErrors {
				t.Fatal("stale admission flags accepted")
			}
		})
	}
}
