package platform

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"
)

// Explicitly opt in against the local read-only Trino qualification fixture.
func TestSQLLiveAPIWithPostgres(t *testing.T) {
	endpoint := os.Getenv("DATASLATE_TEST_TRINO_ENDPOINT")
	dsn := os.Getenv("DATASLATE_TEST_DATABASE_URL")
	if endpoint == "" || dsn == "" {
		t.Skip("isolated Trino endpoint and PostgreSQL required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s, err := OpenPostgres(ctx, dsn, false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	ws, err := s.CreateWorkspace(ctx, "Live SQL API test", newResource("test-").ID)
	if err != nil {
		t.Fatal(err)
	}
	b := sqlBinding()
	b.WorkspaceID = ws.ID
	b.Endpoint = endpoint
	b.CAFile = os.Getenv("DATASLATE_TEST_TRINO_CA")
	b.TokenFile = os.Getenv("DATASLATE_TEST_TRINO_TOKEN_FILE")
	b.Catalog = "tpch"
	b.Schema = "tiny"
	b.MaxRunSeconds = 45
	b.AllowedSubjects[0].EngineUser = "team-alice"
	if err = b.Validate(); err != nil {
		t.Fatal(err)
	}
	p := executionPrincipal()
	p.Workspaces = map[string]string{ws.ID: "editor"}
	h := queryHandler(s, p, []SQLBinding{b})
	input := sqlNotebook()
	input.Cells[0].Source = "SELECT nationkey,name,regionkey,CAST('9223372036854775807' AS BIGINT) AS exact_value FROM tpch.tiny.nation ORDER BY nationkey"
	notebook, err := s.SaveNotebook(ctx, ws.ID, "", input)
	if err != nil {
		t.Fatal(err)
	}
	in := QueryInput{BindingID: b.ID, IdempotencyKey: newResource("live-").ID, Notebook: SQLSelection{ID: notebook.ID, Revision: notebook.Revision, CellID: notebook.Cells[0].ID}}
	body, _ := json.Marshal(in)
	base := "/api/v1/workspaces/" + ws.ID + "/queries"
	response := request(h, "POST", base, string(body))
	if response.Code != 202 {
		t.Fatal(response.Code, response.Body)
	}
	var q Query
	if json.Unmarshal(response.Body.Bytes(), &q) != nil {
		t.Fatal("invalid accepted query")
	}
	if err = s.DeleteNotebook(ctx, ws.ID, notebook.ID, notebook.Revision); err != nil {
		t.Fatal(err)
	}
	backend := &FileSQLBackend{Bindings: []SQLBinding{b}}
	for !q.Terminal() {
		if _, err = ReconcileQueryOnce(ctx, s, backend, "live-api-test"); err != nil {
			t.Fatal(err)
		}
		q, err = s.Query(ctx, ws.ID, q.ID, subjectFor(p))
		if err != nil {
			t.Fatal(err)
		}
		if !q.Terminal() {
			select {
			case <-ctx.Done():
				t.Fatal("live query did not complete", q.State, q.Message)
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
	if q.State != "succeeded" {
		t.Fatal("live query failed", q.State, q.Message)
	}
	response = request(h, "GET", base+"/"+q.ID+"/result", "")
	if response.Code != 200 {
		t.Fatal(response.Code, response.Body)
	}
	var result QueryResult
	if json.Unmarshal(response.Body.Bytes(), &result) != nil || !result.Complete || result.Truncated || result.RowCount != 5 {
		t.Fatal("governed result invalid", result.RowCount, result.Complete, result.Truncated)
	}
	for _, row := range result.Rows {
		if len(row) != 4 || row[3] == nil || *row[3] != "9223372036854775807" {
			t.Fatal("exact numeric value changed")
		}
		if row[1] == nil || *row[1] != "REDACTED" || row[2] == nil || *row[2] != "1" {
			t.Fatal("engine row policy not enforced")
		}
	}
	response = request(h, "GET", base+"/"+q.ID+"/source", "")
	if response.Code != 200 {
		t.Fatal("deleted notebook source snapshot lost")
	}
	b.PolicyVersion = "v2"
	h = queryHandler(s, p, []SQLBinding{b})
	response = request(h, "GET", base+"/"+q.ID+"/result", "")
	if response.Code != 403 {
		t.Fatal("policy version did not revoke retained rows")
	}
	t.Log("real Trino TLS/JWT query via API and PostgreSQL: five policy-filtered rows; exact bigint, deleted-source snapshot, and retained-result revocation verified")
}
