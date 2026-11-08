package trino

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"
)

// This test requires the disposable TPCH policy fixture described in deploy/sql.
// Its generated tokens/CA are supplied by file; no production credentials belong
// in CI. Skipping is explicit and is not evidence of live qualification.
func TestLiveGovernedFixture(t *testing.T) {
	endpoint := os.Getenv("DATASLATE_TEST_TRINO_ENDPOINT")
	if endpoint == "" {
		t.Skip("requires isolated pinned Trino policy fixture")
	}
	client, err := New(Config{Endpoint: endpoint, CAFile: os.Getenv("DATASLATE_TEST_TRINO_CA_FILE"), TokenFile: os.Getenv("DATASLATE_TEST_TRINO_TOKEN_FILE")})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	stmt := Statement{User: "team-alice", Catalog: "tpch", Schema: "tiny", TraceToken: "dataslate-live-qualification", MaxRunSeconds: 60}
	execute := func(sql string) (string, [][]json.RawMessage, *QueryError) {
		t.Helper()
		stmt.SQL = sql
		page, err := client.Submit(ctx, stmt)
		if err != nil {
			t.Fatal(err)
		}
		id := page.ID
		rows := page.Rows
		for count := 0; page.NextURI != "" && page.Error == nil; count++ {
			if count >= 100 {
				t.Fatal("too many fixture pages")
			}
			page, err = client.Next(ctx, page.Cursor, stmt.User)
			if err != nil {
				t.Fatal(err)
			}
			rows = append(rows, page.Rows...)
		}
		return id, rows, page.Error
	}
	id, rows, queryErr := execute("SELECT nationkey,name,regionkey FROM tpch.tiny.nation ORDER BY nationkey")
	if queryErr != nil || len(rows) != 5 {
		t.Fatalf("row policy failed: %d rows, error=%v", len(rows), queryErr)
	}
	for _, row := range rows {
		if len(row) != 3 || string(row[1]) != `"REDACTED"` || string(row[2]) != "1" {
			t.Fatal("mask/filter not applied")
		}
	}
	observed, err := client.Observe(ctx, id, stmt.User)
	if err != nil || observed.State != "FINISHED" {
		t.Fatalf("terminal observation failed: %+v %v", observed, err)
	}
	_, rows, queryErr = execute("SELECT CAST(9007199254740993 AS BIGINT), CAST('12345678901234567890.123456789' AS DECIMAL(38,9))")
	if queryErr != nil || len(rows) != 1 || string(rows[0][0]) != "9007199254740993" || string(rows[0][1]) != `"12345678901234567890.123456789"` {
		t.Fatalf("exact scalar values lost: %s, error=%v", rows, queryErr)
	}
	for _, sql := range []string{"CREATE TABLE tpch.tiny.denied AS SELECT 1", "SELECT * FROM tpch.tiny.region", "SELECT * FROM system.runtime.nodes", "WITH SESSION query_max_run_time='2h' SELECT 1"} {
		_, _, e := execute(sql)
		if e == nil || e.Name != "PERMISSION_DENIED" {
			t.Fatalf("expected engine policy denial: %v", e)
		}
	}
	stmt.SQL = "SELECT count(*) FROM tpch.tiny.nation a CROSS JOIN tpch.tiny.nation b CROSS JOIN tpch.tiny.nation c CROSS JOIN tpch.tiny.nation d CROSS JOIN tpch.tiny.nation e CROSS JOIN tpch.tiny.nation f CROSS JOIN tpch.tiny.nation g CROSS JOIN tpch.tiny.nation h CROSS JOIN tpch.tiny.nation i CROSS JOIN tpch.tiny.nation j CROSS JOIN tpch.tiny.nation k CROSS JOIN tpch.tiny.nation l"
	page, err := client.Submit(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	if page.NextURI == "" {
		t.Fatal("cancellation fixture needs an active cursor")
	}
	if err = client.Cancel(ctx, page.Cursor, stmt.User); err != nil {
		t.Fatal(err)
	}
	for attempts := 0; attempts < 30; attempts++ {
		// A queued statement is dispatched on its first GET. Cancellation
		// registers a callback for that dispatch; observe its statement result
		// before falling back to the coordinator's retained query info.
		next, nextErr := client.Next(ctx, page.Cursor, stmt.User)
		if nextErr == nil {
			if next.Error != nil {
				if next.Error.Name != "USER_CANCELED" {
					t.Fatalf("wrong cancellation cause: %+v", next.Error)
				}
				return
			}
			if next.NextURI != "" {
				page = next
			}
		}
		state, err := client.Observe(ctx, page.ID, stmt.User)
		if err != nil {
			t.Fatal(err)
		}
		if state.State == "FAILED" {
			if state.ErrorName != "USER_CANCELED" {
				t.Fatalf("wrong cancellation cause: %+v", state)
			}
			return
		}
		if state.State == "FINISHED" {
			t.Fatal("fixture finished before cancellation; not a cancellation proof")
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("cancellation was acknowledged but not observed terminal")
}
