package trino

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

func publicationTarget() PublishTarget {
	return PublishTarget{Catalog: "lake", Schema: "team", Table: "orders", Location: "s3://team-lake/orders/", Version: "9007199254740993", Fields: []DeltaField{{"id", "long"}, {"region", "string"}}, MinReaderVersion: 1, MinWriterVersion: 2}
}

func metadataFixture(sql string) map[string]any {
	page := map[string]any{"id": "metadata_123"}
	column := func(name, kind string) map[string]any { return map[string]any{"name": name, "type": kind} }
	switch {
	case strings.Contains(sql, "$properties"):
		page["columns"] = []map[string]any{column("key", "varchar"), column("value", "varchar")}
		page["data"] = [][]any{{"location", "s3://team-lake/orders"}, {"delta.minReaderVersion", "1"}, {"delta.minWriterVersion", "2"}}
	case strings.Contains(sql, "$history"):
		page["columns"] = []map[string]any{column("version", "bigint")}
		page["data"] = [][]any{{json.Number("9007199254740993")}}
	case strings.HasPrefix(sql, "DESCRIBE"):
		page["columns"] = []map[string]any{column("Column", "varchar"), column("Type", "varchar"), column("Extra", "varchar"), column("Comment", "varchar")}
		page["data"] = [][]any{{"id", "bigint", "", ""}, {"region", "varchar", "", ""}}
	default:
		panic("unexpected metadata query")
	}
	return page
}

func TestDeltaRegistrationIsOneGeneratedMutation(t *testing.T) {
	var calls atomic.Int32
	client, _, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		body, _ := io.ReadAll(r.Body)
		want := `CALL "lake".system.register_table(schema_name => 'team', table_name => 'orders', table_location => 's3://team-lake/orders')`
		if r.Method != "POST" || string(body) != want || r.Header.Get("X-Trino-User") != "publisher" || r.Header.Get("X-Trino-Trace-Token") != "publication-test" {
			t.Errorf("unexpected generated registration request: %s %s", r.Method, body)
		}
		// An unknown dispatch is an error. It must not trigger a second POST.
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	if _, err := client.Register(context.Background(), publicationTarget(), "publisher", "publication-test"); !errors.Is(err, ErrTransport) {
		t.Fatalf("error: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatal("registration was retried")
	}
}

func TestDeltaTargetRejectsInjectionUnsupportedPrecisionAndNoncanonicalValues(t *testing.T) {
	mutations := []func(*PublishTarget){
		func(v *PublishTarget) { v.Catalog = `lake"; DROP TABLE orders` },
		func(v *PublishTarget) { v.Schema = "team.other" },
		func(v *PublishTarget) { v.Table = "orders$history" },
		func(v *PublishTarget) { v.Location = "s3://team-lake/a/../orders/" },
		func(v *PublishTarget) { v.Location = "s3://team-lake/orders%27/" },
		func(v *PublishTarget) { v.Location = "s3://team-lake/orders/?key=secret" },
		func(v *PublishTarget) { v.Location = "s3://user@team-lake/orders/" },
		func(v *PublishTarget) { v.Location = "s3://127.0.0.1/orders/" },
		func(v *PublishTarget) { v.Location = "file:///lake/orders/" },
		func(v *PublishTarget) { v.Location = "s3://team-lake/" },
		func(v *PublishTarget) { v.Version = "01" },
		func(v *PublishTarget) { v.Version = "1e3" },
		func(v *PublishTarget) { v.Version = "9223372036854775808" },
		func(v *PublishTarget) { v.Fields = []DeltaField{{"ts", "timestamp"}} },
		func(v *PublishTarget) { v.Fields = []DeltaField{{"id", "long"}, {"ID", "long"}} },
		func(v *PublishTarget) { v.ReaderFeatures = []string{"deletionVectors"} },
		func(v *PublishTarget) { v.WriterFeatures = []string{"appendOnly"} },
		func(v *PublishTarget) { v.MinReaderVersion = 3 },
	}
	var calls atomic.Int32
	client, _, _ := testClient(t, func(http.ResponseWriter, *http.Request) { calls.Add(1) })
	for i, mutation := range mutations {
		v := publicationTarget()
		mutation(&v)
		if _, err := client.Register(context.Background(), v, "publisher", "test"); !errors.Is(err, ErrDeltaTarget) {
			t.Errorf("mutation %d: %v", i, err)
		}
		if _, err := client.InspectDelta(context.Background(), v, "publisher", "test"); !errors.Is(err, ErrDeltaTarget) {
			t.Errorf("inspection mutation %d: %v", i, err)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid target touched network")
	}
	v := publicationTarget()
	v.Location = "file:///lake/orders/"
	v.AllowLocalQualification = true
	if v.Validate() != nil {
		t.Fatal("explicit local qualification rejected")
	}
	encoded, _ := json.Marshal(v)
	if strings.Contains(string(encoded), "AllowLocalQualification") {
		t.Fatal("local option exposed on wire")
	}
}

func TestDeltaReadbackVerifiesLocationVersionSchemaWithoutClaimingUUID(t *testing.T) {
	var sqls []string
	client, _, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		sql := string(body)
		sqls = append(sqls, sql)
		if r.Method != "POST" || r.Header.Get("X-Trino-User") != "publisher" {
			t.Error("metadata identity changed")
		}
		_ = json.NewEncoder(w).Encode(metadataFixture(sql))
	})
	got, err := client.InspectDelta(context.Background(), publicationTarget(), "publisher", "publication-test")
	if err != nil {
		t.Fatal(err)
	}
	if !got.Exists || !got.VersionPresent || got.Location != "s3://team-lake/orders" || !reflect.DeepEqual(got.Fields, publicationTarget().Fields) {
		t.Fatalf("wrong readback: %+v", got)
	}
	want := []string{`SELECT key, value FROM "lake"."team"."orders$properties"`, `SELECT version FROM "lake"."team"."orders$history" WHERE version = 9007199254740993 LIMIT 2`, `DESCRIBE "lake"."team"."orders"`}
	if !reflect.DeepEqual(sqls, want) {
		t.Fatalf("unexpected SQL: %v", sqls)
	}
}

func TestDeltaOnlyExplicitFirstTargetNotFoundMeansAbsent(t *testing.T) {
	for _, name := range []string{"TABLE_NOT_FOUND", "SCHEMA_NOT_FOUND", "PERMISSION_DENIED", "CATALOG_NOT_FOUND", "INTERNAL_ERROR"} {
		t.Run(name, func(t *testing.T) {
			client, _, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintf(w, `{"id":"metadata_123","error":{"errorCode":1,"errorName":%q,"errorType":"USER_ERROR"}}`, name)
			})
			got, err := client.InspectDelta(context.Background(), publicationTarget(), "publisher", "test")
			absent := name == "TABLE_NOT_FOUND" || name == "SCHEMA_NOT_FOUND"
			if absent {
				if err != nil || got.Exists {
					t.Fatalf("not absent: %+v %v", got, err)
				}
			} else if err == nil {
				t.Fatal("denial treated as absence")
			}
		})
	}
	client, _, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), "$history") {
			fmt.Fprint(w, `{"id":"metadata_123","error":{"errorCode":1,"errorName":"TABLE_NOT_FOUND","errorType":"USER_ERROR"}}`)
			return
		}
		_ = json.NewEncoder(w).Encode(metadataFixture(string(body)))
	})
	if _, err := client.InspectDelta(context.Background(), publicationTarget(), "publisher", "test"); err == nil {
		t.Fatal("later metadata failure treated as absence")
	}
}

func TestDeltaReadbackRejectsMismatchedOrAmbiguousMetadata(t *testing.T) {
	for _, name := range []string{"wrong_location", "duplicate_location", "wrong_protocol", "noncanonical_protocol", "unknown_feature", "wrong_feature_state", "missing_version", "duplicate_version", "string_version", "changed_schema", "extra_column", "timestamp", "metadata_type"} {
		t.Run(name, func(t *testing.T) {
			client, _, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				sql := string(body)
				p := metadataFixture(sql)
				rows := p["data"].([][]any)
				if strings.Contains(sql, "$properties") {
					switch name {
					case "wrong_location":
						rows[0][1] = "s3://other-lake/orders"
					case "duplicate_location":
						rows = append(rows, rows[0])
					case "wrong_protocol":
						rows[1][1] = "2"
					case "noncanonical_protocol":
						rows[1][1] = "01"
					case "unknown_feature":
						rows = append(rows, []any{"delta.feature.deletionVectors", "supported"})
					case "wrong_feature_state":
						rows = append(rows, []any{"delta.feature.appendOnly", "enabled"})
					case "metadata_type":
						p["columns"].([]map[string]any)[0]["type"] = "bigint"
					}
				}
				if strings.Contains(sql, "$history") {
					switch name {
					case "missing_version":
						rows = nil
					case "duplicate_version":
						rows = append(rows, rows[0])
					case "string_version":
						rows[0][0] = "9007199254740993"
					}
				}
				if strings.HasPrefix(sql, "DESCRIBE") {
					switch name {
					case "changed_schema":
						rows[0][0] = "other"
					case "extra_column":
						rows = append(rows, []any{"extra", "bigint", "", ""})
					case "timestamp":
						rows[0][1] = "timestamp(3) with time zone"
					}
				}
				p["data"] = rows
				_ = json.NewEncoder(w).Encode(p)
			})
			if _, err := client.InspectDelta(context.Background(), publicationTarget(), "publisher", "test"); !errors.Is(err, ErrDeltaMetadata) {
				t.Fatalf("accepted mismatch: %v", err)
			}
		})
	}
}

func TestDeltaFeaturesAreVerifiedAsUnion(t *testing.T) {
	target := publicationTarget()
	target.MinReaderVersion = 3
	target.MinWriterVersion = 7
	target.ReaderFeatures = []string{"deletionVectors"}
	target.WriterFeatures = []string{"deletionVectors", "appendOnly"}
	client, _, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		p := metadataFixture(string(body))
		if strings.Contains(string(body), "$properties") {
			p["data"] = [][]any{{"location", target.Location}, {"delta.minReaderVersion", "3"}, {"delta.minWriterVersion", "7"}, {"delta.feature.appendOnly", "supported"}, {"delta.feature.deletionVectors", "supported"}}
		}
		_ = json.NewEncoder(w).Encode(p)
	})
	got, err := client.InspectDelta(context.Background(), target, "publisher", "test")
	if err != nil || !reflect.DeepEqual(got.Features, []string{"appendOnly", "deletionVectors"}) {
		t.Fatalf("wrong feature union: %+v %v", got, err)
	}
}

func TestDeltaMetadataPagingIsBoundedAndCancelsLastCursor(t *testing.T) {
	for _, mode := range []string{"repeated_cursor", "changed_columns", "too_many_rows", "error_with_cursor"} {
		t.Run(mode, func(t *testing.T) {
			var origin string
			var cancels atomic.Int32
			client, server, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "DELETE" {
					cancels.Add(1)
					w.WriteHeader(204)
					return
				}
				p := metadataFixture(`SELECT key, value FROM "lake"."team"."orders$properties"`)
				p["nextUri"] = origin + "/v1/statement/executing/metadata_123/token/1"
				if r.Method == "GET" {
					switch mode {
					case "changed_columns":
						p["columns"].([]map[string]any)[0]["name"] = "changed"
					case "too_many_rows":
						rows := make([][]any, 1025)
						for i := range rows {
							rows[i] = []any{fmt.Sprint(i), "x"}
						}
						p["data"] = rows
					case "error_with_cursor":
						p["data"] = nil
						p["error"] = map[string]any{"errorCode": 1, "errorName": "TABLE_NOT_FOUND", "errorType": "USER_ERROR"}
					}
				}
				_ = json.NewEncoder(w).Encode(p)
			})
			origin = server.URL
			if _, err := client.InspectDelta(context.Background(), publicationTarget(), "publisher", "test"); !errors.Is(err, ErrDeltaMetadata) {
				t.Fatalf("expected bounded metadata error: %v", err)
			}
			if cancels.Load() != 1 {
				t.Fatal("unfinished metadata query not cancelled")
			}
		})
	}
}
