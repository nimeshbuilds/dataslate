package platform

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"dataslate.local/api/internal/catalogs/polaris"
)

// Explicitly opt in against isolated fixtures, never a production catalog.
func TestCatalogLiveAPIWithPostgres(t *testing.T) {
	configPath := os.Getenv("DATASLATE_TEST_POLARIS_CONFIG")
	dsn := os.Getenv("DATASLATE_TEST_DATABASE_URL")
	if configPath == "" || dsn == "" {
		t.Skip("isolated Polaris config and PostgreSQL required")
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal("fixture config unavailable")
	}
	var config polaris.Config
	if json.Unmarshal(data, &config) != nil {
		t.Fatal("invalid fixture config")
	}
	ctx := context.Background()
	s, err := OpenPostgres(ctx, dsn, false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	ws, err := s.CreateWorkspace(ctx, "Live Polaris API test", newResource("test-").ID)
	if err != nil {
		t.Fatal(err)
	}
	b := catalogBinding()
	b.WorkspaceID = ws.ID
	b.Connection = config
	b.AllowedSubjects[0].Tables = []polaris.Identifier{{Namespace: []string{"engineering", "raw"}, Name: "orders"}}
	p := executionPrincipal()
	p.Workspaces = map[string]string{ws.ID: "viewer"}
	h := catalogHandler(s, p, []CatalogBinding{b}, nil)
	base := "/api/v1/workspaces/" + ws.ID + "/catalog-bindings/polaris-lake"
	response := request(h, "POST", base+"/table", `{"namespace":["engineering","raw"],"name":"orders"}`)
	if response.Code != 200 {
		t.Fatal("live metadata read failed", response.Code, response.Body)
	}
	var table struct {
		BindingID, WorkspaceID string
		CurrentSchema          polaris.Schema
	}
	if json.Unmarshal(response.Body.Bytes(), &table) != nil || table.BindingID != b.ID || table.WorkspaceID != ws.ID || len(table.CurrentSchema.Fields) != 2 {
		t.Fatal("live metadata mismatch")
	}
	if strings.Contains(response.Body.String(), "s3://") || strings.Contains(response.Body.String(), "secret") || strings.Contains(response.Body.String(), "metadataLocation") {
		t.Fatal("unsafe metadata projection")
	}
	observation, err := s.CatalogObservation(ctx, ws.ID, b.ID)
	if err != nil || !observation.Fresh(b, observation.CheckedAt) {
		t.Fatal("discovery not persisted", err)
	}
	b.AllowedSubjects = []CatalogSubject{}
	h = catalogHandler(s, p, []CatalogBinding{b}, nil)
	response = request(h, "POST", base+"/table", `{"namespace":["engineering","raw"],"name":"orders"}`)
	if response.Code != 403 {
		t.Fatal("revoked live table remained visible")
	}
	t.Log("real TLS/OAuth Polaris metadata through API and PostgreSQL: schema verified; current grant revocation enforced")
}
