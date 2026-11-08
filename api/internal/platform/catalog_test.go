package platform

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dataslate.local/api/internal/catalogs/polaris"
)

func catalogBinding() CatalogBinding {
	return CatalogBinding{ID: "polaris-lake", WorkspaceID: "default", Name: "Lake metadata", Kind: "polaris", PolicyVersion: "v1", Connection: polaris.Config{Endpoint: "https://polaris.example.com/api/catalog", Warehouse: "lake", Realm: "DataSlate", Auth: polaris.Auth{BearerTokenFile: "/run/secrets/polaris/token"}}, AllowedSubjects: []CatalogSubject{{ExecutionSubject: subjectFor(executionPrincipal()), Tables: []polaris.Identifier{{Namespace: []string{"analytics"}, Name: "orders"}}}}}
}

const catalogBase = "/api/v1/workspaces/default/catalog-bindings"
const catalogSelection = `{"namespace":["analytics"],"name":"orders"}`

type fakeCatalogBackend struct {
	discovered, loaded       int
	discoverError, loadError error
	noCapabilities           bool
	onDiscover, onLoad       func()
}

func (b *fakeCatalogBackend) Discover(_ context.Context, binding CatalogBinding) (polaris.Capabilities, error) {
	b.discovered++
	if b.onDiscover != nil {
		b.onDiscover()
	}
	endpoints := []string{catalogLoadEndpoint}
	if b.noCapabilities {
		endpoints = []string{}
	}
	return polaris.Capabilities{Catalog: binding.Connection.Warehouse, Prefix: binding.Connection.Warehouse, Endpoints: endpoints}, b.discoverError
}
func (b *fakeCatalogBackend) LoadTable(_ context.Context, _ CatalogBinding, id polaris.Identifier) (polaris.Table, error) {
	b.loaded++
	if b.onLoad != nil {
		b.onLoad()
	}
	snapshot := int64(9223372036854775807)
	return polaris.Table{Identifier: id, FormatVersion: 2, UUID: "12345678-1234-1234-1234-123456789abc", Location: "s3://private/raw/", MetadataLocation: "s3://private/metadata/", LastUpdatedMS: 1700000000000, CurrentSnapshotID: &snapshot, CurrentSchema: polaris.Schema{ID: 0, Fields: []polaris.Field{{ID: 1, Name: "id", Required: true, Type: json.RawMessage(`"long"`)}}}}, b.loadError
}
func catalogHandler(s Store, p Principal, b []CatalogBinding, backend CatalogBackend) http.Handler {
	return (&Server{Config: Config{AuthMode: "oidc", CatalogBindings: b}, Store: s, Auth: testAuth{p: p}, CatalogBackend: backend}).Handler()
}
func TestCatalogGrantBoundaryAndMetadataProjection(t *testing.T) {
	s := NewMemoryStore()
	b := catalogBinding()
	p := executionPrincipal()
	p.Workspaces["default"] = "viewer"
	backend := &fakeCatalogBackend{}
	h := catalogHandler(s, p, []CatalogBinding{b}, backend)
	for _, path := range []string{catalogBase, catalogBase + "/polaris-lake/namespaces", catalogBase + "/polaris-lake/tables?namespace=" + url.QueryEscape(`["analytics"]`)} {
		r := request(h, "GET", path, "")
		if r.Code != 200 {
			t.Fatal(r.Code, r.Body)
		}
		for _, secret := range []string{"polaris.example.com", "/run/secrets", `"allowedSubjects"`, `"connection"`} {
			if strings.Contains(r.Body.String(), secret) {
				t.Fatal("private configuration leaked", r.Body)
			}
		}
	}
	if backend.discovered != 0 || backend.loaded != 0 {
		t.Fatal("grant index unexpectedly queried service catalog")
	}
	r := request(h, "POST", catalogBase+"/polaris-lake/table", catalogSelection)
	if r.Code != 200 || backend.discovered != 1 || backend.loaded != 1 {
		t.Fatal(r.Code, r.Body, backend)
	}
	if !strings.Contains(r.Body.String(), `"currentSnapshotId":"9223372036854775807"`) {
		t.Fatal("snapshot precision lost", r.Body)
	}
	for _, secret := range []string{"s3://", "location", "Location", "secret", "token"} {
		if strings.Contains(r.Body.String(), secret) {
			t.Fatal("storage credential/location leaked", r.Body)
		}
	}
	r = request(h, "GET", catalogBase, "")
	if !strings.Contains(r.Body.String(), `"discoveryVerified":true`) {
		t.Fatal("real discovery not reflected", r.Body)
	}
	request(h, "POST", catalogBase+"/polaris-lake/table", catalogSelection)
	if backend.discovered != 1 {
		t.Fatal("fresh discovery needlessly repeated")
	}
	for _, body := range []string{`{"namespace":["private"],"name":"orders"}`, `{"namespace":["analytics"],"name":"other"}`} {
		r = request(h, "POST", catalogBase+"/polaris-lake/table", body)
		if r.Code != 403 {
			t.Fatal("table grant bypass", r.Code)
		}
	}
	other, _ := s.CreateWorkspace(context.Background(), "Other", "catalog-other")
	p.Workspaces[other.ID] = "viewer"
	h = catalogHandler(s, p, []CatalogBinding{b}, backend)
	r = request(h, "POST", "/api/v1/workspaces/"+other.ID+"/catalog-bindings/polaris-lake/table", catalogSelection)
	if r.Code != 403 {
		t.Fatal("cross workspace grant bypass", r.Code)
	}
	p.Issuer = "https://other.example.com"
	h = catalogHandler(s, p, []CatalogBinding{b}, backend)
	r = request(h, "POST", catalogBase+"/polaris-lake/table", catalogSelection)
	if r.Code != 403 {
		t.Fatal("cross issuer grant bypass")
	}
}
func TestCatalogDiscoveryExpiryFailuresAndCapabilities(t *testing.T) {
	for _, scenario := range []string{"expired", "config change", "discovery error", "no load", "table missing", "backend error"} {
		t.Run(scenario, func(t *testing.T) {
			s := NewMemoryStore()
			b := catalogBinding()
			backend := &fakeCatalogBackend{}
			observation := CatalogObservation{WorkspaceID: "default", BindingID: b.ID, ConfigHash: b.Hash(), Verified: true, CheckedAt: time.Now().Add(-time.Hour), Capabilities: []string{catalogLoadEndpoint}}
			if scenario == "config change" {
				observation.CheckedAt = time.Now()
				observation.ConfigHash = "old-private-hash"
			}
			s.SaveCatalogObservation(context.Background(), observation)
			expected := 200
			switch scenario {
			case "discovery error":
				backend.discoverError = errors.New("private token error")
				expected = 503
			case "no load":
				backend.noCapabilities = true
				expected = 501
			case "table missing":
				backend.loadError = &polaris.Error{StatusCode: 404}
				expected = 404
			case "backend error":
				backend.loadError = errors.New("private location error")
				expected = 503
			}
			h := catalogHandler(s, executionPrincipal(), []CatalogBinding{b}, backend)
			r := request(h, "POST", catalogBase+"/polaris-lake/table", catalogSelection)
			if r.Code != expected || backend.discovered != 1 || strings.Contains(r.Body.String(), "private") {
				t.Fatal(r.Code, r.Body, backend)
			}
			if scenario == "discovery error" {
				o, _ := s.CatalogObservation(context.Background(), "default", b.ID)
				if o.Verified {
					t.Fatal("failed discovery stayed healthy")
				}
			}
			if scenario == "no load" && backend.loaded != 0 {
				t.Fatal("unadvertised load performed")
			}
		})
	}
}
func TestCatalogInFlightRevocationFailsClosed(t *testing.T) {
	for _, stage := range []string{"discovery", "table"} {
		for _, change := range []string{"grant", "policy version", "malformed", "connection"} {
			t.Run(stage+"/"+change, func(t *testing.T) {
				s := NewMemoryStore()
				b := catalogBinding()
				file := filepath.Join(t.TempDir(), "bindings.json")
				write := func(value any) {
					data, _ := json.Marshal(value)
					if err := os.WriteFile(file, data, 0600); err != nil {
						t.Fatal(err)
					}
				}
				write([]CatalogBinding{b})
				revoke := func() {
					switch change {
					case "grant":
						b.AllowedSubjects = []CatalogSubject{}
					case "policy version":
						b.PolicyVersion = "v2"
					case "connection":
						b.Connection.Warehouse = "other"
					case "malformed":
						write(map[string]any{"invalid": true})
						return
					}
					write([]CatalogBinding{b})
				}
				backend := &fakeCatalogBackend{}
				if stage == "discovery" {
					backend.onDiscover = revoke
				} else {
					backend.onLoad = revoke
				}
				h := (&Server{Config: Config{AuthMode: "oidc", CatalogBindingsFile: file}, Store: s, Auth: testAuth{p: executionPrincipal()}, CatalogBackend: backend}).Handler()
				r := request(h, "POST", catalogBase+"/polaris-lake/table", catalogSelection)
				expected := 403
				if change == "malformed" {
					expected = 503
				}
				if r.Code != expected || strings.Contains(r.Body.String(), "currentSchema") {
					t.Fatal("in-flight response leaked", r.Code, r.Body)
				}
				if stage == "discovery" && backend.loaded != 0 {
					t.Fatal("load after revoked discovery")
				}
			})
		}
	}
}
func exerciseCatalogObservation(t *testing.T, s Store, ws string) {
	t.Helper()
	ctx := context.Background()
	b := catalogBinding()
	b.WorkspaceID = ws
	good := CatalogObservation{WorkspaceID: ws, BindingID: b.ID, ConfigHash: b.Hash(), Verified: true, CheckedAt: time.Now().UTC(), Capabilities: []string{catalogLoadEndpoint}}
	if err := s.SaveCatalogObservation(ctx, good); err != nil {
		t.Fatal(err)
	}
	stale := good
	stale.CheckedAt = stale.CheckedAt.Add(-time.Minute)
	stale.Verified = false
	s.SaveCatalogObservation(ctx, stale)
	out, err := s.CatalogObservation(ctx, ws, b.ID)
	if err != nil || !out.Fresh(b, time.Now()) {
		t.Fatal("stale callback overwrote newer observation", out, err)
	}
	out.Capabilities[0] = "mutated"
	out, _ = s.CatalogObservation(ctx, ws, b.ID)
	if out.Capabilities[0] != catalogLoadEndpoint {
		t.Fatal("shared observation mutated")
	}
	if _, err = s.CatalogObservation(ctx, "wrong-workspace", b.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("cross-workspace observation leak")
	}
	if out.Fresh(b, time.Now().Add(CatalogDiscoveryTTL)) {
		t.Fatal("expired observation healthy")
	}
	b.PolicyVersion = "v2"
	if out.Fresh(b, time.Now()) {
		t.Fatal("old policy discovery healthy")
	}
}
func TestCatalogObservationStores(t *testing.T) {
	t.Run("memory", func(t *testing.T) { exerciseCatalogObservation(t, NewMemoryStore(), "default") })
	t.Run("postgres", func(t *testing.T) {
		dsn := os.Getenv("DATASLATE_TEST_DATABASE_URL")
		if dsn == "" {
			t.Skip("isolated PostgreSQL required")
		}
		s, err := OpenPostgres(context.Background(), dsn, false)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		if err = s.Migrate(context.Background()); err != nil {
			t.Fatal(err)
		}
		ws, err := s.CreateWorkspace(context.Background(), "Catalog tests", newResource("test-").ID)
		if err != nil {
			t.Fatal(err)
		}
		exerciseCatalogObservation(t, s, ws.ID)
	})
}
func TestCatalogConfigurationAndIdentifiers(t *testing.T) {
	b := catalogBinding()
	if err := b.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*CatalogBinding){func(b *CatalogBinding) { b.Connection.Endpoint = "http://insecure" }, func(b *CatalogBinding) { b.Connection.Auth.BearerTokenFile = "relative" }, func(b *CatalogBinding) { b.AllowedSubjects = nil }, func(b *CatalogBinding) { b.PolicyVersion = "" }, func(b *CatalogBinding) { b.AllowedSubjects = append(b.AllowedSubjects, b.AllowedSubjects[0]) }, func(b *CatalogBinding) { b.AllowedSubjects[0].Tables[0].Name = "../escape" }} {
		b = catalogBinding()
		change(&b)
		if b.Validate() == nil {
			t.Fatal("invalid config accepted", b)
		}
	}
	b = catalogBinding()
	file := filepath.Join(t.TempDir(), "bindings.json")
	data, _ := json.Marshal([]CatalogBinding{b})
	os.WriteFile(file, data, 0600)
	if _, err := LoadCatalogBindings(file); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(file, []byte(strings.Replace(string(data), `"kind":"polaris"`, `"kind":"polaris","unexpected":true`, 1)), 0600)
	if _, err := LoadCatalogBindings(file); err == nil {
		t.Fatal("unknown admin config accepted")
	}
	h := catalogHandler(NewMemoryStore(), executionPrincipal(), []CatalogBinding{b}, &fakeCatalogBackend{})
	for _, body := range []string{`{"namespace":[],"name":"orders"}`, `{"namespace":["a/b"],"name":"orders"}`, `{"namespace":["analytics"],"name":"orders","endpoint":"https://evil"}`} {
		r := request(h, "POST", catalogBase+"/polaris-lake/table", body)
		if r.Code != 400 && r.Code != 422 {
			t.Fatal("invalid selector accepted", r.Code, r.Body)
		}
	}
	if r := request(h, "GET", catalogBase+"/polaris-lake/tables?namespace=[]", ""); r.Code != 422 {
		t.Fatal("empty namespace accepted")
	}
}
func TestCatalogRealTLSAdapterSafeResponse(t *testing.T) {
	discoveries, loads := 0, 0
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer catalog-token" || r.Header.Get("Polaris-Realm") != "DataSlate" || r.Header.Get("X-Iceberg-Access-Delegation") != "" {
			t.Error("wrong upstream credentials or vending requested")
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/catalog/v1/config":
			discoveries++
			if r.URL.Query().Get("warehouse") != "lake" {
				t.Error("wrong warehouse")
			}
			w.Write([]byte(`{"defaults":{"prefix":"lake","s3.secret-access-key":"private"}}`))
		case "/api/catalog/v1/lake/namespaces/analytics/tables/orders":
			loads++
			w.Write([]byte(`{"metadata-location":"s3://private/metadata.json","config":{"s3.secret-access-key":"private"},"metadata":{"format-version":2,"table-uuid":"12345678-1234-1234-1234-123456789abc","location":"s3://private/table/","last-updated-ms":1700000000000,"current-snapshot-id":9223372036854775807,"current-schema-id":0,"schemas":[{"schema-id":0,"type":"struct","fields":[{"id":1,"name":"id","required":true,"type":"long"}]}]}}`))
		default:
			t.Error("unexpected upstream request", r.URL)
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.pem")
	token := filepath.Join(dir, "token")
	os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: upstream.Certificate().Raw}), 0600)
	os.WriteFile(token, []byte("catalog-token"), 0600)
	b := catalogBinding()
	b.Connection.Endpoint = upstream.URL + "/api/catalog"
	b.Connection.CAFile = ca
	b.Connection.Auth.BearerTokenFile = token
	h := catalogHandler(NewMemoryStore(), executionPrincipal(), []CatalogBinding{b}, nil)
	r := request(h, "POST", catalogBase+"/polaris-lake/table", catalogSelection)
	if r.Code != 200 || discoveries != 1 || loads != 1 || strings.Contains(r.Body.String(), "private") || !strings.Contains(r.Body.String(), `"currentSnapshotId":"9223372036854775807"`) {
		t.Fatal(r.Code, r.Body, discoveries, loads)
	}
}
