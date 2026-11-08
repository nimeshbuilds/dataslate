package polaris

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func fixture(t *testing.T, handler http.HandlerFunc) (*Client, *httptest.Server) {
	t.Helper()
	s := httptest.NewTLSServer(handler)
	t.Cleanup(s.Close)
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.pem")
	if e := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.Certificate().Raw}), 0600); e != nil {
		t.Fatal(e)
	}
	token := filepath.Join(dir, "token")
	if e := os.WriteFile(token, []byte("first-token"), 0600); e != nil {
		t.Fatal(e)
	}
	c, e := New(Config{Endpoint: s.URL + "/api/catalog", Warehouse: "lake", Realm: "org", CAFile: ca, Auth: Auth{BearerTokenFile: token}})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(c.Close)
	return c, s
}
func writeJSON(t *testing.T, w http.ResponseWriter, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if e := json.NewEncoder(w).Encode(v); e != nil {
		t.Error(e)
	}
}
func requireError(t *testing.T, e error, kind string) {
	t.Helper()
	var p *Error
	if !errors.As(e, &p) || p.Kind != kind {
		t.Fatalf("got error %v, want %s", e, kind)
	}
}
func TestNamespacesEncodingAndBearerRotation(t *testing.T) {
	count := 0
	c, _ := fixture(t, func(w http.ResponseWriter, r *http.Request) {
		count++
		want := "first-token"
		if count == 2 {
			want = "second-token"
		}
		if r.Header.Get("Authorization") != "Bearer "+want || r.Header.Get("Polaris-Realm") != "org" || r.Header.Get("X-Iceberg-Access-Delegation") != "" {
			t.Error("authentication/delegation mismatch")
		}
		if r.Method != "GET" {
			t.Error("not read only")
		}
		if count == 1 {
			if r.URL.Query().Get("parent") != "a\x1fb" || r.URL.Query().Get("pageToken") != "a+b/&=" || r.URL.Query().Get("pageSize") != "10" {
				t.Errorf("query %v", r.URL.Query())
			}
			writeJSON(t, w, map[string]any{"namespaces": [][]string{{"a", "b", "c"}}, "next-page-token": "next"})
		} else {
			if r.URL.EscapedPath() != "/api/catalog/v1/lake/namespaces/a%1Fb/tables" {
				t.Errorf("escaped path %s", r.URL.EscapedPath())
			}
			writeJSON(t, w, map[string]any{"identifiers": []Identifier{{[]string{"a", "b"}, "orders"}}, "next-page-token": nil})
		}
	})
	p, e := c.ListNamespaces(context.Background(), []string{"a", "b"}, "a+b/&=", 10)
	if e != nil || p.NextPageToken != "next" {
		t.Fatalf("%+v %v", p, e)
	}
	if e = os.WriteFile(c.config.Auth.BearerTokenFile, []byte("second-token\n"), 0600); e != nil {
		t.Fatal(e)
	}
	tables, e := c.ListTables(context.Background(), []string{"a", "b"}, "", 10)
	if e != nil || len(tables.Identifiers) != 1 {
		t.Fatalf("%+v %v", tables, e)
	}
}
func TestDiscoveryDoesNotAdoptServerConfiguration(t *testing.T) {
	mismatch := false
	c, _ := fixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("warehouse") != "lake" {
			t.Error("wrong warehouse")
		}
		prefix := "lake"
		if mismatch {
			prefix = "other"
		}
		writeJSON(t, w, map[string]any{"defaults": map[string]string{"prefix": prefix, "uri": "https://attacker.invalid", "token": "secret", "oauth2-server-uri": "https://attacker.invalid"}, "overrides": map[string]string{"s3.secret-access-key": "secret"}, "endpoints": []string{readEndpoints[0], "POST /v1/{prefix}/namespaces"}})
	})
	result, e := c.Discover(context.Background())
	if e != nil || len(result.Endpoints) != 1 || result.Prefix != "lake" {
		t.Fatalf("%+v %v", result, e)
	}
	b, _ := json.Marshal(result)
	if strings.Contains(string(b), "secret") || strings.Contains(string(b), "attacker") {
		t.Fatal("server config escaped projection")
	}
	mismatch = true
	_, e = c.Discover(context.Background())
	requireError(t, e, "catalog prefix mismatch")
}
func TestOAuthStylesCacheRotationAndUnauthorized(t *testing.T) {
	for _, style := range []string{"post", "basic"} {
		t.Run(style, func(t *testing.T) {
			tokens, reads := 0, 0
			reject := false
			c, s := fixture(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/oauth/token" {
					tokens++
					if r.Method != "POST" {
						t.Error("OAuth method")
					}
					if e := r.ParseForm(); e != nil {
						t.Error(e)
					}
					secret := "alpha &?"
					if tokens > 1 {
						secret = "beta +/"
					}
					if style == "post" {
						if r.Form.Get("client_id") != "id:/" || r.Form.Get("client_secret") != secret || r.Header.Get("Authorization") != "" {
							t.Error("invalid post auth")
						}
					} else {
						id, pw, ok := r.BasicAuth()
						if !ok || id != url.QueryEscape("id:/") || pw != url.QueryEscape(secret) || r.Form.Get("client_secret") != "" {
							t.Error("invalid basic auth")
						}
					}
					if r.Form.Get("scope") != "PRINCIPAL_ROLE:reader" || r.Form.Get("grant_type") != "client_credentials" {
						t.Error("invalid grant")
					}
					writeJSON(t, w, map[string]any{"access_token": fmt.Sprintf("token-%d", tokens), "token_type": "bearer", "expires_in": 3600})
					return
				}
				reads++
				if r.Header.Get("Authorization") != fmt.Sprintf("Bearer token-%d", tokens) {
					t.Error("wrong cached token")
				}
				if reject {
					w.WriteHeader(401)
					return
				}
				writeJSON(t, w, map[string]any{})
			})
			file := filepath.Join(t.TempDir(), "secret")
			if e := os.WriteFile(file, []byte("alpha &?"), 0600); e != nil {
				t.Fatal(e)
			}
			c.config.Auth = Auth{ClientID: "id:/", ClientSecretFile: file, TokenEndpoint: s.URL + "/oauth/token", Style: style, Scope: "PRINCIPAL_ROLE:reader"}
			for i := 0; i < 2; i++ {
				if _, e := c.Discover(context.Background()); e != nil {
					t.Fatal(e)
				}
			}
			if tokens != 1 {
				t.Fatal("token not cached")
			}
			if e := os.WriteFile(file, []byte("beta +/"), 0600); e != nil {
				t.Fatal(e)
			}
			if _, e := c.Discover(context.Background()); e != nil {
				t.Fatal(e)
			}
			if tokens != 2 {
				t.Fatal("secret rotation not applied")
			}
			reject = true
			_, e := c.Discover(context.Background())
			requireError(t, e, "request rejected")
			if reads != 4 || tokens != 2 {
				t.Fatal("hidden retry")
			}
			reject = false
			if _, e = c.Discover(context.Background()); e != nil {
				t.Fatal(e)
			}
			if tokens != 3 {
				t.Fatal("401 cache not cleared")
			}
		})
	}
}
func TestNoRedirectsOrCredentialLeak(t *testing.T) {
	var hits atomic.Int64
	attacker := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1); w.WriteHeader(200) }))
	defer attacker.Close()
	for _, oauth := range []bool{false, true} {
		t.Run(fmt.Sprint(oauth), func(t *testing.T) {
			c, s := fixture(t, func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, attacker.URL, http.StatusTemporaryRedirect)
			})
			if oauth {
				file := filepath.Join(t.TempDir(), "secret")
				os.WriteFile(file, []byte("client-secret"), 0600)
				c.config.Auth = Auth{ClientID: "id", ClientSecretFile: file, TokenEndpoint: s.URL + "/oauth/token"}
			}
			_, e := c.Discover(context.Background())
			var p *Error
			if !errors.As(e, &p) || p.StatusCode != 307 {
				t.Fatalf("redirect: %v", e)
			}
			if strings.Contains(e.Error(), "secret") {
				t.Fatal("error leaked credential")
			}
		})
	}
	if hits.Load() != 0 {
		t.Fatal("followed redirect")
	}
}
func tableResponse() map[string]any {
	return map[string]any{
		"metadata-location":   "s3://bucket/lake/table/metadata/v1.json",
		"config":              map[string]string{"token": "TOPSECRET", "s3.secret-access-key": "TOPSECRET"},
		"storage-credentials": []any{map[string]string{"access-key": "TOPSECRET"}},
		"metadata": map[string]any{"format-version": 2, "table-uuid": "11111111-2222-3333-4444-555555555555", "location": "s3://bucket/lake/table", "last-updated-ms": 1234, "current-schema-id": 0, "properties": map[string]string{"secret": "TOPSECRET"}, "schemas": []any{map[string]any{"schema-id": 0, "type": "struct", "fields": []any{
			map[string]any{"id": 1, "name": "id", "required": true, "type": "long", "doc": "TOPSECRET", "initial-default": "TOPSECRET"},
			map[string]any{"id": 2, "name": "nested", "required": false, "type": map[string]any{"type": "list", "element-id": 3, "element-required": false, "secret": "TOPSECRET", "element": map[string]any{"type": "struct", "secret": "TOPSECRET", "fields": []any{map[string]any{"id": 4, "name": "price", "required": false, "type": "decimal(18, 2)", "doc": "TOPSECRET"}}}}},
		}}}}}
}
func TestTableProjectionAndNamespaceEncoding(t *testing.T) {
	c, _ := fixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.EscapedPath() != "/api/catalog/v1/lake/namespaces/a%1Fb/tables/order%20items" || r.URL.Query().Get("snapshots") != "refs" {
			t.Errorf("path: %s", r.RequestURI)
		}
		writeJSON(t, w, tableResponse())
	})
	table, e := c.LoadTable(context.Background(), Identifier{[]string{"a", "b"}, "order items"})
	if e != nil {
		t.Fatal(e)
	}
	b, _ := json.Marshal(table)
	if strings.Contains(string(b), "TOPSECRET") || strings.Contains(string(b), "config") || strings.Contains(string(b), "initial-default") {
		t.Fatal("credential/config escaped projection")
	}
	if table.CurrentSchema.Fields[0].Name != "id" || !table.CurrentSchema.Fields[0].Required || table.FormatVersion != 2 {
		t.Fatal("missing schema")
	}
}
func TestRejectsMalformedAndOutOfScopeResponses(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		tables     bool
	}{
		{"missing", "{}", false}, {"null", "null", false}, {"scope", `{"namespaces":[["other","child"]]}`, false},
		{"depth", `{"namespaces":[["a","b","child"]]}`, false}, {"duplicate", `{"namespaces":[["a","b"],["a","b"]]}`, false},
		{"separator", `{"namespaces":[["a","x\u001fy"]]}`, false}, {"page-token", `{"namespaces":[],"next-page-token":"\nsecret"}`, false},
		{"table-scope", `{"identifiers":[{"namespace":["other"],"name":"orders"}]}`, true},
		{"multi-json", `{"namespaces":[]} {"namespaces":[]}`, false}, {"oversize", `{"padding":"` + strings.Repeat("x", maxResponse) + `","namespaces":[]}`, false},
		{"invalid-utf8", "{\"namespaces\":[],\"x\":\"\xff\"}", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := fixture(t, func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, tc.body) })
			var e error
			if tc.tables {
				_, e = c.ListTables(context.Background(), []string{"a"}, "", 10)
			} else {
				_, e = c.ListNamespaces(context.Background(), []string{"a"}, "", 10)
			}
			if e == nil {
				t.Fatal("accepted unsafe response")
			}
		})
	}
}
func TestInvalidConfigAndInputsNeverSend(t *testing.T) {
	count := 0
	c, _ := fixture(t, func(w http.ResponseWriter, r *http.Request) { count++; writeJSON(t, w, map[string]any{}) })
	for _, bad := range []string{"http://host/catalog", "https://user:pw@host/catalog", "https://host/catalog?token=secret", "https://host/catalog?", "https://host/catalog#fragment", "https://host/a/../b", "https://host/a/%2e%2e/b", "https://host/a%2Fb", "https://host/a//b", "https://host/%1F", "https://host/a\\b"} {
		t.Run(bad, func(t *testing.T) {
			cfg := c.config
			cfg.Endpoint = bad
			if _, e := New(cfg); e == nil {
				t.Fatal("accepted endpoint")
			}
			cfg = c.config
			cfg.Auth = Auth{ClientID: "id", ClientSecretFile: "unused", TokenEndpoint: bad}
			if _, e := New(cfg); e == nil {
				t.Fatal("accepted OAuth endpoint")
			}
		})
	}
	for _, bad := range []string{"../path", "a/b", "a%2Fb", "a\x1fb", "a?b", "a#b", "a\\b", "", strings.Repeat("a", 129)} {
		_, e := c.ListTables(context.Background(), []string{bad}, "", 10)
		if e == nil {
			t.Fatal("accepted namespace")
		}
		_, e = c.LoadTable(context.Background(), Identifier{[]string{"ok"}, bad})
		if e == nil {
			t.Fatal("accepted table")
		}
	}
	for _, tok := range []string{"\x00", "\t", strings.Repeat("x", 4097)} {
		if _, e := c.ListNamespaces(context.Background(), nil, tok, 10); e == nil {
			t.Fatal("accepted page token")
		}
	}
	if count != 0 {
		t.Fatal("invalid input reached network")
	}
}
func TestTLSValidationAndCancellation(t *testing.T) {
	c, _ := fixture(t, func(w http.ResponseWriter, r *http.Request) { writeJSON(t, w, map[string]any{}) })
	cfg := c.config
	cfg.CAFile = ""
	bad, e := New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer bad.Close()
	if _, e = bad.Discover(context.Background()); e == nil {
		t.Fatal("trusted unknown CA")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, e = c.Discover(ctx)
	requireError(t, e, "transport failed")
}
func TestInvalidOAuthResponseAndCredentialFiles(t *testing.T) {
	for _, body := range []string{`{"access_token":"abc","token_type":"bearer","expires_in":0}`, `{"access_token":"abc","token_type":"bearer","expires_in":86401}`, `{"access_token":"abc\r\nInjected","token_type":"bearer","expires_in":100}`, `{"access_token":"abc","token_type":"basic","expires_in":100}`} {
		t.Run(body, func(t *testing.T) {
			c, s := fixture(t, func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, body) })
			c.config.Auth = Auth{ClientID: "id", ClientSecretFile: c.config.Auth.BearerTokenFile, TokenEndpoint: s.URL}
			_, e := c.Discover(context.Background())
			requireError(t, e, "invalid OAuth response")
		})
	}
	c, _ := fixture(t, func(w http.ResponseWriter, r *http.Request) { t.Error("bad file reached network") })
	for _, contents := range []string{"", "bad\ntoken", strings.Repeat("x", 16385)} {
		os.WriteFile(c.config.Auth.BearerTokenFile, []byte(contents), 0600)
		if _, e := c.Discover(context.Background()); e == nil {
			t.Fatal("accepted token file")
		}
	}
	c.config.Auth.BearerTokenFile = t.TempDir()
	if _, e := c.Discover(context.Background()); e == nil {
		t.Fatal("accepted directory")
	}
}

func TestRejectsUnsafeTableMetadata(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(map[string]any)
	}{
		{"version", func(m map[string]any) { m["format-version"] = 4 }},
		{"uuid", func(m map[string]any) { m["table-uuid"] = "invalid" }},
		{"storage-url", func(m map[string]any) { m["location"] = "https://attacker.invalid/?token=secret" }},
		{"storage-secret", func(m map[string]any) { m["location"] = "s3://user:password@bucket/path" }},
		{"schema-missing", func(m map[string]any) { m["current-schema-id"] = 99 }},
		{"field-required", func(m map[string]any) {
			delete(m["schemas"].([]any)[0].(map[string]any)["fields"].([]any)[0].(map[string]any), "required")
		}},
		{"decimal", func(m map[string]any) {
			m["schemas"].([]any)[0].(map[string]any)["fields"].([]any)[0].(map[string]any)["type"] = "decimal(99, 2)"
		}},
		{"nested-url", func(m map[string]any) {
			m["schemas"].([]any)[0].(map[string]any)["fields"].([]any)[0].(map[string]any)["type"] = map[string]any{"type": "remote", "uri": "https://attacker.invalid"}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := tableResponse()
			tc.change(body["metadata"].(map[string]any))
			c, _ := fixture(t, func(w http.ResponseWriter, r *http.Request) { writeJSON(t, w, body) })
			if _, e := c.LoadTable(context.Background(), Identifier{[]string{"a"}, "orders"}); e == nil {
				t.Fatal("accepted unsafe table metadata")
			}
		})
	}
}

func TestValidateConfigDoesNotReadFiles(t *testing.T) {
	config := Config{Endpoint: "https://catalog.invalid/api/catalog", Warehouse: "lake", Realm: "org", CAFile: "/missing/ca", Auth: Auth{BearerTokenFile: "/missing/token"}}
	if e := ValidateConfig(config); e != nil {
		t.Fatal(e)
	}
	if _, e := New(config); e == nil {
		t.Fatal("New did not validate CA contents")
	}
	config.Auth.ClientID = "ambiguous"
	if e := ValidateConfig(config); e == nil {
		t.Fatal("ambiguous auth accepted")
	}
}
func TestMetadataIntegerBounds(t *testing.T) {
	for _, key := range []string{"current-schema-id", "last-updated-ms", "field-id", "nested-id"} {
		t.Run(key, func(t *testing.T) {
			body := tableResponse()
			m := body["metadata"].(map[string]any)
			switch key {
			case "current-schema-id":
				m[key] = int64(2147483648)
			case "last-updated-ms":
				m[key] = int64(9007199254740992)
			case "field-id":
				m["schemas"].([]any)[0].(map[string]any)["fields"].([]any)[0].(map[string]any)["id"] = int64(2147483648)
			case "nested-id":
				m["schemas"].([]any)[0].(map[string]any)["fields"].([]any)[1].(map[string]any)["type"].(map[string]any)["element-id"] = int64(2147483648)
			}
			c, _ := fixture(t, func(w http.ResponseWriter, r *http.Request) { writeJSON(t, w, body) })
			if _, e := c.LoadTable(context.Background(), Identifier{[]string{"a"}, "orders"}); e == nil {
				t.Fatal("accepted lossy or invalid metadata ID")
			}
		})
	}
}
