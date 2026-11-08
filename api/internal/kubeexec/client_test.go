package kubeexec

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestCredentialsRotateAndResponsesNeverLeakSecrets(t *testing.T) {
	want := "token-one"
	c, cfg := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+want {
			t.Errorf("wrong rotated credential")
		}
		w.WriteHeader(403)
		_, _ = w.Write([]byte("secret-from-api token-one token-two"))
	})
	for _, token := range []string{"token-one", "token-two"} {
		want = token
		if err := os.WriteFile(cfg.TokenFile, []byte(token), 0600); err != nil {
			t.Fatal(err)
		}
		err := c.Discover(context.Background())
		if err == nil || !statusIs(err, 403) {
			t.Fatalf("unexpected error: %v", err)
		}
		if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "token") {
			t.Fatalf("credential/body leaked: %v", err)
		}
	}
}

func TestDiscoveryAcceptsOnlyNamespacedApacheV1(t *testing.T) {
	for _, valid := range []bool{true, false} {
		t.Run(map[bool]string{true: "matching", false: "wrong API"}[valid], func(t *testing.T) {
			c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/apis/spark.apache.org/v1" {
					t.Errorf("unexpected discovery %s", r.URL.Path)
				}
				version := "spark.apache.org/v1"
				if !valid {
					version = "sparkoperator.k8s.io/v1beta2"
				}
				writeJSON(t, w, 200, map[string]any{"groupVersion": version, "resources": []any{map[string]any{"name": "sparkapplications", "kind": "SparkApplication", "namespaced": true, "verbs": []string{"get", "create", "delete"}}}})
			})
			err := c.Discover(context.Background())
			if (err == nil) != valid {
				t.Fatalf("unexpected discovery outcome %v", err)
			}
		})
	}
}

func TestHTTPAndInvalidCredentialsAreRejected(t *testing.T) {
	_, cfg := testClient(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	for name, mutate := range map[string]func(*Config){"http": func(c *Config) { c.ServerURL = "http://127.0.0.1:9999" }, "userinfo": func(c *Config) { c.ServerURL = "https://token@example.com" }, "path": func(c *Config) { c.ServerURL += "/api" }, "query": func(c *Config) { c.ServerURL += "?token=secret" }, "no namespaces": func(c *Config) { c.AllowedNamespaces = nil }, "invalid namespace": func(c *Config) { c.AllowedNamespaces = []string{"../secrets"} }, "missing CA": func(c *Config) { c.CAFile = "/nonexistent/ca" }, "missing token": func(c *Config) { c.TokenFile = "/nonexistent/token" }} {
		t.Run(name, func(t *testing.T) {
			bad := cfg
			mutate(&bad)
			if _, err := New(bad); err == nil {
				t.Fatal("unsafe configuration accepted")
			}
		})
	}
}

func TestRedirectDoesNotForwardCredentials(t *testing.T) {
	hits := 0
	destination := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++; w.WriteHeader(200) }))
	defer destination.Close()
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, destination.URL, 307) })
	if err := c.Discover(context.Background()); err == nil {
		t.Fatal("redirect followed")
	}
	if hits != 0 {
		t.Fatal("credential forwarded to redirect target")
	}
}

func TestUntrustedTLSAndOversizedBodiesFailClosed(t *testing.T) {
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", maxResponseBytes+1)))
	})
	if err := c.Discover(context.Background()); err == nil {
		t.Fatal("oversized response accepted")
	}
	untrusted := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("untrusted server reached") }))
	defer untrusted.Close()
	// A CA with no roots proves verification remains enabled even for loopback.
	transport := c.http.Transport.(*http.Transport).Clone()
	transport.TLSClientConfig = transport.TLSClientConfig.Clone()
	transport.TLSClientConfig.RootCAs = nil
	c.http.Transport = transport
	c.base = untrusted.URL
	if err := c.Discover(context.Background()); err == nil {
		t.Fatal("untrusted TLS accepted")
	}
}
