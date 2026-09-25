package platform

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/go-jose/go-jose/v4"
)

func signToken(t *testing.T, key *rsa.PrivateKey, claims map[string]any) string {
	t.Helper()
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: key}, (&jose.SignerOptions{}).WithType("at+jwt").WithHeader("kid", "test"))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(claims)
	obj, err := signer.Sign(b)
	if err != nil {
		t.Fatal(err)
	}
	token, err := obj.CompactSerialize()
	if err != nil {
		t.Fatal(err)
	}
	return token
}
func TestJWTSignatureIssuerAudienceExpiryAndMembership(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	otherKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	jwks := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: &key.PublicKey, KeyID: "test", Algorithm: "RS256", Use: "sig"}}})
	}))
	defer jwks.Close()
	auth := &JWTAuth{verifier: oidc.NewVerifier("https://issuer.example", oidc.NewRemoteKeySet(context.Background(), jwks.URL), &oidc.Config{ClientID: "dataslate-api", SupportedSigningAlgs: []string{oidc.RS256}})}
	base := func() map[string]any {
		return map[string]any{"iss": "https://issuer.example", "sub": "alice", "aud": "dataslate-api", "exp": time.Now().Add(time.Hour).Unix(), "iat": time.Now().Unix(), "token_use": "access", "dataslate_roles": []string{"platform-admin"}, "dataslate_workspaces": map[string]string{"default": "editor"}}
	}
	token := signToken(t, key, base())
	p, err := auth.Authenticate(context.Background(), "Bearer "+token)
	if err != nil || p.Subject != "alice" || !p.Can("default", true) || p.Can("other", false) || !p.PlatformAdmin {
		t.Fatalf("valid JWT/membership failed: %v", err)
	}
	for name, change := range map[string]func(map[string]any){"wrong-audience": func(c map[string]any) { c["aud"] = "browser-client" }, "wrong-issuer": func(c map[string]any) { c["iss"] = "https://evil.example" }, "expired": func(c map[string]any) { c["exp"] = time.Now().Add(-time.Hour).Unix() }, "missing-exp": func(c map[string]any) { delete(c, "exp") }, "missing-sub": func(c map[string]any) { delete(c, "sub") }, "not-yet-valid": func(c map[string]any) { c["nbf"] = time.Now().Add(time.Hour).Unix() }, "id-token": func(c map[string]any) { c["token_use"] = "id" }, "invalid-roles": func(c map[string]any) { c["dataslate_roles"] = "platform-admin" }} {
		t.Run(name, func(t *testing.T) {
			claims := base()
			change(claims)
			if _, err := auth.Authenticate(context.Background(), "Bearer "+signToken(t, key, claims)); err == nil {
				t.Fatal("invalid JWT accepted")
			}
		})
	}
	if _, err := auth.Authenticate(context.Background(), "Bearer "+signToken(t, otherKey, base())); err == nil {
		t.Fatal("invalid signature accepted")
	}
	h := (&Server{Config: Config{AuthMode: "oidc"}, Store: NewMemoryStore(), Auth: auth}).Handler()
	r := httptest.NewRequest("GET", "/api/v1/workspaces/default/notebooks", nil)
	r.Header.Set("X-Forwarded-User", "alice")
	r.Header.Set("X-Forwarded-Groups", "platform-admin")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatal("forwarded identity headers bypassed auth")
	}
}
func TestConfigurationFailsClosed(t *testing.T) {
	good := Config{Addr: ":8080", AuthMode: "oidc", DatabaseURL: "postgres://db/dataslate?sslmode=verify-full", Issuer: "https://issuer.example", Audience: "api", ClientID: "browser"}
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*Config){func(c *Config) { c.DatabaseURL = "" }, func(c *Config) { c.Issuer = "http://issuer.example" }, func(c *Config) { c.Audience = "" }, func(c *Config) { c.Audience = c.ClientID }, func(c *Config) { c.AuthMode = "none" }, func(c *Config) { c.AuthMode = "development"; c.DatabaseURL = "" }} {
		c := good
		change(&c)
		if c.Validate() == nil {
			t.Fatalf("unsafe config accepted %+v", c)
		}
	}
	c := Config{Addr: "127.0.0.1:8080", AuthMode: "development"}
	if c.Validate() != nil {
		t.Fatal("explicit loopback development rejected")
	}
	for _, dsn := range []string{"postgres://localhost/db?sslmode=disable", "postgres://localhost/db?sslmode=require", "postgres://localhost/db?sslmode=prefer"} {
		if db, err := OpenPostgres(context.Background(), dsn, true); err == nil {
			db.Close()
			t.Fatal("unverified production TLS accepted")
		}
	}
}
func TestDevelopmentDNSRebindingAndOrigin(t *testing.T) {
	h := (&Server{Config: Config{AuthMode: "development"}, Store: NewMemoryStore(), Auth: DevelopmentAuth{}}).Handler()
	for _, tc := range []struct {
		host, origin string
		want         int
	}{{"evil.example:8080", "", 403}, {"127.0.0.1:8080", "https://evil.example", 403}, {"127.0.0.1:8080", "null", 403}, {"127.0.0.1:8080", "http://localhost:5173", 201}, {"localhost:8080", "http://127.0.0.1:5173", 201}} {
		r := httptest.NewRequest("POST", "http://"+tc.host+"/api/v1/workspaces/default/notebooks", strings.NewReader(createBody()))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Origin", tc.origin)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.want {
			t.Fatalf("%s %s: got %d want %d", tc.host, tc.origin, w.Code, tc.want)
		}
	}
}
