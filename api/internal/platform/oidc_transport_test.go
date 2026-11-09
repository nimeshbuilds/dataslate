package platform

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"io"
	"log"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
)

func oidcTestServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	s := httptest.NewUnstartedServer(handler)
	s.Config.ErrorLog = log.New(io.Discard, "", 0)
	s.StartTLS()
	t.Cleanup(s.Close)
	return s
}

func oidcTestCA(t *testing.T, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "issuer-ca.pem")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func oidcTestCertificate(s *httptest.Server) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.Certificate().Raw})
}

func TestOIDCPrivateCAIsScopedAndRefreshesKeys(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	var hits atomic.Int64
	var signingKey atomic.Pointer[rsa.PrivateKey]
	signingKey.Store(key)
	jwks := oidcTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: &signingKey.Load().PublicKey, KeyID: "test", Algorithm: "RS256", Use: "sig"}}})
	})
	var issuer *httptest.Server
	issuer = oidcTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"issuer": issuer.URL, "jwks_uri": jwks.URL, "id_token_signing_alg_values_supported": []string{"RS256"}})
	})
	ca := oidcTestCA(t, oidcTestCertificate(issuer))
	config := Config{Issuer: issuer.URL, Audience: "dataslate-api", OIDCCAFile: ca}
	auth, err := NewJWTAuth(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	// A distinct HTTPS origin advertised in trusted metadata is legitimate.
	// The verifier must retain the scoped CA for the subsequent JWKS fetch.
	token := signToken(t, key, map[string]any{"iss": issuer.URL, "sub": "alice", "aud": config.Audience, "exp": time.Now().Add(time.Minute).Unix(), "dataslate_workspaces": map[string]string{"workspace-one": "editor"}})
	p, err := auth.Authenticate(context.Background(), "Bearer "+token)
	if err != nil || !p.Can("workspace-one", true) || hits.Load() != 1 {
		t.Fatalf("scoped discovery/JWKS trust failed: %v, key requests %d", err, hits.Load())
	}
	rotated, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	signingKey.Store(rotated)
	token = signToken(t, rotated, map[string]any{"iss": issuer.URL, "sub": "alice", "aud": config.Audience, "exp": time.Now().Add(time.Minute).Unix()})
	if _, err := auth.Authenticate(context.Background(), "Bearer "+token); err != nil || hits.Load() != 2 {
		t.Fatalf("scoped trust did not survive key rotation: %v, requests %d", err, hits.Load())
	}
	// Never alter the global transport or make an unconfigured issuer trusted.
	config.OIDCCAFile = ""
	if _, err := NewJWTAuth(context.Background(), config); err == nil {
		t.Fatal("private issuer accepted without its configured CA")
	}
	if resp, err := http.Get(issuer.URL); err == nil {
		resp.Body.Close()
		t.Fatal("OIDC trust leaked into the global HTTP client")
	}
	roots, err := oidcTrustRoots(ca)
	if err != nil {
		t.Fatal(err)
	}
	system, err := x509.SystemCertPool()
	if err != nil {
		t.Fatal(err)
	}
	for _, subject := range system.Subjects() {
		found := false
		for _, actual := range roots.Subjects() {
			if string(subject) == string(actual) {
				found = true
				break
			}
		}
		if !found {
			t.Fatal("adding issuer trust removed a system root")
		}
	}
}

func TestOIDCTrustFilesFailClosedWithoutDisclosure(t *testing.T) {
	server := oidcTestServer(t, func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("private-provider-response")) })
	valid := oidcTestCertificate(server)
	for name, data := range map[string][]byte{
		"empty": {}, "garbage": []byte("private-certificate-value"),
		"mixed-garbage":       append(append([]byte{}, valid...), []byte("private-certificate-value")...),
		"leading-garbage":     append([]byte("private-certificate-value"), valid...),
		"not-certificate":     pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte("private-certificate-value")}),
		"invalid-certificate": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte("private-certificate-value")}),
		"large":               []byte(strings.Repeat("x", maxOIDCDocumentBytes+1)),
	} {
		t.Run(name, func(t *testing.T) {
			file := oidcTestCA(t, data)
			_, err := NewJWTAuth(context.Background(), Config{Issuer: server.URL, OIDCCAFile: file})
			if err == nil || strings.Contains(err.Error(), file) || strings.Contains(err.Error(), "private-") {
				t.Fatal("invalid CA did not fail with a redacted configuration error")
			}
		})
	}
	for _, file := range []string{filepath.Join(t.TempDir(), "missing-sensitive-path"), t.TempDir(), os.DevNull} {
		if _, err := newOIDCHTTPClient(file); err == nil || strings.Contains(err.Error(), file) {
			t.Fatal("unreadable/non-regular CA did not fail closed")
		}
	}
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "unrelated test root"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	wrong := oidcTestCA(t, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	if _, err := NewJWTAuth(context.Background(), Config{Issuer: server.URL, OIDCCAFile: wrong}); err == nil {
		t.Fatal("unrelated CA authenticated issuer")
	}
}

func TestOIDCDiscoveryAndKeyEndpointsStayHTTPSAndBounded(t *testing.T) {
	var redirects atomic.Int64
	target := oidcTestServer(t, func(w http.ResponseWriter, r *http.Request) { redirects.Add(1) })
	var server *httptest.Server
	server = oidcTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")[0] {
		case "redirect":
			http.Redirect(w, r, target.URL, http.StatusFound)
		case "loop":
			http.Redirect(w, r, server.URL+r.URL.Path, http.StatusFound)
		case "downgrade":
			http.Redirect(w, r, "http://127.0.0.1:9/", http.StatusFound)
		case "oversized":
			w.Write([]byte(strings.Repeat("x", maxOIDCDocumentBytes+1)))
		case "upstream-error":
			http.Error(w, "private-provider-response", http.StatusInternalServerError)
		default:
			json.NewEncoder(w).Encode(map[string]any{"issuer": server.URL + "/" + strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")[0], "jwks_uri": "http://127.0.0.1:9/keys"})
		}
	})
	ca := oidcTestCA(t, oidcTestCertificate(server))
	for _, mode := range []string{"redirect", "loop", "downgrade", "oversized", "upstream-error", "http-keys"} {
		t.Run(mode, func(t *testing.T) {
			_, err := NewJWTAuth(context.Background(), Config{Issuer: server.URL + "/" + mode, OIDCCAFile: ca})
			if err == nil || strings.Contains(err.Error(), server.URL) || strings.Contains(err.Error(), "private-provider-response") {
				t.Fatal("unsafe discovery accepted or diagnostic disclosed provider data")
			}
		})
	}
	if redirects.Load() != 0 {
		t.Fatal("cross-origin redirect was contacted")
	}
	client, err := newOIDCHTTPClient(ca)
	if err != nil {
		t.Fatal(err)
	}
	for _, endpoint := range []string{"http://127.0.0.1:9/", "https://secret@127.0.0.1:9/", server.URL + "#fragment"} {
		if resp, err := client.Get(endpoint); err == nil {
			resp.Body.Close()
			t.Fatal("insecure endpoint reached transport")
		}
	}
}
