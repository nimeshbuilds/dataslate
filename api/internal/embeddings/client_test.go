package embeddings

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
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func fixture(t *testing.T, h http.HandlerFunc) (*Client, *httptest.Server) {
	t.Helper()
	s := httptest.NewTLSServer(h)
	t.Cleanup(s.Close)
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.pem")
	token := filepath.Join(dir, "token")
	if e := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.Certificate().Raw}), 0600); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(token, []byte("first-token"), 0600); e != nil {
		t.Fatal(e)
	}
	c, e := New(Config{Endpoint: s.URL + "/v1/embeddings", Model: "text-embedding-3-small", Dimensions: 3, CAFile: ca, Auth: Auth{BearerTokenFile: token}})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(c.Close)
	return c, s
}
func result(vectors [][]float64) map[string]any {
	data := []any{}
	for i, v := range vectors {
		data = append(data, map[string]any{"object": "embedding", "index": i, "embedding": v})
	}
	return map[string]any{"object": "list", "model": "text-embedding-3-small", "data": data}
}
func jsonResponse(t *testing.T, w http.ResponseWriter, v any) {
	t.Helper()
	if e := json.NewEncoder(w).Encode(v); e != nil {
		t.Error(e)
	}
}
func typed(t *testing.T, e error, ambiguous bool) *Error {
	t.Helper()
	var p *Error
	if !errors.As(e, &p) || p.Ambiguous != ambiguous {
		t.Fatalf("unexpected error: %v", e)
	}
	return p
}
func TestRequestAndIndexMapping(t *testing.T) {
	c, _ := fixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/v1/embeddings" || r.Header.Get("Authorization") != "Bearer first-token" || r.Header.Get("Content-Type") != "application/json" || r.Header.Get("Idempotency-Key") != "" {
			t.Error("request boundary mismatch")
		}
		var data map[string]any
		if json.NewDecoder(r.Body).Decode(&data) != nil {
			t.Fatal("invalid request")
		}
		if len(data) != 4 || data["encoding_format"] != "float" || data["dimensions"] != float64(3) || data["model"] != "text-embedding-3-small" || !reflect.DeepEqual(data["input"], []any{"alpha", "βeta"}) {
			t.Errorf("request: %+v", data)
		}
		v := result([][]float64{{1, 2, 3}, {4, 5, 6}})
		items := v["data"].([]any)
		v["data"] = []any{items[1], items[0]}
		v["url"] = "https://attacker.invalid"
		jsonResponse(t, w, v)
	})
	vectors, e := c.Embed(context.Background(), []string{"alpha", "βeta"})
	if e != nil || !reflect.DeepEqual(vectors, [][]float32{{1, 2, 3}, {4, 5, 6}}) {
		t.Fatalf("%v %v", vectors, e)
	}
}
func TestBearerRotationAndNoErrorDisclosure(t *testing.T) {
	requests := 0
	c, _ := fixture(t, func(w http.ResponseWriter, r *http.Request) {
		requests++
		want := "first-token"
		if requests > 1 {
			want = "rotated-token"
		}
		if r.Header.Get("Authorization") != "Bearer "+want {
			t.Error("rotation failed")
		}
		if requests == 2 {
			w.WriteHeader(429)
			io.WriteString(w, "TOPSECRET customer input and rotated-token")
			return
		}
		jsonResponse(t, w, result([][]float64{{1, 0, 0}}))
	})
	if _, e := c.Embed(context.Background(), []string{"private-customer-input"}); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(c.config.Auth.BearerTokenFile, []byte("rotated-token\n"), 0600); e != nil {
		t.Fatal(e)
	}
	_, e := c.Embed(context.Background(), []string{"private-customer-input"})
	p := typed(t, e, true)
	if p.StatusCode != 429 || requests != 2 || strings.Contains(e.Error(), "TOPSECRET") || strings.Contains(e.Error(), "private") || strings.Contains(e.Error(), "rotated-token") {
		t.Fatal("retry/disclosure")
	}
}
func TestOAuthCacheRotationAnd401NoRetry(t *testing.T) {
	for _, style := range []string{"post", "basic"} {
		t.Run(style, func(t *testing.T) {
			tokens, posts := 0, 0
			deny := false
			c, s := fixture(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/token" {
					tokens++
					r.ParseForm()
					secret := "alpha +/"
					if tokens > 1 {
						secret = "beta ?&"
					}
					if style == "post" {
						if r.Form.Get("client_id") != "id:/" || r.Form.Get("client_secret") != secret {
							t.Error("OAuth form")
						}
					} else {
						id, pw, ok := r.BasicAuth()
						if !ok || id != url.QueryEscape("id:/") || pw != url.QueryEscape(secret) || r.Form.Get("client_secret") != "" {
							t.Error("OAuth basic")
						}
					}
					if r.Form.Get("grant_type") != "client_credentials" || r.Form.Get("scope") != "embeddings:create" {
						t.Error("grant/scope")
					}
					jsonResponse(t, w, map[string]any{"access_token": fmt.Sprintf("token-%d", tokens), "token_type": "Bearer", "expires_in": 3600})
					return
				}
				posts++
				if r.Header.Get("Authorization") != fmt.Sprintf("Bearer token-%d", tokens) {
					t.Error("wrong token")
				}
				if deny {
					w.WriteHeader(401)
					return
				}
				jsonResponse(t, w, result([][]float64{{1, 0, 0}}))
			})
			file := filepath.Join(t.TempDir(), "secret")
			os.WriteFile(file, []byte("alpha +/"), 0600)
			c.config.Auth = Auth{ClientID: "id:/", ClientSecretFile: file, TokenEndpoint: s.URL + "/token", Scope: "embeddings:create", Style: style}
			for i := 0; i < 2; i++ {
				if _, e := c.Embed(context.Background(), []string{"one"}); e != nil {
					t.Fatal(e)
				}
			}
			if tokens != 1 {
				t.Fatal("cache")
			}
			os.WriteFile(file, []byte("beta ?&"), 0600)
			if _, e := c.Embed(context.Background(), []string{"one"}); e != nil {
				t.Fatal(e)
			}
			if tokens != 2 {
				t.Fatal("rotation")
			}
			deny = true
			_, e := c.Embed(context.Background(), []string{"one"})
			typed(t, e, true)
			if tokens != 2 || posts != 4 {
				t.Fatal("automatic retry")
			}
			deny = false
			if _, e = c.Embed(context.Background(), []string{"one"}); e != nil {
				t.Fatal(e)
			}
			if tokens != 3 {
				t.Fatal("401 cache not cleared")
			}
		})
	}
}
func TestRedirectNeverForwardsCredentialsOrInput(t *testing.T) {
	var hits atomic.Int32
	attacker := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	defer attacker.Close()
	c, s := fixture(t, func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, attacker.URL, 307) })
	_, e := c.Embed(context.Background(), []string{"private"})
	if typed(t, e, true).StatusCode != 307 {
		t.Fatal("redirect status")
	}
	c.config.Auth = Auth{ClientID: "id", ClientSecretFile: c.config.Auth.BearerTokenFile, TokenEndpoint: s.URL + "/token"}
	_, e = c.Embed(context.Background(), []string{"private"})
	if typed(t, e, false).StatusCode != 307 {
		t.Fatal("OAuth redirect status")
	}
	if hits.Load() != 0 {
		t.Fatal("redirect followed")
	}
}
func TestInputLimitsBeforeAuthenticationOrNetwork(t *testing.T) {
	c, _ := fixture(t, func(w http.ResponseWriter, r *http.Request) { t.Error("invalid input reached network") })
	c.config.Auth.BearerTokenFile = "/missing/token"
	inputs := [][]string{nil, {}, {""}, {"\t\n"}, {"invalid\xff"}, {"nul\x00byte"}, {strings.Repeat("a", MaxInputBytes+1)}, make([]string, MaxInputs+1)}
	huge := make([]string, 9)
	for i := range huge {
		huge[i] = strings.Repeat("x", 32768)
	}
	inputs = append(inputs, huge)
	for _, in := range inputs {
		_, e := c.Embed(context.Background(), in)
		typed(t, e, false)
	}
}
func TestMalformedResponsesFailWithoutPartialVectors(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"wrong-model", `{"object":"list","model":"other","data":[{"object":"embedding","index":0,"embedding":[1,2,3]}]}`},
		{"missing-index", `{"object":"list","model":"text-embedding-3-small","data":[{"object":"embedding","embedding":[1,2,3]}]}`},
		{"out-of-range", `{"object":"list","model":"text-embedding-3-small","data":[{"object":"embedding","index":1,"embedding":[1,2,3]}]}`},
		{"wrong-dimension", `{"object":"list","model":"text-embedding-3-small","data":[{"object":"embedding","index":0,"embedding":[1,2]}]}`},
		{"null-element", `{"object":"list","model":"text-embedding-3-small","data":[{"object":"embedding","index":0,"embedding":[1,null,3]}]}`},
		{"zero-norm", `{"object":"list","model":"text-embedding-3-small","data":[{"object":"embedding","index":0,"embedding":[0,0,0]}]}`},
		{"float32-overflow", `{"object":"list","model":"text-embedding-3-small","data":[{"object":"embedding","index":0,"embedding":[1e39,0,0]}]}`},
		{"float32-underflow", `{"object":"list","model":"text-embedding-3-small","data":[{"object":"embedding","index":0,"embedding":[1e-99,0,0]}]}`},
		{"nan", `{"object":"list","model":"text-embedding-3-small","data":[{"object":"embedding","index":0,"embedding":[NaN,0,0]}]}`},
		{"multiple-json", `{} {}`}, {"oversized", strings.Repeat(" ", maxResponse+1)}, {"utf8", "\xff"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := fixture(t, func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, tc.body) })
			out, e := c.Embed(context.Background(), []string{"one"})
			typed(t, e, true)
			if out != nil {
				t.Fatal("returned partial vectors")
			}
		})
	}
}
func TestDuplicateIndicesRejected(t *testing.T) {
	c, _ := fixture(t, func(w http.ResponseWriter, r *http.Request) {
		v := result([][]float64{{1, 0, 0}, {0, 1, 0}})
		v["data"].([]any)[1].(map[string]any)["index"] = 0
		jsonResponse(t, w, v)
	})
	_, e := c.Embed(context.Background(), []string{"one", "two"})
	typed(t, e, true)
}
func TestCancellationAndLostResponseAreNotRetried(t *testing.T) {
	var calls atomic.Int32
	c, _ := fixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		conn, _, e := w.(http.Hijacker).Hijack()
		if e != nil {
			t.Error(e)
			return
		}
		conn.Close()
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, e := c.Embed(ctx, []string{"one"})
	typed(t, e, false)
	if calls.Load() != 0 {
		t.Fatal("pre-canceled dispatch")
	}
	_, e = c.Embed(context.Background(), []string{"one"})
	typed(t, e, true)
	if calls.Load() != 1 {
		t.Fatal("transport retried")
	}
}
func TestTLSAndEndpointValidation(t *testing.T) {
	c, _ := fixture(t, func(w http.ResponseWriter, r *http.Request) { jsonResponse(t, w, result([][]float64{{1, 0, 0}})) })
	cfg := c.config
	cfg.CAFile = ""
	bad, e := New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer bad.Close()
	_, e = bad.Embed(context.Background(), []string{"one"})
	typed(t, e, true)
	for _, endpoint := range []string{"http://host/v1/embeddings", "https://user:secret@host/v1/embeddings", "https://host/a/../b", "https://host/a%2Fb", "https://host//a", "https://host/a?token=secret", "https://host/a#fragment"} {
		cfg := c.config
		cfg.Endpoint = endpoint
		if e := ValidateConfig(cfg); e == nil {
			t.Fatal("accepted unsafe endpoint")
		}
		cfg = c.config
		cfg.Auth = Auth{ClientID: "id", ClientSecretFile: "/secret", TokenEndpoint: endpoint}
		if e := ValidateConfig(cfg); e == nil {
			t.Fatal("accepted unsafe OAuth endpoint")
		}
	}
	cfg = c.config
	cfg.CAFile = "/missing/ca"
	cfg.Auth.BearerTokenFile = "/missing/token"
	if e := ValidateConfig(cfg); e != nil {
		t.Fatal("syntax validation read files")
	}
}
func TestOAuthFailureNeverDispatchesEmbeddings(t *testing.T) {
	count := 0
	c, s := fixture(t, func(w http.ResponseWriter, r *http.Request) {
		count++
		if r.URL.Path != "/token" {
			t.Fatal("embedding dispatch despite failed auth")
		}
		jsonResponse(t, w, map[string]any{"access_token": "abc", "token_type": "bearer", "expires_in": 0})
	})
	c.config.Auth = Auth{ClientID: "id", ClientSecretFile: c.config.Auth.BearerTokenFile, TokenEndpoint: s.URL + "/token"}
	_, e := c.Embed(context.Background(), []string{"one"})
	typed(t, e, false)
	if count != 1 {
		t.Fatal("OAuth retried")
	}
}
func TestConcurrentOAuthUsesOneToken(t *testing.T) {
	var tokens atomic.Int32
	c, s := fixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			tokens.Add(1)
			time.Sleep(10 * time.Millisecond)
			jsonResponse(t, w, map[string]any{"access_token": "abc", "token_type": "bearer", "expires_in": 3600})
			return
		}
		jsonResponse(t, w, result([][]float64{{1, 0, 0}}))
	})
	c.config.Auth = Auth{ClientID: "id", ClientSecretFile: c.config.Auth.BearerTokenFile, TokenEndpoint: s.URL + "/token"}
	done := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func() { _, e := c.Embed(context.Background(), []string{"one"}); done <- e }()
	}
	for i := 0; i < 8; i++ {
		if e := <-done; e != nil {
			t.Fatal(e)
		}
	}
	if tokens.Load() != 1 {
		t.Fatal("concurrent token requests")
	}
}
