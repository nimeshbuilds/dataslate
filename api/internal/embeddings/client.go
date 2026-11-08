// Package embeddings implements a bounded OpenAI-compatible embeddings boundary.
// Caller authorization and explicit data-transfer consent precede every request.
package embeddings

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const maxResponse = 4 << 20
const MaxInputs = 64
const MaxInputBytes = 32768
const MaxTotalInputBytes = 262144

type Auth struct {
	BearerTokenFile  string `json:"bearerTokenFile,omitempty"`
	ClientID         string `json:"clientId,omitempty"`
	ClientSecretFile string `json:"clientSecretFile,omitempty"`
	TokenEndpoint    string `json:"tokenEndpoint,omitempty"`
	Scope            string `json:"scope,omitempty"`
	Style            string `json:"style,omitempty"`
}
type Config struct {
	Endpoint   string `json:"endpoint"`
	Model      string `json:"model"`
	Dimensions int    `json:"dimensions"`
	CAFile     string `json:"caFile,omitempty"`
	Auth       Auth   `json:"auth"`
}
type Error struct {
	Kind       string
	StatusCode int
	Ambiguous  bool
}

func (e *Error) Error() string {
	if e.StatusCode != 0 {
		return fmt.Sprintf("embeddings %s (HTTP %d)", e.Kind, e.StatusCode)
	}
	return "embeddings " + e.Kind
}
func failure(kind string) error { return &Error{Kind: kind} }

type Client struct {
	config      Config
	base        *url.URL
	http        *http.Client
	mu          sync.Mutex
	token       string
	tokenExpiry time.Time
	secretHash  [32]byte
}

// ValidateConfig checks configuration syntax without reading files or contacting
// an identity provider. New additionally loads CA trust; credentials rotate at request time.
func ValidateConfig(config Config) error {
	_, err := endpoint(config.Endpoint)
	if err != nil {
		return err
	}
	if !modelPattern.MatchString(config.Model) || config.Dimensions < 1 || config.Dimensions > 2000 {
		return failure("invalid model or dimensions")
	}
	a := config.Auth
	if a.BearerTokenFile != "" {
		if a.ClientID != "" || a.ClientSecretFile != "" || a.TokenEndpoint != "" || a.Scope != "" || a.Style != "" {
			return failure("ambiguous authentication")
		}
	} else {
		if a.ClientID == "" || len(a.ClientID) > 512 || strings.ContainsAny(a.ClientID, "\r\n\x00") || a.ClientSecretFile == "" {
			return failure("authentication required")
		}
		if _, err = endpoint(a.TokenEndpoint); err != nil {
			return failure("invalid OAuth endpoint")
		}
		if a.Style != "" && a.Style != "post" && a.Style != "basic" {
			return failure("invalid OAuth style")
		}
		if len(a.Scope) > 1024 || strings.ContainsAny(a.Scope, "\r\n\x00") {
			return failure("invalid OAuth scope")
		}
	}
	return nil
}

func New(config Config) (*Client, error) {
	if err := ValidateConfig(config); err != nil {
		return nil, err
	}
	base, _ := endpoint(config.Endpoint)
	roots, err := x509.SystemCertPool()
	if err != nil {
		return nil, failure("system trust unavailable")
	}
	if config.CAFile != "" {
		cert, e := readFile(config.CAFile, 1<<20)
		if e != nil || !roots.AppendCertsFromPEM(cert) {
			return nil, failure("invalid CA file")
		}
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}, ResponseHeaderTimeout: 40 * time.Second, IdleConnTimeout: 30 * time.Second, MaxIdleConnsPerHost: 4}
	client := &http.Client{Transport: transport, Timeout: 45 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return &Client{config: config, base: base, http: client}, nil
}
func endpoint(value string) (*url.URL, error) {
	u, err := url.Parse(value)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" || u.RawPath != "" || strings.ContainsAny(value, "\r\n\x00\\%") || strings.Contains(u.Path, "//") {
		return nil, failure("HTTPS endpoint required")
	}
	for _, p := range strings.Split(u.Path, "/") {
		if p == "." || p == ".." {
			return nil, failure("invalid endpoint path")
		}
	}
	u.Path = strings.TrimRight(u.Path, "/")
	return u, nil
}
func readFile(path string, limit int64) ([]byte, error) {
	info, e := os.Stat(path)
	if e != nil || !info.Mode().IsRegular() {
		return nil, failure("credential file unavailable")
	}
	f, e := os.Open(path)
	if e != nil {
		return nil, failure("credential file unavailable")
	}
	defer f.Close()
	if info, e = f.Stat(); e != nil || !info.Mode().IsRegular() {
		return nil, failure("credential file unavailable")
	}
	b, e := io.ReadAll(io.LimitReader(f, limit+1))
	if e != nil || len(b) == 0 || int64(len(b)) > limit {
		return nil, failure("invalid credential file")
	}
	return b, nil
}

var tokenPattern = regexp.MustCompile(`^[A-Za-z0-9._~+/-]+=*$`)

func validToken(token string) bool {
	return len(token) > 0 && len(token) <= 16384 && tokenPattern.MatchString(token)
}
func (c *Client) bearer(ctx context.Context) (string, error) {
	a := c.config.Auth
	if a.BearerTokenFile != "" {
		b, e := readFile(a.BearerTokenFile, 16384)
		if e != nil {
			return "", e
		}
		token := strings.TrimSpace(string(b))
		if !validToken(token) {
			return "", failure("invalid bearer token")
		}
		return token, nil
	}
	secret, err := readFile(a.ClientSecretFile, 65536)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(secret)
	c.mu.Lock()
	defer c.mu.Unlock()
	if hash == c.secretHash && c.token != "" && time.Now().Add(30*time.Second).Before(c.tokenExpiry) {
		return c.token, nil
	}
	values := url.Values{"grant_type": {"client_credentials"}}
	if a.Scope != "" {
		values.Set("scope", a.Scope)
	}
	if a.Style != "basic" {
		values.Set("client_id", a.ClientID)
		values.Set("client_secret", string(secret))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.TokenEndpoint, strings.NewReader(values.Encode()))
	if err != nil {
		return "", failure("invalid OAuth request")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.GetBody = nil // Never replay credential POSTs transparently.
	if a.Style == "basic" {
		req.SetBasicAuth(url.QueryEscape(a.ClientID), url.QueryEscape(string(secret)))
	}
	response, err := c.http.Do(req)
	if err != nil {
		return "", failure("OAuth transport failed")
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return "", &Error{Kind: "OAuth rejected", StatusCode: response.StatusCode}
	}
	var result struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err = decode(response.Body, 65536, &result); err != nil || !validToken(result.AccessToken) || !strings.EqualFold(result.TokenType, "bearer") || result.ExpiresIn <= 0 || result.ExpiresIn > 86400 {
		return "", failure("invalid OAuth response")
	}
	c.token = result.AccessToken
	c.secretHash = hash
	c.tokenExpiry = time.Now().Add(time.Duration(result.ExpiresIn) * time.Second)
	return c.token, nil
}
func decode(reader io.Reader, limit int64, out any) error {
	b, e := io.ReadAll(io.LimitReader(reader, limit+1))
	if e != nil || int64(len(b)) > limit || !utf8.Valid(b) {
		return failure("response limit or encoding")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	if d.Decode(out) != nil {
		return failure("invalid response")
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return failure("invalid response")
	}
	return nil
}

var modelPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9:/_.-]{0,127}$`)

// Embed makes one attempt. Errors after dispatch are conservatively marked
// Ambiguous: the provider may have processed/billed the request. No retries,
// response URLs, tool execution or fallback embedding models are involved.
func (c *Client) Embed(ctx context.Context, inputs []string) ([][]float32, error) {
	if len(inputs) < 1 || len(inputs) > MaxInputs {
		return nil, failure("invalid input count")
	}
	total := 0
	for _, input := range inputs {
		if !utf8.ValidString(input) || strings.TrimSpace(input) == "" || len(input) > MaxInputBytes || strings.ContainsRune(input, 0) {
			return nil, failure("invalid input text")
		}
		total += len(input)
	}
	if total > MaxTotalInputBytes {
		return nil, failure("input byte limit")
	}
	if ctx.Err() != nil {
		return nil, failure("request canceled before dispatch")
	}
	token, err := c.bearer(ctx)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(struct {
		Input          []string `json:"input"`
		Model          string   `json:"model"`
		Dimensions     int      `json:"dimensions"`
		EncodingFormat string   `json:"encoding_format"`
	}{inputs, c.config.Model, c.config.Dimensions, "float"})
	if err != nil {
		return nil, failure("invalid request")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.config.Endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, failure("invalid request")
	}
	req.GetBody = nil // net/http must not replay a POST on an idle connection failure.
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if ctx.Err() != nil {
		return nil, failure("request canceled before dispatch")
	}
	response, err := c.http.Do(req)
	if err != nil {
		return nil, &Error{Kind: "transport failed", Ambiguous: true}
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		if response.StatusCode == http.StatusUnauthorized {
			c.mu.Lock()
			c.token = ""
			c.mu.Unlock()
		}
		return nil, &Error{Kind: "provider rejected", StatusCode: response.StatusCode, Ambiguous: true}
	}
	var result struct {
		Object string `json:"object"`
		Model  string `json:"model"`
		Data   []struct {
			Object    string     `json:"object"`
			Index     *int       `json:"index"`
			Embedding []*float64 `json:"embedding"`
		} `json:"data"`
	}
	if decode(response.Body, maxResponse, &result) != nil || result.Object != "list" || result.Model != c.config.Model || len(result.Data) != len(inputs) {
		return nil, &Error{Kind: "invalid provider response", Ambiguous: true}
	}
	out := make([][]float32, len(inputs))
	seen := make([]bool, len(inputs))
	for _, item := range result.Data {
		if item.Object != "embedding" || item.Index == nil || *item.Index < 0 || *item.Index >= len(inputs) || seen[*item.Index] || len(item.Embedding) != c.config.Dimensions {
			return nil, &Error{Kind: "invalid embedding shape", Ambiguous: true}
		}
		index := *item.Index
		seen[index] = true
		vector := make([]float32, c.config.Dimensions)
		norm := float64(0)
		for i, number := range item.Embedding {
			if number == nil {
				return nil, &Error{Kind: "null embedding component", Ambiguous: true}
			}
			value := *number
			v := float32(value)
			if math.IsNaN(value) || math.IsInf(value, 0) || math.IsInf(float64(v), 0) {
				return nil, &Error{Kind: "nonfinite embedding", Ambiguous: true}
			}
			vector[i] = v
			norm += float64(v) * float64(v)
		}
		if norm <= 0 || math.IsNaN(norm) || math.IsInf(norm, 0) {
			return nil, &Error{Kind: "zero or invalid embedding norm", Ambiguous: true}
		}
		out[index] = vector
	}
	return out, nil
}
func (c *Client) Close() { c.http.CloseIdleConnections() }
