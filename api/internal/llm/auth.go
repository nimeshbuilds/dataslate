// Package llm implements bounded structured generation for Sylon.
// Caller authorization and explicit data-transfer consent precede every request.
package llm

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const maxResponse = 256 << 10

type Auth struct {
	BearerTokenFile  string `json:"bearerTokenFile,omitempty"`
	ClientID         string `json:"clientId,omitempty"`
	ClientSecretFile string `json:"clientSecretFile,omitempty"`
	TokenEndpoint    string `json:"tokenEndpoint,omitempty"`
	Scope            string `json:"scope,omitempty"`
	Style            string `json:"style,omitempty"`
}
type Config struct {
	Protocol        string `json:"protocol"`
	Endpoint        string `json:"endpoint"`
	Model           string `json:"model"`
	MaxOutputTokens int    `json:"maxOutputTokens"`
	CAFile          string `json:"caFile,omitempty"`
	Auth            Auth   `json:"auth"`
}
type Error struct {
	Kind       string
	StatusCode int
	Ambiguous  bool
}

func (e *Error) Error() string {
	if e.StatusCode != 0 {
		return fmt.Sprintf("llm %s (HTTP %d)", e.Kind, e.StatusCode)
	}
	return "llm " + e.Kind
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
	if !modelPattern.MatchString(config.Model) || config.MaxOutputTokens < 256 || config.MaxOutputTokens > 8192 || (config.Protocol != "responses" && config.Protocol != "chat-completions") {
		return failure("invalid protocol, model or output limit")
	}
	for _, path := range []string{config.CAFile, config.Auth.BearerTokenFile, config.Auth.ClientSecretFile} {
		if path != "" && (!filepath.IsAbs(path) || strings.ContainsRune(path, 0)) {
			return failure("absolute credential and CA file paths required")
		}
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
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}, ResponseHeaderTimeout: 55 * time.Second, IdleConnTimeout: 30 * time.Second, MaxIdleConnsPerHost: 4}
	client := &http.Client{Transport: transport, Timeout: 60 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
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
	if !uniqueJSON(b) {
		return failure("invalid JSON structure")
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

func (c *Client) Close() { c.http.CloseIdleConnections() }
