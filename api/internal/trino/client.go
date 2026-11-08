// Package trino implements the bounded direct-result Trino statement protocol.
// It does not authorize callers: the control plane must choose the engine user
// from a current administrator grant, and Trino must enforce data permissions.
package trino

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
)

const MaxPageBytes = 16 * 1024 * 1024
const MaxSQLBytes = 64 * 1024

var ErrTransport = errors.New("query engine request is unconfirmed")
var ErrProtocol = errors.New("query engine returned an invalid direct-protocol response")
var ErrCredentials = errors.New("query engine credentials are unavailable")
var headerValue = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:@-]{0,255}$`)
var queryID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

type Config struct {
	Endpoint  string
	CAFile    string
	TokenFile string
}

type Client struct {
	base      *url.URL
	tokenFile string
	http      *http.Client
}

// New requires TLS and a token file for the engine's own audience. API access
// tokens and caller-supplied URLs must never be passed through to this client.
func New(config Config) (*Client, error) {
	base, err := url.Parse(config.Endpoint)
	if err != nil || base.Scheme != "https" || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" || base.RawPath != "" || (base.Path != "" && base.Path != "/") || config.TokenFile == "" {
		return nil, errors.New("query engine requires a root HTTPS endpoint and token file")
	}
	base.Path = ""
	roots, err := x509.SystemCertPool()
	if err != nil {
		return nil, errors.New("system trust store is unavailable")
	}
	if config.CAFile != "" {
		pem, err := os.ReadFile(config.CAFile)
		if err != nil || !roots.AppendCertsFromPEM(pem) {
			return nil, errors.New("query engine CA file is unavailable or invalid")
		}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// No proxy from process-wide environment: this credential-bearing connection
	// follows its explicit endpoint and cluster network policy only.
	transport.Proxy = nil
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}
	transport.ResponseHeaderTimeout = 15 * time.Second
	return &Client{base: base, tokenFile: config.TokenFile, http: &http.Client{
		Transport: transport, Timeout: 20 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}, nil
}

type Statement struct {
	SQL, User, Catalog, Schema, TraceToken string
	MaxRunSeconds                          int
}

type Cursor struct {
	ID      string `json:"id"`
	NextURI string `json:"nextUri"`
}

type Column struct {
	Name          string          `json:"name"`
	Type          string          `json:"type"`
	TypeSignature json.RawMessage `json:"typeSignature,omitempty"`
}

type QueryError struct {
	Code int    `json:"errorCode"`
	Name string `json:"errorName"`
	Type string `json:"errorType"`
	// Engine message/stack traces may contain SQL and row values; do not expose
	// them through shared diagnostics or persist them in an unscoped event log.
}

type Page struct {
	Cursor
	Columns []Column            `json:"columns,omitempty"`
	Rows    [][]json.RawMessage `json:"data,omitempty"`
	Error   *QueryError         `json:"error,omitempty"`
}

// Submit sends exactly one POST. A lost acknowledgment is not permission to
// resubmit: the engine may still be executing the first statement.
func (c *Client) Submit(ctx context.Context, statement Statement) (Page, error) {
	if len(statement.SQL) == 0 || len(statement.SQL) > MaxSQLBytes || strings.ContainsRune(statement.SQL, 0) || !headerValue.MatchString(statement.User) || !headerValue.MatchString(statement.Catalog) || !headerValue.MatchString(statement.Schema) || !headerValue.MatchString(statement.TraceToken) || statement.MaxRunSeconds < 1 || statement.MaxRunSeconds > 3600 {
		return Page{}, errors.New("query statement has invalid size, identity, context or lifetime")
	}
	headers := http.Header{}
	headers.Set("Content-Type", "text/plain; charset=utf-8")
	headers.Set("X-Trino-Catalog", statement.Catalog)
	headers.Set("X-Trino-Schema", statement.Schema)
	headers.Set("X-Trino-Source", "dataslate")
	headers.Set("X-Trino-Trace-Token", statement.TraceToken)
	headers.Set("X-Trino-Time-Zone", "UTC")
	// The controller owns the requested deadline. The engine profile sets its
	// independent maximum and denies session-property overrides; granting this
	// user query_max_run_time would also permit SQL to raise that property.
	return c.request(ctx, http.MethodPost, c.base.String()+"/v1/statement", statement.User, statement.SQL, headers, "")
}

func (c *Client) Next(ctx context.Context, cursor Cursor, user string) (Page, error) {
	if err := c.validateCursor(cursor); err != nil {
		return Page{}, err
	}
	return c.request(ctx, http.MethodGet, cursor.NextURI, user, "", nil, cursor.ID)
}

// Cancel returning nil confirms engine acceptance (204), not a separately
// observed terminal result. The durable caller must retain that distinction.
func (c *Client) Cancel(ctx context.Context, cursor Cursor, user string) error {
	if err := c.validateCursor(cursor); err != nil {
		return err
	}
	_, err := c.request(ctx, http.MethodDelete, cursor.NextURI, user, "", nil, cursor.ID)
	return err
}

func (c *Client) validateCursor(cursor Cursor) error {
	if !queryID.MatchString(cursor.ID) || len(cursor.NextURI) > 4096 {
		return ErrProtocol
	}
	u, err := url.Parse(cursor.NextURI)
	if err != nil || u.Scheme != c.base.Scheme || u.Host != c.base.Host || u.User != nil || u.Fragment != "" || u.RawQuery != "" || u.RawPath != "" || !strings.HasPrefix(u.Path, "/v1/statement/") {
		return ErrProtocol
	}
	// Direct statement URLs contain the query ID as a complete path segment.
	// Reject traversal and links to a different query even on the same server.
	found := false
	for _, part := range strings.Split(u.Path, "/") {
		if part == "." || part == ".." || strings.Contains(part, "\\") {
			return ErrProtocol
		}
		found = found || part == cursor.ID
	}
	if !found {
		return ErrProtocol
	}
	return nil
}

func (c *Client) token() (string, error) {
	f, err := os.Open(c.tokenFile)
	if err != nil {
		return "", ErrCredentials
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, 64*1024+1))
	if err != nil || len(raw) > 64*1024 {
		return "", ErrCredentials
	}
	token := strings.TrimSpace(string(raw))
	if token == "" || strings.ContainsAny(token, " \t\r\n\x00") {
		return "", ErrCredentials
	}
	return token, nil
}

func (c *Client) request(ctx context.Context, method, target, user, sql string, headers http.Header, expectedID string) (Page, error) {
	if !headerValue.MatchString(user) {
		return Page{}, errors.New("query engine user is invalid")
	}
	token, err := c.token()
	if err != nil {
		return Page{}, err
	}
	req, err := http.NewRequestWithContext(ctx, method, target, strings.NewReader(sql))
	if err != nil {
		return Page{}, ErrTransport
	}
	if headers != nil {
		req.Header = headers.Clone()
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Trino-User", user)
	req.Header.Set("Accept", "application/json")
	response, err := c.http.Do(req)
	if err != nil {
		return Page{}, ErrTransport
	}
	defer response.Body.Close()
	if method == http.MethodDelete {
		if response.StatusCode != http.StatusNoContent {
			return Page{}, ErrTransport
		}
		return Page{}, nil
	}
	if response.StatusCode != http.StatusOK {
		return Page{}, ErrTransport
	}
	if response.Header.Get("X-Trino-Query-Data-Encoding") != "" {
		return Page{}, ErrProtocol // No object-store redirects or opaque forwarded credentials.
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, MaxPageBytes+1))
	if err != nil {
		return Page{}, ErrTransport
	}
	if len(data) > MaxPageBytes {
		return Page{}, ErrProtocol
	}
	var page Page
	decoder := json.NewDecoder(bytes.NewReader(data))
	if decoder.Decode(&page) != nil || decoder.Decode(&struct{}{}) != io.EOF || !queryID.MatchString(page.ID) || expectedID != "" && page.ID != expectedID || len(page.Columns) > 1000 || len(page.Rows) > 100000 {
		return Page{}, ErrProtocol
	}
	if page.NextURI != "" {
		if err = c.validateCursor(page.Cursor); err != nil {
			return Page{}, err
		}
	}
	for _, column := range page.Columns {
		if column.Name == "" || len(column.Name) > 4096 || column.Type == "" || len(column.Type) > 8192 {
			return Page{}, ErrProtocol
		}
	}
	for _, row := range page.Rows {
		if len(row) > 1000 || len(page.Columns) > 0 && len(row) != len(page.Columns) {
			return Page{}, ErrProtocol
		}
	}
	return page, nil
}
