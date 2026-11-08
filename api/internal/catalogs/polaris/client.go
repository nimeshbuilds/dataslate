// Package polaris provides a bounded read-only Iceberg REST metadata client.
// DataSlate caller/binding authorization remains the control plane's job.
package polaris

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
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const maxResponse = 8 << 20
const maxPage = 1000

type Auth struct {
	BearerTokenFile  string `json:"bearerTokenFile,omitempty"`
	ClientID         string `json:"clientId,omitempty"`
	ClientSecretFile string `json:"clientSecretFile,omitempty"`
	TokenEndpoint    string `json:"tokenEndpoint,omitempty"`
	Scope            string `json:"scope,omitempty"`
	Style            string `json:"style,omitempty"`
}
type Config struct {
	Endpoint  string `json:"endpoint"`
	Warehouse string `json:"warehouse"`
	Realm     string `json:"realm"`
	CAFile    string `json:"caFile,omitempty"`
	Auth      Auth   `json:"auth"`
}
type Error struct {
	Kind       string
	StatusCode int
}

func (e *Error) Error() string {
	if e.StatusCode != 0 {
		return fmt.Sprintf("polaris %s (HTTP %d)", e.Kind, e.StatusCode)
	}
	return "polaris " + e.Kind
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
	if !component(config.Warehouse) || !component(config.Realm) {
		return failure("invalid catalog or realm")
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
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}, ResponseHeaderTimeout: 10 * time.Second, IdleConnTimeout: 30 * time.Second, MaxIdleConnsPerHost: 4}
	client := &http.Client{Transport: transport, Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
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
func component(value string) bool {
	if !utf8.ValidString(value) || len(value) < 1 || len(value) > 128 || value == "." || value == ".." {
		return false
	}
	for _, r := range value {
		if r < 32 || r == 127 || strings.ContainsRune("/\\?#%", r) {
			return false
		}
	}
	return true
}
func namespace(value []string, empty bool) bool {
	if len(value) > 16 || (!empty && len(value) == 0) {
		return false
	}
	for _, p := range value {
		if !component(p) {
			return false
		}
	}
	return true
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
	req.Header.Set("Polaris-Realm", c.config.Realm)
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
func (c *Client) get(ctx context.Context, path string, query url.Values, out any) error {
	token, err := c.bearer(ctx)
	if err != nil {
		return err
	}
	target := *c.base
	escaped := c.base.EscapedPath() + path
	decoded, err := url.PathUnescape(escaped)
	if err != nil {
		return failure("invalid request path")
	}
	target.Path = decoded
	target.RawPath = escaped
	target.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return failure("invalid request")
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Polaris-Realm", c.config.Realm)
	// Never request X-Iceberg-Access-Delegation or follow server-supplied URLs.
	response, err := c.http.Do(req)
	if err != nil {
		return failure("transport failed")
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		if response.StatusCode == 401 {
			c.mu.Lock()
			c.token = ""
			c.mu.Unlock()
		}
		return &Error{Kind: "request rejected", StatusCode: response.StatusCode}
	}
	return decode(response.Body, maxResponse, out)
}
func (c *Client) Close() { c.http.CloseIdleConnections() }

type Capabilities struct {
	Catalog   string   `json:"catalog"`
	Prefix    string   `json:"prefix"`
	Endpoints []string `json:"endpoints"`
}

var readEndpoints = []string{"GET /v1/{prefix}/namespaces", "GET /v1/{prefix}/namespaces/{namespace}/tables", "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}"}

func (c *Client) Discover(ctx context.Context) (Capabilities, error) {
	var r struct {
		Defaults  map[string]string `json:"defaults"`
		Overrides map[string]string `json:"overrides"`
		Endpoints []string          `json:"endpoints"`
	}
	if e := c.get(ctx, "/v1/config", url.Values{"warehouse": {c.config.Warehouse}}, &r); e != nil {
		return Capabilities{}, e
	}
	prefix := r.Defaults["prefix"]
	if p, ok := r.Overrides["prefix"]; ok {
		prefix = p
	}
	if prefix != "" && prefix != c.config.Warehouse {
		return Capabilities{}, failure("catalog prefix mismatch")
	}
	if len(r.Endpoints) > 256 {
		return Capabilities{}, failure("invalid endpoint discovery")
	}
	out := Capabilities{Catalog: c.config.Warehouse, Prefix: c.config.Warehouse, Endpoints: []string{}}
	for _, known := range readEndpoints {
		if r.Endpoints == nil || slices.Contains(r.Endpoints, known) {
			out.Endpoints = append(out.Endpoints, known)
		}
	}
	return out, nil
}

type NamespacePage struct {
	Namespaces    [][]string `json:"namespaces"`
	NextPageToken string     `json:"nextPageToken,omitempty"`
}
type Identifier struct {
	Namespace []string `json:"namespace"`
	Name      string   `json:"name"`
}
type TablePage struct {
	Identifiers   []Identifier `json:"identifiers"`
	NextPageToken string       `json:"nextPageToken,omitempty"`
}

func pageQuery(token string, size int) (url.Values, error) {
	if size < 1 || size > maxPage || !pageToken(token) {
		return nil, failure("invalid page request")
	}
	q := url.Values{"pageSize": {strconv.Itoa(size)}}
	if token != "" {
		q.Set("pageToken", token)
	}
	return q, nil
}
func pageToken(token string) bool {
	if len(token) > 4096 || !utf8.ValidString(token) {
		return false
	}
	for _, r := range token {
		if r < 32 || r == 127 {
			return false
		}
	}
	return true
}
func (c *Client) ListNamespaces(ctx context.Context, parent []string, token string, size int) (NamespacePage, error) {
	if !namespace(parent, true) {
		return NamespacePage{}, failure("invalid namespace")
	}
	q, e := pageQuery(token, size)
	if e != nil {
		return NamespacePage{}, e
	}
	if len(parent) > 0 {
		q.Set("parent", strings.Join(parent, "\x1f"))
	}
	var r struct {
		Namespaces [][]string `json:"namespaces"`
		Next       string     `json:"next-page-token"`
	}
	if e = c.get(ctx, "/v1/"+url.PathEscape(c.config.Warehouse)+"/namespaces", q, &r); e != nil {
		return NamespacePage{}, e
	}
	if r.Namespaces == nil || len(r.Namespaces) > maxPage || !pageToken(r.Next) {
		return NamespacePage{}, failure("invalid namespace response")
	}
	seen := map[string]bool{}
	for _, ns := range r.Namespaces {
		if !namespace(ns, false) || len(ns) != len(parent)+1 || !slices.Equal(ns[:len(parent)], parent) || seen[strings.Join(ns, "\x1f")] {
			return NamespacePage{}, failure("namespace response scope mismatch")
		}
		seen[strings.Join(ns, "\x1f")] = true
	}
	return NamespacePage{r.Namespaces, r.Next}, nil
}
func (c *Client) ListTables(ctx context.Context, ns []string, token string, size int) (TablePage, error) {
	if !namespace(ns, false) {
		return TablePage{}, failure("invalid namespace")
	}
	q, e := pageQuery(token, size)
	if e != nil {
		return TablePage{}, e
	}
	var r struct {
		Identifiers []Identifier `json:"identifiers"`
		Next        string       `json:"next-page-token"`
	}
	if e = c.get(ctx, "/v1/"+url.PathEscape(c.config.Warehouse)+"/namespaces/"+url.PathEscape(strings.Join(ns, "\x1f"))+"/tables", q, &r); e != nil {
		return TablePage{}, e
	}
	if r.Identifiers == nil || len(r.Identifiers) > maxPage || !pageToken(r.Next) {
		return TablePage{}, failure("invalid table response")
	}
	seen := map[string]bool{}
	for _, id := range r.Identifiers {
		if !slices.Equal(id.Namespace, ns) || !component(id.Name) || seen[id.Name] {
			return TablePage{}, failure("table response scope mismatch")
		}
		seen[id.Name] = true
	}
	return TablePage{r.Identifiers, r.Next}, nil
}

type Field struct {
	ID       int             `json:"id"`
	Name     string          `json:"name"`
	Required bool            `json:"required"`
	Type     json.RawMessage `json:"type"`
}

func (f *Field) UnmarshalJSON(data []byte) error {
	var v struct {
		ID       int             `json:"id"`
		Name     string          `json:"name"`
		Required *bool           `json:"required"`
		Type     json.RawMessage `json:"type"`
	}
	if json.Unmarshal(data, &v) != nil || v.Required == nil {
		return failure("invalid field nullability")
	}
	*f = Field{ID: v.ID, Name: v.Name, Required: *v.Required, Type: v.Type}
	return nil
}

type Schema struct {
	ID     int     `json:"id"`
	Fields []Field `json:"fields"`
}
type Table struct {
	Identifier        Identifier `json:"identifier"`
	FormatVersion     int        `json:"formatVersion"`
	UUID              string     `json:"uuid"`
	Location          string     `json:"location"`
	MetadataLocation  string     `json:"metadataLocation,omitempty"`
	LastUpdatedMS     int64      `json:"lastUpdatedMs"`
	CurrentSnapshotID *int64     `json:"currentSnapshotId,omitempty"`
	CurrentSchema     Schema     `json:"currentSchema"`
}

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func storageLocation(value string, required bool) bool {
	if value == "" {
		return !required
	}
	if len(value) > 4096 || strings.ContainsAny(value, "\r\n\x00") {
		return false
	}
	u, e := url.Parse(value)
	if e != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return false
	}
	if u.User != nil {
		if _, password := u.User.Password(); password || !slices.Contains([]string{"abfs", "abfss", "wasb", "wasbs"}, u.Scheme) {
			return false
		}
	}
	return slices.Contains([]string{"s3", "s3a", "gs", "abfs", "abfss", "wasb", "wasbs", "file"}, u.Scheme) && u.Path != "" && (u.Scheme == "file" || u.Hostname() != "")
}
func (c *Client) LoadTable(ctx context.Context, id Identifier) (Table, error) {
	if !namespace(id.Namespace, false) || !component(id.Name) {
		return Table{}, failure("invalid table identifier")
	}
	var r struct {
		MetadataLocation string `json:"metadata-location"`
		Metadata         struct {
			FormatVersion     int    `json:"format-version"`
			UUID              string `json:"table-uuid"`
			Location          string `json:"location"`
			LastUpdatedMS     int64  `json:"last-updated-ms"`
			CurrentSnapshotID *int64 `json:"current-snapshot-id"`
			CurrentSchemaID   int    `json:"current-schema-id"`
			Schemas           []struct {
				ID     int     `json:"schema-id"`
				Type   string  `json:"type"`
				Fields []Field `json:"fields"`
			} `json:"schemas"`
		} `json:"metadata"`
	}
	path := "/v1/" + url.PathEscape(c.config.Warehouse) + "/namespaces/" + url.PathEscape(strings.Join(id.Namespace, "\x1f")) + "/tables/" + url.PathEscape(id.Name)
	if e := c.get(ctx, path, url.Values{"snapshots": {"refs"}}, &r); e != nil {
		return Table{}, e
	}
	m := r.Metadata
	if m.FormatVersion < 1 || m.FormatVersion > 3 || !uuidPattern.MatchString(m.UUID) || !storageLocation(m.Location, true) || !storageLocation(r.MetadataLocation, false) || m.LastUpdatedMS < 0 || m.LastUpdatedMS > 9007199254740991 || m.CurrentSchemaID < 0 || m.CurrentSchemaID > 2147483647 || len(m.Schemas) > 1000 {
		return Table{}, failure("invalid table metadata")
	}
	var current *Schema
	for _, s := range m.Schemas {
		if s.ID == m.CurrentSchemaID {
			if current != nil || s.Type != "struct" || len(s.Fields) > 1000 {
				return Table{}, failure("invalid table schema")
			}
			fields, e := safeFields(s.Fields, 0)
			if e != nil {
				return Table{}, e
			}
			current = &Schema{s.ID, fields}
		}
	}
	if current == nil {
		return Table{}, failure("current schema missing")
	}
	return Table{Identifier: Identifier{append([]string(nil), id.Namespace...), id.Name}, FormatVersion: m.FormatVersion, UUID: m.UUID, Location: m.Location, MetadataLocation: r.MetadataLocation, LastUpdatedMS: m.LastUpdatedMS, CurrentSnapshotID: m.CurrentSnapshotID, CurrentSchema: *current}, nil
}
func safeFields(fields []Field, depth int) ([]Field, error) {
	if len(fields) > 1000 {
		return nil, failure("field count limit")
	}
	out := make([]Field, 0, len(fields))
	ids := map[int]bool{}
	names := map[string]bool{}
	for _, f := range fields {
		if f.ID <= 0 || f.ID > 2147483647 || ids[f.ID] || !component(f.Name) || names[f.Name] {
			return nil, failure("invalid table field")
		}
		ids[f.ID] = true
		names[f.Name] = true
		t, e := safeType(f.Type, depth)
		if e != nil {
			return nil, e
		}
		f.Type = t
		out = append(out, f)
	}
	return out, nil
}

var primitive = regexp.MustCompile(`^(boolean|int|long|float|double|date|time|timestamp|timestamptz|timestamp_ns|timestamptz_ns|string|uuid|binary|unknown|variant|fixed\[[1-9][0-9]{0,8}\]|decimal\([1-9][0-9]?, ?[0-9]{1,2}\))$`)

func safeType(raw json.RawMessage, depth int) (json.RawMessage, error) {
	if depth > 16 || len(raw) > 65536 {
		return nil, failure("table type limit")
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		if !primitive.MatchString(text) {
			return nil, failure("unsupported table type")
		}
		if strings.HasPrefix(text, "decimal(") {
			parts := strings.Split(strings.TrimSuffix(strings.TrimPrefix(text, "decimal("), ")"), ",")
			precision, _ := strconv.Atoi(parts[0])
			scale, _ := strconv.Atoi(strings.TrimSpace(parts[1]))
			if precision > 38 || scale > precision {
				return nil, failure("invalid decimal type")
			}
		}
		return json.Marshal(text)
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal(raw, &obj) != nil {
		return nil, failure("invalid table type")
	}
	var kind string
	if json.Unmarshal(obj["type"], &kind) != nil {
		return nil, failure("invalid table type")
	}
	clean := map[string]any{"type": kind}
	switch kind {
	case "struct":
		var fields []Field
		if json.Unmarshal(obj["fields"], &fields) != nil {
			return nil, failure("invalid nested schema")
		}
		safe, e := safeFields(fields, depth+1)
		if e != nil {
			return nil, e
		}
		clean["fields"] = safe
	case "list", "map":
		keys := []string{"element"}
		if kind == "map" {
			keys = []string{"key", "value"}
		}
		for _, key := range keys {
			typ, e := safeType(obj[key], depth+1)
			if e != nil {
				return nil, e
			}
			var id int
			if json.Unmarshal(obj[key+"-id"], &id) != nil || id <= 0 || id > 2147483647 {
				return nil, failure("invalid nested field id")
			}
			clean[key] = typ
			clean[key+"-id"] = id
			if key != "key" {
				var required bool
				if json.Unmarshal(obj[key+"-required"], &required) != nil {
					return nil, failure("invalid nested nullability")
				}
				clean[key+"-required"] = required
			}
		}
	default:
		return nil, failure("unsupported table type")
	}
	return json.Marshal(clean)
}
