// Package kubeexec implements the Apache Spark Kubernetes Operator 1.0.0 API.
// It deliberately has no kubeconfig or ambient cloud credential fallback.
package kubeexec

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
)

const maxResponseBytes = 2 << 20

var dnsName = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)

type Config struct {
	ServerURL         string
	CAFile            string
	TokenFile         string
	AllowedNamespaces []string
	Timeout           time.Duration
}

// ConfigFromEnv uses the projected Kubernetes service account credentials.
// The token file is reopened for each request, including after token rotation.
func ConfigFromEnv() Config {
	c := Config{ServerURL: os.Getenv("DATASLATE_KUBERNETES_API_URL"), CAFile: os.Getenv("DATASLATE_KUBERNETES_CA_FILE"), TokenFile: os.Getenv("DATASLATE_KUBERNETES_TOKEN_FILE"), AllowedNamespaces: strings.Fields(os.Getenv("DATASLATE_EXECUTION_NAMESPACES"))}
	if c.ServerURL == "" {
		c.ServerURL = "https://kubernetes.default.svc"
	}
	if c.CAFile == "" {
		c.CAFile = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	}
	if c.TokenFile == "" {
		c.TokenFile = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	}
	return c
}

type Client struct {
	base       string
	tokenFile  string
	http       *http.Client
	namespaces map[string]bool
}

// APIError contains a safe category and status, never a response body or URL.
type APIError struct {
	Status   int
	Category string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("Kubernetes %s (status %d)", e.Category, e.Status)
}
func statusIs(err error, status int) bool {
	var e *APIError
	return errors.As(err, &e) && e.Status == status
}

func New(c Config) (*Client, error) {
	u, err := url.Parse(c.ServerURL)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return nil, errors.New("Kubernetes endpoint must be an HTTPS origin")
	}
	if len(c.AllowedNamespaces) == 0 {
		return nil, errors.New("explicit execution namespace allowlist is required")
	}
	namespaces := map[string]bool{}
	for _, ns := range c.AllowedNamespaces {
		if !dnsName.MatchString(ns) {
			return nil, errors.New("invalid execution namespace")
		}
		namespaces[ns] = true
	}
	ca, err := os.ReadFile(c.CAFile)
	if err != nil {
		return nil, errors.New("cannot read Kubernetes CA certificate")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("Kubernetes CA certificate is invalid")
	}
	if _, err := readToken(c.TokenFile); err != nil {
		return nil, err
	}
	if c.Timeout == 0 {
		c.Timeout = 8 * time.Second
	}
	if c.Timeout < time.Second || c.Timeout > 15*time.Second {
		return nil, errors.New("Kubernetes timeout must be between 1 and 15 seconds")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}, DialContext: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: c.Timeout, IdleConnTimeout: 60 * time.Second, MaxIdleConnsPerHost: 4}
	return &Client{base: strings.TrimRight(c.ServerURL, "/"), tokenFile: c.TokenFile, namespaces: namespaces, http: &http.Client{Transport: transport, Timeout: c.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("Kubernetes redirects are forbidden") }}}, nil
}

func readToken(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", errors.New("cannot read Kubernetes service account token")
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 64*1024+1))
	if err != nil || len(b) > 64*1024 {
		return "", errors.New("invalid Kubernetes service account token")
	}
	token := strings.TrimSpace(string(b))
	if token == "" || strings.ContainsAny(token, " \t\r\n") {
		return "", errors.New("invalid Kubernetes service account token")
	}
	return token, nil
}

func (c *Client) request(ctx context.Context, method, path string, input, output any) error {
	b, err := c.requestBytes(ctx, method, path, input, maxResponseBytes)
	if err != nil {
		return err
	}
	if output == nil {
		return nil
	}
	if err := json.Unmarshal(b, output); err != nil {
		return &APIError{Category: "invalid JSON response"}
	}
	return nil
}

func (c *Client) requestBytes(ctx context.Context, method, path string, input any, limit int64) ([]byte, error) {
	var body []byte
	var err error
	if input != nil {
		body, err = json.Marshal(input)
		if err != nil {
			return nil, errors.New("invalid Kubernetes request")
		}
	}
	token, err := readToken(c.tokenFile)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, bytes.NewReader(body))
	if err != nil {
		return nil, errors.New("invalid Kubernetes request")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	if input != nil {
		req.Header.Set("Content-Type", "application/json")
		if method == http.MethodPatch {
			req.Header.Set("Content-Type", "application/json-patch+json")
		}
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, &APIError{Category: "request failed; outcome requires reconciliation"}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &APIError{Status: resp.StatusCode, Category: "request rejected"}
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, &APIError{Category: "incomplete response; outcome requires reconciliation"}
	}
	if int64(len(b)) > limit {
		return nil, &APIError{Category: "response exceeded size limit"}
	}
	return b, nil
}

// Discover checks the served version and resource verbs without cluster-wide CRD privileges.
func (c *Client) Discover(ctx context.Context) error {
	var discovery struct {
		GroupVersion string `json:"groupVersion"`
		Resources    []struct {
			Name       string   `json:"name"`
			Kind       string   `json:"kind"`
			Namespaced bool     `json:"namespaced"`
			Verbs      []string `json:"verbs"`
		} `json:"resources"`
	}
	if err := c.request(ctx, http.MethodGet, "/apis/spark.apache.org/v1", nil, &discovery); err != nil {
		return err
	}
	if discovery.GroupVersion != "spark.apache.org/v1" {
		return errors.New("Apache Spark v1 API is unavailable")
	}
	for _, r := range discovery.Resources {
		if r.Name != "sparkapplications" || r.Kind != "SparkApplication" || !r.Namespaced {
			continue
		}
		verbs := map[string]bool{}
		for _, v := range r.Verbs {
			verbs[v] = true
		}
		if verbs["get"] && verbs["create"] && verbs["delete"] {
			return nil
		}
	}
	return errors.New("Apache Spark v1 SparkApplication resource is unavailable")
}
