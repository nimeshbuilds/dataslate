package platform

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const maxOIDCDocumentBytes = 1 << 20

func validOIDCEndpoint(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "https" && u.Hostname() != "" && u.User == nil && u.Fragment == ""
}

func newOIDCHTTPClient(caFile string) (*http.Client, error) {
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, errors.New("OIDC HTTP transport is unavailable")
	}
	transport := base.Clone()
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	if caFile != "" {
		roots, err := oidcTrustRoots(caFile)
		if err != nil {
			return nil, err
		}
		transport.TLSClientConfig.RootCAs = roots
	}
	return &http.Client{
		Transport: oidcTransport{base: transport},
		Timeout:   10 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 3 || !validOIDCEndpoint(req.URL.String()) || len(via) == 0 || !strings.EqualFold(req.URL.Host, via[0].URL.Host) {
				return errors.New("OIDC redirect is not permitted")
			}
			return nil
		},
	}, nil
}

func oidcTrustRoots(path string) (*x509.CertPool, error) {
	invalid := errors.New("OIDC CA file must be a readable PEM certificate bundle of at most 1 MiB")
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() > maxOIDCDocumentBytes {
		return nil, invalid
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, invalid
	}
	defer f.Close()
	info, err = f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > maxOIDCDocumentBytes {
		return nil, invalid
	}
	data, err := io.ReadAll(io.LimitReader(f, maxOIDCDocumentBytes+1))
	if err != nil || len(data) > maxOIDCDocumentBytes {
		return nil, invalid
	}
	roots, err := x509.SystemCertPool()
	if err != nil {
		return nil, errors.New("OIDC system certificate trust is unavailable")
	}
	certificates := 0
	for len(bytes.TrimSpace(data)) > 0 {
		data = bytes.TrimSpace(data)
		// pem.Decode skips leading garbage; reject it explicitly instead of
		// silently accepting a partially malformed or secret-bearing bundle.
		if !bytes.HasPrefix(data, []byte("-----BEGIN CERTIFICATE-----")) {
			return nil, invalid
		}
		block, rest := pem.Decode(data)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return nil, invalid
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, invalid
		}
		roots.AddCert(cert)
		certificates++
		data = rest
	}
	if certificates == 0 {
		return nil, invalid
	}
	return roots, nil
}

// The library reads discovery/JWKS responses in full. Bound those reads before
// handing the body over, and enforce HTTPS on discovered key endpoints as well
// as on the configured issuer. No provider response or URL appears in errors.
type oidcTransport struct{ base http.RoundTripper }

func (t oidcTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if !validOIDCEndpoint(req.URL.String()) {
		return nil, errors.New("OIDC requires a valid HTTPS endpoint")
	}
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		return nil, errors.New("OIDC HTTPS request failed")
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxOIDCDocumentBytes+1))
	resp.Body.Close()
	if err != nil || len(data) > maxOIDCDocumentBytes {
		return nil, errors.New("OIDC response exceeded its read limit")
	}
	resp.Body = io.NopCloser(bytes.NewReader(data))
	resp.ContentLength = int64(len(data))
	return resp, nil
}
