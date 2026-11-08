package platform

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"dataslate.local/api/internal/catalogs/polaris"
)

const CatalogDiscoveryTTL = 5 * time.Minute
const catalogLoadEndpoint = "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}"

type CatalogSubject struct {
	ExecutionSubject
	Tables []polaris.Identifier `json:"tables"`
}
type CatalogBinding struct {
	ID              string           `json:"id"`
	WorkspaceID     string           `json:"workspaceId"`
	Name            string           `json:"name"`
	Kind            string           `json:"kind"`
	PolicyVersion   string           `json:"policyVersion"`
	Connection      polaris.Config   `json:"connection"`
	AllowedSubjects []CatalogSubject `json:"allowedSubjects"`
}

func catalogComponent(value string) bool {
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
func catalogNamespace(ns []string) bool {
	if len(ns) < 1 || len(ns) > 16 {
		return false
	}
	for _, v := range ns {
		if !catalogComponent(v) {
			return false
		}
	}
	return true
}
func catalogIdentifier(id polaris.Identifier) bool {
	return catalogNamespace(id.Namespace) && catalogComponent(id.Name)
}
func identifierKey(id polaris.Identifier) string { data, _ := json.Marshal(id); return string(data) }
func (b CatalogBinding) Validate() error {
	if !identifier.MatchString(b.ID) || !identifier.MatchString(b.WorkspaceID) || !utf8.ValidString(b.Name) || strings.TrimSpace(b.Name) == "" || len(b.Name) > 200 || b.Kind != "polaris" || !sqlHeaderValue.MatchString(b.PolicyVersion) {
		return errors.New("catalog binding requires valid identity, Polaris kind and policy version")
	}
	if err := polaris.ValidateConfig(b.Connection); err != nil {
		return errors.New("invalid Polaris connection configuration")
	}
	for _, p := range []string{b.Connection.CAFile, b.Connection.Auth.BearerTokenFile, b.Connection.Auth.ClientSecretFile} {
		if p != "" && (!filepath.IsAbs(p) || strings.ContainsAny(p, "\r\n\x00")) {
			return errors.New("catalog credentials and trust require absolute server-private file paths")
		}
	}
	if b.AllowedSubjects == nil || len(b.AllowedSubjects) > 1000 {
		return errors.New("catalog allowedSubjects must be an explicit bounded array")
	}
	seen := map[ExecutionSubject]bool{}
	for _, subject := range b.AllowedSubjects {
		if subject.Issuer == "" || subject.Subject == "" || len(subject.Issuer) > 2048 || len(subject.Subject) > 512 || strings.ContainsAny(subject.Issuer+subject.Subject, "\r\n\x00") || seen[subject.ExecutionSubject] || subject.Tables == nil || len(subject.Tables) > 1000 {
			return errors.New("catalog subjects require unique issuer/subject and explicit bounded table grants")
		}
		seen[subject.ExecutionSubject] = true
		tables := map[string]bool{}
		for _, table := range subject.Tables {
			key := identifierKey(table)
			if !catalogIdentifier(table) || tables[key] {
				return errors.New("catalog grants require distinct exact table identifiers")
			}
			tables[key] = true
		}
	}
	return nil
}
func LoadCatalogBindings(path string) ([]CatalogBinding, error) {
	if path == "" {
		return nil, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.New("catalog bindings file unavailable")
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 4*1024*1024+1))
	if err != nil || len(data) > 4*1024*1024 {
		return nil, errors.New("catalog bindings exceed 4 MiB or cannot be read")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var out []CatalogBinding
	if decoder.Decode(&out) != nil || out == nil || decoder.Decode(&struct{}{}) != io.EOF || len(out) > 1000 {
		return nil, errors.New("catalog bindings require one bounded JSON array")
	}
	seen := map[string]bool{}
	for _, b := range out {
		if err = b.Validate(); err != nil {
			return nil, err
		}
		key := b.WorkspaceID + "/" + b.ID
		if seen[key] {
			return nil, errors.New("duplicate catalog binding")
		}
		seen[key] = true
	}
	return out, nil
}
func (b CatalogBinding) Tables(subject ExecutionSubject) ([]polaris.Identifier, bool) {
	for _, grant := range b.AllowedSubjects {
		if grant.ExecutionSubject == subject {
			return grant.Tables, len(grant.Tables) > 0
		}
	}
	return nil, false
}
func (b CatalogBinding) Allows(subject ExecutionSubject, id polaris.Identifier) bool {
	tables, _ := b.Tables(subject)
	return slices.ContainsFunc(tables, func(t polaris.Identifier) bool { return t.Name == id.Name && slices.Equal(t.Namespace, id.Namespace) })
}
func (b CatalogBinding) Hash() string {
	data, _ := json.Marshal(b)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

type CatalogObservation struct {
	WorkspaceID  string    `json:"workspaceId"`
	BindingID    string    `json:"bindingId"`
	ConfigHash   string    `json:"configHash"`
	Verified     bool      `json:"verified"`
	CheckedAt    time.Time `json:"checkedAt"`
	Capabilities []string  `json:"capabilities"`
}

func (o CatalogObservation) Fresh(b CatalogBinding, now time.Time) bool {
	return o.WorkspaceID == b.WorkspaceID && o.BindingID == b.ID && o.ConfigHash == b.Hash() && o.Verified && !o.CheckedAt.After(now) && now.Sub(o.CheckedAt) < CatalogDiscoveryTTL
}

type CatalogBackend interface {
	Discover(context.Context, CatalogBinding) (polaris.Capabilities, error)
	LoadTable(context.Context, CatalogBinding, polaris.Identifier) (polaris.Table, error)
}

// Connections and credentials are administrator-controlled; caller grants are checked by the API.
type PolarisCatalogBackend struct {
	mu      sync.Mutex
	clients map[polaris.Config]*polaris.Client
}

func (b *PolarisCatalogBackend) client(config polaris.Config) (*polaris.Client, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.clients == nil {
		b.clients = map[polaris.Config]*polaris.Client{}
	}
	if client := b.clients[config]; client != nil {
		return client, nil
	}
	client, err := polaris.New(config)
	if err != nil {
		return nil, err
	}
	if len(b.clients) >= 128 {
		for key, old := range b.clients {
			old.Close()
			delete(b.clients, key)
			break
		}
	}
	b.clients[config] = client
	return client, nil
}
func (b *PolarisCatalogBackend) Discover(ctx context.Context, binding CatalogBinding) (polaris.Capabilities, error) {
	client, err := b.client(binding.Connection)
	if err != nil {
		return polaris.Capabilities{}, err
	}
	return client.Discover(ctx)
}
func (b *PolarisCatalogBackend) LoadTable(ctx context.Context, binding CatalogBinding, id polaris.Identifier) (polaris.Table, error) {
	client, err := b.client(binding.Connection)
	if err != nil {
		return polaris.Table{}, err
	}
	return client.LoadTable(ctx, id)
}
func sameCatalogBinding(a, b CatalogBinding) bool { return reflect.DeepEqual(a, b) }
