package platform

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"dataslate.local/api/internal/catalogs/polaris"
	"dataslate.local/api/internal/embeddings"
)

const MaxSearchSourceBytes = 64000
const MaxSearchChunks = 32
const SearchChunkBytes = 2000
const MaxSearchJobs = 100
const SearchCatalogTTL = 15 * time.Minute

var ErrSearchCapacity = errors.New("search index capacity exceeded")
var ErrSearchRevision = errors.New("search source revision changed")
var ErrSearchLease = errors.New("search lease lost")

type SearchCatalogGrant struct {
	BindingID string               `json:"bindingId"`
	Tables    []polaris.Identifier `json:"tables"`
}
type SearchSources struct {
	Notebooks         bool                 `json:"notebooks"`
	IncludeCellSource bool                 `json:"includeCellSource"`
	Catalogs          []SearchCatalogGrant `json:"catalogs"`
}
type SearchBinding struct {
	ID                   string             `json:"id"`
	WorkspaceID          string             `json:"workspaceId"`
	Name                 string             `json:"name"`
	PolicyVersion        string             `json:"policyVersion"`
	Mode                 string             `json:"mode"`
	Sources              SearchSources      `json:"sources"`
	MaxDocuments         int                `json:"maxDocuments"`
	Embedding            *embeddings.Config `json:"embedding,omitempty"`
	DataTransferAccepted bool               `json:"dataTransferAccepted"`
}

func (b SearchBinding) Validate() error {
	if !identifier.MatchString(b.ID) || !identifier.MatchString(b.WorkspaceID) || strings.TrimSpace(b.Name) == "" || !utf8.ValidString(b.Name) || len(b.Name) > 200 || !sqlHeaderValue.MatchString(b.PolicyVersion) || b.MaxDocuments < 1 || b.MaxDocuments > 1000 || (b.Mode != "lexical" && b.Mode != "hybrid") || (!b.Sources.Notebooks && b.Sources.IncludeCellSource) || len(b.Sources.Catalogs) > 1000 {
		return errors.New("invalid search binding identity, sources, mode or document budget")
	}
	if b.Mode == "hybrid" {
		if b.Embedding == nil || !b.DataTransferAccepted {
			return errors.New("hybrid search requires configured embeddings and explicit data transfer acceptance")
		}
		if err := embeddings.ValidateConfig(*b.Embedding); err != nil {
			return errors.New("invalid search embedding configuration")
		}
		for _, p := range []string{b.Embedding.CAFile, b.Embedding.Auth.BearerTokenFile, b.Embedding.Auth.ClientSecretFile} {
			if p != "" && (!filepath.IsAbs(p) || strings.ContainsAny(p, "\r\n\x00")) {
				return errors.New("embedding credential and CA paths must be absolute")
			}
		}
	} else if b.Embedding != nil || b.DataTransferAccepted {
		return errors.New("lexical mode must not configure embedding transfers")
	}
	seen := map[string]bool{}
	for _, g := range b.Sources.Catalogs {
		if !identifier.MatchString(g.BindingID) || seen[g.BindingID] || g.Tables == nil || len(g.Tables) > 1000 {
			return errors.New("search catalog grants must be explicit and unique")
		}
		seen[g.BindingID] = true
		tables := map[string]bool{}
		for _, id := range g.Tables {
			key := identifierKey(id)
			if !catalogIdentifier(id) || tables[key] {
				return errors.New("invalid search table grant")
			}
			tables[key] = true
		}
	}
	return nil
}
func (b SearchBinding) Hash() string {
	data, _ := json.Marshal(b)
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}
func (b SearchBinding) AllowsCatalog(id SearchCatalogSelection) bool {
	for _, g := range b.Sources.Catalogs {
		if g.BindingID == id.BindingID {
			for _, table := range g.Tables {
				if identifierKey(table) == identifierKey(id.Identifier) {
					return true
				}
			}
		}
	}
	return false
}
func LoadSearchBindings(path string) ([]SearchBinding, error) {
	if path == "" {
		return nil, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.New("search bindings unavailable")
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, 4*1024*1024+1))
	if err != nil || len(data) > 4*1024*1024 {
		return nil, errors.New("search bindings exceed 4 MiB")
	}
	var out []SearchBinding
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if d.Decode(&out) != nil || out == nil || len(out) > 1000 || d.Decode(&struct{}{}) != io.EOF {
		return nil, errors.New("search bindings require one bounded array")
	}
	seen := map[string]bool{}
	for _, b := range out {
		if err = b.Validate(); err != nil {
			return nil, err
		}
		key := b.WorkspaceID + "/" + b.ID
		if seen[key] {
			return nil, errors.New("duplicate search binding")
		}
		seen[key] = true
	}
	return out, nil
}

type SearchNotebookSelection struct {
	ID       string `json:"id"`
	Revision int    `json:"revision"`
}
type SearchCatalogSelection struct {
	BindingID string `json:"bindingId"`
	polaris.Identifier
}
type SearchCatalogProvenance struct {
	SearchCatalogSelection
	PolicyVersion string `json:"policyVersion"`
}
type SearchIndexInput struct {
	BindingID      string                   `json:"bindingId"`
	IdempotencyKey string                   `json:"idempotencyKey"`
	Notebook       *SearchNotebookSelection `json:"notebook,omitempty"`
	Catalog        *SearchCatalogSelection  `json:"catalog,omitempty"`
}

func (in SearchIndexInput) Validate() error {
	if !identifier.MatchString(in.BindingID) || !idempotencyKeyPattern.MatchString(in.IdempotencyKey) || (in.Notebook == nil) == (in.Catalog == nil) {
		return errors.New("index intent requires binding, key and exactly one saved source")
	}
	if n := in.Notebook; n != nil && (!identifier.MatchString(n.ID) || n.Revision < 1) {
		return errors.New("indexing requires exact notebook revision")
	}
	if c := in.Catalog; c != nil && (!identifier.MatchString(c.BindingID) || !catalogIdentifier(c.Identifier)) {
		return errors.New("invalid catalog source")
	}
	return nil
}
func searchFingerprint(in SearchIndexInput) string {
	in.IdempotencyKey = ""
	data, _ := json.Marshal(in)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

type SearchJob struct {
	Resource
	WorkspaceID      string                   `json:"workspaceId"`
	BindingID        string                   `json:"bindingId"`
	SubmittedBy      ExecutionSubject         `json:"submittedBy"`
	PolicyVersion    string                   `json:"policyVersion"`
	Notebook         *SearchNotebookSelection `json:"notebook,omitempty"`
	Catalog          *SearchCatalogSelection  `json:"catalog,omitempty"`
	State            string                   `json:"state"`
	Message          string                   `json:"message"`
	EmbeddingStarted bool                     `json:"embeddingStarted"`
	DocumentID       string                   `json:"documentId,omitempty"`
}

func (j SearchJob) Terminal() bool {
	return j.State == "succeeded" || j.State == "failed" || j.State == "unknown" || j.State == "superseded"
}
func newSearchJob(b SearchBinding, owner ExecutionSubject, in SearchIndexInput) SearchJob {
	return SearchJob{Resource: newResource("idx-"), WorkspaceID: b.WorkspaceID, BindingID: b.ID, SubmittedBy: owner, PolicyVersion: b.PolicyVersion, Notebook: in.Notebook, Catalog: in.Catalog, State: "queued", Message: "Queued for current source and policy validation."}
}

type SearchChunk struct {
	Text   string    `json:"text"`
	Vector []float32 `json:"vector,omitempty"`
}
type SearchDocument struct {
	ID            string                   `json:"id"`
	WorkspaceID   string                   `json:"workspaceId"`
	BindingID     string                   `json:"bindingId"`
	PolicyHash    string                   `json:"policyHash"`
	AuthorityHash string                   `json:"authorityHash,omitempty"`
	Kind          string                   `json:"kind"`
	Title         string                   `json:"title"`
	Notebook      *SearchNotebookSelection `json:"notebook,omitempty"`
	Catalog       *SearchCatalogProvenance `json:"catalog,omitempty"`
	IndexedAt     time.Time                `json:"indexedAt"`
	ExpiresAt     *time.Time               `json:"expiresAt,omitempty"`
	Chunks        []SearchChunk            `json:"chunks"`
}

func searchDocumentID(job SearchJob) string {
	var source any = job.Notebook
	if job.Catalog != nil {
		source = job.Catalog
	}
	data, _ := json.Marshal(source)
	if job.Notebook != nil {
		data = []byte("notebook:" + job.Notebook.ID)
	}
	h := sha256.Sum256(data)
	return "sdoc-" + hex.EncodeToString(h[:12])
}
func chunkSearchText(text string) ([]SearchChunk, error) {
	if !utf8.ValidString(text) || strings.ContainsRune(text, 0) || len(text) == 0 || len(text) > MaxSearchSourceBytes {
		return nil, errors.New("source exceeds 64000 UTF-8 bytes; no partial index was published")
	}
	out := []SearchChunk{}
	for len(text) > 0 {
		n := len(text)
		if n > SearchChunkBytes {
			n = SearchChunkBytes
			for !utf8.RuneStart(text[n]) {
				n--
			}
		}
		out = append(out, SearchChunk{Text: text[:n]})
		text = text[n:]
	}
	if len(out) > MaxSearchChunks {
		return nil, errors.New("source exceeds 32 chunks; no partial index was published")
	}
	return out, nil
}
func searchNotebookText(n Notebook, includeSource bool) string {
	languages := map[string]bool{}
	for _, cell := range n.Cells {
		languages[cell.Language] = true
	}
	list := []string{}
	for lang := range languages {
		list = append(list, lang)
	}
	sort.Strings(list)
	text := fmt.Sprintf("Notebook: %s\nKind: %s\nLanguages: %s\n", n.Name, n.Kind, strings.Join(list, ", "))
	if includeSource {
		for _, cell := range n.Cells {
			text += "\nCell " + cell.ID + " (" + cell.Language + "):\n" + cell.Source
		}
	}
	return text
}
func searchVectorValid(v []float32, dimensions int) bool {
	if len(v) != dimensions || dimensions < 1 || dimensions > 2000 {
		return false
	}
	var norm float64
	for _, n := range v {
		f := float64(n)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return false
		}
		norm += f * f
	}
	return norm > 0 && !math.IsInf(norm, 0)
}

type SearchRequest struct {
	BindingID string `json:"bindingId"`
	Query     string `json:"query"`
	Mode      string `json:"mode"`
	Limit     int    `json:"limit"`
}

func (r SearchRequest) Validate() error {
	if !identifier.MatchString(r.BindingID) || !utf8.ValidString(r.Query) || strings.TrimSpace(r.Query) == "" || len(r.Query) > 4096 || strings.ContainsRune(r.Query, 0) || (r.Mode != "lexical" && r.Mode != "hybrid") || r.Limit < 1 || r.Limit > 20 {
		return errors.New("search requires a binding, 1–4096 byte query, lexical/hybrid mode and limit 1–20")
	}
	return nil
}

type SearchHit struct {
	DocumentID string                   `json:"documentId"`
	Kind       string                   `json:"kind"`
	Title      string                   `json:"title"`
	Snippet    string                   `json:"snippet"`
	Score      float64                  `json:"score"`
	Notebook   *SearchNotebookSelection `json:"notebook,omitempty"`
	Catalog    *SearchCatalogProvenance `json:"catalog,omitempty"`
	IndexedAt  time.Time                `json:"indexedAt"`
	ExpiresAt  *time.Time               `json:"expiresAt,omitempty"`
}
type SearchScope struct {
	WorkspaceID, BindingID, PolicyHash string
	Notebooks                          bool
	Catalogs                           map[string]string
}

func (s SearchScope) Allows(doc SearchDocument) bool {
	if doc.WorkspaceID != s.WorkspaceID || doc.BindingID != s.BindingID || doc.PolicyHash != s.PolicyHash {
		return false
	}
	if doc.Notebook != nil {
		return s.Notebooks
	}
	if doc.Catalog == nil || doc.ExpiresAt == nil || !doc.ExpiresAt.After(time.Now()) {
		return false
	}
	return s.Catalogs[searchCatalogKey(doc.Catalog.SearchCatalogSelection)] == doc.AuthorityHash && doc.AuthorityHash != ""
}
func searchCatalogKey(id SearchCatalogSelection) string {
	data, _ := json.Marshal(id)
	return string(data)
}
func searchSnippet(text string) string {
	if len(text) <= 1200 {
		return text
	}
	n := 1200
	for !utf8.RuneStart(text[n]) {
		n--
	}
	return text[:n]
}

type SearchEmbedder interface {
	Embed(context.Context, embeddings.Config, []string) ([][]float32, error)
}
type GatewaySearchEmbedder struct {
	mu      sync.Mutex
	clients map[embeddings.Config]*embeddings.Client
}

func (e *GatewaySearchEmbedder) Embed(ctx context.Context, config embeddings.Config, input []string) ([][]float32, error) {
	e.mu.Lock()
	if e.clients == nil {
		e.clients = map[embeddings.Config]*embeddings.Client{}
	}
	client := e.clients[config]
	if client == nil {
		var err error
		client, err = embeddings.New(config)
		if err != nil {
			e.mu.Unlock()
			return nil, err
		}
		if len(e.clients) >= 128 {
			for key, old := range e.clients {
				old.Close()
				delete(e.clients, key)
				break
			}
		}
		e.clients[config] = client
	}
	e.mu.Unlock()
	return client.Embed(ctx, input)
}
