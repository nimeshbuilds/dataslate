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
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"dataslate.local/api/internal/llm"
)

const MaxSylonHistoryMessages = 20
const MaxSylonDependencies = 20
const MaxSylonWorkspaceJobs = 100

var ErrSylonCapacity = errors.New("conversation or workspace assistant capacity exhausted")
var ErrSylonRevision = errors.New("saved assistant context revision changed")
var ErrSylonLease = errors.New("assistant lease lost")
var ErrSylonAccess = errors.New("assistant context access denied")
var ErrSylonCancelled = errors.New("assistant output cancelled")

type SylonContextPolicy struct {
	NotebookCellSource bool     `json:"notebookCellSource"`
	CatalogBindingIDs  []string `json:"catalogBindingIds"`
	SearchBindingIDs   []string `json:"searchBindingIds"`
}
type SylonBinding struct {
	ID                   string             `json:"id"`
	WorkspaceID          string             `json:"workspaceId"`
	Name                 string             `json:"name"`
	PolicyVersion        string             `json:"policyVersion"`
	Provider             llm.Config         `json:"provider"`
	DataTransferAccepted bool               `json:"dataTransferAccepted"`
	AllowedSubjects      []ExecutionSubject `json:"allowedSubjects"`
	Context              SylonContextPolicy `json:"context"`
	MaxContextBytes      int                `json:"maxContextBytes"`
}

func (b SylonBinding) Validate() error {
	if !identifier.MatchString(b.ID) || !identifier.MatchString(b.WorkspaceID) || !utf8.ValidString(b.Name) || strings.TrimSpace(b.Name) == "" || len(b.Name) > 200 || !sqlHeaderValue.MatchString(b.PolicyVersion) || !b.DataTransferAccepted || b.MaxContextBytes < 1024 || b.MaxContextBytes > 98304 || b.AllowedSubjects == nil || len(b.AllowedSubjects) > 1000 {
		return errors.New("Sylon requires valid identity, explicit transfer approval and bounded context profile")
	}
	if err := llm.ValidateConfig(b.Provider); err != nil {
		return errors.New("invalid Sylon provider configuration")
	}
	for _, p := range []string{b.Provider.CAFile, b.Provider.Auth.BearerTokenFile, b.Provider.Auth.ClientSecretFile} {
		if p != "" && (!filepath.IsAbs(p) || strings.ContainsAny(p, "\r\n\x00")) {
			return errors.New("Sylon credential and CA paths must be absolute")
		}
	}
	seen := map[ExecutionSubject]bool{}
	for _, subject := range b.AllowedSubjects {
		if subject.Issuer == "" || subject.Subject == "" || len(subject.Issuer) > 2048 || len(subject.Subject) > 512 || strings.ContainsAny(subject.Issuer+subject.Subject, "\r\n\x00") || seen[subject] {
			return errors.New("Sylon subjects must be exact unique issuer/subject pairs")
		}
		seen[subject] = true
	}
	for _, ids := range [][]string{b.Context.CatalogBindingIDs, b.Context.SearchBindingIDs} {
		if len(ids) > 1000 {
			return errors.New("Sylon context binding limit exceeded")
		}
		seen := map[string]bool{}
		for _, id := range ids {
			if !identifier.MatchString(id) || seen[id] {
				return errors.New("Sylon context bindings must be exact and unique")
			}
			seen[id] = true
		}
	}
	return nil
}
func (b SylonBinding) Allows(owner ExecutionSubject) bool {
	return slices.Contains(b.AllowedSubjects, owner)
}
func (b SylonBinding) Hash() string {
	data, _ := json.Marshal(b)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
func LoadSylonBindings(path string) ([]SylonBinding, error) {
	if path == "" {
		return nil, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.New("Sylon bindings unavailable")
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, 4*1024*1024+1))
	if err != nil || len(data) > 4*1024*1024 {
		return nil, errors.New("Sylon bindings exceed 4 MiB")
	}
	var out []SylonBinding
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&out) != nil || out == nil || len(out) > 1000 || decoder.Decode(&struct{}{}) != io.EOF {
		return nil, errors.New("Sylon bindings require one bounded array")
	}
	seen := map[string]bool{}
	for _, b := range out {
		if err = b.Validate(); err != nil {
			return nil, err
		}
		key := b.WorkspaceID + "/" + b.ID
		if seen[key] {
			return nil, errors.New("duplicate Sylon binding")
		}
		seen[key] = true
	}
	return out, nil
}

type SylonConversation struct {
	Resource
	WorkspaceID string           `json:"workspaceId"`
	BindingID   string           `json:"bindingId"`
	Owner       ExecutionSubject `json:"owner"`
	Title       string           `json:"title"`
}
type SylonConversationInput struct {
	BindingID string `json:"bindingId"`
	Title     string `json:"title"`
}

func (in SylonConversationInput) Validate() error {
	if !identifier.MatchString(in.BindingID) || !utf8.ValidString(in.Title) || strings.TrimSpace(in.Title) == "" || len(in.Title) > 200 || strings.ContainsRune(in.Title, 0) {
		return errors.New("Sylon conversation requires a binding and bounded title")
	}
	return nil
}

type SylonSearchSelection struct {
	BindingID string `json:"bindingId"`
	Query     string `json:"query"`
	Mode      string `json:"mode"`
}
type SylonMessageInput struct {
	IdempotencyKey string                   `json:"idempotencyKey"`
	Prompt         string                   `json:"prompt"`
	Notebook       *SQLSelection            `json:"notebook,omitempty"`
	Catalogs       []SearchCatalogSelection `json:"catalogs"`
	Search         *SylonSearchSelection    `json:"search,omitempty"`
}

func (in SylonMessageInput) Validate() error {
	if !idempotencyKeyPattern.MatchString(in.IdempotencyKey) || !utf8.ValidString(in.Prompt) || strings.TrimSpace(in.Prompt) == "" || len(in.Prompt) > 8192 || strings.ContainsRune(in.Prompt, 0) || len(in.Catalogs) > 4 {
		return errors.New("Sylon message requires a key, 1–8192 byte prompt and at most four catalogs")
	}
	if n := in.Notebook; n != nil && (!identifier.MatchString(n.ID) || !identifier.MatchString(n.CellID) || n.Revision < 1) {
		return errors.New("cell context requires exact saved notebook revision and cell")
	}
	seen := map[string]bool{}
	for _, c := range in.Catalogs {
		key := searchCatalogKey(c)
		if !identifier.MatchString(c.BindingID) || !catalogIdentifier(c.Identifier) || seen[key] {
			return errors.New("invalid or duplicate catalog context")
		}
		seen[key] = true
	}
	if q := in.Search; q != nil {
		if err := (SearchRequest{BindingID: q.BindingID, Query: q.Query, Mode: q.Mode, Limit: 4}).Validate(); err != nil {
			return err
		}
	}
	return nil
}
func sylonFingerprint(in SylonMessageInput) string {
	in.IdempotencyKey = ""
	data, _ := json.Marshal(in)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

type SylonNotebookReference struct {
	ID       string `json:"id"`
	Revision int    `json:"revision"`
	CellID   string `json:"cellId,omitempty"`
}
type SylonCitation struct {
	ID               string                   `json:"id"`
	Kind             string                   `json:"kind"`
	Title            string                   `json:"title"`
	Historical       bool                     `json:"historical"`
	Notebook         *SylonNotebookReference  `json:"notebook,omitempty"`
	Catalog          *SearchCatalogProvenance `json:"catalog,omitempty"`
	SearchDocumentID string                   `json:"searchDocumentId,omitempty"`
}

// Dependencies include every supplied source, regardless of whether a model chooses to cite it.
type SylonDependency struct {
	Kind             string                  `json:"kind"`
	NotebookID       string                  `json:"notebookId,omitempty"`
	Catalog          *SearchCatalogSelection `json:"catalog,omitempty"`
	AuthorityHash    string                  `json:"authorityHash,omitempty"`
	SearchBindingID  string                  `json:"searchBindingId,omitempty"`
	SearchPolicyHash string                  `json:"searchPolicyHash,omitempty"`
}
type SylonPreparedContext struct {
	Sources      string            `json:"sources"`
	Instructions string            `json:"instructions"`
	Input        string            `json:"input"`
	Citations    []SylonCitation   `json:"citations"`
	Dependencies []SylonDependency `json:"dependencies"`
}
type SylonApplyInput struct {
	NotebookID string `json:"notebookId"`
	Revision   int    `json:"revision"`
	CellID     string `json:"cellId"`
}
type SylonApplyReceipt struct {
	JobID            string    `json:"jobId"`
	WorkspaceID      string    `json:"workspaceId"`
	NotebookID       string    `json:"notebookId"`
	CellID           string    `json:"cellId"`
	PreviousRevision int       `json:"previousRevision"`
	Revision         int       `json:"revision"`
	AppliedAt        time.Time `json:"appliedAt"`
}
type SylonJob struct {
	Resource
	WorkspaceID     string                   `json:"workspaceId"`
	ConversationID  string                   `json:"conversationId"`
	BindingID       string                   `json:"bindingId"`
	SubmittedBy     ExecutionSubject         `json:"submittedBy"`
	PolicyVersion   string                   `json:"policyVersion"`
	State           string                   `json:"state"`
	Message         string                   `json:"message"`
	DispatchStarted bool                     `json:"dispatchStarted"`
	CancelRequested bool                     `json:"cancelRequested"`
	Prompt          string                   `json:"prompt"`
	Notebook        *SQLSelection            `json:"notebook,omitempty"`
	Catalogs        []SearchCatalogSelection `json:"catalogs"`
	Search          *SylonSearchSelection    `json:"search,omitempty"`
	Citations       []SylonCitation          `json:"citations"`
	Result          *llm.Result              `json:"result,omitempty"`
	Applied         *SylonApplyReceipt       `json:"applied,omitempty"`
}

func (j SylonJob) Terminal() bool {
	return j.State == "succeeded" || j.State == "failed" || j.State == "unknown" || j.State == "cancelled"
}
func newSylonJob(c SylonConversation, b SylonBinding, in SylonMessageInput) SylonJob {
	catalogs := in.Catalogs
	if catalogs == nil {
		catalogs = []SearchCatalogSelection{}
	}
	return SylonJob{Resource: newResource("syl-"), WorkspaceID: c.WorkspaceID, ConversationID: c.ID, BindingID: b.ID, SubmittedBy: c.Owner, PolicyVersion: b.PolicyVersion, State: "queued", Message: "Queued for current context authorization.", Prompt: in.Prompt, Notebook: in.Notebook, Catalogs: catalogs, Search: in.Search, Citations: []SylonCitation{}}
}

type SylonWork struct {
	Job        SylonJob
	PolicyHash string
	Context    SylonPreparedContext
}
type SylonLease struct {
	ID, Owner string
	Fence     int64
}

type SylonGenerator interface {
	Generate(context.Context, llm.Config, llm.Request) (llm.Result, error)
}
type ProviderSylonGenerator struct {
	mu      sync.Mutex
	clients map[llm.Config]*llm.Client
}

func (p *ProviderSylonGenerator) Generate(ctx context.Context, config llm.Config, in llm.Request) (llm.Result, error) {
	p.mu.Lock()
	if p.clients == nil {
		p.clients = map[llm.Config]*llm.Client{}
	}
	client := p.clients[config]
	if client == nil {
		var err error
		client, err = llm.New(config)
		if err != nil {
			p.mu.Unlock()
			return llm.Result{}, err
		}
		if len(p.clients) >= 128 {
			for key, old := range p.clients {
				old.Close()
				delete(p.clients, key)
				break
			}
		}
		p.clients[config] = client
	}
	p.mu.Unlock()
	return client.Generate(ctx, in)
}
