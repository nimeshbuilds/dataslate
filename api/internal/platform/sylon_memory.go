package platform

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"dataslate.local/api/internal/llm"
)

const MaxSylonMemories = 100
const MaxSylonMemorySelections = 4

var ErrMemoryAccess = errors.New("current memory authority required")
var ErrMemoryRevision = errors.New("memory revision changed")
var ErrMemoryCapacity = errors.New("private memory capacity exhausted")

type SylonMemoryPolicy struct {
	Enabled        bool `json:"enabled"`
	DefaultTTLDays int  `json:"defaultTTLDays"`
	MaxTTLDays     int  `json:"maxTTLDays"`
}

func (p SylonMemoryPolicy) Validate() error {
	if !p.Enabled || p.DefaultTTLDays < 1 || p.MaxTTLDays < p.DefaultTTLDays || p.MaxTTLDays > 365 {
		return errors.New("memory requires explicit enablement and TTL days 1..365")
	}
	return nil
}

type SylonMemorySelection struct {
	ID       string `json:"id"`
	Revision int    `json:"revision"`
}
type SylonMemorySource struct {
	JobID    string `json:"jobId"`
	Revision int    `json:"revision"`
}
type SylonMemoryReference struct {
	ID          string `json:"id"`
	Revision    int    `json:"revision"`
	SourceJobID string `json:"sourceJobId"`
}
type SylonMemory struct {
	Resource
	WorkspaceID   string            `json:"workspaceId"`
	Owner         ExecutionSubject  `json:"owner"`
	BindingID     string            `json:"bindingId"`
	PolicyVersion string            `json:"policyVersion"`
	Title         string            `json:"title"`
	Content       string            `json:"content"`
	ExpiresAt     time.Time         `json:"expiresAt"`
	Source        SylonMemorySource `json:"source"`
	PurgePending  bool              `json:"purgePending"`
}
type SylonMemoryWork struct {
	Memory       SylonMemory
	PolicyHash   string
	State        string
	Dependencies []SylonDependency
	OutputHash   string
}
type SylonMemorySummary struct {
	ID           string    `json:"id"`
	WorkspaceID  string    `json:"workspaceId"`
	Revision     int       `json:"revision"`
	State        string    `json:"state"`
	ExpiresAt    time.Time `json:"expiresAt"`
	PurgePending bool      `json:"purgePending"`
	Title        string    `json:"title,omitempty"`
	BindingID    string    `json:"bindingId,omitempty"`
}
type SylonMemoryReceipt struct {
	MemoryID         string    `json:"memoryId"`
	WorkspaceID      string    `json:"workspaceId"`
	Operation        string    `json:"operation"`
	PreviousRevision int       `json:"previousRevision"`
	Revision         int       `json:"revision"`
	AppliedAt        time.Time `json:"appliedAt"`
}
type SylonMemoryMutation struct {
	Receipt  SylonMemoryReceipt `json:"receipt"`
	Replayed bool               `json:"replayed"`
	Memory   SylonMemorySummary `json:"memory"`
}
type SylonMemoryInput struct {
	IdempotencyKey string             `json:"idempotencyKey"`
	Source         *SylonMemorySource `json:"source,omitempty"`
	Revision       int                `json:"revision,omitempty"`
	Title          string             `json:"title"`
	Content        string             `json:"content"`
	ExpiresAt      time.Time          `json:"expiresAt"`
}
type SylonMemoryDeleteInput struct {
	IdempotencyKey string `json:"idempotencyKey"`
	Revision       int    `json:"revision"`
}
type SylonMemoryRecallInput struct {
	BindingID string `json:"bindingId"`
	Query     string `json:"query"`
	Limit     int    `json:"limit"`
}
type SylonMemoryHit struct {
	Memory SylonMemory `json:"memory"`
	Score  float64     `json:"score"`
}

func (in SylonMemoryInput) Validate(create bool) error {
	if !idempotencyKeyPattern.MatchString(in.IdempotencyKey) || !utf8.ValidString(in.Title) || strings.TrimSpace(in.Title) == "" || len(in.Title) > 200 || strings.ContainsRune(in.Title, 0) || !utf8.ValidString(in.Content) || strings.TrimSpace(in.Content) == "" || len(in.Content) > 4096 || strings.ContainsRune(in.Content, 0) || in.ExpiresAt.IsZero() {
		return errors.New("memory requires a key, bounded title/content and explicit expiration")
	}
	if create {
		if in.Source == nil || !identifier.MatchString(in.Source.JobID) || in.Source.Revision < 1 || in.Revision != 0 {
			return errors.New("memory requires exact succeeded source job revision")
		}
	} else if in.Source != nil || in.Revision < 1 {
		return errors.New("correction requires expected revision and cannot change provenance")
	}
	return nil
}
func SylonOutputHash(result *llm.Result) string {
	data, _ := json.Marshal(result)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
func memoryFingerprint(op, id string, in any) string {
	data, _ := json.Marshal(struct {
		Operation, ID string
		Input         any
	}{op, id, in})
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
func memoryDeadline(b SylonBinding, expiry time.Time) bool {
	return b.Memory != nil && b.Memory.Enabled && expiry.After(time.Now()) && !expiry.After(time.Now().Add(time.Duration(b.Memory.MaxTTLDays)*24*time.Hour))
}
func cloneMemory(w SylonMemoryWork) SylonMemoryWork {
	data, _ := json.Marshal(w)
	var out SylonMemoryWork
	_ = json.Unmarshal(data, &out)
	return out
}
func memoryContains(deps []SylonDependency, id string, through int) bool {
	for _, d := range deps {
		if d.Kind == "memory" && d.MemoryID == id && d.MemoryRevision <= through {
			return true
		}
	}
	return false
}
func memoryReceipt(w SylonMemoryWork, op string, previous int) SylonMemoryReceipt {
	return SylonMemoryReceipt{w.Memory.ID, w.Memory.WorkspaceID, op, previous, w.Memory.Revision, w.Memory.UpdatedAt}
}

type SylonMemoryStore interface {
	SylonMemoryWork(context.Context, string, string, ExecutionSubject) (SylonMemoryWork, error)
	SylonMemories(context.Context, string, ExecutionSubject, string, bool) ([]SylonMemoryWork, error)
	SylonMemoryMutation(context.Context, string, ExecutionSubject, string, string) (SylonMemoryReceipt, bool, error)
	CreateSylonMemory(context.Context, SylonMemoryWork, string, string) (SylonMemoryReceipt, bool, error)
	CorrectSylonMemory(context.Context, string, string, ExecutionSubject, SylonMemoryInput, string) (SylonMemoryReceipt, bool, error)
	DeleteSylonMemory(context.Context, string, string, ExecutionSubject, SylonMemoryDeleteInput, string) (SylonMemoryReceipt, bool, error)
	PurgeSylonMemories(context.Context, int) (bool, error)
}

func memorySelectionDependencies(selections []SylonMemorySelection) []SylonDependency {
	out := []SylonDependency{}
	for _, m := range selections {
		out = append(out, SylonDependency{Kind: "memory", MemoryID: m.ID, MemoryRevision: m.Revision})
	}
	return out
}
