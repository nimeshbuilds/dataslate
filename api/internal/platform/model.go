package platform

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"regexp"
	"strings"
	"time"
)

var (
	ErrNotFound = errors.New("resource not found")
	ErrConflict = errors.New("revision or unique name conflict")
	identifier  = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
	dnsLabel    = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)
)

type Resource struct {
	ID        string    `json:"id"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
	Revision  int       `json:"revision"`
}
type Workspace struct {
	Resource
	Name string `json:"name"`
	Slug string `json:"slug"`
}
type Cell struct {
	ID       string `json:"id"`
	Language string `json:"language"`
	Source   string `json:"source"`
}
type Notebook struct {
	Resource
	WorkspaceID string `json:"workspaceId"`
	Name        string `json:"name"`
	Kind        string `json:"kind"`
	Cells       []Cell `json:"cells"`
}
type NotebookInput struct {
	Name     string `json:"name"`
	Kind     string `json:"kind"`
	Cells    []Cell `json:"cells"`
	Revision int    `json:"revision,omitempty"`
}

func (n NotebookInput) Validate(update bool) error {
	if strings.TrimSpace(n.Name) == "" || len(n.Name) > 200 {
		return errors.New("name must contain 1–200 bytes")
	}
	if n.Kind != "notebook" && n.Kind != "sql" {
		return errors.New("kind must be notebook or sql")
	}
	if len(n.Cells) > 200 || n.Cells == nil {
		return errors.New("cells must be an array of at most 200 cells")
	}
	seen := map[string]bool{}
	for _, c := range n.Cells {
		if !identifier.MatchString(c.ID) || seen[c.ID] {
			return errors.New("cell IDs must be unique lowercase identifiers, at most 63 characters")
		}
		seen[c.ID] = true
		if c.Language != "python" && c.Language != "sql" {
			return errors.New("cell language must be python or sql")
		}
		if n.Kind == "sql" && c.Language != "sql" {
			return errors.New("SQL documents can contain only SQL cells")
		}
		if len(c.Source) > 256*1024 {
			return errors.New("cell source exceeds 256 KiB")
		}
	}
	if update && n.Revision < 1 {
		return errors.New("revision is required for update")
	}
	if !update && n.Revision != 0 {
		return errors.New("revision is assigned by the server on creation")
	}
	return nil
}
func newResource(prefix string) Resource {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	now := time.Now().UTC()
	return Resource{ID: prefix + hex.EncodeToString(b[:]), CreatedAt: now, UpdatedAt: now, Revision: 1}
}
func cloneNotebook(n Notebook) Notebook { n.Cells = append([]Cell{}, n.Cells...); return n }
