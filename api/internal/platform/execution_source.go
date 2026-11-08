package platform

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

const MaxNotebookSourceBytes = 256 * 1024

var ErrNotebookRevision = errors.New("saved notebook revision changed")
var ErrNotebookNotExecutable = errors.New("notebook is not executable as a Python batch")

type NotebookSelection struct {
	ID       string `json:"id"`
	Revision int    `json:"revision"`
}
type NotebookProvenance struct {
	NotebookSelection
	SHA256  string   `json:"sha256"`
	CellIDs []string `json:"cellIds"`
	Bytes   int      `json:"bytes"`
}
type RunSource struct {
	Source    string               `json:"source"`
	Notebook  NotebookProvenance   `json:"notebook,omitzero"`
	Ingestion *IngestionProvenance `json:"ingestion,omitempty"`
}
type SourceArtifact struct {
	CreationStarted bool   `json:"creationStarted"`
	Name            string `json:"name"`
	UID             string `json:"uid,omitempty"`
	State           string `json:"state"`
}

// SourceBackend owns the immutable, deterministic source artifact lifecycle.
// Prepare is safe to retry only while no Spark submission has begun; UID changes are never adopted.
type SourceObservation struct {
	Exists bool
	UID    string
}
type SourceBackend interface {
	ObserveSource(context.Context, Run) (SourceObservation, error)
	PrepareSource(context.Context, Run) (SourceArtifact, error)
	BindSource(context.Context, Run) error
	DeleteSource(context.Context, Run) error
}

func validArtifactTransition(from, to string) bool {
	switch from {
	case "pending":
		return to == "pending" || to == "ready" || to == "cleanup_pending" || to == "deleted"
	case "ready":
		return to == "ready" || to == "cleanup_pending" || to == "deleted"
	case "cleanup_pending":
		return to == "cleanup_pending" || to == "deleted"
	case "deleted":
		return to == "deleted"
	}
	return false
}
func (r Run) NeedsReconciliation() bool {
	return !r.Terminal() || r.Ingestion != nil && !r.WriterReleased || r.SourceArtifact != nil && r.SourceArtifact.State != "deleted"
}
func snapshotNotebook(r Run, n Notebook) (Run, error) {
	if r.Profile.Kind() != NotebookPythonBatch || r.Notebook == nil || n.WorkspaceID != r.WorkspaceID || n.ID != r.Notebook.ID {
		return Run{}, ErrNotebookNotExecutable
	}
	if n.Revision != r.Notebook.Revision {
		return Run{}, ErrNotebookRevision
	}
	if n.Kind != "notebook" || len(n.Cells) == 0 {
		return Run{}, ErrNotebookNotExecutable
	}
	var source strings.Builder
	source.WriteString("# DataSlate immutable Python batch snapshot.\n" +
		"def __dataslate_batch(__cells, __compile=compile, __exec=exec):\n" +
		"    __namespace = {\"__name__\": \"__main__\", \"__file__\": __file__, \"__package__\": None, \"__spec__\": None, \"__builtins__\": __builtins__}\n" +
		"    for __source, __filename in __cells:\n" +
		"        __exec(__compile(__source, __filename, \"exec\"), __namespace)\n" +
		"__dataslate_batch([\n")
	ids := make([]string, 0, len(n.Cells))
	for _, cell := range n.Cells {
		if cell.Language != "python" || !identifier.MatchString(cell.ID) || !utf8.ValidString(cell.Source) || strings.ContainsRune(cell.Source, 0) {
			return Run{}, ErrNotebookNotExecutable
		}
		quoted, _ := json.Marshal(cell.Source)
		filename, _ := json.Marshal(fmt.Sprintf("dataslate://%s/%d/%s.py", n.ID, n.Revision, cell.ID))
		fmt.Fprintf(&source, "    (%s, %s), # cell: %s\n", quoted, filename, cell.ID)
		if source.Len() > MaxNotebookSourceBytes {
			return Run{}, ErrNotebookNotExecutable
		}
		ids = append(ids, cell.ID)
	}
	source.WriteString("])\n")
	if source.Len() > MaxNotebookSourceBytes {
		return Run{}, ErrNotebookNotExecutable
	}
	code := source.String()
	hash := sha256.Sum256([]byte(code))
	p := NotebookProvenance{NotebookSelection: NotebookSelection{ID: n.ID, Revision: n.Revision}, SHA256: hex.EncodeToString(hash[:]), CellIDs: ids, Bytes: len(code)}
	r.Notebook = &p
	r.Source = &RunSource{Source: code, Notebook: p}
	r.SourceArtifact = &SourceArtifact{Name: r.KubernetesName + "-source", State: "pending"}
	return r, nil
}

// ValidateSource checks independently persisted bytes against the immutable public provenance.
func (r Run) ValidateSource() error {
	if r.Profile.Kind() == FileIngestionBatch {
		return r.validateIngestionSource()
	}
	if r.Profile.Kind() != NotebookPythonBatch || r.Notebook == nil || r.Source == nil || r.SourceArtifact == nil || r.SourceArtifact.Name != r.KubernetesName+"-source" {
		return ErrNotebookNotExecutable
	}
	p := r.Notebook
	s := r.Source
	if p.ID == "" || p.Revision < 1 || len(p.CellIDs) == 0 || p.Bytes < 1 || p.Bytes > MaxNotebookSourceBytes || len(s.Source) != p.Bytes || !utf8.ValidString(s.Source) || s.Notebook.ID != p.ID || s.Notebook.Revision != p.Revision || s.Notebook.SHA256 != p.SHA256 || s.Notebook.Bytes != p.Bytes || strings.Join(s.Notebook.CellIDs, "\x00") != strings.Join(p.CellIDs, "\x00") {
		return ErrNotebookNotExecutable
	}
	sum := sha256.Sum256([]byte(s.Source))
	if hex.EncodeToString(sum[:]) != p.SHA256 {
		return ErrNotebookNotExecutable
	}
	return nil
}
