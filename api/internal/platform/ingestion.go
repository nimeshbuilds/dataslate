package platform

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"reflect"
	"regexp"
	"strings"
)

const FileIngestionBatch = "file-ingestion-batch"

var (
	ErrIngestionRevision         = errors.New("saved ingestion pipeline revision changed")
	ErrIngestionNotExecutable    = errors.New("ingestion pipeline cannot execute")
	ErrIngestionStorageDenied    = errors.New("ingestion locations are outside binding storage grants")
	ErrIngestionConfigImmutable  = errors.New("ingestion configuration is immutable; create a new pipeline with new write locations")
	ErrIngestionLocationConflict = errors.New("ingestion write location overlaps a permanent reservation")
	ErrIngestionWriterBusy       = errors.New("ingestion writer is active or its cleanup is unresolved")
	ingestionFieldName           = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)
	ingestionPathSegment         = regexp.MustCompile(`^[A-Za-z0-9_-][A-Za-z0-9_.=-]*$`)
	ingestionBucket              = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
)

type IngestionField struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}
type IngestionSource struct {
	Format string           `json:"format"`
	URI    string           `json:"uri"`
	Schema []IngestionField `json:"schema"`
}
type IngestionDestination struct {
	URI string `json:"uri"`
}
type IngestionSpec struct {
	Source             IngestionSource      `json:"source"`
	Destination        IngestionDestination `json:"destination"`
	CheckpointURI      string               `json:"checkpointUri"`
	MaxFilesPerTrigger int                  `json:"maxFilesPerTrigger"`
}
type IngestionPipeline struct {
	Resource
	WorkspaceID string `json:"workspaceId"`
	Name        string `json:"name"`
	IngestionSpec
}
type IngestionPipelineInput struct {
	Name string `json:"name"`
	IngestionSpec
	Revision int `json:"revision,omitempty"`
}
type PipelineSelection struct {
	ID       string `json:"id"`
	Revision int    `json:"revision"`
}
type IngestionProvenance struct {
	PipelineSelection
	SHA256         string `json:"sha256"`
	Bytes          int    `json:"bytes"`
	SourceURI      string `json:"sourceUri"`
	DestinationURI string `json:"destinationUri"`
	CheckpointURI  string `json:"checkpointUri"`
}
type IngestionStorage struct {
	ReadPrefixes  []string `json:"readPrefixes"`
	WritePrefixes []string `json:"writePrefixes"`
}
type ingestionReservation struct{ PipelineID, WorkspaceID, DestinationURI, CheckpointURI string }

func validateS3Prefix(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || len(raw) > 2048 || u.Scheme != "s3" || !ingestionBucket.MatchString(u.Host) || strings.Contains(u.Host, "..") || strings.Contains(u.Host, ".-") || strings.Contains(u.Host, "-.") || net.ParseIP(u.Host) != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" || u.ForceQuery || raw != "s3://"+u.Host+u.Path || !strings.HasSuffix(u.Path, "/") || len(u.Path) < 3 {
		return errors.New("locations require canonical s3://bucket/nonempty-prefix/ URIs without credentials, query, escapes or wildcards")
	}
	for _, segment := range strings.Split(strings.Trim(u.Path, "/"), "/") {
		if !ingestionPathSegment.MatchString(segment) {
			return errors.New("S3 prefix contains an unsupported or traversal path segment")
		}
	}
	if strings.Contains(u.Path, "//") {
		return errors.New("S3 prefix contains empty segments")
	}
	return nil
}
func prefixesOverlap(a, b string) bool { return strings.HasPrefix(a, b) || strings.HasPrefix(b, a) }
func (s IngestionSpec) Validate() error {
	if s.Source.Format != "json" && s.Source.Format != "parquet" {
		return errors.New("source format must be json or parquet")
	}
	if s.MaxFilesPerTrigger < 1 || s.MaxFilesPerTrigger > 10000 {
		return errors.New("maxFilesPerTrigger must be 1–10000")
	}
	paths := []string{s.Source.URI, s.Destination.URI, s.CheckpointURI}
	for i, p := range paths {
		if err := validateS3Prefix(p); err != nil {
			return err
		}
		for _, q := range paths[:i] {
			if prefixesOverlap(p, q) {
				return errors.New("source, destination and checkpoint prefixes must be disjoint")
			}
		}
	}
	if len(s.Source.Schema) == 0 || len(s.Source.Schema) > 200 {
		return errors.New("an explicit schema of 1–200 fields is required")
	}
	seen := map[string]bool{}
	for _, f := range s.Source.Schema {
		key := strings.ToLower(f.Name)
		if !ingestionFieldName.MatchString(f.Name) || seen[key] {
			return errors.New("schema fields require unique case-insensitive simple names")
		}
		seen[key] = true
		switch f.Type {
		case "string", "long", "double", "boolean", "timestamp", "date":
		default:
			return errors.New("unsupported schema field type")
		}
	}
	return nil
}
func (in IngestionPipelineInput) Validate(update bool) error {
	if strings.TrimSpace(in.Name) == "" || len(in.Name) > 200 {
		return errors.New("name must contain 1–200 bytes")
	}
	if update && in.Revision < 1 || !update && in.Revision != 0 {
		return errors.New("revision is required only for updates")
	}
	return in.IngestionSpec.Validate()
}
func (s *IngestionStorage) Validate() error {
	if s == nil || len(s.ReadPrefixes) == 0 || len(s.WritePrefixes) == 0 || len(s.ReadPrefixes) > 100 || len(s.WritePrefixes) > 100 {
		return errors.New("ingestion bindings require 1–100 explicit readPrefixes and writePrefixes")
	}
	for _, group := range [][]string{s.ReadPrefixes, s.WritePrefixes} {
		seen := map[string]bool{}
		for _, p := range group {
			if e := validateS3Prefix(p); e != nil {
				return e
			}
			if seen[p] {
				return errors.New("duplicate ingestion storage prefix")
			}
			seen[p] = true
		}
	}
	return nil
}
func (s *IngestionStorage) Allows(spec IngestionSpec) bool {
	if s == nil {
		return false
	}
	contains := func(prefixes []string, p string) bool {
		for _, prefix := range prefixes {
			if strings.HasPrefix(p, prefix) {
				return true
			}
		}
		return false
	}
	return contains(s.ReadPrefixes, spec.Source.URI) && contains(s.WritePrefixes, spec.Destination.URI) && contains(s.WritePrefixes, spec.CheckpointURI)
}
func (r ingestionReservation) conflicts(spec IngestionSpec) bool {
	for _, a := range []string{r.DestinationURI, r.CheckpointURI} {
		for _, b := range []string{spec.Destination.URI, spec.CheckpointURI} {
			if prefixesOverlap(a, b) {
				return true
			}
		}
	}
	return false
}
func cloneIngestion(p IngestionPipeline) IngestionPipeline {
	p.Source.Schema = append([]IngestionField{}, p.Source.Schema...)
	return p
}
func snapshotIngestion(r Run, p IngestionPipeline) (Run, error) {
	if r.Profile.Kind() != FileIngestionBatch || r.Ingestion == nil || r.Notebook != nil || p.WorkspaceID != r.WorkspaceID || p.ID != r.Ingestion.ID {
		return Run{}, ErrIngestionNotExecutable
	}
	if p.Revision != r.Ingestion.Revision {
		return Run{}, ErrIngestionRevision
	}
	if p.IngestionSpec.Validate() != nil {
		return Run{}, ErrIngestionNotExecutable
	}
	if !r.Profile.IngestionStorage.Allows(p.IngestionSpec) {
		return Run{}, ErrIngestionStorageDenied
	}
	contract := ingestionSourceContract(r, p.IngestionSpec)
	config := struct {
		Version          int    `json:"version"`
		PipelineID       string `json:"pipelineId"`
		RunID            string `json:"runId"`
		PipelineRevision int    `json:"pipelineRevision"`
		IngestionSpec
		TimeoutSeconds       int    `json:"timeoutSeconds"`
		ReceiptBindingSHA256 string `json:"receiptBindingSha256"`
	}{1, p.ID, r.ID, p.Revision, p.IngestionSpec, r.Profile.RunLimitSeconds(), contract.ReceiptBindingSHA256}
	data, _ := json.Marshal(config)
	quoted, _ := json.Marshal(string(data))
	code := fmt.Sprintf("# DataSlate immutable file ingestion snapshot.\nimport json\nfrom dataslate_ingestion import run\nrun(json.loads(%s))\n", quoted)
	sum := sha256.Sum256([]byte(code))
	provenance := IngestionProvenance{PipelineSelection: PipelineSelection{p.ID, p.Revision}, SHA256: hex.EncodeToString(sum[:]), Bytes: len(code), SourceURI: p.Source.URI, DestinationURI: p.Destination.URI, CheckpointURI: p.CheckpointURI}
	r.Ingestion = &provenance
	r.Source = &RunSource{Source: code, Ingestion: &provenance, IngestionContract: &contract}
	r.IngestionOutcome = &IngestionOutcomeSummary{State: "pending"}
	r.SourceArtifact = &SourceArtifact{Name: r.KubernetesName + "-source", State: "pending"}
	return r, nil
}
func (r Run) validateIngestionSource() error {
	if r.Ingestion == nil || r.Notebook != nil || r.Source == nil || r.Source.Ingestion == nil || r.SourceArtifact == nil || r.SourceArtifact.Name != r.KubernetesName+"-source" {
		return ErrIngestionNotExecutable
	}
	p := r.Ingestion
	if c := r.Source.IngestionContract; c != nil {
		expected := ingestionSourceContract(r, c.Spec)
		if c.Spec.Validate() != nil || !reflect.DeepEqual(*c, expected) || c.Spec.Source.URI != p.SourceURI || c.Spec.Destination.URI != p.DestinationURI || c.Spec.CheckpointURI != p.CheckpointURI {
			return ErrIngestionNotExecutable
		}
	}
	if !identifier.MatchString(p.ID) || p.Revision < 1 || p.Bytes < 1 || p.Bytes > MaxNotebookSourceBytes || len(r.Source.Source) != p.Bytes || !reflect.DeepEqual(p, r.Source.Ingestion) {
		return ErrIngestionNotExecutable
	}
	for _, path := range []string{p.SourceURI, p.DestinationURI, p.CheckpointURI} {
		if validateS3Prefix(path) != nil {
			return ErrIngestionNotExecutable
		}
	}
	sum := sha256.Sum256([]byte(r.Source.Source))
	if hex.EncodeToString(sum[:]) != p.SHA256 {
		return ErrIngestionNotExecutable
	}
	return nil
}
func (r Run) SourceSHA256() string {
	if r.Ingestion != nil {
		return r.Ingestion.SHA256
	}
	if r.Notebook != nil {
		return r.Notebook.SHA256
	}
	return ""
}

// IngestionDrainBackend releases only after application and owned compute absence are verified.
type IngestionDrainBackend interface {
	DrainIngestion(context.Context, Run) (bool, error)
}
