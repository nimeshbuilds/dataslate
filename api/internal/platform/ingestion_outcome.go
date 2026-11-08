package platform

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

const MaxIngestionReceiptBytes = 4096

type IngestionOutcomeSummary struct {
	State      string `json:"state"`
	ReasonCode string `json:"reasonCode,omitempty"`
}
type IngestionRuntime struct {
	Spark string `json:"spark"`
	Delta string `json:"delta"`
}
type IngestionDeltaTable struct {
	ID               string   `json:"id"`
	Version          string   `json:"version"`
	SchemaSHA256     string   `json:"schemaSha256"`
	MinReaderVersion int      `json:"minReaderVersion"`
	MinWriterVersion int      `json:"minWriterVersion"`
	ReaderFeatures   []string `json:"readerFeatures"`
	WriterFeatures   []string `json:"writerFeatures"`
}
type IngestionReceipt struct {
	Version              int                  `json:"version"`
	PipelineID           string               `json:"pipelineId"`
	RunID                string               `json:"runId"`
	PipelineRevision     int                  `json:"pipelineRevision"`
	ReceiptBindingSHA256 string               `json:"receiptBindingSha256"`
	State                string               `json:"state"`
	Runtime              IngestionRuntime     `json:"runtime"`
	ContractSHA256       string               `json:"contractSha256"`
	QueryID              string               `json:"queryId"`
	CompletedBatches     string               `json:"completedBatches"`
	ProcessedFiles       string               `json:"processedFiles"`
	ProcessedRows        string               `json:"processedRows"`
	LastBatchID          *string              `json:"lastBatchId"`
	TableExists          bool                 `json:"tableExists"`
	Table                *IngestionDeltaTable `json:"table"`
}
type IngestionEvidence struct {
	ApplicationUID string `json:"applicationUid"`
	DriverUID      string `json:"driverUid"`
	DriverName     string `json:"driverName"`
	SourceSHA256   string `json:"sourceSha256"`
	ReceiptSHA256  string `json:"receiptSha256"`
}
type IngestionOutcome struct {
	RunID       string `json:"runId"`
	WorkspaceID string `json:"workspaceId"`
	IngestionOutcomeSummary
	RecordedAt *time.Time         `json:"recordedAt,omitempty"`
	Receipt    *IngestionReceipt  `json:"receipt,omitempty"`
	Evidence   *IngestionEvidence `json:"evidence,omitempty"`
}

// A private, independently persisted source contract avoids parsing generated code.
type IngestionSourceContract struct {
	Spec                 IngestionSpec `json:"spec"`
	ReceiptBindingSHA256 string        `json:"receiptBindingSha256"`
	ContractSHA256       string        `json:"contractSha256"`
	SchemaSHA256         string        `json:"schemaSha256"`
}
type IngestionTermination struct {
	Final                                          bool
	ApplicationUID, DriverUID, DriverName, Message string
	ExitCode                                       int
}
type IngestionOutcomeBackend interface {
	CaptureIngestionOutcome(context.Context, Run) (IngestionTermination, error)
}

var receiptHex = regexp.MustCompile(`^[a-f0-9]{64}$`)
var receiptDecimal = regexp.MustCompile(`^(0|[1-9][0-9]{0,18})$`)
var receiptFeature = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]{0,63}$`)
var receiptUUID = regexp.MustCompile(`^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$`)
var ErrIngestionOutcome = errors.New("verified ingestion outcome is unavailable")

func digestJSON(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func ingestionSourceContract(r Run, spec IngestionSpec) IngestionSourceContract {
	fields := make([]map[string]any, 0, len(spec.Source.Schema))
	for _, f := range spec.Source.Schema {
		fields = append(fields, map[string]any{"name": f.Name, "type": f.Type, "nullable": true})
	}
	normalized := spec
	normalized.Source.URI = strings.Replace(spec.Source.URI, "s3://", "s3a://", 1)
	normalized.Destination.URI = strings.Replace(spec.Destination.URI, "s3://", "s3a://", 1)
	normalized.CheckpointURI = strings.Replace(spec.CheckpointURI, "s3://", "s3a://", 1)
	// Nested maps deliberately mirror Python's sort_keys canonical JSON.
	schema := make([]map[string]any, 0, len(spec.Source.Schema))
	for _, f := range spec.Source.Schema {
		schema = append(schema, map[string]any{"name": f.Name, "type": f.Type, "nullable": f.Nullable})
	}
	physical := map[string]any{"version": 1, "pipelineId": r.Ingestion.ID, "source": map[string]any{"format": normalized.Source.Format, "uri": normalized.Source.URI, "schema": schema}, "destination": map[string]any{"uri": normalized.Destination.URI}, "checkpointUri": normalized.CheckpointURI, "runtime": map[string]any{"spark": "4.1.3", "delta": "4.3.1"}, "semantics": "immutable-file-append-v1"}
	return IngestionSourceContract{Spec: spec, ReceiptBindingSHA256: digestJSON(map[string]any{"runId": r.ID, "pipelineId": r.Ingestion.ID, "pipelineRevision": r.Ingestion.Revision, "profile": r.Profile, "spec": spec}), ContractSHA256: digestJSON(physical), SchemaSHA256: digestJSON(fields)}
}

// Reject duplicate keys and excessive nesting before typed decode; Go's default decoder accepts duplicates.
func validateUniqueJSON(data []byte) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	var value func(int) error
	value = func(depth int) error {
		if depth > 16 {
			return ErrIngestionOutcome
		}
		t, e := d.Token()
		if e != nil {
			return e
		}
		delimiter, ok := t.(json.Delim)
		if !ok {
			return nil
		}
		switch delimiter {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				key, e := d.Token()
				if e != nil {
					return e
				}
				k, ok := key.(string)
				if !ok || seen[k] {
					return ErrIngestionOutcome
				}
				seen[k] = true
				if e = value(depth + 1); e != nil {
					return e
				}
			}
			_, e = d.Token()
			return e
		case '[':
			for d.More() {
				if e = value(depth + 1); e != nil {
					return e
				}
			}
			_, e = d.Token()
			return e
		default:
			return ErrIngestionOutcome
		}
	}
	if e := value(0); e != nil {
		return e
	}
	if _, e := d.Token(); e != io.EOF {
		return ErrIngestionOutcome
	}
	return nil
}
func verifyIngestionReceipt(r Run, t IngestionTermination, now time.Time) (IngestionOutcome, error) {
	fail := func() (IngestionOutcome, error) { return IngestionOutcome{}, ErrIngestionOutcome }
	if r.State != "succeeded" || r.KubernetesUID == "" || r.Source == nil || r.Source.IngestionContract == nil || r.ValidateSource() != nil || !t.Final || t.ExitCode != 0 || t.ApplicationUID != r.KubernetesUID || t.DriverUID == "" || t.DriverName == "" || len(t.Message) == 0 || len(t.Message) > MaxIngestionReceiptBytes || !utf8.ValidString(t.Message) || validateUniqueJSON([]byte(t.Message)) != nil {
		return fail()
	}
	var required map[string]json.RawMessage
	if json.Unmarshal([]byte(t.Message), &required) != nil || len(required) != 15 {
		return fail()
	}
	for _, key := range []string{"version", "pipelineId", "runId", "pipelineRevision", "receiptBindingSha256", "state", "runtime", "contractSha256", "queryId", "completedBatches", "processedFiles", "processedRows", "lastBatchId", "tableExists", "table"} {
		if raw, ok := required[key]; !ok || key != "lastBatchId" && key != "table" && bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return fail()
		}
	}
	if !exactReceiptObject(required["runtime"], []string{"spark", "delta"}) {
		return fail()
	}
	if !bytes.Equal(bytes.TrimSpace(required["table"]), []byte("null")) && !exactReceiptObject(required["table"], []string{"id", "version", "schemaSha256", "minReaderVersion", "minWriterVersion", "readerFeatures", "writerFeatures"}) {
		return fail()
	}
	var receipt IngestionReceipt
	d := json.NewDecoder(strings.NewReader(t.Message))
	d.DisallowUnknownFields()
	if d.Decode(&receipt) != nil || d.Decode(&struct{}{}) != io.EOF {
		return fail()
	}
	c := r.Source.IngestionContract
	if receipt.Version != 2 || receipt.PipelineID != r.Ingestion.ID || receipt.RunID != r.ID || receipt.PipelineRevision != r.Ingestion.Revision || receipt.ReceiptBindingSHA256 != c.ReceiptBindingSHA256 || receipt.ContractSHA256 != c.ContractSHA256 || receipt.State != "succeeded" || receipt.Runtime != (IngestionRuntime{"4.1.3", "4.3.1"}) || !receiptUUID.MatchString(receipt.QueryID) {
		return fail()
	}
	for _, n := range []string{receipt.CompletedBatches, receipt.ProcessedFiles, receipt.ProcessedRows} {
		if !receiptDecimal.MatchString(n) {
			return fail()
		}
	}
	if receipt.LastBatchID != nil && !receiptDecimal.MatchString(*receipt.LastBatchID) {
		return fail()
	}
	if receipt.TableExists != (receipt.Table != nil) {
		return fail()
	}
	if table := receipt.Table; table != nil {
		if !receiptUUID.MatchString(table.ID) || !receiptDecimal.MatchString(table.Version) || table.SchemaSHA256 != c.SchemaSHA256 || table.MinReaderVersion < 1 || table.MinReaderVersion > 3 || table.MinWriterVersion < 1 || table.MinWriterVersion > 7 || table.ReaderFeatures == nil || table.WriterFeatures == nil {
			return fail()
		}
		for _, features := range [][]string{table.ReaderFeatures, table.WriterFeatures} {
			seen := map[string]bool{}
			if len(features) > 16 {
				return fail()
			}
			for i, feature := range features {
				if !receiptFeature.MatchString(feature) || seen[feature] || i > 0 && features[i-1] >= feature {
					return fail()
				}
				seen[feature] = true
			}
		}
	}
	hash := sha256.Sum256([]byte(t.Message))
	return IngestionOutcome{RunID: r.ID, WorkspaceID: r.WorkspaceID, IngestionOutcomeSummary: IngestionOutcomeSummary{State: "verified"}, RecordedAt: &now, Receipt: &receipt, Evidence: &IngestionEvidence{ApplicationUID: t.ApplicationUID, DriverUID: t.DriverUID, DriverName: t.DriverName, SourceSHA256: r.SourceSHA256(), ReceiptSHA256: hex.EncodeToString(hash[:])}}, nil
}
func validOutcomeUpdate(r Run, o IngestionOutcome) bool {
	if r.Ingestion == nil || !r.Terminal() || o.RunID != r.ID || o.WorkspaceID != r.WorkspaceID || o.RecordedAt == nil {
		return false
	}
	if r.IngestionOutcome == nil || r.IngestionOutcome.State != "pending" {
		return false
	}
	if o.State == "unavailable" {
		return o.Receipt == nil && o.Evidence == nil && len(o.ReasonCode) > 0 && len(o.ReasonCode) <= 80
	}
	return o.State == "verified" && r.State == "succeeded" && o.Receipt != nil && o.Evidence != nil && o.Evidence.ApplicationUID == r.KubernetesUID
}
func cloneOutcome(o IngestionOutcome) IngestionOutcome {
	b, _ := json.Marshal(o)
	var copy IngestionOutcome
	_ = json.Unmarshal(b, &copy)
	return copy
}
func sameExecutionProfile(a, b ExecutionProfile) bool { return reflect.DeepEqual(a, b) }

func captureIngestionOutcome(ctx context.Context, backend ExecutionBackend, r Run, now time.Time) (IngestionOutcome, bool) {
	unavailable := func(reason string) (IngestionOutcome, bool) {
		return IngestionOutcome{RunID: r.ID, WorkspaceID: r.WorkspaceID, IngestionOutcomeSummary: IngestionOutcomeSummary{State: "unavailable", ReasonCode: reason}, RecordedAt: &now}, false
	}
	if r.State != "succeeded" {
		return unavailable("run_not_succeeded")
	}
	if r.Source == nil || r.Source.IngestionContract == nil {
		return unavailable("legacy_runtime_contract")
	}
	capture, ok := backend.(IngestionOutcomeBackend)
	if !ok {
		return unavailable("capture_unsupported")
	}
	t, e := capture.CaptureIngestionOutcome(ctx, r)
	if e != nil || !t.Final {
		if r.OutcomeCaptureStartedAt != nil && now.Sub(*r.OutcomeCaptureStartedAt) >= 5*time.Minute {
			return unavailable("capture_deadline_exceeded")
		}
		return IngestionOutcome{}, true
	}
	if t.Message == "" {
		return unavailable("receipt_missing")
	}
	outcome, e := verifyIngestionReceipt(r, t, now)
	if e != nil {
		return unavailable("receipt_invalid")
	}
	return outcome, false
}

func exactReceiptObject(data json.RawMessage, keys []string) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil || len(fields) != len(keys) {
		return false
	}
	for _, key := range keys {
		value, ok := fields[key]
		if !ok || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return false
		}
	}
	return true
}
