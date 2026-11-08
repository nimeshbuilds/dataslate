package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"dataslate.local/api/internal/trino"
)

var ErrPublicationDeadline = errors.New("publication deadline elapsed")

var ErrPublicationBusy = errors.New("publication target has unresolved work")
var ErrPublicationTarget = errors.New("publication target conflicts with permanent ownership")
var ErrPublicationSchema = errors.New("publication schema is not supported by the qualified SQL reader")
var ErrPublicationAccess = errors.New("publication permission changed or was revoked")

type PublicationReader struct {
	ExecutionSubject
	SQLBindingID string `json:"sqlBindingId"`
}
type PublicationBindingSummary struct {
	ID                 string `json:"id"`
	WorkspaceID        string `json:"workspaceId"`
	Name               string `json:"name"`
	PolicyVersion      string `json:"policyVersion"`
	PipelineID         string `json:"pipelineId"`
	ExecutionBindingID string `json:"executionBindingId"`
	Catalog            string `json:"catalog"`
	Schema             string `json:"schema"`
	Table              string `json:"table"`
	MaxRunSeconds      int    `json:"maxRunSeconds"`
}
type PublicationBinding struct {
	PublicationBindingSummary
	DestinationURI             string              `json:"destinationUri"`
	Endpoint                   string              `json:"endpoint"`
	CAFile                     string              `json:"caFile,omitempty"`
	TokenFile                  string              `json:"tokenFile"`
	EngineUser                 string              `json:"engineUser"`
	RegistrationPolicyAccepted bool                `json:"registrationPolicyAccepted"`
	AllowedPublishers          []ExecutionSubject  `json:"allowedPublishers"`
	Readers                    []PublicationReader `json:"readers"`
}

func (b PublicationBinding) Allows(owner ExecutionSubject) bool {
	for _, s := range b.AllowedPublishers {
		if s == owner {
			return true
		}
	}
	return false
}
func (b PublicationBinding) Hash() string { return digestJSON(b) }
func (b PublicationBinding) targetKey() string {
	return digestJSON([]string{canonicalPublicationEndpoint(b.Endpoint), strings.ToLower(b.Catalog), strings.ToLower(b.Schema), strings.ToLower(b.Table)})
}
func (b PublicationBinding) mappingKey() string {
	return digestJSON([]string{b.WorkspaceID, b.ID, b.PipelineID, b.ExecutionBindingID, b.DestinationURI, b.targetKey()})
}
func (b PublicationBinding) Validate() error {
	sb := SQLBinding{ID: b.ID, WorkspaceID: b.WorkspaceID, Name: b.Name, SQLProfile: SQLProfile{SQLContext: SQLContext{PolicyVersion: b.PolicyVersion, Engine: "trino", Catalog: b.Catalog, Schema: b.Schema, MaxRunSeconds: b.MaxRunSeconds}, Endpoint: b.Endpoint, CAFile: b.CAFile, TokenFile: b.TokenFile, ReadOnlyPolicyAccepted: true}, AllowedSubjects: []SQLSubject{}}
	if sb.Validate() != nil || !b.RegistrationPolicyAccepted || !identifier.MatchString(b.PipelineID) || !identifier.MatchString(b.ExecutionBindingID) || !ingestionFieldName.MatchString(b.Catalog) || !ingestionFieldName.MatchString(b.Schema) || !ingestionFieldName.MatchString(b.Table) || !sqlHeaderValue.MatchString(b.EngineUser) || validateS3Prefix(b.DestinationURI) != nil || canonicalPublicationEndpoint(b.Endpoint) == "" {
		return errors.New("publication binding requires an exact approved Trino target, S3 destination, private credentials, pipeline and registration policy acceptance")
	}
	if b.AllowedPublishers == nil || b.Readers == nil || len(b.AllowedPublishers) > 1000 || len(b.Readers) > 1000 {
		return errors.New("publication grants require explicit bounded publisher and reader arrays")
	}
	seen := map[ExecutionSubject]bool{}
	for _, subject := range b.AllowedPublishers {
		if !validPublicationSubject(subject) || seen[subject] {
			return errors.New("invalid or duplicate publication publisher")
		}
		seen[subject] = true
	}
	seen = map[ExecutionSubject]bool{}
	for _, reader := range b.Readers {
		if !validPublicationSubject(reader.ExecutionSubject) || !identifier.MatchString(reader.SQLBindingID) || seen[reader.ExecutionSubject] {
			return errors.New("invalid or duplicate publication reader")
		}
		seen[reader.ExecutionSubject] = true
	}
	return nil
}
func validPublicationSubject(s ExecutionSubject) bool {
	return s.Issuer != "" && s.Subject != "" && len(s.Issuer) <= 2048 && len(s.Subject) <= 512
}
func LoadPublicationBindings(path string) ([]PublicationBinding, error) {
	if path == "" {
		return nil, nil
	}
	f, e := os.Open(path)
	if e != nil {
		return nil, errors.New("publication bindings file unavailable")
	}
	defer f.Close()
	data, e := io.ReadAll(io.LimitReader(f, 1024*1024+1))
	if e != nil || len(data) > 1024*1024 {
		return nil, errors.New("publication bindings exceed limit")
	}
	var out []PublicationBinding
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if d.Decode(&out) != nil || out == nil || d.Decode(&struct{}{}) != io.EOF || len(out) > 1000 {
		return nil, errors.New("publication bindings require one bounded JSON array")
	}
	seen := map[string]bool{}
	targets := map[string]bool{}
	for _, b := range out {
		if e = b.Validate(); e != nil {
			return nil, e
		}
		key := b.WorkspaceID + "/" + b.ID
		if seen[key] || targets[b.targetKey()] {
			return nil, errors.New("duplicate publication identity or engine target")
		}
		seen[key] = true
		targets[b.targetKey()] = true
	}
	return out, nil
}

type PublicationTarget struct {
	Catalog string `json:"catalog"`
	Schema  string `json:"schema"`
	Table   string `json:"table"`
}
type TablePublication struct {
	Resource
	WorkspaceID            string            `json:"workspaceId"`
	BindingID              string            `json:"bindingId"`
	PolicyVersion          string            `json:"policyVersion"`
	RunID                  string            `json:"runId"`
	SubmittedBy            ExecutionSubject  `json:"submittedBy"`
	Pipeline               PipelineSelection `json:"pipeline"`
	Target                 PublicationTarget `json:"target"`
	DeltaVersion           string            `json:"deltaVersion"`
	State                  string            `json:"state"`
	ReconciliationComplete bool              `json:"reconciliationComplete"`
	RegistrationStarted    bool              `json:"registrationStarted"`
	Message                string            `json:"message"`
	PublishedTableID       string            `json:"publishedTableId,omitempty"`
	VerifiedAt             *time.Time        `json:"verifiedAt,omitempty"`
}
type PublicationInput struct {
	RunID          string `json:"runId"`
	BindingID      string `json:"bindingId"`
	IdempotencyKey string `json:"idempotencyKey"`
}

func (in PublicationInput) Validate() error {
	if !identifier.MatchString(in.RunID) || !identifier.MatchString(in.BindingID) || !idempotencyKeyPattern.MatchString(in.IdempotencyKey) {
		return errors.New("valid runId, bindingId and idempotencyKey are required")
	}
	return nil
}

type PublishedTableSQL struct {
	BindingID     string `json:"bindingId"`
	PolicyVersion string `json:"policyVersion"`
	Draft         string `json:"draft"`
}
type PublishedTable struct {
	ID                   string            `json:"id"`
	WorkspaceID          string            `json:"workspaceId"`
	Name                 string            `json:"name"`
	BindingID            string            `json:"bindingId"`
	PolicyVersion        string            `json:"policyVersion"`
	Target               PublicationTarget `json:"target"`
	Format               string            `json:"format"`
	Fields               []IngestionField  `json:"fields"`
	VerifiedDeltaVersion string            `json:"verifiedDeltaVersion"`
	VerifiedAt           time.Time         `json:"verifiedAt"`
	QueryMode            string            `json:"queryMode"`
	SQL                  PublishedTableSQL `json:"sql"`
}
type PublicationWork struct {
	Publication         TablePublication
	Binding             PublicationBinding
	PolicyHash          string
	Run                 Run
	Contract            IngestionSourceContract
	Outcome             IngestionOutcome
	Cursor              trino.Cursor
	Final               bool
	PreviouslyPublished bool
}
type PublishedTableRecord struct {
	Table      PublishedTable
	PolicyHash string
	MappingKey string
}
type PublicationLease struct {
	ID, Owner string
	Fence     int64
}
type PublicationUpdate struct {
	State, Message                          string
	RegistrationStarted, Final, RetainLease bool
	Cursor                                  *trino.Cursor
	Verified                                bool
	RetryAfter                              time.Duration
}
type PublicationStore interface {
	CreatePublication(context.Context, PublicationWork, string, string) (PublicationWork, bool, error)
	Publications(context.Context, string, ExecutionSubject) ([]PublicationWork, error)
	Publication(context.Context, string, string, ExecutionSubject) (PublicationWork, error)
	PublishedTable(context.Context, string, string) (PublishedTableRecord, error)
	AcquirePublication(context.Context, string, time.Duration) (PublicationWork, PublicationLease, error)
	AdvancePublication(context.Context, PublicationLease, PublicationUpdate) (PublicationWork, error)
}

func newPublication(b PublicationBinding, r Run, source RunSource, o IngestionOutcome) (PublicationWork, error) {
	if r.Ingestion == nil || r.State != "succeeded" || !r.WriterReleased || source.IngestionContract == nil || o.State != "verified" || o.Receipt == nil || o.Receipt.Table == nil || !o.Receipt.TableExists {
		return PublicationWork{}, ErrIngestionOutcome
	}
	if r.WorkspaceID != b.WorkspaceID || r.BindingID != b.ExecutionBindingID || r.Ingestion.ID != b.PipelineID || r.Ingestion.DestinationURI != b.DestinationURI || !b.Allows(r.SubmittedBy) {
		return PublicationWork{}, ErrPublicationAccess
	}
	for _, field := range source.IngestionContract.Spec.Source.Schema {
		if field.Type == "timestamp" {
			return PublicationWork{}, ErrPublicationSchema
		}
	}
	p := TablePublication{Resource: newResource("pub-"), WorkspaceID: r.WorkspaceID, BindingID: b.ID, PolicyVersion: b.PolicyVersion, RunID: r.ID, SubmittedBy: r.SubmittedBy, Pipeline: r.Ingestion.PipelineSelection, Target: PublicationTarget{b.Catalog, b.Schema, b.Table}, DeltaVersion: o.Receipt.Table.Version, State: "queued", Message: "Queued for verified Delta publication."}
	r.Source = nil
	return PublicationWork{Publication: p, Binding: b, PolicyHash: b.Hash(), Run: r, Contract: *source.IngestionContract, Outcome: o}, nil
}
func (w PublicationWork) target() trino.PublishTarget {
	t := w.Outcome.Receipt.Table
	fields := make([]trino.DeltaField, 0, len(w.Contract.Spec.Source.Schema))
	for _, f := range w.Contract.Spec.Source.Schema {
		fields = append(fields, trino.DeltaField{Name: f.Name, Type: f.Type})
	}
	return trino.PublishTarget{Catalog: w.Binding.Catalog, Schema: w.Binding.Schema, Table: w.Binding.Table, Location: w.Binding.DestinationURI, Version: t.Version, Fields: fields, MinReaderVersion: t.MinReaderVersion, MinWriterVersion: t.MinWriterVersion, ReaderFeatures: t.ReaderFeatures, WriterFeatures: t.WriterFeatures}
}
func clonePublication(w PublicationWork) PublicationWork {
	data, _ := json.Marshal(w)
	var clone PublicationWork
	_ = json.Unmarshal(data, &clone)
	return clone
}
func publicationTable(w PublicationWork, now time.Time) PublishedTableRecord {
	fields := append([]IngestionField{}, w.Contract.Spec.Source.Schema...)
	for i := range fields {
		fields[i].Nullable = true
	}
	return PublishedTableRecord{PolicyHash: w.PolicyHash, MappingKey: w.Binding.mappingKey(), Table: PublishedTable{ID: w.Binding.ID, WorkspaceID: w.Binding.WorkspaceID, Name: w.Binding.Name, BindingID: w.Binding.ID, PolicyVersion: w.Binding.PolicyVersion, Target: w.Publication.Target, Format: "delta", Fields: fields, VerifiedDeltaVersion: w.Publication.DeltaVersion, VerifiedAt: now, QueryMode: "latest"}}
}
func updatePublication(w PublicationWork, u PublicationUpdate, now time.Time) (PublicationWork, error) {
	if (u.Verified || u.RegistrationStarted) && !now.Before(w.Publication.CreatedAt.Add(time.Duration(w.Binding.MaxRunSeconds)*time.Second)) {
		return PublicationWork{}, ErrPublicationDeadline
	}
	if w.Final {
		return PublicationWork{}, ErrConflict
	}
	switch u.State {
	case "queued", "checking", "registering", "verifying", "published", "failed", "unknown":
	default:
		return PublicationWork{}, ErrConflict
	}
	if len(u.Message) > 2048 {
		return PublicationWork{}, ErrConflict
	}
	if (u.State == "published" || u.State == "failed") && !u.Final {
		return PublicationWork{}, ErrConflict
	}
	if u.Final && u.State != "published" && u.State != "failed" && u.State != "unknown" {
		return PublicationWork{}, ErrConflict
	}
	p := &w.Publication
	p.State = u.State
	p.Message = u.Message
	p.RegistrationStarted = p.RegistrationStarted || u.RegistrationStarted
	p.UpdatedAt = now
	p.Revision++
	w.Final = u.Final
	p.ReconciliationComplete = u.Final
	if u.Cursor != nil {
		if w.Cursor.ID != "" && w.Cursor.ID != u.Cursor.ID {
			return PublicationWork{}, ErrConflict
		}
		w.Cursor = *u.Cursor
	}
	if u.Verified {
		if u.State != "published" || !u.Final {
			return PublicationWork{}, ErrConflict
		}
		p.VerifiedAt = &now
		p.PublishedTableID = w.Binding.ID
	}
	if u.State == "published" && !u.Verified {
		return PublicationWork{}, ErrConflict
	}
	return w, nil
}

func validatePublicationReaders(b PublicationBinding, bindings []SQLBinding) error {
	for _, sql := range bindings {
		if canonicalPublicationEndpoint(sql.Endpoint) == canonicalPublicationEndpoint(b.Endpoint) {
			if sql.TokenFile == b.TokenFile {
				return errors.New("publication credentials must be separate from SQL credentials")
			}
			for _, reader := range sql.AllowedSubjects {
				if reader.EngineUser == b.EngineUser {
					return errors.New("publication engine identity must differ from SQL reader identities")
				}
			}
		}
	}
	for _, reader := range b.Readers {
		found := false
		for _, sql := range bindings {
			if sql.WorkspaceID == b.WorkspaceID && sql.ID == reader.SQLBindingID && canonicalPublicationEndpoint(sql.Endpoint) == canonicalPublicationEndpoint(b.Endpoint) && sql.Catalog == b.Catalog && sql.Schema == b.Schema {
				_, found = sql.Policy(reader.ExecutionSubject)
			}
		}
		if !found {
			return errors.New("publication reader requires a matching approved SQL engine binding and exact identity grant")
		}
	}
	return nil
}

func validatePublicationExecution(b PublicationBinding, bindings []ExecutionBinding) error {
	for _, execution := range bindings {
		if execution.WorkspaceID == b.WorkspaceID && execution.ID == b.ExecutionBindingID && execution.Kind() == FileIngestionBatch && execution.IngestionStorage != nil {
			for _, other := range bindings {
				if other.Namespace == execution.Namespace && other.Kind() != FileIngestionBatch {
					return errors.New("publication ingestion requires a namespace separate from arbitrary application execution")
				}
			}
			allowed := false
			for _, prefix := range execution.IngestionStorage.WritePrefixes {
				if strings.HasPrefix(b.DestinationURI, prefix) {
					allowed = true
				}
			}
			if !allowed {
				return ErrPublicationAccess
			}
			for _, owner := range b.AllowedPublishers {
				if !execution.Allows(owner) {
					return ErrPublicationAccess
				}
			}
			return nil
		}
	}
	return errors.New("publication requires a matching ingestion binding with destination write grant")
}
func ValidatePublicationConfiguration(bindings []PublicationBinding, execution []ExecutionBinding, sql []SQLBinding) error {
	for _, b := range bindings {
		if e := b.Validate(); e != nil {
			return e
		}
		if e := validatePublicationReaders(b, sql); e != nil {
			return e
		}
		if e := validatePublicationExecution(b, execution); e != nil {
			return e
		}
	}
	return nil
}

func canonicalPublicationEndpoint(raw string) string {
	u, e := url.Parse(raw)
	if e != nil {
		return ""
	}
	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	if host == "" || strings.Contains(host, "%") {
		return ""
	}
	for _, r := range host {
		if r > 127 {
			return ""
		}
	}
	if ip := net.ParseIP(host); ip != nil {
		host = ip.String()
	}
	port := u.Port()
	if port != "" {
		n, e := strconv.Atoi(port)
		if e != nil || n < 1 || n > 65535 {
			return ""
		}
		port = strconv.Itoa(n)
	}
	if port != "" && port != "443" {
		host = net.JoinHostPort(host, port)
	} else if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	return "https://" + host
}
