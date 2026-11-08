package platform

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"dataslate.local/api/internal/trino"
)

const MaxQueryRows = 10000
const MaxQueryBytes = 8 * 1024 * 1024
const MaxWorkspaceQueries = 100

var ErrSQLRevision = errors.New("saved SQL revision changed")
var ErrSQLNotExecutable = errors.New("saved cell cannot execute as SQL")
var ErrQueryCapacity = errors.New("workspace query capacity exceeded")
var sqlHeaderValue = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:@-]{0,255}$`)

type SQLContext struct {
	PolicyVersion string `json:"policyVersion"`
	Engine        string `json:"engine"`
	Catalog       string `json:"catalog"`
	Schema        string `json:"schema"`
	MaxRunSeconds int    `json:"maxRunSeconds"`
}
type SQLProfile struct {
	SQLContext
	Endpoint               string `json:"endpoint"`
	CAFile                 string `json:"caFile,omitempty"`
	TokenFile              string `json:"tokenFile"`
	ReadOnlyPolicyAccepted bool   `json:"readOnlyPolicyAccepted"`
}
type SQLSubject struct {
	ExecutionSubject
	EngineUser string `json:"engineUser"`
}
type SQLBinding struct {
	ID          string `json:"id"`
	WorkspaceID string `json:"workspaceId"`
	Name        string `json:"name"`
	SQLProfile
	AllowedSubjects []SQLSubject `json:"allowedSubjects"`
}
type SQLPolicy struct {
	SQLProfile
	EngineUser string `json:"engineUser"`
}

func (b SQLBinding) Policy(subject ExecutionSubject) (SQLPolicy, bool) {
	for _, s := range b.AllowedSubjects {
		if s.ExecutionSubject == subject {
			return SQLPolicy{SQLProfile: b.SQLProfile, EngineUser: s.EngineUser}, true
		}
	}
	return SQLPolicy{}, false
}
func (b SQLBinding) Validate() error {
	u, e := url.Parse(b.Endpoint)
	if !identifier.MatchString(b.ID) || !identifier.MatchString(b.WorkspaceID) || strings.TrimSpace(b.Name) == "" || len(b.Name) > 200 || b.Engine != "trino" || !sqlHeaderValue.MatchString(b.PolicyVersion) || !sqlHeaderValue.MatchString(b.Catalog) || !sqlHeaderValue.MatchString(b.Schema) || b.MaxRunSeconds < 1 || b.MaxRunSeconds > 3600 || !b.ReadOnlyPolicyAccepted {
		return errors.New("SQL binding requires valid identity, Trino catalog/schema, 1–3600 second limit and explicit engine read-only policy acceptance")
	}
	if e != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" || u.ForceQuery || (u.Path != "" && u.Path != "/") || !filepath.IsAbs(b.TokenFile) || b.CAFile != "" && !filepath.IsAbs(b.CAFile) {
		return errors.New("SQL binding requires a root HTTPS endpoint and absolute server-private credential/trust file paths")
	}
	if b.AllowedSubjects == nil || len(b.AllowedSubjects) > 1000 {
		return errors.New("SQL binding requires an explicit allowedSubjects array")
	}
	seen := map[ExecutionSubject]bool{}
	users := map[string]bool{}
	for _, s := range b.AllowedSubjects {
		if s.Issuer == "" || s.Subject == "" || len(s.Issuer) > 2048 || len(s.Subject) > 512 || seen[s.ExecutionSubject] || !sqlHeaderValue.MatchString(s.EngineUser) || users[s.EngineUser] {
			return errors.New("SQL subjects must be unique issuer/subject pairs with distinct explicit engine users")
		}
		seen[s.ExecutionSubject] = true
		users[s.EngineUser] = true
	}
	return nil
}
func LoadSQLBindings(path string) ([]SQLBinding, error) {
	if path == "" {
		return nil, nil
	}
	f, e := os.Open(path)
	if e != nil {
		return nil, errors.New("SQL bindings file is unavailable")
	}
	defer f.Close()
	data, e := io.ReadAll(io.LimitReader(f, 1024*1024+1))
	if e != nil || len(data) > 1024*1024 {
		return nil, errors.New("SQL bindings file exceeds 1 MiB or cannot be read")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var out []SQLBinding
	if decoder.Decode(&out) != nil || out == nil || decoder.Decode(&struct{}{}) != io.EOF || len(out) > 1000 {
		return nil, errors.New("SQL bindings require one bounded JSON array")
	}
	seen := map[string]bool{}
	for _, b := range out {
		if e = b.Validate(); e != nil {
			return nil, e
		}
		key := b.WorkspaceID + "/" + b.ID
		if seen[key] {
			return nil, errors.New("duplicate SQL binding")
		}
		seen[key] = true
	}
	return out, nil
}

type SQLSelection struct {
	ID       string `json:"id"`
	Revision int    `json:"revision"`
	CellID   string `json:"cellId"`
}
type SQLProvenance struct {
	SQLSelection
	SHA256 string `json:"sha256"`
	Bytes  int    `json:"bytes"`
}
type QueryInput struct {
	BindingID      string       `json:"bindingId"`
	IdempotencyKey string       `json:"idempotencyKey"`
	Notebook       SQLSelection `json:"notebook"`
}

func (in QueryInput) Validate() error {
	if !identifier.MatchString(in.BindingID) || !idempotencyKeyPattern.MatchString(in.IdempotencyKey) || !identifier.MatchString(in.Notebook.ID) || !identifier.MatchString(in.Notebook.CellID) || in.Notebook.Revision < 1 {
		return errors.New("SQL query requires a binding, idempotency key and exact saved notebook/cell revision")
	}
	return nil
}
func queryFingerprint(in QueryInput) string {
	data, _ := json.Marshal(struct {
		BindingID string
		Notebook  SQLSelection
	}{in.BindingID, in.Notebook})
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

type QueryResultSummary struct {
	Reason    string `json:"reason,omitempty"`
	RowCount  int    `json:"rowCount"`
	Bytes     int    `json:"bytes"`
	Truncated bool   `json:"truncated"`
	Complete  bool   `json:"complete"`
}
type Query struct {
	Resource
	WorkspaceID       string             `json:"workspaceId"`
	BindingID         string             `json:"bindingId"`
	SubmittedBy       ExecutionSubject   `json:"submittedBy"`
	Context           SQLContext         `json:"context"`
	Notebook          SQLProvenance      `json:"notebook"`
	State             string             `json:"state"`
	Message           string             `json:"message"`
	SubmissionStarted bool               `json:"submissionStarted"`
	CancelRequested   bool               `json:"cancelRequested"`
	CancelAccepted    bool               `json:"cancelAccepted"`
	EngineQueryID     string             `json:"engineQueryId,omitempty"`
	Result            QueryResultSummary `json:"result"`
}
type QueryColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}
type QueryResult struct {
	QueryID     string        `json:"queryId"`
	WorkspaceID string        `json:"workspaceId"`
	Columns     []QueryColumn `json:"columns"`
	Rows        [][]*string   `json:"rows"`
	QueryResultSummary
}
type QuerySource struct {
	QueryID     string        `json:"queryId"`
	WorkspaceID string        `json:"workspaceId"`
	Source      string        `json:"source"`
	Notebook    SQLProvenance `json:"notebook"`
}
type QueryWork struct {
	Processed []string
	Query     Query
	Policy    SQLPolicy
	Cursor    trino.Cursor
	Source    QuerySource
	Result    QueryResult
}
type QueryLease struct {
	QueryID, Owner string
	Fence          int64
}
type QueryUpdate struct {
	Processed                                                       []string
	State, Message, EngineQueryID                                   string
	SubmissionStarted, CancelRequested, CancelAccepted, RetainLease bool
	Cursor                                                          *trino.Cursor
	Result                                                          *QueryResult
	RetryAfter                                                      time.Duration
}

func (q Query) Terminal() bool {
	return q.State == "succeeded" || q.State == "failed" || q.State == "cancelled"
}
func (q Query) Deadline() time.Time {
	return q.CreatedAt.Add(time.Duration(q.Context.MaxRunSeconds) * time.Second)
}
func newQuery(ws string, b SQLBinding, subject ExecutionSubject, selection SQLSelection) Query {
	resource := newResource("qry-")
	return Query{Resource: resource, WorkspaceID: ws, BindingID: b.ID, SubmittedBy: subject, Context: b.SQLContext, Notebook: SQLProvenance{SQLSelection: selection}, State: "queued", Message: "Queued for governed SQL execution.", Result: QueryResultSummary{Bytes: 4}}
}
func snapshotSQL(q Query, n Notebook) (QuerySource, error) {
	if q.WorkspaceID != n.WorkspaceID || q.Notebook.ID != n.ID {
		return QuerySource{}, ErrSQLNotExecutable
	}
	if q.Notebook.Revision != n.Revision {
		return QuerySource{}, ErrSQLRevision
	}
	for _, cell := range n.Cells {
		if cell.ID == q.Notebook.CellID {
			if cell.Language != "sql" || strings.TrimSpace(cell.Source) == "" || len(cell.Source) > trino.MaxSQLBytes || strings.ContainsRune(cell.Source, 0) || !utf8.ValidString(cell.Source) {
				return QuerySource{}, ErrSQLNotExecutable
			}
			hash := sha256.Sum256([]byte(cell.Source))
			p := SQLProvenance{SQLSelection: q.Notebook.SQLSelection, SHA256: hex.EncodeToString(hash[:]), Bytes: len(cell.Source)}
			return QuerySource{QueryID: q.ID, WorkspaceID: q.WorkspaceID, Source: cell.Source, Notebook: p}, nil
		}
	}
	return QuerySource{}, ErrSQLNotExecutable
}
func emptyQueryResult(q Query) QueryResult {
	return QueryResult{QueryID: q.ID, WorkspaceID: q.WorkspaceID, Columns: []QueryColumn{}, Rows: [][]*string{}, QueryResultSummary: QueryResultSummary{Bytes: 4}}
}
func cloneQueryResult(r QueryResult) QueryResult {
	r.Columns = append([]QueryColumn{}, r.Columns...)
	rows := make([][]*string, len(r.Rows))
	for i, row := range r.Rows {
		rows[i] = make([]*string, len(row))
		for j, value := range row {
			if value != nil {
				v := *value
				rows[i][j] = &v
			}
		}
	}
	r.Rows = rows
	return r
}
func applyQueryUpdate(q Query, u QueryUpdate, now time.Time) (Query, error) {
	if !validRunState(u.State) || len(u.Message) > 2048 || len(u.EngineQueryID) > 128 || q.EngineQueryID != "" && u.EngineQueryID != "" && q.EngineQueryID != u.EngineQueryID {
		return Query{}, ErrConflict
	}
	if q.Terminal() && (u.State != q.State || u.SubmissionStarted || u.CancelRequested || u.Cursor != nil || u.Result != nil) {
		return Query{}, ErrConflict
	}
	q.SubmissionStarted = q.SubmissionStarted || u.SubmissionStarted
	q.CancelRequested = q.CancelRequested || u.CancelRequested
	q.CancelAccepted = q.CancelAccepted || u.CancelAccepted
	if u.EngineQueryID != "" {
		q.EngineQueryID = u.EngineQueryID
	}
	if q.CancelRequested && u.State != "succeeded" && u.State != "failed" && u.State != "cancelled" {
		u.State = "cancel_requested"
	}
	q.State = u.State
	q.Message = u.Message
	q.Revision++
	q.UpdatedAt = now
	if u.Result != nil {
		if u.Result.QueryID != q.ID || u.Result.WorkspaceID != q.WorkspaceID || u.Result.RowCount != len(u.Result.Rows) || len(u.Result.Rows) > MaxQueryRows || u.Result.Bytes > MaxQueryBytes || u.Result.Bytes < 4 || u.Result.Complete && (u.State != "succeeded" || u.Result.Truncated) {
			return Query{}, ErrConflict
		}
		columns, _ := json.Marshal(u.Result.Columns)
		rows, _ := json.Marshal(u.Result.Rows)
		if len(columns)+len(rows) != u.Result.Bytes {
			return Query{}, ErrConflict
		}
		for _, row := range u.Result.Rows {
			if len(row) != len(u.Result.Columns) {
				return Query{}, ErrConflict
			}
		}
		q.Result = u.Result.QueryResultSummary
	}
	if u.Cursor != nil && u.Cursor.ID != q.EngineQueryID {
		return Query{}, ErrConflict
	}
	if len(u.Processed) > 4096 {
		return Query{}, ErrConflict
	}
	return q, nil
}

type QueryStore interface {
	CreateQuery(context.Context, Query, SQLPolicy, string, string) (Query, bool, error)
	Queries(context.Context, string, ExecutionSubject) ([]Query, error)
	Query(context.Context, string, string, ExecutionSubject) (Query, error)
	QuerySource(context.Context, string, string, ExecutionSubject) (QuerySource, error)
	QueryResult(context.Context, string, string, ExecutionSubject) (QueryResult, error)
	QueryPolicy(context.Context, string, string, ExecutionSubject) (SQLPolicy, error)
	CancelQuery(context.Context, string, string, ExecutionSubject) (Query, error)
	AcquireQuery(context.Context, string, time.Duration) (QueryWork, QueryLease, error)
	AdvanceQuery(context.Context, QueryLease, QueryUpdate) (Query, error)
}
type SQLBackend interface {
	Authorize(context.Context, Query, SQLPolicy) (bool, error)
	Submit(context.Context, QueryWork) (trino.Page, error)
	Next(context.Context, QueryWork) (trino.Page, error)
	Cancel(context.Context, QueryWork) error
}
