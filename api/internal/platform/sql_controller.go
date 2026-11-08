package platform

import (
	"bytes"
	"context"
	"crypto/sha256"
	"dataslate.local/api/internal/trino"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"
)

var sqlScalarType = regexp.MustCompile(`^(boolean|tinyint|smallint|integer|bigint|real|double|decimal\([0-9]+,\s*[0-9]+\)|varchar(\([0-9]+\))?|char\([0-9]+\)|date|time(\([0-9]+\))?( with time zone)?|timestamp(\([0-9]+\))?( with time zone)?|varbinary|uuid|ipaddress)$`)
var sqlNumber = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)

func scalarCell(raw json.RawMessage) (*string, error) {
	if bytes.Equal(raw, []byte("null")) {
		return nil, nil
	}
	var value string
	if len(raw) > 0 && raw[0] == '"' {
		if json.Unmarshal(raw, &value) != nil {
			return nil, errors.New("invalid scalar string")
		}
	} else if bytes.Equal(raw, []byte("true")) || bytes.Equal(raw, []byte("false")) || sqlNumber.Match(raw) {
		value = string(raw)
	} else {
		return nil, errors.New("unsupported complex result cell")
	}
	return &value, nil
}
func appendQueryPage(result QueryResult, page trino.Page) (QueryResult, error) {
	result = cloneQueryResult(result)
	if len(page.Columns) > 0 {
		columns := make([]QueryColumn, len(page.Columns))
		for i, c := range page.Columns {
			if len(c.Type) > 256 || !sqlScalarType.MatchString(strings.ToLower(c.Type)) {
				return result, errors.New("unsupported_result_type")
			}
			columns[i] = QueryColumn{Name: c.Name, Type: c.Type}
		}
		if len(result.Columns) > 0 && !reflect.DeepEqual(columns, result.Columns) {
			return result, errors.New("result_schema_changed")
		}
		if len(result.Columns) == 0 {
			encoded, _ := json.Marshal(columns)
			if len(encoded)+2 > MaxQueryBytes {
				result.Truncated = true
				result.Reason = "byte_limit"
				return result, nil
			}
			result.Bytes = len(encoded) + 2
		}
		result.Columns = columns
	}
	if len(page.Rows) > 0 && len(result.Columns) == 0 {
		return result, errors.New("result_columns_missing")
	}
	for _, raw := range page.Rows {
		if len(raw) != len(result.Columns) {
			return result, errors.New("result_row_width")
		}
		if len(result.Rows) >= MaxQueryRows {
			result.Truncated = true
			result.Reason = "row_limit"
			break
		}
		row := make([]*string, len(raw))
		for i, cell := range raw {
			v, err := scalarCell(cell)
			if err != nil {
				return result, err
			}
			row[i] = v
		}
		encoded, _ := json.Marshal(row)
		size := len(encoded)
		if len(result.Rows) > 0 {
			size++
		}
		if result.Bytes+size > MaxQueryBytes {
			result.Truncated = true
			result.Reason = "byte_limit"
			break
		}
		result.Rows = append(result.Rows, row)
		result.RowCount = len(result.Rows)
		result.Bytes += size
	}
	result.RowCount = len(result.Rows)
	return result, nil
}

// FileSQLBackend authorizes each reconciliation and again immediately before
// POST. Persisted private endpoints remain usable for cancellation after revoke.
type FileSQLBackend struct {
	mu           sync.Mutex
	clients      map[trino.Config]*trino.Client
	BindingsFile string
	Bindings     []SQLBinding
}

func (b *FileSQLBackend) current() ([]SQLBinding, error) {
	if b.BindingsFile != "" {
		return LoadSQLBindings(b.BindingsFile)
	}
	return b.Bindings, nil
}
func (b *FileSQLBackend) Authorize(ctx context.Context, q Query, policy SQLPolicy) (bool, error) {
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	bindings, err := b.current()
	if err != nil {
		return false, err
	}
	for _, binding := range bindings {
		if binding.ID == q.BindingID && binding.WorkspaceID == q.WorkspaceID {
			if err = binding.Validate(); err != nil {
				return false, err
			}
			current, allowed := binding.Policy(q.SubmittedBy)
			return allowed && current == policy, nil
		}
	}
	return false, nil
}
func (b *FileSQLBackend) client(policy SQLPolicy) (*trino.Client, error) {
	config := trino.Config{Endpoint: policy.Endpoint, CAFile: policy.CAFile, TokenFile: policy.TokenFile}
	b.mu.Lock()
	defer b.mu.Unlock()
	if c := b.clients[config]; c != nil {
		return c, nil
	}
	c, err := trino.New(config)
	if err != nil {
		return nil, err
	}
	if b.clients == nil {
		b.clients = map[trino.Config]*trino.Client{}
	}
	b.clients[config] = c
	return c, nil
}
func (b *FileSQLBackend) Submit(ctx context.Context, w QueryWork) (trino.Page, error) {
	allowed, err := b.Authorize(ctx, w.Query, w.Policy)
	if err != nil || !allowed {
		return trino.Page{}, errors.New("SQL grant is unavailable or revoked")
	}
	if w.Query.CancelRequested || w.Query.Terminal() || !time.Now().Before(w.Query.Deadline()) {
		return trino.Page{}, errors.New("SQL query no longer permits submission")
	}
	c, err := b.client(w.Policy)
	if err != nil {
		return trino.Page{}, err
	}
	return c.Submit(ctx, trino.Statement{SQL: w.Source.Source, User: w.Policy.EngineUser, Catalog: w.Policy.Catalog, Schema: w.Policy.Schema, TraceToken: w.Query.ID, MaxRunSeconds: w.Policy.MaxRunSeconds})
}
func (b *FileSQLBackend) Next(ctx context.Context, w QueryWork) (trino.Page, error) {
	c, err := b.client(w.Policy)
	if err != nil {
		return trino.Page{}, err
	}
	return c.Next(ctx, w.Cursor, w.Policy.EngineUser)
}
func (b *FileSQLBackend) Cancel(ctx context.Context, w QueryWork) error {
	c, err := b.client(w.Policy)
	if err != nil {
		return err
	}
	return c.Cancel(ctx, w.Cursor, w.Policy.EngineUser)
}

// SQLStatusBackend can verify terminal state when a statement cursor retires.
type SQLStatusBackend interface {
	ObserveQuery(context.Context, QueryWork) (trino.QueryStatus, error)
}

func (b *FileSQLBackend) ObserveQuery(ctx context.Context, w QueryWork) (trino.QueryStatus, error) {
	c, err := b.client(w.Policy)
	if err != nil {
		return trino.QueryStatus{}, err
	}
	return c.Observe(ctx, w.Query.EngineQueryID, w.Policy.EngineUser)
}

// ReconcileQueryOnce makes one durable, leased statement/page transition. A
// pending cursor is retried verbatim after a transport error, never a new POST.
func ReconcileQueryOnce(ctx context.Context, store QueryStore, backend SQLBackend, owner string) (bool, error) {
	work, lease, err := store.AcquireQuery(ctx, owner, 30*time.Second)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	op, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	checkpoint := func(u QueryUpdate) error {
		q, e := store.AdvanceQuery(ctx, lease, u)
		if e == nil {
			work.Query = q
			if u.Processed != nil {
				work.Processed = u.Processed
			}
			if u.Cursor != nil {
				work.Cursor = *u.Cursor
			}
			if u.Result != nil {
				work.Result = *u.Result
			}
		}
		return e
	}
	unknown := func(message string) error {
		state := "unknown"
		if work.Query.CancelRequested {
			state = "cancel_requested"
		}
		return checkpoint(QueryUpdate{State: state, Message: message, RetryAfter: 5 * time.Second})
	}
	observeTerminal := func() (bool, error) {
		observer, ok := backend.(SQLStatusBackend)
		if !ok || work.Query.EngineQueryID == "" {
			return false, nil
		}
		status, e := observer.ObserveQuery(op, work)
		if e != nil || status.ID != work.Query.EngineQueryID {
			return false, nil
		}
		result := cloneQueryResult(work.Result)
		result.Complete = false
		state, message := "", ""
		switch status.State {
		case "FINISHED":
			state = "succeeded"
			message = "Engine completion confirmed, but complete result pages are unavailable."
			result.Truncated = true
			result.Reason = "result_unavailable"
		case "FAILED":
			state = "failed"
			message = "Engine failure confirmed."
			if work.Query.CancelRequested && status.ErrorName == "USER_CANCELED" {
				state = "cancelled"
				message = "Engine cancellation confirmed."
			}
		default:
			return false, nil
		}
		return true, checkpoint(QueryUpdate{State: state, Message: message, Result: &result})
	}
	if !work.Query.CancelRequested && !time.Now().Before(work.Query.Deadline()) {
		if err = checkpoint(QueryUpdate{State: "cancel_requested", CancelRequested: true, Message: "Query lifetime elapsed; engine cancellation requested.", RetainLease: true}); err != nil {
			return true, err
		}
	}
	if !work.Query.CancelRequested {
		allowed, e := backend.Authorize(op, work.Query, work.Policy)
		if e != nil {
			return true, unknown("Current SQL policy is unavailable; execution is paused.")
		}
		if !allowed {
			if err = checkpoint(QueryUpdate{State: "cancel_requested", CancelRequested: true, Message: "SQL grant or profile changed; engine cancellation requested.", RetainLease: true}); err != nil {
				return true, err
			}
		}
	}
	var page trino.Page
	processed := append([]string{}, work.Processed...)
	if !work.Query.SubmissionStarted {
		if work.Query.CancelRequested {
			return true, checkpoint(QueryUpdate{State: "cancelled", Message: "Cancelled before any engine submission."})
		}
		sum := sha256.Sum256([]byte(work.Source.Source))
		if work.Source.QueryID != work.Query.ID || work.Source.WorkspaceID != work.Query.WorkspaceID || work.Source.Notebook != work.Query.Notebook || hex.EncodeToString(sum[:]) != work.Query.Notebook.SHA256 || len(work.Source.Source) != work.Query.Notebook.Bytes {
			return true, unknown("Saved SQL source integrity could not be verified.")
		}
		if op.Err() != nil {
			return true, unknown("Operation deadline elapsed before submission.")
		}
		if err = checkpoint(QueryUpdate{State: "submitting", SubmissionStarted: true, Message: "Submitting the saved SQL snapshot.", RetainLease: true}); err != nil {
			return true, err
		}
		if work.Query.CancelRequested {
			return true, checkpoint(QueryUpdate{State: "cancelled", Message: "Cancellation accepted before engine submission."})
		}
		page, err = backend.Submit(op, work)
		if err != nil {
			return true, unknown("Engine submission outcome is unknown; no automatic resubmission will occur.")
		}
	} else {
		if work.Cursor.NextURI == "" {
			if done, e := observeTerminal(); done {
				return true, e
			}
			return true, unknown("No confirmed engine cursor is available. Manual reconciliation is required; resubmission is blocked.")
		}
		if work.Query.CancelRequested && !work.Query.CancelAccepted {
			if err = backend.Cancel(op, work); err != nil {
				return true, unknown("Engine cancellation acceptance is unconfirmed.")
			}
			if err = checkpoint(QueryUpdate{State: "cancel_requested", CancelAccepted: true, Message: "Engine accepted cancellation; terminal confirmation is pending.", RetainLease: true}); err != nil {
				return true, err
			}
		}
		page, err = backend.Next(op, work)
		if err != nil {
			if done, e := observeTerminal(); done {
				return true, e
			}
			return true, unknown("Engine page or terminal outcome is unconfirmed; the same cursor will be retried.")
		}
		digest := func(uri string) string { sum := sha256.Sum256([]byte(uri)); return hex.EncodeToString(sum[:]) }
		cyclic := page.NextURI == work.Cursor.NextURI && len(page.Rows) > 0
		if page.NextURI != "" && page.NextURI != work.Cursor.NextURI {
			for _, seen := range processed {
				if seen == digest(page.NextURI) {
					cyclic = true
				}
			}
		}
		if cyclic || len(processed) >= 4096 {
			return true, checkpoint(QueryUpdate{State: "cancel_requested", CancelRequested: true, Message: "Engine cursor repeated or exceeded the page bound; rows were not appended and cancellation is pending.", RetryAfter: time.Second})
		}
		if page.NextURI != work.Cursor.NextURI {
			processed = append(processed, digest(work.Cursor.NextURI))
		}
	}
	if page.ID == "" || work.Query.EngineQueryID != "" && page.ID != work.Query.EngineQueryID {
		return true, unknown("Engine query identity could not be verified.")
	}
	result := cloneQueryResult(work.Result)
	state, message := "running", "Reading governed SQL results."
	if page.Error != nil {
		if page.NextURI != "" {
			return true, unknown("Engine error did not include confirmed terminal state.")
		}
		state = "failed"
		message = "Engine rejected or failed the query. No successful result is claimed."
		if work.Query.CancelRequested && page.Error.Name == "USER_CANCELED" {
			state = "cancelled"
			message = "Engine cancellation confirmed."
		}
		result.Complete = false
	} else {
		if !work.Query.CancelRequested && !result.Truncated {
			var e error
			result, e = appendQueryPage(result, page)
			if e != nil {
				result.Truncated = true
				result.Reason = "unsupported_result"
				message = "Result contains unsupported or inconsistent values; retained preview is incomplete."
			}
		}
		if page.NextURI == "" {
			state = "succeeded"
			message = "Engine query completed."
			result.Complete = !result.Truncated && !work.Query.CancelRequested
		} else if result.Truncated {
			state = "cancel_requested"
			message = "Result preview limit or supported-type boundary reached; remaining engine query cancellation requested."
		} else if work.Query.CancelRequested {
			state = "cancel_requested"
			message = "Waiting for engine terminal confirmation."
		}
	}
	return true, checkpoint(QueryUpdate{State: state, EngineQueryID: page.ID, Message: message, Cursor: &page.Cursor, Processed: processed, Result: &result, CancelRequested: state == "cancel_requested", RetryAfter: time.Second})
}
