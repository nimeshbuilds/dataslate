package platform

import (
	"bytes"
	"context"
	"dataslate.local/api/internal/airflow"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

var ErrWorkflowAccess = errors.New("workflow grant changed or was revoked")
var workflowDAG = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,199}$`)
var workflowBundle = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:/@+-]{0,255}$`)

type WorkflowBindingSummary struct {
	ID             string `json:"id"`
	WorkspaceID    string `json:"workspaceId"`
	Name           string `json:"name"`
	PolicyVersion  string `json:"policyVersion"`
	DAGID          string `json:"dagId"`
	DAGVersion     int    `json:"dagVersion"`
	BundleVersion  string `json:"bundleVersion"`
	AirflowVersion string `json:"airflowVersion"`
	MaxRunSeconds  int    `json:"maxRunSeconds"`
}
type WorkflowBinding struct {
	WorkflowBindingSummary
	Endpoint               string             `json:"endpoint"`
	CAFile                 string             `json:"caFile,omitempty"`
	TokenFile              string             `json:"tokenFile"`
	MetadataAccessAccepted bool               `json:"metadataAccessAccepted"`
	AllowedSubjects        []ExecutionSubject `json:"allowedSubjects"`
}

func (b WorkflowBinding) Validate() error {
	u, e := url.Parse(b.Endpoint)
	if !identifier.MatchString(b.ID) || !identifier.MatchString(b.WorkspaceID) || strings.TrimSpace(b.Name) == "" || len(b.Name) > 120 || !identifier.MatchString(b.PolicyVersion) || !workflowDAG.MatchString(b.DAGID) || b.DAGVersion < 1 || b.DAGVersion > 1000000 || !workflowBundle.MatchString(b.BundleVersion) || b.AirflowVersion != "3.4.0" || b.MaxRunSeconds < 60 || b.MaxRunSeconds > 86400 || e != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" || (u.Path != "" && u.Path != "/") || !b.MetadataAccessAccepted || b.AllowedSubjects == nil || len(b.AllowedSubjects) > 1000 || !safeWorkflowPath(b.TokenFile) || (b.CAFile != "" && !safeWorkflowPath(b.CAFile)) {
		return errors.New("workflow binding requires exact Airflow 3.4.0 DAG/bundle, HTTPS credentials, explicit metadata grants and bounded lifetime")
	}
	seen := map[ExecutionSubject]bool{}
	for _, s := range b.AllowedSubjects {
		if !validPublicationSubject(s) || seen[s] {
			return errors.New("invalid or duplicate workflow grant")
		}
		seen[s] = true
	}
	return nil
}
func safeWorkflowPath(path string) bool {
	return filepath.IsAbs(path) && filepath.Clean(path) == path && len(path) <= 4096 && !strings.ContainsAny(path, "\x00\r\n")
}
func (b WorkflowBinding) Allows(s ExecutionSubject) bool {
	for _, p := range b.AllowedSubjects {
		if p == s {
			return true
		}
	}
	return false
}
func (b WorkflowBinding) Hash() string { return digestJSON(b) }
func (b WorkflowBinding) Target() airflow.Target {
	return airflow.Target{DAGID: b.DAGID, DAGVersion: b.DAGVersion, BundleVersion: b.BundleVersion}
}
func LoadWorkflowBindings(path string) ([]WorkflowBinding, error) {
	if path == "" {
		return nil, nil
	}
	f, e := os.Open(path)
	if e != nil {
		return nil, errors.New("workflow binding file unavailable")
	}
	defer f.Close()
	data, e := io.ReadAll(io.LimitReader(f, 1024*1024+1))
	if e != nil || len(data) > 1024*1024 || validateUniqueJSON(data) != nil {
		return nil, errors.New("workflow binding file exceeds limit")
	}
	var out []WorkflowBinding
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if d.Decode(&out) != nil || out == nil || len(out) > 1000 || d.Decode(&struct{}{}) != io.EOF {
		return nil, errors.New("workflow bindings require one bounded JSON array")
	}
	seen := map[string]bool{}
	for _, b := range out {
		if e = b.Validate(); e != nil {
			return nil, e
		}
		key := b.WorkspaceID + "/" + b.ID
		if seen[key] {
			return nil, errors.New("duplicate workflow binding")
		}
		seen[key] = true
	}
	return out, nil
}

type WorkflowInput struct {
	BindingID      string `json:"bindingId"`
	IdempotencyKey string `json:"idempotencyKey"`
	PolicyVersion  string `json:"policyVersion"`
	DAGID          string `json:"dagId"`
	DAGVersion     int    `json:"dagVersion"`
	BundleVersion  string `json:"bundleVersion"`
}

func (in WorkflowInput) Validate() error {
	if !identifier.MatchString(in.BindingID) || !idempotencyKeyPattern.MatchString(in.IdempotencyKey) || !identifier.MatchString(in.PolicyVersion) || !workflowDAG.MatchString(in.DAGID) || in.DAGVersion < 1 || in.DAGVersion > 1000000 || !workflowBundle.MatchString(in.BundleVersion) {
		return errors.New("workflow requires a bindingId, 8–128 character idempotencyKey and exact reviewed policyVersion, dagId, dagVersion and bundleVersion")
	}
	return nil
}

type WorkflowDefinition struct {
	BindingID       string         `json:"bindingId"`
	WorkspaceID     string         `json:"workspaceId"`
	PolicyVersion   string         `json:"policyVersion"`
	DAGID           string         `json:"dagId"`
	DAGVersion      int            `json:"dagVersion"`
	BundleVersion   string         `json:"bundleVersion"`
	ObservedAt      time.Time      `json:"observedAt"`
	Paused          bool           `json:"paused"`
	Stale           bool           `json:"stale"`
	HasImportErrors bool           `json:"hasImportErrors"`
	ScheduleSummary string         `json:"scheduleSummary"`
	Timezone        string         `json:"timezone"`
	NextRunAt       *time.Time     `json:"nextRunAt,omitempty"`
	VersionMatches  bool           `json:"versionMatches"`
	CanTrigger      bool           `json:"canTrigger"`
	ReasonCode      string         `json:"reasonCode"`
	Tasks           []airflow.Task `json:"tasks"`
}

func workflowDefinition(b WorkflowBinding, d airflow.Definition) WorkflowDefinition {
	out := WorkflowDefinition{BindingID: b.ID, WorkspaceID: b.WorkspaceID, PolicyVersion: b.PolicyVersion, DAGID: b.DAGID, DAGVersion: b.DAGVersion, BundleVersion: b.BundleVersion, ObservedAt: time.Now().UTC(), Paused: d.Paused, Stale: d.Stale, HasImportErrors: d.HasImportErrors, ScheduleSummary: d.ScheduleSummary, Timezone: d.Timezone, NextRunAt: d.NextRunAt, VersionMatches: d.Matches(b.Target()), ReasonCode: "ready", Tasks: d.Tasks}
	switch {
	case !out.VersionMatches:
		out.ReasonCode = "version_mismatch"
	case out.Stale:
		out.ReasonCode = "stale"
	case out.HasImportErrors:
		out.ReasonCode = "import_errors"
	case out.Paused:
		out.ReasonCode = "paused"
	default:
		out.CanTrigger = true
	}
	return out
}

type WorkflowHistoryRun struct {
	DAGID         string     `json:"dagId"`
	BackendRunID  string     `json:"backendRunId"`
	State         string     `json:"state"`
	RunType       string     `json:"runType"`
	DAGVersion    int        `json:"dagVersion"`
	BundleVersion string     `json:"bundleVersion"`
	LogicalDate   *time.Time `json:"logicalDate,omitempty"`
	StartedAt     *time.Time `json:"startedAt,omitempty"`
	EndedAt       *time.Time `json:"endedAt,omitempty"`
}
type WorkflowRun struct {
	Resource
	WorkspaceID            string                 `json:"workspaceId"`
	BindingID              string                 `json:"bindingId"`
	PolicyVersion          string                 `json:"policyVersion"`
	DAGID                  string                 `json:"dagId"`
	DAGVersion             int                    `json:"dagVersion"`
	BundleVersion          string                 `json:"bundleVersion"`
	BackendRunID           string                 `json:"backendRunId"`
	SubmittedBy            ExecutionSubject       `json:"submittedBy"`
	State                  string                 `json:"state"`
	DispatchStarted        bool                   `json:"dispatchStarted"`
	ReconciliationComplete bool                   `json:"reconciliationComplete"`
	Message                string                 `json:"message"`
	ObservedAt             *time.Time             `json:"observedAt,omitempty"`
	StartedAt              *time.Time             `json:"startedAt,omitempty"`
	EndedAt                *time.Time             `json:"endedAt,omitempty"`
	Tasks                  []airflow.TaskInstance `json:"tasks"`
	TasksTruncated         bool                   `json:"tasksTruncated"`
}
type WorkflowWork struct {
	Run        WorkflowRun
	Binding    WorkflowBinding
	PolicyHash string
}

func newWorkflow(b WorkflowBinding, s ExecutionSubject) WorkflowWork {
	r := WorkflowRun{Resource: newResource("wf-"), WorkspaceID: b.WorkspaceID, BindingID: b.ID, PolicyVersion: b.PolicyVersion, DAGID: b.DAGID, DAGVersion: b.DAGVersion, BundleVersion: b.BundleVersion, SubmittedBy: s, State: "queued", Message: "Waiting for workflow controller admission.", Tasks: []airflow.TaskInstance{}}
	r.BackendRunID = "dataslate-" + r.ID
	return WorkflowWork{r, b, b.Hash()}
}
func (w WorkflowWork) Receipt() airflow.Receipt {
	return airflow.Receipt{RunID: w.Run.ID, WorkspaceID: w.Run.WorkspaceID, BindingID: w.Run.BindingID, PolicyVersion: w.Run.PolicyVersion}
}
func cloneWorkflow(w WorkflowWork) WorkflowWork {
	data, _ := json.Marshal(w)
	var out WorkflowWork
	_ = json.Unmarshal(data, &out)
	return out
}

type WorkflowLease struct {
	ID, Owner string
	Fence     int64
}
type WorkflowUpdate struct {
	State, Message                      string
	DispatchStarted, Final, RetainLease bool
	Observation                         *airflow.Run
	Tasks                               []airflow.TaskInstance
	TasksTruncated                      bool
	RetryAfter                          time.Duration
}
type WorkflowStore interface {
	CreateWorkflow(context.Context, WorkflowWork, string, string) (WorkflowWork, bool, error)
	Workflows(context.Context, string, ExecutionSubject) ([]WorkflowWork, error)
	Workflow(context.Context, string, string, ExecutionSubject) (WorkflowWork, error)
	AcquireWorkflow(context.Context, string, time.Duration) (WorkflowWork, WorkflowLease, error)
	AdvanceWorkflow(context.Context, WorkflowLease, WorkflowUpdate) (WorkflowWork, error)
}

func updateWorkflow(w WorkflowWork, u WorkflowUpdate, now time.Time) (WorkflowWork, error) {
	r := w.Run
	if r.ReconciliationComplete || u.RetryAfter < 0 || u.RetryAfter > time.Minute {
		return WorkflowWork{}, ErrConflict
	}
	switch u.State {
	case "checking", "submitting", "running", "succeeded", "failed", "unknown":
	default:
		return WorkflowWork{}, ErrConflict
	}
	deadline := r.CreatedAt.Add(time.Duration(w.Binding.MaxRunSeconds) * time.Second)
	if u.DispatchStarted {
		if r.DispatchStarted || u.State != "submitting" || !u.RetainLease || !now.Before(deadline) {
			return WorkflowWork{}, ErrConflict
		}
		r.DispatchStarted = true
	}
	if u.State == "succeeded" && (u.Observation == nil || u.Observation.State != "success") {
		return WorkflowWork{}, ErrConflict
	}
	if u.State == "failed" && r.DispatchStarted && (u.Observation == nil || u.Observation.State != "failed") {
		return WorkflowWork{}, ErrConflict
	}
	if u.Observation != nil {
		if !now.Before(deadline) {
			return WorkflowWork{}, ErrConflict
		}
		if !r.DispatchStarted || u.Observation.RunID != r.BackendRunID || !u.Observation.Matches(w.Binding.Target(), w.Receipt()) {
			return WorkflowWork{}, ErrConflict
		}
		r.ObservedAt = &now
		r.StartedAt = u.Observation.StartedAt
		r.EndedAt = u.Observation.EndedAt
		r.Tasks = append([]airflow.TaskInstance{}, u.Tasks...)
		r.TasksTruncated = u.TasksTruncated
	}
	if u.Final && u.State != "succeeded" && u.State != "failed" && u.State != "unknown" {
		return WorkflowWork{}, ErrConflict
	}
	if (u.State == "succeeded" || u.State == "failed") && !u.Final {
		return WorkflowWork{}, ErrConflict
	}
	r.State = u.State
	r.Message = u.Message
	r.ReconciliationComplete = u.Final
	r.Revision++
	r.UpdatedAt = now
	w.Run = r
	return w, nil
}
