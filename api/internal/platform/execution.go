package platform

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/distribution/reference"
)

// ExecutionSubject is an issuer-qualified identity, never an email/display name.
type ExecutionSubject struct {
	Issuer  string `json:"issuer"`
	Subject string `json:"subject"`
}

const ApprovedApplication = "approved-app"
const NotebookPythonBatch = "notebook-python-batch"
const NotebookApplicationFile = "local:///opt/dataslate/source/source.py"

type ExecutionProfile struct {
	IngestionStorage    *IngestionStorage `json:"ingestionStorage,omitempty"`
	ExecutionKind       string            `json:"executionKind,omitempty"`
	Operator            string            `json:"operator"`
	Image               string            `json:"image"`
	MainApplicationFile string            `json:"mainApplicationFile"`
	Namespace           string            `json:"namespace"`
	ServiceAccount      string            `json:"serviceAccount"`
	ExecutorInstances   int               `json:"executorInstances"`
	MaxRunSeconds       int               `json:"maxRunSeconds,omitempty"`
}

func (p ExecutionProfile) Kind() string {
	if p.ExecutionKind == "" {
		return ApprovedApplication
	}
	return p.ExecutionKind
}
func (p ExecutionProfile) ApplicationFile() string {
	if p.Kind() == NotebookPythonBatch || p.Kind() == FileIngestionBatch {
		return NotebookApplicationFile
	}
	return p.MainApplicationFile
}

// RunLimitSeconds treats omitted/legacy zero as a bounded one-hour lifetime.
func (p ExecutionProfile) RunLimitSeconds() int {
	if p.MaxRunSeconds == 0 {
		return 3600
	}
	return p.MaxRunSeconds
}

type ExecutionBinding struct {
	ID          string `json:"id"`
	WorkspaceID string `json:"workspaceId"`
	Name        string `json:"name"`
	ExecutionProfile
	AllowedSubjects []ExecutionSubject `json:"allowedSubjects"`
}

func (b ExecutionBinding) Allows(p ExecutionSubject) bool {
	for _, allowed := range b.AllowedSubjects {
		if allowed == p {
			return true
		}
	}
	return false
}
func (b ExecutionBinding) Validate() error {
	if b.Kind() != ApprovedApplication && b.Kind() != NotebookPythonBatch && b.Kind() != FileIngestionBatch {
		return errors.New("executionKind must be approved-app, notebook-python-batch or file-ingestion-batch")
	}
	if (b.Kind() == NotebookPythonBatch || b.Kind() == FileIngestionBatch) && b.MainApplicationFile != "" {
		return errors.New("notebook execution uses only its immutable server-generated source; mainApplicationFile must be empty")
	}
	if b.Kind() == FileIngestionBatch {
		if err := b.IngestionStorage.Validate(); err != nil {
			return err
		}
	} else if b.IngestionStorage != nil {
		return errors.New("ingestionStorage is allowed only on ingestion bindings")
	}
	if b.MaxRunSeconds != 0 && (b.MaxRunSeconds < 60 || b.MaxRunSeconds > 86400) {
		return errors.New("maxRunSeconds must be zero (default 3600) or between 60 and 86400")
	}
	if !identifier.MatchString(b.ID) || !identifier.MatchString(b.WorkspaceID) || strings.TrimSpace(b.Name) == "" || len(b.Name) > 200 {
		return errors.New("execution binding requires valid IDs and name")
	}
	if b.Operator != "apache" {
		return errors.New("execution supports the Apache operator adapter only; Kubeflow remains preview-only")
	}
	if _, err := CompileSpark(SparkInput{Name: "binding-check", Operator: b.Operator, Image: b.Image, MainApplicationFile: b.ApplicationFile(), Namespace: b.Namespace, ServiceAccount: b.ServiceAccount, ExecutorInstances: b.ExecutorInstances}, b.WorkspaceID); err != nil {
		return err
	}
	image, err := reference.ParseNormalizedNamed(b.Image)
	if err != nil {
		return err
	}
	digest, ok := image.(reference.Digested)
	if !ok || digest.Digest().Algorithm().String() != "sha256" {
		return errors.New("execution images must be pinned by sha256 digest")
	}
	if !strings.HasPrefix(b.ApplicationFile(), "local:///") {
		return errors.New("execution application must be embedded in the immutable image using local:///; mutable S3 source is not executable")
	}
	if b.AllowedSubjects == nil || len(b.AllowedSubjects) > 1000 {
		return errors.New("binding requires an explicit allowedSubjects array with at most 1000 issuer-qualified subjects; an empty array denies all subjects")
	}
	seen := map[ExecutionSubject]bool{}
	for _, s := range b.AllowedSubjects {
		if s.Issuer == "" || s.Subject == "" || len(s.Issuer) > 2048 || len(s.Subject) > 512 || seen[s] {
			return errors.New("binding subjects must be unique, nonempty issuer/subject pairs")
		}
		seen[s] = true
	}
	return nil
}
func LoadExecutionBindings(path string) ([]ExecutionBinding, error) {
	if path == "" {
		return nil, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.New("cannot read execution bindings file")
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, 1024*1024+1))
	if err != nil || len(data) > 1024*1024 {
		return nil, errors.New("execution bindings file exceeds 1 MiB or could not be read")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	var bindings []ExecutionBinding
	if err = d.Decode(&bindings); err != nil || bindings == nil {
		return nil, errors.New("execution bindings must be a JSON array")
	}
	if d.Decode(&struct{}{}) != io.EOF {
		return nil, errors.New("execution bindings must contain one JSON array")
	}
	if len(bindings) > 1000 {
		return nil, errors.New("too many execution bindings")
	}
	seen := map[string]bool{}
	for _, b := range bindings {
		if err = b.Validate(); err != nil {
			return nil, fmt.Errorf("execution binding %q: %w", b.ID, err)
		}
		key := b.WorkspaceID + "/" + b.ID
		if seen[key] {
			return nil, errors.New("duplicate execution binding identity")
		}
		seen[key] = true
	}
	return bindings, nil
}

type RunEvent struct {
	At      time.Time `json:"at"`
	State   string    `json:"state"`
	Message string    `json:"message"`
}
type Run struct {
	Resource
	Ingestion      *IngestionProvenance `json:"ingestion,omitempty"`
	WriterReleased bool                 `json:"writerReleased,omitempty"`
	Notebook       *NotebookProvenance  `json:"notebook,omitempty"`
	SourceArtifact *SourceArtifact      `json:"sourceArtifact,omitempty"`
	// Source is an independently persisted snapshot loaded only for the controller.
	Source            *RunSource       `json:"-"`
	WorkspaceID       string           `json:"workspaceId"`
	BindingID         string           `json:"bindingId"`
	SubmittedBy       ExecutionSubject `json:"submittedBy"`
	Profile           ExecutionProfile `json:"profile"`
	State             string           `json:"state"`
	CancelRequested   bool             `json:"cancelRequested"`
	SubmissionStarted bool             `json:"submissionStarted"`
	KubernetesName    string           `json:"kubernetesName"`
	KubernetesUID     string           `json:"kubernetesUid,omitempty"`
	Message           string           `json:"message"`
	ObservedAt        *time.Time       `json:"observedAt,omitempty"`
	Events            []RunEvent       `json:"events"`
	EventsTruncated   bool             `json:"eventsTruncated"`
}

func (r Run) Terminal() bool {
	return r.State == "succeeded" || r.State == "failed" || r.State == "cancelled"
}

// ExecutionDeadline includes queue time. Enforcement requests cancellation and
// still requires actual backend deletion/terminal evidence before completion.
func (r Run) ExecutionDeadline() time.Time {
	return r.CreatedAt.Add(time.Duration(r.Profile.RunLimitSeconds()) * time.Second)
}
func cloneRun(r Run) Run {
	if r.Profile.IngestionStorage != nil {
		grants := *r.Profile.IngestionStorage
		grants.ReadPrefixes = append([]string{}, grants.ReadPrefixes...)
		grants.WritePrefixes = append([]string{}, grants.WritePrefixes...)
		r.Profile.IngestionStorage = &grants
	}
	if r.Ingestion != nil {
		p := *r.Ingestion
		r.Ingestion = &p
	}
	if r.Notebook != nil {
		n := *r.Notebook
		n.CellIDs = append([]string{}, n.CellIDs...)
		r.Notebook = &n
	}
	if r.SourceArtifact != nil {
		a := *r.SourceArtifact
		r.SourceArtifact = &a
	}
	if r.Source != nil {
		source := *r.Source
		if r.Notebook != nil {
			source.Notebook = *r.Notebook
			source.Notebook.CellIDs = append([]string{}, source.Notebook.CellIDs...)
		}
		if r.Source.Ingestion != nil {
			p := *r.Source.Ingestion
			source.Ingestion = &p
		}
		r.Source = &source
	}
	r.Events = append([]RunEvent{}, r.Events...)
	if r.ObservedAt != nil {
		t := *r.ObservedAt
		r.ObservedAt = &t
	}
	return r
}

// runSummary avoids transferring or copying execution history for the recent-runs list.
// Callers use the owned run/events endpoints when they need retained history.
func runSummary(r Run) Run {
	r.Source = nil
	r.EventsTruncated = r.EventsTruncated || len(r.Events) > 0
	r.Events = []RunEvent{}
	if r.ObservedAt != nil {
		observed := *r.ObservedAt
		r.ObservedAt = &observed
	}
	return cloneRun(r)
}
func newRun(ws string, b ExecutionBinding, subject ExecutionSubject) Run {
	resource := newResource("run-")
	r := Run{Resource: resource, WorkspaceID: ws, BindingID: b.ID, SubmittedBy: subject, Profile: b.ExecutionProfile, State: "queued", KubernetesName: "ds-" + strings.TrimPrefix(resource.ID, "run-"), Message: "Queued for the execution controller.", Events: []RunEvent{}}
	r.Events = append(r.Events, RunEvent{At: r.CreatedAt, State: r.State, Message: r.Message})
	return r
}

var idempotencyKeyPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{8,128}$`)

type RunInput struct {
	BindingID      string             `json:"bindingId"`
	IdempotencyKey string             `json:"idempotencyKey"`
	Pipeline       *PipelineSelection `json:"pipeline,omitempty"`
	Notebook       *NotebookSelection `json:"notebook,omitempty"`
}

func (in RunInput) Validate() error {
	if in.Pipeline != nil && (in.Notebook != nil || !identifier.MatchString(in.Pipeline.ID) || in.Pipeline.Revision < 1) {
		return errors.New("pipeline requires a valid id and positive saved revision, without notebook selection")
	}
	if in.Notebook != nil && (!identifier.MatchString(in.Notebook.ID) || in.Notebook.Revision < 1) {
		return errors.New("notebook requires a valid id and positive saved revision")
	}
	if !identifier.MatchString(in.BindingID) || !idempotencyKeyPattern.MatchString(in.IdempotencyKey) {
		return errors.New("bindingId and an 8–128 character idempotencyKey containing letters, digits, dot, underscore, colon or hyphen are required")
	}
	return nil
}
func runFingerprint(in RunInput) string {
	data := []byte(in.BindingID) // Keep approved-app idempotency fingerprints stable.
	if in.Notebook != nil {
		data, _ = json.Marshal(struct {
			BindingID string
			Notebook  NotebookSelection
		}{in.BindingID, *in.Notebook})
	}
	if in.Pipeline != nil {
		data, _ = json.Marshal(struct {
			BindingID string
			Pipeline  PipelineSelection
		}{in.BindingID, *in.Pipeline})
	}
	v := sha256.Sum256(data)
	return hex.EncodeToString(v[:])
}

// RunLease is a durable fencing token. A late worker cannot update a run after lease expiry/reassignment.
type RunLease struct {
	RunID, Owner string
	Fence        int64
}
type RunUpdate struct {
	WriterReleased                                            bool
	SourceArtifact                                            *SourceArtifact
	State, UID, Message                                       string
	RetryAfter                                                time.Duration
	SubmissionStarted, RetainLease, Observed, CancelRequested bool
}
type BackendObservation struct {
	Exists              bool
	UID, State, Message string
}
type RunLogs struct {
	Text      string `json:"text"`
	Truncated bool   `json:"truncated"`
}

var ErrCapabilityUnavailable = errors.New("capability unavailable")
var ErrLeaseLost = errors.New("execution lease lost")
var ErrRunCapacity = errors.New("workspace active run capacity exceeded")

const MaxActiveWorkspaceRuns = 100
const MaxRunEvents = 200

func appendRunEvent(r Run, event RunEvent) Run {
	r.Events = append(r.Events, event)
	if len(r.Events) > MaxRunEvents {
		r.Events = append([]RunEvent{}, r.Events[len(r.Events)-MaxRunEvents:]...)
		r.EventsTruncated = true
	}
	return r
}

// ExecutionAuthorizer is mandatory in the production controller wrapper. In-memory test backends may omit it.
type ExecutionAuthorizer interface {
	Authorize(context.Context, Run) (bool, error)
}

type ExecutionBackend interface {
	Observe(context.Context, Run) (BackendObservation, error)
	Submit(context.Context, Run) (BackendObservation, error)
	Cancel(context.Context, Run) (BackendObservation, error)
	Logs(context.Context, Run) (RunLogs, error)
}

func validRunState(s string) bool {
	switch s {
	case "queued", "submitting", "running", "succeeded", "failed", "cancel_requested", "cancelled", "unknown":
		return true
	}
	return false
}
func applyRunUpdate(r Run, u RunUpdate, now time.Time) (Run, error) {
	if !validRunState(u.State) || len(u.Message) > 2048 || len(u.UID) > 256 {
		return Run{}, errors.New("invalid run update")
	}
	if r.KubernetesUID != "" && u.UID != "" && r.KubernetesUID != u.UID {
		return Run{}, ErrConflict
	}
	if r.Terminal() && (u.State != r.State || u.UID != "" && u.UID != r.KubernetesUID || u.SubmissionStarted || u.CancelRequested) {
		return Run{}, ErrConflict
	}
	if u.SourceArtifact != nil {
		if r.SourceArtifact == nil || u.SourceArtifact.Name != r.SourceArtifact.Name || len(u.SourceArtifact.UID) > 256 || (r.SourceArtifact.UID != "" && u.SourceArtifact.UID != r.SourceArtifact.UID) {
			return Run{}, ErrConflict
		}
		if !validArtifactTransition(r.SourceArtifact.State, u.SourceArtifact.State) {
			return Run{}, ErrConflict
		}
		a := *u.SourceArtifact
		a.CreationStarted = a.CreationStarted || r.SourceArtifact.CreationStarted
		r.SourceArtifact = &a
	}
	if u.UID != "" {
		r.KubernetesUID = u.UID
	}
	r.SubmissionStarted = r.SubmissionStarted || u.SubmissionStarted
	r.CancelRequested = r.CancelRequested || u.CancelRequested
	if u.Observed {
		t := now
		r.ObservedAt = &t
	}
	// Cancellation is a request, not a manufactured terminal outcome. The backend may complete before deletion.
	if r.CancelRequested && u.State != "succeeded" && u.State != "failed" && u.State != "cancelled" {
		u.State = "cancel_requested"
	}
	if r.State != u.State || r.Message != u.Message {
		r = appendRunEvent(r, RunEvent{At: now, State: u.State, Message: u.Message})
	}
	if u.WriterReleased {
		if r.Ingestion == nil || !r.Terminal() {
			return Run{}, ErrConflict
		}
		r.WriterReleased = true
	}
	r.State = u.State
	if r.Terminal() && r.SourceArtifact != nil && r.SourceArtifact.State != "deleted" {
		a := *r.SourceArtifact
		a.State = "cleanup_pending"
		r.SourceArtifact = &a
	}
	r.Message = u.Message
	r.UpdatedAt = now
	r.Revision++
	return r, nil
}
func subjectFor(p Principal) ExecutionSubject {
	issuer := p.Issuer
	if p.Development {
		issuer = "development"
	}
	return ExecutionSubject{Issuer: issuer, Subject: p.Subject}
}

// ReconcileOnce performs at most one leased unit of work; no external effect runs inside a database transaction.
// Unknown submission outcomes are observed but never blindly submitted again.
func ReconcileOnce(ctx context.Context, store Store, backend ExecutionBackend, owner string) (bool, error) {
	return reconcileOnce(ctx, store, backend, owner, time.Now)
}

func reconcileOnce(ctx context.Context, store Store, backend ExecutionBackend, owner string, now func() time.Time) (bool, error) {
	return reconcileOnceWithTimeout(ctx, store, backend, owner, now, 20*time.Second)
}
func reconcileOnceWithTimeout(ctx context.Context, store Store, backend ExecutionBackend, owner string, now func() time.Time, operationTimeout time.Duration) (bool, error) {
	r, lease, err := store.AcquireRun(ctx, owner, 30*time.Second)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	op, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	source := r.Source
	checkpoint := func(u RunUpdate) error {
		updated, e := store.AdvanceRun(ctx, lease, u)
		if e == nil {
			r = updated
			r.Source = source
		}
		return e
	}
	finish := checkpoint
	unknown := func(message string) error {
		return finish(RunUpdate{State: "unknown", Message: message, RetryAfter: 5 * time.Second})
	}
	if r.Terminal() {
		if r.Ingestion != nil && !r.WriterReleased {
			drain, ok := backend.(IngestionDrainBackend)
			if !ok {
				return true, finish(RunUpdate{State: r.State, Message: r.Message, RetryAfter: 5 * time.Second})
			}
			released, e := drain.DrainIngestion(op, r)
			if e != nil || !released {
				return true, finish(RunUpdate{State: r.State, Message: r.Message, RetryAfter: 5 * time.Second})
			}
			if e = checkpoint(RunUpdate{State: r.State, Message: r.Message, WriterReleased: true, RetainLease: true}); e != nil {
				return true, e
			}
		}
		if r.SourceArtifact == nil || r.SourceArtifact.State == "deleted" {
			return true, nil
		}
		artifacts, ok := backend.(SourceBackend)
		pending := func() error {
			a := *r.SourceArtifact
			a.State = "cleanup_pending"
			return finish(RunUpdate{State: r.State, Message: r.Message, SourceArtifact: &a, RetryAfter: 5 * time.Second})
		}
		if !ok {
			return true, pending()
		}
		observed, e := artifacts.ObserveSource(op, r)
		if e != nil {
			return true, pending()
		}
		if observed.Exists {
			if observed.UID == "" {
				return true, pending()
			}
			a := *r.SourceArtifact
			a.UID = observed.UID
			a.State = "cleanup_pending"
			if e = checkpoint(RunUpdate{State: r.State, Message: r.Message, SourceArtifact: &a, RetainLease: true}); e != nil {
				return true, e
			}
			if e = artifacts.DeleteSource(op, r); e != nil {
				return true, pending()
			}
		} else if r.SourceArtifact.CreationStarted && r.SourceArtifact.UID == "" {
			// A lost create may still complete. Never claim orphan cleanup from one absence observation.
			return true, pending()
		}
		a := *r.SourceArtifact
		a.State = "deleted"
		return true, finish(RunUpdate{State: r.State, Message: r.Message, SourceArtifact: &a})
	}
	checkDeadline := func() error {
		if !r.CancelRequested && !now().Before(r.ExecutionDeadline()) {
			err = checkpoint(RunUpdate{State: "cancel_requested", CancelRequested: true, Message: "Maximum run lifetime elapsed; cancellation requested.", RetainLease: true})
			return err
		}
		return nil
	}
	if err = checkDeadline(); err != nil {
		return true, err
	}
	// Existing cancellation is a cleanup obligation, not permission to execute.
	// A missing grant file must not block user/deadline cancellation of an owned resource.
	// The backend still verifies the immutable profile, namespace, ownership and UID.
	if authorizer, ok := backend.(ExecutionAuthorizer); ok && !r.CancelRequested {
		allowed, e := authorizer.Authorize(op, r)
		if e != nil {
			return true, unknown("Execution policy is unavailable; no backend operation was attempted.")
		}
		if !allowed && !r.CancelRequested {
			err = checkpoint(RunUpdate{State: "cancel_requested", CancelRequested: true, Message: "Execution grant revoked or runtime binding changed; cancellation requested.", RetainLease: true})
			if err != nil {
				return true, err
			}
		}
	}
	observed, err := backend.Observe(op, r)
	if err != nil {
		return true, unknown("Cannot confirm backend state; reconciliation will retry without assuming success.")
	}
	if observed.UID != "" && r.KubernetesUID != "" && observed.UID != r.KubernetesUID {
		return true, unknown("Backend identity changed; automatic submission is blocked.")
	}
	if err = checkDeadline(); err != nil {
		return true, err
	}
	if observed.Exists {
		if observed.UID == "" {
			return true, unknown("Backend returned no resource identity; automatic submission is blocked.")
		}
		r.KubernetesUID = observed.UID
		if r.SourceArtifact != nil && !r.CancelRequested && observed.State != "succeeded" && observed.State != "failed" {
			// Bind source lifetime to the verified application only after its UID is durably recorded.
			if err = checkpoint(RunUpdate{State: r.State, UID: observed.UID, Message: r.Message, Observed: true, RetainLease: true}); err != nil {
				return true, err
			}
			artifacts, ok := backend.(SourceBackend)
			if !ok {
				return true, unknown("Source artifact backend is unavailable.")
			}
			if err = artifacts.BindSource(op, r); err != nil {
				return true, unknown("Source artifact ownership could not be confirmed.")
			}
		}
		if observed.State == "succeeded" || observed.State == "failed" {
			return true, finish(RunUpdate{State: observed.State, UID: observed.UID, Message: observed.Message, Observed: true, RetryAfter: 5 * time.Second})
		}
		if r.CancelRequested {
			// Persist the observed UID before deletion. A lost DELETE response can then be reconciled by UID-aware absence.
			err = checkpoint(RunUpdate{State: "cancel_requested", UID: r.KubernetesUID, Message: "Cancellation requested; backend identity confirmed.", Observed: true, RetainLease: true})
			if err != nil {
				return true, err
			}
			observed, err = backend.Cancel(op, r)
			if err != nil {
				return true, unknown("Cancellation requested; backend deletion is not yet confirmed.")
			}
			if !observed.Exists {
				return true, finish(RunUpdate{State: "cancelled", UID: r.KubernetesUID, Message: "Backend resource deletion confirmed.", Observed: true})
			}
			if observed.State == "succeeded" || observed.State == "failed" {
				return true, finish(RunUpdate{State: observed.State, UID: r.KubernetesUID, Message: observed.Message, Observed: true})
			}
			return true, finish(RunUpdate{State: "cancel_requested", UID: r.KubernetesUID, Message: "Cancellation requested; waiting for backend resource deletion.", Observed: true, RetryAfter: 5 * time.Second})
		}
		state := observed.State
		if state != "running" && state != "submitting" {
			state = "unknown"
		}
		return true, finish(RunUpdate{State: state, UID: observed.UID, Message: observed.Message, Observed: true, RetryAfter: 5 * time.Second})
	}
	if r.CancelRequested {
		if r.SubmissionStarted && r.KubernetesUID == "" {
			return true, unknown("Cancellation requested, but an earlier submission outcome is unknown; manual verification is required.")
		}
		return true, finish(RunUpdate{State: "cancelled", Message: "No backend resource remains.", Observed: true})
	}
	if r.SubmissionStarted || r.KubernetesUID != "" {
		return true, unknown("Previously submitted resource is absent; automatic resubmission is blocked to prevent duplicate data writes.")
	}
	if r.SourceArtifact != nil {
		artifacts, ok := backend.(SourceBackend)
		if !ok {
			return true, unknown("Source artifact backend is unavailable.")
		}
		if err = r.ValidateSource(); err != nil {
			return true, unknown("Stored execution source integrity could not be verified.")
		}
		if op.Err() != nil {
			return true, unknown("Source preparation deadline elapsed before any new source operation; reconciliation will retry.")
		}
		a := *r.SourceArtifact
		wasStarted := a.CreationStarted
		a.CreationStarted = true
		if err = checkpoint(RunUpdate{State: r.State, Message: "Preparing the immutable saved execution source.", SourceArtifact: &a, RetainLease: true}); err != nil {
			return true, err
		}
		if r.CancelRequested {
			if !wasStarted {
				a.State = "deleted"
				return true, finish(RunUpdate{State: "cancelled", Message: "Cancelled before source preparation.", SourceArtifact: &a})
			}
			return true, finish(RunUpdate{State: "cancelled", Message: "Cancelled before Spark submission; source cleanup pending."})
		}
		prepared, e := artifacts.PrepareSource(op, r)
		if e != nil {
			return true, unknown("Execution source preparation is unconfirmed; no Spark submission was attempted.")
		}
		if prepared.UID == "" || prepared.Name != a.Name || prepared.State != "ready" {
			return true, unknown("Execution source preparation returned no verified identity.")
		}
		if err = checkpoint(RunUpdate{State: r.State, Message: "Immutable execution source prepared.", SourceArtifact: &prepared, RetainLease: true}); err != nil {
			return true, err
		}
		if err = checkDeadline(); err != nil {
			return true, err
		}
		if r.CancelRequested {
			return true, finish(RunUpdate{State: "cancelled", Message: "Cancelled before Spark submission; source cleanup pending."})
		}
	}
	if op.Err() != nil {
		return true, unknown("Backend operation deadline elapsed before the submission checkpoint; reconciliation will retry without recording a possible Spark submission.")
	}
	err = checkpoint(RunUpdate{State: "submitting", Message: "Submitting the immutable application.", SubmissionStarted: true, RetainLease: true, Observed: true})
	if err != nil {
		return true, err
	}
	if r.CancelRequested {
		return true, finish(RunUpdate{State: "cancelled", Message: "Cancellation accepted before backend submission.", Observed: true})
	}
	observed, err = backend.Submit(op, r)
	if err != nil {
		return true, unknown("Submission outcome is unknown; observation will continue without blind retry.")
	}
	if !observed.Exists || observed.UID == "" {
		return true, unknown("Submission returned no confirmed resource identity; observation will continue.")
	}
	state := observed.State
	if state != "running" && state != "succeeded" && state != "failed" {
		state = "submitting"
	}
	return true, finish(RunUpdate{State: state, UID: observed.UID, Message: observed.Message, Observed: true, RetryAfter: 5 * time.Second})
}
