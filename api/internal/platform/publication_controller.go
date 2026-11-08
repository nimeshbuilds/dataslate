package platform

import (
	"context"
	"errors"
	"time"

	"dataslate.local/api/internal/trino"
)

type PublicationBackend interface {
	Authorize(context.Context, PublicationWork) (bool, error)
	Inspect(context.Context, PublicationWork) (trino.DeltaObservation, error)
	Register(context.Context, PublicationWork) (trino.Page, error)
	Next(context.Context, PublicationWork) (trino.Page, error)
	Cancel(context.Context, PublicationWork) error
}
type FilePublicationBackend struct{ BindingsFile, ExecutionBindingsFile, SQLBindingsFile string }

func (b *FilePublicationBackend) Authorize(_ context.Context, w PublicationWork) (bool, error) {
	bindings, e := LoadPublicationBindings(b.BindingsFile)
	if e != nil {
		return false, e
	}
	execution, e := LoadExecutionBindings(b.ExecutionBindingsFile)
	if e != nil {
		return false, e
	}
	sql, e := LoadSQLBindings(b.SQLBindingsFile)
	if e != nil {
		return false, e
	}
	for _, p := range bindings {
		if validatePublicationReaders(p, sql) != nil || validatePublicationExecution(p, execution) != nil {
			continue
		}
		if p.WorkspaceID == w.Publication.WorkspaceID && p.ID == w.Publication.BindingID && p.Hash() == w.PolicyHash && p.Allows(w.Publication.SubmittedBy) {
			for _, x := range execution {
				if x.WorkspaceID == w.Run.WorkspaceID && x.ID == w.Run.BindingID && x.Allows(w.Run.SubmittedBy) && sameExecutionProfile(x.ExecutionProfile, w.Run.Profile) {
					return true, nil
				}
			}
		}
	}
	return false, nil
}
func (b *FilePublicationBackend) client(ctx context.Context, w PublicationWork) (*trino.Client, error) {
	allowed, e := b.Authorize(ctx, w)
	if e != nil || !allowed {
		return nil, ErrPublicationAccess
	}
	return trino.New(trino.Config{Endpoint: w.Binding.Endpoint, CAFile: w.Binding.CAFile, TokenFile: w.Binding.TokenFile})
}
func (b *FilePublicationBackend) Inspect(ctx context.Context, w PublicationWork) (trino.DeltaObservation, error) {
	c, e := b.client(ctx, w)
	if e != nil {
		return trino.DeltaObservation{}, e
	}
	return c.InspectDelta(ctx, w.target(), w.Binding.EngineUser, w.Publication.ID)
}
func (b *FilePublicationBackend) Register(ctx context.Context, w PublicationWork) (trino.Page, error) {
	c, e := b.client(ctx, w)
	if e != nil {
		return trino.Page{}, e
	}
	return c.Register(ctx, w.target(), w.Binding.EngineUser, w.Publication.ID)
}
func (b *FilePublicationBackend) Next(ctx context.Context, w PublicationWork) (trino.Page, error) {
	c, e := b.client(ctx, w)
	if e != nil {
		return trino.Page{}, e
	}
	return c.Next(ctx, w.Cursor, w.Binding.EngineUser)
}
func (b *FilePublicationBackend) Cancel(ctx context.Context, w PublicationWork) error {
	c, e := trino.New(trino.Config{Endpoint: w.Binding.Endpoint, CAFile: w.Binding.CAFile, TokenFile: w.Binding.TokenFile})
	if e != nil {
		return e
	}
	return c.Cancel(ctx, w.Cursor, w.Binding.EngineUser)
}

// ReconcilePublicationOnce owns registration retries: once dispatch is recorded,
// only cursor progress and metadata readback are permitted. No Spark is submitted.
func ReconcilePublicationOnce(ctx context.Context, store PublicationStore, backend PublicationBackend, owner string) (bool, error) {
	work, lease, e := store.AcquirePublication(ctx, owner, 2*time.Minute)
	if errors.Is(e, ErrNotFound) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	deadline := work.Publication.CreatedAt.Add(time.Duration(work.Binding.MaxRunSeconds) * time.Second)
	operationDeadline := time.Now().Add(90 * time.Second)
	if deadline.Before(operationDeadline) {
		operationDeadline = deadline
	}
	op, cancel := context.WithDeadline(ctx, operationDeadline)
	defer cancel()
	persist := func(u PublicationUpdate) error {
		var err error
		work, err = store.AdvancePublication(ctx, lease, u)
		return err
	}
	stop := func(message string) error {
		state := "failed"
		if work.Publication.RegistrationStarted {
			state = "unknown"
		}
		return persist(PublicationUpdate{State: state, Message: message, Final: true})
	}
	allowed, e := backend.Authorize(op, work)
	if e != nil || !allowed {
		return true, stop("Publication policy changed or was revoked; registration is not retried.")
	}
	if !time.Now().Before(deadline) {
		if work.Cursor.NextURI != "" {
			_ = backend.Cancel(op, work)
		}
		return true, stop("Publication deadline elapsed. Any dispatched registration remains unresolved; no rollback or retry is inferred.")
	}
	if work.Publication.RegistrationStarted && work.Cursor.NextURI != "" {
		page, err := backend.Next(op, work)
		if err != nil {
			cursor := trino.Cursor{ID: work.Cursor.ID}
			return true, persist(PublicationUpdate{State: "unknown", Message: "Registration cursor is unavailable; catalog evidence will be checked without resubmission.", Cursor: &cursor, RetryAfter: 5 * time.Second})
		}
		if len(page.Rows) != 0 {
			return true, stop("Registration returned an unexpected result; catalog state remains unconfirmed.")
		}
		message := "Registration acknowledged; independent catalog verification is pending."
		state := "verifying"
		if page.Error != nil {
			message = "Registration reported an error; catalog state will be checked without retry."
			state = "unknown"
			page.NextURI = ""
		}
		return true, persist(PublicationUpdate{State: state, Message: message, Cursor: &page.Cursor, RetryAfter: time.Second})
	}
	observation, e := backend.Inspect(op, work)
	if e != nil {
		if errors.Is(e, trino.ErrDeltaMetadata) || errors.Is(e, trino.ErrDeltaTarget) {
			return true, stop("Catalog metadata conflicts with the verified ingestion outcome; publication was not confirmed.")
		}
		return true, persist(PublicationUpdate{State: "unknown", Message: "Catalog verification is unavailable; no registration retry is permitted after dispatch.", RetryAfter: 5 * time.Second})
	}
	if observation.Exists {
		if !work.Publication.RegistrationStarted && !work.PreviouslyPublished {
			return true, stop("The target already exists without a prior verified DataSlate mapping; it was not adopted.")
		}
		if !observation.VersionPresent {
			return true, stop("The recorded Delta version is absent; publication was not confirmed.")
		}
		allowed, e = backend.Authorize(op, work)
		if e != nil || !allowed {
			return true, stop("Publication policy changed during verification; output was withheld.")
		}
		if op.Err() != nil || !time.Now().Before(deadline) {
			return true, stop("Publication deadline elapsed during verification; completion was not recorded.")
		}
		return true, persist(PublicationUpdate{State: "published", Message: "Catalog location, recorded version, schema and protocol verified. SQL reads the current table.", Verified: true, Final: true})
	}
	if work.Publication.RegistrationStarted {
		return true, persist(PublicationUpdate{State: "unknown", Message: "Dispatched registration is not visible yet; only catalog reconciliation will continue.", RetryAfter: 5 * time.Second})
	}
	if work.PreviouslyPublished {
		return true, stop("A previously verified catalog mapping is missing; it will not be silently recreated.")
	}
	if e = persist(PublicationUpdate{State: "registering", Message: "Registration dispatch recorded before the engine request.", RegistrationStarted: true, RetainLease: true}); e != nil {
		return true, e
	}
	// Reauthorize after durable fencing and immediately before the only write POST.
	allowed, e = backend.Authorize(op, work)
	if e != nil || !allowed {
		return true, stop("Publication policy changed before dispatch; registration was not retried.")
	}
	if op.Err() != nil || !time.Now().Before(deadline) {
		return true, stop("Publication deadline elapsed before registration dispatch; no request was sent and no automatic retry is permitted.")
	}
	page, e := backend.Register(op, work)
	if e != nil {
		return true, persist(PublicationUpdate{State: "unknown", Message: "Registration acknowledgment was lost; catalog evidence will be reconciled without resubmission.", RetryAfter: 5 * time.Second})
	}
	if len(page.Rows) != 0 {
		return true, stop("Registration returned an unexpected result; catalog state remains unconfirmed.")
	}
	state, message := "verifying", "Registration acknowledged; independent catalog verification is pending."
	if page.Error != nil {
		state = "unknown"
		message = "Registration reported an error; catalog evidence will be checked without retry."
		page.NextURI = ""
	}
	return true, persist(PublicationUpdate{State: state, Message: message, Cursor: &page.Cursor, RetryAfter: time.Second})
}
