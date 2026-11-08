package platform

import (
	"context"
	"dataslate.local/api/internal/airflow"
	"errors"
	"time"
)

type WorkflowEngine interface {
	Definition(context.Context, airflow.Target) (airflow.Definition, error)
	Trigger(context.Context, airflow.Target, string, airflow.Receipt) (airflow.Run, error)
	Run(context.Context, airflow.Target, string, airflow.Receipt) (airflow.Run, error)
	Tasks(context.Context, airflow.Target, string) ([]airflow.TaskInstance, bool, error)
	History(context.Context, airflow.Target) ([]airflow.Run, bool, error)
	Close()
}
type WorkflowBackend interface {
	Authorize(context.Context, WorkflowWork) error
	Open(context.Context, WorkflowBinding) (WorkflowEngine, error)
}
type FileWorkflowBackend struct{ BindingsFile string }

func (b *FileWorkflowBackend) Authorize(_ context.Context, w WorkflowWork) error {
	bindings, e := LoadWorkflowBindings(b.BindingsFile)
	if e != nil {
		return ErrWorkflowAccess
	}
	for _, binding := range bindings {
		if binding.WorkspaceID == w.Run.WorkspaceID && binding.ID == w.Run.BindingID && binding.Hash() == w.PolicyHash && binding.Allows(w.Run.SubmittedBy) {
			return nil
		}
	}
	return ErrWorkflowAccess
}
func (b *FileWorkflowBackend) Open(_ context.Context, binding WorkflowBinding) (WorkflowEngine, error) {
	if binding.Validate() != nil {
		return nil, ErrWorkflowAccess
	}
	return airflow.New(airflow.Config{Endpoint: binding.Endpoint, CAFile: binding.CAFile, TokenFile: binding.TokenFile})
}
func ReconcileWorkflowOnce(ctx context.Context, store WorkflowStore, backend WorkflowBackend, owner string) (bool, error) {
	work, lease, e := store.AcquireWorkflow(ctx, owner, 2*time.Minute)
	if errors.Is(e, ErrNotFound) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	finish := func(state, message string, final bool) error {
		_, e := store.AdvanceWorkflow(ctx, lease, WorkflowUpdate{State: state, Message: message, Final: final, RetryAfter: 5 * time.Second})
		return e
	}
	denied := func() error {
		state := "failed"
		if work.Run.DispatchStarted {
			state = "unknown"
		}
		return finish(state, "Workflow grant changed. No further backend operation is authorized; an already submitted run may continue in Airflow.", true)
	}
	if backend.Authorize(ctx, work) != nil {
		return true, denied()
	}
	deadline := work.Run.CreatedAt.Add(time.Duration(work.Binding.MaxRunSeconds) * time.Second)
	if !time.Now().Before(deadline) {
		state := "failed"
		message := "Workflow admission deadline elapsed before dispatch."
		if work.Run.DispatchStarted {
			state = "unknown"
			message = "Observation deadline elapsed. The Airflow run may still be active; investigate its original run ID."
		}
		return true, finish(state, message, true)
	}
	opDeadline := time.Now().Add(90 * time.Second)
	if deadline.Before(opDeadline) {
		opDeadline = deadline
	}
	op, done := context.WithDeadline(ctx, opDeadline)
	defer done()
	engine, e := backend.Open(op, work.Binding)
	if e != nil {
		state := "checking"
		if work.Run.DispatchStarted {
			state = "unknown"
		}
		return true, finish(state, "Airflow credentials or transport are unavailable. Retaining the original request.", false)
	}
	defer engine.Close()
	if !work.Run.DispatchStarted {
		work, e = store.AdvanceWorkflow(ctx, lease, WorkflowUpdate{State: "checking", Message: "Verifying the approved Airflow bundle and current schedule.", RetainLease: true})
		if e != nil {
			return true, e
		}
		definition, e := engine.Definition(op, work.Binding.Target())
		if e != nil {
			return true, finish("checking", "The approved Airflow DAG cannot yet be verified. No trigger was sent.", false)
		}
		if !workflowDefinition(work.Binding, definition).CanTrigger {
			return true, finish("failed", "The deployed DAG is paused, stale, invalid, or differs from the approved version. No trigger was sent.", true)
		}
		if backend.Authorize(op, work) != nil {
			return true, denied()
		}
		if op.Err() != nil || !time.Now().Before(deadline) {
			return true, finish("failed", "Workflow admission deadline elapsed before dispatch.", true)
		}
		// This durable transition is the only permission to send a POST. A crash
		// after it leaves readback-only recovery, including when no POST reached Airflow.
		work, e = store.AdvanceWorkflow(ctx, lease, WorkflowUpdate{State: "submitting", Message: "Trigger intent recorded. Confirming the exact Airflow run.", DispatchStarted: true, RetainLease: true})
		if e != nil {
			return true, e
		}
		// Recheck immediately after the potentially blocked store transaction.
		if backend.Authorize(op, work) != nil {
			return true, denied()
		}
		if op.Err() != nil || !time.Now().Before(deadline) {
			return true, finish("unknown", "Dispatch was reserved but its deadline elapsed. The original run must be investigated.", true)
		}
		_, _ = engine.Trigger(op, work.Binding.Target(), work.Run.BackendRunID, work.Receipt())
		// A trigger acknowledgment alone never establishes execution or success.
	}
	if backend.Authorize(op, work) != nil {
		return true, denied()
	}
	observed, e := engine.Run(op, work.Binding.Target(), work.Run.BackendRunID, work.Receipt())
	if e != nil {
		return true, finish("unknown", "Airflow has not confirmed the exact owned run and bundle. Readback will continue without another trigger.", false)
	}
	if backend.Authorize(op, work) != nil {
		return true, denied()
	}
	tasks, truncated, e := engine.Tasks(op, work.Binding.Target(), work.Run.BackendRunID)
	if e != nil {
		tasks = []airflow.TaskInstance{}
		truncated = true
	}
	if backend.Authorize(op, work) != nil {
		return true, denied()
	}
	state := "submitting"
	message := "Airflow confirmed the run is queued."
	final := false
	switch observed.State {
	case "running":
		state = "running"
		message = "Airflow is executing the workflow."
	case "success":
		state = "succeeded"
		message = "Airflow confirmed workflow success."
		final = true
	case "failed":
		state = "failed"
		message = "Airflow confirmed workflow failure. Its configured retry policy remains authoritative."
		final = true
	}
	_, e = store.AdvanceWorkflow(ctx, lease, WorkflowUpdate{State: state, Message: message, Final: final, Observation: &observed, Tasks: tasks, TasksTruncated: truncated, RetryAfter: 5 * time.Second})
	return true, e
}
