package platform

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestExecutionLifetimeValidationAndLegacyDefault(t *testing.T) {
	for _, seconds := range []int{0, 60, 3600, 86400} {
		b := executionTestBinding()
		b.MaxRunSeconds = seconds
		if err := b.Validate(); err != nil {
			t.Fatalf("%d: %v", seconds, err)
		}
	}
	for _, seconds := range []int{-1, 1, 59, 86401} {
		b := executionTestBinding()
		b.MaxRunSeconds = seconds
		if err := b.Validate(); err == nil {
			t.Fatalf("unbounded/invalid lifetime %d accepted", seconds)
		}
	}
	r := Run{Resource: Resource{CreatedAt: time.Date(2026, 11, 7, 0, 0, 0, 0, time.UTC)}}
	if !r.ExecutionDeadline().Equal(r.CreatedAt.Add(time.Hour)) {
		t.Fatal("legacy profile has no bounded default")
	}
}

func TestDeadlineIncludesQueueAndExactBoundary(t *testing.T) {
	for _, mode := range []string{"before deadline", "exact deadline", "during observation", "legacy default"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			s := NewMemoryStore()
			b := executionTestBinding()
			b.MaxRunSeconds = 60
			if mode == "legacy default" {
				b.MaxRunSeconds = 0
			}
			r := newRun("default", b, b.AllowedSubjects[0])
			r.CreatedAt = time.Now().Add(-2 * time.Hour)
			deadline := r.ExecutionDeadline()
			if _, _, err := s.CreateRun(ctx, r, "lifetime-test-key", "same"); err != nil {
				t.Fatal(err)
			}
			calls := 0
			now := func() time.Time {
				calls++
				if mode == "before deadline" || (mode == "during observation" && calls == 1) {
					return deadline.Add(-time.Nanosecond)
				}
				return deadline
			}
			backend := &fakeExecutionBackend{store: s}
			if _, err := reconcileOnce(ctx, s, backend, "deadline-worker", now); err != nil {
				t.Fatal(err)
			}
			stored, _ := s.Run(ctx, "default", r.ID, r.SubmittedBy)
			if mode == "before deadline" {
				if backend.submits != 1 || stored.State != "running" {
					t.Fatal("unexpired job not submitted")
				}
				return
			}
			if backend.submits != 0 || !stored.CancelRequested || stored.State != "cancelled" || stored.SubmissionStarted {
				t.Fatalf("expired queued job was submitted or not cancelled: %+v", stored)
			}
			found := false
			for _, event := range stored.Events {
				if strings.Contains(event.Message, "Maximum run lifetime") {
					found = true
				}
			}
			if !found {
				t.Fatal("expiry reason not retained in history")
			}
		})
	}
}

func TestExpiredActiveRunCheckpointsUIDBeforeLostDeleteResponse(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	b := executionTestBinding()
	b.MaxRunSeconds = 60
	r := newRun("default", b, b.AllowedSubjects[0])
	r.CreatedAt = time.Now().Add(-time.Hour)
	r.SubmissionStarted = true
	if _, _, err := s.CreateRun(ctx, r, "active-lifetime-key", "same"); err != nil {
		t.Fatal(err)
	}
	backend := &fakeExecutionBackend{observation: BackendObservation{Exists: true, UID: "observed-before-delete", State: "running"}, cancelErr: errors.New("lost delete response")}
	now := func() time.Time { return r.ExecutionDeadline() }
	if _, err := reconcileOnce(ctx, s, backend, "deadline-worker", now); err != nil {
		t.Fatal(err)
	}
	stored, _ := s.Run(ctx, "default", r.ID, r.SubmittedBy)
	if backend.cancels != 1 || backend.submits != 0 || stored.KubernetesUID != "observed-before-delete" || !stored.CancelRequested || stored.Terminal() {
		t.Fatalf("lost deletion was unsafe: %+v", stored)
	}
	// A replacement worker sees absence after the successful but unacknowledged deletion.
	backend.observation = BackendObservation{}
	backend.cancelErr = nil
	dueRun(s, r.ID)
	if _, err := reconcileOnce(ctx, s, backend, "replacement-worker", now); err != nil {
		t.Fatal(err)
	}
	stored, _ = s.Run(ctx, "default", r.ID, r.SubmittedBy)
	if stored.State != "cancelled" || backend.submits != 0 {
		t.Fatal("deletion confirmation did not preserve single submission")
	}
}

func TestDeadlineNeverInventsTerminationOrOverridesObservedCompletion(t *testing.T) {
	for _, mode := range []string{"unknown submission", "backend offline", "already succeeded", "completed during cancellation"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			s := NewMemoryStore()
			b := executionTestBinding()
			b.MaxRunSeconds = 60
			r := newRun("default", b, b.AllowedSubjects[0])
			r.CreatedAt = time.Now().Add(-time.Hour)
			r.SubmissionStarted = true
			if _, _, err := s.CreateRun(ctx, r, "uncertain-deadline-key", "same"); err != nil {
				t.Fatal(err)
			}
			fake := &fakeExecutionBackend{}
			var backend ExecutionBackend = fake
			switch mode {
			case "backend offline":
				fake.observeErr = errors.New("offline")
			case "already succeeded":
				fake.observation = BackendObservation{Exists: true, UID: "completed-uid", State: "succeeded", Message: "Observed success"}
			case "completed during cancellation":
				fake.observation = BackendObservation{Exists: true, UID: "completed-uid", State: "running"}
				backend = &successOnCancel{fake}
			}
			if _, err := reconcileOnce(ctx, s, backend, "deadline-worker", func() time.Time { return r.ExecutionDeadline() }); err != nil {
				t.Fatal(err)
			}
			stored, _ := s.Run(ctx, "default", r.ID, r.SubmittedBy)
			if fake.submits != 0 || !stored.CancelRequested {
				t.Fatal("expired run was resubmitted or lacked durable cancellation")
			}
			if strings.HasPrefix(mode, "already") || mode == "completed during cancellation" {
				if stored.State != "succeeded" {
					t.Fatal("deadline overwrote actual completion")
				}
			} else if stored.Terminal() {
				t.Fatal("deadline manufactured terminal evidence")
			}
		})
	}
}

type successOnCancel struct{ *fakeExecutionBackend }

func (b *successOnCancel) Cancel(context.Context, Run) (BackendObservation, error) {
	b.cancels++
	return BackendObservation{Exists: true, UID: "completed-uid", State: "succeeded", Message: "Observed success during cancellation"}, nil
}
