package platform

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"
)

type memoryExecution struct {
	Run                Run
	Fingerprint, Owner string
	Fence              int64
	Until, Next        time.Time
}

func runKey(ws string, subject ExecutionSubject, key string) string {
	b, _ := json.Marshal([]string{ws, subject.Issuer, subject.Subject, key})
	return string(b)
}
func (s *MemoryStore) CreateRun(_ context.Context, r Run, key, fingerprint string) (Run, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.workspaces[r.WorkspaceID]; !ok {
		return Run{}, false, ErrNotFound
	}
	k := runKey(r.WorkspaceID, r.SubmittedBy, key)
	if id, ok := s.runKeys[k]; ok {
		existing := s.executions[id]
		if existing.Fingerprint != fingerprint {
			return Run{}, false, ErrConflict
		}
		return cloneRun(existing.Run), true, nil
	}
	active := 0
	for _, entry := range s.executions {
		if entry.Run.WorkspaceID == r.WorkspaceID && entry.Run.NeedsReconciliation() {
			active++
		}
	}
	if active >= MaxActiveWorkspaceRuns {
		return Run{}, false, ErrRunCapacity
	}
	if r.Profile.Kind() == NotebookPythonBatch {
		if r.Notebook == nil {
			return Run{}, false, ErrNotebookNotExecutable
		}
		n, ok := s.notebooks[r.Notebook.ID]
		if !ok || n.WorkspaceID != r.WorkspaceID {
			return Run{}, false, ErrNotFound
		}
		var err error
		r, err = snapshotNotebook(r, n)
		if err != nil {
			return Run{}, false, err
		}
	} else if r.Profile.Kind() == FileIngestionBatch {
		var err error
		r, err = s.snapshotIngestionLocked(r)
		if err != nil {
			return Run{}, false, err
		}
	} else if r.Ingestion != nil {
		return Run{}, false, ErrIngestionNotExecutable
	} else if r.Notebook != nil {
		return Run{}, false, ErrNotebookNotExecutable
	}
	s.executions[r.ID] = &memoryExecution{Run: cloneRun(r), Fingerprint: fingerprint}
	s.runKeys[k] = r.ID
	return cloneRun(r), false, nil
}
func (s *MemoryStore) Runs(_ context.Context, ws string, subject ExecutionSubject) ([]Run, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []Run{}
	for _, entry := range s.executions {
		r := entry.Run
		if r.WorkspaceID == ws && r.SubmittedBy == subject {
			out = append(out, runSummary(r))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	if len(out) > 200 {
		out = out[:200]
	}
	return out, nil
}
func (s *MemoryStore) Run(_ context.Context, ws, id string, subject ExecutionSubject) (Run, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.executions[id]
	if !ok || entry.Run.WorkspaceID != ws || entry.Run.SubmittedBy != subject {
		return Run{}, ErrNotFound
	}
	return cloneRun(entry.Run), nil
}
func (s *MemoryStore) CancelRun(_ context.Context, ws, id string, subject ExecutionSubject) (Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.executions[id]
	if !ok || entry.Run.WorkspaceID != ws || entry.Run.SubmittedBy != subject {
		return Run{}, ErrNotFound
	}
	r := entry.Run
	if r.Terminal() || r.CancelRequested {
		return cloneRun(r), nil
	}
	now := time.Now().UTC()
	r.CancelRequested = true
	r.State = "cancel_requested"
	r.Message = "Cancellation requested; backend confirmation is pending."
	r.Revision++
	r.UpdatedAt = now
	r = appendRunEvent(r, RunEvent{At: now, State: r.State, Message: r.Message})
	entry.Run = r
	entry.Next = now
	return cloneRun(r), nil
}
func (s *MemoryStore) AcquireRun(_ context.Context, owner string, ttl time.Duration) (Run, RunLease, error) {
	if owner == "" || len(owner) > 200 || ttl <= 0 || ttl > time.Minute {
		return Run{}, RunLease{}, errors.New("invalid execution lease request")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	var selected *memoryExecution
	for _, entry := range s.executions {
		if !entry.Run.NeedsReconciliation() || entry.Until.After(now) || entry.Next.After(now) {
			continue
		}
		if selected == nil || entry.Next.Before(selected.Next) {
			selected = entry
		}
	}
	if selected == nil {
		return Run{}, RunLease{}, ErrNotFound
	}
	selected.Owner = owner
	selected.Fence++
	selected.Until = now.Add(ttl)
	return cloneRun(selected.Run), RunLease{RunID: selected.Run.ID, Owner: owner, Fence: selected.Fence}, nil
}
func (s *MemoryStore) AdvanceRun(_ context.Context, lease RunLease, u RunUpdate) (Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.executions[lease.RunID]
	now := time.Now().UTC()
	if !ok || entry.Owner != lease.Owner || entry.Fence != lease.Fence || !entry.Until.After(now) {
		return Run{}, ErrLeaseLost
	}
	r, err := applyRunUpdate(entry.Run, u, now)
	if err != nil {
		return Run{}, err
	}
	entry.Run = r
	if !u.RetainLease {
		entry.Owner = ""
		entry.Until = time.Time{}
		entry.Next = now.Add(u.RetryAfter)
	}
	return cloneRun(r), nil
}

func (s *MemoryStore) RunSource(ctx context.Context, ws, id string, subject ExecutionSubject) (RunSource, error) {
	r, err := s.Run(ctx, ws, id, subject)
	if err != nil {
		return RunSource{}, err
	}
	if r.Source == nil {
		return RunSource{}, ErrNotFound
	}
	return *r.Source, nil
}
