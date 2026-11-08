package platform

import (
	"context"
	"sort"
	"time"
)

type memoryMutationRecord struct {
	Fingerprint string
	Receipt     SylonMemoryReceipt
}
type memoryPurge struct {
	WorkspaceID, MemoryID string
	Through               int
}

func memoryMutationKey(ws string, o ExecutionSubject, key string) string {
	return ws + "\x00" + o.Issuer + "\x00" + o.Subject + "\x00" + key
}
func (s *MemoryStore) memoryMutation(ws string, o ExecutionSubject, key, fp string) (SylonMemoryReceipt, bool, error) {
	v, ok := s.memoryMutations[memoryMutationKey(ws, o, key)]
	if !ok {
		return SylonMemoryReceipt{}, false, nil
	}
	if v.Fingerprint != fp {
		return SylonMemoryReceipt{}, false, ErrConflict
	}
	return v.Receipt, true, nil
}
func (s *MemoryStore) SylonMemoryMutation(_ context.Context, ws string, o ExecutionSubject, key, fp string) (SylonMemoryReceipt, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.memoryMutation(ws, o, key, fp)
}
func (s *MemoryStore) memoryWork(ws, id string, o ExecutionSubject) (SylonMemoryWork, error) {
	v, ok := s.memories[id]
	if !ok || v.Memory.WorkspaceID != ws || v.Memory.Owner != o {
		return SylonMemoryWork{}, ErrNotFound
	}
	return cloneMemory(v), nil
}
func (s *MemoryStore) SylonMemoryWork(_ context.Context, ws, id string, o ExecutionSubject) (SylonMemoryWork, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.memoryWork(ws, id, o)
}
func (s *MemoryStore) SylonMemories(_ context.Context, ws string, o ExecutionSubject, binding string, recall bool) ([]SylonMemoryWork, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []SylonMemoryWork{}
	for _, v := range s.memories {
		m := v.Memory
		if m.WorkspaceID != ws || m.Owner != o || v.State == "deleted" || (binding != "" && m.BindingID != binding) {
			continue
		}
		if recall && (v.State != "active" || !m.ExpiresAt.After(time.Now())) {
			continue
		}
		if v.State == "expired" && !m.PurgePending {
			continue
		}
		out = append(out, cloneMemory(v))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Memory.UpdatedAt.After(out[j].Memory.UpdatedAt) })
	if len(out) > 100 {
		out = out[:100]
	}
	return out, nil
}
func (s *MemoryStore) memoryDepsCurrent(ws string, o ExecutionSubject, deps []SylonDependency) bool {
	for _, d := range deps {
		if d.Kind != "memory" {
			continue
		}
		m, e := s.memoryWork(ws, d.MemoryID, o)
		if e != nil || m.State != "active" || m.Memory.Revision != d.MemoryRevision || !m.Memory.ExpiresAt.After(time.Now()) {
			return false
		}
		j, e := s.sylonWork(ws, m.Memory.Source.JobID, o)
		if e != nil || j.Work.Job.State != "succeeded" || j.Work.Job.Result == nil || SylonOutputHash(j.Work.Job.Result) != m.OutputHash {
			return false
		}
	}
	return true
}
func (s *MemoryStore) CreateSylonMemory(_ context.Context, w SylonMemoryWork, key, fp string) (SylonMemoryReceipt, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := w.Memory
	if r, ok, e := s.memoryMutation(m.WorkspaceID, m.Owner, key, fp); ok || e != nil {
		return r, ok, e
	}
	j, e := s.sylonWork(m.WorkspaceID, m.Source.JobID, m.Owner)
	if e != nil {
		return SylonMemoryReceipt{}, false, e
	}
	if !m.ExpiresAt.After(time.Now()) || j.Work.Job.State != "succeeded" || j.Work.Job.Revision != m.Source.Revision || j.Work.Job.Result == nil || SylonOutputHash(j.Work.Job.Result) != w.OutputHash || j.Work.PolicyHash != w.PolicyHash || !s.memoryDepsCurrent(m.WorkspaceID, m.Owner, w.Dependencies) {
		return SylonMemoryReceipt{}, false, ErrMemoryRevision
	}
	count := 0
	for _, v := range s.memories {
		if v.Memory.WorkspaceID == m.WorkspaceID && v.Memory.Owner == m.Owner && v.State == "active" {
			count++
		}
	}
	if count >= MaxSylonMemories || len(w.Dependencies) >= MaxSylonDependencies {
		return SylonMemoryReceipt{}, false, ErrMemoryCapacity
	}
	s.memories[m.ID] = cloneMemory(w)
	r := memoryReceipt(w, "create", 0)
	s.memoryMutations[memoryMutationKey(m.WorkspaceID, m.Owner, key)] = memoryMutationRecord{fp, r}
	return r, false, nil
}
func (s *MemoryStore) CorrectSylonMemory(_ context.Context, ws, id string, o ExecutionSubject, in SylonMemoryInput, fp string) (SylonMemoryReceipt, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok, e := s.memoryMutation(ws, o, in.IdempotencyKey, fp); ok || e != nil {
		return r, ok, e
	}
	w, e := s.memoryWork(ws, id, o)
	if e != nil {
		return SylonMemoryReceipt{}, false, e
	}
	if !in.ExpiresAt.After(time.Now()) || w.Memory.Revision != in.Revision || w.State != "active" || !w.Memory.ExpiresAt.After(time.Now()) || !s.memoryDepsCurrent(ws, o, w.Dependencies) {
		return SylonMemoryReceipt{}, false, ErrMemoryRevision
	}
	previous := w.Memory.Revision
	w.Memory.Revision++
	w.Memory.Title = in.Title
	w.Memory.Content = in.Content
	w.Memory.ExpiresAt = in.ExpiresAt
	w.Memory.UpdatedAt = time.Now().UTC()
	w.Memory.PurgePending = true
	s.memories[id] = w
	s.memoryPurges = append(s.memoryPurges, memoryPurge{ws, id, previous})
	r := memoryReceipt(w, "correct", previous)
	s.memoryMutations[memoryMutationKey(ws, o, in.IdempotencyKey)] = memoryMutationRecord{fp, r}
	return r, false, nil
}
func (s *MemoryStore) DeleteSylonMemory(_ context.Context, ws, id string, o ExecutionSubject, in SylonMemoryDeleteInput, fp string) (SylonMemoryReceipt, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok, e := s.memoryMutation(ws, o, in.IdempotencyKey, fp); ok || e != nil {
		return r, ok, e
	}
	w, e := s.memoryWork(ws, id, o)
	if e != nil {
		return SylonMemoryReceipt{}, false, e
	}
	if w.Memory.Revision != in.Revision || w.State == "deleted" {
		return SylonMemoryReceipt{}, false, ErrMemoryRevision
	}
	previous := w.Memory.Revision
	w.Memory.Revision++
	w.State = "deleted"
	w.Memory.Title = ""
	w.Memory.Content = ""
	w.Memory.UpdatedAt = time.Now().UTC()
	w.Memory.PurgePending = true
	s.memories[id] = w
	s.memoryPurges = append(s.memoryPurges, memoryPurge{ws, id, previous})
	r := memoryReceipt(w, "delete", previous)
	s.memoryMutations[memoryMutationKey(ws, o, in.IdempotencyKey)] = memoryMutationRecord{fp, r}
	return r, false, nil
}
func (s *MemoryStore) invalidateConversationMemories(ws, conversation string) {
	for id, w := range s.memories {
		if w.Memory.WorkspaceID != ws || w.State != "active" {
			continue
		}
		j, ok := s.sylonJobs[w.Memory.Source.JobID]
		if ok && j.Work.Job.ConversationID == conversation {
			s.memoryPurges = append(s.memoryPurges, memoryPurge{ws, id, w.Memory.Revision})
			w.State = "deleted"
			w.Memory.Revision++
			w.Memory.Title = ""
			w.Memory.Content = ""
			w.Memory.PurgePending = true
			w.Memory.UpdatedAt = time.Now().UTC()
			s.memories[id] = w
		}
	}
}
func redactMemoryJob(w *SylonWork) {
	w.Job.MemoryInvalidated = true
	w.Job.State = "failed"
	w.Job.Message = "A memory dependency was corrected, forgotten or expired; stored derived output was redacted."
	w.Job.CancelRequested = true
	w.Job.Result = nil
	w.Job.Citations = []SylonCitation{}
	w.Context = SylonPreparedContext{}
	w.Job.Revision++
	w.Job.UpdatedAt = time.Now().UTC()
}
func (s *MemoryStore) PurgeSylonMemories(_ context.Context, limit int) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit < 1 || limit > 100 {
		limit = 100
	}
	now := time.Now()
	expired := 0
	for id, w := range s.memories {
		if expired < limit && w.State == "active" && !w.Memory.ExpiresAt.After(now) {
			expired++
			w.State = "expired"
			w.Memory.Title = ""
			w.Memory.Content = ""
			w.Memory.PurgePending = true
			s.memories[id] = w
			s.memoryPurges = append(s.memoryPurges, memoryPurge{w.Memory.WorkspaceID, id, w.Memory.Revision})
		}
	}
	if len(s.memoryPurges) == 0 {
		return false, nil
	}
	p := s.memoryPurges[0]
	remaining := limit
	for _, j := range s.sylonJobs {
		if remaining > 0 && j.Work.Job.WorkspaceID == p.WorkspaceID && memoryContains(j.Work.Context.Dependencies, p.MemoryID, p.Through) {
			redactMemoryJob(&j.Work)
			j.Fence++
			j.Until = time.Time{}
			remaining--
		}
	}
	if remaining == 0 {
		return true, nil
	}
	for id, w := range s.memories {
		if remaining > 0 && w.State == "active" && w.Memory.WorkspaceID == p.WorkspaceID && memoryContains(w.Dependencies, p.MemoryID, p.Through) {
			remaining--
			s.memoryPurges = append(s.memoryPurges, memoryPurge{p.WorkspaceID, id, w.Memory.Revision})
			w.State = "deleted"
			w.Memory.Revision++
			w.Memory.Title = ""
			w.Memory.Content = ""
			w.Memory.PurgePending = true
			s.memories[id] = w
		}
	}
	if remaining == 0 {
		return true, nil
	}
	s.memoryPurges = s.memoryPurges[1:]
	pending := false
	for _, other := range s.memoryPurges {
		if other.MemoryID == p.MemoryID {
			pending = true
		}
	}
	w, ok := s.memories[p.MemoryID]
	if ok {
		w.Memory.PurgePending = pending
		s.memories[p.MemoryID] = w
	}
	return true, nil
}
