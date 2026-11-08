package platform

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"
)

type SylonStore interface {
	CreateSylonConversation(context.Context, SylonConversation) (SylonConversation, error)
	SylonConversations(context.Context, string, ExecutionSubject) ([]SylonConversation, error)
	SylonConversation(context.Context, string, string, ExecutionSubject) (SylonConversation, error)
	DeleteSylonConversation(context.Context, string, string, ExecutionSubject) error
	SylonHistory(context.Context, string, string, ExecutionSubject) ([]SylonWork, error)
	CreateSylonJob(context.Context, SylonWork, string, string) (SylonJob, bool, error)
	SylonWork(context.Context, string, string, ExecutionSubject) (SylonWork, error)
	CancelSylonJob(context.Context, string, string, ExecutionSubject) (SylonJob, error)
	AcquireSylonJob(context.Context, string, time.Duration) (SylonWork, SylonLease, error)
	SaveSylonContext(context.Context, SylonLease, SylonPreparedContext) error
	MarkSylonDispatch(context.Context, SylonLease, string) error
	FinishSylonJob(context.Context, SylonLease, string, string, *SylonPreparedContext, *SylonJob) error
	ApplySylonCode(context.Context, string, string, ExecutionSubject, SylonApplyInput) (SylonApplyReceipt, error)
}
type memorySylonJob struct {
	Work                    SylonWork
	Key, Fingerprint, Owner string
	Fence                   int64
	Until                   time.Time
}

func cloneSylonWork(work SylonWork) SylonWork {
	data, _ := json.Marshal(work)
	var out SylonWork
	json.Unmarshal(data, &out)
	return out
}
func (s *MemoryStore) CreateSylonConversation(_ context.Context, c SylonConversation) (SylonConversation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.workspaces[c.WorkspaceID]; !ok {
		return SylonConversation{}, ErrNotFound
	}
	count := 0
	for _, old := range s.sylonConversations {
		if old.WorkspaceID == c.WorkspaceID && old.Owner == c.Owner {
			count++
		}
	}
	if count >= 100 {
		return SylonConversation{}, ErrSylonCapacity
	}
	s.sylonConversations[c.ID] = c
	return c, nil
}
func (s *MemoryStore) SylonConversations(_ context.Context, ws string, owner ExecutionSubject) ([]SylonConversation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []SylonConversation{}
	for _, c := range s.sylonConversations {
		if c.WorkspaceID == ws && c.Owner == owner {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}
func (s *MemoryStore) sylonConversation(ws, id string, owner ExecutionSubject) (SylonConversation, error) {
	c, ok := s.sylonConversations[id]
	if !ok || c.WorkspaceID != ws || c.Owner != owner {
		return SylonConversation{}, ErrNotFound
	}
	return c, nil
}
func (s *MemoryStore) SylonConversation(_ context.Context, ws, id string, owner ExecutionSubject) (SylonConversation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sylonConversation(ws, id, owner)
}
func (s *MemoryStore) DeleteSylonConversation(_ context.Context, ws, id string, owner ExecutionSubject) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.sylonConversation(ws, id, owner); err != nil {
		return err
	}
	s.invalidateConversationMemories(ws, id)
	delete(s.sylonConversations, id)
	for key, j := range s.sylonJobs {
		if j.Work.Job.ConversationID == id {
			delete(s.sylonJobs, key)
		}
	}
	return nil
}
func (s *MemoryStore) SylonHistory(_ context.Context, ws, id string, owner ExecutionSubject) ([]SylonWork, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, err := s.sylonConversation(ws, id, owner); err != nil {
		return nil, err
	}
	out := []SylonWork{}
	for _, j := range s.sylonJobs {
		if j.Work.Job.ConversationID == id {
			out = append(out, cloneSylonWork(j.Work))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Job.CreatedAt.Before(out[j].Job.CreatedAt) })
	return out, nil
}
func (s *MemoryStore) CreateSylonJob(_ context.Context, work SylonWork, key, fingerprint string) (SylonJob, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j := work.Job
	c, err := s.sylonConversation(j.WorkspaceID, j.ConversationID, j.SubmittedBy)
	if err != nil {
		return SylonJob{}, false, err
	}
	if c.BindingID != j.BindingID {
		return SylonJob{}, false, ErrConflict
	}
	for _, old := range s.sylonJobs {
		if old.Work.Job.ConversationID == j.ConversationID && old.Key == key {
			if old.Fingerprint != fingerprint {
				return SylonJob{}, false, ErrConflict
			}
			return cloneSylonWork(old.Work).Job, true, nil
		}
	}
	historyMessages, active := 0, 0
	for _, old := range s.sylonJobs {
		if old.Work.Job.ConversationID == j.ConversationID {
			if old.Key == key {
				if old.Fingerprint != fingerprint {
					return SylonJob{}, false, ErrConflict
				}
				return cloneSylonWork(old.Work).Job, true, nil
			}
			historyMessages++
			if old.Work.Job.Result != nil {
				historyMessages++
			}
			if !old.Work.Job.Terminal() {
				return SylonJob{}, false, ErrSylonCapacity
			}
		}
		if old.Work.Job.WorkspaceID == j.WorkspaceID && !old.Work.Job.Terminal() {
			active++
		}
	}
	if historyMessages+2 > MaxSylonHistoryMessages || active >= MaxSylonWorkspaceJobs {
		return SylonJob{}, false, ErrSylonCapacity
	}
	if j.Notebook != nil {
		n, ok := s.notebooks[j.Notebook.ID]
		if !ok || n.WorkspaceID != j.WorkspaceID {
			return SylonJob{}, false, ErrNotFound
		}
		if n.Revision != j.Notebook.Revision {
			return SylonJob{}, false, ErrSylonRevision
		}
		found := false
		for _, cell := range n.Cells {
			if cell.ID == j.Notebook.CellID && (cell.Language == "python" || cell.Language == "sql") {
				found = true
			}
		}
		if !found {
			return SylonJob{}, false, ErrSylonRevision
		}
	}
	if !s.memoryDepsCurrent(j.WorkspaceID, j.SubmittedBy, memorySelectionDependencies(j.Memories)) {
		return SylonJob{}, false, ErrMemoryAccess
	}
	work = cloneSylonWork(work)
	s.sylonJobs[j.ID] = &memorySylonJob{Work: work, Key: key, Fingerprint: fingerprint}
	c.Revision++
	c.UpdatedAt = time.Now().UTC()
	s.sylonConversations[c.ID] = c
	return work.Job, false, nil
}
func (s *MemoryStore) sylonWork(ws, id string, owner ExecutionSubject) (*memorySylonJob, error) {
	j, ok := s.sylonJobs[id]
	if !ok || j.Work.Job.WorkspaceID != ws || j.Work.Job.SubmittedBy != owner {
		return nil, ErrNotFound
	}
	return j, nil
}
func (s *MemoryStore) SylonWork(_ context.Context, ws, id string, owner ExecutionSubject) (SylonWork, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, err := s.sylonWork(ws, id, owner)
	if err != nil {
		return SylonWork{}, err
	}
	return cloneSylonWork(j.Work), nil
}
func cancelSylon(j *SylonJob) {
	if j.Terminal() {
		return
	}
	j.CancelRequested = true
	if !j.DispatchStarted {
		j.State = "cancelled"
		j.Message = "Cancelled before any provider dispatch."
	} else {
		j.State = "cancel_requested"
		j.Message = "Output will be discarded; provider execution or billing may continue."
	}
	j.Revision++
	j.UpdatedAt = time.Now().UTC()
}
func (s *MemoryStore) CancelSylonJob(_ context.Context, ws, id string, owner ExecutionSubject) (SylonJob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.sylonWork(ws, id, owner)
	if err != nil {
		return SylonJob{}, err
	}
	cancelSylon(&j.Work.Job)
	return cloneSylonWork(j.Work).Job, nil
}
func (s *MemoryStore) AcquireSylonJob(_ context.Context, owner string, ttl time.Duration) (SylonWork, SylonLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var next *memorySylonJob
	now := time.Now()
	for _, j := range s.sylonJobs {
		if !j.Work.Job.Terminal() && !j.Until.After(now) && (next == nil || j.Work.Job.CreatedAt.Before(next.Work.Job.CreatedAt)) {
			next = j
		}
	}
	if next == nil {
		return SylonWork{}, SylonLease{}, ErrNotFound
	}
	next.Owner = owner
	next.Fence++
	next.Until = now.Add(ttl)
	if next.Work.Job.State == "queued" {
		next.Work.Job.State = "preparing"
	}
	next.Work.Job.Revision++
	next.Work.Job.UpdatedAt = now.UTC()
	return cloneSylonWork(next.Work), SylonLease{next.Work.Job.ID, owner, next.Fence}, nil
}
func (s *MemoryStore) sylonLeased(l SylonLease) (*memorySylonJob, error) {
	j, ok := s.sylonJobs[l.ID]
	if !ok {
		return nil, ErrNotFound
	}
	if j.Owner != l.Owner || j.Fence != l.Fence || !j.Until.After(time.Now()) {
		return nil, ErrSylonLease
	}
	return j, nil
}
func (s *MemoryStore) SaveSylonContext(_ context.Context, l SylonLease, prepared SylonPreparedContext) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.sylonLeased(l)
	if err != nil {
		return err
	}
	if j.Work.Job.CancelRequested || j.Work.Job.Terminal() {
		return ErrSylonCancelled
	}
	if !s.memoryDepsCurrent(j.Work.Job.WorkspaceID, j.Work.Job.SubmittedBy, prepared.Dependencies) {
		return ErrMemoryAccess
	}
	j.Work.Context = prepared
	j.Work.Job.Citations = prepared.Citations
	j.Work = cloneSylonWork(j.Work)
	return nil
}
func (s *MemoryStore) MarkSylonDispatch(_ context.Context, l SylonLease, stage string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.sylonLeased(l)
	if err != nil {
		return err
	}
	if j.Work.Job.CancelRequested || j.Work.Job.Terminal() {
		return ErrSylonCancelled
	}
	if stage != "preparing" && stage != "generating" {
		return errors.New("invalid provider stage")
	}
	if !s.memoryDepsCurrent(j.Work.Job.WorkspaceID, j.Work.Job.SubmittedBy, j.Work.Context.Dependencies) {
		return ErrMemoryAccess
	}
	j.Work.Job.DispatchStarted = true
	j.Work.Job.State = stage
	j.Work.Job.Revision++
	j.Work.Job.UpdatedAt = time.Now().UTC()
	return nil
}
func finishSylon(work *SylonWork, state, message string, prepared *SylonPreparedContext, resultJob *SylonJob) error {
	if !(SylonJob{State: state}).Terminal() {
		return errors.New("invalid final Sylon state")
	}
	if work.Job.CancelRequested {
		state = "cancelled"
		message = "Output discarded. Provider execution or billing may have continued."
		resultJob = nil
	}
	work.Job.State = state
	work.Job.Message = message
	work.Job.Revision++
	work.Job.UpdatedAt = time.Now().UTC()
	if prepared != nil {
		work.Context = *prepared
		work.Job.Citations = prepared.Citations
	}
	if resultJob != nil && state == "succeeded" {
		work.Job.Result = resultJob.Result
	} else {
		work.Job.Result = nil
	}
	return nil
}
func (s *MemoryStore) FinishSylonJob(_ context.Context, l SylonLease, state, message string, prepared *SylonPreparedContext, resultJob *SylonJob) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.sylonLeased(l)
	if err != nil {
		return err
	}
	deps := append([]SylonDependency{}, j.Work.Context.Dependencies...)
	if prepared != nil {
		deps = append(deps, prepared.Dependencies...)
	}
	if !s.memoryDepsCurrent(j.Work.Job.WorkspaceID, j.Work.Job.SubmittedBy, deps) {
		redactMemoryJob(&j.Work)
		j.Until = time.Time{}
		return nil
	}
	if state == "succeeded" && !j.Work.Job.CancelRequested {
		if selected := j.Work.Job.Notebook; selected != nil {
			n, ok := s.notebooks[selected.ID]
			if !ok || n.WorkspaceID != j.Work.Job.WorkspaceID || n.Revision != selected.Revision {
				state, message, resultJob = "failed", "The selected saved notebook changed before publication; output was discarded.", nil
			}
		}
	}
	if err = finishSylon(&j.Work, state, message, prepared, resultJob); err != nil {
		return err
	}
	j.Work = cloneSylonWork(j.Work)
	j.Until = time.Time{}
	return nil
}
func sylonApplyNotebook(j SylonJob, n Notebook, in SylonApplyInput) (Notebook, error) {
	if j.State != "succeeded" || j.CancelRequested || j.Result == nil || j.Result.ProposedCode == nil || j.Notebook == nil {
		return Notebook{}, ErrConflict
	}
	if in.NotebookID != j.Notebook.ID || in.Revision != j.Notebook.Revision || in.CellID != j.Notebook.CellID || n.ID != in.NotebookID || n.WorkspaceID != j.WorkspaceID || n.Revision != in.Revision {
		return Notebook{}, ErrSylonRevision
	}
	n = cloneNotebook(n)
	found := false
	for i, cell := range n.Cells {
		if cell.ID == in.CellID {
			if cell.Language != j.Result.ProposedCode.Language {
				return Notebook{}, ErrConflict
			}
			n.Cells[i].Source = j.Result.ProposedCode.Source
			found = true
		}
	}
	if !found {
		return Notebook{}, ErrNotFound
	}
	input := NotebookInput{Name: n.Name, Kind: n.Kind, Cells: n.Cells, Revision: n.Revision}
	if err := input.Validate(true); err != nil {
		return Notebook{}, err
	}
	n.Revision++
	n.UpdatedAt = time.Now().UTC()
	return n, nil
}
func (s *MemoryStore) ApplySylonCode(_ context.Context, ws, id string, owner ExecutionSubject, in SylonApplyInput) (SylonApplyReceipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.sylonWork(ws, id, owner)
	if err != nil {
		return SylonApplyReceipt{}, err
	}
	if !s.memoryDepsCurrent(ws, owner, j.Work.Context.Dependencies) || j.Work.Job.MemoryInvalidated {
		return SylonApplyReceipt{}, ErrMemoryAccess
	}
	if old := j.Work.Job.Applied; old != nil {
		if old.NotebookID != in.NotebookID || old.CellID != in.CellID || old.PreviousRevision != in.Revision {
			return SylonApplyReceipt{}, ErrConflict
		}
		return *old, nil
	}
	n, ok := s.notebooks[in.NotebookID]
	if !ok || n.WorkspaceID != ws {
		return SylonApplyReceipt{}, ErrNotFound
	}
	next, err := sylonApplyNotebook(j.Work.Job, n, in)
	if err != nil {
		return SylonApplyReceipt{}, err
	}
	s.notebooks[next.ID] = next
	s.history[next.ID] = append(s.history[next.ID], cloneNotebook(next))
	for key, doc := range s.searchDocuments {
		if doc.Notebook != nil && doc.Notebook.ID == next.ID {
			delete(s.searchDocuments, key)
		}
	}
	receipt := SylonApplyReceipt{JobID: id, WorkspaceID: ws, NotebookID: next.ID, CellID: in.CellID, PreviousRevision: in.Revision, Revision: next.Revision, AppliedAt: next.UpdatedAt}
	j.Work.Job.Applied = &receipt
	j.Work.Job.Revision++
	j.Work.Job.UpdatedAt = next.UpdatedAt
	return receipt, nil
}
