package platform

import (
	"context"
	"sort"
	"sync"
	"time"
)

type Store interface {
	PublicationStore
	IngestionOutcome(context.Context, string, string, ExecutionSubject) (IngestionOutcome, error)
	SylonMemoryStore
	SylonStore
	SearchStore
	CatalogStore
	QueryStore
	Ready(context.Context) error
	Mode() string
	Workspaces(context.Context) ([]Workspace, error)
	CreateWorkspace(context.Context, string, string) (Workspace, error)
	Workspace(context.Context, string) (Workspace, error)
	Notebooks(context.Context, string) ([]Notebook, error)
	Notebook(context.Context, string, string) (Notebook, error)
	SaveNotebook(context.Context, string, string, NotebookInput) (Notebook, error)
	DeleteNotebook(context.Context, string, string, int) error
	Revisions(context.Context, string, string) ([]Notebook, error)
	IngestionPipelines(context.Context, string) ([]IngestionPipeline, error)
	IngestionPipeline(context.Context, string, string) (IngestionPipeline, error)
	SaveIngestionPipeline(context.Context, string, string, IngestionPipelineInput) (IngestionPipeline, error)
	DeleteIngestionPipeline(context.Context, string, string, int) error
	CreateRun(context.Context, Run, string, string) (Run, bool, error)
	Runs(context.Context, string, ExecutionSubject) ([]Run, error)
	Run(context.Context, string, string, ExecutionSubject) (Run, error)
	RunSource(context.Context, string, string, ExecutionSubject) (RunSource, error)
	CancelRun(context.Context, string, string, ExecutionSubject) (Run, error)
	AcquireRun(context.Context, string, time.Duration) (Run, RunLease, error)
	AdvanceRun(context.Context, RunLease, RunUpdate) (Run, error)
}
type MemoryStore struct {
	publications            map[string]*memoryPublication
	publicationKeys         map[string]string
	publicationReservations map[string]publicationReservation
	publishedTables         map[string]PublishedTableRecord
	ingestionOutcomes       map[string]IngestionOutcome
	memories                map[string]SylonMemoryWork
	memoryMutations         map[string]memoryMutationRecord
	memoryPurges            []memoryPurge
	sylonConversations      map[string]SylonConversation
	sylonJobs               map[string]*memorySylonJob
	searchJobs              map[string]*memorySearchJob
	searchKeys              map[string]string
	searchDocuments         map[string]SearchDocument
	catalogObservations     map[string]CatalogObservation
	queries                 map[string]*memoryQuery
	queryKeys               map[string]string
	mu                      sync.RWMutex
	workspaces              map[string]Workspace
	notebooks               map[string]Notebook
	history                 map[string][]Notebook
	executions              map[string]*memoryExecution
	runKeys                 map[string]string
	ingestion               map[string]IngestionPipeline
	ingestionReservations   map[string]ingestionReservation
}

func NewMemoryStore() *MemoryStore {
	now := time.Now().UTC()
	return &MemoryStore{publications: map[string]*memoryPublication{}, publicationKeys: map[string]string{}, publicationReservations: map[string]publicationReservation{}, publishedTables: map[string]PublishedTableRecord{}, ingestionOutcomes: map[string]IngestionOutcome{}, memories: map[string]SylonMemoryWork{}, memoryMutations: map[string]memoryMutationRecord{}, sylonConversations: map[string]SylonConversation{}, sylonJobs: map[string]*memorySylonJob{}, searchJobs: map[string]*memorySearchJob{}, searchKeys: map[string]string{}, searchDocuments: map[string]SearchDocument{}, catalogObservations: map[string]CatalogObservation{}, queries: map[string]*memoryQuery{}, queryKeys: map[string]string{}, workspaces: map[string]Workspace{"default": {Resource: Resource{ID: "default", CreatedAt: now, UpdatedAt: now, Revision: 1}, Name: "Development workspace", Slug: "default"}}, notebooks: map[string]Notebook{}, history: map[string][]Notebook{}, executions: map[string]*memoryExecution{}, runKeys: map[string]string{}, ingestion: map[string]IngestionPipeline{}, ingestionReservations: map[string]ingestionReservation{}}
}
func (*MemoryStore) Ready(context.Context) error { return nil }
func (*MemoryStore) Mode() string                { return "memory" }
func (s *MemoryStore) Workspaces(context.Context) ([]Workspace, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []Workspace{}
	for _, w := range s.workspaces {
		out = append(out, w)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}
func (s *MemoryStore) Workspace(_ context.Context, id string) (Workspace, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	w, ok := s.workspaces[id]
	if !ok {
		return w, ErrNotFound
	}
	return w, nil
}
func (s *MemoryStore) CreateWorkspace(_ context.Context, name, slug string) (Workspace, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, w := range s.workspaces {
		if w.Slug == slug {
			return Workspace{}, ErrConflict
		}
	}
	w := Workspace{Resource: newResource("ws-"), Name: name, Slug: slug}
	s.workspaces[w.ID] = w
	return w, nil
}
func (s *MemoryStore) Notebooks(_ context.Context, ws string) ([]Notebook, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []Notebook{}
	for _, n := range s.notebooks {
		if n.WorkspaceID == ws {
			out = append(out, cloneNotebook(n))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}
func (s *MemoryStore) Notebook(_ context.Context, ws, id string) (Notebook, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n, ok := s.notebooks[id]
	if !ok || n.WorkspaceID != ws {
		return Notebook{}, ErrNotFound
	}
	return cloneNotebook(n), nil
}
func (s *MemoryStore) SaveNotebook(_ context.Context, ws, id string, in NotebookInput) (Notebook, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.workspaces[ws]; !ok {
		return Notebook{}, ErrNotFound
	}
	n := Notebook{Resource: newResource("nb-"), WorkspaceID: ws}
	if id != "" {
		old, ok := s.notebooks[id]
		if !ok || old.WorkspaceID != ws {
			return Notebook{}, ErrNotFound
		}
		if old.Revision != in.Revision {
			return Notebook{}, ErrConflict
		}
		n = old
		n.Revision++
		n.UpdatedAt = time.Now().UTC()
	}
	n.Name = in.Name
	n.Kind = in.Kind
	n.Cells = append([]Cell{}, in.Cells...)
	for key, doc := range s.searchDocuments {
		if doc.Notebook != nil && doc.Notebook.ID == n.ID {
			delete(s.searchDocuments, key)
		}
	}
	s.notebooks[n.ID] = n
	s.history[n.ID] = append(s.history[n.ID], cloneNotebook(n))
	return cloneNotebook(n), nil
}
func (s *MemoryStore) DeleteNotebook(_ context.Context, ws, id string, rev int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.notebooks[id]
	if !ok || n.WorkspaceID != ws {
		return ErrNotFound
	}
	if n.Revision != rev {
		return ErrConflict
	}
	for key, doc := range s.searchDocuments {
		if doc.Notebook != nil && doc.Notebook.ID == id {
			delete(s.searchDocuments, key)
		}
	}
	for key, job := range s.searchJobs {
		if job.Work.Job.Notebook != nil && job.Work.Job.Notebook.ID == id {
			delete(s.searchKeys, job.Key)
			delete(s.searchJobs, key)
		}
	}
	delete(s.notebooks, id)
	delete(s.history, id)
	return nil
}
func (s *MemoryStore) Revisions(_ context.Context, ws, id string) ([]Notebook, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n, ok := s.notebooks[id]
	if !ok || n.WorkspaceID != ws {
		return nil, ErrNotFound
	}
	out := []Notebook{}
	for _, v := range s.history[id] {
		out = append(out, cloneNotebook(v))
	}
	return out, nil
}
