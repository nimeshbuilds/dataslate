package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"dataslate.local/api/internal/llm"
)

func contextSylonBinding() SylonBinding {
	return SylonBinding{ID: "sylon-test", WorkspaceID: "default", Name: "Sylon", PolicyVersion: "v1", Provider: llm.Config{Protocol: "responses", Endpoint: "https://api.example.com/responses", Model: "fixture-model", MaxOutputTokens: 1024, Auth: llm.Auth{BearerTokenFile: "/run/secrets/sylon/token"}}, DataTransferAccepted: true, AllowedSubjects: []ExecutionSubject{subjectFor(executionPrincipal())}, Context: SylonContextPolicy{NotebookCellSource: true, CatalogBindingIDs: []string{"polaris-lake"}, SearchBindingIDs: []string{"workspace-search"}}, MaxContextBytes: 98304}
}
func contextSylonWork(b SylonBinding, id string) SylonWork {
	return SylonWork{PolicyHash: b.Hash(), Job: SylonJob{Resource: Resource{ID: id, Revision: 1}, WorkspaceID: b.WorkspaceID, BindingID: b.ID, ConversationID: "conversation-one", SubmittedBy: b.AllowedSubjects[0], Prompt: "Explain the authorized context.", State: "queued", Catalogs: []SearchCatalogSelection{}}}
}
func contextServer(b SylonBinding, store Store) *Server {
	return &Server{Config: Config{AuthMode: "oidc", SylonBindings: []SylonBinding{b}}, Store: store}
}
func contextNotebook(t *testing.T, store Store) Notebook {
	t.Helper()
	n, err := store.SaveNotebook(context.Background(), "default", "", NotebookInput{Name: "Finance notebook", Kind: "notebook", Cells: []Cell{{ID: "selected", Language: "python", Source: "SELECTED_CONTEXT"}, {ID: "private", Language: "python", Source: "UNSELECTED_SECRET"}}})
	if err != nil {
		t.Fatal(err)
	}
	return n
}
func TestSylonContextSelectedCellAndBounds(t *testing.T) {
	b := contextSylonBinding()
	store := NewMemoryStore()
	s := contextServer(b, store)
	n := contextNotebook(t, store)
	work := contextSylonWork(b, "syl-first")
	work.Job.Notebook = &SQLSelection{ID: n.ID, Revision: n.Revision, CellID: "selected"}
	prepared, err := s.PrepareSylonContext(context.Background(), b, work, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(prepared.Input, "SELECTED_CONTEXT") || strings.Contains(prepared.Input, "UNSELECTED_SECRET") || len(prepared.Citations) != 1 || len(prepared.Dependencies) != 1 || prepared.Dependencies[0].Kind != "notebook-cell" {
		t.Fatal("wrong selected-source projection")
	}
	if prepared.Citations[0].ID != "syl-first-ctx-1" || prepared.Citations[0].Historical || prepared.Citations[0].Notebook.CellID != "selected" {
		t.Fatal("wrong citation")
	}
	work.Job.Notebook.Revision++
	if _, err = s.PrepareSylonContext(context.Background(), b, work, nil, nil); !errors.Is(err, ErrSylonRevision) {
		t.Fatal("stale saved source accepted", err)
	}
	work.Job.Notebook.Revision = n.Revision
	b.Context.NotebookCellSource = false
	s.Config.SylonBindings = []SylonBinding{b}
	work.PolicyHash = b.Hash()
	if _, err = s.PrepareSylonContext(context.Background(), b, work, nil, nil); !errors.Is(err, ErrSylonAccess) {
		t.Fatal("cell source sent without transfer permission", err)
	}
	b.Context.NotebookCellSource = true
	b.MaxContextBytes = 1024
	s.Config.SylonBindings = []SylonBinding{b}
	work.PolicyHash = b.Hash()
	work.Job.Prompt = strings.Repeat("a", 900)
	if _, err = s.PrepareSylonContext(context.Background(), b, work, nil, nil); err == nil {
		t.Fatal("oversized full context silently truncated")
	}
}
func TestSylonContextHistoricalSourcesAreFlatAndRevocable(t *testing.T) {
	ctx := context.Background()
	b := contextSylonBinding()
	store := NewMemoryStore()
	s := contextServer(b, store)
	n := contextNotebook(t, store)
	first := contextSylonWork(b, "syl-first")
	first.Job.Notebook = &SQLSelection{ID: n.ID, Revision: 1, CellID: "selected"}
	prepared, err := s.PrepareSylonContext(ctx, b, first, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	first.Context = prepared
	first.Job.State = "succeeded"
	first.Job.Result = &llm.Result{Answer: "First answer", Citations: []string{prepared.Citations[0].ID}}
	n, err = store.SaveNotebook(ctx, "default", n.ID, NotebookInput{Name: n.Name, Kind: n.Kind, Cells: []Cell{{ID: "selected", Language: "python", Source: "NEW_CONTEXT"}}, Revision: 1})
	if err != nil {
		t.Fatal(err)
	}
	second := contextSylonWork(b, "syl-second")
	second.Job.Prompt = "Follow up"
	second.Context, err = s.PrepareSylonContext(ctx, b, second, []SylonWork{first}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Context.Citations) != 1 || !second.Context.Citations[0].Historical || !strings.Contains(second.Context.Input, "SELECTED_CONTEXT") || strings.Contains(second.Context.Input, "NEW_CONTEXT") {
		t.Fatal("historical source was relabeled as current")
	}
	if second.Context.Sources != "[]" {
		t.Fatal("history recursively stored in current sources")
	}
	second.Job.State = "failed"
	third := contextSylonWork(b, "syl-third")
	third.Context, err = s.PrepareSylonContext(ctx, b, third, []SylonWork{first, second}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(third.Context.Input, "SELECTED_CONTEXT") != 1 || !strings.Contains(third.Context.Input, `"state":"failed"`) {
		t.Fatal("history recursively duplicated or failed user prompt dropped")
	}
	if err = store.DeleteNotebook(ctx, "default", n.ID, n.Revision); err != nil {
		t.Fatal(err)
	}
	if _, err = s.PrepareSylonContext(ctx, b, third, []SylonWork{first, second}, nil); !errors.Is(err, ErrSylonAccess) {
		t.Fatal("deleted historical dependency reused", err)
	}
}
func TestSylonContextHistoryOwnershipPolicyAndMessageCount(t *testing.T) {
	b := contextSylonBinding()
	s := contextServer(b, NewMemoryStore())
	work := contextSylonWork(b, "syl-new")
	past := contextSylonWork(b, "syl-old")
	past.Job.State = "failed"
	for name, mutate := range map[string]func(*SylonWork){"owner": func(w *SylonWork) { w.Job.SubmittedBy.Subject = "other" }, "workspace": func(w *SylonWork) { w.Job.WorkspaceID = "other" }, "conversation": func(w *SylonWork) { w.Job.ConversationID = "other" }, "policy": func(w *SylonWork) { w.PolicyHash = "old" }, "active": func(w *SylonWork) { w.Job.State = "generating" }} {
		t.Run(name, func(t *testing.T) {
			p := past
			mutate(&p)
			if _, err := s.PrepareSylonContext(context.Background(), b, work, []SylonWork{p}, nil); !errors.Is(err, ErrSylonAccess) {
				t.Fatal("unauthorized history accepted", err)
			}
		})
	}
	history := []SylonWork{}
	for i := 0; i < 18; i++ {
		p := contextSylonWork(b, fmt.Sprintf("syl-past-%d", i))
		p.Job.State = "unknown"
		history = append(history, p)
	}
	if _, err := s.PrepareSylonContext(context.Background(), b, work, history, nil); err != nil {
		t.Fatal("valid failed-user-message history rejected", err)
	}
	p := contextSylonWork(b, "syl-overflow")
	p.Job.State = "cancelled"
	history = append(history, p)
	if _, err := s.PrepareSylonContext(context.Background(), b, work, history, nil); !errors.Is(err, ErrSylonCapacity) {
		t.Fatal("physical history message cap not enforced", err)
	}
}
func TestSylonContextCatalogProjectionAndMidLoadRevocation(t *testing.T) {
	b := contextSylonBinding()
	catalog := catalogBinding()
	backend := &fakeCatalogBackend{}
	s := contextServer(b, NewMemoryStore())
	s.Config.CatalogBindings = []CatalogBinding{catalog}
	s.CatalogBackend = backend
	work := contextSylonWork(b, "syl-catalog")
	work.Job.Catalogs = []SearchCatalogSelection{{BindingID: catalog.ID, Identifier: catalog.AllowedSubjects[0].Tables[0]}}
	prepared, err := s.PrepareSylonContext(context.Background(), b, work, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(prepared.Citations) != 1 || len(prepared.Dependencies) != 1 || !strings.Contains(prepared.Input, `schema`) {
		t.Fatal("schema context missing")
	}
	for _, private := range []string{"s3://", "metadataLocation", "private/", "polaris.example.com", "/run/secrets"} {
		if strings.Contains(prepared.Input, private) {
			t.Fatal("private catalog metadata leaked")
		}
	}
	backend.onLoad = func() { s.Config.CatalogBindings[0].AllowedSubjects = []CatalogSubject{} }
	if _, err = s.PrepareSylonContext(context.Background(), b, work, nil, nil); !errors.Is(err, ErrSylonAccess) {
		t.Fatal("mid-load revocation ignored", err)
	}
}

type sylonContextSearchStore struct {
	Store
	scope  SearchScope
	hits   []SearchHit
	after  func()
	vector bool
}

func (s *sylonContextSearchStore) SearchVectorAvailable(context.Context) (bool, error) {
	return s.vector, nil
}
func (s *sylonContextSearchStore) Search(_ context.Context, scope SearchScope, _ SearchRequest, _ []float32) ([]SearchHit, error) {
	s.scope = scope
	if s.after != nil {
		s.after()
	}
	return s.hits, nil
}

func TestSylonContextSearchTransferAndCatalogNarrowing(t *testing.T) {
	b := contextSylonBinding()
	b.Context.NotebookCellSource = false
	b.Context.CatalogBindingIDs = nil
	store := &sylonContextSearchStore{Store: NewMemoryStore()}
	s := contextServer(b, store)
	search := searchBinding()
	search.Sources.IncludeCellSource = true
	s.Config.SearchBindings = []SearchBinding{search}
	work := contextSylonWork(b, "syl-search")
	work.Job.Search = &SylonSearchSelection{BindingID: search.ID, Query: "finance", Mode: "lexical"}
	if _, err := s.PrepareSylonContext(context.Background(), b, work, nil, nil); !errors.Is(err, ErrSylonAccess) {
		t.Fatal("search bypassed notebook-source opt-in", err)
	}
	search.Sources.IncludeCellSource = false
	catalog := catalogBinding()
	search.Sources.Catalogs = []SearchCatalogGrant{{BindingID: catalog.ID, Tables: catalog.AllowedSubjects[0].Tables}}
	s.Config.SearchBindings = []SearchBinding{search}
	s.Config.CatalogBindings = []CatalogBinding{catalog}
	if _, err := s.PrepareSylonContext(context.Background(), b, work, nil, nil); err != nil {
		t.Fatal(err)
	}
	if len(store.scope.Catalogs) != 0 {
		t.Fatal("Sylon-unapproved catalog entered ranking scope")
	}
	n := contextNotebook(t, store)
	store.hits = []SearchHit{{DocumentID: "doc-1", Kind: "notebook", Title: n.Name, Snippet: "Finance metadata", Notebook: &SearchNotebookSelection{ID: n.ID, Revision: n.Revision}}}
	prepared, err := s.PrepareSylonContext(context.Background(), b, work, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(prepared.Citations) != 1 || len(prepared.Dependencies) != 2 || prepared.Dependencies[1].Kind != "notebook-metadata" || strings.Contains(prepared.Input, "SELECTED_CONTEXT") {
		t.Fatal("metadata search context expanded to source")
	}
}
func TestSylonContextHybridDispatchCheckpointAndRechecks(t *testing.T) {
	b := contextSylonBinding()
	store := &sylonContextSearchStore{Store: NewMemoryStore(), vector: true}
	s := contextServer(b, store)
	search := hybridSearchBinding()
	s.Config.SearchBindings = []SearchBinding{search}
	embed := &fakeSearchEmbedder{}
	s.SearchEmbedder = embed
	work := contextSylonWork(b, "syl-hybrid")
	work.Job.Search = &SylonSearchSelection{BindingID: search.ID, Query: "revenue", Mode: "hybrid"}
	if _, err := s.PrepareSylonContext(context.Background(), b, work, nil, nil); err == nil || embed.calls != 0 {
		t.Fatal("external request without durable checkpoint")
	}
	checkpoint := false
	embed.hook = func() {
		if !checkpoint {
			t.Error("provider preceded checkpoint")
		}
	}
	if _, err := s.PrepareSylonContext(context.Background(), b, work, nil, func() error { checkpoint = true; return nil }); err != nil {
		t.Fatal(err)
	}
	before := embed.calls
	if _, err := s.PrepareSylonContext(context.Background(), b, work, nil, func() error { s.Config.SearchBindings[0].PolicyVersion = "revoked"; return nil }); !errors.Is(err, ErrSylonAccess) || embed.calls != before {
		t.Fatal("checkpoint-to-dispatch policy change ignored", err)
	}
	s.Config.SearchBindings = []SearchBinding{search}
	embed.hook = func() { s.Config.SearchBindings[0].PolicyVersion = "revoked" }
	if _, err := s.PrepareSylonContext(context.Background(), b, work, nil, func() error { return nil }); !errors.Is(err, ErrSylonAccess) {
		t.Fatal("post-provider policy change ignored", err)
	}
}
func TestSylonContextEveryDependencyRemainsAuthorized(t *testing.T) {
	b := contextSylonBinding()
	store := NewMemoryStore()
	n := contextNotebook(t, store)
	s := contextServer(b, store)
	deps := []SylonDependency{{Kind: "notebook-cell", NotebookID: n.ID}}
	if err := s.AuthorizeSylonDependencies(context.Background(), b, b.AllowedSubjects[0], deps); err != nil {
		t.Fatal(err)
	}
	// Dependencies are independent of the model's chosen citation subset.
	if err := store.DeleteNotebook(context.Background(), "default", n.ID, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.AuthorizeSylonDependencies(context.Background(), b, b.AllowedSubjects[0], deps); !errors.Is(err, ErrSylonAccess) {
		t.Fatal("uncited dependency revocation ignored")
	}
	if err := s.AuthorizeSylonDependencies(context.Background(), b, b.AllowedSubjects[0], []SylonDependency{{Kind: "unknown"}}); !errors.Is(err, ErrSylonAccess) {
		t.Fatal("unknown dependency accepted")
	}
}
func TestSylonContextSelectedRevisionRecheckedAfterExternalRead(t *testing.T) {
	b := contextSylonBinding()
	store := NewMemoryStore()
	n := contextNotebook(t, store)
	s := contextServer(b, store)
	catalog := catalogBinding()
	s.Config.CatalogBindings = []CatalogBinding{catalog}
	backend := &fakeCatalogBackend{}
	s.CatalogBackend = backend
	backend.onLoad = func() {
		_, err := store.SaveNotebook(context.Background(), "default", n.ID, NotebookInput{Name: n.Name, Kind: n.Kind, Cells: n.Cells, Revision: 1})
		if err != nil {
			t.Error(err)
		}
	}
	work := contextSylonWork(b, "syl-edit")
	work.Job.Notebook = &SQLSelection{ID: n.ID, Revision: 1, CellID: "selected"}
	work.Job.Catalogs = []SearchCatalogSelection{{BindingID: catalog.ID, Identifier: catalog.AllowedSubjects[0].Tables[0]}}
	if _, err := s.PrepareSylonContext(context.Background(), b, work, nil, nil); !errors.Is(err, ErrSylonRevision) {
		t.Fatal("current selected revision became stale", err)
	}
}
func TestSylonContextSearchCurrentRevisionFilter(t *testing.T) {
	b := contextSylonBinding()
	store := &sylonContextSearchStore{Store: NewMemoryStore()}
	n := contextNotebook(t, store)
	s := contextServer(b, store)
	search := searchBinding()
	s.Config.SearchBindings = []SearchBinding{search}
	store.hits = []SearchHit{{DocumentID: "doc-old", Kind: "notebook", Title: n.Name, Snippet: "Old secret metadata", Notebook: &SearchNotebookSelection{ID: n.ID, Revision: 1}, IndexedAt: time.Now()}}
	store.after = func() {
		_, err := store.SaveNotebook(context.Background(), "default", n.ID, NotebookInput{Name: n.Name, Kind: n.Kind, Cells: n.Cells, Revision: 1})
		if err != nil {
			t.Error(err)
		}
	}
	work := contextSylonWork(b, "syl-filter")
	work.Job.Search = &SylonSearchSelection{BindingID: search.ID, Query: "old", Mode: "lexical"}
	prepared, err := s.PrepareSylonContext(context.Background(), b, work, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(prepared.Citations) != 0 || strings.Contains(prepared.Input, "Old secret") {
		t.Fatal("stale hit entered model context")
	}
	var input map[string]any
	if json.Unmarshal([]byte(prepared.Input), &input) != nil {
		t.Fatal("invalid structured provider input")
	}
}

func TestSylonContextHistoricalDependencyAndCitationLimits(t *testing.T) {
	b := contextSylonBinding()
	store := NewMemoryStore()
	n := contextNotebook(t, store)
	s := contextServer(b, store)
	work := contextSylonWork(b, "syl-new")
	past := contextSylonWork(b, "syl-past")
	past.Job.State = "failed"
	source := sylonContextSource{Citation: SylonCitation{ID: "syl-past-ctx-1", Kind: "notebook", Notebook: &SylonNotebookReference{ID: n.ID, Revision: 1, CellID: "selected"}}, Text: "old cell source"}
	encoded, _ := json.Marshal([]sylonContextSource{source})
	past.Context.Sources = string(encoded)
	if _, err := s.PrepareSylonContext(context.Background(), b, work, []SylonWork{past}, nil); !errors.Is(err, ErrSylonAccess) {
		t.Fatal("historical source without dependency accepted", err)
	}
	past.Context.Dependencies = []SylonDependency{{Kind: "notebook-metadata", NotebookID: n.ID}}
	if _, err := s.PrepareSylonContext(context.Background(), b, work, []SylonWork{past}, nil); !errors.Is(err, ErrSylonAccess) {
		t.Fatal("historical cell source downgraded to metadata", err)
	}
	past.Context.Dependencies = []SylonDependency{{Kind: "notebook-cell", NotebookID: n.ID}}
	if _, err := s.PrepareSylonContext(context.Background(), b, work, []SylonWork{past}, nil); err != nil {
		t.Fatal(err)
	}
	history := []SylonWork{}
	for i := 0; i < 3; i++ {
		p := contextSylonWork(b, fmt.Sprintf("syl-many-%d", i))
		p.Job.State = "failed"
		p.Context.Dependencies = past.Context.Dependencies
		sources := []sylonContextSource{}
		for k := 1; k <= 7; k++ {
			item := source
			item.Citation.ID = fmt.Sprintf("%s-ctx-%d", p.Job.ID, k)
			sources = append(sources, item)
		}
		data, _ := json.Marshal(sources)
		p.Context.Sources = string(data)
		history = append(history, p)
	}
	if _, err := s.PrepareSylonContext(context.Background(), b, work, history, nil); err == nil {
		t.Fatal("21 source citations silently truncated")
	}
	if err := s.AuthorizeSylonDependencies(context.Background(), b, b.AllowedSubjects[0], make([]SylonDependency, 21)); !errors.Is(err, ErrSylonAccess) {
		t.Fatal("dependency bound missing")
	}
}
