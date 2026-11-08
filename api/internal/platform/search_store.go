package platform

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strings"
	"time"
)

type SearchLease struct {
	ID, Owner string
	Fence     int64
}
type SearchWork struct {
	Job        SearchJob
	PolicyHash string
}
type SearchStore interface {
	SearchChanges(context.Context, SearchBinding) ([]SearchNotebookSelection, error)
	CreateSearchJob(context.Context, SearchJob, string, string, string, int) (SearchJob, bool, error)
	SearchJob(context.Context, string, string, ExecutionSubject) (SearchJob, error)
	AcquireSearchJob(context.Context, string, time.Duration) (SearchWork, SearchLease, error)
	MarkSearchEmbedding(context.Context, SearchLease) error
	FinishSearchJob(context.Context, SearchLease, string, string, *SearchDocument, int) error
	Search(context.Context, SearchScope, SearchRequest, []float32) ([]SearchHit, error)
	FilterCurrentSearchHits(context.Context, string, []SearchHit) ([]SearchHit, error)
	SearchVectorAvailable(context.Context) (bool, error)
}
type memorySearchJob struct {
	Work                    SearchWork
	Key, Fingerprint, Owner string
	Fence                   int64
	Until                   time.Time
}

func cloneSearchJob(job SearchJob) SearchJob {
	data, _ := json.Marshal(job)
	var out SearchJob
	json.Unmarshal(data, &out)
	return out
}
func cloneSearchDocument(doc SearchDocument) SearchDocument {
	data, _ := json.Marshal(doc)
	var out SearchDocument
	json.Unmarshal(data, &out)
	return out
}
func searchDocumentKey(doc SearchDocument) string {
	return doc.WorkspaceID + "/" + doc.BindingID + "/" + doc.ID
}
func (s *MemoryStore) SearchChanges(_ context.Context, b SearchBinding) ([]SearchNotebookSelection, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []SearchNotebookSelection{}
	admitted := map[string]bool{}
	for _, doc := range s.searchDocuments {
		if doc.WorkspaceID == b.WorkspaceID && doc.BindingID == b.ID && doc.PolicyHash == b.Hash() && doc.Notebook != nil {
			admitted[doc.Notebook.ID] = true
		}
	}
	for _, n := range s.notebooks {
		if n.WorkspaceID != b.WorkspaceID {
			continue
		}
		attempted := false
		for _, j := range s.searchJobs {
			if j.Work.Job.WorkspaceID == b.WorkspaceID && j.Work.Job.BindingID == b.ID && j.Work.PolicyHash == b.Hash() && j.Work.Job.Notebook != nil && j.Work.Job.Notebook.ID == n.ID {
				admitted[n.ID] = true
				if j.Work.Job.Notebook.Revision == n.Revision {
					attempted = true
					break
				}
			}
		}
		if !attempted {
			out = append(out, SearchNotebookSelection{n.ID, n.Revision})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if admitted[out[i].ID] != admitted[out[j].ID] {
			return admitted[out[i].ID]
		}
		return out[i].ID < out[j].ID
	})
	if len(out) > 1000 {
		out = out[:1000]
	}
	return out, nil
}

func searchJobKey(j SearchJob, key string) string {
	data, _ := json.Marshal([]string{j.WorkspaceID, j.SubmittedBy.Issuer, j.SubmittedBy.Subject, key})
	return string(data)
}

func (s *MemoryStore) CreateSearchJob(_ context.Context, j SearchJob, hash, key, fingerprint string, budget int) (SearchJob, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.workspaces[j.WorkspaceID]; !ok {
		return SearchJob{}, false, ErrNotFound
	}
	k := searchJobKey(j, key)
	if id, ok := s.searchKeys[k]; ok {
		old := s.searchJobs[id]
		if old.Fingerprint != fingerprint {
			return SearchJob{}, false, ErrConflict
		}
		return cloneSearchJob(old.Work.Job), true, nil
	}
	if j.Notebook != nil {
		n, ok := s.notebooks[j.Notebook.ID]
		if !ok || n.WorkspaceID != j.WorkspaceID {
			return SearchJob{}, false, ErrNotFound
		}
		if n.Revision != j.Notebook.Revision {
			return SearchJob{}, false, ErrSearchRevision
		}
	}
	reserved := map[string]bool{}
	for _, doc := range s.searchDocuments {
		if doc.WorkspaceID == j.WorkspaceID && doc.BindingID == j.BindingID && doc.PolicyHash == hash && (doc.ExpiresAt == nil || doc.ExpiresAt.After(time.Now())) {
			reserved[doc.ID] = true
		}
	}
	for _, old := range s.searchJobs {
		if old.Work.Job.WorkspaceID == j.WorkspaceID && old.Work.Job.BindingID == j.BindingID && old.Work.PolicyHash == hash && !old.Work.Job.Terminal() {
			reserved[searchDocumentID(old.Work.Job)] = true
		}
	}
	reserved[searchDocumentID(j)] = true
	if budget < 1 || budget > 1000 || len(reserved) > budget {
		return SearchJob{}, false, ErrSearchCapacity
	}
	count := 0
	for _, old := range s.searchJobs {
		if old.Work.Job.WorkspaceID == j.WorkspaceID && !old.Work.Job.Terminal() {
			count++
		}
	}
	if count >= MaxSearchJobs {
		return SearchJob{}, false, ErrSearchCapacity
	}
	j = cloneSearchJob(j)
	s.searchJobs[j.ID] = &memorySearchJob{Work: SearchWork{j, hash}, Key: k, Fingerprint: fingerprint}
	s.searchKeys[k] = j.ID
	return cloneSearchJob(j), false, nil
}
func (s *MemoryStore) SearchJob(_ context.Context, ws, id string, owner ExecutionSubject) (SearchJob, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.searchJobs[id]
	if !ok || j.Work.Job.WorkspaceID != ws || j.Work.Job.SubmittedBy != owner {
		return SearchJob{}, ErrNotFound
	}
	return cloneSearchJob(j.Work.Job), nil
}
func (s *MemoryStore) AcquireSearchJob(_ context.Context, owner string, ttl time.Duration) (SearchWork, SearchLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var next *memorySearchJob
	now := time.Now()
	for _, j := range s.searchJobs {
		if !j.Work.Job.Terminal() && !j.Until.After(now) && (next == nil || j.Work.Job.CreatedAt.Before(next.Work.Job.CreatedAt)) {
			next = j
		}
	}
	if next == nil {
		return SearchWork{}, SearchLease{}, ErrNotFound
	}
	next.Owner = owner
	next.Fence++
	next.Until = now.Add(ttl)
	next.Work.Job.State = "indexing"
	next.Work.Job.UpdatedAt = now.UTC()
	next.Work.Job.Revision++
	return SearchWork{cloneSearchJob(next.Work.Job), next.Work.PolicyHash}, SearchLease{next.Work.Job.ID, owner, next.Fence}, nil
}
func (s *MemoryStore) searchLeased(l SearchLease) (*memorySearchJob, error) {
	j, ok := s.searchJobs[l.ID]
	if !ok {
		return nil, ErrNotFound
	}
	if j.Owner != l.Owner || j.Fence != l.Fence || !j.Until.After(time.Now()) {
		return nil, ErrSearchLease
	}
	return j, nil
}
func (s *MemoryStore) MarkSearchEmbedding(_ context.Context, l SearchLease) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.searchLeased(l)
	if err != nil {
		return err
	}
	if j.Work.Job.EmbeddingStarted {
		return ErrConflict
	}
	j.Work.Job.EmbeddingStarted = true
	j.Work.Job.Revision++
	j.Work.Job.UpdatedAt = time.Now().UTC()
	return nil
}
func validSearchDocument(doc SearchDocument) bool {
	if len(doc.Chunks) < 1 || len(doc.Chunks) > MaxSearchChunks || doc.ID == "" || doc.PolicyHash == "" || (doc.Notebook == nil) == (doc.Catalog == nil) {
		return false
	}
	for _, c := range doc.Chunks {
		if len(c.Text) == 0 || len(c.Text) > SearchChunkBytes {
			return false
		}
		if len(c.Vector) > 0 && !searchVectorValid(c.Vector, len(c.Vector)) {
			return false
		}
	}
	return true
}
func (s *MemoryStore) FinishSearchJob(_ context.Context, l SearchLease, state, message string, doc *SearchDocument, maxDocuments int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.searchLeased(l)
	if err != nil {
		return err
	}
	if !(SearchJob{State: state}).Terminal() {
		return errors.New("invalid final index state")
	}
	if doc != nil {
		if state != "succeeded" || !validSearchDocument(*doc) || doc.WorkspaceID != j.Work.Job.WorkspaceID || doc.BindingID != j.Work.Job.BindingID || doc.PolicyHash != j.Work.PolicyHash {
			return errors.New("invalid index publication")
		}
		if doc.Notebook != nil {
			n, ok := s.notebooks[doc.Notebook.ID]
			if !ok || n.WorkspaceID != doc.WorkspaceID || n.Revision != doc.Notebook.Revision {
				state = "superseded"
				message = "Source changed before index publication."
				doc = nil
			}
		}
	}
	if doc != nil {
		count := 0
		key := searchDocumentKey(*doc)
		for k, old := range s.searchDocuments {
			if old.WorkspaceID == doc.WorkspaceID && old.BindingID == doc.BindingID {
				if old.PolicyHash != doc.PolicyHash || old.ExpiresAt != nil && !old.ExpiresAt.After(time.Now()) {
					delete(s.searchDocuments, k)
					continue
				}
				if k != key {
					count++
				}
			}
		}
		if count >= maxDocuments {
			state = "failed"
			message = "Index document budget reached; no document was published."
			doc = nil
		} else if old, ok := s.searchDocuments[key]; ok && old.IndexedAt.After(doc.IndexedAt) {
			state = "superseded"
			message = "A newer index snapshot already exists."
			doc = nil
		} else {
			s.searchDocuments[key] = cloneSearchDocument(*doc)
		}
	}
	j.Work.Job.State = state
	j.Work.Job.Message = message
	j.Work.Job.UpdatedAt = time.Now().UTC()
	j.Work.Job.Revision++
	if doc != nil {
		j.Work.Job.DocumentID = doc.ID
	}
	j.Until = time.Time{}
	return nil
}
func (s *MemoryStore) SearchVectorAvailable(context.Context) (bool, error) { return false, nil }

// Memory mode uses lexical retrieval only. Production hybrid scoring is PostgreSQL/pgvector.
func (s *MemoryStore) Search(_ context.Context, scope SearchScope, in SearchRequest, vector []float32) ([]SearchHit, error) {
	if in.Mode == "hybrid" {
		return nil, errors.New("hybrid search requires PostgreSQL with pgvector")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	type candidate struct {
		doc   SearchDocument
		text  string
		score float64
	}
	candidates := []candidate{}
	terms := strings.Fields(strings.ToLower(in.Query))
	for _, doc := range s.searchDocuments {
		if !scope.Allows(doc) {
			continue
		}
		if doc.Notebook != nil {
			n, ok := s.notebooks[doc.Notebook.ID]
			if !ok || n.WorkspaceID != scope.WorkspaceID || n.Revision != doc.Notebook.Revision {
				continue
			}
		}
		for _, chunk := range doc.Chunks {
			score := 0.0
			for _, term := range terms {
				if strings.Contains(strings.ToLower(chunk.Text), term) {
					score++
				}
			}
			if score > 0 {
				candidates = append(candidates, candidate{doc, chunk.Text, score})
			}
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].score != candidates[j].score {
			return candidates[i].score > candidates[j].score
		}
		return candidates[i].doc.ID < candidates[j].doc.ID
	})
	out := []SearchHit{}
	seen := map[string]bool{}
	for _, c := range candidates {
		if seen[c.doc.ID] {
			continue
		}
		seen[c.doc.ID] = true
		out = append(out, searchHit(c.doc, c.text, c.score))
		if len(out) == in.Limit {
			break
		}
	}
	return out, nil
}
func searchHit(doc SearchDocument, text string, score float64) SearchHit {
	if math.IsNaN(score) || math.IsInf(score, 0) {
		score = 0
	}
	return SearchHit{DocumentID: doc.ID, Kind: doc.Kind, Title: doc.Title, Snippet: searchSnippet(text), Score: score, Notebook: doc.Notebook, Catalog: doc.Catalog, IndexedAt: doc.IndexedAt, ExpiresAt: doc.ExpiresAt}
}

func (s *MemoryStore) FilterCurrentSearchHits(_ context.Context, ws string, hits []SearchHit) ([]SearchHit, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []SearchHit{}
	for _, hit := range hits {
		if hit.Notebook != nil {
			n, ok := s.notebooks[hit.Notebook.ID]
			if !ok || n.WorkspaceID != ws || n.Revision != hit.Notebook.Revision {
				continue
			}
		} else if hit.ExpiresAt == nil || !hit.ExpiresAt.After(time.Now()) {
			continue
		}
		out = append(out, hit)
	}
	return out, nil
}
