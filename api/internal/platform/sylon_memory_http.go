package platform

import (
	"context"
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

func memoryFailure(w http.ResponseWriter, e error) {
	switch {
	case errors.Is(e, ErrMemoryAccess), errors.Is(e, ErrSylonAccess):
		fail(w, 403, "memory_access_denied", "Current private memory and source grants are required.")
	case errors.Is(e, ErrMemoryRevision), errors.Is(e, ErrSylonRevision):
		fail(w, 409, "memory_revision_conflict", "The memory or source revision changed.")
	case errors.Is(e, ErrMemoryCapacity):
		fail(w, 429, "memory_capacity", "Private memory capacity or the shared provenance limit is exhausted.")
	default:
		storeFailure(w, e)
	}
}
func (s *Server) memoryBinding(ctx context.Context, ws, id string, o ExecutionSubject) (SylonBinding, error) {
	b, e := s.authorizedSylonBinding(ws, id, o)
	if e != nil {
		return b, ErrMemoryAccess
	}
	if b.Memory == nil || !b.Memory.Enabled {
		return b, ErrMemoryAccess
	}
	return b, nil
}
func (s *Server) memorySummary(ctx context.Context, p Principal, w SylonMemoryWork) SylonMemorySummary {
	m := w.Memory
	out := SylonMemorySummary{ID: m.ID, WorkspaceID: m.WorkspaceID, Revision: m.Revision, State: "restricted", ExpiresAt: m.ExpiresAt, PurgePending: m.PurgePending}
	if w.State == "deleted" {
		out.State = "deleted"
		return out
	}
	if w.State == "expired" || !m.ExpiresAt.After(time.Now()) {
		out.State = "expired"
		out.PurgePending = w.State != "expired" || m.PurgePending
		return out
	}
	b, e := s.memoryBinding(ctx, m.WorkspaceID, m.BindingID, m.Owner)
	if e == nil && memoryRoleAllowed(p, w) {
		if _, e = s.AuthorizeSylonMemory(ctx, b, m.Owner, SylonMemorySelection{ID: m.ID, Revision: m.Revision}); e == nil {
			out.State = "available"
			out.Title = m.Title
			out.BindingID = m.BindingID
		}
	}
	return out
}
func (s *Server) memoryMutationResponse(w http.ResponseWriter, r *http.Request, receipt SylonMemoryReceipt, replay bool, status int) {
	item, e := s.Store.SylonMemoryWork(r.Context(), receipt.WorkspaceID, receipt.MemoryID, subjectFor(principal(r)))
	if e != nil {
		memoryFailure(w, e)
		return
	}
	if replay {
		status = 200
	}
	writeJSON(w, status, SylonMemoryMutation{Receipt: receipt, Replayed: replay, Memory: s.memorySummary(r.Context(), principal(r), item)})
}
func (s *Server) memoryReplay(w http.ResponseWriter, r *http.Request, key, fp string) bool {
	receipt, replayed, e := s.Store.SylonMemoryMutation(r.Context(), r.PathValue("workspaceId"), subjectFor(principal(r)), key, fp)
	if e != nil {
		memoryFailure(w, e)
		return true
	}
	if replayed {
		s.memoryMutationResponse(w, r, receipt, true, 200)
		return true
	}
	return false
}
func (s *Server) listSylonMemories(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	items, e := s.Store.SylonMemories(r.Context(), r.PathValue("workspaceId"), subjectFor(principal(r)), "", false)
	if e != nil {
		memoryFailure(w, e)
		return
	}
	out := []SylonMemorySummary{}
	for _, item := range items {
		out = append(out, s.memorySummary(r.Context(), principal(r), item))
	}
	writeJSON(w, 200, map[string]any{"items": out})
}
func (s *Server) getSylonMemory(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	ws, o := r.PathValue("workspaceId"), subjectFor(principal(r))
	item, e := s.Store.SylonMemoryWork(r.Context(), ws, r.PathValue("id"), o)
	if e != nil {
		memoryFailure(w, e)
		return
	}
	b, e := s.memoryBinding(r.Context(), ws, item.Memory.BindingID, o)
	if e != nil {
		memoryFailure(w, e)
		return
	}
	item, e = s.AuthorizeSylonMemory(r.Context(), b, o, SylonMemorySelection{ID: item.Memory.ID, Revision: item.Memory.Revision})
	if e != nil {
		memoryFailure(w, e)
		return
	}
	if !memoryRoleAllowed(principal(r), item) {
		memoryFailure(w, ErrMemoryAccess)
		return
	}
	writeJSON(w, 200, item.Memory)
}
func (s *Server) saveSylonMemory(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	var in SylonMemoryInput
	if !decode(w, r, &in) {
		return
	}
	if e := in.Validate(true); e != nil {
		fail(w, 422, "invalid_memory", e.Error())
		return
	}
	fp := memoryFingerprint("create", "", in)
	if s.memoryReplay(w, r, in.IdempotencyKey, fp) {
		return
	}
	ws, o := r.PathValue("workspaceId"), subjectFor(principal(r))
	if !principal(r).Can(ws, true) {
		memoryFailure(w, ErrMemoryAccess)
		return
	}
	origin, e := s.Store.SylonWork(r.Context(), ws, in.Source.JobID, o)
	if e != nil {
		memoryFailure(w, e)
		return
	}
	if origin.Job.Revision != in.Source.Revision || origin.Job.State != "succeeded" || origin.Job.Result == nil {
		memoryFailure(w, ErrMemoryRevision)
		return
	}
	b, e := s.memoryBinding(r.Context(), ws, origin.Job.BindingID, o)
	if e != nil {
		memoryFailure(w, e)
		return
	}
	if e = s.authorizeSylonWork(r.Context(), origin, false); e != nil {
		memoryFailure(w, e)
		return
	}
	if len(origin.Context.Dependencies) >= MaxSylonDependencies {
		memoryFailure(w, ErrMemoryCapacity)
		return
	}
	if !memoryDeadline(b, in.ExpiresAt) {
		fail(w, 422, "invalid_memory", "Expiration must be in the future and within the administrator's TTL limit.")
		return
	}
	item := SylonMemoryWork{Memory: SylonMemory{Resource: newResource("mem-"), WorkspaceID: ws, Owner: o, BindingID: b.ID, PolicyVersion: b.PolicyVersion, Title: in.Title, Content: in.Content, ExpiresAt: in.ExpiresAt.UTC(), Source: *in.Source}, State: "active", PolicyHash: origin.PolicyHash, Dependencies: append([]SylonDependency{}, origin.Context.Dependencies...), OutputHash: SylonOutputHash(origin.Job.Result)}
	receipt, replayed, e := s.Store.CreateSylonMemory(r.Context(), item, in.IdempotencyKey, fp)
	if e != nil {
		memoryFailure(w, e)
		return
	}
	s.memoryMutationResponse(w, r, receipt, replayed, 201)
}
func (s *Server) correctSylonMemory(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	var in SylonMemoryInput
	if !decode(w, r, &in) {
		return
	}
	if e := in.Validate(false); e != nil {
		fail(w, 422, "invalid_memory", e.Error())
		return
	}
	id, ws, o := r.PathValue("id"), r.PathValue("workspaceId"), subjectFor(principal(r))
	fp := memoryFingerprint("correct", id, in)
	if s.memoryReplay(w, r, in.IdempotencyKey, fp) {
		return
	}
	if !principal(r).Can(ws, true) {
		memoryFailure(w, ErrMemoryAccess)
		return
	}
	item, e := s.Store.SylonMemoryWork(r.Context(), ws, id, o)
	if e != nil {
		memoryFailure(w, e)
		return
	}
	b, e := s.memoryBinding(r.Context(), ws, item.Memory.BindingID, o)
	if e != nil {
		memoryFailure(w, e)
		return
	}
	if _, e = s.AuthorizeSylonMemory(r.Context(), b, o, SylonMemorySelection{ID: id, Revision: in.Revision}); e != nil {
		memoryFailure(w, e)
		return
	}
	if !memoryDeadline(b, in.ExpiresAt) {
		fail(w, 422, "invalid_memory", "Expiration must be in the future and within the administrator's TTL limit.")
		return
	}
	receipt, replayed, e := s.Store.CorrectSylonMemory(r.Context(), ws, id, o, in, fp)
	if e != nil {
		memoryFailure(w, e)
		return
	}
	s.memoryMutationResponse(w, r, receipt, replayed, 200)
}
func (s *Server) deleteSylonMemory(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	var in SylonMemoryDeleteInput
	if !decode(w, r, &in) {
		return
	}
	if !idempotencyKeyPattern.MatchString(in.IdempotencyKey) || in.Revision < 1 {
		fail(w, 422, "invalid_memory", "Deletion requires an exact revision and persistent request key.")
		return
	}
	id := r.PathValue("id")
	fp := memoryFingerprint("delete", id, in)
	if s.memoryReplay(w, r, in.IdempotencyKey, fp) {
		return
	}
	receipt, replayed, e := s.Store.DeleteSylonMemory(r.Context(), r.PathValue("workspaceId"), id, subjectFor(principal(r)), in, fp)
	if e != nil {
		memoryFailure(w, e)
		return
	}
	s.memoryMutationResponse(w, r, receipt, replayed, 200)
}
func memoryTerms(text string) []string {
	return strings.FieldsFunc(strings.ToLower(text), func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) })
}
func memoryScore(m SylonMemory, terms []string) float64 {
	title := map[string]bool{}
	body := map[string]bool{}
	for _, t := range memoryTerms(m.Title) {
		title[t] = true
	}
	for _, t := range memoryTerms(m.Content) {
		body[t] = true
	}
	score := 0.0
	seen := map[string]bool{}
	for _, t := range terms {
		if seen[t] {
			continue
		}
		seen[t] = true
		if title[t] {
			score += 2
		}
		if body[t] {
			score++
		}
	}
	return score
}
func (s *Server) recallSylonMemories(w http.ResponseWriter, r *http.Request) {
	if !s.workspace(w, r, false) {
		return
	}
	var in SylonMemoryRecallInput
	if !decode(w, r, &in) {
		return
	}
	if !identifier.MatchString(in.BindingID) || !utf8.ValidString(in.Query) || len(in.Query) > 4096 || strings.ContainsRune(in.Query, 0) || len(memoryTerms(in.Query)) == 0 || in.Limit < 1 || in.Limit > 4 {
		fail(w, 422, "invalid_memory", "Recall requires a bounded lexical query and limit 1..4.")
		return
	}
	ws, o := r.PathValue("workspaceId"), subjectFor(principal(r))
	b, e := s.memoryBinding(r.Context(), ws, in.BindingID, o)
	if e != nil {
		memoryFailure(w, e)
		return
	}
	candidates, e := s.Store.SylonMemories(r.Context(), ws, o, b.ID, true)
	if e != nil {
		memoryFailure(w, e)
		return
	}
	authorized := []SylonMemory{}
	for _, candidate := range candidates {
		current, e := s.AuthorizeSylonMemory(r.Context(), b, o, SylonMemorySelection{ID: candidate.Memory.ID, Revision: candidate.Memory.Revision})
		if e == nil && memoryRoleAllowed(principal(r), current) {
			authorized = append(authorized, current.Memory)
		}
	}
	// The corpus is at most100 private records; every candidate is authorized
	// before any content enters scoring. There is no embedding or provider call.
	hits := []SylonMemoryHit{}
	terms := memoryTerms(in.Query)
	for _, m := range authorized {
		if score := memoryScore(m, terms); score > 0 {
			hits = append(hits, SylonMemoryHit{m, score})
		}
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Score != hits[j].Score {
			return hits[i].Score > hits[j].Score
		}
		return hits[i].Memory.ID < hits[j].Memory.ID
	})
	if len(hits) > in.Limit {
		hits = hits[:in.Limit]
	}
	out := []SylonMemoryHit{}
	for _, hit := range hits {
		current, e := s.AuthorizeSylonMemory(r.Context(), b, o, SylonMemorySelection{ID: hit.Memory.ID, Revision: hit.Memory.Revision})
		if e == nil && memoryRoleAllowed(principal(r), current) {
			hit.Memory = current.Memory
			out = append(out, hit)
		}
	}
	current, e := s.memoryBinding(r.Context(), ws, b.ID, o)
	if e != nil || current.Hash() != b.Hash() {
		memoryFailure(w, ErrMemoryAccess)
		return
	}
	writeJSON(w, 200, map[string]any{"workspaceId": ws, "bindingId": b.ID, "mode": "lexical", "query": in.Query, "items": out, "complete": true})
}

// Cell-source access continues to require editor rights when derived through a
// private memory. Metadata-only context remains available to granted viewers.
func memoryRoleAllowed(p Principal, w SylonMemoryWork) bool {
	if p.Can(w.Memory.WorkspaceID, true) {
		return true
	}
	for _, d := range w.Dependencies {
		if d.Kind == "notebook-cell" {
			return false
		}
	}
	return p.Can(w.Memory.WorkspaceID, false)
}
