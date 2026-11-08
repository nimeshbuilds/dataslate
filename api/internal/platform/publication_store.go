package platform

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
)

type publicationReservation struct {
	MappingKey, ActiveID string
	Published            bool
}
type memoryPublication struct {
	Work               PublicationWork
	Owner, Fingerprint string
	Fence              int64
	Until, Next        time.Time
}

func (s *MemoryStore) CreatePublication(_ context.Context, w PublicationWork, key, fp string) (PublicationWork, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := w.Publication
	k := runKey(p.WorkspaceID, p.SubmittedBy, key)
	if id, ok := s.publicationKeys[k]; ok {
		old := s.publications[id]
		if old.Fingerprint != fp {
			return PublicationWork{}, false, ErrConflict
		}
		return clonePublication(old.Work), true, nil
	}
	entry, ok := s.executions[p.RunID]
	if !ok || entry.Run.WorkspaceID != p.WorkspaceID || entry.Run.SubmittedBy != p.SubmittedBy {
		return PublicationWork{}, false, ErrNotFound
	}
	source := entry.Run.Source
	if source == nil {
		return PublicationWork{}, false, ErrIngestionOutcome
	}
	fresh, e := newPublication(w.Binding, entry.Run, *source, s.ingestionOutcomes[p.RunID])
	if e != nil {
		return PublicationWork{}, false, e
	}
	if fresh.PolicyHash != w.PolicyHash {
		return PublicationWork{}, false, ErrConflict
	}
	fresh.Publication.Resource = p.Resource
	w = fresh
	active := 0
	for _, entry := range s.publications {
		if entry.Work.Publication.WorkspaceID == p.WorkspaceID && !entry.Work.Final {
			active++
		}
	}
	if active >= 100 {
		return PublicationWork{}, false, ErrRunCapacity
	}
	target := w.Binding.targetKey()
	reservation, exists := s.publicationReservations[target]
	if exists && reservation.MappingKey != w.Binding.mappingKey() {
		return PublicationWork{}, false, ErrPublicationTarget
	}
	if reservation.ActiveID != "" {
		return PublicationWork{}, false, ErrPublicationBusy
	}
	w.PreviouslyPublished = reservation.Published
	reservation.MappingKey = w.Binding.mappingKey()
	reservation.ActiveID = p.ID
	s.publicationReservations[target] = reservation
	s.publicationKeys[k] = p.ID
	s.publications[p.ID] = &memoryPublication{Work: clonePublication(w), Fingerprint: fp}
	return clonePublication(w), false, nil
}
func (s *MemoryStore) Publications(_ context.Context, ws string, owner ExecutionSubject) ([]PublicationWork, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []PublicationWork{}
	for _, p := range s.publications {
		if p.Work.Publication.WorkspaceID == ws && p.Work.Publication.SubmittedBy == owner {
			out = append(out, clonePublication(p.Work))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Publication.CreatedAt.After(out[j].Publication.CreatedAt) })
	if len(out) > 200 {
		out = out[:200]
	}
	return out, nil
}
func (s *MemoryStore) Publication(_ context.Context, ws, id string, owner ExecutionSubject) (PublicationWork, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.publications[id]
	if !ok || p.Work.Publication.WorkspaceID != ws || p.Work.Publication.SubmittedBy != owner {
		return PublicationWork{}, ErrNotFound
	}
	return clonePublication(p.Work), nil
}
func (s *MemoryStore) PublishedTable(_ context.Context, ws, id string) (PublishedTableRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.publishedTables[ws+"/"+id]
	if !ok {
		return PublishedTableRecord{}, ErrNotFound
	}
	p.Table.Fields = append([]IngestionField{}, p.Table.Fields...)
	return p, nil
}
func validPublicationLease(owner string, ttl time.Duration) bool {
	return owner != "" && len(owner) <= 200 && ttl > 0 && ttl <= 2*time.Minute
}
func (s *MemoryStore) AcquirePublication(_ context.Context, owner string, ttl time.Duration) (PublicationWork, PublicationLease, error) {
	if !validPublicationLease(owner, ttl) {
		return PublicationWork{}, PublicationLease{}, ErrConflict
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	var chosen *memoryPublication
	for _, entry := range s.publications {
		if entry.Work.Final || entry.Next.After(now) || entry.Until.After(now) {
			continue
		}
		if chosen == nil || entry.Next.Before(chosen.Next) {
			chosen = entry
		}
	}
	if chosen == nil {
		return PublicationWork{}, PublicationLease{}, ErrNotFound
	}
	chosen.Owner = owner
	chosen.Fence++
	chosen.Until = now.Add(ttl)
	return clonePublication(chosen.Work), PublicationLease{chosen.Work.Publication.ID, owner, chosen.Fence}, nil
}
func (s *MemoryStore) AdvancePublication(_ context.Context, l PublicationLease, u PublicationUpdate) (PublicationWork, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	entry, ok := s.publications[l.ID]
	if !ok || entry.Owner != l.Owner || entry.Fence != l.Fence || !entry.Until.After(now) {
		return PublicationWork{}, ErrLeaseLost
	}
	w, e := updatePublication(entry.Work, u, now)
	if e != nil {
		return PublicationWork{}, e
	}
	reservation := s.publicationReservations[w.Binding.targetKey()]
	if reservation.ActiveID != w.Publication.ID {
		return PublicationWork{}, ErrPublicationTarget
	}
	entry.Work = w
	if u.Verified {
		s.publishedTables[w.Publication.WorkspaceID+"/"+w.Binding.ID] = publicationTable(w, now)
		reservation.Published = true
	}
	// Ambiguous dispatch retains the target forever until explicit operator recovery.
	if w.Final && (w.Publication.State == "published" || !w.Publication.RegistrationStarted) {
		reservation.ActiveID = ""
	}
	s.publicationReservations[w.Binding.targetKey()] = reservation
	if !u.RetainLease {
		entry.Owner = ""
		entry.Until = time.Time{}
		entry.Next = now.Add(u.RetryAfter)
	}
	return clonePublication(w), nil
}

func (s *PostgresStore) CreatePublication(ctx context.Context, w PublicationWork, key, fp string) (PublicationWork, bool, error) {
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return PublicationWork{}, false, e
	}
	defer tx.Rollback(ctx)
	p := w.Publication
	// Global target reservation before workspace row prevents cross-workspace adoption.
	if _, e = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(72610524003)"); e != nil {
		return PublicationWork{}, false, e
	}
	var ws string
	if e = tx.QueryRow(ctx, "SELECT id FROM dataslate_workspaces WHERE id=$1 FOR UPDATE", p.WorkspaceID).Scan(&ws); e != nil {
		return PublicationWork{}, false, storeError(e)
	}
	var old PublicationWork
	var oldFP string
	e = tx.QueryRow(ctx, "SELECT document,fingerprint FROM dataslate_publications WHERE workspace_id=$1 AND issuer=$2 AND subject=$3 AND idempotency_key=$4", p.WorkspaceID, p.SubmittedBy.Issuer, p.SubmittedBy.Subject, key).Scan(&old, &oldFP)
	if e == nil {
		if oldFP != fp {
			return PublicationWork{}, false, ErrConflict
		}
		return old, true, nil
	}
	if !errors.Is(e, pgx.ErrNoRows) {
		return PublicationWork{}, false, e
	}
	var r Run
	var source RunSource
	var outcome IngestionOutcome
	e = tx.QueryRow(ctx, `SELECT r.document,s.document,o.document FROM dataslate_runs r JOIN dataslate_run_sources s ON s.run_id=r.id JOIN dataslate_ingestion_outcomes o ON o.run_id=r.id AND o.workspace_id=r.workspace_id WHERE r.id=$1 AND r.workspace_id=$2 AND r.issuer=$3 AND r.subject=$4 FOR SHARE OF r`, p.RunID, p.WorkspaceID, p.SubmittedBy.Issuer, p.SubmittedBy.Subject).Scan(&r, &source, &outcome)
	if e != nil {
		return PublicationWork{}, false, storeError(e)
	}
	fresh, e := newPublication(w.Binding, r, source, outcome)
	if e != nil {
		return PublicationWork{}, false, e
	}
	fresh.Publication.Resource = p.Resource
	w = fresh
	var active int
	if e = tx.QueryRow(ctx, "SELECT count(*) FROM dataslate_publications WHERE workspace_id=$1 AND NOT final", p.WorkspaceID).Scan(&active); e != nil {
		return PublicationWork{}, false, e
	}
	if active >= 100 {
		return PublicationWork{}, false, ErrRunCapacity
	}
	var reservation publicationReservation
	e = tx.QueryRow(ctx, "SELECT mapping_key,COALESCE(active_id,''),published FROM dataslate_publication_targets WHERE target_key=$1 FOR UPDATE", w.Binding.targetKey()).Scan(&reservation.MappingKey, &reservation.ActiveID, &reservation.Published)
	if e != nil && !errors.Is(e, pgx.ErrNoRows) {
		return PublicationWork{}, false, e
	}
	if e == nil && reservation.MappingKey != w.Binding.mappingKey() {
		return PublicationWork{}, false, ErrPublicationTarget
	}
	if reservation.ActiveID != "" {
		return PublicationWork{}, false, ErrPublicationBusy
	}
	w.PreviouslyPublished = reservation.Published
	_, e = tx.Exec(ctx, `INSERT INTO dataslate_publication_targets(target_key,mapping_key,workspace_id,binding_id,active_id,published) VALUES($1,$2,$3,$4,$5,false) ON CONFLICT(target_key) DO UPDATE SET active_id=EXCLUDED.active_id`, w.Binding.targetKey(), w.Binding.mappingKey(), p.WorkspaceID, p.BindingID, p.ID)
	if e != nil {
		return PublicationWork{}, false, e
	}
	_, e = tx.Exec(ctx, `INSERT INTO dataslate_publications(id,workspace_id,issuer,subject,idempotency_key,fingerprint,document,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, p.ID, p.WorkspaceID, p.SubmittedBy.Issuer, p.SubmittedBy.Subject, key, fp, w, p.CreatedAt)
	if e != nil {
		return PublicationWork{}, false, e
	}
	e = tx.Commit(ctx)
	return w, false, e
}
func (s *PostgresStore) Publications(ctx context.Context, ws string, o ExecutionSubject) ([]PublicationWork, error) {
	rows, e := s.pool.Query(ctx, "SELECT document FROM dataslate_publications WHERE workspace_id=$1 AND issuer=$2 AND subject=$3 ORDER BY created_at DESC LIMIT 200", ws, o.Issuer, o.Subject)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []PublicationWork{}
	for rows.Next() {
		var w PublicationWork
		if e = rows.Scan(&w); e != nil {
			return nil, e
		}
		out = append(out, w)
	}
	return out, rows.Err()
}
func (s *PostgresStore) Publication(ctx context.Context, ws, id string, o ExecutionSubject) (PublicationWork, error) {
	var w PublicationWork
	e := s.pool.QueryRow(ctx, "SELECT document FROM dataslate_publications WHERE workspace_id=$1 AND id=$2 AND issuer=$3 AND subject=$4", ws, id, o.Issuer, o.Subject).Scan(&w)
	return w, storeError(e)
}
func (s *PostgresStore) PublishedTable(ctx context.Context, ws, id string) (PublishedTableRecord, error) {
	var r PublishedTableRecord
	e := s.pool.QueryRow(ctx, "SELECT document FROM dataslate_published_tables WHERE workspace_id=$1 AND binding_id=$2", ws, id).Scan(&r)
	return r, storeError(e)
}
func (s *PostgresStore) AcquirePublication(ctx context.Context, owner string, ttl time.Duration) (PublicationWork, PublicationLease, error) {
	if !validPublicationLease(owner, ttl) {
		return PublicationWork{}, PublicationLease{}, ErrConflict
	}
	var w PublicationWork
	l := PublicationLease{Owner: owner}
	e := s.pool.QueryRow(ctx, `WITH candidate AS(SELECT id FROM dataslate_publications WHERE NOT final AND next_reconcile_at<=clock_timestamp() AND (lease_until IS NULL OR lease_until<=clock_timestamp()) ORDER BY next_reconcile_at,id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE dataslate_publications p SET lease_owner=$1,lease_fence=lease_fence+1,lease_until=clock_timestamp()+($2*interval '1 millisecond') FROM candidate WHERE p.id=candidate.id RETURNING p.document,p.id,p.lease_fence`, owner, ttl.Milliseconds()).Scan(&w, &l.ID, &l.Fence)
	return w, l, storeError(e)
}
func (s *PostgresStore) AdvancePublication(ctx context.Context, l PublicationLease, u PublicationUpdate) (PublicationWork, error) {
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return PublicationWork{}, e
	}
	defer tx.Rollback(ctx)
	// Shared target lock order matches creation. No network request runs in this transaction.
	if _, e = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(72610524003)"); e != nil {
		return PublicationWork{}, e
	}
	var w PublicationWork
	if e = tx.QueryRow(ctx, "SELECT document FROM dataslate_publications WHERE id=$1 FOR UPDATE", l.ID).Scan(&w); e != nil {
		return PublicationWork{}, storeError(e)
	}
	var now time.Time
	if e = tx.QueryRow(ctx, "SELECT clock_timestamp() FROM dataslate_publications WHERE id=$1 AND lease_owner=$2 AND lease_fence=$3 AND lease_until>clock_timestamp()", l.ID, l.Owner, l.Fence).Scan(&now); errors.Is(e, pgx.ErrNoRows) {
		return PublicationWork{}, ErrLeaseLost
	} else if e != nil {
		return PublicationWork{}, e
	}
	w, e = updatePublication(w, u, now)
	if e != nil {
		return PublicationWork{}, e
	}
	if u.Verified {
		record := publicationTable(w, now)
		if _, e = tx.Exec(ctx, `INSERT INTO dataslate_published_tables(workspace_id,binding_id,document) VALUES($1,$2,$3) ON CONFLICT(workspace_id,binding_id) DO UPDATE SET document=EXCLUDED.document`, w.Publication.WorkspaceID, w.Binding.ID, record); e != nil {
			return PublicationWork{}, e
		}
	}
	release := w.Final && (w.Publication.State == "published" || !w.Publication.RegistrationStarted)
	tag, e := tx.Exec(ctx, `UPDATE dataslate_publication_targets SET published=published OR $3,active_id=CASE WHEN $4 THEN NULL ELSE active_id END WHERE target_key=$1 AND active_id=$2`, w.Binding.targetKey(), w.Publication.ID, u.Verified, release)
	if e != nil {
		return PublicationWork{}, e
	}
	if tag.RowsAffected() != 1 {
		return PublicationWork{}, ErrPublicationTarget
	}
	tag, e = tx.Exec(ctx, `UPDATE dataslate_publications SET document=$2,final=$3,lease_owner=CASE WHEN $4 THEN lease_owner ELSE NULL END,lease_until=CASE WHEN $4 THEN lease_until ELSE NULL END,next_reconcile_at=clock_timestamp()+($5*interval '1 millisecond') WHERE id=$1 AND lease_owner=$6 AND lease_fence=$7 AND lease_until>clock_timestamp() AND (NOT $8 OR $9::timestamptz>clock_timestamp())`, l.ID, w, w.Final, u.RetainLease, u.RetryAfter.Milliseconds(), l.Owner, l.Fence, u.Verified || u.RegistrationStarted, w.Publication.CreatedAt.Add(time.Duration(w.Binding.MaxRunSeconds)*time.Second))
	if e != nil {
		return PublicationWork{}, e
	}
	if tag.RowsAffected() != 1 {
		return PublicationWork{}, ErrLeaseLost
	}
	e = tx.Commit(ctx)
	return w, e
}
