package platform

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"dataslate.local/api/internal/trino"
)

func outcomeFixture(t *testing.T, s Store, ws string) (Run, RunSource, IngestionOutcome, PublicationBinding) {
	t.Helper()
	ctx := context.Background()
	b := ingestionBinding()
	b.WorkspaceID = ws
	p, e := s.SaveIngestionPipeline(ctx, ws, "", ingestionInput())
	if e != nil {
		t.Fatal(e)
	}
	r, _, e := s.CreateRun(ctx, ingestionRun(ws, b, p), "publication-ingestion-intent", "ingestion")
	if e != nil {
		t.Fatal(e)
	}
	acquired, lease, e := s.AcquireRun(ctx, "fixture", time.Minute)
	if e != nil || acquired.ID != r.ID {
		t.Fatal(e)
	}
	source := *acquired.Source
	r, e = s.AdvanceRun(ctx, lease, RunUpdate{State: "succeeded", UID: "app-uid", Message: "Finished", SubmissionStarted: true, RetainLease: true})
	if e != nil {
		t.Fatal(e)
	}
	r.Source = &source
	receipt := validReceipt(r)
	data, _ := json.Marshal(receipt)
	o, e := verifyIngestionReceipt(r, IngestionTermination{Final: true, ApplicationUID: "app-uid", DriverUID: "driver-uid", DriverName: "driver-pod", ExitCode: 0, Message: string(data)}, time.Now().UTC())
	if e != nil {
		t.Fatal(e)
	}
	artifact := *r.SourceArtifact
	artifact.State = "deleted"
	r, e = s.AdvanceRun(ctx, lease, RunUpdate{State: "succeeded", Message: "Finished", WriterReleased: true, SourceArtifact: &artifact, IngestionOutcome: &o})
	if e != nil {
		t.Fatal(e)
	}
	binding := PublicationBinding{PublicationBindingSummary: PublicationBindingSummary{ID: "orders-published", WorkspaceID: ws, Name: "Orders", PolicyVersion: "v1", PipelineID: p.ID, ExecutionBindingID: b.ID, Catalog: "lake", Schema: "team", Table: "orders", MaxRunSeconds: 300}, DestinationURI: p.Destination.URI, Endpoint: "https://trino.example.test", TokenFile: "/private/publisher-token", EngineUser: "publisher", RegistrationPolicyAccepted: true, AllowedPublishers: []ExecutionSubject{r.SubmittedBy}, Readers: []PublicationReader{{ExecutionSubject: r.SubmittedBy, SQLBindingID: "sql-orders"}}}
	return r, source, o, binding
}
func validReceipt(r Run) IngestionReceipt {
	c := r.Source.IngestionContract
	batch := "0"
	return IngestionReceipt{Version: 2, PipelineID: r.Ingestion.ID, RunID: r.ID, PipelineRevision: r.Ingestion.Revision, ReceiptBindingSHA256: c.ReceiptBindingSHA256, State: "succeeded", Runtime: IngestionRuntime{"4.1.3", "4.3.1"}, ContractSHA256: c.ContractSHA256, QueryID: "00000000-0000-4000-8000-000000000001", CompletedBatches: "1", ProcessedFiles: "1", ProcessedRows: "2", LastBatchID: &batch, TableExists: true, Table: &IngestionDeltaTable{ID: "00000000-0000-4000-8000-000000000002", Version: "0", SchemaSHA256: c.SchemaSHA256, MinReaderVersion: 1, MinWriterVersion: 2, ReaderFeatures: []string{}, WriterFeatures: []string{}}}
}
func enqueuePublication(t *testing.T, s Store, r Run, source RunSource, o IngestionOutcome, b PublicationBinding, key string) PublicationWork {
	t.Helper()
	w, e := newPublication(b, r, source, o)
	if e != nil {
		t.Fatal(e)
	}
	w, _, e = s.CreatePublication(context.Background(), w, key, digestJSON([]string{r.ID, b.ID}))
	if e != nil {
		t.Fatal(e)
	}
	return w
}
func publicationDue(t *testing.T, s Store, id string) {
	t.Helper()
	switch db := s.(type) {
	case *MemoryStore:
		db.mu.Lock()
		db.publications[id].Next = time.Time{}
		db.mu.Unlock()
	case *PostgresStore:
		if _, e := db.pool.Exec(context.Background(), "UPDATE dataslate_publications SET next_reconcile_at=clock_timestamp() WHERE id=$1", id); e != nil {
			t.Fatal(e)
		}
	}
}

type fakePublicationBackend struct {
	allowed, exists, lost, changed bool
	registers, inspects            int
	nextErr                        error
}

func (b *fakePublicationBackend) Authorize(context.Context, PublicationWork) (bool, error) {
	return b.allowed, nil
}
func (b *fakePublicationBackend) Inspect(_ context.Context, w PublicationWork) (trino.DeltaObservation, error) {
	b.inspects++
	if b.changed {
		b.allowed = false
	}
	return trino.DeltaObservation{Exists: b.exists, VersionPresent: b.exists}, nil
}
func (b *fakePublicationBackend) Register(context.Context, PublicationWork) (trino.Page, error) {
	b.registers++
	b.exists = true
	if b.lost {
		return trino.Page{}, trino.ErrTransport
	}
	return trino.Page{Cursor: trino.Cursor{ID: "query_registration"}}, nil
}
func (b *fakePublicationBackend) Next(context.Context, PublicationWork) (trino.Page, error) {
	return trino.Page{}, b.nextErr
}
func (b *fakePublicationBackend) Cancel(context.Context, PublicationWork) error { return nil }
func TestPublicationDurableReplayUnknownAndReaderProjection(t *testing.T) {
	forSylonStores(t, func(t *testing.T, s Store, ws string) {
		r, source, o, b := outcomeFixture(t, s, ws)
		w := enqueuePublication(t, s, r, source, o, b, "publication-exact-intent")
		backend := &fakePublicationBackend{allowed: true, lost: true}
		if _, e := ReconcilePublicationOnce(context.Background(), s, backend, "worker-one"); e != nil {
			t.Fatal(e)
		}
		read, e := s.Publication(context.Background(), ws, w.Publication.ID, r.SubmittedBy)
		if e != nil || read.Publication.State != "unknown" || !read.Publication.RegistrationStarted {
			t.Fatal(read, e)
		}
		publicationDue(t, s, w.Publication.ID)
		if _, e = ReconcilePublicationOnce(context.Background(), s, backend, "worker-restart"); e != nil {
			t.Fatal(e)
		}
		read, e = s.Publication(context.Background(), ws, w.Publication.ID, r.SubmittedBy)
		if e != nil || read.Publication.State != "published" || backend.registers != 1 {
			t.Fatal("ambiguous registration was not resolved exactly once", read, e, backend.registers)
		}
		replay, replayed, e := s.CreatePublication(context.Background(), w, "publication-exact-intent", digestJSON([]string{r.ID, b.ID}))
		if e != nil || !replayed || replay.Publication.ID != w.Publication.ID {
			t.Fatal("replay", e)
		}
		if _, _, e = s.CreatePublication(context.Background(), w, "publication-exact-intent", "changed"); !errors.Is(e, ErrConflict) {
			t.Fatal("changed intent accepted", e)
		}
		table, e := s.PublishedTable(context.Background(), ws, b.ID)
		if e != nil || table.Table.Fields[0].Nullable != true || table.Table.VerifiedDeltaVersion != "0" {
			t.Fatal(table, e)
		}
		data, _ := json.Marshal(table.Table)
		if strings.Contains(string(data), "s3://") || strings.Contains(string(data), r.ID) || strings.Contains(string(data), r.SubmittedBy.Subject) {
			t.Fatal("shared table leaked private provenance", string(data))
		}
		if _, e = s.Publication(context.Background(), ws, w.Publication.ID, ExecutionSubject{Issuer: r.SubmittedBy.Issuer, Subject: "other"}); !errors.Is(e, ErrNotFound) {
			t.Fatal("other owner accessed operation")
		}
		// Source edits/deletion do not rewrite the persisted completed run/outcome.
		if e = s.DeleteIngestionPipeline(context.Background(), ws, r.Ingestion.ID, r.Ingestion.Revision); e != nil {
			t.Fatal(e)
		}
		out, e := s.IngestionOutcome(context.Background(), ws, r.ID, r.SubmittedBy)
		if e != nil || out.State != "verified" {
			t.Fatal(out, e)
		}
	})
}
func TestPublicationForeignTargetRevocationAndLeaseFence(t *testing.T) {
	forSylonStores(t, func(t *testing.T, s Store, ws string) {
		r, source, o, b := outcomeFixture(t, s, ws)
		w := enqueuePublication(t, s, r, source, o, b, "foreign-target-intent")
		backend := &fakePublicationBackend{allowed: true, exists: true}
		if _, e := ReconcilePublicationOnce(context.Background(), s, backend, "worker"); e != nil {
			t.Fatal(e)
		}
		read, e := s.Publication(context.Background(), ws, w.Publication.ID, r.SubmittedBy)
		if e != nil || read.Publication.State != "failed" || backend.registers != 0 {
			t.Fatal("foreign mapping adopted", read, e)
		}
		w = enqueuePublication(t, s, r, source, o, b, "fenced-target-intent")
		_, lease, e := s.AcquirePublication(context.Background(), "expired-worker", time.Millisecond)
		if e != nil {
			t.Fatal(e)
		}
		time.Sleep(3 * time.Millisecond)
		if _, e = s.AdvancePublication(context.Background(), lease, PublicationUpdate{State: "published", Verified: true, Final: true}); !errors.Is(e, ErrLeaseLost) {
			t.Fatal("expired publication committed", e)
		}
		backend = &fakePublicationBackend{allowed: true, changed: true}
		if _, e = ReconcilePublicationOnce(context.Background(), s, backend, "new-worker"); e != nil {
			t.Fatal(e)
		}
		read, e = s.Publication(context.Background(), ws, w.Publication.ID, r.SubmittedBy)
		if e != nil || read.Publication.State != "unknown" || backend.registers != 0 {
			t.Fatal("revoked policy dispatched", read, e)
		}
	})
}
func TestPublicationConcurrentTargetReservation(t *testing.T) {
	forSylonStores(t, func(t *testing.T, s Store, ws string) {
		r, source, o, b := outcomeFixture(t, s, ws)
		var wg sync.WaitGroup
		var mu sync.Mutex
		success, busy := 0, 0
		for _, key := range []string{"concurrent-intent-one", "concurrent-intent-two"} {
			wg.Go(func() {
				w, e := newPublication(b, r, source, o)
				if e != nil {
					t.Error(e)
					return
				}
				_, _, e = s.CreatePublication(context.Background(), w, key, key)
				mu.Lock()
				defer mu.Unlock()
				if e == nil {
					success++
				} else if errors.Is(e, ErrPublicationBusy) {
					busy++
				} else {
					t.Error(e)
				}
			})
		}
		wg.Wait()
		if success != 1 || busy != 1 {
			t.Fatal(success, busy)
		}
	})
}
func TestPublicationHTTPCurrentPoliciesAndNoCredentialExposure(t *testing.T) {
	s := NewMemoryStore()
	r, source, o, b := outcomeFixture(t, s, "default")
	execution := ingestionBinding()
	sql := sqlBinding()
	sql.ID = "sql-orders"
	sql.Catalog = b.Catalog
	sql.Schema = b.Schema
	sql.Endpoint = b.Endpoint
	sql.AllowedSubjects = []SQLSubject{{ExecutionSubject: r.SubmittedBy, EngineUser: "reader"}}
	server := &Server{Store: s, Auth: testAuth{p: executionPrincipal()}, Config: Config{AuthMode: "oidc", ExecutionBindings: []ExecutionBinding{execution}, PublicationBindings: []PublicationBinding{b}, SQLBindings: []SQLBinding{sql}}}
	h := server.Handler()
	response := request(h, "GET", "/api/v1/workspaces/default/publication-bindings", "")
	if response.Code != 200 || strings.Contains(response.Body.String(), "private") || strings.Contains(response.Body.String(), b.Endpoint) {
		t.Fatal(response.Code, response.Body)
	}
	input, _ := json.Marshal(PublicationInput{RunID: r.ID, BindingID: b.ID, IdempotencyKey: "http-publication-intent"})
	response = request(h, "POST", "/api/v1/workspaces/default/table-publications", string(input))
	if response.Code != 202 {
		t.Fatal(response.Code, response.Body)
	}
	var result struct {
		Publication TablePublication `json:"publication"`
	}
	json.Unmarshal(response.Body.Bytes(), &result)
	backend := &fakePublicationBackend{allowed: true}
	ReconcilePublicationOnce(context.Background(), s, backend, "worker")
	publicationDue(t, s, result.Publication.ID)
	ReconcilePublicationOnce(context.Background(), s, backend, "worker")
	response = request(h, "GET", "/api/v1/workspaces/default/published-tables/"+b.ID, "")
	if response.Code != 200 || !strings.Contains(response.Body.String(), "queryMode") || strings.Contains(response.Body.String(), "s3://") {
		t.Fatal(response.Code, response.Body)
	}
	server.Config.PublicationBindings[0].Readers = []PublicationReader{}
	response = request(h, "GET", "/api/v1/workspaces/default/published-tables/"+b.ID, "")
	if response.Code != 403 {
		t.Fatal("revoked table readable", response.Code, response.Body)
	}
	server.Config.ExecutionBindings = nil
	response = request(h, "GET", "/api/v1/workspaces/default/runs/"+r.ID+"/ingestion-outcome", "")
	if response.Code != 403 {
		t.Fatal("revoked outcome readable", response.Code, response.Body)
	}
	source.IngestionContract.Spec.Source.Schema[0].Type = "timestamp"
	if _, e := newPublication(b, r, source, o); !errors.Is(e, ErrPublicationSchema) {
		t.Fatal("timestamp publication accepted", e)
	}
}

func TestPublicationEndpointAndPrivilegeBoundaries(t *testing.T) {
	s := NewMemoryStore()
	_, _, _, b := outcomeFixture(t, s, "default")
	variant := b
	variant.Endpoint = "https://TRINO.example.test.:0443/"
	variant.Catalog = "LAKE"
	variant.Schema = "TEAM"
	variant.Table = "ORDERS"
	if b.targetKey() != variant.targetKey() {
		t.Fatal("equivalent engine targets have different reservations")
	}
	sql := sqlBinding()
	sql.ID = "sql-orders"
	sql.Endpoint = b.Endpoint
	sql.Catalog = b.Catalog
	sql.Schema = b.Schema
	if e := ValidatePublicationConfiguration([]PublicationBinding{b}, []ExecutionBinding{ingestionBinding()}, []SQLBinding{sql}); e != nil {
		t.Fatal(e)
	}
	for name, mutate := range map[string]func(*PublicationBinding, *SQLBinding, *ExecutionBinding){"shared identity": func(b *PublicationBinding, s *SQLBinding, _ *ExecutionBinding) {
		b.EngineUser = s.AllowedSubjects[0].EngineUser
	}, "shared token": func(b *PublicationBinding, s *SQLBinding, _ *ExecutionBinding) { b.TokenFile = s.TokenFile }, "no publisher execution grant": func(_ *PublicationBinding, _ *SQLBinding, e *ExecutionBinding) {
		e.AllowedSubjects = []ExecutionSubject{}
	}, "no destination grant": func(_ *PublicationBinding, _ *SQLBinding, e *ExecutionBinding) {
		e.IngestionStorage = &IngestionStorage{ReadPrefixes: []string{"s3://customer-lake/raw/"}, WritePrefixes: []string{"s3://other-lake/write/"}}
	}, "ungranted reader": func(_ *PublicationBinding, s *SQLBinding, _ *ExecutionBinding) { s.AllowedSubjects = []SQLSubject{} }} {
		t.Run(name, func(t *testing.T) {
			p, reader, execution := b, sql, ingestionBinding()
			mutate(&p, &reader, &execution)
			if ValidatePublicationConfiguration([]PublicationBinding{p}, []ExecutionBinding{execution}, []SQLBinding{reader}) == nil {
				t.Fatal("unsafe configuration accepted")
			}
		})
	}
}

func TestPostgresPublicationFencesAfterBlockingRows(t *testing.T) {
	for _, phase := range []string{"lease", "deadline"} {
		t.Run(phase, func(t *testing.T) {
			s, ws := searchTestStore(t)
			ctx := context.Background()
			r, source, o, b := outcomeFixture(t, s, ws)
			if phase == "deadline" {
				b.MaxRunSeconds = 1
			}
			w := enqueuePublication(t, s, r, source, o, b, "blocked-publication-intent")
			ttl := 2 * time.Minute
			if phase == "lease" {
				ttl = 300 * time.Millisecond
			}
			_, lease, e := s.AcquirePublication(ctx, "blocked-worker", ttl)
			if e != nil {
				t.Fatal(e)
			}
			tx, e := s.pool.Begin(ctx)
			if e != nil {
				t.Fatal(e)
			}
			defer tx.Rollback(ctx)
			if phase == "lease" {
				var id string
				e = tx.QueryRow(ctx, "SELECT id FROM dataslate_publications WHERE id=$1 FOR UPDATE", w.Publication.ID).Scan(&id)
			} else {
				record := publicationTable(w, time.Now().Add(-time.Hour))
				if _, e = s.pool.Exec(ctx, "INSERT INTO dataslate_published_tables(workspace_id,binding_id,document) VALUES($1,$2,$3)", ws, b.ID, record); e != nil {
					t.Fatal(e)
				}
				var id string
				e = tx.QueryRow(ctx, "SELECT binding_id FROM dataslate_published_tables WHERE workspace_id=$1 AND binding_id=$2 FOR UPDATE", ws, b.ID).Scan(&id)
			}
			if e != nil {
				t.Fatal(e)
			}
			result := make(chan error, 1)
			go func() {
				_, err := s.AdvancePublication(ctx, lease, PublicationUpdate{State: "published", Verified: true, Final: true})
				result <- err
			}()
			blocked := false
			limit := time.Now().Add(2 * time.Second)
			for time.Now().Before(limit) {
				if e = s.pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1::integer=ANY(pg_blocking_pids(pid)))", int32(tx.Conn().PgConn().PID())).Scan(&blocked); e != nil {
					t.Fatal(e)
				}
				if blocked {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			if !blocked {
				t.Fatal("did not observe the actual publication row wait")
			}
			wait := 350 * time.Millisecond
			if phase == "deadline" {
				wait = time.Until(w.Publication.CreatedAt.Add(time.Second)) + 30*time.Millisecond
			}
			if wait > 0 {
				time.Sleep(wait)
			}
			if e = tx.Commit(ctx); e != nil {
				t.Fatal(e)
			}
			if e = <-result; !errors.Is(e, ErrLeaseLost) && !errors.Is(e, ErrPublicationDeadline) {
				t.Fatal("late publication was accepted", e)
			}
			current, e := s.Publication(ctx, ws, w.Publication.ID, r.SubmittedBy)
			if e != nil || current.Publication.State != "queued" || current.Publication.ReconciliationComplete {
				t.Fatal("failed atomic publication changed status", current, e)
			}
			table, e := s.PublishedTable(ctx, ws, b.ID)
			if phase == "lease" && !errors.Is(e, ErrNotFound) {
				t.Fatal("expired lease published table", e)
			}
			if phase == "deadline" && (e != nil || !table.Table.VerifiedAt.Before(time.Now().Add(-time.Minute))) {
				t.Fatal("deadline write escaped transaction rollback", table, e)
			}
		})
	}
}

func TestPublicationRejectsArbitraryCodeNamespace(t *testing.T) {
	s := NewMemoryStore()
	_, _, _, b := outcomeFixture(t, s, "default")
	execution := ingestionBinding()
	other := notebookBinding()
	other.WorkspaceID = "another-team"
	other.Namespace = execution.Namespace
	if validatePublicationExecution(b, []ExecutionBinding{execution, other}) == nil {
		t.Fatal("untrusted application namespace accepted for receipt capture")
	}
	other.Namespace = "separate-apps"
	if e := validatePublicationExecution(b, []ExecutionBinding{execution, other}); e != nil {
		t.Fatal(e)
	}
	for _, endpoint := range []string{"https://trino:0", "https://trino:65536", "https://:443"} {
		invalid := b
		invalid.Endpoint = endpoint
		if invalid.Validate() == nil {
			t.Fatal("invalid endpoint accepted", endpoint)
		}
	}
}
