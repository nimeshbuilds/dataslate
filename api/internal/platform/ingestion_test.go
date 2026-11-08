package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

func ingestionInput() IngestionPipelineInput {
	return IngestionPipelineInput{Name: "Orders", IngestionSpec: IngestionSpec{Source: IngestionSource{Format: "json", URI: "s3://customer-lake/raw/orders/", Schema: []IngestionField{{Name: "id", Type: "long", Nullable: false}, {Name: "amount", Type: "double", Nullable: true}}}, Destination: IngestionDestination{URI: "s3://customer-lake/delta/orders/"}, CheckpointURI: "s3://customer-lake/checkpoints/orders/", MaxFilesPerTrigger: 100}}
}
func ingestionBinding() ExecutionBinding {
	b := notebookBinding()
	b.ID = "file-ingestion"
	b.ExecutionKind = FileIngestionBatch
	b.IngestionStorage = &IngestionStorage{ReadPrefixes: []string{"s3://customer-lake/raw/"}, WritePrefixes: []string{"s3://customer-lake/delta/", "s3://customer-lake/checkpoints/"}}
	return b
}
func ingestionRun(ws string, b ExecutionBinding, p IngestionPipeline) Run {
	r := newRun(ws, b, b.AllowedSubjects[0])
	r.Ingestion = &IngestionProvenance{PipelineSelection: PipelineSelection{ID: p.ID, Revision: p.Revision}}
	return r
}
func TestIngestionValidationAndStorageBoundary(t *testing.T) {
	if err := ingestionBinding().Validate(); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"s3://lake/prefix/", "s3://my.bucket/data/year=2026/", "s3://123-lake/a-b_c.v1/"} {
		if err := validateS3Prefix(path); err != nil {
			t.Fatal(path, err)
		}
	}
	for _, path := range []string{"s3://bucket/", "s3://bucket/key", "s3a://bucket/path/", "https://bucket/path/", "s3://user:pass@bucket/path/", "s3://bucket/path/?token=secret", "s3://bucket/path/#fragment", "s3://bucket/a%2fb/", "s3://bucket/a//b/", "s3://bucket/../", "s3://bucket/./", "s3://bucket/ path/", "s3://bucket/*/", "s3://127.0.0.1/a/", "s3://a..b/path/", "s3://Uppercase/path/", "s3://bucket//path/"} {
		t.Run(path, func(t *testing.T) {
			if validateS3Prefix(path) == nil {
				t.Fatal("unsafe path accepted")
			}
		})
	}
	for name, change := range map[string]func(*IngestionPipelineInput){"overlap": func(p *IngestionPipelineInput) { p.Destination.URI = p.Source.URI + "nested/" }, "schema missing": func(p *IngestionPipelineInput) { p.Source.Schema = nil }, "case collision": func(p *IngestionPipelineInput) { p.Source.Schema[1].Name = "ID" }, "SQL column": func(p *IngestionPipelineInput) { p.Source.Schema[0].Name = "id);drop" }, "unsupported type": func(p *IngestionPipelineInput) { p.Source.Schema[0].Type = "decimal(38,2)" }, "unbounded files": func(p *IngestionPipelineInput) { p.MaxFilesPerTrigger = 10001 }, "wrong format": func(p *IngestionPipelineInput) { p.Source.Format = "delta" }} {
		t.Run(name, func(t *testing.T) {
			p := ingestionInput()
			change(&p)
			if p.Validate(false) == nil {
				t.Fatal("invalid pipeline accepted")
			}
		})
	}
	b := ingestionBinding()
	if !b.IngestionStorage.Allows(ingestionInput().IngestionSpec) {
		t.Fatal("valid grant denied")
	}
	p := ingestionInput()
	p.Source.URI = "s3://customer-lake/raw-private/orders/"
	if b.IngestionStorage.Allows(p.IngestionSpec) {
		t.Fatal("prefix boundary bypass")
	}
	b.MainApplicationFile = "local:///other.py"
	if b.Validate() == nil {
		t.Fatal("application override accepted")
	}
	b = ingestionBinding()
	b.IngestionStorage = nil
	if b.Validate() == nil {
		t.Fatal("storage grant missing")
	}
	b = notebookBinding()
	b.IngestionStorage = ingestionBinding().IngestionStorage
	if b.Validate() == nil {
		t.Fatal("storage grant accepted on arbitrary notebook binding")
	}
}
func exerciseIngestionPersistence(t *testing.T, s Store, ws string) {
	t.Helper()
	ctx := context.Background()
	b := ingestionBinding()
	b.WorkspaceID = ws
	in := ingestionInput()
	in.Destination.URI += ws + "/"
	in.CheckpointURI += ws + "/"
	p, err := s.SaveIngestionPipeline(ctx, ws, "", in)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.IngestionPipeline(ctx, "other", p.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("cross workspace read", err)
	}
	selection := RunInput{BindingID: b.ID, IdempotencyKey: "fixed-ingestion-request", Pipeline: &PipelineSelection{ID: p.ID, Revision: p.Revision}}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	runs := make(chan Run, 8)
	for i := 0; i < 8; i++ {
		wg.Go(func() {
			r, _, e := s.CreateRun(ctx, ingestionRun(ws, b, p), selection.IdempotencyKey, runFingerprint(selection))
			errs <- e
			runs <- r
		})
	}
	wg.Wait()
	close(errs)
	close(runs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	var original Run
	for r := range runs {
		if original.ID != "" && original.ID != r.ID {
			t.Fatal("idempotency race")
		}
		original = r
	}
	if original.Notebook != nil || original.Ingestion == nil || original.WriterReleased || original.SourceArtifact == nil {
		t.Fatal("wrong provenance")
	}
	source, e := s.RunSource(ctx, ws, original.ID, b.AllowedSubjects[0])
	if e != nil {
		t.Fatal(e)
	}
	original.Source = &source
	if original.ValidateSource() != nil {
		t.Fatal("source integrity")
	}
	if source.Notebook.ID != "" {
		t.Fatal("ingestion masquerades as notebook")
	}
	if _, _, e = s.CreateRun(ctx, ingestionRun(ws, b, p), "second-ingestion-request", "different"); !errors.Is(e, ErrIngestionWriterBusy) {
		t.Fatal("concurrent writer accepted", e)
	}
	other := b.AllowedSubjects[0]
	other.Subject = "other"
	if _, e = s.RunSource(ctx, ws, original.ID, other); !errors.Is(e, ErrNotFound) {
		t.Fatal("cross owner source exposed", e)
	}
	in.Revision = 1
	in.Name = "Renamed"
	p, err = s.SaveIngestionPipeline(ctx, ws, p.ID, in)
	if err != nil || p.Revision != 2 {
		t.Fatal("rename", err)
	}
	if _, _, e = s.CreateRun(ctx, ingestionRun(ws, b, IngestionPipeline{Resource: Resource{ID: p.ID, Revision: 1}}), "stale-revision-intent", "new"); !errors.Is(e, ErrIngestionRevision) {
		t.Fatal("stale selection accepted", e)
	}
	in.Revision = 2
	in.CheckpointURI += "replacement/"
	if _, err = s.SaveIngestionPipeline(ctx, ws, p.ID, in); !errors.Is(err, ErrIngestionConfigImmutable) {
		t.Fatal("checkpoint changed", err)
	}
	if err = s.DeleteIngestionPipeline(ctx, ws, p.ID, 1); !errors.Is(err, ErrConflict) {
		t.Fatal("stale delete accepted")
	}
	if err = s.DeleteIngestionPipeline(ctx, ws, p.ID, 2); err != nil {
		t.Fatal(err)
	}
	replay, replayed, e := s.CreateRun(ctx, ingestionRun(ws, b, IngestionPipeline{Resource: Resource{ID: p.ID, Revision: 1}}), selection.IdempotencyKey, runFingerprint(selection))
	if e != nil || !replayed || replay.ID != original.ID {
		t.Fatal("snapshot recovery after deletion", e)
	}
	preserved, e := s.RunSource(ctx, ws, original.ID, b.AllowedSubjects[0])
	if e != nil || preserved.Source != source.Source {
		t.Fatal("source lost after deletion", e)
	}
	duplicate := ingestionInput()
	duplicate.Destination.URI = original.Ingestion.DestinationURI + "nested/"
	duplicate.CheckpointURI = original.Ingestion.CheckpointURI + "other/"
	q, e := s.SaveIngestionPipeline(ctx, ws, "", duplicate)
	if e != nil {
		t.Fatal(e)
	}
	if _, _, e = s.CreateRun(ctx, ingestionRun(ws, b, q), "different-pipeline-request", "different-pipeline"); !errors.Is(e, ErrIngestionLocationConflict) {
		t.Fatal("deleted pipeline reservation reused", e)
	}
	denied := ingestionInput()
	denied.Destination.URI = "s3://outside-lake/forbidden/"
	q, e = s.SaveIngestionPipeline(ctx, ws, "", denied)
	if e != nil {
		t.Fatal(e)
	}
	if _, _, e = s.CreateRun(ctx, ingestionRun(ws, b, q), "denied-storage-intent", "denied"); !errors.Is(e, ErrIngestionStorageDenied) {
		t.Fatal("ungranted write accepted", e)
	}
}
func TestMemoryIngestionSnapshotAndWriterReservation(t *testing.T) {
	exerciseIngestionPersistence(t, NewMemoryStore(), "default")
}
func TestPostgresIngestionSnapshotAndWriterReservation(t *testing.T) {
	dsn := os.Getenv("DATASLATE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set isolated DATASLATE_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	s, e := OpenPostgres(ctx, dsn, false)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if e = s.Migrate(ctx); e != nil {
		t.Fatal(e)
	}
	ws, e := s.CreateWorkspace(ctx, "Ingestion test", newResource("test-").ID)
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		for _, table := range []string{"dataslate_runs", "dataslate_ingestion_pipelines", "dataslate_ingestion_reservations"} {
			s.pool.Exec(ctx, "DELETE FROM "+table+" WHERE workspace_id=$1", ws.ID)
		}
		s.pool.Exec(ctx, "DELETE FROM dataslate_workspaces WHERE id=$1", ws.ID)
	}()
	exerciseIngestionPersistence(t, s, ws.ID)
}
func TestIngestionSourcePassesOnlyDataToRuntime(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	p, e := s.SaveIngestionPipeline(ctx, "default", "", ingestionInput())
	if e != nil {
		t.Fatal(e)
	}
	r, _, e := s.CreateRun(ctx, ingestionRun("default", ingestionBinding(), p), "runtime-source-intent", "runtime")
	if e != nil {
		t.Fatal(e)
	}
	python, e := exec.LookPath("python3")
	if e != nil {
		t.Skip("python3 not available")
	}
	stub := `import sys,types,json
m=types.ModuleType("dataslate_ingestion")
m.run=lambda c: print(json.dumps(c))
sys.modules["dataslate_ingestion"]=m
exec(sys.stdin.read())
`
	cmd := exec.Command(python, "-c", stub)
	cmd.Stdin = strings.NewReader(r.Source.Source)
	out, e := cmd.CombinedOutput()
	if e != nil {
		t.Fatal(e, string(out))
	}
	var config map[string]any
	if json.Unmarshal(out, &config) != nil || config["runId"] != r.ID || config["pipelineId"] != p.ID || config["pipelineRevision"] != float64(1) || config["version"] != float64(1) {
		t.Fatal("wrong runtime config", string(out))
	}
	if _, ok := config["name"]; ok {
		t.Fatal("display field forwarded")
	}
}
func TestIngestionHTTPRolesSchemaAndOwner(t *testing.T) {
	s := NewMemoryStore()
	in := ingestionInput()
	body, _ := json.Marshal(in)
	p, _ := s.SaveIngestionPipeline(context.Background(), "default", "", in)
	for _, tc := range []struct {
		role        string
		read, write int
	}{{"viewer", 200, 403}, {"editor", 200, 201}, {"admin", 200, 201}, {"", 403, 403}} {
		t.Run(tc.role, func(t *testing.T) {
			principal := executionPrincipal()
			principal.Workspaces["default"] = tc.role
			h := executionHandler(s, principal, []ExecutionBinding{ingestionBinding()}, nil)
			if got := request(h, "GET", "/api/v1/workspaces/default/ingestion-pipelines", ""); got.Code != tc.read {
				t.Fatal(got.Code, got.Body)
			}
			if got := request(h, "POST", "/api/v1/workspaces/default/ingestion-pipelines", string(body)); got.Code != tc.write {
				t.Fatal(got.Code, got.Body)
			}
		})
	}
	h := executionHandler(s, executionPrincipal(), []ExecutionBinding{ingestionBinding()}, nil)
	runBody := fmt.Sprintf(`{"bindingId":"file-ingestion","idempotencyKey":"http-ingestion-request","pipeline":{"id":%q,"revision":1}}`, p.ID)
	response := request(h, "POST", "/api/v1/workspaces/default/runs", runBody)
	if response.Code != 202 {
		t.Fatal(response.Code, response.Body)
	}
	var r Run
	json.Unmarshal(response.Body.Bytes(), &r)
	raw, _ := os.ReadFile("../../openapi.yaml")
	var doc any
	json.Unmarshal(raw, &doc)
	compiler := jsonschema.NewCompiler()
	compiler.AddResource("contract.json", doc)
	for _, tc := range []struct{ path, schema string }{{"/api/v1/status", "Status"}, {"/api/v1/workspaces/default/ingestion-pipelines/" + p.ID, "IngestionPipeline"}, {"/api/v1/workspaces/default/runs/" + r.ID, "Run"}, {"/api/v1/workspaces/default/runs/" + r.ID + "/source", "RunSource"}} {
		response := request(h, "GET", tc.path, "")
		schema, e := compiler.Compile("contract.json#/components/schemas/" + tc.schema)
		if e != nil {
			t.Fatal(e)
		}
		var body any
		json.Unmarshal(response.Body.Bytes(), &body)
		if e = schema.Validate(body); e != nil {
			t.Fatal(tc.path, e)
		}
	}
	other := executionPrincipal()
	other.Subject = "another"
	oh := executionHandler(s, other, []ExecutionBinding{ingestionBinding()}, nil)
	for _, suffix := range []string{"", "/source", "/events", "/logs"} {
		if got := request(oh, "GET", "/api/v1/workspaces/default/runs/"+r.ID+suffix, ""); got.Code != 404 {
			t.Fatal("owner bypass", suffix, got.Code)
		}
	}
}

type ingestionDrainTestBackend struct {
	sourceTestBackend
	drained bool
	drains  int
}

func (b *ingestionDrainTestBackend) DrainIngestion(context.Context, Run) (bool, error) {
	b.drains++
	return b.drained, nil
}
func TestIngestionTerminalWriterAndSourceDrainIndependent(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	p, _ := s.SaveIngestionPipeline(ctx, "default", "", ingestionInput())
	b := ingestionBinding()
	r, _, e := s.CreateRun(ctx, ingestionRun("default", b, p), "drain-ingestion-intent", "drain")
	if e != nil {
		t.Fatal(e)
	}
	_, lease, e := s.AcquireRun(ctx, "fixture", 30*time.Second)
	if e != nil {
		t.Fatal(e)
	}
	r, e = s.AdvanceRun(ctx, lease, RunUpdate{State: "succeeded", UID: "application-uid", SubmissionStarted: true, Message: "Completed"})
	if e != nil {
		t.Fatal(e)
	}
	backend := &ingestionDrainTestBackend{}
	if _, e = ReconcileOnce(ctx, s, backend, "worker"); e != nil {
		t.Fatal(e)
	}
	r, _ = s.Run(ctx, "default", r.ID, r.SubmittedBy)
	if r.WriterReleased || !r.NeedsReconciliation() {
		t.Fatal("terminal status released writer")
	}
	if _, _, e = s.CreateRun(ctx, ingestionRun("default", b, p), "new-while-draining", "new"); !errors.Is(e, ErrIngestionWriterBusy) {
		t.Fatal("new writer while drainpending", e)
	}
	s.mu.Lock()
	s.executions[r.ID].Next = time.Time{}
	s.mu.Unlock()
	backend.drained = true
	if _, e = ReconcileOnce(ctx, s, backend, "worker"); e != nil {
		t.Fatal(e)
	}
	r, _ = s.Run(ctx, "default", r.ID, r.SubmittedBy)
	if !r.WriterReleased || r.SourceArtifact.State != "deleted" || r.NeedsReconciliation() {
		t.Fatal("drain not complete", r)
	}
	if _, _, e = s.CreateRun(ctx, ingestionRun("default", b, p), "explicit-next-intent", "next"); e != nil {
		t.Fatal("explicit run after confirmeddrain", e)
	}
}

func TestIngestionCrossWorkspaceReservationRace(t *testing.T) {
	stores := map[string]func() (Store, func()){"memory": func() (Store, func()) { return NewMemoryStore(), func() {} }}
	if dsn := os.Getenv("DATASLATE_TEST_DATABASE_URL"); dsn != "" {
		stores["postgres"] = func() (Store, func()) {
			s, err := OpenPostgres(context.Background(), dsn, false)
			if err != nil {
				t.Fatal(err)
			}
			if err = s.Migrate(context.Background()); err != nil {
				t.Fatal(err)
			}
			return s, s.Close
		}
	}
	for name, factory := range stores {
		t.Run(name, func(t *testing.T) {
			s, closeStore := factory()
			defer closeStore()
			ctx := context.Background()
			a, err := s.CreateWorkspace(ctx, "First", newResource("race-a-").ID)
			if err != nil {
				t.Fatal(err)
			}
			b, err := s.CreateWorkspace(ctx, "Second", newResource("race-b-").ID)
			if err != nil {
				t.Fatal(err)
			}
			if pg, ok := s.(*PostgresStore); ok {
				defer func() {
					for _, ws := range []string{a.ID, b.ID} {
						for _, table := range []string{"dataslate_runs", "dataslate_ingestion_pipelines", "dataslate_ingestion_reservations"} {
							pg.pool.Exec(ctx, "DELETE FROM "+table+" WHERE workspace_id=$1", ws)
						}
						pg.pool.Exec(ctx, "DELETE FROM dataslate_workspaces WHERE id=$1", ws)
					}
				}()
			}
			in := ingestionInput()
			in.Destination.URI += a.ID + "/"
			in.CheckpointURI += a.ID + "/"
			var wg sync.WaitGroup
			errs := make(chan error, 2)
			for _, ws := range []string{a.ID, b.ID} {
				p, err := s.SaveIngestionPipeline(ctx, ws, "", in)
				if err != nil {
					t.Fatal(err)
				}
				binding := ingestionBinding()
				binding.WorkspaceID = ws
				wg.Go(func() {
					_, _, err := s.CreateRun(ctx, ingestionRun(ws, binding, p), "global-reservation-race", "same-location")
					errs <- err
				})
			}
			wg.Wait()
			close(errs)
			success, conflict := 0, 0
			for err := range errs {
				if err == nil {
					success++
				} else if errors.Is(err, ErrIngestionLocationConflict) {
					conflict++
				} else {
					t.Fatal(err)
				}
			}
			if success != 1 || conflict != 1 {
				t.Fatal("global writer prefix race", success, conflict)
			}
		})
	}
}
func TestIngestionRunSummaryCannotMutatePersistedPolicy(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	b := ingestionBinding()
	p, _ := s.SaveIngestionPipeline(ctx, "default", "", ingestionInput())
	r, _, err := s.CreateRun(ctx, ingestionRun("default", b, p), "summary-isolation-key", "summary")
	if err != nil {
		t.Fatal(err)
	}
	runs, _ := s.Runs(ctx, "default", r.SubmittedBy)
	runs[0].Profile.IngestionStorage.ReadPrefixes[0] = "s3://attacker/"
	runs[0].Ingestion.CheckpointURI = "s3://attacker/checkpoint/"
	runs[0].SourceArtifact.State = "deleted"
	stored, _ := s.Run(ctx, "default", r.ID, r.SubmittedBy)
	if stored.Profile.IngestionStorage.ReadPrefixes[0] != b.IngestionStorage.ReadPrefixes[0] || stored.Ingestion.CheckpointURI != p.CheckpointURI || stored.SourceArtifact.State != "pending" {
		t.Fatal("caller mutation changed durable policy")
	}
}
