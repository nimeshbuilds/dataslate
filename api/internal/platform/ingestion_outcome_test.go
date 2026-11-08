package platform

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestIngestionOutcomeStrictReceiptAndBinding(t *testing.T) {
	s := NewMemoryStore()
	r, source, _, _ := outcomeFixture(t, s, "default")
	r.Source = &source
	receipt := validReceipt(r)
	base, _ := json.Marshal(receipt)
	for name, change := range map[string]func(map[string]any){
		"wrong run": func(m map[string]any) { m["runId"] = "run-wrong" }, "wrong binding": func(m map[string]any) { m["receiptBindingSha256"] = strings.Repeat("0", 64) }, "wrong contract": func(m map[string]any) { m["contractSha256"] = strings.Repeat("0", 64) }, "float counter": func(m map[string]any) { m["processedRows"] = 2 }, "noncanonical counter": func(m map[string]any) { m["processedRows"] = "02" }, "missing field": func(m map[string]any) { delete(m, "lastBatchId") }, "extra field": func(m map[string]any) { m["credentials"] = "secret" }, "wrong runtime": func(m map[string]any) { m["runtime"].(map[string]any)["delta"] = "unknown" }, "wrong physical schema": func(m map[string]any) { m["table"].(map[string]any)["schemaSha256"] = strings.Repeat("0", 64) }, "features unordered": func(m map[string]any) {
			m["table"].(map[string]any)["writerFeatures"] = []string{"zFeature", "aFeature"}
		}, "feature invalid": func(m map[string]any) { m["table"].(map[string]any)["writerFeatures"] = []string{"unsafe_feature"} }, "table absent": func(m map[string]any) { m["table"] = nil },
	} {
		t.Run(name, func(t *testing.T) {
			var m map[string]any
			json.Unmarshal(base, &m)
			change(m)
			data, _ := json.Marshal(m)
			_, e := verifyIngestionReceipt(r, IngestionTermination{Final: true, ApplicationUID: r.KubernetesUID, DriverUID: "driver", DriverName: "driver", Message: string(data)}, time.Now())
			if e == nil {
				t.Fatal("invalid evidence accepted")
			}
		})
	}
	for _, raw := range []string{string(base) + "{}", strings.Replace(string(base), `"version":2`, `"version":2,"version":2`, 1), strings.Repeat("x", 4097)} {
		if _, e := verifyIngestionReceipt(r, IngestionTermination{Final: true, ApplicationUID: r.KubernetesUID, DriverUID: "driver", DriverName: "driver", Message: raw}, time.Now()); e == nil {
			t.Fatal("invalid complete document accepted")
		}
	}
	for _, termination := range []IngestionTermination{{Final: true, ApplicationUID: "replacement", DriverUID: "driver", DriverName: "driver", Message: string(base)}, {Final: true, ApplicationUID: r.KubernetesUID, DriverUID: "driver", DriverName: "driver", Message: string(base), ExitCode: 1}, {ApplicationUID: r.KubernetesUID, DriverUID: "driver", DriverName: "driver", Message: string(base)}} {
		if _, e := verifyIngestionReceipt(r, termination, time.Now()); e == nil {
			t.Fatal("unconfirmed driver accepted")
		}
	}
}

type outcomeBackend struct {
	fakeExecutionBackend
	store                Store
	term                 IngestionTermination
	err                  error
	drains               int
	persistedBeforeDrain bool
}

func (b *outcomeBackend) CaptureIngestionOutcome(context.Context, Run) (IngestionTermination, error) {
	return b.term, b.err
}
func (b *outcomeBackend) DrainIngestion(ctx context.Context, r Run) (bool, error) {
	b.drains++
	o, e := b.store.IngestionOutcome(ctx, r.WorkspaceID, r.ID, r.SubmittedBy)
	b.persistedBeforeDrain = e == nil && o.State != "pending"
	return true, nil
}
func TestIngestionOutcomePersistedBeforeDrainAndMissingDeadline(t *testing.T) {
	forSylonStores(t, func(t *testing.T, s Store, ws string) {
		ctx := context.Background()
		binding := ingestionBinding()
		binding.WorkspaceID = ws
		p, e := s.SaveIngestionPipeline(ctx, ws, "", ingestionInput())
		if e != nil {
			t.Fatal(e)
		}
		r, _, e := s.CreateRun(ctx, ingestionRun(ws, binding, p), "outcome-capture-intent", "outcome")
		if e != nil {
			t.Fatal(e)
		}
		acquired, lease, e := s.AcquireRun(ctx, "setup", time.Minute)
		if e != nil {
			t.Fatal(e)
		}
		source := acquired.Source
		r, e = s.AdvanceRun(ctx, lease, RunUpdate{State: "succeeded", UID: "app-uid", Message: "Complete"})
		if e != nil {
			t.Fatal(e)
		}
		r.Source = source
		receipt := validReceipt(r)
		raw, _ := json.Marshal(receipt)
		backend := &outcomeBackend{store: s, term: IngestionTermination{Final: true, ApplicationUID: r.KubernetesUID, DriverUID: "driver-uid", DriverName: "driver", Message: string(raw)}}
		if _, e = ReconcileOnce(ctx, s, backend, "capture-worker"); e != nil {
			t.Fatal(e)
		}
		if backend.drains != 1 || !backend.persistedBeforeDrain {
			t.Fatal("compute drained before durable outcome")
		}
		out, e := s.IngestionOutcome(ctx, ws, r.ID, r.SubmittedBy)
		if e != nil || out.State != "verified" {
			t.Fatal(out, e)
		}
		// Independent source cleanup may remain pending; outcome is already durable.
		if _, e = s.IngestionOutcome(ctx, ws, r.ID, ExecutionSubject{Issuer: r.SubmittedBy.Issuer, Subject: "foreign"}); !errors.Is(e, ErrNotFound) {
			t.Fatal("outcome owner boundary missing")
		}
		started := time.Now().Add(-6 * time.Minute)
		r.OutcomeCaptureStartedAt = &started
		backend.err = errors.New("temporary unavailable")
		out, retry := captureIngestionOutcome(ctx, backend, r, time.Now())
		if retry || out.State != "unavailable" || out.ReasonCode != "capture_deadline_exceeded" {
			t.Fatal("receipt cleanup blocked indefinitely", out, retry)
		}
	})
}

func TestIngestionOutcomeCanonicalRuntimeVector(t *testing.T) {
	spec := IngestionSpec{Source: IngestionSource{Format: "json", URI: "s3://customer-lake/landing/orders/", Schema: []IngestionField{{Name: "order_id", Type: "long", Nullable: false}, {Name: "amount", Type: "double", Nullable: false}, {Name: "ordered_at", Type: "timestamp", Nullable: false}}}, Destination: IngestionDestination{URI: "s3://customer-lake/delta/orders/"}, CheckpointURI: "s3://customer-lake/checkpoints/orders/", MaxFilesPerTrigger: 100}
	contract := ingestionSourceContract(Run{Ingestion: &IngestionProvenance{PipelineSelection: PipelineSelection{ID: "pipeline-orders", Revision: 1}}}, spec)
	if contract.ContractSHA256 != "eafb3c4f0cd10037da518def09d66e1ee8de54decb201e317bf36877d0c7f31d" || contract.SchemaSHA256 != "0f4996a2620755ccb2b33ac38e27d5bd450ade45f6296cd7dfe2f98ccf342e5a" {
		t.Fatal("runtime physical contract canonicalization differs", contract.ContractSHA256, contract.SchemaSHA256)
	}
}
