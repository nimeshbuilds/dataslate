package kubeexec

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"dataslate.local/api/internal/platform"
)

func policyFile(t *testing.T, r platform.Run) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "bindings.json")
	b := platform.ExecutionBinding{ID: r.BindingID, WorkspaceID: r.WorkspaceID, Name: "Approved job", ExecutionProfile: r.Profile, AllowedSubjects: []platform.ExecutionSubject{r.SubmittedBy}}
	raw, _ := json.Marshal([]platform.ExecutionBinding{b})
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestPolicyReloadRevokesGrantAndRejectsMissingPolicy(t *testing.T) {
	r := exampleRun()
	c := &Client{namespaces: map[string]bool{r.Profile.Namespace: true}}
	if _, err := NewGuarded(c, ""); err == nil {
		t.Fatal("controller allowed absent policy")
	}
	path := policyFile(t, r)
	b, err := NewGuarded(c, path)
	if err != nil {
		t.Fatal(err)
	}
	if allowed, err := b.Authorize(context.Background(), r); err != nil || !allowed {
		t.Fatalf("allowed=%v %v", allowed, err)
	}
	changed := r
	changed.Profile.ExecutorInstances++
	if allowed, err := b.Authorize(context.Background(), changed); err != nil || allowed {
		t.Fatal("old profile grant permitted changed profile")
	}
	changed = r
	changed.SubmittedBy.Subject = "other-user"
	if allowed, err := b.Authorize(context.Background(), changed); err != nil || allowed {
		t.Fatal("foreign identity retained grant")
	}
	deny := platform.ExecutionBinding{ID: r.BindingID, WorkspaceID: r.WorkspaceID, Name: "Approved job", ExecutionProfile: r.Profile, AllowedSubjects: []platform.ExecutionSubject{}}
	denyJSON, _ := json.Marshal([]platform.ExecutionBinding{deny})
	if err = os.WriteFile(path, denyJSON, 0600); err != nil {
		t.Fatal(err)
	}
	if allowed, err := b.Authorize(context.Background(), r); err != nil || allowed {
		t.Fatal("empty subject list did not deny all safely")
	}
	if err = os.WriteFile(path, []byte("[]"), 0600); err != nil {
		t.Fatal(err)
	}
	if allowed, err := b.Authorize(context.Background(), r); err != nil || allowed {
		t.Fatal("removed binding retained grant")
	}
	if err = os.WriteFile(path, []byte("invalid"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = b.Authorize(context.Background(), r); err == nil {
		t.Fatal("invalid policy treated as successful read")
	}
}

func TestPolicyReloadImmediatelyBeforePOST(t *testing.T) {
	r := exampleRun()
	path := policyFile(t, r)
	creates := 0
	c, _ := testClient(t, func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			creates++
			w.WriteHeader(500)
			return
		}
		// Revoke after the first authorization check, while existence is checked.
		if err := os.WriteFile(path, []byte("[]"), 0600); err != nil {
			t.Error(err)
		}
		w.WriteHeader(404)
	})
	b, err := NewGuarded(c, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.Submit(context.Background(), r); err == nil {
		t.Fatal("revoked run submitted")
	}
	if creates != 0 {
		t.Fatal("submission occurred after policy revocation")
	}
}
