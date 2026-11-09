package platform

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestStatusWorkspacePermissionMatchesMutationAndDoesNotGrantMembership(t *testing.T) {
	for _, tc := range []struct {
		name string
		p    Principal
		want bool
	}{
		{"memberless", Principal{Subject: "user"}, false},
		{"viewer", Principal{Subject: "user", Workspaces: map[string]string{"default": "viewer"}}, false},
		{"editor", Principal{Subject: "user", Workspaces: map[string]string{"default": "editor"}}, false},
		{"workspace-admin", Principal{Subject: "user", Workspaces: map[string]string{"default": "admin"}}, false},
		{"platform-admin", Principal{Subject: "admin", PlatformAdmin: true}, true},
		{"development-admin", Principal{Subject: "development-admin", Development: true, PlatformAdmin: true}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := serveTest(NewMemoryStore(), tc.p)
			r := httptest.NewRequest(http.MethodGet, "/api/v1/status", nil)
			r.Header.Set("X-Forwarded-Groups", "platform-admin")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			var status struct {
				Permissions map[string]bool `json:"permissions"`
			}
			if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &status) != nil {
				t.Fatal("authenticated status did not return permissions")
			}
			allowed, present := status.Permissions["createWorkspace"]
			if !present || allowed != tc.want || len(status.Permissions) != 1 {
				t.Fatal("status permissions did not match verified principal")
			}
			created := request(h, "POST", "/api/v1/workspaces", `{"name":"Created","slug":"created"}`)
			if allowed != (created.Code == 201) {
				t.Fatalf("reported permission disagreed with mutation: %d", created.Code)
			}
			if allowed && !tc.p.Development {
				var ws Workspace
				if json.Unmarshal(created.Body.Bytes(), &ws) != nil {
					t.Fatal("invalid creation response")
				}
				if w := request(h, "GET", "/api/v1/workspaces/"+ws.ID+"/notebooks", ""); w.Code != 403 {
					t.Fatal("creation implicitly granted workspace data membership")
				}
			}
		})
	}
}
