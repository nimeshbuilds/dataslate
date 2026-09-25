package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

type testAuth struct {
	p   Principal
	err error
}

func (a testAuth) Authenticate(context.Context, string) (Principal, error) { return a.p, a.err }
func serveTest(s Store, p Principal) http.Handler {
	return (&Server{Config: Config{AuthMode: "oidc"}, Store: s, Auth: testAuth{p: p}}).Handler()
}
func request(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func input() NotebookInput {
	return NotebookInput{Name: "Revenue", Kind: "notebook", Cells: []Cell{{ID: "cell-1", Language: "python", Source: "print('hello')"}}}
}
func createBody() string { b, _ := json.Marshal(input()); return string(b) }
func TestAuthorizationAndWorkspaceIsolation(t *testing.T) {
	s := NewMemoryStore()
	other, _ := s.CreateWorkspace(context.Background(), "Other", "other")
	n, _ := s.SaveNotebook(context.Background(), "default", "", input())
	for _, tc := range []struct {
		name                string
		p                   Principal
		read, write, create int
	}{
		{"absent", Principal{Subject: "u"}, 403, 403, 403},
		{"viewer", Principal{Subject: "u", Workspaces: map[string]string{"default": "viewer"}}, 200, 403, 403},
		{"editor", Principal{Subject: "u", Workspaces: map[string]string{"default": "editor"}}, 200, 201, 403},
		{"workspace-admin", Principal{Subject: "u", Workspaces: map[string]string{"default": "admin"}}, 200, 201, 403},
		{"platform-admin-not-member", Principal{Subject: "u", PlatformAdmin: true}, 403, 403, 201},
		{"unknown-role", Principal{Subject: "u", Workspaces: map[string]string{"default": "owner"}}, 403, 403, 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := serveTest(s, tc.p)
			for _, r := range []struct {
				method, path, body string
				want               int
			}{{"GET", "/api/v1/workspaces/default/notebooks", "", tc.read}, {"POST", "/api/v1/workspaces/default/notebooks", createBody(), tc.write}, {"POST", "/api/v1/workspaces", `{"name":"Created","slug":"created"}`, tc.create}} {
				w := request(h, r.method, r.path, r.body)
				if w.Code != r.want {
					t.Fatalf("%s: got %d want %d: %s", r.path, w.Code, r.want, w.Body)
				}
			}
		})
	}
	p := Principal{Subject: "u", Workspaces: map[string]string{other.ID: "editor"}}
	h := serveTest(s, p)
	for _, method := range []string{"GET", "PUT", "DELETE"} {
		body := createBody()
		if method == "PUT" {
			v := input()
			v.Revision = 1
			b, _ := json.Marshal(v)
			body = string(b)
		}
		w := request(h, method, "/api/v1/workspaces/"+other.ID+"/notebooks/"+n.ID+"?revision=1", body)
		if w.Code != 404 {
			t.Fatalf("cross-workspace %s returned %d", method, w.Code)
		}
	}
	w := request(h, "GET", "/api/v1/workspaces", "")
	if strings.Contains(w.Body.String(), `"id":"default"`) {
		t.Fatal("workspace list leaked other membership")
	}
}
func TestNotebookRoundTripRevisionAndDelete(t *testing.T) {
	h := serveTest(NewMemoryStore(), Principal{Development: true, PlatformAdmin: true})
	w := request(h, "POST", "/api/v1/workspaces/default/notebooks", createBody())
	if w.Code != 201 {
		t.Fatal(w.Body)
	}
	var n Notebook
	json.Unmarshal(w.Body.Bytes(), &n)
	url := "/api/v1/workspaces/default/notebooks/" + n.ID
	in := input()
	in.Revision = n.Revision
	in.Cells[0].Source = "print('updated')"
	b, _ := json.Marshal(in)
	w = request(h, "PUT", url, string(b))
	if w.Code != 200 {
		t.Fatal(w.Body)
	}
	w = request(h, "PUT", url, string(b))
	if w.Code != 409 {
		t.Fatalf("stale save: %d", w.Code)
	}
	w = request(h, "GET", url, "")
	if !strings.Contains(w.Body.String(), "updated") || !strings.Contains(w.Body.String(), `"revision":2`) {
		t.Fatal(w.Body)
	}
	w = request(h, "GET", url+"/revisions", "")
	var history struct{ Items []Notebook }
	json.Unmarshal(w.Body.Bytes(), &history)
	if len(history.Items) != 2 || history.Items[0].Cells[0].Source != "print('hello')" {
		t.Fatal("history was overwritten")
	}
	if w = request(h, "DELETE", url+"?revision=1", ""); w.Code != 409 {
		t.Fatal("stale delete accepted")
	}
	if w = request(h, "DELETE", url+"?revision=2", ""); w.Code != 204 {
		t.Fatal(w.Body)
	}
	if w = request(h, "GET", url, ""); w.Code != 404 {
		t.Fatal("deleted document still visible")
	}
}
func TestMutationValidationAndNoExecution(t *testing.T) {
	h := serveTest(NewMemoryStore(), Principal{Development: true, PlatformAdmin: true})
	for _, tc := range []struct {
		body string
		want int
	}{
		{`{"name":"n","kind":"sql","cells":[],"workspaceId":"victim"}`, 400},
		{`{"name":"n","kind":"sql","cells":[{"id":"x","language":"python","source":"x"}]}`, 422},
		{`{"name":"n","kind":"notebook","cells":[{"id":"x","language":"sql","source":"x","outputs":[]}]}`, 400},
		{`{"name":"n","kind":"notebook","cells":null}`, 422},
		{createBody() + `{}`, 400},
		{`{"name":"` + strings.Repeat("a", 1024*1024) + `"}`, 413},
	} {
		w := request(h, "POST", "/api/v1/workspaces/default/notebooks", tc.body)
		if w.Code != tc.want {
			t.Fatalf("got %d want %d", w.Code, tc.want)
		}
	}
	for _, path := range []string{"runs", "sylon/messages"} {
		w := request(h, "POST", "/api/v1/workspaces/default/"+path, `{}`)
		if w.Code != 501 || !strings.Contains(w.Body.String(), "capability_unavailable") {
			t.Fatal(w.Body)
		}
	}
	r := httptest.NewRequest("POST", "/api/v1/workspaces/default/notebooks", strings.NewReader(createBody()))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Origin", "https://evil.example")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatal("cross-site development write accepted")
	}
}
func TestUnauthenticatedFailClosed(t *testing.T) {
	h := (&Server{Config: Config{AuthMode: "oidc"}, Store: NewMemoryStore(), Auth: testAuth{err: errors.New("invalid")}}).Handler()
	for _, path := range []string{"/api/v1/status", "/api/v1/modules", "/api/v1/workspaces"} {
		w := request(h, "GET", path, "")
		if w.Code != 401 {
			t.Fatalf("%s returned %d", path, w.Code)
		}
	}
	if w := request(h, "GET", "/api/v1/auth/config", ""); w.Code != 200 {
		t.Fatal("public auth config blocked")
	}
}
func TestConcurrentSaveHasSingleWinner(t *testing.T) {
	s := NewMemoryStore()
	n, _ := s.SaveNotebook(context.Background(), "default", "", input())
	h := serveTest(s, Principal{Development: true})
	v := input()
	v.Revision = 1
	b, _ := json.Marshal(v)
	codes := make(chan int, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Go(func() {
			codes <- request(h, "PUT", fmt.Sprintf("/api/v1/workspaces/default/notebooks/%s", n.ID), string(b)).Code
		})
	}
	wg.Wait()
	close(codes)
	counts := map[int]int{}
	for c := range codes {
		counts[c]++
	}
	if counts[200] != 1 || counts[409] != 1 {
		t.Fatalf("lost update: %v", counts)
	}
}
