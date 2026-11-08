package trino

import (
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func testClient(t *testing.T, handler http.HandlerFunc) (*Client, *httptest.Server, string) {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.pem")
	token := filepath.Join(dir, "token")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(token, []byte("engine-token-one\n"), 0600); err != nil {
		t.Fatal(err)
	}
	client, err := New(Config{Endpoint: server.URL, CAFile: ca, TokenFile: token})
	if err != nil {
		t.Fatal(err)
	}
	return client, server, token
}

func statement() Statement {
	return Statement{SQL: "SELECT 9007199254740993", User: "team-alice", Catalog: "delta", Schema: "analytics", TraceToken: "query-one", MaxRunSeconds: 60}
}

func TestDirectProtocolPreservesExactValuesAndRotatesToken(t *testing.T) {
	var calls atomic.Int32
	var origin string
	client, server, token := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		call := calls.Add(1)
		if r.Header.Get("X-Trino-User") != "team-alice" {
			t.Error("wrong execution identity")
		}
		switch call {
		case 1:
			if r.Method != "POST" || r.URL.Path != "/v1/statement" {
				t.Error("wrong submit route")
			}
			if r.Header.Get("Authorization") != "Bearer engine-token-one" || r.Header.Get("X-Trino-Catalog") != "delta" || r.Header.Get("X-Trino-Session") != "" {
				t.Error("missing approved context")
			}
			fmt.Fprintf(w, `{"id":"query_123","nextUri":%q}`, origin+"/v1/statement/executing/query_123/token/1")
		case 2:
			if r.Method != "GET" || r.Header.Get("Authorization") != "Bearer engine-token-two" {
				t.Error("rotated token not read")
			}
			fmt.Fprint(w, `{"id":"query_123","columns":[{"name":"count","type":"bigint"},{"name":"amount","type":"decimal(38,9)"}],"data":[[9007199254740993,"12345678901234567890.123456789"]]}`)
		default:
			t.Error("unexpected retry")
		}
	})
	origin = server.URL
	page, err := client.Submit(context.Background(), statement())
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(token, []byte("engine-token-two"), 0600); err != nil {
		t.Fatal(err)
	}
	page, err = client.Next(context.Background(), page.Cursor, "team-alice")
	if err != nil {
		t.Fatal(err)
	}
	if page.NextURI != "" || string(page.Rows[0][0]) != "9007199254740993" || string(page.Rows[0][1]) != `"12345678901234567890.123456789"` {
		t.Fatalf("precision or terminal protocol lost: %#v", page)
	}
}

func TestNoCredentialRedirectOrAutomaticPostRetry(t *testing.T) {
	var leaked, calls atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { leaked.Add(1) }))
	defer other.Close()
	client, _, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Redirect(w, r, other.URL, http.StatusTemporaryRedirect)
	})
	if _, err := client.Submit(context.Background(), statement()); !errors.Is(err, ErrTransport) {
		t.Fatalf("expected transport failure: %v", err)
	}
	if calls.Load() != 1 || leaked.Load() != 0 {
		t.Fatal("redirect or mutation retry occurred")
	}
}

func TestCursorCannotChangeOriginPathOrQueryIdentity(t *testing.T) {
	var calls atomic.Int32
	client, server, _ := testClient(t, func(http.ResponseWriter, *http.Request) { calls.Add(1) })
	for _, target := range []string{
		"https://other.example/v1/statement/executing/query_123/token/1",
		server.URL + "/v1/query/query_123",
		server.URL + "/v1/statement/executing/query_other/token/1",
		server.URL + "/v1/statement/../query_123",
		server.URL + "/v1/statement/%2e%2e/query_123",
		server.URL + "/v1/statement/executing/query_123/token/1?redirect=elsewhere",
		server.URL + "/v1/statement/executing/query_123/token/1#fragment",
	} {
		cursor := Cursor{ID: "query_123", NextURI: target}
		if _, err := client.Next(context.Background(), cursor, "team-alice"); !errors.Is(err, ErrProtocol) {
			t.Errorf("unsafe next link accepted %q: %v", target, err)
		}
		if err := client.Cancel(context.Background(), cursor, "team-alice"); !errors.Is(err, ErrProtocol) {
			t.Errorf("unsafe cancellation link accepted: %v", err)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("unsafe cursor touched network")
	}
}

func TestResponseFailuresNeverManufactureSuccess(t *testing.T) {
	responses := []string{
		`{}`, `{"id":"query_other"}`, `{"id":"query_123"} {}`,
		`{"id":"query_123","data":{"encoding":"json","segments":[]}}`,
		`{"id":"query_123","columns":[{"name":"x","type":"bigint"}],"data":[[1,2]]}`,
		`{"id":"query_123","nextUri":"https://other.example/v1/statement/executing/query_123/token/1"}`,
		strings.Repeat(" ", MaxPageBytes+1),
	}
	for _, response := range responses {
		t.Run(fmt.Sprint(len(response), "-", response[:min(32, len(response))]), func(t *testing.T) {
			client, server, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, response) })
			_, err := client.Next(context.Background(), Cursor{ID: "query_123", NextURI: server.URL + "/v1/statement/executing/query_123/token/1"}, "team-alice")
			if !errors.Is(err, ErrProtocol) {
				t.Fatalf("expected invalid response: %v", err)
			}
		})
	}
}

func TestEngineErrorRetainsCodeWithoutSensitiveMessage(t *testing.T) {
	client, _, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"id":"query_123","error":{"errorCode":4,"errorName":"PERMISSION_DENIED","errorType":"USER_ERROR","message":"private source row and SQL","failureInfo":{"stack":["secret"]}}}`)
	})
	page, err := client.Submit(context.Background(), statement())
	if err != nil || page.Error == nil || page.Error.Name != "PERMISSION_DENIED" {
		t.Fatalf("engine denial lost: %+v %v", page, err)
	}
}

func TestCancellationRequires204AndKeepsEngineIdentity(t *testing.T) {
	var calls atomic.Int32
	client, server, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "DELETE" || r.Header.Get("X-Trino-User") != "team-alice" {
			t.Error("cancellation identity mismatch")
		}
		if calls.Add(1) == 1 {
			w.WriteHeader(202)
		} else {
			w.WriteHeader(204)
		}
	})
	cursor := Cursor{ID: "query_123", NextURI: server.URL + "/v1/statement/executing/query_123/token/1"}
	if !errors.Is(client.Cancel(context.Background(), cursor, "team-alice"), ErrTransport) {
		t.Fatal("202 incorrectly treated as cancellation acknowledgement")
	}
	if err := client.Cancel(context.Background(), cursor, "team-alice"); err != nil {
		t.Fatal(err)
	}
}

func TestInvalidCredentialsAndHeadersFailBeforeNetwork(t *testing.T) {
	var calls atomic.Int32
	client, _, token := testClient(t, func(http.ResponseWriter, *http.Request) { calls.Add(1) })
	bad := statement()
	bad.User = "alice\r\nX-Trino-User:admin"
	if _, err := client.Submit(context.Background(), bad); err == nil {
		t.Fatal("header injection accepted")
	}
	if err := os.WriteFile(token, []byte("secret\ninjected"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Submit(context.Background(), statement()); !errors.Is(err, ErrCredentials) {
		t.Fatal("multiline token accepted")
	}
	if calls.Load() != 0 {
		t.Fatal("invalid credentials touched network")
	}
}
