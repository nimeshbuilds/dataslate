package trino

import (
	"context"
	"fmt"
	"net/http"
	"testing"
)

func TestObserveOwnerCheckedTerminalStatus(t *testing.T) {
	client, _, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.RequestURI() != "/v1/query/query_123?pruned=true" || r.Header.Get("X-Trino-User") != "team-alice" || r.Header.Get("Authorization") != "Bearer engine-token-one" {
			t.Error("wrong status request")
		}
		fmt.Fprint(w, `{"queryId":"query_123","state":"FAILED","session":{"user":"team-alice","secret":"private"},"errorCode":{"code":1,"name":"USER_CANCELED","type":"USER_ERROR"},"query":"secret SQL","failureInfo":{"message":"secret rows"}}`)
	})
	status, err := client.Observe(context.Background(), "query_123", "team-alice")
	if err != nil || status.State != "FAILED" || status.ErrorName != "USER_CANCELED" {
		t.Fatalf("terminal observation missing: %+v %v", status, err)
	}
}

func TestObserveRejectsForeignOrUnconfirmedStatus(t *testing.T) {
	cases := []struct {
		name, body string
		code       int
	}{
		{"foreign owner", `{"queryId":"query_123","state":"FINISHED","session":{"user":"other"}}`, 200},
		{"foreign query", `{"queryId":"other","state":"FINISHED","session":{"user":"team-alice"}}`, 200},
		{"missing owner", `{"queryId":"query_123","state":"FINISHED"}`, 200},
		{"unknown state", `{"queryId":"query_123","state":"CANCELLED","session":{"user":"team-alice"}}`, 200},
		{"failed without error", `{"queryId":"query_123","state":"FAILED","session":{"user":"team-alice"}}`, 200},
		{"retired", `{"queryId":"query_123","state":"FINISHED","session":{"user":"team-alice"}}`, 410},
		{"denied", `private`, 403},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, _, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(tc.code); fmt.Fprint(w, tc.body) })
			if _, err := client.Observe(context.Background(), "query_123", "team-alice"); err == nil {
				t.Fatal("unconfirmed termination accepted")
			}
		})
	}
}
