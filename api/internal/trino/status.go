package trino

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
)

// QueryStatus is an owner-checked observation, independent of statement cursor
// retention. It never contains SQL, sessions, plans, traces or row values.
// This endpoint is pinned to the Trino 484 coordinator API.
type QueryStatus struct {
	ID        string
	State     string
	ErrorCode int
	ErrorName string
	ErrorType string
}

func (c *Client) Observe(ctx context.Context, id, user string) (QueryStatus, error) {
	if !queryID.MatchString(id) || !headerValue.MatchString(user) {
		return QueryStatus{}, ErrProtocol
	}
	token, err := c.token()
	if err != nil {
		return QueryStatus{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base.String()+"/v1/query/"+id+"?pruned=true", nil)
	if err != nil {
		return QueryStatus{}, ErrTransport
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Trino-User", user)
	req.Header.Set("Accept", "application/json")
	response, err := c.http.Do(req)
	if err != nil {
		return QueryStatus{}, ErrTransport
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return QueryStatus{}, ErrTransport
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, MaxPageBytes+1))
	if err != nil {
		return QueryStatus{}, ErrTransport
	}
	if len(raw) > MaxPageBytes {
		return QueryStatus{}, ErrProtocol
	}
	var value struct {
		ID      string `json:"queryId"`
		State   string `json:"state"`
		Session struct {
			User string `json:"user"`
		} `json:"session"`
		ErrorCode *struct {
			Code int    `json:"code"`
			Name string `json:"name"`
			Type string `json:"type"`
		} `json:"errorCode"`
	}
	if json.Unmarshal(raw, &value) != nil || value.ID != id || value.Session.User != user {
		return QueryStatus{}, ErrProtocol
	}
	switch value.State {
	case "QUEUED", "WAITING_FOR_RESOURCES", "DISPATCHING", "PLANNING", "STARTING", "RUNNING", "FINISHING", "FINISHED", "FAILED":
	default:
		return QueryStatus{}, ErrProtocol
	}
	status := QueryStatus{ID: id, State: value.State}
	if value.ErrorCode != nil {
		if value.ErrorCode.Code < 0 || !headerValue.MatchString(value.ErrorCode.Name) || !headerValue.MatchString(value.ErrorCode.Type) {
			return QueryStatus{}, ErrProtocol
		}
		status.ErrorCode, status.ErrorName, status.ErrorType = value.ErrorCode.Code, value.ErrorCode.Name, value.ErrorCode.Type
	}
	if value.State == "FAILED" && value.ErrorCode == nil {
		return QueryStatus{}, ErrProtocol
	}
	if value.State == "FINISHED" && value.ErrorCode != nil {
		return QueryStatus{}, ErrProtocol
	}
	return status, nil
}
