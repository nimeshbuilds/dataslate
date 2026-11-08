// Package airflow implements the pinned Airflow 3.4 public API. It never
// forwards caller tokens, retries a trigger, reads logs/XCom, or cancels workers.
package airflow

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
)

var ErrUnavailable = errors.New("Airflow request is unconfirmed")
var ErrProtocol = errors.New("Airflow response does not match the approved workflow")
var ErrNotFound = errors.New("Airflow resource was not found")
var ErrDenied = errors.New("Airflow denied the request")
var ErrConflict = errors.New("Airflow run identity already exists")
var idPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,199}$`)
var runPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:+@=-]{0,249}$`)
var bundlePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:/@+-]{0,255}$`)
var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

type Config struct{ Endpoint, CAFile, TokenFile string }
type Client struct {
	base      string
	tokenFile string
	http      *http.Client
}

func New(cfg Config) (*Client, error) {
	u, e := url.Parse(cfg.Endpoint)
	if e != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" || (u.Path != "" && u.Path != "/") || cfg.TokenFile == "" {
		return nil, ErrProtocol
	}
	roots, e := x509.SystemCertPool()
	if e != nil {
		return nil, ErrUnavailable
	}
	if cfg.CAFile != "" {
		pem, e := os.ReadFile(cfg.CAFile)
		if e != nil || !roots.AppendCertsFromPEM(pem) {
			return nil, ErrUnavailable
		}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}
	transport.ResponseHeaderTimeout = 10 * time.Second
	return &Client{base: strings.TrimSuffix(cfg.Endpoint, "/"), tokenFile: cfg.TokenFile, http: &http.Client{Transport: transport, Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (c *Client) Close() { c.http.CloseIdleConnections() }
func (c *Client) request(ctx context.Context, method, path string, body any, out any) error {
	token, e := os.ReadFile(c.tokenFile)
	if e != nil || len(token) == 0 || len(token) > 16384 {
		return ErrUnavailable
	}
	value := strings.TrimSpace(string(token))
	if value == "" || strings.ContainsAny(value, "\r\n \t") {
		return ErrUnavailable
	}
	var payload []byte
	if body != nil {
		payload, e = json.Marshal(body)
		if e != nil {
			return ErrProtocol
		}
	}
	req, e := http.NewRequestWithContext(ctx, method, c.base+"/api/v2"+path, bytes.NewReader(payload))
	if e != nil {
		return ErrProtocol
	}
	req.Header.Set("Authorization", "Bearer "+value)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, e := c.http.Do(req)
	if e != nil {
		return ErrUnavailable
	}
	defer res.Body.Close()
	if res.StatusCode == 404 {
		return ErrNotFound
	}
	if res.StatusCode == 401 || res.StatusCode == 403 {
		return ErrDenied
	}
	if res.StatusCode == 409 {
		return ErrConflict
	}
	if res.StatusCode != 200 && res.StatusCode != 201 {
		return ErrUnavailable
	}
	data, e := io.ReadAll(io.LimitReader(res.Body, 2*1024*1024+1))
	if e != nil || len(data) > 2*1024*1024 || !strings.HasPrefix(strings.ToLower(res.Header.Get("Content-Type")), "application/json") {
		return ErrProtocol
	}
	if e = uniqueJSON(data); e != nil {
		return e
	}
	d := json.NewDecoder(bytes.NewReader(data))
	if d.Decode(out) != nil || d.Decode(&struct{}{}) != io.EOF {
		return ErrProtocol
	}
	return nil
}

// Reject ambiguous duplicate JSON keys at every object level, including conf.
func uniqueJSON(data []byte) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	var read func(int) error
	read = func(depth int) error {
		if depth > 32 {
			return ErrProtocol
		}
		t, e := d.Token()
		if e != nil {
			return ErrProtocol
		}
		switch t {
		case json.Delim('{'):
			seen := map[string]bool{}
			for d.More() {
				k, e := d.Token()
				s, ok := k.(string)
				if e != nil || !ok || seen[s] {
					return ErrProtocol
				}
				seen[s] = true
				if e = read(depth + 1); e != nil {
					return e
				}
			}
			t, e = d.Token()
			if e != nil || t != json.Delim('}') {
				return ErrProtocol
			}
		case json.Delim('['):
			for d.More() {
				if e = read(depth + 1); e != nil {
					return e
				}
			}
			t, e = d.Token()
			if e != nil || t != json.Delim(']') {
				return ErrProtocol
			}
		}
		return nil
	}
	if e := read(0); e != nil {
		return e
	}
	if _, e := d.Token(); e != io.EOF {
		return ErrProtocol
	}
	return nil
}

type Version struct {
	ID            string `json:"id"`
	Number        int    `json:"version_number"`
	DAGID         string `json:"dag_id"`
	BundleVersion string `json:"bundle_version"`
}
type Target struct {
	DAGID         string
	DAGVersion    int
	BundleVersion string
}

func (t Target) Valid() bool {
	return idPattern.MatchString(t.DAGID) && t.DAGVersion > 0 && t.DAGVersion <= 1000000 && bundlePattern.MatchString(t.BundleVersion)
}

type Task struct {
	ID                string   `json:"id"`
	DownstreamTaskIDs []string `json:"downstreamTaskIds"`
}
type Definition struct {
	DAGID                          string
	Version                        Version
	Paused, Stale, HasImportErrors bool
	ScheduleSummary, Timezone      string
	NextRunAt                      *time.Time
	Tasks                          []Task
}

func (c *Client) Definition(ctx context.Context, t Target) (Definition, error) {
	if !t.Valid() {
		return Definition{}, ErrProtocol
	}
	var version struct {
		Version string `json:"version"`
	}
	if e := c.request(ctx, "GET", "/version", nil, &version); e != nil {
		return Definition{}, e
	}
	if version.Version != "3.4.0" {
		return Definition{}, ErrProtocol
	}
	var dag struct {
		DAGID           string     `json:"dag_id"`
		Paused          *bool      `json:"is_paused"`
		Stale           *bool      `json:"is_stale"`
		ImportErrors    *bool      `json:"has_import_errors"`
		ScheduleSummary *string    `json:"timetable_summary"`
		Timezone        string     `json:"timezone"`
		NextRunAt       *time.Time `json:"next_dagrun_run_after"`
		Version         *Version   `json:"latest_dag_version"`
	}
	path := "/dags/" + t.DAGID
	if e := c.request(ctx, "GET", path+"/details", nil, &dag); e != nil {
		return Definition{}, e
	}
	if dag.DAGID != t.DAGID || dag.Paused == nil || dag.Stale == nil || dag.ImportErrors == nil || dag.Version == nil || !validVersion(*dag.Version, t.DAGID) || len(dag.Timezone) > 128 {
		return Definition{}, ErrProtocol
	}
	summary := ""
	if dag.ScheduleSummary != nil {
		summary = *dag.ScheduleSummary
	}
	if len(summary) > 512 {
		return Definition{}, ErrProtocol
	}
	var tasks struct {
		Tasks []struct {
			ID         string   `json:"task_id"`
			Downstream []string `json:"downstream_task_ids"`
		} `json:"tasks"`
		Total *int `json:"total_entries"`
	}
	if e := c.request(ctx, "GET", path+"/tasks", nil, &tasks); e != nil {
		return Definition{}, e
	}
	if tasks.Total == nil || *tasks.Total != len(tasks.Tasks) || len(tasks.Tasks) > 200 {
		return Definition{}, ErrProtocol
	}
	out := Definition{DAGID: t.DAGID, Version: *dag.Version, Paused: *dag.Paused, Stale: *dag.Stale, HasImportErrors: *dag.ImportErrors, ScheduleSummary: summary, Timezone: dag.Timezone, NextRunAt: dag.NextRunAt, Tasks: []Task{}}
	seen := map[string]bool{}
	for _, task := range tasks.Tasks {
		if !idPattern.MatchString(task.ID) || seen[task.ID] || len(task.Downstream) > 200 {
			return Definition{}, ErrProtocol
		}
		seen[task.ID] = true
		ds := append([]string{}, task.Downstream...)
		out.Tasks = append(out.Tasks, Task{task.ID, ds})
	}
	for _, task := range out.Tasks {
		ds := map[string]bool{}
		for _, id := range task.DownstreamTaskIDs {
			if !seen[id] || ds[id] || id == task.ID {
				return Definition{}, ErrProtocol
			}
			ds[id] = true
		}
	}
	confirmed := dag
	confirmed.DAGID = ""
	confirmed.Version = nil
	confirmed.Paused = nil
	confirmed.Stale = nil
	confirmed.ImportErrors = nil
	confirmed.ScheduleSummary = nil
	confirmed.Timezone = ""
	confirmed.NextRunAt = nil
	if e := c.request(ctx, "GET", path+"/details", nil, &confirmed); e != nil {
		return Definition{}, e
	}
	if confirmed.DAGID != t.DAGID || confirmed.Version == nil || *confirmed.Version != out.Version || confirmed.Paused == nil || confirmed.Stale == nil || confirmed.ImportErrors == nil || len(confirmed.Timezone) > 128 {
		return Definition{}, ErrProtocol
	}
	out.Paused = *confirmed.Paused
	out.Stale = *confirmed.Stale
	out.HasImportErrors = *confirmed.ImportErrors
	out.ScheduleSummary = ""
	if confirmed.ScheduleSummary != nil {
		out.ScheduleSummary = *confirmed.ScheduleSummary
	}
	if len(out.ScheduleSummary) > 512 {
		return Definition{}, ErrProtocol
	}
	out.Timezone = confirmed.Timezone
	out.NextRunAt = confirmed.NextRunAt
	return out, nil
}
func validVersion(v Version, dag string) bool {
	return uuidPattern.MatchString(v.ID) && v.Number > 0 && v.Number <= 1000000 && v.DAGID == dag && bundlePattern.MatchString(v.BundleVersion)
}
func (d Definition) Matches(t Target) bool {
	return d.DAGID == t.DAGID && d.Version.Number == t.DAGVersion && d.Version.BundleVersion == t.BundleVersion
}

type Receipt struct {
	RunID         string `json:"runId"`
	WorkspaceID   string `json:"workspaceId"`
	BindingID     string `json:"bindingId"`
	PolicyVersion string `json:"policyVersion"`
}
type Conf struct {
	DataSlate Receipt `json:"dataslate"`
}
type Run struct {
	DAGID         string          `json:"dag_id"`
	RunID         string          `json:"dag_run_id"`
	State         string          `json:"state"`
	RunType       string          `json:"run_type"`
	Versions      []Version       `json:"dag_versions"`
	BundleVersion string          `json:"bundle_version"`
	Conf          json.RawMessage `json:"conf"`
	LogicalDate   *time.Time      `json:"logical_date"`
	StartedAt     *time.Time      `json:"start_date"`
	EndedAt       *time.Time      `json:"end_date"`
}

func (r Run) validate(dag, id string) error {
	if r.DAGID != dag || !runPattern.MatchString(r.RunID) || (id != "" && r.RunID != id) || len(r.Versions) != 1 || !validVersion(r.Versions[0], dag) || r.BundleVersion != r.Versions[0].BundleVersion {
		return ErrProtocol
	}
	switch r.State {
	case "queued", "running", "success", "failed":
	default:
		return ErrProtocol
	}
	switch r.RunType {
	case "manual", "scheduled", "backfill", "asset_triggered":
	default:
		return ErrProtocol
	}
	return nil
}
func (r Run) Matches(t Target, receipt Receipt) bool {
	if r.validate(t.DAGID, "") != nil || r.RunType != "manual" || r.Versions[0].Number != t.DAGVersion || r.BundleVersion != t.BundleVersion {
		return false
	}
	var conf Conf
	d := json.NewDecoder(bytes.NewReader(r.Conf))
	d.DisallowUnknownFields()
	return d.Decode(&conf) == nil && d.Decode(&struct{}{}) == io.EOF && conf.DataSlate == receipt
}
func (c *Client) Trigger(ctx context.Context, t Target, id string, receipt Receipt) (Run, error) {
	if !t.Valid() || !idPattern.MatchString(id) {
		return Run{}, ErrProtocol
	}
	body := map[string]any{"dag_run_id": id, "logical_date": nil, "bundle_version": t.BundleVersion, "conf": Conf{receipt}}
	var run Run
	e := c.request(ctx, "POST", "/dags/"+t.DAGID+"/dagRuns", body, &run)
	if e != nil {
		return Run{}, e
	}
	if run.validate(t.DAGID, id) != nil || !run.Matches(t, receipt) {
		return Run{}, ErrProtocol
	}
	return run, nil
}
func (c *Client) Run(ctx context.Context, t Target, id string, receipt Receipt) (Run, error) {
	if !t.Valid() || !idPattern.MatchString(id) {
		return Run{}, ErrProtocol
	}
	var run Run
	e := c.request(ctx, "GET", "/dags/"+t.DAGID+"/dagRuns/"+id, nil, &run)
	if e != nil {
		return Run{}, e
	}
	if run.validate(t.DAGID, id) != nil || !run.Matches(t, receipt) {
		return Run{}, ErrProtocol
	}
	return run, nil
}
func (c *Client) History(ctx context.Context, t Target) ([]Run, bool, error) {
	if !t.Valid() {
		return nil, false, ErrProtocol
	}
	var response struct {
		Runs  []Run `json:"dag_runs"`
		Total *int  `json:"total_entries"`
	}
	e := c.request(ctx, "GET", "/dags/"+t.DAGID+"/dagRuns?limit=50&order_by=-run_after", nil, &response)
	if e != nil {
		return nil, false, e
	}
	if response.Total == nil || *response.Total < len(response.Runs) || len(response.Runs) > 50 {
		return nil, false, ErrProtocol
	}
	seen := map[string]bool{}
	for _, run := range response.Runs {
		if run.validate(t.DAGID, "") != nil || seen[run.RunID] {
			return nil, false, ErrProtocol
		}
		seen[run.RunID] = true
	}
	return response.Runs, *response.Total > len(response.Runs), nil
}

type TaskInstance struct {
	ID        string     `json:"id"`
	MapIndex  int        `json:"mapIndex"`
	TryNumber int        `json:"tryNumber"`
	State     string     `json:"state"`
	StartedAt *time.Time `json:"startedAt,omitempty"`
	EndedAt   *time.Time `json:"endedAt,omitempty"`
}

func (c *Client) Tasks(ctx context.Context, t Target, id string) ([]TaskInstance, bool, error) {
	if !t.Valid() || !idPattern.MatchString(id) {
		return nil, false, ErrProtocol
	}
	var response struct {
		Tasks []struct {
			DAGID     string     `json:"dag_id"`
			RunID     string     `json:"dag_run_id"`
			ID        string     `json:"task_id"`
			MapIndex  int        `json:"map_index"`
			TryNumber int        `json:"try_number"`
			State     *string    `json:"state"`
			StartedAt *time.Time `json:"start_date"`
			EndedAt   *time.Time `json:"end_date"`
		} `json:"task_instances"`
		Total *int `json:"total_entries"`
	}
	e := c.request(ctx, "GET", "/dags/"+t.DAGID+"/dagRuns/"+id+"/taskInstances?limit=200", nil, &response)
	if e != nil {
		return nil, false, e
	}
	if response.Total == nil || *response.Total < len(response.Tasks) || len(response.Tasks) > 200 {
		return nil, false, ErrProtocol
	}
	out := []TaskInstance{}
	seen := map[string]map[int]bool{}
	for _, task := range response.Tasks {
		if task.DAGID != t.DAGID || task.RunID != id || !idPattern.MatchString(task.ID) || task.MapIndex < -1 || task.TryNumber < 0 || task.TryNumber > 10000 {
			return nil, false, ErrProtocol
		}
		if seen[task.ID] == nil {
			seen[task.ID] = map[int]bool{}
		}
		if seen[task.ID][task.MapIndex] {
			return nil, false, ErrProtocol
		}
		seen[task.ID][task.MapIndex] = true
		state := "pending"
		if task.State != nil {
			state = *task.State
		}
		switch state {
		case "pending", "removed", "scheduled", "queued", "running", "success", "restarting", "failed", "up_for_retry", "up_for_reschedule", "upstream_failed", "skipped", "deferred":
		default:
			return nil, false, ErrProtocol
		}
		out = append(out, TaskInstance{task.ID, task.MapIndex, task.TryNumber, state, task.StartedAt, task.EndedAt})
	}
	return out, *response.Total > len(out), nil
}
