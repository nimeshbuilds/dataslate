// Test-only harness: copy beneath api/.qa before building so internal packages
// remain internal. Uses real handlers, PostgreSQL and the production model
// transport with a local HTTPS protocol fixture. Authentication is deliberately
// development-only; this is not the production executable or an OIDC check.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"sync"
	"syscall"
	"time"

	"dataslate.local/api/internal/llm"
	"dataslate.local/api/internal/platform"
)

type fixtureConfig struct {
	Owned    bool   `json:"owned"`
	Database string `json:"database"`
	AdminURL string `json:"adminURL"`
}

const harnessProfile = "dataslate-memory-workspace-v2"

type ownershipMarker struct {
	Profile  string `json:"profile"`
	Database string `json:"database"`
	Endpoint string `json:"databaseEndpoint"`
}

type harnessState struct {
	Profile      string `json:"profile"`
	Database     string `json:"database"`
	BindingHash  string `json:"bindingHash"`
	WorkspaceID  string `json:"workspaceId"`
	BindingID    string `json:"bindingId"`
	APIAddress   string `json:"apiAddress"`
	ProviderAddr string `json:"providerAddress"`
}

func privateRead(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Size() > 1<<20 {
		return nil, errors.New("qualification file must be a bounded private regular file")
	}
	return os.ReadFile(path)
}
func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return atomicPrivateWrite(path, append(data, '\n'))
}
func atomicPrivateWrite(path string, data []byte) error {
	if info, err := os.Lstat(path); err == nil {
		if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
			return errors.New("unsafe qualification output file")
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".qualification-write-")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	if err = file.Chmod(0600); err == nil {
		_, err = file.Write(data)
	}
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(name, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
func prepareDirectory(path string, marker ownershipMarker) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("state directory must be an absolute clean path")
	}
	err := os.Mkdir(path, 0700)
	if err == nil {
		return writeJSON(filepath.Join(path, "owner.json"), marker)
	}
	if !os.IsExist(err) {
		return errors.New("cannot create private qualification directory")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		return errors.New("existing state directory must be private and cannot be a symlink")
	}
	data, err := privateRead(filepath.Join(path, "owner.json"))
	if err != nil {
		return errors.New("existing directory has no valid qualification ownership marker")
	}
	var old ownershipMarker
	if json.Unmarshal(data, &old) != nil || old != marker {
		return errors.New("state directory belongs to a different database or qualification profile")
	}
	return nil
}

type evidenceSession struct {
	ID                         string     `json:"id"`
	StartedAt                  time.Time  `json:"startedAt"`
	EndedAt                    *time.Time `json:"endedAt,omitempty"`
	InterruptionDetectedAt     *time.Time `json:"interruptionDetectedAt,omitempty"`
	Outcome                    string     `json:"outcome"`
	Attempts                   int64      `json:"attempts"`
	Accepted                   int64      `json:"accepted"`
	Rejected                   int64      `json:"rejected"`
	MemoryCitationsTransferred int64      `json:"memoryCitationsTransferred"`
}
type evidenceDocument struct {
	Profile                    string            `json:"profile"`
	ProtocolFixture            bool              `json:"protocolFixture"`
	ExternalProvider           bool              `json:"externalProvider"`
	Attempts                   int64             `json:"attempts"`
	Accepted                   int64             `json:"accepted"`
	Rejected                   int64             `json:"rejected"`
	MemoryCitationsTransferred int64             `json:"memoryCitationsTransferred"`
	Sessions                   []evidenceSession `json:"sessions"`
}
type evidenceLog struct {
	sync.Mutex
	path string
	doc  evidenceDocument
}

func newEvidence(path string) (*evidenceLog, error) {
	e := &evidenceLog{path: path, doc: evidenceDocument{Profile: harnessProfile, ProtocolFixture: true, Sessions: []evidenceSession{}}}
	data, err := privateRead(path)
	if err == nil {
		if json.Unmarshal(data, &e.doc) != nil || e.doc.Profile != harnessProfile || !e.doc.ProtocolFixture || e.doc.ExternalProvider || len(e.doc.Sessions) > 127 || e.doc.Attempts < 0 || e.doc.Accepted < 0 || e.doc.Rejected < 0 || e.doc.Accepted+e.doc.Rejected > e.doc.Attempts || e.doc.MemoryCitationsTransferred < 0 {
			return nil, errors.New("retained provider evidence is invalid")
		}
		var attempts, accepted, rejected, memories int64
		for i := range e.doc.Sessions {
			s := &e.doc.Sessions[i]
			if s.ID == "" || s.Attempts < 0 || s.Accepted < 0 || s.Rejected < 0 || s.Accepted+s.Rejected > s.Attempts || s.MemoryCitationsTransferred < 0 {
				return nil, errors.New("retained provider session is invalid")
			}
			attempts += s.Attempts
			accepted += s.Accepted
			rejected += s.Rejected
			memories += s.MemoryCitationsTransferred
			if s.Outcome == "running" {
				now := time.Now().UTC()
				s.Outcome = "interrupted"
				s.InterruptionDetectedAt = &now
			}
		}
		if attempts != e.doc.Attempts || accepted != e.doc.Accepted || rejected != e.doc.Rejected || memories != e.doc.MemoryCitationsTransferred {
			return nil, errors.New("retained provider counters are inconsistent")
		}
	} else if !os.IsNotExist(err) {
		return nil, errors.New("cannot read provider evidence")
	}
	id := make([]byte, 16)
	if _, err = rand.Read(id); err != nil {
		return nil, err
	}
	e.doc.Sessions = append(e.doc.Sessions, evidenceSession{ID: hex.EncodeToString(id), StartedAt: time.Now().UTC(), Outcome: "running"})
	return e, writeJSON(e.path, e.doc)
}
func (e *evidenceLog) record(event string, memories int64) error {
	e.Lock()
	defer e.Unlock()
	s := &e.doc.Sessions[len(e.doc.Sessions)-1]
	switch event {
	case "attempt":
		e.doc.Attempts++
		s.Attempts++
	case "accepted":
		e.doc.Accepted++
		s.Accepted++
		e.doc.MemoryCitationsTransferred += memories
		s.MemoryCitationsTransferred += memories
	case "rejected":
		e.doc.Rejected++
		s.Rejected++
	default:
		return errors.New("invalid evidence event")
	}
	if err := writeJSON(e.path, e.doc); err != nil {
		return errors.New("cannot persist provider evidence")
	}
	return nil
}
func (e *evidenceLog) finish(outcome string) error {
	e.Lock()
	defer e.Unlock()
	s := &e.doc.Sessions[len(e.doc.Sessions)-1]
	now := time.Now().UTC()
	s.EndedAt = &now
	s.Outcome = outcome
	if err := writeJSON(e.path, e.doc); err != nil {
		return errors.New("cannot persist final provider evidence")
	}
	return nil
}
func exactKeys(value map[string]any, keys ...string) bool {
	if len(value) != len(keys) {
		return false
	}
	for _, key := range keys {
		if _, ok := value[key]; !ok {
			return false
		}
	}
	return true
}
func exactAnswerSchema(properties map[string]any) bool {
	citations, ok := properties["citations"].(map[string]any)
	if !ok {
		return false
	}
	items, ok := citations["items"].(map[string]any)
	if !ok {
		return false
	}
	expectedItems := map[string]any{"type": "string"}
	count := 0
	if raw, exists := items["enum"]; exists {
		ids, ok := raw.([]any)
		if !ok || len(ids) == 0 || len(ids) > llm.MaxCitations {
			return false
		}
		seen := map[string]bool{}
		for _, raw := range ids {
			id, ok := raw.(string)
			if !ok || id == "" || len(id) > 200 || seen[id] {
				return false
			}
			seen[id] = true
		}
		count = len(ids)
		expectedItems["enum"] = ids
	}
	code := map[string]any{"type": "object", "additionalProperties": false, "required": []any{"language", "source"}, "properties": map[string]any{"language": map[string]any{"type": "string", "enum": []any{"python", "sql"}}, "source": map[string]any{"type": "string"}}}
	expected := map[string]any{"answer": map[string]any{"type": "string"}, "citations": map[string]any{"type": "array", "items": expectedItems, "maxItems": float64(count)}, "proposedCode": map[string]any{"anyOf": []any{code, map[string]any{"type": "null"}}}}
	return reflect.DeepEqual(properties, expected)
}

func responsesInput(r *http.Request) (string, error) {
	var body map[string]any
	data, err := io.ReadAll(io.LimitReader(r.Body, (256<<10)+1))
	if err != nil || len(data) > 256<<10 {
		return "", errors.New("request exceeds bounds")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	if decoder.Decode(&body) != nil || decoder.Decode(new(any)) != io.EOF || !exactKeys(body, "model", "store", "stream", "instructions", "input", "max_output_tokens", "text") || body["model"] != "fixture-model" || body["store"] != false || body["stream"] != false || body["max_output_tokens"] != float64(1024) {
		return "", errors.New("request shape rejected")
	}
	instructions, ok := body["instructions"].(string)
	if !ok || instructions == "" {
		return "", errors.New("instructions missing")
	}
	text, ok := body["text"].(map[string]any)
	if !ok || !exactKeys(text, "format") {
		return "", errors.New("format missing")
	}
	format, ok := text["format"].(map[string]any)
	if !ok || !exactKeys(format, "type", "name", "strict", "schema") || format["type"] != "json_schema" || format["name"] != "sylon_answer" || format["strict"] != true {
		return "", errors.New("strict format missing")
	}
	schema, ok := format["schema"].(map[string]any)
	if !ok || !exactKeys(schema, "type", "additionalProperties", "required", "properties") || schema["type"] != "object" || schema["additionalProperties"] != false || !reflect.DeepEqual(schema["required"], []any{"answer", "citations", "proposedCode"}) {
		return "", errors.New("structured schema rejected")
	}
	properties, ok := schema["properties"].(map[string]any)
	if !ok || !exactKeys(properties, "answer", "citations", "proposedCode") || !exactAnswerSchema(properties) {
		return "", errors.New("answer schema rejected")
	}
	messages, ok := body["input"].([]any)
	if !ok || len(messages) != 1 {
		return "", errors.New("input missing")
	}
	message, ok := messages[0].(map[string]any)
	if !ok || !exactKeys(message, "type", "role", "content") || message["type"] != "message" || message["role"] != "user" {
		return "", errors.New("input message rejected")
	}
	content, ok := message["content"].([]any)
	if !ok || len(content) != 1 {
		return "", errors.New("input content rejected")
	}
	part, ok := content[0].(map[string]any)
	if !ok || !exactKeys(part, "type", "text") || part["type"] != "input_text" {
		return "", errors.New("input part rejected")
	}
	input, ok := part["text"].(string)
	if !ok || len(input)+len(instructions) > llm.MaxInputBytes {
		return "", errors.New("input text rejected")
	}
	var payload map[string]json.RawMessage
	if json.Unmarshal([]byte(input), &payload) != nil || len(payload) != 3 || payload["history"] == nil || payload["userPrompt"] == nil || payload["sources"] == nil {
		return "", errors.New("context rejected")
	}
	return input, nil
}

func retainProviderListener(provider *httptest.Server, address string) error {
	if address == "" {
		return nil
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil || host != "127.0.0.1" {
		return errors.New("invalid retained provider address")
	}
	_ = provider.Listener.Close()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return errors.New("retained provider port is unavailable")
	}
	// Preserve the non-nil, closed original on failure so deferred cleanup is safe.
	provider.Listener = listener
	return nil
}

func run() (resultErr error) {
	fixturePath := flag.String("fixture", "", "Private explicitly owned loopback TLS database fixture")
	directory := flag.String("state-dir", "", "Private retained harness state directory")
	address := flag.String("addr", "127.0.0.1:8080", "Loopback API address")
	flag.Parse()
	host, port, err := net.SplitHostPort(*address)
	portNumber, portErr := strconv.Atoi(port)
	if err != nil || portErr != nil || portNumber < 1 || portNumber > 65535 || host != "127.0.0.1" || *fixturePath == "" || *directory == "" {
		return errors.New("explicit fixture, state directory and literal loopback address required")
	}
	data, err := privateRead(*fixturePath)
	if err != nil {
		return errors.New("cannot read private fixture")
	}
	var fixture fixtureConfig
	if json.Unmarshal(data, &fixture) != nil || !fixture.Owned || !regexp.MustCompile(`^dataslate_[a-z_]+qualification$`).MatchString(fixture.Database) {
		return errors.New("an explicitly owned qualification database is required")
	}
	u, err := url.Parse(fixture.AdminURL)
	if err != nil || u.Scheme != "postgresql" || u.Hostname() != "127.0.0.1" || u.Port() == "" || u.Path != "/"+fixture.Database || u.Query().Get("sslmode") != "verify-full" || u.Query().Get("sslrootcert") == "" {
		return errors.New("only the named loopback database with verified TLS is accepted")
	}
	if err = prepareDirectory(*directory, ownershipMarker{Profile: harnessProfile, Database: fixture.Database, Endpoint: u.Host}); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var evidence *evidenceLog
	defer func() {
		if evidence != nil {
			outcome := "stopped"
			if resultErr != nil {
				outcome = "failed"
			}
			if e := evidence.finish(outcome); e != nil {
				resultErr = e
			}
		}
	}()
	startup, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	db, err := platform.OpenPostgres(startup, fixture.AdminURL, true)
	if err != nil {
		return errors.New("owned PostgreSQL TLS connection failed")
	}
	defer func() {
		done := make(chan struct{})
		go func() { db.Close(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			resultErr = errors.New("qualification database shutdown timed out")
		}
	}()
	if err = db.Migrate(startup); err != nil {
		return errors.New("owned qualification database migration failed")
	}
	statePath := filepath.Join(*directory, "state.json")
	state := harnessState{Profile: harnessProfile, Database: fixture.Database, BindingID: "memory-qualification", APIAddress: *address}
	if data, err = privateRead(statePath); err == nil {
		if json.Unmarshal(data, &state) != nil || state.Profile != harnessProfile || state.Database != fixture.Database || state.APIAddress != *address || state.WorkspaceID == "" || state.BindingID != "memory-qualification" {
			return errors.New("retained qualification state is invalid")
		}
	} else if !os.IsNotExist(err) {
		return errors.New("cannot read retained state")
	} else {
		workspace, e := db.CreateWorkspace(startup, "Memory browser qualification", fmt.Sprintf("memory-qa-%d", time.Now().UnixNano()))
		if e != nil {
			return errors.New("cannot create owned qualification workspace")
		}
		state.WorkspaceID = workspace.ID
	}
	if _, err = db.Workspace(startup, state.WorkspaceID); err != nil {
		return errors.New("retained workspace does not exist")
	}
	if err = writeJSON(statePath, state); err != nil {
		return errors.New("cannot retain workspace identity")
	}
	tokenPath := filepath.Join(*directory, "provider-token")
	token, err := privateRead(tokenPath)
	if os.IsNotExist(err) {
		random := make([]byte, 32)
		if _, err = rand.Read(random); err != nil {
			return err
		}
		token = []byte(hex.EncodeToString(random))
		err = atomicPrivateWrite(tokenPath, token)
	}
	if err != nil || len(token) != 64 {
		return errors.New("cannot prepare qualification provider token")
	}
	evidence, err = newEvidence(filepath.Join(*directory, "provider-evidence.json"))
	if err != nil {
		return err
	}

	errorsOut := make(chan error, 1)
	var failureOnce sync.Once
	reportFailure := func(err error) { failureOnce.Do(func() { errorsOut <- err }) }
	provider := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := evidence.record("attempt", 0); err != nil {
			reportFailure(err)
			http.Error(w, "fixture evidence unavailable", 500)
			return
		}
		reject := func(status int) {
			if err := evidence.record("rejected", 0); err != nil {
				reportFailure(err)
				status = 500
			}
			http.Error(w, "fixture request rejected", status)
		}
		if r.Method != "POST" || r.URL.Path != "/v1/responses" || r.Header.Get("Authorization") != "Bearer "+string(token) {
			reject(403)
			return
		}
		input, err := responsesInput(r)
		if err != nil {
			reject(422)
			return
		}
		// Count typed source citations without retaining private context text.
		var countMemory func(any) int64
		countMemory = func(value any) int64 {
			var n int64
			switch v := value.(type) {
			case map[string]any:
				if v["kind"] == "memory" {
					n++
				}
				for _, child := range v {
					n += countMemory(child)
				}
			case []any:
				for _, child := range v {
					n += countMemory(child)
				}
			}
			return n
		}
		var decoded any
		_ = json.Unmarshal([]byte(input), &decoded)
		if err := evidence.record("accepted", countMemory(decoded)); err != nil {
			reportFailure(err)
			http.Error(w, "fixture evidence unavailable", 500)
			return
		}
		answer, _ := json.Marshal(llm.Result{Answer: "Protocol fixture answer: report dates in UTC and review dataset permissions before use.", Citations: []string{}, ProposedCode: nil})
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"id": "fixture-response", "object": "response", "model": "fixture-model", "status": "completed", "output": []any{map[string]any{"type": "message", "role": "assistant", "status": "completed", "content": []any{map[string]any{"type": "output_text", "text": string(answer)}}}}})
	}))
	defer func() { _ = provider.Listener.Close() }()
	provider.Config.ReadHeaderTimeout = 5 * time.Second
	provider.Config.ReadTimeout = 10 * time.Second
	provider.Config.WriteTimeout = 10 * time.Second
	if err = retainProviderListener(provider, state.ProviderAddr); err != nil {
		return err
	}
	provider.StartTLS()
	defer provider.Close()
	state.ProviderAddr = provider.Listener.Addr().String()
	caPath := filepath.Join(*directory, "provider-ca.pem")
	if err = atomicPrivateWrite(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: provider.Certificate().Raw})); err != nil {
		return err
	}
	binding := platform.SylonBinding{ID: state.BindingID, WorkspaceID: state.WorkspaceID, Name: "Local protocol fixture", PolicyVersion: "memory-qa-v1", Provider: llm.Config{Protocol: "responses", Endpoint: provider.URL + "/v1/responses", Model: "fixture-model", MaxOutputTokens: 1024, CAFile: caPath, Auth: llm.Auth{BearerTokenFile: tokenPath}}, DataTransferAccepted: true, AllowedSubjects: []platform.ExecutionSubject{{Issuer: "development", Subject: "development-admin"}}, Context: platform.SylonContextPolicy{NotebookCellSource: true}, Memory: &platform.SylonMemoryPolicy{Enabled: true, DefaultTTLDays: 30, MaxTTLDays: 365}, MaxContextBytes: 98304}
	if err = binding.Validate(); err != nil {
		return errors.New("qualification binding rejected")
	}
	if state.BindingHash != "" && state.BindingHash != binding.Hash() {
		return errors.New("retained assistant profile changed")
	}
	state.BindingHash = binding.Hash()
	config := platform.Config{AuthMode: "development", Addr: *address, SylonBindings: []platform.SylonBinding{binding}}
	api := &platform.Server{Config: config, Store: db, Auth: platform.DevelopmentAuth{}}
	worker := &platform.SylonController{Config: config, Store: db, Generator: &platform.ProviderSylonGenerator{}}
	listener, err := net.Listen("tcp", *address)
	if err != nil {
		return errors.New("qualification API port is unavailable")
	}
	server := &http.Server{Handler: api.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 25 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	defer listener.Close()
	workerCtx, cancelWorker := context.WithCancel(ctx)
	workerDone := make(chan struct{})
	defer func() {
		cancelWorker()
		shutdown, done := context.WithTimeout(context.Background(), 10*time.Second)
		if err := server.Shutdown(shutdown); err != nil {
			resultErr = errors.New("qualification HTTP shutdown failed")
		}
		done()
		_ = server.Close()
		select {
		case <-workerDone:
		case <-time.After(10 * time.Second):
			resultErr = errors.New("qualification worker shutdown timed out")
		}
	}()
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			reportFailure(errors.New("qualification API serving failed"))
		}
	}()
	go func() {
		defer close(workerDone)
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-workerCtx.Done():
				return
			case <-ticker.C:
				check, done := context.WithTimeout(workerCtx, 115*time.Second)
				_, e := worker.ReconcileOnce(check, "memory-browser-qualification")
				done()
				if e != nil && workerCtx.Err() == nil {
					reportFailure(errors.New("qualification reconciliation failed"))
					return
				}
			}
		}
	}()
	if err = writeJSON(statePath, state); err != nil {
		return errors.New("cannot retain qualification state")
	}
	fmt.Println("Memory qualification ready: real PostgreSQL, loopback development identity, local HTTPS protocol fixture.")
	select {
	case <-ctx.Done():
	case err = <-errorsOut:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	return nil
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
