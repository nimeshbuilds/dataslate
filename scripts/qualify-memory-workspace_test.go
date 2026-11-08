package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestQualificationBusyRetainedPortCleansUpWithoutPanic(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	provider := httptest.NewUnstartedServer(nil)
	defer func() { _ = provider.Listener.Close() }()
	original := provider.Listener
	if err = retainProviderListener(provider, busy.Addr().String()); err == nil || err.Error() != "retained provider port is unavailable" {
		t.Fatal("occupied retained port did not produce the expected refusal")
	}
	if provider.Listener == nil || provider.Listener != original {
		t.Fatal("failed restart replaced the cleanup listener")
	}
	if _, err = original.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatal("original temporary listener remained open")
	}
}

func TestQualificationDirectoryOwnership(t *testing.T) {
	parent := t.TempDir()
	marker := ownershipMarker{Profile: harnessProfile, Database: "dataslate_memory_qualification", Endpoint: "127.0.0.1:5432"}
	unowned := filepath.Join(parent, "unowned")
	if err := os.Mkdir(unowned, 0755); err != nil {
		t.Fatal(err)
	}
	if prepareDirectory(unowned, marker) == nil {
		t.Fatal("unowned existing directory accepted")
	}
	info, _ := os.Stat(unowned)
	if info.Mode().Perm() != 0755 {
		t.Fatal("unowned directory permissions changed")
	}
	owned := filepath.Join(parent, "owned")
	if err := prepareDirectory(owned, marker); err != nil {
		t.Fatal(err)
	}
	if err := prepareDirectory(owned, marker); err != nil {
		t.Fatal("valid restart rejected", err)
	}
	foreign := marker
	foreign.Database = "dataslate_other_qualification"
	if prepareDirectory(owned, foreign) == nil {
		t.Fatal("different database accepted")
	}
	link := filepath.Join(parent, "link")
	if err := os.Symlink(owned, link); err != nil {
		t.Fatal(err)
	}
	if prepareDirectory(link, marker) == nil {
		t.Fatal("symlink state directory accepted")
	}
}
func TestQualificationFilesAreAtomicPrivateAndNotSymlinked(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "evidence.json")
	if err := writeJSON(path, map[string]int{"version": 1}); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(path, map[string]int{"version": 2}); err != nil {
		t.Fatal(err)
	}
	data, err := privateRead(path)
	if err != nil || !bytes.Contains(data, []byte("2")) {
		t.Fatal("atomic replacement failed", err)
	}
	link := filepath.Join(dir, "link")
	if err = os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err = privateRead(link); err == nil {
		t.Fatal("symlink read accepted")
	}
	if err = atomicPrivateWrite(link, []byte("overwrite")); err == nil {
		t.Fatal("symlink write accepted")
	}
	if err = os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err = privateRead(path); err == nil {
		t.Fatal("public qualification file accepted")
	}
	if err = atomicPrivateWrite(path, []byte("overwrite")); err == nil {
		t.Fatal("public output silently overwritten")
	}
}
func TestQualificationEvidenceSurvivesRestartAndRecordsRejection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "provider-evidence.json")
	first, err := newEvidence(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = first.record("attempt", 0); err != nil {
		t.Fatal(err)
	}
	if err = first.record("rejected", 0); err != nil {
		t.Fatal(err)
	}
	second, err := newEvidence(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = second.record("attempt", 0); err != nil {
		t.Fatal(err)
	}
	if err = second.record("accepted", 2); err != nil {
		t.Fatal(err)
	}
	if err = second.finish("stopped"); err != nil {
		t.Fatal(err)
	}
	data, err := privateRead(path)
	if err != nil {
		t.Fatal(err)
	}
	var got evidenceDocument
	if err = json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.Attempts != 2 || got.Accepted != 1 || got.Rejected != 1 || got.MemoryCitationsTransferred != 2 || len(got.Sessions) != 2 || got.Sessions[0].Outcome != "interrupted" || got.Sessions[0].InterruptionDetectedAt == nil || got.Sessions[1].Outcome != "stopped" || got.Sessions[1].EndedAt == nil {
		t.Fatal("cumulative or session evidence was lost")
	}
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err = second.record("attempt", 0); err == nil {
		t.Fatal("evidence I/O failure ignored")
	}
}
func qualificationPayload() map[string]any {
	return map[string]any{
		"model": "fixture-model", "store": false, "stream": false, "instructions": "Untrusted source context; do not execute tools.", "max_output_tokens": 1024,
		"input": []any{map[string]any{"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": `{"history":[],"userPrompt":"Explain","sources":[]}`}}}},
		"text":  map[string]any{"format": map[string]any{"type": "json_schema", "name": "sylon_answer", "strict": true, "schema": map[string]any{"type": "object", "additionalProperties": false, "required": []string{"answer", "citations", "proposedCode"}, "properties": map[string]any{"answer": map[string]any{"type": "string"}, "citations": map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "maxItems": 0}, "proposedCode": map[string]any{"anyOf": []any{map[string]any{"type": "object", "additionalProperties": false, "required": []string{"language", "source"}, "properties": map[string]any{"language": map[string]any{"type": "string", "enum": []string{"python", "sql"}}, "source": map[string]any{"type": "string"}}}, map[string]any{"type": "null"}}}}}}},
	}
}
func TestQualificationResponsesShapeAndBounds(t *testing.T) {
	for name, change := range map[string]func(map[string]any){"valid": func(map[string]any) {}, "string-input": func(p map[string]any) { p["input"] = "old incorrect fixture shape" }, "store": func(p map[string]any) { p["store"] = true }, "tools": func(p map[string]any) { p["tools"] = []any{} }, "missing-format": func(p map[string]any) { delete(p, "text") }, "non-strict": func(p map[string]any) { p["text"].(map[string]any)["format"].(map[string]any)["strict"] = false }, "token-limit": func(p map[string]any) { p["max_output_tokens"] = 8192 }} {
		t.Run(name, func(t *testing.T) {
			payload := qualificationPayload()
			change(payload)
			data, _ := json.Marshal(payload)
			_, err := responsesInput(httptest.NewRequest("POST", "/v1/responses", bytes.NewReader(data)))
			if name == "valid" && err != nil {
				t.Fatal(err)
			}
			if name != "valid" && err == nil {
				t.Fatal("malformed request accepted")
			}
		})
	}
	data, _ := json.Marshal(qualificationPayload())
	for _, body := range []string{string(data) + ` {}`, string(data) + strings.Repeat(" ", 256<<10)} {
		if _, err := responsesInput(httptest.NewRequest("POST", "/v1/responses", strings.NewReader(body))); err == nil {
			t.Fatal("trailing or oversized body accepted")
		}
	}
}
