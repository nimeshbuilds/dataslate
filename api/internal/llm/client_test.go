package llm

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const goodAnswer = `{"answer":"Use a bounded SQL query.","citations":["context-1"],"proposedCode":{"language":"sql","source":"SELECT 1"}}`

func fixtureRequest() Request {
	return Request{Instructions: "Use only the authorized context. Propose code; do not execute it.", Input: "Explain this notebook", AllowedCitationIDs: []string{"context-1"}}
}
func responseBody(answer string) map[string]any {
	return map[string]any{"object": "response", "model": "fixture-model", "status": "completed", "store": false,
		"output": []any{map[string]any{"type": "reasoning", "summary": []any{}}, map[string]any{"type": "message", "status": "completed", "role": "assistant", "content": []any{map[string]any{"type": "output_text", "text": answer, "annotations": []any{}}}}},
		"usage":  map[string]any{"input_tokens": 12, "output_tokens": 20, "total_tokens": 32}}
}
func chatBody(answer string) map[string]any {
	return map[string]any{"object": "chat.completion", "model": "fixture-model", "choices": []any{map[string]any{"index": 0, "finish_reason": "stop", "message": map[string]any{"role": "assistant", "content": answer, "refusal": nil}}}, "usage": map[string]any{"prompt_tokens": 12, "completion_tokens": 20, "total_tokens": 32}}
}
func fixture(t *testing.T, protocol string, handler http.HandlerFunc) (*Client, Config, *httptest.Server) {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.pem")
	token := filepath.Join(dir, "token")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(token, []byte("first-token"), 0600); err != nil {
		t.Fatal(err)
	}
	config := Config{Protocol: protocol, Endpoint: server.URL + "/exact/inference", Model: "fixture-model", MaxOutputTokens: 1024, CAFile: ca, Auth: Auth{BearerTokenFile: token}}
	client, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client, config, server
}
func checkError(t *testing.T, err error, ambiguous bool) *Error {
	t.Helper()
	var typed *Error
	if !errors.As(err, &typed) || typed.Ambiguous != ambiguous {
		t.Fatalf("wrong bounded error: %#v", err)
	}
	if strings.Contains(err.Error(), "CUSTOMERSECRET") || strings.Contains(err.Error(), "first-token") {
		t.Fatal("error leaked provider content")
	}
	return typed
}
func TestResponsesStructuredRequestAndResult(t *testing.T) {
	client, _, _ := fixture(t, "responses", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/exact/inference" || r.Method != "POST" || r.Header.Get("Authorization") != "Bearer first-token" {
			t.Error("wrong endpoint or authentication")
		}
		var body map[string]any
		if json.NewDecoder(r.Body).Decode(&body) != nil {
			t.Error("invalid body")
		}
		for _, key := range []string{"tools", "previous_response_id", "conversation", "temperature", "reasoning", "response_format"} {
			if _, ok := body[key]; ok {
				t.Errorf("unexpected field %s", key)
			}
		}
		if body["store"] != false || body["stream"] != false || body["max_output_tokens"] != float64(1024) || body["model"] != "fixture-model" {
			t.Error("wrong protocol controls")
		}
		format := body["text"].(map[string]any)["format"].(map[string]any)
		if format["strict"] != true || format["type"] != "json_schema" {
			t.Error("missing strict schema")
		}
		schema := format["schema"].(map[string]any)
		if schema["additionalProperties"] != false || len(schema["required"].([]any)) != 3 {
			t.Error("invalid structured schema")
		}
		json.NewEncoder(w).Encode(responseBody(goodAnswer))
	})
	out, err := client.Generate(context.Background(), fixtureRequest())
	if err != nil {
		t.Fatal(err)
	}
	if out.ProposedCode == nil || out.ProposedCode.Language != "sql" || out.ProposedCode.Source != "SELECT 1" || !reflect.DeepEqual(out.Citations, []string{"context-1"}) || out.Usage == nil || *out.Usage != (Usage{12, 20, 32}) {
		t.Fatalf("wrong safe projection: %+v", out)
	}
}
func TestChatStructuredRequestAndResult(t *testing.T) {
	client, _, _ := fixture(t, "chat-completions", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if body["store"] != false || body["stream"] != false || body["max_completion_tokens"] != float64(1024) {
			t.Error("wrong chat controls")
		}
		for _, key := range []string{"tools", "max_tokens", "max_output_tokens", "input", "reasoning_effort"} {
			if _, ok := body[key]; ok {
				t.Errorf("unexpected %s", key)
			}
		}
		messages := body["messages"].([]any)
		if len(messages) != 2 || messages[0].(map[string]any)["role"] != "developer" {
			t.Error("wrong message hierarchy")
		}
		format := body["response_format"].(map[string]any)
		if format["type"] != "json_schema" || format["json_schema"].(map[string]any)["strict"] != true {
			t.Error("missing strict schema")
		}
		json.NewEncoder(w).Encode(chatBody(goodAnswer))
	})
	out, err := client.Generate(context.Background(), fixtureRequest())
	if err != nil {
		t.Fatal(err)
	}
	if out.Usage == nil || out.Usage.TotalTokens != 32 || out.ProposedCode == nil {
		t.Fatal("chat projection failed")
	}
}
func TestProviderEnvelopeFailures(t *testing.T) {
	for _, protocol := range []string{"responses", "chat-completions"} {
		t.Run(protocol, func(t *testing.T) {
			base := responseBody
			if protocol == "chat-completions" {
				base = chatBody
			}
			cases := map[string]func(map[string]any){
				"wrong-model":   func(m map[string]any) { m["model"] = "unexpected-model" },
				"null-object":   func(m map[string]any) { m["object"] = nil },
				"partial-usage": func(m map[string]any) { m["usage"] = map[string]any{"total_tokens": 1} },
				"null-usage-number": func(m map[string]any) {
					m["usage"] = map[string]any{"input_tokens": nil, "output_tokens": 1, "total_tokens": 1}
				},
				"overflow-usage": func(m map[string]any) {
					m["usage"] = map[string]any{"input_tokens": 1e30, "output_tokens": 1, "total_tokens": 1}
				},
			}
			if protocol == "responses" {
				message := func(m map[string]any) map[string]any { return m["output"].([]any)[1].(map[string]any) }
				cases["incomplete"] = func(m map[string]any) { m["status"] = "incomplete" }
				cases["error"] = func(m map[string]any) { m["error"] = map[string]any{"message": "CUSTOMERSECRET"} }
				cases["incomplete-details"] = func(m map[string]any) { m["incomplete_details"] = map[string]any{"reason": "max_output_tokens"} }
				cases["stored"] = func(m map[string]any) { m["store"] = true }
				cases["tool"] = func(m map[string]any) {
					m["output"].([]any)[0] = map[string]any{"type": "function_call", "name": "execute"}
				}
				cases["unknown-output"] = func(m map[string]any) { m["output"].([]any)[0] = map[string]any{"type": "future-action"} }
				cases["reasoning-incomplete"] = func(m map[string]any) { m["output"].([]any)[0].(map[string]any)["status"] = "in_progress" }
				cases["message-incomplete"] = func(m map[string]any) { message(m)["status"] = "in_progress" }
				cases["wrong-role"] = func(m map[string]any) { message(m)["role"] = "user" }
				cases["refusal"] = func(m map[string]any) {
					message(m)["content"] = []any{map[string]any{"type": "refusal", "refusal": "CUSTOMERSECRET"}}
				}
				cases["null-text"] = func(m map[string]any) { message(m)["content"].([]any)[0].(map[string]any)["text"] = nil }
				cases["annotations"] = func(m map[string]any) {
					message(m)["content"].([]any)[0].(map[string]any)["annotations"] = []any{map[string]any{"url": "https://untrusted.example"}}
				}
				cases["multiple-messages"] = func(m map[string]any) { m["output"] = append(m["output"].([]any), message(m)) }
				cases["too-many-items"] = func(m map[string]any) { m["output"] = make([]any, 33) }
				cases["usage-over-budget"] = func(m map[string]any) {
					m["usage"] = map[string]any{"input_tokens": 1, "output_tokens": 9000, "total_tokens": 9001}
				}
				cases["usage-wrong-total"] = func(m map[string]any) {
					m["usage"] = map[string]any{"input_tokens": 1, "output_tokens": 2, "total_tokens": 4}
				}
			} else {
				choice := func(m map[string]any) map[string]any { return m["choices"].([]any)[0].(map[string]any) }
				message := func(m map[string]any) map[string]any { return choice(m)["message"].(map[string]any) }
				cases["length"] = func(m map[string]any) { choice(m)["finish_reason"] = "length" }
				cases["content-filter"] = func(m map[string]any) { choice(m)["finish_reason"] = "content_filter" }
				cases["null-index"] = func(m map[string]any) { choice(m)["index"] = nil }
				cases["wrong-index"] = func(m map[string]any) { choice(m)["index"] = 1 }
				cases["multiple-choices"] = func(m map[string]any) { m["choices"] = append(m["choices"].([]any), choice(m)) }
				cases["refusal"] = func(m map[string]any) { message(m)["refusal"] = "CUSTOMERSECRET" }
				cases["null-content"] = func(m map[string]any) { message(m)["content"] = nil }
				cases["wrong-role"] = func(m map[string]any) { message(m)["role"] = "tool" }
				cases["tool"] = func(m map[string]any) { message(m)["tool_calls"] = []any{map[string]any{"type": "function"}} }
				cases["function"] = func(m map[string]any) { message(m)["function_call"] = map[string]any{"name": "execute"} }
				cases["negative-usage"] = func(m map[string]any) {
					m["usage"] = map[string]any{"prompt_tokens": -1, "completion_tokens": 1, "total_tokens": 0}
				}
			}
			for name, mutate := range cases {
				t.Run(name, func(t *testing.T) {
					client, _, _ := fixture(t, protocol, func(w http.ResponseWriter, r *http.Request) {
						body := base(goodAnswer)
						mutate(body)
						json.NewEncoder(w).Encode(body)
					})
					_, err := client.Generate(context.Background(), fixtureRequest())
					checkError(t, err, true)
				})
			}
		})
	}
}
func TestStructuredAnswerValidation(t *testing.T) {
	cases := map[string]string{
		"invalid": "CUSTOMERSECRET", "null": "null", "array": "[]", "trailing": goodAnswer + "{}",
		"unknown":            `{"answer":"ok","citations":[],"proposedCode":null,"tool":"CUSTOMERSECRET"}`,
		"missing-answer":     `{"citations":[],"proposedCode":null}`,
		"null-answer":        `{"answer":null,"citations":[],"proposedCode":null}`,
		"blank-answer":       `{"answer":" ","citations":[],"proposedCode":null}`,
		"null-citations":     `{"answer":"ok","citations":null,"proposedCode":null}`,
		"missing-code":       `{"answer":"ok","citations":[]}`,
		"unknown-citation":   `{"answer":"ok","citations":["not-authorized"],"proposedCode":null}`,
		"url-citation":       `{"answer":"ok","citations":["https://untrusted.example"],"proposedCode":null}`,
		"duplicate-citation": `{"answer":"ok","citations":["context-1","context-1"],"proposedCode":null}`,
		"duplicate-field":    `{"answer":"CUSTOMERSECRET","answer":"ok","citations":[],"proposedCode":null}`,
		"wrong-language":     `{"answer":"ok","citations":[],"proposedCode":{"language":"shell","source":"echo test"}}`,
		"null-code-source":   `{"answer":"ok","citations":[],"proposedCode":{"language":"sql","source":null}}`,
		"null-code-language": `{"answer":"ok","citations":[],"proposedCode":{"language":null,"source":"SELECT 1"}}`,
		"empty-code":         `{"answer":"ok","citations":[],"proposedCode":{"language":"sql","source":""}}`,
		"code-extra-field":   `{"answer":"ok","citations":[],"proposedCode":{"language":"sql","source":"SELECT 1","execute":true}}`,
		"answer-limit":       fmt.Sprintf(`{"answer":%q,"citations":[],"proposedCode":null}`, strings.Repeat("a", MaxAnswerBytes+1)),
		"code-limit":         fmt.Sprintf(`{"answer":"ok","citations":[],"proposedCode":{"language":"python","source":%q}}`, strings.Repeat("x", MaxCodeBytes+1)),
		"nul-answer":         `{"answer":"a\u0000b","citations":[],"proposedCode":null}`,
	}
	for name, answer := range cases {
		t.Run(name, func(t *testing.T) {
			client, _, _ := fixture(t, "responses", func(w http.ResponseWriter, r *http.Request) { json.NewEncoder(w).Encode(responseBody(answer)) })
			_, err := client.Generate(context.Background(), fixtureRequest())
			checkError(t, err, true)
		})
	}
}
func TestResponseBoundsAndJSONAmbiguity(t *testing.T) {
	for name, body := range map[string]string{
		"oversize": strings.Repeat("x", maxResponse+1), "invalidUTF8": "\xff", "trailing": "{}{}",
		"duplicate": `{"object":"CUSTOMERSECRET","object":"response"}`,
		"deep":      strings.Repeat("[", 40) + "0" + strings.Repeat("]", 40),
	} {
		t.Run(name, func(t *testing.T) {
			client, _, _ := fixture(t, "responses", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body) })
			_, err := client.Generate(context.Background(), fixtureRequest())
			checkError(t, err, true)
		})
	}
}
func TestOptionalUsageAndNoCitations(t *testing.T) {
	for _, protocol := range []string{"responses", "chat-completions"} {
		for _, null := range []bool{false, true} {
			t.Run(fmt.Sprint(protocol, null), func(t *testing.T) {
				client, _, _ := fixture(t, protocol, func(w http.ResponseWriter, r *http.Request) {
					text := `{"answer":"An uncited answer.","citations":[],"proposedCode":null}`
					body := responseBody(text)
					if protocol == "chat-completions" {
						body = chatBody(text)
					}
					if null {
						body["usage"] = nil
					} else {
						delete(body, "usage")
					}
					json.NewEncoder(w).Encode(body)
				})
				in := fixtureRequest()
				in.AllowedCitationIDs = nil
				out, err := client.Generate(context.Background(), in)
				if err != nil || out.Usage != nil || out.Citations == nil || len(out.Citations) != 0 || out.ProposedCode != nil {
					t.Fatal("optional fields incorrectly projected", err)
				}
			})
		}
	}
}
func TestRequestAndConfigValidation(t *testing.T) {
	var requests atomic.Int32
	client, config, _ := fixture(t, "responses", func(w http.ResponseWriter, r *http.Request) { requests.Add(1) })
	for name, mutate := range map[string]func(*Request){
		"blank": func(r *Request) { r.Input = " " }, "empty-instructions": func(r *Request) { r.Instructions = "" },
		"too-large": func(r *Request) { r.Input = strings.Repeat("x", MaxInputBytes) }, "nul": func(r *Request) { r.Input = "a\x00b" },
		"invalidUTF8": func(r *Request) { r.Input = "\xff" }, "duplicate-id": func(r *Request) { r.AllowedCitationIDs = []string{"context-1", "context-1"} },
		"url-id": func(r *Request) { r.AllowedCitationIDs = []string{"https://bad.example"} }, "id-count": func(r *Request) { r.AllowedCitationIDs = make([]string, 21) },
	} {
		t.Run(name, func(t *testing.T) {
			in := fixtureRequest()
			mutate(&in)
			_, err := client.Generate(context.Background(), in)
			checkError(t, err, false)
		})
	}
	if requests.Load() != 0 {
		t.Fatal("invalid input dispatched")
	}
	for name, mutate := range map[string]func(*Config){
		"protocol": func(c *Config) { c.Protocol = "" }, "model": func(c *Config) { c.Model = "model\n" }, "low": func(c *Config) { c.MaxOutputTokens = 255 }, "high": func(c *Config) { c.MaxOutputTokens = 8193 },
		"http": func(c *Config) { c.Endpoint = "http://example.com/v1" }, "credentials": func(c *Config) { c.Endpoint = "https://token@example.com/v1" }, "query": func(c *Config) { c.Endpoint += "?key=x" },
		"traversal": func(c *Config) { c.Endpoint += "/../other" }, "escaped": func(c *Config) { c.Endpoint += "/%2F" }, "fragment": func(c *Config) { c.Endpoint += "#x" },
		"relative-token": func(c *Config) { c.Auth.BearerTokenFile = "relative" }, "relative-ca": func(c *Config) { c.CAFile = "relative" },
		"ambiguous-auth": func(c *Config) { c.Auth.ClientID = "client" }, "missing-auth": func(c *Config) { c.Auth = Auth{} },
	} {
		t.Run(name, func(t *testing.T) {
			c := config
			mutate(&c)
			if ValidateConfig(c) == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
}
func TestBearerRotationAndNoProviderRetry(t *testing.T) {
	var calls atomic.Int32
	var observed []string
	client, config, _ := fixture(t, "responses", func(w http.ResponseWriter, r *http.Request) {
		observed = append(observed, r.Header.Get("Authorization"))
		if calls.Add(1) == 1 {
			http.Error(w, "CUSTOMERSECRET", 429)
			return
		}
		json.NewEncoder(w).Encode(responseBody(goodAnswer))
	})
	_, err := client.Generate(context.Background(), fixtureRequest())
	e := checkError(t, err, true)
	if e.StatusCode != 429 || calls.Load() != 1 {
		t.Fatal("provider attempt replayed")
	}
	os.WriteFile(config.Auth.BearerTokenFile, []byte("rotated-token"), 0600)
	if _, err = client.Generate(context.Background(), fixtureRequest()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(observed, []string{"Bearer first-token", "Bearer rotated-token"}) {
		t.Fatal("bearer file not reread")
	}
}
func TestOAuthStylesCacheRotationAnd401(t *testing.T) {
	for _, style := range []string{"post", "basic"} {
		t.Run(style, func(t *testing.T) {
			var tokenCalls, providerCalls atomic.Int32
			client, config, server := fixture(t, "responses", func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/oauth" {
					count := tokenCalls.Add(1)
					r.ParseForm()
					if r.Form.Get("grant_type") != "client_credentials" || r.Form.Get("scope") != "sylon" {
						t.Error("wrong grant")
					}
					expected := "initial secret"
					if count > 1 {
						expected = "rotated secret"
					}
					if style == "basic" {
						id, secret, ok := r.BasicAuth()
						if !ok || id != url.QueryEscape("client id") || secret != url.QueryEscape(expected) {
							t.Error("wrong Basic auth")
						}
					} else if r.Form.Get("client_id") != "client id" || r.Form.Get("client_secret") != expected {
						t.Error("wrong POST auth")
					}
					json.NewEncoder(w).Encode(map[string]any{"access_token": fmt.Sprintf("token-%d", count), "token_type": "Bearer", "expires_in": 3600})
					return
				}
				count := providerCalls.Add(1)
				if count == 3 {
					http.Error(w, "CUSTOMERSECRET", 401)
					return
				}
				json.NewEncoder(w).Encode(responseBody(goodAnswer))
			})
			client.Close()
			secret := filepath.Join(t.TempDir(), "secret")
			os.WriteFile(secret, []byte("initial secret"), 0600)
			config.Auth = Auth{ClientID: "client id", ClientSecretFile: secret, TokenEndpoint: server.URL + "/oauth", Scope: "sylon", Style: style}
			client, err := New(config)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			for i := 0; i < 2; i++ {
				if _, err = client.Generate(context.Background(), fixtureRequest()); err != nil {
					t.Fatal(err)
				}
			}
			if tokenCalls.Load() != 1 {
				t.Fatal("OAuth cache not reused")
			}
			os.WriteFile(secret, []byte("rotated secret"), 0600)
			_, err = client.Generate(context.Background(), fixtureRequest())
			checkError(t, err, true)
			if tokenCalls.Load() != 2 || providerCalls.Load() != 3 {
				t.Fatal("rotation or 401 attempted hidden retry")
			}
			if _, err = client.Generate(context.Background(), fixtureRequest()); err != nil {
				t.Fatal(err)
			}
			if tokenCalls.Load() != 3 {
				t.Fatal("401 cache not cleared")
			}
		})
	}
}
func TestOAuthFailureBeforeGeneration(t *testing.T) {
	var generated atomic.Int32
	client, config, server := fixture(t, "responses", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth" {
			http.Error(w, "CUSTOMERSECRET", 403)
			return
		}
		generated.Add(1)
	})
	client.Close()
	secret := filepath.Join(t.TempDir(), "secret")
	os.WriteFile(secret, []byte("secret"), 0600)
	config.Auth = Auth{ClientID: "client", ClientSecretFile: secret, TokenEndpoint: server.URL + "/oauth"}
	client, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	_, err = client.Generate(context.Background(), fixtureRequest())
	checkError(t, err, false)
	if generated.Load() != 0 {
		t.Fatal("OAuth failure still dispatched generation")
	}
}
func TestNoRedirectOrAmbientProxy(t *testing.T) {
	var destination atomic.Int32
	other := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { destination.Add(1) }))
	defer other.Close()
	t.Setenv("HTTPS_PROXY", other.URL)
	t.Setenv("HTTP_PROXY", other.URL)
	client, _, _ := fixture(t, "responses", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, other.URL+"/leak", 307) })
	_, err := client.Generate(context.Background(), fixtureRequest())
	e := checkError(t, err, true)
	if e.StatusCode != 307 || destination.Load() != 0 {
		t.Fatal("redirect or ambient proxy followed")
	}
}
func TestTLSAndCancellation(t *testing.T) {
	var calls atomic.Int32
	client, config, _ := fixture(t, "responses", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		json.NewEncoder(w).Encode(responseBody(goodAnswer))
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := client.Generate(ctx, fixtureRequest())
	checkError(t, err, false)
	if calls.Load() != 0 {
		t.Fatal("canceled request sent")
	}
	config.CAFile = ""
	untrusted, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer untrusted.Close()
	_, err = untrusted.Generate(context.Background(), fixtureRequest())
	checkError(t, err, true)
	if calls.Load() != 0 {
		t.Fatal("unverified TLS accepted")
	}
}
func TestConnectionLossIsNotRetried(t *testing.T) {
	var calls atomic.Int32
	client, _, _ := fixture(t, "responses", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		connection, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		connection.Close()
	})
	_, err := client.Generate(context.Background(), fixtureRequest())
	checkError(t, err, true)
	if calls.Load() != 1 {
		t.Fatal("generation replayed after lost response")
	}
}
func TestConcurrentOAuthAndGeneration(t *testing.T) {
	var tokenCalls, providerCalls atomic.Int32
	client, config, server := fixture(t, "responses", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth" {
			tokenCalls.Add(1)
			json.NewEncoder(w).Encode(map[string]any{"access_token": "cached-token", "token_type": "Bearer", "expires_in": 3600})
			return
		}
		providerCalls.Add(1)
		json.NewEncoder(w).Encode(responseBody(goodAnswer))
	})
	client.Close()
	secret := filepath.Join(t.TempDir(), "secret")
	os.WriteFile(secret, []byte("secret"), 0600)
	config.Auth = Auth{ClientID: "client", ClientSecretFile: secret, TokenEndpoint: server.URL + "/oauth"}
	client, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var group sync.WaitGroup
	for i := 0; i < 8; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			if _, err := client.Generate(context.Background(), fixtureRequest()); err != nil {
				t.Error(err)
			}
		}()
	}
	group.Wait()
	if tokenCalls.Load() != 1 || providerCalls.Load() != 8 {
		t.Fatal("concurrent cache or generation mismatch")
	}
}
func TestCanceledInFlightGenerationIsAmbiguous(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	client, _, _ := fixture(t, "responses", func(w http.ResponseWriter, r *http.Request) { close(started); <-release })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := client.Generate(ctx, fixtureRequest()); done <- err }()
	<-started
	cancel()
	checkError(t, <-done, true)
	close(release)
}
