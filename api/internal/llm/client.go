package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

const MaxInputBytes = 128 << 10
const MaxAnswerBytes = 32 << 10
const MaxCodeBytes = 64 << 10
const MaxCitations = 20

var modelPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9:/_.-]{0,127}$`)
var citationPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9:._-]{0,127}$`)

type Request struct {
	Instructions       string
	Input              string
	AllowedCitationIDs []string
}
type Code struct {
	Language string `json:"language"`
	Source   string `json:"source"`
}
type Usage struct {
	InputTokens  int64 `json:"inputTokens"`
	OutputTokens int64 `json:"outputTokens"`
	TotalTokens  int64 `json:"totalTokens"`
}
type Result struct {
	Answer       string   `json:"answer"`
	Citations    []string `json:"citations"`
	ProposedCode *Code    `json:"proposedCode"`
	Usage        *Usage   `json:"usage,omitempty"`
}

func validText(s string, limit int) bool {
	return utf8.ValidString(s) && strings.TrimSpace(s) != "" && len(s) <= limit && !strings.ContainsRune(s, 0)
}
func (in Request) validate() error {
	if !validText(in.Instructions, MaxInputBytes) || !validText(in.Input, MaxInputBytes) || len(in.Instructions)+len(in.Input) > MaxInputBytes || len(in.AllowedCitationIDs) > MaxCitations {
		return failure("invalid input bounds")
	}
	seen := map[string]bool{}
	for _, id := range in.AllowedCitationIDs {
		if !citationPattern.MatchString(id) || seen[id] {
			return failure("invalid citation allowlist")
		}
		seen[id] = true
	}
	return nil
}

func outputSchema(ids []string) map[string]any {
	item := map[string]any{"type": "string"}
	if len(ids) > 0 {
		item["enum"] = ids
	}
	code := map[string]any{"type": "object", "additionalProperties": false, "required": []string{"language", "source"}, "properties": map[string]any{
		"language": map[string]any{"type": "string", "enum": []string{"python", "sql"}}, "source": map[string]any{"type": "string"},
	}}
	return map[string]any{"type": "object", "additionalProperties": false, "required": []string{"answer", "citations", "proposedCode"}, "properties": map[string]any{
		"answer":       map[string]any{"type": "string"},
		"citations":    map[string]any{"type": "array", "items": item, "maxItems": len(ids)},
		"proposedCode": map[string]any{"anyOf": []any{code, map[string]any{"type": "null"}}},
	}}
}
func (c *Client) payload(in Request) ([]byte, error) {
	format := map[string]any{"name": "sylon_answer", "strict": true, "schema": outputSchema(in.AllowedCitationIDs)}
	request := map[string]any{"model": c.config.Model, "store": false, "stream": false}
	if c.config.Protocol == "responses" {
		format["type"] = "json_schema"
		request["instructions"] = in.Instructions
		request["input"] = []any{map[string]any{"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": in.Input}}}}
		request["max_output_tokens"] = c.config.MaxOutputTokens
		request["text"] = map[string]any{"format": format}
	} else {
		request["messages"] = []any{map[string]any{"role": "developer", "content": in.Instructions}, map[string]any{"role": "user", "content": in.Input}}
		request["max_completion_tokens"] = c.config.MaxOutputTokens
		request["response_format"] = map[string]any{"type": "json_schema", "json_schema": format}
	}
	return json.Marshal(request)
}

// Generate makes one bounded non-streaming attempt. The caller authorizes every
// context item and persists attempt intent before calling. No tools, hidden
// history, provider URL following, fallback, or automatic retries are performed.
func (c *Client) Generate(ctx context.Context, in Request) (Result, error) {
	if err := in.validate(); err != nil {
		return Result{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	if ctx.Err() != nil {
		return Result{}, failure("request canceled before dispatch")
	}
	payload, err := c.payload(in)
	if err != nil {
		return Result{}, failure("invalid request")
	}
	token, err := c.bearer(ctx)
	if err != nil {
		return Result{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.config.Endpoint, bytes.NewReader(payload))
	if err != nil {
		return Result{}, failure("invalid request")
	}
	req.GetBody = nil // Prevent net/http from transparently replaying a POST.
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if ctx.Err() != nil {
		return Result{}, failure("request canceled before dispatch")
	}
	response, err := c.http.Do(req)
	if err != nil {
		return Result{}, &Error{Kind: "transport failed", Ambiguous: true}
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		if response.StatusCode == http.StatusUnauthorized {
			c.mu.Lock()
			c.token = ""
			c.mu.Unlock()
		}
		return Result{}, &Error{Kind: "provider rejected", StatusCode: response.StatusCode, Ambiguous: true}
	}
	var raw json.RawMessage
	if decode(response.Body, maxResponse, &raw) != nil {
		return Result{}, providerFailure("invalid provider response")
	}
	var answer string
	var usage *Usage
	if c.config.Protocol == "responses" {
		answer, usage, err = c.responseText(raw)
	} else {
		answer, usage, err = c.chatText(raw)
	}
	if err != nil {
		return Result{}, err
	}
	out, err := parseAnswer(answer, in.AllowedCitationIDs)
	if err != nil {
		return Result{}, err
	}
	out.Usage = usage
	return out, nil
}
func providerFailure(kind string) error { return &Error{Kind: kind, Ambiguous: true} }
func absentOrNull(raw json.RawMessage) bool {
	return len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}
func emptyArray(raw json.RawMessage) bool {
	return absentOrNull(raw) || bytes.Equal(bytes.TrimSpace(raw), []byte("[]"))
}

func (c *Client) responseText(raw []byte) (string, *Usage, error) {
	var response struct {
		Object, Model, Status           string
		Error, IncompleteDetails, Usage json.RawMessage
		Store                           *bool
		Output                          []struct {
			Type, Role, Status string
			Content            []struct {
				Type        string
				Text        *string
				Annotations json.RawMessage
			}
		}
	}
	// Explicit tag is needed for the underscore used by the provider protocol.
	var envelope struct {
		IncompleteDetails json.RawMessage `json:"incomplete_details"`
	}
	if json.Unmarshal(raw, &response) != nil || json.Unmarshal(raw, &envelope) != nil || response.Object != "response" || response.Model != c.config.Model || response.Store != nil && *response.Store || !absentOrNull(response.Error) || !absentOrNull(envelope.IncompleteDetails) {
		return "", nil, providerFailure("invalid response envelope")
	}
	if response.Status != "completed" {
		return "", nil, providerFailure("response incomplete")
	}
	if len(response.Output) < 1 || len(response.Output) > 32 {
		return "", nil, providerFailure("invalid output items")
	}
	answer := ""
	for _, item := range response.Output {
		switch item.Type {
		case "reasoning":
			// Reasoning is deliberately neither returned nor persisted/replayed.
			if item.Status != "" && item.Status != "completed" {
				return "", nil, providerFailure("incomplete reasoning item")
			}
		case "message":
			if answer != "" || item.Role != "assistant" || item.Status != "completed" || len(item.Content) != 1 {
				return "", nil, providerFailure("invalid output message")
			}
			part := item.Content[0]
			if part.Type == "refusal" {
				return "", nil, providerFailure("provider refused")
			}
			if part.Type != "output_text" || part.Text == nil || *part.Text == "" || !emptyArray(part.Annotations) {
				return "", nil, providerFailure("unexpected output content")
			}
			answer = *part.Text
		default:
			return "", nil, providerFailure("unexpected output action")
		}
	}
	if answer == "" {
		return "", nil, providerFailure("missing output text")
	}
	usage, err := c.parseUsage(response.Usage, false)
	return answer, usage, err
}

func (c *Client) chatText(raw []byte) (string, *Usage, error) {
	var response struct {
		Object, Model string
		Usage         json.RawMessage
		Choices       []struct {
			Index        *int
			FinishReason string `json:"finish_reason"`
			Message      struct {
				Role         string
				Content      *string
				Refusal      *string
				ToolCalls    json.RawMessage `json:"tool_calls"`
				FunctionCall json.RawMessage `json:"function_call"`
			}
		}
	}
	if json.Unmarshal(raw, &response) != nil || response.Object != "chat.completion" || response.Model != c.config.Model || len(response.Choices) != 1 {
		return "", nil, providerFailure("invalid chat response")
	}
	choice := response.Choices[0]
	if choice.Index == nil || *choice.Index != 0 || choice.FinishReason != "stop" {
		return "", nil, providerFailure("chat response incomplete")
	}
	message := choice.Message
	if message.Refusal != nil && *message.Refusal != "" {
		return "", nil, providerFailure("provider refused")
	}
	if message.Role != "assistant" || message.Content == nil || *message.Content == "" || !emptyArray(message.ToolCalls) || !absentOrNull(message.FunctionCall) {
		return "", nil, providerFailure("unexpected chat action or content")
	}
	usage, err := c.parseUsage(response.Usage, true)
	return *message.Content, usage, err
}
func (c *Client) parseUsage(raw json.RawMessage, chat bool) (*Usage, error) {
	if absentOrNull(raw) {
		return nil, nil
	}
	var u struct {
		Input      *int64 `json:"input_tokens"`
		Output     *int64 `json:"output_tokens"`
		Prompt     *int64 `json:"prompt_tokens"`
		Completion *int64 `json:"completion_tokens"`
		Total      *int64 `json:"total_tokens"`
	}
	if json.Unmarshal(raw, &u) != nil {
		return nil, providerFailure("invalid usage")
	}
	input, output := u.Input, u.Output
	if chat {
		input, output = u.Prompt, u.Completion
	}
	if input == nil || output == nil || u.Total == nil || *input < 0 || *input > 1000000000 || *output < 0 || *output > int64(c.config.MaxOutputTokens) || *u.Total != *input+*output {
		return nil, providerFailure("invalid usage")
	}
	return &Usage{InputTokens: *input, OutputTokens: *output, TotalTokens: *u.Total}, nil
}

func strictDecode(data []byte, out any) bool {
	if !utf8.Valid(data) || !uniqueJSON(data) {
		return false
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	return d.Decode(out) == nil && d.Decode(new(any)) == io.EOF
}
func parseAnswer(text string, allowed []string) (Result, error) {
	var raw struct {
		Answer       *string         `json:"answer"`
		Citations    *[]string       `json:"citations"`
		ProposedCode json.RawMessage `json:"proposedCode"`
	}
	if !strictDecode([]byte(text), &raw) || raw.Answer == nil || !validText(*raw.Answer, MaxAnswerBytes) || raw.Citations == nil || len(*raw.Citations) > MaxCitations || len(raw.ProposedCode) == 0 {
		return Result{}, providerFailure("invalid structured answer")
	}
	grant := map[string]bool{}
	seen := map[string]bool{}
	for _, id := range allowed {
		grant[id] = true
	}
	for _, id := range *raw.Citations {
		if !grant[id] || seen[id] {
			return Result{}, providerFailure("invalid answer citations")
		}
		seen[id] = true
	}
	result := Result{Answer: *raw.Answer, Citations: *raw.Citations}
	if !absentOrNull(raw.ProposedCode) {
		var code struct {
			Language *string `json:"language"`
			Source   *string `json:"source"`
		}
		if !strictDecode(raw.ProposedCode, &code) || code.Language == nil || (*code.Language != "python" && *code.Language != "sql") || code.Source == nil || !validText(*code.Source, MaxCodeBytes) {
			return Result{}, providerFailure("invalid code proposal")
		}
		result.ProposedCode = &Code{Language: *code.Language, Source: *code.Source}
	}
	return result, nil
}

// Reject ambiguous duplicate JSON object keys and excessive nesting, rather
// than accepting the last occurrence through encoding/json's default behavior.
func uniqueJSON(data []byte) bool {
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	var value func(int) bool
	value = func(depth int) bool {
		if depth > 32 {
			return false
		}
		token, err := d.Token()
		if err != nil {
			return false
		}
		delim, compound := token.(json.Delim)
		if !compound {
			return true
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				key, err := d.Token()
				if err != nil {
					return false
				}
				s, ok := key.(string)
				if !ok || seen[s] {
					return false
				}
				seen[s] = true
				if !value(depth + 1) {
					return false
				}
			}
			end, err := d.Token()
			return err == nil && end == json.Delim('}')
		case '[':
			for d.More() {
				if !value(depth + 1) {
					return false
				}
			}
			end, err := d.Token()
			return err == nil && end == json.Delim(']')
		default:
			return false
		}
	}
	if !value(0) {
		return false
	}
	_, err := d.Token()
	return err == io.EOF
}
