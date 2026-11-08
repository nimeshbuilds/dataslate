# Sylon generation transport

This package makes one bounded model request and returns a validated answer and optional code proposal. It does not authorize platform resources, execute code, call tools, retain conversations, search data, or silently retry a provider operation. Platform services must authorize every context item and persist their dispatch checkpoint before calling `Generate`.

The administrator chooses an exact HTTPS endpoint, model, protocol, output budget, CA trust and authentication:

```json
{
  "protocol": "responses",
  "endpoint": "https://api.openai.com/v1/responses",
  "model": "your-tested-pinned-model",
  "maxOutputTokens": 4096,
  "auth": {"bearerTokenFile": "/run/secrets/sylon/token"}
}
```

`responses` uses the native Responses wire format with `instructions`, typed user input, `max_output_tokens`, and strict JSON Schema in `text.format`. The explicit `chat-completions` gateway mode uses developer/user messages, `max_completion_tokens`, and strict `response_format.json_schema`. Both send `store:false` and `stream:false`, without tools, previous response IDs, provider history, reasoning settings, or protocol fallback. The provider must support this exact structured-output contract. Requests to providers with different native protocols require a separately qualified compatible gateway. See the official [Responses migration guide](https://developers.openai.com/api/docs/guides/migrate-to-responses) and [structured-output guide](https://developers.openai.com/api/docs/guides/structured-outputs).

The returned provider model must exactly match the configured model. Use a pinned model identifier that the endpoint returns unchanged, or a gateway whose explicitly qualified alias obeys that contract. An alias resolving to a different response model fails closed. `store:false` requests stateless operation; it does not establish a provider-wide zero-retention policy.

`Auth` supports a rotating bearer token file, or OAuth client credentials with `clientId`, `clientSecretFile`, exact HTTPS `tokenEndpoint`, optional `scope`, and `style: "post" | "basic"`. Do not combine the mechanisms. Credential and CA paths are absolute; referenced files must be bounded regular files. OAuth tokens are cached only until expiry or credential rotation. A 401 clears the cache for a future explicit operation and never triggers replay. TLS verification is mandatory, redirects and environment proxies are disabled, and errors omit provider bodies and credentials. A changed CA file requires a new client or process restart.

The input is `Request{Instructions, Input, AllowedCitationIDs}`. Instructions and input together are at most 128 KiB of nonblank UTF-8 without NUL; there are at most 20 unique server-assigned citation IDs. These are byte limits, not a model tokenizer or guarantee that a configured model accepts the context. The whole call, including OAuth, has a 60-second deadline and respects a shorter caller deadline. A response body is capped at 256 KiB, output tokens at the configured 256–8,192, answer text at 32 KiB, and proposed source at 64 KiB.

The strict structured answer has exactly `answer`, `citations`, and `proposedCode`. Citations must be unique IDs from the supplied allowlist. Code is null or `{language:"python"|"sql",source}`. This validates shape and provenance references; it does not establish factual correctness or code safety. Platform review and execution authorization remain separate. Usage is a nullable pointer: absent provider counts remain absent; supplied nonnegative integer counts must be internally consistent and within the output budget. No cost is invented.

Responses must be completed and contain one completed assistant text message. Recognized reasoning items are discarded; refusals, incomplete messages, tool actions, unexpected output types and provider annotations are rejected. Chat must contain one stopped assistant choice without function/tool calls or refusal. Duplicate JSON keys, excessive nesting, malformed shapes, extra answer fields and invalid citations fail closed. No provider URLs are followed. Every error after inference dispatch is conservatively marked `Error.Ambiguous=true`; callers must not automatically retry. Input, credential and OAuth failures before inference dispatch have `Ambiguous=false`.

ChatGPT/Codex subscription sign-in is a separate integration. The current [Sign in with ChatGPT preview route](https://developers.openai.com/siwc/token-sharing-open-source/preview-limitations) requires streaming and excludes parameters used here, including `max_output_tokens`. This package does not accept arbitrary subscription tokens as a supported substitute for API credentials.

`go test -race ./internal/llm` passes 15 top-level tests with real local TLS servers, including both protocol requests, usage, malformed/null/oversized responses, refusal/incomplete/tool output rejection, code/citation limits, rotating bearer/OAuth authentication, concurrent token caching, redirect/proxy refusal, TLS verification, cancellation and lost-response no-replay behavior. `go vet ./internal/llm` passes. No paid provider call, retrieval-quality evaluation, model-specific compatibility, cloud identity or production load qualification is claimed by those tests.
