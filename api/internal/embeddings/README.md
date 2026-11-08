# Embeddings provider boundary

This package sends explicitly authorized text to one configured HTTPS embeddings endpoint using the OpenAI-compatible JSON protocol. It does not index documents, authorize users, select source text, store vectors, or manage retry intent; the DataSlate platform must perform those steps before and after the call.

```go
client, err := embeddings.New(embeddings.Config{
    Endpoint: "https://gateway.example/v1/embeddings", // exact endpoint
    Model: "text-embedding-3-small",
    Dimensions: 1536,
    CAFile: "/etc/dataslate/embedding-ca.pem", // optional additional trust
    Auth: embeddings.Auth{BearerTokenFile: "/etc/dataslate/embedding-token"},
})
// ValidateConfig performs syntax checks without reading files or contacting APIs.
// New additionally loads TLS trust. Embed reads current credential files.
vectors, err := client.Embed(ctx, []string{"authorized document metadata"})
```

Only the configured model is accepted in the response. Dimensions are explicit and limited to 1–2000 to match the current platform's pgvector indexing profile. Gateways must honor `model`, `dimensions`, `input` and `encoding_format:"float"`. Native provider protocols that differ from this contract require a separate adapter or a compatible gateway. No fallback model or endpoint is selected silently.

Authentication accepts either a rotating bearer-token file (including an OAuth-issued JWT), or explicit OAuth client credentials: `ClientID`, `ClientSecretFile`, `TokenEndpoint`, optional `Scope`, and `Style:"post"` or `"basic"`. The client rereads mounted credentials; changing a secret invalidates its cached access token. No tokens or client secrets appear in configuration resources, logs or errors. The adapter reads file references only. Platform deployment validation should require approved absolute mount paths. Access tokens are bounded and validated before use.

TLS uses system trust plus the configured CA file, TLS 1.2 or later, and hostname verification. Redirects, URL credentials/query strings, path escaping/traversal, environment proxies, arbitrary request headers and insecure TLS are not supported. The exact endpoint must be provisioned by an administrator; workspace inputs cannot choose it. Responses cannot supply a new URL or execute tools.

A call accepts 1–64 non-empty UTF-8 strings, at most 32,768 bytes each and 262,144 bytes total. These are **byte bounds, not a tokenizer**. The platform must additionally chunk according to the configured model's token limit and its explicit data-transfer policy. The HTTP response is limited to 4 MiB. Results must contain exactly one uniquely indexed embedding per input, the configured model and dimension, finite float32 values, and a nonzero norm. Null components, overflow, underflow to an all-zero vector, invalid JSON/UTF-8 and duplicate indices fail without returning partial vectors. Returned vectors preserve input order even when the provider reorders response entries.

Each embeddings POST is attempted once. There is no automatic retry or body replay. After dispatch, failures are conservatively marked `Error.Ambiguous=true`: a provider may already have processed or billed the request. HTTP status is exposed without its response body. Local validation or credential/OAuth failure occurs before embeddings dispatch and is marked non-ambiguous for that embedding operation. A 401 clears a cached OAuth token for a later explicit operation; it does not retry the current request. The platform must persist its attempt boundary and require an explicit new indexing intent after an error. A network timeout is not evidence that the provider did no work.

The official [embeddings API reference](https://developers.openai.com/api/reference/resources/embeddings/methods/create) defines the request, indexed float responses, model token limits and dimension option. [The embeddings guide](https://developers.openai.com/api/docs/guides/embeddings) describes dimension reduction. The dimension parameter requires a model that supports it; older models or gateways that reject it are not silently downgraded. This implementation was tested against local HTTPS fixtures, including OAuth rotation and adversarial responses; no paid or external provider request was made and no provider account/model availability is claimed.

Run:

```sh
go test -race ./internal/embeddings
go vet ./internal/embeddings
```
