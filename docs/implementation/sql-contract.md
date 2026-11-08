# Governed SQL and retained results

DataSlate submits an exact saved SQL cell to an administrator-configured Trino engine through a separate durable query controller. The engine enforces catalog access, read-only execution, row filters, column masks and constrained impersonation. DataSlate does not use a `SELECT` string check as a security boundary. Current binding grants control execution and retained-source/result access; workspace membership alone never supplies an engine identity.

## Administrator configuration

The API and SQL controller read `DATASLATE_SQL_BINDINGS_FILE`, a bounded JSON array. An empty array denies new execution and causes an operating controller to request cancellation of revoked work. Each binding contains:

```json
{
  "id": "governed-sql",
  "workspaceId": "default",
  "name": "Governed lakehouse SQL",
  "engine": "trino",
  "endpoint": "https://trino.example.com",
  "caFile": "/var/run/dataslate/trino/ca.crt",
  "tokenFile": "/var/run/dataslate/trino/token",
  "catalog": "lake",
  "schema": "analytics",
  "maxRunSeconds": 300,
  "policyVersion": "v1",
  "readOnlyPolicyAccepted": true,
  "allowedSubjects": [
    {"issuer": "https://identity.example.com", "subject": "employee-123", "engineUser": "employee-123"}
  ]
}
```

`endpoint` is a root HTTPS URL; `tokenFile` is an absolute path and optional `caFile` is also absolute. Only the controller mounts or reads those credential/trust files. The API publishes the binding ID, workspace, name and `{engine,catalog,schema,maxRunSeconds,policyVersion}`. It never publishes endpoints, credential paths or engine-user mappings. Subject grants are unique issuer/subject pairs with distinct explicit engine users within a binding. Empty grants deny access. Production API configuration requires each issuer to match the configured OIDC issuer.

The bearer token is an engine service credential, not the browser's API access token. Trino must constrain that service principal's impersonation to the mapped users and apply their data permissions. Arbitrary header forwarding, endpoint selection, roles, session settings and credentials are not accepted from the browser. Managed Trino configuration and qualification are separate from a binding's explicit `readOnlyPolicyAccepted` acknowledgement; acknowledgement is not automatic proof of engine policy.

`policyVersion` is mandatory and part of the immutable accepted context. Restrictive engine-policy changes must first rotate the DataSlate policy version or revoke the binding, then update engine policy. Old retained source and result requests fail closed after that version change. DataSlate cannot detect an out-of-band change to Trino/Ranger/IAM policy: current binding checks do not magically re-evaluate historical row values against an independently edited engine policy. Restoring an old version can restore access to old results, so policy versions must be monotonic operational identities.

## API resources

All routes are below `/api/v1/workspaces/{workspaceId}`:

- `GET /sql-bindings` returns the caller's current explicitly granted bindings; workspace viewers cannot submit queries.
- `POST /queries` accepts `{bindingId,idempotencyKey,notebook:{id,revision,cellId}}`.
- `GET /queries` returns the latest 200 queries submitted by the exact issuer/subject in that workspace; `GET /queries/{id}` reads status.
- `POST /queries/{id}/cancel` accepts `{}` and returns the updated query with 202. Cancellation is a request, not fabricated completion.
- `GET /queries/{id}/source` returns `{queryId,workspaceId,source,notebook}` for the immutable saved SQL snapshot.
- `GET /queries/{id}/result` returns the complete retained result envelope as JSON, bounded to 10,000 retained rows and 8 MiB of encoded columns plus rows. It is not a paginated engine cursor or an object-store redirect.

Every access requires workspace membership and exact query ownership. Editors/admins can submit/cancel. Shared notebook documents never acquire execution outputs, and workspace administrators do not gain a bypass to another subject's results. Source/result reads additionally reload the current binding and require the exact accepted private profile, engine-user mapping and policy version before and after storage retrieval. Missing/malformed current policy returns 503; removed or changed authorization returns 403. Metadata and cancellation remain available to the owner after grant revocation.

A saved SQL cell can belong to a SQL document or a mixed notebook. The selected cell must be SQL, nonempty valid UTF-8, at most 64 KiB and free of NUL bytes. Its current saved document revision is locked while the query and independent source snapshot commit. No cell execution order or multi-cell transaction is implied. Exact idempotent replay is resolved before rereading the document, including after an edit or deletion. Initial acceptance is 202; replay is 200; a changed key/body is 409. A lost response never authorizes an automatic new key.

A query has resource identity/timestamps/revision, `workspaceId`, `bindingId`, `submittedBy`, accepted public `context`, immutable `notebook:{id,revision,cellId,sha256,bytes}`, `state`, `message`, `submissionStarted`, `cancelRequested`, `cancelAccepted`, optional `engineQueryId`, and `result:{rowCount,bytes,truncated,complete,reason?}`. The states are `queued`, `submitting`, `running`, `succeeded`, `failed`, `cancel_requested`, `cancelled`, `unknown`.

## Controller and recovery

The standalone `dataslate-sql-controller` uses PostgreSQL with TLS verification, `DATASLATE_SQL_BINDINGS_FILE`, and optional `DATASLATE_SQL_CONTROLLER_ADDR` (default `:8082`). It can operate without Spark or a Spark operator. `/healthz` checks its listener; `/readyz` checks recent loop progress and the exact database schema. Neither endpoint asserts engine connectivity or policy correctness. `api/SQLControllerDockerfile` builds its separate container.

Migration 005 adds durable query metadata, private accepted engine policy/cursor, fenced leases, independently retained source/results and consumed-cursor hashes. Workspace row locking makes owner-scoped idempotency and the 100 active/unresolved query capacity atomic across API replicas. A 30-second lease fences late workers; one reconciliation has a bounded 20-second engine operation context.

The controller records `submissionStarted` before its single POST. A missing response with no confirmed engine query ID remains `unknown`; it never resubmits automatically. For a known cursor it persists each accepted page, the next cursor, processed-cursor hashes and result metadata in one transaction. A retry after a failed transaction requests exactly the same page URL. Repeated data-bearing cursors, cursor cycles and more than 4096 processed pages request cancellation without appending duplicate rows. This is bounded direct JSON protocol support; Trino's spooled-result protocol is rejected.

The transport verifies HTTPS, configured CA trust, exact same-origin statement paths, query identity, bounded response sizes and engine-user identity on query-status observations. Redirects and process-wide HTTP proxies are disabled. Tokens are reread from their file on each request. TLS clients reuse connections; CA file content rotation requires controller restart, while a changed CA path changes the profile and revokes prior execution/output access.

A binding deadline includes queue time and requests cancellation. The API accepts 1–3600 seconds. DataSlate does not send a session override for `query_max_run_time`: enabling that override would also permit caller SQL to try changing it. The managed engine sets a one-hour server ceiling and denies user system/catalog session changes. A controller deadline is not an exact wall-clock spend guarantee, particularly if a POST response is lost or the engine is unreachable.

A DELETE acknowledgement records `cancelAccepted`; it does not prove terminal state. The controller checks the statement page and, when a cursor retires, the engine's authenticated query-status resource. A verified `FAILED` with `USER_CANCELED` confirms cancellation. Other failures remain failed. If the engine reports `FINISHED` after result pages were lost, the query may be `succeeded` but its result is explicitly incomplete/truncated with `reason: "result_unavailable"`. Active/expired/unverifiable status remains unresolved. A missing cursor alone never proves success or cancellation.

## Result representation and visualization

`QueryResult` is `{queryId,workspaceId,columns:[{name,type}],rows:[[string|null]],rowCount,bytes,truncated,complete,reason?}`. Every supported non-null scalar becomes a string, including booleans, integers and decimals; JSON number text is preserved exactly rather than converted through floating point. Type metadata distinguishes numeric, text, date/time, boolean and binary-like values. Array/map/row and unsupported result types are explicitly incomplete; no silent JSON stringification is used as a substitute for typed support.

`rowCount` is retained rows. `bytes` counts the JSON-encoded column array plus row array; empty results start at four bytes. The envelope adds a small amount of identity/status metadata. Reaching the row/byte/type boundary retains a marked preview and requests engine cancellation if more pages remain. It does not claim the full query result fits the preview. Partial data may also remain after engine failure or user cancellation.

`complete: true` requires successful engine completion, every supported page retained, and no truncation or interrupted retrieval. `truncated: false` by itself is not proof of completeness while a query runs. Charts/profiling should use only complete retained results, or display an unmistakable preview scope; high-precision values must not silently become inaccurate JavaScript chart numbers. JSON export uses the same owner and current-policy checked result route. CSV, JDBC/ODBC, durable dashboard sharing and downloads of results larger than the retained cap are separate features.

## Validation scope

Tests cover exact SQL snapshots and replay after deletion, memory/PostgreSQL idempotency races, ownership, current-policy/epoch revocation, source integrity, explicit engine identity, bigint/decimal preservation, row/byte bounds, complex types, page loss/cycles, lease fencing, ambiguous POST, pre-submit revocation/deadlines, cancellation acknowledgement versus observed terminal status, lost-result completion and OpenAPI response schemas. TLS protocol fixtures exercise the control plane and transport together. These are automated implementation checks, not production qualification of Trino's deployed authentication, read-only policy, row filters, masks, S3/Polaris catalogs, federated connectors or EKS network/identity.

## Local live API qualification

`TestSQLLiveAPIWithPostgres` passed against the isolated local Trino 484 Kubernetes fixture with real TLS/JWT and PostgreSQL. A saved SQL cell submitted through the API returned five rows under the engine row filter and column mask, preserved the maximum signed 64-bit integer exactly, retained source after notebook deletion, and denied persisted results after policy-version rotation. The opt-in test uses `DATASLATE_TEST_TRINO_ENDPOINT`, `DATASLATE_TEST_TRINO_CA`, `DATASLATE_TEST_TRINO_TOKEN_FILE`, and `DATASLATE_TEST_DATABASE_URL`. This establishes the local integration path; it does not qualify EKS, S3, production identity, availability, or arbitrary catalog workloads.
