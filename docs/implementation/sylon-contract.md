# Sylon conversation and code-assistance contract

Sylon provides private asynchronous assistant conversations, saved-cell troubleshooting and code proposals through an administrator-approved model endpoint. It can use explicitly selected notebook cells, authorized Polaris table schemas, and authorized lexical or hybrid search. It never executes generated code or SQL. Applying a proposal and running the resulting saved document are separate user actions through existing authorized APIs.

This is an implemented integration with memory/PostgreSQL lifecycle tests and a real local TLS model-provider fixture. External provider subscriptions, EKS/OIDC deployment, production capacity, personal/team agent memory, autonomous tool execution and production model quality remain separate qualification or implementation work. An API-compatible paid service or gateway must provide an approved API credential; consumer chat subscriptions are not assumed to grant API access.

## Administrator configuration

`DATASLATE_SYLON_BINDINGS_FILE` contains an array of bindings. Each has `id`, `workspaceId`, `name`, `policyVersion`, `provider`, `dataTransferAccepted:true`, exact `allowedSubjects:[{issuer,subject}]`, `context:{notebookCellSource,catalogBindingIds,searchBindingIds}`, and `maxContextBytes` from 1,024 to 98,304. Duplicate identifiers or subject pairs are rejected. The file is bounded to 4 MiB and 1,000 bindings.

Provider configuration has `protocol:responses|chat-completions`, an exact HTTPS `endpoint`, approved `model`, `maxOutputTokens:256..8192`, optional absolute `caFile`, and `auth`. Authentication uses either an absolute `bearerTokenFile`, or `clientId`, absolute `clientSecretFile`, HTTPS `tokenEndpoint`, `scope`, and `style:post|basic`. OAuth credentials are independently rotated and cached by the adapter. Tokens and credential values never enter public resources. The control-plane API validates configuration without opening provider credentials; the Sylon worker mounts provider credentials. Existing catalog and hybrid search components require their own narrowly scoped credentials.

The complete binding hash is recorded with each job and rechecked against current configuration before context transfer, generation, history retrieval and proposal application. Rotate `policyVersion` before or with restrictive upstream policy changes. DataSlate cannot detect an administrator changing an upstream model or storage policy without changing its binding. Existing history with a changed binding hash is denied; start a new conversation under the new profile. No automatic model fallback occurs.

## API and ownership

Routes below are under `/api/v1/workspaces/{workspaceId}/sylon`. Every request requires verified API authentication and current workspace membership. Private resources are owned by exact issuer and subject, not display name. Generic conversations are available to an explicitly granted viewer; selecting source code and applying changes require editor/admin access.

| Route | Contract |
| --- | --- |
| `GET /bindings` | Safe approved model/context summaries; no endpoints, secret paths or provider credentials. |
| `GET /conversations` | Current-owner conversations with an available granted binding. |
| `POST /conversations` | `{bindingId,title}` creates a private conversation, status 201. |
| `GET /conversations/{id}` | `{conversation,messages}`; reauthorizes every message and all source dependencies. |
| `DELETE /conversations/{id}` | Status 204; owner may erase after binding revocation. Cascading deletion prevents late publication. |
| `POST /conversations/{id}/messages` | `{idempotencyKey,prompt,notebook?,catalogs?,search?}`, status 202; exact replay status 200. |
| `GET /jobs/{id}` | Current-owner job with current assistant and source authorization. |
| `POST /jobs/{id}/cancel` | `{}` returns status 202 and only `{jobId,workspaceId,state,cancelRequested}`. |
| `POST /jobs/{id}/apply` | `{notebookId,revision,cellId}` returns a durable one-use apply receipt, status 200. |

A notebook selection is `{id,revision,cellId}` and must identify an exact current saved Python or SQL cell when queued and when new context is prepared. Catalog selectors are `{bindingId,namespace:[components],name}`. Search selection is `{bindingId,query,mode:lexical|hybrid}`. No browser-supplied endpoint, model, identity, credential or arbitrary tool definition is accepted. Unknown request fields are rejected. The earlier `/sylon/messages` foundation route remains an explicit unavailable capability; clients use the conversation routes.

Jobs expose resource timestamps/revision, workspace/conversation/binding IDs, `submittedBy`, `policyVersion`, state, message, explicit `dispatchStarted`/`cancelRequested` booleans, prompt and selections, citations, optional result and optional apply receipt. States are `queued`, `preparing`, `generating`, `succeeded`, `failed`, `unknown`, `cancel_requested`, `cancelled`. Public responses never include provider requests, private prepared context, access-policy hashes or credentials.

Result format is `{answer,citations:[citation IDs],proposedCode:null|{language,source},usage?}`. Usage is absent when the provider does not report it, rather than fabricated as zero. Code is accepted only for a selected saved cell and its existing language. Citation records expose source identity/revision and a `historical` flag; notebook flags are recalculated on read after a newer revision is saved.

## Context and history

Cell-source transfer requires the separate `context.notebookCellSource` opt-in. Selecting a search profile cannot bypass this: source-containing search profiles are denied when source transfer is disabled. Catalog search is narrowed before ranking to Sylon-approved bindings and the caller's current exact table grants. Direct catalog context uses safe live schema metadata, never locations, credentials or data rows. Query results, Spark logs, execution outputs and arbitrary external URLs are excluded.

All supplied sources become durable authorization dependencies, including uncited material. Model omission of a citation does not remove a dependency. Before reusing or displaying derived history, notebook existence and catalog/search binding hashes and grants are checked again. Historical notebook content remains accessible only while the notebook exists and the caller retains workspace and assistant access; the saved historical revision is explicitly labeled. New context selections must use the current revision. Deletion of any required notebook or revocation of a source policy denies derived output.

Limits: prompt 8,192 UTF-8 bytes; four explicitly selected catalogs; search top four; twenty physical history messages (each user prompt and each available assistant response counts); twenty distinct dependencies and twenty citations; configured assembled context ceiling up to 98,304 bytes; provider request text at most 128 KiB. Exceeding a bound fails instead of silently truncating protected context. History stores this turn's sources separately and reassembles them once; it does not recursively embed previous requests. Failed, cancelled and uncertain user turns remain labeled history. A new conversation is required once its history budget is full.

There are at most 100 conversations per owner/workspace, one active job per conversation, and 100 active jobs per workspace. Exact replay remains available before capacity checks. Private output retention lasts until conversation deletion; this is not a personal/team memory system.

## Durable dispatch, cancellation and apply

The API persists the message intent before any provider call. A PostgreSQL controller lease is fenced by owner, sequence and wall-clock expiration after row-lock acquisition. The worker has a 120-second lease, bounded 30-second context preparation, up to 60 seconds for OAuth plus model generation, and bounded persistence/authorization windows. Readiness allows a legitimate in-flight turn to complete.

Before any hybrid embedding or generation POST, the controller durably marks `dispatchStarted`. It rechecks current authority around provider calls. A restarted worker with a dispatched job records `unknown`; it never repeats a potentially billable POST automatically. The adapter makes one attempt, disables redirects/proxy fallback, requires exact model identity, rejects tools/refusals/incomplete or malformed structured output, and validates all returned citation IDs. Provider errors are reported without exposing provider response bodies.

Cancellation before dispatch prevents a new provider call. Cancellation after dispatch discards output but does not claim remote execution or billing stopped. The minimal cancellation receipt is available to the owner after binding revocation. Conversation deletion removes private jobs and prevents a controller from publishing into a deleted conversation. A final selected-notebook revision check occurs under the storage lock/transaction, so an edit racing with publication discards the generated response. Applying a proposal uses an independent optimistic revision check.

Apply is a single atomic transaction: validate exact job target and current notebook revision/language, save the new cell source, append revision history, invalidate old search documents/update the notebook indexing outbox, and persist the apply receipt. A concurrent or repeated exact apply returns the original receipt. Replaying after a later user edit does not write again; a different body conflicts. The receipt includes previous/new revision and application timestamp. The notebook contains no private assistant output. Running the changed code still requires an explicit permitted Spark or SQL submission.

## Deployment and evidence

`dataslate-sylon-controller` requires verified-TLS PostgreSQL and `DATASLATE_SYLON_BINDINGS_FILE`. Its default probe address is `:8084` (`DATASLATE_SYLON_CONTROLLER_ADDR`). Selected catalog/search context additionally requires those existing binding files and only their relevant credentials/network egress. The worker does not need Kubernetes API credentials. Apply migration `008_sylon.sql` with the migration role before starting runtime roles.

Automated tests exercise both stores for idempotency races, owner/workspace isolation, bounded history, cancellation, uncertain dispatch recovery, expiry fencing, exact one-use apply, historical provenance, deletion/revocation and source-edit publication races. A real HTTPS Responses fixture plus PostgreSQL verifies the production LLM adapter through job publication and apply. TLS transport tests separately cover OAuth/bearer rotation, response validation and ambiguity. These tests establish the implemented boundary; they do not assert quality, cost or reliability for an untested external model or production cluster.

### Backend checkpoint — November 7, 2026

The final backend checkpoint passed 22 Sylon top-level tests with their memory/PostgreSQL/adversarial subcases, the complete API race suite, `go vet ./...`, and whitespace/error checks. PostgreSQL tests use disposable per-test schemas so a controller cannot claim another test's abandoned job. pgvector qualification was mandatory in this run, not silently skipped:

```sh
DATASLATE_TEST_DATABASE_URL='<isolated-test-database>' \
DATASLATE_TEST_REQUIRE_VECTOR=1 go -C api test -race ./...
go -C api vet ./...
git diff --check
```

The Sylon checkpoint includes a real local HTTPS Responses provider combined with PostgreSQL, final result publication, and explicit proposal application; it does not use an external paid provider. Public receipts were validated against OpenAPI through actual handlers. Separate blocked-row-lock tests verify that expired Sylon and search leases cannot dispatch after waiting for an unchanged PostgreSQL row. SDK/CLI routes and request/recovery semantics were reviewed against the frozen contract. Container, production TLS/SCRAM process startup and browser qualification are recorded separately in the repository validation record as they complete.
