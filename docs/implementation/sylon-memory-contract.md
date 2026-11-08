# Private Sylon memory contract

This implementation adds explicitly reviewed, owner-private memory to Sylon. A memory begins with an owned succeeded Sylon job, retains that job's complete source provenance, and can be explicitly selected for later assistant context. It is a bounded foundation for later agent memory. Shared/team memory, automatic extraction, embeddings for recall, autonomous tools, and Agent Studio are separate roadmap capabilities.

## Administrator policy and authority

A Sylon binding must explicitly include:

```json
{"memory":{"enabled":true,"defaultTTLDays":30,"maxTTLDays":365}}
```

Both day counts are required, must be integers from 1 through 365, and the default cannot exceed the maximum. Omitting `memory` disables it. The default is a client suggestion; every save/correction sends an explicit RFC3339 `expiresAt`. Expiration must be future and within the binding's maximum. Changing this policy changes the full Sylon binding hash, so older memories and derived history require their original current policy and become restricted when that hash changes.

Identity is the exact issuer and subject within one workspace. Only the owner can read, recall, correct, or erase a memory. Save and correction require editor access. Metadata-only memory can remain readable by an explicitly granted viewer; notebook-cell-derived memory requires current editor access on detail, recall, message admission, history, and content-bearing management/replay responses. Minimal management and deletion remain available to an owner with workspace membership after source or binding revocation.

Memory preserves all original context dependencies, including uncited sources and prior memory dependencies. Corrections change title, content, and expiry only. They do not change the origin, policies, output fingerprint, or source permissions. Current notebook existence, catalog table grants and binding hashes, search policy, memory revisions and expiry are rechecked before retrieval and provider transfer. A later apply receipt may advance the origin job revision without changing its immutable answer; that does not invalidate the memory. Deleting the origin conversation invalidates and purges its originating memories.

Workspace role checks use the identity admitted by the current API request. Workers recheck current administrator bindings, source existence, policies and memory state; they do not introspect live identity-provider roles after admission. This is the same admitted-identity boundary as other asynchronous Sylon jobs. No immediate identity-provider role-revocation guarantee is made for a request already dispatched.

## Public API

All routes require authenticated workspace membership and use the prefix `/api/v1/workspaces/{workspaceId}/sylon/memories`.

| Method and suffix | Behavior |
| --- | --- |
| `GET /` | Newest 100 non-deleted management summaries. Expired summaries appear only while purge is pending. |
| `GET /{id}` | Full current authorized, unexpired memory. |
| `POST /` | Save reviewed content from exact `source: {jobId, revision}`. |
| `PUT /{id}` | Correct exact expected `revision`, keeping immutable provenance. |
| `DELETE /{id}` | JSON body `{idempotencyKey, revision}`; forget and increment revision. |
| `POST /recall` | `{bindingId, query, limit}`; local lexical recall with limit 1 through 4. |

Create input is `{idempotencyKey, source, title, content, expiresAt}`. Correction input is `{idempotencyKey, revision, title, content, expiresAt}`. Titles are nonempty and at most 200 UTF-8 bytes; content is nonempty and at most 4096 UTF-8 bytes. NULs and malformed UTF-8 are rejected. No unbound note creation route exists.

Mutation response:

```json
{
  "receipt": {
    "memoryId": "mem-example", "workspaceId": "default",
    "operation": "correct", "previousRevision": 1, "revision": 2,
    "appliedAt": "2026-11-07T12:00:00Z"
  },
  "replayed": false,
  "memory": {
    "id": "mem-example", "workspaceId": "default", "revision": 2,
    "state": "available", "expiresAt": "2026-12-07T12:00:00Z",
    "purgePending": false, "title": "Reviewed summary", "bindingId": "approved-model"
  }
}
```

Create returns 201, exact replay returns 200, and correction/deletion return 200. Idempotency keys are scoped to exact owner and workspace across memory operations. The same key with different input conflicts. Receipts retain the original operation and increment `previousRevision` by exactly one, including deletion. The accompanying summary reflects current state and can have a later revision even during an otherwise fresh response. Replay never returns old title/content/source data after revocation or deletion.

Summary states are `available`, `restricted`, `expired`, and `deleted`. Only `available` includes `title` and `bindingId`. Restricted summaries contain only ID, workspace, revision, state, expiry and purge status. Full memory includes resource timestamps, revision, owner, binding/policy version, title, content, expiry, origin `{jobId, revision}`, and `purgePending`; full reads succeeding means the memory is currently available. A correction remains unavailable while its older derived copies await purge.

Errors include 403 `memory_access_denied`, 409 `memory_revision_conflict`, 409 `conflict` for changed idempotent input, 429 `memory_capacity`, 422 `invalid_memory`, 404 for missing or foreign-owned resources, and 503 for persistence failure. Policy loading errors fail closed. Management cannot distinguish an inaccessible dependency's detailed failure cause.

## Recall and reviewed transfer

Recall performs no model, embedding, catalog-network, or external provider request. It loads at most 100 active private heads for the selected binding and authorizes every candidate before any content enters scoring. Scoring splits lowercase Unicode letters/digits into words, awards two points for a unique query word in the title and one in the content, discards zero-score candidates, and breaks ties by memory ID. Hits are authorized again before response. `complete: true` describes ranking this bounded currently authorized private corpus; it is not a semantic-search quality claim.

At most 100 heads can remain active per owner/workspace. Expired heads continue consuming that budget until the worker materializes their expiry; this bounds pending expiry work as well as recall. Tombstones and mutation receipts do not consume active capacity. PostgreSQL partial indexes exclude completed expired/deleted heads from management scans. Management returns the newest 100 eligible summaries without pagination; exact known IDs and mutation receipts remain addressable.

Recall only suggests candidates. Sending a Sylon message must explicitly include `memories: [{id, revision}]`, with at most four unique selections. The memory text, not its raw origin context or origin answer, enters the prepared context. All original dependencies still accompany it. Its citation is `{kind:"memory", memory:{id, revision, sourceJobId}, ...}` and is labeled historical user-curated content. It is untrusted context, never provider instructions. Existing cell-source transfer opt-in and catalog/search allowlists cannot be bypassed by memory.

The common 20-dependency and 98,304-byte configured context limits apply to memories and history together. Saving a memory requires room for its own typed dependency; a source already at 20 dependencies is rejected rather than creating an unusable artifact. Context is rejected when it cannot fit; protected sources are not silently trimmed. There are no provider requests for saving, correcting, erasing or recalling. The existing explicit message action is what authorizes approved-provider transfer.

## Persistence, publication and erasure

Migration `009_sylon_memory.sql` adds current memory heads, immutable mutation receipts, job-to-memory use edges, and a durable purge queue. Private heads retain immutable provenance and a hash of the origin output. Prior memory bodies are not stored in a separate revision history. Correction replaces the head body immediately; old copies in prepared contexts or derived responses are asynchronously redacted.

Correction, deletion, and expiry immediately make dependent history unavailable through current exact-revision authorization, before the purge worker runs. The Sylon worker processes expiry and purge obligations in bounded batches (at most 100 heads/jobs/descendants per stage). It redacts prepared context, citations and generated output, marks the job `memoryInvalidated`, and invalidates descendant memories. Purged history cannot become usable merely because its dependency list was erased. New explicit conversations can use a corrected memory after purge drains.

Short PostgreSQL memory-affecting mutations take the workspace row lock first, then stable resource locks. Provider requests are outside these transactions. Save-context, dispatch, finish, apply, memory mutation, source-conversation deletion and purge share this order. Deadline validation uses fresh database time after dependency locks, and publication/apply revalidate after notebook waits. Worker leases are fenced again after blocking locks. Finish validates both already-persisted and newly supplied prepared dependencies and records use edges, including a cancelled preparation path. A cancelled job cannot restore forgotten text without becoming purge-addressable.

Erase does not undo code already explicitly applied to a notebook, erase user-authored prompts or unrelated notebook history, or promise erasure by an external provider. Minimal tombstones, source dependency identifiers, output hashes and idempotency receipts remain for authorization, audit and safe replay. SQL results, rows and logs are not implicit memory context. Backups and provider retention remain operator-controlled; the API's erasure statement concerns live DataSlate copies described here.

## Qualification evidence

Focused tests run against both MemoryStore and isolated PostgreSQL 17 with verified TLS and pgvector enabled. They cover concurrent idempotent correction, stale revisions, issuer/workspace ownership, immutable provenance, the 100-head limit, source-conversation deletion, viewer downgrade, policy revocation, safe replay after deletion, local recall, OpenAPI responses, and transitive correction purge. Independent context tests cover complete provenance closure, no automatic recall, source-policy transfer restrictions, no silent dependency truncation, and invalidated history.

Two concrete publication races have regression tests: cancellation before context-save followed by finish could formerly leave unindexed forgotten text; expiry while apply waited on a genuinely locked notebook row could formerly modify that notebook. Both now fail closed on MemoryStore/PostgreSQL where applicable. The PostgreSQL test observes the blocking session rather than assuming a sleep proves lock acquisition. Full API race tests and vet are the final integration checks; deployment/browser/container evidence is recorded separately in validation documentation. No paid external provider or production cloud qualification is implied.
