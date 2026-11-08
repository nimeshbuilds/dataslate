# Sylon memory adapter decision — November 2026

Research checked November 7–8, 2026. This is a component and security decision, not qualification of an installed Headroom or Hindsight service. The September research remains historical. DataSlate's implemented Sylon conversation contract is authoritative for current behavior.

Keep the first durable private memory service in DataSlate's existing PostgreSQL store. Treat Headroom as an optional future context optimizer and Hindsight as an optional advanced-memory adapter. Neither becomes the authority for DataSlate identities, source permissions, retention or tool execution.

## Current upstream findings

The observed releases are [Headroom v0.43.1, November 5](https://github.com/headroomlabs-ai/headroom/releases/tag/v0.43.1) and [Hindsight v1.2.0, November 3](https://github.com/vectorize-io/hindsight/releases/tag/v1.2.0). A release tag is a research reference; adopting a component additionally requires pinned dependencies/images and integration tests.

Headroom's default proxy is designed around a single user's local process. Its documentation explicitly says it is not a multi-tenant isolation boundary. It sees complete request traffic and provider credentials. Compression originals default to plaintext SQLite with lazy expiration, and its session telemetry is enabled by default. A shared proxy would therefore introduce identity, credential, persistence, configuration and egress boundaries that DataSlate must separately control. [Headroom security model](https://docs.headroomlabs.ai/docs/security-model)

Headroom's Compress-Cache-Retrieve flow injects a retrieval tool and handles continuation calls. This does not fit Sylon's current bounded, single-attempt generation transport, which rejects tool actions and does not make hidden provider retries. A transparent proxy is not an interchangeable implementation of that transport. [CCR architecture](https://docs.headroomlabs.ai/docs/ccr)

A future optimizer should run on explicitly eligible context before DataSlate's existing provider adapter. It must not control provider credentials, retry ownership, tool dispatch or persistent memory. Preserve exact active code, SQL, identifiers, policy text, citations and execution receipts. Start with repetitive logs/listings only once those authorized sources exist. Upstream describes limited benefits for short exchanges and code-only contexts and qualifies its example compression figures by workload. Measure actual task correctness, end-to-end latency and total usage; do not promise a fixed saving or infer answer accuracy from recoverable originals. [Headroom limitations](https://docs.headroomlabs.ai/docs/limitations)

Hindsight now documents server-enforced read tag scopes, separate write scopes, by-ID restrictions and recorded scopes for background refreshes. Its default API-key tenant extension still uses a shared `public` schema; background work lacks original request headers. Any adapter needs authenticated server-derived identities, strict scope modes and a persisted authorization envelope for deferred work. Bank IDs and client-supplied tags cannot establish authority. [Hindsight extensions](https://hindsight.vectorize.io/developer/extensions)

Hindsight's recall `max_tokens` is not a hard bound on the entire response: metadata is excluded and the highest-ranked result may be returned whole when nothing fits. DataSlate must bound decoded responses and assembled context itself. [Recall behavior](https://hindsight.vectorize.io/developer/retrieval)

Memory Defense is opt-in per bank, screens subsequent writes, and uses known patterns for redaction/blocking. It does not replace permission checks or guarantee that every secret is detected. Existing memories are not retroactively scrubbed. [Memory Defense](https://hindsight.vectorize.io/developer/memory-defense)

Hindsight supports Helm, external PostgreSQL and independently scaled workers. That makes a later managed or external Kubernetes profile feasible, but adds another worker lifecycle, model configuration and operational ownership boundary. [Installation](https://hindsight.vectorize.io/developer/installation)

## Initial DataSlate boundary

The first supported slice should contain explicitly saved, job-derived private memories. Free-form notes, automatic capture, inferred instructions, team sharing, autonomous recall and automatic model calls are separate capabilities. A memory is derived assistant content, not an authoritative record that a tool or query succeeded.

Each memory belongs to an exact workspace and owner `(issuer, subject)`, with a revision, source job/conversation, policy identity, expiry, and the complete server-resolved dependency closure. Include all source context even when the model omitted its citation. A memory derived through prior history must retain that history's restrictions. A dependent memory must retain parent memory revision dependencies as well as underlying source dependencies, so correction or forgetting cannot leave an unrestricted derivative behind.

Users select at most four exact memory IDs/revisions for a turn. Resolve them through current assistant policy and source-transfer opt-ins. Recheck availability, revision, expiry and all original permissions before transfer, after generation, on history reads and before applying proposed code. Final publication needs an atomic storage check against concurrent correction, expiry and deletion. Preserve the existing total dependency and context-byte budgets; do not silently discard protected material to fit.

Source conversation deletion invalidates or removes its memories. Forgetting, correction and expiry must invalidate dependent history and proposals, including uncited dependencies. Tombstones or generations must prevent queued background work from recreating forgotten content when asynchronous indexing/consolidation is later introduced. Backups need their own retention and restore/deletion replay policy. Forgetting cannot retract a request already delivered to a provider or automatically roll back a notebook edit the user previously applied; those boundaries must remain visible in the product contract.

Recalled text is untrusted data. It cannot change tool permissions, locked instructions, provider endpoints, credential handling or approval requirements. Source provenance is constructed by the server; browser-supplied hashes or dependency lists cannot confer authority.

## Optional expansion

Use explicit, bounded lexical recall for the initial private collection. Optional pgvector ranking can be evaluated later; it must not introduce automatic recall or model transfer. Keep private memory out of shared notebook/catalog indexes. Apply current scope before ranking and before any provider/reranker transfer. For small collections, exact filtered vector search is easier to qualify than approximate indexes whose filtered recall depends on index/search configuration. [pgvector filtering](https://github.com/pgvector/pgvector#filtering)

PostgreSQL row policies can provide defense in depth, but are not automatic: owners and privileged roles have bypass behavior. Preserve distinct migration/runtime roles and application authorization; qualify any added RLS session-context mechanism under connection pooling. [PostgreSQL row security](https://www.postgresql.org/docs/current/ddl-rowsecurity.html)

Add a Hindsight adapter only when graph/temporal recall and consolidation demonstrate value over the native baseline. Require fixed authenticated endpoints, bounded responses, source provenance, custom current-identity scopes, operation reconciliation, deletion propagation, separate database ownership, pinned images and explicit model-transfer approval. Test the OSS deployment actually shipped; upstream benchmark or cloud claims do not qualify DataSlate.

Add Headroom only after representative data-engineering evaluations show useful savings without correctness loss. A qualified transform must have explicit input classes, protected fields, timeout/failure behavior and metrics. Any retained originals need DataSlate-controlled scope, expiry, deletion and current-access checks. Disable unauthorized telemetry, shared memory and instruction mutation. Full CCR requires a separately designed authorized tool loop and durable continuation receipts.

Durable agent execution checkpoints are a separate concern. Introducing a workflow framework solely to persist these private memories is unnecessary; a future agent runtime can consume the same authorized memory service.

## Qualification gates

- Cross-owner/workspace IDs and stale binding policies are denied.
- Source revocation, correction, expiry and deletion during preparation or generation prevent publication or later reuse.
- History-derived and uncited dependencies retain their restrictions.
- Source conversation deletion removes memories and prevents late recreation/publication.
- Concurrent forget/correct/apply operations preserve revision and ownership rules.
- Memory injection text cannot alter system instructions, permissions or execution behavior.
- Provider calls retain durable attempt ownership and do not repeat after ambiguous failures.
- Optional optimization is compared against identical baselines for correctness, latency, reported token usage and total infrastructure/model cost.
