**Agent memory and context efficiency: component decision**

Research snapshot: September 24–25, 2026. Scope: a Kubernetes lakehouse serving teams inside one organization, with a native assistant and user-authored agents. This note supplements [agent architecture](research-dataslate-agents.md). Component choices below are recommendations based on primary documentation, not completed deployment or performance qualification.

**The likely name is Headroom.**

The closest match to the user's clarification—an open-source project with a similar name that improves token efficiency—is **[Headroom](https://github.com/headroomlabs-ai/headroom)**. The canonical repository is `headroomlabs-ai/headroom`; older references use `chopratejas/headroom`. It exposes a library, proxy, and MCP surface for reducing model context, including tool output compression and recovery of retained originals. This is a likely identification, not confirmation of which project the user saw.

The relevant Headroom project uses **Apache-2.0**, and its observed latest release is **v0.38.0, September 21, 2026**. Do not confuse it with unrelated repositories also named Headroom. [Tagged license](https://github.com/headroomlabs-ai/headroom/blob/v0.38.0/LICENSE), [release](https://github.com/headroomlabs-ai/headroom/releases/tag/v0.38.0).

Literal **Headliner AI** is a podcast/video product in the official results reviewed, not an identified agent-memory substrate. **Hindsight** is another similar-sounding project, but it primarily provides durable agent memory. These should remain separate candidates. [Headliner](https://www.headliner.app/video-podcasting-clip-videos-caption-videos/), [Hindsight](https://github.com/vectorize-io/hindsight).

**Decision: include memory in the AI core; qualify Headroom as an optional context optimizer.**

Use **LangGraph OSS + PostgreSQL checkpoints + a platform-owned memory service using PostgreSQL/pgvector** as the first supported stack. Add Headroom through a small, pinned middleware interface for selected content types. Keep a **Hindsight adapter** available for a later advanced-memory profile if experiments justify its operational and model costs. Do not install Mem0, Hindsight, Letta, and LangGraph as four overlapping authorities.

LangGraph distinguishes thread checkpoints from cross-thread stores. Its Postgres store supports optional pgvector search. Neither thread IDs nor memory namespaces are authorization mechanisms. The product must enforce caller access around the persistence interfaces. [LangGraph memory concepts](https://docs.langchain.com/oss/python/concepts/memory), [PostgresStore reference](https://reference.langchain.com/python/langgraph.store.postgres/base/PostgresStore), [LangGraph MIT license](https://github.com/langchain-ai/langgraph/blob/main/LICENSE).

**Memory is several different subsystems.**

| Subsystem | What belongs there | Proposed implementation |
|---|---|---|
| Execution checkpoint | Graph position, tool receipts, pending review, recent messages, resumable state | LangGraph PostgreSQL checkpointer behind the run service |
| Working context | Current question, relevant cell/schema/error, compacted conversation, selected tool results | Product context assembler with explicit token budgets |
| Episodic memory | A completed investigation, decision, or successful workflow with dates and sources | Versioned PostgreSQL records; pgvector plus full-text retrieval |
| Semantic memory | User preferences, approved business vocabulary, durable facts | Typed PostgreSQL records; references to authoritative catalog/semantic definitions |
| Procedural memory | Reviewed agent instructions, runbooks, examples, reusable tools | Versioned agent/project resources in Git and metadata storage |
| Knowledge retrieval | Documents, catalog descriptions, supported table extracts | ACL-aware ingestion and indexing; governed queries for live data |
| Prompt-prefix cache | Provider-side reuse of repeated prompt computation | Provider adapter capability with scoped retention/configuration |
| Tool/result cache | Previously computed outputs with freshness and policy context | Bounded object/metadata cache; optional Valkey if deployment scale warrants it |
| Compression originals | Unabridged context referenced by compressed tool results | Short-lived, access-controlled artifact storage with retrieval receipts |

Checkpointing resumes a run. Retrieval selects knowledge. Compaction reduces a conversation representation. Prompt caching can reduce repeated processing without reducing the amount of context represented. These capabilities complement each other; none makes another unnecessary.

**Why this initial stack.**

The platform already requires PostgreSQL, identities, an authorization service, artifacts, and agent workers. Reusing these yields fewer independent databases and lifecycle models. Start with structured memories and exact filtered retrieval for small collections. Add vector/full-text ranking when a task benefits from semantic recall. One vector per conversation is an inadequate design; retain independently attributable items that can be corrected and removed.

pgvector supports exact and approximate nearest-neighbor search. Its current documentation notes that filtering approximate searches can produce too few results and that tenants sharing an approximate index can affect each other's recall and speed. Use partitioned indexes or separate tables for meaningful security/workload domains, then measure filtered recall. Always apply authorization before content reaches the agent or reranker. [pgvector filtering and tenancy](https://github.com/pgvector/pgvector#filtering).

PostgreSQL row policies provide useful defense in depth, but database owners, superusers, and `BYPASSRLS` roles have special behavior. Use constrained runtime roles, controlled session context, correctly configured policies, and separate migration privileges. Do not give user-authored agents a shared database credential. [PostgreSQL row security](https://www.postgresql.org/docs/current/ddl-rowsecurity.html).

**Comparison of the specialized options.**

| Component | Useful contribution | Decision and limitation |
|---|---|---|
| Headroom | Context compression, recovery of originals, provider-context optimization; library/proxy/MCP integrations | Optional selected transforms inside our AI request path; it does not own product identities, agent publishing, or lake authorization |
| Hindsight | Retain/recall/reflect interfaces, fact and experience memory, consolidated observations, semantic/keyword/entity/time retrieval | Preferred advanced-memory evaluation candidate; require our permission and provenance adapter |
| Mem0 OSS | Extracted memory, configurable models/vector stores, library or self-hosted service | Reasonable alternative to benchmark if personalization/fact retrieval dominates; do not add beside Hindsight without a distinct use case |
| Letta | A complete stateful-agent runtime and memory-oriented agent experience | Alternative runtime to investigate independently, rather than add as a memory database underneath LangGraph |
| LangGraph persistence/store | Durable execution state and structured cross-thread memory storage | Default substrate; does not by itself implement all memory quality, curation, or security behavior |

Hindsight is MIT-licensed and documents a Helm deployment with separate API/worker scaling and external PostgreSQL. Its retained facts can be consolidated into observations and queried under a token budget. [Hindsight repository](https://github.com/vectorize-io/hindsight), [installation](https://hindsight.vectorize.io/developer/installation), [memory architecture](https://hindsight.vectorize.io/).

Its authentication extension needs deliberate integration: the built-in API-key extension uses the same `public` schema for all accepted requests; separate tenant schemas require another/custom extension. Operation validation is also extensible. A bank name is not an authorization check. Background consolidation has no original request headers, so a reliable scope/identity envelope must be persisted for deferred work. [Hindsight extensions](https://hindsight.vectorize.io/developer/extensions).

Mem0's core repository is Apache-2.0 and actively maintained. Current self-hosting includes PostgreSQL/pgvector, API-key authentication, and a dashboard. Its README explicitly distinguishes the published managed-platform benchmark results from the OSS implementation's available optimizations. We must test the actual OSS image we would ship. [Mem0 repository](https://github.com/mem0ai/mem0), [self-hosting](https://docs.mem0.ai/open-source/overview), [deployment options](https://docs.mem0.ai/platform/platform-vs-oss).

Letta's old Kubernetes/Docker guidance needs special caution: official documentation now says the old Docker image is no longer actively maintained/supported. The repository directs active work to `letta-ai/letta-code` and its App Server; the old V1 server is historical. Letta remains active, but installing the old container from a tutorial is not an appropriate new production default. [Letta current direction](https://github.com/letta-ai/letta), [legacy Docker status](https://docs.letta.com/v1-sdk/docker).

**How Headroom fits without weakening correctness.**

Proposed request path:

```text
Caller and agent permissions
  → authorized tools / memory retrieval
  → product context selection and freshness checks
  → optional qualified Headroom transforms
  → provider-native caching and model adapter
  → model response / typed tool requests
```

Use library integration first so the product retains per-request settings and scope. A shared transparent proxy introduces another identity, caching, and configuration boundary. If offered later, it must use the same policy envelope and disable arbitrary client reconfiguration. The platform should own model selection, reasoning budgets, egress, audit, and credential exchange even when middleware offers overlapping features.

Start compression with repetitive logs and tool-search listings. Preserve original errors, exact SQL, schema constraints, policy statements, active cell code, tool-call IDs, and execution receipts. Large data results should be aggregated by Spark/SQL into the exact answer, not compressed into a sample and then presented as a full-table computation.

Headroom documents content-specific behavior, protected recent code, compression thresholds, optional ML-model overhead, and passthrough on unsupported content. These constraints matter for notebook troubleshooting. A compressor that is beneficial on redundant JSON may be unhelpful on a short schema or precise code change. [Headroom limitations](https://github.com/headroomlabs-ai/headroom/blob/main/wiki/LIMITATIONS.md).

For recoverable compression, keep the original under an opaque artifact identifier tied to the request/security scope, not a bearer secret embedded in text. Retrieval rechecks current caller/agent access and source validity; expired/revoked originals cannot be reconstructed by the model. Bound TTL and bytes. Do not enable broad cross-agent shared memory or automatic edits to agent instructions merely because the middleware exposes them. Reviewed procedural updates should use the normal agent-version workflow.

Compare optimizer-enabled runs to identical baselines using provider-reported usage where available. Include retrieval retries, compression/embedding/consolidation calls, cache writes, outputs, CPU, and latency. Report observed tokens and billed usage separately from hypothetical savings estimates. No general claim of zero accuracy loss or a fixed reduction percentage is justified for this product before testing.

**Token efficiency starts before compression.**

1. Search authorized schemas and load relevant tables instead of sending the entire metastore.
2. Load tools by task; avoid thousands of definitions on every request.
3. Use SQL/Spark for aggregation, filtering, and joins. Return compact typed results with citations and snapshot IDs.
4. Retain the current goal, decisions, unresolved errors, exact identifiers, and source references when compacting; retrieve old detail only when needed.
5. Reuse deterministic tool results only while their input, data snapshot, principal scope, and policy version remain valid.
6. Keep stable prompt sections stable and apply provider-native caching where supported. Provider retention and model capabilities remain explicit connection settings. [Claude prompt caching](https://platform.claude.com/docs/en/build-with-claude/prompt-caching).
7. Use model/reasoning profiles for autocomplete, extraction, troubleshooting, and planning, chosen through our evaluations. An error or difficult task may justify more reasoning.
8. Run optional memory consolidation asynchronously with quotas and freshness markers, without hiding its costs.

The model context budget should be configurable by deployment, with categories for policies/instructions, working state, selected knowledge, tool definitions, recent results, and output headroom. A plain “use 80% of the model window” rule does not guarantee good answers.

**Memory security and quality are product responsibilities.**

Every durable item should carry an immutable ID, organization/workspace, sharing scope, subject, memory kind, provenance references, originating caller/agent/run, source snapshot/time, sensitivity, model/extraction version, created/updated/expiry times, and status. Distinguish user assertions, observed tool receipts, model hypotheses, and approved organizational facts. A model saying it completed an action must not become an authoritative record when the actual tool receipt disagrees.

Personal, project, and team memory are separate collections with explicit sharing. A summary inherits the restrictions of all its contributing sources. If a team needs a broadly shared version, generate it solely from content already allowed for that audience. Do not summarize privileged rows into an unrestricted “team memory.”

For live lake data, preferentially remember a query/metric/source reference and execution timestamp, then re-query under current rights. Exact historical results require authorized snapshot semantics and retention. Memory must never turn last week's answer into an unqualified current answer.

Correction must preserve event time and validity: “owner changed in September” and “the July report was wrong” are different updates. Record supersession/contradiction links and provenance; expose unresolved conflicts. Promote durable business definitions and operational procedures through ordinary versioned resources rather than allowing an agent's inferred recollection to silently change them.

Revocation invalidates related result caches, active memory selections, and dependent summaries. The next turn must not reuse a checkpoint containing restricted content just because its thread still exists. Rebuild/redact its effective context or block resume until that is possible. Source reauthorization belongs on the retrieval and resume paths, not only at ingestion time.

Deletion spans memories, embeddings, source fragments, dependent summaries, checkpoint copies, compression originals, traces, and exports covered by the retention contract. Maintain tombstones so asynchronous extraction cannot recreate a deleted item. Backups need documented expiration and deletion replay after restore. Hindsight exposes document deletion and re-consolidation behaviors, but the platform must verify all derivative artifacts across its own services. [Hindsight document lifecycle](https://hindsight.vectorize.io/developer/api/documents).

Treat recalled content as untrusted data. A remembered instruction may inform a task but cannot expand tool permissions, alter locked system rules, or approve an action. Gateway credentials and secrets must never be saved as memories. Memory access should be auditable without making sensitive memory contents visible to every operations user.

**The UI should make memory understandable.**

An agent's Memory tab should show personal versus shared items, their sources, last confirmation, expiration, and conflicts. Authorized users can remember, correct, forget, pause automatic capture, or promote a reviewed item into a project resource. Agent runs should show which memory items informed the answer, whether context was compacted, and whether a result was refreshed or reused. An administrator needs budget, retention, egress, and effective-policy settings without reading every conversation.

**Release gates.**

| Test | Required outcome |
|---|---|
| Two users, same question, different row policies | Each answer and retrieved memory respects that user's allowed data |
| Guessed thread, bank, memory, or artifact IDs | Access denied independently of identifier secrecy |
| Team memory synthesized from restricted evidence | Rejected or restricted to the contributing source scope |
| Role revoked after checkpoint or summary creation | Restricted content excluded on retrieval/resume/cache hit |
| Delete while consolidation is queued | Deleted content does not reappear; dependent artifacts are invalidated |
| Conflicting facts and changed ownership | Dates, supersession, and uncertainty remain visible |
| Compression omits one critical numeric value/error | Task correctness gate detects regression; qualified profile falls back |
| Original compressed content expires | Retrieval returns a clear expiry and re-fetches only if authorized |
| Agent claims success after a timed-out write | Tool receipt reconciliation remains authoritative |
| Memory/optimizer/backend pod restart | State is durable where promised; requests resume without duplicate effects |
| Enabled versus disabled optimization | Compare end-to-end task success, latency, total usage and infrastructure cost |

**Final component recommendation:** PostgreSQL/pgvector and LangGraph persistence in the initial AI installation; native product memory controls; optional qualified Headroom library middleware; Hindsight as the first advanced-memory adapter to benchmark. Keep Mem0 as a credible alternative and Letta as a separate runtime choice, not mandatory additions.
