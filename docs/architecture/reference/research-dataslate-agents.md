**DataSlate agents and Doot: product and implementation research**

September 24–25, 2026. Extension to the [Kubernetes blueprint](kubernetes-lakehouse-product-blueprint.md) and [cloud lake / AI experience specification](cloud-lake-and-ai-experience.md). Target: teams within one organization. This is a proposed architecture informed by current primary documentation; integrations have not been deployed or security-certified.

**Product recommendation.** Brand the platform **DataSlate** and its built-in assistant **Doot**. Doot can help with notebook cells, SQL, data questions, and operational diagnosis, while **Agents** is a first-class workspace destination for creating and operating the organization's own assistants. “Agent Studio” is a suitable name for the builder. Users should be able to turn a successful Doot interaction into an agent draft, while choosing its actual capabilities, permissions, data definitions, and evaluation cases before publication.

The first release should include governed data analyst, document knowledge assistant, and pipeline diagnostic templates. These cover real daily work without requiring a visual multi-agent graph editor to be the first experience. A supervisor template can follow once individual agents have dependable tool and identity contracts. Keep Doot's familiar appearance across surfaces, but visibly distinguish the built-in assistant, the organization's published agents, and editable drafts.

**What current Databricks actually offers.** Current documentation describes UI prototyping in AI Playground, Knowledge Assistant, Supervisor Agent, Python authoring with multiple frameworks, MCP/tools, tracing/evaluation, and beta registration of externally hosted Agent Services. These are distinct capabilities; deploying a model alone does not reproduce the lifecycle. [Databricks agent overview](https://docs.databricks.com/aws/en/agents/custom-agents/build-agents), [agent service registration](https://docs.databricks.com/aws/en/ai-gateway/agent-services).

Supervisor Agent coordinates Genie agents, custom agents, MCP servers, catalog functions, and other tools. It requires access checks for the end user and constituent resources. Its documented sandbox has specific restrictions, illustrating that code execution is a separate product/security subsystem. DataSlate should implement its own qualified restrictions rather than assume an agent framework supplies them. [Supervisor Agent](https://docs.databricks.com/aws/en/agents/agent-bricks/multi-agent-supervisor).

The authoring flow on Databricks Apps uses templates and MLflow's agent interfaces to connect code, streaming responses, tracing, tool confirmations, and deployment. Some features and limitations vary by hosting path. Copy the complete development workflow as a product objective, while publishing DataSlate's own support matrix. [Databricks agent authoring](https://docs.databricks.com/aws/en/agents/custom-agents/author-agent).

**Separate three execution concepts in the API.**

| Resource/work | Meaning | Appropriate backend |
|---|---|---|
| Interactive agent run | Bounded model/tool decisions, streaming progress, possible human input | Isolated agent worker, persistent graph checkpoints, product run service |
| Scheduled automation | An authorized recurring invocation with fixed identity, data scope, budget and output destination | Existing DataSlate/Airflow scheduling integration starts an ordinary agent run |
| Model or agent endpoint | An authenticated API address with version routing, scaling and request admission | Kubernetes Deployment/Service behind DataSlate gateway; optional serving integration |

Airflow is suitable for the schedule and surrounding deterministic data workflow. It should not become the low-latency token stream or own every conversational step. A model endpoint does not by itself provide tools, durable state, authorization, or safe code execution. Avoid two schedulers or two controllers independently retrying the same logical run.

**Builder experience: configure, test, then publish.**

1. Choose a template, name, purpose, owner, and workspace. Doot can generate a proposed configuration from a goal.
2. Choose an approved model connection/alias. Display supported tool calling, streaming, structured outputs, data region, limits, and the effective authentication profile.
3. Select knowledge: governed tables, semantic spaces, approved document collections, or a defined API. Show which sources are current and which need indexing or permission setup.
4. Select versioned tools. Describe what each can read or change in plain language; show resource scope and run identity. The administrator controls which tool types and endpoints can be added.
5. Set behavior, output schema, memory policy, budget, deadline, maximum steps, and any configured action-review policy. Advanced code is available in the same project, using an immutable runtime environment.
6. Test in a side-by-side playground: configuration/code, conversation, tool calls, citations, trace, latency, token usage, and effective identity. Permission simulations must be access-controlled and must not expose data to an unauthorized tester.
7. Run evaluations, create an immutable version, and promote to a deployment. Share through workspace chat, an API, an approved MCP endpoint, or a scheduled workflow.

A developer can export/edit/import the same declarative definition through Git and CLI. The UI must not maintain a separate hidden agent definition. Put an ordinary readable form before an optional graph view; display the actual executed trace rather than a decorative graph.

**Reuse OSS foundations, retain product ownership.**

| Foundation | Useful contribution | DataSlate still builds |
|---|---|---|
| LangGraph OSS library | State graphs, persistent checkpoints, streaming, interrupts, bounded retry/fault-handling mechanisms | Request admission, user authorization, tenant-safe persistence access, run ownership, tools, deployment, audit, cancellation and UI |
| MLflow | Traces, evaluations, prompt/version associations, experiment and artifact records | Product-facing lifecycle, access mapping, publication gates, secure trace view and cross-service correlation |
| MLflow Agent Server / agent interfaces | Serving wrapper and common streaming/schema integration | Authenticated public API, request isolation, deployment policy, resilience and provider compatibility |
| MCP protocol and SDKs | Standard external tool discovery/invocation/auth mechanisms | Curated tool inventory, trust policy, effective identity, secret brokering and capability enforcement |
| Existing DataSlate run/query/catalog services | Spark workloads, governed queries, catalog metadata, output publication | Agent adapters to these existing interfaces; avoid privileged alternate paths |

Use the LangGraph library with a PostgreSQL-backed checkpointer for the first supported runtime. Its checkpoint layer stores conversation/graph state; cross-thread long-term stores are separate. In-memory checkpointers lose state on restart. An opaque thread identifier selects state but is not proof of authorization. [LangGraph persistence](https://docs.langchain.com/oss/python/langgraph/persistence).

MLflow provides a suitable shared evaluation/trace substrate. Its newer MCP Registry is **experimental**, introduced in 3.15.0; keep an adapter boundary rather than making experimental registry semantics the sole authority for production permissions. MLflow Agent Server supplies serving conveniences, not the complete Kubernetes tenant boundary. [MLflow GenAI](https://mlflow.org/docs/latest/genai/), [MCP Registry](https://mlflow.org/docs/latest/genai/mcp-registry/), [Agent Server](https://mlflow.org/docs/latest/genai/serving/agent-server/).

MLflow now supports opt-in workspaces and resource authorization. Its documentation explicitly distinguishes logical workspace separation from hard infrastructure isolation. Map DataSlate users and workspaces explicitly, qualify the chosen version's auth behavior, and use separate deployments for security domains that need them. Do not give every author a token that reads all traces. [MLflow workspaces](https://mlflow.org/docs/latest/self-hosting/workspaces).

Supporting other authoring libraries later is practical through a documented agent interface. “Bring any framework” should initially mean a user-supplied, reviewed container implementing that interface; it does not imply every framework inherits checkpoint/resume behavior or identical event semantics.

**Proposed configurable resources.** These names are design proposals, not existing shipped APIs.

| Resource | Essential fields |
|---|---|
| `AgentDefinition` | Workspace, owner, instructions/template, authoring source, model alias, knowledge references, selected tools, response schema |
| `AgentVersion` | Immutable definition hash, source commit, image digest, prompt/tool/schema versions, resolved model connection/config, evaluation references |
| `AgentDeployment` | Version, endpoint, invoke grants, identity mode, resource profile, autoscaling, traffic policy, network policy, budget |
| `ToolDefinition` / `ToolVersion` | Typed input/output, implementation digest or remote schema snapshot, declared side effects, allowed resources, authentication binding, idempotency/reconciliation contract |
| `KnowledgeBinding` | Catalog/semantic/index references, authorized retrieval mode, freshness policy, document ACL mapping, embedding model/version |
| `AgentRun` / `ToolInvocation` | Caller and execution principal, delegation, parent/child relationships, selected version, policy versions, attempt, state, deadline, operation receipt, trace references |
| `ActionReview` | Exact action hash and arguments, affected resources, requesting run, authorized reviewers, decision, expiry, single-use status |
| `MemoryPolicy` | User/team sharing scope, retention, allowed data classes, encryption, deletion, provenance and reauthorization behavior |
| `EvaluationSuite` | Dataset versions, expected tool behavior/results, authorization cases, quality metrics, budgets, thresholds and reviewer feedback |

Configuration should inherit organization → workspace → project → deployment defaults with a visible effective-settings view. Lower levels can tighten controls but cannot loosen an administrator's locked policy. Validation returns precise errors for incompatible provider capabilities, forbidden endpoints, missing source permissions, or unsupported execution profiles.

**Identity is checked independently of the model.** For interactive data agents, every tool operates with the caller's effective rights, further limited by the agent deployment and tool policy. The serving pod's Kubernetes identity pays for infrastructure and obtains narrowly scoped service credentials; it is not an automatic data privilege for the human. For scheduled agents, use an explicitly selected workload principal with fixed grants and approved output recipients. Invocation rights alone do not grant access to every dataset or subagent used by an agent.

Never silently fall back from a missing/expired caller delegation to an app service principal. This needs a specific regression test: current Databricks documentation warns that one user-client helper can fall back to its app identity if the forwarded user token is missing. DataSlate's caller-scoped mode should fail closed. [Databricks agent authentication](https://docs.databricks.com/aws/en/agents/custom-agents/agent-authentication).

All tools route through an enforcement service outside user-authored agent code. Typed tools accept stable resource IDs; they do not trust model-supplied SQL comments, bucket paths, identities, or URLs to define authority. Query tools use DataSlate's governed engine with the current caller and row/column policies. Job tools submit ordinary authorized DataSlate runs with fixed source and runtime. Mutating tools have explicit grants and configured review requirements; read-only and already authorized operations do not need unnecessary repeated dialogs.

Provider authentication, end-user login, and data-tool authorization remain separate. Server-side secret references can represent API tokens, OAuth clients, JWT assertions, token exchange, or workload identity as specified in the cloud/AI extension. Restrict allowed endpoint destinations and credential audiences, rotate credentials, and exclude them from prompts, checkpoints, traces, and client-side configuration.

For HTTP MCP, validate that access tokens are intended for the MCP server. Obtain separately scoped downstream credentials using the supported exchange/auth flow. Do not forward an arbitrary inbound token to another API. The current MCP authorization specification explicitly requires audience binding and forbids token passthrough. [MCP authorization](https://modelcontextprotocol.io/specification/2025-11-25/basic/authorization), [MCP security guidance](https://modelcontextprotocol.io/docs/2025-11-25/tutorials/security/security_best_practices).

**Memory and retrieval are data-access paths.** Partition conversations, checkpoints, embeddings, caches, generated files, feedback and traces by workspace and applicable user/security scope. Filter document retrieval using current source ACLs before content reaches the model; a vector similarity result is not authorization. Prefer links/resource references for sensitive durable memory. If content is retained, record source identity, policy and snapshot so later access can be reauthorized, redacted, or invalidated after revocation. Do not allow an unrestricted team summary to remember one user's restricted rows.

Logs and traces may contain table rows, prompts, tool results, or customer text. Configure bounded/redacted content capture, encryption, retention, artifact grants and deletion. Operators can inspect health without automatically reading the content of every conversation. Memory deletion must cover persisted graph state and vector/object artifacts according to the documented policy, not just hide a conversation in the UI.

**Code execution needs a real sandbox contract.** Generated Python runs in disposable, restricted workers with no cluster-admin credentials, raw lake credentials, host mounts, unrestricted network, or inherited notebook secrets. Provide only authorized bounded extracts and an approved tool channel. Enforce CPU/memory/process/time/output limits. Use an optional stronger runtime class or dedicated node pool when required. Trusted deployable agent code is still isolated from the API/control-plane process; users cannot upload code into the shared privileged service.

The notebook Doot assistant proposes cell edits anchored to the current revision, presents a diff, and preserves undo. Applying and running remain explicit actions or a deliberately configured authorized mode. A pipeline-repair agent should publish a change proposal, validate it, and promote through the ordinary job-version workflow; it should not silently patch a running production notebook.

**Durable actions require more than a checkpoint.** LangGraph interrupts can persist a pause for human input, but resuming restarts the affected node; effects before the interrupt may repeat. Use persistent `ActionReview` records and tool invocation receipts. Bind a review decision to the exact action/arguments/version, verify the reviewer, enforce expiry, consume it once, and reauthorize the operation when resumed. Changed arguments create a new action. [LangGraph interrupts](https://docs.langchain.com/oss/python/langgraph/interrupts).

Each side-effecting tool has a stable operation ID and a defined idempotency/reconciliation strategy. After a timeout with unknown outcome, reconcile against the destination rather than ask the model whether to retry. Node retries do not make an external write transactional. Classify transient provider failures separately from invalid inputs, denied permissions, or unknown write outcomes. Set limits on steps, recursive subagents, concurrency, elapsed time, tokens, and monetary budget. [LangGraph fault tolerance](https://docs.langchain.com/oss/python/langgraph/fault-tolerance).

Cancellation persists first, blocks new dispatch, and propagates to descendants/provider requests where supported. It does not undo a completed external action. Suspended human-review runs should release active compute. Upgrade/rollback must preserve the graph and code version associated with existing runs or drain them deliberately; a new graph cannot safely interpret old checkpoints merely because its agent name matches.

**Evaluation and release gates.** Capture deterministic assertions for tool arguments, query results, authorized sources, output schema, and action counts; supplement them with human review and calibrated model judges. Re-run the suite when prompts, tools, provider/model aliases, runtime, semantic definitions, or data access policies change. MLflow can associate traces/evaluations with app versions and prompts, while DataSlate owns pass/fail promotion policy. [MLflow version tracking](https://mlflow.org/docs/latest/genai/version-tracking/).

Initial acceptance tests must include: two users asking the same question under different row policies; guessed thread IDs; cross-workspace trace/artifact access; expired delegation with a privileged app identity available; malicious instructions in retrieved documents; forbidden egress; tool schema drift; recursive agent loops; duplicate dispatch after pod death; a paused action resumed after role revocation; cancel during tool dispatch; and a checkpoint restored during a deployment upgrade. These are release criteria, not claims that the selected libraries have already passed them.

**First coherent delivery.** Ship Agent Studio with the three templates, native playground, typed governed tools, per-user chat, persisted runs, evaluation comparison, private endpoint deployment, and a trace/usage page. Follow with custom Python agent packages, scheduled agents, approved external MCP tools, and a supervisor template. This creates the in-house agent capability the user requested while sharing DataSlate's existing identity, query, runtime, deployment, and operational foundations.
