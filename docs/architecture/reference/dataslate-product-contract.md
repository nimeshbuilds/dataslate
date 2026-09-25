**DataSlate: product identity, Doot, internal agents and the experience contract**

**Naming update:** DataSlate and Doot below are historical working names. The user requested alternatives; [the current consolidated plan](platform-components-and-naming.md) recommends LakeMantle / Sylun as a preliminary, uncleared pairing. The behavior and security requirements continue to apply regardless of the final name. The [memory specification](research-agent-memory.md) adds PostgreSQL/pgvector memory, optional Headroom compression and memory lifecycle controls; [the result specification](research-results-visualization.md) adds the shared Table / Visualize / Profile / Execution experience.

Working product direction, September 24–25, 2026. DataSlate is the user's preferred working name for the Kubernetes platform; Doot is the preferred assistant name. This document adds requirements to the [architecture blueprint](kubernetes-lakehouse-product-blueprint.md) and [cloud lake / AI specification](cloud-lake-and-ai-experience.md). It describes an intended product and a sample interface concept, not an implemented or security-certified platform.

**Product identity.** Use **DataSlate** for the whole platform, **Doot** for the integrated assistant, **Doot Code** for its notebook/SQL engineering capabilities, and **Agent Studio** for building reusable internal agents. Published customer-created agents have their own names and owners; they are not all named Doot. The product descriptor can be “The Kubernetes data workspace.”

DataSlate owns the complete connection → data → development → execution → governance → analysis → AI lifecycle. Open-source backend services are implementation foundations. Users should be able to complete everyday work through DataSlate's UI, API and CLI, while administrators retain useful access to upstream diagnostics.

Doot's visual direction can be a small hovering messenger with a slate-like core and a restrained light/spark motif. Use a recognizable compact mark beside cell actions and a richer character only where space supports it. Match light/dark themes, provide a monochrome form and respect reduced motion. Execution states such as awaiting review, stopped, failed and completed remain explicit text. The current concept uses a provisional sparkle mark, not a completed mascot or logo.

The user wants Databricks-level familiarity and quality. Treat notebook interaction, SQL authoring, source/history, catalog navigation, compute attachment and contextual assistance as concrete behavior references. Establish DataSlate's own consistent visual identity across those workflows. A static screen is only an early review artifact; autosave, keyboard operation, reconnect, collaboration, error recovery and performance determine the actual experience.

**One recognizable interface across resources.**

| Area | Principal work |
|---|---|
| Workspace | Projects, folders/Git, notebooks, SQL, dashboards, recent and pinned resources |
| Data | Catalogs, tables/files, connections, discovery, quality, lineage, access and maintenance |
| Pipelines | Ingestion, transformations, immutable deployment revisions, schedules and backfills |
| Runs | Unified progress, attempts, logs, output snapshots, failure explanation and recovery |
| Compute | Runtime environments, sessions, jobs, quotas, admission and resource use |
| AI | Doot conversations, published agents, Agent Studio, evaluations, deployments and traces |
| Administration | Identity, policies, effective configuration, approved providers/tools, integrations and health |

Use shared components for object headers, search, permissions, versions, activity, dependency references, configuration origins, run timelines and diff review. Keep tabs and selected resources when switching tasks. Doot opens in context, with visible references to the current cell, source revision, runtime, table or run. Changing workspace must clear or deliberately reauthorize that context rather than silently reusing another team's data.

Current Databricks distinguishes Genie Code from curated domain agents; its setup and notebook collaboration documentation provide useful reference behaviors. We should preserve that clarity between the built-in assistant, a saved domain assistant and independently deployed automation. [Genie Code](https://docs.databricks.com/aws/en/genie-code/features-capabilities), [Genie Agent setup](https://docs.databricks.com/aws/en/genie-agents/set-up), [notebook collaboration](https://docs.databricks.com/aws/en/notebooks/notebooks-collaborate).

**Creating an internal agent is a core AI-enabled workflow.** The initial templates should cover a governed data analyst, a pipeline investigator and a document knowledge assistant. Start with forms and a test playground; add a visual supervisor/multi-agent graph when the underlying identity and execution contracts are dependable.

The creation flow is:

1. Choose a template or create a draft from a useful Doot conversation. Select a purpose, owner and workspace.
2. Choose an approved model connection and task alias; display capabilities and data-transmission policy.
3. Bind knowledge: permitted tables, semantic definitions, document collections or approved APIs, with their source permissions and freshness.
4. Select versioned typed tools and scopes. Explain what each tool can read or change and which identity it uses.
5. Set instructions, output schema, memory policy, limits and applicable action-review behavior. Advanced authors can use a supported code interface and reproducible runtime.
6. Test with a conversation and inspect the actual tool calls, evidence, identity, trace, latency and usage. Restricted permission simulation cannot reveal data to an unauthorized tester.
7. Evaluate against versioned examples, create an immutable version and publish it to authorized workspace users or a private endpoint. Scheduled use requires a separately authorized execution identity and output destination.

A draft does not become a deployment merely because the model responds successfully. Published versions bind source, instructions, tool schemas, runtime image, model configuration, knowledge references and evaluation evidence. Updating a model alias, prompt, tool or runtime triggers the relevant regression evaluation and rollout policy. Keep authorized access to previous versions and a defined rollback path.

Current Databricks offers UI prototyping, knowledge/supervisor agents, custom code, tools and deployment interfaces. This supports the feasibility of the desired lifecycle, while DataSlate must implement its own control plane and security boundaries. [Databricks agent building](https://docs.databricks.com/aws/en/agents/custom-agents/build-agents), [supervisor agents](https://docs.databricks.com/aws/en/agents/agent-bricks/multi-agent-supervisor).

**Reuse execution libraries while keeping DataSlate authoritative.** Use a qualified LangGraph OSS runtime with durable PostgreSQL-backed state as the initial implementation candidate. Use MLflow for traces, evaluations, prompt/version associations and model/agent artifacts. Treat optional serving wrappers and experimental registry features as adapters. Agent code runs outside the privileged product API process. [LangGraph persistence](https://docs.langchain.com/oss/python/langgraph/persistence), [MLflow GenAI](https://mlflow.org/docs/latest/genai/).

The architecture is DataSlate API → policy/admission → agent worker → model gateway and typed tool service → existing DataSlate query/run/catalog APIs. All paths emit correlated events. The model receives tool descriptions and scoped results; it cannot select its own effective user or retrieve administrative credentials.

Use three distinct execution concepts: a conversational AgentRun, a schedule invoking a versioned agent, and an endpoint exposing a deployed version. Airflow can own the schedule; the agent runtime owns the bounded conversation/decision graph; the product owns request admission, tool effects, identity, cancellation and deployment. A suspended review should release active compute. Avoid two components independently retrying the same action.

First-class resources include AgentDefinition, AgentVersion, AgentDeployment, AgentRun, ToolDefinition/Version, ToolInvocation, KnowledgeBinding, MemoryPolicy, EvaluationSuite and ActionReview. Reuse the existing identity, Run/Attempt, artifact and audit models. Each resource has an owner, workspace, immutable or versioned identity and an explicit authorization policy. A conversational thread ID is a selector, not proof of access.

**Security must sit outside the agent's reasoning.** For caller-scoped interactive use, effective tool access is limited by the caller's current rights, deployment capability grants, tool scope and organization policy. If delegated user credentials are missing or expired, fail closed. Never fall back to a more privileged app identity.

Scheduled agents use an explicitly approved service principal with fixed purpose, grants, destinations and limits. Separate permission to invoke an immutable deployment from permission to edit its instructions/code, add tools, select run-as identity or publish changes. Someone who can change a privileged agent's behavior can potentially exercise its privileges; draft and deployment permissions must reflect that fact.

Ordinary authorized read operations should not trigger repeated dialogs. Consequential actions follow configured grants and review rules, with any decision bound to the exact action, arguments, version, reviewer and expiry. Persist and atomically consume single-use action authorization; changed arguments require a new decision, and retries reconcile the existing invocation receipt. A model's automatic approval classifier can assist workflow but cannot enforce authorization. Databricks itself documents this boundary for Genie Code's classifier. [Genie Code agent mode](https://docs.databricks.com/aws/en/genie-code/agent-mode).

External MCP servers and APIs are explicit integrations. Validate their identity and token audience, use scoped downstream credentials, restrict destinations and version the accepted tool schemas. A newly discovered remote tool does not automatically become available to existing agents. MCP's authorization specification forbids token passthrough; do not forward a UI or upstream access token to an arbitrary downstream tool. [MCP authorization](https://modelcontextprotocol.io/specification/2025-11-25/basic/authorization).

Memory, retrieved documents, embeddings, generated files, conversations and traces are data-access paths. Enforce workspace/user scope, source ACLs, retention, deletion and revocation behavior. Do not put restricted rows into an unrestricted team memory summary. Separate operational health visibility from permission to read prompt/tool content. The existing row/column enforcement service remains authoritative for all data-query tools.

Generated code executes in constrained disposable workloads with bounded CPU/memory/time/output, restricted network access and approved data inputs. It does not inherit the user's broad cloud credentials, control-plane service account or unrelated notebook secrets. Supported privileged engineering workflows remain explicit deployed jobs, subject to the normal revision and delegation controls.

Durable checkpoints do not make external actions exactly-once. Store tool-operation receipts and reconcile uncertain outcomes before retry. LangGraph interrupts may restart a node on resume, so pre-interrupt effects need idempotency or a separate durable boundary. Revocation and cancellation are checked before dispatch and again when a paused action resumes. New graph code must not casually interpret old checkpoints. [LangGraph interrupts](https://docs.langchain.com/oss/python/langgraph/interrupts).

**Highly configurable means inspectable and constrained configuration.**

| Level | Typical responsibilities |
|---|---|
| Installation | Managed/external dependencies, backend versions, storage, ingress/TLS, secret stores, observability, node/runtime support |
| Organization | Identity, approved model/tool endpoints, data egress, regions, maximum quotas and prohibited capabilities |
| Workspace | Team membership, scoped connections, available runtimes, AI enablement and budgets within organization limits |
| Project/resource | Preferred environment/model, source/data bindings, schedules, retention and tool selection within delegated scope |
| Run | Validated parameters and temporary overrides permitted by the deployment contract |

Defaults can be overridden where permitted; hard restrictions cannot be weakened at lower levels. Allowed sets intersect, resource ceilings narrow, and scalar defaults follow a documented precedence rule. An organization's AI-off rule, forbidden endpoint or identity constraint cannot be overridden by a project manifest, notebook or Doot prompt.

Provide an Effective settings view with value, source, allowed range, locked reason and last validation. Show a preview of affected resources before a configuration change. Resolve defaults into each immutable deployment/run record, while continuing to enforce current revocations. Secret values never appear in diffs, manifests or prompts; configurations hold secret references.

UI forms, declarative manifests, CLI/API calls and Doot tools use the same schema and validation service. Advanced options should be supported extensions with discoverable capabilities. Arbitrary raw pod templates, security plugin settings, Java options, service-account names or secret mounts are execution privileges, not harmless personalization.

**Seamless backend integration is an operational contract.** Every adapter reports ownership mode, supported version/capabilities, credential mode, readiness, last verification and health. Use one correlated run ID through Spark, ingestion, maintenance, orchestration and agents, with separate scoped security principals where required; retain upstream evidence alongside normalized status. A failure in an optional AI provider must not break ordinary notebook editing. A failed policy dependency must not cause protected queries to proceed.

Make updates and recovery part of the experience: preview changes, test on a canary workspace, reconcile active workloads, preserve immutable revisions, retain audit and expose actionable degraded states. Avoid requiring users to separately configure the same identity or role in several backend consoles. The administrator can inspect backend-specific details when necessary, while the product owns synchronization and explains incomplete configuration.

**Quality criteria for this product promise.** Users must complete the principal lake-to-notebook-to-job-to-query workflow without copying Kubernetes YAML. Notebook suggestions cannot block typing, stale patches cannot overwrite collaborative edits, and every accepted edit remains undoable. Users should see the effective identity, data scope, model destination and action limits before agent publication. Every queued/failed operation exposes a responsible component and a permitted next action. The UI must explain why a setting or capability is unavailable without exposing forbidden resource names or secrets.

Release tests cover two users with different row policies, guessed conversation IDs, leaked trace/artifact access, missing caller delegation, revoked roles during review, malicious retrieved instructions, forbidden egress, recursive agents, duplicate tool dispatch after pod death, cancel races, and resumed runs across upgrades. Evaluate correct answers and tool effects, not fluent explanations alone. See [the agent research note](research-dataslate-agents.md) for the technical evidence and fuller test plan.

**Scope and sequencing.** Include Agent Studio and a small set of governed internal-agent templates in the first production AI capability. Build them on the established query, identity and execution APIs during beta. Arbitrary framework hosting, supervisor graphs, external marketplace tools and autonomous production modification remain subsequent qualifications. This adds meaningful work to the earlier estimate; staffing and delivery should be revised after the runtime, delegation and UI proof, rather than treating agent creation as a free side effect of enabling a model.

**Working-name observations.** A bounded public search found adjacent uses: Socrata/Tyler documentation refers to DataSlate pages; brAInam publishes a Doot AI assistant; SPARC Labs uses Doot for an AI-agent architecture. Keep the user's working names while recording these overlaps. “Doot by DataSlate” clarifies product association but does not establish exclusive rights or availability. No domain registration or trademark clearance was performed. [Socrata documentation](https://support.socrata.com/hc/en-us/articles/202950278-Understanding-User-Roles), [Doot app](https://play.google.com/store/apps/details?id=ai.brainam.doot), [Doot architecture](https://www.sparc-labs.in/resources/doot/).
