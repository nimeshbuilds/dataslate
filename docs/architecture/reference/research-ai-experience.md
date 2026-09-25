# Mini Genie, model gateways, and the notebook/SQL experience

Research snapshot: 24 September 2026. This note extends the Kubernetes lakehouse product blueprint for an installation shared by teams within one organization. It describes a proposed product; integrations have not been implemented or certified. OpenAI-specific findings were checked using the OpenAI Docs skill and fetched official documentation. Other claims use primary provider/project sources.

## Product decision

Make AI one optional platform capability with three connected experiences: data conversation, inline development assistance, and an engineering agent. They share the same model connections, identity rules, semantic definitions, execution APIs, and audit trail. If AI is disabled or unavailable, the notebook, SQL editor, ingestion, scheduling, and troubleshooting fundamentals continue to work.

Current Databricks documentation establishes a useful benchmark: Genie Code spans notebooks, SQL, files, jobs, and dashboards, and provides inline completion, quick fixes, deeper error diagnosis, and catalog-aware help. Its notebook documentation describes cell-level prompts, SQL/Python assistance, multi-cell agent changes, and a diff-based accept/reject workflow. Accepting an ordinary suggested edit does not itself execute the code. This is a product behavior reference, not an available OSS Genie implementation. [Databricks capabilities](https://docs.databricks.com/aws/en/genie-code/features-capabilities), [notebook coding help](https://docs.databricks.com/gcp/en/notebooks/code-assistant).

The proposed minimum experience:

| Surface | User experience | Required product context and behavior |
|---|---|---|
| Data conversation | Ask business questions; inspect SQL and chart; drill into results; save as query/dashboard | Permission-filtered catalog and semantic definitions; governed query execution; explicit freshness and provenance |
| Notebook cell | Ghost-text completions; selection-aware edits; explain/refactor/document; generate cells from a prompt | Stable cell IDs, source revisions, language, compatible runtime, neighboring relevant cells, and permitted table schemas |
| Failed notebook cell | “Explain error” and “Suggest fix” beside the original error | Cell/run correlation, stack trace, environment version, relevant preceding executions, engine diagnostics; patch preview |
| SQL editor | Schema-aware completion, natural-language SQL, explain plan, fix parser/permission errors, improve query | Exact dialect, catalog/schema selection, authorized metadata, current SQL text, server-side EXPLAIN and execution controls |
| Job/run page | Explain failure and propose a code/configuration change | Immutable deployed revision, attempt history, authorized logs, lineage, recent environment changes |
| Ingestion setup | Suggest schema mappings and validation rules | Sample provenance, parser options, source contract; changes remain explicit configuration |

## Build a first-class editor, not a chat window attached to an iframe

Reuse the notebook document model, editor components, and kernel protocols where practical. JupyterLab explicitly publishes reusable components and standalone application examples; Jupyter Server exposes contents, session, kernel, and interrupt/restart APIs. These are useful foundations, but they do not deliver our authorization model, deployment history, SQL service, AI experience, or unified application navigation. [JupyterLab reuse](https://jupyterlab.readthedocs.io/en/stable/extension/components.html), [Jupyter Server API](https://jupyter-server.readthedocs.io/en/latest/developers/rest-api.html).

The product should provide durable autosave, revision history, keyboard operation, split panes, multi-cursor editing, cell folding, search, rich tables and charts, downloadable permitted results, output truncation/pagination, reliable reconnect, compute status, and clear cancellation. Reuse CodeMirror 6 through the selected Jupyter components or qualify another editor in a short prototype; avoid maintaining two competing editors without a clear need.

Keep ordinary language intelligence separate from AI completion. Language-server/kernel completions, SQL metadata completion, formatting, diagnostics, and navigation should remain available without a model connection. AI completion is a separately configurable, cancellable enrichment. Completion latency cannot block typing. Debounce requests, cancel superseded requests, discard responses for outdated text revisions, and render ghost text only when its insertion anchor still matches.

Every AI edit carries a base document revision and affected cell IDs. Apply a structured patch with a diff, not a whole-notebook replacement. If another collaborator changes those cells, show the conflict or regenerate against the new revision. Preserve undo. Do not attach execution credentials to collaborative document state. Authorize each execution separately; document sharing does not imply kernel or secret sharing.

Cell troubleshooting must differentiate source errors, dependency errors, stale/out-of-order state, permission failures, compute admission failures, OOMs, unavailable storage, and actual data-quality problems. Show the original diagnostic beside the explanation. An unavailable LLM does not hide the stack trace or prevent retry. “Fix permission error” should explain/request the appropriate grant through existing workflows, not suggest bypassing enforcement.

Execution remains an explicit product action. Offer “Apply changes” and “Apply and run” when the user has execution rights, plus a clearly scoped agent mode that can run authorized development actions. Production table writes, environment changes, and deployed job edits follow the same permissions and promotion rules as human actions. A generated script cannot select a more privileged run-as identity than its user is allowed to delegate.

## ModelConnection: separate protocol, authentication, and capabilities

Introduce a versioned `ModelConnection` resource, scoped to an organization or workspace. Its fields should cover endpoint type, allowed endpoint/base URL, provider API version, region/project/deployment identifiers, model aliases, authentication configuration, secret references, trust bundle, timeout/retry budgets, capabilities, data-handling policy, and usage limits. This is proposed resource design rather than a currently shipped API.

An organization may connect its existing LLM gateway, direct provider APIs, or a self-operated inference endpoint. We should not force it to install an additional gateway. An optional gateway adapter can reuse its routing, keys, and accounting, while our product still owns per-user data authorization and tool permissions.

Treat these as separate concerns:

1. **Application login:** OIDC authenticates the person using the lakehouse UI.
2. **Model access:** the backend authenticates to the configured gateway/provider, using workload identity, OAuth, API keys, or another supported mechanism.
3. **Data/tool access:** the product evaluates the initiating user's current permissions for every catalog, query, file, log, and execution tool.

A backend service token can pay for inference without becoming the identity for data queries. Conversely, a valid model API token does not authorize access to any lakehouse table.

| Endpoint family | Authentication to support | Important distinction |
|---|---|---|
| Organization LLM gateway | API key/custom approved header; OAuth client credentials; private-key client assertion; supported token exchange/on-behalf-of flow; optional mTLS | Configure the gateway's actual grant, audience, scopes, and response protocol; a URL plus “JWT” is insufficient |
| OpenAI API | Server-held project/service-account API key or configured workload identity federation | Official current docs support short-lived API bearer tokens; federation maps the workload to a pre-existing project service account |
| Claude API | API key or configured workload identity federation | Current docs support bearer keys/access tokens; legacy `x-api-key` remains supported; native Messages semantics need an adapter |
| Azure OpenAI / approved Foundry endpoint | Microsoft Entra workload identity/managed identity or resource key where supported | Endpoint, deployment, API version, token audience, and assigned Azure role must match |
| Google Vertex AI / applicable Google model endpoint | Google client authentication/ADC with workload federation or service identity; supported API-key modes explicitly qualified | Google Cloud and Gemini Developer API configurations are distinct; do not reuse tokens blindly |
| Gemini Developer API | API key using the documented interface | Model/version capabilities and quota differ from Google Cloud deployments |
| Amazon Bedrock | AWS SDK and temporary role credentials/SigV4; documented short-lived Bedrock bearer-key option | Endpoint/API family, region, IAM permissions, and supported operations still matter |
| xAI | API bearer key; mTLS where configured and available | Inference and management keys have different scope; the inference service needs only inference access |
| Mistral, Cohere, Groq and further APIs | Provider adapter or a qualified compatible-endpoint profile | Each provider/model needs contract tests; model listing does not prove tool or structured-output behavior |
| Self-hosted inference | Private gateway authentication, workload identity, mTLS, or administrator-approved internal token | Model-serving runtime and model license are separate from gateway access; never expose an unauthenticated public endpoint |

Sources for the provider claims: [OpenAI authentication](https://developers.openai.com/api/reference/overview), [OpenAI federation](https://developers.openai.com/api/docs/guides/workload-identity-federation), [Claude authentication](https://platform.claude.com/docs/en/manage-claude/authentication), [Claude federation](https://platform.claude.com/docs/en/manage-claude/workload-identity-federation), [Azure keyless connections](https://learn.microsoft.com/en-us/azure/developer/ai/keyless-connections), [Google ADC](https://docs.cloud.google.com/docs/authentication/application-default-credentials), [Gemini API keys](https://ai.google.dev/gemini-api/docs/api-key), [Bedrock endpoints](https://docs.aws.amazon.com/bedrock/latest/userguide/endpoints.html), [Bedrock API keys](https://docs.aws.amazon.com/bedrock/latest/userguide/api-keys.html), [xAI inference authentication](https://docs.x.ai/developers/rest-api-reference/inference), [xAI mTLS](https://docs.x.ai/developers/advanced-api-usage/mtls), [Mistral platform](https://docs.mistral.ai/), [Cohere platform](https://docs.cohere.com/docs/the-cohere-platform), [Groq compatibility](https://console.groq.com/docs/openai).

OAuth is a protocol; JWT is a token representation. An access token may be opaque or a JWT. An OIDC ID token for the UI is not automatically an access token for an LLM gateway. A JWT used as a client assertion at the token endpoint is also distinct from the resulting access token. Validate the token type and issuer/audience/signature/lifetime according to the configured provider profile; perform token exchange only where an established trust relationship supports it. [OAuth JWT client assertions](https://datatracker.ietf.org/doc/rfc7523/), [JWT access-token profile](https://www.rfc-editor.org/rfc/rfc9068.html).

Use provider SDK credential implementations where appropriate. Acquire and renew short-lived credentials in the backend; isolate token caches by connection, identity/delegation, audience, and scope. Respect provider-specific refresh semantics. For example, OpenAI's documented federation exchange does not return a refresh token; renew by performing a new exchange. Anthropic documents token rotation and replay conditions, so a generic “reuse the same JWT forever” helper is insufficient. Never silently fall back from a failed workload identity to an old static key. Diagnose issuer, audience, expired token, failed exchange, insufficient scope, wrong deployment, and missing model permission without logging the credential.

Secret values must stay out of browsers, notebook source, Spark configs exposed to users, logs, prompts, and exported connection manifests. Store references to an approved secret store and inject credentials only into the authorized model-access process. Restrict gateway endpoint changes to administrators; custom URLs need network/SSRF controls, certificate verification, and an explicit path to approved private endpoints.

Personal ChatGPT/Codex, Claude, and Grok subscription integration remains a separate, provider-specific official-client feature. Do not reinterpret their login tokens as arbitrary pooled inference credentials. Current OpenAI documentation additionally describes Codex workload federation for managed workspaces, in beta and requiring enablement; that differs from ordinary API federation and does not establish universal personal-subscription access. [OpenAI workload identity guide](https://developers.openai.com/api/docs/guides/workload-identity-federation).

## Compatibility is more than an OpenAI-compatible URL

Maintain capability declarations and run contract checks for each endpoint/model combination: text and code generation, context limits, streaming event shape, tool-call schema and IDs, structured output, multimodal input, embeddings, cancellation, usage reporting, rate limits, and model-version availability. Configure manual model IDs when gateways intentionally disable model enumeration.

Use a normalized internal stream such as text delta, proposed tool call, validated tool result, usage, completed, failed, and cancelled. Preserve provider request IDs. Do not pretend every provider supports the same reasoning controls or pass unknown parameters through indiscriminately. Expose missing features honestly: a text-only model can explain code but should not be advertised as a qualified autonomous engineering agent.

“Stop” must cancel queued platform work, stop downstream tool execution, abort the active transport, and invoke a provider's cancellation API when supported. Closing a socket does not guarantee a remote model stopped or that billing ceased. Prevent automatic retries from duplicating tool side effects; keep tool operation IDs and completion state in our durable run record.

Model aliases such as `code-fast`, `code-deep`, and `data-analysis` let administrators select different qualified models per task. The UI need not expose dozens of provider knobs. Route by declared capability, policy, observed latency, and cost. Fallback must remain within approved endpoints, data-handling rules, and required capabilities; do not send confidential context to a public provider because a private gateway is temporarily unavailable.

## One context and tool service for all AI surfaces

Build a permission-aware context assembler. A request includes workspace, actor, selected surface, source revision/cell ID, active environment, catalog references, and context-sharing policy. It retrieves only what is needed: code, authorized schemas/descriptions, reviewed metrics, selected errors/log windows, and explicitly permitted sample data. No automatic full data-lake export or embedding of every row is necessary.

Keep retrieved data and metadata distinct from instructions. A table comment, data value, source file, stack trace, or external document may contain malicious instructions; it cannot grant tools, expand credentials, alter destinations, or override organization rules. Prompt instructions improve behavior but are not the enforcement boundary.

Expose narrow tools through our existing APIs: search permitted assets, describe schema, retrieve metric definition, validate/explain SQL, execute governed read query, inspect authorized run, propose document patch, apply permitted patch, and execute an authorized development run. Tools perform fresh authorization on every call and return bounded results. Default data conversation to the governed read path; separate write/deploy tools by capability and user permission.

Result caches, conversation storage, snippets, generated charts, and notebook outputs need access control. A shared conversation must not reveal rows or logs unavailable to its viewers. Include effective identity, policy version, and data snapshot/freshness in result-cache decisions, and recheck access before reopening saved results. Model-provider authentication cannot replace these controls.

## Mini Genie needs semantic setup and evaluation, not only a model key

The setup wizard should let a steward select allowed catalogs/tables, review table and column descriptions, define business metrics, joins and synonyms, specify time/calendar conventions, add reviewed example queries, and assemble benchmark questions. Keep these versioned with owners and review history. Reuse the same semantic assets in dashboards, SQL assistance, and chat.

Current Databricks Genie documentation supports benchmark questions with gold SQL answers and compares result sets for chat-mode evaluation; generated gold SQL still requires human review. It also provides a review loop for correcting responses and saving useful query examples. This is a sound benchmark for our proposed quality workflow. [Genie evaluation and monitoring](https://docs.databricks.com/aws/en/genie-agents/monitor).

Measure answer correctness against data, not fluency. Evaluate joins, duplicate handling, null semantics, fiscal calendars, ambiguous requests, unavailable datasets, schema changes, stale data, and denied access. Compare query results on pinned snapshots when repeatability matters. Add engineering evaluations where a fix must pass the intended test or rerun rather than merely make the error message disappear.

Monitor task success, accepted/reverted suggestions, incorrect edits, latency, cancellation behavior, per-workspace cost, model errors, and permission violations. Feedback is a review signal, not permission to change shared semantic definitions automatically. Model upgrades and gateway changes run the same benchmark pack before a production alias moves.

## Acceptance gates

1. A user configures an existing private OAuth gateway and a direct API-key provider through the same connection wizard; each gets a diagnostic test without exposing credentials.
2. Workload-token rotation, wrong audience, expired keys, signing-key rotation, and provider outages produce the correct behavior; failed authentication never broadens access or changes provider silently.
3. Fast typing and collaborative edits cannot cause an old AI completion/patch to corrupt new text. Accepted edits remain undoable and do not unexpectedly execute code.
4. SQL chat, inline SQL generation, notebook query tools, previews, exports, saved answers, and agent tools return only the initiating user's authorized data.
5. A prompt-injection payload in a table description, cell output, or error message cannot trigger secret retrieval, new endpoint use, privileged execution, or unauthorized data access.
6. Cancelling a suggestion or agent stops subsequent actions. A provider retry never runs the same table write or job deployment twice.
7. A failed cell can be explained, patched, and deliberately rerun with all context linked to the correct source revision and runtime. Stale kernel state is visible.
8. Loss of the AI service leaves normal notebook and SQL development usable. Disabled capabilities have clear availability states.
9. Model/gateway changes pass agreed semantic and coding benchmarks, including denied-access cases. Readiness depends on measured task success, not the provider's branding.

The first implementation should cover a polished Python/SQL notebook, SQL editor, one direct API adapter, one organizational OAuth gateway, and permission-aware context/tools. Qualify cloud-native auth and additional providers through the same adapter contract. Maintain the ambition of broad provider support while publishing an explicit tested matrix.
