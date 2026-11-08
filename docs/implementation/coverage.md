# Complete DataSlate capability coverage

DataSlate is a Kubernetes data and AI platform; Sylon is its assistant. This inventory preserves the full requested scope, including the broader capabilities. Sequencing reflects dependencies, not removal from scope. A module appearing in navigation or the deployment planner is not evidence that its backend integration exists.

Status vocabulary: **foundation** = resource models and initial code are being built; **planned** = implementation contract exists, backend work outstanding; **integrated** = real backend flow passes automated integration checks; **qualified** = published supported profile passes cloud, security, recovery and performance gates. The first repository implementation is a foundation, not a complete production platform.

The selected first customer workflow is **ingestion → Spark → governed SQL → charts**, on EKS/S3. The [delivery and qualification plan](delivery-plan.md) defines its implementation order, security boundary, acceptance journey and proposed release gates. Broader scope below remains intact.

## Product and authoring

| Capability | Foundation/component | Required acceptance evidence | Initial track |
|---|---|---|---|
| Workspaces and team resources | Go API + PostgreSQL | Cross-workspace authorization; membership removal; durable revisions | Foundation |
| Notebook and SQL source | React/Jupyter/CodeMirror | Save/reopen, conflicts, autosave, source/history, clean scheduled execution | Source editing/persistence and opt-in saved Python batch snapshots implemented; interactive execution/schedules remain separate |
| Collaborative editing | Jupyter/Yjs | Concurrent changes, reconnect, document permissions without leaking outputs | Planned |
| Interactive kernels | Jupyter + isolated pods | Attach/reconnect/stop, credential revocation, kernel failure recovery | Planned |
| Notebook language and utility compatibility | Qualified Python/SQL/Scala/R kernels and native utility APIs | Per-runtime execution, widgets/parameters, files, package isolation and explicit unsupported proprietary APIs | Planned |
| Table/Visualize/Profile/Execution | Precise SQL result service + native table/chart views | Correct full-data aggregates, exact decimals, bounded preview, current result ACL | Trino results and bounded-preview tables/charts/profiles implemented; complete dataset aggregation and Spark display output remain planned |
| Dashboards and semantics | Native product modules | Parameters, units, metric lineage, viewer rights, snapshot vs refresh semantics | Planned |
| Shared business semantic layer | Versioned metric/join/dimension definitions | Consistent metric meaning across SQL, dashboards and Sylon; grain/fan-out, time zone, unit, synonym and policy tests | Planned |
| Visual pipeline designer | Product graph editor + versioned executable source | Round-trip supported graph/code, validation, compare and promotion | Planned |
| Git/project bundles | Git + declarative manifests | Reproducible dev/prod promotion with immutable code/environment references | Planned |
| Infrastructure-as-code and developer integration | Declarative resource provider + IDE/SDK interfaces | Import/state reconciliation, drift, environment promotion, least-privileged automation and rollback contracts | Planned |
| API, SDK and CLI | OpenAPI + Go/Python clients | Same resources, errors, permissions and optimistic concurrency as UI | Foundation |
| Accessibility and responsiveness | Shared design system | Keyboard completion, focus order, screen-reader names, narrow viewport | Foundation |

## Lake and execution

| Capability | Foundation/component | Required acceptance evidence | Initial track |
|---|---|---|---|
| Spark application lifecycle | Apache/Kubeflow adapters | Create, observe, cancel, reconcile after controller restart; no duplicate commit | Apache batch adapter and durable lifecycle implemented; local operator checks recorded; EKS/S3 qualification pending; Kubeflow preview only |
| Spark SQL service | Kyuubi + Spark SQL + Ranger candidate | UC identifiers, user propagation, filtering/masking, denied escape paths | Planned; critical gate |
| Governed distributed SQL | Trino + DataSlate durable SQL controller | Exact user mapping, engine read-only policy, filtering/masking, durable result ownership and cancellation | Implemented; real distributed Trino TLS/JWT/local policy tests pass; cloud connectors/HA/capacity pending |
| Spark Connect | Authenticated gateway + isolated server | No raw endpoint, correct identity, compatible APIs, session reconnect | Planned |
| Native acceleration | Comet candidate | Differential correctness, denied access, fallback and measured performance | Planned |
| Packages and runtimes | BuildKit + OCI registry | Pinned JDK/Scala/Spark/Delta/UC artifacts, isolated builds, provenance and rollback | Pinned ingestion image and offline real Spark/Delta container suite implemented; runtime registry/build isolation/UC profiles planned |
| S3 lake attachment | S3A + scoped EKS identities | Read/write/refresh driver+executor+catalog credentials; private network/KMS | First cloud qualification |
| Azure/GCP lake attachment | ABFS/GCS connectors + federation | Equivalent identity, commit, private endpoint and maintenance tests | Planned profiles |
| Dynamic dataset discovery | Catalog-first and bounded file scanner | JSON/Parquet schemas, Delta protocols, Iceberg snapshots/deletes; no unsafe guessing | Planned |
| Unity Catalog | OSS UC integration | Catalog/schema/table/volume functions supported explicitly, grants and ownership | Planned; critical gate |
| Delta tables | Qualified Delta protocol | Snappy data policy, merge, schema change, time travel, CDF, concurrent writers | Incremental append/Snappy/checkpoint runtime tested on host JVM and production OCI; cloud, merge, CDF and broader table lifecycle pending |
| Verified Delta SQL publication | Durable publication controller + Trino Delta + HMS/Glue | Owned runtime receipt, one registration intent, independent metadata readback, current reader grants and explicit SQL handoff | Implemented with API/store/browser checks and native local Spark/Delta/HMS/Trino evidence; EKS/S3, full Kubernetes receipt capture, Glue and joined production deployment pending |
| Iceberg | Authoritative REST catalog; Polaris profile | Qualified format versions, deletes, schema evolution, ownership and maintenance | Native v2 schema/discovery metadata and exact grant API/UI integrated with real local Polaris TLS/OAuth; table data/query/cloud/maintenance pending |
| Table interoperability | Qualified UniForm read profile | Publication/cache freshness, no independent writes to Delta-owned metadata | Planned |
| Federation | Trino or qualified source-specific adapter | Pushdown/dialect, credentials, read permissions, source outages and limits | Planned |
| Batch ingestion | Strict Spark file ingestion; dlt connector track | Cursor state, staging/checks/commit, schema drift, restart and backfill | Versioned JSON/Parquet→Delta definitions, scoped prefixes, exclusive writers, checkpoint recovery and strict schema checks implemented; 17 local runtime scenarios pass; cloud/connectors pending |
| Streaming/CDC | Kafka, Structured Streaming, Debezium | Offset continuity, deletes/order, exclusive checkpoint, replay and recovery | Planned |
| Push ingestion endpoint | Product ingest API + durable buffering | Admission, dedup policy, durable acknowledgement, publication and backpressure | Planned |
| Declarative ETL | Spark Declarative Pipelines | Dependency graph, incremental state, expectations and deterministic update | Planned |
| dbt | Qualified dbt adapter | Target dialect/catalog compatibility and grants, source/model lineage | Planned |
| Workflow orchestration | Airflow 3.4.0 + durable DataSlate workflow controller | DAG versions, schedules, backfill, one retry owner, coherent run timeline | Approved immutable-bundle manual triggers, deployed schedules/graphs, sanitized upstream history and owner task observations implemented across UI/API/CLI; real local Airflow/PostgreSQL/browser flow passes. An optional pinned LocalExecutor deployment profile renders for trusted DAG authors. Cluster installation/upgrades, DAG/schedule editing, backfills, alerts and production task identities remain open |
| Maintenance | Format-native controller | Compaction/stats/retention with external-reader and stream-lag safeguards | Planned |
| Quality and observability | Rules + lineage + telemetry | Blocking checks before publish, quarantine, freshness and actionable errors | Planned |

## Sylon, agents and machine learning

| Capability | Foundation/component | Required acceptance evidence | Initial track |
|---|---|---|---|
| Sylon ask-data | Semantics + governed queries | Correct cited results, ambiguous questions, policy-aware answers and charts | Authorized notebook/catalog/search context and cited metadata answers implemented; governed query tools, semantics and answer evaluation pending |
| Sylon Code | Context assembler + edit tools | Exact cell/error/runtime context, conflict-aware diff, separate apply/run actions | Selected saved-cell assistance and atomic revision-checked Python/SQL proposals implemented; explicit apply never runs code; execution-error/output context and interactive tools pending |
| Provider/gateway connections | Per-provider adapters | Streaming, tool calls, OAuth/API keys/WIF where supported, expiry and quotas | Embeddings and Responses/Chat Completions HTTPS adapters with bearer/OAuth credentials, explicit data transfer, bounded structured replies and no automatic paid retry implemented; streaming, subscription login and individual native provider profiles pending |
| Agent Studio | Native UI + LangGraph | Templates, tools, instructions, tests, publish, rollback, scheduled/service modes | Planned |
| Durable agent runtime | LangGraph + PostgreSQL | Restart/cancel/resume, bounded loops, tool receipts and no duplicate actions | Planned |
| Agent memory | PostgreSQL/pgvector + provenance | Personal/project/team scopes, correction, expiration, revocation and deletion | Private job-derived Sylon memory with explicit save/correction/expiry/deletion, exact reviewed lexical recall, transitive source grants and durable local purge implemented; team/project sharing, semantic memory indexing and autonomous agent integration pending |
| Context efficiency | Optional Headroom | Cost per successful task including overhead/retries; protected exact content | Planned |
| Advanced memory | Hindsight adapter | Tenant mapping, authorized recall, consolidation/deletion propagation | Planned |
| MCP/tools | Typed tool broker | Audience checks, no token passthrough, scoped tools and atomic action reviews | Planned |
| AI evaluations/playground | MLflow + product benchmark sets | Agent/model/prompt/tool versions tied to traces, regressions block promotion | Planned |
| AI search/RAG | pgvector/full-text; dedicated index adapter when needed | Sync, hybrid relevance, filters, authorized retrieval and deletions | Durable notebook/Polaris metadata index, exact current-policy filtering, lexical/hybrid retrieval, source invalidation and bounded embedding transfer implemented; production relevance/scale/provider qualification pending |
| SQL AI functions | Governed model-invocation functions | Row-level egress control, deterministic receipts, cost limits and failure semantics | Planned |
| ML experiments/registry | MLflow | Isolated trace/artifact access, model lineage and reproducible runs | Planned |
| Distributed training | Qualified Spark/GPU training profiles | Gang/admission policy, checkpoints, failed worker recovery, resource limits | Planned |
| Feature store | Feast + qualified offline/online stores | Point-in-time correctness, materialization, online freshness and access | Planned |
| Model/LLM serving | KServe + compatible runtimes/vLLM | Auth, canary, resource capacity, model licenses, per-profile scaling | Planned |
| Drift/monitoring | Model/data checks + telemetry | Baselines, schedules, alerts, linked versions and reproducible investigations | Planned |

## Administration, applications and collaboration

| Capability | Foundation/component | Required acceptance evidence | Initial track |
|---|---|---|---|
| SSO and team RBAC | OIDC + product authorization | Issuer/audience/signature/expiry, roles, revocation and no fallback identity | Foundation |
| Identity and group lifecycle | OIDC/SCIM + service-principal administration | Join/change/leave propagation, group mapping, automation identities, bounded revocation and ownership transfer | Planned |
| Row/column policies | Catalog + engine enforcement | Equivalent enforcement across governed SQL/notebook/AI/result/export paths; deny restricted users raw-storage bypass; separately document privileged engineering access | Local Trino read-only/row-filter/mask enforcement and policy-version result revocation tested; cross-engine/cloud enforcement remains a critical gate |
| Classification and policy UI | Product metadata and policy adapter | Versioned changes, effective-policy explanation and enforcement acknowledgement | Planned |
| Installation/adoption | Helm + read-only preflight/dependency planner | Exact version/API/owner detection, canary, no silent adoption/CRD upgrades | Rendered-chart preflight implemented with explicit context, ownership/CRD/operator checks, credential-key references and bounded diagnostics; 38 fixture/native Helm checks pass. Live enforcement, canary, complete installation and upgrades remain unqualified |
| Self-contained distribution | Managed qualified dependencies | Complete reproducible installation including optional self-hosted AI profile | Individual pinned Trino, Polaris and Airflow profiles exist; complete reproducible installation and recovery remain unqualified |
| Existing-service integration | Adapter contracts | Permission/capability verification and explicit unsupported-mode reasons | Explicit Spark/Trino/Polaris/provider/Airflow bindings and read-only prerequisite inventory implemented; existing services retain ownership. Complete supported-version and production integration qualification remain open |
| Secrets/identity federation | Cloud stores/ESO/OpenBao + OIDC/WIF | Rotation, scoped mounts, secret references, no credentials in UI/logs | Configuration foundation |
| Isolation and quotas | Kubernetes RBAC/admission/network policies | Driver cannot choose another SA/secret; quota and workload escape tests | Explicit compute profile with local admission/RBAC/network probes; cloud and malicious-code isolation qualification pending |
| Audit and diagnostics | Product events + OTel | Actor/execution identity correlation, safe logs, access control, support bundle | Planned |
| Backup/restore/upgrade | PostgreSQL + object artifacts + migration controller | Restore to clean cluster, compatible downgrade policy and deletion replay | Planned; migration foundation |
| Operational PostgreSQL | CNPG/application databases | Resource binding, migrations, credentials and qualified HA/restore | Planned |
| Advanced operational database lifecycle | Separately qualified storage/compute backend | Isolated writable branches, point-in-time recovery, snapshots, autoscaling/read replicas and pause/resume semantics; CNPG baseline alone does not establish parity | Planned; backend qualification required |
| Apps | Isolated Python/Node deployments | Build/deploy/auth/egress/resources/logs/rollback under one lifecycle | Planned |
| Cross-org sharing | Delta Sharing profile | Recipient identity, supported tables, revocation, audit and retention | Planned |
| Clean rooms | Separate governed collaboration protocol | Threat model, constrained computations, output controls and audited release | Planned |
| Marketplace | Product catalog/distribution services | Publisher ownership, package verification, entitlement and updates | Planned |

## Adoption, economics and product operations

These capabilities turn backend integration into a product teams can adopt and operate. Their production acceptance remains open; a successful foundation CI run does not establish it.

| Capability | Foundation/component | Required acceptance evidence | Initial track |
|---|---|---|---|
| Workload assessment and migration | Importers + compatibility analyzer + comparison runner | Supported notebook/SQL/job import, explicit unsupported APIs, data reconciliation, parallel run, cutover/recovery and portable export | Planned |
| BI/dbt client compatibility | Qualified SQL protocol and client adapters | Real-client authentication, catalogs/types, cancellation/timeouts and concurrent workloads against the selected engine | Planned |
| SQL workload management | Engine admission + product compute policies | Queue/isolation behavior, competing batch/interactive workloads, cold starts, bounded capacity and idle shutdown | Planned |
| Cost attribution and budgets | Usage ledger + cloud billing/resource measurements | Team/workload/attempt allocation including idle/shared costs; budget actions, retries and total-cost baseline | Planned |
| Integrated runtime releases | Release bill of materials + OCI/chart registry | Signed immutable artifacts, dependency matrix, SBOM/licenses, vulnerability handling, supported upgrade and deprecation policy | Planned |
| Qualification evidence | Versioned fixture/correctness/failure benchmark suites | Per-release/profile artifacts for correctness, security, recovery, capacity and known limitations; failing gates block promotion | Local database, native Spark/Delta/Trino/Polaris/Airflow, browser and configuration evidence recorded by boundary; full supported-profile cloud, recovery and capacity qualification remains open |
| Supportability | Health/run timelines + safe diagnostic bundle + runbooks | An operator can identify and recover a deliberate failure without leaked credentials/customer data; explicit escalation and ownership | Planned |
| Guided onboarding and usability | Installation wizard/preflight + worked examples | New-user completion, time to first governed result, actionable failed prerequisites and second-team onboarding | Read-only CLI preflight and integrated workflow views implemented; installation wizard, complete managed deployment and measured onboarding acceptance remain planned |

## Sequenced delivery, full scope retained

1. Control-plane/auth/persistence and actual source-authoring foundation; install/package it, prove contracts and failure states.
2. EKS/S3 execution and governed UC/Delta profile; live notebook/SQL/results, ingestion, scheduling and maintenance. Reject unsupported combinations.
3. Complete Sylon/Agent Studio/memory/evaluation lifecycle on the same governed APIs; implement broader ML/search/serving/features tracks in parallel with their prerequisites.
4. Add apps/operational databases/federation/sharing and the remaining advanced collaboration capabilities; qualify alternative cloud/table/runtime profiles.
5. Production qualification across the complete supported scope: upgrades, restore, auth revocation, workload isolation, concurrency and measured capacity. Publish a support matrix based on evidence.

Each adapter must implement discover → authorize → verify → configure → execute → observe → cancel/reconcile → upgrade/recover where applicable. Read-only adapters need an explicit smaller contract. A logo, resource entry or rendered manifest satisfies none of the unimplemented execution stages.
