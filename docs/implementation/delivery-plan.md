# DataSlate delivery and qualification plan

Updated: November 8, 2026. The selected first workload is **data engineering: ingestion → Spark → governed SQL → charts**, on AWS EKS and S3, for teams within one organization. The [complete capability inventory](coverage.md) remains in scope. This plan sequences working releases and their evidence; it does not declare planned capabilities implemented.

## Full-platform commitment and parallel workstreams

The product target includes the full breadth of value in a Databricks-style platform. The first workflow is an integration milestone, not a smaller product definition. Broad capabilities must have their own implementation workstreams and acceptance evidence; they must not disappear into an undefined future phase.

The component choices below inherit the existing architecture and remain candidates until combined qualification. Reusing an upstream service does not automatically implement its DataSlate product experience.

| Workstream | Complete product responsibility | Component direction |
|---|---|---|
| Workspace and development | Collaborative notebooks, Python/SQL and qualified Scala/R runtimes, widgets, files, package environments, debugging, source history, IDE/Git workflows and reusable projects | DataSlate React editor, CodeMirror, Jupyter/Yjs, Git and immutable environment builds |
| Lake and table management | Existing lake attachment, discovery, managed/external tables, schema evolution, transactions, time travel/change feeds, compaction/statistics/retention and safe interoperability | Delta, Iceberg/Polaris and separately qualified cloud connectors |
| Compute and SQL engines | Batch and interactive Spark, SQL sessions/warehouses, pools, autoscaling/admission, isolation, cancellation, profiling and native acceleration | Spark operators, Spark Connect, Kyuubi; separately qualified Trino and Comet profiles |
| Ingestion and transformation | File arrival, batch connectors, streaming, CDC, incremental/declarative pipelines, quality checks, quarantine, replay and schema drift | Spark, Kafka/Debezium, dlt, declarative pipeline and dbt integrations |
| Workflow automation | Task graphs, schedules/events, parameters, retries, backfills, alerts, versioned promotion and a coherent run timeline | Airflow adapter plus DataSlate job/control-plane resources |
| Catalog and governance | Catalogs/schemas/tables/volumes/functions, search, ownership/grants, tags/classification, row/column policy, lineage, audit and credential boundaries | UC OSS, qualified engine policy enforcement, lineage ingestion and native policy UI |
| Analytics and business semantics | SQL editor, Table/Visualize/Profile, dashboards, consistent metrics/joins/units, parameterized sharing, BI clients and governed exports | DataSlate Result Service, Arrow, table/chart UI and qualified SQL client adapters |
| Sylon and agent development | Ask-data, cell assistance, troubleshooting, gateway/model choice, tool orchestration, Agent Studio, memory, RAG, traces, quality/cost evaluations and safe deployment | Native Sylon product, LangGraph, MLflow, PostgreSQL/pgvector; optional context/memory adapters |
| Machine learning | Experiment tracking, model registry, training, GPU scheduling, feature engineering/store, batch/online inference, monitoring and model lifecycle | MLflow, Feast, KServe and qualified training/serving runtimes including vLLM |
| Applications and operational databases | Build/deploy/authenticate Python/Node data apps, connect governed resources and manage application Postgres; separately evaluate branching/autoscaling/scale-to-zero capabilities | Kubernetes app controller and OCI builds; CNPG for the Postgres baseline, advanced database backend qualification outstanding |
| Collaboration beyond a team | Cross-workspace collaboration, governed recipient sharing, federation, clean rooms and marketplace/distribution lifecycle | Native product resources with qualified sharing/federation protocols; dedicated clean-room security design |
| Enterprise administration | Organization/workspace hierarchy, SSO, group/service-principal lifecycle, scoped permissions, private networking, secrets/KMS, quotas, cost policy and audit | OIDC/SCIM interfaces, Kubernetes admission/networking and cloud identity/secrets adapters |
| Delivery and operations | Guided install, explicit managed/external ownership, health/debugging, backups/restores, upgrades, support matrix, signed releases and incident runbooks | Helm/operators, PostgreSQL, object artifacts, telemetry and DataSlate lifecycle services |
| Ecosystem and adoption | API/SDK/CLI consistency, declarative project bundles, infrastructure-as-code, migration assessment/import/export, BI/dbt compatibility and worked examples | OpenAPI clients, native bundle/resource controllers and compatibility tooling |

Every workstream must deliver: (1) real backend behavior, (2) an integrated UI/API/CLI contract, (3) security and resource ownership, (4) observable failure/recovery behavior, and (5) reproducible qualification evidence. A functioning upstream dashboard or an embedded iframe does not meet that definition.

Develop independent contracts, UI, adapters and test fixtures in parallel where resources allow. Integrate them through shared identity, durable operations, artifacts, semantic metadata and audit contracts. Promotion remains dependency-based: agents cannot be qualified against unqualified query/result permissions, and dashboards cannot be certified with invented data outputs.

Two parity boundaries need explicit additional work. Comet is an acceleration candidate and cannot be advertised as Photon-equivalent without measurements. CNPG provides a PostgreSQL operating baseline, not proof of Lakebase-style instant branching, storage/compute separation or scale-to-zero. Databricks documents those database capabilities separately in its [Lakebase architecture](https://docs.databricks.com/aws/en/oltp/projects/architecture). Track their outcomes as separate requirements and qualify a suitable backend rather than marking them complete because Postgres is installed.

Project promotion also needs more than Git links: immutable bundles should connect code, jobs, pipelines, dashboards, models and application resources across development/test/production. Current Databricks references include [Declarative Automation Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/) and [application deployment](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/deploy); DataSlate needs corresponding complete lifecycle behavior.

## Product outcome

The first customer hypothesis is an organization already operating EKS, S3 and Spark/open-table workloads, with a platform team supporting several internal data teams. DataSlate should give that team one maintained workspace and operating model, with measurable improvement in workload delivery, operational effort or total cost.

Kubernetes deployment and open source are delivery choices. The adoption test is whether users can reliably get trusted data into their existing analytical workflow, recover from failures, and understand costs without assembling the integrations themselves. A self-hosted installation can cost more than a managed service once idle capacity, maintenance and staffing are counted.

Use a few design partners to validate this hypothesis alongside engineering. Ask them to supply a representative pipeline, current operating effort, security requirements, performance baseline and the actual reason they would switch. Official vendor documentation establishes capabilities, not buyer intent or comparative economics.

## Current evidence

The [validation record](validation.md) separates published CI, native backend checks, database tests and browser behavior. Approved Spark applications, exact saved Python notebook batches and incremental file ingestion use durable controllers and immutable provenance. The implemented publication path captures verified Delta output, registers one approved catalog mapping and opens a governed SQL draft; native Spark → Delta → Hive Metastore → Trino checks verify the same lake output. Polaris metadata, lexical/hybrid search and Sylon conversations, cell proposals and private memory have their own recorded evidence.

Approved Airflow 3.4.0 workflows now expose deployed schedules/task graphs, sanitized scheduled/manual history and durable owner-scoped triggers through the shared UI/API/CLI. A real local Airflow scheduler, PostgreSQL-backed DataSlate controller and browser completed a reviewed immutable-bundle run without intercepted responses; its three EmptyOperator tasks establish orchestration behavior, not Spark or cloud task execution. The rendered-chart Kubernetes preflight is implemented and read-only. An optional pinned Airflow LocalExecutor profile renders a single scheduler, DAG processor and HTTPS API for trusted DAG authors with external database/storage/secret prerequisites. Interactive kernels, complete cluster installation, DAG/schedule editing, backfills, maintenance, operations and broader platform modules remain incomplete. None of these local checks establishes a working EKS/S3 production installation.

The core installation now has [real local Kubernetes evidence](../../deploy/core-installation-qualification.md): reviewed generated values, distinct TLS database identities, staged Helm migration/grants, four-user Keycloak PKCE, access-token-derived workspace permissions, ingress/network boundaries, and exact notebook readback after PostgreSQL/API restarts. The first post-restart IdP login required a retry. This advances the core-installation gate; it does not qualify EKS/S3, complete backend installation, previous-release upgrades, backup/restore or high availability. The [quickstart](../../deploy/quickstart.md) and [Pages guide](https://nimeshbuilds.github.io/dataslate/quickstart.html) expose the supported development and production setup steps.

Historical observations at publication revision `a18ddd2` (the execution implementation below addresses these; they are not the current code state):

- `api/internal/platform/http.go` reports execution disabled and returns a capability error for runs.
- `api/internal/platform/postgres.go` applies/checks only migration 1. Extend the migration runner before introducing execution tables; adding a second SQL file alone will not apply it.
- `api/migrations/001_initial.sql` deletes notebook history when its document is deleted. A submitted run needs an independent immutable source snapshot so later edits/deletion do not change its provenance.
- Application service accounts deliberately have no Kubernetes execution permissions. Execution needs a separate controller with narrow privileges and enforced admission rules.

## Why incumbent capabilities matter

These are documented capabilities and our engineering implications, not independent claims about purchase behavior or product superiority.

| Documented capability | Implication for DataSlate |
|---|---|
| Databricks integrates engineering, analytics, governance and AI, with managed runtimes and infrastructure | A unified UI needs shared lifecycle, identity, policy and diagnostics behind it; linking several dashboards is insufficient. [Platform overview](https://docs.databricks.com/aws/en/introduction/) |
| Databricks serverless allocates/manages compute for notebooks, jobs and pipelines | Deliver compute policies, sensible defaults, startup visibility, idle shutdown and recovery inside the customer's cluster. Measure how much platform work remains. [Serverless compute](https://docs.databricks.com/aws/en/compute/serverless/) |
| Snowflake separates warehouses and offers multi-cluster concurrency scaling | Batch Spark completion does not prove a responsive BI service. Qualify SQL admission, queues, workload isolation and concurrency separately. [Architecture](https://docs.snowflake.com/en/user-guide/intro-key-concepts), [multi-cluster warehouses](https://docs.snowflake.com/en/user-guide/warehouses-multicluster) |
| Databricks warns that direct storage access can bypass catalog controls | Storage credentials, arbitrary code and query enforcement must share one explicit threat model. [Governance guidance](https://docs.databricks.com/aws/en/data-governance/unity-catalog/best-practices) |
| Snowflake evaluates natural-language SQL using verified queries and regression results | Sylon needs business semantics, held-out questions and execution-based correctness checks, as well as provider connectivity. [Analyst evaluations](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst-evaluations) |
| Snowflake supplies workload-conversion tooling and an integration ecosystem | Migration assessment, familiar BI/dbt clients, side-by-side validation and rollback are product work. [Migration tools](https://docs.snowflake.com/en/migrations/README), [ecosystem](https://docs.snowflake.com/en/user-guide/ecosystem) |

Availability and limitations vary by edition, region, engine and release. Our support matrix must be equally explicit.

## Delivery sequence

Each milestone is a deployed, observable user journey. Architecture/security work runs alongside the execution implementation, before any production claim. Dates and service commitments follow measured capacity and staffing; these milestones are dependency order, not delivery estimates.

| Milestone | Product result | Required exit evidence |
|---|---|---|
| M0: reproducible environment and trust boundary | Install the foundation; choose one runtime/operator profile; define privileged engineering versus restricted analytical access | Real OIDC login, PostgreSQL TLS, running restricted containers, compatibility discovery, scoped S3 identities and a successful governance feasibility experiment |
| M1: durable batch execution | Submit a saved immutable Python revision, follow logs/progress, cancel and recover through UI/API/CLI | Real Spark execution on EKS/S3, independent result comparison, idempotent acceptance, controller restart and ambiguous-create recovery, denied identity/prefix substitution |
| M2: trustworthy ingestion and publication | Incrementally ingest a supported file source to a managed Delta table, transform it and schedule the immutable pipeline | Snappy-Parquet policy, schema drift/quarantine, cursor/checkpoint handling, updates/deletes where supported, replay/backfill and commit reconciliation; correct data after failures |
| M3: governed SQL and visualization | A restricted reader queries authorized rows/columns and sees accurate tables/charts/profile/export | Qualified catalog/engine/identity combination, blocked raw-storage bypass, exact types, full-data aggregates, current result permissions, cancellation, BI-client and concurrency checks |
| M4: operable pilot release | A team installs/upgrades/restores the supported profile and diagnoses a failed workflow | Clean-cluster restore, safe upgrade, managed/external lifecycle tests, usage/cost records, support bundle, signed artifacts and a documented capacity envelope |
| M5: Sylon on trusted APIs | Ask data questions, troubleshoot cells and review suggested edits using the configured provider | Authorized context/results, verified semantic answers, safe tool execution, provider failure handling, token/cost limits, prompt-injection tests and repeatable evaluations |
| M6: full-platform release waves | Integrate the parallel Agent Studio, memory, ML/serving/features, apps/databases, sharing, federation, developer-platform and cloud/table workstreams | Each capability passes its own integration and production gates in the full inventory; no inherited certification from a neighboring component |

M1 is an engineering batch capability. The selected first customer release includes M0–M4; a raw-file engineering job is not a substitute for the governed SQL gate. Focus the first ingestion source on deterministic object-store files; add Kafka/CDC through separate replay/ordering/checkpoint gates. Retain other connectors in the roadmap rather than claim a universal source adapter.

## Immediate implementation backlog

The ordered backlog below retains the original dependencies. The current implementation checkpoint is:

| Area | Implemented evidence | Remaining gate |
|---|---|---|
| Acceptance and persistence | Ordered migrations, atomic owner-scoped idempotency/capacity, independent saved-revision source snapshots and fenced leases | Production upgrade/restore and operational retention policy |
| Execution | Apache operator discovery, immutable bindings, source preparation, observation, logs, deadlines, revocation, cancellation and explicit artifact cleanup | EKS/S3 credentials/data correctness; full pinned lakehouse runtime tuple; broader engine profiles |
| User workflow | Run UI, saved Python batch review, immutable provenance, governed SQL results with exact types and preview charts, shared SDK/CLI | Interactive cells/sessions, Spark display artifacts and full-data aggregation |
| Compute boundary | Restricted namespace generator, admission, quotas, split API/controller/workload identities and local negative probes | Cloud IAM/CNI, malicious-code threat model and complete installation qualification |
| Data platform | Incremental file-to-Delta ingestion, verified publication, native same-table Spark/Delta/HMS/Trino readback, SQL draft handoff, Polaris metadata, hybrid search, Sylon and private memory | Joined Kubernetes/EKS/S3 publication and governance, maintenance and broader roadmap capabilities |
| Workflow orchestration | Approved Airflow immutable bundles, observed schedule/task graphs, durable manual triggers and current-grant UI/API/CLI; native scheduler/controller/browser proof; opt-in LocalExecutor deployment renderer | Cluster installation/upgrade qualification, DAG/schedule editing, parameters/backfills, production task isolation and cloud identities |
| Installation | Read-only inventory of the actual rendered chart, explicit context and ownership, CRD/operator/credential-reference checks with actionable reports | Live admission/network/IAM enforcement, canary, complete managed install, upgrade and restore |

The **ingest → publish the committed Delta table → governed Trino query → visualize** implementation is in place. Publication ties a verified runtime receipt to the pipeline revision, run and Delta version, records one registration intent and independently reads catalog metadata. The workspace opens an authorized SQL draft and requires explicit save/run; browser result/chart tests use labeled transport fixtures. Native Spark/Delta/HMS/Trino qualification separately queries that same produced table with filtering and masking. Polaris remains an Iceberg metadata adapter and does not publish Delta.

The next qualification gate joins these implemented pieces under real Kubernetes receipt capture and EKS/S3 identities, storage, KMS and catalog permissions, then repeats recovery and restricted-reader checks within approved cloud resource limits. Airflow orchestration must also be qualified with actual Spark/data tasks and their separate lake identities. Current native orchestration uses EmptyOperator tasks; it is not evidence of a complete scheduled cloud ingestion pipeline.

None of M0–M6 is promoted to a completed production milestone solely from these local checks. In particular, M1 still requires its EKS/S3 exit evidence.

1. Qualify the joined EKS/S3 journey: real v2 receipt capture, scoped driver/executor/catalog credentials, S3/KMS correctness, current engine policy and interrupted publication recovery. Keep privileged engineering separate from restricted analytics.
2. Add interactive notebook/session execution, Spark display artifacts, package environments and full-data analytical results while preserving saved-source, output and current-access boundaries.
3. Extend table lifecycle and ingestion: supported schema evolution, merge/change feeds, connectors/CDC, quality/quarantine, retention and maintenance with reader/checkpoint safeguards.
4. Complete workflow operations beyond approved DAG integration: qualify the rendered Airflow profile through cluster installation and upgrades, then add versioned DAG/schedule editing, safe parameters, backfills, alerts and promotion. Qualify Airflow task retries against durable DataSlate execution and cloud identities, with one scheduler and retry owner.
5. Turn prerequisite inventory into a complete installation and operations experience. Add isolated canaries, real OIDC, dependency health, upgrades, restore into a clean environment, retention and support diagnostics. Read-only preflight remains an inventory, not installation or enforcement proof.
6. Advance the parallel semantics/dashboards, Sylon tools/Agent Studio, shared memory, ML/serving/features, apps/databases and sharing workstreams against the same governed resources. Each needs a real user journey and its own failure/security evidence.
7. Publish supported runtime/dependency tuples, signed artifacts, capacity/cost measurements and migration/support guidance after their gates pass. Preserve the implemented idempotency, lease/fence, owner/current-policy and unknown-outcome contracts as every adapter expands.

## Security decision required before M3

Restricted query users must not obtain credentials or an alternate endpoint capable of reading unfiltered source files. SQL row filters cannot constrain arbitrary Python code holding raw S3 access. Evaluate the UC/Delta/Kyuubi/Ranger candidate combination as a complete system, including catalog names, authenticated user propagation, masks, filters and denied escape paths. Reject unsupported combinations if the boundary cannot be proved.

Define an explicitly privileged engineering profile for users authorized for raw data and a governed query profile for restricted readers. Where an interactive notebook invokes governed queries, its user-controlled kernel must remain outside the trusted enforcement service. Do not promise equivalent row/column protection for unrestricted engineering compute. Kubernetes namespace isolation by itself is not data-level enforcement.

The test matrix must cover SQL, notebook kernels, Spark Connect, file/SDK access, alternate catalogs, user packages/UDFs, result caches, logs, charts, exports, derived tables and later Sylon/tools/memory. Test cross-workspace requests, workload identity substitution, direct service access, expired/revoked rights and permission changes during a session. Previously downloaded data cannot be recalled; define what subsequent-access revocation guarantees.

## Golden acceptance journey

Use synthetic fixtures, two workspaces and separate administrator, engineer and restricted-reader identities. Expected results must be generated independently of the code under test.

1. Install the exact supported profile into a clean test environment; sign in through OIDC and verify production persistence and TLS.
2. Attach an existing S3 dataset without changing its ownership/format. Create a separate managed ingestion target with explicit storage scope.
3. Ingest deterministic data, including invalid rows, schema changes and replay; compare the resulting Delta data and publication receipt with expected values.
4. Save a notebook, submit its revision, edit the document afterward and prove the running code is unchanged. Observe logs, attempts and terminal state through UI and CLI.
5. Query as the restricted reader; compare table output, full-data charts, profile and exported data against independently expected authorized results.
6. Schedule a versioned run. Interrupt submission and publication at known points, including after commit but before acknowledgment; reconcile without duplicate logical publication.
7. Cancel before dispatch, during execution and around completion. Preserve truthful outcomes and clean up only owned resources.
8. Revoke access and test existing sessions, caches, result URLs, logs and exports. Attempt direct-file and cross-team access.
9. Restore control plane, catalog and artifacts into a clean environment, reconcile with the surviving lake and repeat the workflow.
10. Upgrade from the previous supported version and verify data, identity, schedules and results. Test explicit recovery for migrations; Helm rollback does not undo database changes.

## Proposed qualification gates

These are initial acceptance targets to refine with pilot workloads. None is a completed test result or customer SLA.

| Dimension | Required evidence before claiming support |
|---|---|
| Correctness | All cases in the versioned golden corpus pass, including nulls, decimals, time zones, deletes, concurrent writes and schema changes; no unexplained differences |
| Security | No unauthorized access in the defined adversarial matrix; the enabled release profile blocks unqualified escape paths |
| Revocation | Define and measure a bounded revocation window across sessions and artifacts; a proposed restricted-query target is 60 seconds after acknowledged revocation, requiring work beyond the current JWT-expiry behavior |
| Durability | At least 100 controlled failure-injection cases, with no lost acknowledged operations or duplicate logical publication in the specified scenarios; unknown outcomes are eventually reconciled |
| Recovery | Three clean-environment restore drills; document separate control-plane/catalog/lake/checkpoint failure domains. Provisional pilot objectives: RPO ≤5 minutes and RTO ≤60 minutes, subject to implemented backups and measured evidence |
| Capacity | Establish a baseline at 1/10/100 GB, 20 concurrent readers and five batch jobs on disclosed hardware. Measure cold/warm p50/p95/p99, queue time, throughput and failures before committing latency limits |
| Sustained operation | Seven-day representative soak with injected engine/controller/node failures, resource limits, credential rotation and no unreconciled operations at completion; this does not establish an availability SLA |
| Upgrade and ownership | Previous-release upgrade, explicit migration recovery, external-service detach/reconnect and safe uninstall; no silent adoption or modification of externally owned CRDs/data |
| Supply chain | Versioned bill of materials, immutable images/chart packages, provenance/signatures, SBOM, license notices, vulnerability review and documented remediation/exception process |
| Usability | Measure install completion after documented infrastructure prerequisites, time to first correct governed result, and time to diagnose a deliberately failed run with a new user |

Every release must retain evidence keyed by source revision, image digests, configuration/profile, dataset version, test environment, workload identities, run/attempt identifiers and observed outcomes. A failed mandatory gate blocks promotion. `foundation → integrated → qualified` is a per-capability, per-profile progression.

## Adoption, economics and support work

**Migration:** inventory notebooks, SQL, job definitions, packages, policies, data formats and downstream consumers. Import supported resources; produce an explicit unsupported-feature report for proprietary APIs/dialects. Run equivalent workloads side by side, compare exact data semantics and define cutover/recovery. Preserve a portable export path for DataSlate's own jobs, notebooks, policies and metadata. Do not assume open files make the entire application portable.

**SQL/BI:** qualify the chosen SQL service with actual BI/dbt clients, including authentication, catalogs, data types, cancellation, timeouts, connection reuse and concurrent dashboards. Evaluate additional serving engines only against measured requirements. Comet/native acceleration follows a correct vanilla Spark baseline and must preserve security and results; it is not Photon.

**Cost:** show usage by team, workload and attempt, including retries, idle capacity and shared-service allocation. Compare cost per correct, authorized, completed workload at equivalent service levels. Include compute, Kubernetes baseline, storage/requests, network, database/catalog, telemetry, backups, model API usage, operational labor, support and migration effort. Do not promise savings until measured with the customer's baseline.

**Sylon:** build a business semantic layer for metrics, joins, units, time zones and synonyms; track dataset/query provenance in answers. Keep evaluation questions separate from runtime examples. Evaluate ambiguity, wrong schema, denied data, injection, model/provider outage and tool retries, not only plausible SQL. Model connections use provider-supported API/gateway authentication; a consumer chat subscription is not a universal programmatic credential. Private Sylon memory now supplies scoped access, correction, expiration and deletion of derived state; shared memory and autonomous agent integration remain separate workstreams.

**Operations/support:** provide a health and dependency view, correlated run/query/audit timelines, redacted diagnostic bundles, backup status, upgrade preflights and runbooks. Name ownership for every dependency. Paid support/hosting requires explicit service boundaries, incident response and escalation, release maintenance and security response. Certifications and uptime commitments require their own evidence; documentation and successful tests alone do not grant them.

**Design-partner acceptance:** the first release succeeds when several independent teams complete the workflow, recover it after failure, operate it with documented effort and choose to retain it for a measurable benefit. Expand the qualified matrix when there is demand and capacity to maintain it. Keep every broader feature in the inventory without advertising untested combinations.

## Live qualification prerequisites

Implementation and local/CI integration can proceed independently. Cloud qualification requires an explicitly designated EKS test cluster/account and region, synthetic-data S3 location, registry, OIDC test identities/claims, IAM/KMS/network configuration, approved resource limits and spend cap, plus cleanup/retention ownership. No cloud provisioning, customer-data access or production mutation was performed by this planning work.
