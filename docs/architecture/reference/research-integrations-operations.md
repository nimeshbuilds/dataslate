# Kubernetes lakehouse product: integrations and operations research

Research date: 24 September 2026. This is a documentation and repository assessment, not a deployed-system benchmark. Recommendations below are architecture proposals. `stable`, `latest`, and repository default branches are moving targets; observed versions are evidence about current interfaces, not a certified release bill of materials.

## Decision

Build one product control plane over a deliberately small set of mature execution and storage components. For the first production target—multiple teams inside one organization—use Airflow for scheduling trusted, product-generated workflows, dlt OSS workers for bounded batch ingestion, optional Debezium/Kafka for CDC, and MLflow for experiment/model/AI telemetry metadata. Start with existing production PostgreSQL and S3-compatible storage; offer explicitly managed Kubernetes profiles for dependencies when the operator wants them.

The value is in consistent identity, authoring, run semantics, error recovery, credentials, data ownership, and upgrades. A collection of upstream UIs behind single sign-on would leave the hardest integration work to the customer.

## Recommended component boundary

| Product capability | First supported backend | Kubernetes implementation | Product responsibility |
|---|---|---|---|
| Schedules, task dependencies, retries, backfills | Airflow 3 through supported public interfaces | Existing Airflow connection or managed Helm release | Compile versioned workflow definitions; safe parameters; normalize run presentation; preserve underlying IDs |
| Python/API/database batch ingestion | dlt OSS library in a certified runtime image | Short-lived Job with workspace identity | Connection wizard, discovery, schema contracts, state coordination, promotion to catalog tables, metrics |
| Log-based database replication | Debezium on Kafka Connect; Kafka when justified | Existing Kafka/Connect or opt-in Strimzi deployment | Prerequisite checks, source offsets, replay, deletes, schema policy, lakehouse commit correctness |
| Experiment tracking, model registry, prompts, traces | MLflow | Private Deployment, SQL backend, object artifacts | Unified identity, workspace mapping, UI views, SDK configuration, access checks |
| Durable operational metadata | PostgreSQL | Existing service or optional CloudNativePG Cluster | Database separation, migrations, backups, tested restore, capacity |
| Lake files/artifacts/logs | Certified object-store endpoint | External S3 service; optional Rook/Ceph profile | Credential scoping, retention, supported API capabilities, recovery expectations |
| Telemetry | Existing observability integration first | Export metrics/traces/logs using standard interfaces | Cross-component correlation and actionable platform health |

Do not require Kafka, Airbyte, an ML-serving fleet, or a new storage cluster for the simplest Spark batch pipeline. These are capabilities enabled when the workload requires them.

## Airflow: use the scheduler, preserve its semantics

Airflow 3 provides a supported public interface: the REST API, Python client, and `airflow.sdk` Task SDK. The current stable API uses `/api/v2`; endpoint permissions are enforced by the configured auth manager. Integrations should not read or mutate Airflow's internal metadata schema. [Public interface](https://airflow.apache.org/docs/apache-airflow/3.0.3/public-airflow-interface.html), [API permission reference](https://airflow.apache.org/docs/apache-airflow/stable/security/api_permissions_ref.html).

Airflow currently documents DAG bundle versioning and backfill behavior. A workflow edit must not silently change the code a previously launched run is expected to execute. [DAG bundles](https://airflow.apache.org/docs/apache-airflow/3.3.2/administration-and-deployment/dag-bundles.html), [DAG-run documentation source](https://github.com/apache/airflow/blob/main/airflow-core/docs/core-concepts/dag-run.rst).

Recommended integration contract:

1. Our versioned `WorkflowSpec` is the desired definition. A compiler produces a deterministic, trusted DAG and immutable task descriptors. Persist the compilation hash and runtime image digest.
2. Airflow is authoritative for schedule/data-interval and DAG/task scheduling state. Our platform maintains an indexed projection plus IDs and timestamps; it does not run a second scheduler for the same workflow.
3. The execution controller is authoritative for the actual submitted SparkApplication/Job/session attempt and observed backend status. Airflow tasks call the platform execution API with scoped machine credentials, rather than each independently interpreting tenant policy or constructing arbitrary Kubernetes manifests.
4. Identify a submission by a stable key such as `(workflow-version, logical-interval, task-id, attempt)`. Retries reuse or supersede an execution deterministically; a network timeout must not produce two untracked applications.
5. Preserve separate concepts for retry attempt, repair failed tasks, rerun complete workflow, and backfill a time range. The UI must show which code version, interval, input snapshots, identity, and runtime each uses.
6. A requested cancellation is not a completed cancellation. Reconcile until underlying workloads are terminated or report an explicit failure to cancel.
7. Publish task outputs as table snapshot IDs or object references, not large data in orchestration metadata. Trigger downstream work after the output commit succeeds.

The Airflow Kubernetes provider has a `SparkKubernetesOperator` that creates SparkApplication objects, can retrieve logs, and supports restart reattachment. Its existence validates the integration pattern; it does not remove the need to certify the exact Spark operator API and reconcile product-level run identity. The current docs still reference a historical GoogleCloudPlatform operator schema. [Provider reference](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/_api/airflow/providers/cncf/kubernetes/operators/spark_kubernetes/index.html).

### Multi-team warning that changes the architecture

Airflow 3.3.2's security documentation explicitly says its experimental `multi_team` feature does not provide complete task-execution isolation. DAG parsing and trigger code can reach privileged state; users able to author arbitrary DAG Python remain highly trusted. UI/API role restrictions do not turn one Airflow installation into a hostile-tenant sandbox. [Airflow security model](https://airflow.apache.org/docs/apache-airflow/stable/security/security_model.html).

For the first shared deployment, permit only trusted compiler-generated DAGs. Users' Python, SQL, notebooks, and connector code execute in tenant-scoped workload pods; they are not imported by the shared DAG processor or triggerer. Shared Airflow credentials can submit only through the platform's narrow execution API; individual DAG parameters cannot choose another workspace, arbitrary service account, pod template, or secret. Native arbitrary Airflow DAG support belongs in a trusted-administrator mode or a separate Airflow deployment per trust boundary.

Airflow has one configured auth manager at a time. Connecting an existing Airflow must therefore avoid silently replacing its authentication system. Validate API access, supported versions, auth manager behavior, DAG distribution method, and ownership before enabling operations. [Auth manager documentation](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/auth-manager/index.html).

### UI integration

Render our own DAG graph, task timeline, logs, schedules, parameters, retries, and backfill controls through supported APIs. Attach the product run ID, Airflow DAG/run/task IDs, Kubernetes UID, Spark application ID, and output snapshot IDs to one timeline. Offer the upstream Airflow UI as an administrator troubleshooting link; it is not the normal user workflow.

Imported external DAGs need an explicit distinction: observed/external versus product-managed. Do not claim arbitrary Python DAGs can be round-tripped safely through a visual editor. A read-only imported graph plus authorized trigger/cancel actions is a sensible first external integration.

### Dagster alternative

Dagster is a credible asset-centric alternative and has an OSS deployment architecture, Kubernetes integration, and a GraphQL-speaking webserver. Its separate code locations can be useful for runtime isolation. [Kubernetes integration](https://dagster.io/integrations/dagster-kubernetes), [Webserver and GraphQL](https://docs.dagster.io/guides/operate/webserver), [OSS deployment source](https://github.com/dagster-io/dagster/blob/master/docs/docs/deployment/oss/deployment-options/docker.md).

For this product, pick one scheduling backend for the first supported distribution. Airflow fits the requested familiarity and existing-cluster integration. Define an orchestration adapter boundary, but do not fund complete Airflow and Dagster parity in v1. Dagster also needs a deliberate product identity/authorization design; assume neither its UI nor a reverse proxy establishes execution tenancy automatically.

## Ingestion: connector count is not ingestion reliability

### Default batch path

dlt OSS is Apache-2.0 licensed and includes schema inference/evolution, incremental loading, write dispositions, state, and CLI inspection. dltHub is a separate commercial product. [OSS versus dltHub](https://dlthub.com/docs/hub/getting-started/oss-and-dlthub).

The OSS filesystem destination supports object stores, Parquet and other file formats, and Delta/Iceberg tables, with destination-specific merge behavior. However, the current dedicated dltHub Delta and Iceberg destination pages describe commercial features. Do not copy their entire feature set into an OSS support promise. [OSS filesystem destination](https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem), [commercial Delta destination](https://dlthub.com/docs/hub/ingestion/delta), [commercial Iceberg destination](https://dlthub.com/docs/hub/ingestion/iceberg).

Recommended first path: dlt extracts into immutable, workspace-scoped staging/bronze files; a controlled Spark commit step writes the authoritative table and registers it with the chosen catalog. A run manifest links extraction state, source window, staged files, schema, and final snapshot. This extra boundary makes catalog registration, concurrency, table-format feature support, and replay observable. Direct Delta/Iceberg writers can be certified subsequently rather than assumed equivalent across libraries.

For each connector, define and display: supported authentication, discovery, source selection, cursor/primary key requirements, full refresh, incremental behavior, deletion handling, types, rate limits, retries, schema evolution, CDC availability, and lifecycle status. Initially certify a small useful set: PostgreSQL batch, S3/Parquet/CSV/JSON, a generic REST pattern with explicit pagination, and file upload. Additional connectors should meet the same test contract.

Cursor-based loading tracks a source cursor and overlap deduplication; it is not automatically a database change log. dlt's own docs distinguish incremental-range deduplication from destination merge and support stateless bounded backfill. [Cursor incremental loading](https://dlthub.com/docs/general-usage/incremental/cursor). Our connector design must explicitly handle late updates, deleted rows, null cursors, clock/time-zone issues, source snapshot consistency, and concurrent runs. Only one active mutable cursor owner should advance a connection/resource state; partitioned backfills use separate bounded state.

### Airbyte should be optional, with a licensing decision

Airbyte is Kubernetes-oriented and documents Helm deployment. It can be useful when users already have its connectors and operations. [Deployment documentation source](https://github.com/airbytehq/airbyte/blob/master/docs/platform/deploying-airbyte/deploying-airbyte.md).

It is not an unrestricted OSS default for our product. Airbyte states that its platform/connectors use ELv2, and its 2025 announcement describes the move of connectors from MIT to ELv2. The license FAQ distinguishes internal usage from exposing functionality as a managed offering. Integration into a redistributed or hosted competing product needs a concrete license assessment or commercial agreement, based on the actual distribution model. [2025 licensing announcement](https://airbyte.com/blog/move-to-elv2), [License FAQ](https://github.com/airbytehq/airbyte/blob/master/docs/community/licenses/license-faq.md).

Connecting an existing Airbyte is a useful optional adapter. Do not make every tenant run an entire Airbyte stack: that duplicates platform services, databases, connector workers, upgrade work, and capacity. Do not estimate a per-tenant footprint from marketing or anecdotal memory; measure pinned images and representative concurrent syncs. Our ingestion UI should reflect Airbyte connection/job state through its supported APIs when that adapter is selected.

### CDC path

Use Debezium plus Kafka Connect for supported log-based sources, and existing Kafka by preference; Strimzi is the Kubernetes counterpart when an operator explicitly installs the streaming profile. Strimzi manages Kafka and Kafka Connect resources and publishes compatibility information. [Strimzi deployment guide](https://strimzi.io/docs/operators/0.50.1/full/deploying), [project and license](https://github.com/strimzi/strimzi-kafka-operator).

Debezium PostgreSQL captures insert/update/delete events and can emit duplicates during recovery. It uses source offsets and replication slots; an unavailable WAL position can prevent restart. Primary-key changes require handling deletion of the old key and creation of the new one. Therefore do not advertise an unconditional end-to-end exactly-once property. [Debezium PostgreSQL connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html).

The lakehouse sink needs a tested correctness protocol:

- Retain source identifier, partition/key, operation, source position, transaction/order information where available, ingestion time, and schema version in bronze events.
- Merge by primary key and source ordering; an older replayed update must not resurrect a row after a newer delete.
- Persist a sink commit marker plus consumed offsets/checkpoint in a recoverable scheme. Idempotency is tested by crashes on either side of the commit/ack boundary.
- Treat tombstones separately from domain deletes; make source table/PK/replica-identity prerequisites visible before enabling replication.
- Additive compatible schema changes may be automatic; breaking changes pause/quarantine and require an explicit migration policy.
- Monitor initial snapshot, replication lag, retained WAL, Kafka retention, consumer lag, schema errors, rejected records, and destination commit progress.
- Support resnapshot as a first-class recovery process. It must not silently truncate an already governed table or advance offsets beyond unpublished output.

These are product requirements/inferences, not claims that installing Strimzi implements them. Every source/sink/version pair needs failure-injection tests.

## MLflow integration

Use MLflow's documented REST/SDK surfaces for experiment runs, parameters/metrics, artifacts, models, and AI traces. A production server can proxy artifact access and use SQL metadata storage. [Tracking server architecture](https://mlflow.org/docs/latest/self-hosting/architecture/tracking-server/).

Current MLflow docs describe optional workspaces with scoped resources and artifact prefixes; they explicitly warn these are logical separation rather than a hard isolation boundary. They require SQL storage. Current authentication docs identify RBAC APIs introduced in MLflow 3.13.0. These newer capabilities should be validated against the pinned release rather than repeating outdated claims that MLflow has no workspace/RBAC features. [Workspaces](https://mlflow.org/docs/latest/self-hosting/workspaces), [RBAC](https://mlflow.org/docs/latest/self-hosting/security/role-based-access-control/).

Our product should set workspace identity server-side, not trust an arbitrary browser-provided workspace header. Verify direct SDK/API access with service identities, artifacts, traces, registry aliases, permissions, and search results. For stronger trust boundaries offer separate MLflow deployments/storage identities. Model serving, traffic management, feature serving, vector search, GPU scheduling, and endpoint autoscaling are separate integrations; a registry alone is not a serving platform.

## Production dependencies and Kubernetes lifecycle

### PostgreSQL

Prefer an existing managed or organization-operated PostgreSQL endpoint for the first production install; alternatively support a dedicated CloudNativePG installation. Use distinct databases and roles for product metadata, catalog, Airflow, MLflow, and identity components. Sharing a PostgreSQL cluster may reduce cost, but should not mean sharing superuser credentials, uncoordinated migrations, or one schema.

CloudNativePG is Apache-2.0 licensed and documents recovery using the Barman Cloud plugin or supported volume snapshots. Its former native Barman integration is deprecated in favor of plugins. [Project](https://github.com/cloudnative-pg/cloudnative-pg), [Backup source](https://github.com/cloudnative-pg/cloudnative-pg/blob/main/docs/src/backup.md), [Recovery source](https://github.com/cloudnative-pg/cloudnative-pg/blob/main/docs/src/recovery.md).

Do not deploy Airflow's quickstart PostgreSQL as the platform's production database merely because it is a Helm dependency; the Airflow production guide itself recommends an external production database. [Airflow production guide](https://airflow.apache.org/docs/helm-chart/stable/production-guide.html).

### Object storage

Start with a customer-provided, certified S3 endpoint. Test the precise API behavior needed by table formats, runtime libraries, multipart upload, conditional writes, credential issuance, TLS, bucket policies, and object listing; “S3-compatible” is not sufficient evidence of table-commit compatibility.

For an all-in-cluster deployment, Rook/Ceph is a mature candidate and provides S3 object storage alongside block/file storage. It has meaningful host-device, capacity, and privilege prerequisites. Treat its deployment as a separate storage profile with explicit device selection and failure-domain planning; never silently consume discovered disks. [Rook quickstart](https://www.rook.io/docs/rook/latest-release/Getting-Started/quickstart/), [Rook prerequisites](https://www.rook.io/docs/rook/latest/Getting-Started/Prerequisites/prerequisites/).

Do not select the archived MinIO community repository as a new maintained default: GitHub currently marks it archived on 25 April 2026, and its README says it is no longer maintained and describes source-only AGPLv3 distribution. Existing compatible customer endpoints can still be evaluated independently. [MinIO repository](https://github.com/minio/minio).

### Installation contracts

Every optional dependency needs `external` or `managed` modes, separate from whether its capability is enabled. `external` names the endpoint, resource scope, version, credentials, and health checks and never transfers ownership. `managed` creates only resources explicitly assigned to this product's lifecycle. A preflight can recommend missing components, but installation should render a plan and avoid silently adopting or replacing existing operators.

Use separate release layers:

1. Cluster prerequisites and optional cluster-scoped operators/CRDs, owned by the cluster administrator.
2. Namespaced product control plane and managed dependency instances.
3. Workspace resources, policies, runtime definitions, and immutable artifacts.
4. Ephemeral execution workloads controlled by the execution reconciler.

Helm installs CRDs specially but does not automatically upgrade or delete them; a normal application `helm upgrade` is not a complete operator lifecycle strategy. [Helm CRD guide](https://helm.sh/docs/chart_best_practices/custom_resource_definitions/).

Preflight must discover API group/version/schema, operator ownership/watch scope, admission webhooks, RBAC permissions, service accounts, default StorageClass, network-policy enforcement, ingress/TLS, object storage, external databases, DNS, resource capacity, and version compatibility. The presence of a CRD is insufficient proof that the right controller is installed and healthy.

For GitOps, support rendered manifests and documented ordering/readiness instead of relying on imperative Helm hooks. Airflow's own chart requires different migration-job settings for Argo CD/Flux and similar tools. [Airflow Helm chart](https://airflow.apache.org/docs/helm-chart/stable/index.html). Choose one authority for each object's desired state: GitOps owns declared configuration, operators own runtime children/status, and the UI submits changes through the chosen configuration workflow. Avoid fighting reconcilers.

### Upgrades, recovery, and uninstall

Each release should publish a tested bill of materials: Kubernetes range; charts and image digests; CRD API/schema versions; Spark, Scala, JDK, Python, Delta/Iceberg client matrix; optional native accelerator architecture; Airflow/provider versions; database migration level; connector/CDC schemas; and plugin license provenance. A release is a tested combination, not “latest” packages from ten projects.

Upgrade protocol: preflight and backup verification; pause new submissions if required; preserve/reconcile active runs; apply compatible CRD changes; migrate databases with a defined forward/rollback policy; roll controllers; canary one workspace; resume; verify state and permissions. Image rollback cannot reverse a destructive schema migration. Operator upgrades that affect external customer workloads require independent ownership and change control.

Back up operational databases, catalog metadata, workflow definitions, runtime manifests, object data, ML artifacts, encryption-key references, streaming offsets/checkpoints, and CDC state according to their recovery contracts. Test restoration into a clean cluster with identity and permissions intact. A successful backup job is not proof of a usable recovery.

Uninstall should retain data/PVCs and externally owned resources by default, with a separate reviewed purge plan. Deleted Kubernetes manifests should not implicitly delete catalog tables or the only copies of artifacts. Provide an export path for workflow specs, table metadata, notebooks, runtime lockfiles, and audit records.

## Nearby competitors and integration references

Stackable already markets a Kubernetes-native, integrated OSS data platform and lists Spark, Airflow, Kafka, Trino, NiFi, Superset, and OPA among its components. Its interoperability testing and support are real competitive dimensions. Its demos include JupyterHub/Keycloak and lakehouse examples. Our differentiation must be the unified authoring/governance/run experience, not a claim that assembling Kubernetes operators is unique. [Stackable platform](https://stackable.tech/en/), [Product information](https://docs.stackable.tech/home/stable/compliance/product-information/), [Demos](https://stackable.tech/en/demos/).

OpenLakeForge is an Apache-2.0 alpha with Kubernetes/Terraform/Helm, dlt, Iceberg, dbt-trino, Trino, Dagster, Polaris/Glue, and optional OpenMetadata/Superset. It describes an end-to-end batch platform and explicitly calls out ongoing work on footprint and installability. It is a useful integration reference, not evidence that the desired Databricks-style UX, Spark notebooks, or robust tenant execution already exists. [OpenLakeForge repository](https://github.com/malon64/openlakeforge).

## Evidence required before calling this production-ready

| Test | Required observable outcome |
|---|---|
| Install with existing Spark operator, Postgres, identity provider and object store | No ownership changes; compatible workload executes; incompatible capabilities fail preflight with an actionable reason |
| Install optional dependency profile | All images pinned; resource footprint measured; dependencies restored after node loss |
| Workflow retry during API/operator/network interruption | One traceable attempt per idempotency key; no duplicate untracked workloads or table commits |
| CDC crash around commit/checkpoint; update then delete then replay | Destination matches source; deleted rows are not resurrected; recovery position is auditable |
| Schema breaking change during scheduled ingestion | Policy-consistent quarantine/pause; downstream consumers are not silently corrupted |
| Workspace A submits code attempting workspace B access | Denial at execution/data/secret/API boundaries, including logs, ML artifacts, and connector state |
| Airflow restart and backfill | Logical intervals and immutable code identity survive; platform and backend states converge |
| Upgrade with running jobs | Documented handling of active runs; no lost metadata; failed canary prevents fleet rollout |
| Fresh-cluster restore | Catalog, jobs, identities, secrets references, artifacts and data become usable within measured RTO/RPO |
| Removal of a dependency or product release | External resources and retained data survive; dangling credentials and active workloads are handled |

No resource-size minimum or throughput target is asserted here without a running benchmark. First establish idle cost, startup latency, ingestion throughput, concurrent workloads, and recovery behavior for the certified deployment profiles. The product's promises should be tied to those measurements and to the exact release matrix.
