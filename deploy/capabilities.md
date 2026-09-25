# DataSlate deployment capability inventory

**Foundation** means implemented application/deployment code, with local verification described in the repository. **Planned** means the contract and component direction exist but the backend integration does not yet execute. **No integration is currently production-qualified on EKS.** A module advertised by the planning API is not an installed service.

The self-contained product profile is committed scope. Each stateful/backend module needs installation, connection validation, identity, health, backup, upgrades and uninstall ownership before it can be enabled. The modular profiles below avoid requiring a second copy of an existing compatible tool.

| Module ID | Backend/component direction | Managed/external ownership | Current implemented boundary / remaining work |
|---|---|---|---|
| application core | Go API, React UI | Managed | **Foundation:** chart, same-origin routing, OIDC/PG config, API/web probes, resource limits, migration Job, network policy. Live rollout gate outstanding. |
| `postgres` | PostgreSQL; optional CNPG | Both | **Foundation:** existing TLS connection/CA references or persistent three-instance Cluster. Scheduled backup/restore qualification planned. |
| `identity` | OIDC; optional Keycloak | External now, both intended | **Foundation:** external provider integration/config. Managed Keycloak installation and lifecycle planned. |
| `spark-operator` | Apache operator; existing Kubeflow adapter | Both | **Foundation:** checksum-locked separate Apache bootstrap; explicit preview adapter selection. Discovery/adoption/upgrade workflow and live reconciliation planned. |
| `spark` | Spark, Spark Connect, isolated Jupyter kernels | Both | Manifest preview foundation; runtime build, submit/watch/cancel, interactive routing and admission planned. |
| `delta` | Delta Lake; Snappy Parquet ingestion policy | Both | Planned runtime compatibility and multi-writer/cloud commit tests. Existing tables keep their format/features. |
| `unity-catalog` | Unity Catalog OSS | Both | Planned catalog API, storage credentials, grants and engine qualification. |
| `kyuubi` | Kyuubi/Spark SQL | Both | Planned engine sessions, execution/results, cancellation and user propagation. |
| `ranger` | Apache Ranger | Both | Planned catalog-aware masking/row-filter enforcement; combined UC/Delta/Kyuubi compatibility is a release gate. |
| `zookeeper` | ZooKeeper for qualified Kyuubi HA | Both | Planned lifecycle, service discovery and recovery. |
| `ingestion` | dlt + controlled Spark publication | Both | Planned connector registry, secrets, schema discovery, staging/checks/publication. |
| `kafka` | Kafka; optional Strimzi | Both | Planned secure connectivity and optional operator-managed installation. |
| `cdc` | Debezium/Kafka Connect | Both | Planned source permissions, checkpoints, ordering, deletes and replay. |
| `pipelines` | Spark declarative/structured streaming | Both | Planned pipeline compiler, incremental state, quality checks and recovery. |
| `airflow` | Apache Airflow | Both | Planned native UI/API adapter, generated workflows and a single retry authority. |
| `maintenance` | DataSlate controller + native Delta/Iceberg procedures | Both | Planned ownership/retention-aware scheduling; no automatic vacuum on imported lakes. |
| `lineage` | OpenLineage + DataSlate mappings/views | Both | Planned event capture, permissions, run/resource correlation and retention. |
| `dashboards` | Governed Result Service, charts, saved queries | Managed | Planned ECharts/AG Grid results, backend aggregation, sharing and viewer-identity reruns. Editors are not query execution. |
| `mlflow` | MLflow | Both | Planned experiment/artifact/model registry and trace/evaluation integration. |
| `sylon` | Provider adapters / enterprise LLM gateway | Both | Planned SQL assistance, cell troubleshooting, suggestions, provider auth and evaluations. No synthetic model responses. |
| `agents` | LangGraph + typed tools + approved MCP | Both | Planned durable execution, action authorization/receipts, Agent Studio and publishing. |
| `memory` | Policy-aware PostgreSQL/pgvector; optional Hindsight | Both | Planned workspace/person/team ACLs, provenance, TTL, deletion and recall evaluations. |
| `headroom` | Headroom context compression | Both | Planned optional middleware after authorization; no automatic cross-team learning. |
| `model-serving` | KServe + qualified runtimes/vLLM | Both | Planned model deployment, private endpoints, GPU isolation, request policy and rollout. |
| `feature-store` | Feast + qualified lake/online adapters | Both | Planned point-in-time datasets, materialization and feature serving. |
| `search` | PostgreSQL/pgvector; optional larger search backend | Both | Planned catalog/document/vector search with permission-aware retrieval and evaluation. |
| `apps` | Isolated Kubernetes applications | Both | Planned build/deploy, routes, identity, resource limits and lifecycle. |
| `sharing` | Qualified Delta Sharing profile | Both | Planned recipient/grant lifecycle, access audit and shared-data boundaries. Clean rooms/marketplace need additional product/security work. |
| `iceberg` | Native Iceberg + optional Polaris | Both | Planned explicit alternate runtime/catalog profile; does not silently replace Delta or UC. |
| `federation` | Qualified Trino connectors | Both | Planned query/identity mappings, pushdown and cross-source policies. |
| `observability` | OpenTelemetry/Prometheus/Grafana/logs/traces | Both | Probes foundation; full run correlation, telemetry dashboards, alerting and retention planned. |
| `secrets` | Existing cloud secret manager; ESO/OpenBao optional | Both | Secret references foundation; managed secret service and automatic rotation workflows planned. |
| `registry` | OCI registry; optional Harbor/BuildKit | Both | Image references/Dockerfiles foundation; managed registry, isolated builds and signing pipeline planned. |

## Delivery profiles

1. **Reuse existing infrastructure:** EKS, customer S3, registry, OIDC, PostgreSQL and ingress; deploy DataSlate core, then enable individual qualified adapters. Current chart implements the core part of this profile.
2. **Managed data platform:** existing EKS/S3 with separately managed compatible operators and CNPG, then managed catalog, SQL, ingestion, scheduling, AI/ML and observability releases. Only CNPG Cluster and separate operator bootstrap code exist today.
3. **Full self-contained software distribution:** add managed identity, secrets, registry and all enabled backend modules to profile 2, retaining explicit operator ownership and persistent-data lifecycles. Kubernetes, cloud storage/permissions, compute capacity and DNS remain infrastructure prerequisites. This profile is planned, not an existing installation switch.

Broad compatibility is qualified per profile and runtime bill of materials. Adding arbitrary packages to a notebook image, swapping a catalog backend or enabling native acceleration can change behavior and security. Images must use locked packages and pass the runtime's correctness, dependency/license and identity tests before being advertised as supported.
