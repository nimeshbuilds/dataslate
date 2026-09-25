# DataSlate architecture decisions

The user selected **DataSlate** and **Sylon**, a new source repository, EKS/S3 first and an open-source distribution with paid support/hosting. Older names and local-first proposals in `reference/` are historical research, not current decisions.

## One product, complete scope

Own the UI/API/CLI contract, identity, resource model, execution lifecycle, environment builds, backend adapters, result service, governed tools, installation and operations. Reuse engines and durable services instead of implementing replacement Spark/storage systems. Broad BI, ML, agents, serving, feature store, operational databases, applications, sharing, clean rooms and marketplace are tracked in [coverage](../implementation/coverage.md).

Build a modular control plane with durable PostgreSQL operations/outbox, then split services only for demonstrated isolation or scaling needs. Each asynchronous side effect needs idempotency and reconciliation. Do not equate a successful pod exit with successful data publication.

## Runtime and governance profiles

- First candidate: Spark/Delta/UC OSS with Kyuubi/Ranger for governed SQL. Exact artifacts, user identity, catalog naming, row filters, masking, credentials and escape prevention require combined tests.
- Native acceleration: Comet evaluation after a correct vanilla Spark reference. This is a feature category comparable to native acceleration, not the Photon implementation.
- Batch Spark operator: discover exact Apache or Kubeflow CRD/controller/version, authorize and canary before use. Kyuubi owns its own SQL engine lifecycle.
- Table interoperability: connected lakes preserve format/ownership. Delta is the new-ingestion default with explicit Snappy-Parquet policy. Iceberg/Polaris/Trino is a separately supported profile.
- Cloud IO: S3 first, ADLS/GCS later profiles; qualify credentials on drivers, executors and catalog services separately. No broad secret fallback.

## Authoring, results and AI

The workspace owns notebook/SQL editing, source revisions, compute attachment, scheduling, results and contextual assistance. Shared documents do not automatically share outputs or execution identities. Table/Visualize/Profile/Execution is the shared result model. Full-data charts aggregate through governed backends; samples are labeled.

Sylon uses the same authorized resource and data APIs as people. Agent Studio builds versioned agents with tools, memory, evaluations, deployment and rollback. LangGraph PostgreSQL checkpoints handle durable run state. Permission-aware PostgreSQL/pgvector memory handles later recall; Headroom is an optional context optimizer. Deletion, source provenance and revoked rights apply to derived memory, checkpoints, traces and compression originals.

## Self-contained and existing services

Every dependency declares `managed` or `external` ownership separately from whether a capability is enabled. External integration requires version, API, permissions and capability verification. Self-contained distribution means provisioning the supported dependencies required for the selected modules, including an optional model-serving profile; it does not mean silently consuming disks, replacing CRDs or installing every module into every cluster.

The current Helm foundation covers the application and database boundary. Full distribution integration is work tracked explicitly in the coverage inventory. Production requires OIDC, persistent storage, TLS, isolation, quotas, backups, recovery, upgrades and truthful degraded states.

## Research

- [Consolidated component analysis](reference/platform-components-and-naming.md)
- [Kubernetes architecture and build plan](reference/kubernetes-lakehouse-product-blueprint.md)
- [Storage, ingestion and AI experience](reference/cloud-lake-and-ai-experience.md)
- [Results and visualization](reference/research-results-visualization.md)
- [Memory and token efficiency](reference/research-agent-memory.md)
- [Security and catalog integration](reference/research-security-catalog.md)

These are documentation/source-based research snapshots. Their runtime candidates and public-source observations require qualification against actual pinned distributions. Their naming shortlist was superseded by the user's DataSlate/Sylon decision.
