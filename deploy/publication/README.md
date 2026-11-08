# Verified Delta table publication

This optional controller connects a completed DataSlate file-ingestion run to an existing, reviewed Trino Delta catalog. It uses a Hive Metastore or AWS Glue. Apache Polaris remains the separate Iceberg catalog adapter.

The workspace chart adds `publication.enabled`, `bindings`, `credentials`, `engineEgress` and `controller` values. Execution and SQL must be enabled. Each binding pins one workspace, saved pipeline ID, ingestion execution binding, S3 destination, catalog/schema/table, publication identity, policy version, publishers and readers. See the [exact JSON contract](../../docs/implementation/publication-contract.md). Helm replaces `credentialsId` with generated mounted credential paths; raw token values and arbitrary paths are rejected.

Publication-enabled ingestion requires a dedicated namespace without notebook or arbitrary application execution bindings. Restrict external workloads and Kubernetes identities in that namespace too: the runtime receipt is trusted only within this boundary. The existing execution binding must use `file-ingestion-batch`, grant the publishers, and permit the destination prefix. Each reader must independently have the named SQL binding for the same engine/catalog/schema. Publication credentials use a different token Secret key from SQL credentials; the publisher engine user differs from every SQL reader on that engine. Configuration changes that restrict access require policy version rotation and a coordinated deployment of engine and DataSlate grants.

The explicit publication reader grant exposes the entire declared column schema, including column names that Trino may deny to that reader. It grants no row or value access; engine policy still controls queries. Do not grant this metadata access when hidden column names are sensitive. Per-reader schema projection is not implemented.

The publication worker receives only publication credentials, configuration, and PostgreSQL trust. It has no Kubernetes API token or RBAC and no provider/model credentials. Its network policy permits DNS, the configured database and exact engine destinations. The API receives publication configuration without its credentials. Projected Secrets remain rotatable; credentials and engine access must remain available while accepted operations need reconciliation.

Before enabling the binding, the operator must provision the schema and review:

- The pinned Trino Delta connector with `delta.register-table-procedure.enabled=true`, `delta.security=SYSTEM`, an approved HMS/Glue catalog and storage identity.
- The [exact publisher and reader rules](../sql/README.md), including row filters, masks and metadata denial. Publisher ownership permits privileged DDL on its exact target; the credential is worker-only.
- The immutable ingestion runtime, compute admission policy, S3 workload identities, explicit source/output/checkpoint prefixes and writer drain behavior.
- Database migrations, backups, credential rotation, engine query limits and operational monitoring.

Publication sends one generated registration request after persisting intent. It does not adopt an unrelated existing table, overwrite a mapping, unregister on failure, reset a checkpoint or retry Spark. Verification checks location, retained Delta version, schema and protocol. An unknown dispatched registration keeps its target reservation; investigate engine/catalog state before an explicit administrator recovery. No automated destructive repair is exposed.

The production image is built from `api/PublicationControllerDockerfile`. The controller listens on port 8085 for `/healthz` and `/readyz`; the Helm chart does not expose a public Service for it. The native runtime and Trino path has [local qualification evidence](../../runtime/ingestion/LAKE_QUALIFICATION.md). Helm rendering and native local tests do not establish an EKS/S3 production support claim.
