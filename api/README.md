# DataSlate API foundation

A real Go control plane for workspace membership, persistent notebook/SQL documents, revision conflicts and history, deployment dependency planning, and two distinct SparkApplication preview compilers. An opt-in durable controller submits approved immutable Python applications or exact saved Python notebook snapshots to the Apache Spark operator. Additional durable controllers provide Trino SQL, metadata indexing and Sylon generation. Versioned file ingestion and Polaris schema discovery share the workspace API. Interactive kernels, general source connectors and Agent Studio remain separate integration work. The broad module catalog is engineering scope and implementation status, not a list of installed backends.

## Run

From `api/`, explicit local development:

```sh
DATASLATE_AUTH_MODE=development go run ./cmd/dataslate-api
```

This binds `127.0.0.1:8080`, seeds workspace `default`, grants an explicitly marked development administrator and loses all data on restart. Development mode refuses a non-loopback bind, non-loopback HTTP Host, cross-origin browser writes, and database configuration. Literal loopback origins on other ports are permitted for the Vite development proxy. Do not expose it through an external reverse proxy.

Production uses OIDC and PostgreSQL:

```sh
go build -o dataslate-api ./cmd/dataslate-api
./dataslate-api migrate
./dataslate-api serve
```

Configure secrets through the deployment environment, not command-line flags. Migrations are explicit, transactionally applied with a PostgreSQL advisory lock, and embedded in the binary. The API does not auto-migrate or fall back to memory. `/readyz` checks the required migration in the configured database. `/healthz` is process liveness. HTTPS termination is supplied by the deployment ingress/gateway.

| Variable | Meaning |
|---|---|
| `DATASLATE_ADDR` | Listen address, default `127.0.0.1:8080`; Kubernetes uses `:8080` |
| `DATASLATE_AUTH_MODE` | `oidc` by default; `development` must be explicit |
| `DATASLATE_DATABASE_URL` | PostgreSQL connection string; required for production and migrations |
| `PGSSLMODE` | Set to `verify-full` unless already enforced by the connection string |
| `PGSSLROOTCERT` | Optional path to the trusted PostgreSQL CA bundle |
| `DATASLATE_OIDC_ISSUER` | HTTPS issuer URL used for OIDC discovery |
| `DATASLATE_OIDC_AUDIENCE` | API access-token audience; must differ from browser client ID |
| `DATASLATE_OIDC_CLIENT_ID` | Public browser OIDC client identifier |
| `DATASLATE_EXECUTION_BINDINGS_FILE` | Optional mounted JSON policy for immutable applications and issuer-qualified grants |
| `DATASLATE_SQL_BINDINGS_FILE` | Optional granted Trino identities and read-only policy versions; shared with SQL controller |
| `DATASLATE_CATALOG_BINDINGS_FILE` | Optional Polaris connection references and exact current table grants |
| `DATASLATE_SEARCH_BINDINGS_FILE` | Optional lexical/hybrid source, model and transfer profiles; shared with search controller |
| `DATASLATE_SYLON_BINDINGS_FILE` | Optional approved provider/model, context-transfer policy and exact subjects; provider credentials are mounted only in the Sylon controller |
| `DATASLATE_EXECUTION_NAMESPACES` | Explicit space-separated Kubernetes execution namespace allowlist, retained until runs are drained |
| `DATASLATE_OIDC_SCOPES` | Space-separated browser scopes; defaults to `openid profile`; configure the provider-specific API scope too |

The production connector rejects unverified/disabled TLS and insecure connection fallbacks. pgx respects `PGSSLMODE` and `PGSSLROOTCERT`; explicit DSN options override environment settings, so an insecure override fails startup. The migration command requires the database configuration only, not an identity-provider connection. Serve initializes OIDC discovery and fails closed on discovery/configuration errors.

## Identity contract

Only bearer access tokens from the configured issuer and API audience are accepted. Signature, expiration and optional `nbf` are checked using `coreos/go-oidc` and a JWKS-backed verifier. Supported signing algorithms are RS256, ES256 and PS256. If `token_use` exists, it must be `access`. ID tokens for the browser's distinct audience are rejected. Forwarded user/group headers are ignored. Token revocation follows the identity provider's access-token lifetime; immediate introspection/revocation is not implemented, so use appropriately short-lived access tokens.

```json
{
  "sub": "user-identifier",
  "dataslate_roles": ["platform-admin"],
  "dataslate_workspaces": {
    "ws-example": "editor"
  }
}
```

- `viewer`: read workspace documents and revision history.
- `editor` or workspace `admin`: read, create, update, delete, preview Spark manifests.
- `platform-admin`: create workspaces. This does not implicitly grant workspace data membership.

Creation returns the new workspace ID. The administrator must assign membership for that ID through the identity provider and refresh the access token. Identity-provider group synchronization, membership administration UI, audit-event persistence, fine-grained notebook sharing and data-engine authorization are subsequent integration work. There is no row-level data security claim in this document API.

## API and compiler constraints

[OpenAPI](openapi.yaml) is OpenAPI 3.1 in JSON syntax, which is valid YAML. Main endpoints follow [the shared foundation contract](../docs/implementation/foundation-contract.md). Additional endpoints:

- `DELETE /api/v1/workspaces/{workspaceId}/notebooks/{id}?revision=N` deletes the document and its saved revision history; optimistic revision is mandatory.
- `GET /api/v1/workspaces/{workspaceId}/notebooks/{id}/revisions` returns `{items:[Notebook]}`.
- `GET .../execution-bindings` and the run create/list/get/cancel/events/logs routes implement [approved batch execution](../docs/implementation/execution-contract.md). Submission is unavailable until explicit bindings are configured.
- Versioned `ingestion-pipelines` and pipeline run selection implement [incremental file ingestion](../docs/implementation/ingestion-contract.md).
- `sql-bindings` and `queries` implement [governed SQL](../docs/implementation/sql-contract.md), exact saved-cell snapshots, cancellation and owner-scoped precise results.
- `catalog-bindings` exposes [authorized Polaris metadata](../docs/implementation/catalog-contract.md), discovery verification and native Iceberg schema reads.
- `search`, `search/status`, `search/index` and `search/index-jobs` implement [authorized lexical/hybrid retrieval](../docs/implementation/search-contract.md) and durable indexing.
- `sylon/bindings`, `sylon/conversations` and `sylon/jobs` implement [Sylon conversations and code proposals](../docs/implementation/sylon-contract.md). Context and outputs require current grants; code application is explicit, revision-checked and never executes. The obsolete `POST .../sylon/messages` route remains unavailable.

Every notebook mutation validates IDs, document kind, unique cell IDs and allowed languages. SQL documents contain SQL cells only. Cell sources are capped at 256 KiB, documents at 200 cells, requests at 1 MiB. Unknown JSON fields and multiple JSON values are rejected; user outputs are not stored in shared notebook documents. Document revisions are atomic and retained until document deletion. A saved Python batch run retains its own independent source snapshot after later edits/deletion. It compiles cells separately with stable traceback filenames and shared globals in one fresh batch process, capped at 256 KiB of generated source. Interactive kernels and rich Spark display output are not implemented yet; Trino queries have their own owner-scoped typed result service.

The planner supports the initial `aws` profile, requires explicit optional dependencies, orders them topologically and rejects unknown modules, duplicates, modes and cycles. External HTTPS API/management endpoint syntax may be supplied; absence is a warning requiring later coordinates and secret references. It does not probe networks or perform an installation. PostgreSQL and identity are implicit core dependencies. Managed versus external is planned ownership, not existing-service detection.

Spark previews currently support `.py` entrypoints only, with `local:///`, `s3://` or `s3a://`; the API has no Java/Scala main-class input yet. Image references require a tag other than `latest` or a digest. Tags are still mutable: deployment admission must enforce approved image digests. Executor count is bounded 1–100, dynamic allocation is disabled. Runtime 4.1.3 is a compatibility candidate, not a production-certified bundle.

- [Apache operator 1.0.0 Python example](https://github.com/apache/spark-kubernetes-operator/blob/1.0.0/examples/pi-python.yaml): `spark.apache.org/v1`, `spec.pyFiles`, `spec.runtimeVersions`, `spec.sparkConf`.
- [Kubeflow operator 2.5.2 CRD](https://github.com/kubeflow/spark-operator/blob/v2.5.2/config/crd/bases/sparkoperator.k8s.io_sparkapplications.yaml): `sparkoperator.k8s.io/v1beta2`, `spec.type: Python`, `spec.mainApplicationFile`, `spec.driver` and `spec.executor`.

The preview accepts names and service-account choices for review but does not authorize their future use. Batch submission uses administrator-controlled bindings, digest-pinned images, issuer-qualified execution grants and durable reconciliation. Notebook bindings explicitly authorize arbitrary Python under their engineering workload identity; they cannot provide row-level protection from users who have raw data credentials. Namespace admission, workload identity and a compatible runtime require separate qualification. The API is unprivileged with execution disabled; when enabled it receives read-only SparkApplication/Pod/log access in explicit namespaces. Only the separate controller can create/delete SparkApplications and manage protected notebook source ConfigMaps. API source inspection reads the owned PostgreSQL snapshot and needs no ConfigMap access.

## Verification

```sh
go test -race ./...
go vet ./...
```

Tests cover real HTTP handlers, persisted memory documents, unknown fields and body bounds, membership roles, cross-workspace IDs, stale and concurrent revisions, signed JWTs with JWKS, audience/issuer/expiry/signature failures, development DNS rebinding and origins, production configuration, both manifest schemas and dependency cycles. Tests do not replace live identity-provider or cluster qualification.

To exercise PostgreSQL against an isolated test database, set `DATASLATE_TEST_DATABASE_URL` and run the same tests. The test migrates the database, creates unique test resources, opens a second connection to verify persistence, races revision updates and removes its test resources. Only that test constructor permits unverified TLS for an isolated CI PostgreSQL service. Without the variable, the integration test is explicitly skipped.

API and workspace OCI image builds passed in the [November publication CI](../docs/implementation/validation.md). A later local checkpoint verified API/controller/web container startup and real Spark execution. Current local Trino and Polaris qualification is recorded in their deployment profiles; separate API/PostgreSQL tests exercise real backends. These are bounded checks, not a complete production installation. Remaining live gates include cloud CA/identity, full browser IdP login, EKS/S3, native lake query interoperability, UC/Ranger enforcement, upgrades, restores, capacity and resource budgets.


## Execution controller

Build `go build ./cmd/dataslate-controller` or `api/ControllerDockerfile` with build context `api/`. Production requires verified PostgreSQL TLS, `DATASLATE_EXECUTION_BINDINGS_FILE`, and `DATASLATE_EXECUTION_NAMESPACES`. It uses Kubernetes projected service-account credentials, reopens the token file for rotation, verifies the CA and refuses redirects. No kubeconfig/cloud credential fallback is used. `/healthz` and `/readyz` listen on port 8081 by default (`DATASLATE_CONTROLLER_ADDR`). The API and controller use the same migrated database and policy file.

The controller is the whole-job submission owner: unknown POST outcomes are reconciled without blindly retrying. Namespace quotas, admission and driver permissions remain separately administered. See the [execution contract](../docs/implementation/execution-contract.md) for policy revocation, cancellation, bounded queues/events and the limits of automated fixture tests.
