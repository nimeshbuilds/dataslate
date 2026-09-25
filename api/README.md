# DataSlate API foundation

A real Go control plane for workspace membership, persistent notebook/SQL documents, revision conflicts and history, deployment dependency planning, and two distinct SparkApplication preview compilers. It does **not** execute Spark, SQL, ingestion, models or Sylon. The broad module catalog is engineering scope and implementation status, not a list of installed backends.

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
- `POST .../runs` and `POST .../sylon/messages` return HTTP 501 `capability_unavailable`, after workspace authorization. No execution is simulated.

Every notebook mutation validates IDs, document kind, unique cell IDs and allowed languages. SQL documents contain SQL cells only. Cell sources are capped at 256 KiB, documents at 200 cells, requests at 1 MiB. Unknown JSON fields and multiple JSON values are rejected; user outputs are not stored in shared notebook documents. Revisions are atomic and retained until document deletion. No server execution, notebook output renderer or Python kernel is present yet.

The planner supports the initial `aws` profile, requires explicit optional dependencies, orders them topologically and rejects unknown modules, duplicates, modes and cycles. External HTTPS API/management endpoint syntax may be supplied; absence is a warning requiring later coordinates and secret references. It does not probe networks or perform an installation. PostgreSQL and identity are implicit core dependencies. Managed versus external is planned ownership, not existing-service detection.

Spark previews currently support `.py` entrypoints only, with `local:///`, `s3://` or `s3a://`; the API has no Java/Scala main-class input yet. Image references require a tag other than `latest` or a digest. Tags are still mutable: deployment admission must enforce approved image digests. Executor count is bounded 1–100, dynamic allocation is disabled. Runtime 4.1.3 is a compatibility candidate, not a production-certified bundle.

- [Apache operator 1.0.0 Python example](https://github.com/apache/spark-kubernetes-operator/blob/1.0.0/examples/pi-python.yaml): `spark.apache.org/v1`, `spec.pyFiles`, `spec.runtimeVersions`, `spec.sparkConf`.
- [Kubeflow operator 2.5.2 CRD](https://github.com/kubeflow/spark-operator/blob/v2.5.2/config/crd/bases/sparkoperator.k8s.io_sparkapplications.yaml): `sparkoperator.k8s.io/v1beta2`, `spec.type: Python`, `spec.mainApplicationFile`, `spec.driver` and `spec.executor`.

The preview accepts names and service-account choices for review but does not authorize their future use. Before submission exists, bind workspaces to approved namespaces, runtime images and identities, add admission restrictions, validate against the installed CRD, and establish idempotent submission/reconciliation. The API service account needs no Kubernetes API access in this foundation.

## Verification

```sh
go test -race ./...
go vet ./...
```

Tests cover real HTTP handlers, persisted memory documents, unknown fields and body bounds, membership roles, cross-workspace IDs, stale and concurrent revisions, signed JWTs with JWKS, audience/issuer/expiry/signature failures, development DNS rebinding and origins, production configuration, both manifest schemas and dependency cycles. Tests do not replace live identity-provider or cluster qualification.

To exercise PostgreSQL against an isolated test database, set `DATASLATE_TEST_DATABASE_URL` and run the same tests. The test migrates the database, creates unique test resources, opens a second connection to verify persistence, races revision updates and removes its test resources. Only that test constructor permits unverified TLS for an isolated CI PostgreSQL service. Without the variable, the integration test is explicitly skipped.

Not yet qualified here: live PostgreSQL/cloud CA setup, full external IdP login, OCI image builds, EKS/S3 workload identity, Kubernetes server-side dry runs, Spark execution, UC/Ranger enforcement, external module installations, upgrades, restores and resource budgets. Production release depends on those gates.
