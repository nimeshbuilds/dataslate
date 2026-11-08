# Polaris metadata contract

The platform exposes real Apache Polaris / Iceberg REST metadata through administrator-approved connections and exact table grants. This is a metadata capability: it does not grant raw object storage access, query rows, create tables, vend credentials, or connect the Trino catalog automatically. PostgreSQL stores only short-lived discovery observations; fetched table metadata is not persisted by this API.

## Configuration and authority

`DATASLATE_CATALOG_BINDINGS_FILE` is a mounted JSON array, limited to 4 MiB and 1,000 bindings. Each binding has `id`, `workspaceId`, `name`, `kind: "polaris"`, a mandatory `policyVersion`, private `connection`, and explicit `allowedSubjects`. An empty subject/table array denies access. Viewer, editor and administrator workspace members can read metadata only with a matching exact issuer/subject and table grant; platform administration alone grants no workspace/catalog access.

```json
[
  {
    "id": "lake-metadata",
    "workspaceId": "analytics",
    "name": "Company lake",
    "kind": "polaris",
    "policyVersion": "metadata-v1",
    "connection": {
      "endpoint": "https://polaris.example.com/api/catalog",
      "warehouse": "lake",
      "realm": "DataSlate",
      "caFile": "/var/run/secrets/polaris/ca.crt",
      "auth": {"bearerTokenFile": "/var/run/secrets/polaris/token"}
    },
    "allowedSubjects": [
      {
        "issuer": "https://identity.example.com",
        "subject": "alice",
        "tables": [{"namespace": ["analytics"], "name": "orders"}]
      }
    ]
  }
]
```

`connection` also supports client credentials through `auth: {clientId, clientSecretFile, tokenEndpoint, scope?, style?: "post"|"basic"}` instead of `bearerTokenFile`. Credentials and custom CA paths must be absolute, supplied by operators and mounted into the API. Configuration validation does not read credentials or contact Polaris. The adapter reads token/secret files when used; CA content changes require restarting the API or changing the configured CA path. HTTPS verification is mandatory; arbitrary browser endpoints, insecure TLS, redirects, environment proxies and Iceberg access delegation are disabled.

Bindings are unique by workspace and binding ID. Allowed subject pairs are unique, with up to 1,000 exact table grants each. Namespace arrays contain 1–16 nonempty UTF-8 components of at most 128 bytes. Components cannot be `.` or `..` and cannot contain control characters or `/\\?#%`. Table names use the same component rules. Grants are exact and case-sensitive; there are no wildcard grants or implicit descendants. A shared Polaris service identity is not treated as a user's own catalog authority.

Every request reloads current binding policy. Remote results are returned only after another authorization check confirms the entire binding still matches, including subject grants, connection and policy version. Missing/malformed policy fails closed. Operators must atomically replace binding files to avoid transient partial JSON. Grant/index information uses current policy too. Discovery health does not authorize any table.

## API

Routes are under `/api/v1/workspaces/{workspaceId}`. Errors use the standard error envelope. All responses use `Cache-Control: no-store`.

- `GET /catalog-bindings` returns `{items:[{id,workspaceId,name,kind:"polaris",catalog,policyVersion,discoveryVerified,checkedAt?}]}`. Only granted bindings appear. Connection endpoints, credential paths, realm and other users' grants are omitted.
- `POST /catalog-bindings/{id}/verify` with `{}` makes a real discovery request and returns `{bindingId,workspaceId,verified:true,checkedAt,capabilities:[...]}` on success. This optional action refreshes a persistent observation; it does not prove table existence. An empty advertised read-capability list remains empty.
- `GET /catalog-bindings/{id}/namespaces` returns `{items:[{namespace:[...]}],source:"authorized-grants"}`. These are distinct namespace grants, not a live service catalog enumeration.
- `GET /catalog-bindings/{id}/tables?namespace=["analytics"]` (URL encode the JSON array) returns `{items:[{namespace,name}],source:"authorized-grants"}`. It lists only this subject's exact grant index. The UI must not claim upstream existence from this list.
- `POST /catalog-bindings/{id}/table` with `{namespace:[...],name}` authorizes the exact table, refreshes discovery automatically if absent/stale, verifies advertised load-table support, then performs a real metadata read. No user's periodic verify-click is required.

A discovery observation is scoped to workspace, binding ID and the SHA-256 of the entire current private binding. `discoveryVerified` is computed from a successful observation less than five minutes old. Its timestamp is the remote request start time. An older late-finishing request cannot overwrite a newer observation. Failure invalidates the observation; changing connection, grants or policy version invalidates a prior observation. Observations contain only capability names, timestamp, status and a private configuration hash; they are not a credential cache. This bounded observation cannot detect out-of-band Polaris policy changes immediately. Actual metadata reads still require a successful upstream response.

Table metadata returns `{bindingId,workspaceId,policyVersion,identifier:{namespace,name},formatVersion,uuid,lastUpdatedMs,currentSnapshotId?,currentSchema:{id,fields:[{id,name,required,type}]}}`. Snapshot IDs are exact signed 64-bit decimal strings. Schema/field IDs are bounded signed 32-bit integers; last-updated milliseconds are nonnegative JavaScript-safe integers. Primitive and nested struct/list/map types are validated and projected by the adapter. Table locations, metadata file locations, table properties, credentials, vendor configuration and row data are omitted. Upstream responses are limited to 8 MiB, schema depth and fields are bounded, and upstream URLs are never followed.

The API returns `403 catalog_access_denied` for absent/revoked grants; `503 catalog_policy_unavailable` when current configuration cannot be validated; `503 catalog_backend_unavailable` for failed/invalid upstream responses; `501 catalog_capability_unavailable` when discovery lacks table reads; `422 invalid_catalog_identifier` for invalid selectors; and `404 table_not_found` only after an authorized lookup returns upstream 404. Detailed provider errors and secret paths are not exposed.

## Persistence, verification and limits

Migration `006_catalog.sql` adds `dataslate_catalog_observations`, scoped by workspace/binding. Memory mode implements the same isolation and monotonic observation rules. The API's health/readiness still denotes process/schema readiness, not Polaris connectivity.

Automated tests cover exact subject/table grants, viewer access with explicit grants, cross-workspace and cross-issuer denial, absent and expired discovery, missing advertised capabilities, upstream missing tables and failures, configuration changes, in-flight revocation, malformed current policy, monotonic PostgreSQL/memory observations, and an actual TLS adapter-to-HTTP-handler path. The TLS test verifies no credential vending, excludes raw storage locations/configuration, and preserves a maximum signed 64-bit snapshot ID.

An explicit local Kubernetes qualification fixture also passed `TestCatalogLiveAPIWithPostgres`: the real Polaris service performed TLS/OAuth discovery and loaded the two-field Iceberg table through the platform API; PostgreSQL retained the observation, and revoking the grant denied access. This opt-in test requires `DATASLATE_TEST_POLARIS_CONFIG` (a private configuration file with credential paths) and `DATASLATE_TEST_DATABASE_URL`. The fixture is not production authority.

This contract does not certify a production Polaris deployment, storage integration, Trino-to-Polaris federation, engine-level row/column security, or EKS availability. Deployment and live qualification evidence are tracked separately. Semantic metadata indexing must reuse current table grants and policy versions; indexing itself must never broaden visibility or persist raw query rows.
