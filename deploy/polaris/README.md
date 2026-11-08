# Apache Polaris catalog integration

DataSlate uses Apache Polaris for **native Iceberg** catalog metadata and authorization. Polaris is not a Delta transaction catalog. The Delta ingestion runtime does not dual-write or translate a Delta table into Iceberg, and this adapter does not pretend that catalog discovery makes a table queryable by an arbitrary SQL engine.

The read-only client is in `api/internal/catalogs/polaris`. It implements the Polaris 1.8.0 Iceberg REST endpoints for configuration discovery, paginated namespaces, paginated tables, and loading a table's current schema. Its HTTPS contract is independent of managed provisioning, so an existing Polaris installation remains externally owned. It never calls the management API, registers tables, modifies grants, applies server-returned configuration, requests credential vending, or follows redirects.

## Identity and authorization

The caller must authorize the DataSlate workspace, catalog binding, user and requested operation before calling the adapter. Catalog metadata is protected information. Do not expose a shared administrator token to every workspace or infer that a user may see all tables accessible to a broker account. Prefer an exact subject/delegated token or a narrow, explicitly granted service identity for each authorized binding. Polaris remains authoritative for catalog grants; DataSlate authorization is an additional gate. This metadata client does not implement row filtering or column masking for query engines.

Authentication supports a rotating mounted bearer-token file, or OAuth client credentials using an explicit HTTPS token endpoint and a mounted client-secret file. Both form-post and OAuth Basic styles are supported. The client rereads files and invalidates a cached access token when the secret changes. It validates TLS using system trust plus an optional CA file. There is no HTTP, insecure-TLS, redirect, environment-proxy or node-credential fallback. A 401 clears the cached OAuth token for a later explicit request; requests are not silently retried.

The optional `Polaris-Realm` value and configured catalog are fixed per binding. Iceberg multi-part namespace components are joined with the required unit separator and URL encoded as `%1F`; caller path fragments cannot escape the configured catalog. Response bodies, page sizes, schema depth, identifiers and types are bounded. The safe table projection excludes server configuration, storage credentials, properties, field documentation/defaults and historical snapshot arrays. Table locations are returned as metadata and never fetched by this client. Errors do not include server response bodies or credentials.

Example operator-owned client configuration (IDs and file references only):

```json
{
  "endpoint": "https://polaris.catalog.example/api/catalog",
  "warehouse": "analytics",
  "realm": "DATASLATE",
  "caFile": "/etc/dataslate/polaris/ca.pem",
  "auth": {
    "clientId": "dataslate-catalog-reader",
    "clientSecretFile": "/etc/dataslate/polaris/client-secret",
    "tokenEndpoint": "https://identity.example/oauth/token",
    "scope": "PRINCIPAL_ROLE:dataslate_reader",
    "style": "post"
  }
}
```

For an existing externally managed catalog, mount its approved configuration and credentials, then run the read-only check from `api/`:

```sh
go run ./internal/catalogs/polaris/probe --config /operator/path/client.json \
  --namespace '["engineering","raw"]' --table orders
```

This verifies discovery, namespace/table listing and native table loading through the same client. A successful probe does not qualify query-engine access, row policies, cloud storage access or the entire DataSlate deployment.

## Explicit managed deployment

`render.py` verifies the upstream chart SHA and renders the pinned official chart. It does not contact Kubernetes. `upstream-lock.json` pins chart 1.8.0, server image and matching admin-tool image by immutable digest. No cluster-scoped CRDs or operators are installed. The profile uses PostgreSQL JDBC persistence, shared RSA token keys, required realm headers, TLS-only API port 8443, non-root containers, read-only server root filesystem, explicit resources and a NetworkPolicy. HTTP 8182 is used only for kubelet health probes; it is not exposed to application clients by the policy.

Provision these **operator-owned** resources first:

- An existing PostgreSQL database/user and `polaris_schema` schema. In 1.8.0 the admin tool does not create the database schema. The JDBC URL should require verified TLS for a production database; put its CA/trust material in the operator's database configuration. Database backups and recovery are operational prerequisites.
- A database Secret with `username`, `password`, `jdbcUrl`; a TLS Secret with `tls.crt`, `tls.key`; and a signing-key Secret with `public.pem`, `private.pem`. The TLS certificate must match the hostname used by clients. The signing keys must be shared across replicas. Key/certificate rotation needs a controlled rollout and verification.
- A bootstrap Secret containing `credentials.json` in Polaris's documented realm-to-credentials format. Provision this with a secret manager; do not place its contents in source control, Helm values or shell arguments. Only the one-shot bootstrap Job mounts it. The runtime server does not receive root credentials.
- An existing dedicated ServiceAccount with the approved workload identity. On EKS use IRSA and narrowly scoped S3/STS permissions. Node instance metadata is disabled. No static cloud key is accepted by this profile.
- Explicit network egress entries for the database and the actual storage/identity destinations. The example CIDR is illustrative. Replace it for the cluster/CNI; Service DNAT and network-policy enforcement vary. No unrestricted default egress is generated. Application ingress is limited by namespace and Pod labels; those labels are administrator controlled.

`profile.example.json` contains only references and non-secret configuration. Adjust it, download the exact chart URL in the lock, and render:

```sh
python3 deploy/polaris/render.py --config /operator/path/polaris-profile.json \
  --helm /absolute/path/helm --chart /operator/path/polaris-1.8.0.tgz > /operator/path/polaris.yaml
```

Python requires PyYAML. Review the rendered namespaced resources. Apply the explicit bootstrap Job after the database is ready, verify its completion, then deploy the server and run the read-only check. The Job has no automatic retries and no Helm hook; do not run bootstrap concurrently or silently adopt an existing release. Its `--credentials-file` argument contains only a mounted path, and `--print-credentials` is never used. Protect bootstrap logs because malformed upstream input can produce diagnostic exceptions. Existing installations/upgrades require an explicit migration and backup procedure; this recipe is not an upgrade automation.

The production profile intentionally requires a provisioned durable database, secret store and workload identity. It does not claim that a disposable PostgreSQL container provides production HA, backup or recovery. External OIDC is supported by upstream Polaris; an externally configured Polaris endpoint can be used with this client. The generated managed profile currently uses Polaris's internal OAuth service with shared RSA keys. OIDC realm mapping, identity delegation and cross-engine authorization must be qualified separately before claiming those deployment modes.

## Reproducible local qualification

The optional fixture is restricted to `dataslate-polaris-qualification`, `https://localhost:28443` and a catalog named `qualification`. It uses **ephemeral PostgreSQL and FILE storage**, neither of which is enabled by the production renderer. Polaris explicitly marks FILE storage insecure; the fixture alone sets `ALLOW_INSECURE_STORAGE_TYPES` and `polaris.readiness.ignore-severe-issues=true`. The production renderer never enables either bypass. First apply `production-server.yaml` and verify it reaches Ready with the upstream checks enabled; then use `server.yaml` for the FILE-only table test. The helper generates random credentials in a new mode-0700 directory; credential files/manifests are mode 0600. It never applies them or prints their contents.

```sh
python3 deploy/polaris/prepare_qualification.py --directory /private/new-fixture-dir \
  --helm /absolute/path/helm --chart /operator/path/polaris-1.8.0.tgz
# Use an explicit kubeconfig/context for an owned disposable cluster for every command.
kubectl --kubeconfig /owned/kubeconfig --context owned-local apply -f /private/new-fixture-dir/prerequisites.yaml
kubectl --kubeconfig /owned/kubeconfig --context owned-local -n dataslate-polaris-qualification rollout status deployment/postgres
kubectl --kubeconfig /owned/kubeconfig --context owned-local apply -f /private/new-fixture-dir/bootstrap.yaml
kubectl --kubeconfig /owned/kubeconfig --context owned-local -n dataslate-polaris-qualification wait --for=condition=complete job/polaris-bootstrap --timeout=180s
kubectl --kubeconfig /owned/kubeconfig --context owned-local apply -f /private/new-fixture-dir/production-server.yaml
kubectl --kubeconfig /owned/kubeconfig --context owned-local -n dataslate-polaris-qualification rollout status deployment/polaris
kubectl --kubeconfig /owned/kubeconfig --context owned-local apply -f /private/new-fixture-dir/server.yaml
kubectl --kubeconfig /owned/kubeconfig --context owned-local -n dataslate-polaris-qualification rollout status deployment/polaris
kubectl --kubeconfig /owned/kubeconfig --context owned-local -n dataslate-polaris-qualification port-forward service/polaris 28443:8443 --address 127.0.0.1
```

With that loopback tunnel active, create the isolated native Iceberg fixture and run the read-only production client:

```sh
python3 deploy/polaris/create_qualification_tables.py --directory /private/new-fixture-dir
cd api
go run ./internal/catalogs/polaris/probe --config /private/new-fixture-dir/client.json \
  --namespace '["engineering","raw"]' --table orders
```

The fixture writer uses management operations only on the fixed localhost fixture and never prints response bodies or access tokens. It creates an empty native Iceberg table with a nested namespace and checks unauthenticated denial. Its administrative credential is for fixture creation only, not a production identity model. Delete the owned namespace and securely remove its local credential directory after qualification; this irreversibly deletes the disposable fixture data. Do not use these cleanup steps against an external installation.

Automated checks:

```sh
python3 -m unittest discover -s deploy/polaris -p 'test_*.py' -v
cd api && go test -race ./internal/catalogs/polaris/...
```

Official interfaces: [Polaris 1.8.0 release](https://polaris.apache.org/downloads/1.8.0/), [chart production configuration](https://polaris.apache.org/releases/1.8.0/helm-chart/production/), [authentication](https://polaris.apache.org/releases/1.8.0/helm-chart/authentication/), [persistence](https://polaris.apache.org/releases/1.8.0/helm-chart/persistence/), [admin tool](https://polaris.apache.org/releases/1.8.0/admin-tool/), [pinned Iceberg REST specification](https://github.com/apache/polaris/blob/apache-polaris-1.8.0/spec/iceberg-rest-catalog-open-api.yaml). Pinned compatible interfaces and successful local checks do not establish production cloud qualification.

## Recorded qualification

On 2026-11-07, the pinned 1.8.0 chart/server/admin images ran on an owned local k3s v1.35.0 cluster (Linux arm64). JDBC bootstrap completed and the production profile reached Ready with upstream readiness checks enabled. Its TLS certificate was verified. The separate FILE fixture created a native Iceberg v2 table in a two-part namespace; the production Go client discovered/listed/loaded its two-field schema, and both unauthenticated checks were denied. The NetworkPolicy denied TCP access from a Pod without the approved label, allowed it with the label, and denied it again after label removal. Twelve top-level Go tests with adversarial subcases pass under the race detector; four profile tests pass. See [qualification.json](qualification.json) for scope and explicit exclusions. The local fixture uses Recreate updates to avoid a second JVM exceeding its disposable cluster memory budget.
