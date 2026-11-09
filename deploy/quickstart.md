# DataSlate development and production quickstart

DataSlate is an open-source Kubernetes lakehouse platform under active development. The core UI/API is usable today; optional Spark batch execution, SQL, catalog, ingestion, search, Sylon, publication and Airflow adapters have specific tested profiles. This guide distinguishes running the developer workspace from installing a controlled Kubernetes release. It does not promise Databricks feature parity or a complete production qualification of every integration.

See [current capabilities](capabilities.md), [validation evidence](../docs/implementation/validation.md), and [delivery gates](../docs/implementation/delivery-plan.md). The initial cloud target is EKS/S3 for teams in one organization. Object storage—S3, ADLS/Blob or GCS—is the lake; EBS/Azure Disk/Persistent Disk provide block storage for services such as PostgreSQL. The current setup helper does not provision cloud infrastructure.

## Developer workspace

Clone the repository and use the language versions declared in the source/CI. From its root:

```sh
git clone https://github.com/NimeshBuilds/dataslate.git
cd dataslate
cd web && npm ci && cd ..
```

In the first terminal:

```sh
make dev-api
```

In a second terminal, from the same repository root:

```sh
make dev-web
```

Follow the local URL printed by Vite. These targets run the local API and web development servers; check [the repository README](../README.md) and [Makefile](../Makefile) for ports and exact prerequisites. The development authentication mode and in-memory state are for an isolated developer machine. They are not accepted by the Kubernetes chart and should not be exposed publicly. Stopping a memory-backed API discards its data; development UI previews do not imply a backend engine is running.

Use the test targets before changing deployable images:

```sh
make test
```

Optional runtime suites and their pinned dependencies are documented with each integration. The complete local gate is `scripts/test-local.sh`; read its prerequisite checks before running it. Real Spark, PostgreSQL, Trino, Airflow and browser qualifications are distinct from unit tests. Never point tests at a production database or cluster.

## Production prerequisites and ownership

Use a dedicated namespace, an explicit kubeconfig context and immutable registry images. A cluster administrator must supply or approve:

| Prerequisite | Required behavior |
|---|---|
| Kubernetes and CNI | Core uses Kubernetes 1.29+ APIs; pinned Apache Spark operator separately requires 1.34+. Enforce NetworkPolicy and restricted Pod Security. Validate node architecture, capacity and DNS selectors. |
| Registry | Build/publish reviewed source, inspect digests, scan/sign per your policy, supply pull credentials through existing Secrets. Example repository names are placeholders. |
| PostgreSQL | Dedicated database; independently authenticated migration owner and restricted runtime role; verified server TLS; backup/restore procedure. The application requires its migrated schema. |
| OIDC | Public browser client using Authorization Code + PKCE, separate API access-token audience, trusted role/workspace claims, HTTPS issuer and callback. |
| Ingress/DNS | Existing controller, reviewed network peer model, DNS and trusted HTTPS certificate. The guided profile uses a Kubernetes TLS Secret. |
| Secret delivery | Existing named Kubernetes Secrets supplied by your approved secret manager or secure administrative workflow, never inline Helm values. |
| Operations | Metrics/log access controls, restore drill, rotation, resource/cost limits, tenant-denial and failure tests. |

The setup helper never installs or adopts these dependencies. Use an existing compatible service where appropriate. Optional new operators/services have separate pinned profiles and explicit ownership review. Do not install a second Spark/CNPG operator over another team's CRDs, patch Helm ownership labels, or relabel an existing namespace to bypass preflight.

### Build the core images

The Dockerfiles pin upstream bases; the product images are built from the source revision you choose. With an authenticated registry and a Docker Buildx builder capable of your target node architecture:

```sh
export DATASLATE_REGISTRY=registry.example.com/your-team
export DATASLATE_RELEASE=your-reviewed-source-revision
docker buildx build --platform linux/amd64 --push \
  -t "$DATASLATE_REGISTRY/api:$DATASLATE_RELEASE" api
docker buildx build --platform linux/amd64 --push \
  -t "$DATASLATE_REGISTRY/web:$DATASLATE_RELEASE" web
docker buildx imagetools inspect "$DATASLATE_REGISTRY/api:$DATASLATE_RELEASE"
docker buildx imagetools inspect "$DATASLATE_REGISTRY/web:$DATASLATE_RELEASE"
```

Use `linux/arm64` for ARM nodes, or separately qualify/build the required multi-platform index. Record the published registry digest and put `REGISTRY/IMAGE@sha256:DIGEST` into the setup profile. A tag and a local image ID are not a registry manifest digest. Building API/web does not build optional controller or Spark runtime images; each enabled module has its own pinned profile.

### Configure PostgreSQL and Secrets

Use PostgreSQL 17 according to the current tested profile. An administrator must create a dedicated database owned by the migration role and a **distinct LOGIN runtime role** without SUPERUSER, CREATEDB, CREATEROLE, REPLICATION, BYPASSRLS, role memberships or database/schema/table ownership. Keep both principals' credential lifecycles separate. The migration role must be able to create/migrate all DataSlate public-schema objects; do not share this database with unrelated public tables.

Do not paste passwords into commands, Git or Helm files. Use your database administrator's approved role/credential workflow. If using interactive `psql`, `\password ROLE` avoids writing a password in SQL text; ensure session/audit policy also protects it. Configure server TLS, network restrictions, backups and a tested recovery procedure before considering the installation production ready.

Supply existing Secrets in the DataSlate namespace:

| Example Secret/key | Content and consumer |
|---|---|
| `dataslate-runtime-database` / `database-url` | Verified-TLS URL for the restricted runtime role; API and enabled trusted control-plane services |
| `dataslate-migration-database` / `database-url` | Verified-TLS URL for the owner/migration role; migration Job only |
| `dataslate-database-ca` / `ca.crt` | PEM CA bundle verifying PostgreSQL, mounted as `/etc/dataslate/postgres/ca.crt` |
| `dataslate-ingress-tls` / `tls.crt`, `tls.key` | Certificate/key consumed by the existing ingress controller |
| `dataslate-registry` / `.dockerconfigjson` | Registry pull authentication, when required |
| Optional `identity-ca` / `ca.crt` | Private IdP CA, configured with `oidc.caSecret`; API discovery/JWKS trust only |

Both database URLs must select the same dedicated database using their intended principals, a hostname matching the certificate, and `sslmode=verify-full`. The chart supplies `PGSSLMODE=verify-full` and `PGSSLROOTCERT`; do not weaken these in a URL. Distinct Secret names do **not** prove distinct identities: independently authenticate each URL and inspect `current_user`, `current_database()` and TLS. The preflight intentionally reads names/keys rather than values and cannot prove this.

Prepare a private libpq service named `dataslate-migration` for later administrative SQL. Set database/host/user/port, `sslmode=verify-full`, `sslrootcert` and an owner-only password file through `PGSERVICEFILE`/`PGPASSFILE` or your approved credential mechanism. Keep files outside the repository with mode `0600`. The runbook uses `PGSERVICE=dataslate-migration psql`; it never puts a credential URL on the command line.

The managed CNPG example defaults to its generated shared bootstrap-owner credential. It remains an explicit compatibility profile with separate HA/restore gates, not this hardened setup. If CNPG supplies the database, provision distinct roles and use their external-profile Secret references; managing the database resource and managing application privileges are separate tasks.

### Configure OIDC and initial authorization

Create a public client such as `dataslate-web` with Authorization Code + PKCE S256, redirect URI `https://YOUR_HOST/auth/callback`, and only the required origin. Do not add a browser client secret. Configure access tokens for a distinct audience such as `dataslate-api`; an ID token intended for the browser must not authorize API requests. Include `openid` and any provider-specific scopes required to obtain that API access token.

Only trusted IdP administration may assign these **access-token** claims:

```json
{
  "dataslate_roles": ["platform-admin"],
  "dataslate_workspaces": {"WORKSPACE_ID": "admin"}
}
```

Grant `platform-admin` to a designated bootstrap operator, not all authenticated users. It permits creating a workspace, but does not itself confer access to every workspace. Sign in, create the workspace, record its returned ID, then have the IdP administrator assign that exact ID with `admin`, `editor` or `viewer` membership. Obtain a fresh access token by signing out/in after updating claims. Ordinary team users receive explicit workspace membership; do not let users self-edit trusted claims. Test one permitted identity and a different denied identity across workspaces.

For a private IdP CA, set `oidc.caSecret: {"name":"identity-ca","key":"ca.crt"}` in the helper profile. This trust is scoped to the API's OIDC discovery/JWKS client; it does not change database trust, browser trust, ingress certificates or other gateway clients. Browsers must separately trust the IdP and workspace HTTPS chains. Configure issuer/JWKS destination egress correctly; login may fail closed during provider/network outages.

## Generate and review the installation

Prepare the pinned deployment tools:

```sh
python3 -m venv deploy/.cache/venv
deploy/.cache/venv/bin/python -m pip install -r deploy/tests/requirements.txt
python3 deploy/scripts/fetch-helm.py
HELM_BIN="$PWD/deploy/.cache/helm" PYTHON_BIN="$PWD/deploy/.cache/venv/bin/python" deploy/scripts/check.sh
```

The fetch helper verifies the locked Helm archive and supports its documented platforms. For other platforms use your independently verified Helm executable. No cluster is contacted by these checks.

Copy and edit the **non-secret** profile. The parent output directory must already exist, and the new review directory must not exist:

```sh
cp deploy/setup/profile.example.json /secure/config/dataslate-profile.json
# Edit every example context, endpoint, CIDR, image digest, identity and Secret name.
deploy/.cache/venv/bin/python deploy/setup/generate.py \
  --config /secure/config/dataslate-profile.json \
  --output /secure/config/dataslate-review \
  --helm "$PWD/deploy/.cache/helm"
```

Review `values.yaml`, `rendered-core.yaml`, `namespace.yaml`, `prerequisites.json` and `RUNBOOK.md`. The helper validates and renders; it neither executes the runbook nor verifies the cluster. See [its exact boundary and tests](setup/README.md). Input is limited to the supported profile fields; arbitrary Helm flags and automatic dependency installation are intentionally absent.

The profile selects an existing TLS-Secret ingress controller with exact controller namespace/pod labels. AWS ALB terminates TLS externally and has a different network-peer model: use [the ALB example](examples/eks-external.yaml) as a **separately reviewed overlay**, retaining the helper's role-separated credentials, images and scoped egress. Set correct ACM annotations, `ingress.tls.externallyTerminated`, and ALB subnet peers/security groups; do not retain in-cluster controller selectors for an external ALB. Standard NetworkPolicy cannot allowlist DNS names; account for actual IdP/JWKS address ranges or a controlled egress gateway. NodeLocal DNS and dual-stack networks also require explicit values review. Re-render and preflight the complete final overlay set.

## Install the reviewed core

Follow the generated `RUNBOOK.md`; its commands contain your explicit context, namespace, release and output path. For a new namespace, use `kubectl --context YOUR_CONTEXT create -f namespace.yaml` after confirming it is absent. If it exists, stop and review ownership/security labels; do not delete, adopt or patch it automatically. Provision the named Secrets securely in this namespace before preflight.

The read-only preflight inspects actual server version, existing resources/ownership, Secret keys, and relevant capabilities:

```sh
deploy/.cache/venv/bin/python deploy/preflight/check.py \
  --context YOUR_CONTEXT --namespace dataslate --release dataslate \
  --values /secure/config/dataslate-review/values.yaml \
  --helm "$PWD/deploy/.cache/helm" --format json \
  > /secure/config/dataslate-review/preflight.json
```

If using overlays, repeat `--values` in the **same order** as the intended Helm operation. Read the report: exit 1 indicates blocked prerequisites, exit 2 invalid input/response, and exit 3 unresolved qualification/capabilities. Even observed prerequisites are not a connectivity, CNI, TLS, identity, performance or cloud qualification. Do not suppress unknowns to obtain a green status.

For a genuinely new release, the bootstrap ordering is deliberate:

```sh
# Do not add --wait yet: application grants follow the migration Job.
deploy/.cache/helm install dataslate deploy/helm/dataslate \
  --kube-context YOUR_CONTEXT --namespace dataslate \
  --values /secure/config/dataslate-review/values.yaml --timeout 10m
kubectl --context YOUR_CONTEXT --namespace dataslate \
  wait --for=condition=complete job/dataslate-migrate-1 --timeout=10m
PGSERVICE=dataslate-migration psql -X -v ON_ERROR_STOP=1 \
  -v database=dataslate -v migration_role=dataslate_migrator \
  -v runtime_role=dataslate_runtime -f deploy/setup/runtime-role.sql
kubectl --context YOUR_CONTEXT --namespace dataslate \
  rollout status deployment/dataslate-api --timeout=10m
kubectl --context YOUR_CONTEXT --namespace dataslate \
  rollout status deployment/dataslate-web --timeout=10m
deploy/.cache/venv/bin/python deploy/scripts/verify-install.py --url https://YOUR_HOST
```

Use the names in your generated runbook, not these example names. For custom overlay files, pass them to `helm install` in exactly the reviewed/preflight order. The initial migration Job uses release revision 1. It is a normal release resource, not a hook. Migration failure leaves the API unready; diagnose the database, image, trusted CA, network and schema rather than simply extending timeouts. Increasing privileges on the runtime principal is not a migration fix.

The grants procedure checks the actual migration/runtime role boundary, grants existing application-table CRUD and sequence usage, and keeps the migration ledger read-only. It is transactional and rejects unsafe ownership/membership/effective grants. Review [its assumptions](setup/README.md#database-role-boundary) before applying it. Never give the runtime credential to notebook authors.

The verifier uses trusted HTTPS and no credentials. For private workspace certificates, add `--ca-file /secure/config/ingress-ca.pem` to the verifier; never use an insecure bypass. This CA verifies the workspace endpoint and is separate from `oidc.caSecret`. After smoke checks, complete real PKCE login and workspace-role tests, refresh tokens after IdP claim changes, and test denied cross-workspace access. Anonymous health success alone is not acceptance.

## Enable engines and the lake incrementally

A green core does not mean optional engines are installed. Each layer needs its exact runtime, credentials, bindings and execution evidence:

| Capability | Installation and security boundary |
|---|---|
| Spark applications / Python notebook batches | [Execution guide](README.md#opt-in-batch-execution), separately reviewed [operator bootstrap](bootstrap/README.md), dedicated compute namespace, quotas, admission rules, pinned images and scoped cloud identity |
| Files → Delta | [Ingestion runtime](../runtime/ingestion/README.md), exact connector/format support, immutable source/target bindings, checkpoint/permission and cloud write qualification |
| SQL + charts | [Trino profile](sql/README.md), engine TLS/authentication, distinct governed subject identity, row filters/masks and exact catalog connector configuration |
| Iceberg metadata | [Polaris profile](polaris/README.md), separate catalog/service principals and scoped object-store identity; metadata attachment does not prove table data access |
| SQL publication | [Publication contract](../docs/implementation/publication-contract.md), pinned source/table identity, reviewed target metadata and end-to-end table/read policy checks |
| Semantic search | [Search profile](search/README.md), explicit pgvector extension, exact indexing scope, permission checks and freshness/relevance qualification |
| Sylon assistant / memory | [Sylon contract](../docs/implementation/sylon-contract.md) and [memory contract](../docs/implementation/sylon-memory-contract.md), supported gateway/token/OAuth contract, scoped credentials and private-memory controls |
| Airflow workflows | [Optional Airflow profile](airflow/README.md), reviewed Git DAG bundles, separate reader/broker/task credentials and mandatory token renewal; initial managed profile uses LocalExecutor with trusted DAG authors |

If your organization already owns a compatible backend, use its documented external integration path; do not deploy a duplicate automatically. If opting into a managed profile, review its upstream locks, database/secret/network/storage assumptions and supported security combinations. Render all final values, rerun preflight, and perform the documented live tests. These interfaces do not imply every upstream package, connector, interactive notebook kernel, AI provider or Databricks feature is supported.

## Upgrades, recovery and acceptance

Back up first and prove restore into an isolated environment. Review the new source, API compatibility, schema changes, pinned images and complete chart values. Verify release ownership and use **explicit `helm upgrade`** with the same context/namespace; do not use `upgrade --install` to bypass initial ownership review. Read the resulting release revision, wait for `RELEASE-migrate-REVISION`, rerun `runtime-role.sql` for newly created tables, then check deployment readiness and application behavior. Reusing `-migrate-1` waits for the wrong Job. A Helm rollback does not undo database changes; never use `--atomic` as a database recovery plan.

Before production acceptance, demonstrate restore, failure recovery, secret/certificate/JWT rotation, actual CNI denials, two-user authorization, cluster capacity and cost limits, and the complete ingestion → Spark → governed SQL/chart flow on your cloud. Local component or local Kubernetes evidence is recorded separately from EKS/S3 qualification. Retained CNPG resources/PVCs are not backups; cloud storage and database deletion/adoption need explicit operator decisions.
