# Reviewable production setup

`generate.py` turns a **non-secret JSON profile** into five files for a new DataSlate core installation. It validates inputs and renders the actual repository Helm chart offline. It does not use kubeconfig, contact Kubernetes, install anything, adopt services, fetch dependencies, look up DNS, or inspect Secret values.

Read the complete [development and production quickstart](../quickstart.md) before installing. The generated profile enables API/web, OIDC, external PostgreSQL, ingress and network policies. Spark, SQL, Polaris, search, Sylon, publication and workflows remain explicit optional integrations.

```sh
# Repository root; use the pinned deployment environment and trusted Helm.
cp deploy/setup/profile.example.json /secure/config/dataslate-profile.json
# Edit this non-secret profile, replacing EVERY illustrative endpoint, image
# digest, context, network, identity and Secret reference.
deploy/.cache/venv/bin/python deploy/setup/generate.py \
  --config /secure/config/dataslate-profile.json \
  --output /secure/config/dataslate-review \
  --helm "$PWD/deploy/.cache/helm"
```

The output directory must not exist; its parent must exist. The helper creates it with mode `0700` and files with mode `0600`, refuses overwrites, and emits:

| File | Meaning |
|---|---|
| `values.yaml` | Strict core Helm values with immutable API/web images and separate migration/runtime Secret references |
| `rendered-core.yaml` | Actual offline Helm render for review; never applied by the helper |
| `namespace.yaml` | Restricted Pod Security labels for a **new** namespace; `kubectl create`, never automatic relabeling |
| `prerequisites.json` | Secret names and keys, identity assumptions, external infrastructure, and unresolved qualification gates |
| `RUNBOOK.md` | Context-pinned commands, migration → runtime grants → readiness ordering, explicit upgrade caveats |

The runbook preserves the supplied trusted Helm executable and the Python interpreter running the generator, with shell quoting for paths containing spaces. Keep that environment available, or regenerate the review bundle after moving tools; it does not substitute an unrelated system Python or Helm.

The example image digests and documentation IPs are **illustrative, not released images or reachable endpoints**. Passing validation proves shape and renderability, not existence, ownership, DNS, connectivity, database identities, TLS, CNI enforcement or production readiness. A Helm render is limited to 30 seconds and 4 MiB output. Errors do not echo Helm stderr or input values. Use a trusted Helm executable; this is not a sandbox for an untrusted executable or a modified chart.

The profile accepts only the fields in the example, plus optional `oidc.caSecret: {"name":"identity-ca","key":"ca.crt"}`. It requires unique scopes including `openid`, distinct API audience/browser client ID, distinct database role and credential Secret names, explicit ingress-controller/DNS pod selectors, and canonical scoped destination CIDRs. It rejects unknown fields, inline password fields, duplicate JSON keys, mutable images, unsafe names, HTTP/credential-bearing URLs, unrestricted egress, and CIDRs overlapping loopback/link-local/metadata destinations. DNS resolution and CNI routing still need live inspection. The helper cannot determine whether an arbitrary valid string is a secret; never supply secret values.

This bounded profile uses an ingress controller that consumes a TLS Secret and is reached through matching in-cluster Pod selectors. It does not claim this peer model fits every controller. External ALB peers, NodeLocal DNS, egress gateways and unusual networking require separately reviewed chart values and the live preflight against the **complete final values**, following [deployment reference](../README.md). The helper intentionally has no arbitrary `--set`, chart path, Helm plugin, dependency download or automatic optional-service adoption.

## Database role boundary

`runtime-role.sql` is run **as the migration role, after the migration Job completes**, against a dedicated database using the `public` schema. Roles and passwords must already have been provisioned through approved administration. Supply only non-secret identifiers using `psql -v database=... -v migration_role=... -v runtime_role=...`; use a private libpq service/password file for connection authentication.

The script:

- verifies the selected database and migration identity, migration ledger existence, and distinct roles;
- rejects runtime superuser/CREATEDB/CREATEROLE/REPLICATION/BYPASSRLS, database/schema/relation ownership, and membership in any other role (including `NOINHERIT` memberships that permit `SET ROLE`);
- revokes `CREATE` on `public` from `PUBLIC` and the runtime role and revokes runtime database `CREATE`;
- grants CONNECT, public schema USAGE, SELECT/INSERT/UPDATE/DELETE on existing public tables and USAGE/SELECT on existing sequences;
- retains only SELECT for `dataslate_schema_migrations`, checks effective privilege results, and commits transactionally.

The database must contain only DataSlate application objects: grants apply to **all public tables**, not a hand-maintained name list. The procedure does not create roles/databases, set passwords, change owners, configure RLS or promise to restrict malicious SQL in another schema. Runtime database credentials are for trusted DataSlate service code, never notebook authors. Database RLS is not the application workspace authorization mechanism. Unexpected inherited/PUBLIC ledger or CREATE permissions cause rejection and rollback. PostgreSQL ownership and membership can bypass ordinary object grants; see [PostgreSQL GRANT](https://www.postgresql.org/docs/17/sql-grant.html) and [privilege inquiry functions](https://www.postgresql.org/docs/17/functions-info.html).

No default privileges are granted. **Rerun the script after every schema migration**, so new application tables receive runtime access while the migration ledger remains protected. Rollback of a Helm release does not reverse schema migrations. Platform backups, point-in-time recovery and restore testing are required independently.

The optional managed CNPG example still bootstraps a shared owner credential by default. It is a compatibility profile, not this hardened role-separated setup. To use a CNPG database with this boundary, administer distinct principals explicitly and reference the verified runtime/migration URLs via the external-database profile; the core chart must not silently change an existing database's ownership.

## Tests

```sh
HELM_BIN="$PWD/deploy/.cache/helm" deploy/.cache/venv/bin/python \
  -m unittest discover -s deploy/setup -p 'test_*.py' -v
```

Tests include actual chart renders, migration-only credential projection, OIDC CA scoping, output permissions, unsafe inputs, bounded/sanitized Helm errors and bootstrap ordering. A missing Helm binary fails the render suite. These tests do not contact a cluster or prove the actual role grants; database and Kubernetes qualification are separate evidence.
