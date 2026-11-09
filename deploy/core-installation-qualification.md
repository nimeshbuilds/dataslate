# Core Kubernetes installation qualification

November 8, 2026. This is a **local Linux ARM64 core-installation result**, not an EKS/S3 acceptance, high-availability certificate or complete-platform installation. Use the [development and production quickstart](quickstart.md) to configure a deployment. [Machine-readable evidence](core-installation-qualification.json) records the exact image/context hashes, checks and cleanup.

## What actually ran

A new, explicitly owned Colima VM ran k3s `v1.35.0+k3s1` with 3 CPUs and 5 GiB memory. The final setup helper rendered the real core chart, which Helm 4.3.0 installed into a restricted namespace. Two API replicas and two web replicas used the production Dockerfiles, immutable local image digests, read-only roots, dropped capabilities and no automatically mounted Kubernetes tokens.

The dependencies were real processes in the same disposable cluster:

| Dependency | Qualified configuration |
|---|---|
| PostgreSQL | 17.11, a persistent volume, verified TLS, dedicated database, distinct migration owner and restricted runtime principal |
| Identity provider | Keycloak 26.8.2 with PostgreSQL persistence, HTTPS, a public browser client and Authorization Code + S256 PKCE; separate API audience |
| Ingress | Traefik 3.5.1 and an actual Kubernetes Ingress/TLS Secret; standard ingress resources, no application auth proxy |
| Network boundary | k3s NetworkPolicy enforcement; exact ingress pod selectors and reviewed DNS/database/IdP destinations |

These are fixture dependencies, not newly supported managed Keycloak or Traefik product profiles. Keycloak's first-start build used a writable fixture root; DataSlate's application roots remained read-only. All resources stayed in the owned VM. No cloud, customer data, registry publishing or paid model API was used.

## Observed checks

* The offline generator produced the documented five files. Preflight identified a wrong ingress-class name before installation. After correction it observed 21 prerequisites, with zero blockers and two explicit unresolved live-check categories. The report correctly continued to say `qualification: not-performed`; read-only inventory does not certify the later tests.
* Initial installation completed migration Job `dataslate-migrate-1`. The real migration identity ran `runtime-role.sql`, then both application deployments became ready. A reviewed web-image update exercised Helm revision 2, migration Job `dataslate-migrate-2`, repeated grants and the web rollout. This is not a previous-release schema-upgrade or rollback drill.
* **18 database checks** passed: real runtime TLS/identity, application CRUD, migration-ledger reading, denied public/schema DDL, denied ledger insert/update/delete, repeatable migration-owned grants, missing-input failures, wrong-operator rejection, rejected `NOINHERIT` role membership, rejected runtime schema ownership, and restored permissions after the negative fixtures.
* **Five anonymous HTTPS checks** passed through ingress: web health, API health/readiness, production auth metadata and HTTP 401 for protected status. The verifier used the fixture CA and required hostname validation.
* Actual pod inspection confirmed that API pods receive the runtime Secret and OIDC CA, while the migration Job receives the migration Secret. Runtime deployments do not receive the owner credential. An approved web pod reached API readiness; an unlabelled pod resolved the same service address and was denied the connection.
* **54 browser checks** passed using real Keycloak forms and the real application/API: four user roles, correct PKCE verifier/challenge, API access-token claims, API rejection of ID tokens, workspace filtering, platform-administrator creation, a creation receipt without implicit membership, notebook saving/readback, viewer write denial and cross-workspace denial. Tokens stayed in memory; no storage-state files, traces or screenshots were recorded.
* **51 post-restart browser checks** passed after PostgreSQL restarted on its existing PVC and both API pods were replaced. Fresh logins reopened the exact saved notebook with its original source and revision. The first login failed while Keycloak recovered a closed database connection; the retained retry passed. This proves the stated persistence and recovery, not uninterrupted availability or backup/restore.

The browser fixture explicitly trusts only the generated server certificate's SPKI, after OpenSSL verifies its CA chain and both hostnames. It resolves the two `.test` names inside the isolated Chrome process; it does not alter the host's DNS or trusted CA store. The browser uses loopback forwards on 18443/18444; the separate verifier reached the real ingress service on 443 from inside the owned VM. No browser/API request interception was used.

## Bugs found by the live journey

1. PostgreSQL 17 ignores an argument to `\quit`. The initial SQL guard correctly rolled back but returned success. Guards now raise a constant SQL exception under `ON_ERROR_STOP`, returning exit code 3. Actual PostgreSQL regression tests also run in CI.
2. The web proxy forwarded `$host`, which removed a non-default public port. Same-origin writes through the forwarded HTTPS endpoint were consequently rejected. It now preserves `$http_host`; the API's origin check remains enforced. The rebuilt production web image passed the same real creation/save journey.
3. Production identity claims need not appear in the ID token. Workspace creation visibility now uses the API's permission derived from the verified access token, rather than inspecting ID-token roles. Creating a workspace returns its identifier and the required membership setup instructions; it does not select a workspace the user cannot yet access.

## Reproduction boundaries

The reusable fixture tools are [qualify-core-fixtures.py](../scripts/qualify-core-fixtures.py), [qualify-core-database.py](../scripts/qualify-core-database.py) and [qualify-core-browser.mjs](../scripts/qualify-core-browser.mjs). They are qualification tools, not a production installer.

1. Create a separate disposable cluster and explicit kubeconfig/context. Do not reuse a customer namespace, database, identity provider or working release. The database checker deliberately mutates and restores two unsafe fixture-role configurations; it only accepts the named `colima-dataslate-core-qualification` context.
2. Generate fixtures into a new owner-only directory with `python3 scripts/qualify-core-fixtures.py --output /private/new-fixture`. That directory contains credentials and private keys. Never commit it, print its manifests or include it in diagnostic artifacts.
3. Install the generated fixture resources in that owned cluster. Configure cluster DNS for the private IdP service, a TLS ingress, and loopback-only browser port forwards. Select exact controller labels, service/pod destinations and ports for that cluster's CNI; do not copy ephemeral IPs from this run into another installation.
4. Build the real API/web Dockerfiles for the node architecture. Import them into the fixture runtime or use an explicitly chosen registry, record their immutable digests, and create a reviewed non-secret profile with the [setup helper](setup/README.md). No public DataSlate image availability is implied by this local build.
5. Follow the generated staged installation: preflight, explicit new Helm release, migration Job, migration-owned runtime grants, deployment readiness and HTTPS verification. Use the two different credential Secrets and the private OIDC CA option. For this fixture only, seed `team-alpha` and `team-beta` workspaces to match the generated test-user claims; production workspace creation and membership mapping are documented in the quickstart.
6. Run the database checker with explicit `--kubeconfig`, `--context`, `--private-dir`, `--kubectl` and a new `--output` file. Run the browser checker after `cd web && npm ci`, from the repository root with `node scripts/qualify-core-browser.mjs --private-dir /private/new-fixture --mode seed --output /private/browser-before.json`. It uses the repository's pinned Playwright dependency; `--chrome` can select an installed Chromium executable.
7. For a persistence check, restart the fixture PostgreSQL process and replace both API pods, wait for readiness, then run browser `--mode read --previous /private/browser-before.json --output /private/browser-after.json`. Preserve failures as well as subsequent successes. Read mode authenticates again and opens the exact previous notebook revision; it does not silently create replacement data.
8. Review only the credential-free results for publication. Stop the owned port forwards, delete the owned VM/data and remove fixture credentials/private keys/kubeconfig. Preserve unrelated local clusters, contexts and developer tools.

The [full acceptance plan](../docs/implementation/delivery-plan.md) still requires cloud identity/lake integration, all enabled backend controllers together, backup/restore into a clean environment, previous-release upgrades, sustained recovery/capacity, and the broader product modules. Two replicas on one local node do not establish availability. The four browser identities establish the stated workspace checks, not every notebook/kernel/storage/agent escape path.
