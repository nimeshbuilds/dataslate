# Optional Airflow deployment profile

This opt-in renderer integrates Apache Airflow **3.4.0**, upstream Helm chart **1.22.0**, FAB authentication and the local DataSlate task provider. It produces a reviewable manifest bundle; it does **not** install, discover, adopt, replace or upgrade existing services. An existing Airflow 3.4.0 service can continue to use the [workflow integration](../../docs/implementation/workflow-contract.md) without this profile.

The supported executor here is **LocalExecutor with one scheduler**, bounded to 1–8 simultaneous orchestration tasks. Airflow is hosted on Kubernetes; approved Spark work still goes through DataSlate's separate compute controller. KubernetesExecutor, arbitrary uploaded DAGs and hostile tenant task isolation are not supported by this profile. DAG authors and operators must be trusted: Python DAG imports execute code, and LocalExecutor task processes share the scheduler's metadata database/signing credentials and task connection. DataSlate workspace authorization does not turn those processes into separate security domains.

## Pinned components and installation prerequisites

| Component | Profile contract |
| --- | --- |
| Airflow / Python | 3.4.0 / 3.12; base image OCI digest in `upstream-lock.json` |
| Official chart | 1.22.0; verified archive SHA-256; override its default Airflow version |
| FAB / Git providers | 3.10.2 / 1.0.1; exact upstream constraints copied with checksum |
| DataSlate SDK / provider | Local source packages, both 0.1.0a1; installed/discovered during image build |
| Kubernetes / Helm | Upstream chart requires Kubernetes 1.30.13+ and Helm 3.19.0+; render tests use Helm 4.3.0 |
| Metadata | Dedicated external PostgreSQL database and role; verified TLS, backups and restore procedure owned by operator |
| Logs | Existing shared **ReadWriteMany PVC** accessible to the API, scheduler and DAG processor; storage provisioning, capacity/retention and backups are external |
| Identity | Existing unprivileged ServiceAccount, no Kubernetes API permissions or auto-mounted token; reviewed FAB user/role provisioning required |
| Network | CNI must enforce NetworkPolicy; CoreDNS labels must match the generated DNS rule; explicit CIDRs for PostgreSQL, Git, DataSlate and token endpoints |
| PKI | Existing API certificate/key plus trust bundle; certificate SAN must include `<name>-api-server.<namespace>.svc` |

The generated resources are three Deployments (single-worker API, DAG processor, single scheduler), one bounded migration Job, two ConfigMaps, one HTTPS API Service and two NetworkPolicies. Every container has resource requests/limits, runs as UID 50000 without privilege escalation/capabilities, and has a read-only root filesystem. Writable temporary/bundle directories are size-bounded; logs use the existing PVC. Migration waits are bounded. The scheduler uses `Recreate` to avoid overlapping LocalExecutor schedulers during rollout.

Embedded PostgreSQL/Redis, Celery, triggerer, git-sync, Kubernetes launch RBAC, default account creation and Helm migration hooks are disabled. The chart's unused default broker Secret and create-user ServiceAccount are suppressed. The HTTP task-log Service is removed; API log access uses shared storage. API ingress is limited to the declared DataSlate namespace/pod selector and this Airflow installation. Kubelet HTTPS probes check availability but do not themselves establish certificate trust; the actual execution client verifies the configured CA and hostname.

## Build the runtime image

Use Python 3.12 and a container builder. `build_context.py` copies only the package `.py` sources, packaging metadata and pinned image files, and validates the constraints checksum. It refuses an existing destination. Do not send the repository root, `.qa` fixtures, `.env` files or credentials to a builder.

```sh
python3 deploy/airflow/build_context.py --output /tmp/dataslate-airflow-build
docker build --platform linux/arm64 \
  -f /tmp/dataslate-airflow-build/deploy/airflow/Dockerfile \
  -t your-registry/dataslate-airflow:reviewed /tmp/dataslate-airflow-build
```

Use the platform your cluster actually runs. The image build verifies Airflow, Git, FAB, psycopg and DataSlate package versions, imports and provider discovery. Publish through your registry process, scan/sign it, resolve its resulting immutable digest and set `image` to `registry/path@sha256:...`. The example digest is a placeholder, not a published DataSlate image. The qualified build currently covers Linux arm64; an amd64 deployment still needs its own build/runtime qualification. Changing provider/runtime versions requires a new lock and integrated tests.

The image's startup wrapper rejects missing signing material and database URI/driver/TLS mismatches before invoking the upstream Airflow entrypoint. It rejects alternate database URI and connect/engine-argument environment overrides, including command/secret variants, and prints only a fixed failure category. The image is an operator-built dependency, not an attestation system: use the reviewed recipe and enforce the resulting image digest through your deployment policy.

## External Secret contract

Secret names must be distinct. No Secret values belong in the profile, ConfigMaps, DAG source, bindings, Git or build context.

| Profile field | Required Secret keys | Consumers |
| --- | --- | --- |
| `databaseSecret` | `connection` | Airflow metadata processes/migration waits and migration Job |
| `fernetSecret` | `fernet-key` | Airflow metadata processes; valid single 32-byte URL-safe base64 Fernet key |
| `apiSecret` | `api-secret-key` | All metadata processes and migration containers through the upstream chart; API session signing material, at least 32 independently random characters |
| `jwtSecret` | `jwt-secret` | API and scheduler; at least 32 independently random characters |
| `trustSecret` | `ca.crt` | Public CA bundle for PostgreSQL, Airflow execution API, Git and task integration |
| `tlsSecret` | `tls.crt`, `tls.key` | API main container only; private key is absent from migration-wait mounts |
| `gitConnectionSecret` | `connection` | Scheduler and DAG processor, as `AIRFLOW_CONN_DATASLATE_GIT` |
| `taskConnectionSecret` | `connection` | Scheduler/task processes only, as `AIRFLOW_CONN_DATASLATE_DEFAULT` |

The metadata connection value must use exactly this structure (URL-encode the actual username/password):

```text
postgresql+psycopg://USER:PASSWORD@DATABASE_DNS:5432/airflow?sslmode=verify-full&sslrootcert=/etc/airflow-trust/ca.crt
```

Additional query parameters, weaker SSL modes, alternate CA paths and other drivers are rejected. Airflow derives its async connection with the installed psycopg driver; the same verified TLS parameters apply to both engines. Provision the role/database independently and grant only its own metadata database permissions. The migration Job changes that database schema when the manifest is intentionally deployed; take backups and review upgrades first.

The Git connection is an Airflow JSON connection with type `git`, the reviewed HTTPS repository host and operator-managed credentials. Use ID `dataslate_git`; the renderer pins a full immutable commit and relative DAG directory, disables submodules, and uses a versioned `GitDagBundle`. Only that reviewed bundle is configured. Changing the commit is a reviewed rollout and requires updating corresponding DataSlate binding version/policy after Airflow has parsed it. New DAGs start paused; enable the approved schedule through your Airflow administration process after binding/permission review.

The task connection is the [DataSlate provider connection](../../integrations/airflow/README.md), ID `dataslate_default`. Use its OAuth client-credentials flow or a separately rotated DataSlate bearer token. For private CAs, set its `extra.ca_file` to `/etc/airflow-trust/ca.crt`; include the relevant roots in the trust bundle. Its service principal needs explicit DataSlate workspace and operation grants. It does not inherit the person who pressed Run's identity or row filters. Keep the endpoint, OAuth client/principal, workspace and grants stable across task retries so recovery can find the original owner-scoped request.

Kubernetes Secret environment variables are snapshots: changes to Git/task/database/session secrets require a coordinated pod restart. Long-lived tasks can fetch fresh OAuth access tokens from their configured client credentials, but replacing those client credentials still requires a restart. Do not rotate signing material blindly while runs are active. Fernet multi-key rotation is intentionally outside this first profile and needs a reviewed migration procedure.

## Airflow service identities and token rotation

FAB's database authentication is enabled, self-registration and proxy trust are disabled, and no user/password is generated. Its default login limiter is in-memory and resets on restart; production exposure requires an externally enforced durable rate limit or a separately qualified shared limiter. This profile keeps the API internal and runs one worker; it makes no HA/rate-limit persistence claim. An operator must provision distinct Airflow service accounts/roles using Airflow's administration tools. Grant the DataSlate API metadata reader only required DAG/version/run/task read permissions. Grant the workflow broker only those reads and create-run permission on explicitly approved DAGs. Do not reuse an Admin account. Validate the exact roles against Airflow 3.4.0 before exposing a binding; a default Viewer role can read more metadata than a scoped service account should receive.

The reader and broker **Airflow-audience** access tokens belong in the separate `workflow.readTokenSecret` and `workflow.triggerTokenSecret` references of the main DataSlate chart. They are not mounted into the Airflow pods and are distinct from the **DataSlate-audience task** credentials above. FAB login issues expiring JWTs; this profile sets a 15-minute lifetime and does not implement a renewal controller. **An external credential issuer/rotation service is a production prerequisite.** It must authenticate the two distinct service identities over verified HTTPS, renew before expiry, and update the referenced projected Secrets without `subPath` mounts. DataSlate rereads those token files for every request. A one-time pasted JWT is suitable only for a bounded test. Expired credentials must fail closed; do not lengthen token life to substitute for renewal.

The main DataSlate UI/API retain their own OIDC authentication. This profile does not automatically federate Airflow FAB login with the organization's OAuth provider, embed the Airflow UI, or create tenant-specific DAG identities. Those are separate integration/qualification work.

## Render and review

```sh
python3 deploy/airflow/fetch.py
# Copy profile.example.json to a private reviewed configuration and replace placeholders.
python3 deploy/airflow/render.py --config /path/to/airflow-profile.json \
  --helm /path/to/helm --chart deploy/airflow/.cache/airflow.tgz > /tmp/airflow.yaml
HELM_BIN=/path/to/helm python3 -m unittest discover -s deploy/airflow -p 'test_*.py' -v
```

PyYAML is required for rendering/tests. `AIRFLOW_CHART` optionally points tests at another copy of the same checksum-locked archive. Rendering is local, has bounded time/output, disables Helm plugins, invokes no dependency downloads or DNS lookup flag, and snapshots verified chart bytes before use. Unknown fields, unsafe image references, mutable Git branches, overlapping credential names and metadata/local network destinations (including covering supernets) fail closed. Raw Helm stderr and configuration exceptions are not echoed. The profile JSON is **not** an upstream Helm values file; the adapter also performs checked transformations and adds the isolation policies.

`make test-local` includes these profile tests and requires the fetched chart up front, so its native render cases cannot silently skip. It does not install Airflow into your general development environment. Run `make test-airflow PYTHON=/path/to/pinned-airflow-venv/bin/python` separately after installing the [provider's pinned environment](../../integrations/airflow/README.md); that target exercises provider/TLS/XCom behavior, while actual OCI image and live Kubernetes checks remain separate.

Before any installation, review name collisions and external ownership, create the namespace/ServiceAccount/Secrets/PVC through your existing infrastructure process, verify actual PVC access and certificate SANs, and verify CNI/DNS/egress behavior. Apply the reviewed bundle through your deployment process only after those prerequisites exist. The renderer neither checks current cluster health nor adopts an existing release. The fixed migration Job name is for an initial install; upgrade Job lifecycle, schema compatibility and rollback are not automated. Do not delete/recreate a preexisting Job or release merely to make an install pass.

## Qualification limits

On 2026-11-08, **23/23 profile tests passed**, including rendering the actual checksum-locked chart. The built Linux arm64 image passed provider/FAB/Git discovery, metadata migration with a read-only root filesystem, real synchronous and async PostgreSQL TLS checks, verified HTTPS API health, anonymous rejection, a FAB reader's metadata access and trigger denial, separate broker token issuance, weak-TLS startup rejection without secret output, and rejection of an untrusted database CA. The role test used disposable local service identities; it does not certify a customer's production roles or prove a broker-triggered DAG. The sanitized [native evidence](qualification-2026-11-08.json) records the local image digest and exact image-source hashes. Its image was not pushed to a registry. Test API/container/database/credentials were removed after qualification.

Automated tests cover actual checksum-pinned chart rendering, workload shape, external ownership, Secret placement, HTTPS configuration, immutable Git configuration, blocked unsafe overrides, bounded renderer subprocesses and the allowlisted image context. Native image tests are separate from template tests. Kubernetes installation, admission, NetworkPolicy enforcement, RWX filesystem behavior, scheduled provider-to-Spark execution, OIDC/FAB federation, JWT renewal, multi-node recovery, schema upgrades and backup/restore remain explicit live qualification gates. No cluster was changed by implementing this profile.

References: [upstream chart parameters](https://airflow.apache.org/docs/helm-chart/stable/parameters-ref.html), [production guide](https://airflow.apache.org/docs/helm-chart/stable/production-guide.html), [FAB auth manager](https://airflow.apache.org/docs/apache-airflow-providers-fab/stable/auth-manager/index.html), [Git DAG bundles](https://airflow.apache.org/docs/apache-airflow-providers-git/stable/bundles/git.html). `constraints-3.4.0-python3.12.txt` is the unmodified upstream Apache Airflow constraints file from the URL/checksum in `upstream-lock.json`; upstream Apache-2.0 terms apply.
