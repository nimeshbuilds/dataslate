# Production image qualification

`scripts/qualify-container-images.py` builds the API, execution controller, SQL controller, search controller, Sylon controller and web image using their checked-in digest-pinned Dockerfiles. It runs the resulting immutable image manifests in an explicitly owned Colima VM. It does not publish images or modify Kubernetes resources.

The startup check runs each process without root, writable root filesystem, Linux capabilities or privilege escalation. Four controllers connect to an isolated, already-migrated PostgreSQL database using verified TLS and a restricted runtime role. Empty bindings exercise readiness, idle reconciliation, restart and graceful shutdown. The execution controller discovers a disposable HTTPS Spark API protocol fixture; this check does not execute a Spark application.

The API uses explicit development authentication inside a private container network namespace shared with the web proxy. Only the web port is published, on VM loopback. Checks cover built assets, SPA routing, API proxying, response headers, callback query redaction and rejection of unsafe web upstream configuration. Separate checks require actual application rejection of missing production identity configuration, non-loopback development mode and insecure controller database connections.

## Run

Use an existing disposable Colima profile and the private PostgreSQL fixture prepared for `scripts/qualify-control-plane.py`. That fixture must use an isolated qualification database, verified TLS, a distinct runtime role, and current migrations/grants. The helper refuses optimized Python, so its assertions cannot be disabled accidentally.

```sh
python3 scripts/qualify-container-images.py --mode build \
  --profile dataslate-platform-qualification \
  --output-dir .qa/dataslate-final-oci-20261107

python3 scripts/qualify-container-images.py --mode check \
  --profile dataslate-platform-qualification \
  --output-dir .qa/dataslate-final-oci-20261107 \
  --fixture .qa/controller-postgres.json \
  --ssh-config "$HOME/.colima/_lima/colima-dataslate-platform-qualification/ssh.config"
```

The build phase can run first with `--component api`, then with `--component web` using the same output directory. Each component is snapshotted once; use a fresh output directory when its source changes. The check verifies source-context hashes and launches the exact manifest/config digests captured from BuildKit output, with pulls disabled. Mutable tags do not establish image identity.

Build logs, startup logs and credential-free summaries remain under the ignored output directory. A short-lived reverse SSH tunnel preserves the database's loopback TLS identity. Generated private configuration, environment files, key material, tunnel and test containers are removed after checks; diagnostic archives contain process logs only. The existing VM, PostgreSQL fixture, image artifacts and build cache remain owned by the caller for explicit cleanup.

The checked-in `container-qualification.json` records observed results and scope. These checks do not establish production OIDC login, job correctness, cloud storage or IAM integration, multi-node behavior, HA, or cross-architecture support. Those require separate qualification evidence.
