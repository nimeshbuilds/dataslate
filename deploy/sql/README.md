# Governed SQL with Trino

DataSlate's SQL controller submits saved SQL cells to a pinned Trino 484 service, records immutable source and results, and reconciles cancellation and failures. The transport adapter also works with an externally managed HTTPS Trino endpoint that meets the same authentication and policy contract. See [the SQL contract](../../docs/implementation/sql-contract.md) and [core Helm example](../examples/eks-sql.yaml).

The platform maps each explicitly granted issuer/subject pair to a distinct engine user. A mounted, rotating JWT authenticates the controller's broker account. Trino must permit that broker to impersonate only approved users and must enforce read-only access, exact table grants, row filters and column masks itself. Browser-supplied usernames are never used. Trino policies are an additional boundary to DataSlate workspace and result authorization. This profile does not grant arbitrary Python/S3 access to governed SQL users.

`policy.py` compiles trusted administrator policy into Trino file rules. It denies writes, schema ownership, system metadata, procedures, external functions and session-property changes. Row-filter and mask expressions are trusted administrator SQL, not user input. Review expressions and qualify actual connector behavior before granting a table. Change the platform binding's `policyVersion` before or together with a restrictive engine policy change: this revokes access to cached historical results. DataSlate cannot detect an out-of-band engine ACL edit automatically.

## Managed or existing service

`render.py` verifies the official chart's checksum and makes checked adaptations for stable per-Pod DNS and verified internal TLS. It renders namespaced resources only and never applies them. `upstream-lock.json` pins the chart and immutable image. No Spark operator is needed for this SQL service.

For a managed installation, provision these resources explicitly:

- An existing dedicated ServiceAccount with approved workload identity, narrow storage permissions and no Kubernetes API privileges. Instance metadata credentials are disabled. The profile does not silently create IAM roles.
- `tlsSecret` with `tls.pem` (PEM private key followed by certificate chain) and `ca.crt`. Certificates must cover `<name>.<namespace>.svc` and `*.<name>-nodes.<namespace>.svc`; issue through the organization's cluster CA. The same certificate is used for verified internal TLS. Rotation needs a controlled rollout.
- `authSecret` with `jwt-key.pem` (broker verification public key) and `internal-shared-secret` (random shared cluster credential). Configure the expected JWT issuer/audience; mount the corresponding short-lived broker token only into the DataSlate SQL controller.
- `catalogSecret` containing only reviewed `<catalog>.properties` connector files. Configure connector storage/catalog identity and TLS explicitly. Production examples do not install demonstration connectors or imply Delta/Iceberg query interoperability has been qualified.
- A reserved Service ClusterIP, exact SQL-controller namespace/Pod labels and bounded destination CIDRs/ports for catalog, storage and identity traffic. NetworkPolicy enforcement depends on the cluster CNI and Service DNAT behavior; verify on the target cluster. Stable Pod DNS is used for internal discovery.

Edit the reference-only `profile.example.json`, then:

```sh
python3 deploy/sql/fetch.py
python3 deploy/sql/render.py --config /operator/path/sql-profile.json \
  --helm /absolute/path/helm --chart deploy/.cache/trino.tgz > /operator/path/sql.yaml
```

Review and apply the output using an explicit cluster context. The renderer converts coordinator/worker Deployments into StatefulSets so internal HTTPS has stable DNS. Secret mounts are read-only, root filesystems are read-only, containers run as UID 1000 with no capabilities or API token, and temporary/data directories have size limits. Worker count is explicit; zero workers places execution on the coordinator. The engine enforces a one-hour maximum, and the controller can request an earlier deadline. Horizontal autoscaling, spill tuning, failover, upgrades and production recovery require separate qualification.

For an existing service, configure the API's SQL binding and the independent controller with the endpoint, CA and token references. The external owner remains responsible for read-only policy, impersonation, cancellation, connector permissions and the hard runtime ceiling. Do not deploy this profile over an existing release or treat a successful endpoint connection as proof of governance.

## Reproduce the local fixture

The optional helper creates credentials in a new private directory and renders the same production profile with a TPCH-only connector. It does not contact Kubernetes. It is restricted to the `dataslate-sql-qualification` namespace. Use only an owned disposable cluster with the example Service CIDR, or adapt the fixture explicitly for that cluster. The broker JWT expires after two hours. Never commit the fixture directory.

```sh
python3 deploy/sql/prepare_qualification.py --directory /private/new-sql-fixture \
  --helm /absolute/path/helm --chart deploy/.cache/trino.tgz
kubectl --kubeconfig /owned/kubeconfig --context owned-local apply -f /private/new-sql-fixture/prerequisites.yaml
kubectl --kubeconfig /owned/kubeconfig --context owned-local apply -f /private/new-sql-fixture/server.yaml
kubectl --kubeconfig /owned/kubeconfig --context owned-local -n dataslate-sql-qualification rollout status statefulset/dataslate-trino-coordinator
kubectl --kubeconfig /owned/kubeconfig --context owned-local -n dataslate-sql-qualification rollout status statefulset/dataslate-trino-worker
kubectl --kubeconfig /owned/kubeconfig --context owned-local -n dataslate-sql-qualification port-forward service/dataslate-trino 18443:8443 --address 127.0.0.1
```

In a second terminal, run the actual production transport:

```sh
cd api
DATASLATE_TEST_TRINO_ENDPOINT=https://localhost:18443 \
DATASLATE_TEST_TRINO_CA_FILE=/private/new-sql-fixture/ca.crt \
DATASLATE_TEST_TRINO_TOKEN_FILE=/private/new-sql-fixture/token \
go test -race ./internal/trino -run TestLive -count=1 -v
```

The fixture expects five rows after filtering, masked names, exact signed 64-bit/decimal values, explicit terminal state, denied mutation/metadata/impersonation and cancellation. Production port-forwarding is not an authentication architecture; this tunnel is for the owned local test only. Delete the owned namespace and private directory when finished, after confirming their identity. That cleanup deletes disposable fixture data.

Automated profile checks use PyYAML and the verified Helm/chart files:

```sh
HELM_BIN=/absolute/path/helm python3 -m unittest discover -s deploy/sql -p 'test_*.py' -v
```

Official contracts: [Trino client protocol](https://trino.io/docs/current/develop/client-protocol.html), [JWT authentication](https://trino.io/docs/current/security/jwt.html), [internal TLS](https://trino.io/docs/current/security/internal-communication.html), [file access control](https://trino.io/docs/current/security/file-system-access-control.html). Local tests establish a bounded profile, not EKS/S3, connector-wide, HA or performance qualification.
