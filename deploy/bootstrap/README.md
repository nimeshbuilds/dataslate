# Optional upstream operator bootstrap

DataSlate supports managed/external ownership as a product contract. **The application chart never installs, upgrades, takes over or deletes cluster operators/CRDs.** Administrators must inventory existing releases first; discovering a CRD alone does not establish compatibility, the responsible controller or permission to adopt it.

The reviewed [upstream lock](upstream-lock.json) pins chart URL, chart version, application version and upstream chart SHA-256 as checked on 2026-09-24:

| Component | Chart | Operator | API | Qualification |
|---|---|---|---|---|
| Apache Spark Kubernetes Operator | 1.8.0 | 1.0.0 | `spark.apache.org/v1` | Upstream chart fetched, checksum verified, operator-only values rendered; no live Spark submission |
| CloudNativePG | 0.29.1 | 1.30.1 | `postgresql.cnpg.io/v1` | Upstream chart fetched, checksum verified, Cluster rendered; no live DB/failover/restore |

Sources: [Apache chart index](https://apache.github.io/spark-kubernetes-operator/index.yaml), [Apache operator requirements](https://apache.github.io/spark-kubernetes-operator/), [CNPG chart index](https://cloudnative-pg.github.io/charts/index.yaml), [CNPG chart documentation](https://github.com/cloudnative-pg/charts/blob/main/charts/cloudnative-pg/README.md).

The pinned PostgreSQL operand is `ghcr.io/cloudnative-pg/postgresql:17.10@sha256:eb298ae82db3e3f849db57a44bfe6fed4c4a3c95451655611980340409e5c2ec`, resolved from the upstream registry's multi-platform manifest on 2026-09-24. It remains an integration candidate until the live database tests pass. Patch/CVE evaluation and signed artifact verification belong to releases; a pinned digest does not mean indefinite security.

Fetch and inspect without accessing a cluster:

```sh
python3 deploy/scripts/fetch-upstream.py
helm template dataslate-cnpg deploy/.cache/cloudnative-pg.tgz \
  --namespace cnpg-system -f deploy/bootstrap/cloudnative-pg-values.yaml
helm template dataslate-spark deploy/.cache/spark-operator.tgz \
  --namespace dataslate-operators -f deploy/bootstrap/spark-operator-values.yaml
```

To inventory a selected cluster, administrators can read its Helm releases, Deployments and CRDs. Check both `sparkapplications.spark.apache.org` and `sparkapplications.sparkoperator.k8s.io`; the **Apache and Kubeflow operators have different, incompatible CRD schemas**. Keep an existing compatible Kubeflow installation under its current owner. DataSlate's preview adapter selects the correct API explicitly; no manifest translation or live support is implied merely by preview generation.

When no compatible operator exists, review the rendered RBAC/CRDs and the upstream installation/upgrade notes. The following are explicit installation instructions, not commands the application or tests execute:

```sh
helm install dataslate-cnpg deploy/.cache/cloudnative-pg.tgz \
  --namespace cnpg-system --create-namespace \
  -f deploy/bootstrap/cloudnative-pg-values.yaml --wait --timeout 10m

helm install dataslate-spark deploy/.cache/spark-operator.tgz \
  --namespace dataslate-operators --create-namespace \
  -f deploy/bootstrap/spark-operator-values.yaml --wait --timeout 10m
```

We deliberately show `helm install`, not an unattended `upgrade --install`: existing cluster-wide resources require explicit owner/version review. `--include-crds` during offline rendering can show packaged CRDs for that review; Helm's CRD installation and upgrade behavior differs from normal release resources. Use the upstream upgrade workflow, not deletion/recreation of CRDs.

The Spark values suppress upstream workload namespace/service-account/RBAC creation, and scope watched applications to `dataslate-compute`. That namespace and its admission, quotas, pod identities and execution RBAC must be provisioned separately before real execution is enabled. The operator still requires its own reviewed Kubernetes permissions. Configure different watch namespaces by editing a reviewed values file; do not silently widen to every namespace.

CNPG bootstrap installs the operator only. The application chart later creates the `Cluster` after `postgres.managed.operatorAccepted=true` and CRD discovery succeed. The CNPG operator generates `<cluster>-app` and `<cluster>-ca`; the DataSlate Pods reference those Secrets rather than requesting permission to read arbitrary Secrets from the Kubernetes API. [CNPG application connections](https://cloudnative-pg.io/docs/1.29/applications/).

For a future self-contained distribution, operators, cloud add-ons and optional services will have separate versioned release ownership, compatibility discovery and upgrade plans. This bootstrap is the first implementation of that separation; it does not claim a one-command installation of the entire product yet.
