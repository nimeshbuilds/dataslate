# Kubernetes installation preflight

`check.py` inventories prerequisites for the **actual rendered DataSlate chart** in one explicitly selected Kubernetes context. It produces actionable text or a versioned JSON report without creating, updating, deleting, adopting or installing Kubernetes resources. It is an administrator CLI; it does not use a DataSlate user token.

Requires Python 3.10+, PyYAML, a trusted Helm executable, and a trusted `kubectl` using the intended kubeconfig. Helm rendering runs locally with the supplied values; no dependency downloads, DNS-enabled rendering, server dry-run or plugin invocation is requested. Every Kubernetes command includes `--context`; the tool never reads or changes `current-context` and never prints kubeconfig contents. The kubeconfig's normal credential plugin may run to authenticate these reads.

```sh
python3 deploy/preflight/check.py \
  --context my-isolated-eks-context \
  --namespace dataslate --release dataslate \
  -f deploy/examples/eks-external.yaml \
  -f /secure/location/my-reviewed-values.yaml
```

For managed PostgreSQL and Spark execution, include the corresponding overlays and identify the **existing** operator Deployments explicitly:

```sh
python3 deploy/preflight/check.py \
  --context my-isolated-eks-context \
  --namespace dataslate --release dataslate \
  -f deploy/examples/eks-external.yaml \
  -f deploy/examples/eks-managed-postgres.yaml \
  -f deploy/examples/eks-execution.yaml \
  -f /secure/location/my-reviewed-values.yaml \
  --postgres-operator cnpg-system/cnpg-cloudnative-pg \
  --spark-operator dataslate-operators/dataslate-spark-spark-kubernetes-operator \
  --format json > preflight-report.json
```

Names are examples, not defaults to adopt. Replace all example identities, images, endpoints, namespaces and network ranges before use. The command exits nonzero when prerequisites are blocked or unresolved, so preserve the report even if automation treats the exit as a failure. Reports contain context, object names, owner metadata and operator image references; handle them as internal operational information.

## What it establishes

| Check | Evidence | Limits |
|---|---|---|
| Local chart render | Chart schema/template validation succeeds with the selected layers | Not server admission or installation |
| Cluster | Server Kubernetes version; chart minimum 1.29 | A newer server is not automatically a qualified DataSlate runtime |
| Namespaces | Exact application/compute namespaces are Active | No namespace creation or takeover |
| Spark/CNPG APIs | Required named CRD is Established, namespaced, serves `v1`, and exposes required field types | Not a complete schema diff or runtime compatibility certificate |
| Existing Spark alternatives | Apache and Kubeflow CRDs are distinguished | Kubeflow preview support is not live execution support; existing ownership is preserved |
| Operator Deployments | Explicitly named owner, container images, observed generation and available replicas | Image compatibility, watch scope, webhooks and actual reconciliation still need review |
| Ownership collisions | Each supported rendered object's exact name and Helm owner metadata | Matching labels do not establish upgrade, rollback or migration safety |
| Credential references | Exact Secret names and required key names, deduplicated from rendered workloads, projected volumes, ingress TLS and image pull references | No credential values, validation, expiry, token exchange or rotation checks |
| CNPG-generated credentials | Expected app/CA Secrets can be marked pending before reconciliation | Pending is not success; rerun after the operator creates them |
| Storage/ingress | Explicit class existence | Not CSI/controller readiness, capacity, routing, certificates or load balancer health |
| Compute boundary | Version-pinned restricted Pod Security and quota/LimitRange/NetworkPolicy presence | Counts and labels cannot prove effective admission, isolation, workload identity or S3/KMS access |
| Network/integrations | Served NetworkPolicy API and enabled backend requirements | CNI enforcement, additive policies, SNAT/DNAT, TLS/authentication and live data-plane access remain unknown |

The API, SQL, search, Sylon, publication and workflow worker credentials are discovered from their actual manifests. Enabling a module without rendering its resources cannot manufacture a successful check. Unknown resource kinds produce an explicit unresolved ownership check.

## Status and automation contract

JSON `schemaVersion: 1` contains `product`, `readOnly`, `qualification`, explicit `context`, `namespace`, `release`, UTC `generatedAt`, `state`, status `counts`, and ordered `checks`. Each check has a stable category/resource `id`, `status`, a plain-language `message`, optional `action`, and bounded allowlisted `evidence`. No raw command output is embedded.

| Check status | Meaning |
|---|---|
| `observed` | The stated read-only fact was observed; read the check's limits |
| `blocked` | A definite missing or incompatible prerequisite/collision was observed |
| `unknown` | Access, tooling, response shape or a required live/manual check prevents a conclusion |
| `pending` | An optional/generated prerequisite is absent and must be revisited |
| `unsupported` | An installed alternative is outside the supported execution path |

Exit codes: `0` = all recorded prerequisites observed; `1` = at least one blocker; `2` = invalid CLI/values/rendered input; `3` = unresolved prerequisites without a definite blocker. A complete inventory still returns `3` for live enforcement/integration gates: this command intentionally cannot certify production readiness. `qualification` is always `not-performed`. Never reinterpret exit `3` as installation failure or ignore it as readiness success.

Per-command timeout defaults to 15 seconds; total probe budget to 180 seconds. Configure with `--timeout` (1–60) and `--total-timeout` (1–600). Both output streams are bounded to 4 MiB combined; runaway subprocesses are stopped. At most 300 probes and 500 rendered documents are accepted. YAML is size/depth/node bounded; duplicate/merge keys, recursive aliases and non-JSON scalar types are rejected. Values layers replace arrays and merge maps; changing a values file while rendering aborts the inventory. Raw Helm/kubectl stderr and parse context are deliberately suppressed because they can contain sensitive input. To diagnose a render failure, run Helm directly in your trusted local terminal and review its output there.

## Credential and permission boundary

The checker never lists Secrets or requests a Secret as JSON/YAML. It issues a separate named `kubectl get secret NAME --namespace NS` with a Go template that emits only key names. The supported kubectl stdout is key-only; the checker does not request, decode, persist or print Secret values. Unexpected output is rejected, and bounded stderr is used only for generic error classification, never printed or saved. **Kubernetes `get` permission on a Secret still grants access to its complete contents, and kubectl necessarily receives that complete object before formatting it.** Kubernetes does not offer key-only Secret authorization. Run under a trusted administrator environment; if that permission is inappropriate, let this check remain `unknown` and have the credential owner verify keys. Do not grant `list`/`watch` on Secrets for preflight. [Kubernetes Secret security](https://kubernetes.io/docs/concepts/configuration/secret/), [kubectl get output options](https://kubernetes.io/docs/reference/kubectl/generated/kubectl_get/).

Other reads are server version/discovery, named namespaces and operator CRDs/Deployments/classes, exact rendered object ownership metadata, and compute-namespace quota/LimitRange/NetworkPolicy lists. Use a read-only inventory role scoped to those resources. No Helm release Secret inventory, pod logs, environment-variable output, identity impersonation, service connection, S3 operation, database migration or paid model request occurs.

Read-only policy inspection cannot establish that a CNI enforces NetworkPolicy. Actual enforcement requires an isolated, authorized live test; policies also combine additively. [Kubernetes NetworkPolicy behavior](https://kubernetes.io/docs/concepts/services-networking/network-policies/).

## Existing services and optional managed services

External PostgreSQL and existing engine/catalog/provider services remain externally owned. This tool never translates “found” into “adopt.” A conflicting existing DataSlate object blocks the proposed names; select a free namespace/release/name or agree an explicit migration with its owner.

Managed PostgreSQL creates a DataSlate CNPG `Cluster` only through a separately approved chart deployment. A missing Apache/CNPG operator requires the [reviewed bootstrap workflow](../bootstrap/README.md); preflight only reports that requirement. The optional [Trino](../sql/README.md) and [Polaris](../polaris/README.md) profiles are separate reviewed deployments. No one-command self-contained installer, upgrade coordinator, backup/restore qualification or complete Databricks parity is implied.

## Verification

```sh
python3 -m unittest discover -s deploy/preflight -p 'test_*.py' -v

# Include the real local chart render and native Go-template checks:
DATASLATE_PREFLIGHT_HELM="$(command -v helm)" \
  python3 -m unittest discover -s deploy/preflight -p 'test_*.py' -v
```

The 38 checks (36 fixtures/subprocess checks and two optional native Helm checks) exercise missing/forbidden resources, owner collisions, incompatible and unestablished CRDs, operator generation lag, compute boundary omissions, key-only Secret handling (including empty Secrets), no credential disclosure, read-only command allowlists, explicit context, bounded subprocess output/time and malformed input. They make no live-cluster claim. Required schema field types were also checked against the pinned Apache operator `1.0.0` and CNPG chart `0.29.1` CRDs; actual controller/runtime compatibility remains a separate qualification gate.
