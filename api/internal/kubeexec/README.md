# Apache Spark execution adapter

This adapter issues real Kubernetes API requests for **administrator-approved batch Python applications embedded in digest-pinned images**. It targets Apache Spark Kubernetes Operator **1.0.0**, `spark.apache.org/v1`, and the existing Spark **4.1.3** runtime candidate. Automated adapter tests are not live Spark, EKS, S3 or runtime compatibility qualification.

It does not execute notebook cell source, SQL documents, uploaded packages or arbitrary user Spark settings. The approved image and entrypoint define the immutable application. Binding membership is issuer/subject specific; workspace membership alone cannot grant a compute/storage identity. Runtime images, service accounts, storage policies, pod admission and namespace isolation still require administrator review and live qualification.

## Processes and permissions

The API accepts authorized run requests and reads logs using a separate read-only Kubernetes service account. `cmd/dataslate-controller` reads durable PostgreSQL runs, leases work with fencing and calls the execution adapter. It does not authenticate browser users or run schema migrations. Use `api/ControllerDockerfile` with `api/` as the image build context.

| Process | Required Kubernetes access in each approved execution namespace |
|---|---|
| API | `get` SparkApplications; `list/get` Pods; `get` `pods/log` |
| Controller | `get/create/delete` SparkApplications; `list/get` Pods; `get` `pods/log` |

Both inspect `/apis/spark.apache.org/v1` discovery at startup. This discovery normally uses Kubernetes' authenticated discovery permissions; it does not require reading or writing cluster-scoped CRDs. Discovery verifies the served API and resource shape, not the installed operator image version. Administrators must qualify an externally managed operator before enabling execution.

The controller service account must not be used as the Spark driver's identity. The binding's driver service account requires separately scoped Spark pod-management and workload storage permissions. This adapter never creates service accounts, roles, operators, storage buckets or CRDs.

## Configuration

| Environment variable | Purpose |
|---|---|
| `DATASLATE_DATABASE_URL` | Controller's existing DataSlate PostgreSQL connection, with production certificate/hostname verification |
| `DATASLATE_EXECUTION_NAMESPACES` | Required space-separated namespace allowlist for both API and controller |
| `DATASLATE_EXECUTION_BINDINGS_FILE` | Required administrator configuration for both API and controller; controller rechecks grants against each persisted immutable run profile |
| `DATASLATE_KUBERNETES_API_URL` | Optional HTTPS API origin; default `https://kubernetes.default.svc` |
| `DATASLATE_KUBERNETES_CA_FILE` | Optional CA path; defaults to projected service-account `ca.crt` |
| `DATASLATE_KUBERNETES_TOKEN_FILE` | Optional token path; defaults to projected service-account `token` |
| `DATASLATE_CONTROLLER_ADDR` | Controller health listener; default `:8081` |

The controller exposes `GET /healthz` and `GET /readyz`. Readiness verifies recent reconciliation-loop progress and database schema availability. Run observations separately disclose Kubernetes API failures; readiness is not a promise that every downstream job will succeed. Each reconciliation has a 25-second outer deadline, a 20-second backend deadline and a 30-second durable lease. The initial controller reconciles one due run every two seconds; this is not a large-installation throughput qualification.

HTTPS verification cannot be disabled. Redirects and ambient HTTP proxies are not used. The service-account token file is reopened on every request to honor projected-token rotation. CA rotation requires restarting the process. Responses are bounded to 2 MiB and requests time out. Errors returned to the control plane contain fixed categories rather than Kubernetes bodies, URLs or tokens.

## Lifecycle guarantees and limits

- Run names are deterministic. Ownership labels, namespace, UID and a hash plus exact comparison of the expected specification are verified before adoption, cancellation or log access. Unexpected admission changes fail verification instead of being silently accepted.
- The controller requires the current bindings file and reloads it on every leased run and immediately before POST. Removing a subject/binding, or changing its approved profile, requests cancellation of its queued or running old-profile runs. Malformed/unreadable policy fails closed without new external effects. Kubernetes ConfigMap projection has propagation latency; an administrator needing an immediate stop must also revoke the workload's runtime/storage identity and cancel its cluster resources. OIDC workspace-membership changes are enforced by the API but are not actively introspected by the execution controller. Historical run/log access remains governed by current workspace membership plus original issuer/subject ownership.
- A create conflict or lost response triggers a read of that same identity. The adapter never retries the POST. The reconciler durably records the submission boundary before creating and refuses automatic resubmission after an uncertain result or disappearance; an operator must investigate possible committed data writes.
- Spark whole-job restarts are explicitly disabled. This prevents the upstream operator and DataSlate from independently retrying the same application. Spark's internal task retry behavior remains part of the runtime.
- Generated driver/executor templates require non-root execution, RuntimeDefault seccomp, no privilege escalation and all Linux capabilities dropped. Executors use the binding's service account with automatic Kubernetes API token mounting disabled; the driver retains its token to create executors. Fixed driver RPC and block-manager ports are 7078 and 7079 for explicit NetworkPolicy rules. The image must support these restrictions; cloud workload-identity injection requires separate qualification.
- The run lifetime includes queue time and defaults to one hour (`maxRunSeconds` omitted/zero), with administrator values from 60 seconds to 24 hours. Expiry durably requests cancellation. Backend unavailability, controller downtime and uncertain in-flight submissions remain visible; a timer is not evidence of actual workload termination or a spending guarantee.
- Cancellation sends a UID-preconditioned foreground delete. An accepted DELETE is not completion; the resource must disappear. External interference or unknown submission outcomes remain visible as uncertain states.
- Operator cleanup states are not treated as successful completion. A terminal outcome is recovered only from the latest preceding terminal state in the official history. Unknown/new states remain unknown. Persisted terminal run metadata survives upstream cleanup.
- Failed driver resources are retained for up to one hour; successful resources can be cleaned immediately. The stopped SparkApplication expires after 24 hours. These policies require live operator verification.
- Logs are a bounded tail of at most 500 lines / 64 KiB from the actual owned driver container. The response discloses it is truncated. Pod owner UID and labels are checked, and a pod identity change during retrieval discards the response. Logs become unavailable after upstream cleanup; durable archival, executor logs and structured results remain separate work.

Kubernetes admission and namespace permissions must prevent untrusted users from modifying DataSlate-owned application resources or creating replacement driver Pods. API-side identity checks supplement those controls; they cannot revoke a pod that a cluster administrator has already altered or prevent arbitrary approved code from misusing credentials granted to its service account.

## Verification

`go test -race ./internal/kubeexec` exercises realistic TLS HTTP fixtures for served-resource discovery, create/conflict/lost-response reconciliation, foreign-resource refusal, UID-safe cancellation, exact specification verification, state normalization, token rotation, redirects, oversized responses and owned-log access. The generated manifest is validated against the repository's pinned upstream CRD schema. These fixtures do **not** run Kubernetes admission, garbage collection, an operator, Spark or S3.

Required live gates include production OIDC subject binding, isolated EKS namespace execution, operator discovery/version ownership, admission/defaulting, immutable image startup, driver/executor scheduling and storage identity, actual data correctness, foreground cancellation, loss/restart/lease recovery, log expiry, cluster upgrade and PostgreSQL restore. No EKS or S3 resource has been created or qualified by these tests.

## Upstream contracts inspected

- [Apache 1.0.0 SparkApplication CRD](https://github.com/apache/spark-kubernetes-operator/blob/1.0.0/build-tools/helm/spark-kubernetes-operator/crds/sparkapplications.spark.apache.org-v1.yaml)
- [Application state meanings](https://github.com/apache/spark-kubernetes-operator/blob/1.0.0/spark-operator-api/src/main/java/org/apache/spark/k8s/operator/status/ApplicationStateSummary.java)
- [Terminal state and history transitions](https://github.com/apache/spark-kubernetes-operator/blob/1.0.0/spark-operator-api/src/main/java/org/apache/spark/k8s/operator/status/ApplicationStatus.java)
- [Driver labels](https://github.com/apache/spark-kubernetes-operator/blob/1.0.0/spark-operator/src/main/java/org/apache/spark/k8s/operator/utils/Utils.java)
- [Owner references, including foreground deletion blocking](https://github.com/apache/spark-kubernetes-operator/blob/1.0.0/spark-operator-api/src/main/java/org/apache/spark/k8s/operator/utils/ModelUtils.java)
- [Driver resource ownership](https://github.com/apache/spark-kubernetes-operator/blob/1.0.0/spark-operator/src/main/java/org/apache/spark/k8s/operator/decorators/DriverResourceDecorator.java)
