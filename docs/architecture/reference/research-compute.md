# Kubernetes compute and execution research

Researched 24 September 2026. This note distinguishes upstream facts from proposed product design. No cluster installation, performance benchmark, or compatibility certification was performed. First production scope: teams within one organization.

## Recommendation

Build our own durable compute control plane and user experience over genuine Apache Spark. Do not build a new distributed execution engine initially. Use Apache's upstream Kubernetes operator for a new installation, with a separate adapter for a certified existing Kubeflow operator. Offer Spark Connect for interactive DataFrame/SQL sessions and one isolated SparkApplication per batch attempt. Preserve a classic Spark execution path for code that needs SparkContext, RDDs, JVM internals, or libraries incompatible with Connect.

Ship vanilla Spark as the reference runtime. Evaluate Apache DataFusion Comet and Apache Gluten/Velox as explicit optional runtime profiles. Neither is Photon, neither guarantees identical semantics for every Spark workload, and neither should gate the first useful product. A defensible starting certification candidate is Spark 4.1.3, Scala 2.13, Java 17/21, with the appropriate Delta 4.4.0 artifact for Spark 4.1. This is a proposed combination to test, not a verified bill of materials. If catalog integrations require a different lane, the whole runtime must move together.

## 1. Two operators with different APIs

| Area | Apache Spark Kubernetes Operator | Kubeflow Spark Operator |
|---|---|---|
| Governance | Apache Spark subproject | Kubeflow project |
| Current observed release | 1.0.0, 26 July 2026; chart 1.8.0 | v2.5.2, 31 July 2026 |
| Application API | `spark.apache.org/v1`, `SparkApplication` | `sparkoperator.k8s.io/v1beta2`, `SparkApplication` |
| Interactive server | A long-running SparkApplication running SparkConnectServer | Dedicated `sparkoperator.k8s.io/v1alpha1` SparkConnect resource |
| Important scope | 1.0 release supports Spark 4.0/4.1/4.2 and Kubernetes 1.34–1.36; drops Spark 3.5 | Broader existing installed base; current application API is beta and Connect API alpha |
| License | Apache-2.0 | Apache-2.0 |

Sources: [Apache 1.0.0 release](https://github.com/apache/spark-kubernetes-operator/releases/tag/1.0.0), [Apache tagged example](https://github.com/apache/spark-kubernetes-operator/blob/1.0.0/examples/pi.yaml), [Kubeflow repository](https://github.com/kubeflow/spark-operator), [Kubeflow releases](https://github.com/kubeflow/spark-operator/releases), [Kubeflow Connect guide](https://spark.kubeflow.org/en/latest/user-guide/spark-connect.html).

The same Kind does not imply schema compatibility. Apache uses fields such as `spec.runtimeVersions`, `spec.sparkConf`, and `spec.applicationTolerations`; a Kubeflow manifest cannot simply be posted to the Apache API. The Apache release documentation also distinguishes operator application version from Helm chart version. Pin both in the product's manifest.

### Installation contract we should build

Provide `managed`, `existing`, and `disabled` modes for every integration. For Spark, require an explicit provider after discovery when multiple are present. An install planner should inspect:

1. Served and stored CRD group/versions, schema, conversion configuration, and scope.
2. Controller release/image, readiness, namespace watch scope, selectors, webhooks, and admission availability.
3. Whether the platform service account can create/get/watch/delete applications in each workload namespace.
4. Whether the driver service account can manage the executor pods and necessary Services/ConfigMaps in its namespace.
5. Supported Spark image/Java/Scala, scheduler, storage, pod security, routing, and metrics capabilities.
6. A safe canary job and isolated Connect session before the integration is declared Ready.

CRD presence alone is not proof of a healthy controller. The product should report `Found`, `Compatible`, `Authorized`, and `Verified` separately. Existing mode must not take Helm ownership, modify a shared CRD, replace admission webhooks, or upgrade/delete the cluster operator automatically. Manage our application resources by immutable product IDs and Kubernetes UID; never adopt resources just because their names match. Operator namespaces/selectors must be disjoint if administrators deliberately run multiple instances. Kubeflow documents this deployment concern. [Multiple operator instances](https://spark.kubeflow.org/en/latest/user-guide/running-multiple-instances-of-the-operator.html).

For a new installation, Apache upstream is the preferred strategic default. Its recent stable release is still a reason to run a proper soak test; the version number is not evidence of production fitness for our combination. If customers require Kubernetes below the certified floor, explicitly certify an older lane or Kubeflow adapter rather than promising compatibility without tests. Apache's operations document recommends Kubernetes 1.34+ and explains namespace RBAC setup. [Tagged operations guide](https://github.com/apache/spark-kubernetes-operator/blob/1.0.0/docs/operations.md).

## 2. Interactive sessions and submitted jobs are separate products internally

**Interactive:** Browser → product notebook service → isolated Python kernel → authenticated Connect gateway → dedicated Connect driver/server → executor pods. The kernel holds ordinary Python state and renders notebook outputs; Connect performs Spark computation remotely. A browser disconnect must not automatically kill the backend operation. Reconnection needs persisted session ownership, buffered outputs, bounded output sizes, and an explicit terminal/expired-session state.

Spark Connect does not include authentication. It supports an authenticating proxy over its HTTP/2 gRPC interface. Its API omits SparkContext/RDD access, while supporting DataFrames, many Scala APIs, UDFs with additional setup, and much of Structured Streaming. Thus a generic existing Spark package cannot be declared compatible merely because it imports PySpark. [Spark Connect overview](https://spark.apache.org/docs/latest/spark-connect-overview.html).

For the first production release, share a Connect server only within an explicitly trusted security domain. Per-user servers are the easiest default to reason about; a session ID alone is not an isolation boundary. Do not give an analyst who is subject to row filters a Connect server carrying broader object-store credentials. That would make the SQL policy boundary bypassable. Trusted ETL privileges and governed analytical privileges should remain explicit in the UI.

**Batch:** Product job definition → immutable revision → run → attempt → SparkApplication → driver/executor pods. Do not run scheduled production jobs in a user's mutable notebook session. Produce the job artifact and environment from the saved revision. Python/Scala/JAR jobs and notebook runs should all pass through the same execution service and audit model.

**Classic compatibility:** Allow trusted users to select a classic Spark job profile for unsupported Connect packages; later add a dedicated classic interactive kernel if demanded. Clearly disclose session restart and runtime changes. Spark supports native Kubernetes driver/executor pods and custom images. [Spark on Kubernetes](https://spark.apache.org/docs/latest/running-on-kubernetes.html).

### Connect networking

Kubeflow's current Connect resource creates a server pod and ClusterIP Service, with gRPC on 15002; the server service account manages executors. Server/executor image compatibility and endpoint readiness still need checking. Its controller owns Service selectors and required ports. [Kubeflow Connect networking and configuration](https://spark.kubeflow.org/en/latest/user-guide/spark-connect.html).

Apache upstream's tagged Connect example is a long-running SparkApplication whose main class is `org.apache.spark.sql.connect.service.SparkConnectServer`. [Tagged Connect example](https://github.com/apache/spark-kubernetes-operator/blob/1.0.0/examples/spark-connect-server.yaml). Our Apache adapter must create or request a stable internal endpoint using the installed schema and observed driver labels/ownership, and route it only through our authenticated gateway. Do not assume the Kubeflow service naming convention. The Apache API offers driver service/Ingress configuration and Gateway API route lists; those require their own Gateway API CRDs and controller when used. [Apache custom resource documentation](https://github.com/apache/spark-kubernetes-operator/blob/1.0.0/docs/spark_custom_resources.md).

## 3. The execution state machine we must own

The following is product design rather than an upstream guarantee:

`Accepted → Validated → Queued → Provisioning → Running → Succeeded | Failed | Cancelled`

Also preserve `CancelRequested`, `RetryPending`, and `Unknown`/`Reconciling` internally. An unavailable Kubernetes API must not convert a running job to Failed. Record both desired and observed state, timestamps, reason codes, last reconciliation, external resource UID, Spark application ID, and driver UID. Use watches with resource-version recovery and periodic reconciliation; make every command idempotent.

Before creating the SparkApplication, atomically persist the run snapshot and an outbox event. Snapshot code revision, entrypoint, parameters, image digest, package lock digest, Spark configuration, execution identity, connection references, policy version, input snapshot identifiers when supported, and expected output contracts. Never store plaintext credentials in the run snapshot. A lost API response must not launch a duplicate attempt: use deterministic application identity and a run/attempt label, then reconcile.

**Retries:** Make exactly one component own whole-application retry. Prefer our execution service, with operator restarts disabled for batch runs, because it can preserve distinct attempts and apply shared workflow policy. Spark's own task/stage retries are a different layer and should remain available. Airflow retries should attach to our run/attempt model rather than accidentally multiply retries with the operator. A retry creates a new attempt from the same revision; “run latest” is a separate action. Reevaluate current authorization and credential validity. Never infer that a failed driver means the sink commit did not happen.

**Cancellation:** First persist cancellation intent to prevent replacement attempts. Interrupt a tagged interactive operation or request application termination; then confirm driver and executor termination and cleanup. Spark's `interruptTag` explicitly allows a race where work finishes while it is interrupted. The UI therefore needs truthful outcomes such as “completed before cancellation” and must never claim cancellation rolled back a completed transaction. [Spark interruptTag API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.interruptTag.html).

For batch runs, resource deletion and cleanup are operator-specific. Fence delayed reconciliations using attempt identity and resource UID. Treat timeout, graceful cancellation, forced termination, sink reconciliation, and orphan cleanup as distinct stages. For streaming, define checkpoint ownership and ensure only one live writer uses a checkpoint. Resume from a checkpoint and replay/backfill into a new checkpoint are separate user actions. Exactly-once behavior is a source/sink/transaction property, not something our retry button can provide universally.

The Apache operator exposes differentiated failure states, restart policy, restart counters, driver/executor startup timeouts, retention, and resource TTL. We must normalize those without discarding diagnostic detail; its classification of infrastructure failures is best effort. [Apache application lifecycle reference](https://github.com/apache/spark-kubernetes-operator/blob/1.0.0/docs/spark_custom_resources.md).

## 4. Runtime environments and package support

“All packages” is not a credible promise across Spark/Scala/JDK/Python, CPU architectures, Connect, table formats, native accelerators, cloud SDKs, and native wheels. Support three explicit tiers:

- **Certified:** Signed immutable runtime images whose pinned combinations pass our compatibility suite.
- **User environment:** Lockfile-driven Python/Maven dependency build on an approved base; reproducible and scanned, with narrower support commitments.
- **Custom image:** Administrator-approved image with validation and diagnostics; customer owns unsupported libraries.

Build images in an isolated image-build service; notebook users should not gain Docker host privileges. Runtime package installation can be allowed for experimentation with visible environment drift, but promoting a notebook to a scheduled job must capture a reproducible environment. Keep client and server versions compatible; distribute UDF dependencies to executors as well as the notebook kernel. Cache immutable artifacts by digest and support a private package mirror for restricted clusters.

Spark currently publishes 4.2.0, 4.1.3, 4.0.4, and 3.5.9 in its release streams; Spark 4 uses Scala 2.13. [Spark downloads](https://spark.apache.org/downloads.html). The current Delta 4.4.0 release supplies separate Spark 4.0/4.1/4.2 artifacts, and its generic Scala artifact now targets Spark 4.2. Delta UniForm in that release supports Spark 4.1 but not 4.2. This is a concrete example of why “install latest everywhere” fails. [Delta 4.4.0 release](https://github.com/delta-io/delta/releases/tag/v4.4.0). The general [Delta compatibility page](https://docs.delta.io/releases/) still stops at Delta 4.0, so tagged release evidence must take precedence over that stale summary.

## 5. A Photon-like capability, without pretending it is Photon

### Apache DataFusion Comet

Comet accelerates Spark using Arrow columnar execution and Rust/DataFusion while preserving the Spark integration. Current 1.0.0 is dated 7 August 2026 and Apache-2.0 licensed. It supports native Parquet/SQL paths and Iceberg integration; supported Scala/Java UDF execution can remain in its columnar pipeline. This does not imply universal acceleration of Python code, streaming state, or every connector. [Comet repository](https://github.com/apache/datafusion-comet), [1.0.0 release](https://github.com/apache/datafusion-comet/releases/tag/1.0.0).

The tagged 1.0 installation matrix runs both Comet and Spark SQL CI for Spark 4.0.4 and 4.1.3. Spark 4.2 support is explicitly experimental in those release docs. Published Linux artifacts support amd64 and arm64 with CPU instruction baselines; scheduling must respect those CPU capabilities. [Tagged installation matrix](https://github.com/apache/datafusion-comet/blob/1.0.0/docs/source/user-guide/latest/installation.md).

A material finding: the tagged compatibility guide lists result divergences and an ABFS object-store cache collision across containers in the same storage account. It also documents invalid-UTF-8 and ANSI error differences. The scan guide documents a legacy-date rebasing correctness gap. Those are acceptance blockers for affected workloads, not cosmetic caveats. An upgrade, targeted disablement, or upstream fix must be verified before certification. [Comet 1.0 compatibility](https://github.com/apache/datafusion-comet/blob/1.0.0/docs/source/user-guide/latest/compatibility/index.md), [Parquet scan compatibility](https://github.com/apache/datafusion-comet/blob/1.0.0/docs/source/user-guide/latest/compatibility/scans.md).

### Apache Gluten with Velox

Gluten retains Spark orchestration and offloads supported physical operations to native engines; unsupported operators can fall back. It is an Apache top-level project since March 2026, Apache-2.0 licensed, with current v1.7.0 released 26 August 2026. Its release README lists Spark through 4.1. [Tagged README](https://github.com/apache/gluten/blob/v1.7.0/README.md), [v1.7.0 release](https://github.com/apache/gluten/releases/tag/v1.7.0).

Its release documentation needs careful interpretation: the Velox page's opening version table is old, while the same page has Spark 4.0/4.1 build profiles. The backend builds for x86_64 and aarch64, and uses a maintained IBM Velox fork with required changes. Pin the entire native dependency build and do not substitute arbitrary upstream Velox binaries. [Tagged Velox build guide](https://github.com/apache/gluten/blob/v1.7.0/docs/get-started/Velox.md).

The tagged limitations still state that ANSI mode falls back, and document regex, case sensitivity, historical-date and other semantic differences. Treat the list as reasons for targeted verification, particularly because adjacent documentation is inconsistent. Do not disable ANSI globally just to advertise speed. [Tagged backend limitations](https://github.com/apache/gluten/blob/v1.7.0/docs/velox-backend-limitations.md).

Gluten's Delta support matrix specifically lists Spark 3.5/Delta 3.3 and Spark 4.0/Delta 4.0. Native Delta writes are experimental and off by default; CHECK constraints and TimestampNTZ are unsupported in that table, several features are partial, and UniForm is untested. Its broader Spark support does not certify Delta 4.4 or our complete storage stack. [Tagged Delta feature matrix](https://github.com/apache/gluten/blob/v1.7.0/docs/get-started/VeloxDelta.md).

### Sail

Sail 0.7.1 (24 August 2026) is an Apache-2.0 Rust engine exposing Spark Connect and supporting Delta/Iceberg and catalog integrations. It has a Kubernetes deployment path. It is an interesting future engine provider, not the compatibility default for a product promising broad upstream Spark packages. [Sail repository](https://github.com/lakehq/sail), [release](https://github.com/lakehq/sail/releases/tag/v0.7.1).

Its migration guide explicitly excludes RDDs, marks Structured Streaming partial and pandas-on-Spark planned, and says many Spark/Hadoop settings have no effect. Its compatibility scanner checks implemented DataFrame functions, not behavioral parity or SQL strings. [Migration guide](https://docs.lakesail.com/sail/latest/introduction/migrating-from-spark/).

### Product decision

Start with one vanilla runtime and an experimental Comet profile on the same Spark line. Compare Gluten in a bounded evaluation if Delta-heavy workload results justify the additional lane. Certify one accelerator first; avoid exposing five engines as a substitute for engineering quality. Record accelerated coverage and fallback reasons in the run's query plan. Use result comparison, realistic end-to-end latency, CPU seconds, memory, shuffle, object-store requests, and concurrency tests before making efficiency claims. An engine selection must not silently change SQL dialect or security semantics. Repeatedly emphasize that compatibility is the intersection of engine, format features, catalog, credentials, and package versions.

## 6. Resource efficiency, observability, and security

Proposed default: isolate team namespaces with ResourceQuota/LimitRange and network policy, then enforce per-user/job policy in the control plane. Keep driver placement reliable, allow spot/preemptible executors for restart-tolerant work, configure dynamic allocation with a maximum, and use cluster node autoscaling separately. Reserve room for drivers and small interactive work. For substantial batch contention, add one certified queue/scheduler integration such as YuniKorn or Volcano; upstream Spark documents both. [Spark Kubernetes scheduling](https://spark.apache.org/docs/latest/running-on-kubernetes.html#using-apache-yunikorn-as-customized-scheduler-for-spark-on-kubernetes).

Idle executor shutdown is not equivalent to an idle-free interactive service: the kernel and driver still exist. Use explicit session TTL, idle detection that accounts for active queries and streams, auto-stop with a clear warning about lost memory state, and conservative warm pools where latency warrants their cost. Do not attempt to resume arbitrary Python memory after the pod has gone away.

Capture driver/executor logs continuously to durable storage, Spark event logs to governed object storage, metrics to the existing observability backend, and run transitions to the product DB. Spark provides JSON APIs at `/api/v1` on live drivers and History Server, including jobs, stages, SQL executions, and executors. Build first-class summaries in our UI; keep Spark UI as an advanced diagnostic view behind the same authorization gateway. Persist history before resource TTL cleanup. [Spark monitoring APIs](https://spark.apache.org/docs/latest/monitoring.html).

Never expose raw Spark configuration, pod templates, image selection, mounts, service accounts, or arbitrary secret references to ordinary users without policy validation. Those are execution privileges. Apache's tagged security policy requires suitable pod templates to satisfy restricted Pod Security Admission; merely enabling the namespace label can make default workloads fail to start. [Operator security policy](https://github.com/apache/spark-kubernetes-operator/blob/1.0.0/SECURITY.md). Spark authentication/encryption are not enabled automatically, and Kubernetes pod-list privileges can reveal automatically propagated Spark authentication secrets. [Spark security](https://spark.apache.org/docs/latest/security.html).

## 7. Compute acceptance gates for production

These are proposed gates, not outcomes already demonstrated:

1. Install on a clean certified cluster; attach safely to each certified installed operator; reject incompatible APIs without modifying the cluster.
2. Launch Python/JAR/Spark SQL/streaming jobs and interactive Connect sessions with expected identity, storage access, and isolation.
3. Show correct durable state during API timeouts, controller restarts, browser disconnects, node loss, lost driver, executor OOM, image pull failure, and quota exhaustion.
4. Prove cancel/retry races cannot start duplicate live attempts, falsely roll back committed output, or resurrect cancelled work.
5. Prove package resolution and notebook-to-job promotion reproduce the saved environment.
6. Verify accelerated and vanilla results on decimals, timestamps, time zones, nulls, NaN, regex, malformed input, schema evolution, table deletes/merges, and all enabled format features.
7. Verify cloud credential expiry/refresh, native storage readers, and cross-tenant denial paths independently for each profile.
8. Restore run history and reconnect to surviving jobs after product DB/service recovery; reconcile unknown jobs and clean only resources owned by the product.
9. Soak concurrent workloads with bounded queues and record p50/p95 startup/interactive latency plus actual resource usage. Set advertised SLOs only after measuring them.

The main engineering asset is the durable control plane, runtime certification, and coherent interaction model. Operator installation is the beginning of that work.
