# Saved Python notebook batch implementation and upstream contract

The saved Python batch path is implemented, covered by memory/PostgreSQL/TLS-backend tests, and exercised against a disposable local k3s cluster with Apache operator 1.0.0 and Spark 4.1.3. It executes an independently retained saved notebook revision under an explicitly granted engineering identity. Approved image-embedded applications remain the default. The [execution contract](execution-contract.md) defines the current HTTP, persistence, source lifecycle and security boundaries; EKS/S3 and integrated production identity/durability qualification remain pending.

## Supported upstream mount path

Apache Spark Kubernetes Operator 1.0.0 exposes a Kubernetes PodTemplateSpec at `spec.driverSpec.podTemplateSpec`. Its resource-spec factory serializes that template and passes it to Spark through `spark.kubernetes.driver.podTemplateFile`. A generated template can mount a controller-created ConfigMap:

```yaml
spec:
  mainClass: org.apache.spark.deploy.PythonRunner
  pyFiles: local:///opt/dataslate/source/source.py
  driverSpec:
    podTemplateSpec:
      spec:
        volumes:
          - name: dataslate-source
            configMap:
              name: ds-<run-id>-source
              items:
                - key: source.py
                  path: source.py
        containers:
          - name: spark-kubernetes-driver
            volumeMounts:
              - name: dataslate-source
                mountPath: /opt/dataslate/source
                readOnly: true
```

The same immutable runtime image supplies Python and Spark. The generated driver template may set the fixed container name explicitly in Spark configuration. Caller-supplied pod templates, arbitrary mounts, service accounts and Spark configuration remain prohibited.

Setting `mainClass` to PythonRunner makes the operator submission worker use the first `pyFiles` entry as the primary Python application without adding that same file to supplementary `spark.submit.pyFiles`. This is relevant when the source mount exists only on the driver. Importable modules, supplementary packages and executor-side source distribution require separate defined packaging behavior.

Two tempting alternatives are unsuitable for this design:

- The operator's `spec.configMapSpecs` describes name/labels/annotations/data but has no `immutable` field; its builder creates ordinary mutable ConfigMaps.
- Spark 4.1.3's `spark.kubernetes.driver.volumes.<type>` configuration supports hostPath, emptyDir, NFS and PVC, not a ConfigMap type. Use the operator's typed pod template.

## Submission and recovery contract

1. Set the explicit binding capability `executionKind: "notebook-python-batch"`, separate from fixed-entrypoint approved jobs. Its issuer/subject grant authorizes arbitrary Python execution under that binding's runtime/storage identity. Show that authority when an administrator grants it.
2. Accept a saved notebook ID and expected revision. Fetch and authorize that revision on the server, reject unsupported languages and oversize source, and snapshot source, cell IDs/order, notebook revision and a SHA-256 into the run transaction. Do not read the latest notebook again during reconciliation.
3. Fingerprint idempotency using the binding and exact snapshot selection. A later edit cannot change accepted work; a repeated request returns the original run.
4. Prepare a deterministic `core/v1` ConfigMap with `immutable: true`, ownership labels and source hash. Verify all fields/content on conflict or uncertain creation. Enforce the 256 KiB limit of UTF-8 generated source, comfortably below Kubernetes object and transport limits.
5. Persist artifact name/UID/hash and readiness separately from the execution submission boundary. Creating a source ConfigMap does not execute the job. Reconciliation must recover a prepared artifact after a crash without confusing it with an uncertain Spark submission.
6. Check the current binding grant again, durably mark the execution submission boundary, then POST the deterministic SparkApplication using the exact verified source name. Reuse the current ownership/spec/UID checks and no-blind-retry rule.
7. Once the SparkApplication UID is known, attach its same-namespace owner reference to the immutable ConfigMap using a resource-version/UID-checked metadata update. Kubernetes permits metadata changes to immutable ConfigMaps. Before that point, the ConfigMap has no application owner, so durable source reconciliation uses the run and artifact identity. Protected-source admission intentionally allows only the configured controller to delete these objects; owner references are not an automatic-GC guarantee. Explicit cleanup continues after terminal execution, and an uncertain create with no observed UID remains pending. Never delete a same-named foreign resource.
8. Revoke/cancel both pending preparation and running work. Foreground job deletion and explicit orphan cleanup must not delete a replacement ConfigMap. Preserve the source snapshot and provenance in PostgreSQL according to run retention.

This adds namespaced ConfigMap create/get/metadata-patch/delete permissions to the controller. The API needs no ConfigMap access. A dedicated artifact store would eventually support larger notebooks and packages, but must preserve immutable version identity and grant boundaries.

## Security and user experience

Arbitrary Python can use every storage permission and Kubernetes credential granted to its workload. It cannot enforce row-level policy against raw S3 access by itself. The engineering profile must therefore be separate from governed SQL execution, with clear administrator consent and storage limits.

Execution namespace isolation matters beyond the HTTP API: a Spark driver with Kubernetes pod/ConfigMap permissions in a shared namespace may access other jobs' resources. Use namespaces and workload identities for the intended trust boundary, constrain pod admission, block arbitrary service-account selection and metadata-service access, and verify egress/storage restrictions. A ConfigMap is not secret storage; do not accept embedded credentials as a platform secret mechanism.

The first UX should say **Run saved revision as batch**, show the revision/runtime identity, and link to the actual run, state, cancellation and driver logs. A fresh process per batch has no persistent interactive Python state. Cell-by-cell execution, partial re-execution, interactive sessions, rich display outputs and chart data need an additional runtime/results protocol. The generated runner already compiles each cell with a stable notebook/revision/cell filename and shares a separate user namespace while protecting runner builtins from ordinary variable shadowing. A future runtime/results protocol may emit cell-boundary/output metadata; this batch path currently exposes real driver logs and tracebacks only.

## Qualification boundary

- Exact generated pod template and application pass the pinned CRD schema and a real Kubernetes API server's admission/defaulting.
- Real operator creates the driver with the intended read-only mount; Python reads the snapshot and Spark performs a known transformation.
- Edits after acceptance do not affect executed bytes. Duplicate requests, ConfigMap conflicts, lost responses and controller crashes preserve identity and avoid duplicate Spark submission.
- Cross-workspace/subject access, mutated/replaced ConfigMaps, grant revocation, profile changes and cancellation at every preparation stage fail closed.
- Local Pod admission, driver/executor identities, resource quotas, log access and artifact cleanup are exercised in the isolated live qualification environment. Cloud storage scope and adversarial multiuser qualification remain additional gates; namespace engineering identities are not a hostile-code sandbox.

Primary contracts inspected:

- [Pinned SparkApplication CRD](https://github.com/apache/spark-kubernetes-operator/blob/1.0.0/build-tools/helm/spark-kubernetes-operator/crds/sparkapplications.spark.apache.org-v1.yaml)
- [SparkAppResourceSpecFactory template handling](https://github.com/apache/spark-kubernetes-operator/blob/1.0.0/spark-operator/src/main/java/org/apache/spark/k8s/operator/reconciler/SparkAppResourceSpecFactory.java)
- [SparkAppSubmissionWorker Python entrypoint selection](https://github.com/apache/spark-kubernetes-operator/blob/1.0.0/spark-submission-worker/src/main/java/org/apache/spark/k8s/operator/SparkAppSubmissionWorker.java)
- [ConfigMapSpec fields](https://github.com/apache/spark-kubernetes-operator/blob/1.0.0/spark-operator-api/src/main/java/org/apache/spark/k8s/operator/spec/ConfigMapSpec.java)
- [ConfigMapSpecUtils construction](https://github.com/apache/spark-kubernetes-operator/blob/1.0.0/spark-submission-worker/src/main/java/org/apache/spark/k8s/operator/utils/ConfigMapSpecUtils.java)
- [Spark 4.1.3 Kubernetes deployment and volume configuration](https://spark.apache.org/docs/4.1.3/running-on-kubernetes.html)
