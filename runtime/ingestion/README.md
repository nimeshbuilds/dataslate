# DataSlate ingestion runtime

This is an executable Spark file-to-Delta runtime, integrated through the API's immutable generated application. It drains currently available files with Structured Streaming `availableNow`, exits, and resumes from the same checkpoint on the next explicitly requested run. The runtime does not own a schedule or retry whole applications.

## Runtime profile

| Component | Pinned profile |
| --- | --- |
| Apache Spark / PySpark | 4.1.3 |
| Delta Spark / Delta Storage | 4.3.1, `delta-spark_4.1_2.13` |
| Scala / Hadoop | 2.13 / 3.4.2 supplied by Spark |
| S3A / AWS SDK v2 | Hadoop AWS 3.4.2 / SDK bundle 2.29.52 |
| Analytics accelerator / OpenSSL adapter | 1.2.1 / 2.1.4.Final, Hadoop runtime dependencies |
| OCI base | Apache Spark 4.1.3, multiarchitecture index pinned in Dockerfile |

The exact six additional JVM artifact URLs and SHA-256 hashes are in [jars.lock.json](jars.lock.json). The Dockerfile downloads and verifies them **during image build**. Drivers/executors do not download Maven packages at runtime. The Python module needs PySpark and the JVM jars; it does not use the separate `delta-spark` Python package, whose 4.3.1 distribution would select a different PySpark patch. The runtime verifies Spark/Hadoop versions and the loaded Delta Spark jar's SHA-256 before accessing data.

Upstream basis: [Delta compatibility matrix](https://docs.delta.io/releases/), [Delta 4.3.1 release](https://github.com/delta-io/delta/releases/tag/v4.3.1), [Spark 4.1.3 file-source/checkpoint semantics](https://spark.apache.org/docs/4.1.3/streaming/apis-on-dataframes-and-datasets.html), [Delta idempotent `foreachBatch` writes](https://docs.delta.io/delta-streaming/), [Delta storage guarantees and S3 concurrency](https://docs.delta.io/delta-storage/). These establish supported interfaces, not qualification of a deployed EKS stack.

## Invocation and contract

The control plane generates only a JSON literal and this fixed invocation:

```python
import dataslate_ingestion
# config is decoded from the immutable server-generated JSON snapshot.
dataslate_ingestion.run(config)
```

[examples/orders.json](examples/orders.json) is the complete v1 contract. `pipelineId`, `runId` and `pipelineRevision` are supplied by the control plane. Unknown fields fail. Schema fields must have unique names, explicit supported types and nullability. There are at most 200 fields; names are simple identifiers up to 128 characters. Types: `string`, signed 64-bit `long`, `double`, `boolean`, `date`, `timestamp`.

Locations must be canonical, disjoint `s3://bucket/nonempty-prefix/` paths; the runtime translates them to `s3a`. Paths cannot contain credentials, query parameters, escaping, globbing, traversal or ambiguous repeated separators. S3A uses only the IRSA web-identity provider. `AWS_ROLE_ARN`, `AWS_WEB_IDENTITY_TOKEN_FILE` and `AWS_REGION` must be injected into the workload; static keys, shared credential files and node-role fallback are not selected. Supply least-privilege read access to the source and write/read access to the target/checkpoint through the execution binding's approved workload identity. Projected tokens must also reach executors. No credential is accepted in pipeline JSON. Azure/GCS storage adapters are not enabled by this profile.

The source is a dedicated landing prefix of **immutable files published atomically under unique names**:

- JSON means UTF-8 newline-delimited `.json` object files. Every declared field must be present; nullable fields may contain explicit `null`. Duplicate keys, extra/missing fields, malformed records and implicit type coercion fail. Dates use `YYYY-MM-DD`; timestamps require an RFC 3339 offset or `Z` with at most microsecond precision. JSON numbers must be finite. No multiline JSON, compression, nesting or CSV is silently accepted.
- Parquet means `.parquet` files, including Snappy Parquet. Every file's complete physical Spark schema is inspected, including empty files. Field order, names and types must match. Required values cannot be null. Recursive directories are supported; partition directory names are not inferred into columns.
- Every visible non-hidden source file must match the declared extension. Spark intentionally ignores hidden files/directories. A Delta table must be read as a Delta snapshot, not ingested as raw Parquet; root/nested `_delta_log` ancestors are rejected for discovered input files.

The stream uses Spark's `binaryFile` source for checkpointed file discovery, projecting away `content` so the driver collects only bounded metadata. `maxFilesPerTrigger` (1–10,000) bounds files per microbatch, **not file bytes, total backlog, wall-clock cost or memory per record**. The reader validates rows and writes an append-only Delta transaction with Snappy compression; schema auto-merge is disabled. A malformed microbatch fails without a partial Delta commit for that batch. Prior committed batches remain committed. The very large seen-file age protects late-arriving older files from Spark's short default horizon; it is not an infinite history guarantee.

## Ownership and recovery

The control plane must permanently reserve target/checkpoint prefixes, reject overlapping reservations and serialize runs until the prior Spark application and its associated Pods have been observed drained. The runtime itself is **not a distributed lock**. Its Hadoop `create(overwrite=false)` manifest writes must not be mistaken for a cross-writer conditional lock on S3. Storage IAM must exclude other writers; arbitrary notebooks or external engines with the same raw-storage authority can bypass the platform's ownership rules. Delta's default S3 log store does not make those unrelated writers safe.

A stored contract in both target and checkpoint binds pipeline ID, source, explicit schema, destination, checkpoint and runtime semantics. First use requires empty locations; existing customer Delta tables/checkpoints are not silently adopted. Contract changes require a new pipeline and fresh target/checkpoint. Name, timeout, run ID, revision and batch size do not change the physical contract.

Each Delta microbatch uses a stable `txnAppId` derived from the contract and `txnVersion=batchId`. If the Delta commit succeeds but Spark's checkpoint acknowledgment is lost, the identical batch replay is ignored by Delta. An additional target marker binds Spark's checkpoint query UUID. Missing/reset checkpoints, changed query identities and finite `delta.setTransactionRetentionDuration` fail closed, preventing common replay configurations that silently lose deduplication. Do not edit, delete, restore independently or apply lifecycle expiration to the checkpoint, manifests or Delta log. Recovery from arbitrary checkpoint corruption or independently restored storage needs an explicit operator procedure; these guards do not prove correctness of manually altered state.

The same immutable files/checkpoint/contract are required on restart. **Exactly-once results are not claimed for overwritten files, changed file names, non-atomic publishers, source deletion before commit, duplicate business records, external writers or manual checkpoint resets.** File size/mtime checks detect some mutations; they do not make mutable objects safe.

## Completion evidence

One bounded `DATASLATE_INGESTION_RECEIPT ` JSON line is emitted to owner-scoped driver logs after termination and `SparkSession.stop`. Exceptions propagate and the process fails. Success requires confirmed `awaitTermination`, no active query and no stop failure. Timeout/stop uncertainty never produces a success receipt. This is application evidence, not yet a separately persisted platform receipt or a universal publication acknowledgment.

The receipt reports IDs, runtime, contract hash, query ID and counts. `processedRows`/`processedFiles` include reprocessed batches whose already committed Delta transaction was deduplicated; they are **not newly inserted row counts**. An empty source succeeds with `tableExists:false`; the platform must not claim a table was published in that case. A failed drain can already have committed earlier batches. Cancellation cannot roll them back. Abrupt process/Pod death can leave no receipt at all.

## Build and test

```sh
python3 -m unittest discover -s runtime/ingestion/tests -p 'test_*.py' -v
python3 runtime/ingestion/download_jars.py --directory /tmp/dataslate-ingestion-jars
# Verify an already downloaded set without network:
python3 runtime/ingestion/download_jars.py --directory /tmp/dataslate-ingestion-jars --offline

docker build -t dataslate-ingestion:qualification runtime/ingestion
```

Use the resulting immutable image digest in a `file-ingestion-batch` binding; the Dockerfile alone is not a published or qualified image. Do not invoke an ingestion source through a general notebook profile that grants another writer to the same prefixes.

The separate real-JVM suite is explicit and fails if prerequisites are missing:

```sh
# In an isolated Python environment with pyspark==4.1.3 and Java 21:
python3 runtime/ingestion/qualify.py \
  --python /absolute/path/to/venv/bin/python \
  --java-home /absolute/path/to/jdk \
  --jar-directory /absolute/path/to/jars
```

The runner verifies all locked jars and starts a fresh JVM for each scenario, matching a fresh driver process per production run. The integration harness uses explicit `allow_local_files=True` and disposable directories. Generated production source must never enable that option.

Qualification on 2026-11-07 passed 23 unit tests and 14 real-JVM scenarios on macOS arm64, Python 3.14.6, Temurin 21.0.13+9, PySpark 4.1.3, Delta 4.3.1 and all six locked jars. Scenarios cover incremental/no-op restart, actual Snappy output, a failure after Delta commit before checkpoint acknowledgment and deduplicated replay, JSON/Parquet schema failures, checkpoint/contract identity, finite transaction retention, timeout and Spark-stop errors, invalid UTF-8, nested Delta input rejection, offline S3A/web-identity class resolution, and an empty source.

Local filesystem tests validate program/transaction behavior. Class resolution does not validate live S3A, IRSA or object-store consistency. These results do not qualify EKS cancellation/drain, a full platform deployment or high-volume performance.

The production OCI image was subsequently built and qualified on Linux arm64 with bundled Python 3.10.12/Java 21, UID 185, six verified jar checksums, networking disabled, 4 GiB memory and two CPUs. All 14 real scenarios passed against its built runtime module and jars. The manifest digest and exact scope are recorded in [qualification.json](qualification.json); no image was pushed by this qualification. The container test caught two build boundaries: downloaded jars must be readable by the non-root runtime user, and the harness must support the base image’s Python 3.10.

The CI harness can be built from that production image and runs without dependency downloads:

```sh
docker build -f runtime/ingestion/QualificationDockerfile -t dataslate-ingestion-tests:qualification runtime/ingestion
docker run --rm --network none --memory 4g --cpus 2 dataslate-ingestion-tests:qualification
```
