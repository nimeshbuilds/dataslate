# Local Delta publication qualification

The [2026-11-08 evidence](qualification-v2.json) records the actual v2 receipt runtime and a synthetic Spark → Delta → Hive Thrift metastore → Trino 484 test. It also records the native Go publication adapter's readback. This is shared-volume, single-node qualification. It does not qualify an EKS deployment, S3/IRSA/Glue, Kubernetes receipt capture, production API publication, or performance.

The first offline JVM attempt exhausted the disposable VM disk after image pulls and build cache. Thirteen scenarios passed before that failure. After removing only that VM's disposable build cache, all seventeen scenarios passed from the same production image. The failed and successful logs have separate hashes in the evidence; the first run is not reported as successful.

## Why these components

Trino's [Delta connector](https://trino.io/docs/current/connector/delta-lake.html) supports [Hive Thrift and Glue metastores](https://trino.io/docs/current/object-storage/metastores.html). Existing Delta data is registered with `system.register_table`, enabled explicitly by `delta.register-table-procedure.enabled=true`. This qualification uses the [official Hive standalone metastore image](https://hive.apache.org/development/quickstart/) and its disposable embedded Derby database. Polaris is an Iceberg REST catalog and is not used as a Delta metastore.

The runtime attests a native Delta UUID and one immutable snapshot. Trino's structured `$properties`, `$history`, and `DESCRIBE` independently verify the catalog's location, retained version, ordered primitive fields, protocol and feature union. They do not expose the native Delta UUID. Duplicate property keys must be rejected, because Delta configuration keys can collide with metadata keys in [Trino 484's properties table](https://github.com/trinodb/trino/blob/484/plugin/trino-delta-lake/src/main/java/io/trino/plugin/deltalake/DeltaLakePropertiesTable.java).

## Reproduce in a disposable VM

These commands are for a new, explicitly owned Colima profile. Do not reuse a customer profile or storage. Preserve an ownership record before starting; use the explicit profile on every command. The actual test used four CPUs, 6 GiB memory, a 12 GiB data disk plus an 8 GiB root disk, no host mounts and no default-context activation. A longer profile name exceeded macOS's UNIX socket path limit, so the recorded profile is `dataslate-lake-qualification-261108`.

```sh
colima start --profile dataslate-lake-qualification-261108 \
  --runtime containerd --cpu 4 --memory 6 --disk 12 --root-disk 8 \
  --mount none --activate=false --ssh-agent=false --ssh-config=false

# Render a NEW private directory on the host. It never starts a service.
python3 runtime/ingestion/prepare_lake_qualification.py \
  --directory /absolute/private/new-lake-fixture
```

The renderer creates files at mode 0600 and directories at mode 0700, refuses an existing destination, and generates two-hour JWTs. Only `runtime/` is copied into Trino: it contains the server TLS key, CA/public signing key and configuration. The JWT signing key and client tokens stay outside all container mounts. No cloud credentials are created. Do not print or commit these files.

Copy the runtime build context into `/tmp/dataslate-lake/runtime` inside the owned VM using a tar stream over `colima --profile ... ssh --`; this avoids adding a host mount. Build the unchanged pinned production Dockerfile with `colima --profile ... nerdctl -- build -t dataslate-ingestion:lake-v2 /tmp/dataslate-lake/runtime`. Record and launch the resulting immutable image digest. It is a locally built image, not a published release.

Copy only `qualify_image.py`, `jars.lock.json`, `tests/qualify_spark.py` and `tests/write_lake_fixture.py` into `/tmp/dataslate-lake/qualification`. Keep the runtime module out of that mount so tests import the module from the built image. Run `qualify_image.py` with the production image, networking disabled, 4 GiB memory and two CPUs, mounting this harness read-only at `/qualification`. All seventeen scenarios must pass; never silently rerun an unknown data mutation.

Create a new empty `/tmp/dataslate-lake/data` owned by UID 185. Run the synthetic fixture writer once:

```sh
colima --profile dataslate-lake-qualification-261108 nerdctl -- run --rm \
  --name dataslate-lake-writer --network none --memory 4g --cpus 3 \
  --mount type=bind,src=/tmp/dataslate-lake/qualification,dst=/qualification,ro \
  --mount type=bind,src=/tmp/dataslate-lake/data,dst=/lake \
  --env PYTHONPATH=/opt/dataslate/python:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip \
  --env SPARK_LOCAL_IP=127.0.0.1 \
  --env 'PYSPARK_SUBMIT_ARGS=--master local[2] --conf spark.ui.enabled=false --conf spark.ui.showConsoleProgress=false --conf spark.sql.shuffle.partitions=2 --conf spark.databricks.delta.snapshotPartitions=2 pyspark-shell' \
  --entrypoint python3 dataslate-ingestion:lake-v2 /qualification/write_lake_fixture.py
```

The writer refuses a nonempty data directory. It writes actual Delta version-zero tables and v2 receipts. The `orders` schema is `id long, region string, email string, amount double, active boolean, ordered_on date`. A separate `timestamp_precision` table demonstrates a fidelity limit. Local file support and an alternate receipt path are explicit test-only parameters; production storage authentication is unchanged.

Create a private container network named `dataslate-lake-qualification`. Start these pinned images in sequence after Spark has exited, conserving memory:

- `apache/hive:4.2.1@sha256:b19bb5bd01f233a3f99d941ebf08acaff68c55911dd5fa42ea03928e51860b91`, container name `dataslate-lake-hms`, 1 GiB limit, `SERVICE_NAME=metastore`, `HADOOP_HEAPSIZE=512`, and the data directory mounted read-only at `/lake`. Do not publish its Thrift port.
- `trinodb/trino:484@sha256:2704eb53935af5e582d63d6033e9bf99d20c325e9b0e30b1016ef59a4773979f`, container name `dataslate-lake-trino`, 3 GiB limit, `AWS_EC2_METADATA_DISABLED=true`, the same read-only `/lake` mount, and the renderer's minimal `runtime/` copied into the VM and mounted read-only at `/etc/trino`. Give UID 1000 access to this private runtime directory. Publish only `127.0.0.1:18443:8443`.

Both HMS and Trino need the same `/lake` path: HMS checks the registered directory. Trino's actual `local.location` property is the filesystem path `/`, while table locations are `file:///lake/.../`. This is solely the disposable shared-volume profile; production uses administrator-approved S3 locations.

Wait for verified TLS `/v1/info` to report version 484 and `starting:false`, then run the one-shot probe with a new evidence path:

```sh
python3 runtime/ingestion/qualify_lake_sql.py \
  --endpoint https://127.0.0.1:18443 \
  --private-fixture /absolute/private/new-lake-fixture \
  --output /absolute/private/new-lake-evidence.json
```

The probe registers only the two fixed synthetic tables. It never retries a mutation POST and refuses to overwrite evidence. Do not rerun it blindly against tables already registered: use a new disposable metastore for a full new run. Its structured checks cover version, location, protocol, masked US-only rows, ten authorization denials and timestamp precision. Reader and publisher identities are separate; the publisher owns exact targets and therefore is a privileged DDL identity, not a register-only credential. The broker cannot impersonate it.

The opt-in `TestLiveDeltaPublicationReadback` in `api/internal/trino/publication_live_test.go` independently exercises the production Go transport over this TLS endpoint and rejects a mismatched location. Consult that test for its environment variable names; load token paths without printing token values.

## Fidelity and cleanup

Spark wrote `2026-11-07T12:00:00.123456Z`; Trino returned `2026-11-07 12:00:00.123 UTC`. Therefore timestamp publication is excluded by the first qualified adapter. A successful registration is not proof that all source types retain their precision.

After preserving sanitized evidence and completing all consumers' probes, verify the ownership marker and delete only this Colima profile with its data. Remove only the generated private TLS/JWT fixture directories after checking their markers. Preserve logs, hashes, source and installed tools. The production image, metastore and synthetic lake are disposable and must not remain running as a service.
