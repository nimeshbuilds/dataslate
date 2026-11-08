# File ingestion contract

DataSlate file ingestion is an explicit saved-pipeline capability using the same durable Spark control plane as approved applications and Python notebook batches. It supports JSON Lines or Parquet file prefixes and append-only Snappy-compressed Delta publication. Creating a pipeline is authoring; it does not read storage, discover schema or submit Spark. Other connectors, CDC, continuous streaming, catalog publication, schema evolution, Azure and GCP execution remain separate capabilities.

## Resources and transport

Workspace routes under `/api/v1/workspaces/{workspaceId}`:

- `GET /ingestion-pipelines` returns `{items:[...]}`.
- `POST /ingestion-pipelines` creates a definition; `GET /ingestion-pipelines/{id}` reads it.
- `PUT /ingestion-pipelines/{id}` requires the complete definition and current `revision`. Only `name` can change after creation. This protects the identity of the input schema, destination and checkpoint for future runs.
- `DELETE /ingestion-pipelines/{id}?revision=N` deletes the definition with a revision fence and returns 204. Run snapshots and storage reservations remain.

Creation input:

```json
{
  "name": "Orders to Delta",
  "source": {
    "format": "json",
    "uri": "s3://customer-lake/raw/orders/",
    "schema": [
      {"name": "id", "type": "long", "nullable": false},
      {"name": "amount", "type": "double", "nullable": true}
    ]
  },
  "destination": {"uri": "s3://customer-lake/delta/orders/"},
  "checkpointUri": "s3://customer-lake/checkpoints/orders/",
  "maxFilesPerTrigger": 100
}
```

Responses add `id`, `workspaceId`, `createdAt`, `updatedAt`, `revision`. Workspace viewers read definitions; editors/admins can author, rename, delete and request execution. Execution still requires an explicit issuer-qualified binding grant. Shared definitions never contain run output or credentials.

The schema has 1–200 fields with case-insensitive unique names matching `[A-Za-z_][A-Za-z0-9_]{0,127}` and primitive types `string`, `long`, `double`, `boolean`, `date`, `timestamp`. The API defaults omitted `nullable` to false; generated runtime configuration always includes it. `maxFilesPerTrigger` is 1–10000. JSON records must supply all declared fields; nullable means a field may be null, not absent. The runtime rejects schema drift instead of silently publishing projected or coerced records. Full parsing behavior is specified in `runtime/ingestion/README.md`.

## Storage and execution grants

`executionKind: "file-ingestion-batch"` is opt-in. Its `mainApplicationFile` is empty; the platform generates the entrypoint from a saved revision and mounts an immutable ConfigMap. The approved image must contain `dataslate_ingestion` and its qualified Spark/Delta/S3 dependencies, and is pinned by SHA-256 digest. No submitted dependency URL, code, Spark option, arbitrary Hadoop option, browser credential or secret value is accepted.

The administrator-configured execution profile adds:

```json
{
  "ingestionStorage": {
    "readPrefixes": ["s3://customer-lake/raw/"],
    "writePrefixes": [
      "s3://customer-lake/delta/",
      "s3://customer-lake/checkpoints/"
    ]
  }
}
```

Each list has 1–100 distinct prefixes. Prefixes and resource locations are canonical `s3://bucket/nonempty-prefix/` strings, at most 2048 characters, with a trailing slash. Bucket roots, IP buckets, credentials, ports, escapes, wildcards, query strings, fragments, empty segments and traversal segments are rejected. Prefix segments begin with a letter, digit, underscore or hyphen and then contain only letters, digits, underscore, hyphen, period or equals sign. Source, destination and checkpoint prefixes must not overlap each other. Prefix grants use these canonical slash boundaries; `raw/` cannot authorize `raw-private/`.

At run acceptance, the saved source must fall inside a read grant and both write locations inside write grants. The complete profile is persisted with the run; the controller reloads bindings and compares the profile and subject grant before source creation and Spark submission. Grant revocation or profile changes request cancellation. The source snapshot remains independently available to its submitting subject after definition edits/deletion.

These API checks complement IAM; they do not provide a storage sandbox for arbitrary Python. Ingestion needs a dedicated engineering namespace and workload identity whose IAM limits storage to the designated prefixes. Do not give an arbitrary notebook binding or out-of-band Spark application the same write identity. The reservation protects DataSlate-managed submissions; it cannot fence another product or administrator writing directly to S3.

## Saved revision and replay

Submit through the existing run endpoint:

```json
{
  "bindingId": "file-ingestion",
  "idempotencyKey": "client-generated-intent-001",
  "pipeline": {"id": "pipe-example", "revision": 1}
}
```

The server snapshots the exact current revision transactionally with the run. `pipeline` and `notebook` are mutually exclusive and must match the binding kind. Acceptance returns 202; exact replay returns the same run with 200. A stale selection returns `ingestion_revision_conflict` (409). Reuse exactly the same key and selection after a lost response, including after the definition is renamed or deleted. Replay is resolved before rereading the definition. Membership and the current subject execution grant are still required.

`Run.ingestion` contains `{id,revision,sha256,bytes,sourceUri,destinationUri,checkpointUri}`. It is separate from notebook provenance. `GET /runs/{id}/source` returns `{source,ingestion}` with the exact Python wrapper and provenance, without an empty notebook object. The wrapper parses a JSON string as data and invokes `dataslate_ingestion.run` once with `{version:1,pipelineId,runId,pipelineRevision,source,destination,checkpointUri,maxFilesPerTrigger,timeoutSeconds}`. It uses no SQL interpolation, command invocation or user code.

Run reads, events, logs, source and cancellation require the exact submitting issuer/subject plus workspace membership. Workspace administrators do not gain implicit access to another person's outputs.

## Single writer and cleanup

On the first authorized execution, a global transaction permanently reserves the destination and checkpoint prefixes for the pipeline. Another pipeline cannot reserve an equal, ancestor or descendant destination/checkpoint, even across workspaces. Definition deletion does not release that reservation. Draft definitions do not reserve locations. Changing configuration requires a new definition with fresh write locations, not a checkpoint reset.

Only one run per pipeline can be active or unresolved. Unknown Spark submission, cancellation, failed source cleanup and incomplete writer drain all keep that gate closed. Existing workspace capacity also counts those obligations. Repeated API attempts do not implement whole-job retry; another run requires a new explicit intent after confirmed cleanup.

An ingestion run does not release its writer on a `succeeded`, `failed` or `cancelled` status alone. After terminal evidence, the controller:

1. Requires a durably known application UID if submission ever began. A lost POST with unknown UID never releases automatically.
2. Verifies the immutable SparkApplication, then requests UID-preconditioned foreground deletion, including for completed applications.
3. Waits for application absence and an empty list of Pods labeled with that run ID, then rechecks application absence. Unexpected matching Pods and truncated/error responses retain the writer; the controller never deletes Pods by a label alone.
4. Persists `writerReleased: true`. Source cleanup independently verifies/deletes the immutable source ConfigMap. Both obligations must finish before the next pipeline run.

Driver/executor templates and fixed Spark labels carry `dataslate.io/run`. The supported admission profile preserves that label on update; compute namespace permissions prevent unrelated users changing or fabricating ingestion Pods. Spark's driver and executor label configuration and pod lifecycle are documented in the [Spark 4.1.3 Kubernetes guide](https://spark.apache.org/docs/4.1.3/running-on-kubernetes.html).

Cleanup deletes completed driver Pods, so live log tails become unavailable afterward. This implementation does not yet archive logs or persist parsed ingestion receipts. Spark failure can occur after some microbatches committed to Delta; it does not mean all writes rolled back. Resume uses the same checkpoint and Delta transaction identity; readers can observe completed batches before the whole run succeeds. `--wait-cleanup` distinguishes terminal job status from completed cleanup.

## Persistence, errors and verification

Migration 004 creates pipeline definitions and permanent reservation records and indexes unresolved writers. PostgreSQL uses a global transaction advisory lock to serialize first reservations across workspaces and a workspace row lock for capacity/idempotency. Current pipeline rows are locked against rename/delete while snapshotting. Memory development mode mirrors these rules. Source snapshots remain in the independent run-source table introduced by migration 003.

Typed errors include `invalid_ingestion_pipeline` (422), `ingestion_config_immutable` (409), `ingestion_location_conflict` (409), `ingestion_writer_busy` (409), `ingestion_revision_conflict` (409), `ingestion_storage_denied` (403), `ingestion_not_executable` (422), plus the existing execution grant, workspace, revision, capacity and storage errors. Conflict messages do not reveal another workspace's pipeline identity.

Automated checks cover canonical paths/grants, role and owner boundaries, immutable revision snapshots, deleted-definition replay, same-key races, cross-workspace reservation races, source integrity, runtime wrapper interpretation, writer/source cleanup gating, OpenAPI response schemas, upstream SparkApplication schema and TLS Kubernetes drain fixtures. Real PostgreSQL tests exercise migration and transactional behavior. Live EKS/S3/IRSA, object store fault behavior, and Kubernetes ingestion label/drain behavior remain integrated qualification gates; passing fixtures or local Spark tests is not cloud qualification.
