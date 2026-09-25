**Ingestion and automatic table maintenance for the Kubernetes lakehouse**

Research date: September 25, 2026. This is a proposed product design supported by current upstream documentation; it has not been deployed or validated. It extends the main blueprint's multi-team, single-organization architecture. Recommendations below are product engineering judgments rather than claims that a single upstream project already supplies the whole workflow.

**Make Delta with Snappy-compressed Parquet the default for product-created ingestion tables.** This is a sensible default on any certified object-storage backend. Explicitly configure and record the codec in the immutable runtime/write profile, then verify actual output Parquet footers in certification. Spark currently documents Snappy as its default Parquet compression, but profiles, writer options and other engines can override it. Codec choice is a physical-file setting; Delta and Iceberg are table formats. Existing Iceberg tables retain their format and authoritative catalog, and existing files should not be silently converted when attached. [Spark Parquet configuration](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html).

Data files, transaction metadata, catalog registration and streaming checkpoints are four separate responsibilities. A successful Parquet upload is not a committed Delta table. The writer must commit through Delta, then record the resulting table version and satisfy any product publication/quality contract. Use cloud-native SDK/filesystem access rather than presenting an object-store mount as a generic POSIX disk. The cloud-storage agent's note owns exact AWS/Azure/GCP adapters and credentials.

**The Kafka connector should generate a recoverable pipeline, not just a consumer configuration.** Recommended product path:

```text
Existing Kafka / certified Kafka-compatible service
  -> isolated Spark Structured Streaming reader
  -> immutable bronze Delta event table (Snappy Parquet)
  -> validated, ordered transformation
  -> curated Delta table / governed query endpoint
```

A Kafka source produces key/value bytes and topic, partition, offset and timestamp metadata; headers are optional. Spark supports explicit starting positions and offset-rate limits. On restart, its checkpoint determines progress; a new starting-offset setting does not overwrite an existing checkpoint. Current documentation defaults `failOnDataLoss` to true and warns against concurrent queries sharing a forced consumer group ID. [Spark Kafka integration](https://spark.apache.org/docs/latest/streaming/structured-streaming-kafka-integration.html).

Our pipeline contract should preserve the raw payload, source metadata, parse status, schema ID/version and ingestion identity in bronze. Decode JSON/Avro/Protobuf with an explicit schema policy; a schema-registry adapter is separately certified. Treat a Kafka tombstone as a source-specific event, not automatically a business-table deletion. The connector wizard exposes initial snapshot/backfill versus streaming semantics, expected freshness, maximum throughput, schema changes and error handling in plain language.

For CDC, preserve source transaction/log sequence, primary key, operation and a deterministic event ordering tuple. Before each merge, reduce competing events for the same key according to that ordering. Delta documents ambiguous multi-match merges and shows preprocessing CDC changes before merge; its `foreachBatch` merge must be made idempotent. [Delta merge semantics](https://docs.delta.io/delta-update/).

Persist the last applied sequence per entity or equivalent durable tombstone state. A replayed old insert must not resurrect a row after a later delete. Do not use processing time as the authoritative source ordering. Backfills and live changes need a documented handover watermark, particularly when deletes or multiple source partitions are involved. Declare unsupported ordering contracts rather than silently choose a winner.

Delta's direct streaming sink provides transaction-aware writes. Arbitrary `foreachBatch` code does not become idempotent automatically; Delta supports `txnAppId` and increasing `txnVersion` for qualifying writer paths. Deleting the checkpoint and restarting with the same application identity can cause writes to be ignored when batch numbering starts over. [Delta streaming writes](https://docs.delta.io/delta-streaming/).

The product must therefore own a durable checkpoint URI and pipeline generation ID, allow only one active owner per checkpoint, and distinguish resume from reset/replay. Never expose “delete checkpoint and retry” as a generic repair. Fence duplicate driver launches, record source bounds and resulting table versions, and detect the commit-before-acknowledgment failure. Bronze, silver, quarantine and external side effects are not automatically one atomic multi-table transaction. Failed offset recovery must produce a visible repair workflow with loss/replay implications.

**Automatic maintenance should be an owned product service with format-specific executors.** We build a maintenance controller that evaluates table metrics and policies, then submits low-priority versioned runs through the normal execution API. It is not Databricks predictive optimization installed from OSS. Each table declares an owner, maintenance authority, policy version, retention contract and engine capabilities.

| Work category | Delta implementation candidate | Product behavior |
|---|---|---|
| Small-file control | Optimized writes, optional auto compaction, scheduled `OPTIMIZE` | Trigger from file count/size and recent writes; estimate resource cost and expose before/after metrics |
| Layout | Qualified Z-Order or other certified clustering profile | Use observed query predicates and statistics; keep layout changes separate from ordinary compaction |
| Obsolete data reclamation | `VACUUM` with retained safety checks | Eligible only for owned table roots with validated retention and consumer health |
| Transaction metadata | Format-managed checkpoints and log retention | Observe health and tune certified properties; never delete `_delta_log` files with generic object-store lifecycle rules |
| Query planning | Engine-specific statistics refresh | Qualify `ANALYZE` or equivalent for the selected catalog/engine; data-skipping statistics and SQL planner statistics are separate |
| Catalog visibility | Catalog reconciliation/cache invalidation | Verify committed version/schema visibility, including external writers; record lag explicitly |
| Derived compatibility metadata | Optional UniForm publication monitoring | Keep Delta authoritative; do not run native Iceberg maintenance against derived metadata |

Delta OSS supplies bin-packing `OPTIMIZE`, Z-Ordering, data-skipping statistics, optimized writes and auto compaction. Auto compaction runs synchronously after a successful write and can increase ingestion latency. Bin packing is idempotent; Z-Ordering may rewrite the same partitions again. Optimized writes may add shuffle cost. These operations should not all run after every tiny ingestion batch. [Delta OSS optimizations](https://docs.delta.io/optimizations-oss/).

`VACUUM` is not automatically scheduled by Delta OSS. Its default deleted-file retention is seven days; transaction log retention defaults to thirty days and cleanup follows checkpointing. These settings govern different histories. Vacuum can destroy the ability to time travel, and too-short retention can break active transactions or streams. The upstream command offers dry-run inspection. [Delta utility commands](https://docs.delta.io/delta-utility/).

Choose retention from the maximum promised time-travel window, concurrent transaction duration, stream outage/lag, downstream CDF recovery window and restore obligations, plus margin. Default periods are not universal guarantees. The controller should block destructive maintenance when a known consumer is behind the proposed boundary; unknown external consumers require an owner-approved retention contract. Keep streaming checkpoints in a dedicated owned prefix outside the table root, and exclude them from table cleanup.

Changing `delta.dataSkippingNumIndexedCols` affects future statistics collection; it does not retroactively compute statistics for old files. Transaction-identifier retention also limits the deduplication window for idempotent writes. Expose these as different settings rather than a single “metadata retention” knob. [Delta table properties](https://docs.delta.io/table-properties/).

Delta CDF records changes only after it is enabled, and its history is subject to table retention and vacuum. It is not a permanent event archive by itself. Retain immutable bronze events or another explicit durable history where replay requirements exceed CDF retention. [Delta change data feed](https://docs.delta.io/delta-change-data-feed/).

For optional UniForm, generation/publication depends on runtime and table path: Delta 4.3 added atomic/incremental conversion and 4.4 atomic initialization, while other conversion paths remain. Track the converted Delta version and catalog/reader visibility; wait for verified publication where an operation promises cross-engine freshness. Delta remains the only writer/maintenance authority for such a table. [Delta 4.3 release](https://github.com/delta-io/delta/releases/tag/v4.3.0), [Delta 4.4 release](https://github.com/delta-io/delta/releases/tag/v4.4.0), [detailed source findings](research-cloud-storage.md).

**Native Iceberg needs its own maintenance adapter.** It should expose data-file rewriting, manifest rewriting, position-delete-file rewriting where applicable, snapshot expiration and orphan cleanup as distinct actions. Expiration respects referenced snapshots/branches/tags; orphan deletion has its own age threshold and dry-run mode. The product must validate path normalization and use the chosen table catalog, not walk a bucket and infer which metadata file is newest. [Iceberg procedures](https://iceberg.apache.org/docs/latest/spark-procedures/).

Iceberg warns that too-short orphan retention can delete files belonging to in-progress writes; string/authority mismatches in paths can also cause misidentification. Choose a conservative threshold from the maximum supported write duration. Manifest rewriting addresses planning overhead; compacting data files addresses storage layout. [Iceberg maintenance](https://iceberg.apache.org/docs/latest/maintenance/).

**Use explicit ownership modes when attaching current lakes.** This prevents an otherwise convenient optimizer from becoming an unexpected deletion service.

| Mode | Allowed product behavior |
|---|---|
| Observe / query | Read permitted data and metadata; collect bounded diagnostics; recommend maintenance |
| Managed ingestion into a dedicated prefix | Write newly created product tables, keep separate checkpoints and staging, apply their declared maintenance policy |
| Cooperative writes | Write through the existing table's actual format/catalog and certified commit protocol; recognize external writers and retries |
| Delegated maintenance | Run specifically granted maintenance operations after the owner supplies retention and external-consumer requirements |

Do not default an attached table to destructive cleanup just because its credentials allow delete. Reject overlapping table roots and shared-file layouts for orphan/vacuum operations unless the adapter explicitly understands their ownership. Do not create an independently writable second catalog entry for the same physical table. Product leases serialize our maintenance runs, but cannot constrain external writers. Preserve native optimistic concurrency checks and retry a conflict by replanning from the current snapshot. Delta documents conflicts between compaction and mutations touching the same files. [Delta concurrency control](https://docs.delta.io/concurrency-control/).

**Make maintenance understandable in the UI.** The table page should show one of Off, Observe, Optimize or Optimize and Retain, plus owner, next window, estimated work, last run, retained history, consumer lag and reasons for deferral. Advanced configuration contains file-size targets, allowed operations, layout keys, maximum compute, namespace queue, daily budget and retention bounds. A preview explains the files/snapshots eligible for reclamation. Auto-maintenance is a standing policy granted by the owner; routine compliant runs should not prompt for repeated manual approval.

Suggested scheduling is an assessment after substantial ingestion plus a staggered periodic sweep. Queue only tables exceeding measured thresholds; debounce repeated commits, cap work per table/workspace and favor interactive capacity. Use a distributed lease and durable run ID. A cancelled run or restarted controller must not schedule a second destructive operation blindly. The default schedule and thresholds should be tuned by benchmarks rather than hard-coded as universal nightly full-table rewrites.

**Acceptance gates.** Certification should kill the driver before commit, after commit and before checkpoint acknowledgment; replay older updates after deletes; exhaust source retention; revoke source/storage credentials during refresh; resume after a long outage; introduce compatible and incompatible schema changes; and run external writes during compaction. Maintenance tests must show unchanged current query results, retention-respecting time travel, dry-run accuracy, survival of unrelated prefixes/checkpoints, and no execution without ownership. Test every supported cloud/engine/table-feature profile; URI compatibility alone does not certify the write and deletion semantics.
