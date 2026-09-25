**Existing cloud lakes, automatic maintenance, and an integrated AI workspace**

**Latest additions:** The [consolidated component plan](platform-components-and-naming.md) includes the full stack and replacement-name suggestions. [Result visualization](research-results-visualization.md) defines one shared table/chart/profile experience for SQL, Spark display and AI. [Agent memory](research-agent-memory.md) separates durable state, permission-aware long-term memory and optional Headroom context compression.

September 24–25, 2026. This extends the [Kubernetes lakehouse blueprint](kubernetes-lakehouse-product-blueprint.md) for the latest requirements. The deployment serves teams in one organization. Research establishes candidate components and contracts; cloud writes, identity exchanges, maintenance safety, AI quality and runtime combinations still require certification. The accompanying interface concept uses sample states and no live services.

**The product connects to the organization's lake.** The durable lake lives in AWS S3, Azure Blob Storage/ADLS Gen2 or Google Cloud Storage. These are object stores. Attached EBS volumes, Azure Managed Disks and Google Persistent Disk/Hyperdisk serve component-specific PVCs, caches and temporary execution storage. Azure's term “block blob” does not mean an attached block device. Avoid making an object-storage mount look like a generic POSIX filesystem to transactional writers. [S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingObjects.html), [Azure storage](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blobs-introduction), [GCS](https://docs.cloud.google.com/storage/docs/introduction).

Expose four independent properties in metadata and APIs:

| Layer | Examples | What it determines |
|---|---|---|
| Storage | S3, ADLS Gen2, GCS | Endpoints, identities, network access, encryption and IO |
| Table format | Delta, Iceberg, or plain files | Transactions, snapshots, schema evolution, deletes and concurrency |
| File format | Parquet, JSON, CSV, ORC, Avro | Physical parsing/serialization |
| Compression | Snappy, Zstd, gzip, none as supported | Physical encoding, CPU/IO tradeoffs and reader compatibility |

Thus “Delta table, Parquet data files, Snappy compression, ADLS Gen2 storage” is one valid profile. Iceberg is not another compression or a generic directory reader.

**Cloud adapters must cover both IO and identity.**

| Cloud lake | Typical Spark IO | Preferred identity integration |
|---|---|---|
| AWS S3 | Hadoop S3A; `s3a://bucket/prefix` | Scoped IAM roles with qualified WebIdentity/IRSA or Pod Identity credential provider |
| Azure ADLS Gen2 | ABFS; `abfss://container@account.dfs.core.windows.net/prefix` | Entra workload identity federation; validate effective RBAC/ABAC and ACL permissions |
| Google Cloud Storage | Google Hadoop connector; `gs://bucket/prefix` | GKE workload federation or configured external-Kubernetes federation/ADC |
| Other S3-compatible lake | Qualified endpoint/connector configuration | Its supported scoped credentials; separate storage conformance tests |

Configure trust, credential refresh and SDK dependencies for the actual driver, executors and catalog service. ABFS supporting workload identity does not prove that the selected UC Azure credential-vending code supports the same configuration. Certify server-side credentials and client-side vended credentials separately. Broad credentials must never become an automatic fallback for an RLS-restricted workload. [Hadoop S3A auth](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/authentication.html), [ABFS auth](https://hadoop.apache.org/docs/current/hadoop-azure/index.html), [Google connector configuration](https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md).

On Azure, directory ACLs cannot narrow access already granted by an applicable broad data role. Test effective access across the combined authorization model rather than assuming a restrictive-looking ACL overrides that role. [Azure authorization model](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-access-control-model).

Kubernetes can run outside the cloud containing the lake if supported federation and networking are configured. Prefer compute near storage; cross-cloud transfer costs and latency must be measured. Private endpoints, custom CAs, proxies, KMS permissions and credential rotation belong in the connection test, not in an operator's undocumented setup steps. [Entra federation](https://learn.microsoft.com/en-us/entra/workload-id/workload-identity-federation), [Google external Kubernetes federation](https://docs.cloud.google.com/iam/docs/workload-identity-federation-with-kubernetes).

**Connecting a lake must preserve its ownership.** The wizard asks for endpoint/catalog, permitted prefixes, authentication profile, intended capabilities and discovery scope. It verifies those capabilities and reports exactly what is usable. Connection does not require copying data into a new bucket or converting existing tables.

Use four explicit attachment modes:

- **Observe:** browse and query permitted assets; no writes or maintenance.
- **Managed prefix:** our product creates tables inside an explicitly delegated prefix and owns their lifecycle.
- **Cooperative writes:** existing tables accept writes only under a certified shared commit protocol and ownership agreement.
- **Delegated maintenance:** maintenance authority is separately granted for selected tables and retention rules.

Having a credential capable of deleting objects does not establish that the product is authorized to delete a customer's table history. Routine maintenance within a configured policy can run automatically; the onboarding policy defines that authority once.

**Dynamic discovery should be metadata-aware and bounded.** Read existing catalog entries first. For file prefixes, use a budgeted scanner, optional object notifications and periodic reconciliation. Record the discovery source, current verification, schema fingerprint, format features and supported readers/writers. Object events are discovery hints, not transaction commits.

| Dataset | Required discovery/read behavior |
|---|---|
| JSON/JSON Lines/CSV | Preview with parsing options, sample boundaries, proposed schema and malformed-record handling; review ambiguous inference |
| Plain Parquet | Read schema/footer metadata, detect codecs, nested fields, schema inconsistencies and partition conventions |
| Delta | Read through the Delta protocol; use its catalog when catalog-managed; validate table reader/writer features |
| Iceberg | Follow the authoritative catalog and snapshot/manifests, including deletes; never pick a metadata file by modification time |
| ORC/Avro/additional formats | Add explicit reader profiles, separately declaring write, streaming and SQL support |
| Mixed or unknown objects | Show candidates and unsupported content; avoid silently guessing a table |

A Delta/Iceberg table can contain Parquet files that are no longer live. Listing and reading every Parquet file directly can return incorrect or deleted data. Automatic discovery also must not broaden grants or adopt a second writable catalog. [Delta protocol](https://github.com/delta-io/delta/blob/v4.4.0/PROTOCOL.md), [Iceberg specification](https://iceberg.apache.org/spec/), [Spark JSON support](https://spark.apache.org/docs/4.1.3/sql-data-sources-json.html).

**Default new ingestion output to Delta with Snappy-compressed Parquet.** Honor the requested default explicitly in each immutable write profile. Apply it to ingestion and subsequent compaction, and inspect output footers in certification. Existing connected assets retain their formats/codecs. A codec policy change affects new files unless a rewrite is scheduled.

This is supported by the Spark/Delta writer path. It does not mean every Delta metadata object is a Snappy Parquet data file. Also, current Databricks managed-table defaults are not universally Snappy: new managed tables on Runtime 16+ changed to Zstd in October 2025. Offer Zstd as an explicit alternative with measured CPU/storage tradeoffs. [Spark Parquet](https://spark.apache.org/docs/4.1.3/sql-data-sources-parquet.html), [Delta write implementation](https://github.com/delta-io/delta/blob/v4.4.0/spark/src/main/scala/org/apache/spark/sql/delta/files/TransactionalWrite.scala), [Databricks release notes](https://docs.databricks.com/aws/en/release-notes/product/2025/october).

For Kafka, use a certified Structured Streaming reader writing an immutable bronze Delta event table, followed by validated transformations/ordered merges into curated tables. Preserve topic, partition, offset, key, event/source ordering, payload/schema identity and parse outcome. Tombstones and CDC deletes require source-specific semantics. Stable exclusive checkpoints determine restart position; resume, replay and reset are separate actions. A generic connector emitting Parquet does not produce a Delta table without a Delta commit. [Spark Kafka source](https://spark.apache.org/docs/latest/streaming/structured-streaming-kafka-integration.html), [Delta streaming](https://docs.delta.io/delta-streaming/).

For batch connectors, use extraction → immutable staging manifest → candidate transformation/checks → controlled table commit/publication. Retain rejected records and report schema drift. Blocking quality checks precede publication; post-publication monitoring cannot promise that bad data was never visible. Record each output version and reconcile uncertain commits before retrying.

**Table commits must be correct for each storage profile.** Qualify UC catalog-managed Delta commits for new product-owned tables across the three clouds. The catalog ratifies commits in that model; clients must use the managing catalog. This is a protocol feature, not just giving an external path a catalog name. It also becomes an explicit qualification item for the Kyuubi/Ranger, maintenance and native-acceleration combinations in the master blueprint. [Catalog-managed Delta](https://docs.delta.io/delta-catalog-managed-tables/), [OSS UC Delta API](https://delta.io/blog/2026-07-03-unity-catalog-delta-api/).

Existing external Delta tables retain their commit protocol. Current S3 LogStore guidance has different single-driver and coordinated multi-driver paths. S3 now supports conditional writes, but that does not mean every selected Delta/Hadoop implementation uses them for safe commits. Test concurrent independent writers and failure recovery using the exact libraries and coordination backend. Product database locks cannot coordinate external writers that ignore them. [Delta storage](https://docs.delta.io/delta-storage/), [AWS conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html).

**Automatic maintenance is a policy-controlled service.** Build a maintenance controller that evaluates table health, creates ordinary versioned product runs, schedules within budget/windows and records outcomes. Use one schedule authority; it can publish work through Airflow where enabled. Product leases avoid duplicate internal jobs, while native table concurrency checks handle real commits.

| Work | Delta implementation/contract | Iceberg implementation/contract |
|---|---|---|
| Small-file reduction | OPTIMIZE/bin packing; qualified optimized writes/auto compaction | Rewrite data files |
| Layout | Optional workload-driven Z-Ordering or supported layout profile | Supported sort/partition-aware rewrites |
| Metadata efficiency | Native transaction checkpoints/log retention; catalog state verification | Manifest rewrites and catalog metadata maintenance |
| Query planning | Engine-specific statistics, distinct from transaction metadata | Engine-specific statistics and supported table statistics |
| Historical reclamation | VACUUM under retention contract | Snapshot expiry and orphan/delete-file procedures under retention contract |
| Reader visibility | Commit/catalog/UniForm/cache verification as applicable | Catalog snapshot and reader-cache verification |

Delta OSS provides optimization primitives, but its VACUUM is not automatically scheduled for us. Auto compaction is synchronous post-write work and needs a latency/cost policy. Iceberg maintenance uses its own procedures; never run native Iceberg cleanup against Delta UniForm's derived metadata. [Delta optimization](https://docs.delta.io/optimizations-oss/), [Delta utility commands](https://docs.delta.io/delta-utility/), [Iceberg procedures](https://iceberg.apache.org/docs/latest/spark-procedures/).

Expose **Off, Observe, Optimize, Optimize and Retain** profiles. Show next run, fragmentation, expected work, maintenance budget, retained history and deferral reason. Destructive retention accounts for the promised time-travel window, longest active operation, streaming lag/outage, change-feed consumers and recovery obligations. Unknown external consumers require an agreed retention contract. Do not shorten retention merely because a generic default is seven days. Do not let bucket lifecycle rules independently remove live table/log/checkpoint objects.

UniForm freshness needs a correction to the earlier research: Delta 4.3 introduced atomic/incremental conversion and 4.4 atomic initialization, while other conversion paths remain. Track the actual commit, catalog publication and reader cache; qualify behavior per runtime/table feature instead of assuming all conversion is asynchronous. Maintain one authoritative Delta writer. [Delta 4.3](https://github.com/delta-io/delta/releases/tag/v4.3.0), [Delta 4.4](https://github.com/delta-io/delta/releases/tag/v4.4.0).

**LLM connections need protocol, authentication and capability contracts.** Add a versioned ModelConnection containing endpoint type, API version, model/deployment aliases, region/project, allowed URL, trust bundle, auth profile, secret references, capabilities, timeouts, limits and data-sharing policy. Support an existing organization gateway without requiring another gateway deployment.

Keep application sign-in, provider authentication and data-tool authorization separate. OAuth is the protocol; JWT is one token format. A UI ID token, a JWT client assertion and an LLM access token are not interchangeable. Configure actual token audience, scope, grant, issuer and delegation. Accept opaque access tokens where the provider uses them. [JWT client assertions](https://datatracker.ietf.org/doc/rfc7523/), [JWT access-token profile](https://www.rfc-editor.org/rfc/rfc9068.html).

| Endpoint type | Authentication profiles to qualify |
|---|---|
| Organization gateway | API token/approved header, OAuth client credentials, private-key client assertion, supported token exchange/on-behalf-of, optional mTLS |
| OpenAI and Anthropic | API keys and documented workload identity federation with short-lived API access tokens |
| Azure OpenAI / applicable Foundry endpoints | Entra workload identity/managed identity; supported resource keys |
| Google Vertex AI | ADC/workload federation and applicable supported endpoint authentication |
| Gemini Developer API | Documented API-key interface, separate from Vertex configuration |
| Amazon Bedrock | Temporary-role SDK/SigV4; documented bearer-key modes as separately qualified |
| xAI, Mistral, Cohere, Groq and others | Native provider adapter or tested compatible API, with its supported credentials |
| Self-operated model server | Organization-controlled endpoint authentication and capability profile |

OpenAI and Anthropic now document workload federation; they should not be described as API-key-only. Use SDK credential implementations where suitable, refresh/renew server-side and partition token caches by connection, audience, scopes and delegation. Never silently switch to an old key or another provider after a failed exchange. Personal coding subscriptions remain a separate official-client integration. [OpenAI federation](https://developers.openai.com/api/docs/guides/workload-identity-federation), [Anthropic federation](https://platform.claude.com/docs/en/manage-claude/workload-identity-federation), [Azure keyless connections](https://learn.microsoft.com/en-us/azure/developer/ai/keyless-connections), [Google ADC](https://docs.cloud.google.com/docs/authentication/application-default-credentials), [Bedrock keys](https://docs.aws.amazon.com/bedrock/latest/userguide/api-keys.html).

An “OpenAI-compatible” URL does not establish identical streaming, tool calls, structured outputs, embeddings, usage reporting or cancellation. Run connection/model contract tests and allow manual model IDs for gateways that disable enumeration. Suggested task aliases are `code-fast`, `code-deep` and `data-analysis`; route only within approved data-handling boundaries. Stop cancels further product tools and the transport/provider request where supported, without assuming the provider ceased billing.

**AI should appear inside the development workflow.** Reuse Jupyter's document/editor and kernel interfaces where practical, while building the product shell, resource model, SQL service, context assembly and permissions. Ordinary language-server/kernel completion, formatting and diagnostics remain available without AI. [JupyterLab components](https://jupyterlab.readthedocs.io/en/stable/extension/components.html).

| Surface | Intended experience |
|---|---|
| Notebook cell | Fast cancellable ghost text; prompt-to-cell; explain, refactor, document and generate tests; reviewable edits |
| Failed cell | Original error plus inline Explain/Suggest fix; relevant code, environment, schema and run context; explicit apply and rerun |
| Notebook document | Multi-cell proposals anchored to stable cell IDs/revisions; conflict detection, undo and source diffs |
| SQL editor | Authorized schema autocomplete, exact-dialect generation, explain plan, performance/permission diagnosis, parameterized queries and reusable results |
| Job/pipeline run | Diagnose the exact deployed revision and attempt; propose changes through the normal promotion path |
| Data conversation | Governed questions, SQL/results/charts, metric definitions, freshness and saved query/dashboard actions |

This matches the interaction categories in current Genie Code documentation, while remaining our own implementation. A model connection enables the capability; it does not automatically provide business semantics or successful debugging. [Databricks Genie Code](https://docs.databricks.com/aws/en/genie-code/features-capabilities), [notebook AI workflow](https://docs.databricks.com/gcp/en/notebooks/code-assistant).

Autosave, history, keyboard navigation, rich results, bounded outputs, dependable reconnect and visible compute status are essential editor work. Never freeze typing while waiting for completion. Cancel superseded requests; discard stale completions. Edits carry a base revision and cell IDs, use a diff and preserve undo. Applying code and executing it are distinct actions. Collaboration shares the document, not execution credentials.

The context service uses the caller's authorized code, relevant cells, schemas, reviewed metrics, runtime and bounded diagnostics. Row samples are separately controlled, and secrets are excluded. External data, table comments and errors remain untrusted inputs; they cannot change tool privileges or provider destinations. Data-query tools use the asking user's permissions even if a shared workload credential pays for inference. Saved conversations, outputs and suggestions need their own access checks.

Mini Genie setup selects tables, owners, definitions, joins, synonyms, calendars and reviewed example questions. Evaluate answers on actual query results and denied-access cases. Evaluate code fixes by meaningful validation/reruns, rather than whether the error disappeared. A suggested TRY_CAST that hides invalid data is incomplete unless null/rejection handling is explicit. Model/gateway upgrades rerun the evaluation pack before changing a production alias.

**Add these release gates to the main blueprint.** Read existing lakes without mutation; preserve Iceberg/Delta snapshots and deletes; verify Snappy in real output; run two independent writers and crash at commit boundaries; rotate cloud and LLM credentials during long operations; keep slow streaming/external readers safe during maintenance; test wrong-audience JWTs and provider outages; reject stale collaborative AI patches; preserve RLS through every AI tool; demonstrate notebook/SQL usability when AI is unavailable. Publish support per cloud, table feature, writer, engine and model connection—not a blanket claim that every dataset or gateway behaves identically.

Supporting evidence: [cloud storage](research-cloud-storage.md), [ingestion and maintenance](research-maintenance-ingestion.md), and [AI authentication and editor experience](research-ai-experience.md).
