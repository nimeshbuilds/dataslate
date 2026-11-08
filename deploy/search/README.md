# Search storage and embeddings

Search uses the platform PostgreSQL database. Lexical retrieval uses PostgreSQL full-text search and requires no extension. Hybrid retrieval additionally requires **pgvector 0.8.7** installed by the database operator. DataSlate migrations remain extension-free; the API/controller never create or upgrade extensions. See the [search contract](../../docs/implementation/search-contract.md) for authorization, freshness, indexing limits and lifecycle.

`upstream-lock.json` records the verified source archive and the immutable PostgreSQL 17/pgvector image used by CI. That test image is not a CloudNativePG-compatible production image. For managed CNPG, build and qualify an operator-compatible PostgreSQL image with the pinned extension files, image provenance and the same PostgreSQL major/architecture. For an external database, use the provider's supported extension mechanism and verify the exact version. Neither option silently replaces an existing database or upgrades its extensions.

After installing extension binaries in the server image, the database owner runs:

```sh
psql "$OPERATOR_DATABASE_URL" -f deploy/search/enable-vector.sql
```

The script creates the extension only if absent and fails if its version/schema differs. It does not perform upgrades. `public` must remain protected against application/user-created objects. Grant the actual application role schema USAGE and its application-table privileges; keep it separate from the database/extension owner. Preserve extension/version availability in standby, backup/restore and upgrade procedures. Validate restoration to a clean instance before enabling hybrid production use.

Base migrations store float arrays; hybrid queries explicitly cast to `public.vector` and call the schema-qualified cosine operator. Exact scoring runs only after current workspace, source revision, policy and catalog-grant filtering. Each binding permits at most 1,000 documents, each at most 32 chunks of 2,000 UTF-8 bytes. This profile does not claim an approximate index, billion-vector search or measured production throughput.

The administrator may select a local HTTPS embedding gateway or an approved external endpoint using the [embedding transport contract](../../api/internal/embeddings/README.md). Hybrid mode requires explicit transfer acceptance, a pinned model/dimension and mounted credentials; source code is a separate opt-in. The default source is notebook metadata. Search does not index data rows, execution results, private agent memory, storage locations or credentials. The UI must disclose external transfer for hybrid query text as well as document indexing.

For a lexical-only example binding:

```json
[{"id":"workspace-search","workspaceId":"ws-example","name":"Workspace search",
  "policyVersion":"1","mode":"lexical","maxDocuments":1000,
  "sources":{"notebooks":true,"includeCellSource":false,"catalogs":[]},
  "dataTransferAccepted":false}]
```

Mount the binding file into the API and the independent search controller. Hybrid mode additionally mounts embedding credentials into both. Catalog indexing mounts only the needed Polaris credentials into the controller. Neither workload requires Kubernetes API access. Exact network egress must permit the database and only the configured provider/catalog/identity endpoints.

To require actual vector testing rather than silently skip it:

```sh
cd api
DATASLATE_TEST_DATABASE_URL="$ISOLATED_TEST_DATABASE_URL" \
DATASLATE_TEST_REQUIRE_VECTOR=1 go test -race ./... -count=1
```

The current automated suite uses real PostgreSQL/pgvector and a local TLS embedding fixture; it does not call or bill an external provider. Model quality, cost, token limits, production identity, capacity, recovery and EKS deployment remain separate qualification gates. [pgvector upstream](https://github.com/pgvector/pgvector) documents extension installation and vector operations; [PostgreSQL text search](https://www.postgresql.org/docs/17/textsearch-controls.html) documents lexical ranking.
