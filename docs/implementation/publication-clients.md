# Delta publication through UI, CLI and Python

The workspace, CLI and SDK share the [publication contract](publication-contract.md). A successful Spark run is separate from a verified Delta outcome and a published SQL table. Publication never starts another ingestion run.

## Workspace

Open a completed ingestion run. The output panel shows whether its pinned runtime receipt was captured and verified before compute cleanup. A first empty drain has no table to publish. Processing counters include checkpoint replay and are not counts of newly inserted rows. A failed run may already have committed earlier batches.

Once a nonempty outcome is verified and the writer is released, review an explicitly granted publication target. Submission returns a durable operation. Registration acknowledgment alone is not success: the controller checks the registered lake location, retained Delta version, schema and protocol through Trino. An uncertain response is recovered using the same operation or exact original request; it does not trigger a second registration.

Published tables are listed under current reader grants. **Open in SQL** creates an unsaved draft using the current table and approved query binding. Saving and running are explicit actions. The existing result table, visualization and profile tabs retain their typed values and truncation rules. Later compatible appends may be visible: the preview is not a historical snapshot query. Publication rights do not confer read access.

The explicit publication reader grant exposes the entire declared column schema, including column names that Trino may deny to that reader. It grants no row or value access; engine policy still controls queries. Do not grant this metadata access when hidden column names are sensitive. Per-reader schema projection is not implemented.

## CLI

Use the installed `dataslate` command with your API URL and API access token. The API token is distinct from the worker's Trino credential.

```sh
dataslate run watch --workspace team --id run-example --wait-cleanup
dataslate run outcome --workspace team --id run-example
dataslate publication bindings --workspace team

# Save this exact selection and key before sending. Reuse all three after a lost response.
dataslate publication create --workspace team --run run-example \
  --binding publish-orders --idempotency-key publish-orders-run-example-1

dataslate publication watch --workspace team --id pub-example
dataslate table list --workspace team
dataslate table get --workspace team --id publish-orders
```

`publication watch` uses GET requests only. It exits `0` for verified publication, `3` for failure, and `4` for an unknown result. A watch timeout or interruption does not cancel or resubmit the backend operation. Unknown registration can be reconciled during its configured observation window; an unresolved final operation retains its target reservation for administrator investigation. Do not use a new key as a recovery mechanism. `table get` returns a latest-table SQL draft and never executes it.

## Python

```python
from dataslate import Client

client = Client("https://dataslate.example.com", token=api_access_token)
outcome = client.ingestion_outcome("team", "run-example")
accepted = client.create_table_publication(
    "team", "run-example", "publish-orders",
    idempotency_key="publish-orders-run-example-1",
)
operation_id = accepted["publication"]["id"]
for operation in client.watch_table_publication("team", operation_id):
    print(operation["state"])

if operation["state"] == "published":
    table = client.published_table("team", "publish-orders")
    print(table["sql"]["draft"])
```

Keep Delta versions as canonical decimal strings. They can exceed JavaScript's safe integer range. The SDK checks publication acceptance identity and revision, validates verified-success receipts, and never generates replacement keys or retries a mutation automatically.

## Current compatibility and evidence

Publication supports string, long, double, boolean and date columns. Spark timestamp ingestion remains available, but publication rejects those tables because the pinned Trino reader reduces timestamp precision. Nested or unqualified types are not silently coerced.

The runtime table UUID is attested by the pinned ingestion driver. Trino independently checks location, retained version, ordered schema and protocol/feature union; it does not independently verify that UUID. The [native lake qualification](../../runtime/ingestion/LAKE_QUALIFICATION.md) uses synthetic local storage and private TLS/JWT fixtures. EKS, S3, IRSA, production OIDC and operating a highly available metastore remain separate qualification gates.
