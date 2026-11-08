# DataSlate CLI

The CLI is shipped with `sdk/python` and uses the same HTTP API and authorization as the workspace. Install with Python 3.11+: `python -m pip install ./sdk/python` from the repository root. Reinstall after SDK source changes; tests can run directly with `PYTHONPATH=sdk/python/src`.

```sh
dataslate status
dataslate workspace list
dataslate notebook list --workspace default
dataslate modules
dataslate deployment plan --file examples/deployment-plan.json
dataslate spark preview --workspace default --file examples/spark-preview.json
```

Use `DATASLATE_API_URL` for the server root (default `http://127.0.0.1:8080`). Production requires HTTPS. `DATASLATE_API_TOKEN` supplies an API-audience bearer token acquired through your IdP's approved flow. This foundation CLI has no interactive OAuth login yet. It does not accept secrets on the command line or persist tokens to disk.

`notebook create --file` and `notebook update --id ... --file` accept the JSON body documented in OpenAPI. Update requires the currently observed revision and can fail with a conflict rather than overwrite a concurrent edit. Output is JSON; errors are JSON on stderr with a nonzero exit code.

Deployment plans and Spark previews perform validation and generate artifacts. They do not install services or submit jobs. The UI, SDK and CLI all preserve the explicit `executable:false` / `submitted:false` outcome.


Approved batch execution uses administrator-provisioned immutable application bindings:

```sh
dataslate run bindings --workspace ws-example
dataslate run submit --workspace ws-example --binding approved-ingestion --idempotency-key ingestion-2026-11-07-001
dataslate run list --workspace ws-example
dataslate run get --workspace ws-example --id run-example
dataslate run watch --workspace ws-example --id run-example --max-wait 300
dataslate run events --workspace ws-example --id run-example
dataslate run logs --workspace ws-example --id run-example
dataslate run cancel --workspace ws-example --id run-example
```

Persist an idempotency key before submitting. If a submission response is lost, repeat the **same binding and key** to recover the original run. A different key creates another job. The SDK and CLI do not retry writes automatically. A queued response means the API recorded the request; inspect the observed state to determine whether Spark has actually run it. Cancellation is a request until the backend confirms it.

`run watch` writes changed states as JSON lines. It exits 0 for success, 3 for failed/cancelled terminal outcomes, 1 for API errors or an expired watch budget, and 130 on interruption. Ending a watch does not cancel a job. Each GET is additionally bounded by `--timeout`, so the elapsed watch duration can exceed `--max-wait` by one request timeout. Lists contain at most the 200 most recent owner-visible summaries; retrieve retained history with `run events`. Logs may contain sensitive application output and should be handled accordingly.

Python uses the same contract:

```python
run = client.create_run("ws-example", "approved-ingestion", idempotency_key="ingestion-2026-11-07-001")
for observed in client.watch_run("ws-example", run["id"], max_wait=300):
    print(observed["state"])
```

A binding with `executionKind: notebook-python-batch` explicitly authorizes saved Python notebook code under its engineering workload identity. Submit the exact saved notebook revision:

```sh
dataslate notebook create --workspace ws-example --file examples/python-batch-notebook.json
# Use the ID and revision from the creation response in the following command.
dataslate run submit-notebook --workspace ws-example --binding python-engineering --notebook nb-example --revision 7 --idempotency-key notebook-example-revision7-attempt1
dataslate run source --workspace ws-example --id run-example
```

The [example notebook](../examples/python-batch-notebook.json) computes a Spark aggregation and compares it with an independent Python calculation. A successful run prints `DATASLATE_EXAMPLE_RESULT {"rows": 100, "sumSquares": 328350}` in its actual driver logs. It does not access a lake or demonstrate table commits, governed SQL or charts.

```python
run = client.create_notebook_run(
    "ws-example", "python-engineering", "nb-example", 7,
    idempotency_key="notebook-example-revision7-attempt1",
)
snapshot = client.run_source("ws-example", run["id"])
```

Record the binding, notebook ID, revision and key before submitting. Recovery repeats **all four exact values**. The client never fetches or substitutes the latest revision. A first submission against an edited revision fails with a conflict; an already accepted request can be recovered after further edits or deletion while the execution grant remains valid. Source inspection uses the independent caller-owned run snapshot and does not change the shared notebook. Treat exported source as potentially sensitive.

Each notebook batch starts a fresh Python process and executes its saved Python cells in order. This is not an interactive notebook kernel, partial-cell re-execution or a rich table/chart result service. See the [execution contract](../docs/implementation/execution-contract.md) and [validation record](../docs/implementation/validation.md) for identity, quotas, recovery and qualification boundaries.

## Incremental file ingestion

Save a pipeline before submitting its exact revision. Replace the example S3
prefixes with locations granted by the administrator's ingestion binding:

```sh
dataslate pipeline create --workspace engineering --file examples/file-ingestion-pipeline.json
dataslate pipeline list --workspace engineering
dataslate run submit-pipeline --workspace engineering --binding orders-ingestion \
  --pipeline PIPELINE_ID --revision 1 --idempotency-key orders-import-001
dataslate run watch --workspace engineering --id RUN_ID --wait-cleanup
```

Persist the binding, pipeline ID, revision and idempotency key before submission.
If the response is interrupted, repeat that exact request. A new key requests a
new run which resumes the retained checkpoint; it does not reset or backfill it.
Only one pipeline writer is admitted until the previous workload and source
cleanup have drained. `--wait-cleanup` observes those obligations as well as the
application outcome. A failed run still exits with code 3 after cleanup.

`pipeline get`, `update` and `delete` use the same workspace-scoped API as the UI.
Deletion requires `--revision` so it cannot silently discard concurrent edits.
Updates require the full definition plus its expected `revision`; only the name
may change. Source, schema, destination and checkpoint are immutable because a
changed definition cannot safely inherit the same streaming checkpoint. Deletion
removes the definition while retaining accepted runs, storage reservations and
lake data; it never removes an S3 prefix or resets a checkpoint.

Governed SQL executes an exact saved SQL cell through a granted warehouse binding:

```sh
dataslate query bindings --workspace default
dataslate query submit --workspace default --binding analytics --notebook nb-example \
  --revision 3 --cell sql-cell --idempotency-key saved-sql-request-001
dataslate query watch --workspace default --id qry-example
dataslate query result --workspace default --id qry-example
```

Persist the full submission selection and key before sending. Repeat that exact
request after a lost response; a new key creates another query. `query source`,
`query list`, `query get` and `query cancel` use the same owner-scoped API as the UI.
Cancellation is a request until the engine confirms termination. Watch timeouts do
not cancel queries. Results keep scalars as strings, preserving bigint and decimal
precision; inspect `complete` and `truncated` before treating them as a full dataset.
Current workspace membership, subject grants and policy version govern historical
result access. Shared notebooks contain no execution outputs.

## Catalogs and search

Use the same authorized metadata and search resources as the workspace:

```sh
dataslate catalog bindings --workspace ws-team
dataslate catalog verify --workspace ws-team --binding polaris
dataslate catalog namespaces --workspace ws-team --binding polaris
dataslate catalog table --workspace ws-team --binding polaris --namespace '["analytics","finance"]' --name orders
dataslate search status --workspace ws-team
dataslate search query --workspace ws-team --binding workspace-search --query 'quarterly revenue'
dataslate search index-notebook --workspace ws-team --binding workspace-search --notebook nb-orders --revision 3 --idempotency-key index-orders-v3
dataslate search watch --workspace ws-team --id idx-example
```

Namespace/table lists identify current authorized grants; loading a table performs a real catalog read. Search defaults to lexical mode. `--mode hybrid` sends query text to the administrator-approved embedding provider. Results include current source revisions or catalog policy versions and timestamps; scores are rankings, not probabilities. Catalog snapshots expire and require an explicit `search index-table` refresh. Persist indexing selections and idempotency keys before submission. The CLI never automatically retries an uncertain provider/indexing operation; `unknown`, `superseded` and failed watch outcomes exit nonzero.

## Sylon conversations and proposals

Sylon uses an administrator-approved provider and explicit subject grants. Conversations and generation receipts are private to their owner. A message file preserves the exact prompt, source selection and idempotency key before submission:

```json
{"prompt":"Explain this SQL and suggest a clearer version.",
 "idempotencyKey":"sylon-orders-revision-3",
 "notebook":{"id":"nb-orders","revision":3,"cellId":"sql-cell"}}
```

```sh
dataslate sylon bindings --workspace ws-team
dataslate sylon create --workspace ws-team --binding approved-assistant --title 'Orders analysis'
dataslate sylon send --workspace ws-team --conversation chat-example --file /private/message.json
dataslate sylon watch --workspace ws-team --id syl-example
dataslate sylon get --workspace ws-team --id syl-example
```

Read the answer, server-provided source citations and proposed code. Applying a reviewed proposal is a separate command targeting the same saved cell/revision:

```sh
dataslate sylon apply --workspace ws-team --id syl-example --notebook nb-orders --revision 3 --cell sql-cell
```

Apply updates source once; it never executes SQL or Spark. Use an explicit `query submit` or `run submit-notebook` on the resulting saved revision when ready to execute. An intervening edit causes a revision conflict. `sylon cancel` requests cancellation/discard; a provider request already dispatched may still be processed or billed. An `unknown` watch outcome exits nonzero and does not generate another paid call. `sylon delete --conversation ...` erases the owned conversation and prevents late publication, including after binding revocation; it does not erase data already transferred to a provider.

Optional message contexts use `catalogs:[{bindingId,namespace,name}]` and `search:{bindingId,query,mode}`. The platform restricts them to current approved sources and transfer policies. Credentials, provider endpoints and arbitrary source text are never accepted as context fields. Keep message/answer files private when they contain sensitive data.


## Verified Delta publication

Use `run outcome`, `publication bindings/create/get/list/watch`, and `table list/get` to carry a verified ingestion output into governed SQL. Registration acceptance is not publication success. Persist the exact run, binding and key for recovery; no command silently reruns ingestion or registration. See the [UI, CLI and Python walkthrough](../docs/implementation/publication-clients.md).

## Airflow workflows

Use an administrator-approved Airflow 3.4.0 binding to inspect the current DAG, observed schedule and bounded history, then trigger its exact approved version. Airflow owns the schedule and task retries. No arbitrary DAG source, conf, schedule edits or backfill parameters are accepted by the trigger command.

```sh
dataslate workflow bindings --workspace engineering
dataslate workflow definition --workspace engineering --binding orders
dataslate workflow history --workspace engineering --binding orders
dataslate workflow trigger --workspace engineering --binding orders \
  --dag orders_daily --dag-version 3 --bundle-version REVIEWED_GIT_COMMIT --policy-version v1 \
  --idempotency-key orders-reviewed-20261108-001
dataslate workflow list --workspace engineering
dataslate workflow get --workspace engineering --id WORKFLOW_RUN_ID
dataslate workflow watch --workspace engineering --id WORKFLOW_RUN_ID --max-wait 600
```

Persist the binding, reviewed DAG ID/version, immutable bundle version, policy version and key before submitting. These exact fields are required; the client never fetches or substitutes a newer profile during submission or recovery. A changed binding conflicts before creating a new request. Repeat the complete exact request under the same identity to recover after an interrupted response. The SDK methods are `workflow_bindings`, `workflow_definition`, `workflow_history`, `create_workflow_run`, `workflow_runs`, `workflow_run` and `watch_workflow_run`. The watch generator only reads; it validates run identity and rejects revision regression.

`workflow watch` returns exit code `0` for confirmed success, `3` for failure, `4` for final unknown, `1` for API/response/timeout errors, `2` for invalid local input and `130` for keyboard interruption. A nonfinal unknown keeps being observed. A final unknown or a client timeout does not establish that the Airflow workers stopped. No simulated cancel command is provided. Inspect the same upstream run through the operator runbook instead of submitting a new key to resolve uncertainty.

Manual DataSlate runs are private to the initiating subject and current binding grant. The separately granted Airflow history intentionally includes scheduled and other users' manual-run metadata; it contains no logs, XComs, conf, source or query results. DAG tasks execute with their configured service principal, not the CLI user's token. See the [workflow deployment guide](../deploy/workflow/README.md) and [Airflow task provider](../integrations/airflow/README.md).

For private certificate authorities use `dataslate --ca-file /mounted/ca.crt ...`, `DATASLATE_CA_FILE`, or `Client(..., ca_file='/mounted/ca.crt')`. This adds trusted certificates and preserves certificate/hostname verification. Tokens remain in `DATASLATE_API_TOKEN`; do not place them in command arguments.
