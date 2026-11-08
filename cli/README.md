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
