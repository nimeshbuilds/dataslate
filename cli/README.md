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
