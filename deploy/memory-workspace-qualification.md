# Private memory browser qualification

This helper joins the real DataSlate handlers, PostgreSQL store, generation controller and HTTPS model transport in one **test-only process**. It uses the explicit loopback development identity and a local Responses protocol fixture. It is not the production executable, an OIDC qualification, an external model evaluation or an EKS deployment.

The browser suite has no API route interception. Its prepare phase saves and corrects a memory; its resume phase checks the same record after the helper process restarts, explicitly selects it for another response, deletes it and verifies that dependent history becomes unavailable. The helper's separate tests cover ownership refusal, private atomic files, cumulative request evidence and malformed protocol inputs.

## Disposable fixture and ownership

Use an explicitly owned PostgreSQL qualification database on `127.0.0.1`, with verified TLS. The private fixture JSON must be a regular file with mode `0600`:

```json
{
  "owned": true,
  "database": "dataslate_memory_qualification",
  "adminURL": "postgresql://<fixture-user>:<fixture-password>@127.0.0.1:<fixture-port>/dataslate_memory_qualification?sslmode=verify-full&sslrootcert=/absolute/fixture/root.crt"
}
```

These are placeholders, not credentials. Keep the actual file outside version control and never print it. The named database must match `^dataslate_[a-z_]+qualification$`; the helper migrates that database. Provisioning and final database teardown belong to the fixture owner.

Do not run competing Sylon controllers against this database. Job leases are database-wide: a second helper with different bindings can claim jobs belonging to the first helper. A different workspace alone does not isolate workers. Stop the ordinary API before using port `8080`, and stop an existing Vite process before starting the browser suite on `5173`.

The state directory must either be absent or already contain this helper's matching ownership marker. Its parent must exist. The helper refuses unrelated directories, symlinked managed files, public file permissions, another database endpoint and changed retained assistant profiles. It does not repair ownership by changing permissions on existing directories.

## Build and helper tests

From the repository root, copy the standalone source beneath `api/` so Go's internal-package boundary is preserved:

```sh
mkdir -p api/.qa/memory-workspace
cp scripts/qualify-memory-workspace.go api/.qa/memory-workspace/main.go
cp scripts/qualify-memory-workspace_test.go api/.qa/memory-workspace/main_test.go
(cd api && go test -race ./.qa/memory-workspace)
(cd api && go build -o /tmp/dataslate-memory-workspace ./.qa/memory-workspace)
```

Create a fresh private parent, then start the helper in a dedicated terminal. Substitute the absolute path of the owned fixture; do not put its URL or password on the command line.

```sh
QUALIFICATION_ROOT=$(mktemp -d /tmp/dataslate-memory-browser.XXXXXX)
/tmp/dataslate-memory-workspace \
  -fixture /absolute/private/memory-postgres.json \
  -state-dir "$QUALIFICATION_ROOT/state" \
  -addr 127.0.0.1:8080
```

The helper binds only literal IPv4 loopback. Its provider uses an independently allocated loopback HTTPS port. Retained state pins that provider port, token path, workspace, database identity and full binding hash. The same private state directory must be used after restart. Keep the parent path available in the terminals used below.

## Prepare, restart and resume

Install the web dependencies from the lockfile using the repository's supported Node version. In another terminal, set `QUALIFICATION_ROOT` to the exact parent created above, then run:

```sh
cd web
DATASLATE_MEMORY_PHASE=prepare \
DATASLATE_MEMORY_STATE_FILE="$QUALIFICATION_ROOT/state/state.json" \
DATASLATE_MEMORY_EVIDENCE_DIR="$QUALIFICATION_ROOT/browser-evidence" \
npm run test:memory-live
```

The test configuration starts and stops its own Vite process. After prepare passes, stop the helper with `Ctrl+C` and wait for a successful exit. Start it again with **the same fixture, state directory and API address**, then run the browser test with `DATASLATE_MEMORY_PHASE=resume` and the same state/evidence paths.

A retained provider port conflict or changed profile is a hard failure. Do not silently select another port or delete state to make the restart check pass. New settings require a fresh qualification run. The helper cannot import a state directory from an older harness profile; preserve earlier evidence separately.

## Evidence and cleanup

`state/provider-evidence.json` contains cumulative attempts, accepted and rejected requests, typed memory citation counts, and individual process sessions. Attempts are persisted before validation; accepted means the local fixture validated the request, not that a client received the response. An attempt without an accepted/rejected outcome remains visible after interruption. Normal session stops and failed sessions are recorded; a later startup marks an unclosed session as interrupted without inventing its exit time. Evidence writes are private and atomic, and an I/O failure fails the helper.

The provider fixture validates the actual structured Responses input, strict JSON schema, `store:false`, `stream:false`, token bound and absence of tools. It returns a deterministic answer with no invented usage counts. It does not measure model quality, external provider behavior, token savings or production authentication.

After resume passes, stop the helper and wait for its joined worker and HTTP/database shutdown. Preserve only the sanitized evidence and reviewed screenshots needed for the report. Check the private state's `owner.json` before removing that run's generated token, CA and state directory. The fixture owner then stops and removes the explicitly owned PostgreSQL database/server and its credentials. Never remove an unrelated database, listener or state directory. Sanitized source hashes and test results should identify exactly which helper revision produced the evidence.
