# DataSlate Airflow provider

This Apache-2.0 provider lets a reviewed Airflow DAG submit and await existing DataSlate application, notebook, ingestion, publication and SQL operations. It uses the same DataSlate API, exact saved revisions, permissions and idempotency rules as the UI and CLI. Airflow owns scheduling, dependency order and orchestration retries; DataSlate owns each accepted operation and its backend reconciliation.

The qualified profile is **Airflow 3.4.0, task SDK 1.4.0 and Python 3.12**. Dependency constraints are copied from the [official release constraints](https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.12.txt); the source and SHA-256 are recorded in `constraints.lock.json`. Constraints list many optional providers; they do not install all of them or establish compatibility with them. The Git provider 1.0.1 is needed on Airflow services using the versioned Git bundle profile. Native local workflow qualification is recorded [separately](../../docs/implementation/workflow-qualification.md).

Install into the same reviewed environment used by the DAG processor and task workers, from this repository root:

```sh
python3.12 -m venv .venv-airflow
.venv-airflow/bin/python -m pip install \
  -c integrations/airflow/constraints-3.12.txt \
  ./sdk/python ./integrations/airflow

AIRFLOW_HOME=/tmp/dataslate-provider-tests \
  PYTHONPATH=sdk/python/src:integrations/airflow/src \
  .venv-airflow/bin/python -m unittest discover -s integrations/airflow/tests -v
```

Build immutable worker images for Kubernetes; do not install packages dynamically inside each task. Local test success does not qualify an arbitrary existing Airflow environment or its executors.

## Connection

Create an Airflow connection of type **DataSlate**, conventionally `dataslate_default`, through your approved secrets backend. Keep secret values out of Git, DAG arguments, templates and XComs. The `host` is the complete HTTPS DataSlate root URL (for example `https://dataslate.example.com`); leave Airflow's separate port/schema fields unset.

| Authentication | Login | Password | Extra fields |
|---|---|---|---|
| Bearer | Empty | Access token | `auth_mode: bearer` (default) |
| OAuth client credentials | Client ID | Client secret | `auth_mode: oauth_client_credentials`, `token_endpoint` |

OAuth uses HTTPS `client_secret_basic` and `grant_type=client_credentials`. Optional `scope`, `audience` and `resource` are forwarded only when explicitly configured. The identity provider must issue a token that DataSlate's configured issuer/audience accepts. This profile does not implement browser consent, refresh-token storage, mTLS, private-key JWT or user-subscription authentication.

Common optional extras: `ca_file` (absolute mounted CA path) and `request_timeout` (1–120 seconds, default 20). Private trust supplements normal certificate trust; certificate/hostname verification cannot be disabled. API and token requests refuse redirects. Unknown fields are rejected. OAuth responses are bounded, duplicate JSON fields rejected, and usable Bearer credentials require an expiry of 30–86400 seconds. Resolve connections on every operator execution/sensor poke so rotated bearer tokens or newly issued OAuth tokens are used; credentials never enter the returned operation reference.

The **DataSlate endpoint/installation and authenticated service principal must remain stable across retries and rescheduled observation**. Server idempotency is scoped to that identity. Rotating token bytes for the same principal is supported; switching principals or installations is a migration that needs explicit operational review. The provider does not decode unverified JWT claims to assert identity continuity.

## DAG operations

Use `DataSlateSubmitOperator` followed by `DataSlateOperationSensor`. The submit task returns a minimal reference; the sensor returns it only after verifying the corresponding outcome. The sensor defaults to `mode='reschedule'`, releasing its worker slot between observations. Its default interval is 30 seconds and timeout is 24 hours; choose explicit limits for the DAG. Sensors cannot soft-pass failures. Both operators resolve their DataSlate connection at task runtime.

| `operation` | Required request keys in addition to `workspace_id`, `binding_id` |
|---|---|
| `application` | None; the approved binding declares the application |
| `notebook` | `notebook_id`, exact saved `revision` |
| `ingestion` | `pipeline_id`, exact saved `revision` |
| `publication` | Successful ingestion `run_id` |
| `sql` | `notebook_id`, exact saved `revision`, `cell_id` |

Unknown keys and arbitrary source/SQL/engine parameters are rejected. The [orders example](examples/orders_daily.py) is a six-task ingestion → verified outcome → publication → governed SQL DAG. Replace its resource IDs/revisions and review its service-principal grants before committing it to a versioned GitDagBundle. Its UTC schedule is explicit, `catchup=False`, and `max_active_runs=1`.

The sensor publishes its minimal reference as `return_value` and, by default, separate keys (`multiple_outputs=True`), so `sensor.output['resourceId']` resolves correctly in a downstream task. Explicit `multiple_outputs=False` is supported for callers using only the full return value. XCom contains identifiers and saved selection/provenance, never result rows, logs, source content, output storage locations or credential values. Treat even operational identifiers as internal metadata.

Completion checks distinguish acknowledgement from outcome:

- Compute success needs an owned Kubernetes execution identity; saved notebook/ingestion source artifacts must have completed cleanup.
- Ingestion also waits for writer release and a matching verified version-2 runtime receipt. An empty successful ingestion may legitimately report no table; a later publication still requires an actual committed table.
- Publication needs independent verified completion, not just a registration acknowledgement.
- SQL needs engine-owned completion; no query rows are transferred through XCom.

Task grants belong to the **Airflow service principal**, not the user who clicked the workflow trigger. Give that principal only approved engineering and reader bindings, apply engine-side row/column policies for that identity, and do not treat a trigger grant as permission to browse its private results. An initiating user's policy is not automatically propagated into arbitrary DAG code.

## Retries, uncertainty and stopping

The recovery key hashes the Airflow DAG/run/task/map-index and operation. It excludes the try number, so a retry, clear or restart recovers the same logical DataSlate request. It also excludes mutable request parameters: editing a task's saved resource selection under the same identity must produce a server conflict instead of quietly submitting another job.

A failed DataSlate execution stays failed on retry; a **new explicitly reviewed DAG run** is required for a new logical execution. Airflow retries recover orchestration/transport failures, not a second independent whole-job retry policy. The submit call makes one request per task attempt; uncertain delivery is recovered with the same key. Sensors only GET, never submit replacements. Invalid references or upstream credentials produce sanitized task errors, without API response bodies or tokens.

A pending unknown operation continues to be observed until the configured sensor timeout. Known failure/cancellation and a final unknown publication fail the task for investigation. Sensor timeout, task clearing, or killing an Airflow worker does **not** prove remote termination and does not implicitly cancel DataSlate/Spark/SQL. Use the corresponding operation's own recovery/cancellation workflow and inspect its real backend outcome.

## Evidence and boundaries

The [recorded 23-check qualification](qualification.json) exercises actual installed Airflow operator/sensor classes, task-runner XCom formatting and XComArg resolution, provider discovery and real serialized DAG schema. TLS tests use a real HTTPS server to check OAuth exchange, private-CA verification, hostname rejection, redirect denial and token rotation. Operation-protocol cases use controlled client fixtures for saved identity, failure, evidence and cleanup rules. They do not claim a scheduled production Kubernetes execution of the complete example or a real cloud table/SQL pipeline. That joined deployment remains a separate qualification gate.
