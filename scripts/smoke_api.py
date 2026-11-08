"""Exercise the real foundation API through the Python client in ephemeral development mode."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
from uuid import uuid4

from dataslate import Client, DataSlateError


def expect_error(code, operation):
    try:
        operation()
    except DataSlateError as error:
        assert error.status == code, (error.status, error.code, str(error))
    else:
        raise AssertionError(f"Expected HTTP {code}")


client = Client(sys.argv[1])
status = client.status()
assert status["authMode"] == "development" and status["persistence"] == "memory", "Smoke test requires disposable development API"
assert status["product"] == "DataSlate" and status["assistant"] == "Sylon"
assert status["executionEnabled"] is False
assert status["executionMode"] == "disabled" and status["notebookExecutionEnabled"] is False
suffix = uuid4().hex[:10]
workspace = client.create_workspace("SDK integration " + suffix, "sdk-" + suffix)
other_workspace = client.create_workspace("SDK other " + suffix, "other-" + suffix)
workspace_id = workspace["id"]
assert client.execution_bindings(workspace_id) == {"items": []}
assert client.runs(workspace_id) == {"items": []}
assert client.sql_bindings(workspace_id) == {"items": []}
assert client.queries(workspace_id) == {"items": []}
expect_error(501, lambda: client.create_query(workspace_id,"unconfigured","nb-missing",1,"cell-1",idempotency_key="smoke-sql"))
expect_error(404, lambda: client.query_result(workspace_id,"qry-missing"))
assert client.publication_bindings(workspace_id) == {"items": []}
assert client.table_publications(workspace_id) == {"items": []}
assert client.published_tables(workspace_id) == {"items": []}
expect_error(501, lambda: client.create_table_publication(workspace_id,"run-missing","unconfigured",idempotency_key="smoke-publication"))
expect_error(404, lambda: client.ingestion_outcome(workspace_id,"run-missing"))
expect_error(404, lambda: client.table_publication(workspace_id,"pub-missing"))
expect_error(404, lambda: client.published_table(workspace_id,"unconfigured"))
assert client.catalog_bindings(workspace_id) == {"items": []}
expect_error(403, lambda: client.verify_catalog(workspace_id,"unconfigured"))
expect_error(403, lambda: client.catalog_table(workspace_id,"unconfigured",["analytics"],"orders"))
assert client.search_status(workspace_id) == {"items": []}
expect_error(403, lambda: client.search(workspace_id,"unconfigured","orders"))
expect_error(403, lambda: client.index_notebook(workspace_id,"unconfigured","nb-missing",1,idempotency_key="smoke-index"))
expect_error(404, lambda: client.search_index_job(workspace_id,"idx-missing"))
assert client.sylon_bindings(workspace_id) == {"items": []}
assert client.sylon_conversations(workspace_id) == {"items": []}
expect_error(403, lambda: client.create_sylon_conversation(workspace_id,"unconfigured","Smoke conversation"))
expect_error(404, lambda: client.sylon_conversation(workspace_id,"chat-missing"))
expect_error(404, lambda: client.sylon_job(workspace_id,"syl-missing"))
expect_error(404, lambda: client.cancel_sylon_job(workspace_id,"syl-missing"))
expect_error(404, lambda: client.send_sylon_message(workspace_id,"chat-missing","Explain this table",idempotency_key="smoke-sylon"))
assert client.sylon_memories(workspace_id) == {"items": []}
expect_error(404, lambda: client.sylon_memory(workspace_id, "mem-missing"))
expect_error(403, lambda: client.recall_sylon_memories(workspace_id, "unconfigured", "reporting convention"))
memory_expiry = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
expect_error(404, lambda: client.save_sylon_memory(workspace_id, "syl-missing", 1, "Reviewed memory", "No source exists.",
                                                 memory_expiry, idempotency_key="smoke-memory-save"))
expect_error(404, lambda: client.delete_sylon_memory(workspace_id, "mem-missing", 1, idempotency_key="smoke-memory-delete"))
expect_error(501, lambda: client.create_run(workspace_id, "unconfigured", idempotency_key="smoke-submission"))
expect_error(501, lambda: client.create_notebook_run(workspace_id, "unconfigured", "nb-missing", 1, idempotency_key="smoke-notebook"))
expect_error(404, lambda: client.run(workspace_id, "run-missing"))
expect_error(404, lambda: client.cancel_run(workspace_id, "run-missing"))
expect_error(404, lambda: client.run_events(workspace_id, "run-missing"))
expect_error(404, lambda: client.run_logs(workspace_id, "run-missing"))
expect_error(404, lambda: client.run_source(workspace_id, "run-missing"))
body = {"name": "API integration notebook", "kind": "notebook", "cells": [{"id": "cell-1", "language": "python", "source": "value = 42"}]}
notebook = client.create_notebook(workspace_id, body)
assert client.notebook(workspace_id, notebook["id"])["cells"] == body["cells"]
update = {**body, "revision": notebook["revision"], "cells": [{"id": "cell-1", "language": "python", "source": "value = 43"}]}
saved = client.update_notebook(workspace_id, notebook["id"], update)
assert saved["revision"] == notebook["revision"] + 1
assert client.notebook(workspace_id, notebook["id"])["cells"][0]["source"] == "value = 43"
expect_error(409, lambda: client.update_notebook(workspace_id, notebook["id"], update))
expect_error(404, lambda: client.notebook(other_workspace["id"], notebook["id"]))
pipeline_body = {
    "name": "Orders ingestion", "source": {
        "format": "json", "uri": "s3://dataslate-test/incoming/",
        "schema": [{"name": "order_id", "type": "long", "nullable": False}],
    },
    "destination": {"uri": "s3://dataslate-test/tables/orders/"},
    "checkpointUri": "s3://dataslate-test/checkpoints/orders/",
    "maxFilesPerTrigger": 10,
}
pipeline = client.create_ingestion_pipeline(workspace_id, pipeline_body)
pipeline_id = pipeline["id"]
assert any(p["id"] == pipeline_id for p in client.ingestion_pipelines(workspace_id)["items"])
assert client.ingestion_pipeline(workspace_id, pipeline_id)["source"] == pipeline_body["source"]
pipeline_update = {**pipeline_body, "name": "Orders ingestion renamed", "revision": pipeline["revision"]}
saved_pipeline = client.update_ingestion_pipeline(workspace_id, pipeline_id, pipeline_update)
assert saved_pipeline["revision"] == pipeline["revision"] + 1
expect_error(409, lambda: client.update_ingestion_pipeline(workspace_id, pipeline_id, pipeline_update))
expect_error(404, lambda: client.ingestion_pipeline(other_workspace["id"], pipeline_id))
expect_error(501, lambda: client.create_ingestion_run(workspace_id, "unconfigured", pipeline_id,
                                                     saved_pipeline["revision"], idempotency_key="smoke-ingestion"))
expect_error(409, lambda: client.delete_ingestion_pipeline(workspace_id, pipeline_id, pipeline["revision"]))
client.delete_ingestion_pipeline(workspace_id, pipeline_id, saved_pipeline["revision"])
expect_error(404, lambda: client.ingestion_pipeline(workspace_id, pipeline_id))
ids = {module["id"] for module in client.modules()["items"]}
assert {"spark", "unity-catalog", "mlflow", "agents", "feature-store", "apps", "sharing", "memory", "headroom"} <= ids
root = Path(__file__).resolve().parents[1]
plan = client.deployment_plan(json.loads((root / "examples/deployment-plan.json").read_text()))
assert plan["executable"] is False
assert all(action["verification"] == "required" for action in plan["actions"])
request = json.loads((root / "examples/spark-preview.json").read_text())
for operator, group in (("apache", "spark.apache.org/v1"), ("kubeflow", "sparkoperator.k8s.io/v1beta2")):
    result = client.spark_preview(workspace_id, {**request, "operator": operator})
    assert result["submitted"] is False
    assert result["manifest"]["apiVersion"] == group
print(json.dumps({"result": "passed", "checks": ["API/SDK contract", "disabled execution and owner-scoped routes", "unconfigured SQL denies submission", "unconfigured publication exposes no output or mutation", "unconfigured catalog denies metadata", "unconfigured search denies indexing and transfer", "unconfigured Sylon denies provider use", "private memory denies missing provenance and unconfigured recall", "notebook save/reopen", "revision conflict", "cross-workspace lookup", "pipeline lifecycle and revision fencing", "broader module inventory", "dependency preview", "Apache and Kubeflow manifests"], "liveEngineExecution": False}))
