"""Exercise the real foundation API through the Python client in ephemeral development mode."""

import json
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
suffix = uuid4().hex[:10]
workspace = client.create_workspace("SDK integration " + suffix, "sdk-" + suffix)
other_workspace = client.create_workspace("SDK other " + suffix, "other-" + suffix)
workspace_id = workspace["id"]
body = {"name": "API integration notebook", "kind": "notebook", "cells": [{"id": "cell-1", "language": "python", "source": "value = 42"}]}
notebook = client.create_notebook(workspace_id, body)
assert client.notebook(workspace_id, notebook["id"])["cells"] == body["cells"]
update = {**body, "revision": notebook["revision"], "cells": [{"id": "cell-1", "language": "python", "source": "value = 43"}]}
saved = client.update_notebook(workspace_id, notebook["id"], update)
assert saved["revision"] == notebook["revision"] + 1
assert client.notebook(workspace_id, notebook["id"])["cells"][0]["source"] == "value = 43"
expect_error(409, lambda: client.update_notebook(workspace_id, notebook["id"], update))
expect_error(404, lambda: client.notebook(other_workspace["id"], notebook["id"]))
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
print(json.dumps({"result": "passed", "checks": ["API/SDK contract", "notebook save/reopen", "revision conflict", "cross-workspace lookup", "broader module inventory", "dependency preview", "Apache and Kubeflow manifests"], "liveEngineExecution": False}))
