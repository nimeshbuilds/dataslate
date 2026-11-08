#!/usr/bin/env bash
# Run the local source/integration checks without silently skipping PostgreSQL.
# This does not create infrastructure, build OCI images or certify an EKS install.
set -euo pipefail
cd "$(dirname "$0")/.."
dataslate_test_python="${PYTHON:-python3}"
dataslate_test_helm="${HELM_BIN:-$PWD/deploy/.cache/helm}"
dataslate_test_airflow_chart="${AIRFLOW_CHART:-$PWD/deploy/airflow/.cache/airflow.tgz}"
if [[ -z "${DATASLATE_TEST_DATABASE_URL:-}" ]]; then
  echo 'Set DATASLATE_TEST_DATABASE_URL to an isolated disposable PostgreSQL database. Integration tests create and remove their own schemas.' >&2
  exit 2
fi
# The complete local gate must exercise hybrid retrieval, not silently skip it.
export DATASLATE_TEST_REQUIRE_VECTOR=1
for executable in go npm "$dataslate_test_python" "$dataslate_test_helm"; do
  command -v "$executable" >/dev/null || { echo "Missing prerequisite: $executable" >&2; exit 2; }
done
"$dataslate_test_python" -c 'import yaml' || { echo 'Install deploy/tests/requirements.txt into the selected Python environment.' >&2; exit 2; }
[[ -f "$dataslate_test_airflow_chart" ]] || { echo 'Fetch the pinned optional Airflow chart before this gate: python3 deploy/airflow/fetch.py (or set AIRFLOW_CHART to a verified archive copy).' >&2; exit 2; }
"$dataslate_test_python" deploy/scripts/check-base-images.py
[[ -d web/node_modules ]] || { echo 'Run npm ci in web before these checks.' >&2; exit 2; }
(cd api && go test -race ./... -count=1 && go vet ./...)
PYTHONPATH=sdk/python/src "$dataslate_test_python" -m unittest discover -s sdk/python/tests -v
PYTHON="$dataslate_test_python" bash scripts/smoke.sh
(cd web && npm run build && npm test)
"$dataslate_test_python" -m unittest discover -s runtime/ingestion/tests -p 'test_*.py' -v
HELM_BIN="$dataslate_test_helm" "$dataslate_test_python" -m unittest discover -s deploy/sql -p 'test_*.py' -v
HELM_BIN="$dataslate_test_helm" "$dataslate_test_python" -m unittest discover -s deploy/polaris -p 'test_*.py' -v
HELM_BIN="$dataslate_test_helm" AIRFLOW_CHART="$dataslate_test_airflow_chart" "$dataslate_test_python" -m unittest discover -s deploy/airflow -p 'test_*.py' -v
"$dataslate_test_python" -m unittest discover -s deploy/compute -p 'test_*.py' -v
PYTHON_BIN="$dataslate_test_python" HELM_BIN="$dataslate_test_helm" bash deploy/scripts/check.sh
DATASLATE_PREFLIGHT_HELM="$dataslate_test_helm" "$dataslate_test_python" -m unittest discover -s deploy/preflight -p 'test_*.py' -v
git diff --check
echo 'Local source checks passed. OCI startup, live Kubernetes and cloud qualification are separate checks; see docs/implementation/validation.md.'
