#!/usr/bin/env bash
set -euo pipefail
deploy_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
helm_binary="${HELM_BIN:-helm}"
python_binary="${PYTHON_BIN:-python3}"
"$python_binary" "$deploy_root/tests/check_chart.py" --helm "$helm_binary"
