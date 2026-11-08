"""Validate the profile before the upstream entrypoint can initialize Airflow."""
import os
import sys
from runtime_check import validate_environment

try:
    validate_environment(os.environ, require_jwt=any(arg in ('api-server', 'scheduler') for arg in sys.argv[1:]))
except Exception:
    # A URI, password, token, private key or upstream exception is never an error message.
    raise SystemExit('DataSlate Airflow profile rejected runtime configuration; check the documented Secret and TLS contract') from None
os.execv('/entrypoint', ['/entrypoint', *sys.argv[1:]])
