"""Small synchronous JSON client with explicit transport and credential boundaries."""

from __future__ import annotations

import ipaddress
import json
import math
import re
import time
from http.client import HTTPException
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


class DataSlateError(Exception):
    def __init__(self, code: str, message: str, status: int | None = None):
        super().__init__(message)
        self.code = code
        self.status = status


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # Authorization must never follow a redirect to another origin.
        return None


def _segment(value: str) -> str:
    if not value or value in {".", ".."} or any(c in value for c in "/\\\x00"):
        raise ValueError("A nonempty resource ID without path separators is required")
    return quote(value, safe="")


def _run_key(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9._:-]{8,128}", value):
        raise ValueError("idempotency_key must be 8–128 letters, digits, dots, underscores, colons or hyphens")
    return value


class Client:
    """Connect to DataSlate. Tokens are retained only in this client instance.

    HTTPS is required except for literal loopback addresses and localhost.
    Requests do not retry writes automatically and never follow redirects.
    """

    def __init__(self, base_url: str = "http://127.0.0.1:8080", *, token: str | None = None,
                 timeout: float = 30, max_response_bytes: int = 8 * 1024 * 1024):
        parsed = urlsplit(base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("API URL must have an HTTP(S) scheme and host")
        if parsed.username is not None or parsed.password is not None or parsed.query or parsed.fragment:
            raise ValueError("API URL must not contain credentials, query parameters or fragments")
        if parsed.path not in {"", "/"}:
            raise ValueError("API URL must point to the server root")
        loopback = parsed.hostname == "localhost"
        try:
            loopback = loopback or ipaddress.ip_address(parsed.hostname).is_loopback
        except ValueError:
            pass
        if parsed.scheme == "http" and not loopback:
            raise ValueError("Plain HTTP is permitted only for loopback development")
        if not math.isfinite(timeout) or timeout <= 0 or max_response_bytes <= 0:
            raise ValueError("Timeout and response limit must be positive")
        if token is not None and (not token.strip() or any(c in token for c in "\r\n")):
            raise ValueError("Bearer token must be a nonempty single-line value")
        self.base_url = base_url.rstrip("/")
        self._token = token
        self.timeout = timeout
        self.max_response_bytes = max_response_bytes
        self._opener = build_opener(_NoRedirect())

    def _read_json(self, response) -> Any:
        try:
            raw = response.read(self.max_response_bytes + 1)
        except (HTTPException, TimeoutError, OSError):
            raise DataSlateError("connection_error", "API response was interrupted or malformed") from None
        if len(raw) > self.max_response_bytes:
            raise DataSlateError("response_too_large", "API response exceeded the configured limit")
        if not raw:
            return None
        try:
            return json.loads(raw)
        except (ValueError, UnicodeDecodeError) as exc:
            raise DataSlateError("invalid_response", "API returned invalid JSON") from exc

    def request(self, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
        if not path.startswith("/api/v1/") or any(c in path for c in "\r\n?#"):
            raise ValueError("Only a relative DataSlate API path is accepted")
        headers = {"Accept": "application/json", "User-Agent": "dataslate-python/0.1.0a1"}
        if self._token:
            headers["Authorization"] = "Bearer " + self._token
        data = None
        if body is not None:
            data = json.dumps(body, allow_nan=False).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = Request(self.base_url + path, data=data, headers=headers, method=method)
        try:
            with self._opener.open(request, timeout=self.timeout) as response:
                return self._read_json(response)
        except HTTPError as exc:
            try:
                payload = self._read_json(exc)
                error = payload.get("error", {}) if isinstance(payload, dict) else {}
                if not isinstance(error, dict):
                    error = {}
                code = str(error.get("code", "http_error"))
                message = str(error.get("message", f"API returned HTTP {exc.code}"))
            except DataSlateError:
                code, message = "http_error", f"API returned HTTP {exc.code}"
            finally:
                exc.close()
            raise DataSlateError(code, message, exc.code) from None
        except (URLError, HTTPException, TimeoutError, OSError):
            raise DataSlateError("connection_error", "Could not reach the DataSlate API") from None

    def status(self):
        return self.request("GET", "/api/v1/status")

    def workspaces(self):
        return self.request("GET", "/api/v1/workspaces")

    def create_workspace(self, name: str, slug: str):
        return self.request("POST", "/api/v1/workspaces", {"name": name, "slug": slug})

    def notebooks(self, workspace_id: str):
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/notebooks")

    def notebook(self, workspace_id: str, notebook_id: str):
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/notebooks/{_segment(notebook_id)}")

    def create_notebook(self, workspace_id: str, notebook: dict[str, Any]):
        return self.request("POST", f"/api/v1/workspaces/{_segment(workspace_id)}/notebooks", notebook)

    def update_notebook(self, workspace_id: str, notebook_id: str, notebook: dict[str, Any]):
        return self.request("PUT", f"/api/v1/workspaces/{_segment(workspace_id)}/notebooks/{_segment(notebook_id)}", notebook)

    def modules(self):
        return self.request("GET", "/api/v1/modules")

    def deployment_plan(self, plan: dict[str, Any]):
        return self.request("POST", "/api/v1/deployment-plans", plan)

    def spark_preview(self, workspace_id: str, application: dict[str, Any]):
        return self.request("POST", f"/api/v1/workspaces/{_segment(workspace_id)}/spark-applications/preview", application)

    def execution_bindings(self, workspace_id: str):
        """Return explicitly granted application or notebook engineering bindings."""
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/execution-bindings")

    def runs(self, workspace_id: str):
        """Return at most 200 owned run summaries; fetch events separately."""
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/runs")

    def create_run(self, workspace_id: str, binding_id: str, *, idempotency_key: str):
        """Submit once; reuse the SAME key after an uncertain network outcome.

        A new key means a new job. The caller must persist the key before sending.
        This method never retries a mutation or generates a hidden replacement key.
        """
        return self.request("POST", f"/api/v1/workspaces/{_segment(workspace_id)}/runs",
                            {"bindingId": binding_id, "idempotencyKey": _run_key(idempotency_key)})

    def create_notebook_run(self, workspace_id: str, binding_id: str,
                            notebook_id: str, revision: int, *, idempotency_key: str):
        """Execute an exact saved Python revision using an engineering binding.

        Persist the binding, notebook ID, revision and key before sending. Repeat
        that exact selection after an uncertain response. This method never fetches
        a newer revision, uploads source or retries automatically. The server takes
        an independent immutable source snapshot when accepting the run.
        """
        if isinstance(revision, bool) or not isinstance(revision, int) or revision < 1:
            raise ValueError("revision must be a positive integer for the saved notebook")
        _segment(notebook_id)
        return self.request("POST", f"/api/v1/workspaces/{_segment(workspace_id)}/runs", {
            "bindingId": binding_id, "idempotencyKey": _run_key(idempotency_key),
            "notebook": {"id": notebook_id, "revision": revision},
        })

    def run(self, workspace_id: str, run_id: str):
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/runs/{_segment(run_id)}")

    def cancel_run(self, workspace_id: str, run_id: str):
        """Request cancellation; the returned state is not proof of termination."""
        return self.request("POST", f"/api/v1/workspaces/{_segment(workspace_id)}/runs/{_segment(run_id)}/cancel", {})

    def run_events(self, workspace_id: str, run_id: str):
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/runs/{_segment(run_id)}/events")

    def run_logs(self, workspace_id: str, run_id: str):
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/runs/{_segment(run_id)}/logs")

    def run_source(self, workspace_id: str, run_id: str):
        """Read the caller-owned immutable notebook snapshot, if this is a notebook run."""
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/runs/{_segment(run_id)}/source")

    def watch_run(self, workspace_id: str, run_id: str, *, interval: float = 2,
                  max_wait: float = 300):
        """Yield changed observed states until terminal or the polling budget expires.

        Only GET is repeated. Timeout or interruption does not cancel the backend job.
        Each request also has the client's transport timeout, so completion may exceed
        max_wait by at most one request timeout. Errors propagate without hidden retries.
        """
        if not all(math.isfinite(v) and v > 0 for v in (interval, max_wait)):
            raise ValueError("interval and max_wait must be finite positive seconds")
        deadline = time.monotonic() + max_wait
        previous = None
        while True:
            current = self.run(workspace_id, run_id)
            if (not isinstance(current, dict) or current.get("id") != run_id
                    or current.get("state") not in {"queued", "submitting", "running", "succeeded", "failed", "cancel_requested", "cancelled", "unknown"}
                    or isinstance(current.get("revision"), bool) or not isinstance(current.get("revision"), int)
                    or current["revision"] < 1):
                raise DataSlateError("invalid_response", "Run response has an invalid identity, state or revision")
            if current != previous:
                yield current
                previous = current
            if current["state"] in {"succeeded", "failed", "cancelled"}:
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise DataSlateError("watch_timeout", "Stopped watching; the backend run may still be active")
            time.sleep(min(interval, remaining))
            if time.monotonic() >= deadline:
                raise DataSlateError("watch_timeout", "Stopped watching; the backend run may still be active")
