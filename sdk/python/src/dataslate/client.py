"""Small synchronous JSON client with explicit transport and credential boundaries."""

from __future__ import annotations

import ipaddress
import json
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
        if timeout <= 0 or max_response_bytes <= 0:
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
