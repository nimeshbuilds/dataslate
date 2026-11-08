"""Small synchronous JSON client with explicit transport and credential boundaries."""

from __future__ import annotations

import ipaddress
import json
import math
import re
import time
from datetime import datetime
from http.client import HTTPException
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlsplit
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
    if not isinstance(value, str) or not value or value in {".", ".."} or any(c in value for c in "/\\\x00"):
        raise ValueError("A nonempty resource ID without path separators is required")
    return quote(value, safe="")


def _run_key(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9._:-]{8,128}", value):
        raise ValueError("idempotency_key must be 8–128 letters, digits, dots, underscores, colons or hyphens")
    return value


def _positive_revision(value: int) -> int:
    if type(value) is not int or value < 1:
        raise ValueError("revision must be a positive integer")
    return value


def _rfc3339(value: str) -> datetime:
    if (not isinstance(value, str) or not re.fullmatch(
            r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})", value)):
        raise ValueError("An explicit RFC3339 timestamp with timezone is required")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.utcoffset() is None:
            raise ValueError("missing timezone")
    except ValueError:
        raise ValueError("A valid RFC3339 timestamp is required") from None
    return parsed


def _memory_text(title: str, content: str, expires_at: str) -> dict[str, str]:
    for label, value, limit in (("title", title, 200), ("content", content, 4096)):
        if (not isinstance(value, str) or not value.strip() or "\x00" in value
                or len(value.encode("utf-8")) > limit):
            raise ValueError(f"Memory {label} must be nonempty and at most {limit} UTF-8 bytes")
    _rfc3339(expires_at)
    # The server's clock and administrator policy decide the permitted expiry.
    return {"title": title, "content": content, "expiresAt": expires_at}


def _memory_receipt(value: Any, workspace_id: str, operation: str, previous_revision: int,
                    memory_id: str | None = None) -> Any:
    message = "Memory mutation returned a mismatched receipt; preserve the original request for recovery"
    if not isinstance(value, dict) or type(value.get("replayed")) is not bool:
        raise DataSlateError("invalid_response", message)
    receipt, summary = value.get("receipt"), value.get("memory")
    if (not isinstance(receipt, dict) or not isinstance(summary, dict)
            or receipt.get("workspaceId") != workspace_id or receipt.get("operation") != operation
            or type(receipt.get("previousRevision")) is not int or receipt["previousRevision"] != previous_revision
            or type(receipt.get("revision")) is not int or receipt["revision"] != previous_revision + 1
            or not isinstance(receipt.get("memoryId"), str) or not receipt["memoryId"]
            or memory_id is not None and receipt["memoryId"] != memory_id
            or summary.get("id") != receipt["memoryId"] or summary.get("workspaceId") != workspace_id
            or type(summary.get("revision")) is not int or summary["revision"] < receipt["revision"]
            or summary.get("state") not in {"available", "restricted", "expired", "deleted"}
            or type(summary.get("purgePending")) is not bool):
        raise DataSlateError("invalid_response", message)
    if (set(receipt) - {"memoryId", "workspaceId", "operation", "previousRevision", "revision", "appliedAt"}
            or set(summary) - {"id", "workspaceId", "revision", "state", "expiresAt", "purgePending", "title", "bindingId"}
            or summary["state"] != "available" and ({"title", "bindingId"} & set(summary))):
        raise DataSlateError("invalid_response", message)
    try:
        _segment(receipt["memoryId"])
        _rfc3339(receipt.get("appliedAt"))
        _rfc3339(summary.get("expiresAt"))
        if summary["state"] == "available":
            _segment(summary.get("bindingId"))
            title = summary.get("title")
            if not isinstance(title, str) or not title.strip() or "\x00" in title or len(title.encode("utf-8")) > 200:
                raise ValueError("invalid title")
    except ValueError:
        raise DataSlateError("invalid_response", message) from None
    return value


class Client:
    """Connect to DataSlate. Tokens are retained only in this client instance.

    HTTPS is required except for literal loopback addresses and localhost.
    Requests do not retry writes automatically and never follow redirects.
    """

    def __init__(self, base_url: str = "http://127.0.0.1:8080", *, token: str | None = None,
                 timeout: float = 30, max_response_bytes: int = 16 * 1024 * 1024):
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

    def request(self, method: str, path: str, body: dict[str, Any] | None = None,
                *, query: dict[str, str | int] | None = None) -> Any:
        if not path.startswith("/api/v1/") or any(c in path for c in "\r\n?#"):
            raise ValueError("Only a relative DataSlate API path is accepted")
        if query:
            if any(not isinstance(k, str) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", k)
                   or isinstance(v, bool) or not isinstance(v, (str, int)) for k, v in query.items()):
                raise ValueError("Query parameters require named string or integer values")
            path += "?" + urlencode(query)
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
        """Return explicitly granted application, notebook and ingestion bindings."""
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/execution-bindings")

    def ingestion_pipelines(self, workspace_id: str):
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/ingestion-pipelines")

    def ingestion_pipeline(self, workspace_id: str, pipeline_id: str):
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/ingestion-pipelines/{_segment(pipeline_id)}")

    def create_ingestion_pipeline(self, workspace_id: str, pipeline: dict[str, Any]):
        """Save a versioned file-to-Delta definition; this does not start ingestion."""
        return self.request("POST", f"/api/v1/workspaces/{_segment(workspace_id)}/ingestion-pipelines", pipeline)

    def update_ingestion_pipeline(self, workspace_id: str, pipeline_id: str, pipeline: dict[str, Any]):
        """Update using the expected revision; storage and checkpoint configuration is immutable."""
        return self.request("PUT", f"/api/v1/workspaces/{_segment(workspace_id)}/ingestion-pipelines/{_segment(pipeline_id)}", pipeline)

    def delete_ingestion_pipeline(self, workspace_id: str, pipeline_id: str, revision: int):
        """Remove the definition; retain storage reservations, runs, data and checkpoints."""
        if isinstance(revision, bool) or not isinstance(revision, int) or revision < 1:
            raise ValueError("revision must be a positive integer for the saved pipeline")
        return self.request("DELETE", f"/api/v1/workspaces/{_segment(workspace_id)}/ingestion-pipelines/{_segment(pipeline_id)}",
                            query={"revision": revision})

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

    def create_ingestion_run(self, workspace_id: str, binding_id: str,
                             pipeline_id: str, revision: int, *, idempotency_key: str):
        """Accept one saved pipeline revision; preserve the exact selection and key for recovery.

        A new explicit run resumes the pipeline's retained checkpoint after the old
        writer has drained. This never resets checkpoints or retries a mutation.
        """
        if isinstance(revision, bool) or not isinstance(revision, int) or revision < 1:
            raise ValueError("revision must be a positive integer for the saved pipeline")
        _segment(pipeline_id)
        return self.request("POST", f"/api/v1/workspaces/{_segment(workspace_id)}/runs", {
            "bindingId": binding_id, "idempotencyKey": _run_key(idempotency_key),
            "pipeline": {"id": pipeline_id, "revision": revision},
        })

    def cancel_run(self, workspace_id: str, run_id: str):
        """Request cancellation; the returned state is not proof of termination."""
        return self.request("POST", f"/api/v1/workspaces/{_segment(workspace_id)}/runs/{_segment(run_id)}/cancel", {})

    def run_events(self, workspace_id: str, run_id: str):
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/runs/{_segment(run_id)}/events")

    def run_logs(self, workspace_id: str, run_id: str):
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/runs/{_segment(run_id)}/logs")

    def run_source(self, workspace_id: str, run_id: str):
        """Read the caller-owned immutable notebook or ingestion source snapshot."""
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/runs/{_segment(run_id)}/source")

    def sql_bindings(self, workspace_id: str):
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/sql-bindings")

    def ingestion_outcome(self, workspace_id: str, run_id: str):
        """Read durable runtime evidence. A succeeded run alone is not a verified table."""
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/runs/{_segment(run_id)}/ingestion-outcome")

    def publication_bindings(self, workspace_id: str):
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/publication-bindings")

    def table_publications(self, workspace_id: str):
        """List currently authorized publication operations owned by the caller."""
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/table-publications")

    @staticmethod
    def _publication(value, workspace_id, publication_id=None):
        if (not isinstance(value, dict) or value.get("workspaceId") != workspace_id
                or not isinstance(value.get("id"), str) or not value["id"]
                or publication_id is not None and value["id"] != publication_id
                or not isinstance(value.get("state"), str)
                or value["state"] not in {"queued", "checking", "registering", "verifying", "published", "failed", "unknown"}
                or type(value.get("revision")) is not int or value["revision"] < 1
                or type(value.get("registrationStarted")) is not bool
                or type(value.get("reconciliationComplete")) is not bool
                or value["state"] in {"published", "failed"} and not value["reconciliationComplete"]
                or value["reconciliationComplete"] and value["state"] not in {"published", "failed", "unknown"}
                or not isinstance(value.get("deltaVersion"), str)
                or not re.fullmatch(r"0|[1-9][0-9]{0,18}", value["deltaVersion"])
                or int(value["deltaVersion"]) > 9223372036854775807):
            raise DataSlateError("invalid_response", "Publication has an invalid identity, state, revision or Delta version")
        try:
            _segment(value["id"])
            _segment(value.get("bindingId"))
            _segment(value.get("runId"))
            if value["state"] == "published":
                if value.get("publishedTableId") != value["bindingId"]:
                    raise ValueError("wrong published table")
                _rfc3339(value.get("verifiedAt"))
        except ValueError:
            raise DataSlateError("invalid_response", "Publication returned an invalid resource or verification receipt") from None
        return value

    def create_table_publication(self, workspace_id: str, run_id: str, binding_id: str, *, idempotency_key: str):
        """Publish one verified output. Persist the exact run/binding/key before sending.

        An uncertain response must be recovered with the same request. This does
        not resubmit ingestion, retry a registration, or grant table read access.
        """
        _segment(run_id)
        _segment(binding_id)
        value = self.request("POST", f"/api/v1/workspaces/{_segment(workspace_id)}/table-publications", {
            "runId": run_id, "bindingId": binding_id, "idempotencyKey": _run_key(idempotency_key)})
        if not isinstance(value, dict) or type(value.get("replayed")) is not bool:
            raise DataSlateError("invalid_response", "Publication acceptance is unconfirmed; preserve the original request for recovery")
        publication = self._publication(value.get("publication"), workspace_id)
        if publication["runId"] != run_id or publication["bindingId"] != binding_id:
            raise DataSlateError("invalid_response", "Publication receipt does not match the submitted run and binding")
        return value

    def table_publication(self, workspace_id: str, publication_id: str):
        value = self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/table-publications/{_segment(publication_id)}")
        return self._publication(value, workspace_id, publication_id)

    def published_tables(self, workspace_id: str):
        """List tables under current reader grants, independently of publisher rights."""
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/published-tables")

    def published_table(self, workspace_id: str, table_id: str):
        """Get a latest-table SQL draft. This method neither saves nor executes it."""
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/published-tables/{_segment(table_id)}")

    def watch_table_publication(self, workspace_id: str, publication_id: str, *, interval: float = 2, max_wait: float = 300):
        """GET-only observation. Unresolved final registration stops without a retry.

        The controller may subsequently reconcile an unknown registration. Fetch
        or watch the same operation later; never replace its idempotency key.
        """
        if not all(type(v) in (int, float) and math.isfinite(v) and v > 0 for v in (interval, max_wait)):
            raise ValueError("interval and max_wait must be finite positive seconds")
        deadline, previous = time.monotonic() + max_wait, None
        while True:
            current = self.table_publication(workspace_id, publication_id)
            if previous is not None and current["revision"] < previous["revision"]:
                raise DataSlateError("invalid_response", "Publication revision moved backwards")
            if current != previous:
                yield current
                previous = current
            if current["reconciliationComplete"]:
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise DataSlateError("watch_timeout", "Stopped watching; publication may still be active")
            time.sleep(min(interval, remaining))
            if time.monotonic() >= deadline:
                raise DataSlateError("watch_timeout", "Stopped watching; publication may still be active")

    def catalog_bindings(self, workspace_id: str):
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/catalog-bindings")

    def verify_catalog(self, workspace_id: str, binding_id: str):
        """Probe the configured catalog; this does not grant access or create tables."""
        return self.request("POST", f"/api/v1/workspaces/{_segment(workspace_id)}/catalog-bindings/{_segment(binding_id)}/verify", {})

    def catalog_namespaces(self, workspace_id: str, binding_id: str):
        """List authorized grant identifiers; they are not proof that tables exist."""
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/catalog-bindings/{_segment(binding_id)}/namespaces")

    def catalog_tables(self, workspace_id: str, binding_id: str, namespace: list[str]):
        self._catalog_namespace(namespace)
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/catalog-bindings/{_segment(binding_id)}/tables",
                            query={"namespace": json.dumps(namespace, ensure_ascii=False, separators=(",", ":"))})

    def catalog_table(self, workspace_id: str, binding_id: str, namespace: list[str], name: str):
        """Load current authorized metadata; refresh catalog discovery when needed."""
        self._catalog_namespace(namespace)
        if not isinstance(name, str) or not name or any(ord(c) < 32 for c in name):
            raise ValueError("A table name without control characters is required")
        return self.request("POST", f"/api/v1/workspaces/{_segment(workspace_id)}/catalog-bindings/{_segment(binding_id)}/table",
                            {"namespace": namespace, "name": name})

    @staticmethod
    def _catalog_namespace(namespace: list[str]):
        if (not isinstance(namespace, list) or not namespace or len(namespace) > 8
                or any(not isinstance(part, str) or not part or any(ord(c) < 32 for c in part) for part in namespace)):
            raise ValueError("Namespace must be a nonempty list of at most eight names without control characters")

    def queries(self, workspace_id: str):
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/queries")

    def sylon_bindings(self, workspace_id: str):
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/sylon/bindings")

    def sylon_conversations(self, workspace_id: str):
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/sylon/conversations")

    def create_sylon_conversation(self, workspace_id: str, binding_id: str, title: str):
        _segment(binding_id)
        return self.request("POST", f"/api/v1/workspaces/{_segment(workspace_id)}/sylon/conversations",
                            {"bindingId": binding_id, "title": title})

    def sylon_conversation(self, workspace_id: str, conversation_id: str):
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/sylon/conversations/{_segment(conversation_id)}")

    def delete_sylon_conversation(self, workspace_id: str, conversation_id: str):
        """Erase an owned conversation and prevent later job publication. Does not reverse provider processing."""
        return self.request("DELETE", f"/api/v1/workspaces/{_segment(workspace_id)}/sylon/conversations/{_segment(conversation_id)}")

    def send_sylon_message(self, workspace_id: str, conversation_id: str, prompt: str, *,
                           idempotency_key: str, notebook: dict[str, Any] | None = None,
                           catalogs: list[dict[str, Any]] | None = None, search: dict[str, Any] | None = None,
                           memories: list[dict[str, Any]] | None = None):
        """Accept one private assistant intent. Context uses resource IDs, never provider credentials.

        Persist the exact prompt/context/key before sending. A model response is
        advice or a proposal; applying source and executing it are separate actions.
        """
        if not isinstance(prompt, str) or not prompt.strip() or "\x00" in prompt or len(prompt.encode("utf-8")) > 8192:
            raise ValueError("Sylon prompt must be nonempty and at most 8192 UTF-8 bytes")
        body: dict[str, Any] = {"prompt": prompt, "idempotencyKey": _run_key(idempotency_key)}
        if notebook is not None:
            if (not isinstance(notebook, dict) or set(notebook) != {"id", "revision", "cellId"}
                    or type(notebook["revision"]) is not int or notebook["revision"] < 1):
                raise ValueError("Notebook context requires exact id, positive revision and cellId")
            _segment(notebook["id"])
            _segment(notebook["cellId"])
            body["notebook"] = notebook
        if catalogs is not None:
            if not isinstance(catalogs, list) or len(catalogs) > 4:
                raise ValueError("At most four catalog selections are accepted")
            for catalog in catalogs:
                if not isinstance(catalog, dict) or set(catalog) != {"bindingId", "namespace", "name"}:
                    raise ValueError("Catalog context requires bindingId, namespace and name")
                _segment(catalog["bindingId"])
                self._catalog_namespace(catalog["namespace"])
                if not isinstance(catalog["name"], str) or not catalog["name"] or any(ord(c) < 32 for c in catalog["name"]):
                    raise ValueError("Invalid catalog table name")
            body["catalogs"] = catalogs
        if search is not None:
            if (not isinstance(search, dict) or set(search) != {"bindingId", "query", "mode"}
                    or search["mode"] not in ("lexical", "hybrid") or not isinstance(search["query"], str)
                    or not search["query"].strip() or "\x00" in search["query"] or len(search["query"].encode("utf-8")) > 4096):
                raise ValueError("Search context requires bindingId, bounded query and lexical/hybrid mode")
            _segment(search["bindingId"])
            body["search"] = search
        if memories is not None:
            if not isinstance(memories, list) or len(memories) > 4:
                raise ValueError("At most four explicitly reviewed memory revisions are accepted")
            selected_ids = set()
            for memory in memories:
                if (not isinstance(memory, dict) or set(memory) != {"id", "revision"}
                        or type(memory["revision"]) is not int or memory["revision"] < 1):
                    raise ValueError("Memory context requires an exact id and positive revision")
                _segment(memory["id"])
                if memory["id"] in selected_ids:
                    raise ValueError("A memory can only be selected once")
                selected_ids.add(memory["id"])
            body["memories"] = memories
        return self.request("POST", f"/api/v1/workspaces/{_segment(workspace_id)}/sylon/conversations/{_segment(conversation_id)}/messages", body)

    def sylon_job(self, workspace_id: str, job_id: str):
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/sylon/jobs/{_segment(job_id)}")

    def sylon_memories(self, workspace_id: str):
        """List private memory management summaries, including minimal restricted entries."""
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/sylon/memories")

    def sylon_memory(self, workspace_id: str, memory_id: str):
        """Read memory content only while its source and binding remain authorized."""
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/sylon/memories/{_segment(memory_id)}")

    def save_sylon_memory(self, workspace_id: str, source_job_id: str, source_revision: int,
                          title: str, content: str, expires_at: str, *, idempotency_key: str):
        """Save a reviewed summary of an owned successful answer; never calls a model.

        Preserve the complete request and key. An uncertain response must be
        recovered by replaying that exact intent, not generating a new key.
        """
        _segment(source_job_id)
        body = {**_memory_text(title, content, expires_at), "idempotencyKey": _run_key(idempotency_key),
                "source": {"jobId": source_job_id, "revision": _positive_revision(source_revision)}}
        value = self.request("POST", f"/api/v1/workspaces/{_segment(workspace_id)}/sylon/memories", body)
        return _memory_receipt(value, workspace_id, "create", 0)

    def correct_sylon_memory(self, workspace_id: str, memory_id: str, revision: int,
                             title: str, content: str, expires_at: str, *, idempotency_key: str):
        """Correct exact current memory; invalidates history derived from its old revision."""
        body = {**_memory_text(title, content, expires_at), "idempotencyKey": _run_key(idempotency_key),
                "revision": _positive_revision(revision)}
        value = self.request("PUT", f"/api/v1/workspaces/{_segment(workspace_id)}/sylon/memories/{_segment(memory_id)}", body)
        return _memory_receipt(value, workspace_id, "correct", revision, memory_id)

    def delete_sylon_memory(self, workspace_id: str, memory_id: str, revision: int, *, idempotency_key: str):
        """Revoke a remembered revision and queue local purge; cannot erase provider copies."""
        body = {"idempotencyKey": _run_key(idempotency_key), "revision": _positive_revision(revision)}
        value = self.request("DELETE", f"/api/v1/workspaces/{_segment(workspace_id)}/sylon/memories/{_segment(memory_id)}", body)
        return _memory_receipt(value, workspace_id, "delete", revision, memory_id)

    def recall_sylon_memories(self, workspace_id: str, binding_id: str, query: str, *, limit: int = 4):
        """Find authorized private memories locally; review selections before sending to Sylon."""
        _segment(binding_id)
        if (not isinstance(query, str) or not query.strip() or "\x00" in query
                or len(query.encode("utf-8")) > 4096 or type(limit) is not int or not 1 <= limit <= 4):
            raise ValueError("Memory recall needs a nonempty query up to 4096 UTF-8 bytes and limit 1–4")
        return self.request("POST", f"/api/v1/workspaces/{_segment(workspace_id)}/sylon/memories/recall",
                            {"bindingId": binding_id, "query": query, "limit": limit})

    def cancel_sylon_job(self, workspace_id: str, job_id: str):
        """Request cancellation/discard; an already dispatched provider call may still be billed."""
        return self.request("POST", f"/api/v1/workspaces/{_segment(workspace_id)}/sylon/jobs/{_segment(job_id)}/cancel", {})

    def apply_sylon_proposal(self, workspace_id: str, job_id: str, notebook_id: str, revision: int, cell_id: str):
        """Apply an owned proposal once to its exact saved source target. This never runs code."""
        _segment(notebook_id)
        _segment(cell_id)
        if type(revision) is not int or revision < 1:
            raise ValueError("revision must be a positive integer for the saved notebook")
        return self.request("POST", f"/api/v1/workspaces/{_segment(workspace_id)}/sylon/jobs/{_segment(job_id)}/apply",
                            {"notebookId": notebook_id, "revision": revision, "cellId": cell_id})

    def watch_sylon_job(self, workspace_id: str, job_id: str, *, interval: float = 2, max_wait: float = 300):
        """Poll without replaying generation. Unknown outcomes stop with no automatic model retry."""
        if not all(math.isfinite(v) and v > 0 for v in (interval, max_wait)):
            raise ValueError("interval and max_wait must be finite positive seconds")
        deadline, previous = time.monotonic() + max_wait, None
        while True:
            current = self.sylon_job(workspace_id, job_id)
            if (not isinstance(current, dict) or current.get("id") != job_id
                    or current.get("workspaceId") != workspace_id
                    or current.get("state") not in {"queued", "preparing", "generating", "succeeded", "failed", "unknown", "cancel_requested", "cancelled"}
                    or type(current.get("revision")) is not int or current["revision"] < 1):
                raise DataSlateError("invalid_response", "Sylon job has an invalid identity, state or revision")
            if current != previous:
                yield current
                previous = current
            if current["state"] in {"succeeded", "failed", "unknown", "cancelled"}:
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise DataSlateError("watch_timeout", "Stopped watching; Sylon may still be processing")
            time.sleep(min(interval, remaining))
            if time.monotonic() >= deadline:
                raise DataSlateError("watch_timeout", "Stopped watching; Sylon may still be processing")

    def search_status(self, workspace_id: str):
        """Read public search bindings and vector availability; credentials are never returned."""
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/search/status")

    def search(self, workspace_id: str, binding_id: str, query: str, *, mode: str = "lexical", limit: int = 10):
        """Search current authorized documents. Hybrid mode sends query text to the configured provider.

        Scores express ranking, not calibrated probabilities. Results carry source
        revision/policy and timestamps; catalog snapshots have a bounded expiry.
        """
        _segment(binding_id)
        if (not isinstance(query, str) or not query.strip() or "\x00" in query or len(query.encode("utf-8")) > 4096
                or mode not in ("lexical", "hybrid") or type(limit) is not int or not 1 <= limit <= 20):
            raise ValueError("Search requires nonempty query up to 4096 UTF-8 bytes, lexical/hybrid mode and limit 1–20")
        return self.request("POST", f"/api/v1/workspaces/{_segment(workspace_id)}/search",
                            {"bindingId": binding_id, "query": query, "mode": mode, "limit": limit})

    def index_notebook(self, workspace_id: str, binding_id: str, notebook_id: str,
                       revision: int, *, idempotency_key: str):
        """Index an exact saved revision. Persist the selection/key before sending; never retried here."""
        _segment(binding_id)
        _segment(notebook_id)
        if type(revision) is not int or revision < 1:
            raise ValueError("revision must be a positive integer for the saved notebook")
        return self.request("POST", f"/api/v1/workspaces/{_segment(workspace_id)}/search/index", {
            "bindingId": binding_id, "idempotencyKey": _run_key(idempotency_key),
            "notebook": {"id": notebook_id, "revision": revision},
        })

    def index_catalog_table(self, workspace_id: str, binding_id: str, catalog_binding_id: str,
                            namespace: list[str], name: str, *, idempotency_key: str):
        """Queue an authorized metadata refresh; no table rows or storage credentials are indexed."""
        _segment(binding_id)
        _segment(catalog_binding_id)
        self._catalog_namespace(namespace)
        if not isinstance(name, str) or not name or any(ord(c) < 32 for c in name):
            raise ValueError("A table name without control characters is required")
        return self.request("POST", f"/api/v1/workspaces/{_segment(workspace_id)}/search/index", {
            "bindingId": binding_id, "idempotencyKey": _run_key(idempotency_key),
            "catalog": {"bindingId": catalog_binding_id, "namespace": namespace, "name": name},
        })

    def search_index_job(self, workspace_id: str, job_id: str):
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/search/index-jobs/{_segment(job_id)}")

    def watch_search_index(self, workspace_id: str, job_id: str, *, interval: float = 2,
                           max_wait: float = 300):
        """Poll an owned job. Unknown provider outcomes stop without automatic resubmission."""
        if not all(math.isfinite(v) and v > 0 for v in (interval, max_wait)):
            raise ValueError("interval and max_wait must be finite positive seconds")
        deadline, previous = time.monotonic() + max_wait, None
        while True:
            current = self.search_index_job(workspace_id, job_id)
            if (not isinstance(current, dict) or current.get("id") != job_id
                    or current.get("workspaceId") != workspace_id
                    or current.get("state") not in {"queued", "indexing", "succeeded", "failed", "unknown", "superseded"}
                    or type(current.get("revision")) is not int or current["revision"] < 1):
                raise DataSlateError("invalid_response", "Search job has an invalid identity, state or revision")
            if current != previous:
                yield current
                previous = current
            if current["state"] in {"succeeded", "failed", "unknown", "superseded"}:
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise DataSlateError("watch_timeout", "Stopped watching; indexing may still be active")
            time.sleep(min(interval, remaining))
            if time.monotonic() >= deadline:
                raise DataSlateError("watch_timeout", "Stopped watching; indexing may still be active")

    def create_query(self, workspace_id: str, binding_id: str, notebook_id: str,
                     revision: int, cell_id: str, *, idempotency_key: str):
        """Execute one exact saved SQL cell. Persist this selection and key before sending.

        After an uncertain response, repeat exactly the same selection and key.
        This method never retries POST or fetches a newer notebook revision.
        """
        if isinstance(revision, bool) or not isinstance(revision, int) or revision < 1:
            raise ValueError("revision must be a positive integer for the saved notebook")
        _segment(notebook_id)
        _segment(cell_id)
        return self.request("POST", f"/api/v1/workspaces/{_segment(workspace_id)}/queries", {
            "bindingId": binding_id, "idempotencyKey": _run_key(idempotency_key),
            "notebook": {"id": notebook_id, "revision": revision, "cellId": cell_id},
        })

    def query(self, workspace_id: str, query_id: str):
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/queries/{_segment(query_id)}")

    def query_source(self, workspace_id: str, query_id: str):
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/queries/{_segment(query_id)}/source")

    def query_result(self, workspace_id: str, query_id: str):
        """Read owner-scoped results; scalar values stay strings to preserve exact numbers.

        Inspect complete/truncated before aggregating or exporting as a full result.
        Access is reevaluated against the current binding and policy version.
        """
        return self.request("GET", f"/api/v1/workspaces/{_segment(workspace_id)}/queries/{_segment(query_id)}/result")

    def cancel_query(self, workspace_id: str, query_id: str):
        """Request cancellation; acceptance does not prove engine termination."""
        return self.request("POST", f"/api/v1/workspaces/{_segment(workspace_id)}/queries/{_segment(query_id)}/cancel", {})

    def watch_query(self, workspace_id: str, query_id: str, *, interval: float = 2,
                    max_wait: float = 300):
        """Poll GET only. Timeout/interruption does not cancel the backend query."""
        if not all(math.isfinite(v) and v > 0 for v in (interval, max_wait)):
            raise ValueError("interval and max_wait must be finite positive seconds")
        deadline, previous = time.monotonic() + max_wait, None
        while True:
            current = self.query(workspace_id, query_id)
            if (not isinstance(current, dict) or current.get("id") != query_id
                    or current.get("workspaceId") != workspace_id
                    or current.get("state") not in {"queued", "submitting", "running", "succeeded", "failed", "cancel_requested", "cancelled", "unknown"}
                    or isinstance(current.get("revision"), bool) or not isinstance(current.get("revision"), int)
                    or current["revision"] < 1):
                raise DataSlateError("invalid_response", "Query response has an invalid identity, state or revision")
            if current != previous:
                yield current
                previous = current
            if current["state"] in {"succeeded", "failed", "cancelled"}:
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise DataSlateError("watch_timeout", "Stopped watching; the backend query may still be active")
            time.sleep(min(interval, remaining))
            if time.monotonic() >= deadline:
                raise DataSlateError("watch_timeout", "Stopped watching; the backend query may still be active")

    def watch_run(self, workspace_id: str, run_id: str, *, interval: float = 2,
                  max_wait: float = 300, wait_for_cleanup: bool = False):
        """Yield changed observed states until terminal or the polling budget expires.

        Only GET is repeated. Timeout or interruption does not cancel the backend job.
        Each request also has the client's transport timeout, so completion may exceed
        max_wait by at most one request timeout. Errors propagate without hidden retries.
        Set wait_for_cleanup to observe source cleanup and the ingestion writer drain;
        a terminal application state alone does not permit another pipeline writer.
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
                artifact = current.get("sourceArtifact")
                source_clean = artifact is None or (isinstance(artifact, dict) and artifact.get("state") == "deleted")
                writer_clean = current.get("ingestion") is None or current.get("writerReleased") is True
                if not wait_for_cleanup or (source_clean and writer_clean):
                    return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise DataSlateError("watch_timeout", "Stopped watching; the backend run may still be active")
            time.sleep(min(interval, remaining))
            if time.monotonic() >= deadline:
                raise DataSlateError("watch_timeout", "Stopped watching; the backend run may still be active")
