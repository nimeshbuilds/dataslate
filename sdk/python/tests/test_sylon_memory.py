"""Private memory transport: exact intent recovery, revision receipts and no hidden work."""
import contextlib
import copy
import io
import json
import socket
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from dataslate import Client, DataSlateError
from dataslate.cli import main


EXPIRY = "2026-12-01T12:00:00Z"


def receipt(operation="create", previous=0, *, replayed=False, state="available", current=None):
    summary = {"id": "mem-one", "workspaceId": "team", "revision": current or previous + 1,
               "state": state, "expiresAt": EXPIRY, "purgePending": state == "deleted"}
    if state == "available":
        summary.update(title="UTC reporting", bindingId="assistant")
    return {"receipt": {"memoryId": "mem-one", "workspaceId": "team", "operation": operation,
                        "previousRevision": previous, "revision": previous + 1,
                        "appliedAt": "2026-11-08T12:00:00Z"},
            "replayed": replayed, "memory": summary}


class MemoryHandler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def handle_request(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = json.loads(self.rfile.read(length)) if length else None
        self.server.requests.append((self.command, self.path, body))
        if body and body.get("idempotencyKey") == "uncertain-request":
            self.close_connection = True
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()
            return
        value = self.server.responses.pop(0) if self.server.responses else {"items": []}
        self.send_response(200)
        self.end_headers()
        self.wfile.write(json.dumps(value).encode())

    do_GET = do_POST = do_PUT = do_DELETE = handle_request


class MemoryTests(unittest.TestCase):
    def setUp(self):
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), MemoryHandler)
        self.server.requests, self.server.responses = [], []
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        self.client = Client(self.url)

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def save(self, **changes):
        args = dict(workspace_id="team", source_job_id="syl-one", source_revision=7,
                    title="UTC reporting", content="Report all dates in UTC.", expires_at=EXPIRY,
                    idempotency_key="reviewed-memory")
        return self.client.save_sylon_memory(**(args | changes))

    def test_lifecycle_uses_exact_server_resources_and_no_model_or_execution_route(self):
        self.server.responses = [receipt(), receipt("correct", 1), receipt("delete", 2, state="deleted")]
        self.save()
        self.client.correct_sylon_memory("team", "mem-one", 1, "Corrected", "Use UTC dates.", EXPIRY,
                                         idempotency_key="correct-memory")
        self.client.delete_sylon_memory("team", "mem-one", 2, idempotency_key="delete-memory")
        self.assertEqual(["POST", "PUT", "DELETE"], [r[0] for r in self.server.requests])
        self.assertTrue(all("/sylon/memories" in r[1] for r in self.server.requests))
        self.assertEqual({"jobId": "syl-one", "revision": 7}, self.server.requests[0][2]["source"])
        self.assertEqual(EXPIRY, self.server.requests[0][2]["expiresAt"])
        self.assertEqual({"revision": 2, "idempotencyKey": "delete-memory"}, self.server.requests[2][2])

    def test_uncertain_mutations_do_not_retry_and_exact_intent_can_be_replayed(self):
        for _ in range(2):
            with self.assertRaises(DataSlateError) as caught:
                self.save(idempotency_key="uncertain-request")
            self.assertEqual("connection_error", caught.exception.code)
        self.assertEqual(2, len(self.server.requests))
        self.assertEqual(self.server.requests[0], self.server.requests[1])
        with self.assertRaises(DataSlateError):
            self.client.delete_sylon_memory("team", "mem-one", 3, idempotency_key="uncertain-request")
        self.assertEqual(3, len(self.server.requests))

    def test_receipt_identity_and_revision_mismatch_never_becomes_success(self):
        valid = receipt("correct", 3)
        for field, bad in (("workspaceId", "other"), ("memoryId", "mem-other"),
                           ("operation", "delete"), ("previousRevision", True), ("revision", 5),
                           ("appliedAt", "not-a-date")):
            item = copy.deepcopy(valid)
            item["receipt"][field] = bad
            self.server.responses = [item]
            with self.subTest(field=field), self.assertRaises(DataSlateError) as caught:
                self.client.correct_sylon_memory("team", "mem-one", 3, "Title", "Body", EXPIRY,
                                                 idempotency_key="correct-memory")
            self.assertEqual("invalid_response", caught.exception.code)
        self.assertEqual(6, len(self.server.requests))

    def test_replay_after_revocation_accepts_only_minimal_management_summary(self):
        self.server.responses = [receipt(replayed=True, state="restricted", current=4)]
        result = self.save()
        self.assertTrue(result["replayed"])
        self.assertEqual(1, result["receipt"]["revision"])
        self.assertEqual(4, result["memory"]["revision"])
        self.assertNotIn("title", result["memory"])
        for target, key in (("memory", "content"), ("memory", "title"), ("receipt", "source")):
            bad = receipt(replayed=True, state="restricted")
            bad[target][key] = "protected"
            self.server.responses = [bad]
            with self.assertRaises(DataSlateError):
                self.save()

    def test_invalid_text_expiry_source_and_key_do_not_leave_the_client(self):
        for changes in ({"title": "😀" * 51}, {"content": "😀" * 1025}, {"content": "bad\x00data"},
                        {"expires_at": "2026-12-01"}, {"expires_at": "2026-12-01T12:00:00"},
                        {"expires_at": "2026-02-31T12:00:00Z"}, {"source_revision": True},
                        {"source_job_id": "../other"}, {"idempotency_key": "short"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.save(**changes)
        self.assertEqual([], self.server.requests)

    def test_recall_is_separate_from_explicit_exact_revision_transfer(self):
        self.client.recall_sylon_memories("team", "assistant", "UTC reporting", limit=2)
        self.client.send_sylon_message("team", "chat-one", "Explain reporting", idempotency_key="reviewed-context",
                                       memories=[{"id": "mem-one", "revision": 3}])
        self.assertEqual(("POST", "/api/v1/workspaces/team/sylon/memories/recall", {
            "bindingId": "assistant", "query": "UTC reporting", "limit": 2}), self.server.requests[0])
        self.assertEqual([{"id": "mem-one", "revision": 3}], self.server.requests[1][2]["memories"])
        self.assertNotIn("content", self.server.requests[1][2])
        for selected in ([{"id": "mem-one", "revision": True}], [{"id": "mem-one", "revision": 1, "content": "injected"}],
                         [{"id": "mem-one", "revision": 1}] * 2, [{"id": f"mem-{n}", "revision": 1} for n in range(5)]):
            with self.assertRaises(ValueError):
                self.client.send_sylon_message("team", "chat-one", "Help", idempotency_key="reviewed-context", memories=selected)
        for limit in (0, 5, True):
            with self.assertRaises(ValueError):
                self.client.recall_sylon_memories("team", "assistant", "UTC", limit=limit)
        self.assertEqual(2, len(self.server.requests))

    def test_cli_saved_mutation_file_preserves_intent_and_rejects_authority_injection(self):
        body = {"source": {"jobId": "syl-one", "revision": 7}, "idempotencyKey": "saved-memory-intent",
                "title": "UTC reporting", "content": "Report in UTC.", "expiresAt": EXPIRY}
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "memory.json"
            path.write_text(json.dumps(body))
            self.server.responses = [receipt()]
            args = ["--url", self.url, "sylon", "memory", "save", "--workspace", "team", "--file", str(path)]
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(0, main(args))
            self.assertEqual(body, self.server.requests[0][2])
            body["dependencies"] = []
            path.write_text(json.dumps(body))
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(2, main(args))
        self.assertEqual(1, len(self.server.requests))

    def test_cli_delete_and_message_files_never_substitute_revisions(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "intent.json"
            path.write_text(json.dumps({"idempotencyKey": "delete-memory", "revision": 3}))
            self.server.responses = [receipt("delete", 3, state="deleted")]
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(0, main(["--url", self.url, "sylon", "memory", "delete", "--workspace", "team",
                                          "--id", "mem-one", "--file", str(path)]))
            path.write_text(json.dumps({"idempotencyKey": "memory-context", "prompt": "Help",
                                       "memories": [{"id": "mem-other", "revision": 9}]}))
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(0, main(["--url", self.url, "sylon", "send", "--workspace", "team",
                                          "--conversation", "chat-one", "--file", str(path)]))
        self.assertEqual(3, self.server.requests[0][2]["revision"])
        self.assertEqual([{"id": "mem-other", "revision": 9}], self.server.requests[1][2]["memories"])


if __name__ == "__main__":
    unittest.main()
