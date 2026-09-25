import contextlib
import io
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import tempfile
from threading import Thread
import unittest

from dataslate import Client, DataSlateError
from dataslate.cli import main


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        self.server.requests.append((self.command, self.path, self.headers.get("Authorization")))
        if self.path.endswith("/bad-status"):
            self.wfile.write(b"INVALID STATUS\r\n\r\n")
            self.close_connection = True
            return
        if self.path.endswith(("/truncated", "/truncated-error")):
            self.send_response(500 if self.path.endswith("/truncated-error") else 200)
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            self.wfile.write(b"10\r\nshort")
            self.close_connection = True
            return
        if self.path.endswith("/modules"):
            self.send_response(302)
            self.send_header("Location", self.server.base_url + "/redirected")
            self.end_headers()
            return
        if self.path.endswith("/workspaces"):
            self.send_response(403)
            self.end_headers()
            self.wfile.write(json.dumps({"error": {"code": "forbidden", "message": "Workspace access denied"}}).encode())
            return
        self.send_response(200)
        self.end_headers()
        self.wfile.write(json.dumps({"product": "DataSlate", "assistant": "Sylon", "executionEnabled": False}).encode())

    def do_POST(self):
        self.server.requests.append((self.command, self.path, self.headers.get("Authorization")))
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
        self.send_response(200)
        self.end_headers()
        self.wfile.write(json.dumps({"manifest": body, "submitted": False}).encode())


class ClientTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.server.base_url = "http://127.0.0.1:" + str(cls.server.server_address[1])
        cls.server.requests = []
        cls.thread = Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def setUp(self):
        self.server.requests.clear()
        self.client = Client(self.server.base_url, token="test-only-token")

    def test_token_sent_only_as_header(self):
        self.assertEqual("Sylon", self.client.status()["assistant"])
        self.assertEqual([("GET", "/api/v1/status", "Bearer test-only-token")], self.server.requests)
        self.assertNotIn("test-only-token", repr(self.client))

    def test_structured_denial_not_retried(self):
        with self.assertRaises(DataSlateError) as caught:
            self.client.workspaces()
        self.assertEqual((403, "forbidden"), (caught.exception.status, caught.exception.code))
        self.assertEqual(1, len(self.server.requests))

    def test_redirect_never_followed(self):
        with self.assertRaises(DataSlateError) as caught:
            self.client.modules()
        self.assertEqual(302, caught.exception.status)
        self.assertEqual(1, len(self.server.requests))

    def test_response_limit(self):
        with self.assertRaises(DataSlateError) as caught:
            Client(self.server.base_url, max_response_bytes=10).status()
        self.assertEqual("response_too_large", caught.exception.code)

    def test_broken_transport_always_has_structured_error(self):
        for path in ("truncated", "truncated-error", "bad-status"):
            with self.subTest(path=path), self.assertRaises(DataSlateError):
                self.client.request("GET", "/api/v1/" + path)

    def test_untrusted_transport_rejected(self):
        for url in ("http://example.com", "https://user:secret@example.com", "https://example.com?token=x", "file:///tmp/data", "https://example.com/api"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                Client(url)
        with self.assertRaises(ValueError):
            Client(token="x\r\nAuthorization: injected")
        with self.assertRaises(ValueError):
            self.client.notebook("default", "../other")

    def test_cli_spark_output_remains_preview(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "spark.json"
            path.write_text(json.dumps({"name": "preview-job", "operator": "apache"}))
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                code = main(["--url", self.server.base_url, "spark", "preview", "--workspace", "default", "--file", str(path)])
        self.assertEqual(0, code)
        self.assertFalse(json.loads(output.getvalue())["submitted"])
        self.assertEqual("/api/v1/workspaces/default/spark-applications/preview", self.server.requests[0][1])

    def test_cli_denial_is_machine_readable(self):
        output = io.StringIO()
        with contextlib.redirect_stderr(output):
            code = main(["--url", self.server.base_url, "workspace", "list"])
        self.assertEqual(1, code)
        self.assertEqual("forbidden", json.loads(output.getvalue())["error"]["code"])


if __name__ == "__main__":
    unittest.main()
