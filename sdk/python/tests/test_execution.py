"""Client behavior at the transport boundary, including uncertain submissions."""
import contextlib
import io
import json
import socket
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

from dataslate import Client, DataSlateError
from dataslate.cli import main


class ExecutionHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        payload = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        self.server.requests.append((self.command, self.path, payload))
        if payload.get('idempotencyKey') == 'uncertain-request':
            self.close_connection = True
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()
            return
        self.send_response(202)
        self.end_headers()
        self.wfile.write(json.dumps({'id': 'run-one', 'state': 'cancel_requested' if self.path.endswith('/cancel') else 'queued', 'revision': 1}).encode())

    def do_GET(self):
        self.server.requests.append((self.command, self.path, None))
        self.send_response(200)
        self.end_headers()
        self.wfile.write(json.dumps(self.server.responses.pop(0)).encode())


class ExecutionTests(unittest.TestCase):
    def setUp(self):
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), ExecutionHandler)
        self.server.requests = []
        self.server.responses = []
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f'http://127.0.0.1:{self.server.server_port}'
        self.client = Client(self.url)

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def test_submit_disconnect_does_not_retry_or_change_key(self):
        for _ in range(2):
            with self.assertRaises(DataSlateError) as caught:
                self.client.create_run('team', 'application', idempotency_key='uncertain-request')
            self.assertEqual('connection_error', caught.exception.code)
        self.assertEqual(2, len(self.server.requests))
        self.assertEqual(self.server.requests[0], self.server.requests[1])
        self.assertEqual({'bindingId': 'application', 'idempotencyKey': 'uncertain-request'}, self.server.requests[0][2])

    def test_invalid_key_and_resource_paths_do_not_send(self):
        for key in ('short', 'has space', 'a' * 129, 'bad\nheader'):
            with self.assertRaises(ValueError):
                self.client.create_run('team', 'app', idempotency_key=key)
        for operation in (self.client.run, self.client.cancel_run, self.client.run_logs, self.client.run_events):
            with self.assertRaises(ValueError):
                operation('team', '../foreign')
        self.assertEqual([], self.server.requests)

    def test_watch_cancellation_request_is_not_terminal(self):
        self.server.responses = [
            {'id': 'run-one', 'state': 'cancel_requested', 'revision': 1},
            {'id': 'run-one', 'state': 'cancel_requested', 'revision': 1},
            {'id': 'run-one', 'state': 'cancelled', 'revision': 2},
        ]
        with patch('dataslate.client.time.sleep'):
            states = list(self.client.watch_run('team', 'run-one'))
        self.assertEqual(['cancel_requested', 'cancelled'], [s['state'] for s in states])
        self.assertTrue(all(method == 'GET' for method, _, _ in self.server.requests))

    def test_watch_budget_does_not_cancel_or_resubmit(self):
        self.server.responses = [{'id': 'run-one', 'state': 'unknown', 'revision': 1}]
        with patch('dataslate.client.time.monotonic', side_effect=[10, 20]), self.assertRaises(DataSlateError) as caught:
            list(self.client.watch_run('team', 'run-one', max_wait=1))
        self.assertEqual('watch_timeout', caught.exception.code)
        self.assertEqual([('GET', '/api/v1/workspaces/team/runs/run-one', None)], self.server.requests)

    def test_invalid_watch_budget(self):
        for value in (0, -1, float('nan'), float('inf')):
            with self.assertRaises(ValueError):
                list(self.client.watch_run('team', 'run-one', max_wait=value))
        self.assertEqual([], self.server.requests)

    def test_watch_rejects_foreign_or_malformed_success(self):
        self.server.responses = [
            {'id': 'run-other', 'state': 'succeeded', 'revision': 1},
            {'id': 'run-one', 'state': 'succeeded', 'revision': True},
            {'id': 'run-one', 'state': 'invented', 'revision': 1},
        ]
        for _ in range(3):
            with self.assertRaises(DataSlateError) as caught:
                list(self.client.watch_run('team', 'run-one'))
            self.assertEqual('invalid_response', caught.exception.code)

    def test_notebook_submission_pins_selection_without_reading_latest(self):
        self.client.create_notebook_run('team', 'engineering', 'nb-example', 7,
                                        idempotency_key='notebook-request-7')
        self.assertEqual([('POST', '/api/v1/workspaces/team/runs', {
            'bindingId': 'engineering', 'idempotencyKey': 'notebook-request-7',
            'notebook': {'id': 'nb-example', 'revision': 7},
        })], self.server.requests)

    def test_notebook_uncertain_outcome_retains_exact_request(self):
        for _ in range(2):
            with self.assertRaises(DataSlateError):
                self.client.create_notebook_run('team', 'engineering', 'nb-example', 7,
                                                idempotency_key='uncertain-request')
        self.assertEqual(2, len(self.server.requests))
        self.assertEqual(self.server.requests[0], self.server.requests[1])
        self.assertEqual({'id': 'nb-example', 'revision': 7}, self.server.requests[0][2]['notebook'])

    def test_invalid_notebook_selection_does_not_send(self):
        for revision in (0, -1, 1.5, True, '7', None):
            with self.assertRaises(ValueError):
                self.client.create_notebook_run('team', 'engineering', 'nb-example', revision,
                                                idempotency_key='notebook-request-7')
        with self.assertRaises(ValueError):
            self.client.create_notebook_run('team', 'engineering', '../other', 7,
                                            idempotency_key='notebook-request-7')
        self.assertEqual([], self.server.requests)

    def test_cli_saved_notebook_submission(self):
        with contextlib.redirect_stdout(io.StringIO()):
            result = main(['--url', self.url, 'run', 'submit-notebook', '--workspace', 'team',
                           '--binding', 'engineering', '--notebook', 'nb-example', '--revision', '7',
                           '--idempotency-key', 'notebook-request-7'])
        self.assertEqual(0, result)
        self.assertEqual({'id': 'nb-example', 'revision': 7}, self.server.requests[0][2]['notebook'])

    def test_cli_watch_failure_exit_and_json_lines(self):
        self.server.responses = [{'id': 'run-one', 'state': 'failed', 'revision': 2}]
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            result = main(['--url', self.url, 'run', 'watch', '--workspace', 'team', '--id', 'run-one'])
        self.assertEqual(3, result)
        self.assertEqual('failed', json.loads(out.getvalue())['state'])

    def test_cli_cancel_does_not_claim_job_stopped(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            result = main(['--url', self.url, 'run', 'cancel', '--workspace', 'team', '--id', 'run-one'])
        self.assertEqual(0, result)
        self.assertEqual('cancel_requested', json.loads(out.getvalue())['state'])
        self.assertEqual([('POST', '/api/v1/workspaces/team/runs/run-one/cancel', {})], self.server.requests)

    def test_all_reads_use_workspace_scoped_routes(self):
        operations = [
            (lambda: self.client.execution_bindings('team'), '/execution-bindings'),
            (lambda: self.client.runs('team'), '/runs'),
            (lambda: self.client.run('team', 'run-one'), '/runs/run-one'),
            (lambda: self.client.run_events('team', 'run-one'), '/runs/run-one/events'),
            (lambda: self.client.run_logs('team', 'run-one'), '/runs/run-one/logs'),
            (lambda: self.client.run_source('team', 'run-one'), '/runs/run-one/source'),
        ]
        self.server.responses = [{} for _ in operations]
        for operation, suffix in operations:
            operation()
            self.assertEqual(('GET', '/api/v1/workspaces/team' + suffix, None), self.server.requests[-1])
