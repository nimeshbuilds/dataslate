"""Ingestion submission recovery and writer-drain behavior at the HTTP boundary."""
import contextlib
import io
import json
import threading
import unittest
from http.server import ThreadingHTTPServer
from unittest.mock import patch

from dataslate import Client, DataSlateError
from dataslate.cli import main
from test_execution import ExecutionHandler


class IngestionHandler(ExecutionHandler):
    def do_PUT(self):
        self.do_POST()

    def do_DELETE(self):
        self.server.requests.append((self.command, self.path, None))
        self.send_response(204)
        self.end_headers()


class IngestionTests(unittest.TestCase):
    def setUp(self):
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), IngestionHandler)
        self.server.requests, self.server.responses = [], []
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f'http://127.0.0.1:{self.server.server_port}'
        self.client = Client(self.url)

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def test_pipeline_recovery_keeps_exact_revision_without_latest_lookup(self):
        for _ in range(2):
            with self.assertRaises(DataSlateError) as caught:
                self.client.create_ingestion_run('team', 'ingest', 'pipeline-one', 3,
                                                 idempotency_key='uncertain-request')
            self.assertEqual('connection_error', caught.exception.code)
        self.assertEqual(2, len(self.server.requests))
        self.assertEqual(self.server.requests[0], self.server.requests[1])
        self.assertEqual(('POST', '/api/v1/workspaces/team/runs', {
            'bindingId': 'ingest', 'idempotencyKey': 'uncertain-request',
            'pipeline': {'id': 'pipeline-one', 'revision': 3},
        }), self.server.requests[0])

    def test_invalid_selection_is_rejected_before_network(self):
        for revision in (True, 0, -1, 1.5, '3', None):
            with self.assertRaises(ValueError):
                self.client.create_ingestion_run('team', 'ingest', 'pipeline-one', revision,
                                                 idempotency_key='pipeline-key')
        with self.assertRaises(ValueError):
            self.client.ingestion_pipeline('team', '../foreign')
        with self.assertRaises(ValueError):
            self.client.delete_ingestion_pipeline('team', '../foreign', 3)
        with self.assertRaises(ValueError):
            self.client.delete_ingestion_pipeline('team', 'pipeline-one', True)
        self.assertEqual([], self.server.requests)

    def test_scoped_crud_retains_expected_revision(self):
        self.server.responses = [{'items': []}, {'id': 'pipeline-one', 'revision': 3}]
        self.client.ingestion_pipelines('team')
        self.client.ingestion_pipeline('team', 'pipeline-one')
        body = {'name': 'Orders', 'revision': 3}
        self.client.update_ingestion_pipeline('team', 'pipeline-one', body)
        self.client.delete_ingestion_pipeline('team', 'pipeline-one', 3)
        prefix = '/api/v1/workspaces/team/ingestion-pipelines'
        self.assertEqual([
            ('GET', prefix, None), ('GET', prefix + '/pipeline-one', None),
            ('PUT', prefix + '/pipeline-one', body), ('DELETE', prefix + '/pipeline-one?revision=3', None),
        ], self.server.requests)

    def test_cleanup_watch_keeps_lock_until_both_obligations_complete(self):
        base = {'id': 'run-one', 'state': 'succeeded', 'ingestion': {'id': 'pipeline-one'}}
        self.server.responses = [
            {**base, 'revision': 1, 'writerReleased': False, 'sourceArtifact': {'state': 'cleanup_pending'}},
            {**base, 'revision': 2, 'writerReleased': False, 'sourceArtifact': {'state': 'deleted'}},
            {**base, 'revision': 3, 'writerReleased': True, 'sourceArtifact': {'state': 'deleted'}},
        ]
        with patch('dataslate.client.time.sleep'):
            states = list(self.client.watch_run('team', 'run-one', wait_for_cleanup=True))
        self.assertEqual([1, 2, 3], [s['revision'] for s in states])
        self.assertTrue(all(r[0] == 'GET' for r in self.server.requests))

    def test_cli_pipeline_submission_uses_explicit_revision_and_key(self):
        with contextlib.redirect_stdout(io.StringIO()):
            result = main(['--url', self.url, 'run', 'submit-pipeline', '--workspace', 'team',
                           '--binding', 'ingest', '--pipeline', 'pipeline-one', '--revision', '3',
                           '--idempotency-key', 'pipeline-key'])
        self.assertEqual(0, result)
        self.assertEqual({'id': 'pipeline-one', 'revision': 3}, self.server.requests[0][2]['pipeline'])

    def test_cli_delete_requires_and_transmits_revision(self):
        with contextlib.redirect_stdout(io.StringIO()):
            result = main(['--url', self.url, 'pipeline', 'delete', '--workspace', 'team',
                           '--id', 'pipeline-one', '--revision', '3'])
        self.assertEqual(0, result)
        self.assertEqual([('DELETE', '/api/v1/workspaces/team/ingestion-pipelines/pipeline-one?revision=3', None)], self.server.requests)

    def test_query_values_are_encoded_without_changing_origin_or_path(self):
        self.server.responses = [{}]
        self.client.request('GET', '/api/v1/workspaces', query={'marker':'https://other/?token=a&b=c#d'})
        self.assertEqual('/api/v1/workspaces?marker=https%3A%2F%2Fother%2F%3Ftoken%3Da%26b%3Dc%23d', self.server.requests[0][1])

    def test_cli_cleanup_watch_does_not_claim_failed_run_success(self):
        base = {'id': 'run-one', 'state': 'failed', 'ingestion': {'id': 'pipeline-one'}, 'sourceArtifact': {'state': 'deleted'}}
        self.server.responses = [{**base, 'revision': 3, 'writerReleased': False}, {**base, 'revision': 4, 'writerReleased': True}]
        out = io.StringIO()
        with contextlib.redirect_stdout(out), patch('dataslate.client.time.sleep'):
            result = main(['--url', self.url, 'run', 'watch', '--workspace', 'team', '--id', 'run-one', '--wait-cleanup'])
        self.assertEqual(3, result)
        self.assertEqual(2, len(out.getvalue().splitlines()))
