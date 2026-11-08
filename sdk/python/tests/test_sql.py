"""SQL lifecycle, precise results, and uncertain-submission behavior over HTTP."""
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

class SQLTests(unittest.TestCase):
    def setUp(self):
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), ExecutionHandler)
        self.server.requests, self.server.responses = [], []
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f'http://127.0.0.1:{self.server.server_port}'
        self.client = Client(self.url)
    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
    def test_uncertain_submission_replays_exact_selection(self):
        for _ in range(2):
            with self.assertRaises(DataSlateError) as error:
                self.client.create_query('team', 'warehouse', 'book', 3, 'sql-cell', idempotency_key='uncertain-request')
            self.assertEqual('connection_error', error.exception.code)
        self.assertEqual(2, len(self.server.requests))
        self.assertEqual(self.server.requests[0], self.server.requests[1])
        self.assertEqual(('POST', '/api/v1/workspaces/team/queries', {
            'bindingId': 'warehouse', 'idempotencyKey': 'uncertain-request',
            'notebook': {'id': 'book', 'revision': 3, 'cellId': 'sql-cell'},
        }), self.server.requests[0])
    def test_invalid_selection_stays_off_network(self):
        for revision in (True, 0, -1, 1.5, '3', None):
            with self.assertRaises(ValueError):
                self.client.create_query('team', 'warehouse', 'book', revision, 'sql-cell', idempotency_key='valid-key')
        for cell in ('', '.', '..', '../other'):
            with self.assertRaises(ValueError):
                self.client.create_query('team', 'warehouse', 'book', 3, cell, idempotency_key='valid-key')
        self.assertEqual([], self.server.requests)
    def test_result_preserves_exact_values_and_incomplete_flag(self):
        result = {'queryId':'qry-one', 'workspaceId':'team', 'columns':[{'name':'n','type':'decimal(38,4)'}],
                  'rows':[['9999999999999999999999999999999.1234'], [None]],
                  'rowCount':2, 'truncated':True, 'complete':False}
        self.server.responses = [result]
        self.assertEqual(result, self.client.query_result('team','qry-one'))
    def test_watch_observes_cancellation_instead_of_acknowledgement(self):
        self.server.responses = [{'id':'qry-one','workspaceId':'team','state':state,'revision':i+1}
                                 for i,state in enumerate(('cancel_requested','unknown','cancelled'))]
        with patch('dataslate.client.time.sleep'):
            states = list(self.client.watch_query('team','qry-one'))
        self.assertEqual(['cancel_requested','unknown','cancelled'],[s['state'] for s in states])
        self.assertTrue(all(r[0]=='GET' for r in self.server.requests))
    def test_watch_rejects_foreign_workspace(self):
        self.server.responses = [{'id':'qry-one','workspaceId':'foreign','state':'succeeded','revision':1}]
        with self.assertRaises(DataSlateError) as error:
            list(self.client.watch_query('team','qry-one'))
        self.assertEqual('invalid_response', error.exception.code)
    def test_cli_submit_and_failed_watch(self):
        with contextlib.redirect_stdout(io.StringIO()):
            code = main(['--url',self.url,'query','submit','--workspace','team','--binding','warehouse',
                         '--notebook','book','--revision','3','--cell','sql-cell','--idempotency-key','submit-key'])
        self.assertEqual(0,code)
        self.assertEqual({'id':'book','revision':3,'cellId':'sql-cell'},self.server.requests[0][2]['notebook'])
        self.server.responses = [{'id':'qry-one','workspaceId':'team','state':'failed','revision':2}]
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(3,main(['--url',self.url,'query','watch','--workspace','team','--id','qry-one']))
    def test_cli_result_does_not_round_numbers(self):
        self.server.responses = [{'rows':[['9007199254740993']], 'complete':True,'truncated':False}]
        out=io.StringIO()
        with contextlib.redirect_stdout(out):
            self.assertEqual(0,main(['--url',self.url,'query','result','--workspace','team','--id','qry-one']))
        self.assertEqual([['9007199254740993']],json.loads(out.getvalue())['rows'])
