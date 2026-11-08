"""Search source identity, transfer intent and uncertain-index behavior over HTTP."""
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


class SearchHandler(ExecutionHandler):
    def do_POST(self):
        if self.path.endswith('/search'):
            body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            self.server.requests.append((self.command,self.path,body))
            self.send_response(200)
            self.end_headers()
            self.wfile.write(json.dumps(self.server.responses.pop(0)).encode())
        else:
            super().do_POST()


class SearchTests(unittest.TestCase):
    def setUp(self):
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), SearchHandler)
        self.server.requests, self.server.responses = [], []
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f'http://127.0.0.1:{self.server.server_port}'
        self.client = Client(self.url)

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def test_query_keeps_transfer_mode_and_provenance(self):
        result = {'bindingId':'search','workspaceId':'team','mode':'hybrid','query':'quarterly revenue',
                  'items':[{'documentId':'sdoc-one','kind':'notebook','title':'Quarterly report',
                            'snippet':'Revenue','score':0.032,'notebook':{'id':'book','revision':9},
                            'indexedAt':'2026-11-07T12:00:00Z'}],'complete':True}
        self.server.responses = [result]
        self.assertEqual(result, self.client.search('team','search','quarterly revenue',mode='hybrid',limit=4))
        self.assertEqual(('POST','/api/v1/workspaces/team/search',
                          {'bindingId':'search','query':'quarterly revenue','mode':'hybrid','limit':4}),self.server.requests[0])

    def test_invalid_queries_and_selections_do_not_transfer(self):
        for query in ('', '   ', '😀' * 1025, None):
            with self.assertRaises(ValueError): self.client.search('team','search',query)
        for limit in (True,0,21,1.2):
            with self.assertRaises(ValueError): self.client.search('team','search','revenue',limit=limit)
        for revision in (True,0,-1,1.5):
            with self.assertRaises(ValueError):
                self.client.index_notebook('team','search','book',revision,idempotency_key='source-key')
        with self.assertRaises(ValueError):
            self.client.index_catalog_table('team','search','catalog',['bad\x1fpart'],'orders',idempotency_key='table-key')
        self.assertEqual([],self.server.requests)

    def test_unknown_submission_never_generates_replacement_key(self):
        for _ in range(2):
            with self.assertRaises(DataSlateError) as error:
                self.client.index_notebook('team','search','book',7,idempotency_key='uncertain-request')
            self.assertEqual('connection_error',error.exception.code)
        self.assertEqual(2,len(self.server.requests))
        self.assertEqual(self.server.requests[0],self.server.requests[1])
        self.assertEqual({'bindingId':'search','idempotencyKey':'uncertain-request',
                          'notebook':{'id':'book','revision':7}},self.server.requests[0][2])

    def test_watch_unknown_provider_outcome_is_terminal(self):
        self.server.responses = [{'id':'idx-one','workspaceId':'team','revision':i+1,'state':state}
                                 for i,state in enumerate(('queued','indexing','unknown'))]
        with patch('dataslate.client.time.sleep'):
            states = list(self.client.watch_search_index('team','idx-one'))
        self.assertEqual(['queued','indexing','unknown'],[state['state'] for state in states])
        self.assertTrue(all(request[0]=='GET' for request in self.server.requests))

    def test_watch_rejects_wrong_scope_and_invalid_states(self):
        for changed in ({'workspaceId':'foreign'},{'id':'idx-other'},{'state':'invented'},{'revision':True}):
            self.server.responses = [dict({'id':'idx-one','workspaceId':'team','state':'succeeded','revision':1},**changed)]
            with self.assertRaises(DataSlateError) as error:
                list(self.client.watch_search_index('team','idx-one'))
            self.assertEqual('invalid_response',error.exception.code)

    def test_watch_timeout_does_not_cancel_or_repeat_mutation(self):
        self.server.responses = [{'id':'idx-one','workspaceId':'team','state':'indexing','revision':2}]
        with patch('dataslate.client.time.monotonic',side_effect=[0,2]):
            with self.assertRaises(DataSlateError) as error:
                list(self.client.watch_search_index('team','idx-one',max_wait=1))
        self.assertEqual('watch_timeout',error.exception.code)
        self.assertEqual(1,len(self.server.requests))
        self.assertEqual('GET',self.server.requests[0][0])

    def test_cli_table_source_and_failed_watch(self):
        with contextlib.redirect_stdout(io.StringIO()):
            result = main(['--url',self.url,'search','index-table','--workspace','team','--binding','search',
                           '--catalog-binding','polaris','--namespace','["finance","quarter.one"]',
                           '--name','orders','--idempotency-key','table-key'])
        self.assertEqual(0,result)
        self.assertEqual({'bindingId':'polaris','namespace':['finance','quarter.one'],'name':'orders'},
                         self.server.requests[0][2]['catalog'])
        self.server.responses = [{'id':'idx-one','workspaceId':'team','state':'superseded','revision':2}]
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(3,main(['--url',self.url,'search','watch','--workspace','team','--id','idx-one']))

    def test_cli_default_is_local_lexical_search(self):
        output = io.StringIO()
        self.server.responses = [{'items':[],'mode':'lexical','complete':True}]
        with contextlib.redirect_stdout(output):
            code = main(['--url',self.url,'search','query','--workspace','team','--binding','search','--query','orders'])
        self.assertEqual(0,code)
        self.assertEqual('lexical',self.server.requests[0][2]['mode'])
        self.assertEqual([],json.loads(output.getvalue())['items'])
