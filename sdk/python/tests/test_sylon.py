"""Private assistant intent, explicit apply and transport-recovery boundaries."""
import contextlib
import io
import json
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch
from dataslate import Client, DataSlateError
from dataslate.cli import main
from test_execution import ExecutionHandler


class SylonTests(unittest.TestCase):
    def setUp(self):
        self.server = ThreadingHTTPServer(('127.0.0.1',0),ExecutionHandler)
        self.server.requests,self.server.responses = [],[]
        self.thread = threading.Thread(target=self.server.serve_forever,daemon=True)
        self.thread.start()
        self.url = f'http://127.0.0.1:{self.server.server_port}'
        self.client = Client(self.url)

    def tearDown(self):
        self.server.shutdown(); self.server.server_close(); self.thread.join()

    def test_uncertain_generation_retains_context_and_key(self):
        context = {'id':'nb-one','revision':7,'cellId':'cell-one'}
        for _ in range(2):
            with self.assertRaises(DataSlateError) as error:
                self.client.send_sylon_message('team','chat-one','Explain this cell',
                    idempotency_key='uncertain-request',notebook=context)
            self.assertEqual('connection_error',error.exception.code)
        self.assertEqual(2,len(self.server.requests))
        self.assertEqual(self.server.requests[0],self.server.requests[1])
        self.assertEqual(context,self.server.requests[0][2]['notebook'])

    def test_invalid_context_never_leaves_client(self):
        for context in ({'id':'nb','revision':True,'cellId':'cell'}, {'id':'nb','revision':1,'cellId':'cell','source':'secret'},
                        {'id':123,'revision':1,'cellId':'cell'}):
            with self.assertRaises(ValueError):
                self.client.send_sylon_message('team','chat','Help',idempotency_key='valid-key',notebook=context)
        for prompt in ('', '😀'*2049, 'bad\x00prompt'):
            with self.assertRaises(ValueError):
                self.client.send_sylon_message('team','chat',prompt,idempotency_key='valid-key')
        with self.assertRaises(ValueError):
            self.client.send_sylon_message('team','chat','Help',idempotency_key='valid-key',
                                           search={'bindingId':'search','query':'orders','mode':[]})
        self.assertEqual([],self.server.requests)

    def test_catalog_context_keeps_exact_component_boundaries(self):
        context = [{'bindingId':'polaris','namespace':['engineering','quarter.one'],'name':'orders'}]
        self.client.send_sylon_message('team','chat','Describe orders',idempotency_key='metadata-key',catalogs=context)
        self.assertEqual(context,self.server.requests[0][2]['catalogs'])

    def test_apply_is_one_exact_request_and_does_not_execute(self):
        self.client.apply_sylon_proposal('team','syl-one','nb-one',7,'cell-one')
        self.assertEqual([('POST','/api/v1/workspaces/team/sylon/jobs/syl-one/apply',
                          {'notebookId':'nb-one','revision':7,'cellId':'cell-one'})],self.server.requests)

    def test_watch_cancel_intent_is_not_terminal_and_unknown_stops(self):
        self.server.responses = [{'id':'syl-one','workspaceId':'team','state':state,'revision':i+1}
                                 for i,state in enumerate(('generating','cancel_requested','unknown'))]
        with patch('dataslate.client.time.sleep'):
            states = list(self.client.watch_sylon_job('team','syl-one'))
        self.assertEqual(['generating','cancel_requested','unknown'],[s['state'] for s in states])
        self.assertTrue(all(r[0]=='GET' for r in self.server.requests))

    def test_watch_rejects_foreign_or_malformed_identity(self):
        for override in ({'workspaceId':'foreign'},{'id':'syl-other'},{'revision':False},{'state':'done'}):
            self.server.responses = [dict({'id':'syl-one','workspaceId':'team','state':'succeeded','revision':2},**override)]
            with self.assertRaises(DataSlateError) as error:
                list(self.client.watch_sylon_job('team','syl-one'))
            self.assertEqual('invalid_response',error.exception.code)

    def test_cli_message_file_keeps_intent_and_unknown_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'message.json'
            path.write_text(json.dumps({'prompt':'Help with this SQL','idempotencyKey':'saved-intent',
                                       'notebook':{'id':'nb-one','revision':7,'cellId':'sql-one'}}))
            with contextlib.redirect_stdout(io.StringIO()):
                code = main(['--url',self.url,'sylon','send','--workspace','team','--conversation','chat-one','--file',str(path)])
            self.assertEqual(0,code)
        self.assertEqual('saved-intent',self.server.requests[0][2]['idempotencyKey'])
        self.assertEqual(7,self.server.requests[0][2]['notebook']['revision'])
        self.server.responses = [{'id':'syl-one','workspaceId':'team','state':'unknown','revision':2}]
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(3,main(['--url',self.url,'sylon','watch','--workspace','team','--id','syl-one']))

    def test_cli_rejects_provider_injection_in_message_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'message.json'
            path.write_text(json.dumps({'prompt':'Help','idempotencyKey':'valid-key','endpoint':'https://other.example'}))
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(2,main(['--url',self.url,'sylon','send','--workspace','team','--conversation','chat-one','--file',str(path)]))
        self.assertEqual([],self.server.requests)
