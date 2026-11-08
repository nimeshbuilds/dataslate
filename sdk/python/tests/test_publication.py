"""Publication recovery, receipt integrity, and explicit SQL handoff over HTTP."""
import contextlib
import io
import json
import socket
import threading
import unittest
from http.server import ThreadingHTTPServer
from unittest.mock import patch

from dataslate import Client, DataSlateError
from dataslate.cli import main
from test_execution import ExecutionHandler


def publication(state='queued', revision=1):
    value = {'id':'pub-one','workspaceId':'team','runId':'run-one','bindingId':'publish-orders',
             'revision':revision,'state':state,'registrationStarted':state!='queued',
             'reconciliationComplete':state in ('published','failed','unknown'),
             'deltaVersion':'9007199254740993'}
    if state=='published':
        value.update(publishedTableId='publish-orders',verifiedAt='2026-11-08T00:00:00Z')
    return value


class PublicationHandler(ExecutionHandler):
    def do_POST(self):
        body=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        self.server.requests.append((self.command,self.path,body))
        if body.get('idempotencyKey')=='uncertain-request':
            self.close_connection=True
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()
            return
        self.send_response(202)
        self.end_headers()
        self.wfile.write(json.dumps(self.server.responses.pop(0)).encode())


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.server=ThreadingHTTPServer(('127.0.0.1',0),PublicationHandler)
        self.server.requests,self.server.responses=[],[]
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
        self.url=f'http://127.0.0.1:{self.server.server_port}'
        self.client=Client(self.url)

    def tearDown(self):
        self.server.shutdown();self.server.server_close();self.thread.join()

    def test_unknown_acceptance_preserves_exact_request_without_ingestion_retry(self):
        for _ in range(2):
            with self.assertRaises(DataSlateError) as caught:
                self.client.create_table_publication('team','run-one','publish-orders',idempotency_key='uncertain-request')
            self.assertEqual('connection_error',caught.exception.code)
        self.assertEqual(2,len(self.server.requests))
        self.assertEqual(self.server.requests[0],self.server.requests[1])
        self.assertEqual(('POST','/api/v1/workspaces/team/table-publications',{
            'runId':'run-one','bindingId':'publish-orders','idempotencyKey':'uncertain-request'}),self.server.requests[0])

    def test_acceptance_is_not_publication_and_version_remains_exact(self):
        self.server.responses=[{'publication':publication(),'replayed':False}]
        value=self.client.create_table_publication('team','run-one','publish-orders',idempotency_key='publish-key')
        self.assertEqual('queued',value['publication']['state'])
        self.assertEqual('9007199254740993',value['publication']['deltaVersion'])
        self.assertEqual(1,len(self.server.requests))

    def test_mismatched_and_malformed_acceptance_never_becomes_success(self):
        for field,value in [('workspaceId','other'),('runId','other'),('bindingId','other'),('revision',True),
                            ('state','succeeded'),('state',{}),('deltaVersion',9007199254740993),('deltaVersion','01')]:
            self.server.responses=[{'publication':publication()|{field:value},'replayed':False}]
            with self.assertRaises(DataSlateError) as caught:
                self.client.create_table_publication('team','run-one','publish-orders',idempotency_key='publish-key')
            self.assertEqual('invalid_response',caught.exception.code)
        self.server.responses=[{'publication':publication(),'replayed':'false'}]
        with self.assertRaises(DataSlateError):
            self.client.create_table_publication('team','run-one','publish-orders',idempotency_key='publish-key')

    def test_invalid_selection_is_rejected_before_network(self):
        for w,r,b,k in [('team','../run','binding','publish-key'),('team','run','../binding','publish-key'),
                        ('../team','run','binding','publish-key'),('team','run','binding','short')]:
            with self.assertRaises(ValueError):self.client.create_table_publication(w,r,b,idempotency_key=k)
        self.assertEqual([],self.server.requests)

    def test_watch_verification_not_registration_is_success(self):
        self.server.responses=[publication('registering',2),publication('verifying',3),publication('published',4)]
        with patch('dataslate.client.time.sleep'):
            states=list(self.client.watch_table_publication('team','pub-one'))
        self.assertEqual(['registering','verifying','published'],[v['state'] for v in states])
        self.assertTrue(all(method=='GET' for method,_,_ in self.server.requests))

    def test_watch_unknown_stops_without_mutation_and_cli_exits_four(self):
        self.server.responses=[publication('unknown',3)]
        output=io.StringIO()
        with contextlib.redirect_stdout(output):
            status=main(['--url',self.url,'publication','watch','--workspace','team','--id','pub-one'])
        self.assertEqual(4,status)
        self.assertEqual('unknown',json.loads(output.getvalue())['state'])
        self.assertEqual([('GET','/api/v1/workspaces/team/table-publications/pub-one',None)],self.server.requests)

    def test_unknown_with_active_readback_is_watched_to_verified_publication(self):
        self.server.responses=[publication('unknown',3)|{'reconciliationComplete':False},publication('published',4)]
        with patch('dataslate.client.time.sleep'):
            states=list(self.client.watch_table_publication('team','pub-one'))
        self.assertEqual(['unknown','published'],[v['state'] for v in states])
        self.assertTrue(all(method=='GET' for method,_,_ in self.server.requests))

    def test_watch_rejects_unverified_or_wrong_resource_success(self):
        good=publication('published',4)
        cases=[good|{'id':'other'},good|{'publishedTableId':'other'},good|{'verifiedAt':None},
               good|{'registrationStarted':'true'},good|{'deltaVersion':'9223372036854775808'},
               good|{'reconciliationComplete':False},good|{'reconciliationComplete':'true'},
               good|{'state':'registering'}]
        for value in cases:
            self.server.responses=[value]
            with self.assertRaises(DataSlateError):list(self.client.watch_table_publication('team','pub-one'))

    def test_watch_timeout_and_regression_do_not_mutate(self):
        self.server.responses=[publication('checking',2)]
        with patch('dataslate.client.time.monotonic',side_effect=[10,20]),self.assertRaises(DataSlateError) as caught:
            list(self.client.watch_table_publication('team','pub-one',max_wait=1))
        self.assertEqual('watch_timeout',caught.exception.code)
        self.server.responses=[publication('checking',3),publication('checking',2)]
        with patch('dataslate.client.time.sleep'),self.assertRaises(DataSlateError) as caught:
            list(self.client.watch_table_publication('team','pub-one'))
        self.assertEqual('invalid_response',caught.exception.code)
        self.assertTrue(all(method=='GET' for method,_,_ in self.server.requests))

    def test_table_draft_and_outcome_commands_do_not_execute(self):
        draft={'id':'publish-orders','queryMode':'latest','sql':{'draft':'SELECT * FROM "lake"."team"."orders" LIMIT 100'}}
        self.server.responses=[draft,{'runId':'run-one','state':'unavailable'}]
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(0,main(['--url',self.url,'table','get','--workspace','team','--id','publish-orders']))
            self.assertEqual(0,main(['--url',self.url,'run','outcome','--workspace','team','--id','run-one']))
        self.assertEqual([('GET','/api/v1/workspaces/team/published-tables/publish-orders',None),
                          ('GET','/api/v1/workspaces/team/runs/run-one/ingestion-outcome',None)],self.server.requests)

    def test_cli_published_and_failed_exit_codes(self):
        for state,expected in [('published',0),('failed',3)]:
            self.server.responses=[publication(state,4)]
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(expected,main(['--url',self.url,'publication','watch','--workspace','team','--id','pub-one']))

    def test_invalid_watch_budget_sends_nothing(self):
        for value in (True,None,'1',0,-1,float('nan'),float('inf')):
            with self.assertRaises(ValueError):list(self.client.watch_table_publication('team','pub-one',max_wait=value))
        self.assertEqual([],self.server.requests)
